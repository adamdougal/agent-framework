# Copyright (c) Microsoft. All rights reserved.

"""Behaviour of the shared GA-protocol realtime session, driven through a scripted in-memory connection."""

import asyncio
import base64
import contextlib
import json
import subprocess
import sys
from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
from typing import Any

import pytest
from agent_framework import (
    Content,
    Message,
    RealtimeEvent,
    RealtimeNoiseReduction,
    RealtimeOptions,
    RealtimeTranscription,
    RealtimeTurnDetection,
    tool,
)
from agent_framework.exceptions import (
    RealtimeActiveResponseException,
    RealtimeConnectionException,
    RealtimeControlException,
)

from agent_framework_openai._realtime_session import OpenAIRealtimeSession

Responder = Callable[[dict[str, Any]], Sequence[Mapping[str, Any]]]


def acknowledge_session_updates(event: dict[str, Any]) -> list[Mapping[str, Any]]:
    """Answer every session.update with a session.updated echoing its session, as the services do."""
    if event.get("type") == "session.update":
        return [{"type": "session.updated", "event_id": "evt_srv", "session": event.get("session", {})}]
    return []


class FakeConnection:
    """A scripted OpenAIRealtimeConnection: records what's sent, and serves pushed server events in order."""

    def __init__(self, responder: Responder | None = acknowledge_session_updates) -> None:
        self.sent: list[dict[str, Any]] = []
        self.close_count = 0
        self.responder = responder
        self._incoming: asyncio.Queue[Mapping[str, Any] | BaseException | None] = asyncio.Queue()

    async def send(self, event: Mapping[str, Any]) -> None:
        recorded = json.loads(json.dumps(event))
        self.sent.append(recorded)
        if self.responder is not None:
            for reply in self.responder(recorded):
                self.push(reply)

    async def receive(self) -> Mapping[str, Any] | None:
        item = await self._incoming.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def close(self) -> None:
        self.close_count += 1

    def push(self, *events: Mapping[str, Any]) -> None:
        for event in events:
            self._incoming.put_nowait(event)

    def end(self) -> None:
        self._incoming.put_nowait(None)

    def fail(self, error: BaseException) -> None:
        self._incoming.put_nowait(error)

    def sent_types(self) -> list[str]:
        return [event["type"] for event in self.sent]

    async def wait_sent(self, event_type: str, timeout: float = 1.0) -> dict[str, Any]:
        """Wait for the next event of ``event_type`` sent after this call, and return it."""
        already_sent = len(self.sent)

        async def poll() -> dict[str, Any]:
            while True:
                for event in self.sent[already_sent:]:
                    if event["type"] == event_type:
                        return event
                await asyncio.sleep(0.005)

        return await asyncio.wait_for(poll(), timeout)


def without_event_ids(events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Sent events without the client event_ids the session adds."""
    return [{key: value for key, value in event.items() if key != "event_id"} for event in events]


def request_id_of(request: Mapping[str, Any]) -> str:
    """The af_request_id a response.create request carries in its metadata."""
    return request["response"]["metadata"]["af_request_id"]


def response_created(response_id: str, request_id: str | None = None, **response: Any) -> dict[str, Any]:
    """A response.created event, echoing ``request_id`` in its metadata when given."""
    metadata = {"af_request_id": request_id} if request_id is not None else None
    return {
        "type": "response.created",
        "event_id": f"evt_{response_id}_created",
        "response": {"id": response_id, "status": "in_progress", "metadata": metadata, **response},
    }


def response_done(response_id: str, status: str = "completed", **response: Any) -> dict[str, Any]:
    """A response.done event."""
    return {
        "type": "response.done",
        "event_id": f"evt_{response_id}_done",
        "response": {"id": response_id, "status": status, **response},
    }


def item_truncated(item_id: str, content_index: int, audio_end_ms: int) -> dict[str, Any]:
    """A conversation.item.truncated event."""
    return {
        "type": "conversation.item.truncated",
        "event_id": f"evt_{item_id}_truncated",
        "item_id": item_id,
        "content_index": content_index,
        "audio_end_ms": audio_end_ms,
    }


async def next_event(events: Any, timeout: float = 1.0) -> Any:
    """The next realtime event from a receive() iterator, failing the test instead of hanging."""
    return await asyncio.wait_for(anext(events), timeout)


async def started(
    options: RealtimeOptions | None = None, tools: Sequence[Any] | None = None
) -> tuple[OpenAIRealtimeSession, FakeConnection]:
    """A session opened with ``options`` and ``tools`` over a connection that acknowledges every update."""
    connection = FakeConnection()
    session = OpenAIRealtimeSession(connection)
    await asyncio.wait_for(session.start(options=options, tools=tools), 1)
    return session, connection


async def initial_session(options: RealtimeOptions, tools: Sequence[Any] | None = None) -> dict[str, Any]:
    """The session object of the first session.update that start() sends."""
    _, connection = await started(options, tools)
    return connection.sent[0]["session"]


class Reader:
    """Reads a session's events in the background, as the conversation's receive loop does."""

    def __init__(self, session: OpenAIRealtimeSession) -> None:
        self.events: list[Any] = []
        self.error: BaseException | None = None
        self._task = asyncio.create_task(self._read(session))

    async def _read(self, session: OpenAIRealtimeSession) -> None:
        try:
            async for event in session.receive():
                self.events.append(event)
        except Exception as error:
            self.error = error

    def kinds(self) -> list[str]:
        return [event.kind for event in self.events]

    def of_kind(self, kind: str) -> list[Any]:
        return [event for event in self.events if event.kind == kind]

    async def until(self, condition: Callable[[], bool], timeout: float = 1.0) -> None:
        """Wait until ``condition()`` holds for the events read so far."""

        async def poll() -> None:
            while not condition():  # noqa: ASYNC110 - the condition reads events another task appends
                await asyncio.sleep(0.005)

        await asyncio.wait_for(poll(), timeout)

    @property
    def done(self) -> bool:
        return self._task.done()

    async def stop(self) -> None:
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


@contextlib.asynccontextmanager
async def reading(session: OpenAIRealtimeSession) -> AsyncGenerator[Reader]:
    """Run a background reader over ``session.receive()`` for the duration of the block."""
    reader = Reader(session)
    try:
        yield reader
    finally:
        await reader.stop()


MARKER = {"type": "session.updated", "event_id": "evt_marker", "session": {}}


async def read_through(connection: FakeConnection, reader: Reader, *events: Mapping[str, Any]) -> None:
    """Push ``events`` and wait until the reader has processed them, using a session.updated as a marker."""
    seen = len(reader.of_kind("session_updated"))
    connection.push(*events, MARKER)
    await reader.until(lambda: len(reader.of_kind("session_updated")) > seen)


async def activate_response(connection: FakeConnection, reader: Reader, response_id: str) -> None:
    """Make ``response_id`` the session's active response, as turn detection would."""
    await read_through(connection, reader, response_created(response_id))


async def mapped(*events: Mapping[str, Any], options: RealtimeOptions | None = None) -> list[Any]:
    """The realtime events a session yields for ``events``, after opening with ``options``."""
    session, connection = await started(options)
    async with reading(session) as reader:
        await read_through(connection, reader)
        already_read = len(reader.events)
        await read_through(connection, reader, *events)
    return reader.events[already_read:-1]


AGENT_PART = {"response_id": "resp_1", "item_id": "item_1", "content_index": 0}


def data_bytes(content: Any) -> bytes:
    """The bytes a data content's base64 data URI carries."""
    return base64.b64decode(content.uri.split(",", 1)[1])


def assert_agent_text(event: Any, text: str, merge_mode: str, status: str | None) -> None:
    """Assert ``event`` is agent transcript text for AGENT_PART."""
    assert event.kind == "output_text"
    assert (event.content.type, event.content.text, event.content.merge_mode) == ("text", text, merge_mode)
    assert event.content.status == status
    assert event.additional_properties["core"] == AGENT_PART


def rejection(
    code: str, param: str | None = None, *, event_id: str | None, message: str = "Rejected."
) -> dict[str, Any]:
    """A provider error event; ``event_id`` is the client event id the provider echoes, if any."""
    return {
        "type": "error",
        "event_id": "evt_srv_error",
        "error": {
            "type": "invalid_request_error",
            "code": code,
            "message": message,
            "param": param,
            "event_id": event_id,
        },
    }


# region Opening


async def test_start_without_options_sends_nothing() -> None:
    """Given a fresh session, start() with no options or tools sends no event and session.options == {}."""
    connection = FakeConnection()
    session = OpenAIRealtimeSession(connection)

    await session.start()

    assert connection.sent == []
    assert session.options == {}


async def test_start_sends_session_update_and_returns_on_session_updated() -> None:
    """start(options={"instructions": "Be brief."}) sends one session.update whose session is
    {"type": "realtime", "instructions": "Be brief."} with a client event_id, returns only after session.updated
    arrives, and leaves session.options == {"instructions": "Be brief."}."""
    connection = FakeConnection(responder=None)
    session = OpenAIRealtimeSession(connection)

    starting = asyncio.create_task(session.start(options={"instructions": "Be brief."}))
    await asyncio.sleep(0.01)

    assert not starting.done()
    assert connection.sent_types() == ["session.update"]
    assert connection.sent[0]["session"] == {"type": "realtime", "instructions": "Be brief."}
    assert connection.sent[0]["event_id"]

    connection.push({"type": "session.updated", "event_id": "evt_srv", "session": {"instructions": "Be brief."}})
    await asyncio.wait_for(starting, 1)

    assert session.options == {"instructions": "Be brief."}


async def test_start_waits_for_the_before_initial_update_hook() -> None:
    """A subclass whose _before_initial_update reads until an unsolicited session.updated, as a Foundry agent
    session waits for its definition: start(options={"instructions": "Be brief."}) sends nothing until that event
    arrives, then sends its session.update."""

    class WaitsForDefinition(OpenAIRealtimeSession[Any]):
        async def _before_initial_update(self) -> None:
            while (await self._read_into_buffer())["type"] != "session.updated":
                pass

    connection = FakeConnection()
    session = WaitsForDefinition(connection)

    starting = asyncio.create_task(session.start(options={"instructions": "Be brief."}))
    await asyncio.sleep(0.01)

    assert connection.sent == []

    connection.push({"type": "session.updated", "event_id": "evt_def", "session": {"instructions": "Defined."}})
    await asyncio.wait_for(starting, 1)

    assert connection.sent_types() == ["session.update"]


async def test_events_read_during_start_are_yielded_first() -> None:
    """session.created {id: "sess_1", model: "gpt-realtime", expires_at: 1700000000} and session.updated read during
    start() are the first events receive() yields: session_created with core {session_id: "sess_1",
    model: "gpt-realtime", expires_at: 1700000000}, then session_updated."""
    connection = FakeConnection()
    session = OpenAIRealtimeSession(connection)
    connection.push({
        "type": "session.created",
        "event_id": "evt_0",
        "session": {"id": "sess_1", "model": "gpt-realtime", "expires_at": 1700000000},
    })

    await asyncio.wait_for(session.start(options={"instructions": "Hi"}), 1)
    events = session.receive().__aiter__()
    created = await next_event(events)
    updated = await next_event(events)

    assert created.kind == "session_created"
    assert created.service_event_type == "session.created"
    assert created.additional_properties["core"] == {
        "session_id": "sess_1",
        "model": "gpt-realtime",
        "expires_at": 1700000000,
    }
    assert updated.kind == "session_updated"
    assert updated.service_event_type == "session.updated"


async def test_start_raises_when_initial_update_is_rejected() -> None:
    """An error {code: "invalid_value", param: "session.instructions", event_id: null} while start() waits raises
    RealtimeControlException with code "invalid_value" and param "session.instructions"; receive() then yields the
    claimed error as passthrough, not as error."""

    def reject(event: dict[str, Any]) -> list[Mapping[str, Any]]:
        if event["type"] != "session.update":
            return []
        return [rejection("invalid_value", "session.instructions", event_id=None)]

    connection = FakeConnection(responder=reject)
    session = OpenAIRealtimeSession(connection)

    with pytest.raises(RealtimeControlException) as caught:
        await asyncio.wait_for(session.start(options={"instructions": "x"}), 1)

    assert caught.value.code == "invalid_value"
    assert caught.value.param == "session.instructions"
    claimed = await next_event(session.receive().__aiter__())
    assert claimed.kind == "passthrough"
    assert claimed.service_event_type == "error"


# endregion

# region Translating options for a GA session


async def test_audio_settings_nest_under_audio_input_and_output() -> None:
    """voice "marin", input_audio_format "audio/pcm;rate=24000", output_audio_format "audio/pcmu", turn_detection,
    transcription and noise_reduction become session.audio.output {voice: "marin", format: {type: "audio/pcmu"}} and
    session.audio.input {format: {type: "audio/pcm", rate: 24000}, turn_detection, transcription, noise_reduction}."""
    turn_detection: RealtimeTurnDetection = {"type": "server_vad", "silence_duration_ms": 500}
    transcription: RealtimeTranscription = {"model": "gpt-4o-transcribe", "language": "en"}
    noise_reduction: RealtimeNoiseReduction = {"type": "near_field"}

    sent = await initial_session({
        "voice": "marin",
        "input_audio_format": "audio/pcm;rate=24000",
        "output_audio_format": "audio/pcmu",
        "turn_detection": turn_detection,
        "transcription": transcription,
        "noise_reduction": noise_reduction,
    })

    assert sent == {
        "type": "realtime",
        "audio": {
            "input": {
                "format": {"type": "audio/pcm", "rate": 24000},
                "turn_detection": turn_detection,
                "transcription": transcription,
                "noise_reduction": noise_reduction,
            },
            "output": {"voice": "marin", "format": {"type": "audio/pcmu"}},
        },
    }


async def test_max_tokens_and_output_modalities_translate() -> None:
    """max_tokens 512 becomes max_output_tokens 512, and output_modalities ["audio"] stays output_modalities
    ["audio"]."""
    sent = await initial_session({"max_tokens": 512, "output_modalities": ["audio"]})

    assert sent == {"type": "realtime", "max_output_tokens": 512, "output_modalities": ["audio"]}


@pytest.mark.parametrize(
    ("tool_choice", "expected"),
    [
        ("auto", "auto"),
        ({"mode": "none"}, "none"),
        ({"mode": "required", "required_function_name": "lookup"}, {"type": "function", "name": "lookup"}),
    ],
)
async def test_tool_choice_translates(tool_choice: Any, expected: Any) -> None:
    """ "auto" stays "auto"; ToolMode {mode: "none"} becomes "none"; ToolMode {mode: "required",
    required_function_name: "lookup"} becomes {type: "function", name: "lookup"}."""
    sent = await initial_session({"tool_choice": tool_choice})

    assert sent["tool_choice"] == expected


async def test_tool_choice_allowed_tools_is_ignored_with_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    """ToolMode {mode: "auto", allowed_tools: ["lookup"]} becomes "auto", and a warning says allowed_tools is ignored,
    since realtime sessions can't restrict the declared tools that way."""
    sent = await initial_session({"tool_choice": {"mode": "auto", "allowed_tools": ["lookup"]}})

    assert sent["tool_choice"] == "auto"
    assert "allowed_tools" in caplog.text


async def test_tools_are_declared(caplog: pytest.LogCaptureFixture) -> None:
    """A FunctionTool becomes {type: "function", name, description, parameters}; a Mapping tool is sent as given;
    any other tool is dropped with a warning naming it."""

    @tool
    def lookup(query: str) -> str:
        """Look something up."""
        return query

    hosted = {"type": "mcp", "server_label": "orders", "server_url": "https://example.com/mcp"}

    sent = await initial_session({}, [lookup, hosted, object()])

    assert sent["tools"] == [
        {
            "type": "function",
            "name": "lookup",
            "description": "Look something up.",
            "parameters": lookup.parameters(),
        },
        hosted,
    ]
    assert "object" in caplog.text


async def test_an_unsupported_tool_is_logged_by_type_only(caplog: pytest.LogCaptureFixture) -> None:
    """A tool object the session can't declare, whose repr holds an Authorization header, is dropped with a warning
    naming its type but not the header's token."""

    class HeaderedTool:
        def __repr__(self) -> str:
            return "HeaderedTool(headers={'Authorization': 'Bearer secret-token'})"

    await initial_session({"instructions": "Hi"}, [HeaderedTool()])

    assert "HeaderedTool" in caplog.text
    assert "secret-token" not in caplog.text


async def test_unsupported_option_is_dropped_with_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    """temperature 0.7 and metadata {"a": "b"} aren't GA session fields: neither is sent, and one warning names each."""
    sent = await initial_session({"instructions": "Hi", "temperature": 0.7, "metadata": {"a": "b"}})

    assert sent == {"type": "realtime", "instructions": "Hi"}
    assert "'temperature'" in caplog.text
    assert "'metadata'" in caplog.text


async def test_azure_voice_is_dropped_on_a_ga_session(caplog: pytest.LogCaptureFixture) -> None:
    """voice {type: "azure-standard", name: "en-US-AvaNeural"} isn't sent, and a warning says the session takes voice
    names only."""
    azure_voice: Any = {"type": "azure-standard", "name": "en-US-AvaNeural"}

    sent = await initial_session({"instructions": "Hi", "voice": azure_voice})

    assert sent == {"type": "realtime", "instructions": "Hi"}
    assert "voice" in caplog.text


async def test_a_dropped_voice_object_is_logged_by_name_only(caplog: pytest.LogCaptureFixture) -> None:
    """An Azure voice whose custom_lexicon_url is a SAS URL is dropped on a GA session with a warning naming
    "en-US-AvaNeural", without the URL or its signature."""
    azure_voice: Any = {
        "type": "azure-standard",
        "name": "en-US-AvaNeural",
        "custom_lexicon_url": "https://store.blob.core.windows.net/lexicons/a.xml?sv=2024-01-01&sig=secret-signature",
    }

    await initial_session({"instructions": "Hi", "voice": azure_voice})

    assert "en-US-AvaNeural" in caplog.text
    assert "store.blob.core.windows.net" not in caplog.text
    assert "secret-signature" not in caplog.text


async def test_unsupported_audio_format_raises_before_sending() -> None:
    """input_audio_format "audio/pcm;rate=16000" raises ValueError naming the format, and nothing is sent."""
    connection = FakeConnection()
    session = OpenAIRealtimeSession(connection)

    with pytest.raises(ValueError, match="audio/pcm;rate=16000"):
        await session.start(options={"input_audio_format": "audio/pcm;rate=16000"})

    assert connection.sent == []


# endregion

# region Control calls


async def test_update_session_returns_on_session_updated_and_merges_options() -> None:
    """After start(options={"instructions": "A"}), update_session({"max_tokens": 100}, None) sends session.update,
    returns on session.updated, and session.options == {"instructions": "A", "max_tokens": 100}."""
    session, connection = await started({"instructions": "A"})
    connection.responder = None

    async with reading(session):
        updating = asyncio.create_task(session.update_session({"max_tokens": 100}, None))
        await asyncio.sleep(0.01)
        assert not updating.done()
        assert connection.sent[-1]["type"] == "session.update"
        assert connection.sent[-1]["session"] == {"type": "realtime", "max_output_tokens": 100}

        connection.push({"type": "session.updated", "event_id": "evt_srv", "session": {}})
        await asyncio.wait_for(updating, 1)

    assert session.options == {"instructions": "A", "max_tokens": 100}


async def test_audio_right_after_an_acknowledged_format_change_uses_the_new_format() -> None:
    """update_session({"output_audio_format": "audio/pcmu"}) acknowledged with an audio delta right behind the
    session.updated: that output_audio's media_type is "audio/pcmu"."""
    session, connection = await started()
    connection.responder = None
    audio = {**AGENT_PART, "type": "response.output_audio.delta", "event_id": "e1", "delta": "AQI="}

    async with reading(session) as reader:
        updating = asyncio.create_task(session.update_session({"output_audio_format": "audio/pcmu"}, None))
        await connection.wait_sent("session.update")
        connection.push({"type": "session.updated", "event_id": "evt_srv", "session": {}}, audio)
        await reader.until(lambda: "output_audio" in reader.kinds())
        await asyncio.wait_for(updating, 1)

    assert reader.of_kind("output_audio")[0].content.media_type == "audio/pcmu"


async def test_a_cancelled_updates_late_acknowledgement_does_not_answer_the_next_update() -> None:
    """update_session A ({"max_tokens": 100}) is cancelled after it's sent, then update B ({"instructions": "B."})
    is sent. A's session.updated doesn't resolve B; B resolves on the next one, and session.options holds both."""
    session, connection = await started()
    connection.responder = None

    async with reading(session):
        abandoned = asyncio.create_task(session.update_session({"max_tokens": 100}, None))
        await connection.wait_sent("session.update")
        abandoned.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await abandoned
        updating = asyncio.create_task(session.update_session({"instructions": "B."}, None))
        await connection.wait_sent("session.update")
        connection.push({"type": "session.updated", "event_id": "evt_a", "session": {}})
        await asyncio.sleep(0.01)
        assert not updating.done()

        connection.push({"type": "session.updated", "event_id": "evt_b", "session": {}})
        await asyncio.wait_for(updating, 1)

    assert session.options == {"max_tokens": 100, "instructions": "B."}


async def test_a_cancelled_updates_late_rejection_does_not_fail_the_next_update() -> None:
    """update_session A is cancelled after it's sent, then update B is sent. A's rejection, echoing no event_id as on
    Voice Live, streams as an error and doesn't fail B, which resolves on the session.updated that follows."""
    session, connection = await started()
    connection.responder = None

    async with reading(session) as reader:
        abandoned = asyncio.create_task(session.update_session({"max_tokens": 100}, None))
        await connection.wait_sent("session.update")
        abandoned.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await abandoned
        updating = asyncio.create_task(session.update_session({"instructions": "B."}, None))
        await connection.wait_sent("session.update")
        connection.push(rejection("invalid_value", event_id=None))
        await reader.until(lambda: "error" in reader.kinds())
        assert not updating.done()

        connection.push({"type": "session.updated", "event_id": "evt_b", "session": {}})
        await asyncio.wait_for(updating, 1)

    assert [event.content.error_code for event in reader.of_kind("error")] == ["invalid_value"]
    assert session.options == {"instructions": "B."}


async def test_a_cancelled_updates_echoed_rejection_retires_it_whatever_arrives_first() -> None:
    """update_session A is cancelled after it's sent, then update B is sent. A speech_started, then A's rejection
    echoing A's event_id as GA providers do: the rejection streams as an error, B isn't failed, and B resolves on the
    next session.updated."""
    session, connection = await started()
    connection.responder = None
    speech_started = {
        "type": "input_audio_buffer.speech_started",
        "event_id": "e1",
        "item_id": "i",
        "audio_start_ms": 0,
    }

    async with reading(session) as reader:
        abandoned = asyncio.create_task(session.update_session({"max_tokens": 100}, None))
        abandoned_request = await connection.wait_sent("session.update")
        abandoned.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await abandoned
        updating = asyncio.create_task(session.update_session({"instructions": "B."}, None))
        await connection.wait_sent("session.update")
        connection.push(speech_started, rejection("invalid_value", event_id=abandoned_request["event_id"]))
        await reader.until(lambda: "error" in reader.kinds())
        assert not updating.done()

        connection.push({"type": "session.updated", "event_id": "evt_b", "session": {}})
        await asyncio.wait_for(updating, 1)

    assert session.options == {"instructions": "B."}


async def test_a_cancelled_updates_late_rejection_with_a_server_id_does_not_fail_the_next_update() -> None:
    """update_session A is cancelled after it's sent, then update B is sent. A's rejection, carrying the server's own
    event_id "evt_srv_9", streams as an error and retires A; B resolves on the next session.updated without A's
    settings."""
    session, connection = await started()
    connection.responder = None

    async with reading(session) as reader:
        abandoned = asyncio.create_task(session.update_session({"max_tokens": 100}, None))
        await connection.wait_sent("session.update")
        abandoned.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await abandoned
        updating = asyncio.create_task(session.update_session({"instructions": "B."}, None))
        await connection.wait_sent("session.update")
        connection.push(rejection("invalid_value", event_id="evt_srv_9"))
        await reader.until(lambda: "error" in reader.kinds())
        assert not updating.done()

        connection.push({"type": "session.updated", "event_id": "evt_b", "session": {}})
        await asyncio.wait_for(updating, 1)

    assert session.options == {"instructions": "B."}


async def test_an_acknowledgement_read_as_its_update_is_cancelled_is_not_expected_again() -> None:
    """Update A's session.updated is queued and A is cancelled before A's task runs again; the reader still reads it
    as A's, so update B resolves on its own session.updated and session.options holds both."""
    session, connection = await started()
    connection.responder = None

    async with reading(session):
        abandoned = asyncio.create_task(session.update_session({"max_tokens": 100}, None))
        await connection.wait_sent("session.update")
        connection.push({"type": "session.updated", "event_id": "evt_a", "session": {}})
        abandoned.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await abandoned
        updating = asyncio.create_task(session.update_session({"instructions": "B."}, None))
        await connection.wait_sent("session.update")
        connection.push({"type": "session.updated", "event_id": "evt_b", "session": {}})
        await asyncio.wait_for(updating, 1)

    assert session.options == {"max_tokens": 100, "instructions": "B."}


async def test_an_update_cancelled_while_its_write_drains_is_still_owed_an_answer() -> None:
    """Update A is cancelled after its frame is written but while the write still waits to drain; A's late
    session.updated then doesn't resolve update B."""
    drained = asyncio.Event()

    class DrainingConnection(FakeConnection):
        async def send(self, event: Mapping[str, Any]) -> None:
            await super().send(event)
            if event.get("session") == {"type": "realtime", "max_output_tokens": 100}:
                await drained.wait()

    connection = DrainingConnection(responder=None)
    session = OpenAIRealtimeSession(connection)
    await session.start()

    async with reading(session):
        abandoned = asyncio.create_task(session.update_session({"max_tokens": 100}, None))
        await connection.wait_sent("session.update")
        abandoned.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await abandoned
        updating = asyncio.create_task(session.update_session({"instructions": "B."}, None))
        await connection.wait_sent("session.update")
        connection.push({"type": "session.updated", "event_id": "evt_a", "session": {}})
        await asyncio.sleep(0.01)
        assert not updating.done()

        connection.push({"type": "session.updated", "event_id": "evt_b", "session": {}})
        await asyncio.wait_for(updating, 1)


async def test_a_rejection_read_as_its_draining_update_is_cancelled_streams_unclaimed() -> None:
    """Update A's rejection is queued and A is cancelled while its write still drains, before the reader gets to the
    rejection; it streams as an unclaimed error, not passthrough."""
    drained = asyncio.Event()

    class DrainingConnection(FakeConnection):
        async def send(self, event: Mapping[str, Any]) -> None:
            await super().send(event)
            if event["type"] == "session.update":
                await drained.wait()

    connection = DrainingConnection(responder=None)
    session = OpenAIRealtimeSession(connection)
    await session.start()

    async with reading(session) as reader:
        updating = asyncio.create_task(session.update_session({"max_tokens": 100}, None))
        await connection.wait_sent("session.update")
        connection.push(rejection("invalid_value", event_id=None))
        updating.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await updating
        await read_through(connection, reader)

    assert [event.service_event_type for event in reader.of_kind("error")] == ["error"]
    assert reader.of_kind("passthrough") == []


async def test_a_caller_that_swallowed_an_earlier_cancellation_still_gets_its_rejection() -> None:
    """A task that suppressed its own earlier cancellation without uncancel() calls update_session(); a rejection
    still fails that call with RealtimeControlException."""
    session, connection = await started()
    connection.responder = None

    async def careless_caller() -> None:
        current = asyncio.current_task()
        assert current is not None
        current.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(0)
        await session.update_session({"instructions": "New."}, None)

    async with reading(session):
        updating = asyncio.create_task(careless_caller())
        await connection.wait_sent("session.update")
        connection.push(rejection("invalid_value", event_id=None))

        with pytest.raises(RealtimeControlException):
            await asyncio.wait_for(updating, 1)


RAW_UPDATE = {"type": "session.update", "session": {"type": "realtime", "metadata": {"k": "v"}}}


async def test_a_raw_session_updates_acknowledgement_does_not_answer_a_later_update() -> None:
    """A raw session.update is sent, then update_session({"instructions": "B."}). The raw update's session.updated
    doesn't resolve the typed one, which resolves on the next session.updated; session.options holds only B."""
    session, connection = await started()
    connection.responder = None

    async with reading(session):
        await session.send(RealtimeEvent("passthrough", raw_representation=RAW_UPDATE))
        updating = asyncio.create_task(session.update_session({"instructions": "B."}, None))
        await connection.wait_sent("session.update")
        connection.push({"type": "session.updated", "event_id": "evt_raw", "session": {}})
        await asyncio.sleep(0.01)
        assert not updating.done()

        connection.push({"type": "session.updated", "event_id": "evt_b", "session": {}})
        await asyncio.wait_for(updating, 1)

    assert session.options == {"instructions": "B."}


async def test_an_update_sent_before_a_raw_session_update_takes_the_first_acknowledgement() -> None:
    """update_session B is sent, then a raw session.update while B waits. The first session.updated resolves B and
    the second answers the raw update, so a later update_session C resolves only on its own."""
    session, connection = await started()
    connection.responder = None

    async with reading(session):
        updating = asyncio.create_task(session.update_session({"instructions": "B."}, None))
        await connection.wait_sent("session.update")
        await session.send(RealtimeEvent("passthrough", raw_representation=RAW_UPDATE))
        connection.push({"type": "session.updated", "event_id": "evt_b", "session": {}})
        await asyncio.wait_for(updating, 1)

        later = asyncio.create_task(session.update_session({"instructions": "C."}, None))
        await connection.wait_sent("session.update")
        connection.push({"type": "session.updated", "event_id": "evt_raw", "session": {}})
        await asyncio.sleep(0.01)
        assert not later.done()

        connection.push({"type": "session.updated", "event_id": "evt_c", "session": {}})
        await asyncio.wait_for(later, 1)


async def test_a_rejection_of_a_control_call_sent_before_a_raw_update_fails_that_call() -> None:
    """create_response() is sent, then a raw session.update. A rejection echoing no event_id, as on Voice Live,
    answers the request written first: create_response() raises RealtimeControlException."""
    session, connection = await started()
    connection.responder = None

    async with reading(session):
        creating = asyncio.create_task(session.create_response())
        await connection.wait_sent("response.create")
        await session.send(RealtimeEvent("passthrough", raw_representation=RAW_UPDATE))
        connection.push(rejection("invalid_value", event_id=None))

        with pytest.raises(RealtimeControlException):
            await asyncio.wait_for(creating, 1)


async def test_a_rejection_of_a_truncate_sent_before_a_raw_update_fails_that_truncate() -> None:
    """truncate("item_1", 0, 500) is sent, then a raw session.update. A rejection echoing the server's own event_id
    answers the request written first: truncate() raises RealtimeControlException."""
    session, connection = await started()

    async with reading(session):
        truncating = asyncio.create_task(session.truncate("item_1", 0, 500))
        await connection.wait_sent("conversation.item.truncate")
        await session.send(RealtimeEvent("passthrough", raw_representation=RAW_UPDATE))
        connection.push(rejection("invalid_value", "audio_end_ms", event_id="evt_srv_7"))

        with pytest.raises(RealtimeControlException):
            await asyncio.wait_for(truncating, 1)


async def test_audio_keeps_its_responses_format_after_a_format_change() -> None:
    """While "resp_1" is active, update_session({"output_audio_format": "audio/pcmu"}) is acknowledged: resp_1's
    later audio is still "audio/pcm;rate=24000", as a response in flight doesn't change, and audio of "resp_2",
    created after the update, is "audio/pcmu"."""
    session, connection = await started()
    resp_1_audio = {**AGENT_PART, "type": "response.output_audio.delta", "event_id": "e1", "delta": "AQI="}
    resp_2_audio = {**resp_1_audio, "response_id": "resp_2", "event_id": "e2"}

    async with reading(session) as reader:
        await activate_response(connection, reader, "resp_1")
        await asyncio.wait_for(session.update_session({"output_audio_format": "audio/pcmu"}, None), 1)
        await read_through(
            connection, reader, resp_1_audio, response_done("resp_1"), response_created("resp_2"), resp_2_audio
        )

    assert [event.content.media_type for event in reader.of_kind("output_audio")] == [
        "audio/pcm;rate=24000",
        "audio/pcmu",
    ]


async def test_update_session_with_nothing_left_returns_without_sending(caplog: pytest.LogCaptureFixture) -> None:
    """update_session({"temperature": 0.7}, None) warns, sends nothing and returns at once."""
    session, connection = await started({"instructions": "A"})

    await asyncio.wait_for(session.update_session({"temperature": 0.7}, None), 0.1)

    assert connection.sent_types() == ["session.update"]
    assert "'temperature'" in caplog.text
    assert session.options == {"instructions": "A"}


async def test_create_response_returns_its_own_response_id() -> None:
    """create_response() sends response.create whose response.metadata holds an af_request_id; a response.created
    {id: "resp_1"} echoing that id resolves it to "resp_1"."""
    session, connection = await started()

    async with reading(session):
        creating = asyncio.create_task(session.create_response())
        request = await connection.wait_sent("response.create")
        connection.push(response_created("resp_1", request_id_of(request)))

        assert await asyncio.wait_for(creating, 1) == "resp_1"


async def test_create_response_translates_per_response_options() -> None:
    """create_response({"instructions": "Short.", "max_tokens": 50, "tool_choice": "none", "metadata": {"k": "v"}})
    sends response {instructions: "Short.", max_output_tokens: 50, tool_choice: "none", metadata: {k: "v",
    af_request_id: ...}}."""
    session, connection = await started()

    async with reading(session):
        creating = asyncio.create_task(
            session.create_response({
                "instructions": "Short.",
                "max_tokens": 50,
                "tool_choice": {"mode": "none"},
                "metadata": {"k": "v"},
            })
        )
        request = await connection.wait_sent("response.create")
        connection.push(response_created("resp_1", request_id_of(request)))
        await asyncio.wait_for(creating, 1)

    assert request["response"] == {
        "instructions": "Short.",
        "max_output_tokens": 50,
        "tool_choice": "none",
        "metadata": {"k": "v", "af_request_id": request_id_of(request)},
    }


async def test_create_response_raises_while_a_response_is_active() -> None:
    """After receive() yields response_created for "resp_1", create_response() raises
    RealtimeActiveResponseException and sends nothing."""
    session, connection = await started()

    async with reading(session) as reader:
        connection.push(
            response_created("resp_1"), {"type": "session.updated", "event_id": "evt_marker", "session": {}}
        )
        await reader.until(lambda: "session_updated" in reader.kinds())

        with pytest.raises(RealtimeActiveResponseException):
            await asyncio.wait_for(session.create_response(), 1)

    assert connection.sent == []


async def test_a_response_started_while_create_response_waits_to_send_raises() -> None:
    """create_response() waits behind another write while turn detection starts "resp_vad": it raises
    RealtimeActiveResponseException and sends no response.create."""
    release = asyncio.Event()

    class BlockingConnection(FakeConnection):
        async def send(self, event: Mapping[str, Any]) -> None:
            if event.get("type") == "blocker":
                await release.wait()
            await super().send(event)

    connection = BlockingConnection()
    session = OpenAIRealtimeSession(connection)
    await session.start()

    async with reading(session) as reader:
        blocking = asyncio.create_task(
            session.send(RealtimeEvent("passthrough", raw_representation={"type": "blocker"}))
        )
        await asyncio.sleep(0.01)
        creating = asyncio.create_task(session.create_response())
        await asyncio.sleep(0.01)
        await read_through(connection, reader, response_created("resp_vad"))
        release.set()
        with pytest.raises(RealtimeActiveResponseException):
            await asyncio.wait_for(creating, 1)
        await asyncio.wait_for(blocking, 1)

    assert "response.create" not in connection.sent_types()


async def test_create_response_fails_on_a_lost_race() -> None:
    """While create_response() waits, a response.created {id: "resp_vad", metadata: null} that doesn't carry its
    af_request_id fails it with RealtimeActiveResponseException."""
    session, connection = await started()

    async with reading(session):
        creating = asyncio.create_task(session.create_response())
        await connection.wait_sent("response.create")
        connection.push(response_created("resp_vad"))

        with pytest.raises(RealtimeActiveResponseException):
            await asyncio.wait_for(creating, 1)


async def test_say_uses_the_best_effort_prompt() -> None:
    """With an OpenAI voice, say("Hello there.") sends response.create with instructions "Say exactly the following,
    word for word, and nothing else: Hello there." and returns the id of its own response.created."""
    session, connection = await started({"voice": "marin"})

    async with reading(session):
        saying = asyncio.create_task(session.say("Hello there."))
        request = await connection.wait_sent("response.create")
        connection.push(response_created("resp_1", request_id_of(request)))

        assert await asyncio.wait_for(saying, 1) == "resp_1"

    assert request["response"]["instructions"] == (
        "Say exactly the following, word for word, and nothing else: Hello there."
    )


async def test_say_with_an_azure_voice_sends_a_pre_generated_message() -> None:
    """When the last session.updated echoes audio.output.voice {type: "azure-standard", ...}, say("Hi.") sends
    response.create with pre_generated_assistant_message {type: "message", role: "assistant", content: [{type:
    "text", text: "Hi."}]}, and the next response.created {id: "resp_2", metadata: null} resolves it to "resp_2"."""
    session, connection = await started()
    azure_voice = {"type": "azure-standard", "name": "en-US-AvaNeural"}

    async with reading(session) as reader:
        connection.push({
            "type": "session.updated",
            "event_id": "e1",
            "session": {"audio": {"output": {"voice": azure_voice}}},
        })
        await reader.until(lambda: "session_updated" in reader.kinds())
        saying = asyncio.create_task(session.say("Hi."))
        request = await connection.wait_sent("response.create")
        connection.push(response_created("resp_2"))

        assert await asyncio.wait_for(saying, 1) == "resp_2"

    assert request["response"]["pre_generated_assistant_message"] == {
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": "Hi."}],
    }
    assert "instructions" not in request["response"]


async def test_cancel_response_resolves_on_that_responses_done() -> None:
    """With "resp_1" active, cancel_response() sends response.cancel and returns when response.done for "resp_1"
    arrives with status "cancelled"."""
    session, connection = await started()

    async with reading(session) as reader:
        await activate_response(connection, reader, "resp_1")
        cancelling = asyncio.create_task(session.cancel_response())
        await connection.wait_sent("response.cancel")
        await asyncio.sleep(0.01)
        assert not cancelling.done()

        connection.push(response_done("resp_1", "cancelled"))
        await asyncio.wait_for(cancelling, 1)


async def test_cancel_response_ignores_another_responses_done() -> None:
    """While cancel_response() waits on active "resp_2", a response.done for an earlier "resp_1" doesn't resolve it;
    the one for "resp_2" does."""
    session, connection = await started()

    async with reading(session) as reader:
        await activate_response(connection, reader, "resp_2")
        cancelling = asyncio.create_task(session.cancel_response())
        await connection.wait_sent("response.cancel")
        connection.push(response_done("resp_1"))
        await asyncio.sleep(0.01)
        assert not cancelling.done()

        connection.push(response_done("resp_2", "cancelled"))
        await asyncio.wait_for(cancelling, 1)


async def test_a_cancel_whose_response_finishes_before_it_is_sent_sends_nothing() -> None:
    """With "resp_1" active, cancel_response() waits behind another write while "resp_1" finishes and "resp_2"
    starts. The cancel isn't sent, so it can't cancel "resp_2", and cancel_response() returns."""
    release = asyncio.Event()

    class BlockingConnection(FakeConnection):
        async def send(self, event: Mapping[str, Any]) -> None:
            if event.get("type") == "blocker":
                await release.wait()
            await super().send(event)

    connection = BlockingConnection()
    session = OpenAIRealtimeSession(connection)
    await session.start()

    async with reading(session) as reader:
        await activate_response(connection, reader, "resp_1")
        blocking = asyncio.create_task(
            session.send(RealtimeEvent("passthrough", raw_representation={"type": "blocker"}))
        )
        await asyncio.sleep(0.01)
        cancelling = asyncio.create_task(session.cancel_response())
        await asyncio.sleep(0.01)
        await read_through(connection, reader, response_done("resp_1"), response_created("resp_2"))
        release.set()
        await asyncio.wait_for(cancelling, 1)
        await asyncio.wait_for(blocking, 1)

    assert "response.cancel" not in connection.sent_types()


async def test_cancel_response_without_an_active_response_raises_locally() -> None:
    """With no active response, cancel_response() raises RealtimeControlException with code
    "response_cancel_not_active" and sends nothing."""
    session, connection = await started()

    with pytest.raises(RealtimeControlException) as caught:
        await asyncio.wait_for(session.cancel_response(), 0.1)

    assert caught.value.code == "response_cancel_not_active"
    assert connection.sent == []


async def test_truncate_resolves_and_marks_the_item_truncated_by_the_client() -> None:
    """truncate("item_1", 0, 1500) sends conversation.item.truncate {item_id: "item_1", content_index: 0,
    audio_end_ms: 1500}, returns on conversation.item.truncated for "item_1", and receive() yields item_truncated
    with core {item_id: "item_1", content_index: 0, audio_end_ms: 1500, truncated_by: "client"}."""
    session, connection = await started()

    async with reading(session) as reader:
        truncating = asyncio.create_task(session.truncate("item_1", 0, 1500))
        request = await connection.wait_sent("conversation.item.truncate")
        connection.push(item_truncated("item_1", 0, 1500))
        await asyncio.wait_for(truncating, 1)
        await reader.until(lambda: "item_truncated" in reader.kinds())

    assert {key: request[key] for key in ("item_id", "content_index", "audio_end_ms")} == {
        "item_id": "item_1",
        "content_index": 0,
        "audio_end_ms": 1500,
    }
    truncated = reader.of_kind("item_truncated")[0]
    assert truncated.service_event_type == "conversation.item.truncated"
    assert truncated.additional_properties["core"] == {
        "item_id": "item_1",
        "content_index": 0,
        "audio_end_ms": 1500,
        "truncated_by": "client",
    }


async def test_unrequested_truncation_is_marked_provider() -> None:
    """A conversation.item.truncated for "item_9" that no truncate() requested yields item_truncated with
    truncated_by "provider"."""
    session, connection = await started()

    async with reading(session) as reader:
        await read_through(connection, reader, item_truncated("item_9", 0, 800))

    assert reader.of_kind("item_truncated")[0].additional_properties["core"]["truncated_by"] == "provider"


async def test_truncate_ignores_another_items_truncation() -> None:
    """While truncate("item_1", 0, 1500) waits, a conversation.item.truncated for "item_2" doesn't resolve it; the one
    for "item_1" does."""
    session, connection = await started()

    async with reading(session):
        truncating = asyncio.create_task(session.truncate("item_1", 0, 1500))
        await connection.wait_sent("conversation.item.truncate")
        connection.push(item_truncated("item_2", 0, 800))
        await asyncio.sleep(0.01)
        assert not truncating.done()

        connection.push(item_truncated("item_1", 0, 1500))
        await asyncio.wait_for(truncating, 1)


async def test_a_truncation_read_while_its_truncate_drains_is_marked_client() -> None:
    """The provider's conversation.item.truncated for "item_1" is read while truncate("item_1", 0, 500) still waits
    for its write to drain; it yields truncated_by "client", and a later truncation of "item_1" yields "provider"."""

    class DrainingConnection(FakeConnection):
        async def send(self, event: Mapping[str, Any]) -> None:
            await super().send(event)
            if event["type"] == "conversation.item.truncate":
                self.push(item_truncated("item_1", 0, 500))
                await asyncio.sleep(0.02)

    connection = DrainingConnection()
    session = OpenAIRealtimeSession(connection)
    await session.start()

    async with reading(session) as reader:
        await asyncio.wait_for(session.truncate("item_1", 0, 500), 1)
        await read_through(connection, reader, item_truncated("item_1", 0, 300))

    assert [event.additional_properties["core"]["truncated_by"] for event in reader.of_kind("item_truncated")] == [
        "client",
        "provider",
    ]


async def test_a_truncation_read_before_the_truncate_is_sent_does_not_answer_it() -> None:
    """While another write holds the connection, truncate("item_1", 0, 500) waits to send. A provider truncation of
    the same cut read meanwhile doesn't answer it: once sent, truncate() waits for its own acknowledgement."""
    release = asyncio.Event()

    class BlockingConnection(FakeConnection):
        async def send(self, event: Mapping[str, Any]) -> None:
            if event.get("type") == "blocker":
                await release.wait()
            await super().send(event)

    connection = BlockingConnection()
    session = OpenAIRealtimeSession(connection)
    await session.start()

    async with reading(session) as reader:
        blocking = asyncio.create_task(
            session.send(RealtimeEvent("passthrough", raw_representation={"type": "blocker"}))
        )
        await asyncio.sleep(0.01)
        truncating = asyncio.create_task(session.truncate("item_1", 0, 500))
        await asyncio.sleep(0.01)
        await read_through(connection, reader, item_truncated("item_1", 0, 500))
        release.set()
        await connection.wait_sent("conversation.item.truncate")
        await asyncio.sleep(0.01)
        assert not truncating.done()

        connection.push(item_truncated("item_1", 0, 500))
        await asyncio.wait_for(truncating, 1)
        await asyncio.wait_for(blocking, 1)


async def test_a_second_cut_of_an_item_waits_for_its_own_acknowledgement() -> None:
    """truncate("item_1", 0, 500) is cancelled after it's sent; truncate("item_1", 0, 300) then ignores the 500 ms
    acknowledgement and resolves on the 300 ms one. Both truncations are marked client."""
    session, connection = await started()

    async with reading(session) as reader:
        first = asyncio.create_task(session.truncate("item_1", 0, 500))
        await connection.wait_sent("conversation.item.truncate")
        first.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await first
        second = asyncio.create_task(session.truncate("item_1", 0, 300))
        await connection.wait_sent("conversation.item.truncate")
        connection.push(item_truncated("item_1", 0, 500))
        await reader.until(lambda: len(reader.of_kind("item_truncated")) == 1)
        assert not second.done()

        connection.push(item_truncated("item_1", 0, 300))
        await asyncio.wait_for(second, 1)
        await reader.until(lambda: len(reader.of_kind("item_truncated")) == 2)

    assert [event.additional_properties["core"]["truncated_by"] for event in reader.of_kind("item_truncated")] == [
        "client",
        "client",
    ]


async def test_a_late_rejection_of_a_cancelled_truncate_clears_its_client_mark() -> None:
    """truncate("item_1", 0, 500) is cancelled after it's sent, then rejected by an error echoing its event_id, as GA
    providers do; a later truncation at that cut is marked provider."""
    session, connection = await started()

    async with reading(session) as reader:
        truncating = asyncio.create_task(session.truncate("item_1", 0, 500))
        request = await connection.wait_sent("conversation.item.truncate")
        truncating.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await truncating
        await read_through(
            connection,
            reader,
            rejection("invalid_value", "audio_end_ms", event_id=request["event_id"]),
            item_truncated("item_1", 0, 500),
        )

    assert reader.of_kind("item_truncated")[0].additional_properties["core"]["truncated_by"] == "provider"


async def test_a_truncation_read_right_behind_a_truncates_rejection_is_marked_provider() -> None:
    """truncate("item_1", 0, 500) is rejected by an error echoing no event_id, and a provider truncation at that same
    cut is read right behind it, before truncate() resumes: it's marked provider."""
    session, connection = await started()

    async with reading(session) as reader:
        truncating = asyncio.create_task(session.truncate("item_1", 0, 500))
        await connection.wait_sent("conversation.item.truncate")
        connection.push(rejection("invalid_value", event_id=None), item_truncated("item_1", 0, 500))
        with pytest.raises(RealtimeControlException):
            await asyncio.wait_for(truncating, 1)
        await reader.until(lambda: "item_truncated" in reader.kinds())

    assert reader.of_kind("item_truncated")[0].additional_properties["core"]["truncated_by"] == "provider"


async def test_a_rejected_truncate_leaves_a_later_truncation_to_the_provider() -> None:
    """truncate("item_1", 0, 99999) fails on a claimed rejection; a later conversation.item.truncated at that same
    cut yields truncated_by "provider"."""
    session, connection = await started()

    async with reading(session) as reader:
        truncating = asyncio.create_task(session.truncate("item_1", 0, 99999))
        await connection.wait_sent("conversation.item.truncate")
        connection.push(rejection("invalid_value", "audio_end_ms", event_id=None))
        with pytest.raises(RealtimeControlException):
            await asyncio.wait_for(truncating, 1)
        await read_through(connection, reader, item_truncated("item_1", 0, 99999))

    assert reader.of_kind("item_truncated")[0].additional_properties["core"]["truncated_by"] == "provider"


async def test_a_truncate_cancelled_before_sending_leaves_a_later_truncation_to_the_provider() -> None:
    """While another write holds the connection, truncate("item_1", 0, 500) is cancelled before it's sent; a later
    conversation.item.truncated at that same cut yields truncated_by "provider"."""
    release = asyncio.Event()

    class BlockingConnection(FakeConnection):
        async def send(self, event: Mapping[str, Any]) -> None:
            if event.get("type") == "blocker":
                await release.wait()
            await super().send(event)

    connection = BlockingConnection()
    session = OpenAIRealtimeSession(connection)
    await session.start()

    async with reading(session) as reader:
        blocker = RealtimeEvent("passthrough", raw_representation={"type": "blocker"})
        blocking = asyncio.create_task(session.send(blocker))
        await asyncio.sleep(0.01)
        truncating = asyncio.create_task(session.truncate("item_1", 0, 500))
        await asyncio.sleep(0.01)
        truncating.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await truncating
        release.set()
        await asyncio.wait_for(blocking, 1)
        await read_through(connection, reader, item_truncated("item_1", 0, 500))

    assert "conversation.item.truncate" not in connection.sent_types()
    assert reader.of_kind("item_truncated")[0].additional_properties["core"]["truncated_by"] == "provider"


# endregion

# region Error claiming


async def test_error_echoing_another_requests_id_does_not_claim() -> None:
    """While update_session() waits, an error echoing the event_id of an earlier request doesn't fail it, and
    update_session() still returns on the session.updated that follows."""
    session, connection = await started()

    async with reading(session):
        await asyncio.wait_for(session.update_session({"instructions": "First."}, None), 1)
        earlier_event_id = connection.sent[-1]["event_id"]
        connection.responder = None

        updating = asyncio.create_task(session.update_session({"instructions": "Second."}, None))
        await asyncio.sleep(0.01)
        connection.push(rejection("invalid_value", event_id=earlier_event_id))
        await asyncio.sleep(0.01)
        assert not updating.done()

        connection.push({"type": "session.updated", "event_id": "evt_srv", "session": {}})
        await asyncio.wait_for(updating, 1)


async def test_an_error_echoing_a_raw_commands_event_id_is_not_claimed() -> None:
    """While update_session() waits, a raw command with event_id "raw_1" is rejected by an error echoing "raw_1": the
    update isn't failed, the error streams as error, and the update resolves on its session.updated."""
    session, connection = await started()
    connection.responder = None
    commit = {"type": "input_audio_buffer.commit", "event_id": "raw_1"}

    async with reading(session) as reader:
        updating = asyncio.create_task(session.update_session({"instructions": "New."}, None))
        await connection.wait_sent("session.update")
        await session.send(RealtimeEvent("passthrough", raw_representation=commit))
        connection.push(rejection("input_audio_buffer_commit_empty", event_id="raw_1"))
        await reader.until(lambda: "error" in reader.kinds())
        assert not updating.done()

        connection.push({"type": "session.updated", "event_id": "evt_srv", "session": {}})
        await asyncio.wait_for(updating, 1)


async def test_an_error_echoing_a_sent_messages_event_id_is_not_claimed() -> None:
    """While update_session() waits, a function result sent with send(Message(...)) is rejected by an error echoing
    that event's event_id: the update isn't failed, the error streams as error, and the update resolves on its
    session.updated."""
    session, connection = await started()
    connection.responder = None

    async with reading(session) as reader:
        updating = asyncio.create_task(session.update_session({"instructions": "New."}, None))
        await connection.wait_sent("session.update")
        await session.send(Message("tool", [Content.from_function_result("call_stale", result="42")]))
        connection.push(rejection("invalid_value", event_id=connection.sent[-1].get("event_id")))
        await reader.until(lambda: "error" in reader.kinds())
        assert not updating.done()

        connection.push({"type": "session.updated", "event_id": "evt_srv", "session": {}})
        await asyncio.wait_for(updating, 1)


async def test_claimed_active_response_error_raises_active_response_exception() -> None:
    """While create_response() waits, an error {code: "conversation_already_has_active_response", event_id: null}
    fails it with RealtimeActiveResponseException."""
    session, connection = await started()

    async with reading(session):
        creating = asyncio.create_task(session.create_response())
        await connection.wait_sent("response.create")
        connection.push(rejection("conversation_already_has_active_response", event_id=None))

        with pytest.raises(RealtimeActiveResponseException) as caught:
            await asyncio.wait_for(creating, 1)

    assert caught.value.code == "conversation_already_has_active_response"


async def test_unclaimed_errors_stream_as_error_content_without_deduplication() -> None:
    """With no call waiting, two identical errors {type: "invalid_request_error", code: "bad", message: "Bad.",
    param: "x", event_id: "evt_server"} yield two error events, each holding Content.from_error(message="Bad.",
    error_code="bad") with openai {type: "invalid_request_error", param: "x", event_id: "evt_server"}."""
    session, connection = await started()
    error = rejection("bad", "x", event_id="evt_server", message="Bad.")

    async with reading(session) as reader:
        await read_through(connection, reader, error, error)

    errors = reader.of_kind("error")
    assert len(errors) == 2
    for event in errors:
        assert event.service_event_type == "error"
        assert event.content.type == "error"
        assert event.content.message == "Bad."
        assert event.content.error_code == "bad"
        assert event.content.additional_properties["openai"] == {
            "type": "invalid_request_error",
            "param": "x",
            "event_id": "evt_server",
        }


async def test_late_cancel_not_active_answering_own_cancel_is_passthrough() -> None:
    """cancel_response() resolves on response.done for "resp_1"; an error {code: "response_cancel_not_active",
    event_id: <the cancel's id>} arriving afterwards streams as passthrough, not error."""
    session, connection = await started()

    async with reading(session) as reader:
        await activate_response(connection, reader, "resp_1")
        cancelling = asyncio.create_task(session.cancel_response())
        cancel = await connection.wait_sent("response.cancel")
        connection.push(response_done("resp_1"))
        await asyncio.wait_for(cancelling, 1)
        await read_through(connection, reader, rejection("response_cancel_not_active", event_id=cancel["event_id"]))

    assert reader.of_kind("error") == []
    assert [event.service_event_type for event in reader.of_kind("passthrough")] == ["error"]


# endregion

# region Out-of-band responses, cancellation safety and the connection


async def test_out_of_band_response_is_ignored_by_tracking() -> None:
    """While create_response() waits, a response.created {id: "resp_oob", conversation_id: null, metadata: null}
    doesn't fail it, and its own response.created then resolves it. A later create_response() isn't blocked by
    "resp_oob"."""
    session, connection = await started()

    async with reading(session) as reader:
        creating = asyncio.create_task(session.create_response())
        request = await connection.wait_sent("response.create")
        connection.push(response_created("resp_oob", conversation_id=None))
        await asyncio.sleep(0.01)
        assert not creating.done()
        connection.push(response_created("resp_1", request_id_of(request), conversation_id="conv_1"))
        assert await asyncio.wait_for(creating, 1) == "resp_1"
        await read_through(connection, reader, response_done("resp_1"))

        creating_again = asyncio.create_task(session.create_response())
        request = await connection.wait_sent("response.create")
        connection.push(response_created("resp_2", request_id_of(request), conversation_id="conv_1"))
        assert await asyncio.wait_for(creating_again, 1) == "resp_2"


async def test_cancelled_control_call_does_not_claim_a_later_error() -> None:
    """update_session() cancelled while waiting leaves nothing pending: an error {event_id: null} arriving afterwards
    streams as error, and the next update_session() returns on its own session.updated."""
    session, connection = await started()
    connection.responder = None

    async with reading(session) as reader:
        updating = asyncio.create_task(session.update_session({"instructions": "First."}, None))
        await connection.wait_sent("session.update")
        updating.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await updating

        await read_through(connection, reader, rejection("invalid_value", event_id=None))
        assert len(reader.of_kind("error")) == 1

        connection.responder = acknowledge_session_updates
        await asyncio.wait_for(session.update_session({"instructions": "Second."}, None), 1)


async def test_normal_close_ends_receive_and_fails_a_waiting_call() -> None:
    """When the connection closes normally while update_session() waits, receive() ends and update_session() raises
    RealtimeConnectionException."""
    session, connection = await started()
    connection.responder = None

    async with reading(session) as reader:
        updating = asyncio.create_task(session.update_session({"instructions": "Hi"}, None))
        await connection.wait_sent("session.update")
        connection.end()

        with pytest.raises(RealtimeConnectionException):
            await asyncio.wait_for(updating, 1)
        await reader.until(lambda: reader.done)

    assert reader.error is None


async def test_abnormal_close_raises_from_receive() -> None:
    """When the connection raises RealtimeConnectionException(close_code=1011), receive() raises it with
    close_code 1011, and a waiting update_session() fails with it too."""
    session, connection = await started()
    connection.responder = None

    async with reading(session) as reader:
        updating = asyncio.create_task(session.update_session({"instructions": "Hi"}, None))
        await connection.wait_sent("session.update")
        connection.fail(RealtimeConnectionException("Server error.", close_code=1011))

        with pytest.raises(RealtimeConnectionException) as caught:
            await asyncio.wait_for(updating, 1)
        await reader.until(lambda: reader.done)

    assert caught.value.close_code == 1011
    assert isinstance(reader.error, RealtimeConnectionException)
    assert reader.error.close_code == 1011


async def test_an_unexpected_receive_failure_fails_a_waiting_call() -> None:
    """When the connection's receive() raises RuntimeError("Broken."), receive() raises it, and a waiting
    update_session() fails with RealtimeConnectionException caused by it."""
    session, connection = await started()
    connection.responder = None

    async with reading(session) as reader:
        updating = asyncio.create_task(session.update_session({"instructions": "Hi"}, None))
        await connection.wait_sent("session.update")
        connection.fail(RuntimeError("Broken."))

        with pytest.raises(RealtimeConnectionException) as caught:
            await asyncio.wait_for(updating, 1)
        await reader.until(lambda: reader.done)

    assert isinstance(caught.value.__cause__, RuntimeError)
    assert isinstance(reader.error, RuntimeError)


async def test_second_concurrent_reader_raises() -> None:
    """While one task iterates receive(), starting a second iteration raises RuntimeError."""
    session, connection = await started()

    async with reading(session):
        await asyncio.sleep(0.01)
        with pytest.raises(RuntimeError):
            await next_event(session.receive().__aiter__(), 0.1)


async def test_control_call_after_close_raises_without_sending() -> None:
    """After close(), update_session() raises RealtimeConnectionException and sends nothing."""
    session, connection = await started()
    await session.close()

    with pytest.raises(RealtimeConnectionException):
        await asyncio.wait_for(session.update_session({"instructions": "Hi"}, None), 0.5)

    assert connection.sent == []


async def test_connection_closing_while_opening_raises() -> None:
    """When the connection closes normally before session.updated, start() raises RealtimeConnectionException."""
    connection = FakeConnection(responder=None)
    session = OpenAIRealtimeSession(connection)
    connection.end()

    with pytest.raises(RealtimeConnectionException):
        await asyncio.wait_for(session.start(options={"instructions": "Hi"}), 0.5)


async def test_async_with_returns_the_session_and_closes_it() -> None:
    """``async with session as entered`` gives back the session itself and closes the connection on exit."""
    session, connection = await started()

    async with session as entered:
        assert entered is session

    assert connection.close_count == 1


async def test_close_is_idempotent_and_stops_sending() -> None:
    """close() twice closes the connection once; send() afterwards raises RealtimeConnectionException."""
    session, connection = await started()

    await session.close()
    await session.close()

    assert connection.close_count == 1
    with pytest.raises(RealtimeConnectionException):
        await session.send(Message("user", ["Hi"]))
    assert connection.sent == []


# endregion

# region Mapping events to kinds


async def test_speech_events_map_to_kinds() -> None:
    """input_audio_buffer.speech_started {item_id: "item_1", audio_start_ms: 100} yields speech_started with core
    {item_id: "item_1", audio_start_ms: 100}; speech_stopped likewise with audio_end_ms 900."""
    started_event, stopped_event = await mapped(
        {"type": "input_audio_buffer.speech_started", "event_id": "e1", "item_id": "item_1", "audio_start_ms": 100},
        {"type": "input_audio_buffer.speech_stopped", "event_id": "e2", "item_id": "item_1", "audio_end_ms": 900},
    )

    assert started_event.kind == "speech_started"
    assert started_event.service_event_type == "input_audio_buffer.speech_started"
    assert started_event.additional_properties["core"] == {"item_id": "item_1", "audio_start_ms": 100}
    assert stopped_event.kind == "speech_stopped"
    assert stopped_event.additional_properties["core"] == {"item_id": "item_1", "audio_end_ms": 900}


async def test_response_created_maps_with_its_response_id() -> None:
    """response.created {id: "resp_1"} yields response_created with core {response_id: "resp_1"}."""
    (created,) = await mapped(response_created("resp_1"))

    assert created.kind == "response_created"
    assert created.service_event_type == "response.created"
    assert created.additional_properties["core"] == {"response_id": "resp_1"}


async def test_completed_response_done_emits_usage_then_response_done() -> None:
    """response.done {id: "resp_1", status: "completed", usage: {input_tokens: 10, output_tokens: 20,
    total_tokens: 30}} yields usage (input 10, output 20, total 30; core {response_id: "resp_1"}), then
    response_done with core {response_id: "resp_1", status: "completed"}."""
    usage, done = await mapped(
        response_done("resp_1", usage={"input_tokens": 10, "output_tokens": 20, "total_tokens": 30})
    )

    assert usage.kind == "usage"
    assert usage.service_event_type == "response.done"
    assert usage.content.type == "usage"
    assert usage.content.usage_details == {"input_token_count": 10, "output_token_count": 20, "total_token_count": 30}
    assert usage.additional_properties["core"] == {"response_id": "resp_1"}
    assert done.kind == "response_done"
    assert done.additional_properties["core"] == {"response_id": "resp_1", "status": "completed"}


async def test_failed_response_done_emits_usage_error_then_response_done() -> None:
    """response.done {status: "failed", status_details: {error: {code: "server_error", message: "Boom."}}} yields
    usage, then error (message "Boom.", error_code "server_error", core {response_id}), then response_done with
    error_code "server_error" and error_message "Boom."."""
    events = await mapped(
        response_done(
            "resp_1",
            "failed",
            status_details={
                "type": "failed",
                "error": {"type": "server_error", "code": "server_error", "message": "Boom."},
            },
            usage={"input_tokens": 1, "output_tokens": 0, "total_tokens": 1},
        )
    )

    assert [event.kind for event in events] == ["usage", "error", "response_done"]
    error = events[1]
    assert error.content.type == "error"
    assert error.content.message == "Boom."
    assert error.content.error_code == "server_error"
    assert error.additional_properties["core"] == {"response_id": "resp_1"}
    assert events[2].additional_properties["core"] == {
        "response_id": "resp_1",
        "status": "failed",
        "error_code": "server_error",
        "error_message": "Boom.",
    }


async def test_response_done_carries_the_reason_verbatim() -> None:
    """response.done {status: "cancelled", status_details: {reason: "turn_detected"}} yields response_done with
    status "cancelled" and reason "turn_detected"."""
    (done,) = await mapped(
        response_done("resp_1", "cancelled", status_details={"type": "cancelled", "reason": "turn_detected"})
    )

    assert done.additional_properties["core"] == {
        "response_id": "resp_1",
        "status": "cancelled",
        "reason": "turn_detected",
    }


async def test_usage_maps_detail_keys() -> None:
    """input_token_details {cached_tokens: 4, text_tokens: 6, audio_tokens: 4, cached_tokens_details: {text_tokens: 1,
    audio_tokens: 3}} and output_token_details {text_tokens: 5, audio_tokens: 15, reasoning_tokens: 2} map to
    cache_read_input_token_count 4, text/audio_input_token_count 6/4, text/audio_cache_read_input_token_count 1/3,
    text/audio_output_token_count 5/15 and reasoning_output_token_count 2; image keys are absent."""
    usage, _ = await mapped(
        response_done(
            "resp_1",
            usage={
                "input_tokens": 10,
                "output_tokens": 20,
                "total_tokens": 30,
                "input_token_details": {
                    "cached_tokens": 4,
                    "text_tokens": 6,
                    "audio_tokens": 4,
                    "cached_tokens_details": {"text_tokens": 1, "audio_tokens": 3},
                },
                "output_token_details": {"text_tokens": 5, "audio_tokens": 15, "reasoning_tokens": 2},
            },
        )
    )

    assert usage.content.usage_details == {
        "input_token_count": 10,
        "output_token_count": 20,
        "total_token_count": 30,
        "cache_read_input_token_count": 4,
        "text_input_token_count": 6,
        "audio_input_token_count": 4,
        "text_cache_read_input_token_count": 1,
        "audio_cache_read_input_token_count": 3,
        "text_output_token_count": 5,
        "audio_output_token_count": 15,
        "reasoning_output_token_count": 2,
    }


async def test_item_added_maps_order_and_role() -> None:
    """conversation.item.added for a user message with previous_item_id null yields item_added with core
    {item_id, previous_item_id: None, role: "user"}; a function_call item yields no role."""
    message, call = await mapped(
        {
            "type": "conversation.item.added",
            "event_id": "e1",
            "previous_item_id": None,
            "item": {"id": "item_1", "type": "message", "role": "user", "content": []},
        },
        {
            "type": "conversation.item.added",
            "event_id": "e2",
            "previous_item_id": "item_1",
            "item": {"id": "item_2", "type": "function_call", "call_id": "call_1", "name": "lookup", "arguments": ""},
        },
    )

    assert message.kind == "item_added"
    assert message.additional_properties["core"] == {"item_id": "item_1", "previous_item_id": None, "role": "user"}
    assert call.additional_properties["core"] == {"item_id": "item_2", "previous_item_id": "item_1"}


async def test_input_transcription_maps_to_text() -> None:
    """input_audio_transcription.delta "Hel" yields input_transcription text "Hel" with merge_mode "append";
    .completed "Hello" yields text "Hello" with merge_mode "replace" and status "completed"; both carry core
    {item_id, content_index}."""
    delta, completed = await mapped(
        {
            "type": "conversation.item.input_audio_transcription.delta",
            "event_id": "e1",
            "item_id": "item_1",
            "content_index": 0,
            "delta": "Hel",
        },
        {
            "type": "conversation.item.input_audio_transcription.completed",
            "event_id": "e2",
            "item_id": "item_1",
            "content_index": 0,
            "transcript": "Hello",
        },
    )

    assert delta.kind == "input_transcription"
    assert (delta.content.type, delta.content.text, delta.content.merge_mode) == ("text", "Hel", "append")
    assert delta.additional_properties["core"] == {"item_id": "item_1", "content_index": 0}
    assert completed.kind == "input_transcription"
    assert (completed.content.text, completed.content.merge_mode, completed.content.status) == (
        "Hello",
        "replace",
        "completed",
    )
    assert completed.additional_properties["core"] == {"item_id": "item_1", "content_index": 0}


async def test_input_transcription_usage_stays_on_the_transcript() -> None:
    """input_audio_transcription.completed with usage {type: "tokens", input_tokens: 12, output_tokens: 3,
    total_tokens: 15} yields only input_transcription, whose text content holds that usage under openai."""
    usage = {"type": "tokens", "input_tokens": 12, "output_tokens": 3, "total_tokens": 15}

    events = await mapped({
        "type": "conversation.item.input_audio_transcription.completed",
        "event_id": "e1",
        "item_id": "item_1",
        "content_index": 0,
        "transcript": "Hello",
        "usage": usage,
    })

    assert [event.kind for event in events] == ["input_transcription"]
    assert events[0].content.additional_properties == {"openai": {"usage": usage}}


async def test_input_transcription_failed_maps_error_fields() -> None:
    """input_audio_transcription.failed {item_id, content_index: 0, error: {code: "x", message: "Y"}} yields
    input_transcription_failed with core {item_id, content_index: 0, error_code: "x", error_message: "Y"}."""
    (failed,) = await mapped({
        "type": "conversation.item.input_audio_transcription.failed",
        "event_id": "e1",
        "item_id": "item_1",
        "content_index": 0,
        "error": {"type": "transcription_error", "code": "x", "message": "Y"},
    })

    assert failed.kind == "input_transcription_failed"
    assert failed.content is None
    assert failed.additional_properties["core"] == {
        "item_id": "item_1",
        "content_index": 0,
        "error_code": "x",
        "error_message": "Y",
    }


async def test_output_audio_transcript_maps_to_output_text() -> None:
    """response.output_audio_transcript.delta "Hi" yields output_text "Hi" (append) and .done "Hi there" yields
    output_text "Hi there" (replace, completed), each with core {response_id, item_id, content_index}."""
    delta, done = await mapped(
        {**AGENT_PART, "type": "response.output_audio_transcript.delta", "event_id": "e1", "delta": "Hi"},
        {**AGENT_PART, "type": "response.output_audio_transcript.done", "event_id": "e2", "transcript": "Hi there"},
    )

    assert_agent_text(delta, "Hi", "append", None)
    assert_agent_text(done, "Hi there", "replace", "completed")


async def test_output_text_maps_to_output_text() -> None:
    """response.output_text.delta and .done map to output_text exactly as the audio transcript does."""
    delta, done = await mapped(
        {**AGENT_PART, "type": "response.output_text.delta", "event_id": "e1", "delta": "Hi"},
        {**AGENT_PART, "type": "response.output_text.done", "event_id": "e2", "text": "Hi there"},
    )

    assert_agent_text(delta, "Hi", "append", None)
    assert_agent_text(done, "Hi there", "replace", "completed")


@pytest.mark.parametrize(
    ("options", "media_type"),
    [(None, "audio/pcm;rate=24000"), ({"output_audio_format": "audio/pcmu"}, "audio/pcmu")],
)
async def test_output_audio_maps_to_data_with_the_output_format(
    options: RealtimeOptions | None, media_type: str
) -> None:
    """response.output_audio.delta with base64 of b"\\x01\\x02" yields output_audio holding data b"\\x01\\x02" with
    media_type "audio/pcm;rate=24000" by default, or "audio/pcmu" after output_audio_format "audio/pcmu", and core
    {response_id, item_id, content_index}."""
    (audio,) = await mapped(
        {
            **AGENT_PART,
            "type": "response.output_audio.delta",
            "event_id": "e1",
            "delta": base64.b64encode(b"\x01\x02").decode(),
        },
        options=options,
    )

    assert audio.kind == "output_audio"
    assert audio.content.type == "data"
    assert audio.content.media_type == media_type
    assert data_bytes(audio.content) == b"\x01\x02"
    assert audio.additional_properties["core"] == AGENT_PART


async def test_function_call_item_maps_to_function_call() -> None:
    """response.output_item.done with a function_call item {call_id: "call_1", name: "lookup", arguments: "{}"}
    yields function_call content with core {response_id, item_id}; a message item's output_item.done passes through."""
    call, message = await mapped(
        {
            "type": "response.output_item.done",
            "event_id": "e1",
            "response_id": "resp_1",
            "output_index": 0,
            "item": {"id": "item_1", "type": "function_call", "call_id": "call_1", "name": "lookup", "arguments": "{}"},
        },
        {
            "type": "response.output_item.done",
            "event_id": "e2",
            "response_id": "resp_1",
            "output_index": 1,
            "item": {"id": "item_2", "type": "message", "role": "assistant", "content": []},
        },
    )

    assert call.kind == "function_call"
    assert call.service_event_type == "response.output_item.done"
    assert (call.content.type, call.content.call_id, call.content.name, call.content.arguments) == (
        "function_call",
        "call_1",
        "lookup",
        "{}",
    )
    assert call.additional_properties["core"] == {"response_id": "resp_1", "item_id": "item_1"}
    assert message.kind == "passthrough"


async def test_unknown_events_pass_through_under_openai() -> None:
    """rate_limits.updated {event_id: "e1", rate_limits: [...]} yields passthrough with no content,
    service_event_type "rate_limits.updated" and openai holding the event's top-level fields."""
    wire = {"type": "rate_limits.updated", "event_id": "e1", "rate_limits": [{"name": "tokens", "remaining": 10}]}

    (passthrough,) = await mapped(wire)

    assert passthrough.kind == "passthrough"
    assert passthrough.content is None
    assert passthrough.service_event_type == "rate_limits.updated"
    assert passthrough.additional_properties == {"openai": wire}


# endregion

# region send()


async def test_send_raw_command_verbatim() -> None:
    """send(RealtimeEvent("passthrough", raw_representation={"type": "input_audio_buffer.clear"})) sends exactly that
    mapping."""
    session, connection = await started()

    await session.send(RealtimeEvent("passthrough", raw_representation={"type": "input_audio_buffer.clear"}))

    assert connection.sent == [{"type": "input_audio_buffer.clear"}]


async def test_send_raw_command_without_mapping_raises() -> None:
    """send(RealtimeEvent("passthrough", raw_representation="x")) raises TypeError and sends nothing."""
    session, connection = await started()

    with pytest.raises(TypeError):
        await session.send(RealtimeEvent("passthrough", raw_representation="x"))

    assert connection.sent == []


async def test_send_audio_data_appends_to_the_input_buffer() -> None:
    """send(Message("user", [Content.from_data(b"\\x00\\x01", "audio/pcm;rate=24000")])) sends
    input_audio_buffer.append with audio base64 "AAE="."""
    session, connection = await started()

    await session.send(Message("user", [Content.from_data(b"\x00\x01", "audio/pcm;rate=24000")]))

    assert without_event_ids(connection.sent) == [{"type": "input_audio_buffer.append", "audio": "AAE="}]


async def test_send_external_audio_uri_is_dropped_without_logging_the_uri(caplog: pytest.LogCaptureFixture) -> None:
    """Audio uri content pointing at a SAS URL sends nothing and warns, without the URL or its signature in the log."""
    session, connection = await started()
    sas_url = "https://store.blob.core.windows.net/calls/a.wav?sv=2024-01-01&sig=secret-signature"

    await session.send(Message("user", [Content.from_uri(sas_url, media_type="audio/wav")]))

    assert connection.sent == []
    assert "audio" in caplog.text
    assert "store.blob.core.windows.net" not in caplog.text
    assert "secret-signature" not in caplog.text


async def test_send_user_and_system_text_create_input_text_items() -> None:
    """A user "Hi" and a system "Rules." each send conversation.item.create with a message item of their role and
    content [{type: "input_text", text: ...}]."""
    session, connection = await started()

    await session.send(Message("user", ["Hi"]))
    await session.send(Message("system", ["Rules."]))

    assert without_event_ids(connection.sent) == [
        {
            "type": "conversation.item.create",
            "item": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Hi"}]},
        },
        {
            "type": "conversation.item.create",
            "item": {"type": "message", "role": "system", "content": [{"type": "input_text", "text": "Rules."}]},
        },
    ]


async def test_send_assistant_text_creates_an_output_text_item() -> None:
    """An assistant "Earlier reply." sends a message item with content [{type: "output_text", text: "Earlier
    reply."}]."""
    session, connection = await started()

    await session.send(Message("assistant", ["Earlier reply."]))

    assert connection.sent[0]["item"]["content"] == [{"type": "output_text", "text": "Earlier reply."}]


async def test_send_function_call_creates_a_function_call_item() -> None:
    """Content.from_function_call("call_1", "lookup", arguments={"q": "x"}) sends a function_call item {call_id:
    "call_1", name: "lookup", arguments: '{"q": "x"}'}."""
    session, connection = await started()

    await session.send(Message("assistant", [Content.from_function_call("call_1", "lookup", arguments={"q": "x"})]))

    assert without_event_ids(connection.sent) == [
        {
            "type": "conversation.item.create",
            "item": {"type": "function_call", "call_id": "call_1", "name": "lookup", "arguments": '{"q": "x"}'},
        }
    ]


async def test_send_function_result_creates_an_output_item(caplog: pytest.LogCaptureFixture) -> None:
    """Content.from_function_result("call_1", result="42") sends a function_call_output item {call_id: "call_1",
    output: "42"}; a result also holding an image sends only its text and warns once."""
    session, connection = await started()
    with_image = Content.from_function_result(
        "call_2", result=[Content.from_text("A chart."), Content.from_data(b"\x89PNG", "image/png")]
    )

    await session.send(Message("tool", [Content.from_function_result("call_1", result="42"), with_image]))

    assert without_event_ids(connection.sent) == [
        {
            "type": "conversation.item.create",
            "item": {"type": "function_call_output", "call_id": "call_1", "output": "42"},
        },
        {
            "type": "conversation.item.create",
            "item": {"type": "function_call_output", "call_id": "call_2", "output": "A chart."},
        },
    ]
    assert caplog.text.count("call_2") == 1


async def test_send_a_non_text_function_result_sends_it_as_json() -> None:
    """Function results built directly with result {"temperature": 21} and 0 send outputs '{"temperature": 21}' and
    "0", as the chat clients serialize them, never a non-string output."""
    session, connection = await started()

    await session.send(
        Message(
            "tool",
            [
                Content("function_result", call_id="call_1", result={"temperature": 21}),
                Content("function_result", call_id="call_2", result=0),
            ],
        )
    )

    assert [event["item"]["output"] for event in connection.sent] == ['{"temperature": 21}', "0"]


async def test_send_other_content_is_dropped_with_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    """A function_approval_response content sends nothing and warns; contents around it are still sent in order."""
    session, connection = await started()
    approval = Content.from_function_approval_response(
        True, "approval_1", Content.from_function_call("call_1", "lookup", arguments={})
    )
    image = Content.from_uri("https://example.com/chart.png", media_type="image/png")

    await session.send(Message("user", ["Before", approval, image, "After"]))

    assert [event["item"]["content"][0]["text"] for event in connection.sent] == ["Before", "After"]
    assert "function_approval_response" in caplog.text
    assert "uri" in caplog.text


async def test_concurrent_writes_do_not_interleave() -> None:
    """Two concurrent send()s against a connection whose send yields mid-write produce start/end, start/end, never
    overlapping."""
    writes: list[str] = []

    class SlowConnection(FakeConnection):
        async def send(self, event: Mapping[str, Any]) -> None:
            writes.append(f"start {event['type']}")
            await asyncio.sleep(0.01)
            writes.append(f"end {event['type']}")

    session = OpenAIRealtimeSession(SlowConnection())
    await session.start()

    await asyncio.gather(
        session.send(RealtimeEvent("passthrough", raw_representation={"type": "first"})),
        session.send(RealtimeEvent("passthrough", raw_representation={"type": "second"})),
    )

    assert writes == ["start first", "end first", "start second", "end second"]


# endregion

# region Voice


async def test_voice_change_after_first_audio_is_dropped(caplog: pytest.LogCaptureFixture) -> None:
    """After an output_audio event, update_session({"voice": "cedar"}, None) sends nothing, warns, and returns;
    session.options keeps the voice the provider accepted."""
    session, connection = await started({"voice": "marin"})
    first_audio = {**AGENT_PART, "type": "response.output_audio.delta", "event_id": "e1", "delta": "AAA="}

    async with reading(session) as reader:
        await read_through(connection, reader, first_audio)
        await asyncio.wait_for(session.update_session({"voice": "cedar"}, None), 0.5)

    assert connection.sent_types() == ["session.update"]
    assert "'cedar'" in caplog.text
    assert session.options == {"voice": "marin"}


async def test_ignored_voice_change_warns(caplog: pytest.LogCaptureFixture) -> None:
    """update_session({"voice": "cedar"}) acknowledged by a session.updated echoing voice "marin" logs a warning that
    the provider ignored the voice change."""
    session, connection = await started()
    connection.responder = lambda event: (
        [{"type": "session.updated", "event_id": "e1", "session": {"audio": {"output": {"voice": "marin"}}}}]
        if event["type"] == "session.update"
        else []
    )

    async with reading(session):
        await asyncio.wait_for(session.update_session({"voice": "cedar"}, None), 1)

    assert "ignored" in caplog.text
    assert "'cedar'" in caplog.text


# endregion

# region Packaging


def test_session_module_imports_without_the_realtime_extra() -> None:
    """In a fresh interpreter, importing agent_framework_openai._realtime_session loads neither openai nor
    websockets."""
    probe = (
        "import sys, agent_framework_openai._realtime_session; "
        "print(sorted(name for name in ('openai', 'websockets') if name in sys.modules))"
    )

    completed = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=True)

    assert completed.stdout.strip() == "[]"


# endregion


class TestVerification:
    async def test_image_token_details_map(self) -> None:
        """input_token_details {image_tokens: 7, cached_tokens_details: {image_tokens: 2}} map to
        image_input_token_count 7 and image_cache_read_input_token_count 2."""
        usage, _ = await mapped(
            response_done(
                "resp_1",
                usage={
                    "input_tokens": 9,
                    "output_tokens": 0,
                    "total_tokens": 9,
                    "input_token_details": {"image_tokens": 7, "cached_tokens_details": {"image_tokens": 2}},
                },
            )
        )

        assert usage.content.usage_details["image_input_token_count"] == 7
        assert usage.content.usage_details["image_cache_read_input_token_count"] == 2

    async def test_none_turns_a_feature_off(self) -> None:
        """turn_detection None, transcription None and noise_reduction None are sent as null under audio.input."""
        sent = await initial_session({"turn_detection": None, "transcription": None, "noise_reduction": None})

        assert sent == {
            "type": "realtime",
            "audio": {"input": {"turn_detection": None, "transcription": None, "noise_reduction": None}},
        }

    async def test_error_with_the_calls_own_event_id_claims_it(self) -> None:
        """While update_session() waits, an error echoing its event_id fails it with RealtimeControlException."""
        session, connection = await started()
        connection.responder = None

        async with reading(session):
            updating = asyncio.create_task(session.update_session({"instructions": "New."}, None))
            request = await connection.wait_sent("session.update")
            connection.push(rejection("invalid_value", "session.instructions", event_id=request["event_id"]))

            with pytest.raises(RealtimeControlException) as caught:
                await asyncio.wait_for(updating, 1)

        assert (caught.value.code, caught.value.param) == ("invalid_value", "session.instructions")

    async def test_error_with_a_server_event_id_claims_it(self) -> None:
        """An error with event_id "evt_srv_1", never sent by the session, fails the waiting update_session()."""
        session, connection = await started()
        connection.responder = None

        async with reading(session):
            updating = asyncio.create_task(session.update_session({"instructions": "New."}, None))
            await connection.wait_sent("session.update")
            connection.push(rejection("invalid_value", event_id="evt_srv_1"))

            with pytest.raises(RealtimeControlException):
                await asyncio.wait_for(updating, 1)

    async def test_claimed_update_error_streams_as_passthrough(self) -> None:
        """The error that failed update_session() is yielded as passthrough, not error."""
        session, connection = await started()
        connection.responder = None

        async with reading(session) as reader:
            updating = asyncio.create_task(session.update_session({"instructions": "New."}, None))
            await connection.wait_sent("session.update")
            connection.push(rejection("invalid_value", event_id=None))
            with pytest.raises(RealtimeControlException):
                await asyncio.wait_for(updating, 1)
            await read_through(connection, reader)

        assert [event.service_event_type for event in reader.of_kind("passthrough")] == ["error"]
        assert reader.of_kind("error") == []

    async def test_update_session_with_empty_tools_clears_them(self) -> None:
        """update_session({}, []) sends session.update with tools []."""
        session, connection = await started()

        async with reading(session):
            await asyncio.wait_for(session.update_session({}, []), 1)

        assert connection.sent[-1]["session"] == {"type": "realtime", "tools": []}

    async def test_start_with_only_tools_sends_them(self) -> None:
        """start(tools=[lookup]) sends session.update with type "realtime" and the declared function."""

        @tool
        def lookup(query: str) -> str:
            """Look something up."""
            return query

        sent = await initial_session({}, [lookup])

        assert sent == {
            "type": "realtime",
            "tools": [
                {
                    "type": "function",
                    "name": "lookup",
                    "description": "Look something up.",
                    "parameters": lookup.parameters(),
                }
            ],
        }

    async def test_cancelled_create_response_leaves_nothing_pending(self) -> None:
        """create_response() cancelled while waiting; its response.created arriving later doesn't affect the next
        call's correlation."""
        session, connection = await started()

        async with reading(session) as reader:
            abandoned = asyncio.create_task(session.create_response())
            abandoned_request = await connection.wait_sent("response.create")
            abandoned.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await abandoned
            await read_through(
                connection,
                reader,
                response_created("resp_1", request_id_of(abandoned_request)),
                response_done("resp_1"),
            )

            creating = asyncio.create_task(session.create_response())
            request = await connection.wait_sent("response.create")
            connection.push(response_created("resp_2", request_id_of(request)))

            assert await asyncio.wait_for(creating, 1) == "resp_2"

    async def test_per_response_temperature_is_dropped(self, caplog: pytest.LogCaptureFixture) -> None:
        """create_response({"temperature": 0.9}) warns and sends no temperature."""
        session, connection = await started()

        async with reading(session):
            creating = asyncio.create_task(session.create_response({"temperature": 0.9}))
            request = await connection.wait_sent("response.create")
            connection.push(response_created("resp_1", request_id_of(request)))
            await asyncio.wait_for(creating, 1)

        assert "temperature" not in request["response"]
        assert "'temperature'" in caplog.text
