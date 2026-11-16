# Copyright (c) Microsoft. All rights reserved.

"""Live tests of OpenAIRealtimeClient against an Azure OpenAI realtime deployment.

Marked azure, integration and flaky, and skipped unless AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_REALTIME_MODEL are set.
They authenticate with AzureCliCredential. Each test runs its own reader task under asyncio.timeout(), and checks Azure
OpenAI behaviour the unit tests can only assume.
"""

import asyncio
import contextlib
import os
from collections.abc import AsyncGenerator, Callable, Sequence
from typing import Any

import pytest
from agent_framework import Content, Message, RealtimeEvent, tool
from agent_framework.exceptions import RealtimeConnectionException, RealtimeControlException
from azure.identity.aio import AzureCliCredential

from agent_framework_openai import OpenAIRealtimeClient, OpenAIRealtimeOptions

pytestmark = [
    pytest.mark.azure,
    pytest.mark.flaky,
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("AZURE_OPENAI_ENDPOINT", "") == "" or os.getenv("AZURE_OPENAI_REALTIME_MODEL", "") == "",
        reason="No AZURE_OPENAI_ENDPOINT or AZURE_OPENAI_REALTIME_MODEL provided; skipping realtime tests.",
    ),
]

TURN_TIMEOUT = 60
PCM_24K_BYTES_PER_SECOND = 48_000


class Reader:
    """Reads a session's events in the background, as the conversation's receive loop does."""

    def __init__(self, session: Any) -> None:
        self.events: list[Any] = []
        self._task = asyncio.create_task(self._read(session))

    async def _read(self, session: Any) -> None:
        async for event in session.receive():
            self.events.append(event)

    def of_response(self, response_id: str) -> list[Any]:
        return [event for event in self.events if _core(event).get("response_id") == response_id]

    def response_done(self, response_id: str) -> Any | None:
        return next((event for event in self.of_response(response_id) if event.kind == "response_done"), None)

    async def until(self, condition: Callable[[], Any]) -> None:
        while not condition():
            if self._task.done():
                self._task.result()
                raise AssertionError("The session ended before the condition held.")
            await asyncio.sleep(0.02)

    async def stop(self) -> None:
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


def _core(event: Any) -> dict[str, Any]:
    return (event.additional_properties or {}).get("core", {})


@contextlib.asynccontextmanager
async def live_session(
    options: OpenAIRealtimeOptions | None = None, tools: Sequence[Any] | None = None
) -> AsyncGenerator[tuple[Any, Reader]]:
    """A live Azure OpenAI session with a background reader, authenticated by the Azure CLI."""
    async with contextlib.AsyncExitStack() as stack:
        credential = await stack.enter_async_context(AzureCliCredential())
        client = OpenAIRealtimeClient(credential=credential)
        session = await asyncio.wait_for(client.start_session(options=options, tools=tools), 30)
        stack.push_async_callback(session.close)
        reader = Reader(session)
        stack.push_async_callback(reader.stop)
        yield session, reader


def _final_text(events: list[Any]) -> str:
    finals = [event for event in events if event.kind == "output_text" and event.content.status == "completed"]
    return " ".join(event.content.text for event in finals)


async def test_text_turn_yields_events_in_order() -> None:
    """The first update is acknowledged; a user text turn then yields session_created, response_created, output_text
    deltas and a completed final, output_audio with media type "audio/pcm;rate=24000", usage with audio output
    tokens, then response_done with status "completed"."""
    async with asyncio.timeout(TURN_TIMEOUT):
        options: OpenAIRealtimeOptions = {"instructions": "Answer in one short sentence.", "turn_detection": None}
        async with live_session(options) as (session, reader):
            await session.send(Message("user", ["What colour is a clear daytime sky?"]))
            response_id = await session.create_response()
            await reader.until(lambda: reader.response_done(response_id))

    assert reader.events[0].kind == "session_created"
    kinds = [event.kind for event in reader.of_response(response_id)]
    assert kinds[0] == "response_created"
    assert kinds[-2:] == ["usage", "response_done"]
    texts = [event for event in reader.of_response(response_id) if event.kind == "output_text"]
    assert any(event.content.merge_mode == "append" for event in texts)
    assert _final_text(texts)
    audio = [event for event in reader.of_response(response_id) if event.kind == "output_audio"]
    assert audio
    assert {event.content.media_type for event in audio} == {"audio/pcm;rate=24000"}
    usage = next(event for event in reader.of_response(response_id) if event.kind == "usage")
    assert usage.content.usage_details["audio_output_token_count"] > 0
    assert _core(reader.response_done(response_id))["status"] == "completed"


async def test_say_speaks_the_text_best_effort() -> None:
    """say("Thanks for calling.") returns the id of a response whose final output_text contains "Thanks for
    calling"."""
    async with asyncio.timeout(TURN_TIMEOUT):
        async with live_session({"turn_detection": None}) as (session, reader):
            response_id = await session.say("Thanks for calling.")
            await reader.until(lambda: reader.response_done(response_id))

    assert "thanks for calling" in _final_text(reader.of_response(response_id)).lower()


async def test_rejected_update_is_claimed_and_per_response_options_apply() -> None:
    """An update Azure OpenAI rejects raises RealtimeControlException, and its error, echoing the update's event_id,
    streams as passthrough; create_response({"output_modalities": ["text"]}) in an audio session yields its final
    output_text from response.output_text.done and no output_audio."""
    async with asyncio.timeout(TURN_TIMEOUT):
        async with live_session({"turn_detection": None}) as (session, reader):
            with pytest.raises(RealtimeControlException):
                await session.update_session({"turn_detection": {"type": "not_a_detector"}}, None)

            await session.send(Message("user", ["What colour is a clear daytime sky?"]))
            response_id = await session.create_response({"output_modalities": ["text"]})
            await reader.until(lambda: reader.response_done(response_id))

    claimed = [event for event in reader.events if event.service_event_type == "error"]
    assert [event.kind for event in claimed] == ["passthrough"]
    assert claimed[0].raw_representation["error"]["event_id"].startswith("af_")
    events = reader.of_response(response_id)
    assert _final_text(events)
    text_events = {event.service_event_type for event in events if event.kind == "output_text"}
    assert "response.output_text.done" in text_events
    assert not any(event.kind == "output_audio" for event in events)


async def test_function_call_round_trip() -> None:
    """With a get_time tool declared and tool_choice "required", function_call arrives with call_id, name "get_time"
    and arguments naming Paris; sending its function_result and create_response() yields a follow-up response with
    output_text."""

    @tool
    def get_time(city: str) -> str:
        """Get the current time in a city."""
        return "12:00"

    async with asyncio.timeout(TURN_TIMEOUT):
        options: OpenAIRealtimeOptions = {"turn_detection": None, "tool_choice": "required"}
        async with live_session(options, [get_time]) as (session, reader):
            await session.send(Message("user", ["What time is it in Paris?"]))
            calling = await session.create_response()
            await reader.until(lambda: reader.response_done(calling))
            call = next(event for event in reader.of_response(calling) if event.kind == "function_call")

            await session.send(Message("tool", [Content.from_function_result(call.content.call_id, result="12:00")]))
            answering = await session.create_response({"tool_choice": "none"})
            await reader.until(lambda: reader.response_done(answering))

    assert call.content.name == "get_time"
    assert "paris" in call.content.arguments.lower()
    assert _final_text(reader.of_response(answering))


async def test_cancel_and_truncate() -> None:
    """cancel_response() during a long reply resolves on response_done with status "cancelled" and reason
    "client_cancelled"; truncate(item, 0, 500) yields item_truncated with truncated_by "client"; cancel_response()
    with nothing active raises response_cancel_not_active; a raw response.cancel naming the finished response gets an
    error with code "response_cancel_not_active"."""
    async with asyncio.timeout(TURN_TIMEOUT):
        async with live_session({"turn_detection": None}) as (session, reader):
            await session.send(Message("user", ["Tell me a long story about a lighthouse keeper."]))
            response_id = await session.create_response()

            def audio_bytes() -> int:
                audio = [event for event in reader.of_response(response_id) if event.kind == "output_audio"]
                return sum(len(event.content.get_data_bytes()) for event in audio)

            await reader.until(lambda: audio_bytes() >= PCM_24K_BYTES_PER_SECOND)
            await session.cancel_response()
            await reader.until(lambda: reader.response_done(response_id))
            done = reader.response_done(response_id)
            item_id = next(
                _core(event)["item_id"] for event in reader.of_response(response_id) if event.kind == "output_audio"
            )

            await session.truncate(item_id, 0, 500)
            await reader.until(lambda: any(event.kind == "item_truncated" for event in reader.events))

            with pytest.raises(RealtimeControlException) as caught:
                await session.cancel_response()

            late_cancel = {"type": "response.cancel", "event_id": "late_cancel", "response_id": response_id}
            await session.send(RealtimeEvent("passthrough", raw_representation=late_cancel))
            await reader.until(lambda: any(event.kind == "error" for event in reader.events))

    assert done is not None
    assert _core(done)["status"] == "cancelled"
    assert _core(done)["reason"] == "client_cancelled"
    truncated = next(event for event in reader.events if event.kind == "item_truncated")
    assert _core(truncated) == {"item_id": item_id, "content_index": 0, "audio_end_ms": 500, "truncated_by": "client"}
    assert caught.value.code == "response_cancel_not_active"
    late_error = next(event for event in reader.events if event.kind == "error")
    assert late_error.content.error_code == "response_cancel_not_active"


async def test_typed_settings_are_accepted() -> None:
    """start_session with speed 1.2, a retention-ratio truncation of 0.8, semantic_vad eagerness "low" and a
    transcription prompt is acknowledged, and session.updated echoes each of them."""
    options: OpenAIRealtimeOptions = {
        "speed": 1.2,
        "truncation": {"type": "retention_ratio", "retention_ratio": 0.8},
        "turn_detection": {"type": "semantic_vad", "eagerness": "low", "create_response": False},
        "transcription": {"model": "gpt-4o-transcribe", "prompt": "Contoso Dental"},
    }

    async with asyncio.timeout(TURN_TIMEOUT), live_session(options) as (_, reader):
        await reader.until(lambda: any(event.kind == "session_updated" for event in reader.events))

    echoed = next(event for event in reader.events if event.kind == "session_updated").raw_representation["session"]
    assert echoed["audio"]["output"]["speed"] == 1.2
    assert echoed["truncation"]["retention_ratio"] == 0.8
    assert echoed["audio"]["input"]["turn_detection"]["eagerness"] == "low"
    assert echoed["audio"]["input"]["transcription"]["prompt"] == "Contoso Dental"


async def test_bad_key_raises() -> None:
    """OpenAIRealtimeClient(azure_endpoint=..., api_key="not-a-key").start_session() raises
    RealtimeConnectionException."""
    client = OpenAIRealtimeClient(api_key="not-a-key", azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"])

    with pytest.raises(RealtimeConnectionException):
        await asyncio.wait_for(client.start_session(), 30)
