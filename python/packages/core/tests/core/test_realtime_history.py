# Copyright (c) Microsoft. All rights reserved.

"""Voice history: the call's turns as messages, exchanges reported to context providers, replay and close."""

import asyncio
import logging
from collections.abc import Sequence
from typing import Any

import pytest

from agent_framework import (
    AgentResponseUpdate,
    AgentSession,
    Content,
    ContextProvider,
    InMemoryHistoryProvider,
    Message,
    RealtimeAgent,
    RealtimeEvent,
    RealtimeKind,
    RealtimeOptions,
    SessionContext,
    SupportsAgentRun,
    tool,
)
from agent_framework._realtime import _conversation as realtime_conversation
from agent_framework.exceptions import RealtimeConnectionException

from .realtime_helpers import FakeRealtimeClient, event, eventually, next_update

_TRANSCRIBED: RealtimeOptions = {"transcription": {"model": "gpt-4o-transcribe"}}


@pytest.fixture(autouse=True)
def _short_transcript_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    """Close waits for pending caller transcripts; the tests shouldn't wait the production seconds for them."""
    monkeypatch.setattr(realtime_conversation, "CALLER_TRANSCRIPT_WAIT_SECONDS", 0.2)


def _caller_item(item_id: str) -> RealtimeEvent:
    return event("item_added", item_id=item_id, previous_item_id=None, role="user", input_audio=True)


def _caller_transcript(item_id: str, text: str) -> RealtimeEvent:
    final = Content.from_text(text, merge_mode="replace", status="completed")
    return event("input_transcription", content=final, item_id=item_id, content_index=0)


def _caller_delta(item_id: str, text: str) -> RealtimeEvent:
    delta = Content.from_text(text, merge_mode="append")
    return event("input_transcription", content=delta, item_id=item_id, content_index=0)


def _agent_item(item_id: str) -> RealtimeEvent:
    return event("item_added", item_id=item_id, previous_item_id=None, role="assistant")


def _agent_text(response_id: str, item_id: str, text: str, *, final: bool = True) -> RealtimeEvent:
    content = (
        Content.from_text(text, merge_mode="replace", status="completed")
        if final
        else Content.from_text(text, merge_mode="append")
    )
    return event("output_text", content=content, response_id=response_id, item_id=item_id, content_index=0)


def _agent_audio(response_id: str, item_id: str, milliseconds: int) -> RealtimeEvent:
    """24 kHz 16-bit mono PCM: 48 bytes per millisecond."""
    audio = Content.from_data(b"\x00" * 48 * milliseconds, "audio/pcm;rate=24000")
    return event("output_audio", content=audio, response_id=response_id, item_id=item_id, content_index=0)


def _truncated(item_id: str, audio_end_ms: int) -> RealtimeEvent:
    return event("item_truncated", item_id=item_id, content_index=0, audio_end_ms=audio_end_ms, truncated_by="client")


def _spoken_reply(response_id: str, item_id: str, text: str, milliseconds: int) -> list[RealtimeEvent]:
    """A finished response that speaks ``text`` as ``milliseconds`` of audio in one assistant item."""
    return [
        event("response_created", response_id=response_id),
        _agent_item(item_id),
        _agent_audio(response_id, item_id, milliseconds),
        _agent_text(response_id, item_id, text),
        event("response_done", response_id=response_id, status="completed"),
    ]


def _reply(response_id: str, item_id: str, text: str, *, status: str = "completed") -> list[RealtimeEvent]:
    """A response that says ``text`` in one assistant item and finishes with ``status``."""
    return [
        event("response_created", response_id=response_id),
        _agent_item(item_id),
        _agent_text(response_id, item_id, text),
        event("response_done", response_id=response_id, status=status),
    ]


async def _streamed(conversation: Any, *events: RealtimeEvent) -> None:
    """Have the provider send ``events``, and wait until the conversation has streamed them."""
    conversation._session.emit(*events)  # pyright: ignore[reportPrivateUsage]
    for _ in events:
        await next_update(conversation)


def _dicts(messages: Sequence[Message]) -> list[dict[str, Any]]:
    return [message.to_dict() for message in messages]


def _shapes(messages: Sequence[Message]) -> list[tuple[str, list[str], str | None, str | None]]:
    """Each message's role, content types, author and id, for messages whose contents carry generated ids."""
    return [
        (message.role, [content.type for content in message.contents], message.author_name, message.message_id)
        for message in messages
    ]


class _Recorder(ContextProvider):
    """Records every exchange reported to it."""

    def __init__(self, source_id: str = "recorder") -> None:
        super().__init__(source_id)
        self.reports: list[SessionContext] = []

    async def after_run(
        self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        self.reports.append(context)


class _Loader(_Recorder):
    """Adds context when the conversation opens, and notes whether the realtime session had started by then."""

    def __init__(self, client: FakeRealtimeClient, source_id: str = "loader") -> None:
        super().__init__(source_id)
        self.client = client
        self.loads: list[bool] = []

    async def before_run(
        self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        self.loads.append(bool(self.client.start_session_calls))


class _Failing(_Recorder):
    """Fails its first ``failures`` reports, then records."""

    def __init__(self, source_id: str = "failing", *, failures: int = 1_000) -> None:
        super().__init__(source_id)
        self.failures = failures

    async def after_run(
        self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        if self.failures:
            self.failures -= 1
            raise RuntimeError(f"{self.source_id} store down")
        await super().after_run(agent=agent, session=session, context=context, state=state)


class _Slow(_Recorder):
    """Holds its first report until released, noting whether that report was cancelled."""

    def __init__(self) -> None:
        super().__init__("slow")
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = False

    async def after_run(
        self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        if not self.started.is_set():
            self.started.set()
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
        await super().after_run(agent=agent, session=session, context=context, state=state)


async def _read_all(conversation: Any) -> list[AgentResponseUpdate]:
    return [update async for update in conversation]


async def _until_error(conversation: Any) -> AgentResponseUpdate:
    while True:
        update = await next_update(conversation)
        if any(content.type == "error" for content in update.contents):
            return update


def _texts(report: SessionContext) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """An exchange's inputs and outputs as (role, text) pairs."""
    outputs = report.response.messages if report.response is not None else []
    return (
        [(message.role, message.text) for message in report.input_messages],
        [(message.role, message.text) for message in outputs],
    )


@tool
def get_weather(city: str) -> str:
    """Get the weather for a city."""
    return f"Sunny in {city}"


def _call_response(response_id: str, item_id: str, call_id: str, name: str, **arguments: Any) -> list[RealtimeEvent]:
    """A response that makes one call in a role-less item, then finishes."""
    call = Content.from_function_call(call_id=call_id, name=name, arguments=arguments)
    return [
        event("response_created", response_id=response_id),
        event("item_added", item_id=item_id, previous_item_id=None),
        event("function_call", content=call, response_id=response_id, item_id=item_id),
        event("response_done", response_id=response_id, status="completed"),
    ]


def _blocking_tool() -> tuple[Any, asyncio.Event]:
    """A look_up tool that waits for the returned release."""
    release = asyncio.Event()

    @tool
    async def look_up() -> str:
        """Look something up, slowly."""
        await release.wait()
        return "found"

    return look_up, release


async def _until_tool_result(conversation: Any) -> AgentResponseUpdate:
    while True:
        update = await next_update(conversation)
        if any(content.type == "function_result" for content in update.contents):
            return update


# region History


async def test_history_is_empty_before_anything_is_said() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        assert conversation.history == []


async def test_a_caller_turn_records_its_final_transcript() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(conversation, _caller_item("item_1"), _caller_transcript("item_1", "Hello"))

        assert _dicts(conversation.history) == _dicts([
            Message("user", [Content.from_text("Hello", status="completed")], message_id="item_1")
        ])


async def test_an_agent_turn_records_its_final_transcript_credited_to_the_agent() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, name="receptionist").start_conversation() as conversation:
        await _streamed(conversation, *_reply("resp_1", "item_2", "Hi there"))

        assert _dicts(conversation.history) == _dicts([
            Message(
                "assistant",
                [Content.from_text("Hi there", status="completed")],
                author_name="receptionist",
                message_id="item_2",
            )
        ])


async def test_app_text_is_recorded_as_sent_and_its_echo_is_not_recorded_again() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.send("The caller's account is 1234", create_response=False)
        await _streamed(
            conversation, event("item_added", item_id="item_1", previous_item_id=None, role="user", input_audio=False)
        )

        assert _dicts(conversation.history) == _dicts([Message("user", ["The caller's account is 1234"])])


async def test_initial_messages_are_recorded_as_app_input() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation("You're speaking to Ada.") as conversation:
        assert _dicts(conversation.history) == _dicts([Message("user", ["You're speaking to Ada."])])


async def test_app_input_is_recorded_without_its_audio() -> None:
    client = FakeRealtimeClient()
    audio = Content.from_data(b"\x00\x00", "audio/pcm;rate=24000")

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.send(Message("user", ["Listen to this", audio]), create_response=False)
        await conversation.send(Message("user", [audio]))

        assert _dicts(conversation.history) == _dicts([Message("user", ["Listen to this"])])


async def test_a_function_call_is_recorded_with_its_result_straight_after_it() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, name="receptionist", tools=[get_weather])

    async with agent.start_conversation() as conversation:
        client.session.emit(*_call_response("resp_a", "item_3", "call_1", "get_weather", city="Paris"))
        await _until_tool_result(conversation)

        history = conversation.history
        assert _shapes(history) == [
            ("assistant", ["function_call"], "receptionist", "item_3"),
            ("tool", ["function_result"], "receptionist", None),
        ]
        assert (history[0].contents[0].call_id, history[1].contents[0].call_id) == ("call_1", "call_1")
        assert history[1].contents[0].result == "Sunny in Paris"


async def test_a_failed_transcription_records_a_text_less_caller_turn() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(
            conversation,
            _caller_item("item_1"),
            event("input_transcription_failed", item_id="item_1", content_index=0, error_code="audio_unintelligible"),
        )

        assert _dicts(conversation.history) == _dicts([
            Message(
                "user", [], message_id="item_1", additional_properties={"core": {"text_less": "transcription_failed"}}
            )
        ])


async def test_a_failed_transcription_keeps_the_partial_transcript_as_incomplete() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(
            conversation,
            _caller_item("item_1"),
            _caller_delta("item_1", "I need"),
            _caller_delta("item_1", " to"),
            event("input_transcription_failed", item_id="item_1", content_index=0),
        )

        assert _dicts(conversation.history) == _dicts([
            Message("user", [Content.from_text("I need to", status="incomplete")], message_id="item_1")
        ])


async def test_a_caller_turn_still_waiting_for_its_transcript_shows_as_pending() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(conversation, _caller_item("item_1"))

        assert _dicts(conversation.history) == _dicts([
            Message(
                "user", [], message_id="item_1", additional_properties={"core": {"text_less": "transcript_pending"}}
            )
        ])


async def test_without_transcription_a_caller_turn_is_text_less_at_once() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(options={"transcription": None}) as conversation:
        await _streamed(conversation, _caller_item("item_1"))

        assert _dicts(conversation.history) == _dicts([
            Message("user", [], message_id="item_1", additional_properties={"core": {"text_less": "transcription_off"}})
        ])


async def test_a_truncated_agent_turn_keeps_the_heard_words_as_incomplete() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _streamed(conversation, *_spoken_reply("resp_1", "item_2", "one two three four", 1000))
        await _streamed(conversation, _truncated("item_2", 500))

        assert _dicts(conversation.history) == _dicts([
            Message("assistant", [Content.from_text("one two", status="incomplete")], message_id="item_2")
        ])


async def test_an_agent_turn_cut_at_0_ms_is_left_out() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _streamed(conversation, *_spoken_reply("resp_1", "item_2", "one two three four", 1000))
        await _streamed(conversation, _truncated("item_2", 0))

        assert conversation.history == []


async def test_a_cancelled_response_keeps_the_text_generated_so_far_as_incomplete() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _streamed(
            conversation,
            event("response_created", response_id="resp_1"),
            _agent_item("item_2"),
            _agent_text("resp_1", "item_2", "Let me", final=False),
            _agent_text("resp_1", "item_2", " check", final=False),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )

        assert _dicts(conversation.history) == _dicts([
            Message("assistant", [Content.from_text("Let me check", status="incomplete")], message_id="item_2")
        ])


# endregion

# region Exchanges


async def test_an_exchange_is_reported_once_the_next_input_starts() -> None:
    client = FakeRealtimeClient()
    recorder = _Recorder()
    agent = RealtimeAgent(client, context_providers=[recorder])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(
            conversation,
            _caller_item("item_1"),
            _caller_transcript("item_1", "Hi"),
            *_reply("resp_1", "item_2", "Hello"),
        )
        await asyncio.sleep(0.01)
        assert recorder.reports == []

        await _streamed(conversation, _caller_item("item_3"))
        await eventually(lambda: len(recorder.reports) == 1)

    assert _texts(recorder.reports[0]) == ([("user", "Hi")], [("assistant", "Hello")])


async def test_the_last_exchange_is_reported_at_close() -> None:
    client = FakeRealtimeClient()
    recorder = _Recorder()
    agent = RealtimeAgent(client, context_providers=[recorder])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(
            conversation,
            _caller_item("item_1"),
            _caller_transcript("item_1", "Hi"),
            *_reply("resp_1", "item_2", "Hello"),
        )

    assert len(recorder.reports) == 1
    assert _texts(recorder.reports[0]) == ([("user", "Hi")], [("assistant", "Hello")])


async def test_an_exchange_waits_for_its_pending_caller_transcript() -> None:
    client = FakeRealtimeClient()
    recorder = _Recorder()
    agent = RealtimeAgent(client, context_providers=[recorder])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(
            conversation, _caller_item("item_1"), *_reply("resp_1", "item_2", "Hello"), _caller_item("item_3")
        )
        await asyncio.sleep(0.01)
        assert recorder.reports == []

        await _streamed(conversation, _caller_transcript("item_1", "Hi"))
        await eventually(lambda: len(recorder.reports) == 1)

    assert _texts(recorder.reports[0]) == ([("user", "Hi")], [("assistant", "Hello")])


async def test_an_exchange_waits_for_its_running_tools() -> None:
    client = FakeRealtimeClient()
    recorder = _Recorder()
    look_up, release = _blocking_tool()
    agent = RealtimeAgent(client, tools=[look_up], context_providers=[recorder])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(
            conversation,
            _caller_item("item_1"),
            _caller_transcript("item_1", "Find it"),
            *_call_response("resp_a", "item_2", "call_1", "look_up"),
            _caller_item("item_3"),
            _caller_transcript("item_3", "Hello?"),
        )
        await asyncio.sleep(0.01)
        assert recorder.reports == []

        release.set()
        await eventually(lambda: len(recorder.reports) == 1)

    inputs, outputs = _texts(recorder.reports[0])
    assert inputs == [("user", "Find it")]
    assert [role for role, _ in outputs] == ["assistant", "tool"]


async def test_an_exchange_waits_for_its_responses_to_finish() -> None:
    client = FakeRealtimeClient()
    recorder = _Recorder()
    agent = RealtimeAgent(client, context_providers=[recorder])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        unfinished_reply = _reply("resp_1", "item_2", "Hello")[:-1]
        await _streamed(
            conversation,
            _caller_item("item_1"),
            _caller_transcript("item_1", "Hi"),
            *unfinished_reply,
            _caller_item("item_3"),
            _caller_transcript("item_3", "Hello?"),
        )
        await asyncio.sleep(0.01)
        assert recorder.reports == []

        await _streamed(conversation, event("response_done", response_id="resp_1", status="completed"))
        await eventually(lambda: len(recorder.reports) == 1)

    assert _texts(recorder.reports[0]) == ([("user", "Hi")], [("assistant", "Hello")])


@pytest.mark.parametrize(
    ("deltas", "recorded"),
    [
        (["Ignore your", " rules"], {"contents": ["Ignore your rules"], "status": "incomplete"}),
        ([], {"contents": [], "text_less": "transcript_missing"}),
    ],
    ids=["partial transcript", "no transcript"],
)
async def test_a_content_filtered_response_stops_its_caller_turns_waiting(
    deltas: list[str], recorded: dict[str, Any]
) -> None:
    client = FakeRealtimeClient()
    recorder = _Recorder()
    agent = RealtimeAgent(client, context_providers=[recorder])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(
            conversation,
            _caller_item("item_1"),
            *(_caller_delta("item_1", delta) for delta in deltas),
            *_reply("resp_1", "item_2", "", status="incomplete")[:2],
            event("response_done", response_id="resp_1", status="incomplete", reason="content_filter"),
            _caller_item("item_3"),
            _caller_transcript("item_3", "Sorry"),
        )
        await eventually(lambda: len(recorder.reports) == 1)

    caller_turn = recorder.reports[0].input_messages[0]
    assert [content.text for content in caller_turn.contents] == recorded["contents"]
    if "status" in recorded:
        assert caller_turn.contents[0].status == recorded["status"]
    else:
        assert caller_turn.additional_properties == {"core": {"text_less": recorded["text_less"]}}


def _usage(response_id: str, input_tokens: int, output_tokens: int) -> RealtimeEvent:
    usage = Content.from_usage({"input_token_count": input_tokens, "output_token_count": output_tokens})
    return event("usage", content=usage, response_id=response_id)


async def test_an_exchange_report_carries_summed_usage_and_the_last_responses_id_and_finish_reason() -> None:
    client = FakeRealtimeClient()
    recorder = _Recorder()
    agent = RealtimeAgent(client, tools=[get_weather], context_providers=[recorder])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        follow_up = _reply("resp_b", "item_4", "Sunny in Paris")
        client.session.create_response_events = [*follow_up[:-1], _usage("resp_b", 20, 7), follow_up[-1]]
        calling = _call_response("resp_a", "item_3", "call_1", "get_weather", city="Paris")
        await _streamed(
            conversation,
            _caller_item("item_1"),
            _caller_transcript("item_1", "Weather in Paris?"),
            *calling[:-1],
            _usage("resp_a", 10, 5),
            calling[-1],
        )
        await eventually(lambda: any(message.text == "Sunny in Paris" for message in conversation.history))

    response = recorder.reports[0].response
    assert response is not None
    assert response.usage_details is not None
    assert (response.usage_details["input_token_count"], response.usage_details["output_token_count"]) == (30, 12)
    assert (response.response_id, response.finish_reason) == ("resp_b", "stop")


async def test_a_failing_provider_streams_an_error_and_retries_with_the_next_report() -> None:
    client = FakeRealtimeClient()
    flaky = _Failing("flaky", failures=1)
    agent = RealtimeAgent(client, context_providers=[flaky])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        client.session.emit(
            _caller_item("item_1"),
            _caller_transcript("item_1", "Hi"),
            *_reply("resp_1", "item_2", "Hello"),
            _caller_item("item_3"),
            _caller_transcript("item_3", "Bye"),
        )
        failure = await _until_error(conversation)
        assert "flaky" in (failure.contents[0].message or "")

        await _streamed(conversation, *_reply("resp_2", "item_4", "Goodbye"), _caller_item("item_5"))
        await eventually(lambda: len(flaky.reports) == 2)

    assert [_texts(report) for report in flaky.reports[:2]] == [
        ([("user", "Hi")], [("assistant", "Hello")]),
        ([("user", "Bye")], [("assistant", "Goodbye")]),
    ]


async def test_close_raises_a_provider_failure_that_persists() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, context_providers=[_Failing()])

    with pytest.raises(RuntimeError, match="failing store down"):
        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await _streamed(
                conversation,
                _caller_item("item_1"),
                _caller_transcript("item_1", "Hi"),
                *_reply("resp_1", "item_2", "Hello"),
            )

    assert client.session.close_count == 1
    updates = [update async for update in conversation]
    assert any(content.type == "error" for update in updates for content in update.contents)


async def test_close_raises_an_exception_group_when_several_providers_fail() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, context_providers=[_Failing("memory"), _Failing("history")])

    with pytest.raises(ExceptionGroup) as raised:
        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await _streamed(
                conversation,
                _caller_item("item_1"),
                _caller_transcript("item_1", "Hi"),
                *_reply("resp_1", "item_2", "Hello"),
            )

    assert sorted(str(failure) for failure in raised.value.exceptions) == ["history store down", "memory store down"]


async def test_a_truncation_after_its_exchange_was_reported_changes_nothing() -> None:
    client = FakeRealtimeClient()
    recorder = _Recorder()
    agent = RealtimeAgent(client, context_providers=[recorder])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(
            conversation,
            _caller_item("item_1"),
            _caller_transcript("item_1", "Count"),
            *_spoken_reply("resp_1", "item_2", "one two three four", 1000),
            _caller_item("item_3"),
        )
        await eventually(lambda: len(recorder.reports) == 1)
        await _streamed(conversation, _truncated("item_2", 500))

        reply = conversation.history[1]
        assert (reply.text, reply.contents[0].status) == ("one two three four", "completed")


async def test_close_waits_for_a_report_in_progress_rather_than_cancelling_it() -> None:
    client = FakeRealtimeClient()
    slow = _Slow()
    agent = RealtimeAgent(client, context_providers=[slow])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(
            conversation,
            _caller_item("item_1"),
            _caller_transcript("item_1", "Hi"),
            *_reply("resp_1", "item_2", "Hello"),
            _caller_item("item_3"),
            _caller_transcript("item_3", "Bye"),
            *_reply("resp_2", "item_4", "Goodbye"),
        )
        await slow.started.wait()
        closing = asyncio.create_task(conversation.close())
        await asyncio.sleep(0.01)
        slow.release.set()
        await closing

    assert not slow.cancelled
    assert [_texts(report) for report in slow.reports] == [
        ([("user", "Hi")], [("assistant", "Hello")]),
        ([("user", "Bye")], [("assistant", "Goodbye")]),
    ]


# endregion

# region Start


async def test_before_run_runs_once_at_start() -> None:
    client = FakeRealtimeClient()
    loader = _Loader(client)
    agent = RealtimeAgent(client, context_providers=[loader])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(
            conversation,
            _caller_item("item_1"),
            _caller_transcript("item_1", "Hi"),
            *_reply("resp_1", "item_2", "Hello"),
            _caller_item("item_3"),
            _caller_transcript("item_3", "Bye"),
            *_reply("resp_2", "item_4", "Goodbye"),
        )

    assert loader.loads == [False]


async def test_a_history_provider_that_does_not_load_messages_is_not_asked_to() -> None:
    class StoringOnly(InMemoryHistoryProvider):
        def __init__(self) -> None:
            super().__init__(load_messages=False)
            self.loads = 0

        async def before_run(
            self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
        ) -> None:
            self.loads += 1
            await super().before_run(agent=agent, session=session, context=context, state=state)

    client = FakeRealtimeClient()
    storing_only = StoringOnly()
    session = AgentSession()
    agent = RealtimeAgent(client, context_providers=[storing_only])

    async with agent.start_conversation(session=session, options=_TRANSCRIBED) as conversation:
        await _streamed(
            conversation,
            _caller_item("item_1"),
            _caller_transcript("item_1", "Hi"),
            *_reply("resp_1", "item_2", "Hello"),
        )

    assert storing_only.loads == 0
    stored = await storing_only.get_messages(session.session_id, state=session.state[storing_only.source_id])
    assert [message.text for message in stored] == ["Hi", "Hello"]


async def test_before_run_instructions_and_tools_apply_to_the_session() -> None:
    @tool
    def look_up() -> str:
        """Look something up."""
        return "found"

    class Briefing(ContextProvider):
        async def before_run(
            self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
        ) -> None:
            context.extend_instructions(self.source_id, "Be brief.")
            context.extend_tools(self.source_id, [look_up])

    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, instructions="You're a receptionist.", context_providers=[Briefing("briefing")])

    async with agent.start_conversation() as conversation:
        client.session.emit(*_call_response("resp_a", "item_1", "call_1", "look_up"))
        result = await _until_tool_result(conversation)

    started = client.start_session_calls[0]
    assert started["options"]["instructions"] == "You're a receptionist.\nBe brief."
    assert [declared.name for declared in started["tools"]] == ["look_up"]
    assert result.contents[0].result == "found"


async def test_the_first_exchange_carries_the_start_context() -> None:
    class Briefing(_Recorder):
        async def before_run(
            self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
        ) -> None:
            context.extend_messages(self, [Message("user", ["Ada's account is 1234"])])
            context.extend_instructions(self.source_id, "Be brief.")
            context.metadata["call"] = "call_7"

    client = FakeRealtimeClient()
    briefing = Briefing("briefing")
    agent = RealtimeAgent(client, context_providers=[briefing])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(
            conversation,
            _caller_item("item_1"),
            _caller_transcript("item_1", "Hi"),
            *_reply("resp_1", "item_2", "Hello"),
            _caller_item("item_3"),
            _caller_transcript("item_3", "Bye"),
            *_reply("resp_2", "item_4", "Goodbye"),
        )

    first, second = briefing.reports
    assert [message.text for message in first.context_messages["briefing"]] == ["Ada's account is 1234"]
    assert first.instructions == ["Be brief."]
    assert (second.context_messages, second.instructions) == ({}, [])
    assert first.metadata is second.metadata
    assert first.metadata["call"] == "call_7"


async def test_an_in_memory_history_provider_is_added_when_a_session_is_passed() -> None:
    client = FakeRealtimeClient()
    session = AgentSession()
    agent = RealtimeAgent(client)

    async with agent.start_conversation(session=session, options=_TRANSCRIBED) as conversation:
        await _streamed(
            conversation,
            _caller_item("item_1"),
            _caller_transcript("item_1", "Hi"),
            *_reply("resp_1", "item_2", "Hello"),
        )

    assert [type(provider) for provider in agent.context_providers] == [InMemoryHistoryProvider]
    history = agent.context_providers[0]
    assert isinstance(history, InMemoryHistoryProvider)
    stored = await history.get_messages(session.session_id, state=session.state[history.source_id])
    assert [(message.role, message.text) for message in stored] == [("user", "Hi"), ("assistant", "Hello")]


@pytest.mark.parametrize("case", ["no session", "a loading history provider"])
async def test_no_in_memory_history_provider_is_added_without_a_session_or_with_a_loading_one(case: str) -> None:
    client = FakeRealtimeClient()
    own_history = InMemoryHistoryProvider(source_id="own_history")
    agent = RealtimeAgent(client, context_providers=[own_history] if case == "a loading history provider" else None)
    session = AgentSession() if case == "a loading history provider" else None

    async with agent.start_conversation(session=session):
        pass

    assert agent.context_providers == ([own_history] if case == "a loading history provider" else [])


@pytest.mark.parametrize(
    ("history", "options", "warnings"),
    [
        (True, {}, 1),
        (True, {"transcription": None}, 1),
        (True, _TRANSCRIBED, 0),
        (False, {}, 0),
    ],
    ids=["transcription unset", "transcription off", "transcription on", "no history provider"],
)
async def test_history_providers_without_transcription_log_one_warning(
    history: bool, options: RealtimeOptions, warnings: int, caplog: pytest.LogCaptureFixture
) -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, context_providers=[InMemoryHistoryProvider()] if history else [_Recorder()])

    with caplog.at_level(logging.WARNING):
        async with agent.start_conversation(options=options):
            pass

    assert sum("without text" in record.getMessage() for record in caplog.records) == warnings


# endregion

# region Replay


async def test_loaded_history_replays_before_the_initial_messages() -> None:
    client = FakeRealtimeClient()
    history = InMemoryHistoryProvider()
    session = AgentSession()
    await history.save_messages(
        session.session_id,
        [Message("user", ["Hi"]), Message("assistant", ["Hello"])],
        state=session.state.setdefault(history.source_id, {}),
    )
    agent = RealtimeAgent(client, context_providers=[history])

    async with agent.start_conversation("Context", session=session):
        sent = [(message.role, message.text) for message in client.session.sent if isinstance(message, Message)]

    assert sent == [("user", "Hi"), ("assistant", "Hello"), ("user", "Context")]


# endregion

# region Close


async def test_close_waits_for_a_pending_caller_transcript() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(conversation, _caller_item("item_1"))
        closing = asyncio.create_task(conversation.close())
        await asyncio.sleep(0.05)
        assert client.session.close_count == 0

        client.session.emit(_caller_transcript("item_1", "Goodbye"))
        await closing

    assert client.session.close_count == 1
    assert [message.text for message in conversation.history] == ["Goodbye"]


async def test_close_stops_waiting_after_the_transcript_wait_and_records_the_turn_as_missing() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
        await _streamed(conversation, _caller_item("item_1"))

    assert _dicts(conversation.history) == _dicts([
        Message("user", [], message_id="item_1", additional_properties={"core": {"text_less": "transcript_missing"}})
    ])


# endregion


class TestVerification:
    async def test_close_does_not_wait_when_the_session_already_ended(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(realtime_conversation, "CALLER_TRANSCRIPT_WAIT_SECONDS", 5.0)
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
            await _streamed(conversation, _caller_item("item_1"))
            client.session.end()
            await asyncio.wait_for(_read_all(conversation), 1)

        assert conversation.history[0].additional_properties == {"core": {"text_less": "transcript_missing"}}

    async def test_reporting_does_not_block_the_receive_loop(self) -> None:
        client = FakeRealtimeClient()
        slow = _Slow()
        agent = RealtimeAgent(client, context_providers=[slow])

        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await _streamed(
                conversation,
                _caller_item("item_1"),
                _caller_transcript("item_1", "Hi"),
                *_reply("resp_1", "item_2", "Hello"),
                _caller_item("item_3"),
            )
            await slow.started.wait()
            await _streamed(conversation, _caller_transcript("item_3", "Still there?"))
            assert slow.reports == []
            slow.release.set()

    async def test_other_providers_keep_recording_while_one_fails(self) -> None:
        client = FakeRealtimeClient()
        recorder = _Recorder()
        agent = RealtimeAgent(client, context_providers=[recorder, _Failing()])

        with pytest.raises(RuntimeError, match="failing store down"):
            async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
                await _streamed(
                    conversation,
                    _caller_item("item_1"),
                    _caller_transcript("item_1", "Hi"),
                    *_reply("resp_1", "item_2", "Hello"),
                    _caller_item("item_3"),
                    _caller_transcript("item_3", "Bye"),
                    *_reply("resp_2", "item_4", "Goodbye"),
                    _caller_item("item_5"),
                )
                await eventually(lambda: len(recorder.reports) == 2)

        assert [_texts(report) for report in recorder.reports[:2]] == [
            ([("user", "Hi")], [("assistant", "Hello")]),
            ([("user", "Bye")], [("assistant", "Goodbye")]),
        ]

    async def test_providers_record_each_exchange_in_reverse_registration_order(self) -> None:
        recorded_by: list[str] = []

        class Named(_Recorder):
            async def after_run(
                self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
            ) -> None:
                recorded_by.append(self.source_id)

        client = FakeRealtimeClient()
        agent = RealtimeAgent(client, context_providers=[Named("first"), Named("second")])

        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await _streamed(
                conversation,
                _caller_item("item_1"),
                _caller_transcript("item_1", "Hi"),
                *_reply("resp_1", "item_2", "Hello"),
            )

        assert recorded_by == ["second", "first"]

    async def test_consecutive_inputs_share_one_exchange(self) -> None:
        client = FakeRealtimeClient()
        recorder = _Recorder()
        agent = RealtimeAgent(client, context_providers=[recorder])

        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await conversation.send("The caller is Ada.", create_response=False)
            await _streamed(
                conversation,
                _caller_item("item_1"),
                _caller_transcript("item_1", "Hi"),
                *_reply("resp_1", "item_2", "Hello"),
            )

        assert [_texts(report) for report in recorder.reports] == [
            ([("user", "The caller is Ada."), ("user", "Hi")], [("assistant", "Hello")])
        ]

    async def test_providers_without_a_session_record_into_a_fresh_one(self) -> None:
        sessions: list[AgentSession] = []

        class SessionRecorder(_Recorder):
            async def after_run(
                self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
            ) -> None:
                sessions.append(session)

        client = FakeRealtimeClient()
        agent = RealtimeAgent(client, context_providers=[SessionRecorder()])

        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await _streamed(
                conversation,
                _caller_item("item_1"),
                _caller_transcript("item_1", "Hi"),
                *_reply("resp_1", "item_2", "Hello"),
            )

        assert len(sessions) == 1
        assert isinstance(sessions[0], AgentSession)

    async def test_a_transcript_arriving_with_transcription_off_fills_the_turn_in(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            await _streamed(conversation, _caller_item("item_1"), _caller_transcript("item_1", "Hello"))

            assert _dicts(conversation.history) == _dicts([
                Message("user", [Content.from_text("Hello", status="completed")], message_id="item_1")
            ])

    async def test_a_late_caller_transcript_keeps_its_place_before_the_reply(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
            await _streamed(
                conversation,
                _caller_item("item_1"),
                *_reply("resp_1", "item_2", "It's noon."),
                _caller_transcript("item_1", "What time is it?"),
            )

            assert [(message.role, message.text) for message in conversation.history] == [
                ("user", "What time is it?"),
                ("assistant", "It's noon."),
            ]

    async def test_app_input_keeps_its_place_between_caller_turns(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
            await _streamed(conversation, _caller_item("item_1"), _caller_transcript("item_1", "Hi"))
            await conversation.send("The caller is Ada.", create_response=False)
            await _streamed(conversation, _caller_item("item_3"), _caller_transcript("item_3", "Are you there?"))

            assert [message.text for message in conversation.history] == ["Hi", "The caller is Ada.", "Are you there?"]

    async def test_a_tool_result_stays_after_its_call_when_the_caller_speaks_while_the_tool_runs(self) -> None:
        client = FakeRealtimeClient()
        look_up, release = _blocking_tool()

        async with RealtimeAgent(client, tools=[look_up]).start_conversation(options=_TRANSCRIBED) as conversation:
            await _streamed(
                conversation,
                *_call_response("resp_a", "item_1", "call_1", "look_up"),
                _caller_item("item_2"),
                _caller_transcript("item_2", "Any news?"),
            )
            release.set()
            await _until_tool_result(conversation)

            assert [message.role for message in conversation.history] == ["assistant", "tool", "user"]

    async def test_unclaimed_assistant_and_role_less_items_are_not_recorded(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            await _streamed(
                conversation, _agent_item("item_1"), event("item_added", item_id="item_2", previous_item_id="item_1")
            )

            assert conversation.history == []

    async def test_system_items_without_audio_are_not_recorded(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            await _streamed(conversation, event("item_added", item_id="item_1", previous_item_id=None, role="system"))

            assert conversation.history == []

    async def test_an_agent_turn_with_audio_but_no_text_is_left_out(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            await _streamed(
                conversation,
                event("response_created", response_id="resp_1"),
                _agent_item("item_1"),
                _agent_audio("resp_1", "item_1", 200),
                event("response_done", response_id="resp_1", status="completed"),
            )

            assert conversation.history == []

    async def test_raw_commands_are_not_recorded(self) -> None:
        client = FakeRealtimeClient()
        clear = RealtimeEvent(RealtimeKind("passthrough"), raw_representation={"type": "input_audio_buffer.clear"})

        async with RealtimeAgent(client).start_conversation() as conversation:
            await conversation.send(clear)

            assert conversation.history == []

    async def test_a_greeting_before_any_input_is_an_exchange_with_no_input(self) -> None:
        client = FakeRealtimeClient()
        recorder = _Recorder()
        agent = RealtimeAgent(client, context_providers=[recorder])

        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await _streamed(
                conversation,
                *_reply("resp_1", "item_1", "Thanks for calling."),
                _caller_item("item_2"),
                _caller_transcript("item_2", "Hi"),
            )
            await eventually(lambda: len(recorder.reports) == 1)

        assert _texts(recorder.reports[0]) == ([], [("assistant", "Thanks for calling.")])

    async def test_a_retried_report_records_the_text_first_reported(self) -> None:
        client = FakeRealtimeClient()
        flaky = _Failing("flaky", failures=1)
        agent = RealtimeAgent(client, context_providers=[flaky])

        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            client.session.emit(
                _caller_item("item_1"),
                _caller_transcript("item_1", "Count"),
                *_spoken_reply("resp_1", "item_2", "one two three four", 1000),
                _caller_item("item_3"),
                _caller_transcript("item_3", "Again"),
            )
            await _until_error(conversation)
            await _streamed(conversation, _truncated("item_2", 500))
            await _streamed(conversation, *_reply("resp_2", "item_4", "five"), _caller_item("item_5"))
            await eventually(lambda: len(flaky.reports) == 2)

        assert _texts(flaky.reports[0]) == ([("user", "Count")], [("assistant", "one two three four")])

    async def test_replayed_items_are_not_recorded_again(self) -> None:
        client = FakeRealtimeClient()
        history = InMemoryHistoryProvider()
        session = AgentSession()
        await history.save_messages(
            session.session_id,
            [Message("user", ["Hi"]), Message("assistant", ["Hello"])],
            state=session.state.setdefault(history.source_id, {}),
        )

        async with RealtimeAgent(client, context_providers=[history]).start_conversation(
            session=session
        ) as conversation:
            await _streamed(
                conversation,
                event("item_added", item_id="item_1", previous_item_id=None, role="user", input_audio=False),
                event("item_added", item_id="item_2", previous_item_id="item_1", role="assistant"),
            )

            assert conversation.history == []

    async def test_a_second_conversation_picks_up_the_first_ones_history(self) -> None:
        client = FakeRealtimeClient()
        session = AgentSession()
        agent = RealtimeAgent(client)

        async with agent.start_conversation(session=session, options=_TRANSCRIBED) as conversation:
            await _streamed(
                conversation,
                _caller_item("item_1"),
                _caller_transcript("item_1", "Hi"),
                *_reply("resp_1", "item_2", "Hello"),
            )
        async with agent.start_conversation(session=session, options=_TRANSCRIBED):
            replayed = [
                (message.role, message.text) for message in client.sessions[1].sent if isinstance(message, Message)
            ]

        assert replayed == [("user", "Hi"), ("assistant", "Hello")]

    async def test_close_does_not_wait_for_a_content_filtered_caller_turn(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(realtime_conversation, "CALLER_TRANSCRIPT_WAIT_SECONDS", 5.0)
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
            await _streamed(
                conversation,
                _caller_item("item_1"),
                event("response_created", response_id="resp_1"),
                event("response_done", response_id="resp_1", status="incomplete", reason="content_filter"),
            )
            await asyncio.wait_for(conversation.close(), 1)

    async def test_a_receive_failure_still_reports_the_remaining_exchanges(self) -> None:
        client = FakeRealtimeClient()
        recorder = _Recorder()
        agent = RealtimeAgent(client, context_providers=[recorder])

        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await _streamed(
                conversation,
                _caller_item("item_1"),
                _caller_transcript("item_1", "Hi"),
                *_reply("resp_1", "item_2", "Hello"),
            )
            client.session.fail(RealtimeConnectionException("The connection dropped."))
            with pytest.raises(RealtimeConnectionException):
                await _read_all(conversation)

        assert [_texts(report) for report in recorder.reports] == [([("user", "Hi")], [("assistant", "Hello")])]

    async def test_a_content_filtered_response_leaves_an_earlier_exchanges_caller_turn_waiting(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
            await _streamed(
                conversation,
                _caller_item("item_1"),
                *_reply("resp_1", "item_2", "Hello"),
                _caller_item("item_3"),
                event("response_created", response_id="resp_2"),
                event("response_done", response_id="resp_2", status="incomplete", reason="content_filter"),
            )

            assert [message.additional_properties for message in conversation.history if message.role == "user"] == [
                {"core": {"text_less": "transcript_pending"}},
                {"core": {"text_less": "transcript_missing"}},
            ]
