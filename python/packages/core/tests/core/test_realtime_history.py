# Copyright (c) Microsoft. All rights reserved.

"""Voice history: the call's turns as messages, exchanges reported to context providers, replay and close."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from agent_framework import (
    AgentSession,
    Content,
    ContextProvider,
    FunctionInvocationContext,
    FunctionMiddleware,
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

from .realtime_helpers import FakeRealtimeClient, event, eventually
from .realtime_history_helpers import (
    Failing,
    Loader,
    Recorder,
    agent_audio,
    agent_item,
    agent_text,
    blocking_tool,
    call_response,
    caller_delta,
    caller_item,
    caller_transcript,
    dicts,
    get_weather,
    read_all,
    reply,
    shapes,
    spoken_reply,
    streamed,
    texts,
    truncated,
    until_tool_result,
    usage,
)

_TRANSCRIBED: RealtimeOptions = {"transcription": {"model": "gpt-4o-transcribe"}}


@pytest.fixture(autouse=True)
def _short_transcript_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    """Close waits for pending caller transcripts; the tests shouldn't wait the production seconds for them."""
    monkeypatch.setattr(realtime_conversation, "CALLER_TRANSCRIPT_WAIT_SECONDS", 0.2)


# region History


async def test_history_is_empty_before_anything_is_said() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        assert conversation.history == []


async def test_a_caller_turn_records_its_final_transcript() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
        await streamed(conversation, caller_item("item_1"), caller_transcript("item_1", "Hello"))

        assert dicts(conversation.history) == dicts([
            Message("user", [Content.from_text("Hello", status="completed")], message_id="item_1")
        ])


async def test_changing_a_message_read_from_history_does_not_change_the_history() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.send("Ada's account is 1234", create_response=False)
        conversation.history[0].contents.append(Content.from_text("redacted"))

        assert [len(message.contents) for message in conversation.history] == [1]


async def test_an_agent_turn_records_its_final_transcript_credited_to_the_agent() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, name="receptionist").start_conversation() as conversation:
        await streamed(conversation, *reply("resp_1", "item_2", "Hi there"))

        assert dicts(conversation.history) == dicts([
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
        await streamed(
            conversation, event("item_added", item_id="item_1", previous_item_id=None, role="user", input_audio=False)
        )

        assert dicts(conversation.history) == dicts([Message("user", ["The caller's account is 1234"])])


async def test_initial_messages_are_recorded_as_app_input() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation("You're speaking to Ada.") as conversation:
        assert dicts(conversation.history) == dicts([Message("user", ["You're speaking to Ada."])])


async def test_app_input_is_recorded_without_its_audio() -> None:
    client = FakeRealtimeClient()
    audio = Content.from_data(b"\x00\x00", "audio/pcm;rate=24000")

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.send(Message("user", ["Listen to this", audio]), create_response=False)
        await conversation.send(Message("user", [audio]))

        assert dicts(conversation.history) == dicts([Message("user", ["Listen to this"])])


async def test_app_input_is_recorded_as_its_text_only() -> None:
    client = FakeRealtimeClient()
    picture = Content.from_uri("https://example.com/receipt.png?sig=secret", media_type="image/png")

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.send(Message("user", ["Here's my receipt", picture]), create_response=False)

        assert dicts(conversation.history) == dicts([Message("user", ["Here's my receipt"])])


async def test_changing_a_message_after_sending_it_does_not_change_the_history() -> None:
    client = FakeRealtimeClient()
    sent = Message("user", ["Ada's account is 1234"])

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.send(sent, create_response=False)
        sent.contents[0].text = "something else"

        assert [message.text for message in conversation.history] == ["Ada's account is 1234"]


async def test_a_tool_result_is_recorded_with_its_text_only() -> None:
    @tool
    def receipt() -> list[Content]:
        """Find the caller's receipt."""
        return [
            Content.from_text("Receipt 42, paid"),
            Content.from_uri("https://example.com/receipt.png?sig=secret", media_type="image/png"),
        ]

    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[receipt]).start_conversation() as conversation:
        client.session.emit(*call_response("resp_a", "item_1", "call_1", "receipt"))
        streamed_result = (await until_tool_result(conversation)).contents[0]
        recorded_result = conversation.history[-1].contents[0]

    assert [item.type for item in recorded_result.items or []] == ["text"]
    assert recorded_result.result == "Receipt 42, paid"
    assert [item.type for item in streamed_result.items or []] == ["text", "uri"]


async def test_a_function_call_is_recorded_with_its_result_straight_after_it() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, name="receptionist", tools=[get_weather])

    async with agent.start_conversation() as conversation:
        client.session.emit(*call_response("resp_a", "item_3", "call_1", "get_weather", city="Paris"))
        await until_tool_result(conversation)

        history = conversation.history
        assert shapes(history) == [
            ("assistant", ["function_call"], "receptionist", "item_3"),
            ("tool", ["function_result"], "receptionist", None),
        ]
        assert (history[0].contents[0].call_id, history[1].contents[0].call_id) == ("call_1", "call_1")
        assert history[1].contents[0].result == "Sunny in Paris"


@pytest.mark.parametrize(
    ("options", "events"),
    [
        (_TRANSCRIBED, []),
        ({"transcription": None}, []),
        (_TRANSCRIBED, [event("input_transcription_failed", item_id="item_1", content_index=0, error_code="x")]),
    ],
    ids=["waiting for its transcript", "transcription off", "transcription failed"],
)
async def test_a_caller_turn_without_a_transcript_is_marked_text_less(
    options: RealtimeOptions, events: list[RealtimeEvent]
) -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(options=options) as conversation:
        await streamed(conversation, caller_item("item_1"), *events)

        assert dicts(conversation.history) == dicts([
            Message("user", [], message_id="item_1", additional_properties={"core": {"text_less": True}})
        ])


async def test_a_failed_transcription_keeps_the_partial_transcript_as_incomplete() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
        await streamed(
            conversation,
            caller_item("item_1"),
            caller_delta("item_1", "I need"),
            caller_delta("item_1", " to"),
            event("input_transcription_failed", item_id="item_1", content_index=0),
        )

        assert dicts(conversation.history) == dicts([
            Message("user", [Content.from_text("I need to", status="incomplete")], message_id="item_1")
        ])


@pytest.mark.parametrize(
    ("events", "heard"),
    [
        ([*spoken_reply("resp_1", "item_2", "one two three four", 1000), truncated("item_2", 500)], ["one two"]),
        (
            [
                event("response_created", response_id="resp_1"),
                agent_item("item_2"),
                agent_audio("resp_1", "item_2", 1000),
                event("item_truncated", item_id="item_2", content_index=0, audio_end_ms=500, truncated_by="provider"),
                agent_text("resp_1", "item_2", "one two three four"),
                event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
            ],
            ["one two"],
        ),
        ([*spoken_reply("resp_1", "item_2", "one two three four", 1000), truncated("item_2", 0)], []),
    ],
    ids=["after the transcript", "before the final transcript", "at 0 ms"],
)
async def test_a_cut_agent_turn_keeps_only_the_heard_words_as_incomplete(
    events: list[RealtimeEvent], heard: list[str]
) -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await streamed(conversation, *events)

        assert dicts(conversation.history) == dicts([
            Message("assistant", [Content.from_text(text, status="incomplete")], message_id="item_2") for text in heard
        ])


async def test_a_cancelled_response_keeps_the_text_generated_so_far_as_incomplete() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await streamed(
            conversation,
            event("response_created", response_id="resp_1"),
            agent_item("item_2"),
            agent_text("resp_1", "item_2", "Let me", final=False),
            agent_text("resp_1", "item_2", " check", final=False),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )

        assert dicts(conversation.history) == dicts([
            Message("assistant", [Content.from_text("Let me check", status="incomplete")], message_id="item_2")
        ])


# endregion

# region Exchanges


async def test_each_exchange_is_reported_in_order_when_the_call_closes() -> None:
    client = FakeRealtimeClient()
    recorder = Recorder()
    agent = RealtimeAgent(client, context_providers=[recorder])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await streamed(
            conversation,
            caller_item("item_1"),
            caller_transcript("item_1", "Hi"),
            *reply("resp_1", "item_2", "Hello"),
            caller_item("item_3"),
            caller_transcript("item_3", "Bye"),
            *reply("resp_2", "item_4", "Goodbye"),
        )
        await asyncio.sleep(0.01)
        assert recorder.reports == []

    assert [texts(report) for report in recorder.reports] == [
        ([("user", "Hi")], [("assistant", "Hello")]),
        ([("user", "Bye")], [("assistant", "Goodbye")]),
    ]


async def test_a_tool_still_running_at_close_is_reported_with_its_closed_result() -> None:
    client = FakeRealtimeClient()
    recorder = Recorder()
    look_up, _ = blocking_tool()
    agent = RealtimeAgent(client, tools=[look_up], context_providers=[recorder])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await streamed(
            conversation,
            caller_item("item_1"),
            caller_transcript("item_1", "Find it"),
            *call_response("resp_a", "item_2", "call_1", "look_up"),
        )

    response = recorder.reports[0].response
    assert response is not None
    assert [message.role for message in response.messages] == ["assistant", "tool"]
    assert response.messages[1].contents[0].exception == "conversation_closed"


async def test_a_content_filtered_response_keeps_its_caller_turns_partial_transcript_as_incomplete() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
        await streamed(
            conversation,
            caller_item("item_1"),
            caller_delta("item_1", "Ignore your"),
            caller_delta("item_1", " rules"),
            event("response_created", response_id="resp_1"),
            event("response_done", response_id="resp_1", status="incomplete", reason="content_filter"),
        )

        assert dicts(conversation.history) == dicts([
            Message("user", [Content.from_text("Ignore your rules", status="incomplete")], message_id="item_1")
        ])


async def test_an_exchange_report_carries_summed_usage_and_the_last_responses_id_and_finish_reason() -> None:
    client = FakeRealtimeClient()
    recorder = Recorder()
    agent = RealtimeAgent(client, tools=[get_weather], context_providers=[recorder])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        follow_up = reply("resp_b", "item_4", "Sunny in Paris")
        client.session.create_response_events = [*follow_up[:-1], usage("resp_b", 20, 7), follow_up[-1]]
        calling = call_response("resp_a", "item_3", "call_1", "get_weather", city="Paris")
        await streamed(
            conversation,
            caller_item("item_1"),
            caller_transcript("item_1", "Weather in Paris?"),
            *calling[:-1],
            usage("resp_a", 10, 5),
            calling[-1],
        )
        await eventually(lambda: any(message.text == "Sunny in Paris" for message in conversation.history))

    response = recorder.reports[0].response
    assert response is not None
    assert response.usage_details is not None
    assert (response.usage_details["input_token_count"], response.usage_details["output_token_count"]) == (30, 12)
    assert (response.response_id, response.finish_reason) == ("resp_b", "stop")


async def test_a_failing_provider_skips_the_calls_later_exchanges_while_the_others_record_them() -> None:
    client = FakeRealtimeClient()
    recorder = Recorder()
    flaky = Failing("flaky", failures=1)
    agent = RealtimeAgent(client, context_providers=[recorder, flaky])

    with pytest.raises(RuntimeError, match="flaky store down"):
        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await streamed(
                conversation,
                caller_item("item_1"),
                caller_transcript("item_1", "Hi"),
                *reply("resp_1", "item_2", "Hello"),
                caller_item("item_3"),
                caller_transcript("item_3", "Bye"),
                *reply("resp_2", "item_4", "Goodbye"),
            )

    assert flaky.reports == []
    assert [texts(report) for report in recorder.reports] == [
        ([("user", "Hi")], [("assistant", "Hello")]),
        ([("user", "Bye")], [("assistant", "Goodbye")]),
    ]
    assert not any(content.type == "error" for update in await read_all(conversation) for content in update.contents)


async def test_every_report_shares_one_metadata_even_when_start_left_it_empty() -> None:
    class Counter(Recorder):
        async def after_run(
            self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
        ) -> None:
            context.metadata["reports"] = context.metadata.get("reports", 0) + 1
            await super().after_run(agent=agent, session=session, context=context, state=state)

    client = FakeRealtimeClient()
    counter = Counter("counter")
    agent = RealtimeAgent(client, context_providers=[counter])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await streamed(
            conversation,
            caller_item("item_1"),
            caller_transcript("item_1", "Hi"),
            *reply("resp_1", "item_2", "Hello"),
            caller_item("item_3"),
            caller_transcript("item_3", "Bye"),
            *reply("resp_2", "item_4", "Goodbye"),
        )

    assert [report.metadata["reports"] for report in counter.reports] == [2, 2]


async def test_an_out_of_band_response_is_not_part_of_any_exchange() -> None:
    client = FakeRealtimeClient()
    recorder = Recorder()
    agent = RealtimeAgent(client, context_providers=[recorder])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await streamed(
            conversation,
            caller_item("item_1"),
            caller_transcript("item_1", "I want to cancel"),
            event("response_created", response_id="resp_oob", out_of_band=True),
            usage("resp_oob", 50, 3),
            event("response_done", response_id="resp_oob", status="completed"),
            caller_item("item_2"),
            caller_transcript("item_2", "my subscription"),
            *reply("resp_1", "item_3", "I can help with that."),
            usage("resp_1", 10, 5),
        )

    assert [texts(report) for report in recorder.reports] == [
        ([("user", "I want to cancel"), ("user", "my subscription")], [("assistant", "I can help with that.")])
    ]
    response = recorder.reports[0].response
    assert response is not None
    assert response.response_id == "resp_1"
    assert response.usage_details is not None
    assert (response.usage_details["input_token_count"], response.usage_details["output_token_count"]) == (10, 5)


async def test_a_resumed_call_that_ends_before_anyone_speaks_reports_nothing() -> None:
    client = FakeRealtimeClient()
    session = AgentSession()
    recorder = Recorder()
    agent = RealtimeAgent(client, context_providers=[recorder])
    async with agent.start_conversation(session=session, options=_TRANSCRIBED) as conversation:
        await streamed(
            conversation, caller_item("item_1"), caller_transcript("item_1", "Hi"), *reply("resp_1", "item_2", "Hello")
        )

    async with agent.start_conversation(session=session, options=_TRANSCRIBED) as conversation:
        await streamed(conversation, agent_item("replayed_2"))

    assert [texts(report) for report in recorder.reports] == [([("user", "Hi")], [("assistant", "Hello")])]


async def test_close_raises_a_failing_providers_error_after_cleanup() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, context_providers=[Failing()])

    with pytest.raises(RuntimeError, match="failing store down"):
        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await streamed(
                conversation,
                caller_item("item_1"),
                caller_transcript("item_1", "Hi"),
                *reply("resp_1", "item_2", "Hello"),
            )

    assert client.session.close_count == 1
    await asyncio.wait_for(read_all(conversation), 1)


async def test_close_raises_an_exception_group_when_several_providers_fail() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, context_providers=[Failing("memory"), Failing("history")])

    with pytest.raises(ExceptionGroup) as raised:
        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await streamed(
                conversation,
                caller_item("item_1"),
                caller_transcript("item_1", "Hi"),
                *reply("resp_1", "item_2", "Hello"),
            )

    assert sorted(str(failure) for failure in raised.value.exceptions) == ["history store down", "memory store down"]


async def test_a_reply_still_running_at_hang_up_is_reported_with_the_text_so_far() -> None:
    client = FakeRealtimeClient()
    recorder = Recorder()
    agent = RealtimeAgent(client, context_providers=[recorder])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await streamed(
            conversation,
            caller_item("item_1"),
            caller_transcript("item_1", "Hi"),
            event("response_created", response_id="resp_1"),
            agent_item("item_2"),
            agent_text("resp_1", "item_2", "Let me che", final=False),
        )
        client.session.end()
        await read_all(conversation)

    assert len(recorder.reports) == 1
    report = recorder.reports[0]
    assert texts(report) == ([("user", "Hi")], [("assistant", "Let me che")])
    assert report.response is not None
    assert report.response.messages[0].contents[0].status == "incomplete"


# endregion

# region Start


async def test_before_run_runs_once_at_start() -> None:
    client = FakeRealtimeClient()
    loader = Loader(client)
    agent = RealtimeAgent(client, context_providers=[loader])

    async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
        await streamed(
            conversation,
            caller_item("item_1"),
            caller_transcript("item_1", "Hi"),
            *reply("resp_1", "item_2", "Hello"),
            caller_item("item_3"),
            caller_transcript("item_3", "Bye"),
            *reply("resp_2", "item_4", "Goodbye"),
        )

    assert loader.loads == [False]


async def test_before_run_sees_the_initial_messages_and_the_start_options() -> None:
    seen: list[tuple[list[str], dict[str, Any]]] = []

    class Retriever(ContextProvider):
        async def before_run(
            self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
        ) -> None:
            seen.append(([message.text for message in context.input_messages], dict(context.options)))

    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, context_providers=[Retriever("retriever")])

    async with agent.start_conversation("The caller asked about refunds.", options=_TRANSCRIBED):
        pass

    assert seen == [(["The caller asked about refunds."], dict(_TRANSCRIBED))]


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
        await streamed(
            conversation,
            caller_item("item_1"),
            caller_transcript("item_1", "Hi"),
            *reply("resp_1", "item_2", "Hello"),
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
        client.session.emit(*call_response("resp_a", "item_1", "call_1", "look_up"))
        result = await until_tool_result(conversation)

    started = client.start_session_calls[0]
    assert started["options"]["instructions"] == "You're a receptionist.\nBe brief."
    assert [declared.name for declared in started["tools"]] == ["look_up"]
    assert result.contents[0].result == "found"


async def test_a_providers_start_tools_send_no_tool_update_after_the_first_batch() -> None:
    @tool
    def look_up() -> str:
        """Look something up."""
        return "found"

    class Tools(ContextProvider):
        async def before_run(
            self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
        ) -> None:
            context.extend_tools(self.source_id, [look_up])

    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[get_weather], context_providers=[Tools("tools")])

    async with agent.start_conversation() as conversation:
        client.session.emit(*call_response("resp_a", "item_1", "call_1", "look_up"))
        await until_tool_result(conversation)
        await eventually(lambda: bool(client.session.sent))

    assert [call for call in client.session.calls if call[0] == "update_session"] == []


async def test_the_first_exchange_carries_the_start_context() -> None:
    class Briefing(Recorder):
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
        await streamed(
            conversation,
            caller_item("item_1"),
            caller_transcript("item_1", "Hi"),
            *reply("resp_1", "item_2", "Hello"),
            caller_item("item_3"),
            caller_transcript("item_3", "Bye"),
            *reply("resp_2", "item_4", "Goodbye"),
        )

    first, second = briefing.reports
    assert [message.text for message in first.context_messages["briefing"]] == ["Ada's account is 1234"]
    assert first.instructions == ["Be brief."]
    assert (second.context_messages, second.instructions) == ({}, [])
    assert first.metadata is second.metadata
    assert first.metadata["call"] == "call_7"


async def test_a_session_keeps_the_call_in_an_added_in_memory_history_for_the_next_call() -> None:
    client = FakeRealtimeClient()
    session = AgentSession()
    agent = RealtimeAgent(client)

    async with agent.start_conversation(session=session, options=_TRANSCRIBED) as conversation:
        await streamed(
            conversation,
            caller_item("item_1"),
            caller_transcript("item_1", "Hi"),
            *reply("resp_1", "item_2", "Hello"),
        )
    async with agent.start_conversation(session=session, options=_TRANSCRIBED):
        replayed = [(message.role, message.text) for message in client.sessions[1].sent if isinstance(message, Message)]

    assert [type(provider) for provider in agent.context_providers] == [InMemoryHistoryProvider]
    assert replayed == [("user", "Hi"), ("assistant", "Hello")]


async def test_the_automatic_history_loads_before_the_agents_own_providers() -> None:
    seen: list[list[str]] = []

    class Inspector(ContextProvider):
        async def before_run(
            self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
        ) -> None:
            seen.append([message.text for message in context.get_messages()])

    client = FakeRealtimeClient()
    session = AgentSession()
    agent = RealtimeAgent(client, context_providers=[Inspector("inspector")])
    async with agent.start_conversation(session=session, options=_TRANSCRIBED) as conversation:
        await streamed(
            conversation, caller_item("item_1"), caller_transcript("item_1", "Hi"), *reply("resp_1", "item_2", "Hello")
        )

    async with agent.start_conversation(session=session, options=_TRANSCRIBED):
        pass

    assert seen[1] == ["Hi", "Hello"]


async def test_tools_share_the_session_context_providers_use() -> None:
    @tool
    def ticket(ctx: FunctionInvocationContext) -> str:
        """Read the ticket the caller is calling about."""
        return str(ctx.session.state["ticket"]) if ctx.session is not None else "no session"

    class TicketLoader(ContextProvider):
        async def before_run(
            self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
        ) -> None:
            session.state["ticket"] = "T-1"

    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[ticket], context_providers=[TicketLoader("tickets")])

    async with agent.start_conversation() as conversation:
        client.session.emit(*call_response("resp_a", "item_1", "call_1", "ticket"))
        result = await until_tool_result(conversation)

    assert result.contents[0].result == "T-1"


async def test_function_middleware_a_context_provider_adds_wraps_the_tools() -> None:
    audited: list[str] = []

    class Audit(FunctionMiddleware):
        async def process(self, context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
            audited.append(context.function.name)
            await call_next()

    class Auditor(ContextProvider):
        async def before_run(
            self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
        ) -> None:
            context.extend_middleware(self.source_id, Audit())

    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[get_weather], context_providers=[Auditor("audit")])

    async with agent.start_conversation() as conversation:
        client.session.emit(*call_response("resp_a", "item_1", "call_1", "get_weather", city="Paris"))
        await until_tool_result(conversation)

    assert audited == ["get_weather"]


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
    agent = RealtimeAgent(client, context_providers=[InMemoryHistoryProvider()] if history else [Recorder()])

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
        await streamed(conversation, caller_item("item_1"))
        closing = asyncio.create_task(conversation.close())
        await asyncio.sleep(0.05)
        assert client.session.close_count == 0

        client.session.emit(caller_transcript("item_1", "Goodbye"))
        await closing

    assert client.session.close_count == 1
    assert [message.text for message in conversation.history] == ["Goodbye"]


async def test_close_stops_waiting_after_the_transcript_wait_and_records_the_turn_text_less() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
        await streamed(conversation, caller_item("item_1"))

    assert dicts(conversation.history) == dicts([
        Message("user", [], message_id="item_1", additional_properties={"core": {"text_less": True}})
    ])


# endregion


class TestVerification:
    async def test_close_does_not_wait_when_the_session_already_ended(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(realtime_conversation, "CALLER_TRANSCRIPT_WAIT_SECONDS", 5.0)
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
            await streamed(conversation, caller_item("item_1"))
            client.session.end()
            await asyncio.wait_for(read_all(conversation), 1)

        assert conversation.history[0].additional_properties == {"core": {"text_less": True}}

    async def test_providers_record_each_exchange_in_reverse_registration_order(self) -> None:
        recorded_by: list[str] = []

        class Named(Recorder):
            async def after_run(
                self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
            ) -> None:
                recorded_by.append(self.source_id)

        client = FakeRealtimeClient()
        agent = RealtimeAgent(client, context_providers=[Named("first"), Named("second")])

        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await streamed(
                conversation,
                caller_item("item_1"),
                caller_transcript("item_1", "Hi"),
                *reply("resp_1", "item_2", "Hello"),
            )

        assert recorded_by == ["second", "first"]

    async def test_consecutive_inputs_share_one_exchange(self) -> None:
        client = FakeRealtimeClient()
        recorder = Recorder()
        agent = RealtimeAgent(client, context_providers=[recorder])

        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await conversation.send("The caller is Ada.", create_response=False)
            await streamed(
                conversation,
                caller_item("item_1"),
                caller_transcript("item_1", "Hi"),
                *reply("resp_1", "item_2", "Hello"),
            )

        assert [texts(report) for report in recorder.reports] == [
            ([("user", "The caller is Ada."), ("user", "Hi")], [("assistant", "Hello")])
        ]

    async def test_providers_without_a_session_record_into_a_fresh_one(self) -> None:
        sessions: list[AgentSession] = []

        class SessionRecorder(Recorder):
            async def after_run(
                self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
            ) -> None:
                sessions.append(session)

        client = FakeRealtimeClient()
        agent = RealtimeAgent(client, context_providers=[SessionRecorder()])

        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await streamed(
                conversation,
                caller_item("item_1"),
                caller_transcript("item_1", "Hi"),
                *reply("resp_1", "item_2", "Hello"),
            )

        assert len(sessions) == 1
        assert isinstance(sessions[0], AgentSession)

    async def test_a_transcript_arriving_with_transcription_off_fills_the_turn_in(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            await streamed(conversation, caller_item("item_1"), caller_transcript("item_1", "Hello"))

            assert dicts(conversation.history) == dicts([
                Message("user", [Content.from_text("Hello", status="completed")], message_id="item_1")
            ])

    async def test_a_late_caller_transcript_keeps_its_place_before_the_reply(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
            await streamed(
                conversation,
                caller_item("item_1"),
                *reply("resp_1", "item_2", "It's noon."),
                caller_transcript("item_1", "What time is it?"),
            )

            assert [(message.role, message.text) for message in conversation.history] == [
                ("user", "What time is it?"),
                ("assistant", "It's noon."),
            ]

    async def test_app_input_keeps_its_place_between_caller_turns(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
            await streamed(conversation, caller_item("item_1"), caller_transcript("item_1", "Hi"))
            await conversation.send("The caller is Ada.", create_response=False)
            await streamed(conversation, caller_item("item_3"), caller_transcript("item_3", "Are you there?"))

            assert [message.text for message in conversation.history] == ["Hi", "The caller is Ada.", "Are you there?"]

    async def test_a_tool_result_stays_after_its_call_when_the_caller_speaks_while_the_tool_runs(self) -> None:
        client = FakeRealtimeClient()
        look_up, release = blocking_tool()

        async with RealtimeAgent(client, tools=[look_up]).start_conversation(options=_TRANSCRIBED) as conversation:
            await streamed(
                conversation,
                *call_response("resp_a", "item_1", "call_1", "look_up"),
                caller_item("item_2"),
                caller_transcript("item_2", "Any news?"),
            )
            release.set()
            await until_tool_result(conversation)

            assert [message.role for message in conversation.history] == ["assistant", "tool", "user"]

    @pytest.mark.parametrize(
        "events",
        [
            [agent_item("item_1"), event("item_added", item_id="item_2", previous_item_id="item_1")],
            [event("item_added", item_id="item_1", previous_item_id=None, role="system")],
            [
                event("response_created", response_id="resp_1"),
                agent_item("item_1"),
                agent_audio("resp_1", "item_1", 200),
                event("response_done", response_id="resp_1", status="completed"),
            ],
        ],
        ids=["unclaimed assistant and role-less items", "a system item", "an agent turn with audio but no text"],
    )
    async def test_items_that_carry_no_turn_of_the_call_are_not_recorded(self, events: list[RealtimeEvent]) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            await streamed(conversation, *events)

            assert conversation.history == []

    async def test_raw_commands_are_not_recorded(self) -> None:
        client = FakeRealtimeClient()
        clear = RealtimeEvent(RealtimeKind("passthrough"), raw_representation={"type": "input_audio_buffer.clear"})

        async with RealtimeAgent(client).start_conversation() as conversation:
            await conversation.send(clear)

            assert conversation.history == []

    async def test_a_greeting_before_any_input_is_an_exchange_with_no_input(self) -> None:
        client = FakeRealtimeClient()
        recorder = Recorder()
        agent = RealtimeAgent(client, context_providers=[recorder])

        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await streamed(
                conversation,
                *reply("resp_1", "item_1", "Thanks for calling."),
                caller_item("item_2"),
                caller_transcript("item_2", "Hi"),
            )

        assert texts(recorder.reports[0]) == ([], [("assistant", "Thanks for calling.")])

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
            await streamed(
                conversation,
                event("item_added", item_id="item_1", previous_item_id=None, role="user", input_audio=False),
                event("item_added", item_id="item_2", previous_item_id="item_1", role="assistant"),
            )

            assert conversation.history == []

    async def test_close_does_not_wait_for_a_content_filtered_caller_turn(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(realtime_conversation, "CALLER_TRANSCRIPT_WAIT_SECONDS", 5.0)
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
            await streamed(
                conversation,
                caller_item("item_1"),
                event("response_created", response_id="resp_1"),
                event("response_done", response_id="resp_1", status="incomplete", reason="content_filter"),
            )
            await asyncio.wait_for(conversation.close(), 1)

    async def test_a_receive_failure_still_reports_the_remaining_exchanges(self) -> None:
        client = FakeRealtimeClient()
        recorder = Recorder()
        agent = RealtimeAgent(client, context_providers=[recorder])

        async with agent.start_conversation(options=_TRANSCRIBED) as conversation:
            await streamed(
                conversation,
                caller_item("item_1"),
                caller_transcript("item_1", "Hi"),
                *reply("resp_1", "item_2", "Hello"),
            )
            client.session.fail(RealtimeConnectionException("The connection dropped."))
            with pytest.raises(RealtimeConnectionException):
                await read_all(conversation)

        assert [texts(report) for report in recorder.reports] == [([("user", "Hi")], [("assistant", "Hello")])]

    async def test_close_still_waits_for_an_earlier_exchanges_caller_transcript_after_a_content_filter(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation(options=_TRANSCRIBED) as conversation:
            await streamed(
                conversation,
                caller_item("item_1"),
                *reply("resp_1", "item_2", "Hello"),
                caller_item("item_3"),
                event("response_created", response_id="resp_2"),
                event("response_done", response_id="resp_2", status="incomplete", reason="content_filter"),
            )
            closing = asyncio.create_task(conversation.close())
            await asyncio.sleep(0.05)
            client.session.emit(caller_transcript("item_1", "Hi"))
            await closing

        assert [message.text for message in conversation.history if message.role == "user"] == ["Hi", ""]
