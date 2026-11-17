# Copyright (c) Microsoft. All rights reserved.

"""A realtime agent's half-duplex run: one bounded turn over its own realtime session."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from agent_framework import (
    Agent,
    AgentResponse,
    AgentSession,
    ChatResponse,
    Content,
    FunctionInvocationContext,
    FunctionTool,
    Message,
    MiddlewareTermination,
    RealtimeAgent,
    RealtimeOptions,
    SessionContext,
    SupportsAgentRun,
    function_middleware,
    tool,
)
from agent_framework.exceptions import (
    RealtimeControlException,
    RealtimeControlTimeoutException,
    RealtimeException,
    UserInputRequiredException,
)

from .conftest import MockBaseChatClient
from .realtime_helpers import FakeRealtimeClient, event, eventually
from .realtime_history_helpers import (
    Recorder,
    agent_audio,
    agent_item,
    agent_text,
    call_response,
    caller_item,
    caller_transcript,
    get_weather,
    reply,
    spoken_reply,
    texts,
    usage,
)


def _results(response: AgentResponse) -> list[Content]:
    return [
        content for message in response.messages for content in message.contents if content.type == "function_result"
    ]


# region Driving


async def test_a_text_run_returns_the_replys_transcript() -> None:
    client = FakeRealtimeClient()
    client.replies.append(reply("resp_1", "item_1", "Hello there."))

    response = await RealtimeAgent(client).run("Hi")

    assert response.text == "Hello there."


async def test_the_response_holds_only_the_agents_messages() -> None:
    client = FakeRealtimeClient()
    client.replies.append(reply("resp_1", "item_1", "Hello there."))

    response = await RealtimeAgent(client).run("Hi")

    assert [(message.role, [content.type for content in message.contents]) for message in response.messages] == [
        ("assistant", ["text"])
    ]


async def test_a_streamed_run_yields_every_update_and_finalizes_to_the_same_response() -> None:
    client = FakeRealtimeClient()
    client.replies.append(reply("resp_1", "item_1", "Hello there."))

    stream = RealtimeAgent(client).run("Hi", stream=True)
    updates = [update async for update in stream]
    response = await stream.get_final_response()

    signals = [content.kind for update in updates for content in update.contents if content.type == "realtime_event"]
    assert signals == ["response_created", "item_added", "response_done"]
    assert response.text == "Hello there."


async def test_a_tool_call_runs_and_the_run_ends_at_the_follow_ups_reply() -> None:
    client = FakeRealtimeClient()
    client.replies.extend([
        call_response("resp_1", "item_1", "call_1", "get_weather", city="Paris"),
        reply("resp_2", "item_2", "It's sunny in Paris."),
    ])

    response = await RealtimeAgent(client, tools=[get_weather]).run("What's the weather in Paris?")

    contents = [(content.type, content.result) for message in response.messages for content in message.contents]
    assert contents == [("function_call", None), ("function_result", "Sunny in Paris"), ("text", None)]
    assert response.text == "It's sunny in Paris."
    assert (response.response_id, response.finish_reason) == ("resp_2", "stop")


async def test_a_reply_that_finishes_before_its_request_returns_still_ends_the_run() -> None:
    client = FakeRealtimeClient()
    client.replies.append(reply("resp_1", "item_1", "Hello there."))
    release = client.session_holds["create_response"] = asyncio.Event()

    running = asyncio.ensure_future(RealtimeAgent(client).run("Hi"))
    await eventually(lambda: bool(client.sessions) and bool(client.session.calls) and not client.session.queued_events)
    release.set()
    response = await running

    assert response.text == "Hello there."


async def test_a_call_to_a_tool_that_needs_approval_gets_an_error_result_and_the_tool_never_runs() -> None:
    client = FakeRealtimeClient()
    refunds: list[str] = []

    @tool(approval_mode="always_require")
    def refund(order: str) -> str:
        """Refund an order."""
        refunds.append(order)
        return "Refunded."

    client.replies.extend([
        call_response("resp_1", "item_1", "call_1", "refund", order="7"),
        reply("resp_2", "item_2", "I can't refund that right now."),
    ])

    response = await RealtimeAgent(client, tools=[refund]).run("Please refund order 7.")

    assert [(result.call_id, result.exception) for result in _results(response)] == [("call_1", "unsupported")]
    assert refunds == []
    assert response.text == "I can't refund that right now."


async def test_a_call_to_an_app_run_tool_gets_an_error_result() -> None:
    client = FakeRealtimeClient()
    look_up_account = FunctionTool(name="look_up_account", description="Look up the caller's account; the app answers.")
    client.replies.extend([
        call_response("resp_1", "item_1", "call_1", "look_up_account"),
        reply("resp_2", "item_2", "I can't see your account right now."),
    ])

    response = await RealtimeAgent(client, tools=[look_up_account]).run("What's my balance?")

    assert [(result.call_id, result.exception) for result in _results(response)] == [("call_1", "unsupported")]
    assert response.text == "I can't see your account right now."


async def test_a_call_whose_tool_asks_for_user_input_gets_an_error_result() -> None:
    client = FakeRealtimeClient()

    @tool
    def read_calendar() -> str:
        """Read the caller's calendar."""
        raise UserInputRequiredException([Content.from_oauth_consent_request("https://consent.example")])

    client.replies.extend([
        call_response("resp_1", "item_1", "call_1", "read_calendar"),
        reply("resp_2", "item_2", "I need your consent before I can read your calendar."),
    ])

    response = await RealtimeAgent(client, tools=[read_calendar]).run("What's on my calendar?")

    assert [(result.call_id, result.exception) for result in _results(response)] == [("call_1", "unsupported")]
    assert response.text == "I need your consent before I can read your calendar."


async def test_a_rejected_response_request_raises_closes_the_session_and_reports_nothing() -> None:
    client = FakeRealtimeClient()
    client.create_response_error = RealtimeControlException("Rejected.", code="invalid_value")
    recorder = Recorder()

    with pytest.raises(RealtimeControlException):
        await RealtimeAgent(client, context_providers=[recorder]).run("Hi")

    assert client.session.close_count == 1
    assert recorder.reports == []


async def test_a_rejected_follow_up_fails_the_run() -> None:
    client = FakeRealtimeClient()
    client.replies.append(call_response("resp_1", "item_1", "call_1", "get_weather", city="Paris"))
    client.replies.append(RealtimeControlException("Rejected.", code="invalid_value"))

    with pytest.raises(RealtimeControlException):
        await RealtimeAgent(client, tools=[get_weather]).run("What's the weather in Paris?")


async def test_a_response_that_ends_failed_raises_with_the_providers_error_code_and_message() -> None:
    client = FakeRealtimeClient()
    client.replies.append([
        event("response_created", response_id="resp_1"),
        event("response_done", response_id="resp_1", status="failed", error_code="server_error", error_message="Boom."),
    ])

    with pytest.raises(RealtimeException) as failure:
        await RealtimeAgent(client).run("Hi")

    assert "server_error" in str(failure.value)
    assert "Boom." in str(failure.value)


async def test_the_session_closing_before_the_turn_ends_fails_the_run() -> None:
    client = FakeRealtimeClient()
    client.replies.append([event("response_created", response_id="resp_1")])

    running = asyncio.ensure_future(RealtimeAgent(client).run("Hi"))
    await eventually(lambda: bool(client.sessions) and bool(client.session.calls) and not client.session.queued_events)
    client.session.end()

    with pytest.raises(RealtimeException, match="before the run's reply finished"):
        await running


async def test_audio_only_input_waits_for_the_response_turn_detection_starts() -> None:
    client = FakeRealtimeClient()
    recorder = Recorder()
    clip = Message("user", [Content.from_data(b"\x00" * 4800, "audio/pcm;rate=24000")])

    running = asyncio.ensure_future(RealtimeAgent(client, context_providers=[recorder]).run(clip))
    await eventually(lambda: bool(client.sessions) and bool(client.session.sent))
    client.session.emit(
        event("speech_started", item_id="item_1", audio_start_ms=0),
        event("speech_stopped", item_id="item_1", audio_end_ms=100),
        caller_item("item_1"),
        caller_transcript("item_1", "What time is it?"),
        *reply("resp_1", "item_2", "It's noon."),
    )
    response = await running

    assert response.text == "It's noon."
    assert [call[0] for call in client.session.calls] == []
    assert texts(recorder.reports[0]) == ([("user", "What time is it?")], [("assistant", "It's noon.")])


async def test_the_run_does_not_end_while_the_caller_is_speaking() -> None:
    client = FakeRealtimeClient()
    clip = Message("user", [Content.from_data(b"\x00" * 4800, "audio/pcm;rate=24000")])

    running = asyncio.ensure_future(RealtimeAgent(client).run(clip))
    await eventually(lambda: bool(client.sessions) and bool(client.session.sent))
    client.session.emit(
        event("speech_started", item_id="item_1", audio_start_ms=0),
        event("speech_stopped", item_id="item_1", audio_end_ms=100),
        caller_item("item_1"),
        event("response_created", response_id="resp_1"),
        event("speech_started", item_id="item_2", audio_start_ms=900),
        event("response_done", response_id="resp_1", status="cancelled"),
    )
    await eventually(lambda: not client.session.queued_events)
    client.session.emit(
        event("speech_stopped", item_id="item_2", audio_end_ms=1500),
        caller_item("item_2"),
        *reply("resp_2", "item_3", "Sure, I can help with that."),
    )
    response = await running

    assert response.text == "Sure, I can help with that."


async def test_audio_in_an_earlier_agents_reply_is_left_out_of_the_input() -> None:
    client = FakeRealtimeClient()
    client.replies.append(reply("resp_1", "item_1", "Happy to help."))
    earlier_reply = Message(
        "assistant", ["Hello, how can I help?", Content.from_data(b"\x00" * 4800, "audio/pcm;rate=24000")]
    )

    await RealtimeAgent(client).run([earlier_reply, Message("user", ["I need a refund."])])

    sent = [
        (message.role, [content.type for content in message.contents])
        for message in client.session.sent
        if isinstance(message, Message)
    ]
    assert sent == [("assistant", ["text"]), ("user", ["text"])]


async def test_a_reply_whose_audio_interleaves_its_transcript_returns_the_transcript_once() -> None:
    client = FakeRealtimeClient()
    client.replies.append([
        event("response_created", response_id="resp_1"),
        agent_item("item_1"),
        agent_text("resp_1", "item_1", "Hello ", final=False),
        agent_audio("resp_1", "item_1", 100),
        agent_text("resp_1", "item_1", "there.", final=False),
        agent_audio("resp_1", "item_1", 100),
        agent_text("resp_1", "item_1", "Hello there."),
        event("response_done", response_id="resp_1", status="completed"),
    ])

    response = await RealtimeAgent(client).run("Hi")

    assert response.text == "Hello there."


# endregion


class TestVerification:
    async def test_a_filler_line_a_tool_says_does_not_end_the_run(self) -> None:
        client = FakeRealtimeClient()

        @tool
        async def look_up_order(ctx: FunctionInvocationContext) -> str:
            """Look up an order, saying a filler line first."""
            assert ctx.realtime_conversation is not None
            await ctx.realtime_conversation.say("One moment while I check.")
            client.session.emit(
                event("response_created", response_id="resp_said"),
                event("response_done", response_id="resp_said", status="completed"),
            )
            await eventually(lambda: not client.session.queued_events)
            return "Order 7 shipped yesterday."

        client.replies.extend([
            call_response("resp_1", "item_1", "call_1", "look_up_order"),
            reply("resp_2", "item_2", "Your order shipped yesterday."),
        ])

        response = await RealtimeAgent(client, tools=[look_up_order]).run("Where's order 7?")

        assert response.text == "Your order shipped yesterday."

    async def test_the_run_closes_its_realtime_session(self) -> None:
        client = FakeRealtimeClient()
        client.replies.append(reply("resp_1", "item_1", "Hello there."))

        await RealtimeAgent(client).run("Hi")

        assert client.session.close_count == 1

    async def test_the_response_holds_the_replys_audio(self) -> None:
        client = FakeRealtimeClient()
        client.replies.append(spoken_reply("resp_1", "item_1", "Hello there.", 1000))

        response = await RealtimeAgent(client).run("Hi")

        audio = b"".join(
            content.get_data_bytes()
            for message in response.messages
            for content in message.contents
            if content.type == "data"
        )
        assert audio == b"\x00" * 48 * 1000

    async def test_usage_is_summed_over_the_runs_responses(self) -> None:
        client = FakeRealtimeClient()
        calling = call_response("resp_1", "item_1", "call_1", "get_weather", city="Paris")
        answering = reply("resp_2", "item_2", "It's sunny in Paris.")
        client.replies.extend([
            [*calling[:-1], usage("resp_1", 10, 5), calling[-1]],
            [*answering[:-1], usage("resp_2", 20, 7), answering[-1]],
        ])

        response = await RealtimeAgent(client, tools=[get_weather]).run("What's the weather in Paris?")

        assert response.usage_details is not None
        assert (response.usage_details["input_token_count"], response.usage_details["output_token_count"]) == (30, 12)

    async def test_middleware_termination_ends_the_run_without_a_follow_up(self) -> None:
        client = FakeRealtimeClient()
        client.replies.append(call_response("resp_1", "item_1", "call_1", "get_weather", city="Paris"))

        @function_middleware
        async def stop_the_loop(context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
            raise MiddlewareTermination("Handled by the app.", result="Transferred to a person.")

        agent = RealtimeAgent(client, tools=[get_weather], middleware=[stop_the_loop])
        response = await agent.run("What's the weather in Paris?")

        assert [result.result for result in _results(response)] == ["Transferred to a person."]
        assert [call[0] for call in client.session.calls] == ["create_response"]

    async def test_a_reply_cut_by_the_content_filter_returns_rather_than_raising(self) -> None:
        client = FakeRealtimeClient()
        client.replies.append([
            *reply("resp_1", "item_1", "I can't")[:-1],
            event("response_done", response_id="resp_1", status="incomplete", reason="content_filter"),
        ])

        response = await RealtimeAgent(client).run("Hi")

        assert response.finish_reason == "content_filter"

    async def test_an_unacknowledged_response_request_raises_realtime_control_timeout_exception(self) -> None:
        client = FakeRealtimeClient()
        client.create_response_error = RealtimeControlTimeoutException("No acknowledgement.")
        recorder = Recorder()

        with pytest.raises(RealtimeControlTimeoutException):
            await RealtimeAgent(client, context_providers=[recorder]).run("Hi")

        assert recorder.reports == []

    async def test_cancelling_a_run_closes_its_session_and_reports_nothing(self) -> None:
        client = FakeRealtimeClient()
        client.replies.append([event("response_created", response_id="resp_1")])
        recorder = Recorder()

        running = asyncio.ensure_future(RealtimeAgent(client, context_providers=[recorder]).run("Hi"))
        await eventually(
            lambda: bool(client.sessions) and bool(client.session.calls) and not client.session.queued_events
        )
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running

        assert client.session.close_count == 1
        assert recorder.reports == []

    async def test_leaving_a_streamed_run_early_closes_its_session_and_reports_nothing(self) -> None:
        client = FakeRealtimeClient()
        client.replies.append([event("response_created", response_id="resp_1")])
        recorder = Recorder()

        async with RealtimeAgent(client, context_providers=[recorder]).run("Hi", stream=True) as stream:
            async for _ in stream:
                break

        assert client.session.close_count == 1
        assert recorder.reports == []

    async def test_a_tool_closing_the_conversation_fails_the_run(self) -> None:
        client = FakeRealtimeClient()
        recorder = Recorder()

        @tool
        async def hang_up(ctx: FunctionInvocationContext) -> str:
            """End the call."""
            assert ctx.realtime_conversation is not None
            await ctx.realtime_conversation.close()
            return "Goodbye."

        client.replies.append(call_response("resp_1", "item_1", "call_1", "hang_up"))
        agent = RealtimeAgent(client, tools=[hang_up], context_providers=[recorder])

        with pytest.raises(RealtimeException, match="before the run's reply finished"):
            await agent.run("That's all, thanks.")

        assert recorder.reports == []

    async def test_a_run_with_a_session_reports_one_exchange_after_its_session_closes(self) -> None:
        client = FakeRealtimeClient()
        client.replies.append(reply("resp_1", "item_1", "Hello there."))
        closes_when_reported: list[int] = []

        class CloseChecker(Recorder):
            async def after_run(
                self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
            ) -> None:
                closes_when_reported.append(client.session.close_count)
                await super().after_run(agent=agent, session=session, context=context, state=state)

        recorder = CloseChecker()
        await RealtimeAgent(client, context_providers=[recorder]).run("Hi", session=AgentSession())

        assert [texts(report) for report in recorder.reports] == [([("user", "Hi")], [("assistant", "Hello there.")])]
        assert closes_when_reported == [1]

    async def test_a_runs_tool_call_and_result_are_in_its_one_exchange(self) -> None:
        client = FakeRealtimeClient()
        client.replies.extend([
            call_response("resp_1", "item_1", "call_1", "get_weather", city="Paris"),
            reply("resp_2", "item_2", "It's sunny in Paris."),
        ])
        recorder = Recorder()
        agent = RealtimeAgent(client, tools=[get_weather], context_providers=[recorder])

        await agent.run("What's the weather in Paris?")

        assert len(recorder.reports) == 1
        response = recorder.reports[0].response
        assert response is not None
        assert [(message.role, [content.type for content in message.contents]) for message in response.messages] == [
            ("assistant", ["function_call"]),
            ("tool", ["function_result"]),
            ("assistant", ["text"]),
        ]

    async def test_the_next_run_replays_the_previous_run(self) -> None:
        client = FakeRealtimeClient()
        client.replies.extend([reply("resp_1", "item_1", "Hello there."), reply("resp_2", "item_2", "I'm well.")])
        agent = RealtimeAgent(client)
        session = AgentSession()

        await agent.run("Hi", session=session)
        await agent.run("How are you?", session=session)

        sent = [(message.role, message.text) for message in client.session.sent if isinstance(message, Message)]
        assert sent == [("user", "Hi"), ("assistant", "Hello there."), ("user", "How are you?")]

    async def test_a_run_whose_turn_ended_is_reported_even_if_cancelled_during_close(self) -> None:
        client = FakeRealtimeClient()
        client.replies.append(reply("resp_1", "item_1", "Hello there."))
        reporting = asyncio.Event()
        release = asyncio.Event()

        class SlowStore(Recorder):
            async def after_run(
                self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
            ) -> None:
                reporting.set()
                await release.wait()
                await super().after_run(agent=agent, session=session, context=context, state=state)

        store = SlowStore()
        running = asyncio.ensure_future(RealtimeAgent(client, context_providers=[store]).run("Hi"))
        await asyncio.wait_for(reporting.wait(), 5)
        running.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await running

        assert len(store.reports) == 1

    async def test_an_earlier_agents_tool_calls_and_results_go_in_as_context(self) -> None:
        client = FakeRealtimeClient()
        client.replies.append(reply("resp_1", "item_1", "Your order shipped yesterday."))
        conversation_so_far = [
            Message("user", ["Where's order 7?"]),
            Message("assistant", [Content.from_function_call("call_9", "look_up_order", arguments={"order": "7"})]),
            Message("tool", [Content.from_function_result("call_9", result="Shipped yesterday.")]),
        ]

        response = await RealtimeAgent(client).run(conversation_so_far)

        sent = [
            [content.type for content in message.contents]
            for message in client.session.sent
            if isinstance(message, Message)
        ]
        assert sent == [["text"], ["function_call"], ["function_result"]]
        assert response.text == "Your order shipped yesterday."

    async def test_a_run_with_no_input_requests_a_response(self) -> None:
        client = FakeRealtimeClient()
        client.replies.append(reply("resp_1", "item_1", "Welcome to Contoso Dental."))

        response = await RealtimeAgent(client).run()

        assert [call[0] for call in client.session.calls] == ["create_response"]
        assert response.text == "Welcome to Contoso Dental."

    async def test_per_run_options_tools_middleware_and_function_invocation_kwargs_apply_to_the_run(self) -> None:
        client = FakeRealtimeClient()
        wrapped: list[str] = []

        @tool
        def whoami(ctx: FunctionInvocationContext) -> str:
            """Say who the caller is."""
            return str(ctx.kwargs["user_id"])

        @function_middleware
        async def record(context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
            wrapped.append(context.function.name)
            await call_next()

        client.replies.extend([
            call_response("resp_1", "item_1", "call_1", "whoami"),
            reply("resp_2", "item_2", "You're u1."),
        ])
        options: RealtimeOptions = {"voice": "marin"}

        response = await RealtimeAgent(client).run(
            "Who am I?",
            options=options,
            tools=[whoami],
            middleware=[record],
            function_invocation_kwargs={"user_id": "u1"},
        )

        start = client.start_session_calls[0]
        assert start["options"]["voice"] == "marin"
        assert [tool.name for tool in start["tools"]] == ["whoami"]
        assert wrapped == ["whoami"]
        assert [result.result for result in _results(response)] == ["u1"]

    async def test_client_kwargs_are_ignored(self) -> None:
        client = FakeRealtimeClient()
        client.replies.append(reply("resp_1", "item_1", "Hello there."))

        response = await RealtimeAgent(client).run("Hi", client_kwargs={"timeout": 5})

        assert response.text == "Hello there."

    async def test_a_chat_agent_can_call_a_realtime_agent_as_a_tool(self, chat_client_base: MockBaseChatClient) -> None:
        client = FakeRealtimeClient()
        client.replies.append(reply("resp_1", "item_1", "Order 7 shipped yesterday."))
        orders = RealtimeAgent(client, name="orders", description="Answers questions about orders.")
        chat_client_base.run_responses = [
            ChatResponse(
                messages=[
                    Message(
                        "assistant",
                        [Content.from_function_call("call_1", "orders", arguments={"task": "Where's order 7?"})],
                    )
                ]
            ),
            ChatResponse(messages=[Message("assistant", ["It shipped yesterday."])]),
        ]

        response = await Agent(client=chat_client_base, tools=[orders.as_tool()]).run("Where's order 7?")

        assert [result.result for result in _results(response)] == ["Order 7 shipped yesterday."]
