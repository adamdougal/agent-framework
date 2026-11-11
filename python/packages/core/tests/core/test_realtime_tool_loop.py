# Copyright (c) Microsoft. All rights reserved.

"""The realtime function-tool loop: batches, follow-ups, tool chains and settlement."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent_framework import (
    Agent,
    AgentResponseUpdate,
    AgentSession,
    ChatResponse,
    Content,
    FunctionInvocationContext,
    FunctionMiddleware,
    FunctionTool,
    Message,
    MiddlewareFailure,
    MiddlewareTermination,
    RealtimeAgent,
    RealtimeEvent,
    RealtimeOptions,
    function_middleware,
    tool,
)
from agent_framework.exceptions import (
    RealtimeActiveResponseException,
    RealtimeConnectionException,
    RealtimeControlException,
    ToolExecutionException,
)
from agent_framework.observability import OtelAttr

from .conftest import MockBaseChatClient
from .realtime_helpers import FakeRealtimeClient, FakeRealtimeSession, event, eventually, next_update


@tool
def get_weather(city: str) -> str:
    """Get the weather for a city."""
    return f"Sunny in {city}"


def _call(call_id: str, name: str, *, response_id: str = "resp_1", **arguments: Any) -> RealtimeEvent:
    return event(
        "function_call",
        content=Content.from_function_call(call_id=call_id, name=name, arguments=arguments),
        response_id=response_id,
        item_id=f"item_{call_id}",
    )


def _response(response_id: str, *calls: RealtimeEvent, status: str = "completed") -> list[RealtimeEvent]:
    """The events of one response that made ``calls`` and ended with ``status``."""
    return [
        event("response_created", response_id=response_id),
        *calls,
        event("response_done", response_id=response_id, status=status),
    ]


async def _next_function_result(conversation: Any) -> AgentResponseUpdate:
    """Read updates until a ``function_result`` update arrives."""
    while True:
        update = await next_update(conversation)
        if any(content.type == "function_result" for content in update.contents):
            return update


def _blocking_tool(name: str = "look_up") -> tuple[Any, asyncio.Event, asyncio.Event]:
    """A tool that waits for its release, with the events for 'started' and 'release'."""
    started = asyncio.Event()
    release = asyncio.Event()

    @tool(name=name)
    async def blocking() -> str:
        """Look something up, slowly."""
        started.set()
        await release.wait()
        return f"{name} done"

    return blocking, started, release


async def _next_matching(conversation: Any, kind: str) -> AgentResponseUpdate:
    """Read updates until one with ``kind`` content arrives."""
    while True:
        update = await next_update(conversation)
        if any(content.kind == kind for content in update.contents):
            return update


async def _read_all(conversation: Any) -> list[AgentResponseUpdate]:
    return [update async for update in conversation]


async def _streamed(conversation: Any, count: int) -> None:
    """Wait until the conversation has streamed ``count`` more updates."""
    for _ in range(count):
        await next_update(conversation)


def _tool_span_outcomes(span_exporter: InMemorySpanExporter) -> dict[str, tuple[trace.StatusCode, Any]]:
    """Each finished execute_tool span's status and error.type."""
    return {
        span.name: (span.status.status_code, (span.attributes or {}).get(OtelAttr.ERROR_TYPE))
        for span in span_exporter.get_finished_spans()
        if span.name.startswith("execute_tool")
    }


def _tool_messages(session: FakeRealtimeSession) -> list[Message]:
    return [message for message in session.sent if isinstance(message, Message) and message.role == "tool"]


def _results(session: FakeRealtimeSession) -> list[tuple[str | None, Any]]:
    return [(content.call_id, content.result) for message in _tool_messages(session) for content in message.contents]


# region Batches


async def test_a_call_runs_after_its_response_finishes_and_its_result_goes_to_the_session() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert _results(client.session) == [("call_1", "Sunny in Paris")]


async def test_a_call_does_not_run_before_its_response_finishes() -> None:
    client = FakeRealtimeClient()
    runs: list[str] = []

    @tool
    def record_visit(city: str) -> str:
        """Record a visit."""
        runs.append(city)
        return "recorded"

    async with RealtimeAgent(client, tools=[record_visit]).start_conversation() as conversation:
        client.session.emit(
            event("response_created", response_id="resp_1"), _call("call_1", "record_visit", city="Oslo")
        )
        await next_update(conversation)
        await next_update(conversation)
        await asyncio.sleep(0.01)

        assert runs == []


async def test_a_batch_requests_one_follow_up_after_its_results() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: bool(client.session.calls))
        await asyncio.sleep(0.01)

    assert client.session.calls == [("create_response", None)]
    assert isinstance(client.session.sent[-1], Message)
    assert client.session.sent[-1].role == "tool"


async def test_each_settled_call_streams_a_function_result_update() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[get_weather], name="receptionist")

    async with agent.start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        update = await _next_function_result(conversation)

    assert (update.role, update.author_name, update.agent_id, update.response_id) == (
        "tool",
        "receptionist",
        agent.id,
        "resp_1",
    )
    assert [(content.call_id, content.result) for content in update.contents] == [("call_1", "Sunny in Paris")]


async def test_results_go_out_in_model_order_in_one_message() -> None:
    client = FakeRealtimeClient()

    @tool
    async def slow_lookup() -> str:
        """Look something up slowly."""
        await asyncio.sleep(0.02)
        return "slow"

    @tool
    def fast_lookup() -> str:
        """Look something up quickly."""
        return "fast"

    async with RealtimeAgent(client, tools=[slow_lookup, fast_lookup]).start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "slow_lookup"), _call("call_2", "fast_lookup")))
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert len(_tool_messages(client.session)) == 1
    assert _results(client.session) == [("call_1", "slow"), ("call_2", "fast")]


async def test_calls_run_concurrently_by_default() -> None:
    client = FakeRealtimeClient()
    started = {"first": asyncio.Event(), "second": asyncio.Event()}

    @tool
    async def first() -> str:
        """Wait for the second call to start."""
        started["first"].set()
        await started["second"].wait()
        return "first done"

    @tool
    async def second() -> str:
        """Wait for the first call to start."""
        started["second"].set()
        await started["first"].wait()
        return "second done"

    async with RealtimeAgent(client, tools=[first, second]).start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "first"), _call("call_2", "second")))
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert _results(client.session) == [("call_1", "first done"), ("call_2", "second done")]


async def test_calls_run_in_model_order_without_concurrent_invocation() -> None:
    client = FakeRealtimeClient()
    timeline: list[str] = []

    @tool
    async def first() -> str:
        """Run first."""
        timeline.append("first started")
        await asyncio.sleep(0.01)
        timeline.append("first finished")
        return "first done"

    @tool
    def second() -> str:
        """Run second."""
        timeline.append("second started")
        return "second done"

    agent = RealtimeAgent(
        client, tools=[first, second], function_invocation_configuration={"allow_concurrent_invocation": False}
    )
    async with agent.start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "first"), _call("call_2", "second")))
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert timeline == ["first started", "first finished", "second started"]


async def test_a_call_for_an_unknown_tool_gets_the_not_found_result() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "missing")))
        await eventually(lambda: bool(client.session.calls))

    assert _results(client.session) == [("call_1", 'Error: Requested function "missing" not found.')]
    assert client.session.calls == [("create_response", None)]


async def test_each_call_and_its_result_share_one_occurrence_id() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
        client.session.emit(
            *_response("resp_1", _call("call_1", "get_weather", city="Paris"), _call("call_2", "book_flight"))
        )
        updates = [await next_update(conversation) for _ in range(6)]

    contents = [content for update in updates for content in update.contents]
    calls = {content.call_id: content.id for content in contents if content.type == "function_call"}
    results = {content.call_id: content.id for content in contents if content.type == "function_result"}
    assert all(occurrence_id and occurrence_id.startswith("af-call-") for occurrence_id in calls.values())
    assert len(set(calls.values())) == 2
    assert results == calls


@pytest.mark.parametrize("status", ["cancelled", "incomplete", "failed"])
async def test_calls_from_a_response_that_did_not_complete_do_not_run(status: str) -> None:
    client = FakeRealtimeClient()
    runs: list[str] = []

    @tool
    def record_visit(city: str) -> str:
        """Record a visit."""
        runs.append(city)
        return "recorded"

    async with RealtimeAgent(client, tools=[record_visit]).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "record_visit", city="Oslo"), status=status))
        update = await _next_function_result(conversation)
        await eventually(lambda: bool(_tool_messages(client.session)))
        await asyncio.sleep(0.01)

    [result] = update.contents
    assert (result.call_id, result.exception) == ("call_1", status)
    assert runs == []
    assert [call_id for call_id, _ in _results(client.session)] == ["call_1"]
    assert client.session.calls == []


async def test_function_invocation_kwargs_reach_tools_as_copied_at_start() -> None:
    client = FakeRealtimeClient()

    @tool
    def whoami(ctx: FunctionInvocationContext) -> str:
        """Say who the caller is."""
        return str(ctx.kwargs["user_id"])

    invocation_kwargs = {"user_id": "u1"}
    conversation = RealtimeAgent(client, tools=[whoami]).start_conversation(
        function_invocation_kwargs=invocation_kwargs
    )
    invocation_kwargs["user_id"] = "changed"
    async with conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "whoami")))
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert _results(client.session) == [("call_1", "u1")]


async def test_a_tools_context_carries_the_agent_session() -> None:
    client = FakeRealtimeClient()

    @tool
    def which_session(ctx: FunctionInvocationContext) -> str:
        """Say which agent session the call belongs to."""
        return ctx.session.session_id if ctx.session is not None else "none"

    agent_session = AgentSession(session_id="session_1")
    async with RealtimeAgent(client, tools=[which_session]).start_conversation(session=agent_session):
        client.session.emit(*_response("resp_1", _call("call_1", "which_session")))
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert _results(client.session) == [("call_1", "session_1")]


async def test_a_tools_context_carries_the_provider_owned_session_keys_the_caller_cannot_replace() -> None:
    client = FakeRealtimeClient()
    client.service_session_state_keys = ("provider.thread",)
    seen: list[frozenset[str] | None] = []

    @tool
    def check(ctx: FunctionInvocationContext) -> str:
        """Check the parent's session keys."""
        seen.append(ctx.parent_service_session_state_keys)
        return "checked"

    spoofed = {"_parent_service_session_state_keys": frozenset({"spoofed"})}
    async with RealtimeAgent(client, tools=[check]).start_conversation(function_invocation_kwargs=spoofed):
        client.session.emit(*_response("resp_1", _call("call_1", "check")))
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert seen == [frozenset({"provider.thread"})]


async def test_middleware_that_keeps_session_state_gets_a_session_per_conversation() -> None:
    client = FakeRealtimeClient()
    sessions: list[AgentSession | None] = []

    class SessionStateMiddleware(FunctionMiddleware):
        _requires_session_state = True

        async def process(self, context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
            sessions.append(context.session)
            await call_next()

    agent = RealtimeAgent(client, tools=[get_weather], middleware=[SessionStateMiddleware()])
    for _ in range(2):
        async with agent.start_conversation():
            client.session.emit(
                *_response("resp_1", _call("call_1", "get_weather", city="Paris")),
                *_response("resp_2", _call("call_2", "get_weather", response_id="resp_2", city="Rome")),
            )
            await eventually(lambda: len(_tool_messages(client.session)) == 2)

    first, second, third, fourth = sessions
    assert isinstance(first, AgentSession)
    assert first is second
    assert third is fourth
    assert third is not first


async def test_a_tools_context_carries_the_conversation() -> None:
    client = FakeRealtimeClient()
    seen: list[object] = []

    @tool
    def inspect_conversation(ctx: FunctionInvocationContext) -> str:
        """Record the conversation the call belongs to."""
        seen.append(ctx.realtime_conversation)
        return "seen"

    async with RealtimeAgent(client, tools=[inspect_conversation]).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "inspect_conversation")))
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert _results(client.session) == [("call_1", "seen")]
    assert seen == [conversation]


async def test_function_middleware_sees_the_conversation_on_the_context() -> None:
    client = FakeRealtimeClient()
    seen: list[object] = []

    @function_middleware
    async def record_conversation(context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        seen.append(context.realtime_conversation)
        await call_next()

    agent = RealtimeAgent(client, tools=[get_weather], middleware=[record_conversation])
    async with agent.start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert seen == [conversation]
    assert _results(client.session) == [("call_1", "Sunny in Paris")]


async def test_the_agents_function_middleware_wraps_the_start_middleware() -> None:
    client = FakeRealtimeClient()
    order: list[str] = []

    @function_middleware
    async def agent_middleware(context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        order.append("agent")
        await call_next()

    @function_middleware
    async def start_middleware(context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        order.append("start")
        await call_next()

    agent = RealtimeAgent(client, tools=[get_weather], middleware=[agent_middleware])
    async with agent.start_conversation(middleware=[start_middleware]):
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert order == ["agent", "start"]


async def test_middleware_termination_sends_results_without_a_follow_up() -> None:
    client = FakeRealtimeClient()

    @function_middleware
    async def stop_the_loop(context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        raise MiddlewareTermination("Handled by the app.", result="Transferred to a person.")

    agent = RealtimeAgent(client, tools=[get_weather], middleware=[stop_the_loop])
    async with agent.start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: bool(_tool_messages(client.session)))
        await asyncio.sleep(0.01)

    assert _results(client.session) == [("call_1", "Transferred to a person.")]
    assert client.session.calls == []


async def test_middleware_failure_fails_the_conversation() -> None:
    client = FakeRealtimeClient()

    @function_middleware
    async def guardrail(context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        raise MiddlewareFailure("Blocked by policy.")

    async def read_all(conversation: Any) -> None:
        async for _ in conversation:
            pass

    agent = RealtimeAgent(client, tools=[get_weather], middleware=[guardrail])
    async with agent.start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        with pytest.raises(MiddlewareFailure, match="Blocked by policy"):
            await asyncio.wait_for(read_all(conversation), 1)
        await eventually(lambda: client.session.close_count == 1)


async def test_a_middleware_failure_settles_its_batchs_calls() -> None:
    client = FakeRealtimeClient()
    updates: list[AgentResponseUpdate] = []

    @function_middleware
    async def guardrail(context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        raise MiddlewareFailure("Blocked by policy.")

    async def read_all(conversation: Any) -> None:
        async for update in conversation:
            updates.append(update)

    agent = RealtimeAgent(client, tools=[get_weather], middleware=[guardrail])
    async with agent.start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        with pytest.raises(MiddlewareFailure):
            await asyncio.wait_for(read_all(conversation), 1)

    settled = [content for update in updates for content in update.contents if content.type == "function_result"]
    assert [(content.call_id, content.exception) for content in settled] == [("call_1", "conversation_closed")]


async def test_a_tool_that_cancels_itself_ends_the_conversation_and_its_call_is_settled() -> None:
    client = FakeRealtimeClient()
    updates: list[AgentResponseUpdate] = []

    @tool
    async def give_up() -> str:
        """Give up by cancelling."""
        raise asyncio.CancelledError

    async def read_all(conversation: Any) -> None:
        async for update in conversation:
            updates.append(update)

    async with RealtimeAgent(client, tools=[give_up]).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "give_up")))
        with pytest.raises(ToolExecutionException):
            await asyncio.wait_for(read_all(conversation), 1)

    settled = [content for update in updates for content in update.contents if content.type == "function_result"]
    assert [(content.call_id, content.exception) for content in settled] == [("call_1", "conversation_closed")]
    assert client.session.close_count == 1


async def test_a_tool_that_cancels_itself_while_closing_still_has_its_call_settled() -> None:
    client = FakeRealtimeClient()
    started = asyncio.Event()
    give_up_now = asyncio.Event()

    @tool
    async def give_up_later() -> str:
        """Give up by cancelling, once told to."""
        started.set()
        await give_up_now.wait()
        raise asyncio.CancelledError

    async with RealtimeAgent(client, tools=[give_up_later]).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "give_up_later")))
        await started.wait()
        session_closed = client.session.hold("close")
        closing = asyncio.create_task(conversation.close())
        await eventually(lambda: client.session.close_count == 1)
        give_up_now.set()
        await asyncio.sleep(0.01)
        session_closed.set()
        await asyncio.wait_for(closing, 1)
        updates = await _read_all(conversation)

    settled = [content for update in updates for content in update.contents if content.type == "function_result"]
    assert [(content.call_id, content.exception) for content in settled] == [("call_1", "conversation_closed")]


# endregion

# region Follow-ups


async def test_a_follow_up_waits_for_the_active_response_to_finish() -> None:
    client = FakeRealtimeClient()
    look_up, started, release = _blocking_tool()

    async with RealtimeAgent(client, tools=[look_up]).start_conversation() as conversation:
        client.session.emit(
            *_response("resp_1", _call("call_1", "look_up")), event("response_created", response_id="resp_2")
        )
        await _streamed(conversation, 4)
        await started.wait()
        release.set()
        await eventually(lambda: bool(_tool_messages(client.session)))
        await asyncio.sleep(0.01)
        assert client.session.calls == []

        client.session.emit(event("response_done", response_id="resp_2", status="completed"))
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("create_response", None)]


async def test_a_follow_up_waits_while_the_caller_is_speaking() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation():
        client.session.emit(
            event("speech_started", item_id="item_9", audio_start_ms=100),
            *_response("resp_1", _call("call_1", "get_weather", city="Paris")),
        )
        await eventually(lambda: bool(_tool_messages(client.session)))
        await asyncio.sleep(0.01)
        assert client.session.calls == []

        client.session.emit(event("speech_stopped", item_id="item_9", audio_end_ms=900))
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("create_response", None)]


async def test_a_follow_up_waiting_its_turn_holds_when_the_caller_starts_speaking() -> None:
    client = FakeRealtimeClient()
    audio = Content.from_data(data=b"\x00" * 4800, media_type="audio/pcm;rate=24000")  # 100 ms

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
        client.session.emit(
            event("response_created", response_id="resp_0"),
            event("output_audio", content=audio, response_id="resp_0", item_id="item_a", content_index=0),
            event("response_done", response_id="resp_0", status="completed"),
        )
        await _streamed(conversation, 3)
        cut = client.session.hold("truncate")
        interrupting = asyncio.create_task(conversation.interrupt(played_ms=50))
        await eventually(lambda: bool(client.session.calls))  # the truncate holds the control lock
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: bool(_tool_messages(client.session)))
        await asyncio.sleep(0.01)  # the follow-up now waits its turn behind the truncate

        client.session.emit(event("speech_started", item_id="item_9", audio_start_ms=100))
        await asyncio.sleep(0.01)
        cut.set()
        await interrupting
        await asyncio.sleep(0.02)
        assert [call[0] for call in client.session.calls] == ["truncate"]

        client.session.emit(event("speech_stopped", item_id="item_9", audio_end_ms=900))
        await eventually(lambda: len(client.session.calls) == 2)

    assert client.session.calls[-1] == ("create_response", None)


async def test_a_response_created_after_the_results_counts_as_the_follow_up() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation():
        client.session.emit(
            event("speech_started", item_id="item_9", audio_start_ms=100),
            *_response("resp_1", _call("call_1", "get_weather", city="Paris")),
        )
        await eventually(lambda: bool(_tool_messages(client.session)))

        client.session.emit(
            event("speech_stopped", item_id="item_9", audio_end_ms=900),
            event("response_created", response_id="resp_3"),
            event("response_done", response_id="resp_3", status="completed"),
        )
        await asyncio.sleep(0.02)

    assert client.session.calls == []


async def test_a_follow_up_that_loses_a_race_is_not_retried() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation():
        client.session.create_response_events = [event("response_created", response_id="resp_3")]
        client.session.create_response_error = RealtimeActiveResponseException("Another response started first.")
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: bool(client.session.calls))

        client.session.emit(event("response_done", response_id="resp_3", status="completed"))
        await asyncio.sleep(0.02)

    assert client.session.calls == [("create_response", None)]


async def test_filler_a_tool_requests_does_not_count_as_the_follow_up() -> None:
    client = FakeRealtimeClient()
    say_filler = asyncio.Event()
    release = asyncio.Event()

    @tool
    async def check_account(ctx: FunctionInvocationContext) -> str:
        """Check the account, saying a filler line first."""
        assert ctx.realtime_conversation is not None
        await say_filler.wait()
        await ctx.realtime_conversation.create_response()
        await release.wait()
        return "account checked"

    async with RealtimeAgent(client, tools=[get_weather, check_account]).start_conversation():
        client.session.create_response_events = [event("response_created", response_id="resp_filler")]
        client.session.emit(
            event("speech_started", item_id="item_9", audio_start_ms=100),
            *_response("resp_1", _call("call_1", "get_weather", city="Paris")),
            *_response("resp_2", _call("call_2", "check_account", response_id="resp_2")),
        )
        await eventually(lambda: _results(client.session) == [("call_1", "Sunny in Paris")])
        acknowledge_filler = client.session.hold("create_response")
        say_filler.set()
        await eventually(lambda: len(client.session.calls) == 1)
        client.session.create_response_events = []
        client.session.emit(
            event("response_done", response_id="resp_filler", status="completed"),
            event("speech_stopped", item_id="item_9", audio_end_ms=900),
        )
        await asyncio.sleep(0.01)  # the follow-up wakes while the filler's request still waits for its acknowledgement
        acknowledge_filler.set()

        await eventually(lambda: len(client.session.calls) == 2)
        release.set()

    assert client.session.calls[1] == ("create_response", None)


async def test_follow_ups_due_together_merge_into_one() -> None:
    client = FakeRealtimeClient()

    @tool
    def get_time() -> str:
        """Get the time."""
        return "noon"

    async with RealtimeAgent(client, tools=[get_weather, get_time]).start_conversation():
        client.session.emit(
            *_response("resp_1", _call("call_1", "get_weather", city="Paris")),
            *_response("resp_2", _call("call_2", "get_time", response_id="resp_2")),
            event("response_created", response_id="resp_x"),
        )
        await eventually(lambda: len(_tool_messages(client.session)) == 2)
        await asyncio.sleep(0.01)
        assert client.session.calls == []

        client.session.emit(event("response_done", response_id="resp_x", status="completed"))
        await eventually(lambda: bool(client.session.calls))
        await asyncio.sleep(0.01)

    assert client.session.calls == [("create_response", None)]


async def test_results_sent_after_an_in_flight_follow_ups_response_started_get_another_follow_up() -> None:
    client = FakeRealtimeClient()
    look_up, started, release = _blocking_tool()

    async with RealtimeAgent(client, tools=[get_weather, look_up]).start_conversation() as conversation:
        acknowledge = client.session.hold("create_response")
        client.session.create_response_events = [event("response_created", response_id="resp_3")]
        client.session.emit(
            *_response("resp_1", _call("call_1", "look_up")),
            *_response("resp_2", _call("call_2", "get_weather", response_id="resp_2", city="Paris")),
        )
        for _ in range(3):  # resp_1, resp_2, and the follow-up's resp_3
            await _next_matching(conversation, "response_created")
        client.session.create_response_events = [event("response_created", response_id="resp_4")]
        release.set()
        await eventually(lambda: len(_tool_messages(client.session)) == 2)
        acknowledge.set()

        client.session.emit(event("response_done", response_id="resp_3", status="completed"))
        await eventually(lambda: len(client.session.calls) == 2)
        client.session.emit(event("response_done", response_id="resp_4", status="completed"))
        await asyncio.sleep(0.02)

    assert client.session.calls == [("create_response", None), ("create_response", None)]


async def test_results_sent_while_a_follow_up_is_rejected_still_get_a_follow_up() -> None:
    client = FakeRealtimeClient()
    look_up, started, release = _blocking_tool()

    async with RealtimeAgent(client, tools=[get_weather, look_up]).start_conversation():
        acknowledge = client.session.hold("create_response")
        client.session.create_response_error = RealtimeControlException("The server is busy.")
        client.session.emit(
            *_response("resp_1", _call("call_1", "look_up")),
            *_response("resp_2", _call("call_2", "get_weather", response_id="resp_2", city="Paris")),
        )
        await eventually(lambda: bool(client.session.calls))  # the follow-up for call_2's results is in flight
        client.session.create_response_error = None
        release.set()
        await eventually(lambda: len(_tool_messages(client.session)) == 2)

        acknowledge.set()
        await eventually(lambda: len(client.session.calls) == 2)

    assert client.session.calls == [("create_response", None), ("create_response", None)]


async def test_an_unexpected_failure_in_a_follow_up_ends_the_conversation() -> None:
    client = FakeRealtimeClient()

    async def read_all(conversation: Any) -> None:
        async for _ in conversation:
            pass

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
        client.session.create_response_error = KeyError("response")
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        with pytest.raises(KeyError):
            await asyncio.wait_for(read_all(conversation), 1)

    assert client.session.close_count == 1


async def test_a_failed_follow_up_streams_an_error_and_is_dropped() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
        client.session.create_response_error = RealtimeControlException("Rate limited.", code="rate_limit_exceeded")
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        while True:
            update = await next_update(conversation)
            if any(content.type == "error" for content in update.contents):
                break
        await asyncio.sleep(0.01)

    [error] = update.contents
    assert error.error_code == "rate_limit_exceeded"
    assert client.session.calls == [("create_response", None)]


@pytest.mark.parametrize(
    "tool_choice",
    ["required", {"mode": "required", "required_function_name": "get_weather"}],
    ids=["required", "required function"],
)
async def test_a_follow_up_relaxes_a_required_tool_choice_to_auto(tool_choice: Any) -> None:
    client = FakeRealtimeClient()
    default_options: RealtimeOptions = {"tool_choice": tool_choice}
    agent = RealtimeAgent(client, tools=[get_weather], default_options=default_options)

    async with agent.start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("create_response", {"tool_choice": "auto"})]


# endregion

# region Tool chains and limits


async def test_follow_ups_reaching_max_iterations_turn_tools_off() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[get_weather], function_invocation_configuration={"max_iterations": 2})

    async with agent.start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: len(client.session.calls) == 1)
        client.session.emit(*_response("resp_2", _call("call_2", "get_weather", response_id="resp_2", city="Rome")))
        await eventually(lambda: len(client.session.calls) == 2)

    assert client.session.calls == [("create_response", None), ("create_response", {"tool_choice": "none"})]


async def test_a_response_the_provider_creates_after_the_results_counts_as_a_follow_up() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[get_weather], function_invocation_configuration={"max_iterations": 2})

    async with agent.start_conversation():
        release_send = client.session.hold("send")
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: client.session.send_attempts == 1)
        release_send.set()
        client.session.emit(event("response_created", response_id="resp_2"))  # the provider answers the results
        await asyncio.sleep(0.01)
        client.session.emit(
            _call("call_2", "get_weather", response_id="resp_2", city="Rome"),
            event("response_done", response_id="resp_2", status="completed"),
        )
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("create_response", {"tool_choice": "none"})]


async def test_calls_after_the_chain_used_its_max_iterations_do_not_run() -> None:
    client = FakeRealtimeClient()
    runs: list[str] = []

    @tool
    def record(city: str) -> str:
        """Record a city."""
        runs.append(city)
        return city

    agent = RealtimeAgent(client, tools=[record], function_invocation_configuration={"max_iterations": 1})
    async with agent.start_conversation() as conversation:
        client.session.create_response_events = [event("response_created", response_id="resp_2")]
        client.session.emit(*_response("resp_1", _call("call_1", "record", city="Paris")))
        await eventually(lambda: bool(client.session.calls))
        client.session.emit(event("response_done", response_id="resp_2", status="completed"))
        await asyncio.sleep(0.01)

        client.session.create_response_events = [event("response_created", response_id="resp_3")]
        await conversation.create_response()
        client.session.emit(
            _call("call_3", "record", response_id="resp_3", city="Rome"),
            event("response_done", response_id="resp_3", status="completed"),
        )
        await eventually(lambda: len(_tool_messages(client.session)) == 2)

    assert runs == ["Paris"]
    assert client.session.calls == [("create_response", {"tool_choice": "none"}), ("create_response", None)]


async def test_filler_still_being_requested_does_not_count_toward_max_iterations() -> None:
    client = FakeRealtimeClient()

    @tool
    async def check_account(ctx: FunctionInvocationContext) -> str:
        """Check the account, saying a filler line first."""
        assert ctx.realtime_conversation is not None
        await ctx.realtime_conversation.create_response()
        return "account checked"

    agent = RealtimeAgent(
        client, tools=[get_weather, check_account], function_invocation_configuration={"max_iterations": 2}
    )
    async with agent.start_conversation():
        client.session.create_response_events = [event("response_created", response_id="resp_2")]
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: len(client.session.calls) == 1)  # the first follow-up, resp_2
        client.session.create_response_events = [event("response_created", response_id="resp_filler")]
        acknowledge_filler = client.session.hold("create_response")
        client.session.emit(
            _call("call_2", "check_account", response_id="resp_2"),
            event("response_done", response_id="resp_2", status="completed"),
        )
        await eventually(lambda: len(client.session.calls) == 2)  # the tool's filler, still unacknowledged
        client.session.emit(
            _call("call_3", "get_weather", response_id="resp_filler", city="Rome"),
            event("response_done", response_id="resp_filler", status="completed"),
        )
        await asyncio.sleep(0.01)  # the filler's own batch starts while its request still waits
        acknowledge_filler.set()
        await eventually(lambda: "call_3" in dict(_results(client.session)))

    assert dict(_results(client.session))["call_3"] == "Sunny in Rome"


async def test_the_tool_chain_restarts_when_the_caller_speaks() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[get_weather], function_invocation_configuration={"max_iterations": 2})

    async with agent.start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: len(client.session.calls) == 1)
        client.session.emit(
            event("speech_started", item_id="item_9", audio_start_ms=100),
            event("speech_stopped", item_id="item_9", audio_end_ms=900),
            *_response("resp_2", _call("call_2", "get_weather", response_id="resp_2", city="Rome")),
        )
        await eventually(lambda: len(client.session.calls) == 2)

    assert client.session.calls == [("create_response", None), ("create_response", None)]


async def test_the_tool_chain_restarts_when_the_app_sends_text() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[get_weather], function_invocation_configuration={"max_iterations": 2})

    async with agent.start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: len(client.session.calls) == 1)
        await conversation.send("And tomorrow?", create_response=False)
        client.session.emit(*_response("resp_2", _call("call_2", "get_weather", response_id="resp_2", city="Rome")))
        await eventually(lambda: len(client.session.calls) == 2)

    assert client.session.calls == [("create_response", None), ("create_response", None)]


async def test_sending_audio_does_not_restart_the_tool_chain() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[get_weather], function_invocation_configuration={"max_iterations": 2})

    async with agent.start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: len(client.session.calls) == 1)
        await conversation.send(Content.from_data(data=b"\x00" * 480, media_type="audio/pcm;rate=24000"))
        client.session.emit(*_response("resp_2", _call("call_2", "get_weather", response_id="resp_2", city="Rome")))
        await eventually(lambda: len(client.session.calls) == 2)

    assert client.session.calls == [("create_response", None), ("create_response", {"tool_choice": "none"})]


async def test_a_batch_from_an_earlier_tool_chain_does_not_count_toward_the_new_one() -> None:
    client = FakeRealtimeClient()
    look_up, started, release = _blocking_tool()
    agent = RealtimeAgent(
        client, tools=[look_up, get_weather], function_invocation_configuration={"max_function_calls": 1}
    )

    async with agent.start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "look_up")))
        await started.wait()
        client.session.emit(event("speech_started", item_id="item_9", audio_start_ms=100))
        await asyncio.sleep(0.01)  # the caller's turn starts a new tool chain
        release.set()
        await eventually(lambda: len(_tool_messages(client.session)) == 1)
        client.session.emit(*_response("resp_2", _call("call_2", "get_weather", response_id="resp_2", city="Paris")))
        await eventually(lambda: len(_tool_messages(client.session)) == 2)

    assert _results(client.session)[1] == ("call_2", "Sunny in Paris")


async def test_a_batch_belongs_to_the_chain_of_the_response_that_made_it() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[get_weather], function_invocation_configuration={"max_function_calls": 1})

    async with agent.start_conversation():
        client.session.emit(
            *_response("resp_1", _call("call_1", "get_weather", city="Paris")),
            event("speech_started", item_id="item_9", audio_start_ms=100),  # read before the batch starts
        )
        await eventually(lambda: len(_tool_messages(client.session)) == 1)
        client.session.emit(*_response("resp_2", _call("call_2", "get_weather", response_id="resp_2", city="Rome")))
        await eventually(lambda: len(_tool_messages(client.session)) == 2)

    assert _results(client.session)[1] == ("call_2", "Sunny in Rome")


async def test_a_follow_up_keeps_the_tool_policy_of_the_chain_that_made_it_due() -> None:
    client = FakeRealtimeClient()
    look_up, started, release = _blocking_tool()
    agent = RealtimeAgent(client, tools=[look_up], function_invocation_configuration={"max_function_calls": 1})

    async with agent.start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "look_up")))
        await started.wait()
        client.session.emit(
            event("speech_started", item_id="item_9", audio_start_ms=100),  # a new chain starts
            event("speech_stopped", item_id="item_9", audio_end_ms=900),
        )
        await asyncio.sleep(0.01)
        release.set()  # the old chain's batch reaches max_function_calls
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("create_response", {"tool_choice": "none"})]


async def test_reaching_max_function_calls_turns_tools_off() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[get_weather], function_invocation_configuration={"max_function_calls": 2})

    async with agent.start_conversation():
        client.session.emit(
            *_response(
                "resp_1",
                _call("call_1", "get_weather", city="Paris"),
                _call("call_2", "get_weather", city="Rome"),
            )
        )
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("create_response", {"tool_choice": "none"})]


async def test_calls_after_a_tripped_limit_do_not_run_and_end_the_chain() -> None:
    client = FakeRealtimeClient()
    runs: list[str] = []

    @tool
    def record_visit(city: str) -> str:
        """Record a visit."""
        runs.append(city)
        return "recorded"

    agent = RealtimeAgent(client, tools=[record_visit], function_invocation_configuration={"max_function_calls": 1})
    async with agent.start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "record_visit", city="Paris")))
        await eventually(lambda: len(client.session.calls) == 1)
        client.session.emit(*_response("resp_2", _call("call_2", "record_visit", response_id="resp_2", city="Rome")))
        await eventually(lambda: len(_tool_messages(client.session)) == 2)
        await asyncio.sleep(0.01)

    assert runs == ["Paris"]
    [(call_id, result)] = _results(client.session)[1:]
    assert call_id == "call_2"
    assert "limit" in str(result)
    assert client.session.calls == [("create_response", {"tool_choice": "none"})]


async def test_reaching_max_consecutive_errors_turns_tools_off() -> None:
    client = FakeRealtimeClient()

    @tool
    def broken() -> str:
        """Always fail."""
        raise RuntimeError("down")

    agent = RealtimeAgent(
        client, tools=[broken], function_invocation_configuration={"max_consecutive_errors_per_request": 1}
    )
    async with agent.start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "broken")))
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("create_response", {"tool_choice": "none"})]


async def test_unknown_calls_count_toward_max_consecutive_errors() -> None:
    client = FakeRealtimeClient()

    agent = RealtimeAgent(
        client, tools=[get_weather], function_invocation_configuration={"max_consecutive_errors_per_request": 1}
    )
    async with agent.start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "book_flight")))
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("create_response", {"tool_choice": "none"})]


async def test_reaching_max_duration_turns_tools_off() -> None:
    client = FakeRealtimeClient()

    @tool
    async def slow_lookup() -> str:
        """Look something up slowly."""
        await asyncio.sleep(0.05)
        return "slow"

    agent = RealtimeAgent(client, tools=[slow_lookup], function_invocation_configuration={"max_duration_seconds": 0.01})
    async with agent.start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "slow_lookup")))
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("create_response", {"tool_choice": "none"})]


async def test_disabled_function_invocation_runs_no_calls() -> None:
    client = FakeRealtimeClient()
    runs: list[str] = []

    @tool
    def record_visit(city: str) -> str:
        """Record a visit."""
        runs.append(city)
        return "recorded"

    agent = RealtimeAgent(client, tools=[record_visit], function_invocation_configuration={"enabled": False})
    async with agent.start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "record_visit", city="Paris")))
        await eventually(lambda: bool(client.session.calls))

    assert runs == []
    [(call_id, result)] = _results(client.session)
    assert call_id == "call_1"
    assert "disabled" in str(result)
    assert client.session.calls == [("create_response", {"tool_choice": "none"})]


async def test_disabled_function_invocation_turns_tools_off_whatever_the_calls_were() -> None:
    client = FakeRealtimeClient()

    agent = RealtimeAgent(client, tools=[get_weather], function_invocation_configuration={"enabled": False})
    async with agent.start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "book_flight")))
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("create_response", {"tool_choice": "none"})]


async def test_tools_that_need_approval_or_app_input_get_an_error_result(caplog: pytest.LogCaptureFixture) -> None:
    client = FakeRealtimeClient()

    @tool(approval_mode="always_require")
    def issue_refund(amount: int) -> str:
        """Issue a refund."""
        return f"refunded {amount}"

    ask_a_person = FunctionTool(name="ask_a_person", description="Ask a person; the app answers.")
    agent_session = AgentSession()
    agent = RealtimeAgent(client, tools=[issue_refund, ask_a_person])

    with caplog.at_level(logging.WARNING):
        async with agent.start_conversation(session=agent_session):
            client.session.emit(
                *_response("resp_1", _call("call_1", "issue_refund", amount=5), _call("call_2", "ask_a_person"))
            )
            await eventually(lambda: bool(_tool_messages(client.session)))

    assert [call_id for call_id, _ in _results(client.session)] == ["call_1", "call_2"]
    assert all(str(result).startswith("Error:") for _, result in _results(client.session))
    warnings = [record.getMessage() for record in caplog.records if "approval" in record.getMessage()]
    assert len(warnings) == 2
    assert "issue_refund" in warnings[0]
    assert "ask_a_person" in warnings[1]
    assert agent_session.state == {}


async def test_a_call_runs_the_tool_it_resolved_to_on_arrival() -> None:
    client = FakeRealtimeClient()

    @tool
    def retire_weather(ctx: FunctionInvocationContext) -> str:
        """Stop offering the weather tool."""
        ctx.remove_tools("get_weather")
        return "retired"

    async with RealtimeAgent(client, tools=[get_weather, retire_weather]).start_conversation():
        client.session.emit(
            event("response_created", response_id="resp_2"),
            _call("call_2", "get_weather", response_id="resp_2", city="Paris"),
            *_response("resp_1", _call("call_1", "retire_weather")),
        )
        await eventually(lambda: bool(_tool_messages(client.session)))
        client.session.emit(event("response_done", response_id="resp_2", status="completed"))
        await eventually(lambda: len(_tool_messages(client.session)) == 2)

    assert _results(client.session) == [("call_1", "retired"), ("call_2", "Sunny in Paris")]


async def test_same_named_calls_of_one_response_run_the_tool_the_first_resolved_to() -> None:
    client = FakeRealtimeClient()

    @tool(name="get_weather")
    def new_weather(city: str) -> str:
        """Get the weather from the new service."""
        return f"Cloudy in {city}"

    @tool
    def swap_weather(ctx: FunctionInvocationContext) -> str:
        """Swap to the new weather service."""
        ctx.remove_tools("get_weather")
        ctx.add_tools(new_weather)
        return "swapped"

    async with RealtimeAgent(client, tools=[get_weather, swap_weather]).start_conversation():
        client.session.emit(
            event("response_created", response_id="resp_2"),
            _call("call_2", "get_weather", response_id="resp_2", city="Paris"),
            *_response("resp_1", _call("call_1", "swap_weather")),
        )
        await eventually(lambda: bool(_tool_messages(client.session)))
        client.session.emit(
            _call("call_3", "get_weather", response_id="resp_2", city="Rome"),
            event("response_done", response_id="resp_2", status="completed"),
        )
        await eventually(lambda: len(_tool_messages(client.session)) == 2)

    assert _results(client.session) == [
        ("call_1", "swapped"),
        ("call_2", "Sunny in Paris"),
        ("call_3", "Sunny in Rome"),
    ]


async def test_barge_in_does_not_cancel_a_running_tool() -> None:
    client = FakeRealtimeClient()
    look_up, started, release = _blocking_tool()

    async with RealtimeAgent(client, tools=[look_up]).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "look_up")))
        await started.wait()
        client.session.emit(event("speech_started", item_id="item_9", audio_start_ms=100))
        await _next_matching(conversation, "speech_started")
        await conversation.interrupt()
        release.set()
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert _results(client.session) == [("call_1", "look_up done")]


# endregion

# region Close


async def test_closing_during_a_batch_settles_its_calls_as_conversation_closed() -> None:
    client = FakeRealtimeClient()
    look_up, started, _ = _blocking_tool()

    async with RealtimeAgent(client, tools=[look_up]).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "look_up")))
        await started.wait()
        await conversation.close()
        updates = [update async for update in conversation]

    settled = [content for update in updates for content in update.contents if content.type == "function_result"]
    assert [(content.call_id, content.exception) for content in settled] == [("call_1", "conversation_closed")]
    assert _tool_messages(client.session) == []


async def test_calls_from_a_response_that_finishes_while_closing_do_not_run() -> None:
    client = FakeRealtimeClient()
    runs: list[str] = []

    @tool
    def record(city: str) -> str:
        """Record a city."""
        runs.append(city)
        return city

    async with RealtimeAgent(client, tools=[record]).start_conversation() as conversation:
        release = client.session.hold("close")
        closing = asyncio.create_task(conversation.close())
        await eventually(lambda: client.session.close_count == 1)
        client.session.emit(*_response("resp_1", _call("call_1", "record", city="Paris")))
        await _streamed(conversation, 3)
        release.set()
        await closing
        updates = await _read_all(conversation)

    assert runs == []
    settled = [content for update in updates for content in update.contents if content.type == "function_result"]
    assert [(content.call_id, content.exception) for content in settled] == [("call_1", "conversation_closed")]
    assert _tool_messages(client.session) == []


async def test_closing_during_a_concurrent_batch_records_conversation_closed_on_each_running_tool(
    span_exporter: InMemorySpanExporter,
) -> None:
    client = FakeRealtimeClient()
    look_up_a, started_a, _ = _blocking_tool("look_up_a")
    look_up_b, started_b, _ = _blocking_tool("look_up_b")

    async with RealtimeAgent(client, tools=[look_up_a, look_up_b]).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "look_up_a"), _call("call_2", "look_up_b")))
        await started_a.wait()
        await started_b.wait()
        span_exporter.clear()
        await conversation.close()

    assert _tool_span_outcomes(span_exporter) == {
        "execute_tool look_up_a": (trace.StatusCode.ERROR, "conversation_closed"),
        "execute_tool look_up_b": (trace.StatusCode.ERROR, "conversation_closed"),
    }


async def test_closing_lets_each_running_tool_finish_its_cleanup_as_conversation_closed(
    span_exporter: InMemorySpanExporter,
) -> None:
    client = FakeRealtimeClient()
    cleaned_up: list[str] = []
    started: list[str] = []

    def tool_with_cleanup(name: str, cleanup_seconds: float) -> Any:
        @tool(name=name)
        async def look_up() -> str:
            """Look something up, tidying up when cancelled."""
            started.append(name)
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(cleanup_seconds)
                cleaned_up.append(name)
                raise
            return "done"

        return look_up

    tools = [tool_with_cleanup("quick_tidy", 0), tool_with_cleanup("slow_tidy", 0.05)]
    async with RealtimeAgent(client, tools=tools).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "quick_tidy"), _call("call_2", "slow_tidy")))
        await eventually(lambda: len(started) == 2)
        span_exporter.clear()
        await conversation.close()

    assert sorted(cleaned_up) == ["quick_tidy", "slow_tidy"]
    assert _tool_span_outcomes(span_exporter) == {
        "execute_tool quick_tidy": (trace.StatusCode.ERROR, "conversation_closed"),
        "execute_tool slow_tidy": (trace.StatusCode.ERROR, "conversation_closed"),
    }


async def test_closing_during_a_sequential_batch_records_conversation_closed_on_the_running_tool(
    span_exporter: InMemorySpanExporter,
) -> None:
    client = FakeRealtimeClient()
    look_up_a, started_a, _ = _blocking_tool("look_up_a")
    look_up_b, _, _ = _blocking_tool("look_up_b")
    agent = RealtimeAgent(
        client, tools=[look_up_a, look_up_b], function_invocation_configuration={"allow_concurrent_invocation": False}
    )

    async with agent.start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "look_up_a"), _call("call_2", "look_up_b")))
        await started_a.wait()
        span_exporter.clear()
        await conversation.close()
        updates = [update async for update in conversation]

    assert _tool_span_outcomes(span_exporter) == {
        "execute_tool look_up_a": (trace.StatusCode.ERROR, "conversation_closed"),
    }
    settled = [content for update in updates for content in update.contents if content.type == "function_result"]
    assert [(content.call_id, content.exception) for content in settled] == [
        ("call_1", "conversation_closed"),
        ("call_2", "conversation_closed"),
    ]


async def test_a_tool_that_closes_the_conversation_settles_as_conversation_closed() -> None:
    client = FakeRealtimeClient()

    @tool
    async def hang_up(ctx: FunctionInvocationContext) -> str:
        """End the call."""
        assert ctx.realtime_conversation is not None
        await ctx.realtime_conversation.close()
        return "never reached"

    async with RealtimeAgent(client, tools=[hang_up]).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "hang_up")))
        updates = await asyncio.wait_for(_read_all(conversation), 1)

    settled = [content for update in updates for content in update.contents if content.type == "function_result"]
    assert [(content.call_id, content.exception) for content in settled] == [("call_1", "conversation_closed")]
    assert client.session.close_count == 1


async def test_close_drops_a_pending_follow_up() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
        client.session.emit(
            event("speech_started", item_id="item_9", audio_start_ms=100),
            *_response("resp_1", _call("call_1", "get_weather", city="Paris")),
        )
        await eventually(lambda: bool(_tool_messages(client.session)))
        await conversation.close()
        client.session.emit(event("speech_stopped", item_id="item_9", audio_end_ms=900))
        await asyncio.sleep(0.01)

    assert client.session.calls == []


async def test_close_lets_a_cancelled_follow_up_finish_cleaning_up() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
        client.session.hold("create_response")
        client.session.cleanup_delays["create_response"] = 0.05
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: bool(client.session.calls))

        await conversation.close()

        assert client.session.calls == [("create_response", None), ("cleaned up", "create_response")]


async def test_a_batch_that_finishes_while_closing_requests_no_follow_up() -> None:
    client = FakeRealtimeClient()
    look_up, started, release = _blocking_tool()
    # Entered without `async with`, so a close that hangs fails the test rather than blocking it.
    conversation = RealtimeAgent(client, tools=[look_up]).start_conversation()
    await conversation.__aenter__()
    client.session.emit(
        event("speech_started", item_id="item_9", audio_start_ms=100),
        *_response("resp_1", _call("call_1", "look_up")),
    )
    await started.wait()
    session_closed = client.session.hold("close")
    closing = asyncio.create_task(conversation.close())
    await eventually(lambda: client.session.close_count == 1)
    release.set()
    await asyncio.sleep(0.01)  # the batch finishes while the session is still closing
    session_closed.set()

    await asyncio.wait_for(closing, 1)

    assert client.session.calls == []
    settled = [
        content
        for update in await _read_all(conversation)
        for content in update.contents
        if content.type == "function_result"
    ]
    assert [(content.call_id, content.result) for content in settled] == [("call_1", "look_up done")]
    assert _tool_messages(client.session) == []


async def test_a_result_send_that_fails_as_the_conversation_closes_does_not_fail_it() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
        release_send = client.session.hold("send")
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: client.session.send_attempts == 1)
        session_closed = client.session.hold("close")
        closing = asyncio.create_task(conversation.close())
        await eventually(lambda: client.session.close_count == 1)
        client.session.send_error = RealtimeConnectionException("The socket is closing.")
        release_send.set()
        await asyncio.sleep(0.01)
        session_closed.set()
        await asyncio.wait_for(closing, 1)

        updates = await _read_all(conversation)

    settled = [content for update in updates for content in update.contents if content.type == "function_result"]
    assert [(content.call_id, content.result) for content in settled] == [("call_1", "Sunny in Paris")]
    assert _tool_messages(client.session) == []


async def test_results_of_calls_that_never_ran_are_not_sent_once_closed(caplog: pytest.LogCaptureFixture) -> None:
    client = FakeRealtimeClient()
    caplog.set_level(logging.DEBUG, logger="agent_framework")

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
        release_send = client.session.hold("send")
        app_send = asyncio.create_task(conversation.send("Hi", create_response=False))
        await eventually(lambda: client.session.send_attempts == 1)
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris"), status="cancelled"))
        await _next_function_result(conversation)

        await conversation.close()
        release_send.set()
        await asyncio.wait_for(app_send, 1)
        await asyncio.sleep(0.01)

    assert _tool_messages(client.session) == []
    assert "never ran" not in caplog.text  # close ended the task sending them, rather than leaving it to fail later


async def test_a_chat_runs_tool_context_has_no_realtime_conversation(chat_client_base: MockBaseChatClient) -> None:
    seen: list[object] = []

    @tool
    def inspect_conversation(ctx: FunctionInvocationContext) -> str:
        """Record the conversation the call belongs to."""
        seen.append(ctx.realtime_conversation)
        return "seen"

    chat_client_base.run_responses = [
        ChatResponse(
            messages=[
                Message(
                    role="assistant",
                    contents=[Content.from_function_call(call_id="call_1", name="inspect_conversation", arguments={})],
                )
            ]
        ),
        ChatResponse(messages=[Message(role="assistant", contents=["done"])]),
    ]
    await Agent(client=chat_client_base, tools=[inspect_conversation]).run("Check")

    assert seen == [None]


# endregion


class TestVerification:
    async def test_the_session_ending_during_a_batch_settles_its_calls(self) -> None:
        client = FakeRealtimeClient()
        look_up, started, _ = _blocking_tool()

        async with RealtimeAgent(client, tools=[look_up]).start_conversation() as conversation:
            client.session.emit(*_response("resp_1", _call("call_1", "look_up")))
            await started.wait()
            client.session.end()
            updates = await asyncio.wait_for(_read_all(conversation), 1)

        settled = [content for update in updates for content in update.contents if content.type == "function_result"]
        assert [(content.call_id, content.exception) for content in settled] == [("call_1", "conversation_closed")]

    async def test_merged_follow_ups_turn_tools_off_if_any_asked_to(self) -> None:
        client = FakeRealtimeClient()
        release = asyncio.Event()

        @tool
        async def broken() -> str:
            """Fail once released."""
            await release.wait()
            raise RuntimeError("down")

        agent = RealtimeAgent(
            client,
            tools=[get_weather, broken],
            function_invocation_configuration={"max_consecutive_errors_per_request": 1},
        )
        async with agent.start_conversation():
            client.session.emit(
                event("response_created", response_id="resp_x"),
                *_response("resp_1", _call("call_1", "broken")),
                *_response("resp_2", _call("call_2", "get_weather", response_id="resp_2", city="Paris")),
            )
            await eventually(lambda: len(_tool_messages(client.session)) == 1)
            release.set()
            await eventually(lambda: len(_tool_messages(client.session)) == 2)
            client.session.emit(event("response_done", response_id="resp_x", status="completed"))
            await eventually(lambda: bool(client.session.calls))
            await asyncio.sleep(0.01)

        assert client.session.calls == [("create_response", {"tool_choice": "none"})]

    async def test_tools_added_through_the_context_resolve_later_calls(self) -> None:
        client = FakeRealtimeClient()

        @tool
        def get_time() -> str:
            """Get the time."""
            return "noon"

        @tool
        def offer_time(ctx: FunctionInvocationContext) -> str:
            """Start offering the time tool."""
            ctx.add_tools([get_time])
            return "offered"

        async with RealtimeAgent(client, tools=[offer_time]).start_conversation():
            client.session.emit(*_response("resp_1", _call("call_1", "offer_time")))
            await eventually(lambda: bool(_tool_messages(client.session)))
            client.session.emit(*_response("resp_2", _call("call_2", "get_time", response_id="resp_2")))
            await eventually(lambda: len(_tool_messages(client.session)) == 2)

        assert _results(client.session) == [("call_1", "offered"), ("call_2", "noon")]
