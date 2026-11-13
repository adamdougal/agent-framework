# Copyright (c) Microsoft. All rights reserved.

"""Handing a live realtime conversation off from one agent to another, without reconnecting."""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import pytest

from agent_framework import (
    AgentContext,
    AgentMiddleware,
    AgentSession,
    Content,
    FunctionInvocationContext,
    FunctionMiddleware,
    Message,
    MiddlewareBundle,
    RealtimeAgent,
    RealtimeEvent,
    RealtimeOptions,
    function_middleware,
    tool,
)
from agent_framework.exceptions import (
    MiddlewareException,
    RealtimeControlException,
    RealtimeControlTimeoutException,
    RealtimeConversationClosedException,
)

from .realtime_helpers import FakeRealtimeClient, FakeRealtimeSession, event, eventually, next_update


@tool
def greet() -> str:
    """Greet the caller."""
    return "Hello"


@tool
def get_balance() -> str:
    """Get the caller's balance."""
    return "£42"


@tool
def get_time() -> str:
    """Get the time."""
    return "noon"


@tool
def escalate() -> str:
    """Pass the caller to a supervisor."""
    return "escalated"


def _receptionist(client: FakeRealtimeClient, *, middleware: Sequence[Any] = ()) -> RealtimeAgent:
    default_options: RealtimeOptions = {"voice": "alloy", "tool_choice": "required"}
    return RealtimeAgent(
        client,
        name="receptionist",
        instructions="Greet the caller.",
        tools=[greet],
        default_options=default_options,
        middleware=list(middleware),
    )


def _billing(client: FakeRealtimeClient, *, middleware: Sequence[Any] = ()) -> RealtimeAgent:
    default_options: RealtimeOptions = {"voice": "verse", "tool_choice": "required"}
    return RealtimeAgent(
        client,
        name="billing",
        instructions="Answer billing questions.",
        tools=[get_balance],
        default_options=default_options,
        middleware=list(middleware),
    )


def _recorder(label: str, seen: list[tuple[str, str]]) -> Any:
    @function_middleware
    async def record(context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        seen.append((label, context.function.name))
        await call_next()

    return record


def _call(call_id: str, name: str, *, response_id: str = "resp_1") -> RealtimeEvent:
    return event(
        "function_call",
        content=Content.from_function_call(call_id=call_id, name=name, arguments={}),
        response_id=response_id,
        item_id=f"item_{call_id}",
    )


def _response(response_id: str, *calls: RealtimeEvent) -> list[RealtimeEvent]:
    return [
        event("response_created", response_id=response_id),
        *calls,
        event("response_done", response_id=response_id, status="completed"),
    ]


def _results(session: FakeRealtimeSession) -> list[tuple[str | None, Any]]:
    return [
        (content.call_id, content.result)
        for message in session.sent
        if isinstance(message, Message) and message.role == "tool"
        for content in message.contents
    ]


# region driving


async def test_handoff_sends_the_targets_instructions_voice_tool_choice_and_tools() -> None:
    client = FakeRealtimeClient()
    billing = _billing(client)

    async with _receptionist(client).start_conversation() as conversation:
        await conversation.handoff(billing)

    assert client.session.calls == [
        (
            "update_session",
            {"instructions": "Answer billing questions.", "voice": "verse", "tool_choice": "required"},
            [get_balance],
        )
    ]


async def test_handoff_makes_the_target_the_active_agent() -> None:
    client = FakeRealtimeClient()
    receptionist, billing = _receptionist(client), _billing(client)

    async with receptionist.start_conversation() as conversation:
        assert conversation.active_agent is receptionist
        await conversation.handoff(billing)

        assert conversation.active_agent is billing


async def test_handoff_streams_handoff_completed_naming_source_and_target() -> None:
    client = FakeRealtimeClient()

    async with _receptionist(client).start_conversation() as conversation:
        await conversation.handoff(_billing(client))
        update = await next_update(conversation)

    [content] = update.contents
    assert content.type == "realtime_event"
    assert content.kind == "handoff_completed"
    assert content.additional_properties["core"] == {"source": "receptionist", "target": "billing"}


async def test_handoff_completed_names_an_agent_without_a_name_by_its_id() -> None:
    client = FakeRealtimeClient()

    async with _receptionist(client).start_conversation() as conversation:
        default_options: RealtimeOptions = {"voice": "verse", "tool_choice": "auto"}
        await conversation.handoff(RealtimeAgent(client, id="agent-2", default_options=default_options))
        update = await next_update(conversation)

    assert update.contents[0].additional_properties["core"]["target"] == "agent-2"


async def test_handoff_appends_the_start_options_instructions_to_the_targets() -> None:
    client = FakeRealtimeClient()

    async with _receptionist(client).start_conversation(options={"instructions": "Be brief."}) as conversation:
        await conversation.handoff(_billing(client))

    [(_, options, _)] = client.session.calls
    assert options["instructions"] == "Answer billing questions.\nBe brief."


async def test_handoff_tools_include_the_start_tools() -> None:
    client = FakeRealtimeClient()

    async with _receptionist(client).start_conversation(tools=[get_time]) as conversation:
        await conversation.handoff(_billing(client))

    [(_, _, tools)] = client.session.calls
    assert tools == [get_balance, get_time]


async def test_handoff_tools_add_tools_until_the_next_handoff() -> None:
    client = FakeRealtimeClient()
    receptionist = _receptionist(client)

    async with receptionist.start_conversation() as conversation:
        await conversation.handoff(_billing(client), tools=[escalate])
        await conversation.handoff(receptionist)

    [(_, _, billing_tools), (_, _, receptionist_tools)] = client.session.calls
    assert billing_tools == [get_balance, escalate]
    assert receptionist_tools == [greet]


async def test_a_voice_or_tool_choice_in_the_start_options_holds_across_handoffs() -> None:
    client = FakeRealtimeClient()

    async with _receptionist(client).start_conversation(
        options={"voice": "sage", "tool_choice": "none"}
    ) as conversation:
        await conversation.handoff(_billing(client))

    [(_, options, _)] = client.session.calls
    assert (options["voice"], options["tool_choice"]) == ("sage", "none")


async def test_a_target_without_a_tool_choice_gets_auto() -> None:
    client = FakeRealtimeClient()
    default_options: RealtimeOptions = {"voice": "verse"}
    billing = RealtimeAgent(client, name="billing", default_options=default_options)

    async with _receptionist(client).start_conversation() as conversation:
        await conversation.handoff(billing)

    [(_, options, _)] = client.session.calls
    assert options["tool_choice"] == "auto"


async def test_a_handoff_leaves_voice_out_when_nothing_sets_one() -> None:
    client = FakeRealtimeClient()

    async with _receptionist(client).start_conversation() as conversation:
        await conversation.handoff(RealtimeAgent(client, name="billing"))

    [(_, options, _)] = client.session.calls
    assert "voice" not in options


async def test_a_handoff_sends_empty_instructions_when_nothing_sets_any() -> None:
    client = FakeRealtimeClient()

    async with _receptionist(client).start_conversation() as conversation:
        await conversation.handoff(RealtimeAgent(client, name="billing"))

    [(_, options, _)] = client.session.calls
    assert options["instructions"] == ""


async def test_after_a_handoff_the_targets_tools_run_and_the_sources_do_not() -> None:
    client = FakeRealtimeClient()

    async with _receptionist(client).start_conversation() as conversation:
        await conversation.handoff(_billing(client))
        client.session.emit(*_response("resp_1", _call("call_1", "get_balance"), _call("call_2", "greet")))
        await eventually(lambda: bool(_results(client.session)))

    [(_, balance), (_, greeting)] = _results(client.session)
    assert balance == "£42"
    assert "not found" in greeting


async def test_a_target_on_another_client_raises_value_error_before_sending() -> None:
    client = FakeRealtimeClient()
    receptionist = _receptionist(client)

    async with receptionist.start_conversation() as conversation:
        with pytest.raises(ValueError):
            await conversation.handoff(_billing(FakeRealtimeClient()))

        assert conversation.active_agent is receptionist
    assert client.session.calls == []


async def test_duplicate_tool_names_raise_value_error_before_sending() -> None:
    client = FakeRealtimeClient()

    @tool(name="get_balance")
    def other_balance() -> str:
        """Get the balance from another ledger."""
        return "£0"

    async with _receptionist(client).start_conversation() as conversation:
        with pytest.raises(ValueError):
            await conversation.handoff(_billing(client), tools=[other_balance])

    assert client.session.calls == []


async def test_responses_created_after_a_handoff_are_credited_to_the_target() -> None:
    client = FakeRealtimeClient()
    billing = _billing(client)

    async with _receptionist(client).start_conversation() as conversation:
        await conversation.handoff(billing)
        await next_update(conversation)  # handoff_completed
        client.session.emit(
            event("response_created", response_id="resp_1"),
            event(
                "output_text",
                content=Content.from_text("Your balance is £42."),
                response_id="resp_1",
                item_id="item_1",
                content_index=0,
            ),
        )
        await next_update(conversation)
        update = await next_update(conversation)

    assert (update.author_name, update.agent_id) == ("billing", billing.id)


async def test_a_call_runs_through_the_function_middleware_of_the_agent_active_when_it_arrived() -> None:
    client = FakeRealtimeClient()
    seen: list[tuple[str, str]] = []
    receptionist = _receptionist(client, middleware=[_recorder("receptionist", seen)])
    billing = _billing(client, middleware=[_recorder("billing", seen)])

    async with receptionist.start_conversation() as conversation:
        client.session.emit(*_response("resp_greet", _call("call_1", "greet", response_id="resp_greet")))
        await eventually(lambda: len(_results(client.session)) == 1)
        await conversation.handoff(billing)
        client.session.emit(*_response("resp_balance", _call("call_2", "get_balance", response_id="resp_balance")))
        await eventually(lambda: len(_results(client.session)) == 2)

    assert seen == [("receptionist", "greet"), ("billing", "get_balance")]


async def test_start_middleware_wraps_every_call_inside_the_participants() -> None:
    client = FakeRealtimeClient()
    seen: list[tuple[str, str]] = []
    billing = _billing(client, middleware=[_recorder("billing", seen)])

    async with _receptionist(client).start_conversation(middleware=[_recorder("start", seen)]) as conversation:
        await conversation.handoff(billing)
        client.session.emit(*_response("resp_balance", _call("call_1", "get_balance", response_id="resp_balance")))
        await eventually(lambda: bool(_results(client.session)))

    assert seen == [("billing", "get_balance"), ("start", "get_balance")]


async def test_a_targets_stateful_middleware_gets_a_conversation_local_session() -> None:
    client = FakeRealtimeClient()
    sessions: list[AgentSession | None] = []

    class SessionStateMiddleware(FunctionMiddleware):
        _requires_session_state = True

        async def process(self, context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
            sessions.append(context.session)
            await call_next()

    async with _receptionist(client).start_conversation() as conversation:
        await conversation.handoff(_billing(client, middleware=[SessionStateMiddleware()]))
        client.session.emit(*_response("resp_balance", _call("call_1", "get_balance", response_id="resp_balance")))
        await eventually(lambda: bool(_results(client.session)))

    [session] = sessions
    assert isinstance(session, AgentSession)


async def test_calls_of_one_response_split_by_a_handoff_run_through_each_agents_middleware() -> None:
    client = FakeRealtimeClient()
    seen: list[tuple[str, str]] = []
    billing = _billing(client, middleware=[_recorder("billing", seen)])

    async with _receptionist(client, middleware=[_recorder("receptionist", seen)]).start_conversation() as conversation:
        client.session.emit(event("response_created", response_id="resp_1"), _call("call_1", "greet"))
        await next_update(conversation)
        await next_update(conversation)  # the call has arrived
        await conversation.handoff(billing)
        client.session.emit(
            _call("call_2", "get_balance"), event("response_done", response_id="resp_1", status="completed")
        )
        await eventually(lambda: len(_results(client.session)) == 2)

    assert sorted(seen) == [("billing", "get_balance"), ("receptionist", "greet")]


async def test_a_target_whose_middleware_bundle_has_agent_middleware_is_refused_before_sending() -> None:
    client = FakeRealtimeClient()

    class AuditMiddleware(AgentMiddleware):
        async def process(self, context: AgentContext, call_next: Callable[[], Awaitable[None]]) -> None:
            await call_next()

    bundle = MiddlewareBundle([AuditMiddleware(), _recorder("billing", [])])

    async with _receptionist(client).start_conversation() as conversation:
        with pytest.raises(MiddlewareException):
            await conversation.handoff(_billing(client, middleware=[bundle]))

    assert client.session.calls == []


# endregion


class TestVerification:
    async def test_a_handoff_ignores_the_targets_other_default_options(self) -> None:
        client = FakeRealtimeClient()
        default_options: RealtimeOptions = {"voice": "verse", "temperature": 0.2}
        billing = RealtimeAgent(client, name="billing", default_options=default_options)

        async with _receptionist(client).start_conversation() as conversation:
            await conversation.handoff(billing)

        [(_, options, _)] = client.session.calls
        assert "temperature" not in options

    async def test_handing_back_restores_the_starting_agents_instructions_voice_tool_choice_and_tools(self) -> None:
        client = FakeRealtimeClient()
        receptionist = _receptionist(client)

        async with receptionist.start_conversation() as conversation:
            await conversation.handoff(_billing(client))
            await conversation.handoff(receptionist)

        [_, (_, options, tools)] = client.session.calls
        assert options == {"instructions": "Greet the caller.", "voice": "alloy", "tool_choice": "required"}
        assert tools == [greet]

    async def test_a_rejected_handoff_raises_and_keeps_the_previous_agent_and_tools(self) -> None:
        client = FakeRealtimeClient()
        receptionist = _receptionist(client)

        async with receptionist.start_conversation() as conversation:
            client.session.update_session_error = RealtimeControlException("Invalid voice.", code="invalid_value")
            with pytest.raises(RealtimeControlException):
                await conversation.handoff(_billing(client))
            active = conversation.active_agent
            client.session.emit(*_response("resp_1", _call("call_1", "greet"), _call("call_2", "get_balance")))
            await eventually(lambda: len(_results(client.session)) == 2)

        updates = [update async for update in conversation]
        [(_, greeting), (_, balance)] = _results(client.session)
        assert active is receptionist
        assert greeting == "Hello"
        assert "not found" in balance
        assert not any(
            content.type == "realtime_event" and content.kind == "handoff_completed"
            for update in updates
            for content in update.contents
        )

    async def test_an_unacknowledged_handoff_raises_control_timeout_and_keeps_the_previous_agent(self) -> None:
        client = FakeRealtimeClient()
        receptionist = _receptionist(client)

        async with receptionist.start_conversation(control_timeout=0.05) as conversation:
            client.session.hold("update_session")
            with pytest.raises(RealtimeControlTimeoutException):
                await asyncio.wait_for(conversation.handoff(_billing(client)), 1)

            assert conversation.active_agent is receptionist

    async def test_handoff_waits_behind_another_control_call(self) -> None:
        client = FakeRealtimeClient()

        async with _receptionist(client).start_conversation() as conversation:
            release = client.session.hold("create_response")
            creating = asyncio.create_task(conversation.create_response())
            await eventually(lambda: bool(client.session.calls))
            handing_off = asyncio.create_task(conversation.handoff(_billing(client)))
            await asyncio.sleep(0.01)
            assert [call[0] for call in client.session.calls] == ["create_response"]

            release.set()
            await creating
            await handing_off

        assert [call[0] for call in client.session.calls] == ["create_response", "update_session"]

    async def test_handoff_after_close_raises_conversation_closed(self) -> None:
        client = FakeRealtimeClient()

        async with _receptionist(client).start_conversation() as conversation:
            await conversation.close()

            with pytest.raises(RealtimeConversationClosedException):
                await conversation.handoff(_billing(client))

        assert client.session.calls == []

    async def test_the_response_that_made_a_handoff_call_and_its_result_stay_with_the_source(self) -> None:
        client = FakeRealtimeClient()
        billing = _billing(client)

        @tool
        async def transfer(ctx: FunctionInvocationContext) -> str:
            """Transfer the caller to billing."""
            assert ctx.realtime_conversation is not None
            await ctx.realtime_conversation.handoff(billing)
            return "transferred"

        receptionist = RealtimeAgent(client, name="receptionist", tools=[transfer])
        async with receptionist.start_conversation() as conversation:
            client.session.emit(*_response("resp_1", _call("call_1", "transfer")))
            await eventually(lambda: bool(_results(client.session)))

        updates = [update async for update in conversation]
        made_the_call = [update for update in updates if update.response_id == "resp_1" and update.role is not None]
        assert made_the_call
        assert {update.author_name for update in made_the_call} == {"receptionist"}

    async def test_a_handoff_a_tool_makes_lands_before_the_results_and_the_follow_up(self) -> None:
        client = FakeRealtimeClient()
        billing = _billing(client)

        @tool
        async def transfer(ctx: FunctionInvocationContext) -> str:
            """Transfer the caller to billing."""
            assert ctx.realtime_conversation is not None
            await ctx.realtime_conversation.handoff(billing)
            return "transferred"

        receptionist = RealtimeAgent(client, name="receptionist", tools=[transfer])
        async with receptionist.start_conversation():
            client.session.emit(*_response("resp_1", _call("call_1", "transfer")))
            await eventually(lambda: len(client.session.calls) == 2)

        [(handoff, _, tools), follow_up] = client.session.calls
        assert (handoff, tools) == ("update_session", [get_balance])
        assert follow_up == ("create_response", {"tool_choice": "auto"})

    async def test_a_tool_chain_that_spans_a_handoff_counts_as_one_chain(self) -> None:
        client = FakeRealtimeClient()
        billing = _billing(client)

        @tool
        async def transfer(ctx: FunctionInvocationContext) -> str:
            """Transfer the caller to billing."""
            assert ctx.realtime_conversation is not None
            await ctx.realtime_conversation.handoff(billing)
            return "transferred"

        receptionist = RealtimeAgent(
            client, name="receptionist", tools=[transfer], function_invocation_configuration={"max_iterations": 2}
        )
        async with receptionist.start_conversation():
            client.session.emit(*_response("resp_caller", _call("call_1", "transfer", response_id="resp_caller")))
            await eventually(lambda: len(client.session.calls) == 2)
            # The provider answers the first follow-up, which the fake session numbered resp_1, with another call.
            client.session.emit(*_response("resp_1", _call("call_2", "get_balance")))
            await eventually(lambda: len(client.session.calls) == 3)

        assert client.session.calls[-1] == ("create_response", {"tool_choice": "none"})
