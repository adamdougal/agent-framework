# Copyright (c) Microsoft. All rights reserved.

"""RealtimeHandoffBuilder: a handoff topology of realtime agents that share one live conversation."""

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any, cast

import pytest
from agent_framework import (
    Agent,
    AgentContext,
    AgentMiddleware,
    Content,
    FunctionInvocationContext,
    FunctionTool,
    InMemoryHistoryProvider,
    Message,
    MiddlewareBundle,
    RealtimeAgent,
    RealtimeEvent,
    RealtimeKind,
    RealtimeOptions,
    ToolTypes,
    function_middleware,
    tool,
)
from agent_framework.exceptions import MiddlewareException, RealtimeControlException
from agent_framework.orchestrations import RealtimeHandoffBuilder


class FakeRealtimeSession:
    """A realtime session whose events the test emits, and which records the control calls it gets."""

    def __init__(self, options: Any, tools: Sequence[ToolTypes] | None) -> None:
        self.start_options: dict[str, Any] = dict(options or {})
        self.options: dict[str, Any] = dict(options or {})
        self.start_tools = list(tools or [])
        self.sent: list[RealtimeEvent | Message] = []
        self.calls: list[tuple[Any, ...]] = []
        self.update_session_error: BaseException | None = None
        self._events: asyncio.Queue[RealtimeEvent | None] = asyncio.Queue()
        self._responses = 0

    def emit(self, *events: RealtimeEvent) -> None:
        for event in events:
            self._events.put_nowait(event)

    async def send(self, value: RealtimeEvent | Message) -> None:
        self.sent.append(value)

    async def receive(self) -> AsyncIterator[RealtimeEvent]:
        while (event := await self._events.get()) is not None:
            yield event

    async def update_session(self, options: Any, tools: Sequence[ToolTypes] | None) -> None:
        self.calls.append(("update_session", options, tools))
        if self.update_session_error is not None:
            raise self.update_session_error
        self.options.update(options)

    async def create_response(self, options: Any = None) -> str:
        self.calls.append(("create_response", options))
        self._responses += 1
        return f"follow_up_{self._responses}"

    async def close(self) -> None:
        self._events.put_nowait(None)


class FakeRealtimeClient:
    """A realtime client whose sessions are fakes the test drives."""

    def __init__(self) -> None:
        self.sessions: list[FakeRealtimeSession] = []

    @property
    def session(self) -> FakeRealtimeSession:
        return self.sessions[-1]

    async def start_session(self, *, options: Any = None, tools: Sequence[ToolTypes] | None = None) -> Any:
        self.sessions.append(FakeRealtimeSession(options, tools))
        return self.session


@tool
def greet() -> str:
    """Greet the caller."""
    return "Hello"


@tool
def get_balance() -> str:
    """Get the caller's balance."""
    return "£42"


def _receptionist(client: FakeRealtimeClient, **settings: Any) -> RealtimeAgent:
    default_options = cast(RealtimeOptions, {"voice": "alloy", "tool_choice": "required", **settings})
    return RealtimeAgent(
        client,  # type: ignore[arg-type]
        name="receptionist",
        instructions="Greet the caller.",
        tools=[greet],
        default_options=default_options,
    )


def _billing(client: FakeRealtimeClient, **settings: Any) -> RealtimeAgent:
    default_options = cast(RealtimeOptions, {"voice": "verse", **settings})
    return RealtimeAgent(
        client,  # type: ignore[arg-type]
        name="billing",
        instructions="Answer billing questions.",
        tools=[get_balance],
        default_options=default_options,
    )


def _support(client: FakeRealtimeClient) -> RealtimeAgent:
    return RealtimeAgent(client, name="support", instructions="Fix technical problems.")  # type: ignore[arg-type]


def _event(kind: str, *, content: Content | None = None, **core: Any) -> RealtimeEvent:
    return RealtimeEvent(RealtimeKind(kind), content=content, additional_properties={"core": core})


def _call(call_id: str, name: str, *, response_id: str = "resp_1") -> RealtimeEvent:
    return _event(
        "function_call",
        content=Content.from_function_call(call_id=call_id, name=name, arguments={}),
        response_id=response_id,
        item_id=f"item_{call_id}",
    )


def _response(response_id: str, *calls: RealtimeEvent) -> list[RealtimeEvent]:
    return [
        _event("response_created", response_id=response_id),
        *calls,
        _event("response_done", response_id=response_id, status="completed"),
    ]


def _results(session: FakeRealtimeSession) -> list[tuple[str | None, Any]]:
    return [
        (content.call_id, content.result)
        for message in session.sent
        if isinstance(message, Message) and message.role == "tool"
        for content in message.contents
    ]


async def _eventually(condition: Callable[[], bool]) -> None:
    async with asyncio.timeout(1):
        while True:
            if condition():
                return
            await asyncio.sleep(0.001)


def _recorder(label: str, seen: list[str]) -> Any:
    @function_middleware
    async def record(context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        seen.append(label)
        await call_next()

    return record


# region driving


async def test_build_returns_a_realtime_agent_whose_conversations_start_with_the_start_agent_active() -> None:
    client = FakeRealtimeClient()
    receptionist, billing = _receptionist(client), _billing(client)

    agent = RealtimeHandoffBuilder(participants=[receptionist, billing]).with_start_agent(receptionist).build()

    assert isinstance(agent, RealtimeAgent)
    async with agent.start_conversation() as conversation:
        assert conversation.active_agent is receptionist


async def test_the_session_opens_with_the_start_agents_instructions_voice_and_tool_choice() -> None:
    client = FakeRealtimeClient()
    receptionist = _receptionist(client)
    agent = RealtimeHandoffBuilder(participants=[receptionist, _billing(client)]).with_start_agent(receptionist).build()

    async with agent.start_conversation():
        options = client.session.start_options

    assert (options["instructions"], options["voice"], options["tool_choice"]) == (
        "Greet the caller.",
        "alloy",
        "required",
    )


async def test_by_default_the_start_agent_gets_a_handoff_tool_for_every_other_participant() -> None:
    client = FakeRealtimeClient()
    receptionist = _receptionist(client)
    agent = (
        RealtimeHandoffBuilder(participants=[receptionist, _billing(client), _support(client)])
        .with_start_agent(receptionist)
        .build()
    )

    async with agent.start_conversation():
        tools = client.session.start_tools

    assert [cast(FunctionTool, declared).name for declared in tools] == [
        "greet",
        "handoff_to_billing",
        "handoff_to_support",
    ]


async def test_explicit_handoffs_limit_a_participants_handoff_tools() -> None:
    client = FakeRealtimeClient()
    receptionist, billing = _receptionist(client), _billing(client)
    agent = (
        RealtimeHandoffBuilder(participants=[receptionist, billing, _support(client)])
        .with_start_agent(receptionist)
        .add_handoff(receptionist, [billing])
        .build()
    )

    async with agent.start_conversation():
        tools = client.session.start_tools

    assert [cast(FunctionTool, declared).name for declared in tools] == ["greet", "handoff_to_billing"]


async def test_calling_a_handoff_tool_hands_off_and_returns_handoff_to() -> None:
    client = FakeRealtimeClient()
    receptionist, billing = _receptionist(client), _billing(client)
    agent = RealtimeHandoffBuilder(participants=[receptionist, billing]).with_start_agent(receptionist).build()

    async with agent.start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "handoff_to_billing")))
        await _eventually(lambda: bool(_results(client.session)))
        active = conversation.active_agent

    assert active is billing
    [(_, options, _)] = [call for call in client.session.calls if call[0] == "update_session"]
    assert options["instructions"] == "Answer billing questions."
    assert _results(client.session) == [("call_1", '{"handoff_to": "billing"}')]


async def test_a_handoff_tools_description_defaults_to_naming_its_target() -> None:
    client = FakeRealtimeClient()
    receptionist = _receptionist(client)
    agent = RealtimeHandoffBuilder(participants=[receptionist, _billing(client)]).with_start_agent(receptionist).build()

    async with agent.start_conversation():
        [_, handoff_tool] = client.session.start_tools

    assert cast(FunctionTool, handoff_tool).description == "Handoff to the billing agent."


async def test_a_rejected_handoff_returns_a_result_telling_the_model_to_carry_on() -> None:
    client = FakeRealtimeClient()
    receptionist = _receptionist(client)
    agent = RealtimeHandoffBuilder(participants=[receptionist, _billing(client)]).with_start_agent(receptionist).build()

    async with agent.start_conversation() as conversation:
        client.session.update_session_error = RealtimeControlException("Invalid voice.", code="invalid_value")
        client.session.emit(*_response("resp_1", _call("call_1", "handoff_to_billing")))
        await _eventually(lambda: bool(_results(client.session)))
        active = conversation.active_agent

    [result] = [
        content for message in client.session.sent if isinstance(message, Message) for content in message.contents
    ]
    assert active is receptionist
    assert result.exception is None
    assert "handoff to billing" in str(result.result).lower()
    assert "carry on" in str(result.result).lower()


async def test_of_two_handoffs_in_one_response_the_first_wins() -> None:
    client = FakeRealtimeClient()
    receptionist = _receptionist(client)
    agent = (
        RealtimeHandoffBuilder(participants=[receptionist, _billing(client), _support(client)])
        .with_start_agent(receptionist)
        .build()
    )

    async with agent.start_conversation():
        client.session.emit(
            *_response("resp_1", _call("call_1", "handoff_to_billing"), _call("call_2", "handoff_to_support"))
        )
        await _eventually(lambda: len(_results(client.session)) == 2)

    updates = [call for call in client.session.calls if call[0] == "update_session"]
    [(_, first), (_, second)] = _results(client.session)
    assert len(updates) == 1
    assert first == '{"handoff_to": "billing"}'
    assert "carry on" in second.lower()


async def test_handoff_on_a_built_agents_conversation_accepts_only_participants() -> None:
    client = FakeRealtimeClient()
    receptionist = _receptionist(client)
    agent = RealtimeHandoffBuilder(participants=[receptionist, _billing(client)]).with_start_agent(receptionist).build()

    async with agent.start_conversation() as conversation:
        with pytest.raises(ValueError):
            await conversation.handoff(_support(client))

    assert client.session.calls == []


async def test_the_built_agents_other_default_options_apply_at_start() -> None:
    client = FakeRealtimeClient()
    receptionist = _receptionist(client, temperature=0.2)
    call_options: RealtimeOptions = {"max_tokens": 100}
    agent = (
        RealtimeHandoffBuilder(participants=[receptionist, _billing(client)], default_options=call_options)
        .with_start_agent(receptionist)
        .build()
    )

    async with agent.start_conversation():
        options = client.session.start_options

    assert options["max_tokens"] == 100
    assert "temperature" not in options


async def test_the_built_agents_instructions_are_appended_to_every_participants() -> None:
    client = FakeRealtimeClient()
    receptionist, billing = _receptionist(client), _billing(client)
    call_options: RealtimeOptions = {"instructions": "Never announce a transfer."}
    agent = (
        RealtimeHandoffBuilder(participants=[receptionist, billing], default_options=call_options)
        .with_start_agent(receptionist)
        .build()
    )

    async with agent.start_conversation() as conversation:
        await conversation.handoff(billing)

    [(_, update, _)] = client.session.calls
    assert client.session.start_options["instructions"] == "Greet the caller.\nNever announce a transfer."
    assert update["instructions"] == "Answer billing questions.\nNever announce a transfer."


async def test_the_built_agents_voice_and_tool_choice_apply_to_a_participant_without_its_own() -> None:
    client = FakeRealtimeClient()
    receptionist, support = _receptionist(client), _support(client)
    call_options: RealtimeOptions = {"voice": "sage", "tool_choice": "none"}
    agent = (
        RealtimeHandoffBuilder(participants=[receptionist, support], default_options=call_options)
        .with_start_agent(receptionist)
        .build()
    )

    async with agent.start_conversation() as conversation:
        await conversation.handoff(support)

    [(_, update, _)] = client.session.calls
    started = client.session.start_options
    assert (started["voice"], started["tool_choice"]) == ("alloy", "required")
    assert (update["voice"], update["tool_choice"]) == ("sage", "none")


async def test_the_built_agents_function_middleware_wraps_every_call_outside_the_participants() -> None:
    client = FakeRealtimeClient()
    seen: list[str] = []
    receptionist = _receptionist(client)
    billing = RealtimeAgent(
        client,  # type: ignore[arg-type]
        name="billing",
        tools=[get_balance],
        middleware=[_recorder("billing", seen)],
    )
    agent = (
        RealtimeHandoffBuilder(participants=[receptionist, billing], middleware=[_recorder("built", seen)])
        .with_start_agent(receptionist)
        .build()
    )

    async with agent.start_conversation() as conversation:
        await conversation.handoff(billing)
        client.session.emit(*_response("resp_1", _call("call_1", "get_balance")))
        await _eventually(lambda: bool(_results(client.session)))

    assert seen == ["built", "billing"]


async def test_the_built_agents_function_invocation_configuration_limits_every_participants_tool_chains() -> None:
    client = FakeRealtimeClient()
    receptionist = _receptionist(client)
    agent = (
        RealtimeHandoffBuilder(
            participants=[receptionist, _billing(client)], function_invocation_configuration={"max_iterations": 1}
        )
        .with_start_agent(receptionist)
        .build()
    )

    async with agent.start_conversation():
        client.session.emit(*_response("resp_1", _call("call_1", "handoff_to_billing")))
        await _eventually(lambda: any(call[0] == "create_response" for call in client.session.calls))

    assert client.session.calls[-1] == ("create_response", {"tool_choice": "none"})


def test_participants_must_be_realtime_agents() -> None:
    chat_agent = Agent(client=cast(Any, object()), name="chat")

    with pytest.raises(TypeError):
        RealtimeHandoffBuilder().participants([cast(Any, chat_agent)])


def test_a_built_agent_cannot_be_a_participant() -> None:
    client = FakeRealtimeClient()
    receptionist = _receptionist(client)
    built = RealtimeHandoffBuilder(participants=[receptionist, _billing(client)]).with_start_agent(receptionist).build()

    with pytest.raises(TypeError):
        RealtimeHandoffBuilder().participants([built, _support(client)])


def test_build_rejects_participants_on_different_clients() -> None:
    receptionist = _receptionist(FakeRealtimeClient())
    builder = RealtimeHandoffBuilder(participants=[receptionist, _billing(FakeRealtimeClient())])

    with pytest.raises(ValueError):
        builder.with_start_agent(receptionist).build()


def test_build_rejects_a_participant_tool_named_like_one_of_its_handoff_tools() -> None:
    client = FakeRealtimeClient()

    @tool(name="handoff_to_billing")
    def transfer() -> str:
        """Transfer the caller to billing by phone."""
        return "transferred"

    receptionist = RealtimeAgent(client, name="receptionist", tools=[transfer])  # type: ignore[arg-type]
    builder = RealtimeHandoffBuilder(participants=[receptionist, _billing(client)]).with_start_agent(receptionist)

    with pytest.raises(ValueError, match="handoff_to_billing"):
        builder.build()


def test_build_warns_about_participant_settings_that_wont_apply(caplog: pytest.LogCaptureFixture) -> None:
    client = FakeRealtimeClient()

    class AuditMiddleware(AgentMiddleware):
        async def process(self, context: AgentContext, call_next: Callable[[], Awaitable[None]]) -> None:
            await call_next()

    billing_options: RealtimeOptions = {"temperature": 0.2}
    billing = RealtimeAgent(
        client,  # type: ignore[arg-type]
        name="billing",
        context_providers=[InMemoryHistoryProvider()],
        function_invocation_configuration={"max_iterations": 2},
        default_options=billing_options,
        middleware=[AuditMiddleware()],
    )
    receptionist = _receptionist(client)

    with caplog.at_level(logging.WARNING):
        RealtimeHandoffBuilder(participants=[receptionist, billing]).with_start_agent(receptionist).build()

    [warning] = [record.getMessage() for record in caplog.records if "billing" in record.getMessage()]
    for setting in ("context_providers", "function_invocation_configuration", "temperature", "middleware"):
        assert setting in warning


def test_build_refuses_a_participant_middleware_bundle_with_agent_middleware() -> None:
    client = FakeRealtimeClient()

    class AuditMiddleware(AgentMiddleware):
        async def process(self, context: AgentContext, call_next: Callable[[], Awaitable[None]]) -> None:
            await call_next()

    receptionist = _receptionist(client)
    billing = RealtimeAgent(
        client,  # type: ignore[arg-type]
        name="billing",
        middleware=[MiddlewareBundle([AuditMiddleware(), _recorder("billing", [])])],
    )

    with pytest.raises(MiddlewareException):
        RealtimeHandoffBuilder(participants=[receptionist, billing]).with_start_agent(receptionist).build()


# endregion


class TestVerification:
    async def test_a_handoff_tools_description_falls_back_to_the_targets(self) -> None:
        client = FakeRealtimeClient()
        receptionist = _receptionist(client)
        billing = RealtimeAgent(client, name="billing", description="Answers billing questions.")  # type: ignore[arg-type]
        agent = RealtimeHandoffBuilder(participants=[receptionist, billing]).with_start_agent(receptionist).build()

        async with agent.start_conversation():
            [_, handoff_tool] = client.session.start_tools

        assert cast(FunctionTool, handoff_tool).description == "Answers billing questions."

    async def test_an_add_handoff_description_wins_over_the_targets(self) -> None:
        client = FakeRealtimeClient()
        receptionist = _receptionist(client)
        billing = RealtimeAgent(client, name="billing", description="Answers billing questions.")  # type: ignore[arg-type]
        agent = (
            RealtimeHandoffBuilder(participants=[receptionist, billing])
            .with_start_agent(receptionist)
            .add_handoff(receptionist, [billing], description="Bills and refunds.")
            .build()
        )

        async with agent.start_conversation():
            [_, handoff_tool] = client.session.start_tools

        assert cast(FunctionTool, handoff_tool).description == "Bills and refunds."

    async def test_a_handoff_installs_the_targets_handoff_tools(self) -> None:
        client = FakeRealtimeClient()
        receptionist, billing = _receptionist(client), _billing(client)
        agent = (
            RealtimeHandoffBuilder(participants=[receptionist, billing, _support(client)])
            .with_start_agent(receptionist)
            .build()
        )

        async with agent.start_conversation() as conversation:
            await conversation.handoff(billing)

        [(_, _, tools)] = client.session.calls
        assert [cast(FunctionTool, declared).name for declared in tools] == [
            "get_balance",
            "handoff_to_receptionist",
            "handoff_to_support",
        ]

    def test_build_does_not_warn_about_settings_a_participant_brings(self, caplog: pytest.LogCaptureFixture) -> None:
        client = FakeRealtimeClient()
        options: RealtimeOptions = {"voice": "alloy", "tool_choice": "required"}
        receptionist = RealtimeAgent(
            client,  # type: ignore[arg-type]
            name="receptionist",
            instructions="Greet the caller.",
            tools=[greet],
            default_options=options,
            middleware=[_recorder("receptionist", [])],
        )

        with caplog.at_level(logging.WARNING):
            RealtimeHandoffBuilder(participants=[receptionist, _billing(client)]).with_start_agent(receptionist).build()

        assert not caplog.records
