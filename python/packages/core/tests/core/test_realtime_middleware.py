# Copyright (c) Microsoft. All rights reserved.

"""Conversation middleware around a realtime agent's live conversations."""

from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from agent_framework import (
    AgentContext,
    AgentResponseUpdate,
    Content,
    ConversationContext,
    ConversationMiddleware,
    FunctionInvocationContext,
    FunctionTool,
    Message,
    MiddlewareBundle,
    MiddlewareException,
    MiddlewareTermination,
    RawRealtimeAgent,
    RealtimeAgent,
    RealtimeEvent,
)
from agent_framework.exceptions import RealtimeConnectionException

from .realtime_helpers import FakeRealtimeClient, event, eventually, next_update
from .realtime_history_helpers import Failing, Recorder, call_response, get_weather, reply, streamed

MiddlewareFunction = Callable[[ConversationContext, Callable[[], Awaitable[None]]], Awaitable[None]]


class SessionStartAudit(ConversationMiddleware):
    """Records how many sessions had started before and after each call_next()."""

    def __init__(self, client: FakeRealtimeClient) -> None:
        self.client = client
        self.starts: list[tuple[int, int]] = []

    async def process(self, context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        before = len(self.client.start_session_calls)
        await call_next()
        self.starts.append((before, len(self.client.start_session_calls)))


def layer(name: str, order: list[str]) -> MiddlewareFunction:
    """Conversation middleware that records entering and leaving its process() under ``name``."""

    async def record(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        order.append(f"{name} in")
        await call_next()
        order.append(f"{name} out")

    return record


def sent_texts(client: FakeRealtimeClient) -> list[str]:
    return [item.text for item in client.session.sent if isinstance(item, Message)]


def tool_results(client: FakeRealtimeClient) -> list[object]:
    return [
        content.result
        for item in client.session.sent
        if isinstance(item, Message)
        for content in item.contents
        if content.type == "function_result"
    ]


async def read_until(conversation: Any, content_type: str) -> list[AgentResponseUpdate]:
    """Read updates up to and including the first holding ``content_type`` content."""
    updates: list[AgentResponseUpdate] = []
    while not updates or all(content.type != content_type for content in updates[-1].contents):
        updates.append(await next_update(conversation))
    return updates


def send_hook(hook: Callable[[Message | RealtimeEvent], Any]) -> MiddlewareFunction:
    """Conversation middleware that registers ``hook`` as a send hook."""

    async def register(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        context.send_hooks.append(hook)
        await call_next()

    return register


def update_hook(hook: Callable[[AgentResponseUpdate], Any]) -> MiddlewareFunction:
    """Conversation middleware that registers ``hook`` as an update hook."""

    async def register(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        context.update_hooks.append(hook)
        await call_next()

    return register


def close_hook(hook: Callable[[Exception | None], Any]) -> MiddlewareFunction:
    """Conversation middleware that registers ``hook`` as a close hook."""

    async def register(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        context.close_hooks.append(hook)
        await call_next()

    return register


# region The chain


async def test_process_runs_once_on_entry_and_call_next_starts_the_session() -> None:
    """Entering a RealtimeAgent's conversation runs its ConversationMiddleware's process() once; no session has started
    before call_next(), and one has started after it."""
    client = FakeRealtimeClient()
    audit = SessionStartAudit(client)

    async with RealtimeAgent(client, middleware=[audit]).start_conversation():
        assert audit.starts == [(0, 1)]


async def test_the_context_names_the_starting_agent_and_holds_the_conversation_once_open() -> None:
    """context.agent is the agent that started the conversation; context.conversation is None before call_next() and
    the entered conversation after it."""
    seen: list[tuple[object, object]] = []

    class Observe(ConversationMiddleware):
        async def process(self, context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
            seen.append((context.agent, context.conversation))
            await call_next()
            seen.append((context.agent, context.conversation))

    agent = RealtimeAgent(FakeRealtimeClient(), middleware=[Observe()])

    async with agent.start_conversation() as conversation:
        assert seen == [(agent, None), (agent, conversation)]


async def test_a_function_annotated_with_conversation_context_runs_as_conversation_middleware() -> None:
    """A plain async function whose first parameter is annotated ConversationContext runs once on entry."""
    client = FakeRealtimeClient()
    opened: list[int] = []

    async def audit(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        await call_next()
        opened.append(len(client.start_session_calls))

    async with RealtimeAgent(client, middleware=[audit]).start_conversation():
        assert opened == [1]


async def test_a_raw_realtime_agent_runs_no_conversation_middleware() -> None:
    """RawRealtimeAgent with conversation middleware starts its session without running the middleware."""
    client = FakeRealtimeClient()
    audit = SessionStartAudit(client)

    async with RawRealtimeAgent(client, middleware=[audit]).start_conversation():
        assert (len(client.start_session_calls), audit.starts) == (1, [])


async def test_the_agents_conversation_middleware_runs_outside_the_conversations() -> None:
    """With middleware on the agent and on start_conversation(middleware=...), process() runs in the order
    ['agent', 'conversation'] and unwinds in the order ['conversation', 'agent']."""
    order: list[str] = []
    agent = RealtimeAgent(FakeRealtimeClient(), middleware=[layer("agent", order)])

    async with agent.start_conversation(middleware=[layer("conversation", order)]):
        assert order == ["agent in", "conversation in", "conversation out", "agent out"]


async def test_a_chain_that_never_calls_call_next_refuses_the_session() -> None:
    """When process() returns without calling call_next(), entering raises MiddlewareTermination and no session
    starts."""
    client = FakeRealtimeClient()

    async def refuse(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        return None

    with pytest.raises(MiddlewareTermination):
        async with RealtimeAgent(client, middleware=[refuse]).start_conversation():
            pass

    assert client.start_session_calls == []


async def test_calling_call_next_twice_raises_runtime_error() -> None:
    """A second call_next() raises RuntimeError, and only one session starts."""
    client = FakeRealtimeClient()

    async def open_twice(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        await call_next()
        await call_next()

    with pytest.raises(RuntimeError, match="once"):
        async with RealtimeAgent(client, middleware=[open_twice]).start_conversation():
            pass

    assert len(client.start_session_calls) == 1


async def test_an_exception_after_call_next_closes_the_session_and_entering_raises_it() -> None:
    """process() raising RuntimeError('audit store down') after call_next() closes the session once, and entering raises
    that RuntimeError."""
    client = FakeRealtimeClient()

    async def audit(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        await call_next()
        raise RuntimeError("audit store down")

    with pytest.raises(RuntimeError, match="audit store down"):
        async with RealtimeAgent(client, middleware=[audit]).start_conversation():
            pass

    assert client.session.close_count == 1


# endregion

# region Send hooks


async def test_a_send_hooks_replacement_reaches_the_session_and_history() -> None:
    """An async send hook that redacts 'My card is 4111 1111 1111 1111' to 'My card is [redacted]' makes the session
    receive, and history record, only the redacted user message."""
    client = FakeRealtimeClient()

    async def redact(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        async def hide_card_numbers(item: Message | RealtimeEvent) -> Message | RealtimeEvent:
            if isinstance(item, Message):
                return Message(item.role, [item.text.replace("4111 1111 1111 1111", "[redacted]")])
            return item

        context.send_hooks.append(hide_card_numbers)
        await call_next()

    async with RealtimeAgent(client, middleware=[redact]).start_conversation() as conversation:
        await conversation.send("My card is 4111 1111 1111 1111", create_response=False)

        assert sent_texts(client) == ["My card is [redacted]"]
        assert [message.text for message in conversation.history] == ["My card is [redacted]"]


async def test_middleware_termination_in_a_send_hook_drops_that_message_while_the_others_go_out() -> None:
    """send(['Hello', 'blocked words']) with a hook raising MiddlewareTermination for 'blocked words' sends only
    'Hello'."""
    client = FakeRealtimeClient()

    def block(item: Message | RealtimeEvent) -> None:
        if isinstance(item, Message) and item.text == "blocked words":
            raise MiddlewareTermination("blocked")

    async with RealtimeAgent(client, middleware=[send_hook(block)]).start_conversation() as conversation:
        await conversation.send(["Hello", "blocked words"], create_response=False)

        assert sent_texts(client) == ["Hello"]


async def test_a_send_whose_messages_are_all_dropped_requests_no_response() -> None:
    """When the send hook drops every message, nothing is sent and no create_response call is made."""
    client = FakeRealtimeClient()

    def block_everything(item: Message | RealtimeEvent) -> None:
        raise MiddlewareTermination("blocked")

    async with RealtimeAgent(client, middleware=[send_hook(block_everything)]).start_conversation() as conversation:
        await conversation.send("blocked words")

        assert (sent_texts(client), client.session.calls) == ([], [])


async def test_a_raw_event_passes_the_send_hooks_and_its_replacement_is_sent_raw() -> None:
    """A send hook replacing a raw input_audio_buffer.clear event with an input_audio_buffer.commit event makes the
    session receive only the commit event, with no response requested."""
    client = FakeRealtimeClient()
    commit = RealtimeEvent("passthrough", raw_representation={"type": "input_audio_buffer.commit"})

    def commit_instead(item: Message | RealtimeEvent) -> RealtimeEvent | None:
        return commit if isinstance(item, RealtimeEvent) else None

    async with RealtimeAgent(client, middleware=[send_hook(commit_instead)]).start_conversation() as conversation:
        await conversation.send(RealtimeEvent("passthrough", raw_representation={"type": "input_audio_buffer.clear"}))

        assert (client.session.sent, client.session.calls) == ([commit], [])


async def test_a_send_hooks_raw_replacement_keeps_its_place_among_the_sends_messages() -> None:
    """send(['first', '[commit]', 'third']) with a hook replacing '[commit]' by a raw input_audio_buffer.commit event
    reaches the session in the order ['first', commit, 'third']."""
    client = FakeRealtimeClient()
    commit = RealtimeEvent("passthrough", raw_representation={"type": "input_audio_buffer.commit"})

    def commit_marker(item: Message | RealtimeEvent) -> RealtimeEvent | None:
        return commit if isinstance(item, Message) and item.text == "[commit]" else None

    async with RealtimeAgent(client, middleware=[send_hook(commit_marker)]).start_conversation() as conversation:
        await conversation.send(["first", "[commit]", "third"], create_response=False)

        sent = [item.text if isinstance(item, Message) else item for item in client.session.sent]
        assert sent == ["first", commit, "third"]


async def test_a_send_hook_failure_closes_the_conversation_then_send_and_iteration_raise_it() -> None:
    """A send hook raising RuntimeError('moderation down') closes the session; send() raises that RuntimeError once
    close finished, and iteration raises it too."""
    client = FakeRealtimeClient()

    def moderate(item: Message | RealtimeEvent) -> None:
        raise RuntimeError("moderation down")

    async with RealtimeAgent(client, middleware=[send_hook(moderate)]).start_conversation() as conversation:
        with pytest.raises(RuntimeError, match="moderation down"):
            await conversation.send("Hello")
        assert client.session.close_count == 1

        with pytest.raises(RuntimeError, match="moderation down"):
            async for _ in conversation:
                pass


# endregion

# region Update hooks


async def test_an_update_hooks_replacement_is_yielded_while_history_keeps_the_original() -> None:
    """A sync update hook replacing the agent's text 'Your PIN is 1234' with 'Your PIN is [redacted]' yields the
    redacted text, while conversation.history keeps 'Your PIN is 1234'."""
    client = FakeRealtimeClient()

    def redact_pin(update: AgentResponseUpdate) -> AgentResponseUpdate | None:
        if "1234" not in update.text:
            return None
        return AgentResponseUpdate(
            contents=[Content.from_text(update.text.replace("1234", "[redacted]"))], role=update.role
        )

    async with RealtimeAgent(client, middleware=[update_hook(redact_pin)]).start_conversation() as conversation:
        client.session.emit(*reply("resp_1", "item_1", "Your PIN is 1234"))
        updates = [await next_update(conversation) for _ in range(4)]

        assert [update.text for update in updates if update.text] == ["Your PIN is [redacted]"]
        assert [message.text for message in conversation.history] == ["Your PIN is 1234"]


async def test_a_dropped_function_call_update_is_not_yielded_and_its_tool_still_runs() -> None:
    """An update hook raising MiddlewareTermination for function_call updates hides the call from iteration, while
    get_weather still runs and its result 'Sunny in Paris' is sent to the session."""
    client = FakeRealtimeClient()

    def hide_calls(update: AgentResponseUpdate) -> None:
        if any(content.type == "function_call" for content in update.contents):
            raise MiddlewareTermination("hidden")

    agent = RealtimeAgent(client, tools=[get_weather], middleware=[update_hook(hide_calls)])
    async with agent.start_conversation() as conversation:
        client.session.emit(*call_response("resp_1", "item_1", "call_1", "get_weather", city="Paris"))
        updates = await read_until(conversation, "function_result")

        assert not [update for update in updates if any(content.type == "function_call" for content in update.contents)]
        await eventually(lambda: tool_results(client) == ["Sunny in Paris"])


async def test_update_hooks_run_innermost_first() -> None:
    """With hooks registered by an outer then an inner middleware, each update reaches them in the order
    ['inner', 'outer']."""
    client = FakeRealtimeClient()
    order: list[str] = []

    def tag(name: str) -> Callable[[AgentResponseUpdate], None]:
        return lambda update: order.append(name)

    agent = RealtimeAgent(client, middleware=[update_hook(tag("outer")), update_hook(tag("inner"))])
    async with agent.start_conversation() as conversation:
        client.session.emit(event("response_created", response_id="resp_1"))
        await next_update(conversation)

        assert order == ["inner", "outer"]


async def test_an_async_update_hooks_replacement_is_yielded() -> None:
    """An async update hook replacing the agent's text 'Hello' with 'Hi' yields 'Hi'."""
    client = FakeRealtimeClient()

    async def shorten(update: AgentResponseUpdate) -> AgentResponseUpdate | None:
        if update.text != "Hello":
            return None
        return AgentResponseUpdate(contents=[Content.from_text("Hi")], role=update.role)

    async with RealtimeAgent(client, middleware=[update_hook(shorten)]).start_conversation() as conversation:
        client.session.emit(*reply("resp_1", "item_1", "Hello"))
        updates = [await next_update(conversation) for _ in range(4)]

        assert [update.text for update in updates if update.text] == ["Hi"]


async def test_an_update_hook_failure_closes_the_conversation_then_iteration_raises_it() -> None:
    """An update hook raising RuntimeError('moderation down') on the first of three queued updates closes the session,
    and iteration raises that RuntimeError without yielding the other two."""
    client = FakeRealtimeClient()

    def moderate(update: AgentResponseUpdate) -> None:
        raise RuntimeError("moderation down")

    async with RealtimeAgent(client, middleware=[update_hook(moderate)]).start_conversation() as conversation:
        client.session.emit(*reply("resp_1", "item_1", "Hello")[:3])
        await eventually(lambda: [message.text for message in conversation.history] == ["Hello"])
        yielded: list[AgentResponseUpdate] = []

        with pytest.raises(RuntimeError, match="moderation down"):
            async for update in conversation:
                yielded.append(update)

        assert (yielded, client.session.close_count) == ([], 1)


async def test_after_an_update_hook_failure_a_new_iterator_yields_none_of_the_queued_updates() -> None:
    """An update hook failing with RuntimeError('moderation down') on the first of three queued updates leaves the
    other two undelivered: reading again raises that RuntimeError instead of yielding them."""
    client = FakeRealtimeClient()
    failures = [RuntimeError("moderation down")]

    def moderate_once(update: AgentResponseUpdate) -> None:
        if failures:
            raise failures.pop()

    async with RealtimeAgent(client, middleware=[update_hook(moderate_once)]).start_conversation() as conversation:
        client.session.emit(*reply("resp_1", "item_1", "Hello")[:3])
        await eventually(lambda: [message.text for message in conversation.history] == ["Hello"])
        with pytest.raises(RuntimeError):
            await next_update(conversation)

        with pytest.raises(RuntimeError, match="moderation down"):
            await next_update(conversation)


# endregion

# region Close hooks


async def test_a_close_hook_runs_once_after_the_exchanges_are_reported_with_none() -> None:
    """A sync close hook runs once when the app closes the conversation, gets None, and finds the history recorder
    already holding the call's exchange."""
    client = FakeRealtimeClient()
    recorder = Recorder()
    seen: list[tuple[Exception | None, int]] = []

    def audit(error: Exception | None) -> None:
        seen.append((error, len(recorder.reports)))

    agent = RealtimeAgent(client, context_providers=[recorder], middleware=[close_hook(audit)])
    async with agent.start_conversation() as conversation:
        await conversation.send("Hi", create_response=False)
        await streamed(conversation, *reply("resp_1", "item_1", "Hello"))

    assert seen == [(None, 1)]


async def test_a_close_hook_gets_the_exception_that_ended_the_conversation() -> None:
    """When receiving fails with RealtimeConnectionException('socket died'), the close hook gets that exception."""
    client = FakeRealtimeClient()
    socket_died = RealtimeConnectionException("socket died")
    seen: list[Exception | None] = []

    async with RealtimeAgent(client, middleware=[close_hook(seen.append)]).start_conversation() as conversation:
        client.session.fail(socket_died)
        with pytest.raises(RealtimeConnectionException):
            async for _ in conversation:
                pass

    assert seen == [socket_died]


async def test_close_hooks_run_innermost_first() -> None:
    """With close hooks registered by an outer then an inner middleware, they run in the order ['inner', 'outer']."""
    order: list[str] = []
    outer = close_hook(lambda error: order.append("outer"))
    inner = close_hook(lambda error: order.append("inner"))

    async with RealtimeAgent(FakeRealtimeClient(), middleware=[outer, inner]).start_conversation():
        pass

    assert order == ["inner", "outer"]


async def test_a_failing_close_hook_raises_from_close_after_cleanup() -> None:
    """An inner close hook raising RuntimeError('audit store down') makes close() raise it after the session closed
    and the outer close hook ran."""
    client = FakeRealtimeClient()
    order: list[str] = []

    def fail(error: Exception | None) -> None:
        raise RuntimeError("audit store down")

    outer = close_hook(lambda error: order.append("outer"))
    with pytest.raises(RuntimeError, match="audit store down"):
        async with RealtimeAgent(client, middleware=[outer, close_hook(fail)]).start_conversation():
            pass

    assert (client.session.close_count, order) == (1, ["outer"])


async def test_closing_a_refused_conversation_runs_no_close_hooks() -> None:
    """Calling close() on a conversation whose middleware refused the session doesn't run the close hook it
    registered."""
    closed: list[Exception | None] = []

    async def refuse(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        context.close_hooks.append(closed.append)
        raise MiddlewareTermination("refused")

    conversation = RealtimeAgent(FakeRealtimeClient(), middleware=[refuse]).start_conversation()
    with pytest.raises(MiddlewareTermination):
        async with conversation:
            pass
    await conversation.close()

    assert closed == []


# endregion

# region Categories


async def test_a_bundle_with_a_conversation_member_runs_on_a_realtime_agent() -> None:
    """MiddlewareBundle([audit_conversation, audit_function]) on a RealtimeAgent runs the conversation member on
    entry."""
    client = FakeRealtimeClient()
    audit = SessionStartAudit(client)

    async def audit_calls(context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        await call_next()

    async with RealtimeAgent(client, middleware=[MiddlewareBundle([audit, audit_calls])]).start_conversation():
        assert audit.starts == [(0, 1)]


# endregion


class TestVerification:
    async def test_middleware_termination_before_call_next_refuses_the_session(self) -> None:
        """process() raising MiddlewareTermination before call_next() makes entering raise it, and no session
        starts."""
        client = FakeRealtimeClient()

        async def refuse(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
            raise MiddlewareTermination("refused")

        with pytest.raises(MiddlewareTermination, match="refused"):
            async with RealtimeAgent(client, middleware=[refuse]).start_conversation():
                pass

        assert client.start_session_calls == []

    async def test_send_hooks_run_outermost_first(self) -> None:
        """With send hooks registered by an outer then an inner middleware, each message reaches them in the order
        ['outer', 'inner']."""
        client = FakeRealtimeClient()
        order: list[str] = []

        def tag(name: str) -> Callable[[Message | RealtimeEvent], None]:
            return lambda item: order.append(name)

        agent = RealtimeAgent(client, middleware=[send_hook(tag("outer")), send_hook(tag("inner"))])
        async with agent.start_conversation() as conversation:
            await conversation.send("Hello", create_response=False)

            assert order == ["outer", "inner"]

    async def test_an_app_tool_result_passes_the_send_hooks_before_it_answers_its_call(self) -> None:
        """A declaration-only tool's result 'card 4111' sent by the app is rewritten by a send hook to
        'card [redacted]', and the session receives the rewritten result for that call."""
        client = FakeRealtimeClient()
        account = FunctionTool(name="look_up_account", description="Look up the caller's account; the app answers.")

        def redact(item: Message | RealtimeEvent) -> Message | None:
            if isinstance(item, Message) and item.contents[0].type == "function_result":
                return Message("tool", [Content.from_function_result(call_id="call_1", result="card [redacted]")])
            return None

        agent = RealtimeAgent(client, tools=[account], middleware=[send_hook(redact)])
        async with agent.start_conversation() as conversation:
            client.session.emit(*call_response("resp_1", "item_1", "call_1", "look_up_account"))
            await read_until(conversation, "function_call")
            await conversation.send(Content.from_function_result(call_id="call_1", result="card 4111"))

            await eventually(lambda: tool_results(client) == ["card [redacted]"])

    async def test_control_calls_and_tool_results_do_not_reach_the_send_hooks(self) -> None:
        """say('One moment.') and get_weather's result sent by the tool loop never reach a send hook that records
        every item."""
        client = FakeRealtimeClient()
        seen: list[Message | RealtimeEvent] = []

        agent = RealtimeAgent(client, tools=[get_weather], middleware=[send_hook(seen.append)])
        async with agent.start_conversation() as conversation:
            await conversation.say("One moment.")
            client.session.emit(*call_response("resp_1", "item_1", "call_1", "get_weather", city="Paris"))
            await eventually(lambda: tool_results(client) == ["Sunny in Paris"])

            assert seen == []

    async def test_updates_still_queued_at_close_pass_through_the_update_hooks(self) -> None:
        """An agent text update queued before close and read after it is yielded as the update hook's replacement."""
        client = FakeRealtimeClient()

        def shout(update: AgentResponseUpdate) -> AgentResponseUpdate | None:
            if not update.text:
                return None
            return AgentResponseUpdate(contents=[Content.from_text(update.text.upper())], role=update.role)

        conversation = RealtimeAgent(client, middleware=[update_hook(shout)]).start_conversation()
        async with conversation:
            client.session.emit(*reply("resp_1", "item_1", "hello"))
            await eventually(lambda: [message.text for message in conversation.history] == ["hello"])

        assert [update.text async for update in conversation if update.text] == ["HELLO"]

    async def test_close_hooks_do_not_run_for_a_refused_session(self) -> None:
        """A close hook registered before process() raises MiddlewareTermination never runs."""
        closed: list[Exception | None] = []

        async def refuse(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
            context.close_hooks.append(closed.append)
            raise MiddlewareTermination("refused")

        with pytest.raises(MiddlewareTermination):
            async with RealtimeAgent(FakeRealtimeClient(), middleware=[refuse]).start_conversation():
                pass

        assert closed == []

    async def test_an_exception_after_call_next_reaches_the_close_hooks(self) -> None:
        """process() raising RuntimeError('audit store down') after call_next() passes that RuntimeError to its close
        hook."""
        closed: list[Exception | None] = []
        audit_store_down = RuntimeError("audit store down")

        async def audit(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
            context.close_hooks.append(closed.append)
            await call_next()
            raise audit_store_down

        with pytest.raises(RuntimeError):
            async with RealtimeAgent(FakeRealtimeClient(), middleware=[audit]).start_conversation():
                pass

        assert closed == [audit_store_down]

    async def test_close_hook_and_context_provider_failures_raise_together(self) -> None:
        """A failing context provider and a failing close hook make close() raise an ExceptionGroup holding both
        errors."""
        client = FakeRealtimeClient()
        audit_store_down = RuntimeError("audit store down")

        def fail(error: Exception | None) -> None:
            raise audit_store_down

        agent = RealtimeAgent(client, context_providers=[Failing()], middleware=[close_hook(fail)])
        with pytest.raises(ExceptionGroup) as raised:
            async with agent.start_conversation() as conversation:
                await conversation.send("Hi", create_response=False)
                await streamed(conversation, *reply("resp_1", "item_1", "Hello"))

        assert [str(error) for error in raised.value.exceptions] == ["failing store down", "audit store down"]

    async def test_a_realtime_start_still_raises_for_a_bundle_with_an_agent_member(self) -> None:
        """A RealtimeAgent whose middleware holds MiddlewareBundle([agent_middleware, conversation_middleware]) raises
        MiddlewareException from start_conversation(), and nothing connects."""
        client = FakeRealtimeClient()

        async def audit_runs(context: AgentContext, call_next: Callable[[], Awaitable[None]]) -> None:
            await call_next()

        bundle = MiddlewareBundle([audit_runs, SessionStartAudit(client)])
        with pytest.raises(MiddlewareException, match="agent middleware"):
            RealtimeAgent(client, middleware=[bundle]).start_conversation()

        assert client.start_session_calls == []
