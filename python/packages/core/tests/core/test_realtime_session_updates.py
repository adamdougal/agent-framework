# Copyright (c) Microsoft. All rights reserved.

"""Session updates and verbatim speech in a live realtime conversation, and tool-list changes made by tools."""

import asyncio
import logging
from typing import Any

import pytest

from agent_framework import (
    Content,
    FunctionInvocationContext,
    FunctionTool,
    Message,
    RealtimeAgent,
    RealtimeEvent,
    function_middleware,
    tool,
)
from agent_framework.exceptions import (
    RealtimeActiveResponseException,
    RealtimeControlException,
    RealtimeControlTimeoutException,
    RealtimeConversationClosedException,
    RealtimeUnsupportedException,
)

from .realtime_helpers import FakeRealtimeClient, FakeRealtimeSession, event, eventually, next_update


@tool
def get_weather(city: str) -> str:
    """Get the weather for a city."""
    return f"Sunny in {city}"


@tool
def get_time() -> str:
    """Get the time."""
    return "noon"


def _call(call_id: str, name: str, *, response_id: str = "resp_1", **arguments: str) -> RealtimeEvent:
    return event(
        "function_call",
        content=Content.from_function_call(call_id=call_id, name=name, arguments=arguments),
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


# region update_session


async def test_update_session_sends_its_options_without_tools() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.update_session(options={"instructions": "Speak Spanish."})

    assert client.session.calls == [("update_session", {"instructions": "Speak Spanish."}, None)]


async def test_update_session_with_nothing_asks_the_session_to_update_nothing() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.update_session()

    assert client.session.calls == [("update_session", {}, None)]


async def test_update_session_tools_replace_the_executable_tools() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
        await conversation.update_session(tools=[get_time])
        client.session.emit(
            *_response("resp_1", _call("call_1", "get_time"), _call("call_2", "get_weather", city="Paris"))
        )
        await eventually(lambda: bool(_results(client.session)))

    assert client.session.calls[0] == ("update_session", {}, [get_time])
    [(_, time_result), (_, weather_result)] = _results(client.session)
    assert time_result == "noon"
    assert "not found" in weather_result


async def test_update_session_declares_plain_functions_as_function_tools() -> None:
    client = FakeRealtimeClient()

    def get_date() -> str:
        """Get the date."""
        return "today"

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.update_session(tools=[get_date])

    [(_, _, [declared])] = client.session.calls
    assert isinstance(declared, FunctionTool)
    assert declared.name == "get_date"


async def test_update_session_with_empty_tools_clears_them() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
        await conversation.update_session(tools=[])
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: bool(_results(client.session)))

    assert client.session.calls[0] == ("update_session", {}, [])
    [(_, weather_result)] = _results(client.session)
    assert "not found" in weather_result


async def test_update_session_replaces_the_live_tool_list_in_place() -> None:
    client = FakeRealtimeClient()
    started = asyncio.Event()
    release = asyncio.Event()

    @tool
    async def offer_time(ctx: FunctionInvocationContext) -> str:
        """Start offering the time tool, once the lookup finishes."""
        started.set()
        await release.wait()
        ctx.add_tools(get_time)
        return "offered"

    async with RealtimeAgent(client, tools=[offer_time]).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "offer_time")))
        await started.wait()
        await conversation.update_session(tools=[offer_time, get_weather])
        release.set()
        await eventually(lambda: len(_results(client.session)) == 1)
        client.session.emit(*_response("resp_2", _call("call_2", "get_time", response_id="resp_2")))
        await eventually(lambda: len(_results(client.session)) == 2)

    assert _results(client.session)[1] == ("call_2", "noon")


async def test_update_session_rejects_duplicate_tool_names_before_sending() -> None:
    client = FakeRealtimeClient()

    @tool(name="get_weather")
    def other_weather(city: str) -> str:
        """Get the weather from another service."""
        return f"Cloudy in {city}"

    async with RealtimeAgent(client).start_conversation() as conversation:
        with pytest.raises(ValueError):
            await conversation.update_session(tools=[get_weather, other_weather])

    assert client.session.calls == []


async def test_a_rejected_update_raises_and_leaves_the_executable_tools_unchanged() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
        client.session.update_session_error = RealtimeControlException("Invalid tool.", code="invalid_value")
        with pytest.raises(RealtimeControlException):
            await conversation.update_session(tools=[get_time])
        client.session.emit(
            *_response("resp_1", _call("call_1", "get_weather", city="Paris"), _call("call_2", "get_time"))
        )
        await eventually(lambda: bool(_results(client.session)))

    [(_, weather_result), (_, time_result)] = _results(client.session)
    assert weather_result == "Sunny in Paris"
    assert "not found" in time_result


async def test_update_session_waits_behind_another_control_call() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        release = client.session.hold("create_response")
        creating = asyncio.create_task(conversation.create_response())
        await eventually(lambda: bool(client.session.calls))
        updating = asyncio.create_task(conversation.update_session(options={"instructions": "Speak Spanish."}))
        await asyncio.sleep(0.01)
        assert client.session.calls == [("create_response", None)]

        release.set()
        await creating
        await updating

    assert client.session.calls == [
        ("create_response", None),
        ("update_session", {"instructions": "Speak Spanish."}, None),
    ]


async def test_update_session_after_close_raises_conversation_closed() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.close()

        with pytest.raises(RealtimeConversationClosedException):
            await conversation.update_session(options={"instructions": "Speak Spanish."})

    assert client.session.calls == []


async def test_an_unacknowledged_update_raises_control_timeout() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(control_timeout=0.05) as conversation:
        client.session.hold("update_session")

        with pytest.raises(RealtimeControlTimeoutException):
            await asyncio.wait_for(conversation.update_session(options={"instructions": "Speak Spanish."}), 1)


# endregion

# region say


async def test_say_sends_the_text_and_returns_the_response_id() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        response_id = await conversation.say("Thanks for calling.")

    assert response_id == "resp_said"
    assert client.session.calls == [("say", "Thanks for calling.")]


async def test_say_during_an_active_response_raises_without_saying_anything() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event("response_created", response_id="resp_1"))
        await next_update(conversation)

        with pytest.raises(RealtimeActiveResponseException):
            await conversation.say("Thanks for calling.")

    assert client.session.calls == []


async def _weather_follow_up_after_a_filler_line(
    agent: RealtimeAgent, client: FakeRealtimeClient, say_filler: asyncio.Event, release: asyncio.Event
) -> None:
    """The caller is still speaking, so the weather follow-up waits until after check_account's filler line."""
    async with agent.start_conversation():
        client.session.say_events = [event("response_created", response_id="resp_said")]
        client.session.emit(
            event("speech_started", item_id="item_9", audio_start_ms=100),
            *_response("resp_1", _call("call_1", "get_weather", city="Paris")),
            *_response("resp_2", _call("call_2", "check_account", response_id="resp_2")),
        )
        await eventually(lambda: _results(client.session) == [("call_1", "Sunny in Paris")])
        say_filler.set()
        await eventually(lambda: bool(client.session.calls))
        client.session.emit(
            event("response_done", response_id="resp_said", status="completed"),
            event("speech_stopped", item_id="item_9", audio_end_ms=900),
        )

        await eventually(lambda: len(client.session.calls) == 2)
        release.set()


async def test_a_line_a_tool_says_does_not_count_as_the_follow_up() -> None:
    client = FakeRealtimeClient()
    say_filler = asyncio.Event()
    release = asyncio.Event()

    @tool
    async def check_account(ctx: FunctionInvocationContext) -> str:
        """Check the account, saying a filler line first."""
        assert ctx.realtime_conversation is not None
        await say_filler.wait()
        await ctx.realtime_conversation.say("One moment.")
        await release.wait()
        return "account checked"

    agent = RealtimeAgent(client, tools=[get_weather, check_account])
    await _weather_follow_up_after_a_filler_line(agent, client, say_filler, release)

    assert client.session.calls[:2] == [("say", "One moment."), ("create_response", None)]


# endregion

# region Tool-list changes made by tools


async def test_tools_a_tool_adds_reach_the_session_before_its_result() -> None:
    client = FakeRealtimeClient()

    @tool
    def offer_time(ctx: FunctionInvocationContext) -> str:
        """Start offering the time tool."""
        ctx.add_tools(get_time)
        return "offered"

    async with RealtimeAgent(client, tools=[offer_time]).start_conversation():
        acknowledge = client.session.hold("update_session")
        client.session.emit(*_response("resp_1", _call("call_1", "offer_time")))
        await eventually(lambda: bool(client.session.calls))
        await asyncio.sleep(0.01)
        assert _results(client.session) == []

        acknowledge.set()
        await eventually(lambda: bool(_results(client.session)))

    assert client.session.calls[0] == ("update_session", {}, [offer_time, get_time])


async def test_concurrent_batches_declare_their_tool_changes_once() -> None:
    client = FakeRealtimeClient()
    release_time = asyncio.Event()
    release_weather = asyncio.Event()

    @tool
    async def offer_time(ctx: FunctionInvocationContext) -> str:
        """Start offering the time tool."""
        ctx.add_tools(get_time)
        await release_time.wait()
        return "time offered"

    @tool
    async def offer_weather(ctx: FunctionInvocationContext) -> str:
        """Start offering the weather tool."""
        ctx.add_tools(get_weather)
        await release_weather.wait()
        return "weather offered"

    async with RealtimeAgent(client, tools=[offer_time, offer_weather]).start_conversation():
        client.session.emit(
            *_response("resp_1", _call("call_1", "offer_time")),
            *_response("resp_2", _call("call_2", "offer_weather", response_id="resp_2")),
        )
        await asyncio.sleep(0.01)  # both tools have added theirs
        release_time.set()
        await eventually(lambda: len(_results(client.session)) == 1)
        release_weather.set()
        await eventually(lambda: len(_results(client.session)) == 2)

    updates = [call for call in client.session.calls if call[0] == "update_session"]
    assert updates == [("update_session", {}, [offer_time, offer_weather, get_time, get_weather])]


async def test_a_rejected_tool_update_restores_the_tools_and_still_sends_the_result(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = FakeRealtimeClient()

    @tool
    def offer_time(ctx: FunctionInvocationContext) -> str:
        """Start offering the time tool."""
        ctx.add_tools(get_time)
        return "offered"

    async with RealtimeAgent(client, tools=[offer_time]).start_conversation():
        client.session.update_session_error = RealtimeControlException("Invalid tool schema.", code="invalid_value")
        with caplog.at_level(logging.WARNING):
            client.session.emit(*_response("resp_1", _call("call_1", "offer_time")))
            await eventually(lambda: ("create_response", None) in client.session.calls)
        client.session.emit(*_response("resp_2", _call("call_2", "get_time", response_id="resp_2")))
        await eventually(lambda: len(_results(client.session)) == 2)

    [first, (_, time_result)] = _results(client.session)
    assert first == ("call_1", "offered")
    assert "not found" in time_result
    assert "Invalid tool schema." in caplog.text


async def test_a_timed_out_tool_update_keeps_the_change_and_the_next_batch_resends_it(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = FakeRealtimeClient()

    @tool
    def offer_time(ctx: FunctionInvocationContext) -> str:
        """Start offering the time tool."""
        ctx.add_tools(get_time)
        return "offered"

    async with RealtimeAgent(client, tools=[offer_time]).start_conversation(control_timeout=0.05):
        acknowledge = client.session.hold("update_session")
        with caplog.at_level(logging.WARNING):
            client.session.emit(*_response("resp_1", _call("call_1", "offer_time")))
            await eventually(lambda: bool(_results(client.session)))
        acknowledge.set()
        client.session.emit(*_response("resp_2", _call("call_2", "get_time", response_id="resp_2")))
        await eventually(lambda: len(_results(client.session)) == 2)

    assert _results(client.session)[1] == ("call_2", "noon")
    updates = [call for call in client.session.calls if call[0] == "update_session"]
    assert updates == [("update_session", {}, [offer_time, get_time])] * 2
    assert "acknowledge" in caplog.text


# endregion


class TestVerification:
    async def test_a_batch_after_an_app_update_sends_no_update_of_its_own(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
            await conversation.update_session(tools=[get_time])
            client.session.emit(*_response("resp_1", _call("call_1", "get_time")))
            await eventually(lambda: ("create_response", None) in client.session.calls)

        assert client.session.calls == [("update_session", {}, [get_time]), ("create_response", None)]

    async def test_a_batch_that_changes_no_tools_sends_no_update(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client, tools=[get_weather]).start_conversation():
            client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
            await eventually(lambda: ("create_response", None) in client.session.calls)

        assert client.session.calls == [("create_response", None)]

    async def test_an_options_only_update_keeps_the_executable_tools(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
            await conversation.update_session(options={"instructions": "Speak Spanish."})
            client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
            await eventually(lambda: bool(_results(client.session)))

        assert _results(client.session) == [("call_1", "Sunny in Paris")]

    async def test_a_timed_out_update_leaves_the_executable_tools_unchanged(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client, tools=[get_weather]).start_conversation(control_timeout=0.05) as conversation:
            client.session.hold("update_session")
            with pytest.raises(RealtimeControlTimeoutException):
                await conversation.update_session(tools=[get_time])
            client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
            await eventually(lambda: bool(_results(client.session)))

        assert _results(client.session) == [("call_1", "Sunny in Paris")]

    async def test_a_follow_up_due_during_an_update_is_requested_after_it(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
            acknowledge = client.session.hold("update_session")
            updating = asyncio.create_task(conversation.update_session(options={"tool_choice": "none"}))
            await eventually(lambda: bool(client.session.calls))
            client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
            await eventually(lambda: bool(_results(client.session)))
            await asyncio.sleep(0.01)
            assert client.session.calls == [("update_session", {"tool_choice": "none"}, None)]

            acknowledge.set()
            await updating
            await eventually(lambda: len(client.session.calls) == 2)

        assert client.session.calls[1] == ("create_response", None)

    async def test_say_after_close_raises_conversation_closed(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            await conversation.close()

            with pytest.raises(RealtimeConversationClosedException):
                await conversation.say("Goodbye.")

        assert client.session.calls == []

    async def test_an_unacknowledged_say_raises_control_timeout(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation(control_timeout=0.05) as conversation:
            client.session.hold("say")

            with pytest.raises(RealtimeControlTimeoutException):
                await asyncio.wait_for(conversation.say("One moment."), 1)

    async def test_an_unsupported_say_leaves_the_conversation_usable(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            client.session.say_error = RealtimeUnsupportedException("Voice Live can't speak text with an OpenAI voice.")
            with pytest.raises(RealtimeUnsupportedException):
                await conversation.say("Thanks for calling.")

            assert await conversation.create_response() == "resp_1"

    async def test_a_line_function_middleware_says_does_not_count_as_the_follow_up(self) -> None:
        client = FakeRealtimeClient()
        say_filler = asyncio.Event()
        release = asyncio.Event()

        @tool
        async def check_account() -> str:
            """Check the account, slowly."""
            await release.wait()
            return "account checked"

        @function_middleware
        async def filler_for_slow_tools(context: FunctionInvocationContext, call_next: Any) -> None:
            if context.function.name == "check_account" and context.realtime_conversation is not None:
                await say_filler.wait()
                await context.realtime_conversation.say("One moment.")
            await call_next()

        agent = RealtimeAgent(client, tools=[get_weather, check_account], middleware=[filler_for_slow_tools])
        await _weather_follow_up_after_a_filler_line(agent, client, say_filler, release)

        assert client.session.calls[:2] == [("say", "One moment."), ("create_response", None)]

    async def test_tools_a_tool_removes_reach_the_session_before_its_result(self) -> None:
        client = FakeRealtimeClient()

        @tool
        def retire_weather(ctx: FunctionInvocationContext) -> str:
            """Stop offering the weather tool."""
            ctx.remove_tools("get_weather")
            return "retired"

        async with RealtimeAgent(client, tools=[get_weather, retire_weather]).start_conversation():
            acknowledge = client.session.hold("update_session")
            client.session.emit(*_response("resp_1", _call("call_1", "retire_weather")))
            await eventually(lambda: bool(client.session.calls))
            await asyncio.sleep(0.01)
            assert _results(client.session) == []

            acknowledge.set()
            await eventually(lambda: bool(_results(client.session)))

        assert client.session.calls[0] == ("update_session", {}, [retire_weather])

    async def test_close_during_a_tool_update_settles_the_batch(self) -> None:
        client = FakeRealtimeClient()

        @tool
        def offer_time(ctx: FunctionInvocationContext) -> str:
            """Start offering the time tool."""
            ctx.add_tools(get_time)
            return "offered"

        async with RealtimeAgent(client, tools=[offer_time]).start_conversation() as conversation:
            client.session.hold("update_session")
            client.session.emit(*_response("resp_1", _call("call_1", "offer_time")))
            await eventually(lambda: bool(client.session.calls))

            await conversation.close()
            updates = [update async for update in conversation]

        streamed = [content for update in updates for content in update.contents if content.type == "function_result"]
        assert [(result.call_id, result.result) for result in streamed] == [("call_1", "offered")]
        assert _results(client.session) == []
