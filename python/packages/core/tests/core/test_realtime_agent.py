# Copyright (c) Microsoft. All rights reserved.

"""Realtime agents starting live conversations."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from agent_framework import (
    AgentContext,
    AgentMiddleware,
    Message,
    RealtimeAgent,
    RealtimeConversation,
    RealtimeOptions,
    tool,
)
from agent_framework.exceptions import (
    RealtimeConnectionException,
    RealtimeControlTimeoutException,
    RealtimeConversationClosedException,
)

from .realtime_helpers import FakeRealtimeClient, eventually


@tool
def get_weather(city: str) -> str:
    """Get the weather for a city."""
    return f"Sunny in {city}"


@tool
def get_time() -> str:
    """Get the current time."""
    return "noon"


def test_start_conversation_returns_an_unopened_conversation_without_connecting() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client)

    conversation = agent.start_conversation()

    assert isinstance(conversation, RealtimeConversation)
    assert client.start_session_calls == []


async def test_entering_the_conversation_starts_a_session_with_the_agents_instructions_and_tools() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, instructions="Be brief.", tools=[get_weather])

    async with agent.start_conversation():
        pass

    assert client.start_session_calls == [{"options": {"instructions": "Be brief."}, "tools": [get_weather]}]


async def test_leaving_the_conversation_closes_its_session() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation():
        pass

    assert client.session.close_count == 1


async def test_active_agent_is_the_agent_that_started_the_conversation() -> None:
    agent = RealtimeAgent(FakeRealtimeClient())

    async with agent.start_conversation() as conversation:
        assert conversation.active_agent is agent


async def test_start_options_replace_default_options_key_by_key() -> None:
    client = FakeRealtimeClient()
    default_options: RealtimeOptions = {"voice": "alloy", "temperature": 0.6}
    agent = RealtimeAgent(client, default_options=default_options)

    async with agent.start_conversation(options={"temperature": 0.8}):
        pass

    assert client.start_session_calls[0]["options"] == {"voice": "alloy", "temperature": 0.8}


async def test_start_instructions_are_appended_to_the_agents_instructions() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, instructions="Be brief.")

    async with agent.start_conversation(options={"instructions": "Speak French."}):
        pass

    assert client.start_session_calls[0]["options"]["instructions"] == "Be brief.\nSpeak French."


async def test_constructor_instructions_take_precedence_over_default_options_instructions() -> None:
    client = FakeRealtimeClient()
    default_options: RealtimeOptions = {"instructions": "Ignored."}
    agent = RealtimeAgent(client, instructions="Be brief.", default_options=default_options)

    async with agent.start_conversation():
        pass

    assert client.start_session_calls[0]["options"]["instructions"] == "Be brief."


async def test_start_instructions_are_appended_to_default_options_instructions() -> None:
    client = FakeRealtimeClient()
    default_options: RealtimeOptions = {"instructions": "Be brief."}
    agent = RealtimeAgent(client, default_options=default_options)

    async with agent.start_conversation(options={"instructions": "Speak French."}):
        pass

    assert client.start_session_calls[0]["options"]["instructions"] == "Be brief.\nSpeak French."


async def test_start_metadata_merges_with_default_metadata() -> None:
    client = FakeRealtimeClient()
    default_options: RealtimeOptions = {"metadata": {"a": "1"}}
    agent = RealtimeAgent(client, default_options=default_options)

    async with agent.start_conversation(options={"metadata": {"b": "2"}}):
        pass

    assert client.start_session_calls[0]["options"]["metadata"] == {"a": "1", "b": "2"}


async def test_empty_start_metadata_keeps_the_default_metadata() -> None:
    client = FakeRealtimeClient()
    default_options: RealtimeOptions = {"metadata": {"a": "1"}}
    agent = RealtimeAgent(client, default_options=default_options)

    async with agent.start_conversation(options={"metadata": {}}):
        pass

    assert client.start_session_calls[0]["options"]["metadata"] == {"a": "1"}


async def test_start_tools_follow_the_agents_tools() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[get_weather])

    async with agent.start_conversation(tools=[get_time]):
        pass

    assert [tool.name for tool in client.start_session_calls[0]["tools"]] == ["get_weather", "get_time"]


def test_duplicate_tool_names_raise_value_error_before_connecting() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[get_weather])

    @tool(name="get_weather")
    def other_weather(city: str) -> str:
        """Get the weather another way."""
        return f"Rainy in {city}"

    with pytest.raises(ValueError, match="unique"):
        agent.start_conversation(tools=[other_weather])

    assert client.start_session_calls == []


def test_distinct_agent_tools_sharing_a_name_raise_value_error_before_connecting() -> None:
    client = FakeRealtimeClient()

    @tool(name="get_weather")
    def other_weather(city: str) -> str:
        """Get the weather another way."""
        return f"Rainy in {city}"

    agent = RealtimeAgent(client, tools=[get_weather, other_weather])

    with pytest.raises(ValueError, match="unique"):
        agent.start_conversation()

    assert client.start_session_calls == []


@pytest.mark.parametrize("settings", [{"control_timeout": 0}, {"max_queued_updates": 0}])
def test_invalid_conversation_settings_raise_value_error(settings: dict[str, Any]) -> None:
    client = FakeRealtimeClient()

    with pytest.raises(ValueError):
        RealtimeAgent(client).start_conversation(**settings)


def test_invalid_function_invocation_configuration_raises_value_error_before_connecting() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, function_invocation_configuration={"max_iterations": 0})

    with pytest.raises(ValueError, match="max_iterations"):
        agent.start_conversation()

    assert client.start_session_calls == []


def test_bare_agent_middleware_is_skipped_with_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    class AuditMiddleware(AgentMiddleware):
        async def process(self, context: AgentContext, call_next: Callable[[], Awaitable[None]]) -> None:
            await call_next()

    with caplog.at_level(logging.WARNING):
        RealtimeAgent(FakeRealtimeClient()).start_conversation(middleware=[AuditMiddleware()])

    assert any("Ignoring agent middleware" in record.getMessage() for record in caplog.records)


async def test_initial_messages_are_sent_as_context_without_a_response() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(["Hi", Message("assistant", ["Hello"])]):
        session = client.session
        sent = [message for message in session.sent if isinstance(message, Message)]
        assert [(message.role, message.text) for message in sent] == [("user", "Hi"), ("assistant", "Hello")]
        assert session.calls == []


async def test_a_session_that_does_not_start_in_time_raises_control_timeout() -> None:
    client = FakeRealtimeClient()
    client.start_session_delay = 1.0

    with pytest.raises(RealtimeControlTimeoutException):
        async with RealtimeAgent(client).start_conversation(control_timeout=0.05):
            pass


async def test_a_failure_after_the_session_starts_closes_it_before_propagating() -> None:
    client = FakeRealtimeClient()
    client.send_error = RealtimeConnectionException("socket died")

    with pytest.raises(RealtimeConnectionException, match="socket died"):
        async with RealtimeAgent(client).start_conversation(["Hi"]):
            pass

    assert client.session.close_count == 1


async def test_entering_after_close_raises_conversation_closed_without_connecting() -> None:
    client = FakeRealtimeClient()
    conversation = RealtimeAgent(client).start_conversation()
    await conversation.close()

    with pytest.raises(RealtimeConversationClosedException):
        await conversation.__aenter__()

    assert client.start_session_calls == []


async def test_a_session_that_starts_as_the_conversation_closes_is_closed() -> None:
    client = FakeRealtimeClient()
    client.start_session_release = asyncio.Event()
    conversation = RealtimeAgent(client).start_conversation()
    entering = asyncio.create_task(conversation.__aenter__())
    await eventually(lambda: bool(client.start_session_calls))

    client.start_session_release.set()
    await asyncio.wait_for(conversation.close(), 1)

    with pytest.raises(RealtimeConversationClosedException):
        await asyncio.wait_for(entering, 1)
    assert client.session.close_count == 1


async def test_cancelling_entry_closes_a_session_that_had_already_started() -> None:
    client = FakeRealtimeClient()
    client.start_session_release = asyncio.Event()
    conversation = RealtimeAgent(client).start_conversation()
    entering = asyncio.create_task(conversation.__aenter__())
    await eventually(lambda: bool(client.start_session_calls))

    client.start_session_release.set()
    entering.cancel()

    with pytest.raises(asyncio.CancelledError):
        await entering
    assert client.session.close_count == 1


async def test_close_during_the_initial_messages_closes_the_session_once() -> None:
    client = FakeRealtimeClient()
    release_send = client.session_holds["send"] = asyncio.Event()
    conversation = RealtimeAgent(client).start_conversation("Hi")
    entering = asyncio.create_task(conversation.__aenter__())
    await eventually(lambda: bool(client.sessions) and client.session.send_attempts == 1)

    await asyncio.wait_for(conversation.close(), 1)
    assert client.session.close_count == 1

    release_send.set()
    with pytest.raises(RealtimeConversationClosedException):
        await asyncio.wait_for(entering, 1)
    assert client.session.close_count == 1


async def test_an_initial_send_that_fails_because_close_started_raises_conversation_closed() -> None:
    client = FakeRealtimeClient()
    release_send = client.session_holds["send"] = asyncio.Event()
    conversation = RealtimeAgent(client).start_conversation("Hi")
    entering = asyncio.create_task(conversation.__aenter__())
    await eventually(lambda: bool(client.sessions) and client.session.send_attempts == 1)

    await asyncio.wait_for(conversation.close(), 1)
    client.session.send_error = RealtimeConnectionException("The socket is closed.")
    release_send.set()

    with pytest.raises(RealtimeConversationClosedException):
        await asyncio.wait_for(entering, 1)


async def test_entering_a_conversation_again_raises_runtime_error() -> None:
    conversation = RealtimeAgent(FakeRealtimeClient()).start_conversation()

    async with conversation:
        with pytest.raises(RuntimeError):
            await conversation.__aenter__()

    with pytest.raises(RuntimeError):
        await conversation.__aenter__()


async def test_sending_before_entering_raises_runtime_error() -> None:
    conversation = RealtimeAgent(FakeRealtimeClient()).start_conversation()

    with pytest.raises(RuntimeError):
        await conversation.send("Hi")


class TestVerification:
    async def test_an_explicit_none_in_start_options_reaches_the_session(self) -> None:
        client = FakeRealtimeClient()
        default_options: RealtimeOptions = {"turn_detection": {"type": "server_vad"}}
        agent = RealtimeAgent(client, default_options=default_options)

        async with agent.start_conversation(options={"turn_detection": None}):
            pass

        assert client.start_session_calls[0]["options"] == {"turn_detection": None}

    async def test_a_session_start_failure_propagates(self) -> None:
        client = FakeRealtimeClient()
        client.start_session_error = RealtimeConnectionException("refused")

        with pytest.raises(RealtimeConnectionException, match="refused"):
            async with RealtimeAgent(client).start_conversation():
                pass
