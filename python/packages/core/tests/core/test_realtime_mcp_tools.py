# Copyright (c) Microsoft. All rights reserved.

"""Local MCP tools on realtime agents: their lifecycle and the copies of their functions a conversation runs."""

import asyncio
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import pytest
from mcp.server.mcpserver import MCPServer

from agent_framework import (
    FunctionInvocationContext,
    FunctionTool,
    RealtimeAgent,
    tool,
)
from agent_framework._mcp import MCPTool
from agent_framework.exceptions import RealtimeConversationClosedException, ToolException

from .realtime_helpers import (
    FakeRealtimeClient,
    call_event,
    eventually,
    response_events,
    sent_results,
)


def _orders_server() -> MCPServer:
    server = MCPServer("orders")

    @server.tool()
    def get_delivery_slots(order_id: str) -> str:
        """List the delivery slots available for an order."""
        return f"Order {order_id} can be delivered on Thursday from 9 to 11 AM."

    @server.tool()
    def book_delivery_slot(order_id: str, slot_id: str) -> str:
        """Book a delivery slot for an order."""
        return f"Booked order {order_id} in slot {slot_id}."

    return server


def _search_server() -> MCPServer:
    server = MCPServer("search")

    @server.tool()
    def search(query: str) -> str:
        """Search for a query."""
        return f"Results for {query}."

    return server


class _InProcessMCPTool(MCPTool):
    """An MCP tool whose server runs in the test's process, counting the connections it opens."""

    def __init__(self, server: Any, *, name: str = "orders", **kwargs: Any) -> None:
        super().__init__(name=name, load_prompts=False, **kwargs)
        self._server = server
        self.connections_opened = 0

    def get_mcp_client(self) -> Any:
        self.connections_opened += 1
        return self._server


class _PreparationRecordingMCPTool(_InProcessMCPTool):
    """Records the kwargs each preparation gets, and whether the tool was connected by then."""

    def __init__(self, server: Any) -> None:
        super().__init__(server)
        self.preparations: list[tuple[dict[str, Any], bool]] = []

    async def _prepare_for_run(self, kwargs: Mapping[str, Any]) -> None:
        self.preparations.append((dict(kwargs), self.is_connected))
        await super()._prepare_for_run(kwargs)


class _SlowToConnectMCPTool(_InProcessMCPTool):
    """Holds its connection open until the test releases it."""

    def __init__(self, server: Any) -> None:
        super().__init__(server)
        self.connecting = asyncio.Event()
        self.release = asyncio.Event()

    async def connect(self, *, reset: bool = False) -> None:
        self.connecting.set()
        await self.release.wait()
        await super().connect(reset=reset)


class _SlowToListMCPTool(_InProcessMCPTool):
    """Connects at once, then holds its tool listing until the test releases it."""

    def __init__(self, server: Any) -> None:
        super().__init__(server)
        self.listing = asyncio.Event()
        self.release = asyncio.Event()

    async def load_tools(self) -> None:
        self.listing.set()
        await self.release.wait()
        await super().load_tools()


class _UnreachableMCPTool(MCPTool):
    def __init__(self, *, name: str = "unreachable") -> None:
        super().__init__(name=name, load_prompts=False)

    async def connect(self, *, reset: bool = False) -> None:
        raise ToolException("The MCP server is unreachable.")

    def get_mcp_client(self) -> Any:
        raise NotImplementedError


def _names(tools: Sequence[Any]) -> list[str]:
    return [declared.name for declared in tools]


@tool
def get_time() -> str:
    """Get the time."""
    return "noon"


@tool
def hang_up() -> str:
    """End the call."""
    return "Goodbye."


# region Agent lifecycle


async def test_entering_the_agent_connects_its_mcp_tool() -> None:
    """Given an agent with an MCP tool that isn't connected, the tool is connected inside ``async with agent``."""
    orders = _InProcessMCPTool(_orders_server())
    agent = RealtimeAgent(FakeRealtimeClient(), tools=[orders])

    async with agent:
        assert orders.is_connected


async def test_leaving_the_agent_closes_the_mcp_tool_it_connected() -> None:
    """After ``async with agent`` ends, an MCP tool the agent connected on entry is no longer connected."""
    orders = _InProcessMCPTool(_orders_server())
    agent = RealtimeAgent(FakeRealtimeClient(), tools=[orders])

    async with agent:
        pass

    assert not orders.is_connected


async def test_the_agent_leaves_an_mcp_tool_the_app_connected_open() -> None:
    """An MCP tool the app connected stays connected, on the same connection, inside and after ``async with agent``."""
    orders = _InProcessMCPTool(_orders_server())
    agent = RealtimeAgent(FakeRealtimeClient(), tools=[orders])

    async with orders:
        async with agent:
            assert orders.is_connected
        assert orders.is_connected
        assert orders.connections_opened == 1


async def test_a_failed_connection_on_entry_closes_the_tools_already_connected() -> None:
    """When the second MCP tool fails to connect on entry, its ToolException propagates and the first is closed."""
    orders = _InProcessMCPTool(_orders_server())
    agent = RealtimeAgent(FakeRealtimeClient(), tools=[orders, _UnreachableMCPTool()])

    with pytest.raises(ToolException, match="unreachable"):
        async with agent:
            pass

    assert not orders.is_connected


# endregion

# region Conversation start


async def test_a_conversation_declares_an_mcp_tools_functions() -> None:
    """With the orders MCP tool connected, the session starts with get_delivery_slots and book_delivery_slot."""
    orders = _InProcessMCPTool(_orders_server())
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[orders])

    async with orders, agent.start_conversation():
        assert _names(client.start_session_calls[0]["tools"]) == ["get_delivery_slots", "book_delivery_slot"]


async def test_a_conversation_connects_an_mcp_tool_on_first_use() -> None:
    """On an agent that wasn't entered, starting a conversation connects its MCP tool and declares its functions."""
    orders = _InProcessMCPTool(_orders_server())
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[orders])

    try:
        async with agent.start_conversation():
            assert orders.is_connected
            assert _names(client.start_session_calls[0]["tools"]) == ["get_delivery_slots", "book_delivery_slot"]
    finally:
        await orders.close()


async def test_closing_while_an_mcp_tool_connects_at_start_ends_entry() -> None:
    """Close starting while entry connects an MCP tool makes entry raise the conversation-closed error, unstarted."""
    orders = _SlowToConnectMCPTool(_orders_server())
    client = FakeRealtimeClient()
    conversation = RealtimeAgent(client, tools=[orders]).start_conversation()

    entering = asyncio.create_task(conversation.__aenter__())
    await orders.connecting.wait()
    await conversation.close()

    with pytest.raises(RealtimeConversationClosedException):
        await asyncio.wait_for(entering, 5)
    assert client.start_session_calls == []


async def test_an_mcp_tool_connected_on_first_use_closes_with_the_agent() -> None:
    """An MCP tool passed to start_conversation stays connected after the conversation and closes with the agent."""
    orders = _InProcessMCPTool(_orders_server())
    agent = RealtimeAgent(FakeRealtimeClient())

    async with agent:
        async with agent.start_conversation(tools=[orders]):
            pass
        assert orders.is_connected

    assert not orders.is_connected


async def test_mcp_tools_are_prepared_with_the_conversations_function_invocation_kwargs() -> None:
    """The tool is prepared with exactly {"user_id": "caller-1"} before it connects and again once connected."""
    orders = _PreparationRecordingMCPTool(_orders_server())
    agent = RealtimeAgent(FakeRealtimeClient(), tools=[orders])

    try:
        async with agent.start_conversation(function_invocation_kwargs={"user_id": "caller-1"}):
            pass
    finally:
        await orders.close()

    assert orders.preparations == [({"user_id": "caller-1"}, False), ({"user_id": "caller-1"}, True)]


async def test_clashing_mcp_function_names_fail_entry_before_the_session_starts() -> None:
    """Two MCP tools exposing "search" make entry raise ValueError suggesting tool_name_prefix; no session starts."""
    client = FakeRealtimeClient()
    agent = RealtimeAgent(
        client,
        tools=[_InProcessMCPTool(_search_server(), name="docs"), _InProcessMCPTool(_search_server(), name="tickets")],
    )

    async with agent:
        with pytest.raises(ValueError, match="tool_name_prefix"):
            async with agent.start_conversation():
                pass

    assert client.start_session_calls == []


async def test_an_mcp_tool_named_like_a_function_tool_starts_a_conversation() -> None:
    """A function tool and an MCP tool both named "orders" start a conversation that declares both sets of tools."""
    orders = FunctionTool(
        func=lambda: "Three open orders.", name="orders", description="Summarize the caller's orders."
    )
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, tools=[orders, _InProcessMCPTool(_orders_server(), name="orders")])

    async with agent, agent.start_conversation():
        assert _names(client.start_session_calls[0]["tools"]) == ["orders", "get_delivery_slots", "book_delivery_slot"]


# endregion

# region Session updates


async def test_update_session_connects_an_mcp_tool_and_declares_its_functions() -> None:
    """update_session(tools=[orders]) connects the tool and sends get_delivery_slots and book_delivery_slot."""
    orders = _InProcessMCPTool(_orders_server())
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client)

    async with agent, agent.start_conversation() as conversation:
        await conversation.update_session(tools=[orders])

        assert orders.is_connected
        assert client.session.calls[-1][0] == "update_session"
        assert _names(client.session.calls[-1][2]) == ["get_delivery_slots", "book_delivery_slot"]


async def test_an_update_without_mcp_tools_keeps_its_place_among_control_calls() -> None:
    """update_session(tools=[get_time]) started before create_response() reaches the session first."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        updating = asyncio.create_task(conversation.update_session(tools=[get_time]))
        responding = asyncio.create_task(conversation.create_response())
        await asyncio.gather(updating, responding)

    assert [call[0] for call in client.session.calls] == ["update_session", "create_response"]


async def test_an_update_still_connecting_its_mcp_tool_stops_when_the_conversation_closes() -> None:
    """Close starting while an update's MCP tool connects raises the conversation-closed error and connects nothing."""
    orders = _SlowToConnectMCPTool(_orders_server())
    agent = RealtimeAgent(FakeRealtimeClient())

    async with agent, agent.start_conversation() as conversation:
        updating = asyncio.create_task(conversation.update_session(tools=[orders]))
        await orders.connecting.wait()
    orders.release.set()

    with pytest.raises(RealtimeConversationClosedException):
        await asyncio.wait_for(updating, 5)
    assert not orders.is_connected


async def test_an_update_after_close_connects_no_mcp_tool() -> None:
    """update_session(tools=[orders]) after the conversation closed raises the conversation-closed error unconnected."""
    orders = _InProcessMCPTool(_orders_server())
    agent = RealtimeAgent(FakeRealtimeClient())

    async with agent:
        async with agent.start_conversation() as conversation:
            pass

        with pytest.raises(RealtimeConversationClosedException):
            await conversation.update_session(tools=[orders])

    assert orders.connections_opened == 0


async def test_an_mcp_tool_a_tool_adds_declares_functions_that_run_on_the_server() -> None:
    """A tool adding orders declares its functions, and a later get_delivery_slots call gets the server's slots."""
    orders = _InProcessMCPTool(_orders_server())
    client = FakeRealtimeClient()

    @tool
    def offer_orders(ctx: FunctionInvocationContext) -> str:
        """Start offering the order tools."""
        ctx.add_tools(orders)
        return "offered"

    agent = RealtimeAgent(client, tools=[offer_orders])
    async with agent, agent.start_conversation():
        client.session.emit(*response_events("resp_1", call_event("call_1", "offer_orders")))
        await eventually(lambda: ("create_response", None) in client.session.calls)
        client.session.emit(
            *response_events(
                "resp_2", call_event("call_2", "get_delivery_slots", response_id="resp_2", order_id="1234")
            )
        )
        await eventually(lambda: len(sent_results(client.session)) == 2)

    assert _names(client.session.calls[0][2]) == ["offer_orders", "get_delivery_slots", "book_delivery_slot"]
    assert "Order 1234 can be delivered on Thursday from 9 to 11 AM." in sent_results(client.session)[1][1]


async def test_batches_finishing_while_an_added_mcp_tool_is_declared_leave_its_functions_runnable() -> None:
    """A get_time batch ends while orders lists its tools, and a refused search batch while orders' update waits for
    acknowledgement; get_delivery_slots then gets the server's slots."""
    orders = _SlowToListMCPTool(_orders_server())
    client = FakeRealtimeClient()

    @tool
    def offer_orders(ctx: FunctionInvocationContext) -> str:
        """Start offering the order tools."""
        ctx.add_tools(orders)
        return "orders offered"

    @tool
    def offer_search(ctx: FunctionInvocationContext) -> str:
        """Start offering a hosted search tool that may ask for approval."""
        ctx.add_tools({"type": "mcp", "server_label": "search", "server_url": "https://search.example/mcp"})
        return "search offered"

    agent = RealtimeAgent(client, tools=[offer_orders, offer_search, get_time])
    async with agent, agent.start_conversation():
        acknowledge = client.session.hold("update_session")
        client.session.emit(*response_events("resp_1", call_event("call_1", "offer_orders")))
        await orders.listing.wait()
        client.session.emit(*response_events("resp_2", call_event("call_2", "get_time", response_id="resp_2")))
        await asyncio.sleep(0.01)  # the time batch has finished its call
        orders.release.set()
        await eventually(lambda: bool(client.session.calls))
        client.session.emit(*response_events("resp_3", call_event("call_3", "offer_search", response_id="resp_3")))
        await asyncio.sleep(0.01)  # the search batch has finished its call
        acknowledge.set()
        await eventually(lambda: len(sent_results(client.session)) == 3)
        client.session.emit(
            *response_events(
                "resp_4", call_event("call_4", "get_delivery_slots", response_id="resp_4", order_id="1234")
            )
        )
        await eventually(lambda: len(sent_results(client.session)) == 4)

    assert "Order 1234 can be delivered on Thursday from 9 to 11 AM." in dict(sent_results(client.session))["call_4"]


@pytest.mark.parametrize(
    "refused",
    [
        pytest.param(
            lambda: {"type": "mcp", "server_label": "search", "server_url": "https://search.example/mcp"},
            id="hosted-mcp-tool-that-may-ask-for-approval",
        ),
        pytest.param(_UnreachableMCPTool, id="mcp-tool-that-cannot-connect"),
    ],
)
async def test_a_tool_change_that_cannot_be_declared_leaves_the_declared_tools(refused: Callable[[], Any]) -> None:
    """The refused tool's adder still has its result sent, and a later addition is declared without the refused tool."""
    client = FakeRealtimeClient()

    @tool
    def offer_refused(ctx: FunctionInvocationContext) -> str:
        """Start offering a tool that can't be declared."""
        ctx.add_tools(refused())
        return "refused offered"

    @tool
    def offer_time(ctx: FunctionInvocationContext) -> str:
        """Start offering the time tool."""
        ctx.add_tools(get_time)
        return "time offered"

    async with RealtimeAgent(client, tools=[offer_refused, offer_time]).start_conversation():
        client.session.emit(*response_events("resp_1", call_event("call_1", "offer_refused")))
        await eventually(lambda: ("create_response", None) in client.session.calls)
        client.session.emit(*response_events("resp_2", call_event("call_2", "offer_time", response_id="resp_2")))
        await eventually(lambda: len(sent_results(client.session)) == 2)

    assert sent_results(client.session) == [("call_1", "refused offered"), ("call_2", "time offered")]
    updates = [call for call in client.session.calls if call[0] == "update_session"]
    assert updates == [("update_session", {}, [offer_refused, offer_time, get_time])]


# endregion

# region Handoffs


async def test_a_handoff_connects_the_targets_mcp_tool_and_declares_its_functions() -> None:
    """handoff(billing) connects billing's orders tool and sends get_delivery_slots and book_delivery_slot."""
    orders = _InProcessMCPTool(_orders_server())
    client = FakeRealtimeClient()
    receptionist = RealtimeAgent(client)
    billing = RealtimeAgent(client, tools=[orders])

    async with receptionist, receptionist.start_conversation() as conversation:
        await conversation.handoff(billing)

        assert orders.is_connected
        assert _names(client.session.calls[-1][2]) == ["get_delivery_slots", "book_delivery_slot"]


async def test_a_targets_mcp_tool_connected_by_a_handoff_closes_with_the_agent_that_started_the_call() -> None:
    """Billing's orders tool stays connected after the conversation and closes when the receptionist exits."""
    orders = _InProcessMCPTool(_orders_server())
    client = FakeRealtimeClient()
    receptionist = RealtimeAgent(client)
    billing = RealtimeAgent(client, tools=[orders])

    async with receptionist:
        async with receptionist.start_conversation() as conversation:
            await conversation.handoff(billing)
        assert orders.is_connected

    assert not orders.is_connected


# endregion


class TestVerification:
    async def test_mcp_functions_are_declared_in_the_mcp_tools_place(self) -> None:
        """Agent tools [get_time, orders] and start tools [hang_up] declare get_time, the orders tools, hang_up."""
        client = FakeRealtimeClient()
        agent = RealtimeAgent(client, tools=[get_time, _InProcessMCPTool(_orders_server())])

        async with agent, agent.start_conversation(tools=[hang_up]):
            assert _names(client.start_session_calls[0]["tools"]) == [
                "get_time",
                "get_delivery_slots",
                "book_delivery_slot",
                "hang_up",
            ]

    async def test_a_call_to_an_mcp_function_runs_on_the_server(self) -> None:
        """A get_delivery_slots call for order 1234 sends the server's slots back, then requests a follow-up."""
        client = FakeRealtimeClient()
        agent = RealtimeAgent(client, tools=[_InProcessMCPTool(_orders_server())])

        async with agent, agent.start_conversation():
            client.session.emit(*response_events("resp_1", call_event("call_1", "get_delivery_slots", order_id="1234")))
            await eventually(lambda: bool(client.session.calls))

        [(call_id, result)] = sent_results(client.session)
        assert call_id == "call_1"
        assert "Order 1234 can be delivered on Thursday from 9 to 11 AM." in result
        assert ("create_response", None) in client.session.calls

    async def test_the_first_batch_after_start_sends_no_session_update(self) -> None:
        """After a conversation starts with MCP tools, a batch's results go out without a session update."""
        client = FakeRealtimeClient()
        agent = RealtimeAgent(client, tools=[get_time, _InProcessMCPTool(_orders_server())])

        async with agent, agent.start_conversation():
            client.session.emit(*response_events("resp_1", call_event("call_1", "get_time")))
            await eventually(lambda: bool(client.session.calls))

        assert sent_results(client.session) == [("call_1", "noon")]
        assert [call[0] for call in client.session.calls] == ["create_response"]

    async def test_a_second_conversation_reuses_the_connection(self) -> None:
        """Two conversations, one after the other, on an agent that wasn't entered open one MCP connection."""
        orders = _InProcessMCPTool(_orders_server())
        agent = RealtimeAgent(FakeRealtimeClient(), tools=[orders])

        try:
            async with agent.start_conversation():
                pass
            async with agent.start_conversation():
                pass
        finally:
            await orders.close()

        assert orders.connections_opened == 1

    async def test_an_mcp_tool_that_cannot_connect_fails_entry_before_the_session_starts(self) -> None:
        """When the MCP tool's connection fails, entering raises its ToolException and no session starts."""
        client = FakeRealtimeClient()
        agent = RealtimeAgent(client, tools=[_UnreachableMCPTool()])

        with pytest.raises(ToolException, match="unreachable"):
            async with agent.start_conversation():
                pass

        assert client.start_session_calls == []

    async def test_the_batch_after_an_mcp_update_sends_no_session_update(self) -> None:
        """After update_session(tools=[orders]), a batch's results go out without another session update."""
        client = FakeRealtimeClient()
        agent = RealtimeAgent(client)

        async with agent, agent.start_conversation() as conversation:
            await conversation.update_session(tools=[_InProcessMCPTool(_orders_server())])
            client.session.emit(*response_events("resp_1", call_event("call_1", "get_delivery_slots", order_id="1234")))
            await eventually(lambda: len(client.session.calls) == 2)

        assert [call[0] for call in client.session.calls] == ["update_session", "create_response"]

    async def test_clashing_mcp_function_names_in_an_update_raise_before_sending(self) -> None:
        """update_session with two MCP tools exposing "search" raises ValueError and sends no update."""
        client = FakeRealtimeClient()
        agent = RealtimeAgent(client)
        docs = _InProcessMCPTool(_search_server(), name="docs")
        tickets = _InProcessMCPTool(_search_server(), name="tickets")

        async with agent, agent.start_conversation() as conversation:
            with pytest.raises(ValueError, match="tool_name_prefix"):
                await conversation.update_session(tools=[docs, tickets])

        assert client.session.calls == []

    async def test_a_control_call_does_not_wait_for_an_updates_mcp_connection(self) -> None:
        """While update_session's MCP tool is still connecting, create_response() returns its response id."""
        orders = _SlowToConnectMCPTool(_orders_server())
        client = FakeRealtimeClient()
        agent = RealtimeAgent(client)

        async with agent, agent.start_conversation() as conversation:
            updating = asyncio.create_task(conversation.update_session(tools=[orders]))
            await orders.connecting.wait()

            assert await asyncio.wait_for(conversation.create_response(), 5) == "resp_1"

            orders.release.set()
            await updating

        assert [call[0] for call in client.session.calls] == ["create_response", "update_session"]

    async def test_a_server_side_tool_change_waits_for_the_next_update(self) -> None:
        """A tool the server adds mid-call gets a not-found result until update_session(tools=[orders]) declares it,
        and a call after that update runs it on the server."""
        server = _orders_server()
        orders = _InProcessMCPTool(server)
        client = FakeRealtimeClient()
        agent = RealtimeAgent(client, tools=[orders])

        def cancel_order(order_id: str) -> str:
            return f"Cancelled order {order_id}."

        async with agent, agent.start_conversation() as conversation:
            server.add_tool(cancel_order, description="Cancel an order.")
            await orders.load_tools()
            client.session.emit(
                *response_events(
                    "resp_caller", call_event("call_1", "cancel_order", response_id="resp_caller", order_id="1234")
                )
            )
            await eventually(lambda: bool(client.session.calls))

            await conversation.update_session(tools=[orders])
            # The provider answers the follow-up, which the fake session numbered resp_1, with another call.
            client.session.emit(*response_events("resp_1", call_event("call_2", "cancel_order", order_id="5678")))
            await eventually(lambda: len(client.session.calls) == 3)

        [(_, not_found), (_, cancelled)] = sent_results(client.session)
        assert "not found" in not_found
        assert "Cancelled order 5678." in cancelled
        assert [call[0] for call in client.session.calls] == ["create_response", "update_session", "create_response"]
        assert _names(client.session.calls[1][2]) == ["get_delivery_slots", "book_delivery_slot", "cancel_order"]
