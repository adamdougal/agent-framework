# Copyright (c) Microsoft. All rights reserved.

"""Calls that wait for the app in a live realtime conversation: approvals, app-run tools and hosted approvals."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from mcp.server.mcpserver import MCPServer

from agent_framework import (
    AgentResponseUpdate,
    Content,
    FunctionInvocationContext,
    FunctionTool,
    Message,
    RealtimeAgent,
    RealtimeEvent,
    function_middleware,
    tool,
)
from agent_framework._mcp import MCPTool
from agent_framework.exceptions import RealtimeConversationClosedException, UserInputRequiredException

from .realtime_helpers import FakeRealtimeClient, FakeRealtimeSession, event, eventually

_READERS: set[asyncio.Task[None]] = set()


def _call(call_id: str, name: str, *, response_id: str = "resp_1", **arguments: Any) -> RealtimeEvent:
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


class _InProcessMCPTool(MCPTool):
    """An MCP tool whose server runs in the test's process."""

    def __init__(self, server: Any, **kwargs: Any) -> None:
        super().__init__(name="orders", load_prompts=False, **kwargs)
        self._server = server

    def get_mcp_client(self) -> Any:
        return self._server


def _collect(conversation: Any) -> list[AgentResponseUpdate]:
    """Read the conversation's updates in the background into the returned list."""
    updates: list[AgentResponseUpdate] = []

    async def read() -> None:
        async for update in conversation:
            updates.append(update)

    reader = asyncio.get_running_loop().create_task(read())
    _READERS.add(reader)
    reader.add_done_callback(_READERS.discard)
    return updates


def _of_type(updates: list[AgentResponseUpdate], content_type: str) -> list[AgentResponseUpdate]:
    return [update for update in updates if any(content.type == content_type for content in update.contents)]


def _tool_messages(session: FakeRealtimeSession) -> list[Message]:
    return [message for message in session.sent if isinstance(message, Message) and message.role == "tool"]


def _results(session: FakeRealtimeSession) -> list[tuple[str | None, Any]]:
    return [(content.call_id, content.result) for message in _tool_messages(session) for content in message.contents]


# region Approvals


async def test_a_call_needing_approval_streams_one_approval_request_and_does_not_run() -> None:
    """A response holding a call to an always_require tool finishes. Exactly one function_approval_request update
    streams, role "assistant", credited to the agent, with the response's id. Its id is the call's occurrence id and
    its function_call is the call. The tool hasn't run, and no result has been sent."""
    runs: list[int] = []

    @tool(approval_mode="always_require")
    def issue_refund(amount: int) -> str:
        """Issue a refund."""
        runs.append(amount)
        return f"refunded {amount}"

    client = FakeRealtimeClient()
    call = _call("call_1", "issue_refund", amount=5)

    async with RealtimeAgent(client, tools=[issue_refund], name="billing").start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(*_response("resp_1", call))
        await eventually(lambda: bool(_of_type(updates, "function_approval_request")))
        await asyncio.sleep(0.01)

        assert runs == []
        assert _tool_messages(client.session) == []

    [request_update] = _of_type(updates, "function_approval_request")
    [request] = request_update.contents
    assert (request_update.role, request_update.author_name, request_update.response_id) == (
        "assistant",
        "billing",
        "resp_1",
    )
    assert call.content is not None
    assert request.id == call.content.id
    assert request.function_call is not None
    assert (request.function_call.call_id, request.function_call.name, request.function_call.parse_arguments()) == (
        "call_1",
        "issue_refund",
        {"amount": 5},
    )


async def _approval_request(updates: list[AgentResponseUpdate], index: int = 0) -> Content:
    await eventually(lambda: len(_of_type(updates, "function_approval_request")) > index)
    return _of_type(updates, "function_approval_request")[index].contents[0]


def _refund_tool(runs: list[int]) -> Any:
    @tool(approval_mode="always_require")
    def issue_refund(amount: int) -> str:
        """Issue a refund."""
        runs.append(amount)
        return f"refunded {amount}"

    return issue_refund


async def test_an_approved_call_runs_and_its_result_and_follow_up_go_out() -> None:
    """The app sends request.to_function_approval_response(approved=True). The tool runs, its result goes to the
    session, and the session's only control call is the follow-up create_response."""
    runs: list[int] = []
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_refund_tool(runs)]).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(*_response("resp_1", _call("call_1", "issue_refund", amount=5)))
        request = await _approval_request(updates)

        await conversation.send(request.to_function_approval_response(approved=True))
        await eventually(lambda: bool(client.session.calls))

    assert runs == [5]
    assert _results(client.session) == [("call_1", "refunded 5")]
    assert client.session.calls == [("create_response", None)]


async def test_a_rejected_call_gets_the_rejection_result_and_does_not_run() -> None:
    """Rejected, the call's result is "Error: Tool call invocation was rejected by user.", the tool never runs, and
    the follow-up is requested."""
    runs: list[int] = []
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_refund_tool(runs)]).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(*_response("resp_1", _call("call_1", "issue_refund", amount=5)))
        request = await _approval_request(updates)

        await conversation.send(request.to_function_approval_response(approved=False))
        await eventually(lambda: bool(client.session.calls))

    assert runs == []
    assert _results(client.session) == [("call_1", "Error: Tool call invocation was rejected by user.")]
    assert client.session.calls == [("create_response", None)]


async def test_an_approval_runs_the_recorded_call_whatever_the_answer_embeds() -> None:
    """The app approves with a function_approval_response whose embedded function_call has other arguments. The tool
    runs with the arguments the model sent."""
    runs: list[int] = []
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_refund_tool(runs)]).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(*_response("resp_1", _call("call_1", "issue_refund", amount=5)))
        request = await _approval_request(updates)
        tampered = Content.from_function_call(call_id="call_1", name="issue_refund", arguments={"amount": 5000})

        await conversation.send(
            Content.from_function_approval_response(approved=True, id=request.id or "", function_call=tampered)
        )
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert runs == [5]


async def test_answering_a_call_twice_raises() -> None:
    """After an approval is answered, a second answer for the same request raises ValueError."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_refund_tool([])]).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(*_response("resp_1", _call("call_1", "issue_refund", amount=5)))
        request = await _approval_request(updates)
        await conversation.send(request.to_function_approval_response(approved=True))

        with pytest.raises(ValueError):
            await conversation.send(request.to_function_approval_response(approved=True))


async def test_a_batchs_other_calls_wait_for_its_approvals() -> None:
    """A response calls a tool needing approval and a tool that doesn't. Neither runs before the approval is answered;
    then both run, and their results go out together in model order."""
    runs: list[Any] = []

    @tool
    def look_up(order_id: str) -> str:
        """Look up an order."""
        runs.append(order_id)
        return f"order {order_id} found"

    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_refund_tool(runs), look_up]).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(
            *_response("resp_1", _call("call_1", "issue_refund", amount=5), _call("call_2", "look_up", order_id="42"))
        )
        request = await _approval_request(updates)
        await asyncio.sleep(0.01)

        assert runs == []

        await conversation.send(request.to_function_approval_response(approved=True))
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert [message.role for message in _tool_messages(client.session)] == ["tool"]
    assert _results(client.session) == [("call_1", "refunded 5"), ("call_2", "order 42 found")]


async def test_two_batches_can_wait_for_approval_at_once() -> None:
    """Two responses each make an approval-requiring call. The second is approved first: only its result goes out.
    Then the first is approved: its result goes out."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_refund_tool([])]).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(*_response("resp_1", _call("call_1", "issue_refund", amount=5)))
        first = await _approval_request(updates)
        client.session.emit(*_response("resp_2", _call("call_2", "issue_refund", amount=7, response_id="resp_2")))
        second = await _approval_request(updates, 1)

        await conversation.send(second.to_function_approval_response(approved=True))
        await eventually(lambda: bool(_tool_messages(client.session)))

        assert _results(client.session) == [("call_2", "refunded 7")]

        await conversation.send(first.to_function_approval_response(approved=True))
        await eventually(lambda: len(_tool_messages(client.session)) == 2)

    assert _results(client.session) == [("call_2", "refunded 7"), ("call_1", "refunded 5")]


# region App-run calls


def _account_tool() -> FunctionTool:
    """A tool the app answers from its own state, so it has no implementation."""
    return FunctionTool(name="look_up_account", description="Look up the caller's account; the app answers.")


async def test_an_app_run_call_waits_for_the_apps_result() -> None:
    """A declaration-only tool's call gets no result when its response finishes. The app sends
    Content.from_function_result(call_id=..., result="Account 42"), which goes to the session, and the follow-up is
    requested."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_account_tool()]).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "look_up_account")))
        await asyncio.sleep(0.02)

        assert _tool_messages(client.session) == []

        await conversation.send(Content.from_function_result(call_id="call_1", result="Account 42"))
        await eventually(lambda: bool(client.session.calls))

    assert _results(client.session) == [("call_1", "Account 42")]
    assert client.session.calls == [("create_response", None)]


async def test_an_app_result_sent_before_the_response_finishes_is_used() -> None:
    """The app answers the app-run call as soon as its function_call update streams, before response_done. Once the
    response finishes, that result goes out."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_account_tool()]).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(event("response_created", response_id="resp_1"), _call("call_1", "look_up_account"))
        await eventually(lambda: bool(_of_type(updates, "function_call")))

        await conversation.send(Content.from_function_result(call_id="call_1", result="Account 42"))
        client.session.emit(event("response_done", response_id="resp_1", status="completed"))
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert _results(client.session) == [("call_1", "Account 42")]


async def test_an_answer_for_an_app_run_call_whose_response_did_not_complete_raises() -> None:
    """The response holding an app-run call is cancelled, so the call settles without waiting. The app's later result
    for it raises ValueError."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_account_tool()]).start_conversation() as conversation:
        client.session.emit(
            event("response_created", response_id="resp_1"),
            _call("call_1", "look_up_account"),
            event("response_done", response_id="resp_1", status="cancelled"),
        )
        await eventually(lambda: bool(_tool_messages(client.session)))

        with pytest.raises(ValueError):
            await conversation.send(Content.from_function_result(call_id="call_1", result="Account 42"))


async def test_a_batchs_other_calls_run_without_waiting_for_an_app_run_call() -> None:
    """A response calls an app-run tool and look_up. look_up runs while the app-run call still waits; both results go
    out together once the app answers."""
    looked_up = asyncio.Event()

    @tool
    def look_up(order_id: str) -> str:
        """Look up an order."""
        looked_up.set()
        return f"order {order_id} found"

    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_account_tool(), look_up]).start_conversation() as conversation:
        client.session.emit(
            *_response("resp_1", _call("call_1", "look_up_account"), _call("call_2", "look_up", order_id="42"))
        )
        await asyncio.wait_for(looked_up.wait(), 5)

        assert _tool_messages(client.session) == []

        await conversation.send(Content.from_function_result(call_id="call_1", result="Account 42"))
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert _results(client.session) == [("call_1", "Account 42"), ("call_2", "order 42 found")]


async def test_an_app_result_with_an_occurrence_id_answers_that_call() -> None:
    """Two responses each make an app-run call with call_id "call_1". A result carrying the second call's occurrence id
    answers the second call, and the first still waits."""
    client = FakeRealtimeClient()
    first = _call("call_1", "look_up_account")
    second = _call("call_1", "look_up_account", response_id="resp_2")

    async with RealtimeAgent(client, tools=[_account_tool()]).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", first), *_response("resp_2", second))
        await eventually(lambda: second.content is not None and second.content.id is not None)
        assert first.content is not None and second.content is not None

        await conversation.send(
            Content.from_function_result(call_id="call_1", id=second.content.id, result="second account")
        )
        await eventually(lambda: bool(_tool_messages(client.session)))

        assert _results(client.session) == [("call_1", "second account")]

        await conversation.send(
            Content.from_function_result(call_id="call_1", id=first.content.id, result="first account")
        )
        await eventually(lambda: len(_tool_messages(client.session)) == 2)

    assert _results(client.session) == [("call_1", "second account"), ("call_1", "first account")]


async def test_close_keeps_a_result_the_app_sent_while_another_call_waits() -> None:
    """Two app-run calls wait; the app answers the first, then the call closes. The first keeps the app's result, and
    only the second settles as conversation_closed."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_account_tool()]).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(
            *_response("resp_1", _call("call_1", "look_up_account"), _call("call_2", "look_up_account"))
        )
        await eventually(lambda: len(_of_type(updates, "function_call")) == 2)
        await conversation.send(Content.from_function_result(call_id="call_1", result="Account 42"))

    await eventually(lambda: len(_of_type(updates, "function_result")) == 2)
    settled = {update.contents[0].call_id: update.contents[0] for update in _of_type(updates, "function_result")}
    assert (settled["call_1"].result, settled["call_1"].exception) == ("Account 42", None)
    assert settled["call_2"].exception == "conversation_closed"


async def test_an_app_run_tool_that_needs_approval_uses_the_result_the_app_sent() -> None:
    """A declaration-only tool with approval_mode="always_require" is approved, and the app sends its result. That
    result goes out, and the call doesn't stream again."""
    client = FakeRealtimeClient()
    transfer = FunctionTool(
        name="transfer_to_person", description="Transfer the caller; the app does it.", approval_mode="always_require"
    )

    async with RealtimeAgent(client, tools=[transfer]).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(*_response("resp_1", _call("call_1", "transfer_to_person")))
        request = await _approval_request(updates)
        await conversation.send([
            request.to_function_approval_response(approved=True),
            Content.from_function_result(call_id="call_1", result="transferred"),
        ])
        await eventually(lambda: bool(_tool_messages(client.session)))

    assert _results(client.session) == [("call_1", "transferred")]
    assert len(_of_type(updates, "function_call")) == 1


async def test_a_send_answering_one_call_twice_raises_and_answers_nothing() -> None:
    """One send holds the same approval twice. It raises ValueError, the call stays waiting, and nothing is sent."""
    runs: list[int] = []
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_refund_tool(runs)]).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(*_response("resp_1", _call("call_1", "issue_refund", amount=5)))
        request = await _approval_request(updates)
        approval = request.to_function_approval_response(approved=True)

        with pytest.raises(ValueError):
            await conversation.send(Message("user", [approval, approval]))
        await asyncio.sleep(0.01)

        assert runs == []
        assert _tool_messages(client.session) == []


async def test_a_result_naming_an_answered_occurrence_raises_though_another_call_shares_its_call_id() -> None:
    """Two app-run calls share call_id "call_1". After the first is answered by its occurrence id, the same result
    again raises ValueError instead of answering the second."""
    client = FakeRealtimeClient()
    first = _call("call_1", "look_up_account")
    second = _call("call_1", "look_up_account", response_id="resp_2")

    async with RealtimeAgent(client, tools=[_account_tool()]).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", first), *_response("resp_2", second))
        await eventually(lambda: second.content is not None and second.content.id is not None)
        assert first.content is not None
        answer = Content.from_function_result(call_id="call_1", id=first.content.id, result="first account")
        await conversation.send(answer)

        with pytest.raises(ValueError):
            await conversation.send(answer)


async def test_close_keeps_a_result_the_app_sent_while_a_sibling_tool_runs() -> None:
    """An app-run call and a slow tool share a batch. The app answers while the slow tool runs, then the call closes:
    the app-run call keeps the app's result."""
    started = asyncio.Event()

    @tool
    async def look_up(order_id: str) -> str:
        """Look up an order, slowly."""
        started.set()
        await asyncio.Event().wait()
        return "found"

    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_account_tool(), look_up]).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(
            *_response("resp_1", _call("call_1", "look_up_account"), _call("call_2", "look_up", order_id="42"))
        )
        await asyncio.wait_for(started.wait(), 5)
        await conversation.send(Content.from_function_result(call_id="call_1", result="Account 42"))

    await eventually(lambda: len(_of_type(updates, "function_result")) == 2)
    settled = {update.contents[0].call_id: update.contents[0] for update in _of_type(updates, "function_result")}
    assert (settled["call_1"].result, settled["call_1"].exception) == ("Account 42", None)
    assert settled["call_2"].exception == "conversation_closed"


async def test_close_keeps_a_rejection_the_app_sent_while_a_sibling_approval_waits() -> None:
    """Two calls in one batch need approval. The app rejects the first, then the call closes with the second
    unanswered: the first keeps its rejection result and the second gets conversation_closed."""
    runs: list[int] = []
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_refund_tool(runs)]).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(
            *_response("resp_1", _call("call_1", "issue_refund", amount=5), _call("call_2", "issue_refund", amount=7))
        )
        first = await _approval_request(updates)
        await _approval_request(updates, 1)
        await conversation.send(first.to_function_approval_response(approved=False))

    await eventually(lambda: len(_of_type(updates, "function_result")) == 2)
    settled = {update.contents[0].call_id: update.contents[0] for update in _of_type(updates, "function_result")}
    assert settled["call_1"].result == "Error: Tool call invocation was rejected by user."
    assert settled["call_2"].exception == "conversation_closed"
    assert runs == []


@pytest.mark.parametrize("answering", ["an approval for the app-run call", "a result for the approval call"])
async def test_an_answer_of_the_wrong_kind_raises(answering: str) -> None:
    """An approval response for a call waiting for the app's result, and a function result for a call waiting for
    approval, each raise ValueError, and nothing reaches the session."""
    client = FakeRealtimeClient()
    approval_call = _call("call_1", "issue_refund", amount=5)
    app_run_call = _call("call_2", "look_up_account")

    async with RealtimeAgent(client, tools=[_refund_tool([]), _account_tool()]).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(*_response("resp_1", approval_call, app_run_call))
        await _approval_request(updates)
        assert app_run_call.content is not None
        if answering == "an approval for the app-run call":
            answer = Content.from_function_approval_response(
                approved=True, id=app_run_call.content.id or "", function_call=app_run_call.content
            )
        else:
            answer = Content.from_function_result(call_id="call_1", result="refunded")
        sent_before = list(client.session.sent)

        with pytest.raises(ValueError):
            await conversation.send(answer)

        assert client.session.sent == sent_before


async def test_a_tool_needing_user_input_streams_its_request_and_waits_for_the_apps_result() -> None:
    """A tool raises UserInputRequiredException with an oauth_consent_request. That content streams in an assistant
    update, no result goes out, and the app's function result for the call goes out with the follow-up."""

    @tool
    def read_calendar() -> str:
        """Read the caller's calendar."""
        raise UserInputRequiredException([Content.from_oauth_consent_request("https://consent.example")])

    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[read_calendar]).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(*_response("resp_1", _call("call_1", "read_calendar")))
        await eventually(lambda: bool(_of_type(updates, "oauth_consent_request")))
        await asyncio.sleep(0.01)

        assert _tool_messages(client.session) == []

        await conversation.send(Content.from_function_result(call_id="call_1", result="consented; free at 3pm"))
        await eventually(lambda: bool(client.session.calls))

    [request_update] = _of_type(updates, "oauth_consent_request")
    assert request_update.role == "assistant"
    assert request_update.contents[0].consent_link == "https://consent.example"
    assert _results(client.session) == [("call_1", "consented; free at 3pm")]
    assert client.session.calls == [("create_response", None)]


async def test_text_sent_with_an_answer_is_handled_as_input() -> None:
    """One send holds the app's result for a waiting call and user text. The result goes to its batch, and the text is
    added to the provider's conversation and requests a response."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[_account_tool()]).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "look_up_account")))
        await asyncio.sleep(0.01)

        await conversation.send(
            Message("user", [Content.from_function_result(call_id="call_1", result="Account 42"), "Thanks, go on."])
        )
        await eventually(lambda: bool(_tool_messages(client.session)))

    user_texts = [
        message.text for message in client.session.sent if isinstance(message, Message) and message.role == "user"
    ]
    assert user_texts == ["Thanks, go on."]
    assert _results(client.session) == [("call_1", "Account 42")]
    assert ("create_response", None) in client.session.calls


async def test_approval_waits_count_toward_max_duration() -> None:
    """With max_duration_seconds=0.05, the app approves after 0.1 s. The follow-up is requested with
    tool_choice "none"."""
    client = FakeRealtimeClient()
    agent = RealtimeAgent(
        client, tools=[_refund_tool([])], function_invocation_configuration={"max_duration_seconds": 0.05}
    )

    async with agent.start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(*_response("resp_1", _call("call_1", "issue_refund", amount=5)))
        request = await _approval_request(updates)
        await asyncio.sleep(0.1)

        await conversation.send(request.to_function_approval_response(approved=True))
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("create_response", {"tool_choice": "none"})]


# region Hosted approvals


def _hosted_request(request_id: str = "mcpr_1", call_item_id: str = "mcp_1") -> RealtimeEvent:
    """A provider's approval request for a hosted MCP call, linked to the call item the provider runs."""
    call = Content.from_function_call(
        call_id=call_item_id,
        name="microsoft_docs_search",
        arguments={"query": "Voice Live"},
        hosted=True,
        server_name="mslearn",
    )
    return event(
        "function_approval_request",
        content=Content.from_function_approval_request(id=request_id, function_call=call),
    )


async def test_a_hosted_approval_request_streams_as_an_assistant_update() -> None:
    """A function_approval_request event whose function_call is hosted streams with role "assistant", and nothing is
    sent or run."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, name="researcher").start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(_hosted_request())
        await eventually(lambda: bool(_of_type(updates, "function_approval_request")))

    [request_update] = _of_type(updates, "function_approval_request")
    assert (request_update.role, request_update.author_name) == ("assistant", "researcher")
    assert client.session.sent == []
    assert client.session.calls == []


async def _hosted_request_streamed(client: FakeRealtimeClient, updates: list[AgentResponseUpdate]) -> Content:
    client.session.emit(_hosted_request())
    return await _approval_request(updates)


def _hosted_answers(session: FakeRealtimeSession) -> list[tuple[str | None, bool | None]]:
    return [
        (content.id, content.approved)
        for message in session.sent
        if isinstance(message, Message)
        for content in message.contents
        if content.type == "function_approval_response"
    ]


async def test_approving_a_hosted_request_sends_the_answer_to_the_session() -> None:
    """The app's hosted function_approval_response goes to the session, as sent."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        updates = _collect(conversation)
        request = await _hosted_request_streamed(client, updates)

        await conversation.send(request.to_function_approval_response(approved=True))

    assert _hosted_answers(client.session) == [("mcpr_1", True)]


async def test_a_hosted_approvals_follow_up_waits_for_its_call_to_finish() -> None:
    """After approving a request whose function_call.call_id is "mcp_1", no response is requested until a
    hosted_call_done for item "mcp_1" arrives; then one follow-up is requested."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        updates = _collect(conversation)
        request = await _hosted_request_streamed(client, updates)
        await conversation.send(request.to_function_approval_response(approved=True))
        await asyncio.sleep(0.02)

        assert client.session.calls == []

        client.session.emit(event("hosted_call_done", item_id="mcp_1", status="completed"))
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("create_response", None)]


async def test_a_hosted_call_done_streams_as_an_assistant_update() -> None:
    """A hosted_call_done event streams with role "assistant", like the response that made the call."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(event("hosted_call_done", item_id="mcp_1", status="completed"))
        await eventually(lambda: bool(updates))

    assert updates[0].role == "assistant"


async def test_hosted_calls_approved_together_get_one_follow_up_once_all_are_done() -> None:
    """One send approves requests for "mcp_1" and "mcp_2". The first hosted_call_done requests nothing; the second
    requests one follow-up."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(_hosted_request("mcpr_1", "mcp_1"), _hosted_request("mcpr_2", "mcp_2"))
        first = await _approval_request(updates)
        second = await _approval_request(updates, 1)
        await conversation.send([
            first.to_function_approval_response(approved=True),
            second.to_function_approval_response(approved=True),
        ])

        client.session.emit(event("hosted_call_done", item_id="mcp_1", status="completed"))
        await asyncio.sleep(0.02)

        assert client.session.calls == []

        client.session.emit(event("hosted_call_done", item_id="mcp_2", status="completed"))
        await eventually(lambda: bool(client.session.calls))
        await asyncio.sleep(0.02)

    assert client.session.calls == [("create_response", None)]


async def test_another_hosted_calls_completion_does_not_release_the_follow_up() -> None:
    """After approving the call "mcp_1", a hosted_call_done for "mcp_2" requests nothing."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        updates = _collect(conversation)
        request = await _hosted_request_streamed(client, updates)
        await conversation.send(request.to_function_approval_response(approved=True))

        client.session.emit(event("hosted_call_done", item_id="mcp_2", status="completed"))
        await eventually(lambda: len(_of_type(updates, "realtime_event")) >= 1)
        await asyncio.sleep(0.02)

    assert client.session.calls == []


async def test_approving_an_unlinked_hosted_request_follows_up_at_once() -> None:
    """When the request's function_call.call_id is the request's own id, approving it requests the follow-up without
    waiting."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        updates = _collect(conversation)
        client.session.emit(_hosted_request(request_id="mcpr_1", call_item_id="mcpr_1"))
        request = await _approval_request(updates)

        await conversation.send(request.to_function_approval_response(approved=True))
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("create_response", None)]


async def test_rejecting_a_hosted_request_sends_a_note_then_follows_up() -> None:
    """Rejecting microsoft_docs_search on mslearn sends the rejection, then a system message reading "The user declined
    the call to microsoft_docs_search on mslearn, so it did not run.", then requests the follow-up."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        updates = _collect(conversation)
        request = await _hosted_request_streamed(client, updates)

        await conversation.send(request.to_function_approval_response(approved=False))
        await eventually(lambda: bool(client.session.calls))

    sent = [message for message in client.session.sent if isinstance(message, Message)]
    assert [(message.role, message.contents[0].type) for message in sent] == [
        ("user", "function_approval_response"),
        ("system", "text"),
    ]
    assert sent[1].text == "The user declined the call to microsoft_docs_search on mslearn, so it did not run."
    assert client.session.calls == [("create_response", None)]


class TestVerification:
    async def test_approved_and_other_calls_run_in_model_order_without_concurrent_invocation(self) -> None:
        """With allow_concurrent_invocation=False, a response calls look_up (no approval), then issue_refund
        (approval). After the approval, they run in that order: look_up, then issue_refund. Relies on the existing
        model-order test and on approved calls joining the batch's execution list (approving a call)."""
        runs: list[Any] = []

        @tool
        def look_up(order_id: str) -> str:
            """Look up an order."""
            runs.append(order_id)
            return "found"

        client = FakeRealtimeClient()
        agent = RealtimeAgent(
            client,
            tools=[_refund_tool(runs), look_up],
            function_invocation_configuration={"allow_concurrent_invocation": False},
        )

        async with agent.start_conversation() as conversation:
            updates = _collect(conversation)
            client.session.emit(
                *_response(
                    "resp_1", _call("call_1", "look_up", order_id="42"), _call("call_2", "issue_refund", amount=5)
                )
            )
            request = await _approval_request(updates)
            await conversation.send(request.to_function_approval_response(approved=True))
            await eventually(lambda: bool(_tool_messages(client.session)))

        assert runs == ["42", 5]

    async def test_an_answer_does_not_restart_the_tool_chain(self) -> None:
        """With max_function_calls=1, the approved call uses the chain's budget. A call in the follow-up's response
        doesn't run. Relies on approving a call."""
        runs: list[Any] = []

        @tool
        def look_up(order_id: str) -> str:
            """Look up an order."""
            runs.append(order_id)
            return "found"

        client = FakeRealtimeClient()
        agent = RealtimeAgent(
            client, tools=[_refund_tool(runs), look_up], function_invocation_configuration={"max_function_calls": 1}
        )

        async with agent.start_conversation() as conversation:
            updates = _collect(conversation)
            client.session.emit(*_response("resp_1", _call("call_1", "issue_refund", amount=5)))
            request = await _approval_request(updates)
            await conversation.send(request.to_function_approval_response(approved=True))
            await eventually(lambda: bool(client.session.calls))
            client.session.emit(*_response("resp_2", _call("call_2", "look_up", order_id="42", response_id="resp_2")))
            await eventually(lambda: len(_tool_messages(client.session)) == 2)

        assert runs == [5]
        assert str(_results(client.session)[1][1]).startswith("Error:")

    async def test_answering_while_a_response_is_active_does_not_raise(self) -> None:
        """An answer requests no response of its own. Relies on approving a call."""
        client = FakeRealtimeClient()

        async with RealtimeAgent(client, tools=[_refund_tool([])]).start_conversation() as conversation:
            updates = _collect(conversation)
            client.session.emit(*_response("resp_1", _call("call_1", "issue_refund", amount=5)))
            request = await _approval_request(updates)
            client.session.emit(event("response_created", response_id="resp_2"))
            await eventually(lambda: bool(_of_type(updates, "realtime_event")) and updates[-1].response_id == "resp_2")

            await conversation.send(request.to_function_approval_response(approved=True))
            await eventually(lambda: bool(_tool_messages(client.session)))
            await asyncio.sleep(0.01)

            assert client.session.calls == []

            client.session.emit(event("response_done", response_id="resp_2", status="completed"))
            await eventually(lambda: bool(client.session.calls))

    async def test_a_local_mcp_function_that_needs_approval_asks_for_it(self) -> None:
        """An MCPTool with approval_mode="always_require" makes its function's call stream an approval request, and
        run once approved. Relies on the approval tests."""
        server = MCPServer("orders")

        @server.tool()
        def cancel_order(order_id: str) -> str:
            """Cancel an order."""
            return f"Cancelled order {order_id}."

        orders = _InProcessMCPTool(server, approval_mode="always_require")
        client = FakeRealtimeClient()

        async with RealtimeAgent(client, tools=[orders]).start_conversation() as conversation:
            updates = _collect(conversation)
            client.session.emit(*_response("resp_1", _call("call_1", "cancel_order", order_id="42")))
            request = await _approval_request(updates)

            assert _tool_messages(client.session) == []

            await conversation.send(request.to_function_approval_response(approved=True))
            await eventually(lambda: bool(_tool_messages(client.session)))

        assert "Cancelled order 42" in str(_results(client.session)[0][1])

    async def test_an_additional_tools_call_waits_for_the_apps_result(self) -> None:
        """A tool named in additional_tools is answered by the app. Relies on the app-run tests."""
        runs: list[str] = []

        @tool
        def transfer_to_agent() -> str:
            """Transfer the caller to a person."""
            runs.append("ran")
            return "transferred"

        client = FakeRealtimeClient()
        agent = RealtimeAgent(
            client,
            tools=[transfer_to_agent],
            function_invocation_configuration={"additional_tools": [transfer_to_agent]},
        )

        async with agent.start_conversation() as conversation:
            client.session.emit(*_response("resp_1", _call("call_1", "transfer_to_agent")))
            await asyncio.sleep(0.01)
            await conversation.send(Content.from_function_result(call_id="call_1", result="queued for a person"))
            await eventually(lambda: bool(_tool_messages(client.session)))

        assert runs == []
        assert _results(client.session) == [("call_1", "queued for a person")]

    async def test_an_approval_request_from_middleware_settles_the_call_with_an_error(self) -> None:
        """The call's result is an error, and it goes out with the follow-up."""

        @tool
        def look_up(order_id: str) -> str:
            """Look up an order."""
            return "found"

        @function_middleware
        async def ask_first(context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
            context.result = Content.from_function_approval_request(
                id="policy_1", function_call=Content.from_function_call(call_id="call_1", name="look_up")
            )

        client = FakeRealtimeClient()

        async with RealtimeAgent(client, tools=[look_up], middleware=[ask_first]).start_conversation():
            client.session.emit(*_response("resp_1", _call("call_1", "look_up", order_id="42")))
            await eventually(lambda: bool(client.session.calls))

        assert str(_results(client.session)[0][1]).startswith("Error:")

    async def test_a_response_created_while_a_batch_waits_is_not_its_follow_up(self) -> None:
        """Relies on approving a call."""
        client = FakeRealtimeClient()

        async with RealtimeAgent(client, tools=[_refund_tool([])]).start_conversation() as conversation:
            updates = _collect(conversation)
            client.session.emit(*_response("resp_1", _call("call_1", "issue_refund", amount=5)))
            request = await _approval_request(updates)
            client.session.emit(*_response("resp_2"))

            await conversation.send(request.to_function_approval_response(approved=True))
            await eventually(lambda: bool(client.session.calls))

        assert client.session.calls == [("create_response", None)]

    async def test_the_caller_speaking_leaves_an_approval_request_open(self) -> None:
        """Relies on approving a call."""
        client = FakeRealtimeClient()

        async with RealtimeAgent(client, tools=[_refund_tool([])]).start_conversation() as conversation:
            updates = _collect(conversation)
            client.session.emit(*_response("resp_1", _call("call_1", "issue_refund", amount=5)))
            request = await _approval_request(updates)
            client.session.emit(
                event("speech_started", item_id="item_caller", audio_start_ms=0),
                event("speech_stopped", item_id="item_caller", audio_end_ms=500),
            )

            await conversation.send(request.to_function_approval_response(approved=True))
            await eventually(lambda: bool(_tool_messages(client.session)))

        assert _results(client.session) == [("call_1", "refunded 5")]

    async def test_removing_a_waiting_calls_tool_does_not_stop_it(self) -> None:
        """Relies on approving a call."""
        runs: list[int] = []
        client = FakeRealtimeClient()

        async with RealtimeAgent(client, tools=[_refund_tool(runs)]).start_conversation() as conversation:
            updates = _collect(conversation)
            client.session.emit(*_response("resp_1", _call("call_1", "issue_refund", amount=5)))
            request = await _approval_request(updates)
            await conversation.update_session(tools=[])

            await conversation.send(request.to_function_approval_response(approved=True))
            await eventually(lambda: bool(_tool_messages(client.session)))

        assert runs == [5]

    async def test_close_settles_waiting_calls_as_conversation_closed(self) -> None:
        """An approval call and an app-run call wait. Each streams a result whose exception is
        "conversation_closed", and none is sent. Relies on the approval and app-run tests."""
        client = FakeRealtimeClient()

        async with RealtimeAgent(
            client, tools=[_refund_tool([]), _account_tool()]
        ).start_conversation() as conversation:
            updates = _collect(conversation)
            client.session.emit(
                *_response("resp_1", _call("call_1", "issue_refund", amount=5), _call("call_2", "look_up_account"))
            )
            await _approval_request(updates)

        await eventually(lambda: len(_of_type(updates, "function_result")) == 2)
        settled = [update.contents[0] for update in _of_type(updates, "function_result")]
        assert {(result.call_id, result.exception) for result in settled} == {
            ("call_1", "conversation_closed"),
            ("call_2", "conversation_closed"),
        }
        assert _tool_messages(client.session) == []

    async def test_an_answer_after_close_raises(self) -> None:
        """Relies on approving a call."""
        client = FakeRealtimeClient()

        async with RealtimeAgent(client, tools=[_refund_tool([])]).start_conversation() as conversation:
            updates = _collect(conversation)
            client.session.emit(*_response("resp_1", _call("call_1", "issue_refund", amount=5)))
            request = await _approval_request(updates)
            await conversation.close()

            with pytest.raises(RealtimeConversationClosedException):
                await conversation.send(request.to_function_approval_response(approved=True))

    async def test_history_holds_a_waiting_call_with_its_result_and_no_approval_contents(self) -> None:
        """Relies on approving a call."""
        client = FakeRealtimeClient()

        async with RealtimeAgent(client, tools=[_refund_tool([])]).start_conversation() as conversation:
            updates = _collect(conversation)
            client.session.emit(
                event("response_created", response_id="resp_1"),
                event("item_added", item_id="item_call_1", previous_item_id=None),
                _call("call_1", "issue_refund", amount=5),
                event("response_done", response_id="resp_1", status="completed"),
            )
            request = await _approval_request(updates)
            await conversation.send(request.to_function_approval_response(approved=True))
            await eventually(lambda: bool(_tool_messages(client.session)))

        contents = [content for message in conversation.history for content in message.contents]
        assert [content.type for content in contents] == ["function_call", "function_result"]
        assert contents[1].result == "refunded 5"

    async def test_a_response_before_the_hosted_call_is_done_is_not_its_follow_up(self) -> None:
        """Relies on the hosted follow-up waiting for its call."""
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            updates = _collect(conversation)
            request = await _hosted_request_streamed(client, updates)
            await conversation.send(request.to_function_approval_response(approved=True))
            client.session.emit(*_response("resp_2"))
            client.session.emit(event("hosted_call_done", item_id="mcp_1", status="completed"))
            await eventually(lambda: bool(client.session.calls))

        assert client.session.calls == [("create_response", None)]

    async def test_an_answer_for_an_unknown_hosted_request_raises(self) -> None:
        """Relies on hosted answers and on routing that refuses unknown ids."""
        client = FakeRealtimeClient()
        call = Content.from_function_call(call_id="mcp_9", name="microsoft_docs_search", hosted=True)

        async with RealtimeAgent(client).start_conversation() as conversation:
            with pytest.raises(ValueError):
                await conversation.send(Content.from_function_approval_response(True, "mcpr_9", call))

        assert client.session.sent == []

    async def test_answering_a_hosted_request_twice_raises(self) -> None:
        """Relies on answering twice and hosted answers."""
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            updates = _collect(conversation)
            request = await _hosted_request_streamed(client, updates)
            await conversation.send(request.to_function_approval_response(approved=True))

            with pytest.raises(ValueError):
                await conversation.send(request.to_function_approval_response(approved=True))

    async def test_close_drops_an_owed_hosted_follow_up(self) -> None:
        """Relies on the hosted follow-up waiting for its call."""
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            updates = _collect(conversation)
            request = await _hosted_request_streamed(client, updates)
            await conversation.send(request.to_function_approval_response(approved=True))

        assert client.session.calls == []

    async def test_hosted_declarations_pass_entry_and_updates_untouched(self) -> None:
        """A hosted MCP dict reaches start_session and update_session as given, beside a function tool."""
        learn = {"type": "mcp", "server_label": "mslearn", "server_url": "https://learn.microsoft.com/api/mcp"}
        account = _account_tool()
        client = FakeRealtimeClient()

        async with RealtimeAgent(client, tools=[account, learn]).start_conversation() as conversation:
            await conversation.update_session(tools=[learn])

        assert client.start_session_calls[0]["tools"] == [account, learn]
        assert client.session.calls == [("update_session", {}, [learn])]

    async def test_a_hosted_rejections_note_stays_out_of_history(self) -> None:
        """Relies on the hosted rejection test."""
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            updates = _collect(conversation)
            request = await _hosted_request_streamed(client, updates)
            await conversation.send(request.to_function_approval_response(approved=False))
            await eventually(lambda: bool(client.session.calls))

        assert not [message for message in conversation.history if "declined" in message.text]

    async def test_a_call_needing_approval_in_a_disabled_loop_gets_the_disabled_error_without_asking(self) -> None:
        """With function invocation disabled, a call to an always_require tool settles with the disabled error, and no
        approval request streams. Relies on approvals being asked only for calls that can run."""
        client = FakeRealtimeClient()
        agent = RealtimeAgent(client, tools=[_refund_tool([])], function_invocation_configuration={"enabled": False})

        async with agent.start_conversation() as conversation:
            updates = _collect(conversation)
            client.session.emit(*_response("resp_1", _call("call_1", "issue_refund", amount=5)))
            await eventually(lambda: bool(_tool_messages(client.session)))

        assert _results(client.session) == [("call_1", "Error: function invocation is disabled.")]
        assert _of_type(updates, "function_approval_request") == []
