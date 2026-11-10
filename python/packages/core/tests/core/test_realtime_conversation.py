# Copyright (c) Microsoft. All rights reserved.

"""The live realtime conversation: updates, the update queue, send, control calls and close (spec 006)."""

import asyncio
from typing import Any

import pytest

from agent_framework import AgentResponseUpdate, Content, Message, RealtimeAgent, RealtimeEvent
from agent_framework.exceptions import (
    RealtimeActiveResponseException,
    RealtimeBackpressureException,
    RealtimeConnectionException,
    RealtimeControlException,
    RealtimeControlTimeoutException,
    RealtimeConversationClosedException,
)

from .realtime_helpers import FakeRealtimeClient, FakeRealtimeSession, event, eventually, next_update

# region Updates


async def test_a_signal_becomes_one_update_with_realtime_event_content() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(
            event(
                "speech_started",
                service_event_type="input_audio_buffer.speech_started",
                item_id="item_1",
                audio_start_ms=120,
            )
        )
        update = await next_update(conversation)

    content = update.contents[0]
    assert (content.type, content.kind, content.service_event_type) == (
        "realtime_event",
        "speech_started",
        "input_audio_buffer.speech_started",
    )
    assert content.additional_properties["core"] == {"item_id": "item_1", "audio_start_ms": 120}
    assert update.role == "user"


async def test_a_payload_event_streams_its_own_content() -> None:
    client = FakeRealtimeClient()
    transcript = Content.from_text("Hello there", merge_mode="replace", status="completed")

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event("input_transcription", content=transcript, item_id="item_1", content_index=0))
        update = await next_update(conversation)

    assert update.contents == [transcript]
    assert update.role == "user"


@pytest.mark.parametrize(
    ("kind", "core", "role"),
    [
        ("speech_started", {"item_id": "item_1", "audio_start_ms": 0}, "user"),
        ("speech_stopped", {"item_id": "item_1", "audio_end_ms": 900}, "user"),
        ("input_transcription", {"item_id": "item_1", "content_index": 0}, "user"),
        ("input_transcription_failed", {"item_id": "item_1", "content_index": 0}, "user"),
        ("response_created", {"response_id": "resp_1"}, "assistant"),
        ("response_done", {"response_id": "resp_1", "status": "completed"}, "assistant"),
        (
            "item_truncated",
            {"item_id": "item_2", "content_index": 0, "audio_end_ms": 10, "truncated_by": "provider"},
            "assistant",
        ),
        ("output_text", {"response_id": "resp_1", "item_id": "item_2", "content_index": 0}, "assistant"),
        ("output_audio", {"response_id": "resp_1", "item_id": "item_2", "content_index": 0}, "assistant"),
        ("function_call", {"response_id": "resp_1", "item_id": "item_3"}, "assistant"),
        ("usage", {"response_id": "resp_1"}, "assistant"),
        ("error", {"response_id": "resp_1"}, "assistant"),
        ("error", {}, None),
        ("session_created", {"session_id": "sess_1"}, None),
        ("session_updated", {}, None),
    ],
)
async def test_each_kind_streams_with_its_role(kind: str, core: dict[str, Any], role: str | None) -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event(kind, **core))
        update = await next_update(conversation)

    assert update.role == role


@pytest.mark.parametrize(("item_role", "role"), [({"role": "user"}, "user"), ({}, None)])
async def test_item_added_takes_the_role_of_its_item(item_role: dict[str, Any], role: str | None) -> None:
    client = FakeRealtimeClient()
    core: dict[str, Any] = {"item_id": "item_1", "previous_item_id": None, **item_role}

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event("item_added", **core))
        update = await next_update(conversation)

    assert update.role == role


async def test_updates_carry_the_response_and_item_ids() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(
            event(
                "output_text",
                content=Content.from_text("Hi", merge_mode="append"),
                response_id="resp_1",
                item_id="item_1",
                content_index=0,
            )
        )
        update = await next_update(conversation)

    assert (update.response_id, update.message_id) == ("resp_1", "item_1")


async def test_assistant_updates_carry_the_author_of_the_responses_agent() -> None:
    client = FakeRealtimeClient()
    agent = RealtimeAgent(client, name="receptionist")

    async with agent.start_conversation() as conversation:
        client.session.emit(
            event("response_created", response_id="resp_1"),
            event(
                "output_text",
                content=Content.from_text("Hi", merge_mode="append"),
                response_id="resp_1",
                item_id="item_2",
                content_index=0,
            ),
            event(
                "input_transcription",
                content=Content.from_text("Hello", merge_mode="replace", status="completed"),
                item_id="item_1",
                content_index=0,
            ),
        )
        updates = [await next_update(conversation) for _ in range(3)]

    assert [(update.author_name, update.agent_id) for update in updates] == [
        ("receptionist", agent.id),
        ("receptionist", agent.id),
        (None, None),
    ]


async def test_a_completed_response_finishes_with_stop() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(
            event("response_created", response_id="resp_1"),
            event("response_done", response_id="resp_1", status="completed"),
        )
        updates = [await next_update(conversation) for _ in range(2)]

    assert updates[-1].finish_reason == "stop"


async def test_a_completed_response_with_calls_finishes_with_tool_calls() -> None:
    client = FakeRealtimeClient()
    call = Content.from_function_call(call_id="call_1", name="get_weather", arguments={"city": "Paris"})

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(
            event("response_created", response_id="resp_1"),
            event("function_call", content=call, response_id="resp_1", item_id="item_3"),
            event("response_done", response_id="resp_1", status="completed"),
        )
        updates = [await next_update(conversation) for _ in range(3)]

    assert updates[-1].finish_reason == "tool_calls"


@pytest.mark.parametrize(
    ("reason", "finish_reason"),
    [("content_filter", "content_filter"), ("max_output_tokens", "length"), ("something_else", None)],
)
async def test_an_incomplete_response_finishes_with_its_reason(reason: str, finish_reason: str | None) -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event("response_done", response_id="resp_1", status="incomplete", reason=reason))
        update = await next_update(conversation)

    assert update.finish_reason == finish_reason


async def test_an_update_keeps_its_realtime_event_as_raw_representation() -> None:
    client = FakeRealtimeClient()
    signal = event("session_updated")

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(signal)
        update = await next_update(conversation)

    assert update.raw_representation is signal


def _provider_error(message: str = "Invalid value", code: str = "invalid_value", param: str = "session.voice") -> Any:
    return event(
        "error",
        content=Content.from_error(
            message=message,
            error_code=code,
            additional_properties={"foundry": {"type": "invalid_request_error", "param": param}},
        ),
    )


def _kinds(updates: list[AgentResponseUpdate]) -> list[str | None]:
    return [content.kind or content.type for update in updates for content in update.contents]


async def test_back_to_back_identical_errors_stream_once() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(_provider_error(), _provider_error(), event("session_updated"))
        updates = [await next_update(conversation) for _ in range(2)]

    assert _kinds(updates) == ["error", "session_updated"]


@pytest.mark.parametrize(
    "difference", [{"message": "Something else"}, {"code": "server_error"}, {"param": "session.instructions"}]
)
async def test_different_errors_both_stream(difference: dict[str, str]) -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(_provider_error(), _provider_error(**difference), event("session_updated"))
        updates = [await next_update(conversation) for _ in range(3)]

    assert _kinds(updates) == ["error", "error", "session_updated"]


# endregion

# region Update queue


async def _collect(conversation: Any, timeout: float = 1.0) -> list[AgentResponseUpdate]:
    async def read_all() -> list[AgentResponseUpdate]:
        return [update async for update in conversation]

    return await asyncio.wait_for(read_all(), timeout)


async def test_iteration_ends_after_the_session_ends_and_updates_drain() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event("session_updated"), event("session_updated"))
        client.session.end()
        updates = await _collect(conversation)

    assert _kinds(updates) == ["session_updated", "session_updated"]


async def _read_into(received: list[AgentResponseUpdate], conversation: Any, timeout: float = 1.0) -> None:
    async def read_all() -> None:
        async for update in conversation:
            received.append(update)

    await asyncio.wait_for(read_all(), timeout)


async def test_a_receive_failure_raises_from_iteration_after_queued_updates() -> None:
    client = FakeRealtimeClient()
    received: list[AgentResponseUpdate] = []

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event("session_updated"))
        client.session.fail(RealtimeConnectionException("socket died"))
        with pytest.raises(RealtimeConnectionException, match="socket died"):
            await _read_into(received, conversation)

    assert _kinds(received) == ["session_updated"]


async def test_a_full_update_queue_fails_with_backpressure_and_closes() -> None:
    client = FakeRealtimeClient()
    received: list[AgentResponseUpdate] = []

    async with RealtimeAgent(client).start_conversation(max_queued_updates=2) as conversation:
        client.session.emit(event("session_updated"), event("session_updated"), event("session_updated"))
        await eventually(lambda: client.session._events.empty())  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(RealtimeBackpressureException):
            await _read_into(received, conversation)
        await eventually(lambda: client.session.close_count == 1)

    assert len(received) == 2


# endregion

# region send


def _sent_text(session: FakeRealtimeSession) -> list[tuple[str, str]]:
    return [(message.role, message.text) for message in session.sent if isinstance(message, Message)]


async def test_sending_text_adds_it_and_requests_a_response() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.send("Hi")

    assert _sent_text(client.session) == [("user", "Hi")]
    assert client.session.calls == [("create_response", None)]


async def test_sending_with_create_response_false_requests_no_response() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.send("Hi", create_response=False)

    assert _sent_text(client.session) == [("user", "Hi")]
    assert client.session.calls == []


def _audio(milliseconds: int = 10) -> Content:
    return Content.from_data(data=b"\x00" * 48 * milliseconds, media_type="audio/pcm;rate=24000")


async def test_sending_audio_requests_no_response() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.send(_audio())

    assert len(client.session.sent) == 1
    assert client.session.calls == []


async def test_sending_audio_with_text_requests_a_response() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.send(Message("user", [_audio(), "and this"]))

    assert client.session.calls == [("create_response", None)]


async def test_a_raw_command_goes_out_verbatim_without_a_response() -> None:
    client = FakeRealtimeClient()
    command = RealtimeEvent("passthrough", raw_representation={"type": "input_audio_buffer.clear"})

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.send(command)

    assert client.session.sent == [command]
    assert client.session.calls == []


@pytest.mark.parametrize(
    "app_input",
    [
        Content.from_function_result(call_id="call_1", result="done"),
        Content.from_function_approval_response(
            approved=True,
            id="request_1",
            function_call=Content.from_function_call(call_id="call_1", name="get_weather", arguments={}),
        ),
    ],
    ids=["function_result", "function_approval_response"],
)
async def test_sending_a_tool_result_or_approval_response_raises_value_error(app_input: Content) -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        with pytest.raises(ValueError):
            await conversation.send(Message("tool", [app_input]))

    assert client.session.sent == []


async def test_sending_after_close_raises_conversation_closed() -> None:
    async with RealtimeAgent(FakeRealtimeClient()).start_conversation() as conversation:
        await conversation.close()

        with pytest.raises(RealtimeConversationClosedException):
            await conversation.send("Hi")


async def test_sending_text_during_an_active_response_adds_it_then_raises_active_response() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event("response_created", response_id="resp_1"))
        await next_update(conversation)

        with pytest.raises(RealtimeActiveResponseException):
            await conversation.send("Hi")

    assert _sent_text(client.session) == [("user", "Hi")]
    assert client.session.calls == []


async def test_concurrent_sends_do_not_interleave_their_writes() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await asyncio.gather(
            conversation.send(["1", "2"], create_response=False),
            conversation.send(["3", "4"], create_response=False),
        )

    assert [text for _, text in _sent_text(client.session)] in (["1", "2", "3", "4"], ["3", "4", "1", "2"])


async def test_send_does_not_wait_behind_a_pending_acknowledgement() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        release = client.session.hold("create_response")
        pending = asyncio.create_task(conversation.create_response())
        await eventually(lambda: bool(client.session.calls))

        await asyncio.wait_for(conversation.send("x", create_response=False), 1)

        release.set()
        await pending

    assert _sent_text(client.session) == [("user", "x")]


# endregion

# region Control calls


async def test_create_response_returns_the_response_id() -> None:
    async with RealtimeAgent(FakeRealtimeClient()).start_conversation() as conversation:
        assert await conversation.create_response() == "resp_1"


async def test_create_response_passes_its_options_to_the_session() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.create_response(options={"tool_choice": "none"})

    assert client.session.calls == [("create_response", {"tool_choice": "none"})]


async def test_an_unacknowledged_control_call_raises_control_timeout() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(control_timeout=0.05) as conversation:
        client.session.hold("create_response")

        with pytest.raises(RealtimeControlTimeoutException):
            await asyncio.wait_for(conversation.create_response(), 1)


async def test_control_calls_run_one_at_a_time_in_call_order() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event("response_created", response_id="resp_0"))
        await next_update(conversation)
        release_cancel = client.session.hold("cancel_response")
        interrupting = asyncio.create_task(conversation.interrupt())
        await eventually(lambda: bool(client.session.calls))
        creating = asyncio.create_task(conversation.create_response())
        await asyncio.sleep(0.01)
        assert client.session.calls == [("cancel_response",)]

        client.session.emit(event("response_done", response_id="resp_0", status="cancelled"))
        await next_update(conversation)
        release_cancel.set()
        await interrupting
        await creating

    assert client.session.calls == [("cancel_response",), ("create_response", None)]


async def test_the_timeout_covers_only_the_acknowledgement_not_the_wait_for_the_lock() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(control_timeout=0.2) as conversation:
        client.session.delays = {"create_response": 0.15, "cancel_response": 0.15}
        await asyncio.gather(conversation.create_response(), conversation.interrupt())

    assert client.session.calls == [("create_response", None), ("cancel_response",)]


@pytest.mark.parametrize("raw", [False, True], ids=["message", "raw command"])
async def test_a_send_queued_when_close_starts_never_reaches_the_session(raw: bool) -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        release = client.session.hold("send")
        sending = asyncio.create_task(conversation.send("Hi", create_response=False))
        await eventually(lambda: client.session.send_attempts == 1)
        queued = asyncio.create_task(conversation.send(event("passthrough") if raw else "Bye", create_response=False))
        await asyncio.sleep(0.01)

        await conversation.close()
        release.set()
        await asyncio.wait_for(sending, 1)
        with pytest.raises(RealtimeConversationClosedException):
            await asyncio.wait_for(queued, 1)

    [sent] = client.session.sent
    assert isinstance(sent, Message)
    assert sent.text == "Hi"


async def test_a_timed_out_control_call_finishes_cleaning_up_before_it_raises() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(control_timeout=0.05) as conversation:
        client.session.hold("create_response")
        client.session.cleanup_delays["create_response"] = 0.05
        with pytest.raises(RealtimeControlTimeoutException):
            await conversation.create_response()

        assert client.session.calls == [("create_response", None), ("cleaned up", "create_response")]


async def test_a_control_call_after_close_raises_conversation_closed() -> None:
    async with RealtimeAgent(FakeRealtimeClient()).start_conversation() as conversation:
        await conversation.close()

        with pytest.raises(RealtimeConversationClosedException):
            await conversation.create_response()


async def test_close_releases_a_waiting_control_call() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(control_timeout=10) as conversation:
        client.session.hold("create_response")
        waiting = asyncio.create_task(conversation.create_response())
        await eventually(lambda: bool(client.session.calls))

        await conversation.close()

        with pytest.raises(RealtimeConversationClosedException):
            await asyncio.wait_for(waiting, 1)


@pytest.mark.parametrize("control", ["create_response", "interrupt"])
async def test_a_control_call_queued_when_close_starts_never_reaches_the_session(control: str) -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(control_timeout=10) as conversation:
        client.session.hold("create_response")
        waiting = asyncio.create_task(conversation.create_response())
        await eventually(lambda: bool(client.session.calls))
        queued = asyncio.create_task(
            conversation.create_response() if control == "create_response" else conversation.interrupt(played_ms=0)
        )
        await asyncio.sleep(0.01)

        await conversation.close()

        for call in (waiting, queued):
            with pytest.raises(RealtimeConversationClosedException):
                await asyncio.wait_for(call, 1)
    assert client.session.calls == [("create_response", None)]


async def test_a_created_response_counts_as_active_before_its_event_arrives() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.create_response()
        await conversation.interrupt()

    assert client.session.calls == [("create_response", None), ("cancel_response",)]


async def test_a_created_response_that_already_finished_is_not_active() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        release = client.session.hold("create_response")
        creating = asyncio.create_task(conversation.create_response())
        client.session.emit(
            event("response_created", response_id="resp_1"),
            event("response_done", response_id="resp_1", status="completed"),
        )
        [await next_update(conversation) for _ in range(2)]
        release.set()
        await creating

        await conversation.interrupt()

    assert client.session.calls == [("create_response", None)]


async def test_interrupt_cancels_the_active_response() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event("response_created", response_id="resp_1"))
        await next_update(conversation)

        await conversation.interrupt()

    assert client.session.calls == [("cancel_response",)]


async def test_interrupt_without_an_active_response_sends_nothing() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.interrupt()

    assert client.session.calls == []


async def test_interrupt_treats_response_cancel_not_active_as_done() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event("response_created", response_id="resp_1"))
        await next_update(conversation)
        client.session.cancel_response_error = RealtimeControlException(
            "Cancellation failed: no active response found", code="response_cancel_not_active"
        )

        await conversation.interrupt()

    assert client.session.calls == [("cancel_response",)]


async def test_interrupt_rejects_a_negative_played_ms() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event("response_created", response_id="resp_1"))
        await next_update(conversation)

        with pytest.raises(ValueError):
            await conversation.interrupt(played_ms=-1)

    assert client.session.calls == []


# endregion

# region Close


async def test_close_is_idempotent() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await conversation.close()
        await conversation.close()

    assert client.session.close_count == 1


async def test_the_session_ending_closes_the_conversation() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.end()
        await _collect(conversation)

        with pytest.raises(RealtimeConversationClosedException):
            await conversation.send("Hi")


async def test_a_receive_error_after_the_app_closed_does_not_fail_the_conversation() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.close_raises_in_receive = True
        await conversation.close()

        assert await _collect(conversation) == []


async def test_close_finishes_when_closing_the_session_fails(caplog: pytest.LogCaptureFixture) -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.close_error = RealtimeConnectionException("The socket is stuck.")
        try:
            await asyncio.wait_for(conversation.close(), 1)
        finally:
            client.session.end()  # lets a hung close finish, so a failing run ends

        assert await _collect(conversation) == []
    assert "The socket is stuck." in caplog.text


async def test_cancelling_a_close_call_leaves_the_close_running() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        release = client.session.hold("close")
        first_close = asyncio.create_task(conversation.close())
        await eventually(lambda: client.session.close_count == 1)
        first_close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_close
        release.set()

        await conversation.close()

    assert client.session.close_count == 1


# endregion


class TestVerification:
    @pytest.mark.parametrize("status", ["cancelled", "failed"])
    async def test_cancelled_and_failed_responses_finish_with_none(self, status: str) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            client.session.emit(event("response_done", response_id="resp_1", status=status))
            update = await next_update(conversation)

        assert update.finish_reason is None

    async def test_an_unknown_kind_streams_as_realtime_event_content(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            client.session.emit(event("future_kind", detail="x"))
            update = await next_update(conversation)

        [content] = update.contents
        assert (content.type, content.kind, update.role) == ("realtime_event", "future_kind", None)

    async def test_updates_and_async_iteration_read_the_same_stream(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            client.session.emit(event("session_created", session_id="sess_1"), event("session_updated"))
            first = await next_update(conversation.updates)
            second = await next_update(conversation)

        assert _kinds([first, second]) == ["session_created", "session_updated"]

    async def test_a_claimed_error_propagates_from_create_response(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            client.session.create_response_error = RealtimeControlException("Bad value.", code="invalid_value")

            with pytest.raises(RealtimeControlException, match="Bad value"):
                await conversation.create_response()

    async def test_a_lost_race_propagates_from_create_response(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            client.session.create_response_error = RealtimeActiveResponseException("Another response started.")

            with pytest.raises(RealtimeActiveResponseException):
                await conversation.create_response()

    async def test_a_rejected_cancel_propagates_from_interrupt(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            client.session.emit(event("response_created", response_id="resp_1"))
            await next_update(conversation)
            client.session.cancel_response_error = RealtimeControlException("Rejected.", code="invalid_value")

            with pytest.raises(RealtimeControlException, match="Rejected"):
                await conversation.interrupt()

    async def test_an_unacknowledged_cancel_raises_control_timeout(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation(control_timeout=0.05) as conversation:
            client.session.emit(event("response_created", response_id="resp_1"))
            await next_update(conversation)
            client.session.hold("cancel_response")

            with pytest.raises(RealtimeControlTimeoutException):
                await asyncio.wait_for(conversation.interrupt(), 1)

    async def test_a_passthrough_event_becomes_passthrough_content_with_no_role(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            client.session.emit(event("passthrough", service_event_type="rate_limits.updated"))
            update = await next_update(conversation)

        [content] = update.contents
        assert (content.type, content.kind, content.service_event_type, update.role) == (
            "realtime_event",
            "passthrough",
            "rate_limits.updated",
            None,
        )

    async def test_identical_errors_separated_by_another_event_both_stream(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            client.session.emit(_provider_error(), event("passthrough"), _provider_error())
            updates = [await next_update(conversation) for _ in range(3)]

        assert _kinds(updates) == ["error", "passthrough", "error"]

    async def test_close_wakes_a_waiting_iterator(self) -> None:
        async with RealtimeAgent(FakeRealtimeClient()).start_conversation() as conversation:
            reading = asyncio.create_task(_collect(conversation))
            await asyncio.sleep(0.01)
            await conversation.close()

            assert await reading == []

    async def test_create_response_during_an_active_response_raises_without_sending(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            client.session.emit(event("response_created", response_id="resp_1"))
            await next_update(conversation)

            with pytest.raises(RealtimeActiveResponseException):
                await conversation.create_response()

        assert client.session.calls == []
