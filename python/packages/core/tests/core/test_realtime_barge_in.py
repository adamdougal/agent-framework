# Copyright (c) Microsoft. All rights reserved.

"""Barge-in: cutting the audio the caller didn't hear."""

import asyncio

import pytest

from agent_framework import Content, RealtimeAgent, RealtimeEvent
from agent_framework.exceptions import (
    RealtimeControlException,
    RealtimeControlTimeoutException,
    RealtimeConversationClosedException,
)

from .realtime_helpers import FakeRealtimeClient, FakeRealtimeSession, event, eventually, next_update, shorten_timeouts
from .realtime_history_helpers import spoken_then_written

# Audio below is 24 kHz 16-bit mono PCM, 48 bytes per millisecond, unless a test says otherwise.
PCM_24K = "audio/pcm;rate=24000"


def _audio(
    response_id: str,
    item_id: str,
    byte_count: int,
    *,
    media_type: str = PCM_24K,
    content_index: int = 0,
) -> RealtimeEvent:
    return event(
        "output_audio",
        content=Content.from_data(data=b"\x00" * byte_count, media_type=media_type),
        response_id=response_id,
        item_id=item_id,
        content_index=content_index,
    )


async def _play(conversation: object, session: FakeRealtimeSession, *events: RealtimeEvent) -> None:
    """Have the provider send ``events``, and wait until the conversation has streamed them."""
    session.emit(*events)
    for _ in events:
        await next_update(conversation)


@pytest.mark.parametrize(
    ("messages", "played_ms", "cuts"),
    [
        pytest.param(["item_1"], 959, [("item_1", 959)], id="cut-41-ms-before-the-end"),
        pytest.param(["item_1"], 960, [], id="heard-to-within-40-ms-of-the-end-is-cancelled-but-not-cut"),
        pytest.param(
            ["item_a", "item_b", "item_c"],
            400,
            [("item_a", 400), ("item_b", 0), ("item_c", 0)],
            id="cut-inside-the-first-message-and-all-after-it",
        ),
        pytest.param(["item_a", "item_b"], 1500, [("item_b", 500)], id="played-ms-carries-into-the-second-message"),
        pytest.param(
            ["item_a", "item_b"], 980, [("item_b", 0)], id="a-message-heard-within-its-slack-uses-up-played-ms"
        ),
    ],
)
async def test_interrupt_spends_played_ms_across_the_playing_responses_messages_in_order(
    messages: list[str], played_ms: int, cuts: list[tuple[str, int]]
) -> None:
    """The playing response's messages are each one second long. Interrupting cancels it, cuts the message played_ms
    ends in unless that was heard to within 40 ms of its end, and truncates every later one at 0 ms."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            *(_audio("resp_1", item_id, 48 * 1000) for item_id in messages),
        )
        await conversation.interrupt(played_ms=played_ms)

    assert client.session.calls == [("cancel_response",), *(("truncate", item_id, 0, end) for item_id, end in cuts)]


async def test_each_audio_part_of_a_message_is_measured_and_cut_on_its_own() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_1", 48 * 1000, content_index=0),
            _audio("resp_1", "item_1", 48 * 1000, content_index=1),
            event("response_done", response_id="resp_1", status="completed"),
        )
        await conversation.interrupt(played_ms=1500)

    assert client.session.calls == [("truncate", "item_1", 1, 500)]


async def test_interrupt_cuts_the_latest_response_that_produced_audio() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_1", 48 * 1000),
            event("response_done", response_id="resp_1", status="completed"),
            event("response_created", response_id="resp_2"),
            _audio("resp_2", "item_2", 48 * 1000),
        )
        await conversation.interrupt(played_ms=100)

    assert client.session.calls == [("cancel_response",), ("truncate", "item_2", 0, 100)]


async def test_interrupt_cuts_the_response_playing_when_it_was_called() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_a", 48 * 1000),
        )
        cancelled = client.session.hold("cancel_response")
        interrupting = asyncio.create_task(conversation.interrupt(played_ms=400))
        await eventually(lambda: bool(client.session.calls))
        await _play(
            conversation,
            client.session,
            event("response_done", response_id="resp_1", status="cancelled"),
            event("response_created", response_id="resp_2"),
            _audio("resp_2", "item_b", 48 * 1000),
        )
        cancelled.set()
        await asyncio.wait_for(interrupting, 5)

    assert client.session.calls == [("cancel_response",), ("truncate", "item_a", 0, 400)]


async def test_interrupt_cuts_a_response_that_already_finished() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_1", 48 * 1000),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        await conversation.interrupt(played_ms=300)

    assert client.session.calls == [("truncate", "item_1", 0, 300)]


async def test_interrupting_again_cuts_nothing_more() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_1", 48 * 1000),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        await conversation.interrupt(played_ms=400)
        await conversation.interrupt(played_ms=400)

    assert client.session.calls == [("truncate", "item_1", 0, 400)]


async def test_interrupting_after_a_failed_cut_retries_the_cuts_still_owed() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_a", 48 * 1000),
            _audio("resp_1", "item_b", 48 * 1000),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        client.session.truncate_error = RealtimeControlException("The item can't be truncated now.")
        with pytest.raises(RealtimeControlException):
            await conversation.interrupt(played_ms=400)
        client.session.truncate_error = None

        await conversation.interrupt(played_ms=400)

    assert client.session.calls == [
        ("truncate", "item_a", 0, 400),
        ("truncate", "item_b", 0, 0),
        ("truncate", "item_a", 0, 400),
        ("truncate", "item_b", 0, 0),
    ]


async def test_a_failed_cut_is_retried_after_a_later_response_produces_audio() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_a", 48 * 1000),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        client.session.truncate_error = RealtimeControlException("The item can't be truncated now.")
        with pytest.raises(RealtimeControlException):
            await conversation.interrupt(played_ms=400)
        client.session.truncate_error = None
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_2"),
            _audio("resp_2", "item_b", 48 * 1000),
            event("response_done", response_id="resp_2", status="cancelled", reason="turn_detected"),
        )

        await conversation.interrupt(played_ms=300)

    assert client.session.calls == [
        ("truncate", "item_a", 0, 400),
        ("truncate", "item_a", 0, 400),
        ("truncate", "item_b", 0, 300),
    ]


async def test_a_cut_the_provider_keeps_rejecting_does_not_stop_later_cuts() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_a", 48 * 1000),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        client.session.truncate_rejections["item_a"] = RealtimeControlException("The item was deleted.")
        with pytest.raises(RealtimeControlException):
            await conversation.interrupt(played_ms=400)
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_2"),
            _audio("resp_2", "item_b", 48 * 1000),
            event("response_done", response_id="resp_2", status="cancelled", reason="turn_detected"),
        )

        with pytest.raises(RealtimeControlException, match="deleted"):
            await conversation.interrupt(played_ms=300)

    assert client.session.calls == [
        ("truncate", "item_a", 0, 400),
        ("truncate", "item_a", 0, 400),
        ("truncate", "item_b", 0, 300),
    ]


async def test_a_late_acknowledgement_of_a_timed_out_cut_stops_it_being_retried() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        shorten_timeouts(conversation)
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_a", 48 * 1000),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        release = client.session.hold("truncate")
        with pytest.raises(RealtimeControlTimeoutException):
            await conversation.interrupt(played_ms=400)
        release.set()
        await _play(
            conversation,
            client.session,
            event("item_truncated", item_id="item_a", content_index=0, audio_end_ms=400, truncated_by="client"),
        )

        await conversation.interrupt(played_ms=400)

    assert client.session.calls == [("truncate", "item_a", 0, 400)]


@pytest.mark.parametrize(
    ("media_type", "half_second_bytes"),
    [
        pytest.param("audio/pcm;rate=16000", 16000, id="pcm-16khz"),
        pytest.param("audio/pcm;rate=8000", 8000, id="pcm-8khz"),
        pytest.param("audio/pcm", 24000, id="pcm-without-a-rate-is-24khz"),
        pytest.param("audio/pcmu", 4000, id="g711-mu-law-is-8khz-8-bit"),
        pytest.param("audio/pcma", 4000, id="g711-a-law-is-8khz-8-bit"),
    ],
)
@pytest.mark.parametrize(
    ("played_ms", "cuts"),
    [
        pytest.param(459, [("truncate", "item_1", 0, 459)], id="cut-41-ms-before-its-end"),
        pytest.param(460, [], id="heard-to-within-40-ms-of-its-end"),
    ],
)
async def test_half_a_second_of_audio_is_cut_at_played_ms_unless_heard_to_within_40_ms_of_its_end(
    media_type: str, half_second_bytes: int, played_ms: int, cuts: list[tuple[object, ...]]
) -> None:
    """Half a second of audio in each format barge-in can measure is cut at played_ms 41 ms before its end, and counts
    as heard 40 ms before it."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_1", half_second_bytes, media_type=media_type),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        await conversation.interrupt(played_ms=played_ms)

    assert client.session.calls == cuts


async def test_audio_in_an_unknown_format_is_not_cut() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_1", 48 * 1000, media_type="audio/opus"),
        )
        await conversation.interrupt(played_ms=400)

    assert client.session.calls == [("cancel_response",)]


async def test_a_provider_truncation_cuts_the_responses_later_messages_at_0_ms() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_a", 48 * 1000),
            _audio("resp_1", "item_b", 48 * 1000),
            _audio("resp_1", "item_c", 48 * 1000),
            event("item_truncated", item_id="item_a", content_index=0, audio_end_ms=300, truncated_by="provider"),
        )
        await eventually(lambda: len(client.session.calls) == 2)

    assert client.session.calls == [("truncate", "item_b", 0, 0), ("truncate", "item_c", 0, 0)]


async def test_a_rejected_provider_triggered_cut_does_not_stop_later_cuts_or_end_the_conversation() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.truncate_rejections["item_b"] = RealtimeControlException("The item was deleted.")
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_a", 48 * 1000),
            _audio("resp_1", "item_b", 48 * 1000),
            _audio("resp_1", "item_c", 48 * 1000),
            event("item_truncated", item_id="item_a", content_index=0, audio_end_ms=300, truncated_by="provider"),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        await eventually(lambda: len(client.session.calls) == 2)
        await conversation.interrupt()  # its turn comes only once the cuts have finished

    async for _ in conversation:  # raises the error that ended the conversation, if one did
        pass
    assert client.session.calls == [("truncate", "item_b", 0, 0), ("truncate", "item_c", 0, 0)]


async def test_a_provider_truncation_queued_when_close_starts_cuts_nothing() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(control_timeout=10) as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_a", 48 * 1000),
            _audio("resp_1", "item_b", 48 * 1000),
            event("response_done", response_id="resp_1", status="cancelled"),
        )
        client.session.hold("create_response")
        waiting = asyncio.create_task(conversation.create_response())
        await eventually(lambda: bool(client.session.calls))
        await _play(
            conversation,
            client.session,
            event("item_truncated", item_id="item_a", content_index=0, audio_end_ms=300, truncated_by="provider"),
        )

        await conversation.close()
        with pytest.raises(RealtimeConversationClosedException):
            await asyncio.wait_for(waiting, 5)
        await asyncio.sleep(0.01)

    assert client.session.calls == [("create_response", None)]


async def test_a_provider_truncation_of_a_later_audio_part_cuts_only_what_follows_it() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_1", 48 * 1000, content_index=0),
            _audio("resp_1", "item_1", 48 * 1000, content_index=1),
            _audio("resp_1", "item_2", 48 * 1000),
            event("item_truncated", item_id="item_1", content_index=1, audio_end_ms=300, truncated_by="provider"),
        )
        await eventually(lambda: bool(client.session.calls))
        await asyncio.sleep(0.01)

    assert client.session.calls == [("truncate", "item_2", 0, 0)]


async def test_close_lets_a_cancelled_provider_triggered_cut_finish_cleaning_up() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.hold("truncate")
        client.session.cleanup_delays["truncate"] = 0.05
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_a", 48 * 1000),
            _audio("resp_1", "item_b", 48 * 1000),
            event("item_truncated", item_id="item_a", content_index=0, audio_end_ms=300, truncated_by="provider"),
        )
        await eventually(lambda: bool(client.session.calls))

        await conversation.close()

        assert client.session.calls == [("truncate", "item_b", 0, 0), ("cleaned up", "truncate")]


async def test_audio_that_arrives_after_a_provider_truncation_is_cut_at_0_ms() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_a", 48 * 1000),
            event("item_truncated", item_id="item_a", content_index=0, audio_end_ms=300, truncated_by="provider"),
            _audio("resp_1", "item_b", 48 * 1000),
        )
        await eventually(lambda: bool(client.session.calls))
        await asyncio.sleep(0.01)

    assert client.session.calls == [("truncate", "item_b", 0, 0)]


async def test_a_client_truncation_cuts_nothing_more() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_a", 48 * 1000),
            _audio("resp_1", "item_b", 48 * 1000),
            event("item_truncated", item_id="item_a", content_index=0, audio_end_ms=300, truncated_by="client"),
        )
        await asyncio.sleep(0.05)

    assert client.session.calls == []


async def test_interrupt_deletes_a_later_text_only_message_of_the_cut_response() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            *spoken_then_written(),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        await conversation.interrupt(played_ms=400)

    assert client.session.calls == [("truncate", "item_a", 0, 400), ("delete_item", "item_b")]


async def test_a_later_text_only_message_of_a_cut_response_is_left_out_of_history() -> None:
    client = FakeRealtimeClient()
    spoken = Content.from_text("Your balance is fine", merge_mode="replace", status="completed")

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            *spoken_then_written(),
            event("output_text", content=spoken, response_id="resp_1", item_id="item_a", content_index=0),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        await conversation.interrupt(played_ms=400)
        await _play(
            conversation,
            client.session,
            event("item_truncated", item_id="item_a", content_index=0, audio_end_ms=400, truncated_by="client"),
        )

        assert [message.message_id for message in conversation.history] == ["item_a"]


async def test_a_provider_truncation_deletes_the_responses_later_text_only_messages() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            *spoken_then_written(),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
            event("item_truncated", item_id="item_a", content_index=0, audio_end_ms=300, truncated_by="provider"),
        )
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("delete_item", "item_b")]


async def test_a_running_responses_later_text_only_messages_are_deleted_when_it_finishes() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            *spoken_then_written(),
            event("item_truncated", item_id="item_a", content_index=0, audio_end_ms=300, truncated_by="provider"),
        )
        await asyncio.sleep(0.01)
        assert client.session.calls == []

        await _play(
            conversation,
            client.session,
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        await eventually(lambda: bool(client.session.calls))

    assert client.session.calls == [("delete_item", "item_b")]


async def test_a_failed_delete_raises_from_interrupt_and_is_not_retried() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            *spoken_then_written(),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        client.session.delete_rejections["item_b"] = RealtimeControlException("No such item.", code="item_not_found")

        with pytest.raises(RealtimeControlException):
            await conversation.interrupt(played_ms=400)
        await conversation.interrupt(played_ms=400)

    assert [call for call in client.session.calls if call[0] == "delete_item"] == [("delete_item", "item_b")]


async def test_a_failed_delete_does_not_stop_the_later_deletes() -> None:
    client = FakeRealtimeClient()
    more = Content.from_text("And one more thing...", merge_mode="replace", status="completed")

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            *spoken_then_written(),
            event("item_added", item_id="item_c", previous_item_id="item_b", role="assistant"),
            event("output_text", content=more, response_id="resp_1", item_id="item_c", content_index=0),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        client.session.delete_rejections["item_b"] = RealtimeControlException("No such item.", code="item_not_found")

        with pytest.raises(RealtimeControlException):
            await conversation.interrupt(played_ms=400)

        assert [call for call in client.session.calls if call[0] == "delete_item"] == [
            ("delete_item", "item_b"),
            ("delete_item", "item_c"),
        ]
        assert [message.message_id for message in conversation.history] == []


async def test_unheard_text_leaves_history_as_soon_as_the_cut_response_has_finished() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            *spoken_then_written(),
            event("item_added", item_id="item_c", previous_item_id="item_b", role="assistant"),
            _audio("resp_1", "item_c", 48 * 1000),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        release = client.session.hold("truncate")
        await _play(
            conversation,
            client.session,
            event("item_truncated", item_id="item_a", content_index=0, audio_end_ms=300, truncated_by="provider"),
        )

        assert "item_b" not in [message.message_id for message in conversation.history]
        release.set()


class TestVerification:
    async def test_each_truncate_waits_for_its_acknowledgement(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            await _play(
                conversation,
                client.session,
                event("response_created", response_id="resp_1"),
                _audio("resp_1", "item_a", 48 * 1000),
                _audio("resp_1", "item_b", 48 * 1000),
                event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
            )
            release = client.session.hold("truncate")
            interrupting = asyncio.create_task(conversation.interrupt(played_ms=400))
            await eventually(lambda: bool(client.session.calls))
            await asyncio.sleep(0.01)
            assert client.session.calls == [("truncate", "item_a", 0, 400)]

            release.set()
            await interrupting

        assert client.session.calls == [("truncate", "item_a", 0, 400), ("truncate", "item_b", 0, 0)]

    async def test_an_unacknowledged_truncate_raises_control_timeout(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            shorten_timeouts(conversation)
            await _play(
                conversation,
                client.session,
                event("response_created", response_id="resp_1"),
                _audio("resp_1", "item_1", 48 * 1000),
                event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
            )
            client.session.hold("truncate")

            with pytest.raises(RealtimeControlTimeoutException):
                await asyncio.wait_for(conversation.interrupt(played_ms=400), 5)

    async def test_interrupt_does_not_cut_a_response_the_provider_already_cut(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            await _play(
                conversation,
                client.session,
                event("response_created", response_id="resp_1"),
                _audio("resp_1", "item_a", 48 * 1000),
                _audio("resp_1", "item_b", 48 * 1000),
                event("item_truncated", item_id="item_a", content_index=0, audio_end_ms=300, truncated_by="provider"),
            )
            await eventually(lambda: len(client.session.calls) == 1)
            await conversation.interrupt(played_ms=400)

        assert client.session.calls == [("truncate", "item_b", 0, 0), ("cancel_response",)]

    async def test_messages_before_the_cut_and_messages_with_audio_are_not_deleted(self) -> None:
        client = FakeRealtimeClient()
        written = Content.from_text("Here's a summary.", merge_mode="replace", status="completed")

        async with RealtimeAgent(client).start_conversation() as conversation:
            await _play(
                conversation,
                client.session,
                event("response_created", response_id="resp_1"),
                event("item_added", item_id="item_a", previous_item_id=None, role="assistant"),
                event("output_text", content=written, response_id="resp_1", item_id="item_a", content_index=0),
                event("item_added", item_id="item_b", previous_item_id="item_a", role="assistant"),
                _audio("resp_1", "item_b", 48 * 1000),
                event("item_added", item_id="item_c", previous_item_id="item_b", role="assistant"),
                _audio("resp_1", "item_c", 48 * 1000),
                event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
            )
            await conversation.interrupt(played_ms=400)

        assert client.session.calls == [("truncate", "item_b", 0, 400), ("truncate", "item_c", 0, 0)]
