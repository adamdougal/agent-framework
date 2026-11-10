# Copyright (c) Microsoft. All rights reserved.

"""Barge-in: cutting the unheard audio of the latest response (spec 006, Barge-in)."""

import asyncio
import logging

import pytest

from agent_framework import Content, RealtimeAgent, RealtimeEvent
from agent_framework.exceptions import (
    RealtimeControlException,
    RealtimeControlTimeoutException,
    RealtimeConversationClosedException,
)

from .realtime_helpers import FakeRealtimeClient, FakeRealtimeSession, event, eventually, next_update

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


async def test_interrupt_cuts_a_partly_heard_message_at_played_ms() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_1", 48 * 1000),
        )
        await conversation.interrupt(played_ms=400)

    assert client.session.calls == [("cancel_response",), ("truncate", "item_1", 0, 400)]


async def test_a_message_heard_to_within_40_ms_of_its_end_is_not_cut() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_1", 48 * 1000),
        )
        await conversation.interrupt(played_ms=960)

    assert client.session.calls == [("cancel_response",)]


async def test_played_ms_carries_across_the_responses_messages() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_a", 48 * 1000),
            _audio("resp_1", "item_b", 48 * 1000),
        )
        await conversation.interrupt(played_ms=1500)

    assert client.session.calls == [("cancel_response",), ("truncate", "item_b", 0, 500)]


async def test_messages_after_the_cut_are_truncated_at_0_ms() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_a", 48 * 1000),
            _audio("resp_1", "item_b", 48 * 1000),
            _audio("resp_1", "item_c", 48 * 1000),
        )
        await conversation.interrupt(played_ms=400)

    assert client.session.calls == [
        ("cancel_response",),
        ("truncate", "item_a", 0, 400),
        ("truncate", "item_b", 0, 0),
        ("truncate", "item_c", 0, 0),
    ]


async def test_a_message_heard_within_its_slack_leaves_none_of_played_ms_for_the_next() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_a", 48 * 1000),
            _audio("resp_1", "item_b", 48 * 1000),
        )
        await conversation.interrupt(played_ms=980)

    assert client.session.calls == [("cancel_response",), ("truncate", "item_b", 0, 0)]


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
        ("truncate", "item_a", 0, 400),
        ("truncate", "item_b", 0, 0),
    ]


async def test_a_late_acknowledgement_of_a_timed_out_cut_stops_it_being_retried() -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation(control_timeout=0.05) as conversation:
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


async def _cut_after_400_ms(media_type: str, byte_count: int) -> list[tuple[object, ...]]:
    """Play one message in ``media_type`` and interrupt it after 400 ms; return the session's calls."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_1", byte_count, media_type=media_type),
            event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
        )
        await conversation.interrupt(played_ms=400)

    return client.session.calls


async def test_audio_length_follows_the_pcm_rate() -> None:
    assert await _cut_after_400_ms("audio/pcm;rate=16000", 16000) == [("truncate", "item_1", 0, 400)]


async def test_pcm_without_a_rate_is_24_khz() -> None:
    assert await _cut_after_400_ms("audio/pcm", 24000) == [("truncate", "item_1", 0, 400)]


async def test_g711_audio_is_8_khz_8_bit() -> None:
    assert await _cut_after_400_ms("audio/pcmu", 4000) == [("truncate", "item_1", 0, 400)]


async def test_audio_in_an_unknown_format_is_not_cut(caplog: pytest.LogCaptureFixture) -> None:
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        await _play(
            conversation,
            client.session,
            event("response_created", response_id="resp_1"),
            _audio("resp_1", "item_1", 48 * 1000, media_type="audio/opus"),
        )
        with caplog.at_level(logging.WARNING):
            await conversation.interrupt(played_ms=400)

    assert client.session.calls == [("cancel_response",)]
    assert [record.getMessage() for record in caplog.records if "audio/opus" in record.getMessage()] != []


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
            await asyncio.wait_for(waiting, 1)
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


class TestVerification:
    async def test_a_message_heard_to_41_ms_before_its_end_is_cut_there(self) -> None:
        client = FakeRealtimeClient()

        async with RealtimeAgent(client).start_conversation() as conversation:
            await _play(
                conversation,
                client.session,
                event("response_created", response_id="resp_1"),
                _audio("resp_1", "item_1", 48 * 1000),
            )
            await conversation.interrupt(played_ms=959)

        assert client.session.calls == [("cancel_response",), ("truncate", "item_1", 0, 959)]

    async def test_8_khz_pcm_audio_length(self) -> None:
        assert await _cut_after_400_ms("audio/pcm;rate=8000", 8000) == [("truncate", "item_1", 0, 400)]

    async def test_pcma_audio_is_8_khz_8_bit(self) -> None:
        assert await _cut_after_400_ms("audio/pcma", 4000) == [("truncate", "item_1", 0, 400)]

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

        async with RealtimeAgent(client).start_conversation(control_timeout=0.05) as conversation:
            await _play(
                conversation,
                client.session,
                event("response_created", response_id="resp_1"),
                _audio("resp_1", "item_1", 48 * 1000),
                event("response_done", response_id="resp_1", status="cancelled", reason="turn_detected"),
            )
            client.session.hold("truncate")

            with pytest.raises(RealtimeControlTimeoutException):
                await asyncio.wait_for(conversation.interrupt(played_ms=400), 1)

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
