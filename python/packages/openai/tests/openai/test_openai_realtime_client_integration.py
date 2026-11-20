# Copyright (c) Microsoft. All rights reserved.

"""Live tests of OpenAIRealtimeClient against OpenAI.

Marked integration and flaky, and skipped unless OPENAI_REALTIME_MODEL is set, so CI jobs that set OPENAI_MODEL to a
Responses model skip them. Use the cheapest realtime model, such as gpt-realtime-mini. They authenticate with
OPENAI_API_KEY. Each test runs its own reader task under asyncio.timeout(), and checks OpenAI behaviour the unit tests
can only assume.
"""

import asyncio
import contextlib
import os
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
from agent_framework import Content, Message, RealtimeEvent
from agent_framework.exceptions import RealtimeControlException
from realtime_live_reader import Reader, core  # pyrefly: ignore[missing-import] # pyright: ignore[reportMissingImports]

from agent_framework_openai import OpenAIRealtimeClient, OpenAIRealtimeOptions

pytestmark = [
    pytest.mark.flaky,
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("OPENAI_REALTIME_MODEL", "") == "",
        reason="No OPENAI_REALTIME_MODEL provided; skipping OpenAI realtime tests.",
    ),
]

TURN_TIMEOUT = 60
PCM_24K_BYTES_PER_SECOND = 48_000
CHUNK_BYTES = PCM_24K_BYTES_PER_SECOND // 50
# "Sorry, stop there. Actually, never mind.", 24 kHz 16-bit mono PCM.
INTERRUPT_CLIP = Path(__file__).parent.parent / "assets" / "interrupt.pcm"


def _raw(event_type: str, **fields: Any) -> RealtimeEvent:
    return RealtimeEvent("passthrough", raw_representation={"type": event_type, **fields})


@contextlib.asynccontextmanager
async def live_session(options: OpenAIRealtimeOptions | None = None) -> AsyncGenerator[tuple[Any, Reader]]:
    """A live OpenAI session with a background reader."""
    async with contextlib.AsyncExitStack() as stack:
        session = await asyncio.wait_for(OpenAIRealtimeClient().start_session(options=options), 30)
        stack.push_async_callback(session.close)
        reader = Reader(session)
        stack.push_async_callback(reader.stop)
        yield session, reader


async def test_text_in_and_out() -> None:
    """start_session's update is acknowledged; create_response() returns its id, so response.created echoed its
    metadata; a rejected update raises RealtimeControlException and its error echoes the update's event_id; raw
    response.create during an active response gets "conversation_already_has_active_response" and raw response.cancel
    with none active gets "response_cancel_not_active"; create_response({"max_tokens": 16}) ends incomplete with reason
    "max_output_tokens"; turn detection with auto_truncate True is rejected."""
    options: OpenAIRealtimeOptions = {"output_modalities": ["text"], "turn_detection": None}

    async with asyncio.timeout(TURN_TIMEOUT), live_session(options) as (session, reader):
        await session.send(Message("user", ["Count from one to thirty in words."]))
        counting = await session.create_response()
        await session.send(_raw("response.create", event_id="raw_create"))
        await reader.until(lambda: reader.response_done(counting))
        await session.send(_raw("response.cancel", event_id="raw_cancel"))
        await reader.until(lambda: len(reader.of_type("error")) == 2)

        with pytest.raises(RealtimeControlException) as rejected:
            await session.update_session({"turn_detection": {"type": "not_a_detector"}}, None)
        with pytest.raises(RealtimeControlException) as auto_truncate_rejected:
            await session.update_session(
                {"turn_detection": {"type": "server_vad", "auto_truncate": True}},  # type: ignore[typeddict-unknown-key]
                None,
            )

        await session.send(Message("user", ["Describe the sea in as much detail as you can."]))
        capped = await session.create_response({"max_tokens": 16})
        await reader.until(lambda: reader.response_done(capped))

    created = next(event for event in reader.of_response(counting) if event.kind == "response_created")
    assert created.raw_representation["response"]["metadata"]["af_request_id"]
    assert reader.final_text(counting)
    errors = {
        event.raw_representation["error"]["event_id"]: event.content.error_code
        for event in reader.of_type("error")
        if event.kind == "error"
    }
    assert errors == {
        "raw_create": "conversation_already_has_active_response",
        "raw_cancel": "response_cancel_not_active",
    }
    claimed = [event for event in reader.events if event.kind == "passthrough" and event.service_event_type == "error"]
    assert len(claimed) == 2
    assert claimed[0].raw_representation["error"]["event_id"].startswith("af_")
    assert rejected.value.code is not None
    assert auto_truncate_rejected.value.code is not None
    capped_done = core(reader.response_done(capped))
    assert (capped_done["status"], capped_done.get("reason")) == ("incomplete", "max_output_tokens")


async def test_short_audio_out() -> None:
    """After a short spoken say("Thanks for calling."), whose final output_text holds the text and whose usage reports
    audio output tokens, update_session({"voice": "cedar"}) drops the voice rather than raising, and a raw
    session.update changing the voice is rejected with "cannot_update_voice"; a cancelled reply's response_done has
    reason "client_cancelled"; after truncate(item, 0, 200), a raw conversation.item.retrieve reads back an empty
    transcript."""
    options: OpenAIRealtimeOptions = {"voice": "marin", "turn_detection": None}

    async with asyncio.timeout(TURN_TIMEOUT), live_session(options) as (session, reader):
        thanks = await session.say("Thanks for calling.")
        await reader.until(lambda: reader.response_done(thanks))

        await session.update_session({"voice": "cedar"}, None)
        voice_change = {"type": "realtime", "audio": {"output": {"voice": "cedar"}}}
        await session.send(_raw("session.update", event_id="raw_voice", session=voice_change))
        await reader.until(lambda: any(event.kind == "error" for event in reader.events))

        await session.send(Message("user", ["Tell me a long story about a lighthouse keeper."]))
        story = await session.create_response()
        await reader.until(lambda: reader.audio_bytes(story) >= PCM_24K_BYTES_PER_SECOND // 2)
        await session.cancel_response()
        await reader.until(lambda: reader.response_done(story))
        item_id = next(core(event)["item_id"] for event in reader.of_response(story) if event.kind == "output_audio")
        await session.truncate(item_id, 0, 200)
        await session.send(_raw("conversation.item.retrieve", item_id=item_id))
        await reader.until(lambda: reader.of_type("conversation.item.retrieved"))

    assert "thanks for calling" in reader.final_text(thanks).lower()
    usage = next(event for event in reader.of_response(thanks) if event.kind == "usage")
    assert usage.content.usage_details["audio_output_token_count"] > 0
    voice_error = next(event for event in reader.events if event.kind == "error")
    assert voice_error.content.error_code == "cannot_update_voice"
    assert core(reader.response_done(story))["reason"] == "client_cancelled"
    retrieved = reader.of_type("conversation.item.retrieved")[0].raw_representation["item"]
    assert [part.get("transcript") for part in retrieved["content"]] == [""]


async def test_speech_cancels_a_response_as_turn_detected() -> None:
    """With server VAD on, streaming the interrupt clip while a long reply plays yields speech_started and the reply's
    response_done with status "cancelled" and reason "turn_detected"."""
    options: OpenAIRealtimeOptions = {"turn_detection": {"type": "server_vad", "create_response": False}}
    clip = INTERRUPT_CLIP.read_bytes() + bytes(PCM_24K_BYTES_PER_SECOND)

    async with asyncio.timeout(TURN_TIMEOUT), live_session(options) as (session, reader):
        await session.send(Message("user", ["Tell me a long story about a lighthouse keeper."]))
        story = await session.create_response()
        await reader.until(lambda: reader.audio_bytes(story) >= PCM_24K_BYTES_PER_SECOND // 2)
        for start in range(0, len(clip), CHUNK_BYTES):
            chunk = Content.from_data(clip[start : start + CHUNK_BYTES], "audio/pcm;rate=24000")
            await session.send(Message("user", [chunk]))
            await asyncio.sleep(0.02)
        await reader.until(lambda: reader.response_done(story))

    assert any(event.kind == "speech_started" for event in reader.events)
    done = core(reader.response_done(story))
    assert (done["status"], done.get("reason")) == ("cancelled", "turn_detected")
