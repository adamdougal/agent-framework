# Copyright (c) Microsoft. All rights reserved.

"""Live tests of VoiceLiveClient against a managed Voice Live model.

Marked integration and flaky, and skipped unless AZURE_VOICELIVE_ENDPOINT and AZURE_VOICELIVE_MODEL are set. They
authenticate with AZURE_VOICELIVE_API_KEY when set, else AzureCliCredential. Each test runs its own reader task under
asyncio.timeout(), and checks Voice Live behaviour the unit tests can only assume.
"""

import asyncio
import base64
import contextlib
import logging
import os
from collections.abc import AsyncGenerator, Callable, Sequence
from typing import Any

import pytest
from agent_framework import Content, Message, RealtimeAgent, tool
from agent_framework.exceptions import (
    RealtimeConnectionException,
    RealtimeControlException,
    RealtimeUnsupportedException,
)
from azure.identity.aio import AzureCliCredential

from agent_framework_foundry import AzureVoice, VoiceLiveClient, VoiceLiveOptions

pytestmark = [
    pytest.mark.flaky,
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("AZURE_VOICELIVE_ENDPOINT", "") == "" or os.getenv("AZURE_VOICELIVE_MODEL", "") == "",
        reason="No AZURE_VOICELIVE_ENDPOINT or AZURE_VOICELIVE_MODEL provided; skipping Voice Live integration tests.",
    ),
]

AVA: AzureVoice = {"type": "azure-standard", "name": "en-US-AvaNeural"}
TURN_TIMEOUT = 60
PCM_24K_BYTES_PER_SECOND = 48_000


class Reader:
    """Reads a session's events in the background, as the conversation's receive loop does."""

    def __init__(self, session: Any) -> None:
        self.events: list[Any] = []
        self._task = asyncio.create_task(self._read(session))

    async def _read(self, session: Any) -> None:
        async for event in session.receive():
            self.events.append(event)

    def of_response(self, response_id: str) -> list[Any]:
        return [event for event in self.events if _core(event).get("response_id") == response_id]

    def response_done(self, response_id: str) -> Any | None:
        return next((event for event in self.of_response(response_id) if event.kind == "response_done"), None)

    async def until(self, condition: Callable[[], Any]) -> None:
        while not condition():
            if self._task.done():
                self._task.result()
                raise AssertionError("The session ended before the condition held.")
            await asyncio.sleep(0.02)

    async def stop(self) -> None:
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


def _core(event: Any) -> dict[str, Any]:
    return (event.additional_properties or {}).get("core", {})


@contextlib.asynccontextmanager
async def live_session(
    options: VoiceLiveOptions | None = None, tools: Sequence[Any] | None = None
) -> AsyncGenerator[tuple[Any, Reader]]:
    """A live Voice Live session with a background reader, authenticated by key or the Azure CLI."""
    async with contextlib.AsyncExitStack() as stack:
        credential = None
        if not os.getenv("AZURE_VOICELIVE_API_KEY"):
            credential = await stack.enter_async_context(AzureCliCredential())
        client = VoiceLiveClient(credential=credential)
        session = await asyncio.wait_for(client.start_session(options=options, tools=tools), 30)
        stack.push_async_callback(session.close)
        reader = Reader(session)
        stack.push_async_callback(reader.stop)
        yield session, reader


def _final_text(events: list[Any]) -> str:
    finals = [event for event in events if event.kind == "output_text" and event.content.status == "completed"]
    return " ".join(event.content.text for event in finals)


async def test_text_turn_yields_events_in_order() -> None:
    """The first update is acknowledged; a user text turn then yields session_created, response_created,
    output_text deltas and a completed final, output_audio with media type "audio/pcm;rate=24000", usage, then
    response_done with status "completed"."""
    async with asyncio.timeout(TURN_TIMEOUT):
        options: VoiceLiveOptions = {
            "instructions": "Answer in one short sentence.",
            "voice": AVA,
            "turn_detection": None,
        }
        async with live_session(options) as (session, reader):
            await session.send(Message("user", ["What colour is a clear daytime sky?"]))
            response_id = await session.create_response()
            await reader.until(lambda: reader.response_done(response_id))

    assert reader.events[0].kind == "session_created"
    kinds = [event.kind for event in reader.of_response(response_id)]
    assert kinds[0] == "response_created"
    assert kinds[-2:] == ["usage", "response_done"]
    texts = [event for event in reader.of_response(response_id) if event.kind == "output_text"]
    assert any(event.content.merge_mode == "append" for event in texts)
    assert _final_text(texts)
    audio = [event for event in reader.of_response(response_id) if event.kind == "output_audio"]
    assert audio
    assert {event.content.media_type for event in audio} == {"audio/pcm;rate=24000"}
    assert _core(reader.response_done(response_id))["status"] == "completed"


async def test_say_by_voice_type() -> None:
    """With voice en-US-AvaNeural, say("Thanks for calling.") returns the id of a response whose final
    output_text is the text; with an OpenAI voice, say() raises RealtimeUnsupportedException."""
    async with asyncio.timeout(TURN_TIMEOUT):
        async with live_session({"voice": AVA, "turn_detection": None}) as (session, reader):
            response_id = await session.say("Thanks for calling.")
            await reader.until(lambda: reader.response_done(response_id))

        async with live_session({"voice": "alloy", "turn_detection": None}) as (openai_voiced, _):
            with pytest.raises(RealtimeUnsupportedException):
                await openai_voiced.say("Thanks for calling.")

    assert "Thanks for calling" in _final_text(reader.of_response(response_id))


async def test_conversation_say_speaks_the_text_verbatim() -> None:
    """Through a realtime agent's conversation with voice en-US-AvaNeural, say("Thanks for calling Contoso.") returns
    the id of the response whose final agent transcript is that text."""
    async with asyncio.timeout(TURN_TIMEOUT), contextlib.AsyncExitStack() as stack:
        credential = None
        if not os.getenv("AZURE_VOICELIVE_API_KEY"):
            credential = await stack.enter_async_context(AzureCliCredential())
        options: VoiceLiveOptions = {"voice": AVA, "turn_detection": None}
        client: VoiceLiveClient[VoiceLiveOptions] = VoiceLiveClient(credential=credential)
        agent = RealtimeAgent(client, default_options=options)
        async with agent.start_conversation() as conversation:
            response_id = await conversation.say("Thanks for calling Contoso.")
            finals: list[str] = []
            async for update in conversation:
                if update.response_id != response_id:
                    continue
                finals += [
                    content.text or ""
                    for content in update.contents
                    if content.type == "text" and content.status == "completed"
                ]
                if update.finish_reason is not None:
                    break

    assert "Thanks for calling Contoso" in " ".join(finals)


async def test_rejected_update_is_claimed_and_instructions_are_dropped(caplog: pytest.LogCaptureFixture) -> None:
    """An update Voice Live rejects raises RealtimeControlException, though its errors echo no event_id;
    create_response({"instructions": ...}) warns and still returns a response id."""
    async with asyncio.timeout(TURN_TIMEOUT):
        async with live_session({"voice": AVA, "turn_detection": None}) as (session, reader):
            with pytest.raises(RealtimeControlException):
                await session.update_session({"turn_detection": {"type": "not_a_detector"}}, None)

            with caplog.at_level(logging.WARNING):
                response_id = await session.create_response({"instructions": "Say hello."})
            await reader.until(lambda: reader.response_done(response_id))

    assert "instructions" in caplog.text
    claimed = [event for event in reader.events if event.service_event_type == "error"]
    assert claimed
    assert all(event.kind == "passthrough" for event in claimed)


async def test_function_call_round_trip() -> None:
    """With a get_time tool declared and tool_choice "required", function_call arrives with call_id, name and
    arguments; sending its function_result and create_response() yields a follow-up response with output_text."""

    @tool
    def get_time(city: str) -> str:
        """Get the current time in a city."""
        return "12:00"

    async with asyncio.timeout(TURN_TIMEOUT):
        options: VoiceLiveOptions = {"voice": AVA, "turn_detection": None, "tool_choice": "required"}
        async with live_session(options, [get_time]) as (session, reader):
            await session.send(Message("user", ["What time is it in Paris?"]))
            calling = await session.create_response()
            await reader.until(lambda: reader.response_done(calling))
            call = next(event for event in reader.of_response(calling) if event.kind == "function_call")

            await session.send(Message("tool", [Content.from_function_result(call.content.call_id, result="12:00")]))
            answering = await session.create_response({"tool_choice": {"mode": "none"}})
            await reader.until(lambda: reader.response_done(answering))

    assert call.content.name == "get_time"
    assert "paris" in call.content.arguments.lower()
    assert _final_text(reader.of_response(answering))


async def test_cancel_and_truncate() -> None:
    """cancel_response() during a long reply resolves on its response_done; truncate(item, 0, 500) yields
    item_truncated with truncated_by "client"; cancel_response() with nothing active raises
    response_cancel_not_active."""
    async with asyncio.timeout(TURN_TIMEOUT):
        async with live_session({"voice": AVA, "turn_detection": None}) as (session, reader):
            await session.send(Message("user", ["Tell me a long story about a lighthouse keeper."]))
            response_id = await session.create_response()

            def audio_bytes() -> int:
                audio = [event for event in reader.of_response(response_id) if event.kind == "output_audio"]
                return sum(len(base64.b64decode(event.content.uri.split(",", 1)[1])) for event in audio)

            await reader.until(lambda: audio_bytes() >= PCM_24K_BYTES_PER_SECOND)
            await session.cancel_response()
            await reader.until(lambda: reader.response_done(response_id))
            done = reader.response_done(response_id)
            item_id = next(
                _core(event)["item_id"] for event in reader.of_response(response_id) if event.kind == "output_audio"
            )

            await session.truncate(item_id, 0, 500)
            await reader.until(lambda: any(event.kind == "item_truncated" for event in reader.events))

            with pytest.raises(RealtimeControlException) as caught:
                await session.cancel_response()

    assert done is not None
    assert _core(done)["status"] == "cancelled"
    truncated = next(event for event in reader.events if event.kind == "item_truncated")
    assert _core(truncated) == {"item_id": item_id, "content_index": 0, "audio_end_ms": 500, "truncated_by": "client"}
    assert caught.value.code == "response_cancel_not_active"


async def test_bad_credentials_raise() -> None:
    """VoiceLiveClient(api_key="not-a-key").start_session() raises RealtimeConnectionException."""
    client = VoiceLiveClient(api_key="not-a-key")

    with pytest.raises(RealtimeConnectionException):
        await asyncio.wait_for(client.start_session(), 30)
