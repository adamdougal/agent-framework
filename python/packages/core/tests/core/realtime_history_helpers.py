# Copyright (c) Microsoft. All rights reserved.

"""Provider events, context providers and readers for testing a live conversation's history."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

from agent_framework import (
    AgentResponseUpdate,
    AgentSession,
    Content,
    ContextProvider,
    Message,
    RealtimeEvent,
    SessionContext,
    SupportsAgentRun,
    tool,
)

from .realtime_helpers import FakeRealtimeClient, event, next_update


def caller_item(item_id: str) -> RealtimeEvent:
    return event("item_added", item_id=item_id, previous_item_id=None, role="user", input_audio=True)


def caller_transcript(item_id: str, text: str) -> RealtimeEvent:
    final = Content.from_text(text, merge_mode="replace", status="completed")
    return event("input_transcription", content=final, item_id=item_id, content_index=0)


def caller_delta(item_id: str, text: str) -> RealtimeEvent:
    delta = Content.from_text(text, merge_mode="append")
    return event("input_transcription", content=delta, item_id=item_id, content_index=0)


def agent_item(item_id: str) -> RealtimeEvent:
    return event("item_added", item_id=item_id, previous_item_id=None, role="assistant")


def agent_text(response_id: str, item_id: str, text: str, *, final: bool = True) -> RealtimeEvent:
    content = (
        Content.from_text(text, merge_mode="replace", status="completed")
        if final
        else Content.from_text(text, merge_mode="append")
    )
    return event("output_text", content=content, response_id=response_id, item_id=item_id, content_index=0)


def agent_audio(response_id: str, item_id: str, milliseconds: int) -> RealtimeEvent:
    """24 kHz 16-bit mono PCM: 48 bytes per millisecond."""
    audio = Content.from_data(b"\x00" * 48 * milliseconds, "audio/pcm;rate=24000")
    return event("output_audio", content=audio, response_id=response_id, item_id=item_id, content_index=0)


def truncated(item_id: str, audio_end_ms: int) -> RealtimeEvent:
    return event("item_truncated", item_id=item_id, content_index=0, audio_end_ms=audio_end_ms, truncated_by="client")


def spoken_reply(response_id: str, item_id: str, text: str, milliseconds: int) -> list[RealtimeEvent]:
    return [
        event("response_created", response_id=response_id),
        agent_item(item_id),
        agent_audio(response_id, item_id, milliseconds),
        agent_text(response_id, item_id, text),
        event("response_done", response_id=response_id, status="completed"),
    ]


def reply(response_id: str, item_id: str, text: str, *, status: str = "completed") -> list[RealtimeEvent]:
    return [
        event("response_created", response_id=response_id),
        agent_item(item_id),
        agent_text(response_id, item_id, text),
        event("response_done", response_id=response_id, status=status),
    ]


async def streamed(conversation: Any, *events: RealtimeEvent) -> None:
    conversation._session.emit(*events)  # pyright: ignore[reportPrivateUsage]
    for _ in events:
        await next_update(conversation)


def dicts(messages: Sequence[Message]) -> list[dict[str, Any]]:
    return [message.to_dict() for message in messages]


def shapes(messages: Sequence[Message]) -> list[tuple[str, list[str], str | None, str | None]]:
    """Each message's role, content types, author and id, for messages whose contents carry generated ids."""
    return [
        (message.role, [content.type for content in message.contents], message.author_name, message.message_id)
        for message in messages
    ]


class Recorder(ContextProvider):
    def __init__(self, source_id: str = "recorder") -> None:
        super().__init__(source_id)
        self.reports: list[SessionContext] = []

    async def after_run(
        self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        self.reports.append(context)


class Loader(Recorder):
    def __init__(self, client: FakeRealtimeClient, source_id: str = "loader") -> None:
        super().__init__(source_id)
        self.client = client
        self.loads: list[bool] = []

    async def before_run(
        self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        self.loads.append(bool(self.client.start_session_calls))


class Failing(Recorder):
    def __init__(self, source_id: str = "failing", *, failures: int = 1_000) -> None:
        super().__init__(source_id)
        self.failures = failures

    async def after_run(
        self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        if self.failures:
            self.failures -= 1
            raise RuntimeError(f"{self.source_id} store down")
        await super().after_run(agent=agent, session=session, context=context, state=state)


class Slow(Recorder):
    def __init__(self) -> None:
        super().__init__("slow")
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = False

    async def after_run(
        self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        if not self.started.is_set():
            self.started.set()
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
        await super().after_run(agent=agent, session=session, context=context, state=state)


async def read_all(conversation: Any) -> list[AgentResponseUpdate]:
    return [update async for update in conversation]


async def until_error(conversation: Any) -> AgentResponseUpdate:
    while True:
        update = await next_update(conversation)
        if any(content.type == "error" for content in update.contents):
            return update


def texts(report: SessionContext) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    outputs = report.response.messages if report.response is not None else []
    return (
        [(message.role, message.text) for message in report.input_messages],
        [(message.role, message.text) for message in outputs],
    )


@tool
def get_weather(city: str) -> str:
    """Get the weather for a city."""
    return f"Sunny in {city}"


def call_response(response_id: str, item_id: str, call_id: str, name: str, **arguments: Any) -> list[RealtimeEvent]:
    call = Content.from_function_call(call_id=call_id, name=name, arguments=arguments)
    return [
        event("response_created", response_id=response_id),
        event("item_added", item_id=item_id, previous_item_id=None),
        event("function_call", content=call, response_id=response_id, item_id=item_id),
        event("response_done", response_id=response_id, status="completed"),
    ]


def blocking_tool() -> tuple[Any, asyncio.Event]:
    release = asyncio.Event()

    @tool
    async def look_up() -> str:
        """Look something up, slowly."""
        await release.wait()
        return "found"

    return look_up, release


async def until_tool_result(conversation: Any) -> AgentResponseUpdate:
    while True:
        update = await next_update(conversation)
        if any(content.type == "function_result" for content in update.contents):
            return update


def usage(response_id: str, input_tokens: int, output_tokens: int) -> RealtimeEvent:
    usage = Content.from_usage({"input_token_count": input_tokens, "output_token_count": output_tokens})
    return event("usage", content=usage, response_id=response_id)


def spoken_then_written(response_id: str = "resp_1") -> list[RealtimeEvent]:
    """item_a is spoken, as 1,000 ms of audio, and item_b is only written, so a cut in item_a leaves item_b unheard."""
    written = Content.from_text("The full terms are as follows...", merge_mode="replace", status="completed")
    return [
        event("response_created", response_id=response_id),
        event("item_added", item_id="item_a", previous_item_id=None, role="assistant"),
        agent_audio(response_id, "item_a", 1000),
        event("item_added", item_id="item_b", previous_item_id="item_a", role="assistant"),
        event("output_text", content=written, response_id=response_id, item_id="item_b", content_index=0),
    ]
