# Copyright (c) Microsoft. All rights reserved.

"""Fake realtime clients and sessions for testing the live conversation without a provider."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from types import TracebackType
from typing import Any, Self, cast

from agent_framework import (
    AgentResponseUpdate,
    Content,
    Message,
    RealtimeEvent,
    RealtimeKind,
    RealtimeOptions,
    ToolTypes,
)
from agent_framework.exceptions import RealtimeConnectionException

_END_OF_EVENTS = object()


def event(
    kind: str,
    *,
    content: Content | None = None,
    service_event_type: str | None = None,
    **core: Any,
) -> RealtimeEvent:
    """Build a provider event with ``core`` as its neutral attributes."""
    return RealtimeEvent(
        RealtimeKind(kind),
        content=content,
        service_event_type=service_event_type or f"wire.{kind}",
        additional_properties={"core": core} if core else None,
    )


async def eventually(condition: Callable[[], bool], timeout: float = 1.0) -> None:
    """Wait until ``condition`` holds, failing the test if it doesn't in time."""

    async def poll() -> None:
        while True:
            if condition():
                return
            await asyncio.sleep(0.001)

    await asyncio.wait_for(poll(), timeout)


async def next_update(updates: Any, timeout: float = 1.0) -> AgentResponseUpdate:
    """Read the next update from a conversation, failing the test if none arrives in time."""
    return await asyncio.wait_for(anext(aiter(updates)), timeout)


class FakeRealtimeSession:
    """A realtime session whose events the test emits, and which records what the conversation sends and calls."""

    def __init__(self, options: RealtimeOptions | None = None, tools: Sequence[ToolTypes] | None = None) -> None:
        self.options: RealtimeOptions = cast(RealtimeOptions, dict(options or {}))
        self.tools = tools
        self.sent: list[RealtimeEvent | Message] = []
        self.calls: list[tuple[Any, ...]] = []
        self.close_count = 0
        self.receive_count = 0
        self.send_error: BaseException | None = None
        self._response_count = 0
        self._holds: dict[str, asyncio.Event] = {}
        self.delays: dict[str, float] = {}
        self.cleanup_delays: dict[str, float] = {}
        self.cancel_response_error: BaseException | None = None
        self.create_response_events: list[RealtimeEvent] = []
        self.create_response_error: BaseException | None = None
        self.close_raises_in_receive = False
        self.close_error: BaseException | None = None
        self._events: asyncio.Queue[Any] = asyncio.Queue()

    def emit(self, *events: RealtimeEvent) -> None:
        """Queue provider events for the conversation's receive loop."""
        for event in events:
            self._events.put_nowait(event)

    def end(self) -> None:
        """End the session's events, as a provider closing the connection does."""
        self._events.put_nowait(_END_OF_EVENTS)

    def fail(self, error: BaseException) -> None:
        """Make receiving raise ``error`` once the queued events are read."""
        self._events.put_nowait(error)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def send(self, value: RealtimeEvent | Message) -> None:
        await asyncio.sleep(0)  # a wire write yields to the event loop
        await self._acknowledge("send")
        if self.send_error is not None:
            raise self.send_error
        self.sent.append(value)

    def receive(self) -> AsyncIterator[RealtimeEvent]:
        self.receive_count += 1
        return self._receive()

    async def _receive(self) -> AsyncIterator[RealtimeEvent]:
        while True:
            event = await self._events.get()
            if event is _END_OF_EVENTS:
                return
            if isinstance(event, BaseException):
                raise event
            yield event

    def hold(self, method: str) -> asyncio.Event:
        """Make calls to ``method`` wait, as for an acknowledgement, until the returned event is set."""
        release = asyncio.Event()
        self._holds[method] = release
        return release

    async def _acknowledge(self, method: str) -> None:
        try:
            if (delay := self.delays.get(method)) is not None:
                await asyncio.sleep(delay)
            if (release := self._holds.get(method)) is not None:
                await release.wait()
        except asyncio.CancelledError:
            # A cancellation-safe session may need a moment to stop tracking the call.
            if (cleanup := self.cleanup_delays.get(method)) is not None:
                await asyncio.sleep(cleanup)
                self.calls.append(("cleaned up", method))
            raise

    async def update_session(self, options: RealtimeOptions, tools: Sequence[ToolTypes] | None) -> None:
        self.calls.append(("update_session", options, tools))

    async def say(self, text: str) -> str:
        self.calls.append(("say", text))
        return "resp_said"

    async def create_response(self, options: RealtimeOptions | None = None) -> str:
        self.calls.append(("create_response", options))
        events = list(self.create_response_events)  # the response this call creates, fixed when it's made
        self.emit(*events)
        await self._acknowledge("create_response")
        if self.create_response_error is not None:
            raise self.create_response_error
        for created in events:
            if created.kind == "response_created":
                return str(created.additional_properties["core"]["response_id"])
        self._response_count += 1
        return f"resp_{self._response_count}"

    async def cancel_response(self) -> None:
        self.calls.append(("cancel_response",))
        await self._acknowledge("cancel_response")
        if self.cancel_response_error is not None:
            raise self.cancel_response_error

    async def truncate(self, item_id: str, content_index: int, audio_end_ms: int) -> None:
        self.calls.append(("truncate", item_id, content_index, audio_end_ms))
        await self._acknowledge("truncate")

    async def close(self) -> None:
        self.close_count += 1
        await self._acknowledge("close")
        if self.close_error is not None:
            raise self.close_error  # without ending the events, as a stuck socket does
        if self.close_raises_in_receive:
            self.fail(RealtimeConnectionException("The connection was closed."))
        else:
            self.end()


class FakeRealtimeClient:
    """A realtime client that records each start_session call and returns a fake session."""

    def __init__(self) -> None:
        self.start_session_calls: list[dict[str, Any]] = []
        self.sessions: list[FakeRealtimeSession] = []
        self.start_session_delay = 0.0
        self.start_session_error: BaseException | None = None
        self.send_error: BaseException | None = None

    @property
    def session(self) -> FakeRealtimeSession:
        """The most recently started session."""
        return self.sessions[-1]

    async def start_session(
        self, *, options: RealtimeOptions | None = None, tools: Sequence[ToolTypes] | None = None
    ) -> FakeRealtimeSession:
        self.start_session_calls.append({"options": options, "tools": tools})
        if self.start_session_delay:
            await asyncio.sleep(self.start_session_delay)
        if self.start_session_error is not None:
            raise self.start_session_error
        session = FakeRealtimeSession(options, tools)
        session.send_error = self.send_error
        self.sessions.append(session)
        return session
