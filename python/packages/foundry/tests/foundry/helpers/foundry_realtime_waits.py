# Copyright (c) Microsoft. All rights reserved.

"""Waits and readers shared by the Voice Live and Foundry voice agent tests.

Each provider keeps its own fake; these helpers only wait on what a fake records or a session yields.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable, Sequence
from typing import Any


async def until(condition: Callable[[], Any], timeout: float = 5.0) -> None:
    """Wait until ``condition()`` holds."""

    async def poll() -> None:
        while not condition():  # noqa: ASYNC110 - the condition reads state another task updates
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), timeout)


async def wait_sent(sent: Sequence[dict[str, Any]], event_type: str, timeout: float = 5.0) -> dict[str, Any]:
    """Wait for the next event of ``event_type`` recorded in ``sent`` after this call, and return it."""
    already_sent = len(sent)

    async def poll() -> dict[str, Any]:
        while True:
            for event in sent[already_sent:]:
                if event["type"] == event_type:
                    return event
            await asyncio.sleep(0.005)

    return await asyncio.wait_for(poll(), timeout)


async def read_all(session: Any) -> list[Any]:
    """Every realtime event the session yields until its connection ends."""
    return [event async for event in session.receive()]


@contextlib.asynccontextmanager
async def reading(session: Any) -> AsyncGenerator[list[Any]]:
    """Read the session's events in the background for the duration of the block, as the conversation does."""
    events: list[Any] = []

    async def read() -> None:
        async for event in session.receive():
            events.append(event)

    task = asyncio.create_task(read())
    try:
        yield events
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
