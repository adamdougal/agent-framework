# Copyright (c) Microsoft. All rights reserved.

"""The conversation's bounded update stream, whose end and failure are held outside the buffer."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator

from .._types import AgentResponseUpdate


class UpdateStream:
    """The conversation's updates, read in order by one reader, with its end held outside the buffer."""

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._buffer: deque[AgentResponseUpdate] = deque()
        self._finished = False
        self._failure: Exception | None = None
        self._changed = asyncio.Event()

    def put(self, update: AgentResponseUpdate) -> bool:
        """Buffer ``update``, returning ``False`` when the buffer is full. Updates after the end are dropped."""
        if self._finished:
            return True
        if len(self._buffer) >= self._capacity:
            return False
        self._buffer.append(update)
        self._changed.set()
        return True

    def finish(self, failure: Exception | None = None) -> None:
        """End the stream once the buffered updates are read, raising ``failure`` then if there is one."""
        if self._finished:
            return
        self._finished = True
        self._failure = failure
        self._changed.set()

    async def __aiter__(self) -> AsyncIterator[AgentResponseUpdate]:
        while True:
            if self._buffer:
                yield self._buffer.popleft()
                continue
            if self._finished:
                if self._failure is not None:
                    raise self._failure
                return
            self._changed.clear()
            await self._changed.wait()
