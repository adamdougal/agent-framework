# Copyright (c) Microsoft. All rights reserved.

"""The conversation's bounded update stream, whose end and failure are held outside the buffer."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator

from .._types import AgentResponseUpdate


class UpdateStream:
    """The end lives outside the bounded buffer, so closing or failing wakes the reader even when it's full."""

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._buffer: deque[AgentResponseUpdate] = deque()
        self._finished = False
        self._failure: Exception | None = None
        self._changed = asyncio.Event()

    def put(self, update: AgentResponseUpdate) -> bool:
        """Never block the provider reader on a slow update consumer: a full buffer returns ``False`` instead."""
        if len(self._buffer) >= self._capacity:
            return False
        self._buffer.append(update)
        self._changed.set()
        return True

    def finish(self, failure: Exception | None = None) -> None:
        """The buffered updates are read before the end or the failure, so none are lost."""
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
