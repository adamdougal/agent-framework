# Copyright (c) Microsoft. All rights reserved.

"""What a live conversation knows about the provider's responses (spec 006, Control calls and Tool loop)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .._agents import BaseAgent


class ResponseLedger:
    """What the conversation knows about the provider's responses: who each is credited to, and which are active."""

    def __init__(self) -> None:
        self._agents: dict[str, BaseAgent] = {}
        self._with_calls: set[str] = set()
        self._active: set[str] = set()
        self._finished: set[str] = set()
        self._creation_order: list[str] = []
        self._not_answering: set[str] = set()
        self._idle = asyncio.Event()
        self._idle.set()

    @property
    def any_active(self) -> bool:
        return bool(self._active)

    async def wait_until_idle(self) -> None:
        """Wait until no response is active."""
        await self._idle.wait()

    def created(self, response_id: str, agent: BaseAgent) -> None:
        """Record a response the provider created, credited to the agent active then."""
        self._agents[response_id] = agent
        self._creation_order.append(response_id)
        self._activate(response_id)

    def requested(self, response_id: str) -> None:
        """Record a response the conversation requested, which may already have finished by the time it returns."""
        if response_id not in self._finished:
            self._activate(response_id)

    def finished(self, response_id: str) -> None:
        self._active.discard(response_id)
        self._finished.add(response_id)
        if not self._active:
            self._idle.set()

    @property
    def created_count(self) -> int:
        """How many responses the provider has created so far; a marker for :meth:`answered_since`."""
        return len(self._creation_order)

    def answered_since(self, marker: int) -> bool:
        """Whether a response created after ``marker`` can act on what the conversation sent before it.

        Responses requested as filler, by tools or their middleware, don't count.
        """
        return any(response_id not in self._not_answering for response_id in self._creation_order[marker:])

    def not_answering(self, response_id: str) -> None:
        """Record that a response is filler, which doesn't act on tool results (spec 006, Tool loop)."""
        self._not_answering.add(response_id)

    def _activate(self, response_id: str) -> None:
        self._active.add(response_id)
        self._idle.clear()

    def called(self, response_id: str) -> None:
        """Record that the response made a function call."""
        self._with_calls.add(response_id)

    def made_calls(self, response_id: str) -> bool:
        return response_id in self._with_calls

    def agent_of(self, response_id: str, *, default: BaseAgent) -> BaseAgent:
        return self._agents.get(response_id, default)
