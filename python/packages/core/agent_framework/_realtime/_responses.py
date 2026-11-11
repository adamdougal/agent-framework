# Copyright (c) Microsoft. All rights reserved.

"""What a live conversation knows about the provider's responses."""

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
        self._filler: set[str] = set()
        self._answers_only_earlier: set[str] = set()
        self._idle = asyncio.Event()
        self._idle.set()
        self._requests_in_flight = 0
        self._no_requests_in_flight = asyncio.Event()
        self._no_requests_in_flight.set()

    @property
    def any_active(self) -> bool:
        return bool(self._active)

    @property
    def active(self) -> frozenset[str]:
        return frozenset(self._active)

    async def wait_until_idle(self) -> None:
        """Wait until no response is active."""
        await self._idle.wait()

    def request_started(self) -> None:
        """Record a response request sent to the provider and not yet acknowledged and classified."""
        self._requests_in_flight += 1
        self._no_requests_in_flight.clear()

    def request_settled(self) -> None:
        self._requests_in_flight -= 1
        if not self._requests_in_flight:
            self._no_requests_in_flight.set()

    async def wait_until_requests_settle(self) -> None:
        """Wait until every response request in flight is acknowledged and classified, filler included."""
        await self._no_requests_in_flight.wait()

    def created(self, response_id: str, agent: BaseAgent) -> None:
        """Record a response the provider created, credited to the agent active then.

        A response already finished, such as one ``interrupt()`` cancelled before its creation event was read, stays
        finished.
        """
        self._agents[response_id] = agent
        self._creation_order.append(response_id)
        if response_id not in self._finished:
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

        Filler doesn't count, nor does a follow-up requested before results it can't see.
        """
        return any(
            response_id not in self._filler and response_id not in self._answers_only_earlier
            for response_id in self._creation_order[marker:]
        )

    def responses_since(self, marker: int) -> int:
        """How many responses, apart from filler, the provider created after ``marker``."""
        return sum(1 for response_id in self._creation_order[marker:] if response_id not in self._filler)

    def filler(self, response_id: str) -> None:
        """Record a response that tools or middleware requested, which acts on no tool results."""
        self._filler.add(response_id)

    def answers_only_earlier_results(self, response_id: str) -> None:
        """Record a follow-up requested before results it therefore can't act on, though it answers earlier ones."""
        self._answers_only_earlier.add(response_id)

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
