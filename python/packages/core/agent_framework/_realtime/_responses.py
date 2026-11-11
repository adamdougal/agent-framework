# Copyright (c) Microsoft. All rights reserved.

"""What a live conversation knows about the provider's responses."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .._agents import BaseAgent


class ResponseLedger:
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
        await self._idle.wait()

    def request_started(self) -> None:
        """Keep filler classification ahead of batch-limit checks."""
        self._requests_in_flight += 1
        self._no_requests_in_flight.clear()

    def request_settled(self) -> None:
        self._requests_in_flight -= 1
        if not self._requests_in_flight:
            self._no_requests_in_flight.set()

    async def wait_until_requests_settle(self) -> None:
        await self._no_requests_in_flight.wait()

    def created(self, response_id: str, agent: BaseAgent) -> None:
        """A response already finished stays finished.

        ``interrupt()`` can finish a response before its creation event is read.
        """
        self._agents[response_id] = agent
        self._creation_order.append(response_id)
        if response_id not in self._finished:
            self._activate(response_id)

    def requested(self, response_id: str) -> None:
        """The response may have finished before its request returned, so it isn't revived."""
        if response_id not in self._finished:
            self._activate(response_id)

    def finished(self, response_id: str) -> None:
        self._active.discard(response_id)
        self._finished.add(response_id)
        if not self._active:
            self._idle.set()

    @property
    def created_count(self) -> int:
        return len(self._creation_order)

    def answered_since(self, marker: int) -> bool:
        """Filler doesn't count, nor does a follow-up requested before results it can't see."""
        return any(
            response_id not in self._filler and response_id not in self._answers_only_earlier
            for response_id in self._creation_order[marker:]
        )

    def responses_since(self, marker: int) -> int:
        """Filler isn't a follow-up, so it isn't counted."""
        return sum(1 for response_id in self._creation_order[marker:] if response_id not in self._filler)

    def filler(self, response_id: str) -> None:
        """Filler acts on no tool results, so it never counts as a follow-up."""
        self._filler.add(response_id)

    def answers_only_earlier_results(self, response_id: str) -> None:
        """This follow-up can't act on results sent after it was requested.

        The provider snapshots the conversation when a response is requested.
        """
        self._answers_only_earlier.add(response_id)

    def _activate(self, response_id: str) -> None:
        self._active.add(response_id)
        self._idle.clear()

    def called(self, response_id: str) -> None:
        self._with_calls.add(response_id)

    def made_calls(self, response_id: str) -> bool:
        return response_id in self._with_calls

    def agent_of(self, response_id: str, *, default: BaseAgent) -> BaseAgent:
        return self._agents.get(response_id, default)
