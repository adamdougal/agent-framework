# Copyright (c) Microsoft. All rights reserved.

"""Reporting a live conversation's history to context providers, one exchange at a time."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING

from .._sessions import SessionContext
from .._types import AgentResponse

if TYPE_CHECKING:
    from .._agents import BaseAgent
    from .._sessions import AgentSession, ContextProvider
    from ._history import CallHistory, Exchange


class ExchangeReporter:
    def __init__(
        self,
        agent: BaseAgent,
        session: AgentSession,
        history: CallHistory,
        on_failure: Callable[[ContextProvider, Exception], None],
    ) -> None:
        self._agent = agent
        self._session = session
        self._history = history
        self._on_failure = on_failure
        self._next: dict[ContextProvider, int] = {}
        self._wake = asyncio.Event()
        self._reporting = asyncio.Lock()
        self._start_context = SessionContext(input_messages=[])

    def start(self, context: SessionContext) -> None:
        self._start_context = context

    def wake(self) -> None:
        self._wake.set()

    async def run(self) -> None:
        while True:
            await self._wake.wait()
            self._wake.clear()
            async with self._reporting:
                await self._report(self._history.exchanges()[:-1])

    async def report_remaining(self) -> list[Exception]:
        """Close raises these after cleanup, so a provider that couldn't record the call doesn't fail silently."""
        async with self._reporting:
            return list((await self._report(self._history.exchanges())).values())

    async def _report(self, ready: list[Exchange]) -> dict[ContextProvider, Exception]:
        """A failing provider keeps its place and retries next time; the others carry on."""
        providers = list(reversed(self._agent.context_providers))
        failed: dict[ContextProvider, Exception] = {}
        start = min(self._next.get(provider, 0) for provider in providers)
        for index, exchange in enumerate(ready[start:], start):
            if not exchange.settled:
                break
            self._history.freeze(exchange)
            context = self._context_for(exchange, first=index == 0)
            for provider in providers:
                if provider in failed or self._next.get(provider, 0) != index:
                    continue
                try:
                    await provider.after_run(
                        # A realtime agent has no run(), which SupportsAgentRun requires; providers read its identity.
                        agent=self._agent,  # type: ignore[arg-type]
                        session=self._session,
                        context=context,
                        state=self._session.state.setdefault(provider.source_id, {}),
                    )
                except Exception as error:
                    failed[provider] = error
                    self._on_failure(provider, error)
                    continue
                self._next[provider] = index + 1
        return failed

    def _context_for(self, exchange: Exchange, *, first: bool) -> SessionContext:
        """The start context goes with the first report only, so history doesn't store it again for each exchange."""
        start = self._start_context
        context = SessionContext(
            session_id=self._session.session_id,
            input_messages=exchange.inputs,
            context_messages=dict(start.context_messages) if first else None,
            instructions=list(start.instructions) if first else None,
            tools=list(start.tools) if first else None,
            options=start.options,
            metadata=start.metadata,
        )
        # Providers only read the response, so SessionContext keeps it private and the framework sets it, as chat does.
        context._response = AgentResponse(  # pyright: ignore[reportPrivateUsage]
            messages=exchange.outputs,
            response_id=exchange.response_id,
            finish_reason=exchange.finish_reason,
            usage_details=exchange.usage,
        )
        return context
