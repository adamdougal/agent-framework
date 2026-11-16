# Copyright (c) Microsoft. All rights reserved.

"""Reporting a closed conversation's history to context providers, one exchange at a time."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from .._sessions import SessionContext
from .._types import AgentResponse

if TYPE_CHECKING:
    from .._agents import BaseAgent
    from .._sessions import AgentSession, ContextProvider
    from ._history import Exchange


async def report_exchanges(
    agent: BaseAgent, session: AgentSession, start: SessionContext, exchanges: Sequence[Exchange]
) -> list[Exception]:
    """A failing provider skips the later exchanges, so one store's outage can't lose the call for the others."""
    providers = list(reversed(agent.context_providers))
    failures: dict[ContextProvider, Exception] = {}
    for index, exchange in enumerate(exchanges):
        context = _context_for(session, start, exchange, first=index == 0)
        for provider in providers:
            if provider in failures:
                continue
            try:
                await provider.after_run(
                    # A realtime agent has no run(), which SupportsAgentRun requires; providers read its identity.
                    agent=agent,  # type: ignore[arg-type]
                    session=session,
                    context=context,
                    state=session.state.setdefault(provider.source_id, {}),
                )
            except Exception as error:
                failures[provider] = error
    return list(failures.values())


def _context_for(session: AgentSession, start: SessionContext, exchange: Exchange, *, first: bool) -> SessionContext:
    """The start context goes with the first report only, so history doesn't store it again for each exchange."""
    context = SessionContext(
        session_id=session.session_id,
        input_messages=exchange.inputs,
        context_messages=dict(start.context_messages) if first else None,
        instructions=list(start.instructions) if first else None,
        tools=list(start.tools) if first else None,
        options=start.options,
    )
    # SessionContext swaps an empty dict for a new one, which would stop providers sharing metadata across reports.
    context.metadata = start.metadata
    # Providers only read the response, so SessionContext keeps it private and the framework sets it, as chat does.
    context._response = AgentResponse(  # pyright: ignore[reportPrivateUsage]
        messages=exchange.outputs,
        response_id=exchange.response_id,
        finish_reason=exchange.finish_reason,
        usage_details=exchange.usage,
    )
    return context
