# Copyright (c) Microsoft. All rights reserved.

"""Awaiting a provider's acknowledgement of a control call, within a timeout and until the conversation closes."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any, TypeVar

from ..exceptions import RealtimeControlTimeoutException, RealtimeConversationClosedException

AcknowledgedT = TypeVar("AcknowledgedT")


async def await_acknowledgement(
    acknowledgement: Awaitable[AcknowledgedT], *, action: str, timeout: float, closing: asyncio.Event
) -> AcknowledgedT:
    """Cancel the session's call if the wait ends without it, which the session protocol makes safe.

    The cancelled call finishes unwinding before this returns, so it never overlaps the next control call.

    Raises:
        RealtimeControlTimeoutException: If the acknowledgement doesn't arrive in time; the provider may still apply
            the request.
        RealtimeConversationClosedException: If the conversation starts closing first.
    """
    call = asyncio.ensure_future(acknowledgement)
    closed = asyncio.ensure_future(closing.wait())
    try:
        async with asyncio.timeout(timeout):
            await asyncio.wait({call, closed}, return_when=asyncio.FIRST_COMPLETED)
    except TimeoutError as timed_out:
        raise RealtimeControlTimeoutException(
            f"The provider didn't acknowledge the request to {action} within {timeout} seconds; it may still apply it."
        ) from timed_out
    finally:
        closed.cancel()
        if not call.done():
            call.cancel()
            await _let_cancelled_call_unwind(call)
    if call.done() and not call.cancelled():
        return call.result()
    raise RealtimeConversationClosedException(f"The realtime conversation closed before it could {action}.")


async def _let_cancelled_call_unwind(call: asyncio.Future[Any]) -> None:
    """A second cancellation would otherwise abandon the call mid-cleanup.

    It's held and re-raised once the call has finished.
    """
    cancelled_again: asyncio.CancelledError | None = None
    while not call.done():
        try:
            await asyncio.wait({call})
        except asyncio.CancelledError as cancellation:
            cancelled_again = cancellation
    if cancelled_again is not None:
        raise cancelled_again
