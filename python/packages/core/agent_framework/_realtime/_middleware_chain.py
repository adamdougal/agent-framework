# Copyright (c) Microsoft. All rights reserved.

"""Runs a live conversation's conversation middleware.

It's apart from the public conversation-middleware types because the core middleware module imports those types,
while running them needs the core middleware module.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, TypeVar, cast

from .._middleware import MiddlewareTermination
from ._middleware import ConversationContext, ConversationMiddleware, ConversationMiddlewareTypes

if TYPE_CHECKING:
    from .._types import AgentResponseUpdate, Message
    from ._conversation import RealtimeConversation
    from ._events import RealtimeEvent

ItemT = TypeVar("ItemT")


async def open_through_middleware(
    middleware: Sequence[ConversationMiddlewareTypes],
    context: ConversationContext,
    conversation: RealtimeConversation[Any],
    open_conversation: Callable[[], Awaitable[None]],
) -> None:
    """Raises ``MiddlewareTermination`` when the middleware returns without opening the conversation."""
    opening_attempted = False

    async def call(index: int) -> None:
        nonlocal opening_attempted
        if index == len(middleware):
            if opening_attempted:
                raise RuntimeError("call_next() opens the realtime conversation once; it can't be called again.")
            opening_attempted = True
            await open_conversation()
            context.conversation = conversation
            return
        current = middleware[index]
        if isinstance(current, ConversationMiddleware):
            await current.process(context, lambda: call(index + 1))
        else:
            await current(context, lambda: call(index + 1))

    await call(0)
    if context.conversation is None:
        raise MiddlewareTermination("Conversation middleware refused the realtime session.")


async def through_send_hooks(
    context: ConversationContext, inputs: Sequence[Message | RealtimeEvent]
) -> list[Message | RealtimeEvent]:
    """Each input's replacement, leaving out inputs a hook drops with ``MiddlewareTermination``."""
    survivors: list[Message | RealtimeEvent] = []
    for item in inputs:
        try:
            survivors.append(await _through(context.send_hooks, item))
        except MiddlewareTermination:
            continue
    return survivors


async def through_update_hooks(context: ConversationContext, update: AgentResponseUpdate) -> AgentResponseUpdate | None:
    """Returns ``None`` when a hook drops the update with ``MiddlewareTermination``.

    Middleware registers its hooks on the way in, so the outermost sees each update last, after inner middleware has
    shaped it.
    """
    try:
        return await _through(reversed(context.update_hooks), update)
    except MiddlewareTermination:
        return None


async def _through(hooks: Iterable[Callable[[ItemT], ItemT | Awaitable[ItemT | None] | None]], item: ItemT) -> ItemT:
    for hook in hooks:
        replacement = hook(item)
        if inspect.isawaitable(replacement):
            replacement = await replacement
        if replacement is not None:
            item = cast(ItemT, replacement)
    return item


async def run_close_hooks(context: ConversationContext, error: Exception | None) -> list[Exception]:
    """The outermost middleware closes last, as it would unwind from ``call_next()``.

    One hook's failure doesn't skip the others' cleanup, so failures are returned for close to raise at the end. A
    conversation that never opened has nothing for its middleware to clean up.
    """
    if context.conversation is None:
        return []
    failures: list[Exception] = []
    for hook in reversed(context.close_hooks):
        try:
            outcome = hook(error)
            if inspect.isawaitable(outcome):
                await outcome
        except Exception as failure:
            failures.append(failure)
    return failures
