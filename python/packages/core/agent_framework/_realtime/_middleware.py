# Copyright (c) Microsoft. All rights reserved.

"""Conversation middleware: the policy around a realtime agent's live conversation, from opening to close."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, TypeAlias

from .._feature_stage import ExperimentalFeature, experimental

if TYPE_CHECKING:
    from .._types import AgentResponseUpdate, Message
    from ._agent import RawRealtimeAgent
    from ._conversation import RealtimeConversation
    from ._events import RealtimeEvent


@experimental(feature_id=ExperimentalFeature.REALTIME)
class ConversationContext:
    """What conversation middleware sees of a live conversation, and where it registers its hooks.

    Middleware registers hooks on the way in, before ``call_next()``, so they run in onion order: the outermost
    middleware sees what the app sends first, and what the conversation yields and its close last. A hook may be a plain
    or an async function.

    Attributes:
        agent: The agent that started the conversation. For a handoff agent that's the agent the builder built, not
            the participant speaking.
        conversation: The conversation, once ``call_next()`` has opened it; ``None`` before.
        metadata: Values the conversation's middleware share.
        send_hooks: Hooks that see each value the app passes to ``send(...)``: each message after normalization, its
            tool results and approval responses included, or the raw ``RealtimeEvent``. A hook returns a replacement,
            which is handled as if the app had sent it, or ``None`` to keep the value; raising
            ``MiddlewareTermination`` drops it, so it isn't sent. Control calls and the tool loop's own results don't
            pass through them.
        update_hooks: Hooks that see each update as the app reads it, audio included, even after close. A hook returns
            a replacement or ``None`` to keep the update; raising ``MiddlewareTermination`` drops it. History and the
            tool loop act on what the provider sent, so dropping a ``function_call`` update doesn't stop the call.
            Build a replacement rather than changing the update in place: its contents are the ones history and the
            tool loop hold, so an in-place change would reach them too.
        close_hooks: Hooks that run once the conversation has closed and reported its history, with the exception that
            ended it, such as an error raised from the conversation's ``async with`` block, or ``None``. They run only
            for a conversation that opened.

    Any other exception from a send or update hook ends the conversation: ``send(...)`` or iteration raises it once
    close has finished, and after an update hook's exception no update still queued is delivered. A close hook's
    exception raises from ``close()`` after cleanup.
    """

    def __init__(self, *, agent: RawRealtimeAgent[Any]) -> None:
        """Create the context for a conversation that ``agent`` started.

        Keyword Args:
            agent: The agent that started the conversation.
        """
        self.agent = agent
        self.conversation: RealtimeConversation[Any] | None = None
        self.metadata: dict[str, Any] = {}
        self.send_hooks: list[
            Callable[
                [Message | RealtimeEvent],
                Message | RealtimeEvent | Awaitable[Message | RealtimeEvent | None] | None,
            ]
        ] = []
        self.update_hooks: list[
            Callable[
                [AgentResponseUpdate],
                AgentResponseUpdate | Awaitable[AgentResponseUpdate | None] | None,
            ]
        ] = []
        self.close_hooks: list[Callable[[Exception | None], Awaitable[None] | None]] = []


@experimental(feature_id=ExperimentalFeature.REALTIME)
class ConversationMiddleware(ABC):
    """Middleware around a realtime agent's live conversation, such as a guardrail.

    ``process()`` runs once as the conversation opens, and the hooks it registers on the context see the call from
    there to its close. A plain async function whose first parameter is annotated ``ConversationContext`` works as
    conversation middleware too. ``RealtimeAgent`` runs conversation middleware from its own ``middleware=`` and from
    ``start_conversation(middleware=...)``, in that order; ``RawRealtimeAgent`` and the chat ``Agent`` skip it with a
    warning.

    Examples:
        .. code-block:: python

            from agent_framework import ConversationContext, ConversationMiddleware, MiddlewareTermination


            class BlockedTopicGuardrail(ConversationMiddleware):
                async def process(self, context: ConversationContext, call_next):
                    blocked: set[str] = set()

                    async def check(update):
                        if update.response_id in blocked:
                            raise MiddlewareTermination()  # drop the rest of the reply
                        if update.role == "assistant" and "pineapple" in update.text.lower():
                            blocked.add(update.response_id)
                            await context.conversation.interrupt()
                            raise MiddlewareTermination()

                    context.update_hooks.append(check)
                    await call_next()


            agent = RealtimeAgent(client, middleware=[BlockedTopicGuardrail()])
    """

    @abstractmethod
    async def process(self, context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        """Process a live conversation as it opens.

        Args:
            context: The conversation's context. Register hooks on it before calling ``call_next()``.
            call_next: Opens the conversation, through any inner middleware. Each middleware's ``call_next()`` can be
                called only once.

        Note:
            Raising ``MiddlewareTermination`` before ``call_next()``, or returning without calling it, refuses the
            realtime session: entering the conversation raises ``MiddlewareTermination`` and nothing connects. Any
            exception raised after ``call_next()`` closes the conversation, and entering raises it.
        """


ConversationMiddlewareCallable = Callable[[ConversationContext, Callable[[], Awaitable[None]]], Awaitable[None]]
ConversationMiddlewareTypes: TypeAlias = ConversationMiddleware | ConversationMiddlewareCallable
