# Copyright (c) Microsoft. All rights reserved.

"""A realtime agent's half-duplex run: the turn's updates, and what the run returns from them."""

from __future__ import annotations

import contextlib
import copy
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from .._types import AgentResponse, AgentResponseUpdate, ResponseStream
from ..exceptions import RealtimeConversationClosedException, RealtimeException

if TYPE_CHECKING:
    from ._conversation import RealtimeConversation


def run_stream(
    conversation: RealtimeConversation[Any], *, turn_detection_responds: bool
) -> ResponseStream[AgentResponseUpdate, AgentResponse]:
    return ResponseStream(_turn_updates(conversation, turn_detection_responds), finalizer=response_from)


async def _turn_updates(
    conversation: RealtimeConversation[Any], turn_detection_responds: bool
) -> AsyncIterator[AgentResponseUpdate]:
    async with conversation:
        if not turn_detection_responds:
            # A close that interrupts the request has its own cause, such as a failed response, which iteration raises.
            with contextlib.suppress(RealtimeConversationClosedException):
                await conversation.create_response()
        async for update in conversation:
            yield update


def response_from(updates: Sequence[AgentResponseUpdate]) -> AgentResponse:
    """Caller transcripts and signals stream with a run but aren't the agent's messages, so they're left out."""
    agent_updates: list[AgentResponseUpdate] = []
    for update in updates:
        if update.role not in ("assistant", "tool"):
            continue
        agent_update = copy.copy(update)
        agent_update.contents = [content for content in update.contents if content.type != "realtime_event"]
        if any(content.type == "text" and content.merge_mode == "replace" for content in agent_update.contents):
            _drop_earlier_text(agent_updates, update.message_id)
        agent_updates.append(agent_update)
    return AgentResponse.from_updates(agent_updates)


def _drop_earlier_text(updates: Sequence[AgentResponseUpdate], message_id: str | None) -> None:
    """A final transcript replaces its deltas, but only adjacent text merges, and audio usually falls between them."""
    for update in updates:
        if update.message_id == message_id:
            update.contents = [content for content in update.contents if content.type != "text"]


def failed_response_error(response_done: Mapping[str, Any]) -> RealtimeException:
    """A failed response has no reply to return, and a host that retries the run needs to know it failed."""
    code = response_done.get("error_code") or "an unknown error"
    message = response_done.get("error_message") or "no message"
    return RealtimeException(f"The realtime response failed with {code}: {message}")
