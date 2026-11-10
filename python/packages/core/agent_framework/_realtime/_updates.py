# Copyright (c) Microsoft. All rights reserved.

"""Turning each realtime event into the one update the conversation streams (spec 006, Event kinds)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .._types import AgentResponseUpdate, Content, FinishReasonLiteral
from ._events import RealtimeEvent
from ._kinds import RealtimeKind

if TYPE_CHECKING:
    from .._agents import BaseAgent

# An update takes the role of the item it's about: the caller's speech and transcripts, or the agent's response.
_USER_KINDS = frozenset({"speech_started", "speech_stopped", "input_transcription", "input_transcription_failed"})
_ASSISTANT_KINDS = frozenset({
    "response_created",
    "response_done",
    "item_truncated",
    "output_text",
    "output_audio",
    "function_call",
    "usage",
})

_INCOMPLETE_FINISH_REASONS: dict[str, FinishReasonLiteral] = {
    "content_filter": "content_filter",
    "max_output_tokens": "length",
}


def update_from_event(event: RealtimeEvent, *, author: BaseAgent, made_calls: bool = False) -> AgentResponseUpdate:
    """Build the update for one provider event.

    Args:
        event: The provider event.

    Keyword Args:
        author: The agent credited with assistant and tool output: the one active when its response was created.
        made_calls: Whether the event's response produced function calls, which ``response_done`` reports as
            ``finish_reason="tool_calls"``.
    """
    content = event.content
    if content is None:
        content = Content.from_realtime_event(
            RealtimeKind(event.kind),
            service_event_type=event.service_event_type,
            additional_properties=dict(event.additional_properties),
        )
    core = core_attributes(event)
    role = _role_of(event)
    credited = role in ("assistant", "tool")
    return AgentResponseUpdate(
        contents=[content],
        role=role,
        author_name=author.name if credited else None,
        agent_id=author.id if credited else None,
        response_id=core.get("response_id"),
        message_id=core.get("item_id"),
        finish_reason=_finish_reason_of(core, made_calls) if event.kind == "response_done" else None,
        raw_representation=event,
    )


def tool_result_update(result: Content, *, response_id: str, author: BaseAgent) -> AgentResponseUpdate:
    """Build the ``function_result`` update for one settled call, credited like its response."""
    return AgentResponseUpdate(
        contents=[result],
        role="tool",
        author_name=author.name,
        agent_id=author.id,
        response_id=response_id,
    )


def core_attributes(event: RealtimeEvent) -> dict[str, Any]:
    """Return the event's neutral attributes, which live under ``additional_properties["core"]``."""
    return dict(event.additional_properties.get("core") or {})


def _role_of(event: RealtimeEvent) -> str | None:
    if event.kind in _USER_KINDS:
        return "user"
    if event.kind in _ASSISTANT_KINDS:
        return "assistant"
    if event.kind == "item_added":
        return core_attributes(event).get("role")
    if event.kind == "error" and core_attributes(event).get("response_id"):
        return "assistant"
    return None


def _finish_reason_of(response_done: dict[str, Any], made_calls: bool) -> FinishReasonLiteral | None:
    status = response_done.get("status")
    if status == "completed":
        return "tool_calls" if made_calls else "stop"
    if status == "incomplete":
        return _INCOMPLETE_FINISH_REASONS.get(response_done.get("reason", ""))
    return None
