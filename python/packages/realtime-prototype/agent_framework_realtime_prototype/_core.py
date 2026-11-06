# PROTOTYPE, never merged (adamdougal/agent-framework#10).
"""Core realtime types for the prototype: kinds, events, errors, options and conversation middleware.

Stands in for the additive changes the extended ADR 0035 would make to ``agent-framework-core``. Every change to an
existing core type is applied in memory by ``apply_core_patches()`` and listed in ``CORE_CHANGES``, so the prototype's
findings can name each one.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal, TypedDict

from agent_framework import Content, FunctionInvocationContext
from agent_framework.exceptions import AgentFrameworkException
from opentelemetry import trace

logger = logging.getLogger("agent_framework.realtime")
tracer = trace.get_tracer("agent_framework.realtime")

# region Kinds (#14, #17, #7)

SESSION_CREATED = "session_created"
SESSION_UPDATED = "session_updated"
SPEECH_STARTED = "speech_started"
SPEECH_STOPPED = "speech_stopped"
RESPONSE_CREATED = "response_created"
RESPONSE_DONE = "response_done"
ITEM_ADDED = "item_added"
ITEM_TRUNCATED = "item_truncated"
INPUT_TRANSCRIPTION = "input_transcription"
INPUT_TRANSCRIPTION_FAILED = "input_transcription_failed"
OUTPUT_TEXT = "output_text"
OUTPUT_AUDIO = "output_audio"
FUNCTION_CALL = "function_call"
FUNCTION_RESULT = "function_result"
USAGE = "usage"
ERROR = "error"
PASSTHROUGH = "passthrough"
HANDOFF_COMPLETED = "handoff_completed"

RealtimeKind = Literal[
    "session_created",
    "session_updated",
    "speech_started",
    "speech_stopped",
    "response_created",
    "response_done",
    "item_added",
    "item_truncated",
    "input_transcription",
    "input_transcription_failed",
    "output_text",
    "output_audio",
    "function_call",
    "function_result",
    "usage",
    "error",
    "passthrough",
    "handoff_completed",
]

# endregion

# region Core patches

CORE_CHANGES: list[str] = []


def _from_realtime_event(
    cls: type[Content],
    kind: str,
    *,
    service_event_type: str | None = None,
    attributes: dict[str, Any] | None = None,
    raw_representation: Any = None,
) -> Content:
    return cls(
        "realtime_event",  # type: ignore[arg-type]
        additional_properties={"kind": kind, "service_event_type": service_event_type, **(attributes or {})},
        raw_representation=raw_representation,
    )


def apply_core_patches() -> None:
    """Apply the additive core changes the prototype needs, in memory."""
    if getattr(Content, "from_realtime_event", None) is None:
        Content.from_realtime_event = classmethod(_from_realtime_event)  # type: ignore[attr-defined]
        CORE_CHANGES.append(
            "Content.from_realtime_event(kind=...) and a 'realtime_event' ContentType: Content's constructor "
            "accepts the unknown type at runtime, but ContentType and Content.from_dict's type check (_types.py:2047) "
            "reject it, so a serialised update with realtime-event content wouldn't round-trip."
        )
    if not hasattr(FunctionInvocationContext, "realtime_conversation"):
        FunctionInvocationContext.realtime_conversation = None  # type: ignore[attr-defined]
        CORE_CHANGES.append(
            "FunctionInvocationContext.realtime_conversation: set by an internal function middleware the "
            "conversation always installs, because _auto_invoke_function builds the context itself and only passes "
            "a context to tools without middleware when the tool declares a ctx parameter."
        )
    CORE_CHANGES.append(
        "Content.from_text(status=): Content() already takes status, and text merges keep the latest status "
        "(_types.py:2142), so only the factory keyword is missing. The prototype calls Content('text', status=...)."
    )


def text_content(text: str, *, status: str | None = None) -> Content:
    return Content("text", text=text, status=status)


def event_content(kind: str, service_event_type: str | None = None, **attributes: Any) -> Content:
    return Content.from_realtime_event(  # type: ignore[attr-defined]
        kind, service_event_type=service_event_type, attributes=attributes
    )


def kind_of(content: Content) -> str | None:
    """The realtime kind of realtime-event content, else None."""
    if content.type != "realtime_event":
        return None
    return (content.additional_properties or {}).get("kind")


# endregion

# region Provider events


@dataclass
class RealtimeEvent:
    """One provider event, tagged with an Agent Framework kind (#14).

    ``content`` carries the payload for content kinds; ``attributes`` holds the neutral attributes each kind documents
    (response id, item id, content index, ...). Outbound raw commands are ``kind="passthrough"`` with the wire JSON in
    ``raw_representation``.
    """

    kind: str
    service_event_type: str | None = None
    content: Content | None = None
    attributes: dict[str, Any] = field(default_factory=dict)
    raw_representation: Any = None


# endregion

# region Errors (#12)


class RealtimeError(AgentFrameworkException):
    """Base class for realtime errors."""


class RealtimeControlError(RealtimeError):
    """A control call that the provider rejected, carrying the claimed provider error (#8)."""

    def __init__(self, message: str, *, code: str | None = None, param: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.param = param


class RealtimeControlTimeoutError(RealtimeError):
    """No acknowledgement within the conversation's acknowledgement timeout."""


class RealtimeActiveResponseError(RealtimeError):
    """create_response() or say() while a response was active, or the provider dropped the request (#8 lost race)."""


class RealtimeUnsupportedError(RealtimeError):
    """The provider can't do what was asked, such as say() with an OpenAI voice on Voice Live (#8)."""


class RealtimeConversationClosedError(RealtimeError):
    """The conversation is closed."""


# endregion

# region Options (#8)


class AzureVoice(TypedDict, total=False):
    name: str
    type: str
    rate: str
    temperature: float
    endpoint_id: str


class RealtimeTurnDetection(TypedDict, total=False):
    type: str
    create_response: bool
    interrupt_response: bool
    silence_duration_ms: int
    threshold: float
    prefix_padding_ms: int
    eagerness: str
    auto_truncate: bool


class RealtimeTranscription(TypedDict, total=False):
    model: str
    language: str


class RealtimeOptions(TypedDict, total=False):
    """Neutral, flat keys every client translates. Azure clients widen ``voice`` to an ``AzureVoice``."""

    instructions: str
    voice: str | AzureVoice
    tool_choice: str
    temperature: float
    max_tokens: int
    metadata: dict[str, str]
    input_audio_format: str
    output_audio_format: str
    output_modalities: list[str]
    turn_detection: RealtimeTurnDetection | None
    transcription: RealtimeTranscription | None
    noise_reduction: dict[str, Any] | None
    echo_cancellation: dict[str, Any] | None


PARTICIPANT_KEYS = ("instructions", "voice", "tool_choice")
"""The settings a handoff swaps (#7). Everything else stays as the call started."""

# endregion

# region Conversation middleware (#9)

UpdateHook = Callable[[Any], Awaitable[Any]]
SendHook = Callable[[Any], Awaitable[Any]]
CloseHook = Callable[[BaseException | None], Awaitable[None]]


@dataclass
class ConversationContext:
    agent: Any
    session: Any
    options: dict[str, Any]
    tools: list[Any]
    metadata: dict[str, Any] = field(default_factory=dict)
    conversation: Any = None
    send_hooks: list[SendHook] = field(default_factory=list)
    update_hooks: list[UpdateHook] = field(default_factory=list)
    close_hooks: list[CloseHook] = field(default_factory=list)


class ConversationMiddleware(ABC):
    """Runs once whenever a realtime session opens. Hooks registered on the context see sends, updates and close."""

    @abstractmethod
    async def process(self, context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None: ...


# endregion
