# Copyright (c) Microsoft. All rights reserved.

"""Realtime event kinds, and the neutral attributes each kind carries under ``additional_properties["core"]``."""

# No `from __future__ import annotations`: it would hide `Required[...]` from the TypedDicts' runtime required keys.
from typing import Literal, NewType, Required

from typing_extensions import TypedDict

from .._feature_stage import ExperimentalFeature, experimental

RealtimeKindLiteral = Literal[
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
    "function_approval_request",
    "handoff_completed",
    "usage",
    "error",
    "passthrough",
]
"""The realtime event kinds Agent Framework defines. Experimental under ``ExperimentalFeature.REALTIME``.

Provider clients tag every realtime event with one of these kinds, and core dispatches on it, so apps never match
provider event names. Only core adds kinds; provider packages map anything else to ``"passthrough"``.
"""

RealtimeKind = NewType("RealtimeKind", str)
"""The open type for a realtime event kind. Experimental under ``ExperimentalFeature.REALTIME``.

Annotations accept ``RealtimeKindLiteral | RealtimeKind``, so an update carrying a kind that a newer runtime added
still deserializes. Compare kinds as strings, for example ``content.kind == "speech_started"``.
"""


def require_realtime_kind(kind: object) -> None:
    """Raise ``ValueError`` unless ``kind`` is a non-empty string; kinds a newer runtime adds are accepted."""
    if not isinstance(kind, str) or not kind:
        raise ValueError(f"A realtime event requires a non-empty string kind, not {kind!r}.")


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeSessionCreatedAttributes(TypedDict, total=False):
    """Neutral attributes of ``session_created``, under ``additional_properties["core"]``.

    Keys:
        session_id: The provider's realtime session id. Required; this is the only place it appears.
        model: The model serving the session.
        expires_at: When the session expires, in Unix seconds.
    """

    session_id: Required[str]
    model: str
    expires_at: int


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeSpeechStartedAttributes(TypedDict, total=False):
    """Neutral attributes of ``speech_started``, the barge-in cue, under ``additional_properties["core"]``.

    Keys:
        item_id: The caller audio item the speech belongs to. Required.
        audio_start_ms: Where the speech starts in the session's input audio, in milliseconds. Required.
    """

    item_id: Required[str]
    audio_start_ms: Required[int]


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeSpeechStoppedAttributes(TypedDict, total=False):
    """Neutral attributes of ``speech_stopped``, under ``additional_properties["core"]``.

    Keys:
        item_id: The caller audio item the speech belongs to. Required.
        audio_end_ms: Where the speech ends in the session's input audio, in milliseconds. Required.
    """

    item_id: Required[str]
    audio_end_ms: Required[int]


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeResponseCreatedAttributes(TypedDict, total=False):
    """Neutral attributes of ``response_created``, under ``additional_properties["core"]``.

    Keys:
        response_id: The provider's response id. Required.
        out_of_band: ``True`` for a response outside the conversation, whose output joins no conversation item.
            Absent otherwise.
    """

    response_id: Required[str]
    out_of_band: bool


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeResponseDoneAttributes(TypedDict, total=False):
    """Neutral attributes of ``response_done``, under ``additional_properties["core"]``.

    Keys:
        response_id: The provider's response id. Required.
        status: The provider's response status, verbatim, such as ``"completed"`` or ``"cancelled"``. Required.
        reason: The provider's status reason, verbatim, such as ``"turn_detected"`` or ``"content_filter"``.
        error_code: The error code of a failed response.
        error_message: The error message of a failed response.
    """

    response_id: Required[str]
    status: Required[str]
    reason: str
    error_code: str
    error_message: str


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeItemAddedAttributes(TypedDict, total=False):
    """Neutral attributes of ``item_added``, the provider's conversation order, under ``additional_properties["core"]``.

    Keys:
        item_id: The item's id. Required.
        previous_item_id: The id of the item before it, or ``None`` when it's first. Required.
        role: The role of a message item. Absent for other items, such as function calls.
        input_audio: Whether a user message item holds the caller's audio, rather than text the app or history added.
            Absent for other items.
    """

    item_id: Required[str]
    previous_item_id: Required[str | None]
    role: str
    input_audio: bool


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeItemTruncatedAttributes(TypedDict, total=False):
    """Neutral attributes of ``item_truncated``, under ``additional_properties["core"]``.

    Keys:
        item_id: The truncated assistant item. Required.
        content_index: The truncated content part. Required.
        audio_end_ms: Where the audio was cut, in milliseconds. Required.
        truncated_by: ``"client"`` when this session's own truncate request cut the item, or ``"provider"`` when the
            provider cut it on its own, such as Voice Live's ``auto_truncate``. Required.
    """

    item_id: Required[str]
    content_index: Required[int]
    audio_end_ms: Required[int]
    truncated_by: Required[Literal["client", "provider"]]


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeInputTranscriptionAttributes(TypedDict, total=False):
    """Neutral attributes of ``input_transcription``, the caller transcript, on its realtime event.

    Keys:
        item_id: The caller audio item transcribed. Required.
        content_index: The transcribed content part. Required.
    """

    item_id: Required[str]
    content_index: Required[int]


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeInputTranscriptionFailedAttributes(TypedDict, total=False):
    """Neutral attributes of ``input_transcription_failed``, under ``additional_properties["core"]``.

    Keys:
        item_id: The caller audio item that couldn't be transcribed. Required.
        content_index: The content part that couldn't be transcribed. Required.
        error_code: The provider's error code.
        error_message: The provider's error message.
    """

    item_id: Required[str]
    content_index: Required[int]
    error_code: str
    error_message: str


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeOutputTextAttributes(TypedDict, total=False):
    """Neutral attributes of ``output_text``, the agent transcript, on its realtime event.

    Keys:
        response_id: The response the text belongs to. Required.
        item_id: The assistant item the text belongs to. Required.
        content_index: The content part the text belongs to. Required.
    """

    response_id: Required[str]
    item_id: Required[str]
    content_index: Required[int]


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeOutputAudioAttributes(TypedDict, total=False):
    """Neutral attributes of ``output_audio`` on its realtime event, which barge-in uses to cut unheard audio.

    Keys:
        response_id: The response the audio belongs to. Required.
        item_id: The assistant item the audio belongs to. Required.
        content_index: The content part the audio belongs to. Required.
    """

    response_id: Required[str]
    item_id: Required[str]
    content_index: Required[int]


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeFunctionCallAttributes(TypedDict, total=False):
    """Neutral attributes of ``function_call`` on its realtime event.

    Keys:
        response_id: The response that made the call. Required.
        item_id: The function-call item. Required.
    """

    response_id: Required[str]
    item_id: Required[str]


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeHandoffCompletedAttributes(TypedDict, total=False):
    """Neutral attributes of ``handoff_completed``, under ``additional_properties["core"]``.

    Keys:
        source: The agent handed off from, by ``name``, else ``id``. Required.
        target: The agent handed off to, by ``name``, else ``id``. Required.
    """

    source: Required[str]
    target: Required[str]


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeUsageAttributes(TypedDict, total=False):
    """Neutral attributes of ``usage`` on its realtime event.

    Keys:
        response_id: The response the usage belongs to. Required.
    """

    response_id: Required[str]


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeErrorAttributes(TypedDict, total=False):
    """Neutral attributes of ``error`` on its realtime event.

    Keys:
        response_id: The failed response, when the error comes from one.
    """

    response_id: str
