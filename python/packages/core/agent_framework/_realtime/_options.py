# Copyright (c) Microsoft. All rights reserved.

"""Realtime session options: the neutral settings every realtime client translates."""

from __future__ import annotations

import sys
from collections.abc import Mapping, Sequence
from typing import Literal

from typing_extensions import TypedDict

from .._feature_stage import ExperimentalFeature, experimental
from .._types import ToolMode

if sys.version_info >= (3, 13):
    from typing import TypeVar  # pragma: no cover
else:
    from typing_extensions import TypeVar  # pragma: no cover


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeTurnDetection(TypedDict, total=False):
    """How the provider decides the caller has finished speaking.

    Provider option types extend this with their own settings, such as OpenAI's ``eagerness`` or Voice Live's
    ``auto_truncate``.

    Keys:
        type: The detector, such as ``"server_vad"``, ``"semantic_vad"`` or a provider's own type.
        create_response: Whether the provider requests a response when the caller stops speaking.
        interrupt_response: Whether the caller speaking cancels the active response.
        threshold: The voice-activity threshold, from 0.0 to 1.0.
        prefix_padding_ms: The audio kept from before speech started, in milliseconds.
        silence_duration_ms: The silence that ends the caller's turn, in milliseconds.
    """

    type: str
    create_response: bool
    interrupt_response: bool
    threshold: float
    prefix_padding_ms: int
    silence_duration_ms: int


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeTranscription(TypedDict, total=False):
    """How the provider transcribes the caller's speech into the caller transcript.

    Keys:
        model: The transcription model.
        language: The caller's language, such as ``"en"``.
    """

    model: str
    language: str


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeNoiseReduction(TypedDict, total=False):
    """How the provider reduces noise in the caller's audio.

    Keys:
        type: The reduction, such as ``"near_field"``, ``"far_field"`` or a provider's own type.
    """

    type: str


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeOptions(TypedDict, total=False):
    """Settings for a realtime session, as an agent's defaults, per run or in a session update.

    Each realtime client translates these neutral keys to its provider's session fields. It drops, with a warning, any
    key it can't honor, and raises before connecting for an audio format the provider can't take. ``None`` turns a
    feature off. Provider option types, such as ``OpenAIRealtimeOptions`` and ``VoiceLiveOptions``, extend this.

    Keys:
        instructions: The system instructions.
        voice: The output voice. Azure option types widen it to accept an ``AzureVoice``.
        tool_choice: How the model chooses tools.
        temperature: The sampling temperature.
        max_tokens: The most output tokens a response may use.
        metadata: Key-value pairs attached to the session or response.
        input_audio_format: The caller audio format: ``"audio/pcm;rate=24000"``, ``"audio/pcmu"`` or
            ``"audio/pcma"``.
        output_audio_format: The agent audio format, from the same values.
        output_modalities: What responses produce, such as ``["audio"]`` or ``["text"]``.
        turn_detection: How the caller's turn ends, or ``None`` for no turn detection.
        transcription: How the caller is transcribed, or ``None`` for no caller transcript.
        noise_reduction: How caller audio is cleaned, or ``None`` for none.
    """

    instructions: str
    voice: str
    tool_choice: ToolMode | Literal["auto", "required", "none"]
    temperature: float
    max_tokens: int
    metadata: Mapping[str, str]
    input_audio_format: str
    output_audio_format: str
    output_modalities: Sequence[str]
    turn_detection: RealtimeTurnDetection | None
    transcription: RealtimeTranscription | None
    noise_reduction: RealtimeNoiseReduction | None


RealtimeOptionsT = TypeVar(
    "RealtimeOptionsT",
    bound=TypedDict,  # type: ignore[valid-type]
    default=RealtimeOptions,
)
"""The realtime options type an agent, conversation or client is generic over."""
