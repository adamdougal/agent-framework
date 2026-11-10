# Copyright (c) Microsoft. All rights reserved.

"""Azure realtime option types: Voice Live's session settings, shared with Foundry voice agents.

The fields follow the Voice Live ``2026-07-15`` reference. This module omits ``from __future__ import annotations`` so
``Required[...]`` keys are recorded at runtime.
"""

import sys
from collections.abc import Mapping, Sequence
from typing import Literal, Required

from agent_framework import RealtimeNoiseReduction, RealtimeOptions, RealtimeTranscription, RealtimeTurnDetection
from agent_framework._feature_stage import ExperimentalFeature, experimental
from typing_extensions import TypedDict

if sys.version_info >= (3, 13):
    from typing import TypeVar  # pragma: no cover
else:
    from typing_extensions import TypeVar  # pragma: no cover


@experimental(feature_id=ExperimentalFeature.REALTIME)
class AzureVoice(TypedDict, total=False):
    """An Azure text-to-speech voice, or an ``azure-realtime`` model's native voice.

    Keys:
        type: ``"azure-standard"``, ``"azure-custom"``, ``"azure-personal"`` or ``"azure-realtime-native"``.
        name: The voice name, such as ``"en-US-AvaNeural"``.
        endpoint_id: A custom voice's endpoint ID. Required for ``azure-custom``.
        model: A personal voice's base model, such as ``"DragonLatestNeural"``.
        temperature: The voice's variability, from 0.0 to 1.0.
        custom_lexicon_url: The URL of a custom lexicon.
        custom_text_normalization_url: The URL of custom text normalization.
        prefer_locales: The accents to prefer per language, such as ``["en-GB", "es-ES"]``.
        locale: The one locale to speak every language in.
        style: The speaking style, such as ``"cheerful"``.
        pitch: A pitch adjustment, as the SSML prosody element's ``pitch``.
        rate: A speaking rate adjustment, as the SSML prosody element's ``rate``.
        volume: A volume adjustment, as the SSML prosody element's ``volume``.
    """

    type: Required[Literal["azure-standard", "azure-custom", "azure-personal", "azure-realtime-native"]]
    name: Required[str]
    endpoint_id: str
    model: str
    temperature: float
    custom_lexicon_url: str
    custom_text_normalization_url: str
    prefer_locales: Sequence[str]
    locale: str
    style: str
    pitch: str
    rate: str
    volume: str


@experimental(feature_id=ExperimentalFeature.REALTIME)
class AzureEndOfUtteranceDetection(TypedDict, total=False):
    """Azure end-of-utterance detection, which allows natural pauses before ending the caller's turn.

    Keys:
        model: ``"semantic_detection_v1"``, ``"semantic_detection_v1_en"`` or ``"semantic_detection_v1_multilingual"``.
        threshold_level: ``"low"``, ``"medium"``, ``"high"`` or ``"default"``.
        timeout_ms: The longest wait for more speech, in milliseconds.
    """

    model: str
    threshold_level: Literal["low", "medium", "high", "default"]
    timeout_ms: float


@experimental(feature_id=ExperimentalFeature.REALTIME)
class AzureTurnDetection(RealtimeTurnDetection, total=False):
    """Turn detection with Voice Live's detectors and settings.

    ``type`` may also be ``"azure_semantic_vad"`` or ``"azure_semantic_vad_multilingual"``.

    Keys:
        speech_duration_ms: The shortest speech that starts a turn, in milliseconds.
        end_of_utterance_detection: Semantic end-of-utterance detection.
        auto_truncate: Whether an interrupted response is truncated automatically.
        eagerness: How soon ``semantic_vad`` ends the caller's turn: ``"low"``, ``"medium"``, ``"high"`` or ``"auto"``.
        remove_filler_words: Whether Azure semantic VAD ignores filler words.
        languages: The languages Azure semantic VAD listens for.
    """

    speech_duration_ms: int
    end_of_utterance_detection: AzureEndOfUtteranceDetection
    auto_truncate: bool
    eagerness: Literal["low", "medium", "high", "auto"]
    remove_filler_words: bool
    languages: Sequence[str]


@experimental(feature_id=ExperimentalFeature.REALTIME)
class AzureTranscription(RealtimeTranscription, total=False):
    """Caller transcription with Voice Live's models and settings.

    ``model`` may also be ``"azure-speech"``, ``"mai-transcribe"`` or ``"mai-transcribe-2"``.

    Keys:
        custom_speech: For ``azure-speech``, a map of locale to a custom speech endpoint ID.
        phrase_list: Phrases to bias recognition towards.
        prompt: Text that guides the OpenAI transcription models.
    """

    custom_speech: Mapping[str, str]
    phrase_list: Sequence[str]
    prompt: str


@experimental(feature_id=ExperimentalFeature.REALTIME)
class AzureNoiseReduction(RealtimeNoiseReduction, total=False):
    """Noise reduction with Voice Live's types.

    ``type`` may be ``"azure_deep_noise_suppression"``, ``"azure_deep_noise_suppression_headset"`` or
    ``"azure_personal_noise_suppression"``, as well as OpenAI's ``"near_field"`` and ``"far_field"``.
    """


@experimental(feature_id=ExperimentalFeature.REALTIME)
class AzureEchoCancellation(TypedDict, total=False):
    """Server-side echo cancellation of the caller's audio.

    Keys:
        type: ``"server_echo_cancellation"``.
        reference_source: ``"server"`` for the service's own audio, or ``"client"`` for channel 1 of stereo input.
        channels: 1, or 2 with a ``"client"`` reference.
    """

    type: Literal["server_echo_cancellation"]
    reference_source: Literal["server", "client"]
    channels: int


@experimental(feature_id=ExperimentalFeature.REALTIME)
class AzureAnimation(TypedDict, total=False):
    """Animation output: blendshapes and visemes, which arrive as passthrough events.

    Keys:
        model_name: The animation model.
        outputs: ``"blendshapes"`` and ``"viseme_id"``.
    """

    model_name: str
    outputs: Sequence[Literal["blendshapes", "viseme_id"]]


@experimental(feature_id=ExperimentalFeature.REALTIME)
class AzureInterimResponse(TypedDict, total=False):
    """Interim responses that fill a slow response or tool call.

    Keys:
        type: ``"static-interim-response"`` picks from ``texts``; ``"llm-interim-response"`` generates a line.
        triggers: ``"latency"`` and ``"tool"``.
        latency_threshold_ms: The delay before a latency trigger fires, in milliseconds.
        texts: The lines a static interim response picks from.
        model: The model that generates an LLM interim response.
        instructions: How to generate an LLM interim response.
        max_completion_tokens: The most tokens an LLM interim response may use.
    """

    type: Literal["static-interim-response", "llm-interim-response"]
    triggers: Sequence[Literal["latency", "tool"]]
    latency_threshold_ms: int
    texts: Sequence[str]
    model: str
    instructions: str
    max_completion_tokens: int


@experimental(feature_id=ExperimentalFeature.REALTIME)
class VoiceLiveOptions(RealtimeOptions, total=False):
    """Voice Live session settings: the neutral realtime options, widened for Azure, and Voice Live's own.

    ``input_audio_format`` and ``output_audio_format`` also take ``"audio/pcm;rate=16000"`` and
    ``"audio/pcm;rate=8000"``. Voice Live rejects an update that changes the caller audio's sampling rate.
    Per-response ``instructions`` are dropped with a warning, because Voice Live ignores them.

    Keys:
        voice: An OpenAI voice name, or an ``AzureVoice``.
        turn_detection: How the caller's turn ends, or ``None`` for no turn detection.
        transcription: How the caller is transcribed, or ``None`` for no caller transcript.
        noise_reduction: How caller audio is cleaned, or ``None`` for none.
        echo_cancellation: Server-side echo cancellation, or ``None`` for none.
        animation: Animation output.
        interim_response: Interim responses for slow turns and tool calls, or ``None`` for none.
        reasoning_effort: How hard a reasoning model reasons.
        parallel_tool_calls: Whether the model may call tools in parallel.
        output_audio_timestamp_types: The timestamps reported for the agent's audio: ``"word"``.
    """

    voice: str | AzureVoice  # type: ignore[misc]
    turn_detection: AzureTurnDetection | None  # type: ignore[misc]
    transcription: AzureTranscription | None  # type: ignore[misc]
    noise_reduction: AzureNoiseReduction | None
    echo_cancellation: AzureEchoCancellation | None
    animation: AzureAnimation
    interim_response: AzureInterimResponse | None
    reasoning_effort: Literal["none", "minimal", "low", "medium", "high", "xhigh"]
    parallel_tool_calls: bool
    output_audio_timestamp_types: Sequence[Literal["word"]]


VoiceLiveOptionsT = TypeVar(
    "VoiceLiveOptionsT",
    bound=TypedDict,  # type: ignore[valid-type]
    default=VoiceLiveOptions,
)
"""The options type a ``VoiceLiveClient`` is generic over."""
