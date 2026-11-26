# Copyright (c) Microsoft. All rights reserved.

"""OpenAI realtime option types: the GA protocol's session settings beyond the neutral realtime options."""

# No ``from __future__ import annotations``: required keys need evaluated annotations at runtime.
import sys
from collections.abc import Mapping
from typing import Literal, Required

from agent_framework import RealtimeOptions, RealtimeTranscription, RealtimeTurnDetection
from agent_framework._feature_stage import ExperimentalFeature, experimental
from typing_extensions import TypedDict

if sys.version_info >= (3, 13):
    from typing import TypeVar  # pragma: no cover
else:
    from typing_extensions import TypeVar  # pragma: no cover


@experimental(feature_id=ExperimentalFeature.REALTIME)
class OpenAIRealtimeTurnDetection(RealtimeTurnDetection, total=False):
    """Turn detection with OpenAI's detectors and settings.

    ``type`` is ``"server_vad"`` or ``"semantic_vad"``.

    Keys:
        eagerness: How soon ``semantic_vad`` ends the caller's turn: ``"low"``, ``"medium"``, ``"high"`` or ``"auto"``.
        idle_timeout_ms: For ``server_vad``, how long the caller may stay silent after the agent's audio finishes
            before the model is prompted to respond, in milliseconds, or ``None`` for no timeout.
    """

    eagerness: Literal["low", "medium", "high", "auto"]
    idle_timeout_ms: int | None


@experimental(feature_id=ExperimentalFeature.REALTIME)
class OpenAIRealtimeTranscription(RealtimeTranscription, total=False):
    """Caller transcription with OpenAI's settings.

    Keys:
        prompt: Text that guides the transcription model, such as names and terms the caller is likely to say.
    """

    prompt: str


@experimental(feature_id=ExperimentalFeature.REALTIME)
class OpenAIRealtimeTruncation(TypedDict, total=False):
    """Truncation that keeps a fraction of the conversation each time it exceeds the input token limit.

    Dropping more than the minimum each time truncates less often, which keeps more of the prompt cache.

    Keys:
        type: ``"retention_ratio"``.
        retention_ratio: The fraction of the conversation's tokens after the instructions to keep, from 0.0 to 1.0.
        token_limits: Token limits that replace the model's, such as ``{"post_instructions": 5000}``.
    """

    type: Required[Literal["retention_ratio"]]
    retention_ratio: Required[float]
    token_limits: Mapping[str, int]


@experimental(feature_id=ExperimentalFeature.REALTIME)
class OpenAIRealtimeOptions(RealtimeOptions, total=False):
    """OpenAI Realtime and Azure OpenAI Realtime session settings: the neutral realtime options and the GA protocol's.

    The GA protocol has no session ``temperature`` or ``metadata``, so the session drops them with a warning;
    ``metadata`` still applies to a single response. The settings below apply to the session only, not to a single
    response.

    Keys:
        turn_detection: How the caller's turn ends, or ``None`` for no turn detection.
        transcription: How the caller is transcribed, or ``None`` for no caller transcript.
        speed: How fast the agent speaks, from 0.25 to 1.5, where 1.0 is normal. It changes only between responses.
        truncation: How the conversation is truncated when it exceeds the model's input token limit: ``"auto"``,
            ``"disabled"`` to fail instead, or a retention ratio.
        reasoning_effort: How hard a reasoning model, such as ``gpt-realtime-2``, reasons.
        parallel_tool_calls: Whether a reasoning model may call tools in parallel.
    """

    turn_detection: OpenAIRealtimeTurnDetection | None  # type: ignore[misc]
    transcription: OpenAIRealtimeTranscription | None  # type: ignore[misc]
    speed: float
    truncation: Literal["auto", "disabled"] | OpenAIRealtimeTruncation
    reasoning_effort: Literal["minimal", "low", "medium", "high", "xhigh"]
    parallel_tool_calls: bool


OpenAIRealtimeOptionsT = TypeVar(
    "OpenAIRealtimeOptionsT",
    bound=TypedDict,  # type: ignore[valid-type]
    default=OpenAIRealtimeOptions,
)
