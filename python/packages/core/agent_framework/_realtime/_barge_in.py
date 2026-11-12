# Copyright (c) Microsoft. All rights reserved.

"""Barge-in: tracking the agent's generated audio, and cutting what the caller didn't hear."""

from __future__ import annotations

import logging
from typing import NamedTuple

from .._types import _get_data_bytes  # pyright: ignore[reportPrivateUsage]
from ._events import RealtimeEvent
from ._updates import core_attributes

logger = logging.getLogger("agent_framework")

FULLY_HEARD_SLACK_MS = 40
"""Audio within this much of a message's end counts as heard: apps measure playback with clocks and round it."""

_PCM_SAMPLE_BYTES = 2
_DEFAULT_PCM_RATE = 24000
_G711_MEDIA_TYPES = frozenset({"audio/pcmu", "audio/pcma"})
_G711_BYTES_PER_MS = 8


class Truncation(NamedTuple):
    item_id: str
    content_index: int
    audio_end_ms: int


class _AudioPart:
    """Parts are measured separately because provider truncation offsets are per content part."""

    def __init__(self, item_id: str, content_index: int) -> None:
        self.item_id = item_id
        self.content_index = content_index
        self.generated_ms: float | None = 0.0

    def is_part(self, item_id: str, content_index: int) -> bool:
        return self.item_id == item_id and self.content_index == content_index


class AudioLedger:
    """The audio each response generated, and the cuts each response is owed.

    The first ``interrupt(played_ms=...)`` or provider truncation of a response fixes its cuts, and any audio part of
    that response that arrives later is owed a cut at 0 ms. Each cut stays owed until the provider acknowledges it, so a
    later ``interrupt()`` retries the cuts that failed or weren't sent, and never sends an acknowledged one again.
    """

    def __init__(self) -> None:
        self._parts_by_response: dict[str, list[_AudioPart]] = {}
        self._owed_by_response: dict[str, list[Truncation]] = {}
        self._latest_response_id: str | None = None
        self._cut_points: dict[str, str] = {}

    @property
    def latest_response_id(self) -> str | None:
        """Target the last response with audio, not a newer response that has produced none."""
        return self._latest_response_id

    def on_output_audio(self, event: RealtimeEvent) -> list[Truncation]:
        """Late audio in a cut response was never heard, so it must be truncated at 0 ms."""
        core = core_attributes(event)
        response_id = core["response_id"]
        parts = self._parts_by_response.setdefault(response_id, [])
        self._latest_response_id = response_id
        part = next((part for part in parts if part.is_part(core["item_id"], core["content_index"])), None)
        owed_now: list[Truncation] = []
        if part is None:
            part = _AudioPart(core["item_id"], core["content_index"])
            parts.append(part)
            if (owed := self._owed_by_response.get(response_id)) is not None:
                owed_now.append(Truncation(part.item_id, part.content_index, 0))
                owed.extend(owed_now)
        self._measure(part, event)
        return owed_now

    def _measure(self, part: _AudioPart, event: RealtimeEvent) -> None:
        if event.content is None or part.generated_ms is None:
            return
        media_type = event.content.media_type or ""
        bytes_per_ms = _bytes_per_ms(media_type)
        if bytes_per_ms is None:
            part.generated_ms = None
            logger.warning("Barge-in can't measure %r audio, so interrupt() won't cut it.", media_type)
            return
        part.generated_ms += len(_get_data_bytes(event.content) or b"") / bytes_per_ms

    def on_item_truncated(self, event: RealtimeEvent) -> list[Truncation]:
        """A provider's own cut leaves the response's later parts unheard.

        A client truncation acknowledges a cut the conversation sent, even one whose control call timed out first.
        """
        core = core_attributes(event)
        if core.get("truncated_by") != "provider":
            for owed in self._owed_by_response.values():
                owed[:] = [
                    cut for cut in owed if (cut.item_id, cut.content_index) != (core["item_id"], core["content_index"])
                ]
            return []
        for response_id, parts in self._parts_by_response.items():
            position = next(
                (
                    position
                    for position, part in enumerate(parts)
                    if part.is_part(core["item_id"], core["content_index"])
                ),
                None,
            )
            if position is not None:
                if response_id in self._owed_by_response:
                    return []
                owed = [Truncation(later.item_id, later.content_index, 0) for later in parts[position + 1 :]]
                self._owed_by_response[response_id] = owed
                self._cut_points[response_id] = core["item_id"]
                return list(owed)
        return []

    def cut(self, response_id: str | None, played_ms: int) -> list[Truncation]:
        """Only the first barge-in on a response plans its cuts; later ones retry the cuts not yet acknowledged.

        Earlier responses' unacknowledged cuts come first: by the next barge-in, a later response has usually produced
        audio and become the target.
        """
        retries = [cut for owner, owed in self._owed_by_response.items() if owner != response_id for cut in owed]
        if response_id is None:
            return retries
        if (owed := self._owed_by_response.get(response_id)) is None:
            parts = self._parts_by_response[response_id]
            if any(part.generated_ms is None for part in parts):
                return retries
            owed = _plan_cuts(parts, played_ms)
            self._owed_by_response[response_id] = owed
            if owed:
                self._cut_points[response_id] = owed[0].item_id
        return [*retries, *owed]

    def is_owed(self, truncation: Truncation) -> bool:
        return any(truncation in owed for owed in self._owed_by_response.values())

    def response_cut_at(self, item_id: str) -> str | None:
        return next((response_id for response_id, cut in self._cut_points.items() if cut == item_id), None)

    def cut_point(self, response_id: str) -> str | None:
        """The message where the caller stopped hearing the response, if it was cut."""
        return self._cut_points.get(response_id)

    def has_audio(self, item_id: str) -> bool:
        return any(part.item_id == item_id for parts in self._parts_by_response.values() for part in parts)

    def generated_ms(self, item_id: str, content_index: int) -> float | None:
        for parts in self._parts_by_response.values():
            for part in parts:
                if part.is_part(item_id, content_index):
                    return part.generated_ms
        return None

    def acknowledged(self, truncation: Truncation) -> None:
        for owed in self._owed_by_response.values():
            if truncation in owed:
                owed.remove(truncation)


def _plan_cuts(parts: list[_AudioPart], played_ms: int) -> list[Truncation]:
    remaining = float(played_ms)
    for position, part in enumerate(parts):
        generated_ms = part.generated_ms or 0.0
        if remaining >= generated_ms - FULLY_HEARD_SLACK_MS:
            remaining = max(0.0, remaining - generated_ms)
            continue
        return [
            Truncation(part.item_id, part.content_index, int(remaining)),
            *(Truncation(later.item_id, later.content_index, 0) for later in parts[position + 1 :]),
        ]
    return []


def _bytes_per_ms(media_type: str) -> float | None:
    """G.711 (``audio/pcmu``, ``audio/pcma``) is 8 kHz with 1 byte per sample.

    ``audio/pcm`` is 16-bit mono at its ``rate`` parameter.
    """
    essence = media_type.split(";")[0].strip()
    if essence in _G711_MEDIA_TYPES:
        return _G711_BYTES_PER_MS
    if essence != "audio/pcm":
        return None
    parameters = dict(parameter.strip().split("=", 1) for parameter in media_type.split(";")[1:] if "=" in parameter)
    return int(parameters.get("rate", _DEFAULT_PCM_RATE)) * _PCM_SAMPLE_BYTES / 1000
