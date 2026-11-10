# Copyright (c) Microsoft. All rights reserved.

"""Barge-in: tracking the agent's generated audio, and cutting what the caller didn't hear (spec 006, Barge-in)."""

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
    """One cut to send to the provider: the item's audio after ``audio_end_ms`` is discarded."""

    item_id: str
    content_index: int
    audio_end_ms: int


class _AudioPart:
    """One content part of an assistant message's audio, which the provider cuts on its own."""

    def __init__(self, item_id: str, content_index: int) -> None:
        self.item_id = item_id
        self.content_index = content_index
        self.generated_ms: float | None = 0.0

    def is_part(self, item_id: str, content_index: int) -> bool:
        return self.item_id == item_id and self.content_index == content_index


class AudioLedger:
    """The audio each response generated, and the cuts each response is owed.

    The first ``interrupt(played_ms=...)`` or provider truncation of a response fixes its cuts. Each cut stays owed
    until the provider acknowledges it, so a later ``interrupt()`` retries the cuts that failed or weren't sent, and
    never sends an acknowledged one again.
    """

    def __init__(self) -> None:
        self._parts_by_response: dict[str, list[_AudioPart]] = {}
        self._owed_by_response: dict[str, list[Truncation]] = {}
        self._latest_response_id: str | None = None
        self._unmeasurable_media_types: set[str] = set()

    def on_output_audio(self, event: RealtimeEvent) -> None:
        core = core_attributes(event)
        parts = self._parts_by_response.setdefault(core["response_id"], [])
        self._latest_response_id = core["response_id"]
        part = next((part for part in parts if part.is_part(core["item_id"], core["content_index"])), None)
        if part is None:
            part = _AudioPart(core["item_id"], core["content_index"])
            parts.append(part)
        if event.content is None or part.generated_ms is None:
            return
        media_type = event.content.media_type or ""
        bytes_per_ms = _bytes_per_ms(media_type)
        if bytes_per_ms is None:
            part.generated_ms = None
            if media_type not in self._unmeasurable_media_types:
                self._unmeasurable_media_types.add(media_type)
                logger.warning("Barge-in can't measure %r audio, so interrupt() won't cut it.", media_type)
            return
        part.generated_ms += len(_get_data_bytes(event.content) or b"") / bytes_per_ms

    def on_item_truncated(self, event: RealtimeEvent) -> list[Truncation]:
        """Record a truncation; when the provider cut on its own, owe cuts at 0 ms for the response's later parts.

        A client truncation acknowledges a cut the conversation sent, even one whose control call timed out first, so
        that cut is no longer owed.
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
                return list(owed)
        return []

    def cut(self, played_ms: int) -> list[Truncation]:
        """The cuts owed for a barge-in after ``played_ms`` of the latest response's audio.

        The first call for a response plans them from ``played_ms``; later calls return those still unacknowledged.
        """
        response_id = self._latest_response_id
        if response_id is None:
            return []
        if (owed := self._owed_by_response.get(response_id)) is not None:
            return list(owed)
        parts = self._parts_by_response[response_id]
        if any(part.generated_ms is None for part in parts):
            return []
        owed = _plan_cuts(parts, played_ms)
        self._owed_by_response[response_id] = owed
        return list(owed)

    def is_owed(self, truncation: Truncation) -> bool:
        return any(truncation in owed for owed in self._owed_by_response.values())

    def acknowledged(self, truncation: Truncation) -> None:
        """Record that the provider made a cut, which is then no longer owed."""
        for owed in self._owed_by_response.values():
            if truncation in owed:
                owed.remove(truncation)


def _plan_cuts(parts: list[_AudioPart], played_ms: int) -> list[Truncation]:
    """Cut the part where playback stopped at ``played_ms``, and every later part at 0 ms."""
    remaining = float(played_ms)
    for position, part in enumerate(parts):
        generated_ms = part.generated_ms or 0.0
        if remaining >= generated_ms - FULLY_HEARD_SLACK_MS:
            # A part heard within the slack counts as heard in full, and leaves no negative remainder.
            remaining = max(0.0, remaining - generated_ms)
            continue
        return [
            Truncation(part.item_id, part.content_index, int(remaining)),
            *(Truncation(later.item_id, later.content_index, 0) for later in parts[position + 1 :]),
        ]
    return []


def _bytes_per_ms(media_type: str) -> float | None:
    """How many bytes of ``media_type`` audio play in one millisecond.

    G.711 (``audio/pcmu``, ``audio/pcma``) is 8 kHz with 1 byte per sample, and ``audio/pcm`` is 16-bit mono at its
    ``rate`` parameter.
    """
    essence = media_type.split(";")[0].strip()
    if essence in _G711_MEDIA_TYPES:
        return _G711_BYTES_PER_MS
    if essence != "audio/pcm":
        return None
    parameters = dict(parameter.strip().split("=", 1) for parameter in media_type.split(";")[1:] if "=" in parameter)
    return int(parameters.get("rate", _DEFAULT_PCM_RATE)) * _PCM_SAMPLE_BYTES / 1000
