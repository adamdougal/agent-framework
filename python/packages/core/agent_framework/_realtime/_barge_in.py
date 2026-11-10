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


class _AudioItem:
    def __init__(self, item_id: str, content_index: int) -> None:
        self.item_id = item_id
        self.content_index = content_index
        self.generated_ms: float | None = 0.0
        self.cut = False


class AudioLedger:
    """The audio each response generated, which ``interrupt(played_ms=...)`` cuts."""

    def __init__(self) -> None:
        self._items_by_response: dict[str, list[_AudioItem]] = {}
        self._latest_response_id: str | None = None
        self._unmeasurable_media_types: set[str] = set()

    def on_output_audio(self, event: RealtimeEvent) -> None:
        core = core_attributes(event)
        items = self._items_by_response.setdefault(core["response_id"], [])
        self._latest_response_id = core["response_id"]
        item = next((item for item in items if item.item_id == core["item_id"]), None)
        if item is None:
            item = _AudioItem(core["item_id"], core["content_index"])
            items.append(item)
        if event.content is None or item.generated_ms is None:
            return
        media_type = event.content.media_type or ""
        bytes_per_ms = _bytes_per_ms(media_type)
        if bytes_per_ms is None:
            item.generated_ms = None
            if media_type not in self._unmeasurable_media_types:
                self._unmeasurable_media_types.add(media_type)
                logger.warning("Barge-in can't measure %r audio, so interrupt() won't cut it.", media_type)
            return
        item.generated_ms += len(_get_data_bytes(event.content) or b"") / bytes_per_ms

    def on_item_truncated(self, event: RealtimeEvent) -> list[Truncation]:
        """Record a truncation; when the provider cut on its own, plan cutting the response's later messages at 0 ms.

        A client truncation confirms a cut ``interrupt()`` already planned, so it plans nothing more.
        """
        core = core_attributes(event)
        if core.get("truncated_by") != "provider":
            return []
        for items in self._items_by_response.values():
            positions = [position for position, item in enumerate(items) if item.item_id == core["item_id"]]
            if positions:
                unheard = items[positions[0] + 1 :]
                for item in (items[positions[0]], *unheard):
                    item.cut = True
                return [Truncation(item.item_id, item.content_index, 0) for item in unheard]
        return []

    def cut(self, played_ms: int) -> list[Truncation]:
        """Plan the truncations for a barge-in after ``played_ms`` of the latest response's audio."""
        if self._latest_response_id is None:
            return []
        items = self._items_by_response[self._latest_response_id]
        if any(item.cut or item.generated_ms is None for item in items):
            return []
        remaining = float(played_ms)
        for position, item in enumerate(items):
            generated_ms = item.generated_ms or 0.0
            if remaining >= generated_ms - FULLY_HEARD_SLACK_MS:
                # A message heard within the slack counts as heard in full, and leaves no negative remainder.
                remaining = max(0.0, remaining - generated_ms)
                continue
            unheard = items[position + 1 :]
            for cut_item in (item, *unheard):
                cut_item.cut = True
            return [
                Truncation(item.item_id, item.content_index, int(remaining)),
                *(Truncation(later.item_id, later.content_index, 0) for later in unheard),
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
