# Copyright (c) Microsoft. All rights reserved.

"""Which input a realtime session hears as the caller speaking."""

from __future__ import annotations

import copy
from collections.abc import Sequence

from .._types import Content, Message


def is_audio(content: Content) -> bool:
    return content.type in ("data", "uri") and content.has_top_level_media_type("audio")


def is_audio_only(messages: Sequence[Message]) -> bool:
    contents = [content for message in messages for content in message.contents]
    return bool(contents) and all(is_audio(content) for content in contents)


def without_agents_audio(messages: Sequence[Message]) -> list[Message]:
    """A workflow passes a run earlier agents' replies, whose audio the session would send as the caller speaking."""
    kept: list[Message] = []
    for message in messages:
        if message.role != "user":
            message = copy.copy(message)
            message.contents = [content for content in message.contents if not is_audio(content)]
        if message.contents:
            kept.append(message)
    return kept


def refuse_audio_by_uri(messages: Sequence[Message]) -> None:
    """A session drops audio it doesn't hold, so turn detection would never hear it and a run would wait forever.

    Raises:
        ValueError: If any message holds audio as a URI.
    """
    if any(content.type == "uri" and is_audio(content) for message in messages for content in message.contents):
        raise ValueError("A realtime session can only send audio it holds as data; pass it with Content.from_data().")
