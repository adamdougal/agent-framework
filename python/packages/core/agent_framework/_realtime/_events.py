# Copyright (c) Microsoft. All rights reserved.

"""The realtime event a provider session exchanges with the conversation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .._feature_stage import ExperimentalFeature, experimental
from .._types import Content, _copy_additional_properties  # pyright: ignore[reportPrivateUsage]
from ._kinds import RealtimeKind, RealtimeKindLiteral, require_realtime_kind


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeEvent:
    """One provider event, tagged with an Agent Framework kind.

    Provider sessions turn every server event into one or more realtime events, and the conversation turns each one
    into one ``AgentResponseUpdate``. A realtime event never leaves the process, so it isn't serializable; updates are
    what hosts serialize.

    An app can also pass a realtime event to ``send(...)`` as a raw provider command. It then has
    ``kind="passthrough"``, and its ``raw_representation`` is the wire event as a JSON-serializable mapping, which the
    session sends verbatim.

    Attributes:
        kind: The Agent Framework kind, which core dispatches on.
        content: The payload of a payload kind, such as text for ``output_text``. ``None`` for signals and passthrough
            events, whose realtime-event content the conversation builds.
        service_event_type: The provider's event name, verbatim.
        additional_properties: The kind's neutral attributes under ``"core"``, as its ``Realtime*Attributes``
            TypedDict documents, and documented provider extras under the provider package's namespace.
        raw_representation: The provider's own event object. Never shown in ``repr``.
    """

    def __init__(
        self,
        kind: RealtimeKindLiteral | RealtimeKind,
        *,
        content: Content | None = None,
        service_event_type: str | None = None,
        additional_properties: Mapping[str, Any] | None = None,
        raw_representation: Any | None = None,
    ) -> None:
        """Create a realtime event.

        Args:
            kind: The Agent Framework kind. Any non-empty string is accepted, so kinds a newer runtime adds still work.

        Keyword Args:
            content: The payload of a payload kind.
            service_event_type: The provider's event name, verbatim.
            additional_properties: Neutral attributes under ``"core"`` and provider extras under the provider's
                namespace. They're copied one level deep, so later changes to the caller's mappings don't show here.
            raw_representation: The provider's own event object, or the wire event of a raw command.

        Raises:
            ValueError: If ``kind`` isn't a non-empty string.
        """
        require_realtime_kind(kind)
        self.kind = kind
        self.content = content
        self.service_event_type = service_event_type
        self.additional_properties: Mapping[str, Any] = _copy_additional_properties(additional_properties)
        self.raw_representation = raw_representation

    def __repr__(self) -> str:
        """Describe the event without its raw representation, which can be a large provider object."""
        return (
            f"RealtimeEvent(kind={self.kind!r}, service_event_type={self.service_event_type!r}, "
            f"content={self.content!r}, additional_properties={self.additional_properties!r})"
        )
