# Copyright (c) Microsoft. All rights reserved.

"""The protocols a realtime provider client and its live session implement."""

from __future__ import annotations

from collections.abc import AsyncIterable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Protocol, Self, runtime_checkable

from .._feature_stage import ExperimentalFeature, experimental
from .._types import Message
from ._events import RealtimeEvent
from ._options import RealtimeOptionsT

if TYPE_CHECKING:
    from .._tools import ToolTypes


@runtime_checkable
@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeClientSessionProtocol(Protocol[RealtimeOptionsT]):
    """A provider's live realtime session: one connection, its events and its control calls.

    Provider clients implement it, and the realtime conversation drives it. The conversation serializes control calls,
    so a session never runs two at once, though ``send(...)`` can run alongside one.

    Control calls have no timeout of their own: the conversation bounds each one with ``asyncio.timeout()``. A session
    must be cancellation-safe, so a cancelled control call leaves nothing pending and stops claiming errors. While a
    control call waits, the session claims a provider error that carries its request id, no id or a server id, and
    raises it from that call as ``RealtimeControlException`` with the provider's code and param.

    Attributes:
        options: The session's current settings, including accepted session updates.
    """

    options: RealtimeOptionsT

    async def __aenter__(self) -> Self:
        """Return the session itself; leaving the ``async with`` block closes it."""
        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the session."""
        ...

    async def send(self, value: RealtimeEvent | Message) -> None:
        """Send input or a raw command to the provider, without waiting for an acknowledgement.

        A ``RealtimeEvent`` is a raw command: its ``raw_representation`` is the wire event, sent verbatim and untracked.
        A ``Message`` is mapped one content at a time, in order:

        - audio ``data`` or ``uri`` content is appended to the input audio buffer, whatever the role;
        - ``text`` becomes a message item with the message's role, and assistant text is replayed as output text;
        - ``function_call`` content becomes a function-call item;
        - ``function_result`` content becomes a function-call output item with the result as a string;
        - a hosted ``function_approval_response`` becomes the provider's approval response;
        - any other content is dropped with a warning.

        Sending never requests a response; callers use ``create_response(...)`` for that.

        Args:
            value: A raw command, or a message to add to the provider's conversation.
        """
        ...

    def receive(self) -> AsyncIterable[RealtimeEvent]:
        """Iterate the provider's events, each tagged with a kind.

        Only one reader may iterate. Iteration ends when the session closes, and raises
        ``RealtimeConnectionException`` when the connection fails.
        """
        ...

    async def update_session(self, options: RealtimeOptionsT, tools: Sequence[ToolTypes] | None) -> None:
        """Update the live session's settings and, when ``tools`` isn't ``None``, replace its declared tools.

        The session drops, with a warning, any key it can't honor, and returns at once if nothing is left to send.
        Otherwise it returns when the provider acknowledges the update.

        Args:
            options: The settings to send.
            tools: The tools to declare instead of the current ones, ``[]`` to clear them, or ``None`` to leave them.

        Raises:
            RealtimeControlException: If the provider rejects the update.
        """
        ...

    async def create_response(self, options: RealtimeOptionsT | None = None) -> str:
        """Request a response, and return its id once the provider reports that response created.

        Args:
            options: Settings for this response only.

        Raises:
            RealtimeActiveResponseException: If a response is active, or another response started instead.
            RealtimeControlException: If the provider rejects the request.
        """
        ...

    async def say(self, text: str) -> str:
        """Have the agent speak ``text`` verbatim, and return the response id once it's created.

        Raises:
            RealtimeActiveResponseException: If a response is active, or another response started instead.
            RealtimeUnsupportedException: If the provider can't speak text verbatim with the session's voice.
            RealtimeControlException: If the provider rejects the request.
        """
        ...

    async def cancel_response(self) -> None:
        """Cancel the active response, and return once that response finishes.

        Raises:
            RealtimeControlException: If the provider rejects the cancel, including its error for having no active
                response, which the conversation treats as done.
        """
        ...

    async def truncate(self, item_id: str, content_index: int, audio_end_ms: int) -> None:
        """Cut an assistant item's audio after ``audio_end_ms``, and return once the provider confirms it.

        Args:
            item_id: The assistant item to cut.
            content_index: The content part to cut.
            audio_end_ms: How much of the audio the caller heard, in milliseconds.

        Raises:
            RealtimeControlException: If the provider rejects the truncation.
        """
        ...

    async def close(self) -> None:
        """Close the connection. Closing again does nothing."""
        ...


@runtime_checkable
@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeClientProtocol(Protocol[RealtimeOptionsT]):
    """A realtime provider client, which opens live sessions with one provider."""

    async def start_session(
        self, *, options: RealtimeOptionsT | None = None, tools: Sequence[ToolTypes] | None = None
    ) -> RealtimeClientSessionProtocol[RealtimeOptionsT]:
        """Connect, apply the initial settings, and return the session once it's ready for input.

        The caller owns the returned session and closes it. If opening fails or is cancelled before the session is
        returned, the client closes whatever it opened before re-raising, so nothing is left for the caller to clean up.
        Clients translate the options and drop, with a warning, any key they can't honor.

        Keyword Args:
            options: The session's initial settings.
            tools: The tools to declare.

        Raises:
            RealtimeConnectionException: If the connection fails.
            RealtimeControlException: If the provider rejects the initial settings.
            ValueError: If an audio format is one the provider can't take.
        """
        ...
