# Copyright (c) Microsoft. All rights reserved.

"""The shared realtime session for services that speak the OpenAI Realtime protocol.

``OpenAIRealtimeSession`` implements Agent Framework's realtime session protocol over wire JSON, so it imports nothing
but core and the standard library. Each provider client opens an ``OpenAIRealtimeConnection`` through its service's
SDK, used only as a transport, and hands it to a session. Services whose protocol differs in places, such as Voice
Live's preview shape, subclass the session and override its protected hooks.

Spec 006: Provider clients, Control calls and Event kinds.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import uuid
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Protocol, Self, cast, runtime_checkable

from agent_framework import Content, FunctionTool, Message, RealtimeEvent
from agent_framework._feature_stage import ExperimentalFeature, experimental
from agent_framework._realtime._kinds import RealtimeKindLiteral
from agent_framework._realtime._options import RealtimeOptionsT
from agent_framework._types import _get_data_bytes_as_str  # pyright: ignore[reportPrivateUsage]
from agent_framework.exceptions import (
    RealtimeActiveResponseException,
    RealtimeConnectionException,
    RealtimeControlException,
)

if TYPE_CHECKING:
    from agent_framework import ToolTypes

logger = logging.getLogger("agent_framework.openai")

DEFAULT_AUDIO_FORMAT = "audio/pcm;rate=24000"
"""The audio format a session uses when its options name none: 24 kHz, 16-bit PCM."""


@runtime_checkable
@experimental(feature_id=ExperimentalFeature.REALTIME)
class OpenAIRealtimeConnection(Protocol):
    """A transport carrying OpenAI Realtime wire JSON, opened by a provider client through its service's SDK.

    The connection maps its transport's failures to the realtime exceptions: ``send`` and ``receive`` raise
    ``RealtimeConnectionException``, with the close code when there is one and the transport's error as its cause,
    when the socket closes abnormally or fails.
    """

    async def send(self, event: Mapping[str, Any]) -> None:
        """Send one wire event, a JSON-serializable mapping."""
        ...

    async def receive(self) -> Mapping[str, Any] | None:
        """Return the next wire event, or ``None`` once the connection has closed normally."""
        ...

    async def close(self) -> None:
        """Close the connection. Closing again does nothing."""
        ...


@experimental(feature_id=ExperimentalFeature.REALTIME)
class OpenAIRealtimeSession(Generic[RealtimeOptionsT]):
    """A live realtime session for a service that speaks the OpenAI Realtime protocol.

    It implements ``RealtimeClientSessionProtocol`` over an ``OpenAIRealtimeConnection``: it translates neutral
    options to the protocol's session and response objects, maps every server event to a realtime event kind, and
    runs the typed control calls. A provider client builds it on an open connection and awaits ``start(...)``.

    Control calls resolve while something iterates ``receive()``, as the realtime conversation's receive loop does;
    only ``start(...)`` reads the connection itself. While a control call waits, it claims a provider error that
    carries its request id, no id, or an id this session never sent. The session tracks the active response itself,
    so ``create_response(...)``, ``say(...)`` and ``cancel_response()`` answer from that tracking without asking the
    provider, which matters on services that drop invalid events silently.

    Subclasses adapt it to a service by overriding the protected hooks: ``_SESSION_OPTION_KEYS``, ``_honored``,
    ``_voice_change_allowed``, ``_session_payload``, ``_response_payload``, ``_say_payload``, ``_current_voice``,
    ``_ASSISTANT_TEXT_PART``, ``_EVENT_ALIASES``, ``_event_namespace``, ``_INITIAL_UPDATE_REQUIRED`` and
    ``_before_initial_update``.

    Attributes:
        options: The session's accepted settings: the initial ones, then each acknowledged update, without the keys
            it dropped.
    """

    _SESSION_OPTION_KEYS: ClassVar[frozenset[str]] = frozenset({
        "instructions",
        "voice",
        "tool_choice",
        "max_tokens",
        "input_audio_format",
        "output_audio_format",
        "output_modalities",
        "turn_detection",
        "transcription",
        "noise_reduction",
    })
    """The neutral options this protocol's session object takes. Others are dropped with a warning."""

    _ASSISTANT_TEXT_PART: ClassVar[str] = "output_text"
    """The content part type assistant text is replayed as."""

    _INITIAL_UPDATE_REQUIRED: ClassVar[bool] = False
    """Whether ``start(...)`` sends a session.update even with nothing to apply, for services that reject any other
    event first."""

    _VOICE_OBJECTS_ACCEPTED: ClassVar[bool] = False
    """Whether the service takes voice objects, such as an ``AzureVoice``, as well as voice names."""

    _EVENT_ALIASES: ClassVar[Mapping[str, str]] = {}
    """A service's own event names, mapped to the GA names the session handles them as."""

    def __init__(self, connection: OpenAIRealtimeConnection) -> None:
        """Create a session over an open connection. Await ``start(...)`` before using it.

        Args:
            connection: The connection the provider client opened.
        """
        self._connection = connection
        self.options: RealtimeOptionsT = cast("RealtimeOptionsT", {})
        self._buffered: list[RealtimeEvent] = []
        self._pending: _PendingControl | None = None
        self._active_response_id: str | None = None
        self._session_state: dict[str, Any] = {}
        self._heard_output_audio = False
        self._truncate_cuts: dict[str, tuple[str, int, int]] = {}
        """The cut each sent truncate request asked for, by request event_id, until it's acknowledged or rejected."""
        self._sent_event_ids: set[str] = set()
        self._cancel_event_ids: set[str] = set()
        self._abandoned_updates: list[_PendingControl] = []
        self._write_lock = asyncio.Lock()
        self._reading = False
        self._closed = False

    # region Session protocol

    async def start(self, *, options: RealtimeOptionsT | None = None, tools: Sequence[ToolTypes] | None = None) -> None:
        """Apply the initial settings and tools, returning once the provider acknowledges them.

        Events read while waiting are kept, so ``receive()`` yields them first. Nothing is sent when there's nothing
        to apply. Callers bound the wait, as the realtime conversation does with ``asyncio.timeout()``.

        Keyword Args:
            options: The session's initial settings.
            tools: The tools to declare.

        Raises:
            RealtimeControlException: If the provider rejects the initial settings.
            ValueError: If an audio format is one the provider can't take.
        """
        await self._before_initial_update()
        honored = self._honored(dict(options or {}))
        session = self._session_payload(honored, tools)
        if not session and not self._INITIAL_UPDATE_REQUIRED:
            self.options = cast("RealtimeOptionsT", honored)
            return
        await self._control(
            {"type": "session.update", "session": session},
            "session.updated",
            accepted_options=honored,
            read_until_acknowledged=True,
        )

    async def send(self, value: RealtimeEvent | Message) -> None:
        """Send input or a raw command to the provider, without waiting for an acknowledgement.

        A ``RealtimeEvent`` is a raw command: its ``raw_representation``, the wire event as a mapping, is sent
        verbatim and untracked. A ``Message`` is mapped one content at a time, in order: audio data is appended to the
        input audio buffer, text becomes a message item with the message's role, and function calls and results become
        their own items, the result as its text. Anything else is dropped with a warning. Sending never requests a
        response.

        Raises:
            TypeError: If a raw command's ``raw_representation`` isn't a mapping.
            RealtimeConnectionException: If the session is closed or the connection fails.
        """
        self._ensure_open()
        if isinstance(value, RealtimeEvent):
            raw_command: Any = value.raw_representation
            if not isinstance(raw_command, Mapping):
                raise TypeError(
                    "A raw realtime command needs its wire event as a JSON-serializable mapping in raw_representation."
                )
            # An error echoing this command's id is its own, so no waiting control call may claim it.
            raw_event_id = cast("Mapping[str, Any]", raw_command).get("event_id")
            if isinstance(raw_event_id, str):
                self._sent_event_ids.add(raw_event_id)
            await self._write(cast("Mapping[str, Any]", raw_command))
            return
        for wire_event in self._message_events(value):
            await self._write(wire_event)

    async def receive(self) -> AsyncIterator[RealtimeEvent]:
        """Iterate the provider's events, each mapped to a realtime event kind.

        Only one task may iterate at a time. Iteration ends when the connection closes normally, and raises
        ``RealtimeConnectionException`` when it fails. Either way, a waiting control call fails.

        Raises:
            RuntimeError: If another task is already iterating.
        """
        if self._reading:
            raise RuntimeError("Another task is already reading this realtime session; receive() allows one reader.")
        self._reading = True
        try:
            while self._buffered:
                yield self._buffered.pop(0)
            async for event in self._events_from_connection():
                yield event
        finally:
            self._reading = False

    async def update_session(self, options: RealtimeOptionsT, tools: Sequence[ToolTypes] | None) -> None:
        """Update the live session's settings and, when ``tools`` isn't ``None``, replace its declared tools.

        Keys the provider can't take now are dropped with a warning, and when nothing is left the call returns at
        once. Otherwise it returns when the provider acknowledges the update, and the accepted keys merge into
        ``options``. If the acknowledgement echoes a different voice than the one sent, the provider ignored the
        change: the session warns and ``options`` keeps the voice the provider kept.

        Raises:
            RealtimeControlException: If the provider rejects the update.
        """
        honored = self._honored(dict(options or {}))
        session = self._session_payload(honored, tools)
        if not session:
            return
        await self._control({"type": "session.update", "session": session}, "session.updated", accepted_options=honored)

    async def create_response(self, options: RealtimeOptionsT | None = None) -> str:
        """Request a response, and return its id once the provider reports that response created.

        Raises:
            RealtimeActiveResponseException: If a response is active, or another response started instead.
            RealtimeControlException: If the provider rejects the request.
        """
        return await self._create_response(self._response_payload(dict(options or {})))

    async def say(self, text: str) -> str:
        """Have the agent speak ``text`` verbatim, and return the response id once it's created.

        With an Azure text-to-speech voice, the session sends the text as a pre-generated assistant message. Otherwise
        it asks the model to repeat the text word for word, which is best effort.

        Raises:
            RealtimeActiveResponseException: If a response is active, or another response started instead.
            RealtimeUnsupportedException: If the provider can't speak text verbatim with the session's voice.
            RealtimeControlException: If the provider rejects the request.
        """
        return await self._create_response(self._say_payload(text))

    async def cancel_response(self) -> None:
        """Cancel the active response, and return once that response finishes.

        Raises:
            RealtimeControlException: With code ``"response_cancel_not_active"`` when no response is active, which the
                session knows from its own tracking without asking the provider, or when the provider rejects the
                cancel.
        """
        if self._active_response_id is None:
            raise RealtimeControlException("No response is active.", code="response_cancel_not_active")
        cancelled_id = self._active_response_id
        await self._control(
            {"type": "response.cancel"},
            "response.done",
            concerns=lambda wire: _mapping(wire.get("response")).get("id") == cancelled_id,
            # A response that finished while the cancel waited to send needs no cancel, and the provider would apply
            # this untargeted one to whichever response is active by then.
            still_needed=lambda: self._active_response_id == cancelled_id,
        )

    async def truncate(self, item_id: str, content_index: int, audio_end_ms: int) -> None:
        """Cut an assistant item's audio after ``audio_end_ms``, and return once the provider confirms it.

        The ``item_truncated`` event this produces says the client truncated the item.

        Raises:
            RealtimeControlException: If the provider rejects the truncation.
        """
        cut = (item_id, content_index, audio_end_ms)
        sent_event_ids: list[str] = []

        def mark_sent(event_id: str) -> None:
            sent_event_ids.append(event_id)
            self._truncate_cuts[event_id] = cut

        try:
            await self._control(
                {
                    "type": "conversation.item.truncate",
                    "item_id": item_id,
                    "content_index": content_index,
                    "audio_end_ms": audio_end_ms,
                },
                "conversation.item.truncated",
                concerns=lambda wire: _truncation_cut(wire) == cut,
                on_sent=mark_sent,
            )
        except RealtimeControlException:
            for event_id in sent_event_ids:
                self._truncate_cuts.pop(event_id, None)
            raise

    async def close(self) -> None:
        """Close the connection. Closing again does nothing."""
        if self._closed:
            return
        self._closed = True
        await self._connection.close()

    async def __aenter__(self) -> Self:
        """Return the session itself; leaving the ``async with`` block closes it."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the session."""
        await self.close()

    # endregion

    # region Hooks a subclass overrides for its service

    def _honored(self, options: dict[str, Any]) -> dict[str, Any]:
        """The options the provider can take now, dropping with a warning any it would refuse."""
        honored: dict[str, Any] = {}
        for key, value in options.items():
            if key in self._SESSION_OPTION_KEYS:
                honored[key] = value
            else:
                logger.warning("Dropping option %r: this realtime session can't honor it.", key)
        if isinstance(honored.get("voice"), Mapping) and not self._VOICE_OBJECTS_ACCEPTED:
            logger.warning("Dropping voice %r: this realtime session takes voice names only.", honored.pop("voice"))
        if "voice" in honored and not self._voice_change_allowed(honored["voice"]):
            logger.warning("Dropping voice %r: the provider can't change the voice now.", honored.pop("voice"))
        return honored

    def _voice_change_allowed(self, voice: Any) -> bool:
        """Whether the provider accepts a new voice now: here, only until the session's first output audio."""
        return not self._heard_output_audio

    def _session_payload(self, options: dict[str, Any], tools: Sequence[ToolTypes] | None) -> dict[str, Any]:
        """Translate honored options and tools into the GA session object, empty when there's nothing to send."""
        session: dict[str, Any] = {}
        audio_input: dict[str, Any] = {}
        audio_output: dict[str, Any] = {}
        for key, value in options.items():
            match key:
                case "instructions" | "output_modalities":
                    session[key] = value
                case "voice":
                    audio_output["voice"] = value
                case "tool_choice":
                    session["tool_choice"] = self._translate_tool_choice(value)
                case "max_tokens":
                    session["max_output_tokens"] = value
                case "input_audio_format":
                    audio_input["format"] = _audio_format(value)
                case "output_audio_format":
                    audio_output["format"] = _audio_format(value)
                case "turn_detection" | "transcription" | "noise_reduction":
                    audio_input[key] = value
                case _:
                    pass
        audio = {name: part for name, part in (("input", audio_input), ("output", audio_output)) if part}
        if audio:
            session["audio"] = audio
        if tools is not None:
            session["tools"] = self._tool_declarations(tools)
        if not session:
            return {}
        return {"type": "realtime", **session}

    @staticmethod
    def _tool_declarations(tools: Sequence[ToolTypes]) -> list[Any]:
        """Declare function tools, pass hosted tools given as mappings through, and drop anything else."""
        declarations: list[Any] = []
        for candidate in tools:
            if isinstance(candidate, FunctionTool):
                declarations.append({
                    "type": "function",
                    "name": candidate.name,
                    "description": candidate.description,
                    "parameters": candidate.parameters(),
                })
            elif isinstance(candidate, Mapping):
                declarations.append(dict(cast("Mapping[str, Any]", candidate)))
            else:
                logger.warning(
                    "Dropping tool %r: a realtime session declares function tools and hosted tools only.", candidate
                )
        return declarations

    @staticmethod
    def _translate_tool_choice(choice: Any) -> Any:
        """The protocol's tool choice for a neutral one: a mode string, or a ToolMode."""
        if isinstance(choice, str):
            return choice
        if choice.get("required_function_name"):
            return {"type": "function", "name": choice["required_function_name"]}
        return choice["mode"]

    def _response_payload(self, options: dict[str, Any]) -> dict[str, Any]:
        """Translate neutral per-response options into the protocol's response object."""
        response: dict[str, Any] = {}
        for key, value in options.items():
            match key:
                case "instructions" | "output_modalities":
                    response[key] = value
                case "metadata":
                    response["metadata"] = dict(value)
                case "max_tokens":
                    response["max_output_tokens"] = value
                case "tool_choice":
                    response["tool_choice"] = self._translate_tool_choice(value)
                case _:
                    logger.warning("Dropping response option %r: this realtime session can't honor it.", key)
        return response

    def _say_payload(self, text: str) -> dict[str, Any]:
        """The response that speaks ``text`` verbatim: pre-generated with an Azure TTS voice, else the prompt."""
        if self._speaks_with_azure_tts_voice():
            return {
                "pre_generated_assistant_message": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "text", "text": text}],
                }
            }
        return {"instructions": f"Say exactly the following, word for word, and nothing else: {text}"}

    def _speaks_with_azure_tts_voice(self) -> bool:
        """Whether the session's current voice is an Azure text-to-speech voice, which can speak pre-generated text.

        ``azure-realtime-native`` voices aren't, because pre-generated speech hasn't been verified with them.
        """
        return _is_azure_tts_voice(self._current_voice())

    def _current_voice(self) -> Any:
        """The voice the provider last reported for this session."""
        return self._session_state.get("audio", {}).get("output", {}).get("voice")

    async def _before_initial_update(self) -> None:
        """Wait for whatever the service sends before it takes the initial session.update. Nothing, by default.

        An override reads the connection with ``_read_into_buffer()``, which keeps each event for ``receive()``.
        """

    def _event_namespace(self, service_event_type: str) -> str:
        """The metadata namespace that owns an event: ``openai``, for every event of the OpenAI Realtime protocol."""
        return "openai"

    # endregion

    # region Control calls

    async def _create_response(self, response: dict[str, Any]) -> str:
        """Request a response and return its id once its own response.created arrives.

        Voice Live returns ``metadata: null`` for a pre-generated response, so a pre-generated request takes the first
        response.created after it.
        """
        if self._active_response_id is not None:
            raise RealtimeActiveResponseException(f"Response {self._active_response_id} is still active.")
        request_id = uuid.uuid4().hex
        response["metadata"] = {**response.get("metadata", {}), "af_request_id": request_id}
        created = await self._control(
            {"type": "response.create", "response": response},
            "response.created",
            request_id=request_id,
            accepts_unechoed="pre_generated_assistant_message" in response,
        )
        return str(created["response"]["id"])

    async def _control(
        self,
        request: dict[str, Any],
        acknowledgement: str,
        *,
        request_id: str | None = None,
        accepts_unechoed: bool = False,
        concerns: Callable[[dict[str, Any]], bool] | None = None,
        accepted_options: dict[str, Any] | None = None,
        on_sent: Callable[[str], None] | None = None,
        still_needed: Callable[[], bool] | None = None,
        read_until_acknowledged: bool = False,
    ) -> Any:
        """Send a control request and wait for its acknowledgement, leaving nothing pending however it ends.

        A reader of receive() settles the acknowledgement, except while opening, when no reader exists yet and the
        session reads the connection itself. A response request's acknowledgement must echo its ``request_id``, or,
        when ``accepts_unechoed``, carry no metadata at all. ``concerns`` tells whether an acknowledgement is about this
        request's target, such as the item it truncates; one that isn't is left alone. ``accepted_options`` merge into
        ``options`` as the acknowledgement is read, so events right behind it already see them. ``on_sent``, given the
        request's event_id, runs as the request starts going out, before its acknowledgement can be read; a request
        counts as sent from then on, since a transport can put the frame on the wire before its send returns. When
        ``still_needed`` says otherwise once the request holds the connection, nothing is sent and the call returns
        None.
        """
        self._ensure_open()
        request = {**request, "event_id": f"af_{uuid.uuid4().hex}"}
        self._sent_event_ids.add(request["event_id"])
        if request["type"] == "response.cancel":
            self._cancel_event_ids.add(request["event_id"])
        acknowledged: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        owner = asyncio.current_task()
        pending = _PendingControl(
            acknowledgement,
            request["event_id"],
            acknowledged,
            request_id,
            accepts_unechoed,
            concerns,
            accepted_options,
            owner=owner,
            owner_cancellations=owner.cancelling() if owner is not None else 0,
        )
        sent = False

        def start_sending() -> bool:
            # Published only now, so nothing read while the request waits for the connection can answer it.
            nonlocal sent
            if still_needed is not None and not still_needed():
                return False
            self._pending = pending
            sent = True
            if on_sent is not None:
                on_sent(request["event_id"])
            return True

        try:
            if not await self._write(request, on_writing=start_sending):
                return None
            while read_until_acknowledged and not acknowledged.done():
                await self._read_into_buffer()
            return await acknowledged
        finally:
            if sent and not pending.answered and request["type"] == "session.update":
                # Nothing correlates session.updated with its request, so a cancelled update's late answer must not
                # be taken for a later update's.
                self._abandoned_updates.append(pending)
            if self._pending is pending:
                self._pending = None

    def _settle_pending(self, event_type: str, wire: dict[str, Any]) -> bool:
        """Resolve or fail the waiting control call when ``wire`` acknowledges it, rejects it, or beats it.

        Returns whether ``wire`` is an error the waiting call claimed.
        """
        if self._answers_abandoned_update(event_type, wire):
            return False
        pending = self._pending
        if pending is None or pending.answered:
            return False
        # A call cancelled a moment ago is still pending until its cleanup runs. Its answer is still read as its own,
        # but its caller is gone, so an error it would have claimed streams unclaimed.
        waiting = pending.caller_waiting()
        if event_type == "error":
            if self._claims(pending, wire):
                pending.answered = True
                if waiting:
                    pending.acknowledged.set_exception(_control_exception(wire))
                return waiting
        elif event_type == pending.acknowledgement and not _is_out_of_band(wire) and pending.concerned_by(wire):
            pending.answered = True
            if pending.answered_by(wire):
                if pending.accepted_options is not None:
                    self._merge_accepted(pending.accepted_options)
                if waiting:
                    pending.acknowledged.set_result(wire)
            elif waiting:
                pending.acknowledged.set_exception(
                    RealtimeActiveResponseException("The provider started another response instead.")
                )
        return False

    def _merge_accepted(self, accepted: dict[str, Any]) -> None:
        """Merge an acknowledged update's options into ``options``, keeping the voice the provider reports it kept.

        A provider can acknowledge an update yet keep the old voice, as Voice Live does between OpenAI voices. The
        voice counts as kept when the acknowledgement echoes every field sent, since a provider fills in defaults.
        """
        accepted = dict(accepted)
        if "voice" in accepted and not _voice_echoed(accepted["voice"], self._current_voice()):
            logger.warning(
                "The provider ignored the voice change to %r and kept %r.", accepted.pop("voice"), self._current_voice()
            )
        self.options = cast("RealtimeOptionsT", {**self.options, **accepted})

    def _answers_abandoned_update(self, event_type: str, wire: dict[str, Any]) -> bool:
        """Whether ``wire`` answers a session update whose caller stopped waiting, in the order they were sent.

        A session.updated answers the oldest, and its accepted options merge into ``options``. An error answers the
        update whose id it echoes, or the oldest when it echoes none or an id this session never sent, and streams as an
        unclaimed error.
        """
        if not self._abandoned_updates:
            return False
        if event_type == "session.updated":
            answered = self._abandoned_updates.pop(0)
            self._merge_accepted(answered.accepted_options or {})
            return True
        if event_type != "error":
            return False
        echoed = _mapping(wire.get("error")).get("event_id")
        if echoed is None or echoed not in self._sent_event_ids:
            self._abandoned_updates.pop(0)
            return True
        for index, abandoned in enumerate(self._abandoned_updates):
            if echoed == abandoned.event_id:
                del self._abandoned_updates[index]
                return True
        return False

    def _claims(self, pending: _PendingControl, error_event: dict[str, Any]) -> bool:
        """Whether the waiting call owns an error: its own id, no id, or an id this session never sent.

        Many errors don't echo the client's event_id, so a serialized call claims every error that can't belong to
        another of this session's requests.
        """
        echoed = _mapping(error_event.get("error")).get("event_id")
        return echoed is None or echoed == pending.event_id or echoed not in self._sent_event_ids

    def _answers_own_cancel(self, error_event: dict[str, Any]) -> bool:
        """Whether an error only says that a response this session cancelled had already finished."""
        error = _mapping(error_event.get("error"))
        return error.get("code") == "response_cancel_not_active" and error.get("event_id") in self._cancel_event_ids

    def _fail_pending(self, error: Exception) -> None:
        """Fail the waiting control call, if any, because its acknowledgement can no longer arrive."""
        pending = self._pending
        if pending is not None and not pending.acknowledged.done():
            pending.acknowledged.set_exception(error)

    # endregion

    # region Reading and writing the connection

    async def _events_from_connection(self) -> AsyncIterator[RealtimeEvent]:
        """Map every wire event until the connection ends, failing any waiting call when it does."""
        try:
            while (wire := await self._connection.receive()) is not None:
                for event in self._handle(dict(wire)):
                    yield event
        except RealtimeConnectionException as error:
            self._fail_pending(error)
            raise
        except Exception as error:
            failure = RealtimeConnectionException(f"The realtime connection failed: {error}")
            failure.__cause__ = error
            self._fail_pending(failure)
            raise
        self._fail_pending(RealtimeConnectionException("The realtime connection closed."))

    async def _read_into_buffer(self) -> dict[str, Any]:
        """Read one wire event before any reader exists, keeping its realtime events for receive()."""
        received = await self._connection.receive()
        if received is None:
            raise RealtimeConnectionException("The realtime connection closed while the session was opening.")
        wire = dict(received)
        self._buffered.extend(self._handle(wire))
        return wire

    async def _write(self, wire_event: Mapping[str, Any], *, on_writing: Callable[[], bool] | None = None) -> bool:
        """Send one wire event, never interleaved with another write, and return whether it was sent.

        ``on_writing`` runs once this write holds the connection, just before the transport sends; when it returns
        False, nothing is sent.
        """
        async with self._write_lock:
            if on_writing is not None and not on_writing():
                return False
            await self._connection.send(wire_event)
            return True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RealtimeConnectionException("The realtime session is closed.")

    def _message_events(self, message: Message) -> list[dict[str, Any]]:
        """Map a message's contents, in order, to the wire events that add them to the provider's conversation."""
        wire_events: list[dict[str, Any]] = []
        for content in message.contents:
            if content.type == "text":
                part_type = self._ASSISTANT_TEXT_PART if message.role == "assistant" else "input_text"
                part = {"type": part_type, "text": content.text}
                wire_events.append(_item_create({"type": "message", "role": message.role, "content": [part]}))
            elif content.type == "function_call":
                arguments = content.arguments
                wire_events.append(
                    _item_create({
                        "type": "function_call",
                        "call_id": content.call_id,
                        "name": content.name,
                        "arguments": arguments if isinstance(arguments, str) else json.dumps(arguments or {}),
                    })
                )
            elif content.type == "function_result":
                if any(item.type != "text" for item in content.items or []):
                    logger.warning(
                        "Dropping the non-text parts of the result for call %s: realtime tool results are text only.",
                        content.call_id,
                    )
                wire_events.append(
                    _item_create({
                        "type": "function_call_output",
                        "call_id": content.call_id,
                        "output": content.result or "",
                    })
                )
            elif content.type in ("data", "uri") and content.has_top_level_media_type("audio"):
                if (audio := _get_data_bytes_as_str(content)) is not None:
                    wire_events.append({"type": "input_audio_buffer.append", "audio": audio})
                else:
                    # The URI isn't logged: it can carry credentials, such as a SAS signature.
                    logger.warning(
                        "Dropping %s audio from a URI: a realtime session can only send audio it holds as data.",
                        content.media_type,
                    )
            else:
                logger.warning("Dropping %s content: a realtime session can't send it.", content.type)
        return wire_events

    # endregion

    # region Mapping server events

    def _handle(self, wire: dict[str, Any]) -> list[RealtimeEvent]:
        """Track state, settle a waiting control call that ``wire`` answers, then map ``wire`` to realtime events.

        Dispatch uses the GA name ``_EVENT_ALIASES`` gives a service's own event name, while events keep the wire
        name as their ``service_event_type``.
        """
        event_type = self._EVENT_ALIASES.get(wire["type"], wire["type"])
        self._track_state(event_type, wire)
        claimed = self._settle_pending(event_type, wire)
        if event_type == "error":
            reported = not claimed and not self._answers_own_cancel(wire)
            return [self._error_event(wire) if reported else self._passthrough(wire)]
        return self._map_event(event_type, wire)

    def _track_state(self, event_type: str, wire: dict[str, Any]) -> None:
        """Keep the active response and the session's reported settings current, before any reader sees the event.

        An out-of-band response (``conversation_id: null``) never becomes the active response.
        """
        if event_type == "response.created" and not _is_out_of_band(wire):
            self._active_response_id = wire["response"]["id"]
        elif event_type == "response.done" and wire["response"]["id"] == self._active_response_id:
            self._active_response_id = None
        elif event_type == "session.updated":
            self._session_state = dict(_mapping(wire.get("session")))
        elif event_type == "response.output_audio.delta":
            self._heard_output_audio = True
        elif event_type == "error" and (echoed := _mapping(wire.get("error")).get("event_id")) in self._truncate_cuts:
            # A provider that echoes the request id rejected one of this session's cuts, even one whose caller left.
            del self._truncate_cuts[echoed]

    def _map_event(self, event_type: str, wire: dict[str, Any]) -> list[RealtimeEvent]:
        """Map one wire event to its realtime events, keyed by its GA event name."""
        match event_type:
            case "session.created":
                session = _mapping(wire.get("session"))
                core = _reported(
                    session_id=session.get("id"), model=session.get("model"), expires_at=session.get("expires_at")
                )
                return [self._event("session_created", wire, core=core)]
            case "session.updated":
                return [self._event("session_updated", wire)]
            case "input_audio_buffer.speech_started":
                return [self._event("speech_started", wire, core=_present(wire, "item_id", "audio_start_ms"))]
            case "input_audio_buffer.speech_stopped":
                return [self._event("speech_stopped", wire, core=_present(wire, "item_id", "audio_end_ms"))]
            case "response.created":
                return [self._event("response_created", wire, core={"response_id": wire["response"]["id"]})]
            case "response.done":
                return self._response_done_events(wire)
            case "conversation.item.added":
                item = wire["item"]
                core = {"item_id": item["id"], "previous_item_id": wire.get("previous_item_id")}
                if item.get("type") == "message":
                    core.update(_present(item, "role"))
                return [self._event("item_added", wire, core=core)]
            case "conversation.item.truncated":
                core = {
                    **_present(wire, "item_id", "content_index", "audio_end_ms"),
                    "truncated_by": self._truncated_by(_truncation_cut(wire)),
                }
                return [self._event("item_truncated", wire, core=core)]
            case "conversation.item.input_audio_transcription.delta":
                text = Content.from_text(wire.get("delta") or "", merge_mode="append")
                return [self._payload("input_transcription", wire, text, _present(wire, "item_id", "content_index"))]
            case "conversation.item.input_audio_transcription.completed":
                # Transcription usage stays on the caller transcript, not in a response's usage.
                text = Content.from_text(
                    wire.get("transcript") or "",
                    merge_mode="replace",
                    status="completed",
                    additional_properties={"openai": {"usage": wire["usage"]}} if wire.get("usage") else None,
                )
                return [self._payload("input_transcription", wire, text, _present(wire, "item_id", "content_index"))]
            case "conversation.item.input_audio_transcription.failed":
                failure = _mapping(wire.get("error"))
                core = {
                    **_present(wire, "item_id", "content_index"),
                    **_reported(error_code=failure.get("code"), error_message=failure.get("message")),
                }
                return [self._event("input_transcription_failed", wire, core=core)]
            case "response.output_audio_transcript.delta" | "response.output_text.delta":
                return [self._agent_text(wire, Content.from_text(wire.get("delta") or "", merge_mode="append"))]
            case "response.output_audio_transcript.done" | "response.output_text.done":
                full_text = wire.get("transcript") or wire.get("text") or ""
                return [self._agent_text(wire, Content.from_text(full_text, merge_mode="replace", status="completed"))]
            case "response.output_audio.delta":
                audio = Content.from_data(base64.b64decode(wire.get("delta") or ""), self._output_media_type())
                core = _present(wire, "response_id", "item_id", "content_index")
                return [self._payload("output_audio", wire, audio, core)]
            case "response.output_item.done" if wire["item"].get("type") == "function_call":
                item = wire["item"]
                call = Content.from_function_call(item["call_id"], item["name"], arguments=item.get("arguments"))
                core = {**_present(wire, "response_id"), "item_id": item["id"]}
                return [self._payload("function_call", wire, call, core)]
            case _:
                return [self._passthrough(wire)]

    def _response_done_events(self, wire: dict[str, Any]) -> list[RealtimeEvent]:
        """One response.done as usage, error if it failed, then response_done, so a reader stopping there saw both."""
        response = wire["response"]
        response_id = response["id"]
        events: list[RealtimeEvent] = []
        if usage := response.get("usage"):
            usage_content = Content.from_usage(_usage_details(usage))  # type: ignore[arg-type]
            events.append(self._payload("usage", wire, usage_content, {"response_id": response_id}))
        status_details = _mapping(response.get("status_details"))
        failure = _mapping(status_details.get("error"))
        if response.get("status") == "failed":
            error_content = Content.from_error(message=failure.get("message"), error_code=failure.get("code"))
            events.append(self._payload("error", wire, error_content, {"response_id": response_id}))
        core = {
            "response_id": response_id,
            **_present(response, "status"),
            **_present(status_details, "reason"),
            **_reported(error_code=failure.get("code"), error_message=failure.get("message")),
        }
        events.append(self._event("response_done", wire, core=core))
        return events

    def _agent_text(self, wire: dict[str, Any], text: Content) -> RealtimeEvent:
        """Agent transcript text, spoken or written, tied to its response, item and content part."""
        return self._payload("output_text", wire, text, _present(wire, "response_id", "item_id", "content_index"))

    def _output_media_type(self) -> str:
        """The media type of the agent's audio: the session's output format, 24 kHz PCM by default."""
        return cast("dict[str, Any]", self.options).get("output_audio_format") or DEFAULT_AUDIO_FORMAT

    def _truncated_by(self, cut: tuple[Any, Any, Any]) -> str:
        """Who made a cut: this session's own truncate request, or the provider on its own."""
        for event_id, sent_cut in self._truncate_cuts.items():
            if sent_cut == cut:
                del self._truncate_cuts[event_id]
                return "client"
        return "provider"

    def _error_event(self, wire: dict[str, Any]) -> RealtimeEvent:
        """An unclaimed provider error as error content, with the provider's details under ``openai``."""
        error = _mapping(wire.get("error"))
        details = _present(error, "type", "param", "event_id")
        content = Content.from_error(
            message=error.get("message"),
            error_code=error.get("code"),
            additional_properties={"openai": details} if details else None,
        )
        return RealtimeEvent("error", content=content, service_event_type=wire["type"], raw_representation=wire)

    def _passthrough(self, wire: dict[str, Any]) -> RealtimeEvent:
        """An event Agent Framework gives no meaning to, carrying its wire fields under its owner's namespace."""
        return RealtimeEvent(
            "passthrough",
            service_event_type=wire["type"],
            additional_properties={self._event_namespace(wire["type"]): dict(wire)},
            raw_representation=wire,
        )

    @staticmethod
    def _event(kind: RealtimeKindLiteral, wire: dict[str, Any], *, core: dict[str, Any] | None = None) -> RealtimeEvent:
        """A signal kind's event, with its neutral attributes under core."""
        return RealtimeEvent(
            kind,
            service_event_type=wire["type"],
            additional_properties={"core": core} if core else None,
            raw_representation=wire,
        )

    @staticmethod
    def _payload(
        kind: RealtimeKindLiteral, wire: dict[str, Any], content: Content, core: dict[str, Any]
    ) -> RealtimeEvent:
        """A payload kind's event: its content, and its neutral attributes under core."""
        return RealtimeEvent(
            kind,
            content=content,
            service_event_type=wire["type"],
            additional_properties={"core": core},
            raw_representation=wire,
        )

    # endregion


@dataclass
class _PendingControl:
    """The one control call waiting for its acknowledgement."""

    acknowledgement: str
    event_id: str
    acknowledged: asyncio.Future[Any]
    request_id: str | None = None
    accepts_unechoed: bool = False
    concerns: Callable[[dict[str, Any]], bool] | None = None
    accepted_options: dict[str, Any] | None = None
    answered: bool = False
    """Whether the provider answered, with an acknowledgement or a claimed error, even after the caller stopped
    waiting."""
    owner: asyncio.Task[Any] | None = None
    owner_cancellations: int = 0
    """The owner's pending cancellation requests when the call began, so one it swallowed earlier doesn't count."""

    def caller_waiting(self) -> bool:
        """Whether the caller still waits for the answer: not cancelled, even while its request is still being sent."""
        cancelled = self.owner is not None and self.owner.cancelling() > self.owner_cancellations
        return not self.acknowledged.done() and not cancelled

    def concerned_by(self, acknowledgement: dict[str, Any]) -> bool:
        """Whether an acknowledgement event is about this call's target, rather than another item or response."""
        return self.concerns is None or self.concerns(acknowledgement)

    def answered_by(self, acknowledgement: dict[str, Any]) -> bool:
        """Whether an acknowledgement event belongs to this call rather than to a request it raced."""
        if self.request_id is None:
            return True
        metadata = acknowledgement.get("response", {}).get("metadata")
        if metadata is None:
            return self.accepts_unechoed
        return bool(metadata.get("af_request_id") == self.request_id)


def _control_exception(wire: dict[str, Any]) -> RealtimeControlException:
    """The exception a control call raises for a provider error it claimed."""
    error = _mapping(wire.get("error"))
    exception_type = (
        RealtimeActiveResponseException
        if error.get("code") == "conversation_already_has_active_response"
        else RealtimeControlException
    )
    return exception_type(error.get("message", ""), code=error.get("code"), param=error.get("param"))


def _voice_echoed(requested: Any, echoed: Any) -> bool:
    """Whether a provider's echoed voice is the one requested: equal, or a voice object with every requested field.

    An acknowledgement that reports no voice can't say the change was ignored.
    """
    if echoed is None:
        return True
    # Compare what went on the wire: JSON turns a requested tuple, such as prefer_locales, into the echo's list.
    requested = json.loads(json.dumps(requested))
    if isinstance(requested, Mapping) and isinstance(echoed, Mapping):
        echoed_fields = cast("Mapping[str, Any]", echoed)
        return all(echoed_fields.get(key) == value for key, value in cast("Mapping[str, Any]", requested).items())
    return bool(requested == echoed)


def _truncation_cut(wire: dict[str, Any]) -> tuple[Any, Any, Any]:
    """A truncation's whole cut: its item, content part and the audio it keeps."""
    return (wire.get("item_id"), wire.get("content_index"), wire.get("audio_end_ms"))


def _is_out_of_band(wire: dict[str, Any]) -> bool:
    """Whether a response event belongs to an out-of-band response, which GA marks with ``conversation_id: null``."""
    response = wire.get("response")
    return isinstance(response, Mapping) and "conversation_id" in response and response["conversation_id"] is None


_AZURE_TTS_VOICE_TYPES = frozenset({"azure-standard", "azure-custom", "azure-personal"})


def _is_azure_tts_voice(voice: Any) -> bool:
    """Whether ``voice`` is an Azure text-to-speech voice, which can speak pre-generated text."""
    return isinstance(voice, Mapping) and cast("Mapping[str, Any]", voice).get("type") in _AZURE_TTS_VOICE_TYPES


_GA_AUDIO_FORMATS: dict[str, dict[str, Any]] = {
    DEFAULT_AUDIO_FORMAT: {"type": "audio/pcm", "rate": 24000},
    "audio/pcmu": {"type": "audio/pcmu"},
    "audio/pcma": {"type": "audio/pcma"},
}


def _audio_format(media_type: str) -> dict[str, Any]:
    """The GA audio format object for a neutral audio media type."""
    if media_type not in _GA_AUDIO_FORMATS:
        raise ValueError(
            f"Unsupported audio format {media_type!r}: this realtime session takes {', '.join(_GA_AUDIO_FORMATS)}."
        )
    return dict(_GA_AUDIO_FORMATS[media_type])


def _item_create(item: dict[str, Any]) -> dict[str, Any]:
    """The wire event that adds ``item`` to the provider's conversation."""
    return {"type": "conversation.item.create", "item": item}


def _usage_details(usage: Mapping[str, Any]) -> dict[str, Any]:
    """Map a response's wire usage to UsageDetails keys, leaving out what the provider didn't report."""
    input_details = _mapping(usage.get("input_token_details"))
    cached_details = _mapping(input_details.get("cached_tokens_details"))
    output_details = _mapping(usage.get("output_token_details"))
    return _reported(
        input_token_count=usage.get("input_tokens"),
        output_token_count=usage.get("output_tokens"),
        total_token_count=usage.get("total_tokens"),
        cache_read_input_token_count=input_details.get("cached_tokens"),
        text_input_token_count=input_details.get("text_tokens"),
        audio_input_token_count=input_details.get("audio_tokens"),
        image_input_token_count=input_details.get("image_tokens"),
        text_cache_read_input_token_count=cached_details.get("text_tokens"),
        audio_cache_read_input_token_count=cached_details.get("audio_tokens"),
        image_cache_read_input_token_count=cached_details.get("image_tokens"),
        text_output_token_count=output_details.get("text_tokens"),
        audio_output_token_count=output_details.get("audio_tokens"),
        reasoning_output_token_count=output_details.get("reasoning_tokens"),
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    """``value`` when it's a JSON object, else an empty mapping, so optional wire fields read uniformly."""
    return cast("Mapping[str, Any]", value) if isinstance(value, Mapping) else {}


def _reported(**values: Any) -> dict[str, Any]:
    """The values the provider reported, leaving out any it didn't rather than setting them to None."""
    return {key: value for key, value in values.items() if value is not None}


def _present(source: Mapping[str, Any], *keys: str) -> dict[str, Any]:
    """The ``keys`` that ``source`` reports, under the same names."""
    return _reported(**{key: source.get(key) for key in keys})
