# Copyright (c) Microsoft. All rights reserved.

"""The live realtime conversation: one provider session, its updates and its control calls."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Coroutine, Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, Self, TypeVar

from .._feature_stage import ExperimentalFeature, experimental
from .._types import AgentResponseUpdate, Content, normalize_messages
from ..exceptions import (
    RealtimeActiveResponseException,
    RealtimeBackpressureException,
    RealtimeControlException,
    RealtimeControlTimeoutException,
    RealtimeConversationClosedException,
    RealtimeException,
)
from ._barge_in import AudioLedger, Truncation
from ._events import RealtimeEvent
from ._options import RealtimeOptionsT
from ._responses import ResponseLedger
from ._tool_loop import FollowUp, ToolLoop, requested_by_a_tool
from ._updates import core_attributes, tool_result_update, update_from_event

logger = logging.getLogger("agent_framework")

AcknowledgedT = TypeVar("AcknowledgedT")

_NO_ACTIVE_RESPONSE = "response_cancel_not_active"

if TYPE_CHECKING:
    from .._middleware import FunctionMiddlewareTypes
    from .._sessions import AgentSession
    from .._tools import FunctionInvocationConfiguration, ToolTypes
    from .._types import AgentRunInputs, Message
    from ._agent import RawRealtimeAgent
    from ._clients import RealtimeClientSessionProtocol


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeConversation(Generic[RealtimeOptionsT]):
    """A live, full-duplex conversation with a realtime agent.

    Create one with ``RealtimeAgent.start_conversation(...)`` and open it with ``async with``; it can be entered only
    once. While it's open, the provider's events stream as ``AgentResponseUpdate`` objects, the agent's tools run when
    a response that called them finishes, and their results are sent back with one follow-up response per batch.

    Read the updates from a single task, and start control calls such as ``interrupt(...)`` as separate tasks so the
    loop keeps reading. Iteration ends when the conversation closes, and raises the error that ended it, such as
    ``RealtimeConnectionException`` or ``RealtimeBackpressureException``, after the queued updates are read.

    Examples:
        .. code-block:: python

            async with agent.start_conversation() as conversation:
                await conversation.send(Message("user", [Content.from_data(audio_chunk, "audio/pcm;rate=24000")]))
                async for update in conversation:
                    for content in update.contents:
                        if content.type == "data":
                            play(content)
    """

    def __init__(
        self,
        agent: RawRealtimeAgent[RealtimeOptionsT],
        *,
        initial_messages: Sequence[Message],
        agent_session: AgentSession | None,
        options: Mapping[str, Any],
        tools: Sequence[ToolTypes],
        invocation_configuration: FunctionInvocationConfiguration,
        function_middleware: Sequence[FunctionMiddlewareTypes],
        function_invocation_kwargs: Mapping[str, Any],
        control_timeout: float,
        max_queued_updates: int,
    ) -> None:
        self._agent = agent
        self._initial_messages = initial_messages
        self._options = options
        self._tools = tools
        self._invocation_configuration = invocation_configuration
        self._control_timeout = control_timeout
        self._max_queued_updates = max_queued_updates
        self._session: RealtimeClientSessionProtocol[RealtimeOptionsT] | None = None
        self._entered = False
        self._closing = asyncio.Event()
        self._write_lock = asyncio.Lock()
        self._control_lock = asyncio.Lock()
        self._close_task: asyncio.Task[None] | None = None
        self._failure: Exception | None = None
        self._updates = _UpdateStream(max_queued_updates)
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._receive_task: asyncio.Task[None] | None = None
        self._responses = ResponseLedger()
        self._caller_quiet = asyncio.Event()
        self._caller_quiet.set()
        self._tool_loop = ToolLoop(
            tools=list(tools),
            configuration=invocation_configuration,
            invocation_kwargs=function_invocation_kwargs,
            function_middleware=function_middleware,
            agent_session=agent_session,
            conversation=self,
            responses=self._responses,
            send_results=self._send_tool_results,
            report_result=self._report_tool_result,
            report_error=self._report_error,
            wait_until_ready=self._wait_until_ready_for_a_follow_up,
            request_follow_up=self._request_follow_up,
            session_options=self._session_options,
            fail=self._fail,
            spawn=self._spawn,
        )
        self._audio = AudioLedger()
        self._previous_event: RealtimeEvent | None = None

    @property
    def active_agent(self) -> RawRealtimeAgent[RealtimeOptionsT]:
        """The agent whose instructions, tools and voice are live, which is the agent that started the conversation."""
        return self._agent

    async def __aenter__(self) -> Self:
        if self._entered:
            raise RuntimeError("A realtime conversation can be entered only once; start a new one instead.")
        self._entered = True
        self._session = await self._acknowledged(
            self._agent.client.start_session(options=self._options, tools=self._tools),  # type: ignore[arg-type]
            "start the realtime session",
        )
        try:
            for message in self._initial_messages:
                await self._session.send(message)
        except BaseException:
            await self._session.close()
            raise
        self._receive_task = asyncio.create_task(self._receive_loop(self._session))
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    @property
    def updates(self) -> AsyncIterable[AgentResponseUpdate]:
        """The conversation's updates, which ``async for update in conversation`` also reads.

        There is one stream, read by one reader. Iteration ends when the conversation closes, and raises the error
        that ended it once the queued updates are read.
        """
        return self

    def __aiter__(self) -> AsyncIterator[AgentResponseUpdate]:
        return self._iterate_updates()

    async def _iterate_updates(self) -> AsyncIterator[AgentResponseUpdate]:
        async for update in self._updates:
            yield update

    async def _receive_loop(self, session: RealtimeClientSessionProtocol[RealtimeOptionsT]) -> None:
        failure: Exception | None = None
        try:
            async for event in session.receive():
                self._dispatch(event)
        except Exception as error:
            failure = error
        # Once close has started, a receive error is a consequence of closing, such as a socket that errors once
        # closed, so it doesn't fail a conversation the app or provider ended.
        if failure is not None and not self._closing.is_set():
            logger.warning("The realtime conversation's receive loop failed: %s", failure)
            self._fail(failure)
        else:
            self._start_close()

    def _dispatch(self, event: RealtimeEvent) -> None:
        repeated = _is_repeated_error(event, self._previous_event)
        self._previous_event = event
        if repeated:
            return
        response_id = core_attributes(event).get("response_id", "")
        if event.kind == "response_created":
            self._responses.created(response_id, self._agent)
        elif event.kind == "response_done":
            self._responses.finished(response_id)
            self._tool_loop.on_response_done(response_id, core_attributes(event).get("status", ""))
        elif event.kind == "function_call":
            self._responses.called(response_id)
            self._tool_loop.on_function_call(event)
        elif event.kind == "speech_started":
            self._caller_quiet.clear()
            self._tool_loop.on_new_input()
        elif event.kind == "speech_stopped":
            self._caller_quiet.set()
        elif event.kind == "output_audio":
            self._audio.on_output_audio(event)
        elif event.kind == "item_truncated" and (truncations := self._audio.on_item_truncated(event)):
            self._spawn(self._truncate_unheard(truncations))
        self._emit(
            update_from_event(
                event,
                author=self._responses.agent_of(response_id, default=self._agent),
                made_calls=self._responses.made_calls(response_id),
            )
        )

    def _emit(self, update: AgentResponseUpdate) -> None:
        if not self._updates.put(update):
            self._fail(
                RealtimeBackpressureException(
                    f"More than {self._max_queued_updates} updates were waiting to be read; the app isn't reading "
                    "the conversation's updates fast enough."
                )
            )

    def _fail(self, error: Exception) -> None:
        """End the conversation with ``error``, which iteration raises once the queued updates are read."""
        if self._failure is None:
            self._failure = error
        self._start_close()

    def _spawn(self, coroutine: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        task = asyncio.create_task(coroutine)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    async def send(self, value: AgentRunInputs | RealtimeEvent, *, create_response: bool = True) -> None:
        """Send input or a raw provider command, and request a response to text or message input.

        A ``RealtimeEvent`` is a raw command: the session sends its ``raw_representation`` verbatim, and the
        conversation doesn't track it. Other input is added to the provider's conversation in order. Audio-only input
        leaves the response to turn detection. Any other input starts a new tool chain, so the tool loop's limits
        count afresh, and then requests a response unless ``create_response`` is ``False``. Adding the input never
        waits behind a control call.

        Args:
            value: The input, or a raw provider command.

        Keyword Args:
            create_response: Whether to request a response after adding text or message input.

        Raises:
            ValueError: If the input holds a function result or approval response; no call in this conversation waits
                for the app's answer.
            RealtimeActiveResponseException: If a response is requested while another is active. The input has
                already been added.
            RealtimeConversationClosedException: If the conversation has started closing.
            RealtimeControlTimeoutException: If the provider doesn't acknowledge the response request in time.
            RealtimeControlException: If the provider rejects the response request.
        """
        self._require_open_session()
        if isinstance(value, RealtimeEvent):
            async with self._write_lock:
                await self._require_open_session().send(value)
            return
        messages = normalize_messages(value)
        _reject_app_input_for_calls(messages)
        async with self._write_lock:
            session = self._require_open_session()  # close may have started while this send waited its turn
            for message in messages:
                await session.send(message)
        if _is_audio_only(messages):
            return  # the caller's audio: turn detection starts the chain and requests the response
        self._tool_loop.on_new_input()
        if create_response:
            await self.create_response()

    def _ready_for_a_follow_up(self) -> bool:
        """Whether no response is active and the caller isn't speaking, so a follow-up never talks over them."""
        return self._caller_quiet.is_set() and not self._responses.any_active

    async def _wait_until_ready_for_a_follow_up(self) -> None:
        while not self._ready_for_a_follow_up():
            await self._caller_quiet.wait()
            await self._responses.wait_until_idle()

    async def _request_follow_up(self, claim: Callable[[], FollowUp | None]) -> str | None:
        """Request the tool loop's follow-up, checking just before the request that it can still go out.

        Returns ``None``, requesting nothing, if the caller started speaking, a response started, or the tool loop no
        longer claims it, while the request waited its turn.
        """
        self._require_open_session()
        async with self._control_lock:
            session = self._require_open_session()
            if not self._ready_for_a_follow_up():
                return None
            follow_up = claim()
            if follow_up is None:
                return None
            return await self._create_response_locked(session, follow_up.options)

    def _session_options(self) -> Mapping[str, Any]:
        """The live session's current settings."""
        return self._session.options if self._session is not None else self._options

    def _report_error(self, error: RealtimeException) -> None:
        """Stream a failure the conversation survives, such as a rejected follow-up, as ``error`` content."""
        code = error.code if isinstance(error, RealtimeControlException) else None
        self._emit(AgentResponseUpdate(contents=[Content.from_error(message=str(error), error_code=code)]))

    def _report_tool_result(self, result: Content, response_id: str) -> None:
        author = self._responses.agent_of(response_id, default=self._agent)
        self._emit(tool_result_update(result, response_id=response_id, author=author))

    async def _send_tool_results(self, results: Message) -> None:
        session = self._require_session()
        async with self._write_lock:
            await session.send(results)

    async def create_response(self, *, options: RealtimeOptionsT | None = None) -> str:
        """Request a response, and return its id once the provider reports it created.

        Nothing is queued: while a response is active, the request raises rather than waiting. A response requested
        by a tool or function middleware of this conversation, such as filler while a tool runs, doesn't count as the
        tool batch's follow-up.

        Keyword Args:
            options: Settings for this response only.

        Returns:
            The provider's id for the new response.

        Raises:
            RealtimeActiveResponseException: If a response is active, or another response started instead.
            RealtimeConversationClosedException: If the conversation has started closing.
            RealtimeControlTimeoutException: If the provider doesn't acknowledge the request in time; it may still
                create the response.
            RealtimeControlException: If the provider rejects the request.
        """
        self._require_open_session()
        async with self._control_lock:
            session = self._require_open_session()  # close may have started while this call waited its turn
            return await self._create_response_locked(session, options)

    async def _create_response_locked(
        self, session: RealtimeClientSessionProtocol[RealtimeOptionsT], options: Mapping[str, Any] | None
    ) -> str:
        if self._responses.any_active:
            raise RealtimeActiveResponseException(
                "A response is active. Call interrupt() first to talk over it; nothing is queued."
            )
        response_id = await self._acknowledged(
            session.create_response(options),  # type: ignore[arg-type]
            "create a response",
        )
        self._responses.requested(response_id)
        if requested_by_a_tool(self):
            self._responses.not_answering(response_id)
        return response_id

    async def interrupt(self, *, played_ms: int | None = None) -> None:
        """Cancel the active response, and cut the audio the caller didn't hear.

        With an active response, waits until it finishes. The provider's error for having no active response counts
        as done, since the response finished first. With no active response, there is nothing to cancel.

        With ``played_ms``, the conversation cuts the latest response that produced audio where playback stopped, and
        cuts its later assistant messages at 0 ms. ``played_ms`` counts that response's audio across its assistant
        messages in order. A message heard to within 40 ms of its end isn't cut, and a response is cut only once. If
        the app is still playing an earlier response while a later one produces audio, ``played_ms`` is measured
        against the later one.

        Keyword Args:
            played_ms: How many milliseconds of the response's audio the caller heard, or ``None`` to cut nothing.

        Raises:
            ValueError: If ``played_ms`` is negative.
            RealtimeConversationClosedException: If the conversation has started closing.
            RealtimeControlTimeoutException: If the provider doesn't acknowledge the cancel or a cut in time.
            RealtimeControlException: If the provider rejects the cancel or a cut.
        """
        if played_ms is not None and played_ms < 0:
            raise ValueError(f"played_ms must be 0 or more, not {played_ms}.")
        self._require_open_session()
        async with self._control_lock:
            session = self._require_open_session()  # close may have started while this call waited its turn
            if self._responses.any_active:
                try:
                    await self._acknowledged(session.cancel_response(), "cancel the active response")
                except RealtimeControlException as rejection:
                    # The response finished before the cancel reached the provider, which is what interrupt() wants.
                    if rejection.code != _NO_ACTIVE_RESPONSE:
                        raise
            if played_ms is not None:
                await self._send_truncations(session, self._audio.cut(played_ms))

    async def _truncate_unheard(self, truncations: Sequence[Truncation]) -> None:
        """Cut messages after one the provider truncated, as a control call."""
        session = self._require_session()
        try:
            async with self._control_lock:
                if self._closing.is_set():
                    return  # the conversation is closing, so the unheard audio no longer matters
                await self._send_truncations(session, truncations)
        except RealtimeException as error:
            logger.warning("Couldn't cut the audio after a provider truncation: %s", error)

    async def _send_truncations(
        self, session: RealtimeClientSessionProtocol[RealtimeOptionsT], truncations: Sequence[Truncation]
    ) -> None:
        for truncation in truncations:
            await self._acknowledged(
                session.truncate(truncation.item_id, truncation.content_index, truncation.audio_end_ms),
                "cut the audio the caller didn't hear",
            )

    async def _acknowledged(self, acknowledgement: Awaitable[AcknowledgedT], action: str) -> AcknowledgedT:
        """Await a provider acknowledgement within the conversation's control timeout, or until close starts.

        The session's call is cancelled if the wait ends without it; the protocol makes that safe. The cancelled call
        finishes unwinding before this returns, so it never overlaps the next control call.
        """
        call = asyncio.ensure_future(acknowledgement)
        closing = asyncio.ensure_future(self._closing.wait())
        try:
            async with asyncio.timeout(self._control_timeout):
                await asyncio.wait({call, closing}, return_when=asyncio.FIRST_COMPLETED)
        except TimeoutError as timeout:
            raise RealtimeControlTimeoutException(
                f"The provider didn't acknowledge the request to {action} within {self._control_timeout} seconds; "
                "it may still apply it."
            ) from timeout
        finally:
            closing.cancel()
            if not call.done():
                call.cancel()
                await asyncio.wait({call})
        if call.done() and not call.cancelled():
            return call.result()
        raise RealtimeConversationClosedException(f"The realtime conversation closed before it could {action}.")

    def _require_session(self) -> RealtimeClientSessionProtocol[RealtimeOptionsT]:
        if self._session is None:
            raise RuntimeError("Enter the realtime conversation with `async with` before using it.")
        return self._session

    def _require_open_session(self) -> RealtimeClientSessionProtocol[RealtimeOptionsT]:
        session = self._require_session()
        if self._closing.is_set():
            raise RealtimeConversationClosedException("The realtime conversation is closed.")
        return session

    async def close(self) -> None:
        """Close the conversation and its realtime session.

        Closing stops new input and control calls at once, then closes the session, cancels running tools with the
        ``conversation_closed`` reason, and streams an error result for each call that didn't finish. Iteration ends
        once the queued updates are read. Closing again waits for the same close, and a cancelled caller doesn't
        cancel the close itself.
        """
        await asyncio.shield(self._start_close())

    def _start_close(self) -> asyncio.Task[None]:
        """Stop accepting input now, and start the close sequence once; later calls return the same task."""
        if self._close_task is None:
            self._closing.set()
            self._tool_loop.stop()
            self._close_task = asyncio.create_task(self._close_sequence())
        return self._close_task

    async def _close_sequence(self) -> None:
        """Spec 006's close sequence, steps 3, 4 and 7; history (#31) and middleware (#40) add the others."""
        try:
            if self._session is not None:
                try:
                    await self._session.close()
                except Exception as error:
                    logger.warning("Closing the realtime session failed: %s", error)
                    # The session may not end its events, so stop reading them rather than wait.
                    if self._receive_task is not None:
                        self._receive_task.cancel()
            if self._receive_task is not None:
                await asyncio.wait({self._receive_task})
            await self._tool_loop.close()
        finally:
            self._updates.finish(self._failure)


class _UpdateStream:
    """The conversation's updates, read in order by one reader, with its end held outside the buffer."""

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._buffer: deque[AgentResponseUpdate] = deque()
        self._finished = False
        self._failure: Exception | None = None
        self._changed = asyncio.Event()

    def put(self, update: AgentResponseUpdate) -> bool:
        """Buffer ``update``, returning ``False`` when the buffer is full. Updates after the end are dropped."""
        if self._finished:
            return True
        if len(self._buffer) >= self._capacity:
            return False
        self._buffer.append(update)
        self._changed.set()
        return True

    def finish(self, failure: Exception | None = None) -> None:
        """End the stream once the buffered updates are read, raising ``failure`` then if there is one."""
        if self._finished:
            return
        self._finished = True
        self._failure = failure
        self._changed.set()

    async def __aiter__(self) -> AsyncIterator[AgentResponseUpdate]:
        while True:
            if self._buffer:
                yield self._buffer.popleft()
                continue
            if self._finished:
                if self._failure is not None:
                    raise self._failure
                return
            self._changed.clear()
            await self._changed.wait()


def _reject_app_input_for_calls(messages: Sequence[Message]) -> None:
    """Raise for tool results and approval responses: no call in this conversation waits for app input."""
    for message in messages:
        for content in message.contents:
            if content.type in ("function_result", "function_approval_response"):
                raise ValueError(
                    f"{content.type} content answers a call that's waiting for the app, and this conversation has "
                    "none: its tool loop runs every call itself."
                )


def _is_audio_only(messages: Sequence[Message]) -> bool:
    """Whether the input is the caller's audio only, which leaves responses to turn detection."""
    contents = [content for message in messages for content in message.contents]
    return bool(contents) and all(_is_audio(content) for content in contents)


def _is_audio(content: Content) -> bool:
    return content.type in ("data", "uri") and content.has_top_level_media_type("audio")


def _is_repeated_error(event: RealtimeEvent, previous: RealtimeEvent | None) -> bool:
    """Whether ``event`` repeats the error just before it, which spec 006 counts once."""
    if event.kind != "error" or previous is None or previous.kind != "error":
        return False
    if event.content is None or previous.content is None:
        return False
    return _error_identity(event.content) == _error_identity(previous.content)


def _error_identity(error: Content) -> tuple[Any, ...]:
    return (error.message, error.error_code, error.additional_properties)
