# Copyright (c) Microsoft. All rights reserved.

"""The live realtime conversation: one provider session, its updates and its control calls."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Coroutine, Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, Self, cast

from .._feature_stage import ExperimentalFeature, experimental
from .._types import AgentResponseUpdate, Content, normalize_messages
from ..exceptions import (
    RealtimeActiveResponseException,
    RealtimeBackpressureException,
    RealtimeControlException,
    RealtimeConversationClosedException,
    RealtimeException,
)
from ._acknowledgements import AcknowledgedT, await_acknowledgement
from ._barge_in import AudioLedger, Truncation
from ._events import RealtimeEvent
from ._options import RealtimeOptionsT
from ._responses import ResponseLedger
from ._tool_loop import ToolLoop, requested_by_a_tool
from ._update_stream import UpdateStream
from ._updates import core_attributes, tool_result_update, update_from_event

logger = logging.getLogger("agent_framework")

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
    once, and not after close starts. While it's open, the provider's events stream as ``AgentResponseUpdate``
    objects, the agent's tools run when a response that called them finishes, and their results are sent back with one
    follow-up response per batch.

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
        self._control_timeout = control_timeout
        self._max_queued_updates = max_queued_updates
        self._session: RealtimeClientSessionProtocol[RealtimeOptionsT] | None = None
        self._entered = False
        self._closing = asyncio.Event()
        self._write_lock = asyncio.Lock()
        self._control_lock = asyncio.Lock()
        self._close_task: asyncio.Task[None] | None = None
        self._failure: Exception | None = None
        self._updates = UpdateStream(max_queued_updates)
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
        if self._closing.is_set():
            raise RealtimeConversationClosedException("The realtime conversation is closed.")
        starting = asyncio.ensure_future(
            self._agent.client.start_session(options=self._options, tools=self._tools)  # type: ignore[arg-type]
        )
        try:
            session = await self._acknowledged(starting, "start the realtime session")
        except BaseException:
            # The session may have started as entry failed; the client closes only what it hasn't returned.
            if starting.done() and not starting.cancelled() and starting.exception() is None:
                await _close_after_failed_entry(starting.result())
            raise
        if self._closing.is_set():
            await _close_after_failed_entry(session)
            raise RealtimeConversationClosedException("The realtime conversation closed while it was starting.")
        # The conversation owns the session from here, so a close that starts while the initial messages go out
        # closes it.
        self._session = session
        try:
            async with self._write_lock:
                for message in self._initial_messages:
                    self._raise_if_closed_while_starting()
                    await session.send(message)
            self._raise_if_closed_while_starting()
        except BaseException as error:
            if not self._closing.is_set():
                # Entry failed on its own, so nothing else will close the session.
                self._session = None
                await _close_after_failed_entry(session)
                raise
            if isinstance(error, RealtimeException) and not isinstance(error, RealtimeConversationClosedException):
                # The close closed the socket under the pending write.
                raise RealtimeConversationClosedException(
                    "The realtime conversation closed while it was starting."
                ) from error
            raise
        self._receive_task = asyncio.create_task(self._receive_loop(session))
        return self

    def _raise_if_closed_while_starting(self) -> None:
        if self._closing.is_set():
            raise RealtimeConversationClosedException("The realtime conversation closed while it was starting.")

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
        return aiter(self._updates)

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
        if _is_repeated_error(event, self._previous_event):
            return
        self._previous_event = event
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
            self._cut_unheard(self._audio.on_output_audio(event))
        elif event.kind == "item_truncated":
            self._cut_unheard(self._audio.on_item_truncated(event))
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
        """Iteration raises ``error`` only after the updates already queued, so none are lost."""
        if self._failure is None:
            self._failure = error
        self._start_close()

    def _spawn(self, coroutine: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        task = asyncio.create_task(coroutine)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_task_done)
        return task

    def _background_task_done(self, task: asyncio.Task[Any]) -> None:
        """An unexpected failure must end the conversation rather than leave it silent."""
        self._background_tasks.discard(task)
        if task.cancelled():
            return
        if isinstance(error := task.exception(), Exception):
            logger.warning("A realtime conversation's background task failed: %s", error)
            self._fail(error)

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
            for message in messages:
                # Close may have started while this send waited its turn, or while an earlier message went out.
                await self._require_open_session().send(message)
        if _is_audio_only(messages):
            return  # the caller's audio: turn detection starts the chain and requests the response
        self._tool_loop.on_new_input()
        if create_response:
            await self.create_response()

    def _ready_for_a_follow_up(self) -> bool:
        """A follow-up must never talk over the caller or another response."""
        return self._caller_quiet.is_set() and not self._responses.any_active

    async def _wait_until_ready_for_a_follow_up(self) -> None:
        while not self._ready_for_a_follow_up():
            await self._caller_quiet.wait()
            await self._responses.wait_until_idle()

    async def _request_follow_up(self, claim: Callable[[], Mapping[str, Any] | None]) -> str | None:
        """The claim is made under the control lock, so it never mistakes filler for the follow-up.

        A tool's filler request holds that lock until it's recorded as filler. Returns ``None`` when the claim says a
        response already answered the results.
        """
        while True:
            await self._wait_until_ready_for_a_follow_up()
            self._require_open_session()
            async with self._control_lock:
                session = self._require_open_session()
                if self._ready_for_a_follow_up():
                    options = claim()
                    if options is None:
                        return None
                    return await self._create_response_locked(session, options or None)
            # The caller started speaking, or a response started, while this waited its turn.

    def _session_options(self) -> Mapping[str, Any]:
        return self._session.options if self._session is not None else self._options

    def _report_error(self, error: RealtimeException) -> None:
        """Failures the conversation survives, such as a rejected follow-up, stream instead of ending it."""
        code = error.code if isinstance(error, RealtimeControlException) else None
        self._emit(AgentResponseUpdate(contents=[Content.from_error(message=str(error), error_code=code)]))

    def _report_tool_result(self, result: Content, response_id: str) -> None:
        author = self._responses.agent_of(response_id, default=self._agent)
        self._emit(tool_result_update(result, response_id=response_id, author=author))

    async def _send_tool_results(self, results: Message) -> None:
        async with self._write_lock:
            await self._require_open_session().send(results)

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
        self._responses.request_started()
        try:
            response_id = await self._acknowledged(
                session.create_response(options),  # type: ignore[arg-type]
                "create a response",
            )
            self._responses.requested(response_id)
            if requested_by_a_tool(self):
                self._responses.filler(response_id)
            return response_id
        finally:
            self._responses.request_settled()

    async def interrupt(self, *, played_ms: int | None = None) -> None:
        """Cancel the active response, and cut the audio the caller didn't hear.

        With an active response, waits until it finishes. The provider's error for having no active response counts
        as done, since the response finished first. Either way, a new response can be requested as soon as this
        returns. With no active response, there is nothing to cancel.

        With ``played_ms``, the conversation cuts the latest response that had produced audio when this was called,
        where playback stopped, and cuts its later audio at 0 ms, including audio that arrives afterwards. ``played_ms``
        counts that response's audio across its assistant messages' audio parts in order, and each part is cut on its
        own. A part heard to within 40 ms of its end isn't cut. The first interrupt with ``played_ms`` fixes a
        response's cuts; a later one retries any the provider didn't acknowledge, and never repeats one it did. If the
        app is still playing an earlier response while a later one produces audio, ``played_ms`` is measured against the
        later one.

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
        # The response the app was playing when it called, before a later one can start while this waits.
        playing = self._audio.latest_response_id
        self._require_open_session()
        async with self._control_lock:
            session = self._require_open_session()  # close may have started while this call waited its turn
            if cancelling := self._responses.active:
                try:
                    await self._acknowledged(session.cancel_response(), "cancel the active response")
                except RealtimeControlException as rejection:
                    # The response finished before the cancel reached the provider, which is what interrupt() wants.
                    if rejection.code != _NO_ACTIVE_RESPONSE:
                        raise
                # The provider has finished it, though its response_done may not have been read yet. A response that
                # started meanwhile stays active.
                for response_id in cancelling:
                    self._responses.finished(response_id)
            if played_ms is not None:
                await self._send_truncations(session, self._audio.cut(playing, played_ms))

    def _cut_unheard(self, truncations: Sequence[Truncation]) -> None:
        if truncations:
            self._spawn(self._truncate_unheard(truncations))

    async def _truncate_unheard(self, truncations: Sequence[Truncation]) -> None:
        session = self._require_session()
        try:
            async with self._control_lock:
                if self._closing.is_set():
                    return
                await self._send_truncations(session, truncations)
        except RealtimeException as error:
            logger.warning("Couldn't cut the audio after a provider truncation: %s", error)

    async def _send_truncations(
        self, session: RealtimeClientSessionProtocol[RealtimeOptionsT], truncations: Sequence[Truncation]
    ) -> None:
        first_failure: RealtimeControlException | None = None
        for truncation in truncations:
            if not self._audio.is_owed(truncation):
                continue  # another control call made this cut while this one waited its turn
            try:
                await self._acknowledged(
                    session.truncate(truncation.item_id, truncation.content_index, truncation.audio_end_ms),
                    "cut the audio the caller didn't hear",
                )
            except RealtimeControlException as failure:
                # The cut stays owed, but one the provider keeps rejecting mustn't stop the cuts after it.
                first_failure = first_failure or failure
                continue
            self._audio.acknowledged(truncation)
        if first_failure is not None:
            raise first_failure

    async def _acknowledged(self, acknowledgement: Awaitable[AcknowledgedT], action: str) -> AcknowledgedT:
        return await await_acknowledgement(
            acknowledgement, action=action, timeout=self._control_timeout, closing=self._closing
        )

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
        """Input stops at once, synchronously, so nothing new starts while the close sequence runs."""
        if self._close_task is None:
            self._closing.set()
            self._tool_loop.stop()
            self._close_task = asyncio.create_task(self._close_sequence())
        return self._close_task

    async def _close_sequence(self) -> None:
        """The updates end last, so iteration sees every settled call before it stops."""
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
            await self._end_background_tasks()
        finally:
            self._updates.finish(self._failure)

    async def _end_background_tasks(self) -> None:
        """Nothing the conversation spawned may outlive it."""
        remaining = list(self._background_tasks)
        for task in remaining:
            task.cancel()
        if remaining:
            await asyncio.wait(remaining)


async def _close_after_failed_entry(session: RealtimeClientSessionProtocol[Any]) -> None:
    """The failure that stopped entry wins over a failure to close."""
    try:
        await session.close()
    except Exception as error:
        logger.warning("Closing the realtime session after entry failed also failed: %s", error)


def _reject_app_input_for_calls(messages: Sequence[Message]) -> None:
    """No call in this conversation waits for app input, so a result or approval would answer a call twice."""
    for message in messages:
        for content in message.contents:
            if content.type in ("function_result", "function_approval_response"):
                raise ValueError(
                    f"{content.type} content answers a call that's waiting for the app, and this conversation has "
                    "none: its tool loop runs every call itself."
                )


def _is_audio_only(messages: Sequence[Message]) -> bool:
    contents = [content for message in messages for content in message.contents]
    return bool(contents) and all(_is_audio(content) for content in contents)


def _is_audio(content: Content) -> bool:
    return content.type in ("data", "uri") and content.has_top_level_media_type("audio")


def _is_repeated_error(event: RealtimeEvent, previous: RealtimeEvent | None) -> bool:
    """Voice Live can repeat one unclaimed rejection three times."""
    if event.kind != "error" or previous is None or previous.kind != "error":
        return False
    if event.content is None or previous.content is None:
        return False
    return _error_identity(event) == _error_identity(previous)


def _error_identity(event: RealtimeEvent) -> tuple[Any, ...]:
    """Neutral attributes are left out because they restate the provider's own fields."""
    error = cast(Content, event.content)
    provider_metadata = {
        namespace: value for namespace, value in event.additional_properties.items() if namespace != "core"
    }
    return (error.message, error.error_code, error.additional_properties, provider_metadata)
