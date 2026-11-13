# Copyright (c) Microsoft. All rights reserved.

"""The live realtime conversation: one provider session, its updates and its control calls."""

from __future__ import annotations

import asyncio
import contextlib
import copy
import logging
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Coroutine, Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, Self, cast

from .._feature_stage import ExperimentalFeature, experimental
from .._middleware import categorize_middleware
from .._sessions import AgentSession, ContextProvider, HistoryProvider, SessionContext
from .._tools import _append_unique_tools, normalize_tools  # pyright: ignore[reportPrivateUsage]
from .._types import AgentResponseUpdate, Content, _append_instructions, normalize_messages  # pyright: ignore[reportPrivateUsage]
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
from ._history import CallHistory
from ._options import RealtimeOptionsT
from ._reporting import ExchangeReporter
from ._responses import ResponseLedger
from ._tool_loop import ToolLoop, requested_by_a_tool
from ._update_stream import UpdateStream
from ._updates import core_attributes, finish_reason_of, tool_result_update, update_from_event

logger = logging.getLogger("agent_framework")

_NO_ACTIVE_RESPONSE = "response_cancel_not_active"

CALLER_TRANSCRIPT_WAIT_SECONDS = 3.0

if TYPE_CHECKING:
    from .._middleware import FunctionMiddlewareTypes
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

    The call's turns are kept as text in ``history``. Opening runs the agent's context providers' ``before_run`` once,
    applies their instructions and tools, and replays the history they load into the new realtime session. Each
    exchange, the input from the caller or the app with the reply and tool calls it drives, then reaches their
    ``after_run`` during the call, and close reports the rest.

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
        if agent_session is None and agent.context_providers:
            # As in chat: state a provider sets up in before_run must reach the tools through the same session.
            agent_session = AgentSession()
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
        self._history = CallHistory()
        self._history_changed = asyncio.Event()
        self._provider_session = agent_session if agent_session is not None else AgentSession()
        self._reporter = (
            ExchangeReporter(agent, self._provider_session, self._history, self._report_context_provider_failure)
            if agent.context_providers
            else None
        )
        self._previous_event: RealtimeEvent | None = None

    @property
    def active_agent(self) -> RawRealtimeAgent[RealtimeOptionsT]:
        """The agent whose instructions, tools and voice are live, which is the agent that started the conversation."""
        return self._agent

    @property
    def history(self) -> list[Message]:
        """A snapshot of the call's turns so far, as text messages in the provider's conversation order.

        It holds the caller's turns as their transcripts, the agent's turns, app input and tool calls with their
        results, including exchanges not yet reported to context providers, and the final record after close. A cut-off
        turn keeps only what the caller heard, marked ``status="incomplete"``. A caller turn without text has no
        contents and says why in ``additional_properties["core"]["text_less"]``: ``"transcript_pending"``,
        ``"transcription_failed"``, ``"transcription_off"`` or ``"transcript_missing"``.
        """
        # Copies, because context providers may hold these same messages in the agent session.
        return copy.deepcopy(self._history.messages())

    async def __aenter__(self) -> Self:
        if self._entered:
            raise RuntimeError("A realtime conversation can be entered only once; start a new one instead.")
        self._entered = True
        if self._closing.is_set():
            raise RealtimeConversationClosedException("The realtime conversation is closed.")
        start_context = await self._prepare_from_context_providers()
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
        self._tool_loop.on_tools_declared(self._tools)
        try:
            async with self._write_lock:
                # Loaded history is already the agent session's, so it's replayed without being recorded again.
                for message in start_context.get_messages():
                    self._raise_if_closed_while_starting()
                    await session.send(message)
                for message in self._initial_messages:
                    self._raise_if_closed_while_starting()
                    await self._add_app_input(session, message)
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
        if self._reporter is not None:
            self._spawn(self._reporter.run())
        return self

    async def _prepare_from_context_providers(self) -> SessionContext:
        start_context = await self._load_start_context()
        self._apply_start_context(start_context)
        if self._reporter is not None:
            self._reporter.start(start_context)
        if not self._options.get("transcription") and any(
            isinstance(provider, HistoryProvider) for provider in self._agent.context_providers
        ):
            # Switching transcription on would bill a second model, which is the app's call.
            logger.warning(
                "The realtime conversation has a history provider but no transcription, so caller turns are stored "
                "without text. Set the transcription option to keep what the caller said."
            )
        return start_context

    async def _load_start_context(self) -> SessionContext:
        """Context providers run once per conversation, as they run once per chat run."""
        context = SessionContext(
            session_id=self._provider_session.session_id,
            input_messages=list(self._initial_messages),
            options=dict(self._options),
        )
        for provider in self._agent.context_providers:
            if isinstance(provider, HistoryProvider) and not provider.load_messages:
                continue
            await provider.before_run(
                # A realtime agent has no run(), which SupportsAgentRun requires; providers read its identity.
                agent=self._agent,  # type: ignore[arg-type]
                session=self._provider_session,
                context=context,
                state=self._provider_session.state.setdefault(provider.source_id, {}),
            )
        return context

    def _apply_start_context(self, context: SessionContext) -> None:
        """As in chat, providers add their instructions, tools and function middleware after the agent's.

        Chat middleware has no model call to wrap here, as with the agent's own.
        """
        if context.instructions:
            self._options = {
                **self._options,
                "instructions": _append_instructions(
                    self._options.get("instructions"), "\n".join(context.instructions)
                ),
            }
        if context.tools:
            self._tools = unique_tools([*self._tools, *normalize_tools(context.tools)])
        if provider_middleware := context.get_middleware():
            self._tool_loop.add_function_middleware(
                categorize_middleware(provider_middleware, supported_categories=("function",))["function"]
            )

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
        core = core_attributes(event)
        response_id = core.get("response_id", "")
        if event.kind == "response_created":
            self._responses.created(response_id, self._agent)
            self._history.on_response_created(event)
        elif event.kind == "response_done":
            status = core.get("status", "")
            self._responses.finished(response_id)
            self._history.on_response_done(
                response_id,
                status,
                core.get("reason"),
                finish_reason_of(core, self._responses.made_calls(response_id)),
            )
            self._tool_loop.on_response_done(response_id, status)
            self._cut_unheard([], self._mark_unheard_text(response_id))
        elif event.kind == "function_call":
            self._responses.called(response_id)
            self._tool_loop.on_function_call(event)
            self._history.on_function_call(event, self._responses.agent_of(response_id, default=self._agent))
        elif event.kind == "speech_started":
            self._caller_quiet.clear()
            self._tool_loop.on_new_input()
        elif event.kind == "speech_stopped":
            self._caller_quiet.set()
        elif event.kind == "item_added":
            if core.get("role") == "user":
                # A caller turn the app commits itself brings no speech_started, only the user item it adds.
                self._tool_loop.on_new_input()
            self._history.on_item_added(event, transcribing=bool(self._session_options().get("transcription")))
        elif event.kind == "output_audio":
            self._cut_unheard(self._audio.on_output_audio(event))
        elif event.kind == "item_truncated":
            truncations = self._audio.on_item_truncated(event)
            self._cut_unheard(truncations, self._mark_unheard_text(self._audio.response_cut_at(core["item_id"])))
            self._history.on_item_truncated(event, self._audio.generated_ms(core["item_id"], core["content_index"]))
        elif event.kind == "input_transcription":
            self._history.on_input_transcription(event)
        elif event.kind == "input_transcription_failed":
            self._history.on_input_transcription_failed(event)
        elif event.kind == "usage":
            self._history.on_usage(event)
        elif event.kind == "output_text":
            self._history.on_output_text(event, self._responses.agent_of(response_id, default=self._agent))
        self._history_changed.set()
        if self._reporter is not None:
            self._reporter.wake()
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
        leaves the response to turn detection or the app, and starts a new tool chain once the caller's turn is
        committed. Any other input starts a new tool chain, so the tool loop's limits count afresh, and then requests a
        response unless ``create_response`` is ``False``. Adding the input never waits behind a control call.

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
                await self._add_app_input(self._require_open_session(), message)
        if _is_audio_only(messages):
            return  # committing the caller's turn starts its chain; turn detection or the app asks for the response
        self._tool_loop.on_new_input()
        if create_response:
            await self.create_response()

    async def _add_app_input(self, session: RealtimeClientSessionProtocol[RealtimeOptionsT], message: Message) -> None:
        await session.send(message)
        self._history.on_app_input(message)

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

    def _report_context_provider_failure(self, provider: ContextProvider, error: Exception) -> None:
        """The call goes on: the provider retries this exchange with the next report, and close raises if it can't."""
        message = f"Context provider '{provider.source_id}' failed to record an exchange: {error}"
        self._emit(AgentResponseUpdate(contents=[Content.from_error(message=message)]))

    def _report_tool_result(self, result: Content, response_id: str) -> None:
        self._history.on_tool_result(result)
        if self._reporter is not None:
            self._reporter.wake()
        author = self._responses.agent_of(response_id, default=self._agent)
        self._emit(tool_result_update(result, response_id=response_id, author=author))

    async def _send_tool_results(self, results: Message) -> None:
        async with self._write_lock:
            await self._require_open_session().send(results)

    async def update_session(
        self,
        *,
        options: RealtimeOptionsT | None = None,
        tools: ToolTypes | Callable[..., Any] | Sequence[ToolTypes | Callable[..., Any]] | None = None,
    ) -> None:
        """Change the live realtime session's settings or tools mid-call, without reconnecting.

        ``options`` merge over the session's current settings. ``tools``, when given, replace both the tools the
        provider declares and the tools the conversation runs, ``[]`` clearing them; calls that already arrived keep
        the tools they resolved to. The call returns once the provider acknowledges the update, so any response
        requested afterwards uses it, or at once when the provider client drops every option it can't honor. Neither
        the agent's defaults nor the agent session change.

        Keyword Args:
            options: The settings to change.
            tools: The tools to use instead of the current ones, or ``None`` to keep them.

        Raises:
            ValueError: If two tools share a name; nothing is sent.
            RealtimeConversationClosedException: If the conversation has started closing.
            RealtimeControlTimeoutException: If the provider doesn't acknowledge the update in time; it may still apply
                it, and the conversation keeps running its current tools.
            RealtimeControlException: If the provider rejects the update; nothing changes.
        """
        declared = None if tools is None else unique_tools(normalize_tools(tools))
        async with self._control_lock:
            await self._update_session_locked(options or {}, declared)
        if declared is not None:
            self._tool_loop.on_tools_declared(declared)

    async def _update_session_locked(self, options: Mapping[str, Any], tools: list[ToolTypes] | None) -> None:
        session = self._require_open_session()  # close may have started while this call waited its turn
        await self._acknowledged(
            session.update_session(options, tools),  # type: ignore[arg-type]
            "update the session",
        )

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
        return await self._request_response(
            lambda: session.create_response(options),  # type: ignore[arg-type]
            "create a response",
        )

    async def _request_response(self, request: Callable[[], Awaitable[str]], action: str) -> str:
        """Called under the control lock. Generated and verbatim responses follow the same rules."""
        if self._responses.any_active:
            raise RealtimeActiveResponseException(
                "A response is active. Call interrupt() first to talk over it; nothing is queued."
            )
        self._responses.request_started()
        try:
            response_id = await self._acknowledged(request(), action)
            self._responses.requested(response_id)
            if requested_by_a_tool(self):
                self._responses.filler(response_id)
            return response_id
        finally:
            self._responses.request_settled()

    async def say(self, text: str) -> str:
        """Have the agent speak ``text`` word for word, and return the response id once it's created.

        With an Azure text-to-speech voice, Voice Live and Foundry voice agents speak the text as a pre-generated
        response. Other providers and voices ask the model to repeat it, which is best effort. Like
        ``create_response(...)``, nothing is queued while a response is active, and a line that a tool or function
        middleware of this conversation says, such as filler while a tool runs, doesn't count as the tool batch's
        follow-up.

        Args:
            text: The words to speak.

        Returns:
            The provider's id for the response that speaks the text.

        Raises:
            RealtimeActiveResponseException: If a response is active, or another response started instead.
            RealtimeUnsupportedException: If the provider can't speak text verbatim with the session's voice, such as
                Voice Live with an OpenAI voice.
            RealtimeConversationClosedException: If the conversation has started closing.
            RealtimeControlTimeoutException: If the provider doesn't acknowledge the request in time; it may still
                speak the text.
            RealtimeControlException: If the provider rejects the request.
        """
        self._require_open_session()
        async with self._control_lock:
            session = self._require_open_session()  # close may have started while this call waited its turn
            return await self._request_response(lambda: session.say(text), "say the text")

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
                truncations = self._audio.cut(playing, played_ms)
                unheard = self._mark_unheard_text(playing)
                cut_failure = await self._send_truncations(session, truncations)
                delete_failure = await self._delete_items(session, unheard)
                if (failure := cut_failure or delete_failure) is not None:
                    raise failure

    def _mark_unheard_text(self, response_id: str | None) -> list[str]:
        """A message without audio is unheard once its response has a cut and can't produce audio for it any more.

        History drops it as soon as that's known, before the provider is asked to delete it, so an exchange reported
        meanwhile can't keep it. A failed delete isn't retried: the conversation can't see a late deletion, so a retry
        could hit "item not found".
        """
        if response_id is None or response_id in self._responses.active:
            return []
        if (cut_point := self._audio.cut_point(response_id)) is None:
            return []
        unheard = [
            item_id
            for item_id in self._history.heard_messages_after(response_id, cut_point)
            if not self._audio.has_audio(item_id)
        ]
        for item_id in unheard:
            self._history.unheard(item_id)
        return unheard

    def _cut_unheard(self, truncations: Sequence[Truncation], unheard: Sequence[str] = ()) -> None:
        if truncations or unheard:
            self._spawn(self._truncate_unheard(truncations, unheard))

    async def _truncate_unheard(self, truncations: Sequence[Truncation], unheard: Sequence[str]) -> None:
        session = self._require_session()
        try:
            async with self._control_lock:
                if self._closing.is_set():
                    return
                cut_failure = await self._send_truncations(session, truncations)
                delete_failure = await self._delete_items(session, unheard)
        except RealtimeException as error:
            cut_failure, delete_failure = error, None
        if (failure := cut_failure or delete_failure) is not None:
            logger.warning("Couldn't cut what the caller didn't hear after a provider truncation: %s", failure)

    async def _send_truncations(
        self, session: RealtimeClientSessionProtocol[RealtimeOptionsT], truncations: Sequence[Truncation]
    ) -> RealtimeControlException | None:
        """A cut the provider rejects mustn't stop the cuts after it, so the first failure is returned at the end."""
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
                # The cut stays owed, so a later interrupt() retries it.
                first_failure = first_failure or failure
                continue
            self._audio.acknowledged(truncation)
        return first_failure

    async def _delete_items(
        self, session: RealtimeClientSessionProtocol[RealtimeOptionsT], item_ids: Sequence[str]
    ) -> RealtimeControlException | None:
        """A delete the provider rejects mustn't stop the deletes after it, so the first failure comes at the end."""
        first_failure: RealtimeControlException | None = None
        for item_id in item_ids:
            try:
                await self._acknowledged(session.delete_item(item_id), "delete a message the caller didn't hear")
            except RealtimeControlException as failure:
                first_failure = first_failure or failure
        return first_failure

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

        Closing stops new input and control calls at once. While the session is still open, it waits up to about 3
        seconds for caller transcripts still on their way. It then closes the session, cancels running tools with the
        ``conversation_closed`` reason, and streams an error result for each call that didn't finish; calls that had
        finished keep their results. It reports the remaining exchanges to the agent's context providers. Iteration
        ends once the queued updates are read. Closing again waits for the same close, and a cancelled caller doesn't
        cancel the close itself.

        Raises:
            Exception: The failure of a context provider that still couldn't record the conversation, after cleanup,
                or an ``ExceptionGroup`` when several couldn't.
        """
        await asyncio.shield(self._start_close())

    def _start_close(self) -> asyncio.Task[None]:
        """Input stops at once, synchronously, so nothing new starts while the close sequence runs."""
        if self._close_task is None:
            self._closing.set()
            self._tool_loop.stop()
            self._close_task = asyncio.create_task(self._close_sequence())
        return self._close_task

    async def _wait_for_caller_transcripts(self) -> None:
        """Transcription runs beside the reply, so a caller's last words can still be on their way when close starts."""
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(CALLER_TRANSCRIPT_WAIT_SECONDS):
                while self._history.waiting_for_caller_transcripts:
                    self._history_changed.clear()
                    await self._history_changed.wait()

    async def _close_sequence(self) -> None:
        """The updates end last, so iteration sees every settled call before it stops."""
        provider_failures: list[Exception] = []
        try:
            if self._receive_task is not None and not self._receive_task.done():
                await self._wait_for_caller_transcripts()
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
            self._history.stop_waiting_for_caller_transcripts()
            self._history.end_unfinished_responses()
            if self._reporter is not None:
                provider_failures = await self._reporter.report_remaining()
            await self._end_background_tasks()
        finally:
            self._updates.finish(self._failure)
        _raise_unrecorded_history(provider_failures)

    async def _end_background_tasks(self) -> None:
        """Nothing the conversation spawned may outlive it."""
        remaining = list(self._background_tasks)
        for task in remaining:
            task.cancel()
        if remaining:
            await asyncio.wait(remaining)


def _raise_unrecorded_history(failures: Sequence[Exception]) -> None:
    """The call's turns really happened, so a provider that couldn't record them must not fail silently."""
    if len(failures) == 1:
        raise failures[0]
    if failures:
        raise ExceptionGroup("Context providers couldn't record the conversation's history.", list(failures))


def unique_tools(tools: Sequence[ToolTypes]) -> list[ToolTypes]:
    """A realtime session declares its tools by name, so two tools can't share one."""
    return _append_unique_tools([], tools, duplicate_error_message="Tool names must be unique.")


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
