# Copyright (c) Microsoft. All rights reserved.

"""The live realtime conversation: one provider session, its updates and its control calls."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import copy
import logging
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Coroutine, Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import replace
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, Self, cast

from .._agents import append_tools_with_mcp_functions
from .._feature_stage import ExperimentalFeature, experimental
from .._mcp import MCPTool
from .._middleware import categorize_middleware
from .._sessions import AgentSession, HistoryProvider, SessionContext
from .._tools import normalize_tools
from .._types import AgentResponseUpdate, Content, _append_instructions, normalize_messages  # pyright: ignore[reportPrivateUsage]
from ..exceptions import (
    RealtimeActiveResponseException,
    RealtimeBackpressureException,
    RealtimeControlException,
    RealtimeConversationClosedException,
    RealtimeException,
)
from ._acknowledgements import AcknowledgedT, await_acknowledgement
from ._app_input import AppInput
from ._barge_in import AudioLedger, Truncation
from ._events import RealtimeEvent
from ._handoffs import CallSettings, handoff_completed, participant_update, start_settings
from ._history import CallHistory
from ._options import RealtimeOptionsT
from ._reporting import report_exchanges
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
    from ._telemetry import ConversationTelemetry


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
    applies their instructions, tools and function middleware, and replays the history they load into the new realtime
    session. Close reports each exchange, the input from the caller or the app with the reply and tool calls it drives,
    to their ``after_run`` in order. A call whose process ends before it closes never reaches them, so an app that
    needs each exchange stored as it happens records it from the updates.

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
        call: CallSettings,
        tools: Sequence[ToolTypes],
        invocation_configuration: FunctionInvocationConfiguration,
        function_middleware: Sequence[FunctionMiddlewareTypes],
        function_invocation_kwargs: Mapping[str, Any],
        mcp_connection_kwargs: Mapping[str, Any],
        mcp_exit_stack: AsyncExitStack,
        control_timeout: float,
        max_queued_updates: int,
        telemetry: ConversationTelemetry | None,
    ) -> None:
        self._owner = agent
        self._active_agent = agent._start_participant()  # pyright: ignore[reportPrivateUsage]
        self._initial_messages = initial_messages
        self._options = options
        self._call = call
        self._tools = tools
        self._mcp_connection_kwargs = mcp_connection_kwargs
        self._mcp_exit_stack = mcp_exit_stack
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
        self._app_input = AppInput()
        self._caller_quiet = asyncio.Event()
        self._caller_quiet.set()
        if agent_session is None and agent.context_providers:
            agent_session = AgentSession()
        self._tool_loop = ToolLoop(
            tools=list(tools),
            configuration=invocation_configuration,
            invocation_kwargs=function_invocation_kwargs,
            owner_middleware=function_middleware,
            call=call,
            agent_session=agent_session,
            conversation=self,
            owner=agent,
            responses=self._responses,
            emit=self._emit,
            app_input=self._app_input,
        )
        self._audio = AudioLedger()
        self._history = CallHistory()
        self._history_changed = asyncio.Event()
        self._provider_session = agent_session if agent_session is not None else AgentSession()
        self._start_context: SessionContext | None = None
        self._previous_event: RealtimeEvent | None = None
        self._telemetry = telemetry
        self._opened_context: contextvars.Context | None = None

    @property
    def active_agent(self) -> RawRealtimeAgent[RealtimeOptionsT]:
        """The agent whose instructions, tools and voice are live; a handoff changes it."""
        return self._active_agent

    @property
    def history(self) -> list[Message]:
        """A snapshot of the call's turns so far, as text messages in the provider's conversation order.

        It holds the caller's turns as their transcripts, the agent's turns, the text of app input as it was sent, and
        tool calls with the text of their results, and holds the final record after close. A cut-off turn keeps only
        what the caller heard, marked ``status="incomplete"``. A caller turn without text, because its transcript
        failed, isn't configured or hasn't arrived, has no contents and ``additional_properties["core"]["text_less"]``
        set to ``True``. The messages are copies: changing them, or a message after sending it, changes neither the
        history nor what context providers store.
        """
        return copy.deepcopy(self._history.messages())

    async def __aenter__(self) -> Self:
        if self._entered:
            raise RuntimeError("A realtime conversation can be entered only once; start a new one instead.")
        self._entered = True
        if self._closing.is_set():
            raise RealtimeConversationClosedException("The realtime conversation is closed.")
        with self._telemetry.opened() if self._telemetry is not None else contextlib.nullcontext():
            await self._open()
        return self

    async def _open(self) -> None:
        start_context = await self._prepare_from_context_providers()
        tools = await self._resolve_tools(self._tools)
        starting = asyncio.ensure_future(
            self._owner.client.start_session(options=self._options, tools=tools)  # type: ignore[arg-type]
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
        self._warn_if_history_lacks_transcription()
        self._tool_loop.on_tools_declared(tools)
        try:
            async with self._write_lock:
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
        self._opened_context = contextvars.copy_context()
        self._receive_task = asyncio.create_task(self._receive_loop(session))

    async def _resolve_tools(self, tools: Sequence[ToolTypes]) -> list[ToolTypes]:
        """The conversation keeps its own list of each MCP tool's functions, so server-side changes wait for an update.

        A tool connected here stays open for the starting agent's lifetime. One still connecting when close starts is
        abandoned, so a hung server can't hold up close, and the connection never joins an agent that has already
        exited. Without MCP tools nothing is awaited, so an update keeps its place among control calls.
        """
        _refuse_hosted_approvals(tools)
        resolving = append_tools_with_mcp_functions(
            [], tools, exit_stack=self._mcp_exit_stack, connection_kwargs=self._mcp_connection_kwargs
        )
        if not any(isinstance(tool, MCPTool) for tool in tools):
            return await resolving
        return await await_acknowledgement(
            resolving, action="connect its MCP tools", timeout=None, closing=self._closing
        )

    async def _prepare_from_context_providers(self) -> SessionContext:
        context = SessionContext(
            session_id=self._provider_session.session_id,
            input_messages=list(self._initial_messages),
            options=dict(self._options),
        )
        for provider in self._owner.context_providers:
            if isinstance(provider, HistoryProvider) and not provider.load_messages:
                continue
            await provider.before_run(
                # A realtime agent has no run(), which SupportsAgentRun requires; providers read its identity.
                agent=self._owner,  # type: ignore[arg-type]
                session=self._provider_session,
                context=context,
                state=self._provider_session.state.setdefault(provider.source_id, {}),
            )
        self._apply_start_context(context)
        self._start_context = context
        return context

    def _warn_if_history_lacks_transcription(self) -> None:
        """The session's settings, not the start options, say whether it transcribes: a provider can apply its own."""
        if not self._session_options().get("transcription") and any(
            isinstance(provider, HistoryProvider) for provider in self._owner.context_providers
        ):
            logger.warning(
                "The realtime conversation has a history provider but no transcription, so caller turns are stored "
                "without text. Set the transcription option to keep what the caller said."
            )

    def _apply_start_context(self, context: SessionContext) -> None:
        # Context providers prepare the call rather than its first speaker, so what they add holds across handoffs.
        options = dict(self._call.options)
        if context.instructions:
            options["instructions"] = _append_instructions(options.get("instructions"), "\n".join(context.instructions))
        provider_middleware = categorize_middleware(context.get_middleware(), supported_categories=("function",))
        self._call = replace(
            self._call,
            options=options,
            start_tools=[*self._call.start_tools, *normalize_tools(context.tools)],
            start_middleware=[*self._call.start_middleware, *provider_middleware["function"]],
        )
        self._options, tools = start_settings(self._active_agent, self._call)
        self._tools = tools
        self._tool_loop.use_call(self._call)

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
        if self._telemetry is not None:
            self._telemetry.on_event(event, self._active_agent)
        core = core_attributes(event)
        response_id = core.get("response_id", "")
        if event.kind == "response_created":
            self._responses.created(response_id, self._active_agent)
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
            self._history.on_function_call(event, self._responses.agent_of(response_id, default=self._active_agent))
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
            self._history.on_output_text(event, self._responses.agent_of(response_id, default=self._active_agent))
        self._history_changed.set()
        self._emit(
            update_from_event(
                event,
                author=self._responses.agent_of(response_id, default=self._active_agent),
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

        Function results and approval responses answer calls waiting for the app: a call to a tool that needs approval,
        a declaration-only tool, or a tool that raised ``UserInputRequiredException``. Each goes to its call's batch,
        which sends the results and requests the follow-up once every call in it has one, so an answer starts no tool
        chain and requests no response. A result names its call by the occurrence ``id`` the call streamed with, or by
        its ``call_id``. The app can answer a declaration-only tool's call as soon as it streams. An approval runs the
        call as the conversation received it, whatever call the response embeds. Other input sent alongside answers is
        handled as above.

        Args:
            value: The input, or a raw provider command.

        Keyword Args:
            create_response: Whether to request a response after adding text or message input.

        Raises:
            ValueError: If the input holds a function result or approval response that answers nothing the
                conversation is waiting for; nothing is sent.
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
        answers = [content for message in messages for content in message.contents if _is_an_answer(content)]
        if answers:
            self._app_input.route(answers)
            messages = _without_answers(messages)
            if not messages:
                return
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

    def _report_tool_result(self, result: Content, response_id: str) -> None:
        self._history.on_tool_result(result)
        author = self._responses.agent_of(response_id, default=self._active_agent)
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

        An MCP tool in ``tools`` is declared as the functions its server offers now, which is how a change to the
        server's tools reaches the call. A tool that isn't connected connects first, before the update takes its turn
        among the control calls, so a slow server doesn't hold up ``interrupt()``; it then stays open for the agent's
        lifetime.

        Keyword Args:
            options: The settings to change.
            tools: The tools to use instead of the current ones, or ``None`` to keep them.

        Raises:
            ValueError: If two tools share a name, including an MCP tool's functions, or a hosted MCP tool doesn't set
                ``require_approval`` to ``"never"``; nothing is sent.
            ToolException: If an MCP tool fails to connect; nothing is sent.
            RealtimeConversationClosedException: If the conversation has started closing.
            RealtimeControlTimeoutException: If the provider doesn't acknowledge the update in time; it may still apply
                it, and the conversation keeps running its current tools.
            RealtimeControlException: If the provider rejects the update; nothing changes.
        """
        self._require_open_session()
        declared = None if tools is None else await self._resolve_tools(normalize_tools(tools))
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

    async def handoff(
        self,
        agent: RawRealtimeAgent[RealtimeOptionsT],
        *,
        tools: ToolTypes | Callable[..., Any] | Sequence[ToolTypes | Callable[..., Any]] | None = None,
    ) -> None:
        """Make ``agent`` the agent speaking in this call, in the same realtime session, without reconnecting.

        The session's instructions, tools, voice and tool choice become ``agent``'s, computed as the conversation
        started them, with ``agent`` in place of the starting agent:

        - instructions are ``agent``'s, followed by the call-wide instructions: the owner's when it doesn't speak
          itself, such as a handoff agent's, then the start options' and the context providers';
        - tools are ``agent``'s, the tools its owner adds for it, such as handoff tools, the start tools, the context
          providers' tools and ``tools``. An MCP tool is declared as the functions its server offers now; one that
          isn't connected connects before the handoff takes its turn among the control calls, and stays open until
          the agent that started the conversation exits;
        - a voice or tool choice in the start options holds for the call; otherwise ``agent``'s applies, then the
          owner's default. Tool choice falls back to ``"auto"``. With no voice to send, the current voice stays.

        Everything else stays as the call started: history, context providers, tool-loop limits and other settings.
        Calls that arrive afterwards run through ``agent``'s function middleware, inside the owner's when it doesn't
        speak itself and outside the start middleware and the context providers', and responses created afterwards
        are credited to ``agent``. Once the provider acknowledges the update, a ``handoff_completed`` update streams
        before this returns. Voice Live rejects a voice change between an Azure voice and an OpenAI voice, which
        fails the handoff.

        Args:
            agent: The agent to hand the call to. It must use the conversation's client instance.

        Keyword Args:
            tools: More tools for ``agent``, until the next handoff.

        Raises:
            ValueError: If ``agent`` uses another client instance or is outside the owner's handoff topology, two
                tools share a name, including an MCP tool's functions, or a hosted MCP tool doesn't set
                ``require_approval`` to ``"never"``; nothing is sent.
            MiddlewareException: If ``agent``'s middleware includes a bundle with middleware a realtime conversation
                can't run, such as agent middleware; nothing is sent.
            ToolException: If an MCP tool fails to connect; nothing is sent.
            RealtimeConversationClosedException: If the conversation has started closing.
            RealtimeControlTimeoutException: If the provider doesn't acknowledge the update in time; it may still apply
                it, and the previous agent stays active.
            RealtimeControlException: If the provider rejects the update; the previous agent stays active.
        """
        if agent.client is not self._owner.client:
            raise ValueError("A handoff target must use the conversation's client instance.")
        # The owner adds the target's tools here, and raises ValueError for an agent outside its topology.
        options, declared = participant_update(agent, self._call, normalize_tools(tools))
        # Sorting the target's middleware now refuses a bundle the conversation can't run before anything changes.
        self._tool_loop.pipeline_for(agent)
        self._require_open_session()
        declared = await self._resolve_tools(declared)
        async with self._control_lock:
            await self._update_session_locked(options, declared)
            self._tool_loop.on_tools_declared(declared)
            source, self._active_agent = self._active_agent, agent
            self._emit(update_from_event(handoff_completed(source, agent), author=agent))

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
        finished keep their results. It then reports the call to the agent's context providers, one ``after_run`` per
        exchange in order, with providers in reverse registration order for each. A provider whose ``after_run`` fails
        gets none of the later exchanges, while the others carry on. Iteration ends once the queued updates are read.
        Closing again waits for the same close, and a cancelled caller doesn't cancel the close itself.

        Raises:
            Exception: The failure of a context provider that couldn't record the call, after cleanup, or an
                ``ExceptionGroup`` when several couldn't.
        """
        await asyncio.shield(self._start_close())

    def _start_close(self) -> asyncio.Task[None]:
        """Input stops at once, synchronously, so nothing new starts while the close sequence runs."""
        if self._close_task is None:
            self._closing.set()
            self._tool_loop.stop()
            # Close runs where the conversation opened, whichever task asked for it, so what it reports nests there.
            context = self._opened_context.copy() if self._opened_context is not None else None
            self._close_task = asyncio.create_task(self._close_sequence(), context=context)
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
            if self._start_context is not None and self._owner.context_providers:
                provider_failures = await report_exchanges(
                    self._owner, self._provider_session, self._start_context, self._history.exchanges()
                )
            await self._end_background_tasks()
        finally:
            unrecorded = _unrecorded_history(provider_failures)
            if self._telemetry is not None:
                self._telemetry.closed(self._failure or unrecorded)
            self._updates.finish(self._failure)
        if unrecorded is not None:
            raise unrecorded

    async def _end_background_tasks(self) -> None:
        """Nothing the conversation spawned may outlive it."""
        remaining = list(self._background_tasks)
        for task in remaining:
            task.cancel()
        if remaining:
            await asyncio.wait(remaining)


def _unrecorded_history(failures: Sequence[Exception]) -> Exception | None:
    if len(failures) == 1:
        return failures[0]
    if failures:
        return ExceptionGroup("Context providers couldn't record the conversation's history.", list(failures))
    return None


async def _close_after_failed_entry(session: RealtimeClientSessionProtocol[Any]) -> None:
    """The failure that stopped entry wins over a failure to close."""
    try:
        await session.close()
    except Exception as error:
        logger.warning("Closing the realtime session after entry failed also failed: %s", error)


def _refuse_hosted_approvals(tools: Sequence[ToolTypes]) -> None:
    """Nothing answers a hosted tool's approval request yet, so its call would wait forever."""
    for tool in tools:
        if not isinstance(tool, Mapping):
            continue
        declaration = cast(Mapping[str, Any], tool)
        if declaration.get("type") == "mcp" and declaration.get("require_approval") != "never":
            raise ValueError(
                f"The hosted MCP tool {declaration.get('server_label')!r} may ask for approval, which realtime "
                "agents don't support yet. Set its require_approval to 'never'."
            )


def _is_an_answer(content: Content) -> bool:
    return content.type in ("function_result", "function_approval_response")


def _without_answers(messages: Sequence[Message]) -> list[Message]:
    remaining: list[Message] = []
    for message in messages:
        contents = [content for content in message.contents if not _is_an_answer(content)]
        if contents:
            remaining.append(copy.copy(message))
            remaining[-1].contents = contents
    return remaining


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
