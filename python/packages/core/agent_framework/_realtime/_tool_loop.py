# Copyright (c) Microsoft. All rights reserved.

"""The realtime function-tool loop: each response's calls run as one batch once it finishes."""

from __future__ import annotations

import asyncio
import copy
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from itertools import groupby
from operator import attrgetter
from typing import TYPE_CHECKING, Any, cast

from .._middleware import FunctionMiddlewarePipeline, categorize_middleware
from .._tools import (
    _CONVERSATION_CLOSED_REASON,  # pyright: ignore[reportPrivateUsage]
    DEFAULT_MAX_CONSECUTIVE_ERRORS_PER_REQUEST,
    DEFAULT_MAX_ITERATIONS,
    FunctionTool,
    _execute_function_calls,  # pyright: ignore[reportPrivateUsage]
    _generate_function_call_occurrence_id,  # pyright: ignore[reportPrivateUsage]
    _get_tool_map,  # pyright: ignore[reportPrivateUsage]
    approval_rejection_result,
)
from .._types import Content, Message
from ..exceptions import (
    RealtimeActiveResponseException,
    RealtimeControlException,
    RealtimeControlTimeoutException,
    RealtimeConversationClosedException,
    RealtimeException,
    ToolException,
    ToolExecutionException,
)
from ._events import RealtimeEvent
from ._updates import app_request_update, core_attributes

logger = logging.getLogger("agent_framework")

# Set while a batch runs and copied into each call's task, so filler a tool requests can't count as a follow-up.
_CALLING_CONVERSATION: ContextVar[object | None] = ContextVar("realtime_calling_conversation", default=None)

if TYPE_CHECKING:
    from .._agents import BaseAgent
    from .._middleware import FunctionMiddlewareTypes
    from .._sessions import AgentSession
    from .._tools import FunctionInvocationConfiguration, ToolTypes
    from .._types import AgentResponseUpdate
    from ._agent import RawRealtimeAgent
    from ._app_input import AppInput
    from ._conversation import RealtimeConversation
    from ._handoffs import CallSettings
    from ._responses import ResponseLedger


@dataclass
class _Call:
    """Each call keeps the tool and middleware it resolved to when it arrived, even if the agent or tools change."""

    content: Content
    response_id: str
    tool: FunctionTool | None
    middleware_pipeline: FunctionMiddlewarePipeline
    result: Content | None = None
    reported: bool = False
    approval: Content | None = None
    decision: asyncio.Future[bool] | None = None
    app_result: asyncio.Future[Content] | None = None
    input_requests: list[Content] = field(default_factory=list[Content])

    @property
    def executed_as(self) -> Content:
        """An approved call runs as its approval, so the executor knows the app allowed it."""
        return self.approval or self.content


class _ToolChain:
    """The responses and calls driven by one caller turn or app input, which the loop's limits count.

    It applies chat's ``FunctionInvocationConfiguration`` limits per chain: ``max_iterations`` counts follow-ups,
    the responses after the chain's first results whoever requested them, and the other limits count across the
    chain's batches.
    """

    def __init__(self, configuration: FunctionInvocationConfiguration) -> None:
        self._configuration = configuration
        self.first_results_marker: int | None = None
        self._calls = 0
        self._error_batches = 0
        self._started_at: float | None = None
        self.tripped = False

    def batch_started(self) -> None:
        if self._started_at is None:
            self._started_at = time.monotonic()

    def count(self, executed_calls: int, *, had_errors: bool) -> None:
        self._calls += executed_calls
        self._error_batches = self._error_batches + 1 if had_errors else 0
        max_function_calls = self._configuration.get("max_function_calls")
        max_duration = self._configuration.get("max_duration_seconds")
        max_error_batches = self._configuration.get(
            "max_consecutive_errors_per_request", DEFAULT_MAX_CONSECUTIVE_ERRORS_PER_REQUEST
        )
        elapsed = time.monotonic() - self._started_at if self._started_at is not None else 0.0
        if max_function_calls is not None and self._calls >= max_function_calls:
            self.tripped = True
        if had_errors and self._error_batches >= max_error_batches:
            self.tripped = True
        if max_duration is not None and elapsed >= max_duration:
            self.tripped = True

    @property
    def max_iterations(self) -> int:
        return self._configuration.get("max_iterations", DEFAULT_MAX_ITERATIONS)


class ToolLoop:
    """Tools run off the receive task, so control calls they make, which await acknowledgements, cannot deadlock it."""

    def __init__(
        self,
        *,
        tools: list[ToolTypes],
        configuration: FunctionInvocationConfiguration,
        invocation_kwargs: Mapping[str, Any],
        owner_middleware: Sequence[FunctionMiddlewareTypes],
        call: CallSettings,
        agent_session: AgentSession | None,
        conversation: RealtimeConversation[Any],
        owner: RawRealtimeAgent[Any],
        responses: ResponseLedger,
        emit: Callable[[AgentResponseUpdate], None],
        app_input: AppInput,
    ) -> None:
        self._tools = tools
        self._declared: list[ToolTypes] = []
        self._configuration = configuration
        self._invocation_kwargs = invocation_kwargs
        self._call = call
        self._agent_session = agent_session
        self._own_middleware: dict[BaseAgent, Sequence[FunctionMiddlewareTypes]] = {}
        self._pipelines: dict[BaseAgent, FunctionMiddlewarePipeline] = {}
        if owner._start_participant() is owner:  # pyright: ignore[reportPrivateUsage]
            self._own_middleware[owner] = owner_middleware
        self._conversation = conversation
        self._responses = responses
        self._emit = emit
        self._app_input = app_input
        self._results_marker = 0
        self._follow_up_task: asyncio.Task[Any] | None = None
        # Each batch's own chain is kept, so a merged follow-up respects every chain's limits.
        self._chains_due: list[_ToolChain] = []
        self._claimed = 0
        self._chain = _ToolChain(configuration)
        self._running_batches: dict[asyncio.Task[Any], list[_Call]] = {}
        self._calls_left_for_close: list[_Call] = []
        self._closing = False
        self._calls_by_response: dict[str, list[_Call]] = {}

    def use_call(self, call: CallSettings) -> None:
        """Context providers add to the call's settings once they've run, before any call arrives."""
        self._call = call
        self._pipelines.clear()

    def on_function_call(self, event: RealtimeEvent) -> None:
        """A call is bound to the tool that existed when the model made it.

        Its occurrence id pairs it with its result even when a provider reuses call ids.
        """
        if event.content is None:
            return
        if event.content.id is None:
            event.content.id = _generate_function_call_occurrence_id()
        response_id = core_attributes(event)["response_id"]
        tool = _get_tool_map(self._tools).get(event.content.name or "")
        pipeline = self.pipeline_for(self._conversation.active_agent)
        call = _Call(event.content, response_id, tool, pipeline)
        if tool is not None and self._answered_by_the_app(tool):
            # The app can answer as soon as it sees the call, before its response finishes.
            call.app_result = self._app_input.result(event.content)
        self._calls_by_response.setdefault(response_id, []).append(call)

    def pipeline_for(self, agent: BaseAgent) -> FunctionMiddlewarePipeline:
        """The host's function middleware wraps the agent's own, which wraps the conversation's.

        Raises:
            MiddlewareException: If the agent's middleware includes a bundle with middleware a realtime conversation
                can't run.
        """
        if (pipeline := self._pipelines.get(agent)) is None:
            if (own := self._own_middleware.get(agent)) is None:
                own = categorize_middleware(agent.middleware, supported_categories=("function",))["function"]
                self._own_middleware[agent] = own
            middleware = [*self._call.host_middleware, *own, *self._call.start_middleware]
            if self._agent_session is None:
                self._agent_session = _session_for_stateful_middleware(middleware)
            pipeline = self._pipelines[agent] = FunctionMiddlewarePipeline(*middleware)
        return pipeline

    def _answered_by_the_app(self, tool: FunctionTool) -> bool:
        app_answered = {additional.name for additional in self._configuration.get("additional_tools") or ()}
        return tool.declaration_only or tool.name in app_answered

    def on_tools_declared(self, tools: Sequence[ToolTypes]) -> None:
        """The provider declares exactly ``tools`` now, at start or after an acknowledged update.

        The executable list changes in place, so a call still running changes it through ``ctx.add_tools()``.
        """
        self._tools[:] = tools
        self._declared = list(tools)

    def on_new_input(self) -> None:
        """Each caller turn or app input is a new request, so the limits count afresh.

        Batches still running count toward their own chain.
        """
        self._chain = _ToolChain(self._configuration)

    def on_response_done(self, response_id: str, status: str) -> None:
        if self._closing:
            return  # close() settles these calls
        calls = self._calls_by_response.pop(response_id, [])
        if not calls:
            return
        if status == "completed":
            # The chain is the one current as the response finished, before later input can start another.
            batch = self._conversation._spawn(  # pyright: ignore[reportPrivateUsage]
                self._run_batch(calls, self._chain)
            )
            self._running_batches[batch] = calls
            batch.add_done_callback(self._forget_batch)
            return
        for call in calls:
            call.result = _settled_with_error(call.content, f"the response ended {status} before the tool ran", status)
        self._conversation._spawn(self._send_unrun_results(calls))  # pyright: ignore[reportPrivateUsage]

    def _forget_batch(self, batch: asyncio.Task[Any]) -> None:
        self._running_batches.pop(batch, None)

    def stop(self) -> None:
        """Called as close starts, so nothing new runs while the session closes.

        The follow-up is cancelled only once, so a second cancel can't cut short its control call's cleanup.
        """
        if self._closing:
            return
        self._closing = True
        if self._follow_up_task is not None:
            self._follow_up_task.cancel()

    async def close(self) -> None:
        """Each running batch is cancelled once, with the ``conversation_closed`` reason.

        The executor forwards the reason to the batch's calls, so their ``execute_tool`` spans record it. Settled
        results stream but aren't sent.
        """
        self.stop()
        if self._follow_up_task is not None:
            await asyncio.wait({self._follow_up_task})
        running = dict(self._running_batches)
        for batch in running:
            batch.cancel(_CONVERSATION_CLOSED_REASON)
        await asyncio.gather(*running, return_exceptions=True)
        unfinished = [*self._calls_left_for_close, *(call for calls in running.values() for call in calls)]
        self._calls_left_for_close.clear()
        unfinished += [call for calls in self._calls_by_response.values() for call in calls]
        self._calls_by_response.clear()
        for call in unfinished:
            if call.result is None:
                # The app may have answered while the batch still waited on something else.
                call.result = (
                    _accepted_app_result(call)
                    or _rejection_sent(call)
                    or _settled_with_error(
                        call.content, "the conversation closed before the tool finished", _CONVERSATION_CLOSED_REASON
                    )
                )
            self._report(call)

    def _report(self, call: _Call) -> None:
        if call.app_result is not None:
            # Settled without the app, so a late answer for it answers nothing.
            self._app_input.withdraw(call.content.id or "")
        if call.result is not None and not call.reported:
            call.reported = True
            self._conversation._report_tool_result(call.result, call.response_id)  # pyright: ignore[reportPrivateUsage]

    async def _send_unrun_results(self, calls: list[_Call]) -> None:
        """Best effort, with no follow-up: the response that made these calls ended without completing."""
        results = [call.result for call in calls if call.result is not None]
        for call in calls:
            self._report(call)
        try:
            await self._conversation._send_tool_results(Message("tool", results))  # pyright: ignore[reportPrivateUsage]
        except RealtimeException as error:
            logger.info("Couldn't send the results of calls that never ran: %s", error)

    async def _run_batch(self, calls: list[_Call], chain: _ToolChain) -> None:
        try:
            await self._run_batch_steps(calls, chain)
        except Exception as error:
            # Close settles this batch's calls. A provider error once close has started, such as a closing socket
            # refusing the results, comes from closing, so it doesn't fail the conversation. MiddlewareFailure, the
            # executor's fail-closed signal, and any other broken batch do.
            self._calls_left_for_close.extend(calls)
            if self._closing and isinstance(error, RealtimeException):
                logger.debug("A tool batch couldn't finish as the realtime conversation closed: %s", error)
                return
            self._conversation._fail(error)  # pyright: ignore[reportPrivateUsage]
        except asyncio.CancelledError as cancellation:
            # Close settles the calls, whatever cancelled the batch; a call close already settles is reported once.
            self._calls_left_for_close.extend(calls)
            if not self._closing:
                # A tool or middleware raised the cancellation, or something outside the conversation cancelled the
                # batch. Either way the conversation can't finish it, so it ends.
                failure = ToolExecutionException("A tool batch was cancelled while the realtime conversation was open.")
                failure.__cause__ = cancellation
                self._conversation._fail(failure)  # pyright: ignore[reportPrivateUsage]
            raise

    async def _run_batch_steps(self, calls: list[_Call], chain: _ToolChain) -> None:
        # A filler response can be created, and finish, before its request returns and marks it as filler, so the
        # follow-up count waits for requests in flight.
        await self._responses.wait_until_requests_settle()
        # Started before any wait for the app, so the duration limit counts approval waits.
        chain.batch_started()
        chain_ends = chain.tripped or self._follow_ups(chain) >= chain.max_iterations
        _bind_namesakes_to_the_first(calls)
        invocation_disabled = not self._configuration.get("enabled", True)
        if invocation_disabled:
            chain.tripped = True  # whatever the calls were, the follow-up turns tools off
        awaiting_approval: list[_Call] = []
        awaiting_result: list[_Call] = []
        for call in calls:
            if call.tool is None:
                call.result = _not_found_result(call.content)
            elif invocation_disabled:
                call.result = _settled_with_error(call.content, "function invocation is disabled", "disabled")
            elif chain_ends:
                call.result = _settled_with_error(
                    call.content, "this tool chain reached its limit, so the tool didn't run", "limit_reached"
                )
            elif call.tool.approval_mode == "always_require":
                awaiting_approval.append(call)
            elif call.app_result is not None:
                awaiting_result.append(call)
        await self._await_approvals(awaiting_approval)
        # An approved call to a tool the app runs still needs the app's result, which it may already have sent.
        awaiting_result += [call for call in awaiting_approval if call.result is None and call.app_result is not None]
        runnable = [call for call in calls if call.result is None and call not in awaiting_result]
        terminated, executed = await self._execute(runnable)
        for call in runnable:
            if call.input_requests:
                self._request_input(call)
                awaiting_result.append(call)
        await self._await_results(awaiting_result)
        # Count synthetic errors too, so unknown calls can't evade the consecutive-error limit.
        chain.count(
            executed,
            had_errors=any(call.result is not None and call.result.exception is not None for call in calls),
        )
        terminated = terminated or chain_ends
        results = [call.result for call in calls if call.result is not None]
        await self._declare_tool_changes()
        await self._conversation._send_tool_results(Message("tool", results))  # pyright: ignore[reportPrivateUsage]
        self._results_marker = self._responses.created_count
        if chain.first_results_marker is None:
            chain.first_results_marker = self._results_marker
        for call in calls:
            self._report(call)
        if not terminated:
            self._follow_up_due(chain)

    async def _await_approvals(self, calls: Sequence[_Call]) -> None:
        decisions = await asyncio.gather(*(self._request_approval(call) for call in calls))
        for call, approved in zip(calls, decisions, strict=True):
            if approved:
                call.approval = Content.from_function_approval_response(
                    approved=True, id=call.content.id or "", function_call=call.content
                )
            else:
                call.result = approval_rejection_result(call.content, id=call.content.id)

    async def _await_results(self, calls: Sequence[_Call]) -> None:
        await asyncio.gather(*(call.app_result for call in calls if call.app_result is not None))
        for call in calls:
            call.result = _accepted_app_result(call)

    def _request_approval(self, call: _Call) -> asyncio.Future[bool]:
        """The request carries the call's occurrence id, which the app's answer must name."""
        request = Content.from_function_approval_request(id=call.content.id or "", function_call=call.content)
        call.decision = self._app_input.approval(request.id or "")
        self._ask_the_app(call, [request])
        return call.decision

    def _request_input(self, call: _Call) -> None:
        """A tool that raised UserInputRequiredException is answered with a result for its call."""
        call.app_result = self._app_input.result(call.content)
        self._ask_the_app(call, call.input_requests)

    def _ask_the_app(self, call: _Call, requests: list[Content]) -> None:
        author = self._responses.agent_of(call.response_id, default=self._conversation.active_agent)
        self._emit(app_request_update(requests, response_id=call.response_id, author=author))

    async def _declare_tool_changes(self) -> None:
        """Tools a call added or removed must reach the provider before the results the model acts on.

        They're prepared as ``update_session(tools=...)`` prepares the app's, before taking a turn, so a slow MCP
        server never holds up other control calls. Results never wait behind another control call unless there's a
        change to declare. A list that changed while it was prepared is prepared again, so an app's update queued
        ahead of it is never undone by a stale copy.
        """
        while _differ(self._declared, self._tools):
            proposed = list(self._tools)
            try:
                declaring = await self._conversation._resolve_tools(proposed)  # pyright: ignore[reportPrivateUsage]
            except (ValueError, ToolException) as refusal:
                logger.warning(
                    "The tool changes a tool call made can't be declared, so the conversation keeps the tools it "
                    "declared before: %s",
                    refusal,
                )
                self.on_tools_declared(self._declared)
                return
            async with self._conversation._control_lock:  # pyright: ignore[reportPrivateUsage]
                if _differ(proposed, self._tools):
                    continue
                # The live list runs what's declared, such as an MCP tool's functions, and stays the list tools change.
                self._tools[:] = declaring
                try:
                    await self._conversation._update_session_locked({}, declaring)  # pyright: ignore[reportPrivateUsage]
                except RealtimeControlTimeoutException as timeout:
                    # The provider may still apply it, so the change stays and the next batch that finishes resends it.
                    logger.warning("The tool changes a tool call made may not have reached the provider: %s", timeout)
                except RealtimeControlException as rejection:
                    logger.warning(
                        "The provider rejected the tool changes a tool call made, so the conversation keeps the tools "
                        "it declared before: %s",
                        rejection,
                    )
                    self.on_tools_declared(self._declared)
                else:
                    # Another batch may have changed the live list while this update waited; that change goes out next.
                    self._declared = declaring
                return

    def _follow_up_due(self, chain: _ToolChain) -> None:
        """Follow-ups due together merge into one request."""
        if self._closing:
            return
        self._chains_due.append(chain)
        if self._follow_up_task is None or self._follow_up_task.done():
            self._follow_up_task = self._conversation._spawn(self._follow_up())  # pyright: ignore[reportPrivateUsage]

    def _follow_ups(self, chain: _ToolChain) -> int:
        """The provider's own responses use up ``max_iterations`` too.

        Any response after the chain's first results counts, whoever requested it, apart from filler.
        """
        if chain.first_results_marker is None:
            return 0
        return self._responses.responses_since(chain.first_results_marker)

    def _follow_up_options(self, chains: Sequence[_ToolChain]) -> dict[str, Any]:
        """A chain gets at most ``max_iterations`` responses with tools on, as in chat's loop.

        A merged follow-up turns tools off if any of its chains needs that. Otherwise a required tool choice relaxes to
        ``"auto"``, as in chat.
        """
        if any(chain.tripped or self._follow_ups(chain) + 1 >= chain.max_iterations for chain in chains):
            return {"tool_choice": "none"}
        tool_choice = self._conversation._session_options().get("tool_choice")  # pyright: ignore[reportPrivateUsage]
        if _requires_a_tool(tool_choice):
            return {"tool_choice": "auto"}
        return {}

    def _claim_follow_up(self) -> dict[str, Any] | None:
        """Called under the control lock, so filler is classified before the claim.

        Returns ``None`` once a response created after the results has answered them, and ``{}`` when the follow-up
        needs no per-response options.
        """
        if self._responses.answered_since(self._results_marker):
            self._chains_due.clear()
            return None
        self._claimed = len(self._chains_due)
        return self._follow_up_options(self._chains_due)

    async def _follow_up(self) -> None:
        while True:
            try:
                response_id = await self._conversation._request_follow_up(  # pyright: ignore[reportPrivateUsage]
                    self._claim_follow_up
                )
            except RealtimeActiveResponseException:
                continue  # another response won the race; it counts if it can act on the results
            except RealtimeConversationClosedException:
                return
            except RealtimeException as error:
                self._conversation._report_error(error)  # pyright: ignore[reportPrivateUsage]
                del self._chains_due[: self._claimed]
                if not self._chains_due:
                    return
                continue  # results went out after the failed request, and are still owed a follow-up
            if response_id is None:
                return  # a response created after the results already saw them, so it is the follow-up
            del self._chains_due[: self._claimed]
            if not self._chains_due:
                return
            # Results went out after the request did. The provider handles client events in order and starts a
            # response from the conversation as it stood, so this response can't act on them, however late its
            # response.created arrives here. Request another.
            self._responses.answers_only_earlier_results(response_id)

    async def _execute(self, calls: Sequence[_Call]) -> tuple[bool, int]:
        """Return whether function middleware ended the loop, and how many calls ran.

        A response's calls only mix agents when the app hands off while they're arriving, so each agent's calls run in
        turn.
        """
        if not calls:
            return False, 0
        terminated, executed = False, 0
        requested_by_tools = _CALLING_CONVERSATION.set(self._conversation)
        try:
            for pipeline, same_agent in groupby(calls, key=attrgetter("middleware_pipeline")):
                batch = await self._run_executor(list(same_agent), pipeline)
                terminated = terminated or batch.should_terminate
                executed += batch.executed_call_count
        except Exception:
            # The executor discards a batch that fails closed, so calls that finished first mustn't keep their results.
            for call in calls:
                call.result = None
            raise
        finally:
            _CALLING_CONVERSATION.reset(requested_by_tools)
        for call in calls:
            if call.result is None and not call.input_requests:
                call.result = _settled_with_error(
                    call.content, "the call paused for an answer it can't get", "unsupported"
                )
        return terminated, executed

    async def _run_executor(self, calls: Sequence[_Call], middleware_pipeline: FunctionMiddlewarePipeline) -> Any:
        return await _execute_function_calls(
            custom_args=dict(self._invocation_kwargs),
            function_calls=[call.executed_as for call in calls],
            # Keep the arrival-time bindings apart from the live list that tools mutate.
            options={"tools": [call.tool for call in calls]},
            live_tools=self._tools,
            config=self._configuration,
            invocation_session=self._agent_session,
            middleware_pipeline=middleware_pipeline,
            realtime_conversation=self._conversation,
            on_call_finished=_keep_results_as_they_finish(calls),
        )


def _keep_results_as_they_finish(calls: Sequence[_Call]) -> Callable[[Content, list[Content]], None]:
    by_content = {id(call.executed_as): call for call in calls}

    def keep(function_call: Content, results: list[Content]) -> None:
        call = by_content[id(function_call)]
        call.result = next((content for content in results if content.type == "function_result"), call.result)
        # Middleware's approval requests can't be resumed here; the call settles with an error instead.
        if call.result is None and not any(content.type == "function_approval_request" for content in results):
            call.input_requests = results

    return keep


def _accepted_app_result(call: _Call) -> Content | None:
    if call.app_result is None or not call.app_result.done() or call.app_result.cancelled():
        return None
    if call.decision is not None and _decided(call.decision) is not True:
        # An early result for a call that needs approval stands only once the app approves the call.
        return None
    # The result carries the call's occurrence id, as a result the loop produced would.
    result = copy.copy(call.app_result.result())
    result.id = call.content.id
    return result


def _rejection_sent(call: _Call) -> Content | None:
    if call.decision is None or _decided(call.decision) is not False:
        return None
    return approval_rejection_result(call.content, id=call.content.id)


def _decided(decision: asyncio.Future[bool]) -> bool | None:
    return decision.result() if decision.done() and not decision.cancelled() else None


def _session_for_stateful_middleware(function_middleware: Sequence[FunctionMiddlewareTypes]) -> AgentSession | None:
    """Separate conversations on one agent must never share such middleware's state, as in chat."""
    if any(getattr(middleware, "_requires_session_state", False) is True for middleware in function_middleware):
        from .._sessions import AgentSession

        return AgentSession()
    return None


def _differ(tools: Sequence[ToolTypes], others: Sequence[ToolTypes]) -> bool:
    return len(tools) != len(others) or any(tool is not other for tool, other in zip(tools, others, strict=True))


def _bind_namesakes_to_the_first(calls: Sequence[_Call]) -> None:
    """The executor maps tools by name, so a tool replaced by a namesake mid-response can't run beside it."""
    first_bindings: dict[str, FunctionTool] = {}
    for call in calls:
        if call.tool is not None:
            call.tool = first_bindings.setdefault(call.tool.name, call.tool)


def _requires_a_tool(tool_choice: Any) -> bool:
    if tool_choice == "required":
        return True
    return isinstance(tool_choice, Mapping) and cast("Mapping[str, Any]", tool_choice).get("mode") == "required"


def requested_by_a_tool(conversation: RealtimeConversation[Any]) -> bool:
    """A response requested from inside a batch is filler, which never answers results."""
    return _CALLING_CONVERSATION.get() is conversation


def _not_found_result(call: Content) -> Content:
    return Content.from_function_result(
        call_id=call.call_id or "",
        result=f'Error: Requested function "{call.name}" not found.',
        exception=str(KeyError(f'Function "{call.name}" not found.')),
        id=call.id,
    )


def _settled_with_error(call: Content, why: str, reason: str) -> Content:
    """``reason`` becomes the result's ``exception``, which says why the call didn't finish."""
    return Content.from_function_result(
        call_id=call.call_id or "", result=f"Error: {why}.", exception=reason, id=call.id
    )
