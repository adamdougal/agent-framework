# Copyright (c) Microsoft. All rights reserved.

"""The realtime function-tool loop: each response's calls run as one batch once it finishes."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from .._middleware import FunctionMiddlewarePipeline
from .._tools import (
    _CONVERSATION_CLOSED_REASON,  # pyright: ignore[reportPrivateUsage]
    DEFAULT_MAX_CONSECUTIVE_ERRORS_PER_REQUEST,
    DEFAULT_MAX_ITERATIONS,
    FunctionTool,
    _execute_function_calls,  # pyright: ignore[reportPrivateUsage]
    _generate_function_call_occurrence_id,  # pyright: ignore[reportPrivateUsage]
    _get_tool_map,  # pyright: ignore[reportPrivateUsage]
)
from .._types import Content, Message
from ..exceptions import (
    RealtimeActiveResponseException,
    RealtimeConversationClosedException,
    RealtimeException,
    ToolExecutionException,
)
from ._events import RealtimeEvent
from ._updates import core_attributes

logger = logging.getLogger("agent_framework")

# Set while a batch runs and copied into each call's task, so filler a tool requests can't count as a follow-up.
_CALLING_CONVERSATION: ContextVar[object | None] = ContextVar("realtime_calling_conversation", default=None)

if TYPE_CHECKING:
    from .._middleware import FunctionMiddlewareTypes
    from .._sessions import AgentSession
    from .._tools import FunctionInvocationConfiguration, ToolTypes
    from ._conversation import RealtimeConversation
    from ._responses import ResponseLedger


@dataclass
class _Call:
    """Each call keeps the tool it resolved to when it arrived, even if the tools change before it runs."""

    content: Content
    response_id: str
    tool: FunctionTool | None
    result: Content | None = None
    reported: bool = False


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
        function_middleware: Sequence[FunctionMiddlewareTypes],
        agent_session: AgentSession | None,
        conversation: RealtimeConversation[Any],
        responses: ResponseLedger,
    ) -> None:
        self._tools = tools
        self._configuration = configuration
        self._invocation_kwargs = invocation_kwargs
        self._middleware_pipeline = FunctionMiddlewarePipeline(*function_middleware)
        self._agent_session = (
            agent_session if agent_session is not None else _session_for_stateful_middleware(function_middleware)
        )
        self._conversation = conversation
        self._responses = responses
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

    def add_tools(self, tools: Sequence[ToolTypes]) -> None:
        self._tools.extend(tools)

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
        self._calls_by_response.setdefault(response_id, []).append(_Call(event.content, response_id, tool))

    def _would_pause(self, tool: FunctionTool) -> bool:
        app_answered = {additional.name for additional in self._configuration.get("additional_tools") or ()}
        return tool.approval_mode == "always_require" or tool.declaration_only or tool.name in app_answered

    def _warn_about_pausing_tool(self, tool: FunctionTool) -> None:
        logger.warning(
            "A realtime conversation got a call to %r, which needs approval or the app's answer; realtime agents "
            "don't support that yet, so the call gets an error result.",
            tool.name,
        )

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
                call.result = _settled_with_error(
                    call.content, "the conversation closed before the tool finished", _CONVERSATION_CLOSED_REASON
                )
            self._report(call)

    def _report(self, call: _Call) -> None:
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
        chain_ends = chain.tripped or self._follow_ups(chain) >= chain.max_iterations
        _bind_namesakes_to_the_first(calls)
        invocation_disabled = not self._configuration.get("enabled", True)
        if invocation_disabled:
            chain.tripped = True  # whatever the calls were, the follow-up turns tools off
        for call in calls:
            if call.tool is None:
                call.result = _not_found_result(call.content)
            elif self._would_pause(call.tool):
                call.result = _settled_with_error(
                    call.content,
                    "this tool needs approval or the app's answer, which this agent can't wait for",
                    "unsupported",
                )
                self._warn_about_pausing_tool(call.tool)
            elif invocation_disabled:
                call.result = _settled_with_error(call.content, "function invocation is disabled", "disabled")
            elif chain_ends:
                call.result = _settled_with_error(
                    call.content, "this tool chain reached its limit, so the tool didn't run", "limit_reached"
                )
        terminated, executed = await self._execute([call for call in calls if call.result is None], chain)
        # Count synthetic errors too, so unknown calls can't evade the consecutive-error limit.
        chain.count(
            executed,
            had_errors=any(call.result is not None and call.result.exception is not None for call in calls),
        )
        terminated = terminated or chain_ends
        results = [call.result for call in calls if call.result is not None]
        await self._conversation._send_tool_results(Message("tool", results))  # pyright: ignore[reportPrivateUsage]
        self._results_marker = self._responses.created_count
        if chain.first_results_marker is None:
            chain.first_results_marker = self._results_marker
        for call in calls:
            self._report(call)
        if not terminated:
            self._follow_up_due(chain)

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

    async def _execute(self, calls: Sequence[_Call], chain: _ToolChain) -> tuple[bool, int]:
        """Return whether function middleware ended the loop, and how many calls ran."""
        if not calls:
            return False, 0
        chain.batch_started()
        requested_by_tools = _CALLING_CONVERSATION.set(self._conversation)
        try:
            batch = await self._run_executor(calls, _keep_results_as_they_finish(calls))
        finally:
            _CALLING_CONVERSATION.reset(requested_by_tools)
        for call in calls:
            if call.result is None:
                call.result = _settled_with_error(
                    call.content, "the call paused for an answer it can't get", "unsupported"
                )
        return batch.should_terminate, batch.executed_call_count

    async def _run_executor(
        self, calls: Sequence[_Call], on_call_finished: Callable[[Content, list[Content]], None]
    ) -> Any:
        return await _execute_function_calls(
            custom_args=dict(self._invocation_kwargs),
            function_calls=[call.content for call in calls],
            # Keep the arrival-time bindings apart from the live list that tools mutate.
            options={"tools": [call.tool for call in calls]},
            live_tools=self._tools,
            config=self._configuration,
            invocation_session=self._agent_session,
            middleware_pipeline=self._middleware_pipeline,
            realtime_conversation=self._conversation,
            on_call_finished=on_call_finished,
        )


def _keep_results_as_they_finish(calls: Sequence[_Call]) -> Callable[[Content, list[Content]], None]:
    """Close may cancel the batch, and a call that already finished must keep its real result."""
    by_content = {id(call.content): call for call in calls}

    def keep(function_call: Content, results: list[Content]) -> None:
        call = by_content[id(function_call)]
        call.result = next((content for content in results if content.type == "function_result"), call.result)

    return keep


def _session_for_stateful_middleware(function_middleware: Sequence[FunctionMiddlewareTypes]) -> AgentSession | None:
    """Separate conversations on one agent must never share such middleware's state, as in chat."""
    if any(getattr(middleware, "_requires_session_state", False) is True for middleware in function_middleware):
        from .._sessions import AgentSession

        return AgentSession()
    return None


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
