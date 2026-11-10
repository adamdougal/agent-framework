# Copyright (c) Microsoft. All rights reserved.

"""The realtime function-tool loop: each response's calls run as one batch once it finishes (spec 006, Tool loop)."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Coroutine, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from .._middleware import FunctionMiddlewarePipeline
from .._tools import (
    _CONVERSATION_CLOSED_REASON,  # pyright: ignore[reportPrivateUsage]
    DEFAULT_MAX_CONSECUTIVE_ERRORS_PER_REQUEST,
    DEFAULT_MAX_ITERATIONS,
    FunctionTool,
    _execute_function_calls,  # pyright: ignore[reportPrivateUsage]
    _get_tool_map,  # pyright: ignore[reportPrivateUsage]
)
from .._types import Content, Message
from ..exceptions import (
    RealtimeActiveResponseException,
    RealtimeConversationClosedException,
    RealtimeException,
)
from ._events import RealtimeEvent
from ._updates import core_attributes

logger = logging.getLogger("agent_framework")

# Set while a batch runs; each call's task copies it, so a request made from a tool can be told apart.
_CALLING_CONVERSATION: ContextVar[object | None] = ContextVar("realtime_calling_conversation", default=None)

if TYPE_CHECKING:
    from .._middleware import FunctionMiddlewareTypes
    from .._sessions import AgentSession
    from .._tools import FunctionInvocationConfiguration, ToolTypes
    from ._conversation import RealtimeConversation
    from ._responses import ResponseLedger


@dataclass
class _Call:
    """One function call the model made, and the tool it resolved to when it arrived."""

    content: Content
    response_id: str
    tool: FunctionTool | None
    result: Content | None = None
    reported: bool = False


class _ToolChain:
    """The follow-ups and calls driven by one caller turn or app input, which the loop's limits count.

    It applies chat's ``FunctionInvocationConfiguration`` limits per chain: ``max_iterations`` counts follow-ups,
    and the other limits count across the chain's batches.
    """

    def __init__(self, configuration: FunctionInvocationConfiguration) -> None:
        self._configuration = configuration
        self.restart()

    def restart(self) -> None:
        self.follow_ups = 0
        self._calls = 0
        self._error_batches = 0
        self._started_at: float | None = None
        self.tripped = False

    def batch_started(self) -> None:
        if self._started_at is None:
            self._started_at = time.monotonic()

    def count(self, executed_calls: int, *, had_errors: bool) -> None:
        """Count a finished batch, tripping the chain when a limit is reached."""
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

    def next_follow_up_turns_tools_off(self) -> bool:
        """Whether the chain's next follow-up must use ``tool_choice: "none"``."""
        max_iterations = self._configuration.get("max_iterations", DEFAULT_MAX_ITERATIONS)
        return self.tripped or self.follow_ups + 1 >= max_iterations


class FollowUp(NamedTuple):
    """A follow-up the tool loop claimed, with its per-response options."""

    options: Mapping[str, Any] | None


class ToolLoop:
    """Runs the function calls of each finished response, and sends their results to the provider."""

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
        send_results: Callable[[Message], Awaitable[None]],
        report_result: Callable[[Content, str], None],
        report_error: Callable[[RealtimeException], None],
        wait_until_ready: Callable[[], Awaitable[None]],
        request_follow_up: Callable[[Callable[[], FollowUp | None]], Awaitable[str | None]],
        session_options: Callable[[], Mapping[str, Any]],
        fail: Callable[[Exception], None],
        spawn: Callable[[Coroutine[Any, Any, Any]], asyncio.Task[Any]],
    ) -> None:
        self._tools = tools
        self._configuration = configuration
        self._invocation_kwargs = invocation_kwargs
        self._middleware_pipeline = FunctionMiddlewarePipeline(*function_middleware)
        self._agent_session = agent_session
        self._conversation = conversation
        self._responses = responses
        self._results_marker = 0
        self._send_results = send_results
        self._request_follow_up = request_follow_up
        self._wait_until_ready = wait_until_ready
        self._session_options = session_options
        self._follow_up_task: asyncio.Task[Any] | None = None
        self._follow_ups_due = 0
        self._follow_ups_due_when_requested = 0
        self._chain = _ToolChain(configuration)
        self._warned_about_pausing_tools = False
        self._running_batches: dict[asyncio.Task[Any], list[_Call]] = {}
        self._failed_batch_calls: list[_Call] = []
        self._closing = False
        self._report_result = report_result
        self._report_error = report_error
        self._fail = fail
        self._spawn = spawn
        self._calls_by_response: dict[str, list[_Call]] = {}

    def on_function_call(self, event: RealtimeEvent) -> None:
        """Record a call, resolving it against the tools available now (spec 006, Control calls, Tools)."""
        if event.content is None:
            return
        response_id = core_attributes(event)["response_id"]
        tool = _get_tool_map(self._tools).get(event.content.name or "")
        self._calls_by_response.setdefault(response_id, []).append(_Call(event.content, response_id, tool))

    def _would_pause(self, tool: FunctionTool) -> bool:
        """Whether the executor would pause this call for an approval or the app's answer (#37 adds both)."""
        app_answered = {additional.name for additional in self._configuration.get("additional_tools") or ()}
        return tool.approval_mode == "always_require" or tool.declaration_only or tool.name in app_answered

    def _warn_about_pausing_tools(self) -> None:
        if not self._warned_about_pausing_tools:
            self._warned_about_pausing_tools = True
            logger.warning(
                "A realtime conversation got a call to a tool that needs approval or the app's answer; realtime agents "
                "don't support that yet, so the call gets an error result."
            )

    def on_new_input(self) -> None:
        """Start a new tool chain: the caller spoke or the app sent input, and the loop's limits count afresh."""
        self._chain.restart()

    def on_response_done(self, response_id: str, status: str) -> None:
        if self._closing:
            return  # close() settles these calls
        calls = self._calls_by_response.pop(response_id, [])
        if not calls:
            return
        if status == "completed":
            batch = self._spawn(self._run_batch(response_id, calls))
            self._running_batches[batch] = calls
            batch.add_done_callback(self._forget_batch)
            return
        for call in calls:
            call.result = _settled_with_error(call.content, f"the response ended {status} before the tool ran", status)
        self._spawn(self._send_unrun_results(response_id, calls))

    def _forget_batch(self, batch: asyncio.Task[Any]) -> None:
        self._running_batches.pop(batch, None)

    def stop(self) -> None:
        """Start no more batches or follow-ups, from the moment close starts; ``close()`` settles the calls left."""
        self._closing = True
        if self._follow_up_task is not None:
            self._follow_up_task.cancel()

    async def close(self) -> None:
        """Stop new batches, cancel running tools, and settle every unfinished call (spec 006, Tool loop, Settlement).

        Each running batch is cancelled once with the ``conversation_closed`` reason, which the executor forwards to
        its calls, so their ``execute_tool`` spans record it. Settled results stream but aren't sent.
        """
        self.stop()
        running = dict(self._running_batches)
        for batch in running:
            batch.cancel(_CONVERSATION_CLOSED_REASON)
        await asyncio.gather(*running, return_exceptions=True)
        unfinished = [*self._failed_batch_calls, *(call for calls in running.values() for call in calls)]
        self._failed_batch_calls.clear()
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
            self._report_result(call.result, call.response_id)

    async def _send_unrun_results(self, response_id: str, calls: list[_Call]) -> None:
        """Send the error results of calls that never ran, best effort, and request no follow-up."""
        results = [call.result for call in calls if call.result is not None]
        for call in calls:
            self._report(call)
        try:
            await self._send_results(Message("tool", results))
        except RealtimeException as error:
            logger.info("Couldn't send the results of calls that never ran: %s", error)

    async def _run_batch(self, response_id: str, calls: list[_Call]) -> None:
        try:
            await self._run_batch_steps(response_id, calls)
        except Exception as error:
            # MiddlewareFailure is the executor's fail-closed signal; anything else is a broken batch. Either way the
            # conversation closes, and close settles this batch's calls.
            self._failed_batch_calls.extend(calls)
            self._fail(error)

    async def _run_batch_steps(self, response_id: str, calls: list[_Call]) -> None:
        chain_ends = self._chain.tripped
        invocation_disabled = not self._configuration.get("enabled", True)
        for call in calls:
            if call.tool is None:
                call.result = _not_found_result(call.content)
            elif self._would_pause(call.tool):
                call.result = _settled_with_error(
                    call.content,
                    "this tool needs approval or the app's answer, which this agent can't wait for",
                    "unsupported",
                )
                self._warn_about_pausing_tools()
            elif invocation_disabled:
                call.result = _settled_with_error(call.content, "function invocation is disabled", "disabled")
                self._chain.tripped = True
            elif chain_ends:
                call.result = _settled_with_error(
                    call.content, "this tool chain reached its limit, so the tool didn't run", "limit_reached"
                )
        terminated, executed = await self._execute([call for call in calls if call.result is None])
        # Errors count across the whole batch, as in chat, where unknown calls get their error result in the executor.
        self._chain.count(
            executed,
            had_errors=any(call.result is not None and call.result.exception is not None for call in calls),
        )
        terminated = terminated or chain_ends
        results = [call.result for call in calls if call.result is not None]
        await self._send_results(Message("tool", results))
        self._results_marker = self._responses.created_count
        for call in calls:
            self._report(call)
        if not terminated:
            self._follow_up_due()

    def _follow_up_due(self) -> None:
        """Request one follow-up once the provider can take it; follow-ups due together merge."""
        self._follow_ups_due += 1
        if self._follow_up_task is None or self._follow_up_task.done():
            self._follow_up_task = self._spawn(self._follow_up())

    def _follow_up_options(self) -> dict[str, Any] | None:
        """Per-response options for the chain's next follow-up.

        The ``max_iterations``-th follow-up turns tools off, so a chain gets at most ``max_iterations`` responses with
        tools on, as chat's loop does. Otherwise a required tool choice relaxes to ``"auto"``, as in chat.
        """
        if self._chain.next_follow_up_turns_tools_off():
            return {"tool_choice": "none"}
        if _requires_a_tool(self._session_options().get("tool_choice")):
            return {"tool_choice": "auto"}
        return None

    def _claim_follow_up(self) -> FollowUp | None:
        """Claim the follow-up as it's requested, under the control lock, or return ``None`` once it isn't due."""
        if self._responses.answered_since(self._results_marker):
            return None
        options = self._follow_up_options()
        # Count it when it's requested: new input during the request starts a new chain, which it isn't part of.
        self._chain.follow_ups += 1
        self._follow_ups_due_when_requested = self._follow_ups_due
        return FollowUp(options)

    async def _follow_up(self) -> None:
        while True:
            await self._wait_until_ready()
            if self._responses.answered_since(self._results_marker):
                return  # a response created after the results already saw them, so it is the follow-up
            try:
                response_id = await self._request_follow_up(self._claim_follow_up)
            except RealtimeActiveResponseException:
                continue  # another response won the race; it counts if it can act on the results
            except RealtimeConversationClosedException:
                return
            except RealtimeException as error:
                self._report_error(error)
                return
            if response_id is None:
                continue  # the caller spoke, a response started, or one answered, while it waited its turn
            if self._follow_ups_due == self._follow_ups_due_when_requested:
                return
            # Results went out while it was being requested, which its response can't act on, so request another.
            self._responses.not_answering(response_id)

    async def _execute(self, calls: Sequence[_Call]) -> tuple[bool, int]:
        """Run ``calls`` through the shared executor.

        Returns:
            Whether function middleware ended the loop, and how many calls ran.
        """
        if not calls:
            return False, 0
        self._chain.batch_started()
        requested_by_tools = _CALLING_CONVERSATION.set(self._conversation)
        try:
            batch = await self._run_executor(calls)
        finally:
            _CALLING_CONVERSATION.reset(requested_by_tools)
        for call, group in zip(calls, batch.result_groups, strict=True):
            call.result = next((content for content in group if content.type == "function_result"), None)
            if call.result is None:
                call.result = _settled_with_error(
                    call.content, "the call paused for an answer it can't get", "unsupported"
                )
        return batch.should_terminate, batch.executed_call_count

    async def _run_executor(self, calls: Sequence[_Call]) -> Any:
        return await _execute_function_calls(
            custom_args=dict(self._invocation_kwargs),
            function_calls=[call.content for call in calls],
            # Each call runs the tool it resolved to when it arrived; the context still sees the live list.
            options={"tools": [call.tool for call in calls]},
            live_tools=self._tools,
            config=self._configuration,
            invocation_session=self._agent_session,
            middleware_pipeline=self._middleware_pipeline,
            realtime_conversation=self._conversation,
        )


def _requires_a_tool(tool_choice: Any) -> bool:
    if tool_choice == "required":
        return True
    return isinstance(tool_choice, Mapping) and cast("Mapping[str, Any]", tool_choice).get("mode") == "required"


def requested_by_a_tool(conversation: RealtimeConversation[Any]) -> bool:
    """Whether the current task is one of ``conversation``'s tool calls or their function middleware."""
    return _CALLING_CONVERSATION.get() is conversation


def _not_found_result(call: Content) -> Content:
    """The error result chat's loop gives a call to a tool it doesn't have."""
    return Content.from_function_result(
        call_id=call.call_id or "",
        result=f'Error: Requested function "{call.name}" not found.',
        exception=str(KeyError(f'Function "{call.name}" not found.')),
    )


def _settled_with_error(call: Content, why: str, reason: str) -> Content:
    """An error result for a call that couldn't run or finish; ``reason`` becomes its ``exception``."""
    return Content.from_function_result(call_id=call.call_id or "", result=f"Error: {why}.", exception=reason)
