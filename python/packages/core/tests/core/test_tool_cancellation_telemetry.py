# Copyright (c) Microsoft. All rights reserved.

"""Telemetry for tool calls cancelled while running (spec 006, Telemetry, "Cancelled tools")."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import Mock

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent_framework import (
    Agent,
    ChatResponse,
    Content,
    FunctionInvocationContext,
    FunctionMiddleware,
    FunctionTool,
    Message,
    MiddlewareFailure,
    tool,
)
from agent_framework.observability import OtelAttr

from .conftest import MockBaseChatClient

# anyio cancels with a message naming its cancel scope's id, so it differs on every cancellation.
ANYIO_CANCEL_MESSAGE = "Cancelled via cancel scope 102d90890; reason: deadline exceeded"


def _slow_tool() -> tuple[FunctionTool, asyncio.Event]:
    """Return a tool that runs until cancelled, and the event it sets once running."""
    started = asyncio.Event()

    @tool(name="slow_tool", description="A tool that runs until cancelled")
    async def slow_tool() -> str:
        started.set()
        await asyncio.Event().wait()
        return "never"

    return slow_tool, started


async def _cancel_while_running(
    slow_tool: FunctionTool, started: asyncio.Event, cancel_message: Any = None
) -> "asyncio.Task[Any]":
    """Invoke the tool, cancel the call once its body is running, and return the finished call."""
    call = asyncio.create_task(slow_tool.invoke(tool_call_id="cancelled_call"))
    await started.wait()
    call.cancel(cancel_message)
    await asyncio.wait([call])
    return call


async def test_cancelled_call_marks_span_error_with_cancelled_error_type(span_exporter: InMemorySpanExporter) -> None:
    """A running call cancelled without a message ends its execute_tool span ERROR, error.type 'cancelled'."""
    slow_tool, started = _slow_tool()
    span_exporter.clear()

    await _cancel_while_running(slow_tool, started)

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].status.status_code == trace.StatusCode.ERROR
    assert spans[0].attributes is not None
    assert spans[0].attributes.get(OtelAttr.ERROR_TYPE) == "cancelled"


@pytest.mark.usefixtures("span_exporter")
async def test_cancelled_call_records_duration_with_cancelled_error_type() -> None:
    """The duration histogram records the cancelled call once, with error.type 'cancelled'."""
    slow_tool, started = _slow_tool()
    histogram = Mock()
    slow_tool._invocation_duration_histogram = histogram

    await _cancel_while_running(slow_tool, started)

    histogram.record.assert_called_once()
    assert histogram.record.call_args.kwargs["attributes"] == {
        OtelAttr.OPERATION: OtelAttr.TOOL_EXECUTION_OPERATION,
        OtelAttr.TOOL_NAME: "slow_tool",
        OtelAttr.TOOL_TYPE: "function",
        OtelAttr.MEASUREMENT_FUNCTION_TAG_NAME: "slow_tool",
        OtelAttr.ERROR_TYPE: "cancelled",
    }


async def test_conversation_closed_cancel_message_becomes_span_error_type(span_exporter: InMemorySpanExporter) -> None:
    """A call cancelled with the 'conversation_closed' message records that reason as error.type."""
    slow_tool, started = _slow_tool()
    span_exporter.clear()

    await _cancel_while_running(slow_tool, started, "conversation_closed")

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes.get(OtelAttr.ERROR_TYPE) == "conversation_closed"


async def test_unknown_cancel_message_records_cancelled_on_span(span_exporter: InMemorySpanExporter) -> None:
    """A cancel message Agent Framework doesn't send records 'cancelled', never the message itself."""
    slow_tool, started = _slow_tool()
    span_exporter.clear()

    await _cancel_while_running(slow_tool, started, ANYIO_CANCEL_MESSAGE)

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes.get(OtelAttr.ERROR_TYPE) == "cancelled"


async def test_unhashable_cancel_message_records_cancelled_on_span(span_exporter: InMemorySpanExporter) -> None:
    """A dict cancel message records 'cancelled' and still propagates CancelledError, not TypeError."""
    slow_tool, started = _slow_tool()
    span_exporter.clear()

    call = await _cancel_while_running(slow_tool, started, {"reason": "conversation_closed"})

    assert call.cancelled()
    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes.get(OtelAttr.ERROR_TYPE) == "cancelled"


@pytest.mark.usefixtures("span_exporter")
async def test_cancelled_call_logs_cancellation_at_info(caplog: pytest.LogCaptureFixture) -> None:
    """A cancelled call logs one INFO line naming the tool and its error.type."""
    slow_tool, started = _slow_tool()
    caplog.set_level(logging.INFO, logger="agent_framework")

    await _cancel_while_running(slow_tool, started, "conversation_closed")

    cancellation_records = [record for record in caplog.records if "cancelled" in record.getMessage()]
    assert [(record.levelno, record.getMessage()) for record in cancellation_records] == [
        (logging.INFO, "Function slow_tool cancelled (conversation_closed).")
    ]


class TestVerification:
    @pytest.mark.usefixtures("span_exporter")
    async def test_cancellation_propagates_unchanged(self) -> None:
        """Awaiting the cancelled call raises CancelledError carrying its original message."""
        slow_tool, started = _slow_tool()

        call = await _cancel_while_running(slow_tool, started, "conversation_closed")

        with pytest.raises(asyncio.CancelledError) as raised:
            await call
        assert raised.value.args == ("conversation_closed",)

    @pytest.mark.usefixtures("span_exporter")
    async def test_conversation_closed_recorded_on_duration_metric(self) -> None:
        """The duration histogram records 'conversation_closed' as error.type."""
        slow_tool, started = _slow_tool()
        histogram = Mock()
        slow_tool._invocation_duration_histogram = histogram

        await _cancel_while_running(slow_tool, started, "conversation_closed")

        histogram.record.assert_called_once()
        assert histogram.record.call_args.kwargs["attributes"][OtelAttr.ERROR_TYPE] == "conversation_closed"

    @pytest.mark.usefixtures("span_exporter")
    async def test_unknown_cancel_message_records_cancelled_on_duration_metric(self) -> None:
        """The duration histogram never records an unknown cancel message as error.type."""
        slow_tool, started = _slow_tool()
        histogram = Mock()
        slow_tool._invocation_duration_histogram = histogram

        await _cancel_while_running(slow_tool, started, ANYIO_CANCEL_MESSAGE)

        histogram.record.assert_called_once()
        assert histogram.record.call_args.kwargs["attributes"][OtelAttr.ERROR_TYPE] == "cancelled"

    @pytest.mark.parametrize("enable_sensitive_data", [True, False], indirect=True)
    async def test_cancelled_span_has_no_status_description_or_exception_event(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        """A cancelled call's span carries no status description and no exception event."""
        slow_tool, started = _slow_tool()
        span_exporter.clear()

        await _cancel_while_running(slow_tool, started, ANYIO_CANCEL_MESSAGE)

        spans = span_exporter.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].status.description is None
        assert [event.name for event in spans[0].events] == []

    @pytest.mark.usefixtures("span_exporter")
    async def test_cancellation_does_not_count_toward_max_invocation_exceptions(self) -> None:
        """A tool limited to one exception still runs after one cancelled call."""
        started = asyncio.Event()
        release = asyncio.Event()

        @tool(name="slow_tool", max_invocation_exceptions=1)
        async def slow_tool() -> str:
            started.set()
            await release.wait()
            return "done"

        await _cancel_while_running(slow_tool, started)
        release.set()
        result = await slow_tool.invoke()

        assert [content.text for content in result] == ["done"]

    async def test_chat_batch_sibling_cancelled_after_middleware_failure_records_cancelled(
        self, span_exporter: InMemorySpanExporter, chat_client_base: MockBaseChatClient
    ) -> None:
        """In a chat run, a parallel call cancelled after MiddlewareFailure records 'cancelled' on its span."""
        slow_tool, started = _slow_tool()

        @tool(name="failing_tool")
        def failing_tool() -> str:
            return "never"

        class FailOnceSlowToolRuns(FunctionMiddleware):
            async def process(
                self, context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]
            ) -> None:
                if context.function.name == "failing_tool":
                    await started.wait()
                    raise MiddlewareFailure("abort the batch")
                await call_next()

        chat_client_base.run_responses = [
            ChatResponse(
                messages=[
                    Message(
                        role="assistant",
                        contents=[
                            Content.from_function_call(call_id="call_1", name="failing_tool", arguments="{}"),
                            Content.from_function_call(call_id="call_2", name="slow_tool", arguments="{}"),
                        ],
                    )
                ]
            )
        ]
        agent = Agent(client=chat_client_base, middleware=[FailOnceSlowToolRuns()], tools=[failing_tool, slow_tool])
        span_exporter.clear()

        with pytest.raises(MiddlewareFailure, match="abort the batch"):
            await agent.run("run both tools")

        tool_spans = [span for span in span_exporter.get_finished_spans() if span.name == "execute_tool slow_tool"]
        assert len(tool_spans) == 1
        assert tool_spans[0].status.status_code == trace.StatusCode.ERROR
        assert tool_spans[0].attributes is not None
        assert tool_spans[0].attributes.get(OtelAttr.ERROR_TYPE) == "cancelled"

    async def test_call_cancelled_before_call_next_gets_no_span(
        self, span_exporter: InMemorySpanExporter, chat_client_base: MockBaseChatClient
    ) -> None:
        """A call cancelled before its tool body runs gets no execute_tool span."""
        slow_tool, _ = _slow_tool()
        middleware_waiting = asyncio.Event()

        class WaitBeforeCallNext(FunctionMiddleware):
            async def process(
                self, context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]
            ) -> None:
                middleware_waiting.set()
                await asyncio.Event().wait()
                await call_next()

        chat_client_base.run_responses = [
            ChatResponse(
                messages=[
                    Message(
                        role="assistant",
                        contents=[Content.from_function_call(call_id="call_1", name="slow_tool", arguments="{}")],
                    )
                ]
            )
        ]
        agent = Agent(client=chat_client_base, middleware=[WaitBeforeCallNext()], tools=[slow_tool])
        span_exporter.clear()

        async def run_agent() -> None:
            await agent.run("run the tool")

        run = asyncio.create_task(run_agent())
        await middleware_waiting.wait()
        run.cancel("conversation_closed")
        await asyncio.wait([run])

        tool_span_names = [span.name for span in span_exporter.get_finished_spans() if "execute_tool" in span.name]
        assert tool_span_names == []
