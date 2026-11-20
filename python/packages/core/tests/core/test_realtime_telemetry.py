# Copyright (c) Microsoft. All rights reserved.

"""Telemetry for live realtime conversations: the conversation span, response spans and their metrics."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, cast

import pytest
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import Histogram, HistogramDataPoint, InMemoryMetricReader
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent_framework import (
    AgentSession,
    Content,
    ContextProvider,
    ConversationContext,
    RawRealtimeAgent,
    RealtimeAgent,
    RealtimeEvent,
    SessionContext,
    SupportsAgentRun,
    UsageDetails,
    observability,
    tool,
)
from agent_framework.exceptions import RealtimeConnectionException
from agent_framework.observability import OtelAttr

from .realtime_helpers import FakeRealtimeClient, event, eventually, next_update


@tool
def get_weather(city: str) -> str:
    """Get the weather for a city."""
    return f"Sunny in {city}"


class _ModelClient(FakeRealtimeClient):
    """A fake client that names its provider, as provider clients do, and the model it was given."""

    OTEL_PROVIDER_NAME = "openai"

    def __init__(self, model: str) -> None:
        super().__init__()
        self.model = model


def _spans(span_exporter: InMemorySpanExporter, operation: str) -> list[ReadableSpan]:
    return [span for span in span_exporter.get_finished_spans() if span.name.split(" ")[0] == operation]


def _attributes(span: ReadableSpan, *keys: str) -> tuple[Any, ...]:
    attributes = span.attributes or {}
    return tuple(attributes.get(key) for key in keys)


def _parent_id(span: ReadableSpan) -> int | None:
    return span.parent.span_id if span.parent is not None else None


def _span_id(span: ReadableSpan) -> int:
    assert span.context is not None
    return span.context.span_id


def _finish_reasons(span_exporter: InMemorySpanExporter) -> Any:
    """The finish reasons of the one realtime_inference span."""
    [span] = _spans(span_exporter, "realtime_inference")
    return _attributes(span, OtelAttr.FINISH_REASONS)[0]


async def _spans_of_response_ending(span_exporter: InMemorySpanExporter, **done: Any) -> list[ReadableSpan]:
    """The realtime_inference spans after resp_1 is created and then done with the ``done`` attributes."""
    client = FakeRealtimeClient()
    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", **done))
        await _until_response_done(conversation)
    return _spans(span_exporter, "realtime_inference")


def _metric_points(reader: InMemoryMetricReader, name: str) -> list[HistogramDataPoint]:
    data = reader.get_metrics_data()
    if data is None:
        return []
    return [
        point
        for resource_metrics in data.resource_metrics
        for scope_metrics in resource_metrics.scope_metrics
        for metric in scope_metrics.metrics
        if metric.name == name
        for point in cast(Histogram, metric.data).data_points
    ]


def _point_attribute(point: HistogramDataPoint, key: str) -> Any:
    return (point.attributes or {}).get(key)


def _call(call_id: str, name: str, *, response_id: str = "resp_1", **arguments: Any) -> RealtimeEvent:
    return event(
        "function_call",
        content=Content.from_function_call(call_id=call_id, name=name, arguments=arguments),
        response_id=response_id,
        item_id=f"item_{call_id}",
    )


def _usage(response_id: str, usage: UsageDetails) -> RealtimeEvent:
    return event("usage", content=Content.from_usage(usage), response_id=response_id)


def _response(response_id: str, *events: RealtimeEvent, status: str = "completed", **done: Any) -> list[RealtimeEvent]:
    """The events of one response: created, ``events``, then done with ``status`` and the ``done`` attributes."""
    return [
        event("response_created", response_id=response_id),
        *events,
        event("response_done", response_id=response_id, status=status, **done),
    ]


async def _until_response_done(conversation: Any, response_id: str = "resp_1") -> None:
    """Read updates until ``response_id``'s response_done streams."""
    while True:
        update = await next_update(conversation)
        if update.response_id == response_id and any(content.kind == "response_done" for content in update.contents):
            return


# region The conversation span


async def test_a_live_conversation_is_one_invoke_agent_span_for_the_agent_that_started_it(
    span_exporter: InMemorySpanExporter,
) -> None:
    """A RealtimeAgent named "Concierge" (id "agent_1", description "Answers the phone.") opens and closes a
    conversation: one finished span "invoke_agent Concierge", INTERNAL, with gen_ai.operation.name "invoke_agent",
    gen_ai.provider.name "microsoft.agent_framework", gen_ai.agent.id "agent_1", gen_ai.agent.name "Concierge" and
    gen_ai.agent.description "Answers the phone.", status UNSET."""
    agent = RealtimeAgent(FakeRealtimeClient(), name="Concierge", id="agent_1", description="Answers the phone.")

    async with agent.start_conversation():
        pass

    [span] = _spans(span_exporter, "invoke_agent")
    assert span.name == "invoke_agent Concierge"
    assert span.kind == trace.SpanKind.INTERNAL
    assert span.status.status_code == trace.StatusCode.UNSET
    assert _attributes(span, OtelAttr.OPERATION, OtelAttr.PROVIDER_NAME, OtelAttr.AGENT_ID) == (
        "invoke_agent",
        "microsoft.agent_framework",
        "agent_1",
    )
    assert _attributes(span, OtelAttr.AGENT_NAME, OtelAttr.AGENT_DESCRIPTION) == ("Concierge", "Answers the phone.")


async def test_a_raw_realtime_agent_emits_no_conversation_or_response_spans(
    span_exporter: InMemorySpanExporter,
) -> None:
    """A RawRealtimeAgent conversation with one finished response emits no invoke_agent span and no
    realtime_inference span."""
    client = FakeRealtimeClient()

    async with RawRealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(*_response("resp_1"))
        await _until_response_done(conversation)

    assert _spans(span_exporter, "invoke_agent") == []
    assert _spans(span_exporter, "realtime_inference") == []


async def test_a_realtime_agent_emits_no_spans_while_instrumentation_is_disabled(
    span_exporter: InMemorySpanExporter,
) -> None:
    """With instrumentation disabled, a RealtimeAgent conversation with one finished response emits no invoke_agent
    span and no realtime_inference span."""
    observability.OBSERVABILITY_SETTINGS.enable_instrumentation = False
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(*_response("resp_1"))
        await _until_response_done(conversation)

    assert _spans(span_exporter, "invoke_agent") == []
    assert _spans(span_exporter, "realtime_inference") == []


async def test_the_conversation_span_records_the_providers_realtime_session_id(
    span_exporter: InMemorySpanExporter,
) -> None:
    """The provider reports session_created with session id "sess_1": the conversation span carries
    gen_ai.realtime_session.id "sess_1"."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event("session_created", session_id="sess_1"))
        await next_update(conversation)

    [span] = _spans(span_exporter, "invoke_agent")
    assert _attributes(span, "gen_ai.realtime_session.id") == ("sess_1",)


# endregion

# region Response spans


async def test_a_response_is_a_realtime_inference_span_under_the_conversation_span(
    span_exporter: InMemorySpanExporter,
) -> None:
    """A client whose OTEL_PROVIDER_NAME is "openai" and model "gpt-realtime" streams resp_1 created then completed:
    one span "realtime_inference gpt-realtime", CLIENT, child of the invoke_agent span, with gen_ai.operation.name
    "realtime_inference", gen_ai.provider.name "openai", gen_ai.request.model "gpt-realtime", gen_ai.response.id
    "resp_1" and gen_ai.response.finish_reasons ("stop",)."""
    client = _ModelClient("gpt-realtime")

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(*_response("resp_1"))
        await _until_response_done(conversation)

    [conversation_span] = _spans(span_exporter, "invoke_agent")
    [span] = _spans(span_exporter, "realtime_inference")
    assert span.name == "realtime_inference gpt-realtime"
    assert span.kind == trace.SpanKind.CLIENT
    assert _parent_id(span) == _span_id(conversation_span)
    assert _attributes(span, OtelAttr.OPERATION, OtelAttr.PROVIDER_NAME, OtelAttr.REQUEST_MODEL) == (
        "realtime_inference",
        "openai",
        "gpt-realtime",
    )
    assert _attributes(span, OtelAttr.RESPONSE_ID, OtelAttr.FINISH_REASONS) == ("resp_1", ("stop",))


async def test_a_tool_calls_span_is_a_sibling_of_the_response_spans_under_the_conversation_span(
    span_exporter: InMemorySpanExporter,
) -> None:
    """resp_1 calls get_weather and completes, the tool runs, and the follow-up resp_2 completes: the execute_tool
    get_weather span and both realtime_inference spans share the invoke_agent span as their parent."""
    client = _ModelClient("gpt-realtime")

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
        client.session.create_response_events = [event("response_created", response_id="resp_2")]
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await eventually(lambda: ("create_response", None) in client.session.calls)
        client.session.emit(event("response_done", response_id="resp_2", status="completed"))
        await _until_response_done(conversation, "resp_2")

    [conversation_span] = _spans(span_exporter, "invoke_agent")
    children = [*_spans(span_exporter, "realtime_inference"), *_spans(span_exporter, "execute_tool")]
    assert len(children) == 3
    assert {_parent_id(span) for span in children} == {_span_id(conversation_span)}


async def test_a_response_span_is_named_for_the_model_the_session_reports_when_the_client_names_none(
    span_exporter: InMemorySpanExporter,
) -> None:
    """A client with no model; session_created reports model "gpt-realtime-2025-08-28": resp_1's span is
    "realtime_inference gpt-realtime-2025-08-28", with gen_ai.response.model "gpt-realtime-2025-08-28" and no
    gen_ai.request.model."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event("session_created", session_id="sess_1", model="gpt-realtime-2025-08-28"))
        client.session.emit(*_response("resp_1"))
        await _until_response_done(conversation)

    [span] = _spans(span_exporter, "realtime_inference")
    assert span.name == "realtime_inference gpt-realtime-2025-08-28"
    assert _attributes(span, OtelAttr.RESPONSE_MODEL, OtelAttr.REQUEST_MODEL) == ("gpt-realtime-2025-08-28", None)


async def test_a_response_span_is_named_realtime_inference_when_no_model_is_known(
    span_exporter: InMemorySpanExporter,
) -> None:
    """A client with no model, and a session that reports none: resp_1's span is named "realtime_inference"."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(*_response("resp_1"))
        await _until_response_done(conversation)

    [span] = _spans(span_exporter, "realtime_inference")
    assert span.name == "realtime_inference"


async def test_a_response_span_names_the_agent_active_when_the_response_was_created(
    span_exporter: InMemorySpanExporter,
) -> None:
    """resp_1 is created while "Receptionist" (id "receptionist_1") is active, the call is handed off to "Billing"
    (id "billing_1"), then resp_1 completes and resp_2 is created and completes: resp_1's span carries
    gen_ai.agent.name "Receptionist" and gen_ai.agent.id "receptionist_1"; resp_2's carries "Billing" and
    "billing_1"."""
    client = FakeRealtimeClient()
    receptionist = RealtimeAgent(client, name="Receptionist", id="receptionist_1")
    billing = RealtimeAgent(client, name="Billing", id="billing_1")

    async with receptionist.start_conversation() as conversation:
        client.session.emit(event("response_created", response_id="resp_1"))
        await next_update(conversation)
        await conversation.handoff(billing)
        client.session.emit(event("response_done", response_id="resp_1", status="completed"), *_response("resp_2"))
        await _until_response_done(conversation, "resp_2")

    spans = {_attributes(span, OtelAttr.RESPONSE_ID)[0]: span for span in _spans(span_exporter, "realtime_inference")}
    assert _attributes(spans["resp_1"], OtelAttr.AGENT_NAME, OtelAttr.AGENT_ID) == ("Receptionist", "receptionist_1")
    assert _attributes(spans["resp_2"], OtelAttr.AGENT_NAME, OtelAttr.AGENT_ID) == ("Billing", "billing_1")


async def test_a_response_span_carries_the_responses_usage(span_exporter: InMemorySpanExporter) -> None:
    """resp_1 reports usage of 10 input tokens, 20 output tokens and 4 cached input tokens: its span carries
    gen_ai.usage.input_tokens 10, gen_ai.usage.output_tokens 20 and gen_ai.usage.cache_read.input_tokens 4."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(
            *_response(
                "resp_1",
                _usage(
                    "resp_1",
                    UsageDetails(input_token_count=10, output_token_count=20, cache_read_input_token_count=4),
                ),
            )
        )
        await _until_response_done(conversation)

    [span] = _spans(span_exporter, "realtime_inference")
    assert _attributes(span, OtelAttr.INPUT_TOKENS, OtelAttr.OUTPUT_TOKENS, OtelAttr.CACHE_READ_INPUT_TOKENS) == (
        10,
        20,
        4,
    )


async def test_the_conversation_span_sums_usage_over_every_response(span_exporter: InMemorySpanExporter) -> None:
    """resp_1 reports 10 input and 20 output tokens, resp_2 reports 5 and 7: the conversation span carries
    gen_ai.usage.input_tokens 15 and gen_ai.usage.output_tokens 27."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(
            *_response("resp_1", _usage("resp_1", UsageDetails(input_token_count=10, output_token_count=20))),
            *_response("resp_2", _usage("resp_2", UsageDetails(input_token_count=5, output_token_count=7))),
        )
        await _until_response_done(conversation, "resp_2")

    [span] = _spans(span_exporter, "invoke_agent")
    assert _attributes(span, OtelAttr.INPUT_TOKENS, OtelAttr.OUTPUT_TOKENS) == (15, 27)


# endregion

# region How a response ended


async def test_a_completed_response_that_called_a_tool_finishes_with_tool_call(
    span_exporter: InMemorySpanExporter,
) -> None:
    """resp_1 calls get_weather and completes: its span's gen_ai.response.finish_reasons is ("tool_call",)."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client, tools=[get_weather]).start_conversation() as conversation:
        client.session.emit(*_response("resp_1", _call("call_1", "get_weather", city="Paris")))
        await _until_response_done(conversation)

    assert _finish_reasons(span_exporter) == ("tool_call",)


async def test_a_content_filtered_response_finishes_with_content_filter_and_no_error_status(
    span_exporter: InMemorySpanExporter,
) -> None:
    """resp_1 ends incomplete with reason "content_filter": finish reasons ("content_filter",), status UNSET, and no
    error.type."""
    [span] = await _spans_of_response_ending(span_exporter, status="incomplete", reason="content_filter")

    assert _attributes(span, OtelAttr.FINISH_REASONS, OtelAttr.ERROR_TYPE) == (("content_filter",), None)
    assert span.status.status_code == trace.StatusCode.UNSET


async def test_an_incomplete_response_with_an_unknown_reason_records_no_finish_reasons(
    span_exporter: InMemorySpanExporter,
) -> None:
    """resp_1 ends incomplete with reason "something_else", which has no finish reason: its span carries no
    gen_ai.response.finish_reasons, and its status is UNSET."""
    [span] = await _spans_of_response_ending(span_exporter, status="incomplete", reason="something_else")

    assert _attributes(span, OtelAttr.FINISH_REASONS) == (None,)
    assert span.status.status_code == trace.StatusCode.UNSET


async def test_a_response_done_without_a_status_ends_its_span_and_the_call_goes_on(
    span_exporter: InMemorySpanExporter,
) -> None:
    """resp_1's response_done arrives without a status, then resp_2 completes: resp_1's span ends with no
    gen_ai.response.finish_reasons, and resp_2's span ends with ("stop",)."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(
            event("response_created", response_id="resp_1"),
            event("response_done", response_id="resp_1"),
            *_response("resp_2"),
        )
        await _until_response_done(conversation, "resp_2")

    spans = {_attributes(span, OtelAttr.RESPONSE_ID)[0]: span for span in _spans(span_exporter, "realtime_inference")}
    assert _attributes(spans["resp_1"], OtelAttr.FINISH_REASONS) == (None,)
    assert _attributes(spans["resp_2"], OtelAttr.FINISH_REASONS) == (("stop",),)


async def test_a_cancelled_response_finishes_with_the_providers_reason(span_exporter: InMemorySpanExporter) -> None:
    """resp_1 ends cancelled with reason "turn_detected": finish reasons ("turn_detected",), status UNSET."""
    [span] = await _spans_of_response_ending(span_exporter, status="cancelled", reason="turn_detected")

    assert _attributes(span, OtelAttr.FINISH_REASONS) == (("turn_detected",),)
    assert span.status.status_code == trace.StatusCode.UNSET


async def test_a_cancelled_response_without_a_reason_finishes_as_interrupted(
    span_exporter: InMemorySpanExporter,
) -> None:
    """resp_1 ends cancelled with no reason: finish reasons ("interrupted",), status UNSET."""
    [span] = await _spans_of_response_ending(span_exporter, status="cancelled")

    assert _attributes(span, OtelAttr.FINISH_REASONS) == (("interrupted",),)
    assert span.status.status_code == trace.StatusCode.UNSET


async def test_a_failed_response_finishes_with_error_and_the_providers_code(
    span_exporter: InMemorySpanExporter,
) -> None:
    """resp_1 ends failed with error code "rate_limit_exceeded": finish reasons ("error",), error.type
    "rate_limit_exceeded", status ERROR."""
    [span] = await _spans_of_response_ending(span_exporter, status="failed", error_code="rate_limit_exceeded")

    assert _attributes(span, OtelAttr.FINISH_REASONS, OtelAttr.ERROR_TYPE) == (("error",), "rate_limit_exceeded")
    assert span.status.status_code == trace.StatusCode.ERROR


async def test_a_failed_response_without_a_code_records_server_error(span_exporter: InMemorySpanExporter) -> None:
    """resp_1 ends failed with no error code: finish reasons ("error",), error.type "server_error", status ERROR."""
    [span] = await _spans_of_response_ending(span_exporter, status="failed")

    assert _attributes(span, OtelAttr.FINISH_REASONS, OtelAttr.ERROR_TYPE) == (("error",), "server_error")
    assert span.status.status_code == trace.StatusCode.ERROR


async def test_a_response_still_running_at_a_normal_close_finishes_as_interrupted(
    span_exporter: InMemorySpanExporter,
) -> None:
    """resp_1 is created and the app closes the conversation before it finishes: its span ends with finish reasons
    ("interrupted",), status UNSET."""
    client = FakeRealtimeClient()

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(event("response_created", response_id="resp_1"))
        await next_update(conversation)

    [span] = _spans(span_exporter, "realtime_inference")
    assert _attributes(span, OtelAttr.FINISH_REASONS) == (("interrupted",),)
    assert span.status.status_code == trace.StatusCode.UNSET


# endregion

# region How the conversation ended


async def test_a_lost_connection_marks_the_conversation_span_error_with_the_failures_type(
    span_exporter: InMemorySpanExporter,
) -> None:
    """The connection fails with RealtimeConnectionException: iteration raises it, and the conversation span ends
    with status ERROR and error.type "RealtimeConnectionException"."""
    client = FakeRealtimeClient()

    with pytest.raises(RealtimeConnectionException):
        async with RealtimeAgent(client).start_conversation() as conversation:
            client.session.fail(RealtimeConnectionException("The connection dropped."))
            async for _ in conversation:
                pass

    [span] = _spans(span_exporter, "invoke_agent")
    assert (span.status.status_code, _attributes(span, OtelAttr.ERROR_TYPE)) == (
        trace.StatusCode.ERROR,
        ("RealtimeConnectionException",),
    )


async def test_a_response_running_when_the_connection_is_lost_finishes_with_error(
    span_exporter: InMemorySpanExporter,
) -> None:
    """resp_1 is created, then the connection fails with RealtimeConnectionException: resp_1's span ends with finish
    reasons ("error",), error.type "RealtimeConnectionException", status ERROR."""
    client = FakeRealtimeClient()

    with pytest.raises(RealtimeConnectionException):
        async with RealtimeAgent(client).start_conversation() as conversation:
            client.session.emit(event("response_created", response_id="resp_1"))
            client.session.fail(RealtimeConnectionException("The connection dropped."))
            async for _ in conversation:
                pass

    [span] = _spans(span_exporter, "realtime_inference")
    assert _attributes(span, OtelAttr.FINISH_REASONS, OtelAttr.ERROR_TYPE) == (
        ("error",),
        "RealtimeConnectionException",
    )
    assert span.status.status_code == trace.StatusCode.ERROR


async def test_a_close_that_raises_because_history_was_not_recorded_marks_the_conversation_span_error(
    span_exporter: InMemorySpanExporter,
) -> None:
    """A context provider's after_run raises HistoryStoreDown for the call's one exchange: leaving the conversation
    raises it, and the conversation span ends with status ERROR and error.type "HistoryStoreDown"."""

    class HistoryStoreDown(Exception):
        pass

    class FailingStore(ContextProvider):
        async def after_run(
            self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
        ) -> None:
            raise HistoryStoreDown("The history store is down.")

    agent = RealtimeAgent(FakeRealtimeClient(), context_providers=[FailingStore("store")])

    with pytest.raises(HistoryStoreDown):
        async with agent.start_conversation() as conversation:
            await conversation.send("Hi", create_response=False)

    [span] = _spans(span_exporter, "invoke_agent")
    assert (span.status.status_code, _attributes(span, OtelAttr.ERROR_TYPE)) == (
        trace.StatusCode.ERROR,
        ("HistoryStoreDown",),
    )


async def test_a_conversation_that_fails_to_open_ends_its_span_with_error(span_exporter: InMemorySpanExporter) -> None:
    """start_session raises RealtimeConnectionException: entering raises it, and one invoke_agent span has ended
    with status ERROR and error.type "RealtimeConnectionException"."""
    client = FakeRealtimeClient()
    client.start_session_error = RealtimeConnectionException("Couldn't connect.")

    with pytest.raises(RealtimeConnectionException):
        async with RealtimeAgent(client).start_conversation():
            pass

    [span] = _spans(span_exporter, "invoke_agent")
    assert (span.status.status_code, _attributes(span, OtelAttr.ERROR_TYPE)) == (
        trace.StatusCode.ERROR,
        ("RealtimeConnectionException",),
    )


async def test_a_conversation_whose_entry_is_cancelled_ends_its_span_with_error(
    span_exporter: InMemorySpanExporter,
) -> None:
    """The app cancels the conversation while start_session is still connecting: entering raises CancelledError, and
    one invoke_agent span has ended with status ERROR and error.type "CancelledError"."""
    client = FakeRealtimeClient()
    client.start_session_release = asyncio.Event()

    async def enter() -> None:
        async with RealtimeAgent(client).start_conversation():
            pass

    entering = asyncio.create_task(enter())
    await eventually(lambda: bool(client.start_session_calls))
    entering.cancel()
    with pytest.raises(asyncio.CancelledError):
        await entering

    [span] = _spans(span_exporter, "invoke_agent")
    assert (span.status.status_code, _attributes(span, OtelAttr.ERROR_TYPE)) == (
        trace.StatusCode.ERROR,
        ("CancelledError",),
    )


async def test_context_provider_spans_nest_under_the_conversation_span_when_the_app_closes_the_call(
    span_exporter: InMemorySpanExporter,
) -> None:
    """A context provider opens a span "load" in before_run and "store" in after_run; the app sends one text input
    and leaves the conversation from its own task inside an "app" span: "load" and "store" are both children of
    the invoke_agent span, which is a child of "app"."""
    tracer = trace.get_tracer("app")

    class Store(ContextProvider):
        async def before_run(
            self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
        ) -> None:
            with tracer.start_as_current_span("load"):
                pass

        async def after_run(
            self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
        ) -> None:
            with tracer.start_as_current_span("store"):
                pass

    agent = RealtimeAgent(FakeRealtimeClient(), context_providers=[Store("store")])

    with tracer.start_as_current_span("app"):
        async with agent.start_conversation() as conversation:
            await conversation.send("Hi", create_response=False)

    spans = {span.name.split(" ")[0]: span for span in span_exporter.get_finished_spans()}
    conversation_id = _span_id(spans["invoke_agent"])
    assert (_parent_id(spans["load"]), _parent_id(spans["store"]), _parent_id(spans["invoke_agent"])) == (
        conversation_id,
        conversation_id,
        _span_id(spans["app"]),
    )


# endregion

# region Metrics


async def test_each_response_records_operation_duration_and_token_usage(
    span_exporter: InMemorySpanExporter, monkeypatch: pytest.MonkeyPatch
) -> None:
    """resp_1 reports 10 input and 20 output tokens: gen_ai.client.operation.duration records one value and
    gen_ai.client.token.usage records 10 with gen_ai.token.type "input" and 20 with "output", each with
    gen_ai.operation.name "realtime_inference" and the client's provider name."""
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader])
    monkeypatch.setattr(observability, "get_meter", lambda *args, **kwargs: meter_provider.get_meter("test"))
    client = _ModelClient("gpt-realtime")

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(
            *_response("resp_1", _usage("resp_1", UsageDetails(input_token_count=10, output_token_count=20)))
        )
        await _until_response_done(conversation)

    durations = _metric_points(reader, OtelAttr.LLM_OPERATION_DURATION)
    tokens = _metric_points(reader, OtelAttr.LLM_TOKEN_USAGE)
    assert [point.count for point in durations] == [1]
    assert sorted((_point_attribute(point, OtelAttr.T_TYPE), point.sum) for point in tokens) == [
        ("input", 10),
        ("output", 20),
    ]
    assert {
        (_point_attribute(point, OtelAttr.OPERATION), _point_attribute(point, OtelAttr.PROVIDER_NAME))
        for point in [*durations, *tokens]
    } == {("realtime_inference", "openai")}


async def test_every_response_in_a_conversation_records_its_own_duration_and_token_usage(
    span_exporter: InMemorySpanExporter, monkeypatch: pytest.MonkeyPatch
) -> None:
    """resp_1 reports 10 input and 20 output tokens, then resp_2 in the same conversation reports 30 and 40:
    gen_ai.client.operation.duration records two values, and gen_ai.client.token.usage records two input values
    totalling 40 and two output values totalling 60."""
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader])
    monkeypatch.setattr(observability, "get_meter", lambda *args, **kwargs: meter_provider.get_meter("test"))
    client = _ModelClient("gpt-realtime")

    async with RealtimeAgent(client).start_conversation() as conversation:
        client.session.emit(
            *_response("resp_1", _usage("resp_1", UsageDetails(input_token_count=10, output_token_count=20))),
            *_response("resp_2", _usage("resp_2", UsageDetails(input_token_count=30, output_token_count=40))),
        )
        await _until_response_done(conversation, "resp_2")

    durations = _metric_points(reader, OtelAttr.LLM_OPERATION_DURATION)
    tokens = _metric_points(reader, OtelAttr.LLM_TOKEN_USAGE)
    assert [point.count for point in durations] == [2]
    assert sorted((_point_attribute(point, OtelAttr.T_TYPE), point.count, point.sum) for point in tokens) == [
        ("input", 2, 40),
        ("output", 2, 60),
    ]


# endregion


class TestVerification:
    async def test_the_conversation_span_opens_inside_conversation_middleware(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        """Conversation middleware opens a span "guardrail" around call_next(): the invoke_agent span is its child."""
        tracer = trace.get_tracer("app")

        async def guardrail(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
            with tracer.start_as_current_span("guardrail"):
                await call_next()

        async with RealtimeAgent(FakeRealtimeClient(), middleware=[guardrail]).start_conversation():
            pass

        spans = {span.name.split(" ")[0]: span for span in span_exporter.get_finished_spans()}
        assert _parent_id(spans["invoke_agent"]) == _span_id(spans["guardrail"])

    async def test_a_response_cancelled_by_the_client_finishes_with_client_cancelled(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        """resp_1 ends cancelled with reason "client_cancelled": finish reasons ("client_cancelled",)."""
        [span] = await _spans_of_response_ending(span_exporter, status="cancelled", reason="client_cancelled")

        assert _attributes(span, OtelAttr.FINISH_REASONS) == (("client_cancelled",),)

    async def test_a_response_cut_at_max_output_tokens_finishes_with_length(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        """resp_1 ends incomplete with reason "max_output_tokens": finish reasons ("length",), status UNSET."""
        [span] = await _spans_of_response_ending(span_exporter, status="incomplete", reason="max_output_tokens")

        assert _attributes(span, OtelAttr.FINISH_REASONS) == (("length",),)
        assert span.status.status_code == trace.StatusCode.UNSET

    async def test_a_topology_agents_conversation_span_names_it_rather_than_its_first_speaker(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        """An agent "Front desk" whose calls start with "Receptionist" speaking: the conversation span is
        "invoke_agent Front desk", while resp_1's span names "Receptionist"."""
        client = FakeRealtimeClient()
        receptionist = RealtimeAgent(client, name="Receptionist")

        class FrontDesk(RealtimeAgent[Any]):
            def _start_participant(self) -> RawRealtimeAgent[Any]:
                return receptionist

        async with FrontDesk(client, name="Front desk").start_conversation() as conversation:
            client.session.emit(*_response("resp_1"))
            await _until_response_done(conversation)

        [conversation_span] = _spans(span_exporter, "invoke_agent")
        [response_span] = _spans(span_exporter, "realtime_inference")
        assert conversation_span.name == "invoke_agent Front desk"
        assert _attributes(response_span, OtelAttr.AGENT_NAME) == ("Receptionist",)
