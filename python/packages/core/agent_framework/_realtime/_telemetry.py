# Copyright (c) Microsoft. All rights reserved.

"""Telemetry for live realtime conversations."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING, Any

from opentelemetry import trace

from .._types import add_usage_details
from ..observability import (
    OtelAttr,
    apply_usage_attributes,
    capture_exception,
    capture_model_response,
    get_agent_span_attributes,
    get_provider_name_attribute,
    get_tracer,
)
from ._updates import core_attributes, finish_reason_of

if TYPE_CHECKING:
    from .._agents import BaseAgent
    from .._types import UsageDetails
    from ._agent import RawRealtimeAgent
    from ._events import RealtimeEvent

_REALTIME_SESSION_ID = "gen_ai.realtime_session.id"


@dataclass
class _Response:
    span: trace.Span
    attributes: dict[str, Any]
    started_at: float
    usage: UsageDetails | None = None


class ConversationTelemetry:
    def __init__(self, owner: RawRealtimeAgent[Any]) -> None:
        self._owner = owner
        self._span: trace.Span = trace.INVALID_SPAN
        self._responses: dict[str, _Response] = {}
        self._session_model: str | None = None
        self._usage: UsageDetails = {}
        self._with_calls: set[str] = set()

    @contextmanager
    def opened(self) -> Generator[None]:
        """Everything the conversation starts while opening, its receive loop included, runs inside its span."""
        attributes = get_agent_span_attributes(self._owner, OtelAttr.AGENT_FRAMEWORK_GEN_AI_SYSTEM)
        self._span = get_tracer().start_span(
            f"invoke_agent {attributes[OtelAttr.AGENT_NAME]}", kind=trace.SpanKind.INTERNAL, attributes=attributes
        )
        try:
            with trace.use_span(self._span, record_exception=False, set_status_on_exception=False):
                yield
        except Exception as error:
            capture_exception(self._span, error)
            self._span.end()
            raise

    def on_event(self, event: RealtimeEvent, active_agent: BaseAgent) -> None:
        core = core_attributes(event)
        if event.kind == "session_created":
            self._session_created(core)
        elif event.kind == "response_created":
            self._response_created(core["response_id"], active_agent)
        elif event.kind == "usage" and event.content is not None and event.content.usage_details:
            self._usage_reported(core["response_id"], event.content.usage_details)
        elif event.kind == "function_call":
            self._with_calls.add(core["response_id"])
        elif event.kind == "response_done":
            self._response_done(core)

    def _session_created(self, core: dict[str, Any]) -> None:
        if session_id := core.get("session_id"):
            self._span.set_attribute(_REALTIME_SESSION_ID, session_id)
        self._session_model = core.get("model")

    def _response_created(self, response_id: str, agent: BaseAgent) -> None:
        client = self._owner.client
        requested_model = getattr(client, "model", None)
        attributes: dict[str, Any] = {
            OtelAttr.OPERATION: "realtime_inference",
            get_provider_name_attribute(): getattr(client, "OTEL_PROVIDER_NAME", "unknown"),
            OtelAttr.RESPONSE_ID: response_id,
            OtelAttr.AGENT_ID: agent.id,
            OtelAttr.AGENT_NAME: agent.name or agent.id,
        }
        if requested_model:
            attributes[OtelAttr.REQUEST_MODEL] = requested_model
        if self._session_model:
            attributes[OtelAttr.RESPONSE_MODEL] = self._session_model
        model = requested_model or self._session_model
        span = get_tracer().start_span(
            f"realtime_inference {model}" if model else "realtime_inference",
            context=trace.set_span_in_context(self._span),
            kind=trace.SpanKind.CLIENT,
            attributes=attributes,
        )
        self._responses[response_id] = _Response(span, attributes, perf_counter())

    def _usage_reported(self, response_id: str, usage: UsageDetails) -> None:
        self._usage = add_usage_details(self._usage, usage)
        if (response := self._responses.get(response_id)) is not None:
            response.usage = usage

    def _response_done(self, core: dict[str, Any]) -> None:
        response_id = core["response_id"]
        if (response := self._responses.pop(response_id, None)) is not None:
            _end_response(response, *_span_outcome(core, response_id in self._with_calls))

    def closed(self, failure: Exception | None) -> None:
        for response in self._responses.values():
            if failure is None:
                _end_response(response, "interrupted", None)
            else:
                _end_response(response, "error", type(failure).__name__)
        usage_attributes: dict[str, Any] = {}
        apply_usage_attributes(usage_attributes, self._usage)
        self._span.set_attributes(usage_attributes)
        if failure is not None:
            capture_exception(self._span, failure)
        self._span.end()


def _span_outcome(response_done: dict[str, Any], made_calls: bool) -> tuple[str | None, str | None]:
    """The finish reason and error type a response span records for how the provider says it ended.

    Spans name a response that called tools ``tool_call``, where updates keep chat's ``tool_calls``.
    """
    if response_done["status"] == "cancelled":
        return response_done.get("reason") or "interrupted", None
    if response_done["status"] == "failed":
        return "error", response_done.get("error_code") or "server_error"
    finish_reason = finish_reason_of(response_done, made_calls)
    return "tool_call" if finish_reason == "tool_calls" else finish_reason, None


def _end_response(response: _Response, finish_reason: str | None, error_type: str | None) -> None:
    attributes = {**response.attributes, OtelAttr.FINISH_REASONS: [finish_reason]}
    if response.usage:
        apply_usage_attributes(attributes, response.usage)
    if error_type is not None:
        attributes[OtelAttr.ERROR_TYPE] = error_type
        response.span.set_status(trace.StatusCode.ERROR)
    capture_model_response(response.span, attributes, perf_counter() - response.started_at)
    response.span.end()
