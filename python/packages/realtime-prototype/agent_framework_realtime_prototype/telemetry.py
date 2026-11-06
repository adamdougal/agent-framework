# PROTOTYPE, never merged (adamdougal/agent-framework#10).
"""In-memory OpenTelemetry capture, so scenarios and the debugger can show the span tree."""

from __future__ import annotations

from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

EXPORTER = InMemorySpanExporter()
_configured = False


def configure() -> InMemorySpanExporter:
    global _configured
    if not _configured:
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(EXPORTER))
        trace.set_tracer_provider(provider)
        from agent_framework.observability import enable_instrumentation

        enable_instrumentation(enable_sensitive_data=True)
        _configured = True
    return EXPORTER


def span_tree(trace_id: int | None = None) -> list[dict[str, Any]]:
    """Finished spans as a nested tree, optionally for one trace."""
    spans = [span for span in EXPORTER.get_finished_spans() if trace_id is None or span.context.trace_id == trace_id]
    nodes = {
        span.context.span_id: {
            "name": span.name,
            "span_id": format(span.context.span_id, "016x"),
            "trace_id": format(span.context.trace_id, "032x"),
            "kind": span.kind.name,
            "status": span.status.status_code.name,
            "start": span.start_time,
            "duration_ms": round((span.end_time - span.start_time) / 1e6, 1),
            "attributes": {
                key: (list(value) if isinstance(value, tuple) else value)
                for key, value in (span.attributes or {}).items()
                if not key.startswith("gen_ai.tool.call.arguments") and key != "gen_ai.system_instructions"
            },
            "events": [event.name for event in span.events],
            "children": [],
            "parent": span.parent.span_id if span.parent else None,
        }
        for span in spans
    }
    roots = []
    for node in sorted(nodes.values(), key=lambda node: node["start"]):
        parent = nodes.get(node["parent"]) if node["parent"] else None
        (parent["children"] if parent else roots).append(node)
    return roots


def render(nodes: list[dict[str, Any]], indent: int = 0) -> str:
    lines = []
    for node in nodes:
        attributes = node["attributes"]
        detail = ", ".join(
            f"{key.split('.')[-1]}={attributes[key]}"
            for key in (
                "gen_ai.agent.name",
                "gen_ai.tool.name",
                "gen_ai.response.finish_reasons",
                "error.type",
                "gen_ai.transfer.target.name",
                "agent_framework.handoff.target",
            )
            if key in attributes
        )
        status = "" if node["status"] == "UNSET" else f" [{node['status']}]"
        lines.append(f"{'  ' * indent}{node['name']} ({node['duration_ms']} ms){status} {detail}".rstrip())
        lines.append(render(node["children"], indent + 1)) if node["children"] else None
    return "\n".join(line for line in lines if line)
