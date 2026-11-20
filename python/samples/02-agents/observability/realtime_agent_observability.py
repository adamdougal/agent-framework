# Copyright (c) Microsoft. All rights reserved.

import asyncio
import os
from collections import defaultdict
from collections.abc import Sequence

from agent_framework import RealtimeAgent, tool
from agent_framework.foundry import AzureVoice, VoiceLiveClient, VoiceLiveOptions
from agent_framework.observability import configure_otel_providers, get_tracer
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

load_dotenv()

"""
Realtime Agent Observability Example

This sample traces a live call on Azure Voice Live. Each call is one trace: the conversation is an invoke_agent span,
each response the model creates is a realtime_inference span beneath it, with the response's token usage and finish
reason, and each tool call is an execute_tool span beside them. The conversation span sums the call's usage. No audio
or transcripts are recorded. The sample sends one text question, prints the agent's reply, then prints the call's
spans as a tree, so it needs no microphone, speaker or observability backend.

Prerequisites:
- Install the realtime extra: pip install "agent-framework-foundry[realtime]" --pre
- Set AZURE_VOICELIVE_ENDPOINT and AZURE_VOICELIVE_MODEL, for example a managed model such as gpt-realtime.
- Set AZURE_VOICELIVE_API_KEY, or run `az login` to use AzureCliCredential.
- Optionally, OTEL_EXPORTER_OTLP_ENDPOINT to send the spans to a backend as well.

Realtime agents are experimental.
"""


@tool
def get_current_time() -> str:
    """Get the current time."""
    return "It's twelve noon."


def print_trace(spans: Sequence[ReadableSpan]) -> None:
    """Print the finished spans as a tree, with each response's finish reasons."""
    children: defaultdict[int | None, list[ReadableSpan]] = defaultdict(list)
    for span in spans:
        children[span.parent.span_id if span.parent else None].append(span)

    def print_children(parent_id: int | None, depth: int) -> None:
        for span in sorted(children[parent_id], key=lambda child: child.start_time or 0):
            finish_reasons = (span.attributes or {}).get("gen_ai.response.finish_reasons")
            suffix = f"  finish_reasons {list(finish_reasons)}" if isinstance(finish_reasons, tuple) else ""
            print(f"{'    ' * depth}{span.name}{suffix}")
            if span.context is not None:
                print_children(span.context.span_id, depth + 1)

    print_children(None, 0)


async def main() -> None:
    # Keeps the spans in memory to print at the end; OTEL_* and ENABLE_* environment variables can add exporters.
    spans = InMemorySpanExporter()
    configure_otel_providers(exporters=[spans])
    # A CLI credential is made only when no key is configured; the sample owns it, so it closes it.
    credential = None if os.getenv("AZURE_VOICELIVE_API_KEY") else AzureCliCredential()
    options: VoiceLiveOptions = {
        "voice": AzureVoice(type="azure-standard", name="en-US-AvaNeural"),
        # The sample sends text, so it turns off listening for the caller's speech.
        "turn_detection": None,
    }
    agent = RealtimeAgent(
        VoiceLiveClient(credential=credential),
        name="concierge",
        instructions="When asked the time, call get_current_time, then answer in one short sentence.",
        tools=[get_current_time],
        default_options=options,
    )

    try:
        # The conversation span nests under whatever span is current when the conversation opens.
        with get_tracer().start_as_current_span("Scenario: Realtime call"):
            async with agent.start_conversation() as conversation:
                print("Caller: What time is it?")
                await conversation.send("What time is it?")
                async for update in conversation:
                    for content in update.contents:
                        if content.type == "text" and update.role == "assistant" and content.status == "completed":
                            print(f"{update.author_name}: {content.text}")
                    # A failed reply has no finish reason, and one that called a tool is followed by its answer.
                    if update.finish_reason != "tool_calls" and any(
                        content.type == "realtime_event" and content.kind == "response_done"
                        for content in update.contents
                    ):
                        break
    finally:
        if credential is not None:
            await credential.close()

    # The provider exports spans in batches, so flush them before reading.
    tracer_provider = trace.get_tracer_provider()
    if isinstance(tracer_provider, TracerProvider):
        tracer_provider.force_flush()
    print()
    print_trace(spans.get_finished_spans())


if __name__ == "__main__":
    asyncio.run(main())

"""
Sample output:

Caller: What time is it?
concierge: I'll check the current time for you.
concierge: It's twelve noon.

Scenario: Realtime call
    invoke_agent concierge
        realtime_inference gpt-realtime-2.1  finish_reasons ['tool_call']
        execute_tool get_current_time
        realtime_inference gpt-realtime-2.1  finish_reasons ['stop']
"""
