# Copyright (c) Microsoft. All rights reserved.

import asyncio
import os

from agent_framework import RealtimeAgent, tool
from agent_framework.foundry import AzureVoice, VoiceLiveClient, VoiceLiveOptions
from agent_framework.observability import configure_otel_providers, get_tracer
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv
from opentelemetry.trace.span import format_trace_id

load_dotenv()

"""
Realtime Agent Observability Example

This sample traces a live call on Azure Voice Live. Each call is one trace: the conversation is an invoke_agent span,
each response the model creates is a realtime_inference span beneath it, with the response's token usage and finish
reason, and each tool call is an execute_tool span beside them. The conversation span sums the call's usage. No audio
or transcripts are recorded. The sample sends one text question and prints the agent's reply, so it needs no
microphone or speaker.

Prerequisites:
- Install the realtime extra: pip install "agent-framework-foundry[realtime]" --pre
- Set AZURE_VOICELIVE_ENDPOINT and AZURE_VOICELIVE_MODEL, for example a managed model such as gpt-realtime.
- Set AZURE_VOICELIVE_API_KEY, or run `az login` to use AzureCliCredential.
- An observability backend that receives OTLP, configured with OTEL_EXPORTER_OTLP_ENDPOINT, or set
  ENABLE_CONSOLE_EXPORTERS=true to print the spans to the console.

Realtime agents are experimental.
"""


@tool
def get_current_time() -> str:
    """Get the current time."""
    return "It's twelve noon."


async def main() -> None:
    # Reads the OTEL_* and ENABLE_* environment variables to choose where telemetry goes.
    configure_otel_providers()
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
        with get_tracer().start_as_current_span("Scenario: Realtime call") as scenario:
            print(f"Trace ID: {format_trace_id(scenario.get_span_context().trace_id)}")
            async with agent.start_conversation() as conversation:
                print("Caller: What time is it?")
                await conversation.send("What time is it?")
                async for update in conversation:
                    for content in update.contents:
                        if content.type == "text" and update.role == "assistant" and content.status == "completed":
                            print(f"{update.author_name}: {content.text}")
                    if update.finish_reason == "stop":
                        break
    finally:
        if credential is not None:
            await credential.close()


if __name__ == "__main__":
    asyncio.run(main())

"""
Sample output, with ENABLE_CONSOLE_EXPORTERS=true printing each span as JSON after it:

Trace ID: 6d0c6ec63df9c4a12ecb488d3fc6529b
Caller: What time is it?
concierge: I'm checking the current time for you now.
concierge: It's twelve noon.

The trace holds these spans:

Scenario: Realtime call
└── invoke_agent concierge
    ├── realtime_inference gpt-realtime-2.1      finish_reasons ["tool_call"]
    ├── execute_tool get_current_time
    └── realtime_inference gpt-realtime-2.1      finish_reasons ["stop"]
"""
