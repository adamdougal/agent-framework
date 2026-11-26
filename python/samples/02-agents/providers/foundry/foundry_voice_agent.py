# Copyright (c) Microsoft. All rights reserved.

import asyncio
import os
import wave

from agent_framework import AgentResponseUpdate, tool
from agent_framework.foundry import FoundryVoiceAgent
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv

load_dotenv()

"""
Foundry Voice Agent Example

This sample talks to an agent published in Microsoft Foundry as a voice agent. The published agent definition
supplies the agent's instructions, voice, tool choice and tools; the app supplies Python implementations for the
definition's functions, matched by name. The sample sends one text message, prints the agent's transcript and tool
results, and saves the spoken reply to foundry_voice_agent.wav, so it needs no microphone or speaker.

Entering the agent with `async with` reads its definition once, through Foundry's agents API, and pins every call to
that version. Pin FOUNDRY_AGENT_VERSION to choose the version yourself.

Prerequisites:
- Install the realtime extra: pip install "agent-framework-foundry[realtime]" --pre
- Publish a voice agent in your Foundry project whose definition declares a get_current_time function, with 24 kHz PCM
  output audio. Definitions published by Semantic Kernel name their tools <Plugin>-<function>, so republish them with
  your Agent Framework tool names.
- Set FOUNDRY_PROJECT_ENDPOINT and FOUNDRY_AGENT_NAME, and optionally FOUNDRY_AGENT_VERSION.
- Run `az login` to use AzureCliCredential.

Foundry voice agents are experimental.
"""

REPLY_PATH = "foundry_voice_agent.wav"
OUTPUT_SAMPLE_RATE = 24_000


# The name matches the function the published definition declares, so this implementation runs its calls.
@tool(name="get_current_time", description="Get the current time.")
def get_current_time() -> str:
    return "It's twelve noon."


async def main() -> None:
    reply_audio = bytearray()
    async with AzureCliCredential() as credential:
        agent = FoundryVoiceAgent(
            project_endpoint=os.getenv("FOUNDRY_PROJECT_ENDPOINT"),
            agent_name=os.getenv("FOUNDRY_AGENT_NAME"),
            agent_version=os.getenv("FOUNDRY_AGENT_VERSION"),
            credential=credential,
            tools=[get_current_time],
        )
        async with agent, agent.start_conversation() as conversation:
            await conversation.send("Hi! What time is it?")
            update: AgentResponseUpdate
            async for update in conversation:
                for content in update.contents:
                    if content.type == "text" and update.role == "assistant" and content.status == "completed":
                        print(f"Agent: {content.text}")
                    elif content.type == "data" and content.has_top_level_media_type("audio"):
                        reply_audio.extend(content.get_data_bytes())
                    elif content.type == "function_result":
                        print(f"[tool result: {content.result}]")
                    elif content.type == "error":
                        print(f"[error: {content.message}]")
                # A failed reply has no finish reason, and one that called a tool is followed by its answer.
                if update.finish_reason != "tool_calls" and any(
                    content.type == "realtime_event" and content.kind == "response_done" for content in update.contents
                ):
                    break

    with wave.open(REPLY_PATH, "wb") as reply_file:
        reply_file.setnchannels(1)
        reply_file.setsampwidth(2)
        reply_file.setframerate(OUTPUT_SAMPLE_RATE)
        reply_file.writeframes(bytes(reply_audio))
    print(f"Saved {len(reply_audio) / (2 * OUTPUT_SAMPLE_RATE):.1f} seconds of reply audio to {REPLY_PATH}.")


if __name__ == "__main__":
    asyncio.run(main())

"""
Sample output:

Agent: I'll quickly check the current time for you.
[tool result: It's twelve noon.]
Agent: It's twelve noon.
Saved 3.7 seconds of reply audio to foundry_voice_agent.wav.
"""
