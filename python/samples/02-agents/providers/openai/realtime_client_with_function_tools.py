# Copyright (c) Microsoft. All rights reserved.

import asyncio
import wave
from typing import Annotated

from agent_framework import AgentResponseUpdate, RealtimeAgent, RealtimeConversation, tool
from agent_framework.openai import OpenAIRealtimeClient, OpenAIRealtimeOptions
from dotenv import load_dotenv
from pydantic import Field

load_dotenv()

"""
OpenAI Realtime Client with Function Tools Example

This sample runs a realtime agent on OpenAI Realtime, with one function tool. It sends one text question, prints the
agent's transcript as it streams, and saves the spoken reply to openai_realtime_reply.wav, so it needs no microphone
or speaker. The agent calls the tool, the conversation runs it and asks for a follow-up response, and the sample stops
at the first response that finishes without calling a tool.

Prerequisites:
- Install the realtime extra: pip install "agent-framework-openai[realtime]" --pre
- Set OPENAI_API_KEY, and OPENAI_REALTIME_MODEL to a realtime model such as gpt-realtime-mini.

Realtime agents are experimental.
"""

REPLY_PATH = "openai_realtime_reply.wav"
OUTPUT_SAMPLE_RATE = 24_000


@tool
def get_weather(location: Annotated[str, Field(description="The city to get the weather for.")]) -> str:
    """Get the weather for a city."""
    return f"The weather in {location} is sunny with a high of 22°C."


async def read_reply(conversation: RealtimeConversation[OpenAIRealtimeOptions], reply_audio: bytearray) -> None:
    """Print the agent's transcript and tool calls, and keep its audio, until a response finishes without a call."""
    update: AgentResponseUpdate
    async for update in conversation:
        for content in update.contents:
            if content.type == "text" and update.role == "assistant" and content.merge_mode == "append":
                print(content.text, end="", flush=True)
            elif content.type == "data" and content.has_top_level_media_type("audio"):
                reply_audio.extend(content.get_data_bytes())
            elif content.type == "function_call":
                print(f"\n[calling {content.name}({content.arguments})]\nAgent: ", end="", flush=True)
            elif content.type == "error":
                print(f"[error: {content.message}]", end="")
            elif content.type == "realtime_event" and content.kind == "response_done" and update.finish_reason == "stop":
                print()
                return


async def main() -> None:
    options: OpenAIRealtimeOptions = {
        "voice": "marin",
        "output_modalities": ["audio"],
        # The sample sends text, so it turns off listening for the caller's speech.
        "turn_detection": None,
    }
    reply_audio = bytearray()

    agent = RealtimeAgent(
        OpenAIRealtimeClient(),
        instructions="You are a weather assistant. Answer in one short sentence.",
        tools=[get_weather],
        default_options=options,
    )
    async with agent.start_conversation() as conversation:
        print("Caller: What's the weather like in Amsterdam?\nAgent: ", end="")
        await conversation.send("What's the weather like in Amsterdam?")
        await read_reply(conversation, reply_audio)

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

Caller: What's the weather like in Amsterdam?
Agent: Let me check the current conditions in Amsterdam so I can give you a quick, clear update.
[calling get_weather({"location":"Amsterdam"})]
Agent: Amsterdam is sunny with a high of 22 degrees Celsius.
Saved 8.6 seconds of reply audio to openai_realtime_reply.wav.
"""
