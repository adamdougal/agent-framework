# Copyright (c) Microsoft. All rights reserved.

import asyncio
import base64
import contextlib
import os
import wave
from collections.abc import Awaitable, Callable

from agent_framework import AgentResponseUpdate, FunctionInvocationContext, RealtimeAgent, RealtimeConversation, tool
from agent_framework.exceptions import RealtimeActiveResponseException
from agent_framework.foundry import AzureVoice, VoiceLiveClient, VoiceLiveOptions
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv

load_dotenv()

"""
Voice Live Filler Line Example

This sample keeps the caller company while a slow tool runs. Function middleware says a fixed filler line with
say() before the tool starts, so the caller hears "One moment while I look that up." instead of silence, and the agent
answers once the tool's result arrives. The sample sends text and saves the spoken replies to
voice_live_filler_line.wav, so it needs no microphone or speaker.

With an Azure voice, Voice Live speaks the line word for word. A line said from a tool or function middleware doesn't
count as the response to the tool's result, so the conversation still asks for the answer afterwards. say() raises
RealtimeActiveResponseException while the agent is speaking, because nothing is queued, so the middleware skips the
line then.

Prerequisites:
- Install the realtime extra: pip install "agent-framework-foundry[realtime]" --pre
- Set AZURE_VOICELIVE_ENDPOINT and AZURE_VOICELIVE_MODEL, for example a managed model such as gpt-realtime.
- Set AZURE_VOICELIVE_API_KEY, or run `az login` to use AzureCliCredential.

Realtime agents are experimental.
"""

REPLY_PATH = "voice_live_filler_line.wav"
OUTPUT_SAMPLE_RATE = 24_000


@tool
async def look_up_order(order_id: str) -> str:
    """Look up when an order will be delivered."""
    await asyncio.sleep(3)  # a slow order system
    return f"Order {order_id} ships tomorrow and arrives on Friday."


async def filler_line(context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
    """Say a filler line before each tool runs in a live conversation."""
    if context.realtime_conversation is not None:
        # The agent may already be speaking, and nothing is queued, so the line is skipped then.
        with contextlib.suppress(RealtimeActiveResponseException):
            await context.realtime_conversation.say("One moment while I look that up.")
    await call_next()


async def read_replies(conversation: RealtimeConversation[VoiceLiveOptions], reply_audio: bytearray) -> None:
    """Print each spoken line, and keep the audio, until the agent answers with the tool's result."""
    has_result = False
    update: AgentResponseUpdate
    async for update in conversation:
        for content in update.contents:
            if content.type == "text" and update.role == "assistant" and content.status == "completed":
                print(f"Agent: {content.text}")
            elif content.type == "data" and content.uri and content.has_top_level_media_type("audio"):
                # The WAV writer needs the decoded 24 kHz, 16-bit mono PCM, not the content's base64 URI.
                reply_audio.extend(base64.b64decode(content.uri.split(";base64,", 1)[1]))
            elif content.type == "function_result":
                print(f"Tool: {content.result}")
                has_result = True
        if has_result and update.finish_reason == "stop":
            return


async def main() -> None:
    # A CLI credential is made only when no key is configured; the sample owns it, so it closes it.
    credential = None if os.getenv("AZURE_VOICELIVE_API_KEY") else AzureCliCredential()
    options: VoiceLiveOptions = {
        "voice": AzureVoice(type="azure-standard", name="en-US-AvaNeural"),
        "output_modalities": ["audio"],
        # The sample sends text, so it turns off listening for the caller's speech.
        "turn_detection": None,
    }
    agent = RealtimeAgent(
        VoiceLiveClient(credential=credential),
        instructions=(
            "You are Contoso's order line. When the caller asks about an order, call look_up_order straight away "
            "without saying anything first, then answer in one sentence."
        ),
        tools=[look_up_order],
        default_options=options,
        middleware=[filler_line],
    )
    reply_audio = bytearray()

    try:
        async with agent.start_conversation() as conversation:
            print("Caller: Where is order 1234?")
            await conversation.send("Where is order 1234?")
            await read_replies(conversation, reply_audio)
    finally:
        if credential is not None:
            await credential.close()

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

Caller: Where is order 1234?
Agent: One moment while I look that up.
Tool: Order 1234 ships tomorrow and arrives on Friday.
Agent: Order 1234 ships tomorrow and will arrive on Friday.
Saved 5.9 seconds of reply audio to voice_live_filler_line.wav.
"""
