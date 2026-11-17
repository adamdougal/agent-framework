# Copyright (c) Microsoft. All rights reserved.

"""
Voice Live Run Example

This sample takes one half-duplex turn with a realtime agent. `agent.run()` sends the caller's text, lets the agent
call its tool and answer, then returns once the reply has finished, as any agent's `run()` does. The sample prints the
agent's transcript and saves its spoken reply to voice_live_run_reply.wav, so it needs no microphone or speaker.

Because `run()` returns an `AgentResponse`, a realtime agent can stand in wherever an agent is run, such as a workflow
step or another agent's tool. For a live, two-way call, use `start_conversation()` instead.

Prerequisites:
- Install the realtime extra: pip install "agent-framework-foundry[realtime]" --pre
- Set AZURE_VOICELIVE_ENDPOINT and AZURE_VOICELIVE_MODEL, for example a managed model such as gpt-realtime.
- Set AZURE_VOICELIVE_API_KEY, or run `az login` to use AzureCliCredential.

Realtime agents are experimental.
"""

import asyncio
import os
import wave

from agent_framework import RealtimeAgent, tool
from agent_framework.foundry import AzureVoice, VoiceLiveClient, VoiceLiveOptions
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv

load_dotenv()

REPLY_PATH = "voice_live_run_reply.wav"
OUTPUT_SAMPLE_RATE = 24_000


@tool
def get_opening_hours(day: str) -> str:
    """Get Contoso Dental's opening hours for a day of the week."""
    return "Closed" if day.lower() in ("saturday", "sunday") else "9 AM to 5 PM"


async def main() -> None:
    # A CLI credential is made only when no key is configured; the sample owns it, so it closes it.
    credential = None if os.getenv("AZURE_VOICELIVE_API_KEY") else AzureCliCredential()
    options: VoiceLiveOptions = {"voice": AzureVoice(type="azure-standard", name="en-US-AvaNeural")}
    agent = RealtimeAgent(
        VoiceLiveClient(credential=credential),
        name="receptionist",
        instructions="You are the receptionist at Contoso Dental. Answer in one short sentence.",
        tools=[get_opening_hours],
        default_options=options,
    )

    try:
        response = await agent.run("Are you open on Saturday?")
    finally:
        if credential is not None:
            await credential.close()

    # The reply can span messages, such as a line the model speaks before it calls the tool, then its answer.
    for message in response.messages:
        if message.role == "assistant" and message.text:
            print(f"Agent: {message.text}")
    reply_audio = b"".join(
        content.get_data_bytes()
        for message in response.messages
        for content in message.contents
        if content.type == "data"
    )
    with wave.open(REPLY_PATH, "wb") as reply_file:
        reply_file.setnchannels(1)
        reply_file.setsampwidth(2)
        reply_file.setframerate(OUTPUT_SAMPLE_RATE)
        reply_file.writeframes(reply_audio)
    print(f"Saved {len(reply_audio) / (2 * OUTPUT_SAMPLE_RATE):.1f} seconds of reply audio to {REPLY_PATH}.")


if __name__ == "__main__":
    asyncio.run(main())

"""
Sample output:

Agent: I'll quickly check our Saturday hours for you.
Agent: We're closed on Saturday.
Saved 4.5 seconds of reply audio to voice_live_run_reply.wav.
"""
