# Copyright (c) Microsoft. All rights reserved.

import asyncio
import os
import wave

from agent_framework import AgentResponseUpdate, RealtimeAgent, RealtimeConversation, tool
from agent_framework.foundry import AzureVoice, VoiceLiveClient, VoiceLiveOptions
from agent_framework.orchestrations import RealtimeHandoffBuilder
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv

load_dotenv()

"""
Voice Live Handoff Example

This sample moves a live call from a receptionist to a billing specialist without reconnecting. RealtimeHandoffBuilder
gives the receptionist a handoff_to_billing tool. When the caller asks about their bill, the model calls it, the
realtime session switches to billing's instructions, tools and voice, and billing answers in its own voice. The
sample sends text and saves the spoken replies to voice_live_handoff.wav, so it needs no microphone or speaker.

The built agent owns the call: its instructions, here a line asking for silent handoffs, are appended to every
participant's, and its options hold for the whole call. Each participant brings who's speaking: its instructions,
tools and voice. On Voice Live, keep the participants' voices in one family, Azure or OpenAI, because Voice Live
rejects a voice change between them.

Prerequisites:
- Install the realtime extra and orchestrations: pip install "agent-framework-foundry[realtime]"
  agent-framework-orchestrations --pre
- Set AZURE_VOICELIVE_ENDPOINT and AZURE_VOICELIVE_MODEL, for example a managed model such as gpt-realtime.
- Set AZURE_VOICELIVE_API_KEY, or run `az login` to use AzureCliCredential.

Realtime agents are experimental.
"""

REPLY_PATH = "voice_live_handoff.wav"
OUTPUT_SAMPLE_RATE = 24_000


@tool
def get_balance() -> str:
    """Get the caller's account balance and due date."""
    return "The balance is 42 pounds, due on Friday."


async def read_until_answered(conversation: RealtimeConversation[VoiceLiveOptions], reply_audio: bytearray) -> None:
    """Print who says what, and keep the audio, until an agent finishes a reply without calling a tool."""
    update: AgentResponseUpdate
    async for update in conversation:
        for content in update.contents:
            if content.type == "text" and update.role == "assistant" and content.status == "completed":
                print(f"{update.author_name}: {content.text}")
            elif content.type == "data" and content.has_top_level_media_type("audio"):
                reply_audio.extend(content.get_data_bytes())
            elif content.type == "realtime_event" and content.kind == "handoff_completed":
                handoff = content.additional_properties["core"]
                print(f"[handed off from {handoff['source']} to {handoff['target']}]")
            elif content.type == "function_result":
                print(f"[tool result: {content.result}]")
            elif content.type == "error":
                print(f"[error: {content.message}]")
        if update.finish_reason == "stop":
            return


async def main() -> None:
    # A CLI credential is made only when no key is configured; the sample owns it, so it closes it.
    credential = None if os.getenv("AZURE_VOICELIVE_API_KEY") else AzureCliCredential()
    client: VoiceLiveClient[VoiceLiveOptions] = VoiceLiveClient(credential=credential)
    receptionist_options: VoiceLiveOptions = {"voice": AzureVoice(type="azure-standard", name="en-US-AvaNeural")}
    billing_options: VoiceLiveOptions = {"voice": AzureVoice(type="azure-standard", name="en-US-AndrewNeural")}
    receptionist = RealtimeAgent(
        client,
        name="receptionist",
        instructions="You are the receptionist at Contoso. Hand billing questions to the billing specialist.",
        default_options=receptionist_options,
    )
    billing = RealtimeAgent(
        client,
        name="billing",
        description="Answers questions about the caller's bill, balance and payments.",
        instructions="You are Contoso's billing specialist. Look up the balance, then answer in one sentence.",
        tools=[get_balance],
        default_options=billing_options,
    )
    call_options: VoiceLiveOptions = {
        "instructions": (
            "When you hand off, call the handoff tool straight away without saying anything first. After a handoff, "
            "don't greet the caller again; just help them."
        ),
        "output_modalities": ["audio"],
        # The sample sends text, so it turns off listening for the caller's speech.
        "turn_detection": None,
    }
    agent = (
        RealtimeHandoffBuilder(participants=[receptionist, billing], default_options=call_options)
        .with_start_agent(receptionist)
        .add_handoff(receptionist, [billing])
        .build()
    )
    reply_audio = bytearray()

    try:
        async with agent.start_conversation() as conversation:
            print("Caller: Hi, how much do I owe on my account?")
            await conversation.send("Hi, how much do I owe on my account?")
            await read_until_answered(conversation, reply_audio)
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

Caller: Hi, how much do I owe on my account?
[handed off from receptionist to billing]
[tool result: {"handoff_to": "billing"}]
[tool result: The balance is 42 pounds, due on Friday.]
billing: You owe 42 pounds, due on Friday.
Saved 2.9 seconds of reply audio to voice_live_handoff.wav.
"""
