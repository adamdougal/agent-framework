# Copyright (c) Microsoft. All rights reserved.

import asyncio
import base64
import os
import wave

from agent_framework import Message
from agent_framework.foundry import AzureVoice, VoiceLiveClient, VoiceLiveOptions
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv

load_dotenv()

"""
Voice Live Session Example

This sample drives an Azure Voice Live realtime session directly, without an agent. It sends one user text message,
prints the agent's transcript as it streams, and saves the spoken reply to voice_live_reply.wav, so it needs no
microphone or speaker.

Prerequisites:
- Install the realtime extra: pip install "agent-framework-foundry[realtime]" --pre
- Set AZURE_VOICELIVE_ENDPOINT and AZURE_VOICELIVE_MODEL, for example a managed model such as gpt-realtime.
- Set AZURE_VOICELIVE_API_KEY, or run `az login` to use AzureCliCredential.

Voice Live support is experimental.
"""

REPLY_PATH = "voice_live_reply.wav"
OUTPUT_SAMPLE_RATE = 24_000


async def main() -> None:
    # A CLI credential is made only when no key is configured; the sample owns it, so it closes it.
    credential = None if os.getenv("AZURE_VOICELIVE_API_KEY") else AzureCliCredential()
    client = VoiceLiveClient(credential=credential)
    options: VoiceLiveOptions = {
        "instructions": (
            "You are the receptionist at Contoso Dental, open 9 AM to 5 PM on weekdays. Answer in one sentence."
        ),
        "voice": AzureVoice(type="azure-standard", name="en-US-AvaNeural"),
        "output_modalities": ["audio"],
        # The sample sends text, so it turns off listening for the caller's speech.
        "turn_detection": None,
    }
    reply_audio = bytearray()

    try:
        async with await client.start_session(options=options) as session:

            async def read_reply() -> None:
                async for event in session.receive():
                    if event.kind == "output_text" and event.content and event.content.merge_mode == "append":
                        print(event.content.text, end="", flush=True)
                    elif event.kind == "output_audio" and event.content and event.content.uri:
                        # The WAV writer needs the decoded 24 kHz, 16-bit mono PCM, not the content's base64 URI.
                        reply_audio.extend(base64.b64decode(event.content.uri.split(";base64,", 1)[1]))
                    elif event.kind == "response_done":
                        return

            # Control calls such as create_response() resolve while something reads the session's events.
            reader = asyncio.create_task(read_reply())
            await session.send(Message("user", ["Hi! What are your opening hours?"]))
            print("Agent: ", end="")
            await session.create_response()
            await reader
            print()
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

Agent: We're open weekdays from 9 AM to 5 PM.
Saved 3.3 seconds of reply audio to voice_live_reply.wav.
"""
