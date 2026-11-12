# Copyright (c) Microsoft. All rights reserved.

import asyncio
import base64
import os
import wave

from agent_framework import AgentResponseUpdate, RealtimeAgent, RealtimeConversation
from agent_framework.foundry import AzureVoice, VoiceLiveClient, VoiceLiveOptions
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv

load_dotenv()

"""
Voice Live Session Update Example

This sample changes a live realtime conversation's instructions and voice mid-call, without reconnecting. A
receptionist answers a question in English. The caller then asks to continue in Spanish, so the app calls
update_session() with Spanish instructions and a Spanish voice, and the next answer comes back in Spanish in that
voice. The sample sends text and saves the spoken replies to voice_live_session_update.wav, so it needs no microphone
or speaker.

update_session() returns once Voice Live acknowledges the update, so the response the next send() requests already
uses the new instructions and voice. Its options merge over the session's current settings, so the new instructions
replace the old ones rather than adding to them.

Prerequisites:
- Install the realtime extra: pip install "agent-framework-foundry[realtime]" --pre
- Set AZURE_VOICELIVE_ENDPOINT and AZURE_VOICELIVE_MODEL, for example a managed model such as gpt-realtime.
- Set AZURE_VOICELIVE_API_KEY, or run `az login` to use AzureCliCredential.

Realtime agents are experimental.
"""

REPLY_PATH = "voice_live_session_update.wav"
OUTPUT_SAMPLE_RATE = 24_000
RECEPTIONIST = "You are the receptionist at Contoso Dental, open 9 AM to 5 PM on weekdays. Answer in one sentence."


async def read_reply(conversation: RealtimeConversation[VoiceLiveOptions], reply_audio: bytearray) -> None:
    """Print the agent's transcript and keep its audio until the reply finishes, or the conversation ends."""
    update: AgentResponseUpdate
    async for update in conversation:
        for content in update.contents:
            if content.type == "text" and update.role == "assistant" and content.merge_mode == "append":
                print(content.text, end="", flush=True)
            elif content.type == "data" and content.uri and content.has_top_level_media_type("audio"):
                # The WAV writer needs the decoded 24 kHz, 16-bit mono PCM, not the content's base64 URI.
                reply_audio.extend(base64.b64decode(content.uri.split(";base64,", 1)[1]))
        if update.finish_reason is not None:
            print()
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
    agent = RealtimeAgent(VoiceLiveClient(credential=credential), instructions=RECEPTIONIST, default_options=options)
    reply_audio = bytearray()

    try:
        async with agent.start_conversation() as conversation:
            print("Caller: What are your opening hours?\nAgent: ", end="")
            await conversation.send("What are your opening hours?")
            await read_reply(conversation, reply_audio)

            print("Caller: ¿Podemos seguir en español?")
            await conversation.update_session(
                options={
                    "instructions": f"{RECEPTIONIST} The caller prefers Spanish, so answer only in Spanish.",
                    "voice": AzureVoice(type="azure-standard", name="es-ES-ElviraNeural"),
                }
            )

            print("Caller: ¿Abren los sábados?\nAgent: ", end="")
            await conversation.send("¿Abren los sábados?")
            await read_reply(conversation, reply_audio)
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

Caller: What are your opening hours?
Agent: We're open Monday through Friday from 9 AM to 5 PM.
Caller: ¿Podemos seguir en español?
Caller: ¿Abren los sábados?
Agent: No, solo abrimos de lunes a viernes de 9 AM a 5 PM.
Saved 8.8 seconds of reply audio to voice_live_session_update.wav.
"""
