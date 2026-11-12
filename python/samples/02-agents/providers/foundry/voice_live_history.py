# Copyright (c) Microsoft. All rights reserved.

"""
Voice Live History Example

This sample holds two conversations with a realtime agent on one agent session. The first records the call in the
session's history, and prints `conversation.history` when it ends, as an app would at hang-up. The second conversation
replays that history into its new realtime session, so the agent still knows the caller's name. It sends text, so it
needs no microphone or speaker.

Passing a session adds an in-memory history provider, as it does for a chat agent. With your own context providers,
each exchange reaches their `after_run` during the call.

Prerequisites:
- Install the realtime extra: pip install "agent-framework-foundry[realtime]" --pre
- Set AZURE_VOICELIVE_ENDPOINT and AZURE_VOICELIVE_MODEL, for example a managed model such as gpt-realtime.
- Set AZURE_VOICELIVE_API_KEY, or run `az login` to use AzureCliCredential.

Realtime agents are experimental.
"""

import asyncio
import os

from agent_framework import AgentSession, RealtimeAgent, RealtimeConversation
from agent_framework.foundry import AzureVoice, VoiceLiveClient, VoiceLiveOptions
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv

load_dotenv()


async def ask(conversation: RealtimeConversation[VoiceLiveOptions], text: str) -> None:
    """Send the caller's text, and print the agent's reply as it streams."""
    print(f"Caller: {text}\nAgent: ", end="")
    await conversation.send(text)
    async for update in conversation:
        if update.role == "assistant":
            for content in update.contents:
                if content.type == "text" and content.merge_mode == "append":
                    print(content.text, end="", flush=True)
        # Every response ends with response_done, even one that failed or was cancelled and has no finish reason.
        if any(content.type == "realtime_event" and content.kind == "response_done" for content in update.contents):
            print()
            return


async def main() -> None:
    # A CLI credential is made only when no key is configured; the sample owns it, so it closes it.
    credential = None if os.getenv("AZURE_VOICELIVE_API_KEY") else AzureCliCredential()
    agent = RealtimeAgent(
        VoiceLiveClient(credential=credential),
        name="receptionist",
        instructions="You are the receptionist at Contoso Dental. Answer in one short sentence.",
    )
    options: VoiceLiveOptions = {
        "voice": AzureVoice(type="azure-standard", name="en-US-AvaNeural"),
        # History keeps the caller's spoken turns as their transcripts, so a voice call needs transcription on.
        "transcription": {"model": "gpt-4o-transcribe"},
        # The sample sends text, so it turns off listening for the caller's speech.
        "turn_detection": None,
    }
    session = AgentSession()

    try:
        async with agent.start_conversation(session=session, options=options) as conversation:
            await ask(conversation, "Hi, I'm Ada. Can I book a check-up?")
        print("\nHistory at hang-up:")
        for message in conversation.history:
            print(f"  {message.author_name or message.role}: {message.text}")

        print("\nA new call on the same session:")
        async with agent.start_conversation(session=session, options=options) as conversation:
            await ask(conversation, "Do you remember my name?")
    finally:
        if credential is not None:
            await credential.close()


if __name__ == "__main__":
    asyncio.run(main())

"""
Sample output:

Caller: Hi, I'm Ada. Can I book a check-up?
Agent: Of course, Ada, what day works best for you?

History at hang-up:
  user: Hi, I'm Ada. Can I book a check-up?
  receptionist: Of course, Ada, what day works best for you?

A new call on the same session:
Caller: Do you remember my name?
Agent: Yes, you're Ada.
"""
