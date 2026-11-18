# Copyright (c) Microsoft. All rights reserved.

import asyncio
import os
import re
import wave
from collections.abc import Awaitable, Callable

from agent_framework import (
    AgentResponseUpdate,
    ConversationContext,
    ConversationMiddleware,
    Message,
    MiddlewareTermination,
    RealtimeAgent,
    RealtimeConversation,
    RealtimeEvent,
)
from agent_framework.foundry import AzureVoice, VoiceLiveClient, VoiceLiveOptions
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv

load_dotenv()

"""
Voice Live Guardrail Example

This sample puts a guardrail around a live call with conversation middleware. Its hooks see the whole call:

- a send hook removes card numbers from what the caller says before it reaches the model;
- an update hook stops a reply that gives investment advice: it interrupts the reply, drops the rest of it, and has
  the agent say a fixed line instead;
- a close hook reports what the guardrail did once the call ends.

The sample sends text and saves the spoken replies to voice_live_guardrail.wav, so it needs no microphone or speaker.
The update hook runs as the app reads each update, so it can await interrupt() and say(). History keeps what the
provider sent, before the hooks change it.

Prerequisites:
- Install the realtime extra: pip install "agent-framework-foundry[realtime]" --pre
- Set AZURE_VOICELIVE_ENDPOINT and AZURE_VOICELIVE_MODEL, for example a managed model such as gpt-realtime.
- Set AZURE_VOICELIVE_API_KEY, or run `az login` to use AzureCliCredential.

Realtime agents are experimental.
"""

REPLY_PATH = "voice_live_guardrail.wav"
OUTPUT_SAMPLE_RATE = 24_000
CARD_NUMBER = re.compile(r"\b(?:\d[ -]?){12,15}\d\b")
BLOCKED_WORDS = ("stock", "invest", "shares")


class BankingGuardrail(ConversationMiddleware):
    """Keeps card numbers away from the model, and investment advice away from the caller."""

    def __init__(self) -> None:
        self.redacted = 0
        self.stopped: set[str] = set()

    async def process(self, context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        replies: dict[str, str] = {}
        own_lines: set[str] = set()

        def remove_card_numbers(item: Message | RealtimeEvent) -> Message | None:
            if not isinstance(item, Message) or not CARD_NUMBER.search(item.text):
                return None
            self.redacted += 1
            return Message(item.role, [CARD_NUMBER.sub("[card number removed]", item.text)])

        async def stop_investment_advice(update: AgentResponseUpdate) -> None:
            if update.response_id in self.stopped:
                raise MiddlewareTermination("The reply was stopped.")
            if update.role != "assistant" or not update.response_id or update.response_id in own_lines:
                return
            reply = replies[update.response_id] = replies.get(update.response_id, "") + update.text
            if any(word in reply.lower() for word in BLOCKED_WORDS) and context.conversation is not None:
                self.stopped.add(update.response_id)
                await context.conversation.interrupt()
                # The guardrail's own line mentions what it blocks, so it mustn't stop that line too.
                own_lines.add(await context.conversation.say("Sorry, I can't give investment advice."))
                raise MiddlewareTermination("The reply was stopped.")

        def report(error: Exception | None) -> None:
            print(f"Guardrail: removed {self.redacted} card number(s) and stopped {len(self.stopped)} reply(ies).")

        context.send_hooks.append(remove_card_numbers)
        context.update_hooks.append(stop_investment_advice)
        context.close_hooks.append(report)
        await call_next()


async def read_replies(
    conversation: RealtimeConversation[VoiceLiveOptions], guardrail: BankingGuardrail, reply_audio: bytearray
) -> None:
    """Print the spoken lines the caller hears, and keep their audio, until a reply that wasn't stopped finishes."""
    update: AgentResponseUpdate
    async for update in conversation:
        for content in update.contents:
            if content.type == "text" and update.role == "assistant" and content.status == "completed":
                print(f"Agent: {content.text}")
            elif content.type == "data" and content.has_top_level_media_type("audio"):
                reply_audio.extend(content.get_data_bytes())
            elif content.type == "error":
                print(f"Error: {content.message}")
            elif (
                content.type == "realtime_event"
                and content.kind == "response_done"
                and update.response_id not in guardrail.stopped
            ):
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
    guardrail = BankingGuardrail()
    agent = RealtimeAgent(
        VoiceLiveClient(credential=credential),
        instructions="You are Contoso Bank's phone assistant. Answer every question in one or two sentences.",
        default_options=options,
        middleware=[guardrail],
    )
    caller = "My card number is 4111 1111 1111 1111. Which stocks should I buy with my savings?"
    reply_audio = bytearray()

    try:
        async with agent.start_conversation() as conversation:
            print(f"Caller: {caller}")
            await conversation.send(caller)
            await read_replies(conversation, guardrail, reply_audio)
            print(f"The model heard: {conversation.history[0].text}")
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

Caller: My card number is 4111 1111 1111 1111. Which stocks should I buy with my savings?
Agent: Sorry, I can't give investment advice.
The model heard: My card number is [card number removed]. Which stocks should I buy with my savings?
Guardrail: removed 1 card number(s) and stopped 1 reply(ies).
Saved 2.6 seconds of reply audio to voice_live_guardrail.wav.
"""
