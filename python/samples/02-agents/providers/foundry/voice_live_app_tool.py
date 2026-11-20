# Copyright (c) Microsoft. All rights reserved.

import asyncio
import os

from agent_framework import AgentResponseUpdate, Content, FunctionTool, RealtimeAgent, RealtimeConversation
from agent_framework.foundry import AzureVoice, VoiceLiveClient, VoiceLiveOptions
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv

load_dotenv()

"""
Voice Live App-Run Tool Example

This sample gives a realtime agent a tool that the app answers itself. A declaration-only FunctionTool has a name,
description and parameters but no implementation, so the conversation never runs it: when the agent calls it, the call
waits for the app's result. Here the app knows who is calling from its own phone-line state, so it answers
get_caller_account with that account, and the agent reads out the balance. The app can answer as soon as the call
streams, even before the response that made it finishes. The sample sends text and prints the transcript, so it needs
no microphone or speaker.

Prerequisites:
- Install the realtime extra: pip install "agent-framework-foundry[realtime]" --pre
- Set AZURE_VOICELIVE_ENDPOINT and AZURE_VOICELIVE_MODEL, for example a managed model such as gpt-realtime.
- Set AZURE_VOICELIVE_API_KEY, or run `az login` to use AzureCliCredential.

Realtime agents are experimental.
"""

# What the app's phone line knows about the call, which the model can't look up for itself.
CALLER_ACCOUNT = "Account 1234 for Ada Lovelace, balance £42.50, next bill due on the 1st."

get_caller_account = FunctionTool(
    name="get_caller_account",
    description="Look up the calling customer's account from the phone line they called on.",
)


async def read_reply(conversation: RealtimeConversation[VoiceLiveOptions]) -> None:
    """Print the transcript and answer the app-run call until the agent answers with the result."""
    has_result = False
    update: AgentResponseUpdate
    async for update in conversation:
        for content in update.contents:
            if content.type == "text" and update.role == "assistant" and content.status == "completed":
                print(f"Agent: {content.text}")
            elif content.type == "function_call" and content.name == get_caller_account.name:
                print(f"App answers {content.name}: {CALLER_ACCOUNT}")
                await conversation.send(
                    Content.from_function_result(call_id=content.call_id or "", result=CALLER_ACCOUNT)
                )
            elif content.type == "function_result":
                has_result = True
            elif content.type == "error":
                print(f"Error: {content.message}")
            elif content.type == "realtime_event" and content.kind == "response_done" and has_result:
                # The first response to finish after the result is the agent's answer.
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
            "You are Contoso's billing line. When the caller asks about their account, call get_caller_account "
            "straight away without saying anything first, then answer in one sentence."
        ),
        tools=[get_caller_account],
        default_options=options,
    )

    try:
        async with agent.start_conversation() as conversation:
            print("Caller: What's the balance on my account?")
            await conversation.send("What's the balance on my account?")
            await read_reply(conversation)
    finally:
        if credential is not None:
            await credential.close()


if __name__ == "__main__":
    asyncio.run(main())

"""
Sample output:

Caller: What's the balance on my account?
App answers get_caller_account: Account 1234 for Ada Lovelace, balance £42.50, next bill due on the 1st.
Agent: Your current balance is £42.50.
"""
