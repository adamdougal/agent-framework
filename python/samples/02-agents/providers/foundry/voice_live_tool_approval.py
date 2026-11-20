# Copyright (c) Microsoft. All rights reserved.

import asyncio
import os

from agent_framework import AgentResponseUpdate, Content, RealtimeAgent, RealtimeConversation, tool
from agent_framework.foundry import AzureVoice, VoiceLiveClient, VoiceLiveOptions
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv

load_dotenv()

"""
Voice Live Tool Approval Example

This sample gives a realtime agent a tool that needs approval before it runs. A caller asks for a refund, the agent
calls issue_refund, and the conversation streams a function_approval_request instead of running it. The sample asks
you at the terminal, as a supervisor would, and sends your decision back with send(...). Approved, the refund runs and
the agent confirms it; rejected, the agent tells the caller it couldn't be done. The caller can keep talking while the
call waits for the decision. The sample sends text and prints the transcript, so it needs no microphone or speaker.

Prerequisites:
- Install the realtime extra: pip install "agent-framework-foundry[realtime]" --pre
- Set AZURE_VOICELIVE_ENDPOINT and AZURE_VOICELIVE_MODEL, for example a managed model such as gpt-realtime.
- Set AZURE_VOICELIVE_API_KEY, or run `az login` to use AzureCliCredential.

Realtime agents are experimental.
"""


@tool(approval_mode="always_require")
def issue_refund(order_id: str, amount: float) -> str:
    """Refund an amount, in pounds, on an order."""
    return f"Refunded £{amount:.2f} on order {order_id}."


async def ask_the_supervisor(conversation: RealtimeConversation[VoiceLiveOptions], request: Content) -> None:
    """Asks at the terminal, off the update loop, so the conversation keeps streaming while you decide."""
    call = request.function_call
    assert call is not None
    answer = await asyncio.to_thread(input, f"Approve {call.name}({call.arguments})? [y/N] ")
    await conversation.send(request.to_function_approval_response(approved=answer.strip().lower() == "y"))


async def read_reply(conversation: RealtimeConversation[VoiceLiveOptions]) -> None:
    """Print the transcript and ask for approval until a reply ends without calling a tool, even a failed one."""
    decisions: list[asyncio.Task[None]] = []
    update: AgentResponseUpdate
    async for update in conversation:
        for content in update.contents:
            if content.type == "text" and update.role == "assistant" and content.status == "completed":
                print(f"Agent: {content.text}")
            elif content.type == "function_approval_request":
                decisions.append(asyncio.create_task(ask_the_supervisor(conversation, content)))
            elif content.type == "function_result":
                print(f"Result: {content.result}")
            elif content.type == "error":
                print(f"Error: {content.message}")
            elif (
                content.type == "realtime_event"
                and content.kind == "response_done"
                and update.finish_reason != "tool_calls"
            ):
                await asyncio.gather(*decisions)
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
            "You are Contoso's refunds line. When the caller asks for a refund, call issue_refund straight away "
            "without saying anything first, then tell them the outcome in one sentence."
        ),
        tools=[issue_refund],
        default_options=options,
    )

    try:
        async with agent.start_conversation() as conversation:
            print("Caller: Please refund twenty pounds on order 1234.")
            await conversation.send("Please refund twenty pounds on order 1234.")
            await read_reply(conversation)
    finally:
        if credential is not None:
            await credential.close()


if __name__ == "__main__":
    asyncio.run(main())

"""
Sample output (approved):

Caller: Please refund twenty pounds on order 1234.
Approve issue_refund({"amount":20,"order_id":"1234"})? [y/N] y
Result: Refunded £20.00 on order 1234.
Agent: The £20 refund has been issued on order 1234.

Sample output (rejected):

Caller: Please refund twenty pounds on order 1234.
Approve issue_refund({"amount":20,"order_id":"1234"})? [y/N] n
Result: Error: Tool call invocation was rejected by user.
Agent: The refund couldn't be processed because the tool call was rejected.
"""
