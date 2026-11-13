# Copyright (c) Microsoft. All rights reserved.

import asyncio
import os
import sys
import wave

from agent_framework import AgentResponseUpdate, MCPStdioTool, RealtimeAgent, RealtimeConversation
from agent_framework.foundry import AzureVoice, VoiceLiveClient, VoiceLiveOptions
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv
from mcp.server.mcpserver import MCPServer

load_dotenv()

"""
Voice Live MCP Tool Example

This sample gives a realtime agent a local MCP tool. A caller asks when their order can be delivered, the agent calls
the orders MCP server's get_delivery_slots tool, and it speaks the slots back. Started with --server, the sample runs
itself as that small orders server over stdio, so it needs no other process. It sends text and saves the spoken reply
to voice_live_mcp_tool.wav, so it needs no microphone or speaker.

The conversation declares the MCP tool's functions to Voice Live as ordinary function tools and runs them in the app.
`async with agent` connects the MCP server, and closes it when the agent exits. Without it, the first conversation
connects the server, which then stays open until the agent exits. An MCP tool the app connected itself is never closed
by the agent, so one connection can serve many calls. The conversation copies the server's tools when it starts; a
later change to the server's tools reaches the call through update_session(tools=[...]).

Prerequisites:
- Install the realtime extra and the MCP client: pip install "agent-framework-foundry[realtime]" mcp tenacity --pre
- Set AZURE_VOICELIVE_ENDPOINT and AZURE_VOICELIVE_MODEL, for example a managed model such as gpt-realtime.
- Set AZURE_VOICELIVE_API_KEY, or run `az login` to use AzureCliCredential.

Realtime agents are experimental.
"""

REPLY_PATH = "voice_live_mcp_tool.wav"
OUTPUT_SAMPLE_RATE = 24_000
DELIVERY_SLOTS = {"S1": "Thursday from 9 to 11 AM", "S2": "Thursday from 1 to 3 PM", "S3": "Friday from 9 to 11 AM"}


def run_orders_server() -> None:
    """Serve the orders MCP server over stdio, when the agent's MCP tool starts this file with --server."""
    server = MCPServer("orders")

    @server.tool(structured_output=False)
    def get_delivery_slots(order_id: str) -> str:
        """List the delivery slots available for an order."""
        slots = "; ".join(f"{slot_id}: {window}" for slot_id, window in DELIVERY_SLOTS.items())
        return f"Order {order_id} can be delivered in these slots: {slots}."

    @server.tool(structured_output=False)
    def book_delivery_slot(order_id: str, slot_id: str) -> str:
        """Book a delivery slot for an order, by its slot ID."""
        if slot_id not in DELIVERY_SLOTS:
            return f"There's no delivery slot {slot_id}."
        return f"Booked order {order_id} for {DELIVERY_SLOTS[slot_id]}."

    server.run()


async def read_reply(conversation: RealtimeConversation[VoiceLiveOptions], reply_audio: bytearray) -> None:
    """Print the transcript and the MCP call, and keep the audio, until the agent answers with the tool's result."""
    has_result = False
    update: AgentResponseUpdate
    async for update in conversation:
        for content in update.contents:
            if content.type == "text" and update.role == "assistant" and content.status == "completed":
                print(f"Agent: {content.text}")
            elif content.type == "data" and content.has_top_level_media_type("audio"):
                # The WAV writer needs the 24 kHz, 16-bit mono PCM itself, not the content's data URI.
                reply_audio.extend(content.get_data_bytes())
            elif content.type == "function_call":
                print(f"MCP call: {content.name}({content.arguments})")
            elif content.type == "function_result":
                print(f"MCP result: {content.result}")
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
    orders = MCPStdioTool(name="orders", command=sys.executable, args=[__file__, "--server"], load_prompts=False)
    agent = RealtimeAgent(
        VoiceLiveClient(credential=credential),
        instructions=(
            "You are Contoso's delivery line. When the caller asks about delivering an order, call "
            "get_delivery_slots straight away without saying anything first, then read out the slots in one sentence."
        ),
        tools=[orders],
        default_options=options,
    )
    reply_audio = bytearray()

    try:
        async with agent, agent.start_conversation() as conversation:
            print("Caller: When can order 1234 be delivered?")
            await conversation.send("When can order 1234 be delivered?")
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
    if "--server" in sys.argv:
        run_orders_server()
    else:
        asyncio.run(main())

"""
Sample output:

Caller: When can order 1234 be delivered?
MCP call: get_delivery_slots({"order_id":"1234"})
MCP result: Order 1234 can be delivered in these slots: S1: Thursday from 9 to 11 AM; S2: Thursday from 1 to 3 PM; S3: Friday from 9 to 11 AM.
Agent: Order 1234 can be delivered Thursday from 9 to 11 AM, Thursday from 1 to 3 PM, or Friday from 9 to 11 AM.
Saved 9.1 seconds of reply audio to voice_live_mcp_tool.wav.
"""
