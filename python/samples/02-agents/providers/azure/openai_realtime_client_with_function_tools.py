# Copyright (c) Microsoft. All rights reserved.

import asyncio

from agent_framework import RealtimeAgent, RealtimeConversation, tool
from agent_framework.openai import OpenAIRealtimeClient, OpenAIRealtimeOptions
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv

load_dotenv()

"""
Azure OpenAI Realtime Client with Function Tools Example

This sample runs a realtime agent on an Azure OpenAI realtime deployment, with one function tool. It sends one text
question and prints the agent's transcript and tool call, so it needs no microphone or speaker. The conversation runs
the tool and asks for a follow-up response, and the sample stops at the first response that ends without calling a
tool.

Passing a credential routes OpenAIRealtimeClient to Azure OpenAI, which it reaches on the resource's /openai/v1
route, so no API version is needed.

Prerequisites:
- Install the realtime extra and Azure Identity: pip install "agent-framework-openai[realtime]" azure-identity --pre
- Set AZURE_OPENAI_ENDPOINT, and AZURE_OPENAI_REALTIME_MODEL to a realtime deployment such as gpt-realtime.
- Run `az login` to use AzureCliCredential.

Realtime agents are experimental.
"""


@tool
def get_weather(location: str) -> str:
    """Get the weather for a city."""
    return f"The weather in {location} is sunny with a high of 22°C."


async def read_reply(conversation: RealtimeConversation[OpenAIRealtimeOptions]) -> None:
    """Print the agent's transcript and tool calls until a response ends without calling a tool."""
    async for update in conversation:
        for content in update.contents:
            if content.type == "text" and update.role == "assistant" and content.status == "completed":
                print(f"Agent: {content.text}")
            elif content.type == "function_call":
                print(f"[calling {content.name}({content.arguments})]")
            elif content.type == "error":
                print(f"Error: {content.message}")
            elif (
                content.type == "realtime_event"
                and content.kind == "response_done"
                and update.finish_reason != "tool_calls"
            ):
                return


async def main() -> None:
    # The sample sends text, so it turns off listening for the caller's speech.
    options: OpenAIRealtimeOptions = {"turn_detection": None}

    async with AzureCliCredential() as credential:
        agent = RealtimeAgent(
            OpenAIRealtimeClient(credential=credential),
            instructions="You are a weather assistant. Answer in one short sentence.",
            tools=[get_weather],
            default_options=options,
        )
        async with agent.start_conversation() as conversation:
            print("Caller: What's the weather like in Amsterdam?")
            await conversation.send("What's the weather like in Amsterdam?")
            await read_reply(conversation)


if __name__ == "__main__":
    asyncio.run(main())

"""
Sample output:

Caller: What's the weather like in Amsterdam?
Agent: Let me check the current conditions in Amsterdam so I can give you a quick, clear update.
[calling get_weather({"location":"Amsterdam"})]
Agent: Amsterdam is sunny with a high of 22 degrees Celsius.
"""
