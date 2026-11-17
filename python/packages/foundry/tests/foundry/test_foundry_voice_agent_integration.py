# Copyright (c) Microsoft. All rights reserved.

"""Live integration tests for FoundryVoiceAgent against a published Foundry voice agent.

Marked integration and flaky, and skipped unless FOUNDRY_PROJECT_ENDPOINT and FOUNDRY_VOICE_AGENT_NAME are set. The
agent must declare get_current_time, get_office_hours, handoff_to_billing and handoff_to_support, with an Azure voice.
FOUNDRY_VOICE_AGENT_VERSION pins a version whose instructions differ from the latest one's. They authenticate with
the Azure CLI login.
"""

import asyncio
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, cast

import pytest
from agent_framework import AgentResponseUpdate, RealtimeAgent, RealtimeEvent, tool
from agent_framework.orchestrations import RealtimeHandoffBuilder
from azure.identity.aio import AzureCliCredential

from agent_framework_foundry import AzureVoice, FoundryVoiceAgent, FoundryVoiceAgentOptions

pytestmark = [
    pytest.mark.flaky,
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FOUNDRY_PROJECT_ENDPOINT", "") == "" or os.getenv("FOUNDRY_VOICE_AGENT_NAME", "") == "",
        reason="No FOUNDRY_PROJECT_ENDPOINT or FOUNDRY_VOICE_AGENT_NAME provided; skipping Foundry voice agent tests.",
    ),
]

TURN_TIMEOUT = 60
SONIA: AzureVoice = {"type": "azure-standard", "name": "en-GB-SoniaNeural"}


@tool(name="get_current_time", description="Get the current time.")
def get_current_time() -> str:
    return "It's twelve noon."


@tool(name="get_office_hours", description="Get the office hours.")
def get_office_hours() -> str:
    return "We're open nine to five, Monday to Friday."


@asynccontextmanager
async def receptionist() -> AsyncGenerator[FoundryVoiceAgent, None]:
    """The published voice agent, pinned to FOUNDRY_VOICE_AGENT_VERSION when it's set."""
    async with AzureCliCredential() as credential:
        yield FoundryVoiceAgent(
            project_endpoint=os.environ["FOUNDRY_PROJECT_ENDPOINT"],
            agent_name=os.environ["FOUNDRY_VOICE_AGENT_NAME"],
            agent_version=os.getenv("FOUNDRY_VOICE_AGENT_VERSION") or None,
            credential=credential,
            name="receptionist",
            description="Greets callers and routes them.",
            tools=[get_current_time, get_office_hours],
        )


def wire_sessions(updates: list[AgentResponseUpdate]) -> list[dict[str, Any]]:
    """The sessions the voice agent reported, in order, starting with its definition."""
    events = [update.raw_representation for update in updates]
    return [
        cast(dict[str, Any], event.raw_representation)["session"]
        for event in events
        if isinstance(event, RealtimeEvent) and event.kind == "session_updated"
    ]


async def test_a_pinned_foundry_voice_agent_runs_a_definition_tool() -> None:
    """The definition's own session.updated carries the pinned version's instructions, asking the time runs the
    get_current_time implementation, and the follow-up response has a final transcript."""
    updates: list[AgentResponseUpdate] = []
    replies: list[str] = []
    async with asyncio.timeout(TURN_TIMEOUT), receptionist() as agent:
        options: FoundryVoiceAgentOptions = {
            "tool_choice": {"mode": "required", "required_function_name": "get_current_time"}
        }
        async with agent, agent.start_conversation(options=options) as conversation:
            published = agent.client.definition
            await conversation.send("What time is it?")
            async for update in conversation:
                updates.append(update)
                replies.extend(
                    content.text or ""
                    for content in update.contents
                    if content.type == "text" and content.status == "completed" and update.role == "assistant"
                )
                if update.finish_reason == "stop":
                    break

    assert published is not None
    assert wire_sessions(updates)[0]["instructions"] == published["instructions"]
    results = [content for update in updates for content in update.contents if content.type == "function_result"]
    assert [str(result.result) for result in results] == ["It's twelve noon."]
    assert replies


async def test_handing_back_restores_the_definition() -> None:
    """In a topology with billing on the agent's client, handoff(billing) then handoff(receptionist) leaves the
    session's instructions, voice and tool names those of the definition."""
    updates: list[AgentResponseUpdate] = []
    async with asyncio.timeout(TURN_TIMEOUT), receptionist() as start:
        billing = RealtimeAgent(
            start.client,
            name="billing",
            description="Answers billing questions.",
            instructions="You answer billing questions in one short sentence.",
            default_options=FoundryVoiceAgentOptions(voice=SONIA),
        )
        built = (
            RealtimeHandoffBuilder[FoundryVoiceAgentOptions](participants=[start, billing])
            .with_start_agent(start)
            .build()
        )
        async with built, built.start_conversation() as conversation:
            published = start.client.definition
            await conversation.handoff(billing)
            await conversation.handoff(start)
            await conversation.close()
            updates.extend([update async for update in conversation])

    assert published is not None
    restored = wire_sessions(updates)[-1]
    assert restored["instructions"] == published["instructions"]
    voice = restored["audio"]["output"]
    voice = (
        voice["voice"] if isinstance(voice["voice"], dict) else {"name": voice["voice"], "type": voice["voice_type"]}
    )
    assert (voice["name"], voice["type"]) == (
        published["audio"]["output"]["voice"],
        published["audio"]["output"]["voice_type"],
    )
    assert [declared.get("name") for declared in restored["tools"]] == [
        declared.get("name") for declared in published["tools"]
    ]
