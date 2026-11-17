# Copyright (c) Microsoft. All rights reserved.

"""Agents published in Microsoft Foundry as voice agents, as realtime agents."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Self, cast

from agent_framework import RawRealtimeAgent, ToolMode, normalize_tools
from agent_framework._feature_stage import ExperimentalFeature, experimental

from ._foundry_voice_client import DEFINITION_TOOLS, FoundryVoiceAgentClient, voice_from
from ._realtime_options import FoundryVoiceAgentOptions

if TYPE_CHECKING:
    from agent_framework import ContextProvider, FunctionInvocationConfiguration, MiddlewareTypes, ToolTypes
    from azure.ai.projects.aio import AIProjectClient
    from azure.core.credentials_async import AsyncTokenCredential


@experimental(feature_id=ExperimentalFeature.FOUNDRY_VOICE_AGENTS)
class RawFoundryVoiceAgent(RawRealtimeAgent[FoundryVoiceAgentOptions]):
    """A realtime agent published in Microsoft Foundry as a voice agent, without middleware or telemetry layers.

    The published agent definition supplies the agent's instructions, voice, tool choice and tools. Entering the agent
    with ``async with`` reads the definition, once for the agent's lifetime, and a conversation can't start before
    then. Entering a ``RealtimeHandoffBuilder`` agent enters its participants. Pin ``agent_version`` so every call runs
    the version you chose.

    ``tools=`` supplies Python implementations for the definition's functions, matched by name. Any tool in a call
    can implement one, such as a handoff tool a handoff topology adds, and the definition's published declaration is
    the one sent. A definition function nothing implements gets an error result, with a warning when a session starts.
    Tools the definition doesn't name are declared alongside its own. Definitions published by Semantic Kernel name
    their tools ``<Plugin>-<function>``, so republish them with the names of your Agent Framework tools.

    In a handoff topology, other participants are realtime agents on this agent's ``client``. Handing back to it
    restores its definition's instructions, voice, tool choice and tools. A change from an Azure voice to an OpenAI
    voice is dropped, because Foundry rejects it, so keep a topology's voices in one family.

    Examples:
        .. code-block:: python

            from agent_framework import tool
            from agent_framework.foundry import FoundryVoiceAgent
            from azure.identity.aio import AzureCliCredential


            @tool(description="Get the current time.")
            def get_current_time() -> str:
                return "It's twelve noon."


            async with AzureCliCredential() as credential:
                agent = FoundryVoiceAgent(
                    project_endpoint="https://<resource>.services.ai.azure.com/api/projects/<project>",
                    agent_name="receptionist-voice",
                    agent_version="3",
                    credential=credential,
                    tools=[get_current_time],
                )
                async with agent, agent.start_conversation() as conversation:
                    await conversation.send("What time is it?")
                    async for update in conversation:
                        print(update.text)
    """

    client: FoundryVoiceAgentClient

    def __init__(
        self,
        *,
        project_client: AIProjectClient | None = None,
        project_endpoint: str | None = None,
        agent_name: str | None = None,
        agent_version: str | None = None,
        credential: AsyncTokenCredential | None = None,
        tools: ToolTypes | Callable[..., Any] | Sequence[ToolTypes | Callable[..., Any]] | None = None,
        default_options: FoundryVoiceAgentOptions | None = None,
        function_invocation_configuration: FunctionInvocationConfiguration | None = None,
        id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        context_providers: Sequence[ContextProvider] | None = None,
        middleware: Sequence[MiddlewareTypes] | None = None,
        env_file_path: str | None = None,
        env_file_encoding: str | None = None,
    ) -> None:
        """Create a Foundry voice agent. Nothing is read or connected until it's entered.

        Keyword Args:
            project_client: The project client to use. The agent never closes it.
            project_endpoint: The Foundry project endpoint, used with ``credential`` when no project client is given.
                Read from ``FOUNDRY_PROJECT_ENDPOINT`` when not passed.
            agent_name: The published voice agent's name. Read from ``FOUNDRY_AGENT_NAME`` when not passed.
            agent_version: The version to pin. Read from ``FOUNDRY_AGENT_VERSION`` when not passed; otherwise the latest
                version when the agent is first entered.
            credential: An async token credential for the ``https://ai.azure.com/.default`` scope.
            tools: Implementations of the definition's functions, matched by name, and any tools to add.
            default_options: Session settings every conversation starts with. Instructions, a voice or a tool choice
                here replace the definition's.
            function_invocation_configuration: Limits and settings for the tool loop.
            id: The agent's id. A new one is generated when ``None``.
            name: The agent's name, which its updates carry as their author. Defaults to the agent name.
            description: A description of the agent, which a handoff tool to it uses.
            context_providers: The agent's context providers.
            middleware: The agent's middleware. Realtime conversations run its function middleware.
            env_file_path: A ``.env`` file to read settings from.
            env_file_encoding: The ``.env`` file's encoding.

        Raises:
            ValueError: If the agent name is missing, or neither a project client nor an endpoint with a credential is
                given.
            ModuleNotFoundError: If the realtime extra isn't installed.
        """
        client = FoundryVoiceAgentClient(
            project_client=project_client,
            project_endpoint=project_endpoint,
            agent_name=agent_name,
            agent_version=agent_version,
            credential=credential,
            env_file_path=env_file_path,
            env_file_encoding=env_file_encoding,
        )
        super().__init__(
            client,
            tools=[DEFINITION_TOOLS, *normalize_tools(tools)],
            default_options=default_options,
            function_invocation_configuration=function_invocation_configuration,
            id=id,
            name=name or client.agent_name,
            description=description,
            context_providers=context_providers,
            middleware=middleware,
        )

    async def __aenter__(self) -> Self:
        """Read the definition the first time the agent is entered, and connect its MCP tools."""
        await self._exit_stack.enter_async_context(self.client)
        definition = cast("Mapping[str, Any]", self.client.definition)
        if self.instructions is None:
            self.instructions = definition.get("instructions")
        self.default_options = {**_published_defaults(definition), **self.default_options}
        await super().__aenter__()
        return self


@experimental(feature_id=ExperimentalFeature.FOUNDRY_VOICE_AGENTS)
class FoundryVoiceAgent(RawFoundryVoiceAgent):
    """A realtime agent published in Microsoft Foundry as a voice agent.

    See ``RawFoundryVoiceAgent`` for how the definition, tools and handoffs work.
    """


def _published_defaults(definition: Mapping[str, Any]) -> dict[str, Any]:
    """The voice and tool choice a definition sets, as the defaults of the agent it publishes."""
    defaults: dict[str, Any] = {}
    if (voice := voice_from(definition.get("audio", {}).get("output", {}))) is not None:
        defaults["voice"] = voice
    if definition.get("tool_choice") is not None:
        defaults["tool_choice"] = _tool_mode(definition["tool_choice"])
    return defaults


def _tool_mode(tool_choice: str | Mapping[str, Any]) -> str | ToolMode:
    """The neutral tool choice for a published one: a mode as it is, or a published function as a required one."""
    if isinstance(tool_choice, str):
        return tool_choice
    return ToolMode(mode="required", required_function_name=tool_choice["name"])
