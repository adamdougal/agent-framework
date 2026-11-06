# PROTOTYPE, never merged (adamdougal/agent-framework#10).
"""``RealtimeAgent`` and ``FoundryVoiceAgent``.

The prototype collapses ADR 0035's Raw/layered split: telemetry is always on, and ``run()`` (half-duplex) isn't
prototyped because no #10 check needs it.
"""

from __future__ import annotations

import contextlib
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from agent_framework import AgentSession, BaseAgent, InMemoryHistoryProvider
from agent_framework._mcp import MCPTool
from agent_framework._sessions import HistoryProvider

from ._conversation import RealtimeConversation
from ._core import PARTICIPANT_KEYS, logger


@dataclass
class ParticipantSettings:
    """Who's speaking: the four settings a handoff swaps (#7)."""

    instructions: str | None
    voice: Any
    tool_choice: Any
    tools: list[Any] = field(default_factory=list)


class RealtimeAgent(BaseAgent):
    conversation_class = RealtimeConversation

    def __init__(
        self,
        *,
        client: Any,
        instructions: str | None = None,
        tools: Sequence[Any] | None = None,
        default_options: dict[str, Any] | None = None,
        name: str | None = None,
        description: str | None = None,
        id: str | None = None,
        context_providers: Sequence[Any] | None = None,
        middleware: Sequence[Any] | None = None,
        function_invocation_configuration: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            id=id, name=name, description=description, context_providers=context_providers, middleware=middleware
        )
        self.client = client
        self.instructions = instructions
        self.tools = list(tools or [])
        self.default_options = dict(default_options or {})
        self.function_invocation_configuration = function_invocation_configuration
        self._entered: list[MCPTool] = []

    def participant_settings(self, provider_session: Any = None) -> ParticipantSettings:
        return ParticipantSettings(
            instructions=self.instructions,
            voice=self.default_options.get("voice"),
            tool_choice=self.default_options.get("tool_choice"),
            tools=list(self.tools),
        )

    def owner_instructions(self) -> str | None:
        return None

    def call_options(self) -> dict[str, Any]:
        return {key: value for key, value in self.default_options.items() if key not in PARTICIPANT_KEYS}

    def start_conversation(
        self,
        *,
        session: AgentSession | None = None,
        options: dict[str, Any] | None = None,
        tools: Sequence[Any] | None = None,
        middleware: Sequence[Any] | None = None,
        function_invocation_kwargs: dict[str, Any] | None = None,
        create_response: bool = False,
        **kwargs: Any,
    ) -> RealtimeConversation:
        if session is not None and not any(isinstance(p, HistoryProvider) for p in self.context_providers):
            self.context_providers.append(InMemoryHistoryProvider())
        if any(_is_agent_middleware(m) for m in middleware or []):
            logger.warning("start_conversation() skips agent middleware; it wraps run() only (#9).")
        return self.conversation_class(
            self,
            start_agent=self._start_participant(),
            session=session,
            options=options,
            tools=tools,
            middleware=middleware,
            function_invocation_kwargs=function_invocation_kwargs,
            create_response=create_response,
            **kwargs,
        )

    def _start_participant(self) -> RealtimeAgent:
        return self

    async def run(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("run() isn't prototyped for #10.")

    async def __aenter__(self) -> RealtimeAgent:
        for tool in self.tools:
            if isinstance(tool, MCPTool) and not tool.is_connected:
                await tool.connect()
                self._entered.append(tool)
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        for tool in self._entered:
            with contextlib.suppress(Exception):
                await tool.close()
        self._entered.clear()


class FoundryVoiceAgent(RealtimeAgent):
    """A published Foundry voice agent. Its definition owns instructions, voice, tool choice and tools (#8)."""

    is_foundry_definition = True

    def __init__(self, *, client: Any, tools: Sequence[Any] | None = None, **kwargs: Any) -> None:
        super().__init__(client=client, **kwargs)
        self.implementations = list(tools or [])
        self.tools = []

    def participant_settings(self, provider_session: Any = None) -> ParticipantSettings:
        definition = getattr(provider_session, "definition", None) or self.client.definition or {}
        output = (definition.get("audio") or {}).get("output") or {}
        voice = output.get("voice")
        if isinstance(voice, str) and output.get("voice_type"):
            # Re-sending the name alone is rejected; Foundry needs the type with it (prototype finding).
            voice = {"name": voice, "type": output["voice_type"]}
        implemented = {getattr(tool, "name", None) for tool in self.implementations}
        for declared in definition.get("tools") or []:
            if declared.get("type") == "function" and declared.get("name") not in implemented:
                logger.warning("Foundry definition tool %r has no implementation.", declared.get("name"))
        return ParticipantSettings(
            instructions=definition.get("instructions"),
            voice=voice,
            tool_choice=definition.get("tool_choice"),
            tools=list(definition.get("tools") or []),
        )


def _is_agent_middleware(middleware: Any) -> bool:
    from agent_framework import AgentMiddleware

    return isinstance(middleware, AgentMiddleware)
