# PROTOTYPE, never merged (adamdougal/agent-framework#10).
"""``RealtimeHandoffBuilder`` (#7): ``HandoffBuilder``'s topology API, building a realtime agent that owns the call.

The real design shares a topology base with ``HandoffBuilder`` through an internal refactor. The prototype copies the
topology API instead, to keep ``agent-framework-orchestrations`` untouched.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from agent_framework import FunctionInvocationContext, tool

from ._agent import RealtimeAgent
from ._conversation import CURRENT_CALL_ID, RealtimeConversation
from ._core import PARTICIPANT_KEYS, RealtimeError, logger

HANDOFF_PREFIX = "handoff_to_"


def _name(agent: RealtimeAgent) -> str:
    return agent.name or agent.id


def _handoff_tool(target: RealtimeAgent, description: str | None) -> Any:
    target_name = _name(target)

    async def handoff(ctx: FunctionInvocationContext) -> dict[str, str]:
        await ctx.realtime_conversation.handoff(target)  # type: ignore[attr-defined]
        return {"handoff_to": target_name}

    return tool(
        handoff,
        name=f"{HANDOFF_PREFIX}{target_name}",
        description=description or target.description or f"Handoff to the {target_name} agent.",
        approval_mode="never_require",
    )


class HandoffRealtimeConversation(RealtimeConversation):
    owner: HandoffRealtimeAgent

    def _active_agent_handoff_tools(self) -> list[Any]:
        return list(self.owner.handoff_tools.get(_name(self._active_agent), []))

    async def handoff(self, agent: RealtimeAgent, *, tools: Sequence[Any] | None = None) -> None:
        if agent not in self.owner.participants_list:
            raise RealtimeError(f"{_name(agent)} isn't a participant of {self.owner.name}.")
        call_id = CURRENT_CALL_ID.get()
        call = self._calls.get(call_id or "")
        response_id = call.response_id if call else None
        if response_id is not None:
            if response_id in self._handoff_responses:
                raise RealtimeError("Another handoff in this response already won.")
            self._handoff_responses.add(response_id)
        try:
            await super().handoff(agent, tools=[*self.owner.handoff_tools.get(_name(agent), []), *(tools or [])])
        except BaseException:
            if response_id is not None:
                self._handoff_responses.discard(response_id)
            raise


class HandoffRealtimeAgent(RealtimeAgent):
    conversation_class = HandoffRealtimeConversation

    def __init__(
        self,
        *,
        participants: Sequence[RealtimeAgent],
        start_agent: RealtimeAgent,
        handoff_tools: dict[str, list[Any]],
        **kwargs: Any,
    ) -> None:
        super().__init__(client=start_agent.client, **kwargs)
        self.participants_list = list(participants)
        self.start_agent = start_agent
        self.handoff_tools = handoff_tools

    def owner_instructions(self) -> str | None:
        return self.default_options.get("instructions")

    def _start_participant(self) -> RealtimeAgent:
        return self.start_agent

    async def __aenter__(self) -> HandoffRealtimeAgent:
        for participant in self.participants_list:
            await participant.__aenter__()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        for participant in self.participants_list:
            await participant.__aexit__(*exc_info)


class RealtimeHandoffBuilder:
    def __init__(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        participants: Sequence[RealtimeAgent] | None = None,
        context_providers: Sequence[Any] | None = None,
        default_options: dict[str, Any] | None = None,
        function_invocation_configuration: dict[str, Any] | None = None,
        middleware: Sequence[Any] | None = None,
    ) -> None:
        self._name = name
        self._description = description
        self._participants: list[RealtimeAgent] = list(participants or [])
        self._start: RealtimeAgent | None = None
        self._edges: dict[str, list[tuple[RealtimeAgent, str | None]]] = {}
        self._context_providers = list(context_providers or [])
        self._default_options = dict(default_options or {})
        self._fic = function_invocation_configuration
        self._middleware = list(middleware or [])

    def participants(self, participants: Sequence[RealtimeAgent]) -> RealtimeHandoffBuilder:
        self._participants = list(participants)
        return self

    def with_start_agent(self, agent: RealtimeAgent) -> RealtimeHandoffBuilder:
        self._start = agent
        return self

    def add_handoff(
        self, source: RealtimeAgent, targets: Sequence[RealtimeAgent], *, description: str | None = None
    ) -> RealtimeHandoffBuilder:
        for target in targets:
            if target is source:
                raise ValueError(f"{_name(source)} can't hand off to itself.")
            self._edges.setdefault(_name(source), []).append((target, description))
        return self

    def build(self) -> HandoffRealtimeAgent:
        if self._start is None:
            raise ValueError("with_start_agent() is required.")
        if self._start not in self._participants:
            raise ValueError("The start agent must be a participant.")
        clients = {id(participant.client) for participant in self._participants}
        if len(clients) != 1:
            raise ValueError("Participants must share one client instance.")
        if any(isinstance(participant, HandoffRealtimeAgent) for participant in self._participants):
            raise ValueError("A built handoff agent can't be a participant.")
        edges = self._edges or {
            _name(source): [(target, None) for target in self._participants if target is not source]
            for source in self._participants
        }
        handoff_tools = {
            source: [_handoff_tool(target, description) for target, description in targets]
            for source, targets in edges.items()
        }
        for participant in self._participants:
            ignored = [key for key in participant.default_options if key not in PARTICIPANT_KEYS]
            if participant.context_providers or ignored:
                logger.warning(
                    "Participant %s's %s won't apply: the built agent owns the call's settings.",
                    _name(participant),
                    ", ".join([*ignored, *(["context_providers"] if participant.context_providers else [])]),
                )
        return HandoffRealtimeAgent(
            participants=self._participants,
            start_agent=self._start,
            handoff_tools=handoff_tools,
            name=self._name,
            description=self._description,
            context_providers=self._context_providers,
            default_options=self._default_options,
            function_invocation_configuration=self._fic,
            middleware=self._middleware,
        )
