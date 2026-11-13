# Copyright (c) Microsoft. All rights reserved.

"""A handoff topology of realtime agents that hand a live call to one another in-session."""

import asyncio
import logging
from collections.abc import Mapping, Sequence
from typing import Any, Generic
from weakref import WeakKeyDictionary

from agent_framework import (
    ContextProvider,
    FunctionInvocationConfiguration,
    FunctionInvocationContext,
    FunctionTool,
    MiddlewareTypes,
    RawRealtimeAgent,
    RealtimeAgent,
    RealtimeConversation,
    ToolTypes,
)
from agent_framework._feature_stage import ExperimentalFeature, experimental
from agent_framework._middleware import categorize_middleware
from agent_framework._realtime._handoffs import PARTICIPANT_OPTIONS
from agent_framework._realtime._options import RealtimeOptionsT
from agent_framework.exceptions import RealtimeException

from ._handoff import HANDOFF_FUNCTION_RESULT_KEY, get_handoff_tool_name, handoff_tool_description
from ._handoff_topology import HandoffConfiguration, HandoffTopologyBuilder

logger = logging.getLogger(__name__)


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeHandoffBuilder(HandoffTopologyBuilder[RawRealtimeAgent[Any]], Generic[RealtimeOptionsT]):
    """Build a realtime agent whose live calls move between participants in the same realtime session.

    It has ``HandoffBuilder``'s topology API: ``participants(...)``, the required ``with_start_agent(...)`` and
    ``add_handoff(...)``, with every participant able to hand off to every other by default. ``build()`` returns a
    realtime agent. Each of its conversations starts with the start agent speaking, and each participant gets a
    ``handoff_to_<name>`` tool per target. A handoff swaps the session's instructions, tools, voice and tool choice
    without reconnecting, so the caller hears no break.

    A participant brings who's speaking: its instructions, tools, voice, tool choice and function middleware. The
    built agent owns the rest of the call: its history and context providers, its tool-loop limits, its other session
    settings, and instructions appended to every participant's. Participants must share one realtime client instance.
    On Voice Live, keep the participants' voices in one family, Azure or OpenAI, because Voice Live rejects a change
    between them.

    Examples:
        .. code-block:: python

            receptionist = RealtimeAgent(client, name="receptionist", instructions="Greet the caller.")
            billing = RealtimeAgent(client, name="billing", description="Answers billing questions.")

            agent = (
                RealtimeHandoffBuilder(participants=[receptionist, billing])
                .with_start_agent(receptionist)
                .add_handoff(receptionist, [billing])
                .build()
            )
            async with agent.start_conversation() as conversation:
                ...
    """

    def __init__(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        participants: Sequence[RawRealtimeAgent[RealtimeOptionsT]] | None = None,
        context_providers: Sequence[ContextProvider] | None = None,
        default_options: RealtimeOptionsT | None = None,
        function_invocation_configuration: FunctionInvocationConfiguration | None = None,
        middleware: Sequence[MiddlewareTypes] | None = None,
    ) -> None:
        """Create a builder for a realtime handoff agent.

        Keyword Args:
            name: The built agent's name.
            description: The built agent's description.
            participants: The realtime agents that take the call in turn. They can also be set with
                ``participants(...)``.
            context_providers: The built agent's context providers, which keep the call's history and context.
            default_options: Session settings for every call. Their instructions are appended to every
                participant's. Their voice and tool choice apply to a participant that sets neither.
            function_invocation_configuration: The tool-loop limits for every call, whichever participant is speaking.
            middleware: The built agent's middleware. Its function middleware wraps every call, outside the
                participant's own.
        """
        super().__init__(participants)
        self._agent_settings: dict[str, Any] = {
            "name": name,
            "description": description,
            "context_providers": context_providers,
            "default_options": default_options,
            "function_invocation_configuration": function_invocation_configuration,
            "middleware": middleware,
        }

    def _check_participant(self, participant: RawRealtimeAgent[Any]) -> None:
        if not isinstance(participant, RawRealtimeAgent):
            raise TypeError(f"Participants must be realtime agents. Got {type(participant).__name__}.")
        if isinstance(participant, _RealtimeHandoffAgent):
            raise TypeError("A built handoff agent can't be a participant of another handoff topology.")

    def _participant_id(self, participant: RawRealtimeAgent[Any]) -> str:
        return participant.name or participant.id

    def build(self) -> RealtimeAgent[RealtimeOptionsT]:
        """Build the realtime handoff agent.

        Participant settings that won't apply, such as their context providers or other default options, are named in
        a warning.

        Returns:
            A realtime agent whose conversations start with the start agent speaking.

        Raises:
            ValueError: If there are no participants, no start agent, participants on different client instances, or a
                participant tool named like one of its handoff tools.
        """
        agents = self._resolve_agents()
        if self._start_id is None:
            raise ValueError("Must call with_start_agent(...) before building the agent.")
        start = agents[self._start_id]
        if any(agent.client is not start.client for agent in agents.values()):
            raise ValueError("Participants must share one realtime client instance, because they share one session.")
        handoffs = self._resolve_handoffs(agents)
        # Weakly keyed, so a built agent that serves many calls doesn't keep their conversations alive.
        turns: WeakKeyDictionary[RealtimeConversation[Any], asyncio.Lock] = WeakKeyDictionary()
        handoff_tools = {
            agent: [
                _handoff_tool(agent, agents[handoff.target_id], handoff, turns)
                for handoff in handoffs.get(agent_id, [])
            ]
            for agent_id, agent in agents.items()
        }
        for agent, tools in handoff_tools.items():
            _reject_clashing_tool_names(self._resolve_to_id(agent), agent, tools)
            _warn_about_settings_that_wont_apply(self._resolve_to_id(agent), agent)
        return _RealtimeHandoffAgent(start, handoff_tools, **self._agent_settings)


def _reject_clashing_tool_names(
    agent_id: str, agent: RawRealtimeAgent[Any], handoff_tools: Sequence[FunctionTool]
) -> None:
    own_names = {getattr(own, "name", None) for own in agent.tools}
    for clash in (handoff_tool.name for handoff_tool in handoff_tools if handoff_tool.name in own_names):
        raise ValueError(
            f"Agent '{agent_id}' already has a tool named '{clash}', which its handoff tool needs. Rename the tool or "
            "the target agent."
        )


def _warn_about_settings_that_wont_apply(agent_id: str, agent: RawRealtimeAgent[Any]) -> None:
    """A participant brings who's speaking; the built agent owns the call's other settings, history and limits."""
    ignored = [key for key in agent.default_options if key not in PARTICIPANT_OPTIONS]
    if agent.context_providers:
        ignored.append("context_providers")
    if agent.function_invocation_configuration is not None:
        ignored.append("function_invocation_configuration")
    middleware = categorize_middleware(agent.middleware)
    if middleware["agent"] or middleware["chat"]:
        ignored.append("middleware other than function middleware")
    if ignored:
        logger.warning(
            "Participant '%s' sets %s, which won't apply: the built agent owns the call's settings, history and "
            "limits.",
            agent_id,
            ", ".join(ignored),
        )


def _handoff_tool(
    source: RawRealtimeAgent[Any],
    target: RawRealtimeAgent[Any],
    handoff: HandoffConfiguration,
    turns: WeakKeyDictionary[RealtimeConversation[Any], asyncio.Lock],
) -> FunctionTool:
    async def hand_off(ctx: FunctionInvocationContext) -> dict[str, str] | str:
        conversation = ctx.realtime_conversation
        if conversation is None:
            raise RuntimeError("A realtime handoff tool runs only in a realtime conversation.")
        # A conversation's handoff tools take turns, and each hands off only from its own participant, so of two
        # handoffs in one response the first wins and the second finds the call has already moved.
        async with turns.setdefault(conversation, asyncio.Lock()):
            if conversation.active_agent is not source:
                return _not_handed_off(handoff, "another handoff already moved the call")
            try:
                await conversation.handoff(target)
            except RealtimeException as error:
                return _not_handed_off(handoff, str(error))
        return {HANDOFF_FUNCTION_RESULT_KEY: handoff.target_id}

    return FunctionTool(
        name=get_handoff_tool_name(handoff.target_id),
        description=handoff_tool_description(handoff.target_id, handoff.description),
        func=hand_off,
    )


def _not_handed_off(handoff: HandoffConfiguration, why: str) -> str:
    """A plain result rather than an error, so the model reads why and carries on instead of retrying."""
    return (
        f"The handoff to {handoff.target_id} didn't happen ({why}). Carry on helping the caller as the current agent."
    )


class _RealtimeHandoffAgent(RealtimeAgent[RealtimeOptionsT]):
    """Owns the call while its participants take turns speaking, so history and limits survive every handoff."""

    def __init__(
        self,
        start: RawRealtimeAgent[Any],
        handoff_tools: Mapping[RawRealtimeAgent[Any], Sequence[FunctionTool]],
        **settings: Any,
    ) -> None:
        super().__init__(start.client, **settings)
        self._start = start
        self._handoff_tools = handoff_tools

    def _start_participant(self) -> RawRealtimeAgent[Any]:
        return self._start

    def _participant_tools(self, participant: RawRealtimeAgent[Any]) -> list[ToolTypes]:
        if participant not in self._handoff_tools:
            raise ValueError(f"{participant.name or participant.id} isn't a participant of this handoff agent.")
        return list(self._handoff_tools[participant])
