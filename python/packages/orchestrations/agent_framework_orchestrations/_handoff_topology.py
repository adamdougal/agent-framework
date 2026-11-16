# Copyright (c) Microsoft. All rights reserved.

"""The topology of a handoff orchestration: its participants, its start agent and who can hand off to whom."""

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Generic, Protocol, Self, TypeVar

from agent_framework import SupportsAgentRun
from agent_framework._workflows._agent_utils import resolve_agent_id

logger = logging.getLogger(__name__)


class _Participant(Protocol):
    @property
    def id(self) -> str: ...

    @property
    def name(self) -> str | None: ...

    @property
    def description(self) -> str | None: ...


ParticipantT = TypeVar("ParticipantT", bound=_Participant)


@dataclass
class HandoffConfiguration:
    """Configuration for handoff routing between agents.

    Attributes:
        target_id: Identifier of the target agent to hand off to
        description: Optional human-readable description of the handoff
    """

    target_id: str
    description: str | None = None

    def __init__(self, *, target: str | SupportsAgentRun, description: str | None = None) -> None:
        """Initialize HandoffConfiguration.

        Args:
            target: Target agent identifier or SupportsAgentRun instance
            description: Optional human-readable description of the handoff
        """
        self.target_id = resolve_agent_id(target) if isinstance(target, SupportsAgentRun) else target
        self.description = description

    def __eq__(self, other: Any) -> bool:
        """Determine equality based on source_id and target_id."""
        if not isinstance(other, HandoffConfiguration):
            return False

        return self.target_id == other.target_id

    def __hash__(self) -> int:
        """Compute hash based on source_id and target_id."""
        return hash(self.target_id)


class HandoffTopologyBuilder(ABC, Generic[ParticipantT]):
    """The topology API that the chat and realtime handoff builders share, so they validate it identically.

    Each builder checks its own participant type and resolves a participant's id its own way.
    """

    def __init__(self, participants: Sequence[ParticipantT] | None = None) -> None:
        self._participants: dict[str, ParticipantT] = {}
        self._start_id: str | None = None
        self._handoff_config: dict[str, set[HandoffConfiguration]] = {}
        if participants:
            self.participants(participants)

    @abstractmethod
    def _check_participant(self, participant: ParticipantT) -> None: ...

    @abstractmethod
    def _participant_id(self, participant: ParticipantT) -> str: ...

    def participants(self, participants: Sequence[ParticipantT]) -> Self:
        """Register the agents that take part in the handoffs.

        Args:
            participants: The agents, each with a unique identifier: its ``name`` if set, otherwise its ``id``.

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If participants is empty, contains duplicates, or `.participants()` has already been called.
            TypeError: If a participant isn't an agent this builder supports.
        """
        if self._participants:
            raise ValueError("participants have already been assigned")

        if not participants:
            raise ValueError("participants cannot be empty")

        named: dict[str, ParticipantT] = {}
        for participant in participants:
            self._check_participant(participant)
            resolved_id = self._resolve_to_id(participant)

            if resolved_id in named:
                raise ValueError(f"Duplicate participant name '{resolved_id}' detected")
            named[resolved_id] = participant

        self._participants = named

        return self

    def add_handoff(
        self,
        source: ParticipantT,
        targets: Sequence[ParticipantT],
        *,
        description: str | None = None,
    ) -> Self:
        """Add handoff routing from a source agent to one or more target agents.

        Call this method multiple times to build the routing graph. If no handoffs are specified, every agent can hand
        off to every other (a mesh topology).

        Args:
            source: The agent that can initiate the handoff.
            targets: One or more target agents that the source can hand off to.
            description: Optional description for the handoff tools. If not provided, each target's description is
                used. If multiple targets are provided, they share the description; call add_handoff() separately
                for each target to describe them differently.

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If source or targets are not in the participants list, or if participants(...) hasn't been
                called yet.

        Note:
            - Handoff tools are automatically registered for each source agent
            - If a source agent is configured multiple times via add_handoff, targets are merged
        """
        if not self._participants:
            raise ValueError("Call participants(...) before add_handoff(...)")

        # Resolve source agent ID
        source_id = self._resolve_to_id(source)
        if source_id not in self._participants:
            raise ValueError(f"Source agent '{source}' is not in the participants list")

        # Resolve all target IDs
        target_ids: list[str] = []
        for target in targets:
            target_id = self._resolve_to_id(target)
            if target_id not in self._participants:
                raise ValueError(f"Target agent '{target}' is not in the participants list")
            target_ids.append(target_id)

        # Merge with existing handoff configuration for this source
        if source_id not in self._handoff_config:
            self._handoff_config[source_id] = set()

        for t in target_ids:
            config = HandoffConfiguration(target=t, description=description)
            if config in self._handoff_config[source_id]:
                logger.warning(f"Handoff from '{source_id}' to '{t}' is already configured; overwriting.")
                # Remove old config so the new one (with updated description) takes effect
                self._handoff_config[source_id].discard(config)
            self._handoff_config[source_id].add(config)

        return self

    def with_start_agent(self, agent: ParticipantT) -> Self:
        """Set the agent that starts the handoffs.

        Args:
            agent: The agent that starts.

        Returns:
            Self for method chaining.
        """
        resolved_id = self._resolve_to_id(agent)
        if self._participants:
            if resolved_id not in self._participants:
                raise ValueError(f"Start agent '{resolved_id}' is not in the participants list")
        else:
            raise ValueError("Call participants(...) before with_start_agent(...)")
        self._start_id = resolved_id

        return self

    def _resolve_agents(self) -> dict[str, ParticipantT]:
        """Resolve participant instances into agent instances.

        Returns:
            Map of executor IDs to agent instances
        """
        if not self._participants:
            raise ValueError("No participants provided. Call .participants() first.")

        return self._participants

    def _resolve_handoffs(self, agents: dict[str, ParticipantT]) -> dict[str, list[HandoffConfiguration]]:
        """Resolve handoff configurations to executor IDs.

        Args:
            agents: Map of agent IDs to agent instances

        Returns:
            Map of executor IDs to list of HandoffConfiguration instances
        """
        # Updated map that used agent resolved IDs as keys
        updated_handoff_configurations: dict[str, list[HandoffConfiguration]] = {}
        if self._handoff_config:
            # Use explicit handoff configuration from add_handoff() calls
            for source_id, handoff_configurations in self._handoff_config.items():
                source_agent = agents.get(source_id)
                if not source_agent:
                    raise ValueError(
                        f"Handoff source agent '{source_id}' not found. "
                        "Please make sure source has been added as a participant."
                    )
                for handoff_config in handoff_configurations:
                    target_agent = agents.get(handoff_config.target_id)
                    if not target_agent:
                        raise ValueError(
                            f"Handoff target agent '{handoff_config.target_id}' not found for source '{source_id}'. "
                            "Please make sure target has been added as a participant."
                        )

                    updated_handoff_configurations.setdefault(self._resolve_to_id(source_agent), []).append(
                        HandoffConfiguration(
                            target=self._resolve_to_id(target_agent),
                            description=handoff_config.description or target_agent.description,
                        )
                    )
        else:
            # Use default handoff configuration: all agents can hand off to all others (mesh topology)
            for source_id, source_agent in agents.items():
                for target_id, target_agent in agents.items():
                    if source_id == target_id:
                        continue  # Skip self-handoff
                    updated_handoff_configurations.setdefault(self._resolve_to_id(source_agent), []).append(
                        HandoffConfiguration(
                            target=self._resolve_to_id(target_agent),
                            description=target_agent.description,
                        )
                    )

        return updated_handoff_configurations

    def _resolve_to_id(self, candidate: str | ParticipantT) -> str:
        """Resolve a participant reference into a concrete executor identifier."""
        if isinstance(candidate, str):
            return candidate
        return self._participant_id(candidate)
