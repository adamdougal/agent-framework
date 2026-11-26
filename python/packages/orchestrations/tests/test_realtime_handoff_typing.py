# Copyright (c) Microsoft. All rights reserved.
# pyright: reportUnnecessaryTypeIgnoreComment=true

from agent_framework import RealtimeAgent, RealtimeOptions
from agent_framework.orchestrations import RealtimeHandoffBuilder


class _ProviderOptions(RealtimeOptions, total=False):
    endpoint_id: str


class _OtherProviderOptions(RealtimeOptions, total=False):
    parallel_tool_calls: bool


def _assert_fluent_registration_checks_participant_options(
    agent: RealtimeAgent[_ProviderOptions], other: RealtimeAgent[_OtherProviderOptions]
) -> None:
    """A bare builder takes any provider's agents; a typed one rejects another options type in every fluent call."""
    RealtimeHandoffBuilder().participants([agent])
    typed = RealtimeHandoffBuilder[_ProviderOptions]()
    typed.participants([other])  # type: ignore
    typed.add_handoff(agent, [other])  # type: ignore
    typed.with_start_agent(other)  # type: ignore
