# Copyright (c) Microsoft. All rights reserved.

"""What a participant brings to a live realtime session when it becomes the agent speaking."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .._types import _append_instructions  # pyright: ignore[reportPrivateUsage]
from ._events import RealtimeEvent

if TYPE_CHECKING:
    from .._middleware import FunctionMiddlewareTypes
    from .._tools import ToolTypes
    from ._agent import RawRealtimeAgent


@dataclass(frozen=True)
class CallSettings:
    """What holds for the whole call, whichever agent is speaking.

    The start middleware wraps each call inside the speaking agent's own, and the host's middleware outside it.
    """

    owner: RawRealtimeAgent[Any]
    options: Mapping[str, Any]
    start_tools: Sequence[ToolTypes]
    start_middleware: Sequence[FunctionMiddlewareTypes]
    participant_defaults: Mapping[str, Any]
    host_middleware: Sequence[FunctionMiddlewareTypes]


PARTICIPANT_OPTIONS = ("voice", "tool_choice")
"""The options a participant brings with its instructions and tools; every other option holds for the call."""


def call_settings(
    owner: RawRealtimeAgent[Any],
    start_options: Mapping[str, Any],
    start_tools: Sequence[ToolTypes],
    owner_middleware: Sequence[FunctionMiddlewareTypes],
    start_middleware: Sequence[FunctionMiddlewareTypes],
) -> CallSettings:
    defaults = {key: value for key, value in owner.default_options.items() if key not in PARTICIPANT_OPTIONS}
    options = _merge_session_options(defaults, start_options)
    if owner._start_participant() is owner:  # pyright: ignore[reportPrivateUsage]
        return CallSettings(owner, options, start_tools, start_middleware, participant_defaults={}, host_middleware=())
    # An owner that doesn't speak itself, such as a handoff agent, adds its instructions to every participant's, its
    # voice and tool choice apply to participants that set none, and its function middleware wraps every call.
    if (instructions := _append_instructions(owner.instructions, start_options.get("instructions"))) is not None:
        options["instructions"] = instructions
    participant_defaults = {
        key: owner.default_options[key] for key in PARTICIPANT_OPTIONS if key in owner.default_options
    }
    return CallSettings(
        owner, options, start_tools, start_middleware, participant_defaults, host_middleware=owner_middleware
    )


def _merge_session_options(default_options: Mapping[str, Any], start_options: Mapping[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {**default_options, **start_options}
    if default_options.get("metadata") is not None and start_options.get("metadata") is not None:
        merged["metadata"] = {**default_options["metadata"], **start_options["metadata"]}
    return merged


def handoff_completed(source: RawRealtimeAgent[Any], target: RawRealtimeAgent[Any]) -> RealtimeEvent:
    """Agents are named as chat handoffs name them, so both kinds of handoff report alike."""
    return RealtimeEvent(
        "handoff_completed", additional_properties={"core": {"source": _named(source), "target": _named(target)}}
    )


def _named(agent: RawRealtimeAgent[Any]) -> str:
    return agent.name or agent.id


def start_settings(agent: RawRealtimeAgent[Any], call: CallSettings) -> tuple[dict[str, Any], list[ToolTypes]]:
    return {**call.options, **_participant_settings(agent, call)}, _declared_tools(agent, call, ())


def participant_update(
    agent: RawRealtimeAgent[Any], call: CallSettings, extra_tools: Sequence[ToolTypes]
) -> tuple[dict[str, Any], list[ToolTypes]]:
    """Nothing the previous agent set may linger.

    Instructions are always sent, and tool choice falls back to "auto", every provider's default, so a router's
    "required" never carries over to a specialist. The provider's default voice isn't known, so with no voice to
    send, the current one stays.
    """
    options = {"instructions": "", "tool_choice": "auto", **_participant_settings(agent, call)}
    return options, _declared_tools(agent, call, extra_tools)


def _declared_tools(
    agent: RawRealtimeAgent[Any], call: CallSettings, extra_tools: Sequence[ToolTypes]
) -> list[ToolTypes]:
    owner_tools = call.owner._participant_tools(agent)  # pyright: ignore[reportPrivateUsage]
    return [*agent.tools, *owner_tools, *call.start_tools, *extra_tools]


def _participant_settings(agent: RawRealtimeAgent[Any], call: CallSettings) -> dict[str, Any]:
    settings: dict[str, Any] = {}
    if (instructions := _append_instructions(agent.instructions, call.options.get("instructions"))) is not None:
        settings["instructions"] = instructions
    for key in PARTICIPANT_OPTIONS:
        if (value := _call_or_participant(key, agent, call)) is not None:
            settings[key] = value
    return settings


def _call_or_participant(key: str, agent: RawRealtimeAgent[Any], call: CallSettings) -> Any:
    """A setting the start options pin holds for the whole call, over every participant's own."""
    return call.options.get(key, agent.default_options.get(key, call.participant_defaults.get(key)))
