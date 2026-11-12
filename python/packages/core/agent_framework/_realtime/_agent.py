# Copyright (c) Microsoft. All rights reserved.

"""Realtime agents, which start live conversations with a realtime provider."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Generic

from .._agents import BaseAgent, _provider_service_session_state_keys  # pyright: ignore[reportPrivateUsage]
from .._feature_stage import ExperimentalFeature, experimental
from .._middleware import categorize_middleware
from .._tools import (
    _PARENT_SERVICE_SESSION_STATE_KEYS_CONTEXT_KEY,  # pyright: ignore[reportPrivateUsage]
    normalize_function_invocation_configuration,
    normalize_tools,
)
from .._types import _append_instructions, normalize_messages  # pyright: ignore[reportPrivateUsage]
from ._conversation import RealtimeConversation, unique_tools
from ._options import RealtimeOptionsT

if TYPE_CHECKING:
    from .._middleware import MiddlewareTypes
    from .._sessions import AgentSession, ContextProvider
    from .._tools import FunctionInvocationConfiguration, ToolTypes
    from .._types import AgentRunInputs
    from ._clients import RealtimeClientProtocol


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RawRealtimeAgent(BaseAgent, Generic[RealtimeOptionsT]):
    """A realtime agent without middleware or telemetry layers.

    Function tools still run when middleware and telemetry are bypassed.

    Examples:
        .. code-block:: python

            agent = RealtimeAgent(client, instructions="You are a helpful voice assistant.", tools=[get_weather])

            async with agent.start_conversation() as conversation:
                await conversation.send("What's the weather in Paris?")
                async for update in conversation:
                    print(update.text)
    """

    def __init__(
        self,
        client: RealtimeClientProtocol[RealtimeOptionsT],
        *,
        instructions: str | None = None,
        tools: ToolTypes | Callable[..., Any] | Sequence[ToolTypes | Callable[..., Any]] | None = None,
        default_options: RealtimeOptionsT | None = None,
        function_invocation_configuration: FunctionInvocationConfiguration | None = None,
        id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        context_providers: Sequence[ContextProvider] | None = None,
        middleware: Sequence[MiddlewareTypes] | None = None,
    ) -> None:
        """Create a realtime agent.

        Args:
            client: The realtime client that opens the provider sessions.

        Keyword Args:
            instructions: The agent's instructions. When ``None``, ``default_options["instructions"]`` is used.
            tools: The tools every conversation declares and runs.
            default_options: The session settings every conversation starts with; per-conversation options merge
                over them.
            function_invocation_configuration: Limits and settings for the tool loop, as for a chat agent.
            id: The agent's id. A new one is generated when ``None``.
            name: The agent's name, which assistant and tool updates carry as their author.
            description: A description of the agent.
            context_providers: The agent's context providers.
            middleware: The agent's middleware. Realtime conversations run its function middleware.
        """
        super().__init__(
            id=id, name=name, description=description, context_providers=context_providers, middleware=middleware
        )
        self.client = client
        defaults: dict[str, Any] = dict(default_options or {})
        default_instructions = defaults.pop("instructions", None)
        self.instructions: str | None = instructions if instructions is not None else default_instructions
        self.tools = normalize_tools(tools)
        self.default_options: Mapping[str, Any] = defaults
        self.function_invocation_configuration = function_invocation_configuration

    def start_conversation(
        self,
        messages: AgentRunInputs | None = None,
        *,
        session: AgentSession | None = None,
        options: RealtimeOptionsT | None = None,
        tools: ToolTypes | Callable[..., Any] | Sequence[ToolTypes | Callable[..., Any]] | None = None,
        middleware: Sequence[MiddlewareTypes] | None = None,
        function_invocation_kwargs: Mapping[str, Any] | None = None,
        control_timeout: float = 10.0,
        max_queued_updates: int = 10_000,
    ) -> RealtimeConversation[RealtimeOptionsT]:
        """Create a live conversation with the provider, which opens when entered with ``async with``.

        Entering the conversation starts the provider session with the merged options and tools, then sends
        ``messages`` as context; it never requests a response. Leaving it closes the conversation and its session.

        Options merge over the agent's ``default_options``: ``instructions`` are appended to the agent's instructions,
        ``metadata`` merges, and other keys replace the default, ``None`` included. ``tools`` join the agent's tools.

        Args:
            messages: Context to add to the provider's conversation when it opens.

        Keyword Args:
            session: The agent session that tool calls and function middleware receive.
            options: Session settings for this conversation.
            tools: Tools for this conversation, in addition to the agent's.
            middleware: Middleware for this conversation, joined to the agent's. Only function middleware runs;
                other middleware is skipped with a warning.
            function_invocation_kwargs: Keyword arguments for every tool call. The mapping is copied when the
                conversation is created.
            control_timeout: Seconds to wait for the provider to acknowledge the session start and each control call
                before raising ``RealtimeControlTimeoutException``.
            max_queued_updates: The most updates that may wait to be read. When the app falls further behind, the
                conversation fails with ``RealtimeBackpressureException`` and closes.

        Returns:
            The conversation, not yet open.

        Raises:
            ValueError: If ``control_timeout`` isn't positive, ``max_queued_updates`` is less than 1, two tools
                share a name, or the function invocation configuration is invalid.
        """
        if control_timeout <= 0:
            raise ValueError("control_timeout must be greater than 0.")
        if max_queued_updates < 1:
            raise ValueError("max_queued_updates must be at least 1.")
        invocation_configuration = normalize_function_invocation_configuration(self.function_invocation_configuration)
        function_middleware = categorize_middleware(self.middleware, middleware, supported_categories=("function",))[
            "function"
        ]
        session_options = _merge_session_options(self.default_options, self.instructions, options or {})
        invocation_kwargs = dict(function_invocation_kwargs or {})
        # As in chat, the invoking agent's provider-owned session keys are recomputed here, so caller kwargs can't
        # replace them, and an agent tool with propagate_session=True keeps them out of its child session.
        invocation_kwargs[_PARENT_SERVICE_SESSION_STATE_KEYS_CONTEXT_KEY] = _provider_service_session_state_keys(self)
        session_tools = unique_tools([*self.tools, *normalize_tools(tools)])
        return RealtimeConversation(
            self,
            initial_messages=normalize_messages(messages),
            agent_session=session,
            options=session_options,
            tools=session_tools,
            invocation_configuration=invocation_configuration,
            function_middleware=function_middleware,
            function_invocation_kwargs=invocation_kwargs,
            control_timeout=control_timeout,
            max_queued_updates=max_queued_updates,
        )


def _merge_session_options(
    default_options: Mapping[str, Any], instructions: str | None, start_options: Mapping[str, Any]
) -> dict[str, Any]:
    merged: dict[str, Any] = {**default_options, **start_options}
    if default_options.get("metadata") is not None and start_options.get("metadata") is not None:
        merged["metadata"] = {**default_options["metadata"], **start_options["metadata"]}
    merged_instructions = _append_instructions(instructions, start_options.get("instructions"))
    if merged_instructions is not None:
        merged["instructions"] = merged_instructions
    return merged


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeAgent(RawRealtimeAgent[RealtimeOptionsT]):
    """A realtime agent.

    See ``RawRealtimeAgent`` for live-conversation behavior and configuration.
    """
