# Copyright (c) Microsoft. All rights reserved.

"""Realtime agents, which start live conversations with a realtime provider."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import AsyncExitStack
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, Self

from .._agents import BaseAgent, _provider_service_session_state_keys  # pyright: ignore[reportPrivateUsage]
from .._feature_stage import ExperimentalFeature, experimental
from .._mcp import MCPTool
from .._middleware import categorize_middleware
from .._sessions import HistoryProvider, InMemoryHistoryProvider
from .._tools import (
    _PARENT_SERVICE_SESSION_STATE_KEYS_CONTEXT_KEY,  # pyright: ignore[reportPrivateUsage]
    normalize_function_invocation_configuration,
    normalize_tools,
)
from .._types import normalize_messages
from ._conversation import RealtimeConversation
from ._handoffs import call_settings, start_settings
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

    A conversation declares and runs each local MCP tool's functions, copied when it starts and on
    ``update_session(tools=...)``. ``async with agent`` connects the agent's MCP tools that aren't connected yet;
    otherwise a conversation connects one when it first needs it. Either way the tool stays open until the agent
    exits, and a tool the app connected itself is never closed by the agent.

    Examples:
        .. code-block:: python

            agent = RealtimeAgent(client, instructions="You are a helpful voice assistant.", tools=[get_weather])

            async with agent.start_conversation() as conversation:
                await conversation.send("What's the weather in Paris?")
                async for update in conversation:
                    print(update.text)

        With an MCP tool, enter the agent so its connection closes with it:

        .. code-block:: python

            orders = MCPStdioTool(name="orders", command="python", args=["orders_server.py"])
            agent = RealtimeAgent(client, instructions="You help callers with their orders.", tools=[orders])

            async with agent, agent.start_conversation() as conversation:
                await conversation.send("When can order 1234 be delivered?")
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
            tools: The tools every conversation declares and runs. An MCP tool stands for its functions.
            default_options: The session settings every conversation starts with; per-conversation options merge
                over them.
            function_invocation_configuration: Limits and settings for the tool loop, as for a chat agent.
            id: The agent's id. A new one is generated when ``None``.
            name: The agent's name, which assistant and tool updates carry as their author.
            description: A description of the agent.
            context_providers: The agent's context providers. A live conversation runs their ``before_run`` once,
                when it opens, and their ``after_run`` once per exchange when it closes.
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
        self._exit_stack = AsyncExitStack()

    async def __aenter__(self) -> Self:
        try:
            for tool in self.tools:
                if isinstance(tool, MCPTool) and not tool.is_connected:
                    await self._exit_stack.enter_async_context(tool)
        except BaseException:
            await self._exit_stack.aclose()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._exit_stack.aclose()

    def _start_participant(self) -> RawRealtimeAgent[RealtimeOptionsT]:
        """The agent that speaks first in this agent's conversations.

        An agent that owns a handoff topology starts its calls with one of its participants rather than itself.
        """
        return self

    def _participant_tools(self, participant: RawRealtimeAgent[RealtimeOptionsT]) -> list[ToolTypes]:
        """The tools this agent adds to its conversations while ``participant`` is speaking.

        An agent that owns a handoff topology adds each participant's handoff tools, and raises ``ValueError`` for an
        agent outside its topology. Any other agent adds none and can hand off to any agent on its client.
        """
        return []

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

        Entering the conversation runs the context providers' ``before_run``, starts the provider session with the
        merged options and tools, replays the history they loaded, then sends ``messages`` as context; it never requests
        a response. The instructions, tools and function middleware the providers add follow the agent's own; chat
        middleware has no model call to wrap, so it's skipped as for the agent's own. Leaving the conversation closes
        it and its session, then reports the call to the context providers one exchange at a time. A call whose process
        ends before it closes never reaches them, so an app that needs each exchange stored as it happens records it
        from the conversation's updates.

        Options merge over the agent's ``default_options``: ``instructions`` are appended to the agent's instructions,
        ``metadata`` merges, and other keys replace the default, ``None`` included. ``tools`` join the agent's tools.
        The conversation can hand the call to another agent on the same client with ``handoff(...)``; an agent that
        owns a handoff topology starts its conversations with its start participant speaking.

        Args:
            messages: Context to add to the provider's conversation when it opens.

        Keyword Args:
            session: The agent session that keeps the conversation's history, and that tool calls and function
                middleware receive. When no context provider loads history, an ``InMemoryHistoryProvider`` is added to
                the agent, as for a chat agent.
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
            ValueError: If ``control_timeout`` isn't positive, ``max_queued_updates`` is less than 1, or the
                function invocation configuration is invalid. Entering the conversation raises ``ValueError``, before
                anything is sent, if two of its tools share a name, counting MCP tools' functions and context
                providers' tools.
        """
        if control_timeout <= 0:
            raise ValueError("control_timeout must be greater than 0.")
        if max_queued_updates < 1:
            raise ValueError("max_queued_updates must be at least 1.")
        invocation_configuration = normalize_function_invocation_configuration(self.function_invocation_configuration)
        own_middleware = categorize_middleware(self.middleware, supported_categories=("function",))["function"]
        start_middleware = categorize_middleware(middleware, supported_categories=("function",))["function"]
        call = call_settings(self, options or {}, normalize_tools(tools), own_middleware, start_middleware)
        session_options, session_tools = start_settings(self._start_participant(), call)
        app_kwargs = dict(function_invocation_kwargs or {})
        invocation_kwargs = dict(app_kwargs)
        # As in chat, the invoking agent's provider-owned session keys are recomputed here, so caller kwargs can't
        # replace them, and an agent tool with propagate_session=True keeps them out of its child session.
        invocation_kwargs[_PARENT_SERVICE_SESSION_STATE_KEYS_CONTEXT_KEY] = _provider_service_session_state_keys(self)
        self._add_history_when_none_loads(session)
        return RealtimeConversation(
            self,
            initial_messages=normalize_messages(messages),
            agent_session=session,
            options=session_options,
            call=call,
            tools=session_tools,
            invocation_configuration=invocation_configuration,
            function_middleware=own_middleware,
            function_invocation_kwargs=invocation_kwargs,
            mcp_connection_kwargs=app_kwargs,
            mcp_exit_stack=self._exit_stack,
            control_timeout=control_timeout,
            max_queued_updates=max_queued_updates,
        )

    def _add_history_when_none_loads(self, session: AgentSession | None) -> None:
        if session is not None and not any(
            provider.load_messages for provider in self.context_providers if isinstance(provider, HistoryProvider)
        ):
            # Load history before providers such as compaction inspect the context.
            self.context_providers.insert(0, InMemoryHistoryProvider())


@experimental(feature_id=ExperimentalFeature.REALTIME)
class RealtimeAgent(RawRealtimeAgent[RealtimeOptionsT]):
    """A realtime agent.

    See ``RawRealtimeAgent`` for live-conversation behavior and configuration.
    """
