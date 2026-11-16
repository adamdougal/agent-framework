# Copyright (c) Microsoft. All rights reserved.

"""Foundry voice agents as a realtime provider, on the shared OpenAI Realtime session."""

from __future__ import annotations

import importlib.util
import logging
from collections.abc import Mapping, Sequence
from types import MappingProxyType, TracebackType
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast

from agent_framework import FunctionTool, load_settings, prepend_agent_framework_to_user_agent
from agent_framework._feature_stage import ExperimentalFeature, experimental
from agent_framework.exceptions import RealtimeConnectionException
from agent_framework_openai._realtime_session import OpenAIRealtimeConnection, OpenAIRealtimeSession
from azure.ai.projects.aio import AIProjectClient
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from ._agent import FoundryAgentSettings
from ._realtime_options import AzureVoice, FoundryVoiceAgentOptions

if TYPE_CHECKING:
    from agent_framework import RealtimeClientSessionProtocol, ToolTypes
    from azure.core.credentials_async import AsyncTokenCredential

logger = logging.getLogger("agent_framework.foundry")

VOICE_AGENT_API_VERSION = "v1"

_STORED_VOICE_FIELDS: Mapping[str, str] = {
    "voice": "name",
    "voice_type": "type",
    "voice_temperature": "temperature",
    "custom_voice_endpoint_id": "endpoint_id",
    "personal_voice_model": "model",
    "custom_lexicon_url": "custom_lexicon_url",
    "custom_text_normalization_url": "custom_text_normalization_url",
    "prefer_locales": "prefer_locales",
    "voice_locale": "locale",
    "style": "style",
    "pitch": "pitch",
    "volume": "volume",
}
"""Where a voice agent stores each field of an Azure voice: flattened beside the voice name, in its definition and in
the sessions it reports."""


class _DefinitionTools:
    """Stands for the agent definition's tools in a tool list, which a Foundry voice agent's session expands.

    The definition's functions and their implementations can't share a list, because tool names must be unique, and
    the implementations can come from anywhere in a call, such as a handoff topology's handoff tools.
    """


DEFINITION_TOOLS = _DefinitionTools()

_INPUT_AUDIO_SETTINGS = ("transcription", "turn_detection", "noise_reduction", "echo_cancellation")

_TRACE_CONTEXT = TraceContextTextMapPropagator()


@experimental(feature_id=ExperimentalFeature.FOUNDRY_VOICE_AGENTS)
class FoundryVoiceAgentClient:
    """A realtime client for an agent published in Microsoft Foundry as a voice agent.

    A ``FoundryVoiceAgent`` builds one, and the other participants of its handoff topology share it as
    ``foundry_agent.client``. Entering it reads the agent's definition through Foundry's agents API: the given
    ``agent_version``, or else the latest version, which it then keeps as ``agent_version``. It keeps that definition
    for its lifetime, read-only, and pins every session to that version, so republishing the agent doesn't change its
    calls. Pin ``agent_version`` to choose the version yourself.

    Each session waits for the definition its voice-agent connection reports before it sends anything, and runs the
    definition's settings until a session update changes them. Whenever the ``FoundryVoiceAgent`` is speaking, its
    definition's tools are declared as published: a function tool named like one of them runs as that function, and
    other tools are declared after them. Settings the app doesn't set, such as transcription, turn detection and audio
    formats, stay the definition's, and the session's ``options`` report them.

    It needs the realtime extra: ``pip install 'agent-framework-foundry[realtime]'``. Settings not passed are read from
    ``FOUNDRY_PROJECT_ENDPOINT``, ``FOUNDRY_AGENT_NAME`` and ``FOUNDRY_AGENT_VERSION``.

    Attributes:
        agent_name: The published voice agent's name.
        agent_version: The version every session is pinned to.
        definition: The definition read on first entry, or ``None`` before then.
    """

    def __init__(
        self,
        *,
        project_client: AIProjectClient | None = None,
        project_endpoint: str | None = None,
        agent_name: str | None = None,
        agent_version: str | None = None,
        credential: AsyncTokenCredential | None = None,
        env_file_path: str | None = None,
        env_file_encoding: str | None = None,
    ) -> None:
        """Create a client for a published Foundry voice agent. Nothing is read or connected until it's entered.

        Keyword Args:
            project_client: The project client to use. The client never closes it.
            project_endpoint: The Foundry project endpoint, used with ``credential`` when no project client is given.
                Read from ``FOUNDRY_PROJECT_ENDPOINT`` when not passed.
            agent_name: The published voice agent's name. Read from ``FOUNDRY_AGENT_NAME`` when not passed.
            agent_version: The version to pin. Read from ``FOUNDRY_AGENT_VERSION`` when not passed; otherwise the latest
                version when the client is first entered.
            credential: An async token credential for the ``https://ai.azure.com/.default`` scope. The client never
                closes it.
            env_file_path: A ``.env`` file to read settings from.
            env_file_encoding: The ``.env`` file's encoding.

        Raises:
            ValueError: If the agent name is missing, or neither a project client nor an endpoint with a credential is
                given.
            ModuleNotFoundError: If the realtime extra isn't installed.
        """
        _require_voice_agents_sdk()
        settings = load_settings(
            FoundryAgentSettings,
            env_prefix="FOUNDRY_",
            project_endpoint=project_endpoint,
            agent_name=agent_name,
            agent_version=agent_version,
            env_file_path=env_file_path,
            env_file_encoding=env_file_encoding,
        )
        agent_name = settings.get("agent_name")
        if not agent_name:
            raise ValueError(
                "Agent name is required. Set via 'agent_name' parameter or 'FOUNDRY_AGENT_NAME' environment variable."
            )
        self.agent_name: str = agent_name
        self.agent_version = settings.get("agent_version")
        self._project_client = project_client
        self._project_access: tuple[str, AsyncTokenCredential] | None = None
        if project_client is None:
            project_endpoint = settings.get("project_endpoint")
            if not project_endpoint:
                raise ValueError(
                    "Either 'project_endpoint' or 'project_client' is required. Set project_endpoint via parameter or "
                    "'FOUNDRY_PROJECT_ENDPOINT' environment variable."
                )
            if credential is None:
                raise ValueError("Azure credential is required when using project_endpoint without a project_client.")
            self._project_access = (project_endpoint, credential)
        self.definition: Mapping[str, Any] | None = None

    @property
    def project_client(self) -> AIProjectClient:
        """The project client: the one passed in, or the one the client opens each time it's entered."""
        if self._project_client is None:
            raise RuntimeError("Enter the Foundry voice agent with 'async with' before using its project client.")
        return self._project_client

    async def __aenter__(self) -> Self:
        # A closed project client can't be reopened, so one the client owns lasts for one entry, and an agent entered
        # once per call still connects on every call.
        if self._project_access is not None:
            endpoint, credential = self._project_access
            self._project_client = AIProjectClient(endpoint=endpoint, credential=credential)
        if self.definition is None:
            try:
                await self._read_definition()
            except BaseException:
                await self._close_owned_project_client()
                raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._close_owned_project_client()

    async def _close_owned_project_client(self) -> None:
        if self._project_access is not None and self._project_client is not None:
            await self._project_client.close()
            self._project_client = None

    async def _read_definition(self) -> None:
        """An agent keeps the definition it reads first, and pins every session to that version."""
        if self.agent_version is None:
            published = (await self.project_client.agents.get(self.agent_name)).as_dict()["versions"]["latest"]
            self.agent_version = published["version"]
        else:
            published = (await self.project_client.agents.get_version(self.agent_name, self.agent_version)).as_dict()
        self.definition = MappingProxyType(published["definition"])

    async def start_session(
        self, *, options: FoundryVoiceAgentOptions | None = None, tools: Sequence[ToolTypes] | None = None
    ) -> RealtimeClientSessionProtocol[FoundryVoiceAgentOptions]:
        """Connect, wait for the definition, apply the initial settings, and return the session once it's ready.

        The caller owns the session and closes it. If opening fails or is cancelled, the socket is closed first. The
        handshake carries the current span's ``traceparent`` and ``tracestate``, so Foundry records its spans for the
        session in the caller's trace.

        Keyword Args:
            options: The session's initial settings, applied over the definition's.
            tools: The tools to declare.

        Raises:
            RuntimeError: If the client hasn't been entered, so its definition hasn't been read.
            RealtimeConnectionException: If connecting fails.
            RealtimeControlException: If Foundry rejects the initial settings.
        """
        if self.definition is None:
            raise RuntimeError(
                "The Foundry voice agent's definition hasn't been read. Enter the agent with 'async with' before "
                "starting a conversation."
            )
        manager = self.project_client.beta.voice_agents.realtime.connect(
            agent_name=self.agent_name, api_version=VOICE_AGENT_API_VERSION, extra_headers=self._handshake_headers()
        )
        try:
            connection = await manager.__aenter__()
        except ConnectionError as error:
            raise RealtimeConnectionException(f"Couldn't connect to the Foundry voice agent: {error}") from error
        session = _FoundryVoiceAgentSession(_FoundryVoiceAgentConnection(manager, connection), self.definition)
        try:
            await session.start(options=options, tools=tools)
        except BaseException:
            await session.close()
            raise
        return session

    def _handshake_headers(self) -> dict[str, Any]:
        """Every session is pinned to the version whose definition the agent holds.

        The SDK forwards an agent_version keyword into the websocket handshake, so the version goes as a header.
        Foundry joins its spans for the session to the caller's trace; baggage can carry application data, so only the
        trace context goes.
        """
        headers = prepend_agent_framework_to_user_agent({"x-agent-version-override": str(self.agent_version)})
        _TRACE_CONTEXT.inject(headers)
        return headers


class _FoundryVoiceAgentSession(OpenAIRealtimeSession[FoundryVoiceAgentOptions]):
    """A Foundry voice agent's session, which speaks the GA protocol.

    Foundry acknowledges client items with conversation.item.created and server items with .added.
    """

    _EVENT_ALIASES: ClassVar[Mapping[str, str]] = {"conversation.item.created": "conversation.item.added"}
    _VOICE_OBJECTS_ACCEPTED: ClassVar[bool] = True
    _SESSION_OPTION_KEYS: ClassVar[frozenset[str]] = OpenAIRealtimeSession._NEUTRAL_OPTION_KEYS | {"echo_cancellation"}

    def __init__(self, connection: OpenAIRealtimeConnection, definition: Mapping[str, Any]) -> None:
        super().__init__(connection)
        self._definition = definition

    async def start(
        self, *, options: FoundryVoiceAgentOptions | None = None, tools: Sequence[ToolTypes] | None = None
    ) -> None:
        if tools is not None and _includes_definition(tools):
            self._warn_about_unimplemented_functions(tools)
        elif tools is not None:
            # The session opens on the definition's settings, so another participant mustn't inherit its tool choice,
            # just as none carries over on a handoff.
            options = cast("FoundryVoiceAgentOptions", {"tool_choice": "auto", **(options or {})})
        await super().start(options=options, tools=tools)

    def _warn_about_unimplemented_functions(self, tools: Sequence[ToolTypes]) -> None:
        implemented = {tool.name for tool in tools if isinstance(tool, FunctionTool)}
        for entry in self._published():
            if entry.get("type") == "function" and entry.get("name") not in implemented:
                logger.warning(
                    "The Foundry voice agent's definition declares function %r, but no tool implements it, so calls "
                    "to it get an error result.",
                    entry.get("name"),
                )

    def _session_payload(self, options: dict[str, Any], tools: Sequence[ToolTypes] | None) -> dict[str, Any]:
        neutral = {key: value for key, value in options.items() if key != "echo_cancellation"}
        session = super()._session_payload(neutral, None)
        if "echo_cancellation" in options:
            session.setdefault("audio", {}).setdefault("input", {})["echo_cancellation"] = options["echo_cancellation"]
        if tools is not None and not self._declares_already(tools):
            session["tools"] = self._declarations(tools)
        if session:
            session["type"] = "realtime"
        return session

    def _declarations(self, tools: Sequence[ToolTypes]) -> list[Any]:
        if not _includes_definition(tools):
            return self._tool_declarations(tools)
        return [dict(entry) for entry in self._published()] + self._tool_declarations(self._added_to_definition(tools))

    def _declares_already(self, tools: Sequence[ToolTypes]) -> bool:
        """Whether the session declares the definition's tools, and ``tools`` add nothing to them.

        Re-sending a definition's own tools is left out, so its hosted tools stay exactly as Foundry set them up.
        """
        if not _includes_definition(tools) or self._added_to_definition(tools):
            return False
        declared = [entry.get("name") for entry in self._session_state.get("tools", [])]
        return declared == [entry.get("name") for entry in self._published()]

    def _published(self) -> list[Mapping[str, Any]]:
        return list(self._definition.get("tools", []))

    def _added_to_definition(self, tools: Sequence[ToolTypes]) -> list[ToolTypes]:
        # A function tool the definition declares runs as that function; the model keeps the published schema.
        published_names = {entry.get("name") for entry in self._published()}
        return [
            tool
            for tool in tools
            if tool is not DEFINITION_TOOLS and not (isinstance(tool, FunctionTool) and tool.name in published_names)
        ]

    def _voice_change_allowed(self, voice: Any) -> bool:
        """Foundry changes voices at any time, but rejects a change from an Azure voice to an OpenAI voice."""
        return not isinstance(self._current_voice(), Mapping) or isinstance(voice, Mapping)

    def _current_voice(self) -> Any:
        return voice_from(self._session_state.get("audio", {}).get("output", {}))

    async def _before_initial_update(self) -> None:
        """The agent definition arrives unasked in a session.updated, which must never pass for an acknowledgement.

        ``options`` start from the audio settings it applies, so history and audio labelling see what the session
        runs even where the app's options are silent.
        """
        while (await self._read_into_buffer())["type"] != "session.updated":
            pass
        self.options = _audio_settings_of(self._session_state)


class _FoundryVoiceAgentConnection:
    """Raw JSON keeps the SDK's models out of the shared protocol mapping."""

    def __init__(self, manager: Any, connection: Any) -> None:
        self._manager = manager
        self._connection = connection

    async def send(self, event: Mapping[str, Any]) -> None:
        try:
            await self._connection.send(event)
        except ConnectionError as error:
            raise RealtimeConnectionException(f"Couldn't send to the Foundry voice agent: {error}") from error

    async def receive(self) -> Mapping[str, Any] | None:
        try:
            event = await self._connection.recv()
        except ConnectionResetError as reset:
            # The SDK chains a cause only for an abnormal close or a transport error.
            if reset.__cause__ is None:
                return None
            raise RealtimeConnectionException(f"The Foundry voice agent's connection failed: {reset}") from reset
        # The SDK returns a typed model for event types it knows, and a dict for the rest.
        return event.as_dict() if hasattr(event, "as_dict") else event

    async def close(self) -> None:
        await self._manager.__aexit__(None, None, None)


def voice_from(audio_output: Mapping[str, Any]) -> str | AzureVoice | None:
    """The voice a voice agent stores in ``audio_output``.

    An Azure voice is stored flattened, so it's rebuilt as an ``AzureVoice``. An OpenAI voice stays a name, and a voice
    object a session echoes as it was sent is returned as it is.
    """
    voice = audio_output.get("voice")
    if not isinstance(voice, str) or not str(audio_output.get("voice_type", "")).startswith("azure"):
        return voice
    rebuilt = {
        field: audio_output[stored]
        for stored, field in _STORED_VOICE_FIELDS.items()
        if audio_output.get(stored) is not None
    }
    return cast("AzureVoice", rebuilt)


def _includes_definition(tools: Sequence[ToolTypes]) -> bool:
    """Whether ``tools`` belong to the agent whose definition they stand in for, which is speaking."""
    return any(tool is DEFINITION_TOOLS for tool in tools)


def _audio_settings_of(session: Mapping[str, Any]) -> FoundryVoiceAgentOptions:
    """The audio settings a voice-agent session runs, as neutral options."""
    audio = session.get("audio", {})
    audio_input, audio_output = audio.get("input", {}), audio.get("output", {})
    settings = {key: audio_input[key] for key in _INPUT_AUDIO_SETTINGS if key in audio_input}
    for option, direction in (("input_audio_format", audio_input), ("output_audio_format", audio_output)):
        if direction.get("format"):
            settings[option] = _media_type(direction["format"])
    return cast("FoundryVoiceAgentOptions", settings)


def _media_type(audio_format: Mapping[str, Any]) -> str:
    """The neutral media type for a GA audio format object, such as ``audio/pcm;rate=24000``."""
    rate = audio_format.get("rate")
    return f"{audio_format['type']};rate={rate}" if rate else str(audio_format["type"])


def _require_voice_agents_sdk() -> None:
    if importlib.util.find_spec("azure.ai.projects.aio._realtime") is None:
        raise ModuleNotFoundError(
            "FoundryVoiceAgentClient needs azure-ai-projects 2.7 or later for the voice-agent connection, which the "
            "realtime extra installs: pip install 'agent-framework-foundry[realtime]'"
        )
