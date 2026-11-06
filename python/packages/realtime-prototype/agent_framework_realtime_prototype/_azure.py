# PROTOTYPE, never merged (adamdougal/agent-framework#10).
"""``VoiceLiveClient`` (preview shape, ``2026-07-15``) and ``FoundryVoiceAgentClient`` (GA shape, preview service) (#8)."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Sequence
from typing import Any

from ._core import RealtimeError, RealtimeUnsupportedError, logger
from ._ga_session import AI_SCOPE, OpenAIRealtimeSession, declare_tool

# region Voice Live


def _is_azure_voice(voice: Any) -> bool:
    return isinstance(voice, dict) and str(voice.get("type", "")).startswith("azure")


class VoiceLiveSession(OpenAIRealtimeSession):
    """Voice Live's preview shape: a flat session, ``response.audio.*`` events and no ``session.type``."""

    metadata_namespace = "voice_live"
    assistant_text_part = "text"
    allows_voice_change_after_audio = True

    def build_session(self, options: dict[str, Any], tools: Sequence[Any] | None) -> dict[str, Any]:
        session: dict[str, Any] = {}
        for key, value in options.items():
            match key:
                case "instructions" | "voice" | "tool_choice" | "temperature":
                    session[key] = value
                case "max_tokens":
                    session["max_response_output_tokens"] = value
                case "output_modalities":
                    session["modalities"] = value
                case "input_audio_format" | "output_audio_format":
                    if value not in (None, "audio/pcm;rate=24000"):
                        raise RealtimeError(f"Prototype Voice Live client only does 24 kHz PCM, not {value}")
                    session[key] = "pcm16"
                case "turn_detection":
                    session["turn_detection"] = value
                case "transcription":
                    session["input_audio_transcription"] = value
                case "noise_reduction":
                    session["input_audio_noise_reduction"] = value
                case "echo_cancellation":
                    session["input_audio_echo_cancellation"] = value
                case _:
                    logger.warning("Dropping option %r: Voice Live can't honour it.", key)
        if tools is not None:
            session["tools"] = [declare_tool(tool) for tool in tools]
        return session

    def build_response(self, options: dict[str, Any]) -> dict[str, Any]:
        if "instructions" in options:
            logger.warning("Dropping per-response instructions: Voice Live ignores them (#13).")
            options = {key: value for key, value in options.items() if key != "instructions"}
        return super().build_response(options)

    def say_response(self, text: str) -> dict[str, Any]:
        if not _is_azure_voice(self.current_voice()):
            raise RealtimeUnsupportedError("say() on Voice Live needs an Azure voice (#8).")
        return {
            "pre_generated_assistant_message": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "text", "text": text}],
            }
        }

    def current_voice(self) -> Any:
        return self.session_state.get("voice")


class _VoiceLiveConnection:
    def __init__(self, manager: Any, connection: Any) -> None:
        self._manager = manager
        self._connection = connection
        self.close_code: int | None = None

    async def send(self, event: dict[str, Any]) -> None:
        await self._connection.send(event)

    async def recv(self) -> dict[str, Any] | None:
        from azure.ai.voicelive.aio import ConnectionClosed

        try:
            raw = await self._connection.recv_bytes()
        except ConnectionClosed as exc:
            self.close_code = getattr(exc, "code", None)
            if self.close_code in (1000, None):
                return None
            raise RealtimeError(f"Voice Live closed with code {self.close_code}") from exc
        if not raw:
            # recv_bytes() returns b"" once the socket is closed locally.
            return None
        return json.loads(raw)

    async def close(self) -> None:
        await self._manager.__aexit__(None, None, None)


class VoiceLiveClient:
    session_class = VoiceLiveSession

    def __init__(
        self,
        *,
        endpoint: str,
        model: str,
        credential: Any = None,
        api_key: str | None = None,
        api_version: str = "2026-07-15",
        profile: str | None = None,
        foundry_resource_override: str | None = None,
        ack_timeout: float = 10.0,
    ) -> None:
        if api_version == "v1":
            raise RealtimeError("VoiceLiveClient doesn't support the undocumented GA-shaped api-version v1 (#8).")
        self.endpoint = endpoint
        self.model = model
        self.credential = credential
        self.api_key = api_key
        self.api_version = api_version
        self.profile = profile
        self.foundry_resource_override = foundry_resource_override
        self.ack_timeout = ack_timeout
        self.provider_name = "azure.ai.foundry"
        self.server_address = endpoint.split("//")[-1].split("/")[0]

    async def connect(self, *, headers: dict[str, str] | None = None) -> VoiceLiveSession:
        from azure.ai.voicelive.aio import connect
        from azure.core.credentials import AzureKeyCredential

        try:
            from azure.ai.voicelive.telemetry import VoiceLiveInstrumentor

            if VoiceLiveInstrumentor().is_instrumented():
                logger.warning(
                    "VoiceLiveInstrumentor is active: it adds a span for every outbound event, audio chunks "
                    "included, alongside Agent Framework's realtime spans. Call VoiceLiveInstrumentor().uninstrument() "
                    "to turn it off."
                )
        except ImportError:
            pass
        query: dict[str, str] = {}
        if self.profile:
            # The SDK silently ignores a profile= keyword, so BYOM goes through query (#8).
            query["profile"] = self.profile
        if self.foundry_resource_override:
            query["foundry-resource-override"] = self.foundry_resource_override
        credential = AzureKeyCredential(self.api_key) if self.api_key else self.credential
        manager = connect(
            credential=credential,
            endpoint=self.endpoint,
            api_version=self.api_version,
            model=self.model,
            query=query or None,
            headers={"User-Agent": "agent-framework-python/realtime-prototype", **(headers or {})},
        )
        connection = await manager.__aenter__()
        return self.session_class(_VoiceLiveConnection(manager, connection), ack_timeout=self.ack_timeout)


# endregion

# region Foundry voice agents


class FoundryVoiceSession(OpenAIRealtimeSession):
    """GA shape. The agent definition arrives in an unsolicited ``session.updated`` after ``session.created``."""

    metadata_namespace = "foundry"
    allows_voice_change_after_audio = True

    def __init__(self, connection: Any, *, ack_timeout: float = 10.0) -> None:
        super().__init__(connection, ack_timeout=ack_timeout)
        self.definition: dict[str, Any] | None = None
        self.definition_delay_s: float | None = None

    async def capture_definition(self) -> dict[str, Any]:
        """Read until the definition's ``session.updated``, buffering events for ``receive()``."""
        start = time.monotonic()
        created_at: float | None = None
        deadline = start + self.ack_timeout
        while self.definition is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RealtimeError("The Foundry voice agent's definition didn't arrive.")
            wire = await asyncio.wait_for(self.connection.recv(), remaining)
            if wire is None:
                raise RealtimeError("Foundry closed before sending its agent definition.")
            self._buffered.append(wire)
            if wire.get("type") == "session.created":
                created_at = time.monotonic()
                self.session_state = wire.get("session") or {}
            elif wire.get("type") == "session.updated":
                self.definition = wire.get("session") or {}
                self.session_state = self.definition
                self.definition_delay_s = time.monotonic() - (created_at or start)
        return self.definition

    def build_session(self, options: dict[str, Any], tools: Sequence[Any] | None) -> dict[str, Any]:
        voice = options.get("voice")
        if voice is not None and _is_azure_voice(self.current_voice()) and not _is_azure_voice(voice):
            logger.warning("Dropping voice %r: Foundry rejects a switch from an Azure to an OpenAI voice (#13).", voice)
            options = {key: value for key, value in options.items() if key != "voice"}
        if isinstance(options.get("voice"), dict):
            # The base drops Azure voice objects; Foundry takes them inside audio.output.voice.
            azure_voice = options["voice"]
            session = super().build_session({k: v for k, v in options.items() if k != "voice"}, tools)
            session.setdefault("audio", {}).setdefault("output", {})["voice"] = azure_voice
            session["type"] = "realtime"
            return session
        return super().build_session(options, tools)

    def say_response(self, text: str) -> dict[str, Any]:
        if _is_azure_voice(self.current_voice()):
            return {
                "pre_generated_assistant_message": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "text", "text": text}],
                }
            }
        return super().say_response(text)

    def current_voice(self) -> Any:
        """Foundry echoes a voice object, or a name plus a sibling voice_type. Normalise to an AzureVoice."""
        output = (self.session_state.get("audio") or {}).get("output") or {}
        voice = output.get("voice")
        if isinstance(voice, str) and str(output.get("voice_type", "")).startswith("azure"):
            return {"name": voice, "type": output["voice_type"]}
        return voice


class _FoundryConnection:
    def __init__(self, manager: Any, connection: Any, project: Any) -> None:
        self._manager = manager
        self._connection = connection
        self._project = project
        self.close_code: int | None = None

    async def send(self, event: dict[str, Any]) -> None:
        await self._connection.send(event)

    async def recv(self) -> dict[str, Any] | None:
        try:
            event = await self._connection.recv()
        except ConnectionResetError as exc:
            if exc.__cause__ is None:
                return None
            raise RealtimeError(f"Foundry voice agent socket closed abnormally: {exc.__cause__}") from exc
        return event.as_dict() if hasattr(event, "as_dict") else dict(event)

    async def close(self) -> None:
        await self._manager.__aexit__(None, None, None)
        await self._project.close()


class FoundryVoiceAgentClient:
    session_class = FoundryVoiceSession

    def __init__(
        self,
        *,
        project_endpoint: str,
        agent_name: str,
        credential: Any,
        agent_version: str | None = None,
        ack_timeout: float = 10.0,
    ) -> None:
        self.project_endpoint = project_endpoint
        self.agent_name = agent_name
        self.agent_version = agent_version
        self.credential = credential
        self.ack_timeout = ack_timeout
        self.provider_name = "azure.ai.foundry"
        self.server_address = project_endpoint.split("//")[-1].split("/")[0]
        self.model = None
        self.definition: dict[str, Any] | None = None

    async def connect(self, *, headers: dict[str, str] | None = None) -> FoundryVoiceSession:
        from azure.ai.projects.aio import AIProjectClient

        project = AIProjectClient(endpoint=self.project_endpoint, credential=self.credential)
        extra_headers = dict(headers or {})
        if self.agent_version:
            # azure-ai-projects 2.7.0 forwards an agent_version keyword into aiohttp's ws_connect (prototype finding).
            extra_headers["x-agent-version-override"] = self.agent_version
        manager = project.beta.voice_agents.realtime.connect(
            agent_name=self.agent_name,
            api_version="v1",
            extra_headers=extra_headers,
        )
        connection = await manager.__aenter__()
        session = self.session_class(_FoundryConnection(manager, connection, project), ack_timeout=self.ack_timeout)
        self.definition = await session.capture_definition()
        self.model = self.definition.get("model")
        return session


# endregion

__all__ = [
    "AI_SCOPE",
    "FoundryVoiceAgentClient",
    "FoundryVoiceSession",
    "VoiceLiveClient",
    "VoiceLiveSession",
]
