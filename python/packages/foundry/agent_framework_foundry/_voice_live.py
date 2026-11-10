# Copyright (c) Microsoft. All rights reserved.

"""Azure Voice Live as a realtime provider, on the shared OpenAI Realtime session."""

from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
import logging
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import date
from types import TracebackType
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Self, TypedDict, cast

from agent_framework import SecretString, load_settings, prepend_agent_framework_to_user_agent
from agent_framework._feature_stage import ExperimentalFeature, experimental
from agent_framework.exceptions import RealtimeConnectionException, RealtimeUnsupportedException
from agent_framework_openai._realtime_session import OpenAIRealtimeSession
from azure.core.credentials import AzureKeyCredential, TokenCredential
from azure.core.credentials_async import AsyncTokenCredential
from azure.core.exceptions import AzureError

from ._realtime_options import VoiceLiveOptions, VoiceLiveOptionsT

if TYPE_CHECKING:
    from agent_framework import RealtimeClientSessionProtocol, ToolTypes

AzureTokenProvider = Callable[[], str | Awaitable[str]]

DEFAULT_API_VERSION = "2026-07-15"
"""The Voice Live API version the client pins: the documented, preview-shaped one."""

_PROVIDED_TOKEN_LIFETIME_SECONDS = 300
"""The expiry reported for a token from a provider callable, which doesn't say. The SDK reads only the token."""

_NORMAL_CLOSURE = 1000
"""The websocket close code for a normal close, including the SDK's report of a socket closed locally."""

logger = logging.getLogger("agent_framework.foundry")

_INPUT_AUDIO_FORMATS: dict[str, dict[str, Any]] = {
    "audio/pcm;rate=24000": {"input_audio_format": "pcm16"},
    "audio/pcm;rate=16000": {"input_audio_format": "pcm16", "input_audio_sampling_rate": 16000},
    "audio/pcmu": {"input_audio_format": "g711_ulaw"},
    "audio/pcma": {"input_audio_format": "g711_alaw"},
}
"""The caller audio formats Voice Live takes, as its session fields."""

_OUTPUT_AUDIO_FORMATS: dict[str, dict[str, Any]] = {
    "audio/pcm;rate=24000": {"output_audio_format": "pcm16"},
    "audio/pcm;rate=16000": {"output_audio_format": "pcm16_16000hz"},
    "audio/pcm;rate=8000": {"output_audio_format": "pcm16_8000hz"},
    "audio/pcmu": {"output_audio_format": "g711_ulaw"},
    "audio/pcma": {"output_audio_format": "g711_alaw"},
}
"""The agent audio formats Voice Live produces, as its session fields."""

_FOUNDRY_EVENT_PREFIXES = (
    "warning",
    "response.animation_",
    "response.audio_timestamp.",
    "response.audio_transcript.annotation.",
    "response.foundry_agent_call",
    "response.invocation.",
    "response.web_search_call.",
    "response.file_search_call.",
    "response.video.",
    "session.avatar.",
)
"""The names of server events that exist only in Voice Live, which carry their payloads under ``foundry``."""


def _check_audio_formats(options: Mapping[str, Any]) -> None:
    """Raise before connecting for an audio format Voice Live can't take: a dropped format would garble the audio."""
    if "input_audio_format" in options:
        _voice_live_format(options["input_audio_format"], _INPUT_AUDIO_FORMATS, "input")
    if "output_audio_format" in options:
        _voice_live_format(options["output_audio_format"], _OUTPUT_AUDIO_FORMATS, "output")


def _voice_live_format(media_type: str, formats: dict[str, dict[str, Any]], direction: str) -> dict[str, Any]:
    """The session fields for a neutral audio media type, raising for one Voice Live can't take."""
    if media_type not in formats:
        raise ValueError(f"Unsupported {direction} audio format {media_type!r}: Voice Live takes {', '.join(formats)}.")
    return dict(formats[media_type])


class VoiceLiveSettings(TypedDict, total=False):
    """Voice Live settings, read from ``AZURE_VOICELIVE_*`` variables when not passed."""

    endpoint: str | None
    api_key: SecretString | None
    model: str | None
    api_version: str | None


@experimental(feature_id=ExperimentalFeature.REALTIME)
class VoiceLiveClient(Generic[VoiceLiveOptionsT]):
    """A realtime client for Azure Voice Live.

    It opens live voice sessions with a managed model or with your own Azure OpenAI realtime deployment.

    It needs the realtime extra: ``pip install 'agent-framework-foundry[realtime]'``. Settings not passed are read from
    ``AZURE_VOICELIVE_ENDPOINT``, ``AZURE_VOICELIVE_MODEL``, ``AZURE_VOICELIVE_API_KEY`` and
    ``AZURE_VOICELIVE_API_VERSION``.

    Examples:
        .. code-block:: python

            from agent_framework import Message
            from agent_framework_foundry import VoiceLiveClient
            from azure.identity.aio import AzureCliCredential

            async with AzureCliCredential() as credential:
                client = VoiceLiveClient(credential=credential)
                async with await client.start_session(options={"instructions": "Be brief."}) as session:
                    await session.send(Message("user", ["Hello!"]))
                    await session.create_response()
                    async for event in session.receive():
                        ...

    Attributes:
        endpoint: The Voice Live endpoint.
        model: The model, or for your own deployment, its name.
        api_version: The Voice Live API version.
        profile: The connection profile, such as ``"byom-azure-openai-realtime"`` for your own deployment.
        foundry_resource_override: The resource that hosts your own deployment.
    """

    OTEL_PROVIDER_NAME: ClassVar[str] = "azure.ai.foundry"

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        model: str | None = None,
        api_key: str | SecretString | None = None,
        credential: AzureKeyCredential | AsyncTokenCredential | TokenCredential | AzureTokenProvider | None = None,
        api_version: str | None = None,
        profile: str | None = None,
        foundry_resource_override: str | None = None,
        env_file_path: str | None = None,
        env_file_encoding: str | None = None,
    ) -> None:
        """Create a Voice Live client. Nothing connects until ``start_session(...)``.

        Keyword Args:
            endpoint: The Voice Live endpoint, such as ``https://<resource>.services.ai.azure.com``. Read from
                ``AZURE_VOICELIVE_ENDPOINT`` when not passed.
            model: The model, such as ``"gpt-realtime"``, or your deployment's name. Read from
                ``AZURE_VOICELIVE_MODEL`` when not passed.
            api_key: The resource key. Read from ``AZURE_VOICELIVE_API_KEY`` when neither it nor ``credential`` is
                passed.
            credential: An ``AzureKeyCredential``, an async or sync token credential, or a callable that returns a
                token. The client never closes it.
            api_version: A dated Voice Live API version, ``2026-07-15`` or later. Defaults to ``2026-07-15``.
            profile: The connection profile, such as ``"byom-azure-openai-realtime"`` for your own deployment.
            foundry_resource_override: The resource that hosts your own deployment.
            env_file_path: A ``.env`` file to read settings from.
            env_file_encoding: The ``.env`` file's encoding.

        Raises:
            ValueError: If the endpoint or model is missing, the API version isn't supported, or both ``api_key`` and
                ``credential`` are passed.
            ModuleNotFoundError: If the realtime extra isn't installed.
        """
        if api_key is not None and credential is not None:
            raise ValueError("Pass either 'api_key' or 'credential' to VoiceLiveClient, not both.")
        _require_voice_live_sdk()
        settings = load_settings(
            VoiceLiveSettings,
            env_prefix="AZURE_VOICELIVE_",
            endpoint=endpoint,
            model=model,
            api_key=api_key,
            api_version=api_version,
            env_file_path=env_file_path,
            env_file_encoding=env_file_encoding,
        )
        resolved_endpoint = settings.get("endpoint")
        resolved_model = settings.get("model")
        if not resolved_endpoint:
            raise ValueError(
                "Voice Live needs an endpoint: pass 'endpoint' or set the AZURE_VOICELIVE_ENDPOINT environment "
                "variable."
            )
        if not resolved_model:
            raise ValueError(
                "Voice Live needs a model: pass 'model' or set the AZURE_VOICELIVE_MODEL environment variable. For "
                "BYOM, it's your deployment's name."
            )
        self.endpoint: str = resolved_endpoint
        self.model: str = resolved_model
        self.api_version = _supported_api_version(settings.get("api_version") or DEFAULT_API_VERSION)
        self.profile = profile
        self.foundry_resource_override = foundry_resource_override
        self._api_key = settings.get("api_key")
        self._credential = credential

    async def start_session(
        self, *, options: VoiceLiveOptionsT | None = None, tools: Sequence[ToolTypes] | None = None
    ) -> RealtimeClientSessionProtocol[VoiceLiveOptionsT]:
        """Connect, apply the initial settings, and return the session once Voice Live acknowledges them.

        The caller owns the session and closes it. If opening fails or is cancelled, the socket is closed first.

        Keyword Args:
            options: The session's initial settings.
            tools: The tools to declare.

        Raises:
            RealtimeConnectionException: If connecting fails, including a rejected key or token.
            RealtimeControlException: If Voice Live rejects the initial settings.
            ValueError: If an audio format is one Voice Live can't take. Nothing connects.
        """
        from azure.ai.voicelive.aio import connect

        _check_audio_formats(options or {})
        manager = connect(
            credential=self._sdk_credential(),
            endpoint=self.endpoint,
            api_version=self.api_version,
            model=self.model,
            query=self._handshake_query() or None,
            headers=prepend_agent_framework_to_user_agent(),
        )
        try:
            connection = await manager.__aenter__()
        except AzureError as error:
            raise RealtimeConnectionException(f"Couldn't connect to Voice Live: {error}") from error
        session = _VoiceLiveSession(_VoiceLiveConnection(manager, connection))
        try:
            await session.start(options=cast("VoiceLiveOptions | None", options), tools=tools)
        except BaseException:
            await session.close()
            raise
        return cast("RealtimeClientSessionProtocol[VoiceLiveOptionsT]", session)

    def _sdk_credential(self) -> AzureKeyCredential | AsyncTokenCredential:
        """The credential the Voice Live SDK takes.

        A key, or an ``AzureKeyCredential``, is sent as the ``api-key`` header, and an async token credential passes
        through. A sync token credential is wrapped so its ``get_token`` runs in a worker thread, rather than blocking
        the event loop as the SDK would.
        """
        credential = self._credential
        if credential is None:
            return AzureKeyCredential(self._api_key.get_secret_value() if self._api_key is not None else "")
        if isinstance(credential, (AzureKeyCredential, AsyncTokenCredential)):
            return credential
        if callable(credential) and not hasattr(credential, "get_token"):
            return _TokenProviderCredential(credential)
        return _ThreadedTokenCredential(credential)

    def _handshake_query(self) -> dict[str, str]:
        """BYOM's query parameters. The SDK ignores a ``profile=`` keyword, so they go through its ``query``."""
        return {
            name: value
            for name, value in (
                ("profile", self.profile),
                ("foundry-resource-override", self.foundry_resource_override),
            )
            if value
        }


class _CredentialAdapter:
    """Presents another credential to the Voice Live SDK as an async token credential.

    It never closes the original, which belongs to the caller.
    """

    def __init__(self, credential: Any) -> None:
        self._credential = credential

    async def close(self) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        pass


class _ThreadedTokenCredential(_CredentialAdapter):
    """A sync token credential, fetching tokens in a worker thread so the event loop isn't blocked."""

    async def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        return await asyncio.to_thread(self._credential.get_token, *scopes, **kwargs)


class _TokenProviderCredential(_CredentialAdapter):
    """An async token credential over a token-provider callable, sync or async, which already knows its scope."""

    async def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        from azure.core.credentials import AccessToken

        token = self._credential()
        if inspect.isawaitable(token):
            token = await token
        return AccessToken(token, int(time.time()) + _PROVIDED_TOKEN_LIFETIME_SECONDS)


class _VoiceLiveSession(OpenAIRealtimeSession[VoiceLiveOptions]):
    """Voice Live's preview-shaped session on the shared OpenAI Realtime session.

    Voice Live ``2026-07-15`` keeps the preview shape: a flat session with no ``type``, ``response.audio.*`` events,
    and ``text`` content parts for assistant text. It rejects every event before the first session.update, so the
    session always sends one when it opens.
    """

    _INITIAL_UPDATE_REQUIRED: ClassVar[bool] = True
    _VOICE_OBJECTS_ACCEPTED: ClassVar[bool] = True
    _ASSISTANT_TEXT_PART: ClassVar[str] = "text"
    _EVENT_ALIASES: ClassVar[Mapping[str, str]] = {
        "response.audio.delta": "response.output_audio.delta",
        "response.audio.done": "response.output_audio.done",
        "response.audio_transcript.delta": "response.output_audio_transcript.delta",
        "response.audio_transcript.done": "response.output_audio_transcript.done",
        "response.text.delta": "response.output_text.delta",
        "response.text.done": "response.output_text.done",
        "conversation.item.created": "conversation.item.added",
    }
    _SESSION_OPTION_KEYS: ClassVar[frozenset[str]] = OpenAIRealtimeSession._SESSION_OPTION_KEYS | {
        "temperature",
        "metadata",
        "echo_cancellation",
        "animation",
        "interim_response",
        "reasoning_effort",
        "parallel_tool_calls",
        "output_audio_timestamp_types",
    }

    def _session_payload(self, options: dict[str, Any], tools: Any) -> dict[str, Any]:
        """Translate honored options and tools into Voice Live's flat session object."""
        session: dict[str, Any] = {}
        for key, value in options.items():
            match key:
                case "max_tokens":
                    session["max_response_output_tokens"] = value
                case "transcription":
                    session["input_audio_transcription"] = value
                case "noise_reduction":
                    session["input_audio_noise_reduction"] = value
                case "echo_cancellation":
                    session["input_audio_echo_cancellation"] = value
                case "tool_choice":
                    session["tool_choice"] = self._translate_tool_choice(value)
                case "output_modalities":
                    # Voice Live can't produce audio alone; the agent transcript always comes with it.
                    modalities = list(value)
                    if "audio" in modalities and "text" not in modalities:
                        modalities.insert(0, "text")
                    session["modalities"] = modalities
                case "input_audio_format":
                    session.update(_voice_live_format(value, _INPUT_AUDIO_FORMATS, "input"))
                case "output_audio_format":
                    session.update(_voice_live_format(value, _OUTPUT_AUDIO_FORMATS, "output"))
                case _:
                    session[key] = value
        if tools is not None:
            session["tools"] = self._tool_declarations(tools)
        return session

    def _say_payload(self, text: str) -> dict[str, Any]:
        """Pre-generated speech, which Voice Live supports only with an Azure TTS voice.

        Voice Live ignores per-response instructions, so the best-effort prompt other providers use can't work here.
        """
        if not self._speaks_with_azure_tts_voice():
            raise RealtimeUnsupportedException(
                "Voice Live speaks text verbatim only with an Azure text-to-speech voice, not "
                f"{self._current_voice()!r}."
            )
        return super()._say_payload(text)

    def _current_voice(self) -> Any:
        """The voice Voice Live last reported, from its flat session."""
        return self._session_state.get("voice")

    def _voice_change_allowed(self, voice: Any) -> bool:
        """Voice Live takes a new voice at any time.

        A change from an OpenAI voice is ignored, which ``update_session`` reports when the echo keeps the old voice.
        """
        return True

    def _event_namespace(self, service_event_type: str) -> str:
        """``foundry`` for an event only Voice Live sends, ``openai`` for an event of the OpenAI Realtime protocol."""
        return "foundry" if service_event_type.startswith(_FOUNDRY_EVENT_PREFIXES) else "openai"

    def _response_payload(self, options: dict[str, Any]) -> dict[str, Any]:
        """The GA response object, adapted to Voice Live.

        Per-response instructions are dropped, because Voice Live ignores them, and the output token cap takes its
        preview name.
        """
        if "instructions" in options:
            logger.warning("Dropping per-response instructions: Voice Live ignores them.")
            options = {key: value for key, value in options.items() if key != "instructions"}
        response = super()._response_payload(options)
        if "max_output_tokens" in response:
            response["max_response_output_tokens"] = response.pop("max_output_tokens")
        return response


class _VoiceLiveConnection:
    """An ``OpenAIRealtimeConnection`` over the Voice Live SDK's websocket, used only as a transport."""

    def __init__(self, manager: Any, connection: Any) -> None:
        self._manager = manager
        self._connection = connection

    async def send(self, event: Mapping[str, Any]) -> None:
        try:
            await self._connection.send(event)
        except AzureError as error:
            raise RealtimeConnectionException(f"Couldn't send to Voice Live: {error}") from error

    async def receive(self) -> Mapping[str, Any] | None:
        from azure.ai.voicelive.aio import ConnectionClosed

        try:
            frame = await self._connection.recv_bytes()
        except ConnectionClosed as closed:
            if closed.code == _NORMAL_CLOSURE:
                return None
            raise RealtimeConnectionException(
                f"Voice Live closed the connection with code {closed.code}.", close_code=closed.code
            ) from closed
        return json.loads(frame)

    async def close(self) -> None:
        await self._manager.__aexit__(None, None, None)


def _require_voice_live_sdk() -> None:
    """Raise an install hint unless the Voice Live SDK, which the ``realtime`` extra installs, is available."""
    try:
        found = importlib.util.find_spec("azure.ai.voicelive") is not None
    except ModuleNotFoundError:
        found = False
    if not found:
        raise ModuleNotFoundError(
            "VoiceLiveClient needs the Voice Live SDK, which the realtime extra installs: "
            "pip install 'agent-framework-foundry[realtime]'"
        )


def _supported_api_version(api_version: str) -> str:
    """``api_version`` if it's a dated version of the preview shape this client speaks, on or after 2026-07-15.

    ISO dates sort as strings. The undocumented, GA-shaped ``v1`` isn't supported.
    """
    try:
        date.fromisoformat(api_version)
    except ValueError:
        supported = False
    else:
        supported = api_version >= DEFAULT_API_VERSION
    if not supported:
        raise ValueError(
            f"Unsupported Voice Live api_version {api_version!r}: VoiceLiveClient speaks the preview shape of "
            f"{DEFAULT_API_VERSION} and later dated versions."
        )
    return api_version
