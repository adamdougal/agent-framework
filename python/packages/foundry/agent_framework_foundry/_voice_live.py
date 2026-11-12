# Copyright (c) Microsoft. All rights reserved.

"""Azure Voice Live as a realtime provider, on the shared OpenAI Realtime session."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import importlib.util
import inspect
import json
import logging
import re
import time
from collections.abc import Awaitable, Callable, Generator, Mapping, Sequence
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
    "audio/pcm;rate=24000": {"input_audio_format": "pcm16", "input_audio_sampling_rate": 24000},
    "audio/pcm;rate=16000": {"input_audio_format": "pcm16", "input_audio_sampling_rate": 16000},
    "audio/pcm;rate=8000": {"input_audio_format": "pcm16", "input_audio_sampling_rate": 8000},
    "audio/pcmu": {"input_audio_format": "g711_ulaw", "input_audio_sampling_rate": 8000},
    "audio/pcma": {"input_audio_format": "g711_alaw", "input_audio_sampling_rate": 8000},
}
"""The caller audio formats Voice Live takes, as its session fields. Each sends its rate, because updates are partial
and would otherwise keep an earlier one."""

_OUTPUT_AUDIO_FORMATS: dict[str, dict[str, Any]] = {
    "audio/pcm;rate=24000": {"output_audio_format": "pcm16"},
    "audio/pcm;rate=16000": {"output_audio_format": "pcm16_16000hz"},
    "audio/pcm;rate=8000": {"output_audio_format": "pcm16_8000hz"},
    "audio/pcmu": {"output_audio_format": "g711_ulaw"},
    "audio/pcma": {"output_audio_format": "g711_alaw"},
}

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


def _check_audio_formats(options: Mapping[str, Any]) -> None:
    """Raise before connecting for an audio format Voice Live can't take: a dropped format would garble the audio."""
    if "input_audio_format" in options:
        _voice_live_format(options["input_audio_format"], _INPUT_AUDIO_FORMATS, "input")
    if "output_audio_format" in options:
        _voice_live_format(options["output_audio_format"], _OUTPUT_AUDIO_FORMATS, "output")


def _voice_live_modalities(output_modalities: Sequence[str]) -> list[str]:
    """Voice Live's modalities: it can't produce audio alone, so the agent transcript always comes with it."""
    modalities = list(output_modalities)
    if "audio" in modalities and "text" not in modalities:
        modalities.insert(0, "text")
    return modalities


def _voice_live_format(media_type: str, formats: dict[str, dict[str, Any]], direction: str) -> dict[str, Any]:
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

            import asyncio

            from agent_framework import Message
            from agent_framework_foundry import VoiceLiveClient
            from azure.identity.aio import AzureCliCredential


            async def print_reply(session) -> None:
                async for event in session.receive():
                    if event.kind == "output_text" and event.content.merge_mode == "append":
                        print(event.content.text, end="")
                    elif event.kind == "response_done":
                        return


            async with AzureCliCredential() as credential:
                client = VoiceLiveClient(credential=credential)
                async with await client.start_session(options={"instructions": "Be brief."}) as session:
                    # Control calls such as create_response() resolve while something reads receive().
                    reader = asyncio.create_task(print_reply(session))
                    await session.send(Message("user", ["Hello!"]))
                    await session.create_response()
                    await reader

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
            model: The model, such as ``"gpt-realtime"``, or, with ``profile="byom-azure-openai-realtime"``, your
                deployment's name. Read from ``AZURE_VOICELIVE_MODEL`` when not passed.
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
            SettingNotFoundError: If the endpoint or model is missing.
            ValueError: If authentication is missing, the API version isn't supported, or both ``api_key`` and
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
            required_fields=["endpoint", "model"],
        )
        if credential is None:
            resolved_api_key = settings.get("api_key")
            if not resolved_api_key:
                raise ValueError(
                    "Voice Live needs authentication: pass 'api_key' or 'credential', or set the "
                    "AZURE_VOICELIVE_API_KEY environment variable."
                )
            credential = AzureKeyCredential(resolved_api_key.get_secret_value())
        self.endpoint = cast("str", settings.get("endpoint"))
        self.model = cast("str", settings.get("model"))
        self.api_version = _supported_api_version(settings.get("api_version") or DEFAULT_API_VERSION)
        self.profile = profile
        self.foundry_resource_override = foundry_resource_override
        self._credential = _sdk_credential(credential)

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
            credential=self._credential,
            endpoint=self.endpoint,
            api_version=self.api_version,
            model=self.model,
            query=self._handshake_query() or None,
            headers=prepend_agent_framework_to_user_agent(),
        )
        handshake = asyncio.ensure_future(manager.__aenter__())
        try:
            connection = await asyncio.shield(handshake)
        except asyncio.CancelledError:
            # The SDK leaks its HTTP session when its handshake is cancelled, so the handshake finishes on its own and
            # whatever it opened is closed. The caller's cancellation still takes effect at once.
            handshake.add_done_callback(functools.partial(_close_after_handshake, manager))
            raise
        except AzureError as error:
            raise RealtimeConnectionException(f"Couldn't connect to Voice Live: {error}") from error
        session = _VoiceLiveSession(_VoiceLiveConnection(manager, connection))
        try:
            await session.start(options=cast("VoiceLiveOptions | None", options), tools=tools)
        except BaseException:
            await session.close()
            raise
        return cast("RealtimeClientSessionProtocol[VoiceLiveOptionsT]", session)

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


def _sdk_credential(
    credential: AzureKeyCredential | AsyncTokenCredential | TokenCredential | AzureTokenProvider,
) -> AzureKeyCredential | AsyncTokenCredential:
    """Sync authentication can block the event loop; adapt it without taking the caller's credential."""
    if isinstance(credential, (AzureKeyCredential, AsyncTokenCredential)):
        return credential
    if callable(credential) and not hasattr(credential, "get_token"):
        return _TokenProviderCredential(credential)
    return _ThreadedTokenCredential(credential)


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
    """An async token credential over a token-provider callable, sync or async, which already knows its scope.

    A sync provider, such as one from ``azure.identity.get_bearer_token_provider()``, can fetch its token with
    blocking I/O, so it runs in a worker thread.
    """

    async def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        from azure.core.credentials import AccessToken

        provider = self._credential
        if inspect.iscoroutinefunction(provider) or inspect.iscoroutinefunction(type(provider).__call__):
            token = await provider()
        else:
            token = await asyncio.to_thread(provider)
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
                    session["modalities"] = _voice_live_modalities(value)
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
                "Voice Live speaks text verbatim only with an Azure text-to-speech voice, and the session's voice "
                "isn't one."
            )
        return super()._say_payload(text)

    def _current_voice(self) -> Any:
        return self._session_state.get("voice")

    def _voice_change_allowed(self, voice: Any) -> bool:
        """Voice Live decides for itself, at any time.

        It changes between Azure voices, ignores a change between OpenAI voices, which the session reports when the
        echo keeps the old voice, and rejects a change between Azure and OpenAI voices.
        """
        return True

    def _event_namespace(self, service_event_type: str) -> str:
        """Protocol ownership, not the hosting service, decides the metadata namespace."""
        return "foundry" if service_event_type.startswith(_FOUNDRY_EVENT_PREFIXES) else "openai"

    def _response_payload(self, options: dict[str, Any]) -> dict[str, Any]:
        """The GA response object, adapted to Voice Live.

        Per-response instructions are dropped, because Voice Live ignores them, and the output token cap and modalities
        take their preview names.
        """
        if "instructions" in options:
            logger.warning("Dropping per-response instructions: Voice Live ignores them.")
            options = {key: value for key, value in options.items() if key != "instructions"}
        response = super()._response_payload(options)
        if "max_output_tokens" in response:
            response["max_response_output_tokens"] = response.pop("max_output_tokens")
        if "output_modalities" in response:
            response["modalities"] = _voice_live_modalities(response.pop("output_modalities"))
        return response


class _VoiceLiveConnection:
    """Raw JSON keeps the SDK's models out of the shared protocol mapping."""

    def __init__(self, manager: Any, connection: Any) -> None:
        self._manager = manager
        self._connection = connection
        self._closing = False

    async def send(self, event: Mapping[str, Any]) -> None:
        try:
            await self._connection.send(event)
        except AzureError as error:
            raise RealtimeConnectionException(f"Couldn't send to Voice Live: {error}") from error

    async def receive(self) -> Mapping[str, Any] | None:
        from azure.ai.voicelive.aio import ConnectionClosed

        while True:
            try:
                frame = await self._connection.recv_bytes()
            except ConnectionClosed as closed:
                if closed.code == _NORMAL_CLOSURE:
                    return None
                raise RealtimeConnectionException(
                    f"Voice Live closed the connection with code {closed.code}.", close_code=closed.code
                ) from closed
            except AzureError as error:
                if self._closing:
                    return None
                raise RealtimeConnectionException(f"Couldn't read from Voice Live: {error}") from error
            if frame:
                return json.loads(frame)
            # The SDK returns an empty frame for websocket control messages, such as the one a local close wakes a
            # blocked read with.
            if self._closing:
                return None

    async def close(self) -> None:
        self._closing = True
        with _without_closing_wake_warning():
            await self._manager.__aexit__(None, None, None)


@contextlib.contextmanager
def _without_closing_wake_warning() -> Generator[None]:
    """The SDK logs a WARNING for the CLOSING message a local close wakes its pending read with, though it's expected.

    The record is dropped only while the connection closes, and by a filter of each close's own, so the SDK's logging is
    otherwise untouched and overlapping closes can't lift each other's filter.
    """
    from aiohttp import WSMsgType

    sdk_logger = logging.getLogger("azure.ai.voicelive.aio._patch")

    def is_not_closing_wake(record: logging.LogRecord) -> bool:
        return not (record.msg == "Unexpected WebSocket message type: %s" and record.args == (WSMsgType.CLOSING,))

    sdk_logger.addFilter(is_not_closing_wake)
    try:
        yield
    finally:
        sdk_logger.removeFilter(is_not_closing_wake)


_CLOSINGS_AFTER_HANDSHAKE: set[asyncio.Task[None]] = set()
"""The event loop holds tasks weakly, so a close task needs a strong reference until it finishes."""


def _close_after_handshake(manager: Any, handshake: asyncio.Future[Any]) -> None:
    if handshake.cancelled() or handshake.exception() is not None:
        return
    closing = asyncio.ensure_future(manager.__aexit__(None, None, None))
    _CLOSINGS_AFTER_HANDSHAKE.add(closing)
    closing.add_done_callback(_CLOSINGS_AFTER_HANDSHAKE.discard)


def _require_voice_live_sdk() -> None:
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

    Only the canonical ``YYYY-MM-DD`` form is a Voice Live version. The undocumented, GA-shaped ``v1`` isn't supported.
    """
    supported = False
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", api_version):
        with contextlib.suppress(ValueError):
            supported = date.fromisoformat(api_version) >= date.fromisoformat(DEFAULT_API_VERSION)
    if not supported:
        raise ValueError(
            f"Unsupported Voice Live api_version {api_version!r}: VoiceLiveClient speaks the preview shape of "
            f"{DEFAULT_API_VERSION} and later dated versions."
        )
    return api_version
