# Copyright (c) Microsoft. All rights reserved.

"""Behaviour of VoiceLiveClient and its session.

The client's transport runs the real azure-ai-voicelive SDK against a loopback aiohttp websocket server on 127.0.0.1;
payload and mapping tests drive the Voice Live session through a scripted in-memory connection. No service calls.
"""

import asyncio
import contextlib
import importlib.util
import json
import re
import threading
import time
from collections.abc import AsyncGenerator, Mapping
from types import TracebackType
from typing import Any, cast

import aiohttp
import pytest
from agent_framework import Message, tool
from agent_framework._telemetry import get_user_agent
from agent_framework.exceptions import (
    RealtimeConnectionException,
    RealtimeControlException,
    RealtimeUnsupportedException,
    SettingNotFoundError,
)
from aiohttp import WSMsgType, web
from azure.core.credentials import AccessToken, AzureKeyCredential
from azure.core.exceptions import AzureError

from agent_framework_foundry import (
    AzureAnimation,
    AzureEchoCancellation,
    AzureInterimResponse,
    AzureNoiseReduction,
    AzureTranscription,
    AzureTurnDetection,
    AzureVoice,
    VoiceLiveOptions,
)
from agent_framework_foundry._voice_live import VoiceLiveClient, _VoiceLiveConnection, _VoiceLiveSession

ENDPOINT = "https://vl.services.ai.azure.com/"
MODEL = "gpt-realtime"


class SyncTokenCredential:
    """A sync token credential that records which thread fetched each token."""

    def __init__(self, token: str) -> None:
        self.token = token
        self.threads: list[int] = []

    def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:
        self.threads.append(threading.get_ident())
        return AccessToken(self.token, int(time.time()) + 3600)


class VoiceLiveServer:
    """A loopback Voice Live endpoint on 127.0.0.1.

    It records the handshake and every event the client sends, greets with session.created, and answers each
    session.update with a session.updated echoing its session, unless told to reject or ignore updates. Tests reach
    the open socket through ``push`` and ``close``.
    """

    def __init__(
        self, *, reject_handshake_with: int | None = None, reject_updates: bool = False, ignore_updates: bool = False
    ) -> None:
        self.reject_handshake_with = reject_handshake_with
        self.reject_updates = reject_updates
        self.ignore_updates = ignore_updates
        self.query: dict[str, str] = {}
        self.headers: dict[str, str] = {}
        self.received: list[dict[str, Any]] = []
        self.socket_closed = asyncio.Event()
        self._websocket: web.WebSocketResponse | None = None
        self._runner: web.AppRunner | None = None
        self.endpoint = ""

    async def __aenter__(self) -> "VoiceLiveServer":
        app = web.Application()
        app.router.add_get("/voice-live/realtime", self._handle)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        port = self._runner.addresses[0][1]
        self.endpoint = f"http://127.0.0.1:{port}"
        return self

    async def __aexit__(self, *args: Any) -> None:
        assert self._runner is not None
        await self._runner.cleanup()

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        self.query = dict(request.query)
        self.headers = dict(request.headers)
        if self.reject_handshake_with is not None:
            return web.Response(status=self.reject_handshake_with, text="Access denied.")
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        self._websocket = websocket
        await websocket.send_json({"type": "session.created", "event_id": "evt_0", "session": {"id": "sess_1"}})
        async for message in websocket:
            if message.type != WSMsgType.TEXT:
                continue
            event = json.loads(message.data)
            self.received.append(event)
            if event["type"] == "session.update" and not self.ignore_updates:
                await websocket.send_json(self._answer_update(event))
        self.socket_closed.set()
        return websocket

    def _answer_update(self, update: dict[str, Any]) -> dict[str, Any]:
        if self.reject_updates:
            return {
                "type": "error",
                "event_id": "evt_err",
                "error": {
                    "type": "invalid_request_error",
                    "code": "invalid_value",
                    "message": "Bad.",
                    "event_id": None,
                },
            }
        return {"type": "session.updated", "event_id": "evt_updated", "session": update.get("session", {})}

    async def push(self, event: dict[str, Any]) -> None:
        assert self._websocket is not None
        await self._websocket.send_json(event)

    async def close(self, code: int) -> None:
        assert self._websocket is not None
        await self._websocket.close(code=code)


async def read_all(session: Any) -> list[Any]:
    """Every realtime event the session yields until its connection ends."""
    return [event async for event in session.receive()]


class FakeConnection:
    """A scripted connection: records what's sent, answers each session.update with an echoing session.updated, and
    serves pushed server events in order.

    Attributes:
        answers_updates: Whether a session.update gets its session.updated.
        echo_overrides: Session fields the session.updated reports in place of the ones sent.
    """

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.answers_updates = True
        self.echo_overrides: dict[str, Any] = {}
        self._incoming: asyncio.Queue[Mapping[str, Any] | None] = asyncio.Queue()

    async def send(self, event: Mapping[str, Any]) -> None:
        recorded = json.loads(json.dumps(event))
        self.sent.append(recorded)
        if recorded["type"] == "session.update" and self.answers_updates:
            echoed = {**recorded["session"], **self.echo_overrides}
            self.push({"type": "session.updated", "event_id": "evt_updated", "session": echoed})

    async def receive(self) -> Mapping[str, Any] | None:
        return await self._incoming.get()

    async def close(self) -> None:
        self._incoming.put_nowait(None)

    def push(self, *events: Mapping[str, Any]) -> None:
        for event in events:
            self._incoming.put_nowait(event)

    async def wait_sent(self, event_type: str, timeout: float = 1.0) -> dict[str, Any]:
        """Wait for the next event of ``event_type`` sent after this call, and return it."""
        already_sent = len(self.sent)

        async def poll() -> dict[str, Any]:
            while True:
                for event in self.sent[already_sent:]:
                    if event["type"] == event_type:
                        return event
                await asyncio.sleep(0.005)

        return await asyncio.wait_for(poll(), timeout)


def response_created(response_id: str, request_id: str | None = None) -> dict[str, Any]:
    """A response.created event, echoing ``request_id`` in its metadata when given."""
    metadata = {"af_request_id": request_id} if request_id is not None else None
    return {
        "type": "response.created",
        "event_id": "evt_created",
        "response": {"id": response_id, "metadata": metadata},
    }


@contextlib.asynccontextmanager
async def reading(session: Any) -> AsyncGenerator[list[Any]]:
    """Read the session's events in the background for the duration of the block, as the conversation does."""
    events: list[Any] = []

    async def read() -> None:
        async for event in session.receive():
            events.append(event)

    task = asyncio.create_task(read())
    try:
        yield events
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def until(condition: Any, timeout: float = 1.0) -> None:
    """Wait until ``condition()`` holds."""

    async def poll() -> None:
        while not condition():  # noqa: ASYNC110 - the condition reads state another task updates
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), timeout)


async def voice_live_session(options: VoiceLiveOptions | None = None) -> tuple[Any, FakeConnection]:
    """A Voice Live session opened with ``options`` over a scripted connection."""
    connection = FakeConnection()
    session = _VoiceLiveSession(connection)
    await asyncio.wait_for(session.start(options=options), 1)
    return session, connection


async def flat_session(options: VoiceLiveOptions) -> dict[str, Any]:
    """The session object of the first session.update a Voice Live session sends for ``options``."""
    _, connection = await voice_live_session(options)
    return connection.sent[0]["session"]


class StaticTokenCredential:
    """An async token credential that records the scopes it was asked for and returns a fixed token."""

    def __init__(self, token: str) -> None:
        self.token = token
        self.scopes: list[tuple[str, ...]] = []

    async def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:
        self.scopes.append(scopes)
        return AccessToken(self.token, int(time.time()) + 3600)

    async def close(self) -> None:
        pass

    async def __aenter__(self) -> "StaticTokenCredential":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        pass


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep a developer's or CI's real AZURE_VOICELIVE_* settings out of the unit tests."""
    for name in ("ENDPOINT", "API_KEY", "MODEL", "API_VERSION"):
        monkeypatch.delenv(f"AZURE_VOICELIVE_{name}", raising=False)


# region Construction and settings


def test_reads_settings_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """With AZURE_VOICELIVE_ENDPOINT "https://vl.services.ai.azure.com/", AZURE_VOICELIVE_MODEL "gpt-realtime" and no
    AZURE_VOICELIVE_API_VERSION, VoiceLiveClient() has endpoint and model from the environment and api_version
    "2026-07-15"."""
    monkeypatch.setenv("AZURE_VOICELIVE_ENDPOINT", ENDPOINT)
    monkeypatch.setenv("AZURE_VOICELIVE_MODEL", MODEL)
    monkeypatch.setenv("AZURE_VOICELIVE_API_KEY", "key")

    client = VoiceLiveClient()

    assert (client.endpoint, client.model, client.api_version) == (ENDPOINT, MODEL, "2026-07-15")


def test_missing_endpoint_raises() -> None:
    """With no endpoint argument or AZURE_VOICELIVE_ENDPOINT, VoiceLiveClient(model="m") raises SettingNotFoundError
    naming 'endpoint' and AZURE_VOICELIVE_ENDPOINT."""
    with pytest.raises(SettingNotFoundError, match="endpoint.*AZURE_VOICELIVE_ENDPOINT"):
        VoiceLiveClient(model=MODEL, api_key="key")


def test_missing_model_raises() -> None:
    """With no model argument or AZURE_VOICELIVE_MODEL, VoiceLiveClient(endpoint=...) raises SettingNotFoundError
    naming 'model' and AZURE_VOICELIVE_MODEL."""
    with pytest.raises(SettingNotFoundError, match="model.*AZURE_VOICELIVE_MODEL"):
        VoiceLiveClient(endpoint=ENDPOINT, api_key="key")


def test_api_version_must_be_a_supported_date() -> None:
    """api_version "2026-09-01" is accepted; "v1", "2025-10-01", "latest", and the non-canonical "20260901" and
    "2026-W36-1", each raise ValueError."""
    assert VoiceLiveClient(endpoint=ENDPOINT, model=MODEL, api_key="key", api_version="2026-09-01").api_version == (
        "2026-09-01"
    )
    for unsupported in ("v1", "2025-10-01", "latest", "20260901", "2026-W36-1"):
        with pytest.raises(ValueError, match=unsupported):
            VoiceLiveClient(endpoint=ENDPOINT, model=MODEL, api_key="key", api_version=unsupported)


def test_api_key_and_credential_together_raise() -> None:
    """VoiceLiveClient(api_key="k", credential=<async credential>) raises ValueError."""
    with pytest.raises(ValueError, match="api_key.*credential"):
        VoiceLiveClient(endpoint=ENDPOINT, model=MODEL, api_key="key", credential=StaticTokenCredential("tok"))


def test_missing_authentication_raises() -> None:
    """With no api_key, credential or AZURE_VOICELIVE_API_KEY, or an empty api_key, VoiceLiveClient(...) raises
    ValueError naming api_key, credential and AZURE_VOICELIVE_API_KEY."""
    for api_key in (None, ""):
        with pytest.raises(ValueError, match="api_key.*credential.*AZURE_VOICELIVE_API_KEY"):
            VoiceLiveClient(endpoint=ENDPOINT, model=MODEL, api_key=api_key)


def test_missing_sdk_raises_the_install_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    """When azure.ai.voicelive can't be found, VoiceLiveClient(...) raises ModuleNotFoundError whose message names
    pip install 'agent-framework-foundry[realtime]'."""
    real_find_spec = importlib.util.find_spec

    def find_spec_without_voice_live(name: str, *args: Any, **kwargs: Any) -> Any:
        return None if name == "azure.ai.voicelive" else real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", find_spec_without_voice_live)

    with pytest.raises(ModuleNotFoundError, match=re.escape("pip install 'agent-framework-foundry[realtime]'")):
        VoiceLiveClient(endpoint=ENDPOINT, model=MODEL, api_key="key")


# endregion

# region Connecting (loopback server)


async def test_start_session_connects_with_version_model_and_key() -> None:
    """start_session() against the loopback server connects with query api-version "2026-07-15" and model
    "gpt-realtime", header api-key "k", and returns once the server acknowledges the first session.update."""
    async with VoiceLiveServer() as server:
        client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, api_key="k")
        session = await asyncio.wait_for(client.start_session(), 5)
        await session.close()

    assert server.query["api-version"] == "2026-07-15"
    assert server.query["model"] == MODEL
    assert server.headers["api-key"] == "k"


async def test_first_event_sent_is_a_session_update_even_without_options() -> None:
    """start_session() with no options sends {"type": "session.update", "session": {}, "event_id": ...} before any
    other event, and returns on session.updated."""
    async with VoiceLiveServer() as server:
        client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, api_key="k")
        session = await asyncio.wait_for(client.start_session(), 5)
        await session.close()
        await asyncio.wait_for(server.socket_closed.wait(), 5)

    first = server.received[0]
    assert (first["type"], first["session"]) == ("session.update", {})
    assert first["event_id"]


async def test_byom_sends_profile_and_resource_override_in_the_query() -> None:
    """profile "byom-azure-openai-realtime" and foundry_resource_override "my-aoai" appear as query parameters
    profile and foundry-resource-override."""
    async with VoiceLiveServer() as server:
        client = VoiceLiveClient(
            endpoint=server.endpoint,
            model="my-deployment",
            api_key="k",
            profile="byom-azure-openai-realtime",
            foundry_resource_override="my-aoai",
        )
        session = await asyncio.wait_for(client.start_session(), 5)
        await session.close()

    assert server.query["profile"] == "byom-azure-openai-realtime"
    assert server.query["foundry-resource-override"] == "my-aoai"
    assert server.query["model"] == "my-deployment"


async def test_azure_key_credential_sends_an_api_key() -> None:
    """credential=AzureKeyCredential("k") sends header api-key "k"."""
    async with VoiceLiveServer() as server:
        client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, credential=AzureKeyCredential("k"))
        session = await asyncio.wait_for(client.start_session(), 5)
        await session.close()

    assert server.headers["api-key"] == "k"


async def test_async_credential_sends_a_bearer_token() -> None:
    """An AsyncTokenCredential returning "tok" sends Authorization "Bearer tok", requested for
    https://ai.azure.com/.default."""
    credential = StaticTokenCredential("tok")

    async with VoiceLiveServer() as server:
        client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, credential=credential)
        session = await asyncio.wait_for(client.start_session(), 5)
        await session.close()

    assert server.headers["Authorization"] == "Bearer tok"
    assert credential.scopes == [("https://ai.azure.com/.default",)]


async def test_sync_credential_is_called_off_the_event_loop() -> None:
    """A sync TokenCredential returning "tok" sends Authorization "Bearer tok", and its get_token runs on a thread
    other than the event loop's."""
    credential = SyncTokenCredential("tok")
    loop_thread = threading.get_ident()

    async with VoiceLiveServer() as server:
        client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, credential=credential)
        session = await asyncio.wait_for(client.start_session(), 5)
        await session.close()

    assert server.headers["Authorization"] == "Bearer tok"
    assert credential.threads
    assert all(thread != loop_thread for thread in credential.threads)


async def test_sync_token_provider_is_called_off_the_event_loop() -> None:
    """A sync token-provider callable, which may fetch its token with blocking I/O, runs on a thread other than the
    event loop's."""
    threads: list[int] = []
    loop_thread = threading.get_ident()

    def provider() -> str:
        threads.append(threading.get_ident())
        return "tok"

    async with VoiceLiveServer() as server:
        client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, credential=provider)
        session = await asyncio.wait_for(client.start_session(), 5)
        await session.close()

    assert threads
    assert all(thread != loop_thread for thread in threads)


async def test_token_provider_callable_sends_a_bearer_token() -> None:
    """A sync callable returning "tok1" and an async callable returning "tok2" each send their token as a bearer
    token."""

    async def async_provider() -> str:
        return "tok2"

    for provider, token in ((lambda: "tok1", "tok1"), (async_provider, "tok2")):
        async with VoiceLiveServer() as server:
            client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, credential=provider)
            session = await asyncio.wait_for(client.start_session(), 5)
            await session.close()

        assert server.headers["Authorization"] == f"Bearer {token}"


async def test_user_agent_carries_the_agent_framework_prefix() -> None:
    """The handshake's User-Agent starts with the agent-framework-python user agent."""
    async with VoiceLiveServer() as server:
        client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, api_key="k")
        session = await asyncio.wait_for(client.start_session(), 5)
        await session.close()

    assert server.headers["User-Agent"].startswith(get_user_agent())


async def test_rejected_handshake_raises_a_connection_exception() -> None:
    """A loopback server answering the upgrade with 401 makes start_session() raise RealtimeConnectionException whose
    message names 401, with close_code None and __cause__ set."""
    async with VoiceLiveServer(reject_handshake_with=401) as server:
        client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, api_key="bad")
        with pytest.raises(RealtimeConnectionException, match="401") as caught:
            await asyncio.wait_for(client.start_session(), 5)

    assert caught.value.close_code is None
    assert caught.value.__cause__ is not None


async def test_abnormal_server_close_raises_with_its_close_code() -> None:
    """After start, the server closing with 1011 makes receive() raise RealtimeConnectionException with close_code
    1011."""
    async with VoiceLiveServer() as server:
        client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, api_key="k")
        session = await asyncio.wait_for(client.start_session(), 5)
        await server.close(1011)

        with pytest.raises(RealtimeConnectionException) as caught:
            await asyncio.wait_for(read_all(session), 5)
        await session.close()

    assert caught.value.close_code == 1011


async def test_sending_on_a_dead_socket_raises() -> None:
    """After the server closes with 1011 and receive() has raised, send(Message("user", ["Hi"])) raises
    RealtimeConnectionException, chained to the SDK's error."""
    async with VoiceLiveServer() as server:
        client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, api_key="k")
        session = await asyncio.wait_for(client.start_session(), 5)
        await server.close(1011)
        with pytest.raises(RealtimeConnectionException):
            await asyncio.wait_for(read_all(session), 5)

        with pytest.raises(RealtimeConnectionException) as caught:
            await session.send(Message("user", ["Hi"]))
        await session.close()

    assert caught.value.__cause__ is not None


async def test_normal_server_close_ends_receive() -> None:
    """After start, the server closing with 1000 ends receive() without an exception."""
    async with VoiceLiveServer() as server:
        client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, api_key="k")
        session = await asyncio.wait_for(client.start_session(), 5)
        await server.close(1000)

        events = await asyncio.wait_for(read_all(session), 5)
        await session.close()

    assert [event.kind for event in events] == ["session_created", "session_updated"]


async def test_closing_the_session_ends_a_blocked_receive() -> None:
    """While receive() waits for the next event, session.close() ends iteration without an exception."""
    async with VoiceLiveServer() as server:
        client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, api_key="k")
        session = await asyncio.wait_for(client.start_session(), 5)
        reading_everything = asyncio.create_task(read_all(session))
        await asyncio.sleep(0.05)
        await session.close()
        events = await asyncio.wait_for(reading_everything, 5)

    assert [event.kind for event in events] == ["session_created", "session_updated"]


class FailingSdkConnection:
    """An SDK connection whose reads fail with an AzureError that isn't ConnectionClosed."""

    async def recv_bytes(self) -> bytes:
        raise AzureError("Bad frame.")


class ClosingManager:
    """An SDK connection manager whose exit does nothing."""

    async def __aexit__(self, *args: Any) -> None:
        pass


async def test_an_sdk_read_error_raises_a_realtime_connection_exception() -> None:
    """An SDK read failing with AzureError("Bad frame.") makes receive() raise RealtimeConnectionException caused
    by it."""
    connection = _VoiceLiveConnection(ClosingManager(), FailingSdkConnection())

    with pytest.raises(RealtimeConnectionException) as caught:
        await connection.receive()

    assert isinstance(caught.value.__cause__, AzureError)


async def test_an_sdk_read_error_after_closing_ends_receive() -> None:
    """Once close() has started, an SDK read failing with AzureError ends receive() normally."""
    connection = _VoiceLiveConnection(ClosingManager(), FailingSdkConnection())
    await connection.close()

    assert await connection.receive() is None


async def test_failed_start_closes_the_socket() -> None:
    """When the server rejects the first session.update with an error {event_id: null}, start_session() raises
    RealtimeControlException and the server sees the socket close."""
    async with VoiceLiveServer(reject_updates=True) as server:
        client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, api_key="k")
        with pytest.raises(RealtimeControlException):
            await asyncio.wait_for(client.start_session(), 5)

        await asyncio.wait_for(server.socket_closed.wait(), 2)


class HandshakeWebSocket:
    """The websocket a delayed handshake finally returns; it records being closed."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self, *, code: int = 1000, message: bytes = b"") -> None:
        self.closed = True


async def test_cancelling_during_the_handshake_closes_what_it_opened(monkeypatch: pytest.MonkeyPatch) -> None:
    """start_session() cancelled while the websocket handshake is in flight raises CancelledError at once; when the
    handshake later completes, the client closes the websocket and the SDK's HTTP session."""
    handshake_started = asyncio.Event()
    finish_handshake = asyncio.Event()
    websocket = HandshakeWebSocket()
    http_sessions: list[aiohttp.ClientSession] = []
    real_init = aiohttp.ClientSession.__init__

    def recording_init(self: aiohttp.ClientSession, *args: Any, **kwargs: Any) -> None:
        real_init(self, *args, **kwargs)
        http_sessions.append(self)

    async def delayed_ws_connect(self: aiohttp.ClientSession, *args: Any, **kwargs: Any) -> HandshakeWebSocket:
        handshake_started.set()
        await finish_handshake.wait()
        return websocket

    monkeypatch.setattr(aiohttp.ClientSession, "__init__", recording_init)
    monkeypatch.setattr(aiohttp.ClientSession, "ws_connect", delayed_ws_connect)
    client = VoiceLiveClient(endpoint=ENDPOINT, model=MODEL, api_key="k")

    starting = asyncio.create_task(client.start_session())
    await asyncio.wait_for(handshake_started.wait(), 5)
    starting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await starting
    finish_handshake.set()

    await until(lambda: websocket.closed and all(http_session.closed for http_session in http_sessions), 2)
    assert http_sessions


async def test_unsupported_audio_format_raises_before_connecting() -> None:
    """start_session(options={"input_audio_format": "audio/pcm;rate=44100"}) raises ValueError without connecting."""
    async with VoiceLiveServer() as server:
        client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, api_key="k")
        with pytest.raises(ValueError, match="audio/pcm;rate=44100"):
            await asyncio.wait_for(client.start_session(options={"input_audio_format": "audio/pcm;rate=44100"}), 5)

    assert server.query == {}


# endregion

# region Voice Live's flat payload


async def test_neutral_options_translate_to_the_flat_session() -> None:
    """instructions, voice "alloy", tool_choice "auto", temperature 0.8, max_tokens 300, metadata {"k": "v"},
    turn_detection, transcription and noise_reduction become top-level instructions, voice, tool_choice, temperature,
    max_response_output_tokens, metadata, turn_detection, input_audio_transcription and input_audio_noise_reduction,
    with no "type" and no "audio" key."""
    turn_detection: AzureTurnDetection = {
        "type": "azure_semantic_vad",
        "silence_duration_ms": 500,
        "remove_filler_words": True,
    }
    transcription: AzureTranscription = {"model": "azure-speech", "language": "en-US", "phrase_list": ["Contoso"]}
    noise_reduction: AzureNoiseReduction = {"type": "azure_deep_noise_suppression"}

    sent = await flat_session({
        "instructions": "Be brief.",
        "voice": "alloy",
        "tool_choice": "auto",
        "temperature": 0.8,
        "max_tokens": 300,
        "metadata": {"k": "v"},
        "turn_detection": turn_detection,
        "transcription": transcription,
        "noise_reduction": noise_reduction,
    })

    assert sent == {
        "instructions": "Be brief.",
        "voice": "alloy",
        "tool_choice": "auto",
        "temperature": 0.8,
        "max_response_output_tokens": 300,
        "metadata": {"k": "v"},
        "turn_detection": turn_detection,
        "input_audio_transcription": transcription,
        "input_audio_noise_reduction": noise_reduction,
    }


async def test_audio_only_modality_becomes_text_and_audio() -> None:
    """output_modalities ["audio"] is sent as modalities ["text", "audio"]; ["text"] stays ["text"]."""
    assert await flat_session({"output_modalities": ["audio"]}) == {"modalities": ["text", "audio"]}
    assert await flat_session({"output_modalities": ["text"]}) == {"modalities": ["text"]}


async def test_audio_formats_and_sampling_rates_translate() -> None:
    """Input "audio/pcm;rate=24000", 16000 and 8000 → input_audio_format "pcm16" with input_audio_sampling_rate
    24000, 16000 or 8000; "audio/pcmu" → "g711_ulaw" and "audio/pcma" → "g711_alaw", each at 8000. Every input format
    sends its rate, because updates are partial and an earlier rate would otherwise stay. Output
    "audio/pcm;rate=16000" → output_audio_format "pcm16_16000hz" and rate 8000 → "pcm16_8000hz".
    "audio/pcm;rate=44100" raises ValueError."""
    assert await flat_session({"input_audio_format": "audio/pcm;rate=24000"}) == {
        "input_audio_format": "pcm16",
        "input_audio_sampling_rate": 24000,
    }
    assert await flat_session({"input_audio_format": "audio/pcm;rate=16000"}) == {
        "input_audio_format": "pcm16",
        "input_audio_sampling_rate": 16000,
    }
    assert await flat_session({"input_audio_format": "audio/pcm;rate=8000"}) == {
        "input_audio_format": "pcm16",
        "input_audio_sampling_rate": 8000,
    }
    assert await flat_session({"input_audio_format": "audio/pcmu"}) == {
        "input_audio_format": "g711_ulaw",
        "input_audio_sampling_rate": 8000,
    }
    assert await flat_session({"input_audio_format": "audio/pcma"}) == {
        "input_audio_format": "g711_alaw",
        "input_audio_sampling_rate": 8000,
    }
    assert await flat_session({"output_audio_format": "audio/pcm;rate=24000"}) == {"output_audio_format": "pcm16"}
    assert await flat_session({"output_audio_format": "audio/pcm;rate=16000"}) == {
        "output_audio_format": "pcm16_16000hz"
    }
    assert await flat_session({"output_audio_format": "audio/pcm;rate=8000"}) == {"output_audio_format": "pcm16_8000hz"}
    assert await flat_session({"output_audio_format": "audio/pcmu"}) == {"output_audio_format": "g711_ulaw"}
    with pytest.raises(ValueError, match="audio/pcm;rate=44100"):
        await flat_session({"input_audio_format": "audio/pcm;rate=44100"})


async def test_tools_are_declared_in_the_flat_session() -> None:
    """start(tools=[lookup]) sends a flat session whose tools hold lookup's function declaration."""

    @tool
    def lookup(query: str) -> str:
        """Look something up."""
        return query

    connection = FakeConnection()
    session = _VoiceLiveSession(connection)
    await asyncio.wait_for(session.start(tools=[lookup]), 1)

    assert connection.sent[0]["session"] == {
        "tools": [
            {
                "type": "function",
                "name": "lookup",
                "description": "Look something up.",
                "parameters": lookup.parameters(),
            }
        ]
    }


async def test_azure_voice_is_sent_as_given() -> None:
    """voice {type: "azure-standard", name: "en-US-AvaNeural", rate: "1.1"} is sent unchanged as voice."""
    voice: AzureVoice = {"type": "azure-standard", "name": "en-US-AvaNeural", "rate": "1.1"}

    assert await flat_session({"voice": voice}) == {"voice": voice}


async def test_azure_only_settings_are_sent() -> None:
    """echo_cancellation → input_audio_echo_cancellation; animation, interim_response, reasoning_effort,
    parallel_tool_calls and output_audio_timestamp_types are sent under their own names."""
    echo_cancellation: AzureEchoCancellation = {"type": "server_echo_cancellation"}
    animation: AzureAnimation = {"outputs": ["viseme_id"]}
    interim_response: AzureInterimResponse = {
        "type": "static-interim-response",
        "texts": ["One moment."],
        "triggers": ["tool"],
    }

    sent = await flat_session({
        "echo_cancellation": echo_cancellation,
        "animation": animation,
        "interim_response": interim_response,
        "reasoning_effort": "low",
        "parallel_tool_calls": False,
        "output_audio_timestamp_types": ["word"],
    })

    assert sent == {
        "input_audio_echo_cancellation": echo_cancellation,
        "animation": animation,
        "interim_response": interim_response,
        "reasoning_effort": "low",
        "parallel_tool_calls": False,
        "output_audio_timestamp_types": ["word"],
    }


async def test_per_response_instructions_are_dropped(caplog: pytest.LogCaptureFixture) -> None:
    """create_response({"instructions": "Short.", "max_tokens": 50}) warns, and sends response
    {max_response_output_tokens: 50, metadata: {af_request_id: ...}} with no instructions."""
    session, connection = await voice_live_session()

    async with reading(session):
        creating = asyncio.create_task(session.create_response({"instructions": "Short.", "max_tokens": 50}))
        request = await connection.wait_sent("response.create")
        connection.push(response_created("resp_1", request["response"]["metadata"]["af_request_id"]))
        await asyncio.wait_for(creating, 1)

    assert request["response"] == {
        "max_response_output_tokens": 50,
        "metadata": {"af_request_id": request["response"]["metadata"]["af_request_id"]},
    }
    assert "instructions" in caplog.text


async def test_per_response_output_modalities_are_sent_as_modalities() -> None:
    """create_response({"output_modalities": ["audio"]}) sends response {modalities: ["text", "audio"], metadata:
    {af_request_id: ...}}, as the session object does."""
    session, connection = await voice_live_session()

    async with reading(session):
        creating = asyncio.create_task(session.create_response({"output_modalities": ["audio"]}))
        request = await connection.wait_sent("response.create")
        connection.push(response_created("resp_1", request["response"]["metadata"]["af_request_id"]))
        await asyncio.wait_for(creating, 1)

    assert request["response"] == {
        "modalities": ["text", "audio"],
        "metadata": {"af_request_id": request["response"]["metadata"]["af_request_id"]},
    }


# endregion

# region Voice Live behaviour on the shared session


async def test_say_with_an_azure_voice_sends_a_pre_generated_message() -> None:
    """When the last session.updated echoes voice {type: "azure-standard", name: ...}, say("Hi.") sends
    pre_generated_assistant_message and resolves on the next response.created with metadata null."""
    session, connection = await voice_live_session({"voice": {"type": "azure-standard", "name": "en-US-AvaNeural"}})

    async with reading(session):
        saying = asyncio.create_task(session.say("Hi."))
        request = await connection.wait_sent("response.create")
        connection.push(response_created("resp_1"))

        assert await asyncio.wait_for(saying, 1) == "resp_1"

    assert request["response"]["pre_generated_assistant_message"] == {
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": "Hi."}],
    }


@pytest.mark.parametrize(
    "voice", ["alloy", {"type": "openai", "name": "alloy"}, {"type": "azure-realtime-native", "name": "ava"}]
)
async def test_say_with_an_openai_voice_raises(voice: Any) -> None:
    """When the last session.updated echoes voice "alloy" (or {type: "openai", ...}, or azure-realtime-native),
    say("Hi.") raises RealtimeUnsupportedException and sends nothing."""
    session, connection = await voice_live_session({"voice": voice})
    already_sent = len(connection.sent)

    with pytest.raises(RealtimeUnsupportedException):
        await asyncio.wait_for(session.say("Hi."), 0.5)

    assert len(connection.sent) == already_sent


async def test_voice_change_after_audio_is_sent() -> None:
    """After an output_audio event, update_session({"voice": {type: "azure-standard", name: "en-GB-SoniaNeural"}})
    sends the voice."""
    sonia = {"type": "azure-standard", "name": "en-GB-SoniaNeural"}
    session, connection = await voice_live_session({"voice": {"type": "azure-standard", "name": "en-US-AvaNeural"}})

    async with reading(session) as events:
        connection.push({
            "type": "response.audio.delta",
            "event_id": "e1",
            "response_id": "r",
            "item_id": "i",
            "content_index": 0,
            "delta": "AAE=",
        })
        await until(lambda: any(event.kind == "output_audio" for event in events))
        await asyncio.wait_for(session.update_session({"voice": sonia}, None), 1)

    assert connection.sent[-1] == {
        "type": "session.update",
        "session": {"voice": sonia},
        "event_id": connection.sent[-1]["event_id"],
    }


async def test_preview_event_names_map_to_kinds() -> None:
    """response.audio.delta → output_audio, response.audio_transcript.delta/.done and response.text.delta/.done →
    output_text, conversation.item.created → item_added, each with service_event_type the preview name."""
    part = {"response_id": "resp_1", "item_id": "item_1", "content_index": 0}
    session, connection = await voice_live_session()

    async with reading(session) as events:
        connection.push(
            {**part, "type": "response.audio.delta", "event_id": "e1", "delta": "AAE="},
            {**part, "type": "response.audio_transcript.delta", "event_id": "e2", "delta": "Hi"},
            {**part, "type": "response.audio_transcript.done", "event_id": "e3", "transcript": "Hi there"},
            {**part, "type": "response.text.delta", "event_id": "e4", "delta": "Hi"},
            {**part, "type": "response.text.done", "event_id": "e5", "text": "Hi there"},
            {
                "type": "conversation.item.created",
                "event_id": "e6",
                "previous_item_id": None,
                "item": {"id": "item_2", "type": "message", "role": "user", "content": []},
            },
        )
        await until(lambda: len(events) == 7)

    mapped = [(event.kind, event.service_event_type) for event in events[1:]]
    assert mapped == [
        ("output_audio", "response.audio.delta"),
        ("output_text", "response.audio_transcript.delta"),
        ("output_text", "response.audio_transcript.done"),
        ("output_text", "response.text.delta"),
        ("output_text", "response.text.done"),
        ("item_added", "conversation.item.created"),
    ]
    assert events[3].content.text == "Hi there"
    assert events[6].additional_properties["core"] == {"item_id": "item_2", "previous_item_id": None, "role": "user"}


async def test_passthrough_namespace_follows_the_events_owner() -> None:
    """warning and response.animation_viseme.delta pass through with their fields under foundry;
    input_audio_buffer.committed passes through under openai."""
    warning = {"type": "warning", "event_id": "e1", "warning": {"message": "Throttled."}}
    viseme = {"type": "response.animation_viseme.delta", "event_id": "e2", "viseme_id": 3, "audio_offset_ms": 10}
    committed = {"type": "input_audio_buffer.committed", "event_id": "e3", "item_id": "item_1"}
    session, connection = await voice_live_session()

    async with reading(session) as events:
        connection.push(warning, viseme, committed)
        await until(lambda: len(events) == 4)

    assert [event.additional_properties for event in events[1:]] == [
        {"foundry": warning},
        {"foundry": viseme},
        {"openai": committed},
    ]


async def test_assistant_text_is_sent_as_a_text_part() -> None:
    """send(Message("assistant", ["Earlier reply."])) sends a message item with content [{type: "text", ...}]."""
    session, connection = await voice_live_session()

    await session.send(Message("assistant", ["Earlier reply."]))

    assert connection.sent[-1]["item"] == {
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": "Earlier reply."}],
    }


async def test_ga_only_settings_are_dropped_with_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    """start(options={"speed": 1.2, "truncation": "auto"}) on a Voice Live session sends neither key and warns naming
    'speed' and 'truncation'."""
    session = await flat_session(cast(VoiceLiveOptions, {"voice": "alloy", "speed": 1.2, "truncation": "auto"}))

    assert "speed" not in session
    assert "truncation" not in session
    assert "'speed'" in caplog.text
    assert "'truncation'" in caplog.text


async def test_cancel_response_sends_no_response_id() -> None:
    """With "resp_1" active, a Voice Live session's cancel_response() sends response.cancel with no response_id."""
    session, connection = await voice_live_session()

    async with reading(session) as events:
        connection.push(response_created("resp_1"))
        await until(lambda: any(event.kind == "response_created" for event in events))
        cancelling = asyncio.create_task(session.cancel_response())
        cancel = await connection.wait_sent("response.cancel")
        connection.push({"type": "response.done", "event_id": "evt_done", "response": {"id": "resp_1"}})
        await asyncio.wait_for(cancelling, 1)

    assert "response_id" not in cancel


# endregion


class TestVerification:
    async def test_cancelled_start_closes_the_socket(self) -> None:
        """start_session() cancelled while waiting for session.updated raises CancelledError and the server sees the
        socket close."""
        async with VoiceLiveServer(ignore_updates=True) as server:
            client = VoiceLiveClient(endpoint=server.endpoint, model=MODEL, api_key="k")
            starting = asyncio.create_task(client.start_session())
            await until(lambda: [event["type"] for event in server.received] == ["session.update"], 5)
            starting.cancel()

            with pytest.raises(asyncio.CancelledError):
                await starting
            await asyncio.wait_for(server.socket_closed.wait(), 2)

    async def test_error_extras_stay_under_openai(self) -> None:
        """An unclaimed Voice Live error yields error content with type and param under openai."""
        session, connection = await voice_live_session()
        error = {
            "type": "error",
            "event_id": "evt_err",
            "error": {
                "type": "invalid_request_error",
                "code": "invalid_value",
                "message": "Bad voice.",
                "param": "session.voice",
                "event_id": None,
            },
        }

        async with reading(session) as events:
            connection.push(error)
            await until(lambda: any(event.kind == "error" for event in events))

        reported = next(event for event in events if event.kind == "error")
        assert (reported.content.message, reported.content.error_code) == ("Bad voice.", "invalid_value")
        assert reported.content.additional_properties == {
            "openai": {"type": "invalid_request_error", "param": "session.voice"}
        }

    async def test_ignored_voice_change_warns_and_keeps_the_old_voice(self, caplog: pytest.LogCaptureFixture) -> None:
        """Voice Live ignores a change between OpenAI voices: update_session({"voice": "echo"}) acknowledged with
        "alloy" still echoed logs the ignored-voice warning, and session.options keeps "alloy"."""
        session, connection = await voice_live_session({"voice": "alloy"})
        connection.echo_overrides = {"voice": "alloy"}

        async with reading(session):
            await asyncio.wait_for(session.update_session({"voice": "echo"}, None), 1)

        assert "ignored the voice change to 'echo'" in caplog.text
        assert session.options["voice"] == "alloy"

    async def test_azure_voice_change_echoed_with_defaults_is_accepted(self, caplog: pytest.LogCaptureFixture) -> None:
        """update_session({"voice": Sonia}) acknowledged with Sonia plus Voice Live's default fields is accepted:
        session.options holds Sonia as requested, and nothing warns."""
        ava: AzureVoice = {"type": "azure-standard", "name": "en-US-AvaNeural"}
        sonia: AzureVoice = {"type": "azure-standard", "name": "en-GB-SoniaNeural"}
        session, connection = await voice_live_session({"voice": ava})
        connection.echo_overrides = {
            "voice": {**sonia, "temperature": None, "rate": None, "expressive": {"tags": None}}
        }

        async with reading(session):
            await asyncio.wait_for(session.update_session({"voice": sonia}, None), 1)

        assert session.options["voice"] == sonia
        assert "ignored the voice change" not in caplog.text

    async def test_an_ignored_voice_is_logged_by_name_only(self, caplog: pytest.LogCaptureFixture) -> None:
        """An ignored change to an Azure voice whose custom_lexicon_url is a SAS URL warns naming the voices, without
        the URL or its signature."""
        ava: AzureVoice = {"type": "azure-standard", "name": "en-US-AvaNeural"}
        sonia: AzureVoice = {
            "type": "azure-standard",
            "name": "en-GB-SoniaNeural",
            "custom_lexicon_url": "https://store.blob.core.windows.net/lexicons/a.xml?sv=2024-01-01&sig=secret-signature",
        }
        session, connection = await voice_live_session({"voice": ava})
        connection.echo_overrides = {"voice": ava}

        async with reading(session):
            await asyncio.wait_for(session.update_session({"voice": sonia}, None), 1)

        assert "ignored the voice change" in caplog.text
        assert "en-GB-SoniaNeural" in caplog.text
        assert "secret-signature" not in caplog.text

    async def test_azure_voice_with_a_tuple_field_is_accepted(self, caplog: pytest.LogCaptureFixture) -> None:
        """update_session({"voice": Sonia with prefer_locales ("en-GB", "es-ES")}) acknowledged with the voice echoed
        from JSON, where the tuple is a list: session.options holds Sonia as requested, and nothing warns."""
        ava: AzureVoice = {"type": "azure-standard", "name": "en-US-AvaNeural"}
        sonia: AzureVoice = {
            "type": "azure-standard",
            "name": "en-GB-SoniaNeural",
            "prefer_locales": ("en-GB", "es-ES"),
        }
        session, _ = await voice_live_session({"voice": ava})

        async with reading(session):
            await asyncio.wait_for(session.update_session({"voice": sonia}, None), 1)

        assert session.options["voice"] == sonia
        assert "ignored the voice change" not in caplog.text
