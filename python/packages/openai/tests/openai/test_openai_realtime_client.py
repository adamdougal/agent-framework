# Copyright (c) Microsoft. All rights reserved.

"""Behaviour of OpenAIRealtimeClient.

The client's transport runs the real openai SDK against a loopback websockets server on 127.0.0.1, over TLS with a
test certificate authority, because openai 2.x connects only over TLS. No service calls.
"""

import asyncio
import datetime
import functools
import importlib.util
import ipaddress
import json
import os
import re
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from http import HTTPStatus
from pathlib import Path
from types import TracebackType
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import pytest
from agent_framework import Message
from agent_framework._telemetry import get_user_agent
from agent_framework.exceptions import RealtimeConnectionException, RealtimeControlException, SettingNotFoundError
from azure.core.credentials import AccessToken
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from openai import AsyncAzureOpenAI, AsyncOpenAI
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response

from agent_framework_openai import OpenAIRealtimeClient

AZURE_ENDPOINT = "https://contoso.openai.azure.com"


@functools.cache
def _test_certificates() -> tuple[bytes, bytes, bytes]:
    """A certificate authority, and a 127.0.0.1 certificate it signed with its key, as PEM.

    The extensions are the ones Python's strict certificate verification requires.
    """
    now = datetime.datetime.now(datetime.UTC)
    validity = (now - datetime.timedelta(days=1), now + datetime.timedelta(days=1))
    authority_key = ec.generate_private_key(ec.SECP256R1())
    authority_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Realtime test authority")])
    authority = (
        x509
        .CertificateBuilder()
        .subject_name(authority_name)
        .issuer_name(authority_name)
        .public_key(authority_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(validity[0])
        .not_valid_after(validity[1])
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(authority_key.public_key()), critical=False)
        .sign(authority_key, hashes.SHA256())
    )
    server_key = ec.generate_private_key(ec.SECP256R1())
    server = (
        x509
        .CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")]))
        .issuer_name(authority_name)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(validity[0])
        .not_valid_after(validity[1])
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(authority_key.public_key()), critical=False)
        .sign(authority_key, hashes.SHA256())
    )
    server_key_pem = server_key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    return (
        authority.public_bytes(serialization.Encoding.PEM),
        server.public_bytes(serialization.Encoding.PEM),
        server_key_pem,
    )


@pytest.fixture(autouse=True)
def trusted_test_authority(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """The client trusts the test authority, as it would a public one."""
    authority_file = tmp_path / "authority.pem"
    authority_file.write_bytes(_test_certificates()[0])
    monkeypatch.setenv("SSL_CERT_FILE", str(authority_file))


def _server_tls(directory: Any) -> ssl.SSLContext:
    _, certificate, key = _test_certificates()
    certificate_file = directory / "server.pem"
    certificate_file.write_bytes(certificate + key)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate_file)
    return context


class RealtimeServer:
    """A loopback OpenAI Realtime endpoint on 127.0.0.1.

    It records each handshake and every event the client sends, greets with session.created, and answers each
    session.update with a session.updated echoing its session, unless told to reject or ignore updates. Tests reach
    the open socket through ``push`` and ``close``.
    """

    def __init__(
        self,
        *,
        reject_handshake_with: int | None = None,
        reject_updates: bool = False,
        ignore_updates: bool = False,
        hold_handshake: bool = False,
    ) -> None:
        self.reject_handshake_with = reject_handshake_with
        self.hold_handshake = hold_handshake
        self.held: ServerConnection | None = None
        self._released = asyncio.Event()
        self._directory = Path(tempfile.mkdtemp())
        self.reject_updates = reject_updates
        self.ignore_updates = ignore_updates
        self.handshakes: list[Request] = []
        self.received: list[dict[str, Any]] = []
        self.socket_closed = asyncio.Event()
        self._connection: ServerConnection | None = None
        self._server: Server | None = None
        self.url = ""

    async def __aenter__(self) -> "RealtimeServer":
        self._server = await serve(
            self._handle, "127.0.0.1", 0, process_request=self._process_request, ssl=_server_tls(self._directory)
        )
        port = next(iter(self._server.sockets)).getsockname()[1]
        self.url = f"https://127.0.0.1:{port}"
        return self

    async def __aexit__(self, *args: Any) -> None:
        assert self._server is not None
        self._released.set()
        self._server.close()
        await self._server.wait_closed()
        shutil.rmtree(self._directory)

    @property
    def path(self) -> str:
        return urlsplit(self.handshakes[-1].path).path

    @property
    def query(self) -> dict[str, str]:
        return dict(parse_qsl(urlsplit(self.handshakes[-1].path).query))

    @property
    def headers(self) -> Headers:
        return self.handshakes[-1].headers

    async def _process_request(self, connection: ServerConnection, request: Request) -> Response | None:
        self.handshakes.append(request)
        if self.hold_handshake:
            self.held = connection
            await self._released.wait()
        if self.reject_handshake_with is not None:
            return connection.respond(HTTPStatus(self.reject_handshake_with), "Access denied.\n")
        return None

    async def _handle(self, connection: ServerConnection) -> None:
        self._connection = connection
        await connection.send(json.dumps({"type": "session.created", "event_id": "evt_0", "session": {"id": "sess_1"}}))
        try:
            async for message in connection:
                event = json.loads(message)
                self.received.append(event)
                if event["type"] == "session.update" and not self.ignore_updates:
                    await connection.send(json.dumps(self._answer_update(event)))
        except ConnectionClosed:
            pass
        finally:
            self.socket_closed.set()

    def _answer_update(self, update: dict[str, Any]) -> dict[str, Any]:
        if self.reject_updates:
            return {
                "type": "error",
                "event_id": "evt_err",
                "error": {
                    "type": "invalid_request_error",
                    "code": "invalid_value",
                    "message": "Bad.",
                    "event_id": update.get("event_id"),
                },
            }
        return {"type": "session.updated", "event_id": "evt_updated", "session": update.get("session", {})}

    async def push(self, event: dict[str, Any]) -> None:
        assert self._connection is not None
        await self._connection.send(json.dumps(event))

    async def close(self, code: int) -> None:
        assert self._connection is not None
        await self._connection.close(code=code)


class StaticTokenCredential:
    """An async token credential that records the scopes it was asked for and returns its tokens in turn."""

    def __init__(self, *tokens: str) -> None:
        self.tokens = tokens
        self.scopes: list[tuple[str, ...]] = []

    async def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:
        self.scopes.append(scopes)
        token = self.tokens[min(len(self.scopes), len(self.tokens)) - 1]
        # Short-lived, so a caching token provider asks again for each connection.
        return AccessToken(token, int(time.time()) + 60)

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


async def until(condition: Callable[[], Any], timeout: float = 5.0) -> None:
    """Wait until ``condition()`` holds."""

    async def poll() -> None:
        while not condition():  # noqa: ASYNC110 - the condition reads state another task updates
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), timeout)


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Settings come only from what each test passes or sets."""
    for name in list(os.environ):
        if name.startswith(("OPENAI_", "AZURE_OPENAI_")):
            monkeypatch.delenv(name)


# region Construction and settings


def test_an_explicit_model_is_the_realtime_model() -> None:
    """OpenAIRealtimeClient("gpt-realtime", api_key="sk-test") has model "gpt-realtime"."""
    client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test")

    assert client.model == "gpt-realtime"


def test_the_openai_realtime_model_variable_comes_before_openai_model(monkeypatch: pytest.MonkeyPatch) -> None:
    """With OPENAI_API_KEY, OPENAI_REALTIME_MODEL="gpt-realtime-mini" and OPENAI_MODEL="gpt-5" set, the client's model
    is "gpt-realtime-mini"."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("OPENAI_REALTIME_MODEL", "gpt-realtime-mini")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-5")

    assert OpenAIRealtimeClient().model == "gpt-realtime-mini"


def test_the_openai_model_variable_is_the_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """With OPENAI_API_KEY and OPENAI_MODEL="gpt-realtime" set and no OPENAI_REALTIME_MODEL, the client's model is
    "gpt-realtime"."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-realtime")

    assert OpenAIRealtimeClient().model == "gpt-realtime"


def test_the_azure_realtime_deployment_variable_comes_before_azure_model(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no OPENAI_API_KEY, and AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, AZURE_OPENAI_REALTIME_MODEL=
    "realtime-deployment" and AZURE_OPENAI_MODEL="chat-deployment" set, the client's model is "realtime-deployment"."""
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", AZURE_ENDPOINT)
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "azure-key")
    monkeypatch.setenv("AZURE_OPENAI_REALTIME_MODEL", "realtime-deployment")
    monkeypatch.setenv("AZURE_OPENAI_MODEL", "chat-deployment")

    assert OpenAIRealtimeClient().model == "realtime-deployment"


def test_the_azure_model_variable_is_the_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no OPENAI_API_KEY, and AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY and AZURE_OPENAI_MODEL="chat-deployment"
    set, the client's model is "chat-deployment"."""
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", AZURE_ENDPOINT)
    monkeypatch.setenv("AZURE_OPENAI_API_KEY", "azure-key")
    monkeypatch.setenv("AZURE_OPENAI_MODEL", "chat-deployment")

    assert OpenAIRealtimeClient().model == "chat-deployment"


def test_azure_routing_reports_the_azure_openai_provider_name() -> None:
    """A client with azure_endpoint, api_key and model reports OTEL_PROVIDER_NAME "azure.ai.openai"; an OpenAI-routed
    client created afterwards still reports "openai"."""
    azure_client = OpenAIRealtimeClient("realtime-deployment", azure_endpoint=AZURE_ENDPOINT, api_key="azure-key")
    openai_client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test")

    assert azure_client.OTEL_PROVIDER_NAME == "azure.ai.openai"
    assert openai_client.OTEL_PROVIDER_NAME == "openai"


def test_an_injected_azure_openai_client_raises() -> None:
    """OpenAIRealtimeClient(model=..., async_client=AsyncAzureOpenAI(...)) raises ValueError whose message names
    azure_endpoint."""
    azure_client = AsyncAzureOpenAI(
        azure_endpoint=AZURE_ENDPOINT, api_key="azure-key", api_version="2025-04-01-preview"
    )

    with pytest.raises(ValueError, match="azure_endpoint"):
        OpenAIRealtimeClient("realtime-deployment", async_client=azure_client)


def test_an_injected_openai_client_without_a_model_raises() -> None:
    """With no model variables set, OpenAIRealtimeClient(async_client=AsyncOpenAI(api_key="sk-test")) raises
    SettingNotFoundError naming OPENAI_REALTIME_MODEL."""
    with pytest.raises(SettingNotFoundError, match="OPENAI_REALTIME_MODEL"):
        OpenAIRealtimeClient(async_client=AsyncOpenAI(api_key="sk-test"))


def test_missing_websockets_raises_the_install_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    """When websockets can't be found, constructing the client raises ModuleNotFoundError naming
    "agent-framework-openai[realtime]"."""
    real_find_spec = importlib.util.find_spec

    def find_spec_without_websockets(name: str, *args: Any, **kwargs: Any) -> Any:
        return None if name == "websockets" else real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", find_spec_without_websockets)

    with pytest.raises(ModuleNotFoundError, match=re.escape("pip install 'agent-framework-openai[realtime]'")):
        OpenAIRealtimeClient("gpt-realtime", api_key="sk-test")


# endregion

# region Connecting (loopback server)


async def test_unsupported_audio_format_raises_before_connecting() -> None:
    """start_session(options={"input_audio_format": "audio/pcm;rate=16000"}) raises ValueError, and the server sees no
    connection."""
    async with RealtimeServer() as server:
        client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test", base_url=f"{server.url}/v1")

        with pytest.raises(ValueError, match="audio/pcm;rate=16000"):
            await client.start_session(options={"input_audio_format": "audio/pcm;rate=16000"})

    assert server.handshakes == []


async def test_openai_connects_to_the_realtime_path_with_its_model_and_key() -> None:
    """With base_url "<server>/v1", api_key "sk-test" and model "gpt-realtime", start_session() connects to path
    "/v1/realtime" with query {"model": "gpt-realtime"} and Authorization "Bearer sk-test"."""
    async with RealtimeServer() as server:
        client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test", base_url=f"{server.url}/v1")
        session = await asyncio.wait_for(client.start_session(), 5)
        await session.close()

    assert server.path == "/v1/realtime"
    assert server.query == {"model": "gpt-realtime"}
    assert server.headers["Authorization"] == "Bearer sk-test"


async def test_a_session_runs_over_the_connection() -> None:
    """start_session(options={"instructions": "Be brief."}) sends session.update with session {"type": "realtime",
    "instructions": "Be brief."} and returns on the server's session.updated; session.send(Message("user", ["Hi"]))
    reaches the server as conversation.item.create; receive() yields session_created first."""
    async with RealtimeServer() as server:
        client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test", base_url=f"{server.url}/v1")
        session = await asyncio.wait_for(client.start_session(options={"instructions": "Be brief."}), 5)
        await session.send(Message("user", ["Hi"]))
        first = await asyncio.wait_for(anext(aiter(session.receive())), 5)
        await until(lambda: len(server.received) == 2)
        await session.close()

    assert server.received[0]["type"] == "session.update"
    assert server.received[0]["session"] == {"type": "realtime", "instructions": "Be brief."}
    assert server.received[1]["type"] == "conversation.item.create"
    assert server.received[1]["item"]["content"] == [{"type": "input_text", "text": "Hi"}]
    assert first.kind == "session_created"


async def test_azure_connects_to_the_v1_realtime_path_without_an_api_version() -> None:
    """With azure_endpoint "<server>", an async credential returning "token-1" and model "realtime-deployment",
    start_session() connects to path "/openai/v1/realtime" with query {"model": "realtime-deployment"} and no
    api-version, sends Authorization "Bearer token-1", and asks the credential for the
    "https://cognitiveservices.azure.com/.default" scope."""
    credential = StaticTokenCredential("token-1")

    async with RealtimeServer() as server:
        client = OpenAIRealtimeClient("realtime-deployment", azure_endpoint=server.url, credential=credential)
        session = await asyncio.wait_for(client.start_session(), 5)
        await session.close()

    assert server.path == "/openai/v1/realtime"
    assert server.query == {"model": "realtime-deployment"}
    assert server.headers["Authorization"] == "Bearer token-1"
    assert credential.scopes == [("https://cognitiveservices.azure.com/.default",)]


async def test_a_sync_token_provider_api_key_sends_a_bearer_token() -> None:
    """With azure_endpoint "<server>" and api_key set to a synchronous callable returning "token-3", as
    azure.identity's get_bearer_token_provider() makes, the handshake carries Authorization "Bearer token-3"."""
    async with RealtimeServer() as server:
        client = OpenAIRealtimeClient("realtime-deployment", azure_endpoint=server.url, api_key=lambda: "token-3")
        session = await asyncio.wait_for(client.start_session(), 5)
        await session.close()

    assert server.headers["Authorization"] == "Bearer token-3"


async def test_rejected_handshake_raises_a_connection_exception() -> None:
    """When the server answers the handshake with 401, start_session() raises RealtimeConnectionException whose message
    doesn't contain the api key "sk-test"."""
    async with RealtimeServer(reject_handshake_with=401) as server:
        client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test", base_url=f"{server.url}/v1")

        with pytest.raises(RealtimeConnectionException) as raised:
            await asyncio.wait_for(client.start_session(), 5)

    assert "sk-test" not in str(raised.value)


async def test_an_unreachable_endpoint_raises_a_connection_exception() -> None:
    """With base_url pointing at a closed port on 127.0.0.1, start_session() raises RealtimeConnectionException."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
    client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test", base_url=f"https://127.0.0.1:{closed_port}/v1")

    with pytest.raises(RealtimeConnectionException):
        await asyncio.wait_for(client.start_session(), 5)


async def test_abnormal_server_close_raises_with_its_close_code() -> None:
    """When the server closes the socket with 1011, iterating receive() raises RealtimeConnectionException with
    close_code 1011."""
    async with RealtimeServer() as server:
        client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test", base_url=f"{server.url}/v1")
        session = await asyncio.wait_for(client.start_session(), 5)
        await server.close(1011)

        with pytest.raises(RealtimeConnectionException) as raised:
            async with asyncio.timeout(5):
                async for _ in session.receive():
                    pass
        await session.close()

    assert raised.value.close_code == 1011


async def test_normal_server_close_ends_receive() -> None:
    """When the server closes the socket with 1000, iterating receive() ends without raising."""
    async with RealtimeServer() as server:
        client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test", base_url=f"{server.url}/v1")
        session = await asyncio.wait_for(client.start_session(), 5)
        await server.close(1000)

        async with asyncio.timeout(5):
            kinds = [event.kind async for event in session.receive()]
        await session.close()

    assert kinds == ["session_created"]


async def test_sending_on_a_dead_socket_raises() -> None:
    """After the server closes the socket with 1011, session.send(Message("user", ["Hi"])) raises
    RealtimeConnectionException."""
    async with RealtimeServer() as server:
        client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test", base_url=f"{server.url}/v1")
        session = await asyncio.wait_for(client.start_session(), 5)
        await server.close(1011)

        with pytest.raises(RealtimeConnectionException):
            await session.send(Message("user", ["Hi"]))
        await session.close()


async def test_failed_start_closes_the_socket() -> None:
    """When the server rejects the initial session.update, start_session() raises RealtimeControlException and the
    server sees the socket close."""
    async with RealtimeServer(reject_updates=True) as server:
        client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test", base_url=f"{server.url}/v1")

        with pytest.raises(RealtimeControlException):
            await asyncio.wait_for(client.start_session(options={"instructions": "Be brief."}), 5)
        await asyncio.wait_for(server.socket_closed.wait(), 1)


async def test_cancelled_start_closes_the_socket() -> None:
    """When the server never answers the initial session.update and the start_session() task is cancelled, the task
    raises CancelledError and the server sees the socket close."""
    async with RealtimeServer(ignore_updates=True) as server:
        client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test", base_url=f"{server.url}/v1")
        starting = asyncio.create_task(client.start_session(options={"instructions": "Be brief."}))
        await until(lambda: len(server.received) == 1)
        starting.cancel()

        with pytest.raises(asyncio.CancelledError):
            await starting
        await asyncio.wait_for(server.socket_closed.wait(), 1)


# endregion


class TestVerification:
    async def test_an_injected_openai_client_with_a_credential_is_used_as_it_is(self) -> None:
        """With async_client=AsyncOpenAI(api_key="sk-injected", base_url="<server>/openai/v1"), a credential and model
        "realtime-deployment", the client reports "azure.ai.openai" and connects through the injected client: path
        "/openai/v1/realtime" with Authorization "Bearer sk-injected", and the credential is never asked for a
        token."""
        credential = StaticTokenCredential("token-1")

        async with RealtimeServer() as server:
            injected = AsyncOpenAI(api_key="sk-injected", base_url=f"{server.url}/openai/v1")
            client = OpenAIRealtimeClient("realtime-deployment", async_client=injected, credential=credential)
            session = await asyncio.wait_for(client.start_session(), 5)
            await session.close()

        assert client.OTEL_PROVIDER_NAME == "azure.ai.openai"
        assert server.path == "/openai/v1/realtime"
        assert server.headers["Authorization"] == "Bearer sk-injected"
        assert credential.scopes == []

    async def test_azure_api_key_is_sent_as_a_bearer_token(self) -> None:
        """With azure_endpoint "<server>" and api_key "azure-key", the handshake carries Authorization
        "Bearer azure-key"."""
        async with RealtimeServer() as server:
            client = OpenAIRealtimeClient("realtime-deployment", azure_endpoint=server.url, api_key="azure-key")
            session = await asyncio.wait_for(client.start_session(), 5)
            await session.close()

        assert server.path == "/openai/v1/realtime"
        assert server.headers["Authorization"] == "Bearer azure-key"

    async def test_azure_token_provider_callable_sends_a_bearer_token(self) -> None:
        """With azure_endpoint "<server>" and api_key set to an async callable returning "token-2", the handshake
        carries Authorization "Bearer token-2"."""

        async def token_provider() -> str:
            return "token-2"

        async with RealtimeServer() as server:
            client = OpenAIRealtimeClient("realtime-deployment", azure_endpoint=server.url, api_key=token_provider)
            session = await asyncio.wait_for(client.start_session(), 5)
            await session.close()

        assert server.headers["Authorization"] == "Bearer token-2"

    async def test_user_agent_carries_the_agent_framework_prefix(self) -> None:
        """The handshake's User-Agent starts with the agent-framework-python user agent."""
        async with RealtimeServer() as server:
            client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test", base_url=f"{server.url}/v1")
            session = await asyncio.wait_for(client.start_session(), 5)
            await session.close()

        assert server.headers["User-Agent"].startswith(get_user_agent())

    def test_no_model_anywhere_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With only OPENAI_API_KEY set, OpenAIRealtimeClient() raises SettingNotFoundError."""
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

        with pytest.raises(SettingNotFoundError):
            OpenAIRealtimeClient()

    def test_an_explicit_model_wins_over_the_variables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With OPENAI_API_KEY, OPENAI_REALTIME_MODEL and OPENAI_MODEL set, OpenAIRealtimeClient("gpt-realtime") has
        model "gpt-realtime"."""
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("OPENAI_REALTIME_MODEL", "gpt-realtime-mini")
        monkeypatch.setenv("OPENAI_MODEL", "gpt-5")

        assert OpenAIRealtimeClient("gpt-realtime").model == "gpt-realtime"

    def test_a_credential_routes_to_azure_even_with_an_openai_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_REALTIME_MODEL set, a client given a credential
        reports "azure.ai.openai" and the Azure deployment as its model."""
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", AZURE_ENDPOINT)
        monkeypatch.setenv("AZURE_OPENAI_REALTIME_MODEL", "realtime-deployment")

        client = OpenAIRealtimeClient(credential=StaticTokenCredential("token-1"))

        assert client.OTEL_PROVIDER_NAME == "azure.ai.openai"
        assert client.model == "realtime-deployment"

    async def test_each_connect_fetches_a_fresh_token(self) -> None:
        """Two start_session() calls on one Azure client with a credential returning "token-1" then "token-2" send
        Authorization "Bearer token-1" then "Bearer token-2"."""
        credential = StaticTokenCredential("token-1", "token-2")

        async with RealtimeServer() as server:
            client = OpenAIRealtimeClient("realtime-deployment", azure_endpoint=server.url, credential=credential)
            for _ in range(2):
                session = await asyncio.wait_for(client.start_session(), 5)
                await session.close()

        assert [handshake.headers["Authorization"] for handshake in server.handshakes] == [
            "Bearer token-1",
            "Bearer token-2",
        ]

    async def test_an_injected_openai_client_connects_where_it_points(self) -> None:
        """OpenAIRealtimeClient("gpt-realtime", async_client=AsyncOpenAI(api_key="sk-injected", base_url="<server>/v1"))
        connects to path "/v1/realtime" with Authorization "Bearer sk-injected"."""
        async with RealtimeServer() as server:
            injected = AsyncOpenAI(api_key="sk-injected", base_url=f"{server.url}/v1")
            client = OpenAIRealtimeClient("gpt-realtime", async_client=injected)
            session = await asyncio.wait_for(client.start_session(), 5)
            await session.close()

        assert server.path == "/v1/realtime"
        assert server.headers["Authorization"] == "Bearer sk-injected"

    async def test_closing_the_session_ends_a_blocked_receive(self) -> None:
        """While a reader waits in receive(), session.close() ends the iteration without raising."""
        async with RealtimeServer() as server:
            client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test", base_url=f"{server.url}/v1")
            session = await asyncio.wait_for(client.start_session(), 5)
            kinds: list[str] = []

            async def read() -> None:
                async for event in session.receive():
                    kinds.append(event.kind)

            reader = asyncio.create_task(read())
            await until(lambda: kinds == ["session_created"])
            await session.close()
            await asyncio.wait_for(reader, 5)

        assert kinds == ["session_created"]

    async def test_no_compression_is_negotiated(self) -> None:
        """The handshake offers no permessage-deflate extension."""
        async with RealtimeServer() as server:
            client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test", base_url=f"{server.url}/v1")
            session = await asyncio.wait_for(client.start_session(), 5)
            await session.close()

        assert "Sec-WebSocket-Extensions" not in server.headers

    async def test_cancelling_during_the_handshake_leaves_nothing_open(self) -> None:
        """When the server holds the handshake and the start_session() task is cancelled, the task raises
        CancelledError and the server sees its connection closed."""
        async with RealtimeServer(hold_handshake=True) as server:
            client = OpenAIRealtimeClient("gpt-realtime", api_key="sk-test", base_url=f"{server.url}/v1")
            starting = asyncio.create_task(client.start_session())
            await until(lambda: server.held is not None)
            starting.cancel()

            with pytest.raises(asyncio.CancelledError):
                await starting
            assert server.held is not None
            await asyncio.wait_for(server.held.connection_lost_waiter, 1)

    def test_client_modules_import_without_websockets(self) -> None:
        """In a fresh interpreter, importing agent_framework_openai._realtime_client and
        agent_framework_openai._realtime_options doesn't load websockets."""
        probe = (
            "import sys, agent_framework_openai._realtime_client, agent_framework_openai._realtime_options; "
            "print('websockets' in sys.modules)"
        )

        completed = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=True)

        assert completed.stdout.strip() == "False"
