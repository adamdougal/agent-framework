"""Probe the realtime extras in the current environment.

For each extra, it checks the wheel declares the extra, imports what the realtime clients import, and makes one
loopback round trip through each transport the clients use: the openai SDK over websockets, the Voice Live SDK over
aiohttp, and azure-ai-projects' voice agent client over aiohttp and TLS.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata as metadata
import ipaddress
import json
import platform
import ssl
import sys
import tempfile
import time
import traceback
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

DISTRIBUTIONS = [
    "agent-framework-core",
    "agent-framework-openai",
    "agent-framework-foundry",
    "openai",
    "websockets",
    "azure-ai-voicelive",
    "azure-ai-projects",
    "aiohttp",
    "azure-core",
    "cryptography",
]
TIMEOUT_SECONDS = 30


def installed_versions() -> dict[str, str | None]:
    versions: dict[str, str | None] = {}
    for distribution in DISTRIBUTIONS:
        try:
            versions[distribution] = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            versions[distribution] = None
    return versions


def declares_extra(distribution: str) -> str:
    extras = metadata.metadata(distribution).get_all("Provides-Extra") or []
    if "realtime" not in extras:
        raise AssertionError(f"{distribution} declares extras {extras}, not 'realtime'")
    requirements = [
        requirement
        for requirement in metadata.requires(distribution) or []
        if "realtime" in requirement
    ]
    return "; ".join(requirements)


async def aiohttp_server(
    handler: Callable[[Any], Awaitable[Any]],
    ssl_context: ssl.SSLContext | None = None,
) -> tuple[Any, int]:
    from aiohttp import web

    app = web.Application()
    app.router.add_get("/{tail:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=ssl_context)
    await site.start()
    port = runner.addresses[0][1]
    return runner, port


def self_signed_context(directory: Path) -> ssl.SSLContext:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    certificate_path = directory / "cert.pem"
    key_path = directory / "key.pem"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certificate_path, key_path)
    return context


def openai_imports() -> str:
    import agent_framework_openai  # noqa: F401
    from openai import AsyncOpenAI  # noqa: F401
    from websockets.asyncio.client import connect  # noqa: F401

    return "agent_framework_openai, openai.AsyncOpenAI, websockets.asyncio.client"


async def openai_loopback() -> str:
    from openai import AsyncOpenAI
    from websockets.asyncio.server import serve

    seen: dict[str, Any] = {}
    handled = asyncio.Event()

    async def handler(connection: Any) -> None:
        try:
            seen["path"] = connection.request.path
            seen["authorization"] = connection.request.headers.get("Authorization")
            await connection.send(json.dumps({"type": "session.created", "event_id": "evt_probe", "session": {}}))
            seen["client_event"] = json.loads(await connection.recv())["type"]
        finally:
            handled.set()

    async with serve(handler, "127.0.0.1", 0) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        client = AsyncOpenAI(api_key="probe-key", websocket_base_url=f"ws://127.0.0.1:{port}/openai/v1")
        async with client.realtime.connect(model="probe-model") as connection:
            first = json.loads(await connection.recv_bytes())
            await connection.send({"type": "response.cancel"})
            await asyncio.wait_for(handled.wait(), TIMEOUT_SECONDS)
        await client.close()

    if first["type"] != "session.created":
        raise AssertionError(f"client received {first}")
    if not seen.get("path", "").startswith("/openai/v1/realtime?model=probe-model"):
        raise AssertionError(f"server saw path {seen.get('path')}")
    if seen.get("authorization") != "Bearer probe-key":
        raise AssertionError(f"server saw Authorization {seen.get('authorization')}")
    if seen.get("client_event") != "response.cancel":
        raise AssertionError(f"server received {seen.get('client_event')}")
    return f"path={seen['path']}"


def foundry_imports() -> str:
    import agent_framework_foundry  # noqa: F401
    import aiohttp  # noqa: F401
    from azure.ai.projects.aio import AIProjectClient  # noqa: F401
    from azure.ai.voicelive.aio import connect  # noqa: F401

    return "agent_framework_foundry, aiohttp, azure.ai.projects.aio, azure.ai.voicelive.aio"


async def voicelive_loopback() -> str:
    from aiohttp import WSMsgType, web
    from azure.ai.voicelive.aio import connect
    from azure.core.credentials import AzureKeyCredential

    seen: dict[str, Any] = {}

    async def handler(request: Any) -> Any:
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        seen["path"] = request.path
        seen["query"] = dict(request.query)
        seen["api_key"] = request.headers.get("api-key")
        message = await websocket.receive()
        if message.type == WSMsgType.TEXT:
            seen["client_event"] = json.loads(message.data)["type"]
        await websocket.send_str(json.dumps({"type": "session.updated", "event_id": "evt_probe", "session": {}}))
        await websocket.close()
        return websocket

    runner, port = await aiohttp_server(handler)
    try:
        async with connect(
            endpoint=f"http://127.0.0.1:{port}",
            credential=AzureKeyCredential("probe-key"),
            model="probe-model",
            api_version="2026-07-15",
        ) as connection:
            await connection.send({"type": "session.update", "session": {"instructions": "probe"}})
            first = json.loads(await connection.recv_bytes())
    finally:
        await runner.cleanup()

    if first["type"] != "session.updated":
        raise AssertionError(f"client received {first}")
    if seen.get("path") != "/voice-live/realtime":
        raise AssertionError(f"server saw path {seen.get('path')}")
    query = seen.get("query", {})
    if query.get("api-version") != "2026-07-15" or query.get("model") != "probe-model":
        raise AssertionError(f"server saw query {query}")
    if seen.get("api_key") != "probe-key":
        raise AssertionError(f"server saw api-key {seen.get('api_key')}")
    if seen.get("client_event") != "session.update":
        raise AssertionError(f"server received {seen.get('client_event')}")
    return f"path={seen['path']}"


class ProbeCredential:
    async def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        from azure.core.credentials import AccessToken

        return AccessToken("probe-token", int(time.time()) + 3600)

    async def close(self) -> None:
        return None

    async def __aenter__(self) -> ProbeCredential:
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None


async def foundry_voice_agent_loopback() -> str:
    from aiohttp import WSMsgType, web
    from azure.ai.projects.aio import AIProjectClient

    seen: dict[str, Any] = {}

    async def handler(request: Any) -> Any:
        websocket = web.WebSocketResponse(protocols=("realtime",))
        await websocket.prepare(request)
        seen["path"] = request.path
        seen["query"] = dict(request.query)
        seen["authorization"] = request.headers.get("Authorization")
        seen["features"] = request.headers.get("Foundry-Features")
        seen["agent_version"] = request.headers.get("x-agent-version-override")
        await websocket.send_str(json.dumps({"type": "session.created", "event_id": "evt_probe", "session": {}}))
        message = await websocket.receive()
        if message.type == WSMsgType.TEXT:
            seen["client_event"] = json.loads(message.data)["type"]
        await websocket.close()
        return websocket

    with tempfile.TemporaryDirectory() as directory:
        runner, port = await aiohttp_server(handler, self_signed_context(Path(directory)))
        project = AIProjectClient(
            endpoint=f"https://127.0.0.1:{port}/api/projects/probe",
            credential=ProbeCredential(),
        )
        try:
            async with project.beta.voice_agents.realtime.connect(
                agent_name="probe-agent",
                api_version="v1",
                extra_headers={"x-agent-version-override": "7"},
                ssl=False,
            ) as connection:
                event = await connection.recv()
                first = event.as_dict() if hasattr(event, "as_dict") else dict(event)
                await connection.send({"type": "response.cancel"})
                for _ in range(100):
                    if "client_event" in seen:
                        break
                    await asyncio.sleep(0.05)
        finally:
            await project.close()
            await runner.cleanup()

    if first.get("type") != "session.created":
        raise AssertionError(f"client received {first}")
    if seen.get("path") != "/api/projects/probe/agents/probe-agent/endpoint/protocols/voice":
        raise AssertionError(f"server saw path {seen.get('path')}")
    if seen.get("query", {}).get("api-version") != "v1":
        raise AssertionError(f"server saw query {seen.get('query')}")
    if seen.get("authorization") != "Bearer probe-token":
        raise AssertionError(f"server saw Authorization {seen.get('authorization')}")
    if seen.get("features") != "VoiceAgents=V1Preview" or seen.get("agent_version") != "7":
        raise AssertionError(f"server saw headers {seen}")
    if seen.get("client_event") != "response.cancel":
        raise AssertionError(f"server received {seen.get('client_event')}")
    return f"path={seen['path']} features={seen['features']}"


CHECKS: dict[str, list[tuple[str, Callable[[], Any]]]] = {
    "openai": [
        ("openai: extra declared", lambda: declares_extra("agent-framework-openai")),
        ("openai: imports", openai_imports),
        ("openai: loopback over websockets", openai_loopback),
    ],
    "foundry": [
        ("foundry: extra declared", lambda: declares_extra("agent-framework-foundry")),
        ("foundry: imports", foundry_imports),
        ("foundry: Voice Live loopback over aiohttp", voicelive_loopback),
        ("foundry: voice agent loopback over aiohttp and TLS", foundry_voice_agent_loopback),
    ],
}


def run_check(name: str, check: Callable[[], Any]) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        outcome = check()
        if asyncio.iscoroutine(outcome):
            outcome = asyncio.run(asyncio.wait_for(outcome, TIMEOUT_SECONDS))
        result: dict[str, Any] = {"check": name, "ok": True, "detail": str(outcome)}
    except BaseException as error:  # noqa: BLE001 - every failure is a probe result
        result = {
            "check": name,
            "ok": False,
            "detail": f"{type(error).__name__}: {error}",
            "traceback": traceback.format_exc(limit=8),
        }
    result["seconds"] = round(time.perf_counter() - started, 2)
    print(f"{'PASS' if result['ok'] else 'FAIL'} {name}: {result['detail']}", flush=True)
    if not result["ok"]:
        print(result["traceback"], flush=True)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extra", action="append", choices=sorted(CHECKS), required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--json", type=Path, required=True)
    arguments = parser.parse_args()

    results = [run_check(name, check) for extra in arguments.extra for name, check in CHECKS[extra]]
    report = {
        "label": arguments.label,
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": f"{sys.platform}-{platform.machine()}",
        "versions": installed_versions(),
        "checks": results,
    }
    arguments.json.parent.mkdir(parents=True, exist_ok=True)
    arguments.json.write_text(json.dumps(report, indent=2))
    print(json.dumps(report["versions"]), flush=True)
    return 0 if all(result["ok"] for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
