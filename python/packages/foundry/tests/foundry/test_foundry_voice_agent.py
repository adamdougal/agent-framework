# Copyright (c) Microsoft. All rights reserved.

"""Behaviour of FoundryVoiceAgent and FoundryVoiceAgentClient: the definition, the session, conversations and handoffs.

They run against a fake AIProjectClient whose agents API returns a scripted definition and whose voice-agent connect
returns a scripted server that echoes each session.update. No service calls.
"""

import asyncio
import base64
import contextlib
import importlib.util
import json
import re
import time
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
from types import TracebackType
from typing import Any, Literal, cast

import pytest
from agent_framework import ConversationContext, InMemoryHistoryProvider, Message, RealtimeAgent, tool
from agent_framework._telemetry import get_user_agent
from agent_framework.exceptions import (
    RealtimeConnectionException,
    RealtimeControlException,
    RealtimeControlTimeoutException,
)
from agent_framework_orchestrations import RealtimeHandoffBuilder
from azure.ai.projects.aio import AIProjectClient
from azure.ai.projects.models import RealtimeServerEventResponseCreated
from azure.core.credentials import AccessToken
from opentelemetry import baggage, context, trace
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from agent_framework_foundry import AzureVoice, FoundryVoiceAgent, FoundryVoiceAgentClient, FoundryVoiceAgentOptions
from agent_framework_foundry import _foundry_voice_client as voice_client_module
from agent_framework_foundry._foundry_voice_client import DEFINITION_TOOLS

ENDPOINT = "https://foundry.services.ai.azure.com/api/projects/contact-centre"

GET_CURRENT_TIME = {
    "type": "function",
    "name": "get_current_time",
    "description": "Get the current time.",
    "parameters": {"type": "object", "properties": {}, "required": []},
    "response_scheduling": "when_idle",
}
GET_OFFICE_HOURS = {
    "type": "function",
    "name": "get_office_hours",
    "description": "Get the office hours.",
    "parameters": {"type": "object", "properties": {}, "required": []},
    "response_scheduling": "when_idle",
}
ORDERS_MCP = {
    "type": "mcp",
    "server_label": "orders",
    "server_url": "https://orders.example/mcp",
    "require_approval": "never",
}

DEFINITION: dict[str, Any] = {
    "kind": "voice",
    "instructions": "You are a receptionist.",
    "audio": {
        "input": {
            "format": {"type": "audio/pcm", "rate": 24000},
            "transcription": {"model": "gpt-4o-transcribe"},
            "turn_detection": {"type": "server_vad", "silence_duration_ms": 800},
        },
        "output": {
            "format": {"type": "audio/pcm", "rate": 24000},
            "voice": "en-US-AvaMultilingualNeural",
            "voice_type": "azure-standard",
            "voice_temperature": 0.8,
        },
    },
    "tools": [GET_CURRENT_TIME, GET_OFFICE_HOURS, ORDERS_MCP],
    "tool_choice": "auto",
}
"""A published voice-agent definition, as the agents API returns it."""

FLATTENED_VOICE_FIELDS = {
    "name": "voice",
    "type": "voice_type",
    "temperature": "voice_temperature",
    "endpoint_id": "custom_voice_endpoint_id",
    "model": "personal_voice_model",
    "custom_lexicon_url": "custom_lexicon_url",
    "custom_text_normalization_url": "custom_text_normalization_url",
    "prefer_locales": "prefer_locales",
    "locale": "voice_locale",
    "style": "style",
    "pitch": "pitch",
    "volume": "volume",
}
"""How a voice-agent session reports each field of an Azure voice object: flattened beside the voice name."""


def flattened(voice: Any) -> dict[str, Any]:
    """The audio.output fields a voice-agent session reports for ``voice``."""
    if not isinstance(voice, Mapping):
        return {"voice": voice, "voice_type": "openai"}
    return {FLATTENED_VOICE_FIELDS[field]: value for field, value in voice.items()}


def session_from(definition: Mapping[str, Any]) -> dict[str, Any]:
    """The session a voice-agent session reports for ``definition``: the GA session, without publishing fields."""
    return {
        "type": "realtime",
        "instructions": definition["instructions"],
        "audio": json.loads(json.dumps(definition["audio"])),
        "tools": [
            {key: value for key, value in tool.items() if key != "response_scheduling"} for tool in definition["tools"]
        ],
        "tool_choice": definition["tool_choice"],
    }


def with_audio_output(**audio_output: Any) -> dict[str, Any]:
    """The test definition with ``audio_output`` as its audio.output, after the output format."""
    definition = json.loads(json.dumps(DEFINITION))
    definition["audio"]["output"] = {"format": {"type": "audio/pcm", "rate": 24000}, **audio_output}
    return definition


def called(response_id: str, *names: str) -> list[dict[str, Any]]:
    """A response that calls each of ``names``, from its response.created to its response.done."""
    events: list[dict[str, Any]] = [{"type": "response.created", "response": {"id": response_id, "metadata": None}}]
    for name in names:
        events.append({
            "type": "response.output_item.done",
            "response_id": response_id,
            "item": {"id": f"item_{name}", "type": "function_call", "call_id": f"call_{name}", "name": name},
        })
    events.append({"type": "response.done", "response": {"id": response_id, "status": "completed"}})
    return events


def tool_outputs(server: Any) -> dict[str, str]:
    """The function call outputs sent so far, by call id."""
    return {
        event["item"]["call_id"]: event["item"]["output"]
        for event in server.sent
        if event["type"] == "conversation.item.create" and event["item"]["type"] == "function_call_output"
    }


class StaticTokenCredential:
    """An async token credential that returns a fixed token."""

    async def get_token(self, *scopes: str, **kwargs: Any) -> AccessToken:
        return AccessToken("token", int(time.time()) + 3600)

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


class FakeModel(dict[str, Any]):
    """An SDK model: a mapping that also offers ``as_dict()``."""

    def as_dict(self) -> dict[str, Any]:
        return json.loads(json.dumps(self))


class FakeAgents:
    """The agents API: versions of one published voice agent, the last of them the latest."""

    def __init__(self, versions: Mapping[str, Mapping[str, Any]]) -> None:
        self.versions = dict(versions)
        self.calls: list[tuple[Any, ...]] = []
        self.closed = False

    def raise_if_closed(self) -> None:
        if self.closed:
            raise ValueError("HTTP transport has already been closed.")

    async def get(self, agent_name: str, **kwargs: Any) -> FakeModel:
        self.raise_if_closed()
        self.calls.append(("get", agent_name))
        latest = list(self.versions)[-1]
        return FakeModel(name=agent_name, versions={"latest": {"version": latest, "definition": self.versions[latest]}})

    async def get_version(self, agent_name: str, agent_version: str, **kwargs: Any) -> FakeModel:
        self.raise_if_closed()
        self.calls.append(("get_version", agent_name, agent_version))
        return FakeModel(name=agent_name, version=agent_version, definition=self.versions[agent_version])


class FakeVoiceAgentServer:
    """One voice-agent socket: greets with session.created and the definition's session.updated, answers each
    session.update with a session.updated echoing the session merged with it, and serves pushed events.

    Attributes:
        holds_definition: Whether the definition's session.updated waits for ``send_definition()``.
        rejects_updates: Whether a session.update gets an error instead of its session.updated.
    """

    def __init__(self, definition: Mapping[str, Any]) -> None:
        self.session = session_from(definition)
        self.holds_definition = False
        self.rejects_updates = False
        self.flattens_voices = False
        self.sent: list[dict[str, Any]] = []
        self.closed = False
        self._incoming: asyncio.Queue[Any] = asyncio.Queue()

    def open(self) -> None:
        self.push({"type": "session.created", "event_id": "evt_created", "session": {"type": "realtime"}})
        if not self.holds_definition:
            self.send_definition()

    def send_definition(self) -> None:
        self.push({"type": "session.updated", "event_id": "evt_definition", "session": dict(self.session)})

    def push(self, *events: Any) -> None:
        for event in events:
            self._incoming.put_nowait(event)

    async def send(self, event: Any) -> None:
        if self.closed:
            raise ConnectionResetError("Cannot write to closing transport")
        recorded = json.loads(json.dumps(event))
        self.sent.append(recorded)
        if recorded["type"] == "session.update":
            if self.rejects_updates:
                self.push({"type": "error", "error": {"type": "invalid_request_error", "message": "Rejected."}})
            else:
                self.session = {**self.session, **self._merged(recorded["session"])}
                self.push({"type": "session.updated", "event_id": "evt_updated", "session": dict(self.session)})

    def _merged(self, update: dict[str, Any]) -> dict[str, Any]:
        """A voice change replaces the whole voice; other audio settings merge into the session's."""
        audio = {direction: dict(settings) for direction, settings in self.session.get("audio", {}).items()}
        for direction, settings in update.get("audio", {}).items():
            current = audio.setdefault(direction, {})
            for key, value in settings.items():
                if key == "voice":
                    for stored in list(current):
                        if stored.startswith("voice") or stored in FLATTENED_VOICE_FIELDS.values():
                            del current[stored]
                    current.update(flattened(value) if self.flattens_voices else {"voice": value})
                else:
                    current[key] = value
        return {**update, "audio": audio} if "audio" in update else update

    async def recv(self) -> Any:
        received = await self._incoming.get()
        if isinstance(received, BaseException):
            raise received
        return received

    async def close(self) -> None:
        self.closed = True
        self.push(ConnectionResetError("The realtime connection was closed."))

    async def wait_sent(self, event_type: str, timeout: float = 5.0) -> dict[str, Any]:
        """Wait for the next event of ``event_type`` sent after this call, and return it."""
        already_sent = len(self.sent)

        async def poll() -> dict[str, Any]:
            while True:
                for event in self.sent[already_sent:]:
                    if event["type"] == event_type:
                        return event
                await asyncio.sleep(0.005)

        return await asyncio.wait_for(poll(), timeout)

    def answer(self, request: Mapping[str, Any]) -> None:
        """Create and finish the response ``request`` asked for."""
        response_id = f"resp_{len(self.sent)}"
        metadata = request["response"].get("metadata")
        self.push(
            {"type": "response.created", "response": {"id": response_id, "metadata": metadata}},
            {"type": "response.done", "response": {"id": response_id, "status": "completed"}},
        )

    def updates(self) -> list[dict[str, Any]]:
        """The sessions of the session.update events sent so far."""
        return [event["session"] for event in self.sent if event["type"] == "session.update"]


async def until(condition: Any, timeout: float = 5.0) -> None:
    """Wait until ``condition()`` holds."""

    async def poll() -> None:
        while not condition():  # noqa: ASYNC110 - the condition reads state another task updates
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), timeout)


async def read_all(session: Any) -> list[Any]:
    """Every realtime event the session yields until its connection ends."""
    return [event async for event in session.receive()]


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


class FakeConnectionManager:
    def __init__(self, server: FakeVoiceAgentServer, handshake_error: BaseException | None) -> None:
        self._server = server
        self._handshake_error = handshake_error

    async def __aenter__(self) -> FakeVoiceAgentServer:
        if self._handshake_error is not None:
            raise self._handshake_error
        self._server.open()
        return self._server

    async def __aexit__(self, *exc_details: Any) -> None:
        await self._server.close()


class FakeProjectClient:
    """An AIProjectClient whose agents API serves ``versions`` and whose voice-agent connect opens fake servers."""

    def __init__(self, versions: Mapping[str, Mapping[str, Any]] | None = None) -> None:
        self.agents = FakeAgents(versions or {"3": DEFINITION})
        self.connect_calls: list[dict[str, Any]] = []
        self.servers: list[FakeVoiceAgentServer] = []
        self.holds_definition = False
        self.rejects_updates = False
        self.handshake_error: BaseException | None = None
        self.session_definition: Mapping[str, Any] | None = None
        self.closed = False
        realtime = type("Realtime", (), {"connect": self._connect})()
        self.beta = type("Beta", (), {"voice_agents": type("VoiceAgents", (), {"realtime": realtime})()})()

    @property
    def server(self) -> FakeVoiceAgentServer:
        """The most recently opened socket."""
        return self.servers[-1]

    def as_project_client(self) -> AIProjectClient:
        return cast(AIProjectClient, self)

    def _connect(self, **kwargs: Any) -> FakeConnectionManager:
        self.agents.raise_if_closed()
        self.connect_calls.append(kwargs)
        version = kwargs.get("extra_headers", {}).get("x-agent-version-override")
        definition = (
            self.session_definition or self.agents.versions.get(version) or list(self.agents.versions.values())[-1]
        )
        server = FakeVoiceAgentServer(definition)
        server.holds_definition = self.holds_definition
        server.rejects_updates = self.rejects_updates
        self.servers.append(server)
        return FakeConnectionManager(server, self.handshake_error)

    async def close(self) -> None:
        self.closed = True
        self.agents.closed = True


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep a developer's or CI's real FOUNDRY_* settings out of the unit tests."""
    for name in ("PROJECT_ENDPOINT", "AGENT_NAME", "AGENT_VERSION"):
        monkeypatch.delenv(f"FOUNDRY_{name}", raising=False)


# region Construction and settings


def test_the_client_reads_the_foundry_project_and_agent_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """With FOUNDRY_PROJECT_ENDPOINT, FOUNDRY_AGENT_NAME and FOUNDRY_AGENT_VERSION set, a client built with only a
    credential names agent 'receptionist' at version '3'."""
    monkeypatch.setenv("FOUNDRY_PROJECT_ENDPOINT", ENDPOINT)
    monkeypatch.setenv("FOUNDRY_AGENT_NAME", "receptionist")
    monkeypatch.setenv("FOUNDRY_AGENT_VERSION", "3")

    client = FoundryVoiceAgentClient(credential=StaticTokenCredential())

    assert (client.agent_name, client.agent_version) == ("receptionist", "3")


def test_a_missing_agent_name_raises_value_error() -> None:
    """Building a client with no agent_name and no FOUNDRY_AGENT_NAME raises ValueError."""
    with pytest.raises(ValueError, match="FOUNDRY_AGENT_NAME"):
        FoundryVoiceAgentClient(project_endpoint=ENDPOINT, credential=StaticTokenCredential())


@pytest.mark.parametrize(
    "access",
    [{}, {"project_endpoint": ENDPOINT}],
    ids=["no endpoint or project client", "endpoint without credential"],
)
def test_missing_project_access_raises_value_error(access: dict[str, Any]) -> None:
    """Parametrized: no project_endpoint and no project_client raises ValueError; a project_endpoint without a
    credential raises ValueError."""
    with pytest.raises(ValueError, match="project_endpoint|credential"):
        FoundryVoiceAgentClient(agent_name="receptionist", **access)


def test_a_projects_sdk_without_voice_agents_raises_the_realtime_extra_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the installed azure-ai-projects has no realtime voice-agent module, as before 2.7, building a client
    raises ModuleNotFoundError whose message names 'agent-framework-foundry[realtime]'."""
    real_find_spec = importlib.util.find_spec

    def find_spec_without_voice_agents(name: str, *args: Any, **kwargs: Any) -> Any:
        return None if name == "azure.ai.projects.aio._realtime" else real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", find_spec_without_voice_agents)

    with pytest.raises(ModuleNotFoundError, match=re.escape("agent-framework-foundry[realtime]")):
        FoundryVoiceAgentClient(
            project_endpoint=ENDPOINT, agent_name="receptionist", credential=StaticTokenCredential()
        )


# endregion

# region The definition


async def test_starting_a_session_before_entering_raises_runtime_error_and_connects_nothing() -> None:
    """start_session() on a client that hasn't been entered raises RuntimeError mentioning 'async with', and the
    voice-agent connect is never called."""
    project = FakeProjectClient()
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )

    with pytest.raises(RuntimeError, match="async with"):
        await client.start_session()

    assert project.connect_calls == []


async def test_entering_reads_the_pinned_versions_definition() -> None:
    """Entering a client pinned to version '3' reads agents.get_version('receptionist', '3'), and client.definition
    holds that version's instructions."""
    project = FakeProjectClient({"3": DEFINITION, "4": {**DEFINITION, "instructions": "Newer."}})
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )

    async with client:
        pass

    assert project.agents.calls == [("get_version", "receptionist", "3")]
    assert client.definition is not None
    assert client.definition["instructions"] == "You are a receptionist."


async def test_entering_without_a_version_reads_the_latest_and_pins_sessions_to_it() -> None:
    """Entering a client with no version reads agents.get('receptionist'), takes the latest version's definition,
    and client.agent_version becomes '7'."""
    project = FakeProjectClient({"6": {**DEFINITION, "instructions": "Older."}, "7": DEFINITION})
    client = FoundryVoiceAgentClient(project_client=project.as_project_client(), agent_name="receptionist")

    async with client:
        pass

    assert project.agents.calls == [("get", "receptionist")]
    assert client.agent_version == "7"
    assert client.definition is not None
    assert client.definition["instructions"] == "You are a receptionist."


async def test_the_definition_is_read_only() -> None:
    """Assigning to a key of client.definition after entering raises TypeError."""
    client = FoundryVoiceAgentClient(
        project_client=FakeProjectClient().as_project_client(), agent_name="receptionist", agent_version="3"
    )

    async with client:
        with pytest.raises(TypeError):
            cast(dict[str, Any], client.definition)["instructions"] = "Something else."


@pytest.mark.parametrize("passed_in", [False, True], ids=["created", "passed in"])
async def test_leaving_closes_a_project_client_it_created_but_never_one_passed_in(
    passed_in: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Parametrized: a client built from an endpoint and credential closes the AIProjectClient it created when it
    exits; a client given project_client leaves it open."""
    created: list[FakeProjectClient] = []

    def create_project_client(**kwargs: Any) -> FakeProjectClient:
        created.append(FakeProjectClient())
        return created[-1]

    monkeypatch.setattr(voice_client_module, "AIProjectClient", create_project_client)
    passed = FakeProjectClient()
    if passed_in:
        client = FoundryVoiceAgentClient(
            project_client=passed.as_project_client(), agent_name="receptionist", agent_version="3"
        )
    else:
        client = FoundryVoiceAgentClient(
            project_endpoint=ENDPOINT, credential=StaticTokenCredential(), agent_name="receptionist", agent_version="3"
        )

    async with client:
        pass

    [project] = [passed] if passed_in else created
    assert project.closed is not passed_in


async def test_a_client_can_be_entered_again_after_it_exits_and_keeps_its_definition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A client built from an endpoint and credential, entered, exited and entered again, starts a session on a project
    client opened for that entry, and reads its definition only on the first entry."""
    created: list[FakeProjectClient] = []

    def create_project_client(**kwargs: Any) -> FakeProjectClient:
        created.append(FakeProjectClient())
        return created[-1]

    monkeypatch.setattr(voice_client_module, "AIProjectClient", create_project_client)
    client = FoundryVoiceAgentClient(
        project_endpoint=ENDPOINT, credential=StaticTokenCredential(), agent_name="receptionist", agent_version="3"
    )

    async with client:
        pass
    async with client:
        session = await asyncio.wait_for(client.start_session(), 5)
        await session.close()

    assert [project.agents.calls for project in created] == [[("get_version", "receptionist", "3")], []]
    assert created[-1].connect_calls


async def test_a_failed_definition_read_closes_the_project_client_it_created(monkeypatch: pytest.MonkeyPatch) -> None:
    """A client built from an endpoint and credential, entered with a version the agent doesn't have, raises the
    lookup's error and closes the AIProjectClient it created."""
    created: list[FakeProjectClient] = []

    def create_project_client(**kwargs: Any) -> FakeProjectClient:
        created.append(FakeProjectClient())
        return created[-1]

    monkeypatch.setattr(voice_client_module, "AIProjectClient", create_project_client)
    client = FoundryVoiceAgentClient(
        project_endpoint=ENDPOINT, credential=StaticTokenCredential(), agent_name="receptionist", agent_version="9"
    )

    with pytest.raises(KeyError):
        async with client:
            pass

    [project] = created
    assert project.closed


# endregion

# region The handshake


async def test_the_handshake_names_the_agent_and_pins_its_version() -> None:
    """start_session() connects with agent_name='receptionist', api_version='v1' and the extra header
    x-agent-version-override: '3', and passes no agent_version keyword."""
    project = FakeProjectClient()
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )

    async with client:
        session = await asyncio.wait_for(client.start_session(), 5)
        await session.close()

    call = project.connect_calls[0]
    assert (call["agent_name"], call["api_version"]) == ("receptionist", "v1")
    assert call["extra_headers"]["x-agent-version-override"] == "3"
    assert "agent_version" not in call


async def test_the_handshake_carries_the_agent_framework_user_agent() -> None:
    """The extra headers carry a User-Agent that starts with Agent Framework's user agent."""
    project = FakeProjectClient()
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )

    async with client:
        session = await asyncio.wait_for(client.start_session(), 5)
        await session.close()

    assert project.connect_calls[0]["extra_headers"]["User-Agent"].startswith(get_user_agent())


async def test_the_handshake_carries_the_current_trace_context_and_no_baggage() -> None:
    """Inside a current span with baggage set, the extra headers carry a traceparent naming that span's trace and
    span ids, and its tracestate, but no baggage header."""
    project = FakeProjectClient()
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )
    span = trace.NonRecordingSpan(
        trace.SpanContext(
            trace_id=0x0AF7651916CD43DD8448EB211C80319C,
            span_id=0xB7AD6B7169203331,
            is_remote=False,
            trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
            trace_state=trace.TraceState([("contoso", "call-centre")]),
        )
    )
    token = context.attach(baggage.set_baggage("caller", "alice"))
    try:
        async with client:
            with trace.use_span(span):
                session = await asyncio.wait_for(client.start_session(), 5)
            await session.close()
    finally:
        context.detach(token)

    headers = {name.lower(): value for name, value in project.connect_calls[0]["extra_headers"].items()}
    assert headers["traceparent"] == "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    assert headers["tracestate"] == "contoso=call-centre"
    assert "baggage" not in headers


# endregion

# region The session


async def test_the_session_sends_nothing_until_the_definition_arrives() -> None:
    """After connecting, the server sends session.created and holds the definition back: start_session() sends
    nothing. Once the definition's session.updated arrives, the session.update goes out and start_session() returns."""
    project = FakeProjectClient()
    project.holds_definition = True
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )

    async with client:
        starting = asyncio.ensure_future(client.start_session(options={"instructions": "Be brief."}))
        await until(lambda: bool(project.servers))
        await asyncio.sleep(0.05)
        sent_before_the_definition = list(project.server.sent)
        project.server.send_definition()
        session = await asyncio.wait_for(starting, 5)
        await session.close()

    assert sent_before_the_definition == []
    assert [event["type"] for event in project.server.sent] == ["session.update"]


async def test_events_arriving_as_sdk_models_are_read_as_their_wire_json() -> None:
    """A server event the SDK returns as a typed model streams as the realtime event kind its JSON maps to, with
    plain, JSON-serializable wire JSON as its raw representation."""
    project = FakeProjectClient()
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )

    async with client:
        session = await asyncio.wait_for(client.start_session(), 5)
        async with reading(session) as events:
            project.server.push(
                RealtimeServerEventResponseCreated({
                    "type": "response.created",
                    "event_id": "evt_1",
                    "response": {"id": "resp_1", "metadata": None},
                })
            )
            await until(lambda: any(event.kind == "response_created" for event in events))
        await session.close()

    created = next(event for event in events if event.kind == "response_created")
    assert json.loads(json.dumps(created.raw_representation))["response"]["id"] == "resp_1"


async def test_a_graceful_close_ends_receive() -> None:
    """When recv() raises ConnectionResetError with no cause, as the SDK does for a normal close, receive() ends
    without raising."""
    project = FakeProjectClient()
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )

    async with client:
        session = await asyncio.wait_for(client.start_session(), 5)
        project.server.push(ConnectionResetError("The realtime connection was closed."))
        events = await asyncio.wait_for(read_all(session), 5)
        await session.close()

    assert [event.kind for event in events] == ["session_created", "session_updated"]


async def test_an_abnormal_close_raises_realtime_connection_exception() -> None:
    """When recv() raises ConnectionResetError caused by an abnormal close, receive() raises
    RealtimeConnectionException."""
    project = FakeProjectClient()
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )
    abnormal = ConnectionResetError("The realtime connection was closed abnormally (code 1011).")
    abnormal.__cause__ = RuntimeError(1011)

    async with client:
        session = await asyncio.wait_for(client.start_session(), 5)
        project.server.push(abnormal)
        with pytest.raises(RealtimeConnectionException):
            await asyncio.wait_for(read_all(session), 5)
        await session.close()


async def test_sending_on_a_dead_socket_raises_realtime_connection_exception() -> None:
    """When the SDK connection's send() raises ConnectionResetError, the session's send() raises
    RealtimeConnectionException."""
    project = FakeProjectClient()
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )

    async with client:
        session = await asyncio.wait_for(client.start_session(), 5)
        project.server.closed = True
        with pytest.raises(RealtimeConnectionException):
            await session.send(Message("user", ["Hello"]))
        await session.close()


async def test_a_failed_handshake_raises_realtime_connection_exception() -> None:
    """When entering the SDK's connection manager raises ConnectionError, start_session() raises
    RealtimeConnectionException."""
    project = FakeProjectClient()
    project.handshake_error = ConnectionError("Failed to open the realtime WebSocket connection: 401")
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )

    async with client:
        with pytest.raises(RealtimeConnectionException, match="401"):
            await asyncio.wait_for(client.start_session(), 5)


async def test_a_rejected_start_closes_the_socket() -> None:
    """When the server answers the start's session.update with an error, start_session() raises
    RealtimeControlException and the SDK connection is closed."""
    project = FakeProjectClient()
    project.rejects_updates = True
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )

    async with client:
        with pytest.raises(RealtimeControlException, match="Rejected"):
            await asyncio.wait_for(client.start_session(options={"instructions": "Be brief."}), 5)

    assert project.server.closed


async def test_conversation_item_created_maps_to_item_added() -> None:
    """A conversation.item.created event streams as item_added with its item id, and keeps
    'conversation.item.created' as its service_event_type."""
    project = FakeProjectClient()
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )

    async with client:
        session = await asyncio.wait_for(client.start_session(), 5)
        async with reading(session) as events:
            project.server.push({
                "type": "conversation.item.created",
                "previous_item_id": None,
                "item": {"id": "item_1", "type": "message", "role": "user", "content": [{"type": "input_text"}]},
            })
            await until(lambda: len(events) == 3)
        await session.close()

    added = events[-1]
    assert (added.kind, added.service_event_type) == ("item_added", "conversation.item.created")
    assert added.additional_properties["core"]["item_id"] == "item_1"


AVA: AzureVoice = {"name": "en-US-AvaMultilingualNeural", "type": "azure-standard", "temperature": 0.8}
"""The test definition's voice, as the agent restores it."""
SONIA: AzureVoice = {"type": "azure-standard", "name": "en-GB-SoniaNeural"}


async def start(project: FakeProjectClient, **options: Any) -> Any:
    """A session of an entered client for agent 'receptionist' version '3', started with ``options``.

    The client stays entered: its project client is a fake that needs no closing.
    """
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )
    await client.__aenter__()
    return await asyncio.wait_for(client.start_session(options=cast(FoundryVoiceAgentOptions, options)), 5)


async def test_an_azure_voice_goes_in_audio_output_voice() -> None:
    """Starting with voice {'type': 'azure-standard', 'name': 'en-GB-SoniaNeural'} sends that object as
    session.audio.output.voice."""
    project = FakeProjectClient()

    session = await start(project, voice=SONIA)
    await session.close()

    assert project.server.updates()[0]["audio"]["output"]["voice"] == SONIA


async def test_echo_cancellation_goes_in_audio_input() -> None:
    """Starting with echo_cancellation {'type': 'server_echo_cancellation'} sends it as
    session.audio.input.echo_cancellation."""
    project = FakeProjectClient()

    session = await start(project, echo_cancellation={"type": "server_echo_cancellation"})
    await session.close()

    assert project.server.updates()[0]["audio"]["input"]["echo_cancellation"] == {"type": "server_echo_cancellation"}


async def test_reasoning_effort_goes_in_session_reasoning() -> None:
    """Starting with reasoning_effort 'low' sends session.reasoning {'effort': 'low'}."""
    project = FakeProjectClient()

    session = await start(project, instructions="Be brief.", reasoning_effort="low")
    await session.close()

    assert project.server.updates()[0].get("reasoning") == {"effort": "low"}


RYAN: AzureVoice = {"type": "azure-standard", "name": "en-GB-RyanNeural"}


def output_audio_delta(response_id: str = "resp_1") -> dict[str, Any]:
    return {
        "type": "response.output_audio.delta",
        "response_id": response_id,
        "item_id": "item_audio",
        "output_index": 0,
        "content_index": 0,
        "delta": base64.b64encode(b"\x00\x00" * 240).decode(),
    }


async def test_a_change_between_azure_voices_is_sent_after_output_audio() -> None:
    """After output audio has streamed, update_session() with another Azure voice sends it, where the GA protocol
    would drop it."""
    project = FakeProjectClient()
    session = await start(project)

    async with reading(session) as events:
        project.server.push(output_audio_delta())
        await until(lambda: any(event.kind == "output_audio" for event in events))
        await asyncio.wait_for(session.update_session({"voice": RYAN}, None), 5)
    await session.close()

    assert project.server.updates()[-1]["audio"]["output"]["voice"] == RYAN


async def test_a_change_from_an_azure_voice_to_an_openai_voice_is_dropped() -> None:
    """With an Azure voice live, update_session() with voice 'marin' sends no voice."""
    project = FakeProjectClient()
    session = await start(project)

    async with reading(session):
        await asyncio.wait_for(session.update_session({"voice": "marin", "instructions": "Be brief."}, None), 5)
    await session.close()

    assert "voice" not in project.server.updates()[-1].get("audio", {}).get("output", {})


async def test_a_flattened_echo_of_every_requested_voice_field_counts_as_the_voice_change() -> None:
    """update_session() with voice {'type': 'azure-standard', 'name': 'en-GB-SoniaNeural', 'temperature': 0.5},
    echoed as voice, voice_type and voice_temperature, leaves session.options['voice'] the requested voice."""
    project = FakeProjectClient()
    session = await start(project)
    project.server.flattens_voices = True

    async with reading(session):
        await asyncio.wait_for(session.update_session({"voice": {**SONIA, "temperature": 0.5}}, None), 5)
    await session.close()

    assert project.server.session["audio"]["output"]["voice_temperature"] == 0.5
    assert session.options["voice"] == {**SONIA, "temperature": 0.5}


async def test_the_sessions_options_report_the_definitions_transcription() -> None:
    """After starting without a transcription option, session.options['transcription'] is the definition's
    {'model': 'gpt-4o-transcribe'}."""
    session = await start(FakeProjectClient())
    await session.close()

    assert session.options["transcription"] == {"model": "gpt-4o-transcribe"}


async def test_a_definitions_g711_output_labels_output_audio_as_audio_pcmu() -> None:
    """With the definition's audio.output.format {'type': 'audio/pcmu'} and no output format option, output audio
    streams as data content with media type 'audio/pcmu'."""
    g711 = json.loads(json.dumps(DEFINITION))
    g711["audio"]["output"]["format"] = {"type": "audio/pcmu"}
    project = FakeProjectClient({"3": g711})
    session = await start(project)

    async with reading(session) as events:
        project.server.push({"type": "response.created", "response": {"id": "resp_1", "metadata": None}})
        project.server.push(output_audio_delta())
        await until(lambda: any(event.kind == "output_audio" for event in events))
    await session.close()

    audio = next(event for event in events if event.kind == "output_audio")
    assert audio.content.media_type == "audio/pcmu"


# endregion

# region The definition's tools


@tool(name="lookup_order", description="Look up an order.")
def lookup_order(order_id: str) -> str:
    return "Shipped"


async def test_the_definition_stand_in_declares_the_definitions_tools_as_published() -> None:
    """With another tool live, update_session() with the definition stand-in alone declares exactly the definition's
    tools, in order and as published: a function with response_scheduling and a hosted MCP entry."""
    project = FakeProjectClient()
    session = await start(project)

    async with reading(session):
        await asyncio.wait_for(session.update_session({}, [lookup_order]), 5)
        await asyncio.wait_for(session.update_session({}, [DEFINITION_TOOLS]), 5)
    await session.close()

    assert project.server.updates()[-1]["tools"] == [GET_CURRENT_TIME, GET_OFFICE_HOURS, ORDERS_MCP]


@tool(name="get_current_time", description="Tells the time in the caller's time zone.")
def get_current_time(time_zone: str = "UTC") -> str:
    return "12:00"


async def test_a_function_tool_the_definition_names_is_declared_as_published() -> None:
    """With another tool live, update_session() with the stand-in and a function tool 'get_current_time' whose own
    description differs declares the definition's published 'get_current_time' once, not the tool's own schema."""
    project = FakeProjectClient()
    session = await start(project)

    async with reading(session):
        await asyncio.wait_for(session.update_session({}, [lookup_order]), 5)
        await asyncio.wait_for(session.update_session({}, [DEFINITION_TOOLS, get_current_time]), 5)
    await session.close()

    assert project.server.updates()[-1]["tools"] == [GET_CURRENT_TIME, GET_OFFICE_HOURS, ORDERS_MCP]


@tool(name="get_office_hours", description="Get the office hours.")
def get_office_hours() -> str:
    return "9 to 5"


async def start_with_tools(project: FakeProjectClient, tools: list[Any]) -> Any:
    client = FoundryVoiceAgentClient(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )
    await client.__aenter__()
    return await asyncio.wait_for(client.start_session(options={"instructions": "Be brief."}, tools=tools), 5)


async def test_the_start_sends_no_tools_key_when_nothing_is_added() -> None:
    """Starting with the stand-in and implementations of every definition function sends a session.update with no
    'tools' key."""
    project = FakeProjectClient()

    session = await start_with_tools(project, [DEFINITION_TOOLS, get_current_time, get_office_hours])
    await session.close()

    assert "tools" not in project.server.updates()[0]


async def test_a_definition_whose_hosted_mcp_tool_may_ask_for_approval_is_refused_before_connecting() -> None:
    """Entering a conversation of an agent whose definition publishes hosted MCP tool 'orders' with require_approval
    'always' raises ValueError naming 'orders', require_approval and Foundry, and nothing connects."""
    asking = {**ORDERS_MCP, "require_approval": "always"}
    project = FakeProjectClient({"3": {**DEFINITION, "tools": [GET_CURRENT_TIME, asking]}})
    agent = FoundryVoiceAgent(project_client=project.as_project_client(), agent_name="receptionist", agent_version="3")

    async with agent:
        with pytest.raises(ValueError, match="'orders'.*require_approval.*Foundry"):
            async with agent.start_conversation():
                pass

    assert project.connect_calls == []


# endregion


# region Settings from the definition


async def test_entering_the_agent_takes_the_definitions_instructions() -> None:
    """After entering, agent.instructions are the definition's 'You are a receptionist.'"""
    agent = FoundryVoiceAgent(
        project_client=FakeProjectClient().as_project_client(), agent_name="receptionist", agent_version="3"
    )

    async with agent:
        assert agent.instructions == "You are a receptionist."


STORED_VOICE: dict[str, Any] = {
    "voice": "en-GB-SoniaNeural",
    "voice_type": "azure-custom",
    "voice_temperature": 0.6,
    "custom_voice_endpoint_id": "endpoint-1",
    "personal_voice_model": "DragonLatestNeural",
    "custom_lexicon_url": "https://contoso.example/lexicon.xml",
    "custom_text_normalization_url": "https://contoso.example/normalization.txt",
    "prefer_locales": ["en-GB"],
    "voice_locale": "en-GB",
    "style": "cheerful",
    "pitch": "+5%",
    "volume": "loud",
}
AZURE_VOICE: dict[str, Any] = {
    "name": "en-GB-SoniaNeural",
    "type": "azure-custom",
    "temperature": 0.6,
    "endpoint_id": "endpoint-1",
    "model": "DragonLatestNeural",
    "custom_lexicon_url": "https://contoso.example/lexicon.xml",
    "custom_text_normalization_url": "https://contoso.example/normalization.txt",
    "prefer_locales": ["en-GB"],
    "locale": "en-GB",
    "style": "cheerful",
    "pitch": "+5%",
    "volume": "loud",
}


@pytest.mark.parametrize(
    ("stored", "expected"),
    [
        (STORED_VOICE, AZURE_VOICE),
        (
            {**STORED_VOICE, "voice_temperature": None},
            {field: value for field, value in AZURE_VOICE.items() if field != "temperature"},
        ),
    ],
    ids=["every field", "a null field"],
)
async def test_entering_the_agent_takes_the_definitions_voice_with_every_stored_field(
    stored: dict[str, Any], expected: dict[str, Any]
) -> None:
    """With the definition's audio.output holding voice, voice_type, voice_temperature, custom_voice_endpoint_id,
    personal_voice_model, custom_lexicon_url, custom_text_normalization_url, prefer_locales, voice_locale, style,
    pitch and volume, the default voice after entering is the AzureVoice with name, type, temperature, endpoint_id,
    model, custom_lexicon_url, custom_text_normalization_url, prefer_locales, locale, style, pitch and volume. A null
    field is left out."""
    project = FakeProjectClient({"3": with_audio_output(**stored)})
    agent = FoundryVoiceAgent(project_client=project.as_project_client(), agent_name="receptionist", agent_version="3")

    async with agent:
        assert agent.default_options["voice"] == expected


async def test_entering_the_agent_takes_the_definitions_tool_choice() -> None:
    """With the definition's tool_choice 'required', the default tool_choice after entering is 'required'."""
    project = FakeProjectClient({"3": {**DEFINITION, "tool_choice": "required"}})
    agent = FoundryVoiceAgent(project_client=project.as_project_client(), agent_name="receptionist", agent_version="3")

    async with agent:
        assert agent.default_options["tool_choice"] == "required"


async def test_a_published_function_tool_choice_becomes_a_required_function() -> None:
    """With the definition's tool_choice {'type': 'function', 'name': 'get_current_time'}, the default tool_choice
    after entering requires 'get_current_time'."""
    published = {**DEFINITION, "tool_choice": {"type": "function", "name": "get_current_time"}}
    agent = FoundryVoiceAgent(
        project_client=FakeProjectClient({"3": published}).as_project_client(),
        agent_name="receptionist",
        agent_version="3",
    )

    async with agent:
        assert agent.default_options["tool_choice"] == {
            "mode": "required",
            "required_function_name": "get_current_time",
        }


async def test_the_apps_default_voice_and_tool_choice_win_over_the_definitions() -> None:
    """An agent built with default_options voice 'en-GB-SoniaNeural' (Azure) and tool_choice 'none' keeps them after
    entering, though the definition sets others."""
    agent = FoundryVoiceAgent(
        project_client=FakeProjectClient().as_project_client(),
        agent_name="receptionist",
        agent_version="3",
        default_options={"voice": SONIA, "tool_choice": "none"},
    )

    async with agent:
        assert (agent.default_options["voice"], agent.default_options["tool_choice"]) == (SONIA, "none")


async def test_the_apps_default_instructions_win_over_the_definitions() -> None:
    """An agent built with default_options instructions 'Always speak French.' keeps them after entering, though the
    definition sets others."""
    agent = FoundryVoiceAgent(
        project_client=FakeProjectClient().as_project_client(),
        agent_name="receptionist",
        agent_version="3",
        default_options={"instructions": "Always speak French."},
    )

    async with agent:
        assert agent.instructions == "Always speak French."


# endregion


def topology(
    project: FakeProjectClient, *, billing_options: FoundryVoiceAgentOptions | None = None, **builder_settings: Any
) -> tuple[FoundryVoiceAgent, RealtimeAgent[FoundryVoiceAgentOptions], RealtimeAgent[FoundryVoiceAgentOptions]]:
    """The Foundry receptionist, a billing agent on its client, and the handoff agent built from them."""
    receptionist = FoundryVoiceAgent(
        project_client=project.as_project_client(),
        agent_name="receptionist",
        agent_version="3",
        tools=[get_current_time],
    )
    billing = RealtimeAgent(
        receptionist.client,
        name="billing",
        description="Answers billing questions.",
        instructions="You handle billing.",
        tools=[lookup_order],
        default_options=billing_options or FoundryVoiceAgentOptions(voice=SONIA),
    )
    built = (
        RealtimeHandoffBuilder(participants=[receptionist, billing], **builder_settings)
        .with_start_agent(receptionist)
        .build()
    )
    return receptionist, billing, built


def tool_names(session: Mapping[str, Any]) -> list[str | None]:
    return [declared.get("name") for declared in session["tools"]]


REASONING_DEFINITION: dict[str, Any] = {**DEFINITION, "reasoning": {"effort": "high"}}


# region Handoffs


@pytest.mark.parametrize(("greeter_tool_choice", "expected"), [(None, "auto"), ("none", "none")], ids=["unset", "set"])
async def test_a_plain_start_agent_does_not_inherit_the_definitions_tool_choice(
    greeter_tool_choice: Literal["none"] | None, expected: str
) -> None:
    """With a definition whose tool_choice requires get_current_time and a plain start agent on foundry_agent.client,
    the start's session update sends the start agent's tool choice, or 'auto' when it sets none."""
    published = {**DEFINITION, "tool_choice": {"type": "function", "name": "get_current_time"}}
    project = FakeProjectClient({"3": published})
    receptionist = FoundryVoiceAgent(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
    )
    greeter_options = FoundryVoiceAgentOptions(voice=SONIA)
    if greeter_tool_choice is not None:
        greeter_options["tool_choice"] = greeter_tool_choice
    greeter = RealtimeAgent(
        receptionist.client, name="greeter", instructions="Greet the caller.", default_options=greeter_options
    )
    built = (
        RealtimeHandoffBuilder[FoundryVoiceAgentOptions](participants=[greeter, receptionist])
        .with_start_agent(greeter)
        .build()
    )

    async with built, built.start_conversation():
        pass

    assert project.server.updates()[0]["tool_choice"] == expected


async def test_handing_back_restores_the_definitions_reasoning_effort() -> None:
    """With a definition whose reasoning effort is 'high' and billing's own 'low', the handoff to billing sends
    reasoning.effort 'low', and handing back sends 'high'."""
    project = FakeProjectClient({"3": REASONING_DEFINITION})
    receptionist, billing, built = topology(
        project, billing_options=FoundryVoiceAgentOptions(voice=SONIA, reasoning_effort="low")
    )

    async with built, built.start_conversation() as conversation:
        await asyncio.wait_for(conversation.handoff(billing), 5)
        await asyncio.wait_for(conversation.handoff(receptionist), 5)

    *_, to_billing, handed_back = project.server.updates()
    assert (to_billing.get("reasoning"), handed_back.get("reasoning")) == ({"effort": "low"}, {"effort": "high"})


# endregion

# region Telemetry


async def answer_one_text_input(project: FakeProjectClient, conversation: Any) -> None:
    """Send "Hi", let the server answer it, and read updates until that response is done."""
    request = asyncio.ensure_future(project.server.wait_sent("response.create"))
    sending = asyncio.ensure_future(conversation.send("Hi"))
    project.server.answer(await request)
    await asyncio.wait_for(sending, 5)
    async with asyncio.timeout(5):
        async for update in conversation:
            if any(content.kind == "response_done" for content in update.contents):
                return


async def test_a_foundry_voice_agents_conversation_is_traced_with_the_foundry_provider_name(
    span_exporter: InMemorySpanExporter,
) -> None:
    """A FoundryVoiceAgent named "receptionist" answers one text input: an "invoke_agent receptionist" span, and
    under it the response's realtime_inference span with gen_ai.provider.name "azure.ai.foundry"."""
    project = FakeProjectClient()
    agent = FoundryVoiceAgent(project_client=project.as_project_client(), agent_name="receptionist", agent_version="3")

    async with agent, agent.start_conversation() as conversation:
        await answer_one_text_input(project, conversation)

    spans = span_exporter.get_finished_spans()
    [conversation_span] = [span for span in spans if span.name == "invoke_agent receptionist"]
    [response_span] = [span for span in spans if span.name.startswith("realtime_inference")]
    assert response_span.parent is not None
    assert conversation_span.context is not None
    assert response_span.parent.span_id == conversation_span.context.span_id
    assert (response_span.attributes or {}).get("gen_ai.provider.name") == "azure.ai.foundry"


# endregion

# region Conversation middleware


async def test_a_foundry_voice_agent_runs_conversation_middleware() -> None:
    """A FoundryVoiceAgent with a ConversationMiddleware runs its process() once when a conversation opens."""
    project = FakeProjectClient()
    opened: list[bool] = []

    async def audit(context: ConversationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        await call_next()
        opened.append(context.conversation is not None)

    agent = FoundryVoiceAgent(
        project_client=project.as_project_client(), agent_name="receptionist", agent_version="3", middleware=[audit]
    )

    async with agent, agent.start_conversation():
        assert opened == [True]


# endregion


class TestVerification:
    async def test_the_handshake_carries_the_conversation_spans_trace_context(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        """A FoundryVoiceAgent conversation's handshake traceparent names the invoke_agent span's trace and span
        ids."""
        project = FakeProjectClient()
        agent = FoundryVoiceAgent(
            project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
        )

        async with agent, agent.start_conversation():
            pass

        [conversation_span] = [
            span for span in span_exporter.get_finished_spans() if span.name == "invoke_agent receptionist"
        ]
        headers = {name.lower(): value for name, value in project.connect_calls[0]["extra_headers"].items()}
        _, trace_id, span_id, _ = headers["traceparent"].split("-")
        assert conversation_span.context is not None
        assert (int(trace_id, 16), int(span_id, 16)) == (
            conversation_span.context.trace_id,
            conversation_span.context.span_id,
        )

    async def test_an_openai_voice_in_the_definition_stays_a_name(self) -> None:
        """With the definition's voice 'marin' and voice_type 'openai', the default voice after entering is 'marin'."""
        project = FakeProjectClient({"3": with_audio_output(voice="marin", voice_type="openai")})
        agent = FoundryVoiceAgent(
            project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
        )

        async with agent:
            assert agent.default_options["voice"] == "marin"

    async def test_the_start_sends_tools_when_the_sessions_definition_declares_other_tools(self) -> None:
        """When the session's definition declares tools other than those in the definition read on entry, as an
        unpinned session would, the start's session.update carries the read definition's tools."""
        project = FakeProjectClient()
        project.session_definition = {**DEFINITION, "tools": [GET_CURRENT_TIME]}

        session = await start_with_tools(project, [DEFINITION_TOOLS, get_current_time, get_office_hours])
        await session.close()

        assert project.server.updates()[0]["tools"] == [GET_CURRENT_TIME, GET_OFFICE_HOURS, ORDERS_MCP]

    async def test_function_tools_the_definition_does_not_name_are_declared_after_its_tools(self) -> None:
        """update_session() with the stand-in and a function tool 'lookup_order' declares the definition's tools, then
        'lookup_order' from its own schema."""
        project = FakeProjectClient()
        session = await start(project)

        async with reading(session):
            await asyncio.wait_for(session.update_session({}, [lookup_order]), 5)
            await asyncio.wait_for(session.update_session({}, [DEFINITION_TOOLS, lookup_order]), 5)
        await session.close()

        declared = project.server.updates()[-1]["tools"]
        assert declared[:3] == [GET_CURRENT_TIME, GET_OFFICE_HOURS, ORDERS_MCP]
        assert (declared[3]["name"], declared[3]["description"]) == ("lookup_order", "Look up an order.")
        assert len(declared) == 4

    async def test_say_with_the_definitions_azure_voice_sends_a_pre_generated_message(self) -> None:
        """With the definition's voice reported flattened (voice 'en-US-AvaMultilingualNeural', voice_type
        'azure-standard'), say('Hello') sends a pre_generated_assistant_message with that text."""
        project = FakeProjectClient()
        session = await start(project)

        async with reading(session):
            saying = asyncio.ensure_future(session.say("Hello"))
            request = await project.server.wait_sent("response.create")
            project.server.push({"type": "response.created", "response": {"id": "resp_1", "metadata": None}})
            await asyncio.wait_for(saying, 5)
        await session.close()

        assert request["response"]["pre_generated_assistant_message"]["content"] == [{"type": "text", "text": "Hello"}]

    async def test_the_definition_events_still_stream(self) -> None:
        """receive() yields session_created and then session_updated for the events read while waiting for the
        definition, before any later event."""
        project = FakeProjectClient()
        session = await start(project)

        async with reading(session) as events:
            project.server.push({"type": "input_audio_buffer.speech_started", "item_id": "item_1", "audio_start_ms": 0})
            await until(lambda: len(events) == 3)
        await session.close()

        assert [event.kind for event in events] == ["session_created", "session_updated", "speech_started"]

    async def test_say_with_an_openai_voice_sends_the_best_effort_prompt(self) -> None:
        """With the definition's voice 'marin', voice_type 'openai', say('Hello') requests a response whose
        instructions ask to say 'Hello' word for word."""
        project = FakeProjectClient({"3": with_audio_output(voice="marin", voice_type="openai")})
        session = await start(project)

        async with reading(session):
            saying = asyncio.ensure_future(session.say("Hello"))
            request = await project.server.wait_sent("response.create")
            request_id = request["response"]["metadata"]["af_request_id"]
            project.server.push({
                "type": "response.created",
                "response": {"id": "resp_1", "metadata": {"af_request_id": request_id}},
            })
            await asyncio.wait_for(saying, 5)
        await session.close()

        assert "word for word" in request["response"]["instructions"]
        assert request["response"]["instructions"].endswith("Hello")

    async def test_an_object_echo_of_the_requested_voice_counts_as_the_voice_change(self) -> None:
        """update_session() with an Azure voice, echoed as a voice object holding every requested field and nulls
        elsewhere, leaves session.options['voice'] the requested voice."""
        project = FakeProjectClient()
        session = await start(project)

        async with reading(session):
            await asyncio.wait_for(session.update_session({"voice": RYAN}, None), 5)
        await session.close()

        assert project.server.session["audio"]["output"]["voice"] == RYAN
        assert session.options["voice"] == RYAN

    async def test_the_sessions_options_report_the_definitions_other_audio_settings(self) -> None:
        """After starting without them, session.options holds the definition's turn_detection, noise_reduction,
        echo_cancellation and input_audio_format 'audio/pcm;rate=24000'."""
        definition = json.loads(json.dumps(DEFINITION))
        definition["audio"]["input"].update({
            "noise_reduction": {"type": "azure_deep_noise_suppression"},
            "echo_cancellation": {"type": "server_echo_cancellation"},
        })
        session = await start(FakeProjectClient({"3": definition}))
        await session.close()

        assert session.options["turn_detection"] == {"type": "server_vad", "silence_duration_ms": 800}
        assert session.options["noise_reduction"] == {"type": "azure_deep_noise_suppression"}
        assert session.options["echo_cancellation"] == {"type": "server_echo_cancellation"}
        assert session.options["input_audio_format"] == "audio/pcm;rate=24000"

    async def test_start_options_merge_over_the_definitions_settings(self) -> None:
        """Starting with transcription {'model': 'azure-speech'} leaves session.options['transcription'] as that,
        and the definition's turn_detection still reported."""
        session = await start(FakeProjectClient(), transcription={"model": "azure-speech"})
        await session.close()

        assert session.options["transcription"] == {"model": "azure-speech"}
        assert session.options["turn_detection"] == {"type": "server_vad", "silence_duration_ms": 800}

    async def test_a_tool_list_without_the_stand_in_declares_function_tools_from_their_own_schema(self) -> None:
        """update_session() with a function tool 'get_current_time' and no stand-in declares that tool's own
        description and parameters."""
        project = FakeProjectClient()
        session = await start(project)

        async with reading(session):
            await asyncio.wait_for(session.update_session({}, [get_current_time]), 5)
        await session.close()

        [declared] = project.server.updates()[-1]["tools"]
        assert declared["description"] == "Tells the time in the caller's time zone."
        assert "time_zone" in declared["parameters"]["properties"]

    async def test_cancelling_the_start_after_connecting_closes_the_socket(self) -> None:
        """Cancelling start_session() while it waits for the definition closes the SDK connection."""
        project = FakeProjectClient()
        project.holds_definition = True
        client = FoundryVoiceAgentClient(
            project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
        )

        async with client:
            starting = asyncio.ensure_future(client.start_session())
            await until(lambda: bool(project.servers))
            starting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await starting

        assert project.server.closed

    async def test_an_agent_that_was_not_entered_cannot_start_a_conversation(self) -> None:
        """Entering a conversation of an agent that wasn't entered raises RuntimeError mentioning 'async with', and
        nothing connects."""
        project = FakeProjectClient()
        agent = FoundryVoiceAgent(
            project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
        )

        with pytest.raises(RuntimeError, match="async with"):
            async with agent.start_conversation():
                pass

        assert project.connect_calls == []

    async def test_a_late_definition_fails_the_conversation_with_a_control_timeout(self) -> None:
        """When the server never sends the definition, entering a conversation with control_timeout=0.1 raises
        RealtimeControlTimeoutException."""
        project = FakeProjectClient()
        project.holds_definition = True
        agent = FoundryVoiceAgent(
            project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
        )

        async with agent:
            with pytest.raises(RealtimeControlTimeoutException):
                async with agent.start_conversation(control_timeout=0.1):
                    pass

        assert project.server.closed

    async def test_the_start_appends_the_start_instructions_to_the_definitions(self) -> None:
        """start_conversation(options={'instructions': 'Be brief.'}) sends instructions 'You are a receptionist.'
        followed by 'Be brief.', the definition's voice and its tool choice."""
        project = FakeProjectClient()
        agent = FoundryVoiceAgent(
            project_client=project.as_project_client(), agent_name="receptionist", agent_version="3"
        )

        async with agent, agent.start_conversation(options={"instructions": "Be brief."}):
            pass

        start = project.server.updates()[0]
        assert start["instructions"].startswith("You are a receptionist.")
        assert start["instructions"].endswith("Be brief.")
        assert start["audio"]["output"]["voice"] == AVA
        assert start["tool_choice"] == "auto"

    async def test_a_definition_call_runs_its_implementation(self) -> None:
        """A get_current_time call in a completed response runs the agent's get_current_time implementation, and its
        result is sent as the call's output."""
        project = FakeProjectClient()
        agent = FoundryVoiceAgent(
            project_client=project.as_project_client(),
            agent_name="receptionist",
            agent_version="3",
            tools=[get_current_time],
        )

        async with agent, agent.start_conversation():
            project.server.push(*called("resp_1", "get_current_time"))
            await until(lambda: "call_get_current_time" in tool_outputs(project.server))

        assert tool_outputs(project.server)["call_get_current_time"] == "12:00"

    async def test_an_unimplemented_definition_function_gets_the_not_found_result(self) -> None:
        """A get_office_hours call, with no implementation, gets the 'Requested function \"get_office_hours\" not
        found' error result."""
        project = FakeProjectClient()
        agent = FoundryVoiceAgent(
            project_client=project.as_project_client(),
            agent_name="receptionist",
            agent_version="3",
            tools=[get_current_time],
        )

        async with agent, agent.start_conversation():
            project.server.push(*called("resp_1", "get_office_hours"))
            await until(lambda: "call_get_office_hours" in tool_outputs(project.server))

        assert (
            'Requested function "get_office_hours" not found' in tool_outputs(project.server)["call_get_office_hours"]
        )

    async def test_the_first_tool_batch_sends_no_tool_update(self) -> None:
        """After a definition call's batch, the results go out with no session.update between the start's and them."""
        project = FakeProjectClient()
        agent = FoundryVoiceAgent(
            project_client=project.as_project_client(),
            agent_name="receptionist",
            agent_version="3",
            tools=[get_current_time],
        )

        async with agent, agent.start_conversation():
            project.server.push(*called("resp_1", "get_current_time"))
            await until(lambda: any(event["type"] == "response.create" for event in project.server.sent))

        assert [event["type"] for event in project.server.sent if event["type"] != "conversation.item.create"] == [
            "session.update",
            "response.create",
        ]

    async def test_a_definition_that_transcribes_keeps_caller_turns_waiting_for_transcripts(self) -> None:
        """With a history provider, no transcription option and a definition that transcribes, a caller turn whose
        transcript arrives after its reply is recorded with that transcript."""
        project = FakeProjectClient()
        agent = FoundryVoiceAgent(
            project_client=project.as_project_client(),
            agent_name="receptionist",
            agent_version="3",
            context_providers=[InMemoryHistoryProvider()],
        )
        caller_turn = {"id": "item_caller", "type": "message", "role": "user", "content": [{"type": "input_audio"}]}
        transcript = {
            "type": "conversation.item.input_audio_transcription.completed",
            "item_id": "item_caller",
            "content_index": 0,
            "transcript": "What time is it?",
        }

        async with agent:
            conversation = agent.start_conversation()
            async with conversation:
                project.server.push({"type": "conversation.item.added", "previous_item_id": None, "item": caller_turn})
                await until(lambda: len(conversation.history) == 1)
                asyncio.get_running_loop().call_later(0.1, project.server.push, transcript)

        assert conversation.history[0].text == "What time is it?"

    async def test_handing_off_to_a_plain_participant_removes_the_definitions_tools(self) -> None:
        """In a built topology, handoff(billing) declares billing's own tools and its handoff tools, and none of the
        definition's."""
        project = FakeProjectClient()
        _, billing, built = topology(project)

        async with built, built.start_conversation() as conversation:
            await asyncio.wait_for(conversation.handoff(billing), 5)

        assert tool_names(project.server.updates()[-1]) == ["lookup_order", "handoff_to_receptionist"]

    async def test_handing_back_restores_the_definition(self) -> None:
        """After handoff(billing) then handoff(receptionist), the update carries the definition's instructions with the
        built agent's appended, its full AzureVoice, its tool choice and its tools as published."""
        project = FakeProjectClient()
        receptionist, billing, built = topology(project, default_options={"instructions": "Hand off silently."})

        async with built, built.start_conversation() as conversation:
            await asyncio.wait_for(conversation.handoff(billing), 5)
            await asyncio.wait_for(conversation.handoff(receptionist), 5)

        handed_back = project.server.updates()[-1]
        assert handed_back["instructions"].startswith("You are a receptionist.")
        assert handed_back["instructions"].endswith("Hand off silently.")
        assert handed_back["audio"]["output"]["voice"] == AVA
        assert handed_back["tool_choice"] == "auto"
        assert handed_back["tools"][:3] == [GET_CURRENT_TIME, GET_OFFICE_HOURS, ORDERS_MCP]
        assert tool_names(handed_back)[3:] == ["handoff_to_billing"]

    async def test_the_built_agents_default_voice_and_tool_choice_do_not_override_the_definitions(self) -> None:
        """With the built agent's default_options voice 'en-GB-RyanNeural' and tool_choice 'none', the start sends the
        definition's voice and tool choice."""
        project = FakeProjectClient()
        _, _, built = topology(project, default_options={"voice": RYAN, "tool_choice": "none"})

        async with built, built.start_conversation():
            pass

        start = project.server.updates()[0]
        assert (start["audio"]["output"]["voice"], start["tool_choice"]) == (AVA, "auto")

    async def test_a_built_handoff_tool_the_definition_names_is_declared_as_published_and_runs(self) -> None:
        """When the definition declares handoff_to_billing, the start declares it as published, and a call to it hands
        the call to billing."""
        handoff_to_billing = {
            "type": "function",
            "name": "handoff_to_billing",
            "description": "Transfer the caller to billing.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        }
        definition = {**DEFINITION, "tools": [GET_CURRENT_TIME, handoff_to_billing]}
        project = FakeProjectClient({"3": definition})
        receptionist, billing, built = topology(project)

        async with built, built.start_conversation() as conversation:
            follow_up = asyncio.ensure_future(project.server.wait_sent("response.create"))
            project.server.push(*called("resp_1", "handoff_to_billing"))
            project.server.answer(await follow_up)
            await until(lambda: conversation.active_agent is billing)
            await asyncio.wait_for(conversation.handoff(receptionist), 5)

        assert "tools" not in project.server.updates()[0]
        assert project.server.updates()[-1]["tools"] == [GET_CURRENT_TIME, handoff_to_billing]

    async def test_a_built_agent_that_was_not_entered_cannot_start_a_conversation(self) -> None:
        """Entering a conversation of a built agent whose Foundry participant wasn't entered raises RuntimeError."""
        project = FakeProjectClient()
        _, _, built = topology(project)

        with pytest.raises(RuntimeError, match="async with"):
            async with built.start_conversation():
                pass

        assert project.connect_calls == []

    async def test_handing_off_to_a_non_start_foundry_participant_applies_its_definition(self) -> None:
        """With a plain start agent on foundry_agent.client, handoff(foundry_agent) sends the definition's
        instructions, voice, tool choice and tools."""
        project = FakeProjectClient()
        receptionist = FoundryVoiceAgent(
            project_client=project.as_project_client(),
            agent_name="receptionist",
            agent_version="3",
            tools=[get_current_time],
        )
        greeter = RealtimeAgent(
            receptionist.client,
            name="greeter",
            instructions="Greet the caller.",
            default_options=FoundryVoiceAgentOptions(voice=SONIA),
        )
        built = (
            RealtimeHandoffBuilder[FoundryVoiceAgentOptions](participants=[greeter, receptionist])
            .with_start_agent(greeter)
            .build()
        )

        async with built, built.start_conversation() as conversation:
            await asyncio.wait_for(conversation.handoff(receptionist), 5)

        handed_off = project.server.updates()[-1]
        assert handed_off["instructions"] == "You are a receptionist."
        assert (handed_off["audio"]["output"]["voice"], handed_off["tool_choice"]) == (AVA, "auto")
        assert handed_off["tools"][:3] == [GET_CURRENT_TIME, GET_OFFICE_HOURS, ORDERS_MCP]

    async def test_the_agents_own_reasoning_effort_wins_over_the_definitions(self) -> None:
        """With a definition whose reasoning effort is 'high' and the agent's default_options 'minimal', the start and,
        after billing's 'low', handing back to the agent send reasoning.effort 'minimal'."""
        project = FakeProjectClient({"3": REASONING_DEFINITION})
        receptionist = FoundryVoiceAgent(
            project_client=project.as_project_client(),
            agent_name="receptionist",
            agent_version="3",
            default_options=FoundryVoiceAgentOptions(reasoning_effort="minimal"),
        )
        billing = RealtimeAgent(
            receptionist.client,
            name="billing",
            default_options=FoundryVoiceAgentOptions(voice=SONIA, reasoning_effort="low"),
        )
        built = (
            RealtimeHandoffBuilder[FoundryVoiceAgentOptions](participants=[receptionist, billing])
            .with_start_agent(receptionist)
            .build()
        )

        async with built, built.start_conversation() as conversation:
            await asyncio.wait_for(conversation.handoff(billing), 5)
            await asyncio.wait_for(conversation.handoff(receptionist), 5)

        opened, *_, handed_back = project.server.updates()
        assert (opened.get("reasoning"), handed_back.get("reasoning")) == ({"effort": "minimal"}, {"effort": "minimal"})
