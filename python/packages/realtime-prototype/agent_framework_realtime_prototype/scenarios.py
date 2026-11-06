# PROTOTYPE, never merged (adamdougal/agent-framework#10).
"""Live scenarios: one per check in #10's Inputs. Each records pass/fail checks with evidence.

Run all: ``uv run python -m agent_framework_realtime_prototype.scenarios``; one: ``... scenarios core-loop``.
Results go to ``results/<id>.json``, kept as evidence for the findings.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import subprocess  # nosec
import sys
import time
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agent_framework._middleware import MiddlewareTermination
from opentelemetry import context as otel_context
from opentelemetry import trace

from . import CORE_CHANGES, ConversationMiddleware, RealtimeAgent, RealtimeError, telemetry
from ._conversation import FULLY_HEARD_TOLERANCE_MS
from .accelerator import (
    BILLING,
    CONTENT_FILTER_FALLBACK,
    SILENT_HANDOFF_SUFFIX,
    azure_voice,
    build_contact_centre,
    get_current_time,
    get_office_hours,
    load_env,
    make_client,
    tool_log,
)
from .harness import SimCall

RESULTS = Path(__file__).resolve().parent.parent / "results"
APP_TRACER = trace.get_tracer("prototype.app")
WIRE_NAME = re.compile(r"[\"'](response|conversation|input_audio_buffer|session|rate_limits)\.[a-z_.]+[\"']")

Emit = Callable[[dict[str, Any]], Awaitable[None]]


@dataclass
class Scenario:
    id: str
    title: str
    tickets: str
    provider: str
    description: str
    watch: str
    steps: list[str]
    run: Callable[[ScenarioContext], Awaitable[None]]

    def describe(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "tickets": self.tickets,
            "provider": self.provider,
            "description": self.description,
            "watch": self.watch,
            "steps": self.steps,
        }


@dataclass
class _Open:
    agent: Any
    conversation: Any
    call: SimCall
    span: Any
    closed: bool = False


@dataclass
class ScenarioContext:
    scenario: Scenario
    credential: Any
    orders: Any
    emit: Emit | None
    checks: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    opened: list[_Open] = field(default_factory=list)
    log_lines: list[str] = field(default_factory=list)

    def log(self, line: str) -> None:
        self.log_lines.append(line)
        print(line, flush=True)

    async def _send(self, event: dict[str, Any]) -> None:
        if self.emit is not None:
            await self.emit(event)

    async def step(self, text: str) -> None:
        self.log(f"--- {text}")
        await self._send({"kind": "Step", "text": text})

    async def check(self, name: str, passed: bool | None, evidence: Any = "") -> None:
        """passed=None records an observation that neither passes nor fails."""
        record = {"name": name, "passed": passed, "evidence": evidence if isinstance(evidence, str) else json.dumps(evidence, default=str)}
        self.checks.append(record)
        mark = {True: "PASS", False: "FAIL", None: "NOTE"}[passed]
        self.log(f"[{mark}] {name}: {record['evidence'][:300]}")
        await self._send({"kind": "Check", **record})

    def note(self, text: str) -> None:
        self.notes.append(text)
        self.log(f"note: {text}")

    async def open(
        self,
        provider: str,
        *,
        build: Callable[[Any], Any] | None = None,
        barge_in: bool = True,
        api_key: str | None = None,
        **conversation_kwargs: Any,
    ) -> SimCall:
        client = make_client(provider, self.credential, api_key=api_key)
        agent = build(client) if build else build_contact_centre(client, provider, mcp_tools=[self.orders])
        await agent.__aenter__()
        span = APP_TRACER.start_span("ws.session", attributes={"scenario": self.scenario.id, "provider": provider})
        token = otel_context.attach(trace.set_span_in_context(span))
        try:
            conversation = agent.start_conversation(**conversation_kwargs)
            await conversation.__aenter__()
        finally:
            otel_context.detach(token)
        call = SimCall(conversation, barge_in=barge_in, log=self.log, on_event=self._forward)
        await call.start()
        self.opened.append(_Open(agent, conversation, call, span))
        return call

    async def _forward(self, event: dict[str, Any]) -> None:
        if event["kind"] != "Update":
            await self._send(event)

    async def hang_up(self, call: SimCall) -> None:
        for opened in self.opened:
            if opened.call is call and not opened.closed:
                opened.closed = True
                await call.stop()
                opened.span.end()
                await opened.agent.__aexit__(None, None, None)

    async def close_all(self) -> None:
        for opened in list(self.opened):
            if not opened.closed:
                with contextlib.suppress(Exception):
                    await self.hang_up(opened.call)

    def trace_of(self, call: SimCall) -> list[dict[str, Any]]:
        for opened in self.opened:
            if opened.call is call:
                return telemetry.span_tree(opened.span.get_span_context().trace_id)
        return []


# region helpers


def history_rows(conversation: Any) -> list[dict[str, Any]]:
    rows = []
    for message in conversation.history:
        row: dict[str, Any] = {"role": message.role, "author": message.author_name, "id": message.message_id}
        for content in message.contents:
            if content.type == "text":
                row["text"], row["status"] = content.text, content.status
            elif content.type == "function_call":
                row["call"], row["call_id"] = content.name, content.call_id
            elif content.type == "function_result":
                row["result"], row["call_id"], row["exception"] = str(content.result), content.call_id, content.exception
        text_less = (message.additional_properties or {}).get("realtime", {}).get("text_less")
        if text_less:
            row["text_less"] = text_less
        rows.append(row)
    return rows


def walk(nodes: list[dict[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any] | None]]:
    out: list[tuple[dict[str, Any], dict[str, Any] | None]] = []

    def visit(node: dict[str, Any], parent: dict[str, Any] | None) -> None:
        out.append((node, parent))
        for child in node["children"]:
            visit(child, node)

    for node in nodes:
        visit(node, None)
    return out


def tool_gaps_ms(conversation: Any) -> list[int]:
    return [
        round((response.done_at - response.first_call_done_at) * 1000)
        for response in conversation._responses.values()  # noqa: SLF001
        if response.first_call_done_at and response.done_at
    ]


def late_transcripts(conversation: Any) -> list[dict[str, Any]]:
    """Caller turns whose final transcript arrived after the reply to them was created."""
    found = []
    items = conversation._items  # noqa: SLF001
    order = conversation._order  # noqa: SLF001
    for item_id in order:
        item = items[item_id]
        if item.role != "user" or item.source != "caller" or item.final_at is None:
            continue
        replies = [
            r for r in conversation._responses.values()  # noqa: SLF001
            if r.exchange == item.exchange and r.created > item.added_at
        ]
        if replies and item.final_at > min(r.created for r in replies):
            first = min(replies, key=lambda r: r.created)
            reply_items = [i for i in first.messages if i in order]
            ordered_first = all(order.index(item_id) < order.index(i) for i in reply_items)
            found.append({
                "item": item_id,
                "transcript_after_reply_ms": round((item.final_at - first.created) * 1000),
                "ordered_before_reply": ordered_first,
            })
    return found


async def wait_until(predicate: Callable[[], bool], timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.05)
    return predicate()


def voice_name(voice: Any) -> Any:
    return voice.get("name") if isinstance(voice, dict) else voice


# endregion

# region scenarios


async def core_loop(ctx: ScenarioContext) -> None:
    call = await ctx.open("voicelive-byom", create_response=True)
    conversation = call.conversation
    session = conversation.provider_session
    await ctx.step("Greeting: the receptionist speaks first (create_response at start)")
    await call.wait_reply_done(30)
    await ctx.check(
        "BYOM through query: the session reports the deployment as its model",
        session.model == "gpt-realtime-2.1",
        {"session.model": session.model, "profile": "byom-azure-openai-realtime"},
    )
    await ctx.check(
        "Receptionist speaks in Ava (per-agent Azure voice)",
        voice_name(session.current_voice()) == "en-US-AvaMultilingualNeural",
        session.current_voice(),
    )
    instructions = session.session_state.get("instructions") or ""
    await ctx.check(
        "'Unified assistant' suffix written once on the builder, applied once",
        instructions.count("CRITICAL CONVERSATION RULES") == 1 and "CRITICAL" not in BILLING,
        f"count={instructions.count('CRITICAL CONVERSATION RULES')}",
    )
    await ctx.step("Caller asks for office hours: one tool call")
    await call.say("hours")
    result = await call.wait_kind("FunctionResult", 30)
    await call.wait_reply_done(30)
    await ctx.check("Tool ran and its result was sent as a string", "office hours" in result["data"]["result"], result["data"])
    await ctx.step("Caller asks about their balance: silent in-session handoff to billing")
    await call.say("balance")
    switch = await call.wait_kind("AgentSwitch", 30, isHandoff=True)
    await call.wait_reply_done(30)
    await ctx.check("handoff_completed drives the agent label", switch["data"]["agentName"] == "billing", switch["data"])
    await ctx.check(
        "Billing speaks in Sonia after the handoff (voice changed in-session)",
        voice_name(session.current_voice()) == "en-GB-SoniaNeural",
        session.current_voice(),
    )
    instructions = session.session_state.get("instructions") or ""
    await ctx.check(
        "Session now carries billing's instructions plus the suffix once",
        instructions.startswith("You are a billing specialist") and instructions.count("CRITICAL CONVERSATION RULES") == 1,
        instructions[:80],
    )
    tool_names = [t.get("name") for t in session.session_state.get("tools") or []]
    await ctx.check(
        "Billing's tools and handoff tools replaced the receptionist's",
        "get_account_balance" in tool_names and "handoff_to_receptionist" in tool_names and "get_office_hours" not in tool_names,
        tool_names,
    )
    await ctx.step("Caller gives the account number: billing's tool")
    await call.say("account")
    await call.wait_kind("FunctionResult", 30, function="get_account_balance")
    await call.wait_reply_done(30)
    await ctx.step("Caller tries a jailbreak: content filter, then the fallback line through say()")
    await call.say("jailbreak")
    filtered = await call.wait_for(
        lambda e: e["kind"] == "ResponseDone" and e["data"].get("finishReason") == "content_filter", 45
    )
    fallback = await call.wait_for(
        lambda e: e["kind"] == "Transcription" and e["data"]["speaker"] != "user" and e["t"] > filtered["t"], 20
    )
    await call.wait_reply_done(20)
    await ctx.check(
        "Content filter surfaces as finish_reason='content_filter' and the fallback is spoken verbatim",
        fallback["data"]["text"] == CONTENT_FILTER_FALLBACK,
        {"filtered": filtered["data"], "spoken": fallback["data"]["text"]},
    )
    await ctx.check(
        "say() correlated without metadata echo (Voice Live returns metadata: null for pre-generated messages)",
        session.unechoed_correlations >= 1,
        f"unechoed correlations: {session.unechoed_correlations}",
    )
    await ctx.step("Hang up")
    await call.settle(1)
    await ctx.hang_up(call)
    events = call.events
    await ctx.check(
        "Handoff calls hidden by the handoff_to_ prefix",
        not any(e["kind"] == "FunctionCall" and e["data"]["function"].startswith("handoff_to_") for e in events),
        [e["data"]["function"] for e in events if e["kind"] == "FunctionCall"],
    )
    rows = history_rows(conversation)
    handoff_index = next(i for i, r in enumerate(rows) if r.get("call") == "handoff_to_billing")
    after = next((r for r in rows[handoff_index + 2 :] if r["role"] == "assistant"), {})
    await ctx.check(
        "Handoff call credited to the receptionist, the follow-up to billing",
        rows[handoff_index]["author"] == "receptionist" and after.get("author") == "billing",
        rows[handoff_index : handoff_index + 3],
    )
    signals = call.loop.signals
    await ctx.check(
        "Accelerator signals rebuilt from updates (cc.content_filter_count, cc.handoff_count)",
        signals.content_filter_count == 1 and signals.handoff_count == 1,
        signals.__dict__,
    )
    source = Path(__file__).with_name("accelerator.py").read_text()
    await ctx.check(
        "The ported loop matches no wire event name (barge-in, transcripts, filter, usage, errors by kind)",
        not WIRE_NAME.search(source),
        WIRE_NAME.findall(source) or "no wire names in accelerator.py",
    )
    tree = ctx.trace_of(call)
    nodes = walk(tree)
    conv_span = next((n for n, _ in nodes if n["name"].startswith("invoke_agent")), None)
    inference = [(n, p) for n, p in nodes if n["name"].startswith("realtime_inference")]
    tools = [(n, p) for n, p in nodes if n["name"].startswith("execute_tool")]
    await ctx.check(
        "One trace under the app root; realtime_inference and execute_tool are siblings under the conversation span",
        tree and tree[0]["name"] == "ws.session" and conv_span is not None
        and all(p is conv_span for _, p in inference) and all(p is conv_span for _, p in tools) and len(tools) >= 3,
        {"roots": [n["name"] for n in tree], "inference": len(inference), "tools": len(tools)},
    )
    handoff_span = next((n for n, _ in tools if "handoff_to_billing" in n["name"]), None)
    await ctx.check(
        "The handoff tool's execute_tool span carries the transfer refinement",
        handoff_span is not None and handoff_span["attributes"].get("gen_ai.transfer.target.name") == "billing",
        handoff_span["attributes"] if handoff_span else None,
    )
    completion = {"turns": len(rows), "transcript": [f"{r['role']}/{r.get('author')}: {r.get('text') or r.get('call') or r.get('result')}" for r in rows]}
    await ctx.check(
        "Hang-up completion event built from conversation.history",
        sum(1 for r in rows if r["role"] == "user") >= 4,
        completion,
    )
    late = late_transcripts(conversation)
    await ctx.check(
        "Late caller transcripts still ordered before the reply they caused",
        all(entry["ordered_before_reply"] for entry in late),
        late or "no late transcripts in this run",
    )
    ctx.metrics["tool_gap_ms"] = tool_gaps_ms(conversation)
    ctx.metrics["close_transcript_wait_s"] = conversation.stats["close_transcript_wait_s"]
    await ctx.check("Function call → response_done gap with an Azure TTS voice (ms)", None, ctx.metrics["tool_gap_ms"])


async def manual_handoff(ctx: ScenarioContext) -> None:
    call = await ctx.open("voicelive", create_response=True)
    conversation = call.conversation
    session = conversation.provider_session
    agent = ctx.opened[-1].agent
    await call.wait_reply_done(30)
    await ctx.step("App escalates: conversation.handoff(support), outside any tool")
    support = next(p for p in agent.participants_list if p.name == "support")
    await conversation.handoff(support)
    switch = await call.wait_kind("AgentSwitch", 10, isHandoff=True)
    tool_names = [t.get("name") for t in session.session_state.get("tools") or []]
    await ctx.check(
        "App-driven handoff swaps the session and installs support's handoff tools",
        conversation.active_agent is support and "handoff_to_billing" in tool_names and "get_troubleshooting_steps" in tool_names,
        {"label": switch["data"], "tools": tool_names, "voice": session.current_voice()},
    )
    await ctx.step("Handoff to an agent outside the topology raises before sending")
    stranger = RealtimeAgent(client=conversation.client, name="stranger", instructions="x")
    sent_before = sum(1 for w in session.wire_log if w["dir"] == "out" and w["event"].get("type") == "session.update")
    raised = None
    try:
        await conversation.handoff(stranger)
    except RealtimeError as exc:
        raised = str(exc)
    sent_after = sum(1 for w in session.wire_log if w["dir"] == "out" and w["event"].get("type") == "session.update")
    await ctx.check("Non-participant handoff raises without a session update", raised is not None and sent_before == sent_after, raised)
    await ctx.step("Caller describes a login problem: support answers")
    await call.say("tech_issue")
    await call.wait_reply_done(40)
    rows = history_rows(conversation)
    answered = [r for r in rows if r["role"] == "assistant" and r.get("text")]
    await ctx.check("Support answers in its own voice", answered and answered[-1]["author"] == "support", answered[-1] if answered else rows)
    await ctx.hang_up(call)
    spans = [n for n, _ in walk(ctx.trace_of(call)) if n["name"] == "realtime.handoff"]
    await ctx.check(
        "handoff() outside a tool gets a realtime.handoff span with source and target",
        len(spans) == 1 and spans[0]["attributes"].get("agent_framework.handoff.target") == "support",
        spans[0]["attributes"] if spans else "no span",
    )


async def handoff_rejected(ctx: ScenarioContext) -> None:
    def build(client: Any) -> Any:
        return build_contact_centre(
            client,
            "voicelive",
            mcp_tools=[ctx.orders],
            voices={
                "receptionist": azure_voice("en-US-AvaMultilingualNeural"),
                "billing": azure_voice("en-US-NotARealVoiceNeural"),
                "support": azure_voice("en-GB-AlfieNeural"),
            },
        )

    call = await ctx.open("voicelive", build=build, create_response=True)
    conversation = call.conversation
    session = conversation.provider_session
    await call.wait_reply_done(30)
    await ctx.step("Caller asks about their balance; billing's voice is invalid, so Voice Live rejects the update")
    await call.say("balance")
    result = await call.wait_for(
        lambda e: e["kind"] == "Update" and any(c["type"] == "function_result" for c in e["data"]["contents"]), 30
    )
    content = next(c for c in result["data"]["contents"] if c["type"] == "function_result")
    timings = [t for t in session.timings if t[0] == "session.updated"]
    await ctx.check(
        "The rejected update fails the handoff tool fast (claimed error, not the 10 s timeout)",
        bool(content.get("exception")) and timings and timings[-1][1] < 2,
        {"result": content, "ack_ms": [round(s * 1000) for _, s in timings]},
    )
    spoken = await call.wait_for(lambda e: e["kind"] == "Transcription" and e["data"]["speaker"] != "user", 40)
    await call.wait_reply_done(30)
    attempts = [c for c in conversation._calls.values() if c.content.name == "handoff_to_billing"]  # noqa: SLF001
    await ctx.check(
        "The receptionist stays active and carries on",
        conversation.active_agent.name == "receptionist" and spoken["data"]["speaker"] == "receptionist",
        {"active": conversation.active_agent.name, "said": spoken["data"]["text"], "handoff_attempts": len(attempts),
         "every_attempt_failed": all(c.result is not None and c.result.exception for c in attempts),
         "model_saw": str(attempts[0].result.result) if attempts else None},
    )
    passthrough = [w for w in session.wire_log if w["dir"] == "in" and w["event"].get("type") == "error"]
    error_events = [e for e in call.events if e["kind"] == "Error"]
    await ctx.check(
        "Claimed errors stream only as passthrough, not as error content",
        passthrough and not error_events,
        {"wire_errors": [w["event"].get("error", {}).get("code") for w in passthrough], "error_updates": error_events},
    )
    await ctx.step("Local check: create_response() while a response is active")
    await call.say("hours", wait=False)
    await wait_until(lambda: session.active_response_id is not None, 30)
    started = time.monotonic()
    try:
        await conversation.create_response()
        local = "no error"
    except RealtimeError as exc:
        local = type(exc).__name__
    await ctx.check(
        "create_response() during an active response raises at once",
        local == "RealtimeActiveResponseError" and time.monotonic() - started < 0.2,
        {"error": local, "ms": round((time.monotonic() - started) * 1000)},
    )
    await call.wait_reply_done(30)
    await ctx.step("The race: create_response() the moment the caller stops talking, against turn detection's response")
    outcomes = []
    for _ in range(2):
        call.mark()
        await call.say("thanks", wait=False)
        await call.wait_for(
            lambda e: e["kind"] == "Update" and any(c["type"] == "speech_stopped" for c in e["data"]["contents"]), 20
        )
        started = time.monotonic()
        try:
            await conversation.create_response()
            outcome = "accepted"
        except RealtimeError as exc:
            outcome = type(exc).__name__
        outcomes.append({"outcome": outcome, "ms": round((time.monotonic() - started) * 1000)})
        with contextlib.suppress(TimeoutError):
            await call.wait_reply_done(20)
    await ctx.check(
        "A create_response() racing turn detection never hangs: the lost-race rule fails it at once, or it wins",
        all(o["ms"] < 3000 for o in outcomes),
        outcomes,
    )
    await ctx.check("Identical errors deduplicated (Voice Live repeats some)", None, session.deduplicated_errors)
    await ctx.hang_up(call)


async def two_handoffs(ctx: ScenarioContext) -> None:
    call = await ctx.open("voicelive-byom", create_response=True)
    conversation = call.conversation
    await call.wait_reply_done(30)
    attempts = 0
    pair: list[Any] = []
    for clip in ("two_handoffs", "two_handoffs"):
        attempts += 1
        await ctx.step(f"Caller asks for billing and support at once (attempt {attempts})")
        await call.say(clip)
        with contextlib.suppress(TimeoutError):
            await call.wait_reply_done(30)
        await call.settle(2)
        by_response: dict[str, list[Any]] = {}
        for c in conversation._calls.values():  # noqa: SLF001
            if c.content.name.startswith("handoff_to_"):
                by_response.setdefault(c.response_id, []).append(c)
        pair = next((calls for calls in by_response.values() if len(calls) >= 2), [])
        if pair:
            break
    if not pair:
        await ctx.step("The model never emitted two handoff calls; send them through one batch directly")
        await ctx.check("Model emitted two handoff calls in one response", None, f"not in {attempts} attempts")
        from agent_framework import Content

        from ._conversation import _Call, _Response

        response = _Response("synthetic", conversation.active_agent, conversation._exchange)  # noqa: SLF001
        for target in ("billing", "support"):
            content = Content.from_function_call(call_id=f"syn_{target}", name=f"handoff_to_{target}", arguments="{}")
            synthetic = _Call(content, "synthetic", None, conversation.active_agent, conversation._executable.get(f"handoff_to_{target}"))  # noqa: SLF001
            response.calls.append(synthetic)
            conversation._calls[content.call_id] = synthetic  # noqa: SLF001
        await conversation._run_batch(response)  # noqa: SLF001
        pair = response.calls
    results = [(c.content.name, str(c.result.result) if c.result else None, c.result.exception if c.result else None) for c in pair]
    winners = [r for r in results if not r[2]]
    await ctx.check(
        "First handoff wins; the other gets an error result",
        len(winners) == 1 and conversation.active_agent.name in winners[0][0],
        {"results": results, "active": conversation.active_agent.name},
    )
    await call.settle(3)
    await ctx.hang_up(call)


async def foundry(ctx: ScenarioContext) -> None:
    from agent_framework import tool

    implementations = [
        tool(get_current_time.func, name="ReceptionistPlugin-get_current_time", description="Get the current time."),
        tool(get_office_hours.func, name="ReceptionistPlugin-get_office_hours", description="Get the office hours."),
    ]

    def build(client: Any) -> Any:
        agent = build_contact_centre(client, "foundry")
        start = agent.start_agent
        start.implementations = [*implementations, get_current_time, get_office_hours]
        return agent

    from opentelemetry.propagate import inject

    call = await ctx.open("foundry", build=build, create_response=True)
    conversation = call.conversation
    session = conversation.provider_session
    definition = session.definition or {}
    await ctx.check(
        "FoundryVoiceAgent captures its definition from the unsolicited session.updated",
        bool(definition.get("instructions")),
        {"delay_ms": round((session.definition_delay_s or 0) * 1000), "tools": [t.get("name") for t in definition.get("tools") or []],
         "voice": (definition.get("audio") or {}).get("output", {}).get("voice"), "tool_choice": definition.get("tool_choice")},
    )
    await call.wait_reply_done(30)
    live_tools = [t.get("name") for t in session.session_state.get("tools") or []]
    definition_tools = [t.get("name") for t in definition.get("tools") or []]
    await ctx.check(
        "The definition's own tools survive the added handoff tools",
        all(name in live_tools for name in definition_tools) and "handoff_to_billing" in live_tools,
        {"live": live_tools},
    )
    await ctx.check(
        "Definition tools published by SK use plugin-qualified names, so AF's name match needs renamed implementations",
        None,
        {"definition": definition_tools, "af_names": ["get_current_time", "handoff_to_billing"]},
    )
    headers: dict[str, str] = {}
    inject(headers)
    await ctx.check(
        "traceparent injected on Foundry's handshake (server-side join not observable locally)",
        None,
        "connect succeeded with a traceparent header" if conversation.session_id else "no session",
    )
    await ctx.step("Caller asks about their balance: handoff to billing on Foundry's client")
    await call.say("balance")
    switch = await call.wait_kind("AgentSwitch", 30)
    await call.wait_reply_done(30)
    await ctx.check(
        "Handoff from the Foundry start agent to billing",
        conversation.active_agent.name == "billing",
        {"label": switch["data"], "voice": session.current_voice(), "called": [e["data"] for e in call.events if e["kind"] == "Update" and any(c["type"] == "function_call" for c in e["data"]["contents"])][-2:]},
    )
    await ctx.step("Caller has a general question: billing hands back to the Foundry receptionist")
    await call.say("general")
    model_handed_back = True
    try:
        await call.wait_for(lambda e: e["kind"] == "AgentSwitch" and e["data"].get("agentName") == "receptionist", 25)
    except TimeoutError:
        model_handed_back = False
        await ctx.step("Billing didn't hand back, so the app calls handoff(receptionist)")
        await conversation.handoff(ctx.opened[-1].agent.start_agent)
    with contextlib.suppress(TimeoutError):
        await call.wait_reply_done(20)
    await ctx.check("The model handed back to the receptionist by itself", None, model_handed_back)
    instructions = session.session_state.get("instructions") or ""
    restored_tools = [t.get("name") for t in session.session_state.get("tools") or []]
    await ctx.check(
        "Handing back restores the definition's instructions, voice and tools",
        conversation.active_agent.name == "receptionist"
        and instructions.startswith((definition.get("instructions") or "")[:60])
        and all(name in restored_tools for name in definition_tools),
        {"active": conversation.active_agent.name, "voice": session.current_voice(), "tools": restored_tools},
    )
    rows = history_rows(conversation)
    calls = [r for r in rows if r.get("call")]
    await ctx.check("Calls made on Foundry, with results", None, [(r["author"], r["call"]) for r in calls])
    await ctx.hang_up(call)


async def barge_in(ctx: ScenarioContext) -> None:
    call = await ctx.open("voicelive-byom", create_response=True)
    conversation = call.conversation
    await call.wait_reply_done(30)
    await ctx.step("Caller asks for every troubleshooting step: a long reply")
    await call.say("long_answer")
    await wait_until(lambda: call.speaker.playing(), 40)
    long_response = None
    for _ in range(3):
        await wait_until(lambda: call.speaker.playing(), 40)
        long_response = call.speaker.last
        await asyncio.sleep(1.0)
        if conversation._responses.get(long_response or "") and conversation._responses[long_response].audio_ms:  # noqa: SLF001
            total = sum(conversation._responses[long_response].audio_ms.values())  # noqa: SLF001
            if total > 8000:
                break
    await asyncio.sleep(3.0)
    await ctx.step("Caller talks over the agent about 4 s into the reply")
    await call.say("interrupt")
    await call.wait_reply_done(30)
    record = next((r for r in call.interrupts if r["was_playing"]), None)
    response = conversation._responses.get(record["response_id"]) if record else None  # noqa: SLF001
    rows = history_rows(conversation)
    cut = [r for r in rows if r.get("status") == "incomplete" and r["role"] == "assistant"]
    generated = sum(response.audio_ms.values()) if response else 0
    item = conversation._items.get(response.messages[0]) if response and response.messages else None  # noqa: SLF001
    full = (item.deltas if item else "") or ""
    await ctx.check(
        "Barge-in with interrupt(played_ms=) keeps only the heard text, marked incomplete",
        record is not None and cut and len(cut[-1]["text"]) < len(full),
        {"played_ms": record["played_ms"] if record else None, "generated_ms": round(generated), "kept": cut[-1]["text"] if cut else None,
         "kept_chars": len(cut[-1]["text"]) if cut else None, "generated_chars": len(full)},
    )
    truncated = [e for e in call.events if e["kind"] == "Update" and any(c["type"] == "item_truncated" for c in e["data"]["contents"])]
    await ctx.check("item_truncated streams as a first-class update", bool(truncated), truncated[:1])
    await ctx.hang_up(call)
    events = [n for n, _ in walk(ctx.trace_of(call)) if "agent_framework.realtime.item_truncated" in n["events"]]
    await ctx.check("item_truncated recorded as an event on the conversation span", bool(events), len(events))


async def aoai(ctx: ScenarioContext) -> None:
    load_env()
    import os

    resource = os.environ["AZURE_OPENAI_ENDPOINT"].split("//")[1].split(".")[0]
    group = subprocess.run(  # nosec
        ["az", "cognitiveservices", "account", "list", "--query", f"[?name=='{resource}'].resourceGroup | [0]", "-o", "tsv"],
        capture_output=True, text=True,
    ).stdout.strip()
    key = subprocess.run(  # nosec
        ["az", "cognitiveservices", "account", "keys", "list", "-n", resource, "-g", group, "--query", "key1", "-o", "tsv"],
        capture_output=True, text=True,
    )
    api_key = key.stdout.strip() or None
    await ctx.step("Connect to Azure OpenAI with an API key sent as a bearer token")
    call = None
    if api_key:
        try:
            call = await ctx.open("aoai", api_key=api_key, create_response=True)
            await ctx.check("Azure OpenAI accepts an API key as a bearer token on the GA realtime route", True, f"session {call.conversation.session_id}")
        except Exception as exc:  # noqa: BLE001
            await ctx.check("Azure OpenAI accepts an API key as a bearer token on the GA realtime route", False, repr(exc)[:300])
    else:
        await ctx.check("Azure OpenAI accepts an API key as a bearer token on the GA realtime route", None, f"no key: {key.stderr.strip()[:200]}")
    if call is None:
        call = await ctx.open("aoai", create_response=True)
    conversation = call.conversation
    await call.wait_reply_done(30)
    await ctx.step("Handoff to billing: Azure OpenAI can't change the voice after the first audio")
    await call.say("balance")
    await call.wait_kind("AgentSwitch", 30)
    await call.wait_reply_done(30)
    await ctx.check(
        "Handoff succeeds with the voice dropped, so billing speaks in the receptionist's voice",
        conversation.active_agent.name == "billing" and conversation.provider_session.current_voice() == "marin",
        {"voice": conversation.provider_session.current_voice()},
    )
    await call.say("account")
    with contextlib.suppress(TimeoutError):
        await call.wait_reply_done(30)
    await ctx.step("Slow tool, then 'is it ready yet?': gpt-realtime-2.1 can split that reply into two messages")
    found = None
    from . import accelerator

    accelerator.SLOW_TOOL_SECONDS = 20
    for attempt in range(4):
        await call.say("history")
        await call.wait_kind("FunctionCall", 30)
        await call.settle(1)
        await call.say("status" if attempt % 2 == 0 else "while_waiting", wait=False)

        def two_messages() -> bool:
            response = conversation._responses.get(conversation.provider_session.active_response_id or "")  # noqa: SLF001
            return bool(response and len(response.messages) >= 2)

        if await wait_until(two_messages, 15):
            found = conversation.provider_session.active_response_id
            await ctx.step(f"Two-message reply {found}: barge in during it (attempt {attempt + 1})")
            await call.say("interrupt")
            break
        await call.wait_reply_done(30)
        await call.settle(20)
    accelerator.SLOW_TOOL_SECONDS = 8
    if found is None:
        await ctx.check("Two-message reply observed", None, "not in 4 attempts")
    else:
        await call.wait_reply_done(30)
        response = conversation._responses[found]  # noqa: SLF001
        items = [conversation._items[i] for i in response.messages]  # noqa: SLF001
        record = call.interrupts[-1] if call.interrupts else None
        await ctx.check(
            "Barge-in cuts every unheard message: later messages truncated at 0 and left out of history",
            all(i.truncated for i in items[1:]) and all(i.dropped for i in items[1:]),
            {"played_ms": record["played_ms"] if record else None,
             "messages": [{"id": i.item_id, "truncated": i.truncated, "dropped": i.dropped, "status": i.status,
                           "generated_ms": round(sum(ms for (iid, _), ms in response.audio_ms.items() if iid == i.item_id))} for i in items]},
        )
    await call.settle(8)
    await ctx.hang_up(call)
    ctx.metrics["tool_gap_ms"] = tool_gaps_ms(conversation)
    await ctx.check("Function call → response_done gap with the model's own voice (ms)", None, ctx.metrics["tool_gap_ms"])


async def slow_tool(ctx: ScenarioContext) -> None:
    call = await ctx.open("voicelive-byom", create_response=True)
    conversation = call.conversation
    await call.wait_reply_done(30)
    await call.say("balance")
    await call.wait_kind("AgentSwitch", 30)
    await call.wait_reply_done(30)
    tool_log.clear()
    await ctx.step("Caller asks for payment history: a slow (8 s) tool starts")
    await call.say("history")
    await call.wait_kind("FunctionCall", 30, function="check_payment_history")
    await ctx.step("Caller keeps talking while it runs")
    await call.settle(1.0)
    await call.say("while_waiting")
    await call.wait_kind("FunctionResult", 30, function="check_payment_history")
    await call.wait_reply_done(30)
    await call.settle(2)
    await ctx.check(
        "Barge-in never cancels a running tool",
        [entry["event"] for entry in tool_log] == ["start", "done"],
        tool_log,
    )
    rows = history_rows(conversation)
    index = next(i for i, r in enumerate(rows) if r.get("call") == "check_payment_history")
    await ctx.check(
        "The result stays straight after its call, even though the caller spoke in between",
        rows[index + 1].get("call_id") == rows[index]["call_id"] and "result" in rows[index + 1],
        rows[index : index + 4],
    )
    responses = sorted(conversation._responses.values(), key=lambda r: r.created)  # noqa: SLF001
    tool_response = next(r for r in responses if any(c.content.name == "check_payment_history" for c in r.calls))
    call_done = next(c for c in tool_response.calls if c.content.name == "check_payment_history")
    later = [r for r in responses if r.created > tool_response.created]
    await ctx.check(
        "The follow-up waited for the response turn detection started",
        all((a.done_at or 0) <= b.created + 0.05 for a, b in zip(later, later[1:])),
        [{"id": r.response_id, "agent": r.agent.name, "created": round(r.created - tool_response.created, 2), "done": round((r.done_at or 0) - tool_response.created, 2), "status": r.status} for r in later],
    )
    ctx.metrics["tool_gap_ms"] = tool_gaps_ms(conversation)
    ctx.metrics["slow_tool_s"] = round(time.monotonic() - call_done.arrived, 1)
    await ctx.check("Function call → response_done gap with an Azure TTS voice (ms)", None, ctx.metrics["tool_gap_ms"])
    await ctx.hang_up(call)


async def hang_up_tool(ctx: ScenarioContext) -> None:
    call = await ctx.open("voicelive-byom", create_response=True)
    conversation = call.conversation
    await call.wait_reply_done(30)
    await call.say("balance")
    await call.wait_kind("AgentSwitch", 30)
    await call.wait_reply_done(30)
    tool_log.clear()
    await ctx.step("Slow tool starts; the caller hangs up 2 s into it")
    await call.say("history")
    await call.wait_kind("FunctionCall", 30, function="check_payment_history")
    await wait_until(lambda: any(e["event"] == "start" for e in tool_log), 10)
    await call.settle(2)
    await ctx.hang_up(call)
    await ctx.check("Closing cancels the running tool", [e["event"] for e in tool_log] == ["start", "cancelled"], tool_log)
    rows = history_rows(conversation)
    result = next((r for r in rows if r.get("result") and r.get("exception")), None)
    await ctx.check(
        "The call settles with conversation_closed and stays in history with its call",
        result is not None and result["exception"] == "conversation_closed",
        result,
    )
    streamed = [e for e in call.events if e["kind"] == "FunctionResult" and e["data"].get("exception") == "conversation_closed"]
    await ctx.check("The settled result streams to a reader still iterating", bool(streamed), streamed[:1])
    await ctx.check("Abandoned tools rebuilt from updates", call.loop.signals.abandoned_tools == 1, call.loop.signals.abandoned_tools)
    span = next((n for n, _ in walk(ctx.trace_of(call)) if n["name"] == "execute_tool check_payment_history"), None)
    await ctx.check(
        "execute_tool span for the cancelled call is ERROR with error.type=conversation_closed (needs the FunctionTool.invoke fix)",
        span is not None and span["status"] == "ERROR",
        {"status": span["status"] if span else None, "error.type": span["attributes"].get("error.type") if span else None},
    )


async def pending_transcript(ctx: ScenarioContext) -> None:
    for wait in (3.0, 0.0):
        call = await ctx.open("voicelive", create_response=True, close_transcript_wait=wait)
        conversation = call.conversation
        await call.wait_reply_done(30)
        await ctx.step(f"Caller speaks; hang up at speech_stopped (close waits up to {wait} s)")
        await call.say("account", wait=False)
        await call.wait_for(lambda e: e["kind"] == "Update" and any(c["type"] == "speech_stopped" for c in e["data"]["contents"]), 20)
        await ctx.hang_up(call)
        rows = [r for r in history_rows(conversation) if r["role"] == "user"]
        waited = conversation.stats["close_transcript_wait_s"]
        if wait:
            await ctx.check(
                "Close waits for the pending caller transcript and keeps it",
                rows and rows[-1].get("text") and waited < wait,
                {"waited_s": waited, "turn": rows[-1] if rows else None},
            )
        else:
            await ctx.check(
                "With no wait, the caller turn is kept text-less with a marker",
                rows and rows[-1].get("text_less") == "transcript_pending" or (rows and rows[-1].get("status") == "incomplete"),
                {"waited_s": waited, "turn": rows[-1] if rows else None},
            )


async def shared_mcp(ctx: ScenarioContext) -> None:
    session_before = id(getattr(ctx.orders, "session", None))
    await ctx.step("Two calls at once, both using the app-wide orders MCP server")
    calls = [await ctx.open("voicelive", create_response=True) for _ in range(2)]
    await asyncio.gather(*(c.wait_reply_done(30) for c in calls))
    await asyncio.gather(*(c.say("slots") for c in calls))
    results = await asyncio.gather(
        *(c.wait_kind("FunctionResult", 40, function="getDeliverySlots") for c in calls), return_exceptions=True
    )
    for c in calls:
        with contextlib.suppress(Exception):
            await c.wait_reply_done(20)
    for c in calls:
        await ctx.hang_up(c)
    await ctx.check(
        "Both calls ran the shared MCP tool",
        all(isinstance(r, dict) for r in results),
        [r["data"]["result"][:80] if isinstance(r, dict) else repr(r) for r in results],
    )
    await ctx.check(
        "The MCP connection was opened once and is still open after both calls",
        ctx.orders.is_connected and id(getattr(ctx.orders, "session", None)) == session_before,
        {"connected": ctx.orders.is_connected},
    )


class _Guardrail(ConversationMiddleware):
    def __init__(self, word: str, noop_hooks: int = 0) -> None:
        self.word = word
        self.noop_hooks = noop_hooks
        self.tripped: str | None = None
        self.dropped = 0
        self.after_trip_delivered = 0

    async def process(self, context: Any, call_next: Any) -> None:
        text: dict[str, str] = {}

        async def noop(update: Any) -> None:
            return None

        async def hook(update: Any) -> Any:
            if self.tripped and update.response_id == self.tripped:
                self.dropped += 1
                raise MiddlewareTermination("guardrail")
            for content in update.contents:
                if content.type == "text" and update.role == "assistant" and update.response_id:
                    text[update.response_id] = text.get(update.response_id, "") + (content.text or "")
                    if self.word in text[update.response_id].lower():
                        self.tripped = update.response_id
                        asyncio.get_running_loop().create_task(context.conversation.interrupt())
                        raise MiddlewareTermination("guardrail")
            return None

        context.update_hooks.extend([noop] * self.noop_hooks)
        context.update_hooks.append(hook)
        await call_next()


async def guardrail(ctx: ScenarioContext) -> None:
    timings = {}
    for hooks in (0, 20):
        guard = _Guardrail("pineapple", noop_hooks=hooks)

        def build(client: Any, guard: _Guardrail = guard) -> Any:
            return RealtimeAgent(
                client=client,
                name="assistant",
                instructions="You are a cheerful assistant. Do what the caller asks.",
                default_options={"voice": azure_voice("en-US-AvaMultilingualNeural"),
                                 "turn_detection": {"type": "server_vad", "silence_duration_ms": 800, "threshold": 0.8}},
                middleware=[guard],
            )

        call = await ctx.open("voicelive", build=build)
        conversation = call.conversation
        await ctx.step(f"Caller asks for 'pineapple' five times; the guardrail interrupts on the first one ({hooks} no-op hooks)")
        await call.say("pineapple")
        with contextlib.suppress(TimeoutError):
            await call.wait_kind("ResponseDone", 30)
        await call.settle(2)
        await ctx.hang_up(call)
        stats = conversation.stats
        timings[hooks] = round(stats["update_hook_s"] / max(stats["updates"], 1) * 1e6, 1)
        if hooks == 0:
            response = conversation._responses.get(guard.tripped or "")  # noqa: SLF001
            await ctx.check(
                "A guardrail hook interrupts and drops the rest of the response",
                guard.tripped is not None and response is not None and response.status == "cancelled" and guard.dropped > 0,
                {"tripped": guard.tripped, "status": response.status if response else None, "dropped_updates": guard.dropped},
            )
    await ctx.check("Update-hook cost per update (µs) with 1 vs 21 hooks, audio chunks included", None, timings)


async def instrumentors(ctx: ScenarioContext) -> None:
    import os

    from azure.ai.voicelive.telemetry import VoiceLiveInstrumentor

    from azure.core.settings import settings

    os.environ["AZURE_EXPERIMENTAL_ENABLE_GENAI_TRACING"] = "true"
    settings.tracing_implementation = "opentelemetry"
    instrumentor = VoiceLiveInstrumentor()
    instrumentor.instrument()
    records: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record.getMessage())

    handler = _Capture()
    logging.getLogger("agent_framework.realtime").addHandler(handler)
    try:
        call = await ctx.open("voicelive", create_response=True)
        await call.wait_reply_done(30)
        await call.say("hours")
        with contextlib.suppress(TimeoutError):
            await call.wait_reply_done(30)
        await ctx.hang_up(call)
    finally:
        instrumentor.uninstrument()
        logging.getLogger("agent_framework.realtime").removeHandler(handler)
    nodes = [n for n, _ in walk(ctx.trace_of(call))]
    sdk = [n for n in nodes if not n["name"].startswith(("ws.session", "invoke_agent", "realtime", "execute_tool"))]
    names: dict[str, int] = {}
    for node in sdk:
        names[node["name"]] = names.get(node["name"], 0) + 1
    await ctx.check(
        "The Voice Live client warns once when VoiceLiveInstrumentor is active",
        any("VoiceLiveInstrumentor" in r for r in records),
        [r for r in records if "Instrumentor" in r][:1],
    )
    await ctx.check("Duplicate SDK spans in the call's trace", None, {"sdk_spans": len(sdk), "by_name": names})
    from azure.ai.projects.aio import _realtime

    patched = any("wrapt" in type(getattr(_realtime.AsyncBetaRealtimeConnection, m, None)).__module__ for m in ("send", "recv"))
    await ctx.check(
        "AIProjectInstrumentor 2.7.0 doesn't patch realtime send/recv (it does from 2.9, per #9)",
        None,
        {"patched": patched},
    )


async def vad_cancelled(ctx: ScenarioContext) -> None:
    for silence_ms in (None, 200):
        def build(client: Any, silence_ms: int | None = silence_ms) -> Any:
            options: dict[str, Any] = {"voice": azure_voice("en-US-AvaMultilingualNeural")}
            if silence_ms:
                options["turn_detection"] = {"type": "server_vad", "silence_duration_ms": silence_ms}
            return RealtimeAgent(
                client=client, name="assistant", instructions="You are a helpful assistant for Acme Corporation.",
                default_options=options,
            )

        call = await ctx.open("voicelive", build=build)
        label = f"silence_duration_ms={silence_ms}" if silence_ms else "Voice Live's default"
        if silence_ms is None:
            await ctx.check("Voice Live's default turn detection", None, call.conversation.provider_session.session_state.get("turn_detection"))
        await ctx.step(f"A long caller turn with pauses ({label})")
        await call.say("long_answer")
        await call.settle(12)
        await ctx.hang_up(call)
        responses = list(call.conversation._responses.values())  # noqa: SLF001
        empty = [r for r in responses if r.status == "cancelled" and not (r.usage or {}).get("output_token_count")]
        spans = [n for n, _ in walk(ctx.trace_of(call)) if n["name"].startswith("realtime_inference")]
        await ctx.check(
            f"Empty cancelled responses become realtime_inference spans ({label})",
            None if not empty else len(spans) == len(responses),
            {"responses": len(responses), "cancelled_empty": len(empty), "reasons": [r.reason for r in empty],
             "span_finish_reasons": [n["attributes"].get("gen_ai.response.finish_reasons") for n in spans]},
        )


SCENARIOS = [
    Scenario(
        "core-loop", "The accelerator's core voice loop", "#10, #7, #9, #14, #17", "voicelive-byom",
        "Greeting, one tool call, a silent in-session handoff with per-agent Azure voices, a billing tool, and the "
        "content-filter fallback line through say(), on Voice Live BYOM (the only Voice Live mode that filters).",
        "The agent label flips on handoff_completed; handoff calls never show; the fallback is spoken word for word; "
        "the span tree nests every response and tool under one conversation span.",
        ["Greeting", "Office hours tool", "Handoff to billing", "Balance tool", "Jailbreak → fallback", "Hang up"],
        core_loop,
    ),
    Scenario(
        "manual-handoff", "App-driven handoff", "#7, #9", "voicelive",
        "The app calls conversation.handoff(support) itself, outside any tool, then tries a non-participant.",
        "A realtime.handoff span appears; the non-participant raises before any session update.",
        ["Greeting", "handoff(support)", "handoff(stranger) raises", "Caller: login problem", "Hang up"],
        manual_handoff,
    ),
    Scenario(
        "handoff-rejected", "Rejected handoff and the lost race", "#7, #8, #12", "voicelive",
        "Billing has an invalid voice, so Voice Live rejects the handoff's session update. Then create_response() races "
        "an active response.",
        "The handoff tool fails in ~60 ms, not 10 s; the receptionist carries on; Voice Live drops the raced request.",
        ["Greeting", "Caller: balance → rejected", "create_response() while active", "Bypass → dropped", "Hang up"],
        handoff_rejected,
    ),
    Scenario(
        "two-handoffs", "Two handoff calls in one response", "#7", "voicelive-byom",
        "The caller asks for billing and support at once. If the model won't emit both calls, they run as one batch.",
        "Exactly one handoff wins; the other gets an error result.",
        ["Greeting", "Caller: both at once", "Hang up"],
        two_handoffs,
    ),
    Scenario(
        "foundry", "Foundry voice agent as the start agent", "#7, #8, #9", "foundry",
        "Starts on the published receptionist-voice agent, hands off to billing on its client, then back.",
        "The definition is captured; its tools survive the added handoff tools; handing back restores it.",
        ["Connect + capture definition", "Caller: balance → billing", "Caller: general → receptionist", "Hang up"],
        foundry,
    ),
    Scenario(
        "barge-in", "Barge-in keeps only the heard text", "#12, #17, #8", "voicelive-byom",
        "The caller talks over a long reply about 4 s in; the app calls interrupt(played_ms=).",
        "History keeps the heard prefix, marked incomplete; item_truncated streams and lands on the span.",
        ["Greeting", "Caller: long question", "Caller interrupts", "Hang up"],
        barge_in,
    ),
    Scenario(
        "aoai", "Azure OpenAI: API key, voice lock, two-message barge-in", "#8, #13", "aoai",
        "Connects with an API key as a bearer token, hands off (voice can't change), then barges into a two-message reply.",
        "Billing keeps the receptionist's voice; every unheard message after the cut is truncated at 0.",
        ["API key connect", "Caller: balance → billing", "Slow tool + 'ready yet?'", "Barge in", "Hang up"],
        aoai,
    ),
    Scenario(
        "slow-tool", "Barge-in during a slow tool", "#16, #17, #13", "voicelive-byom",
        "An 8 s tool runs while the caller keeps talking.",
        "The tool isn't cancelled; its result stays right after its call; the follow-up waits its turn.",
        ["Handoff to billing", "Caller: payment history (slow)", "Caller talks meanwhile", "Result + follow-up", "Hang up"],
        slow_tool,
    ),
    Scenario(
        "hang-up-tool", "Hang-up with a tool still running", "#16, #9, #17", "voicelive-byom",
        "The caller hangs up 2 s into the slow tool.",
        "The tool is cancelled; the call settles with conversation_closed in history and updates.",
        ["Handoff to billing", "Caller: payment history (slow)", "Hang up mid-tool"],
        hang_up_tool,
    ),
    Scenario(
        "pending-transcript", "Hang-up with a caller transcript pending", "#17, #13", "voicelive",
        "The caller hangs up right as they stop talking, once with close's 3 s wait and once with none.",
        "With the wait, the transcript lands in history; without it, a text-less caller turn with a marker.",
        ["Caller speaks; hang up (3 s wait)", "Caller speaks; hang up (no wait)"],
        pending_transcript,
    ),
    Scenario(
        "shared-mcp", "App-wide MCP tools shared across calls", "#16", "voicelive",
        "Two concurrent calls use one connected orders MCP server.",
        "Both get delivery slots; the server connection is opened once.",
        ["Two calls", "Both ask for slots", "Hang up both"],
        shared_mcp,
    ),
    Scenario(
        "guardrail", "Guardrail middleware and hook cost", "#9", "voicelive",
        "A conversation middleware hook interrupts and drops a response that says 'pineapple'; then 20 no-op hooks.",
        "The response is cancelled and its remaining updates never reach the app.",
        ["Guardrail trips", "Measure hook cost"],
        guardrail,
    ),
    Scenario(
        "instrumentors", "Azure SDK instrumentors", "#9", "voicelive",
        "VoiceLiveInstrumentor on, alongside AF's realtime spans.",
        "AF warns once; the trace shows the SDK's duplicate spans.",
        ["Instrument", "Short call", "Count spans"],
        instrumentors,
    ),
    Scenario(
        "vad-cancelled", "Empty cancelled responses under default VAD", "#9, #13", "voicelive",
        "Voice Live's default server VAD splits a long caller turn.",
        "Zero-usage cancelled responses show as realtime_inference spans.",
        ["Long caller turn", "Count cancelled responses"],
        vad_cancelled,
    ),
]

# endregion


async def run_scenario(scenario: Scenario, credential: Any, *, orders: Any, emit: Emit | None = None) -> dict[str, Any]:
    telemetry.configure()
    ctx = ScenarioContext(scenario, credential, orders, emit)
    error = None
    started = time.time()
    try:
        await scenario.run(ctx)
    except Exception as exc:  # noqa: BLE001
        error = "".join(traceback.format_exception(exc))[-2000:]
        ctx.log(f"ERROR: {exc!r}")
        await ctx._send({"kind": "Error", "message": repr(exc)})  # noqa: SLF001
    finally:
        await ctx.close_all()
    result = {
        "id": scenario.id,
        "title": scenario.title,
        "provider": scenario.provider,
        "started": started,
        "seconds": round(time.time() - started, 1),
        "checks": ctx.checks,
        "notes": ctx.notes,
        "metrics": ctx.metrics,
        "error": error,
        "calls": [
            {
                "history": history_rows(o.conversation),
                "signals": o.call.loop.signals.__dict__,
                "interrupts": o.call.interrupts,
                "stats": o.conversation.stats,
                "ack_ms": [(a, round(s * 1000)) for a, s in o.conversation.provider_session.timings],
                "reports": o.conversation.report_log,
                "spans": telemetry.render(telemetry.span_tree(o.span.get_span_context().trace_id)),
                "events": [e for e in o.call.events if e["kind"] != "Update"],
                "wire": [
                    w for w in o.conversation.provider_session.wire_log
                    if not str(w["event"].get("type", "")).endswith(".delta")
                ],
            }
            for o in ctx.opened
            if o.conversation.provider_session is not None
        ],
        "log": ctx.log_lines,
        "core_changes": CORE_CHANGES,
    }
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / f"{scenario.id}.json").write_text(json.dumps(result, indent=2, default=str))
    return result


async def main(ids: list[str]) -> None:
    from azure.identity.aio import AzureCliCredential

    from .accelerator import orders_mcp

    credential = AzureCliCredential()
    orders = orders_mcp()
    await orders.connect()
    summary = []
    try:
        for scenario in SCENARIOS:
            if ids and scenario.id not in ids:
                continue
            print(f"\n===== {scenario.id}: {scenario.title} =====", flush=True)
            result = await run_scenario(scenario, credential, orders=orders)
            passed = sum(1 for c in result["checks"] if c["passed"] is True)
            failed = sum(1 for c in result["checks"] if c["passed"] is False)
            summary.append(f"{scenario.id}: {passed} passed, {failed} failed{' , ERROR' if result['error'] else ''}")
    finally:
        await orders.close()
        await credential.close()
    print("\n" + "\n".join(summary))


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("azure").setLevel(logging.ERROR)
    asyncio.run(main(sys.argv[1:]))
