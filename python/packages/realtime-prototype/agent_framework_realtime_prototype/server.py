# PROTOTYPE, never merged (adamdougal/agent-framework#10).
"""The prototype debugger: a throwaway server modelled on the accelerator's ``/ws`` call route and debugger page.

Run: ``uv run python -m agent_framework_realtime_prototype.server`` then open http://localhost:8765
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import time
from pathlib import Path
from typing import Any

import uvicorn
from azure.identity.aio import AzureCliCredential
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from opentelemetry import trace

from . import CORE_CHANGES, telemetry
from .accelerator import PROVIDERS, CONTENT_FILTER_FALLBACK, VoiceLoop, build_contact_centre, make_client, orders_mcp
from .harness import FRAME_BYTES, FRAME_MS, load_clip
from .make_clips import CLIPS

logger = logging.getLogger("prototype.server")
STATIC = Path(__file__).parent / "static"
app = FastAPI(title="PROTOTYPE realtime voice loop")
tracer = trace.get_tracer("prototype.app")
STATE: dict[str, Any] = {}


@app.on_event("startup")
async def startup() -> None:
    telemetry.configure()
    STATE["credential"] = AzureCliCredential()
    # App-wide MCP tools: connected once, shared by every call (#16).
    STATE["orders"] = orders_mcp()
    await STATE["orders"].connect()


@app.on_event("shutdown")
async def shutdown() -> None:
    with contextlib.suppress(Exception):
        await STATE["orders"].close()
    await STATE["credential"].close()


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/api/info")
async def info() -> dict[str, Any]:
    from .scenarios import SCENARIOS

    return {
        "providers": list(PROVIDERS),
        "clips": {name: text for name, text in CLIPS.items()},
        "fallback": CONTENT_FILTER_FALLBACK,
        "coreChanges": CORE_CHANGES,
        "scenarios": [scenario.describe() for scenario in SCENARIOS],
    }


def history_view(conversation: Any) -> list[dict[str, Any]]:
    rows = []
    for message in conversation.history:
        parts = []
        for content in message.contents:
            if content.type == "text":
                parts.append({"type": "text", "text": content.text, "status": content.status})
            elif content.type == "function_call":
                parts.append({"type": "function_call", "name": content.name, "arguments": content.arguments})
            elif content.type == "function_result":
                parts.append({
                    "type": "function_result",
                    "result": str(content.result),
                    "exception": content.exception,
                })
        rows.append({
            "role": message.role,
            "author": message.author_name,
            "messageId": message.message_id,
            "textLess": (message.additional_properties or {}).get("realtime", {}).get("text_less"),
            "contents": parts,
        })
    return rows


def snapshot(conversation: Any, loop: VoiceLoop | None) -> dict[str, Any]:
    session = conversation.provider_session
    calls = [
        {"name": call.content.name, "state": call.state, "agent": call.agent.name, "callId": call.content.call_id}
        for call in conversation._calls.values()  # noqa: SLF001
    ]
    return {
        "activeAgent": conversation.active_agent.name,
        "sessionId": conversation.session_id,
        "activeResponse": session.active_response_id if session else None,
        "exchange": conversation._exchange,  # noqa: SLF001
        "history": history_view(conversation),
        "reports": conversation.report_log,
        "calls": calls,
        "stats": conversation.stats,
        "signals": loop.signals.__dict__ if loop else None,
        "ackTimings": [(ack, round(seconds * 1000)) for ack, seconds in (session.timings if session else [])],
        "unechoedCorrelations": getattr(session, "unechoed_correlations", 0),
    }


class _Pacer:
    """The server-side 'mic': forwards browser frames, or injects a clip at real time and drops browser frames."""

    def __init__(self, conversation: Any) -> None:
        self.conversation = conversation
        self.clip = b""
        self.clip_mode = False
        self.task = asyncio.create_task(self._run())

    async def browser_frame(self, data: bytes) -> None:
        if not self.clip and not self.clip_mode:
            await self.conversation.send(data)

    async def _run(self) -> None:
        next_send = time.monotonic()
        silent = b"\x00" * FRAME_BYTES
        while True:
            if self.clip:
                frame, self.clip = self.clip[:FRAME_BYTES], self.clip[FRAME_BYTES:]
            elif self.clip_mode:
                frame = silent
            else:
                frame = None
            if frame is not None:
                with contextlib.suppress(Exception):
                    await self.conversation.send(frame)
            next_send += FRAME_MS / 1000
            await asyncio.sleep(max(0, next_send - time.monotonic()))


@app.websocket("/ws")
async def call(websocket: WebSocket) -> None:  # noqa: C901
    await websocket.accept()
    provider = websocket.query_params.get("provider", "voicelive-byom")
    clip_mode = websocket.query_params.get("mode") == "clips"
    send_lock = asyncio.Lock()

    async def send(event: dict[str, Any]) -> None:
        async with send_lock:
            with contextlib.suppress(Exception):
                await websocket.send_text(json.dumps(event, default=str))

    client = make_client(provider, STATE["credential"])
    agent = build_contact_centre(client, provider, mcp_tools=[STATE["orders"]])
    with tracer.start_as_current_span("ws.session", attributes={"provider": provider}) as root:
        trace_id = root.get_span_context().trace_id
        conversation = agent.start_conversation(create_response=True, session=agent.create_session())
        try:
            await conversation.__aenter__()
        except Exception as exc:  # noqa: BLE001
            await send({"kind": "Error", "message": f"Couldn't start the call: {exc!r}"})
            await websocket.close()
            return
        await send({"kind": "Started", "provider": provider, "sessionId": conversation.session_id})
        loop = VoiceLoop(conversation, send)
        pacer = _Pacer(conversation)
        pacer.clip_mode = clip_mode

        async def run_loop() -> None:
            try:
                await loop.run()
            except Exception as exc:  # noqa: BLE001
                await send({"kind": "Error", "message": f"Update loop ended: {exc!r}"})

        async def push_state() -> None:
            while True:
                await asyncio.sleep(0.5)
                await send({"kind": "State", "data": snapshot(conversation, loop)})

        async def control(message: dict[str, Any]) -> None:
            action = message.get("action")
            try:
                if action == "say":
                    await conversation.say(message.get("text") or CONTENT_FILTER_FALLBACK)
                elif action == "interrupt":
                    await conversation.interrupt(played_ms=message.get("playedMs"))
                elif action == "handoff":
                    target = next(p for p in agent.participants_list if p.name == message["target"])
                    await conversation.handoff(target)
                elif action == "create_response":
                    await conversation.create_response()
                elif action == "send_text":
                    await conversation.send(message["text"])
                elif action == "clip":
                    pacer.clip += load_clip(message["clip"])
                await send({"kind": "ControlResult", "action": action, "ok": True})
            except Exception as exc:  # noqa: BLE001
                await send({"kind": "ControlResult", "action": action, "ok": False, "error": repr(exc)})

        loop_task = asyncio.create_task(run_loop())
        state_task = asyncio.create_task(push_state())
        try:
            while True:
                message = json.loads(await websocket.receive_text())
                kind = message.get("kind")
                if kind == "AudioData":
                    await pacer.browser_frame(base64.b64decode(message["audioData"]["data"]))
                elif kind == "Interrupt":
                    asyncio.create_task(control({"action": "interrupt", "playedMs": message.get("playedMs")}))
                elif kind == "Control":
                    if message.get("action") == "hangup":
                        break
                    asyncio.create_task(control(message))
        except WebSocketDisconnect:
            pass
        finally:
            pacer.task.cancel()
            state_task.cancel()
            await conversation.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(loop_task, 10)
    await send({"kind": "State", "data": snapshot(conversation, loop)})
    await send({
        "kind": "Ended",
        "history": history_view(conversation),
        "spans": telemetry.span_tree(trace_id),
        "wire": conversation.provider_session.wire_log[-400:],
    })
    with contextlib.suppress(Exception):
        await websocket.close()


@app.websocket("/scenario")
async def scenario(websocket: WebSocket) -> None:
    from .scenarios import SCENARIOS, run_scenario

    await websocket.accept()
    scenario_id = websocket.query_params["id"]
    selected = next(s for s in SCENARIOS if s.id == scenario_id)
    send_lock = asyncio.Lock()

    async def send(event: dict[str, Any]) -> None:
        async with send_lock:
            with contextlib.suppress(Exception):
                await websocket.send_text(json.dumps(event, default=str))

    result = await run_scenario(selected, STATE["credential"], orders=STATE["orders"], emit=send)
    await send({"kind": "ScenarioDone", "result": result})
    with contextlib.suppress(Exception):
        await websocket.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    uvicorn.run(app, host="127.0.0.1", port=8765, log_level="warning")


if __name__ == "__main__":
    main()
