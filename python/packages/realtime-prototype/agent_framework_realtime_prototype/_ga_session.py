# PROTOTYPE, never merged (adamdougal/agent-framework#10).
"""The shared GA-protocol base session (``OpenAIRealtimeSession``) and ``OpenAIRealtimeClient`` with Azure routing (#8).

The base session maps wire JSON, not SDK models. Each client opens a ``RealtimeConnection`` through its service's SDK,
used only as a transport. Voice Live's and Foundry's sessions subclass this and override only what differs.
"""

from __future__ import annotations

import asyncio
import base64
import json
import time
import uuid
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from agent_framework import Content, FunctionTool

from ._core import (
    ERROR,
    FUNCTION_CALL,
    INPUT_TRANSCRIPTION,
    INPUT_TRANSCRIPTION_FAILED,
    ITEM_ADDED,
    ITEM_TRUNCATED,
    OUTPUT_AUDIO,
    OUTPUT_TEXT,
    PASSTHROUGH,
    RESPONSE_CREATED,
    RESPONSE_DONE,
    SESSION_CREATED,
    SESSION_UPDATED,
    SPEECH_STARTED,
    SPEECH_STOPPED,
    USAGE,
    RealtimeActiveResponseError,
    RealtimeControlError,
    RealtimeControlTimeoutError,
    RealtimeError,
    RealtimeEvent,
    event_content,
    logger,
    text_content,
)

COGNITIVE_SCOPE = "https://cognitiveservices.azure.com/.default"
AI_SCOPE = "https://ai.azure.com/.default"
SAMPLE_RATE = 24000


class RealtimeConnection(Protocol):
    """What a client's SDK transport must provide: raw wire JSON both ways."""

    close_code: int | None

    async def send(self, event: dict[str, Any]) -> None: ...

    async def recv(self) -> dict[str, Any] | None:
        """The next wire event, or None once the socket closed normally."""
        ...

    async def close(self) -> None: ...


@dataclass
class _Pending:
    ack: str
    event_id: str
    future: asyncio.Future[Any]
    request_id: str | None = None
    allow_unechoed: bool = False


def _bytes_ms(data: bytes) -> float:
    return len(data) / (SAMPLE_RATE * 2) * 1000


def declare_tool(tool: Any) -> dict[str, Any]:
    if isinstance(tool, dict):
        return tool
    if isinstance(tool, FunctionTool):
        return {
            "type": "function",
            "name": tool.name,
            "description": tool.description or "",
            "parameters": tool.parameters(),
        }
    raise TypeError(f"Can't declare tool {tool!r}")


def _media_type_to_format(media_type: str | None) -> dict[str, Any] | None:
    if not media_type:
        return None
    if media_type.startswith("audio/pcm"):
        rate = int(media_type.split("rate=")[1]) if "rate=" in media_type else SAMPLE_RATE
        return {"type": "audio/pcm", "rate": rate}
    if media_type in ("audio/pcmu", "audio/pcma"):
        return {"type": media_type}
    raise RealtimeError(f"Unsupported audio format {media_type}")


class OpenAIRealtimeSession:
    """GA-protocol session over a raw connection. Holds #12's typed controls, #14's kinds and #8's error claiming."""

    metadata_namespace = "openai"
    assistant_text_part = "output_text"
    allows_voice_change_after_audio = False

    def __init__(self, connection: RealtimeConnection, *, ack_timeout: float = 10.0) -> None:
        self.connection = connection
        self.ack_timeout = ack_timeout
        self.active_response_id: str | None = None
        self.idle = asyncio.Event()
        self.idle.set()
        self.session_state: dict[str, Any] = {}
        self.session_id: str | None = None
        self.model: str | None = None
        self.heard_audio = False
        self.timings: list[tuple[str, float]] = []
        self._pending: _Pending | None = None
        self._sent_ids: set[str] = set()
        self._last_error: tuple[Any, ...] | None = None
        self._buffered: list[dict[str, Any]] = []
        self._closed = False
        self._t0 = time.monotonic()
        self.wire_log: list[dict[str, Any]] = []
        self.unechoed_correlations = 0
        self.deduplicated_errors = 0

    def _log_wire(self, direction: str, event: dict[str, Any]) -> None:
        if len(self.wire_log) > 5000:
            del self.wire_log[:1000]
        redacted = {
            key: ("<audio>" if key in ("audio", "delta") and "audio" in str(event.get("type", "")) else value)
            for key, value in event.items()
        }
        self.wire_log.append({"t": round(time.monotonic() - self._t0, 3), "dir": direction, "event": redacted})

    # region payloads

    def build_session(self, options: dict[str, Any], tools: Sequence[Any] | None) -> dict[str, Any]:
        session: dict[str, Any] = {}
        audio_in: dict[str, Any] = {}
        audio_out: dict[str, Any] = {}
        for key, value in options.items():
            match key:
                case "instructions":
                    session["instructions"] = value
                case "voice":
                    if self.heard_audio and not self.allows_voice_change_after_audio:
                        logger.warning("Dropping voice: the provider can't change voice after the first audio.")
                        continue
                    if isinstance(value, dict):
                        logger.warning("Dropping Azure voice object: this provider takes OpenAI voice names only.")
                        continue
                    audio_out["voice"] = value
                case "tool_choice":
                    session["tool_choice"] = value
                case "max_tokens":
                    session["max_output_tokens"] = value
                case "output_modalities":
                    session["output_modalities"] = value
                case "input_audio_format":
                    audio_in["format"] = _media_type_to_format(value)
                case "output_audio_format":
                    audio_out["format"] = _media_type_to_format(value)
                case "turn_detection":
                    audio_in["turn_detection"] = value
                case "transcription":
                    audio_in["transcription"] = value
                case "noise_reduction":
                    audio_in["noise_reduction"] = value
                case _:
                    logger.warning("Dropping option %r: this provider can't honour it.", key)
        if audio_in:
            session.setdefault("audio", {})["input"] = audio_in
        if audio_out:
            session.setdefault("audio", {})["output"] = audio_out
        if tools is not None:
            session["tools"] = [declare_tool(tool) for tool in tools]
        if session:
            session["type"] = "realtime"
        return session

    def build_response(self, options: dict[str, Any]) -> dict[str, Any]:
        response: dict[str, Any] = {}
        for key, value in options.items():
            match key:
                case "instructions" | "tool_choice" | "metadata":
                    response[key] = value
                case "max_tokens":
                    response["max_output_tokens"] = value
                case _:
                    logger.warning("Dropping response option %r.", key)
        return response

    def say_response(self, text: str) -> dict[str, Any]:
        return {"instructions": f"Say exactly the following, word for word, and nothing else: {text}"}

    def message_item(self, role: str, text: str) -> dict[str, Any]:
        part = {"user": "input_text", "system": "input_text", "assistant": self.assistant_text_part}[role]
        return {"type": "message", "role": role, "content": [{"type": part, "text": text}]}

    # endregion

    # region sending

    async def send_raw(self, event: dict[str, Any]) -> None:
        if self._closed:
            raise RealtimeError("Realtime session is closed.")
        if event.get("type") != "input_audio_buffer.append":
            self._log_wire("out", event)
        await self.connection.send(event)

    async def _control(
        self, event: dict[str, Any], *, ack: str, request_id: str | None = None, allow_unechoed: bool = False
    ) -> Any:
        event_id = f"af_{uuid.uuid4().hex[:12]}"
        event["event_id"] = event_id
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending = _Pending(ack, event_id, future, request_id, allow_unechoed)
        self._sent_ids.add(event_id)
        sent = time.monotonic()
        try:
            await self.send_raw(event)
            return await asyncio.wait_for(future, self.ack_timeout)
        except TimeoutError as exc:
            raise RealtimeControlTimeoutError(f"No {ack} within {self.ack_timeout}s.") from exc
        finally:
            self.timings.append((ack, time.monotonic() - sent))
            self._pending = None

    async def update_session(
        self, options: dict[str, Any], tools: Sequence[Any] | None, *, force: bool = False
    ) -> dict[str, Any] | None:
        payload = self.build_session(options, tools)
        if not payload and not force:
            return None
        return await self._control({"type": "session.update", "session": payload}, ack="session.updated")

    async def create_response(self, options: dict[str, Any] | None = None) -> str:
        return await self._create_response(self.build_response(dict(options or {})))

    async def say(self, text: str) -> str:
        return await self._create_response(self.say_response(text))

    async def _create_response(self, response: dict[str, Any]) -> str:
        if self.active_response_id is not None:
            raise RealtimeActiveResponseError(f"Response {self.active_response_id} is active.")
        request_id = uuid.uuid4().hex[:12]
        response.setdefault("metadata", {})["af_request_id"] = request_id
        # Voice Live returns metadata: null for pre_generated_assistant_message responses (prototype finding), so those
        # fall back to #12's "first response.created after the send".
        return await self._control(
            {"type": "response.create", "response": response},
            ack="response.created",
            request_id=request_id,
            allow_unechoed="pre_generated_assistant_message" in response,
        )

    async def cancel_response(self) -> bool:
        if self.active_response_id is None:
            return False
        await self.send_raw({"type": "response.cancel", "event_id": f"af_{uuid.uuid4().hex[:12]}"})
        return True

    async def truncate(self, item_id: str, content_index: int, audio_end_ms: int) -> None:
        await self._control(
            {
                "type": "conversation.item.truncate",
                "item_id": item_id,
                "content_index": content_index,
                "audio_end_ms": audio_end_ms,
            },
            ack="conversation.item.truncated",
        )

    async def append_audio(self, data: bytes) -> None:
        await self.send_raw({"type": "input_audio_buffer.append", "audio": base64.b64encode(data).decode()})

    async def create_item(self, item: dict[str, Any]) -> None:
        await self.send_raw({"type": "conversation.item.create", "item": item})

    async def send_function_output(self, call_id: str, output: str) -> None:
        await self.create_item({"type": "function_call_output", "call_id": call_id, "output": output})

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            await self.connection.close()

    # endregion

    # region receiving

    async def receive(self) -> AsyncIterator[RealtimeEvent]:
        while True:
            if self._buffered:
                wire = self._buffered.pop(0)
            else:
                wire = await self.connection.recv()
                if wire is None:
                    return
            for event in self.handle_wire(wire):
                yield event

    def handle_wire(self, wire: dict[str, Any]) -> list[RealtimeEvent]:
        """Resolve acknowledgements and claim errors, then map the event to kinds."""
        event_type = wire.get("type", "")
        self._log_wire("in", wire)
        pending = self._pending
        if event_type == "error":
            error = wire.get("error") or {}
            key = (error.get("code"), error.get("message"), error.get("param"))
            now = time.monotonic()
            error_event_id = error.get("event_id")
            claims = (
                pending is not None
                and not pending.future.done()
                and (error_event_id is None or error_event_id == pending.event_id or error_event_id not in self._sent_ids)
            )
            # A waiting control claims first. Deduplicating first swallowed a retried handoff's identical rejection,
            # which arrived within a second of the first one, leaving the retry to its 10 s timeout (prototype finding).
            if not claims and self._last_error and self._last_error[0] == key and now - self._last_error[1] < 1.0:
                self.deduplicated_errors += 1
                return []
            self._last_error = (key, now)
            if claims and pending is not None:
                pending.future.set_exception(
                    RealtimeControlError(error.get("message", ""), code=error.get("code"), param=error.get("param"))
                )
                return [self._passthrough(wire)]
            if error.get("code") == "response_cancel_not_active":
                return [self._passthrough(wire)]
        elif event_type == "session.created":
            session = wire.get("session") or {}
            self.session_id = session.get("id")
            self.model = session.get("model")
            self.session_state = session
        elif event_type == "session.updated":
            self.session_state = wire.get("session") or {}
            if pending and pending.ack == "session.updated" and not pending.future.done():
                pending.future.set_result(self.session_state)
        elif event_type == "response.created":
            response = wire.get("response") or {}
            self.active_response_id = response.get("id")
            self.idle.clear()
            if pending and pending.ack == "response.created" and not pending.future.done():
                metadata = response.get("metadata")
                echoed = (metadata or {}).get("af_request_id")
                if echoed == pending.request_id or (metadata is None and pending.allow_unechoed):
                    if metadata is None:
                        self.unechoed_correlations += 1
                    pending.future.set_result(response.get("id"))
                else:
                    pending.future.set_exception(
                        RealtimeActiveResponseError("The provider dropped the request: another response started.")
                    )
        elif event_type == "response.done":
            self.active_response_id = None
            self.idle.set()
        elif event_type == "conversation.item.truncated":
            if pending and pending.ack == "conversation.item.truncated" and not pending.future.done():
                pending.future.set_result(None)
        elif event_type in ("response.output_audio.delta", "response.audio.delta"):
            self.heard_audio = True
        return self.map_event(wire)

    def _passthrough(self, wire: dict[str, Any]) -> RealtimeEvent:
        payload = {key: value for key, value in wire.items() if key not in ("audio", "delta")}
        return RealtimeEvent(
            PASSTHROUGH,
            wire.get("type"),
            content=event_content(PASSTHROUGH, wire.get("type"), **{self.metadata_namespace: payload}),
            raw_representation=wire,
        )

    def map_event(self, wire: dict[str, Any]) -> list[RealtimeEvent]:  # noqa: C901
        event_type: str = wire.get("type", "")

        def signal(kind: str, **attributes: Any) -> RealtimeEvent:
            return RealtimeEvent(
                kind, event_type, event_content(kind, event_type, **attributes), attributes, raw_representation=wire
            )

        match event_type:
            case "session.created":
                session = wire.get("session") or {}
                return [signal(SESSION_CREATED, session_id=session.get("id"), model=session.get("model"))]
            case "session.updated":
                return [signal(SESSION_UPDATED, voice=self.current_voice())]
            case "input_audio_buffer.speech_started":
                return [signal(SPEECH_STARTED, audio_start_ms=wire.get("audio_start_ms"), item_id=wire.get("item_id"))]
            case "input_audio_buffer.speech_stopped":
                return [signal(SPEECH_STOPPED, audio_end_ms=wire.get("audio_end_ms"), item_id=wire.get("item_id"))]
            case "response.created":
                response = wire.get("response") or {}
                return [signal(RESPONSE_CREATED, response_id=response.get("id"), metadata=response.get("metadata"))]
            case "response.done":
                response = wire.get("response") or {}
                details = response.get("status_details") or {}
                events = [
                    signal(
                        RESPONSE_DONE,
                        response_id=response.get("id"),
                        status=response.get("status"),
                        reason=details.get("reason"),
                        error=details.get("error"),
                        output_types=[item.get("type") for item in response.get("output") or []],
                    )
                ]
                usage = response.get("usage")
                if usage:
                    details_in = usage.get("input_token_details") or {}
                    details_out = usage.get("output_token_details") or {}
                    usage_details = {
                        "input_token_count": usage.get("input_tokens"),
                        "output_token_count": usage.get("output_tokens"),
                        "total_token_count": usage.get("total_tokens"),
                        "cache_read_input_token_count": details_in.get("cached_tokens"),
                        "audio_input_token_count": details_in.get("audio_tokens"),
                        "text_input_token_count": details_in.get("text_tokens"),
                        "audio_output_token_count": details_out.get("audio_tokens"),
                        "text_output_token_count": details_out.get("text_tokens"),
                    }
                    if details_out.get("reasoning_tokens"):
                        usage_details["reasoning_output_token_count"] = details_out["reasoning_tokens"]
                    events.append(
                        RealtimeEvent(
                            USAGE,
                            event_type,
                            Content.from_usage(usage_details),  # type: ignore[arg-type]
                            {"response_id": response.get("id")},
                            wire,
                        )
                    )
                return events
            case "conversation.item.added" | "conversation.item.created":
                item = wire.get("item") or {}
                text = " ".join(
                    part.get("text") or part.get("transcript") or ""
                    for part in item.get("content") or []
                    if part.get("type") in ("input_text", "text", "output_text")
                )
                return [
                    signal(
                        ITEM_ADDED,
                        item_id=item.get("id"),
                        previous_item_id=wire.get("previous_item_id"),
                        item_type=item.get("type"),
                        role=item.get("role"),
                        call_id=item.get("call_id"),
                        name=item.get("name"),
                        phase=item.get("phase"),
                        content_types=[part.get("type") for part in item.get("content") or []],
                        text=text or None,
                    )
                ]
            case "conversation.item.truncated":
                return [
                    signal(
                        ITEM_TRUNCATED,
                        item_id=wire.get("item_id"),
                        content_index=wire.get("content_index"),
                        audio_end_ms=wire.get("audio_end_ms"),
                    )
                ]
            case "conversation.item.input_audio_transcription.delta":
                return [
                    RealtimeEvent(
                        INPUT_TRANSCRIPTION,
                        event_type,
                        text_content(wire.get("delta") or ""),
                        {"item_id": wire.get("item_id"), "merge_mode": "append"},
                        wire,
                    )
                ]
            case "conversation.item.input_audio_transcription.completed":
                return [
                    RealtimeEvent(
                        INPUT_TRANSCRIPTION,
                        event_type,
                        text_content(wire.get("transcript") or "", status="completed"),
                        {"item_id": wire.get("item_id"), "merge_mode": "replace"},
                        wire,
                    )
                ]
            case "conversation.item.input_audio_transcription.failed":
                return [signal(INPUT_TRANSCRIPTION_FAILED, item_id=wire.get("item_id"), error=wire.get("error"))]
            case (
                "response.output_audio_transcript.delta"
                | "response.output_text.delta"
                | "response.audio_transcript.delta"
                | "response.text.delta"
            ):
                return [
                    RealtimeEvent(
                        OUTPUT_TEXT,
                        event_type,
                        text_content(wire.get("delta") or ""),
                        {
                            "response_id": wire.get("response_id"),
                            "item_id": wire.get("item_id"),
                            "content_index": wire.get("content_index"),
                            "merge_mode": "append",
                        },
                        wire,
                    )
                ]
            case (
                "response.output_audio_transcript.done"
                | "response.output_text.done"
                | "response.audio_transcript.done"
                | "response.text.done"
            ):
                return [
                    RealtimeEvent(
                        OUTPUT_TEXT,
                        event_type,
                        text_content(wire.get("transcript") or wire.get("text") or "", status="completed"),
                        {
                            "response_id": wire.get("response_id"),
                            "item_id": wire.get("item_id"),
                            "content_index": wire.get("content_index"),
                            "merge_mode": "replace",
                        },
                        wire,
                    )
                ]
            case "response.output_audio.delta" | "response.audio.delta":
                data = base64.b64decode(wire.get("delta") or "")
                return [
                    RealtimeEvent(
                        OUTPUT_AUDIO,
                        event_type,
                        Content.from_data(data=data, media_type="audio/pcm;rate=24000"),
                        {
                            "response_id": wire.get("response_id"),
                            "item_id": wire.get("item_id"),
                            "content_index": wire.get("content_index"),
                            "audio_ms": _bytes_ms(data),
                            "audio": data,
                        },
                        None,
                    )
                ]
            case "response.output_item.done" if (wire.get("item") or {}).get("type") == "function_call":
                item = wire["item"]
                return [
                    RealtimeEvent(
                        FUNCTION_CALL,
                        event_type,
                        Content.from_function_call(
                            call_id=item.get("call_id"), name=item.get("name"), arguments=item.get("arguments")
                        ),
                        {"response_id": wire.get("response_id"), "item_id": item.get("id")},
                        wire,
                    )
                ]
            case "error":
                error = wire.get("error") or {}
                return [
                    RealtimeEvent(
                        ERROR,
                        event_type,
                        Content.from_error(
                            message=error.get("message"),
                            error_code=error.get("code"),
                            additional_properties={
                                self.metadata_namespace: {
                                    "type": error.get("type"),
                                    "param": error.get("param"),
                                    "event_id": error.get("event_id"),
                                }
                            },
                        ),
                        {},
                        wire,
                    )
                ]
            case _:
                return [self._passthrough(wire)]

    def current_voice(self) -> Any:
        return ((self.session_state.get("audio") or {}).get("output") or {}).get("voice")

    # endregion


class _OpenAIConnection:
    def __init__(self, manager: Any, connection: Any) -> None:
        self._manager = manager
        self._connection = connection
        self.close_code: int | None = None

    async def send(self, event: dict[str, Any]) -> None:
        await self._connection.send_raw(json.dumps(event))

    async def recv(self) -> dict[str, Any] | None:
        from websockets.exceptions import ConnectionClosed, ConnectionClosedOK

        try:
            return json.loads(await self._connection.recv_bytes())
        except ConnectionClosedOK as exc:
            self.close_code = exc.code
            return None
        except ConnectionClosed as exc:
            self.close_code = exc.code
            raise RealtimeError(f"Realtime socket closed with code {exc.code}") from exc

    async def close(self) -> None:
        await self._connection.close()


class OpenAIRealtimeClient:
    """OpenAI Realtime, or Azure OpenAI through ``azure_endpoint`` on the GA route (no api-version)."""

    session_class = OpenAIRealtimeSession

    def __init__(
        self,
        *,
        model: str,
        azure_endpoint: str | None = None,
        api_key: str | None = None,
        credential: Any = None,
        ack_timeout: float = 10.0,
    ) -> None:
        self.model = model
        self.azure_endpoint = azure_endpoint.rstrip("/") if azure_endpoint else None
        self.api_key = api_key
        self.credential = credential
        self.ack_timeout = ack_timeout
        self.provider_name = "azure.ai.openai" if azure_endpoint else "openai"
        self.server_address = (azure_endpoint or "api.openai.com").split("//")[-1].split("/")[0]

    async def _token(self) -> str:
        if self.api_key:
            return self.api_key
        token = self.credential.get_token(COGNITIVE_SCOPE)
        if asyncio.iscoroutine(token):
            token = await token
        return token.token

    async def connect(self, *, headers: dict[str, str] | None = None) -> OpenAIRealtimeSession:
        from openai import AsyncOpenAI

        kwargs: dict[str, Any] = {"api_key": await self._token()}
        if self.azure_endpoint:
            host = self.azure_endpoint.split("//")[-1]
            kwargs["base_url"] = f"https://{host}/openai/v1"
            kwargs["websocket_base_url"] = f"wss://{host}/openai/v1"
        client = AsyncOpenAI(**kwargs)
        manager = client.realtime.connect(model=self.model, extra_headers=headers or {})
        connection = await manager.enter()
        return self.session_class(_OpenAIConnection(manager, connection), ack_timeout=self.ack_timeout)
