# PROTOTYPE, never merged (adamdougal/agent-framework#10).
"""``RealtimeConversation``: one live, full-duplex conversation over a provider session.

Implements, roughly, #12 (controls), #14 (updates from kinds), #16 (tool batches), #17 (history and exchanges),
#7 (active agent and ``handoff()``) and #9 (conversation middleware and spans).
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import json
import time
import uuid
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from agent_framework import (
    AgentResponse,
    AgentResponseUpdate,
    AgentSession,
    Content,
    FunctionInvocationContext,
    FunctionMiddleware,
    FunctionTool,
    Message,
    SessionContext,
)
from agent_framework._mcp import MCPTool
from agent_framework._middleware import FunctionMiddlewarePipeline, MiddlewareFailure, MiddlewareTermination
from agent_framework._tools import _execute_function_calls, normalize_function_invocation_configuration
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from ._core import (
    ERROR,
    FUNCTION_CALL,
    FUNCTION_RESULT,
    HANDOFF_COMPLETED,
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
    SPEECH_STARTED,
    USAGE,
    ConversationContext,
    RealtimeActiveResponseError,
    RealtimeConversationClosedError,
    RealtimeError,
    RealtimeEvent,
    event_content,
    logger,
    text_content,
    tracer,
)

if TYPE_CHECKING:
    from ._agent import RealtimeAgent

_IN_CONVERSATION: contextvars.ContextVar[Any] = contextvars.ContextVar("af_realtime_conversation", default=None)
CURRENT_CALL_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar("af_realtime_call_id", default=None)
_END = object()
FULLY_HEARD_TOLERANCE_MS = 40
"""Apps compute played_ms from playback clocks and round it. Without slack, a turn the caller heard in full gets cut
1 ms short and stored as incomplete (prototype finding)."""


@dataclass
class _Item:
    item_id: str
    item_type: str
    role: str | None
    source: str = "caller"
    text: str | None = None
    deltas: str = ""
    status: str | None = None
    marker: str | None = None
    author_name: str | None = None
    agent_id: str | None = None
    response_id: str | None = None
    call: _Call | None = None
    exchange: int = 0
    dropped: bool = False
    truncated: bool = False
    added_at: float = field(default_factory=time.monotonic)
    final_at: float | None = None


@dataclass
class _Call:
    content: Content
    response_id: str | None
    item_id: str | None
    agent: Any
    tool: FunctionTool | None
    state: str = "pending"
    result: Content | None = None
    arrived: float = field(default_factory=time.monotonic)


@dataclass
class _Response:
    response_id: str
    agent: Any
    exchange: int
    created: float = field(default_factory=time.monotonic)
    status: str | None = None
    reason: str | None = None
    usage: dict[str, Any] | None = None
    calls: list[_Call] = field(default_factory=list)
    messages: list[str] = field(default_factory=list)
    audio_ms: dict[tuple[str, int], float] = field(default_factory=dict)
    span: Any = None
    has_output_text: bool = False
    first_call_done_at: float | None = None
    done_at: float | None = None


class _ConversationContextMiddleware(FunctionMiddleware):
    """Installs ``ctx.realtime_conversation`` (#16), and records the running call for first-handoff-wins (#7)."""

    def __init__(self, conversation: RealtimeConversation) -> None:
        self.conversation = conversation

    async def process(self, context: FunctionInvocationContext, call_next: Any) -> None:
        context.realtime_conversation = self.conversation  # type: ignore[attr-defined]
        token = CURRENT_CALL_ID.set(context.metadata.get("call_id"))
        try:
            await call_next()
        finally:
            CURRENT_CALL_ID.reset(token)


def stringify_result(result: Content) -> str:
    value = result.result
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        texts = [item.text for item in value if isinstance(item, Content) and item.type == "text" and item.text]
        dropped = [item for item in value if isinstance(item, Content) and item.type != "text"]
        if dropped:
            logger.warning("Dropping %d non-text tool result items: realtime outputs are strings (#16).", len(dropped))
        return "\n".join(texts)
    if value is None:
        return ""
    return json.dumps(value, default=str)


def _cut_at_word(text: str, fraction: float) -> str:
    if fraction >= 1:
        return text
    target = int(len(text) * fraction)
    words = text.split(" ")
    out: list[str] = []
    length = 0
    for word in words:
        next_length = length + len(word) + (1 if out else 0)
        if abs(next_length - target) > abs(length - target) and out:
            break
        out.append(word)
        length = next_length
    return " ".join(out)


class RealtimeConversation:
    """A live realtime conversation, started by ``RealtimeAgent.start_conversation()``."""

    def __init__(
        self,
        owner: RealtimeAgent,
        *,
        start_agent: RealtimeAgent | None = None,
        session: AgentSession | None = None,
        options: dict[str, Any] | None = None,
        tools: Sequence[Any] | None = None,
        middleware: Sequence[Any] | None = None,
        function_invocation_kwargs: dict[str, Any] | None = None,
        ack_timeout: float = 10.0,
        close_transcript_wait: float = 3.0,
        queue_size: int = 20000,
        create_response: bool = False,
    ) -> None:
        self.owner = owner
        self.client = owner.client
        self._active_agent = start_agent or owner
        self._agent_session = session
        self._start_options = dict(options or {})
        self._start_tools = list(tools or [])
        self._extra_middleware = list(middleware or [])
        self._function_kwargs = dict(function_invocation_kwargs or {})
        self.ack_timeout = ack_timeout
        self.close_transcript_wait = close_transcript_wait
        self._initial_response = create_response
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=queue_size)
        self._session: Any = None
        self._control_lock = asyncio.Lock()
        self._receive_task: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._batch_tasks: set[asyncio.Task[Any]] = set()
        self._terminal: BaseException | None = None
        self._closing = False
        self._closed = asyncio.Event()
        self._accepting = True
        self._socket_open = False
        self._executable: dict[str, FunctionTool] = {}
        self._declared: list[Any] = []
        self._items: dict[str, _Item] = {}
        self._order: list[str] = []
        self._responses: dict[str, _Response] = {}
        self._calls: dict[str, _Call] = {}
        self._exchange = 0
        self._exchange_has_output = False
        self._exchange_has_input = False
        self._our_truncations: set[str] = set()
        self._last_audio_response: _Response | None = None
        self._follow_up_task: asyncio.Task[None] | None = None
        self._chain_follow_ups = 0
        self._handoff_responses: set[str] = set()
        self._provider_next: dict[str, int] = {}
        self._report_lock = asyncio.Lock()
        self._start_context: SessionContext | None = None
        self._cp_instructions: list[str] = []
        self._cp_tools: list[Any] = []
        self._middleware_context: ConversationContext | None = None
        self._span: Any = None
        self._span_ctx: Any = None
        self._replayed_ids: set[str] = set()
        self.stats: dict[str, Any] = {"close_transcript_wait_s": None, "update_hook_s": 0.0, "updates": 0}
        self.report_log: list[dict[str, Any]] = []
        self.usage_total: dict[str, int] = {}
        self.provider_errors: list[str] = []

    # region properties

    @property
    def active_agent(self) -> RealtimeAgent:
        return self._active_agent

    @property
    def session_id(self) -> str | None:
        return self._session.session_id if self._session else None

    @property
    def provider_session(self) -> Any:
        return self._session

    @property
    def history(self) -> list[Message]:
        """A read-only snapshot of the call's authored turns so far (#17)."""
        return [message for item_id in self._order for message in self._messages_for(self._items[item_id])]

    # endregion

    # region lifecycle

    async def __aenter__(self) -> RealtimeConversation:
        self._span = tracer.start_span(
            f"invoke_agent {self.owner.name}",
            kind=SpanKind.INTERNAL,
            attributes={
                "gen_ai.operation.name": "invoke_agent",
                "gen_ai.agent.name": self.owner.name or "",
                "gen_ai.agent.id": self.owner.id,
                "gen_ai.provider.name": self.client.provider_name,
                "server.address": self.client.server_address,
            },
        )
        self._span_ctx = trace.set_span_in_context(self._span)
        token = otel_context.attach(self._span_ctx)
        try:
            await self._run_middleware_chain()
        except BaseException as exc:
            self._span.set_status(Status(StatusCode.ERROR, str(exc)))
            self._span.end()
            raise
        finally:
            otel_context.detach(token)
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    async def _run_middleware_chain(self) -> None:
        conversation_middleware = [
            m for m in [*(self.owner.middleware or []), *self._extra_middleware] if _is_conversation_middleware(m)
        ]
        context = ConversationContext(
            agent=self.owner, session=self._agent_session, options=self._start_options, tools=self._start_tools
        )
        self._middleware_context = context

        async def open_session() -> None:
            self._start_options = context.options
            self._start_tools = list(context.tools)
            await self._open()
            context.conversation = self

        async def run(index: int) -> None:
            if index == len(conversation_middleware):
                await open_session()
                return
            await conversation_middleware[index].process(context, lambda: run(index + 1))

        await run(0)
        if self._session is None:
            raise MiddlewareTermination("Conversation middleware refused the realtime session.")

    async def _open(self) -> None:
        await self._run_before_run()
        headers: dict[str, str] = {}
        if self.client.provider_name == "azure.ai.foundry" and hasattr(self.client, "agent_name"):
            from opentelemetry.propagate import inject

            inject(headers)
            headers.pop("baggage", None)
        self._session = await self.client.connect(headers=headers)
        self._session.ack_timeout = self.ack_timeout
        self._socket_open = True
        self._session.receive_started = False
        with trace.use_span(self._span, end_on_exit=False):
            self._receive_task = asyncio.create_task(self._receive_loop(), name="af-realtime-receive")
        call_options = self.owner.call_options()
        call_options.update({key: value for key, value in self._start_options.items() if key not in _PARTICIPANT})
        options, tools = self._settings_for(self._active_agent, self._active_agent_handoff_tools())
        options.update(call_options)
        declared = self._apply_tools(tools, getattr(self._active_agent, "implementations", ()))
        send_tools: list[Any] | None = declared
        if getattr(self._active_agent, "is_foundry_definition", False) and all(isinstance(t, dict) for t in declared):
            # Nothing was added to a Foundry definition's tools, so no tools key is sent (#8).
            send_tools = None
        await self._session.update_session(options, send_tools, force=True)
        await self._replay()
        if self._initial_response:
            await self.create_response()

    async def _run_before_run(self) -> None:
        providers = list(self.owner.context_providers)
        self._start_context = SessionContext(
            session_id=self._agent_session.session_id if self._agent_session else None,
            input_messages=[],
            options=dict(self._start_options),
        )
        if self._agent_session is None:
            return
        for provider in providers:
            await provider.before_run(
                agent=self.owner,
                session=self._agent_session,
                context=self._start_context,
                state=self._agent_session.state.setdefault(provider.source_id, {}),
            )
        self._cp_instructions = list(self._start_context.instructions)
        self._cp_tools = list(self._start_context.tools)

    async def _replay(self) -> None:
        if self._start_context is None:
            return
        for messages in self._start_context.context_messages.values():
            for message in messages:
                for item in self._replay_items(message):
                    item_id = f"afr_{uuid.uuid4().hex[:20]}"
                    item["id"] = item_id
                    self._replayed_ids.add(item_id)
                    await self._session.create_item(item)

    def _replay_items(self, message: Message) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for content in message.contents:
            if content.type == "text" and content.text and message.role in ("user", "system", "assistant"):
                items.append(self._session.message_item(message.role, content.text))
            elif content.type == "function_call":
                arguments = content.arguments if isinstance(content.arguments, str) else json.dumps(content.arguments)
                items.append({
                    "type": "function_call",
                    "call_id": content.call_id,
                    "name": content.name,
                    "arguments": arguments or "{}",
                })
            elif content.type == "function_result":
                items.append({
                    "type": "function_call_output",
                    "call_id": content.call_id,
                    "output": stringify_result(content),
                })
        return items

    async def close(self) -> None:  # noqa: C901
        """#17's close sequence: stop input, wait for caller transcripts, close, settle calls, report, return."""
        if self._closing:
            await self._closed.wait()
            return
        self._closing = True
        self._accepting = False
        error: BaseException | None = self._terminal
        try:
            if self._socket_open:
                start = time.monotonic()
                while time.monotonic() - start < self.close_transcript_wait and self._pending_caller_transcripts():
                    await asyncio.sleep(0.05)
                self.stats["close_transcript_wait_s"] = round(time.monotonic() - start, 3)
            if self._session is not None:
                with contextlib.suppress(Exception):
                    await self._session.close()
            self._socket_open = False
            if self._receive_task is not None:
                self._receive_task.cancel()
                with contextlib.suppress(BaseException):
                    await self._receive_task
            if self._follow_up_task is not None:
                self._follow_up_task.cancel()
            for task in list(self._batch_tasks):
                task.cancel(msg="conversation_closed")
            for task in list(self._batch_tasks):
                with contextlib.suppress(BaseException):
                    await task
            for call in self._calls.values():
                if call.state != "settled":
                    self._settle(call, "Error: the conversation closed before the tool finished.", "conversation_closed")
            for item in self._items.values():
                if item.role == "user" and item.item_type == "message" and item.status is None and not item.marker:
                    item.marker = "transcript_pending"
                    if item.deltas:
                        item.text, item.status = item.deltas, "incomplete"
            for response in self._responses.values():
                if response.span is not None and response.done_at is None:
                    response.span.set_attribute("gen_ai.response.finish_reasons", ["interrupted"])
                    response.span.end()
            await self._report(closing=True)
            for task in list(self._tasks):
                if not task.done():
                    task.cancel()
            if self._middleware_context is not None:
                for hook in self._middleware_context.close_hooks:
                    await hook(error)
        finally:
            if self._span is not None:
                self._span.set_attribute("gen_ai.realtime_session.id", self.session_id or "")
                for key, value in self.usage_total.items():
                    self._span.set_attribute(f"gen_ai.usage.{key}", value)
                if error is not None:
                    self._span.set_status(Status(StatusCode.ERROR, str(error)))
                    self._span.set_attribute("error.type", type(error).__name__)
                self._span.end()
            self._queue.put_nowait(_END) if not self._queue.full() else None
            self._closed.set()

    def _pending_caller_transcripts(self) -> bool:
        return any(
            item.role == "user" and item.source == "caller" and item.status is None and not item.marker
            for item in self._items.values()
        )

    # endregion

    # region updates

    def __aiter__(self) -> AsyncIterator[AgentResponseUpdate]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[AgentResponseUpdate]:
        while True:
            update = await self._queue.get()
            if update is _END:
                if self._terminal is not None and not isinstance(self._terminal, asyncio.CancelledError):
                    raise self._terminal
                return
            yield update

    async def _emit(self, update: AgentResponseUpdate | None) -> None:
        if update is None or self._middleware_context is None:
            return
        start = time.perf_counter()
        try:
            for hook in reversed(self._middleware_context.update_hooks):
                replacement = await hook(update)
                if replacement is not None:
                    update = replacement
        except MiddlewareTermination:
            return
        finally:
            self.stats["update_hook_s"] += time.perf_counter() - start
            self.stats["updates"] += 1
        if self._queue.full():
            self._terminal = RealtimeError("Update queue full: the app isn't reading updates fast enough.")
            asyncio.get_running_loop().create_task(self.close())
            return
        self._queue.put_nowait(update)

    def _update(
        self,
        event: RealtimeEvent,
        content: Content,
        *,
        role: str | None = None,
        agent: Any = None,
        **kwargs: Any,
    ) -> AgentResponseUpdate:
        return AgentResponseUpdate(
            contents=[content],
            role=role,
            author_name=agent.name if agent is not None else None,
            agent_id=agent.id if agent is not None else None,
            raw_representation=event,
            **kwargs,
        )

    # endregion

    # region receive loop

    async def _receive_loop(self) -> None:
        _IN_CONVERSATION.set(self)
        try:
            async for event in self._session.receive():
                await self._handle(event)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            if not self._closing:
                self._terminal = exc
                logger.exception("Realtime receive loop failed")
        finally:
            self._socket_open = False
            if not self._closing:
                asyncio.get_running_loop().create_task(self.close())

    async def _handle(self, event: RealtimeEvent) -> None:  # noqa: C901
        attrs = event.attributes
        kind = event.kind
        update: AgentResponseUpdate | None = None
        if kind == SESSION_CREATED:
            self._span.set_attribute("gen_ai.realtime_session.id", attrs.get("session_id") or "")
            update = self._update(event, event.content)
        elif kind == SPEECH_STARTED:
            self._chain_follow_ups = 0
            update = self._update(event, event.content, role="user")
        elif kind == RESPONSE_CREATED:
            response_id = attrs["response_id"]
            agent = self._active_agent
            response = _Response(response_id, agent, self._exchange)
            response.span = tracer.start_span(
                f"realtime_inference {self._session.model or self.client.model}",
                kind=SpanKind.CLIENT,
                context=self._span_ctx,
                attributes={
                    "gen_ai.operation.name": "realtime_inference",
                    "gen_ai.provider.name": self.client.provider_name,
                    "gen_ai.request.model": self._session.model or self.client.model or "",
                    "gen_ai.response.id": response_id,
                    "gen_ai.agent.name": agent.name or "",
                    "gen_ai.agent.id": agent.id,
                    "server.address": self.client.server_address,
                },
            )
            self._responses[response_id] = response
            update = self._update(event, event.content, role="assistant", agent=agent, response_id=response_id)
        elif kind == ITEM_ADDED:
            self._on_item_added(attrs)
            update = self._update(event, event.content)
        elif kind == OUTPUT_AUDIO:
            response = self._responses.get(attrs.get("response_id") or "")
            if response is not None:
                key = (attrs["item_id"], attrs.get("content_index") or 0)
                response.audio_ms[key] = response.audio_ms.get(key, 0.0) + attrs["audio_ms"]
                if attrs["item_id"] not in response.messages:
                    response.messages.append(attrs["item_id"])
                self._last_audio_response = response
            update = self._update(
                event,
                event.content,
                role="assistant",
                agent=response.agent if response else self._active_agent,
                response_id=attrs.get("response_id"),
                message_id=attrs.get("item_id"),
            )
        elif kind == OUTPUT_TEXT:
            response = self._responses.get(attrs.get("response_id") or "")
            item = self._items.get(attrs.get("item_id") or "")
            if item is not None:
                if attrs["merge_mode"] == "append":
                    item.deltas += event.content.text or ""
                else:
                    item.text = event.content.text
                    item.status = item.status or "completed"
            if response is not None:
                response.has_output_text = True
            update = self._update(
                event,
                event.content,
                role="assistant",
                agent=response.agent if response else self._active_agent,
                response_id=attrs.get("response_id"),
                message_id=attrs.get("item_id"),
                merge_mode=attrs["merge_mode"],
            )
        elif kind == INPUT_TRANSCRIPTION:
            item = self._items.get(attrs.get("item_id") or "")
            if item is not None:
                if attrs["merge_mode"] == "append":
                    item.deltas += event.content.text or ""
                else:
                    item.text = event.content.text
                    item.status = "completed"
                    item.final_at = time.monotonic()
                    self._schedule_report()
            update = self._update(
                event, event.content, role="user", message_id=attrs.get("item_id"), merge_mode=attrs["merge_mode"]
            )
        elif kind == INPUT_TRANSCRIPTION_FAILED:
            item = self._items.get(attrs.get("item_id") or "")
            if item is not None:
                item.marker = "transcription_failed"
                if item.deltas:
                    item.text, item.status = item.deltas, "incomplete"
                self._schedule_report()
            update = self._update(event, event.content, role="user", message_id=attrs.get("item_id"))
        elif kind == ITEM_TRUNCATED:
            self._on_truncated(attrs)
            if self._span is not None:
                self._span.add_event(
                    "agent_framework.realtime.item_truncated",
                    {"item_id": attrs.get("item_id") or "", "audio_end_ms": attrs.get("audio_end_ms") or 0},
                )
            update = self._update(event, event.content, role="assistant")
        elif kind == FUNCTION_CALL:
            call = self._on_function_call(event)
            update = self._update(
                event, event.content, role="assistant", agent=call.agent, response_id=call.response_id
            )
        elif kind == RESPONSE_DONE:
            update = self._on_response_done(event)
        elif kind == USAGE:
            response = self._responses.get(attrs.get("response_id") or "")
            if response is not None:
                response.usage = dict(event.content.usage_details or {})
                for key, value in response.usage.items():
                    if isinstance(value, int):
                        self.usage_total[key] = self.usage_total.get(key, 0) + value
            update = self._update(event, event.content, role="assistant", response_id=attrs.get("response_id"))
        elif kind == ERROR:
            if self._span is not None:
                self._span.add_event(
                    "agent_framework.realtime.error", {"error.type": event.content.error_code or "error"}
                )
            update = self._update(event, event.content)
        else:
            update = self._update(event, event.content) if event.content is not None else None
        await self._emit(update)

    def _on_item_added(self, attrs: dict[str, Any]) -> None:
        item_id = attrs.get("item_id")
        if not item_id or item_id in self._items or item_id in self._replayed_ids:
            return
        item_type = attrs.get("item_type") or "message"
        role = attrs.get("role")
        if item_type == "function_call_output":
            return
        item = _Item(item_id, item_type, role)
        if role in ("user", "system") and item_type == "message":
            item.source = "app" if attrs.get("text") else "caller"
            if item.source == "app":
                item.text, item.status = attrs.get("text"), "completed"
            if self._exchange_has_output:
                self._exchange += 1
                self._exchange_has_output = False
                self._chain_follow_ups = 0
                self._schedule_report()
            self._exchange_has_input = True
        else:
            self._exchange_has_output = True
            response = self._responses.get(self._session.active_response_id or "")
            if response is not None:
                item.response_id = response.response_id
                item.author_name = response.agent.name
                item.agent_id = response.agent.id
                if item_type == "message" and item_id not in response.messages:
                    response.messages.append(item_id)
        item.exchange = self._exchange
        self._items[item_id] = item
        previous = attrs.get("previous_item_id")
        if previous in self._order:
            self._order.insert(self._order.index(previous) + 1, item_id)
        else:
            self._order.append(item_id)

    def _on_truncated(self, attrs: dict[str, Any]) -> None:
        item = self._items.get(attrs.get("item_id") or "")
        if item is None or item.truncated:
            return
        response = self._responses.get(item.response_id or "")
        if response is not None and response.exchange < min(self._provider_next.values(), default=0):
            logger.info("Ignoring late truncation of %s: its exchange was already reported.", item.item_id)
            return
        item.truncated = True
        audio_end_ms = attrs.get("audio_end_ms") or 0
        generated = sum(ms for (item_id, _), ms in (response.audio_ms.items() if response else []) if item_id == item.item_id)
        if audio_end_ms <= 0:
            item.dropped = True
        else:
            source = item.text or item.deltas
            item.text = _cut_at_word(source, audio_end_ms / generated if generated else 1.0)
        item.status = "incomplete"
        if item.item_id not in self._our_truncations and response is not None:
            # Voice Live's auto-truncation cut this message: cut the response's later messages at 0 too (#8).
            later = response.messages[response.messages.index(item.item_id) + 1 :] if item.item_id in response.messages else []
            if later:
                self._spawn(self._truncate_later(later))

    async def _truncate_later(self, item_ids: list[str]) -> None:
        async with self._control_lock:
            for item_id in item_ids:
                self._our_truncations.add(item_id)
                with contextlib.suppress(RealtimeError):
                    await self._session.truncate(item_id, 0, 0)

    def _on_function_call(self, event: RealtimeEvent) -> _Call:
        content = event.content
        attrs = event.attributes
        tool = self._executable.get(content.name or "")
        call = _Call(content, attrs.get("response_id"), attrs.get("item_id"), self._active_agent, tool)
        self._calls[content.call_id] = call
        response = self._responses.get(call.response_id or "")
        if response is not None:
            response.calls.append(call)
            response.first_call_done_at = response.first_call_done_at or time.monotonic()
            call.agent = response.agent
        item = self._items.get(call.item_id or "")
        if item is None and call.item_id:
            self._on_item_added({"item_id": call.item_id, "item_type": "function_call"})
            item = self._items.get(call.item_id)
        if item is not None:
            item.call = call
            item.author_name = call.agent.name
        return call

    def _on_response_done(self, event: RealtimeEvent) -> AgentResponseUpdate:
        attrs = event.attributes
        response = self._responses.get(attrs.get("response_id") or "")
        status, reason = attrs.get("status"), attrs.get("reason")
        if status == "completed":
            finish = "tool_calls" if "function_call" in (attrs.get("output_types") or []) else "stop"
        elif status == "incomplete":
            finish = {"content_filter": "content_filter", "max_output_tokens": "length"}.get(reason or "")
        else:
            finish = None
        if response is not None:
            response.status, response.reason = status, reason
            response.done_at = time.monotonic()
            for item_id in response.messages:
                item = self._items.get(item_id)
                if item is None or item.truncated:
                    continue
                if status != "completed":
                    item.text = item.text or item.deltas
                    item.status = "incomplete"
                    if not item.text:
                        item.dropped = True
                elif item.status is None:
                    item.text, item.status = item.text or item.deltas, "completed"
            self._end_inference_span(response, finish, attrs)
            if response.calls:
                if status == "completed":
                    task = self._spawn(self._run_batch(response))
                    self._batch_tasks.add(task)
                    task.add_done_callback(self._batch_tasks.discard)
                else:
                    for call in response.calls:
                        self._settle(call, f"Error: the response ended {status} before the tool ran.", status or "")
                        self._spawn(self._send_output_best_effort(call))
            self._schedule_report()
        return self._update(
            event,
            event.content,
            role="assistant",
            agent=response.agent if response else None,
            response_id=attrs.get("response_id"),
            finish_reason=finish,
        )

    def _end_inference_span(self, response: _Response, finish: str | None, attrs: dict[str, Any]) -> None:
        span = response.span
        if span is None:
            return
        status, reason = attrs.get("status"), attrs.get("reason")
        if status == "completed":
            span_finish = "tool_call" if finish == "tool_calls" else "stop"
        elif status == "incomplete":
            span_finish = finish or reason or "length"
        elif status == "cancelled":
            span_finish = reason or "interrupted"
        else:
            span_finish = "error"
            error = attrs.get("error") or {}
            span.set_attribute("error.type", (error or {}).get("code") or (error or {}).get("type") or "server_error")
            span.set_status(Status(StatusCode.ERROR))
        span.set_attribute("gen_ai.response.finish_reasons", [span_finish])
        for key, value in (response.usage or {}).items():
            if isinstance(value, int):
                span.set_attribute(f"gen_ai.usage.{key}", value)
        span.end()

    # endregion

    # region tools

    def _apply_tools(self, tools: Sequence[Any], implementations: Sequence[Any] = ()) -> list[Any]:
        """Replace the executable set and return the declared list, in place (#12, #16)."""
        executable: dict[str, FunctionTool] = {}
        declared: list[Any] = []
        for tool in [*implementations]:
            for function in _functions_of(tool):
                executable[function.name] = function
        for tool in tools:
            if isinstance(tool, dict):
                declared.append(tool)
                continue
            for function in _functions_of(tool):
                executable[function.name] = function
                if not any(isinstance(d, dict) and d.get("name") == function.name for d in tools):
                    declared.append(function)
        self._executable = executable
        self._declared[:] = declared
        return declared

    def _function_middleware_for(self, agent: Any) -> list[Any]:
        owner_mw = [m for m in (self.owner.middleware or []) if _is_function_middleware(m)] if agent is not self.owner else []
        own = [m for m in (agent.middleware or []) if _is_function_middleware(m)]
        extra = [m for m in self._extra_middleware if _is_function_middleware(m)]
        return [_ConversationContextMiddleware(self), *owner_mw, *extra, *own]

    async def _run_batch(self, response: _Response) -> None:
        calls = list(response.calls)
        should_terminate = False
        try:
            groups: dict[int, list[_Call]] = {}
            for call in calls:
                groups.setdefault(id(call.agent), []).append(call)
            for group in groups.values():
                runnable = [call for call in group if call.tool is not None]
                for call in group:
                    if call.tool is None:
                        self._settle(
                            call, f'Error: Requested function "{call.content.name}" not found.', "KeyError", emit=False
                        )
                if not runnable:
                    continue
                for call in runnable:
                    call.state = "running"
                tools: dict[str, FunctionTool] = {call.tool.name: call.tool for call in runnable}  # type: ignore[union-attr]
                pipeline = FunctionMiddlewarePipeline(*self._function_middleware_for(group[0].agent))
                config = normalize_function_invocation_configuration(self.owner.function_invocation_configuration)
                batch = await _execute_function_calls(
                    custom_args=dict(self._function_kwargs),
                    function_calls=[call.content for call in runnable],
                    options={"tools": list(tools.values())},
                    config=config,
                    invocation_session=self._agent_session,
                    middleware_pipeline=pipeline,
                )
                should_terminate = should_terminate or batch.should_terminate
                for call, results in zip(runnable, batch.result_groups, strict=False):
                    result = next((c for c in results if c.type == "function_result"), None)
                    if result is None:
                        self._settle(
                            call, "Error: the call paused, which the prototype doesn't support.", "paused", emit=False
                        )
                    else:
                        call.result, call.state = result, "settled"
            for call in calls:
                if call.state != "settled":
                    self._settle(call, "Error: no result.", "missing", emit=False)
            for call in calls:
                await self._session.send_function_output(call.content.call_id, stringify_result(call.result))
                await self._emit_result(call)
        except asyncio.CancelledError:
            for call in calls:
                if call.state != "settled":
                    self._settle(call, "Error: the conversation closed before the tool finished.", "conversation_closed")
            raise
        except MiddlewareFailure as exc:
            self._terminal = exc
            self._spawn(self.close())
            return
        self._schedule_report()
        if not should_terminate:
            limits = normalize_function_invocation_configuration(self.owner.function_invocation_configuration)
            self._chain_follow_ups += 1
            over = self._chain_follow_ups > int(limits.get("max_iterations") or 40)
            await self._request_follow_up({"tool_choice": "none"} if over else None)

    async def _emit_result(self, call: _Call) -> None:
        if call.result is None:
            return
        event = RealtimeEvent(FUNCTION_RESULT, None, call.result)
        await self._emit(self._update(event, call.result, role="tool", agent=call.agent, response_id=call.response_id))

    def _settle(self, call: _Call, message: str, reason: str, *, emit: bool = True) -> None:
        call.result = Content.from_function_result(call_id=call.content.call_id, result=message, exception=reason)
        call.state = "settled"
        if emit and not self._closed.is_set() and not self._queue.full():
            self._queue.put_nowait(
                self._update(
                    RealtimeEvent(FUNCTION_RESULT, None, call.result),
                    call.result,
                    role="tool",
                    agent=call.agent,
                    response_id=call.response_id,
                )
            )

    async def _send_output_best_effort(self, call: _Call) -> None:
        with contextlib.suppress(Exception):
            await self._session.send_function_output(call.content.call_id, stringify_result(call.result))

    async def _request_follow_up(self, options: dict[str, Any] | None) -> None:
        if self._follow_up_task is not None and not self._follow_up_task.done():
            return
        self._follow_up_task = self._spawn(self._follow_up(options))

    async def _follow_up(self, options: dict[str, Any] | None) -> None:
        for _ in range(10):
            await self._session.idle.wait()
            async with self._control_lock:
                if self._session.active_response_id is not None:
                    continue
                try:
                    await self._session.create_response(options)
                    return
                except RealtimeActiveResponseError:
                    continue

    # endregion

    # region history and exchanges

    def _messages_for(self, item: _Item) -> list[Message]:
        if item.dropped:
            return []
        if item.item_type == "function_call":
            call = item.call
            if call is None:
                return []
            messages = [Message("assistant", [call.content], author_name=item.author_name, message_id=item.item_id)]
            if call.result is not None:
                messages.append(Message("tool", [call.result], author_name=item.author_name))
            return messages
        if item.item_type != "message":
            return []
        role = item.role or "assistant"
        text = item.text if item.text is not None else (item.deltas if item.status == "incomplete" else None)
        if role == "user" and not text:
            return [
                Message(
                    "user",
                    [],
                    message_id=item.item_id,
                    additional_properties={"realtime": {"text_less": item.marker or "transcript_pending"}},
                )
            ]
        if text is None:
            return []
        return [
            Message(
                role,
                [text_content(text, status=item.status)],
                author_name=item.author_name if role == "assistant" else None,
                message_id=item.item_id,
            )
        ]

    def _exchange_settled(self, index: int) -> bool:
        for item in self._items.values():
            if item.exchange != index:
                continue
            if item.role == "user" and item.status is None and not item.marker:
                return False
            if item.item_type == "function_call" and (item.call is None or item.call.state != "settled"):
                return False
            if item.role == "assistant" and item.status is None and not item.dropped:
                return False
        return all(response.done_at is not None for response in self._responses.values() if response.exchange == index)

    def _schedule_report(self) -> None:
        if self._agent_session is None or not self.owner.context_providers or self._closing:
            return
        self._spawn(self._report(closing=False))

    async def _report(self, *, closing: bool) -> None:
        if self._agent_session is None:
            return
        async with self._report_lock:
            providers = list(reversed(self.owner.context_providers))
            last = self._exchange if closing else self._exchange - 1
            for provider in providers:
                next_index = self._provider_next.get(provider.source_id, 0)
                while next_index <= last and (closing or self._exchange_settled(next_index)):
                    context = self._exchange_context(next_index)
                    try:
                        await provider.after_run(
                            agent=self.owner,
                            session=self._agent_session,
                            context=context,
                            state=self._agent_session.state.setdefault(provider.source_id, {}),
                        )
                    except Exception as exc:  # noqa: BLE001
                        self.provider_errors.append(f"{provider.source_id}: {exc}")
                        await self._emit(
                            AgentResponseUpdate(
                                contents=[Content.from_error(message=f"{provider.source_id} failed: {exc}")]
                            )
                        )
                        break
                    self.report_log.append({
                        "provider": provider.source_id,
                        "exchange": next_index,
                        "inputs": len(context.input_messages),
                        "outputs": len(context.response.messages) if context.response else 0,
                        "closing": closing,
                    })
                    next_index += 1
                    self._provider_next[provider.source_id] = next_index

    def _exchange_context(self, index: int) -> SessionContext:
        inputs: list[Message] = []
        outputs: list[Message] = []
        for item_id in self._order:
            item = self._items[item_id]
            if item.exchange != index:
                continue
            target = inputs if item.role in ("user", "system") and item.item_type == "message" else outputs
            target.extend(self._messages_for(item))
        start = self._start_context
        context = SessionContext(
            session_id=self._agent_session.session_id if self._agent_session else None,
            input_messages=inputs,
            context_messages=dict(start.context_messages) if start and index == 0 else None,
            instructions=list(start.instructions) if start and index == 0 else None,
            metadata=start.metadata if start else None,
        )
        responses = [response for response in self._responses.values() if response.exchange == index]
        usage: dict[str, int] = {}
        for response in responses:
            for key, value in (response.usage or {}).items():
                if isinstance(value, int):
                    usage[key] = usage.get(key, 0) + value
        context._response = AgentResponse(  # type: ignore[assignment]
            messages=outputs,
            response_id=responses[-1].response_id if responses else None,
            usage_details=usage or None,  # type: ignore[arg-type]
        )
        return context

    # endregion

    # region controls (#12)

    @contextlib.asynccontextmanager
    async def _control(self, name: str, **attributes: Any) -> AsyncIterator[Any]:
        if self._closing:
            raise RealtimeConversationClosedError("The conversation is closed.")
        parent = None if _IN_CONVERSATION.get() is self else self._span_ctx
        with tracer.start_as_current_span(
            f"realtime.{name}",
            context=parent,
            kind=SpanKind.INTERNAL,
            attributes={
                "gen_ai.realtime_session.id": self.session_id or "",
                "gen_ai.provider.name": self.client.provider_name,
                **attributes,
            },
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            try:
                async with self._control_lock:
                    yield span
            except RealtimeError as exc:
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                span.set_attribute("error.type", getattr(exc, "code", None) or type(exc).__name__)
                raise

    async def update_session(self, *, options: dict[str, Any] | None = None, tools: Sequence[Any] | None = None) -> None:
        async with self._control("update_session"):
            await self._update_session_locked(options, tools)

    async def _update_session_locked(
        self, options: dict[str, Any] | None, tools: Sequence[Any] | None, implementations: Sequence[Any] = ()
    ) -> None:
        declared = None
        previous = (dict(self._executable), list(self._declared))
        if tools is not None:
            declared = self._apply_tools(tools, implementations)
        try:
            await self._session.update_session(dict(options or {}), declared)
        except BaseException:
            self._executable, self._declared[:] = previous[0], previous[1]
            raise

    async def create_response(self, *, options: dict[str, Any] | None = None) -> str:
        async with self._control("create_response") as span:
            response_id = await self._session.create_response(options)
            span.set_attribute("gen_ai.response.id", response_id)
            return response_id

    async def say(self, text: str) -> str:
        async with self._control("say") as span:
            response_id = await self._session.say(text)
            span.set_attribute("gen_ai.response.id", response_id)
            return response_id

    async def interrupt(self, *, played_ms: int | None = None) -> None:
        """Cancel the active response, then cut every unheard message of the last spoken response (#8)."""
        async with self._control("interrupt"):
            response = self._last_audio_response
            await self._session.cancel_response()
            if played_ms is None or response is None:
                return
            remaining = float(played_ms)
            cut = False
            for item_id in list(response.messages):
                generated = sum(ms for (iid, _), ms in response.audio_ms.items() if iid == item_id)
                content_index = next((ci for (iid, ci) in response.audio_ms if iid == item_id), 0)
                if cut:
                    self._our_truncations.add(item_id)
                    await self._session.truncate(item_id, content_index, 0)
                    continue
                if remaining >= generated - FULLY_HEARD_TOLERANCE_MS:
                    remaining -= generated
                    continue
                cut = True
                self._our_truncations.add(item_id)
                await self._session.truncate(item_id, content_index, int(max(0, remaining)))

    async def handoff(self, agent: RealtimeAgent, *, tools: Sequence[Any] | None = None) -> None:
        """Swap the live session to another agent's instructions, tools, voice and tool choice (#7)."""
        if agent.client is not self.client:
            raise RealtimeError("A handoff target must use the conversation's client instance.")
        in_tool = CURRENT_CALL_ID.get() is not None
        source = self._active_agent
        span_cm = contextlib.nullcontext() if in_tool else tracer.start_as_current_span(
            "realtime.handoff",
            context=self._span_ctx,
            attributes={"agent_framework.handoff.source": source.name or "", "agent_framework.handoff.target": agent.name or ""},
        )
        with span_cm:
            if in_tool:
                current = trace.get_current_span()
                current.set_attribute("gen_ai.transfer.mode", "pass_control")
                current.set_attribute("gen_ai.transfer.target.name", agent.name or "")
            async with self._control_lock:
                options, all_tools = self._settings_for(agent, tools)
                await self._update_session_locked(options, all_tools, getattr(agent, "implementations", ()))
                self._active_agent = agent
        content = event_content(HANDOFF_COMPLETED, None, source=source.name or source.id, target=agent.name or agent.id)
        await self._emit(self._update(RealtimeEvent(HANDOFF_COMPLETED, None, content), content, role="assistant", agent=agent))

    def _active_agent_handoff_tools(self) -> list[Any]:
        return []

    def _settings_for(self, participant: Any, extra_tools: Sequence[Any] | None) -> tuple[dict[str, Any], list[Any]]:
        settings = participant.participant_settings(self._session)
        instructions = "\n\n".join(
            part
            for part in [
                settings.instructions,
                self.owner.owner_instructions() if participant is not self.owner else None,
                self._start_options.get("instructions"),
                *self._cp_instructions,
            ]
            if part
        )
        options: dict[str, Any] = {}
        if instructions:
            options["instructions"] = instructions
        voice = self._start_options.get("voice", settings.voice)
        tool_choice = self._start_options.get("tool_choice", settings.tool_choice)
        if voice is not None:
            options["voice"] = voice
        options["tool_choice"] = tool_choice or "auto"
        tools = [*settings.tools, *self._start_tools, *self._cp_tools, *(extra_tools or [])]
        return options, tools

    # endregion

    # region input

    async def send(self, value: Any, *, create_response: bool = True) -> None:
        if not self._accepting:
            raise RealtimeConversationClosedError("The conversation is closed.")
        if self._middleware_context is not None:
            try:
                for hook in self._middleware_context.send_hooks:
                    replacement = await hook(value)
                    if replacement is not None:
                        value = replacement
            except MiddlewareTermination:
                return
        if isinstance(value, bytes):
            await self._session.append_audio(value)
            return
        if isinstance(value, RealtimeEvent):
            await self._session.send_raw(value.raw_representation)
            return
        if isinstance(value, Content) and value.type == "data":
            from agent_framework._types import _get_data_bytes

            await self._session.append_audio(_get_data_bytes(value) or b"")
            return
        messages = [Message("user", [value])] if isinstance(value, str) else value if isinstance(value, list) else [value]
        for message in messages:
            for content in message.contents:
                if content.type == "text" and content.text:
                    await self._session.create_item(self._session.message_item(message.role, content.text))
        if create_response:
            await self._session.idle.wait()
            await self.create_response()

    # endregion

    def _spawn(self, coroutine: Any) -> asyncio.Task[Any]:
        task = asyncio.get_running_loop().create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task


_PARTICIPANT = ("instructions", "voice", "tool_choice")


def _functions_of(tool: Any) -> list[FunctionTool]:
    if isinstance(tool, FunctionTool):
        return [tool]
    if isinstance(tool, MCPTool):
        return list(tool.functions)
    if callable(tool):
        from agent_framework import tool as make_tool

        return [make_tool(tool)]
    return []


def _is_conversation_middleware(middleware: Any) -> bool:
    from ._core import ConversationMiddleware

    return isinstance(middleware, ConversationMiddleware)


def _is_function_middleware(middleware: Any) -> bool:
    if isinstance(middleware, FunctionMiddleware):
        return True
    if callable(middleware) and not _is_conversation_middleware(middleware):
        import inspect

        with contextlib.suppress(TypeError, ValueError):
            parameters = list(inspect.signature(middleware).parameters.values())
            return bool(parameters) and parameters[0].annotation in (FunctionInvocationContext, "FunctionInvocationContext")
    return False
