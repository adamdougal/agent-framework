---
status: proposed
contact: adamdougal
date: 2026-11-06
deciders: eavanvalkenburg
---

# Python realtime agents contract

## Scope

This specification defines the contract that [ADR 0054](../decisions/0054-python-realtime-agent.md) selects for Python
realtime agents: the live conversation, its control calls, event kinds, tool loop, voice history, handoffs,
conversation middleware, telemetry and provider clients. It applies to `agent-framework-core`,
`agent-framework-openai`, `agent-framework-foundry` and `agent-framework-orchestrations`.

Every API here is experimental under `ExperimentalFeature.REALTIME`, except the Foundry voice agent classes, which use
`FOUNDRY_VOICE_AGENTS`, and the additive changes to stable APIs listed under
[Changes to existing packages](#changes-to-existing-packages). This specification is normative. Package documentation
may describe it but must not contradict it.

## API shapes

The shapes fix names, parameters and return types. `ToolsInput` stands for the tool types `Agent.run(tools=...)`
accepts.

```python
RealtimeOptionsT = TypeVar("RealtimeOptionsT", bound=TypedDict, default=RealtimeOptions)


class RawRealtimeAgent(BaseAgent, Generic[RealtimeOptionsT]):
    def __init__(
        self, client: RealtimeClientProtocol[RealtimeOptionsT], *, instructions: str | None = None,
        tools: ToolsInput | None = None, default_options: RealtimeOptionsT | None = None,
        function_invocation_configuration: FunctionInvocationConfiguration | None = None,
        id: str | None = None, name: str | None = None, description: str | None = None,
        context_providers: Sequence[ContextProvider] | None = None, middleware: Sequence[MiddlewareTypes] | None = None,
    ) -> None: ...

    def run(
        self, messages: AgentRunInputs | None = None, *, stream: bool = False, session: AgentSession | None = None,
        options: RealtimeOptionsT | None = None, tools: ToolsInput | None = None,
        middleware: Sequence[MiddlewareTypes] | None = None,
        function_invocation_kwargs: Mapping[str, Any] | None = None, client_kwargs: Mapping[str, Any] | None = None,
    ) -> Awaitable[AgentResponse] | ResponseStream[AgentResponseUpdate, AgentResponse]: ...

    def start_conversation(
        self, messages: AgentRunInputs | None = None, *, session: AgentSession | None = None,
        options: RealtimeOptionsT | None = None, tools: ToolsInput | None = None,
        middleware: Sequence[MiddlewareTypes] | None = None,
        function_invocation_kwargs: Mapping[str, Any] | None = None,
    ) -> RealtimeConversation[RealtimeOptionsT]: ...


class RealtimeAgent(<realtime middleware layer>, <realtime telemetry layer>, RawRealtimeAgent[RealtimeOptionsT]): ...


class RealtimeConversation(Generic[RealtimeOptionsT]):  # async context manager; never stored in AgentSession
    updates: AsyncIterable[AgentResponseUpdate]  # `async for update in conversation` iterates the same stream
    active_agent: RawRealtimeAgent[RealtimeOptionsT]  # read-only
    history: Sequence[Message]  # read-only snapshot

    async def send(self, value: AgentRunInputs | RealtimeEvent, *, create_response: bool = True) -> None: ...
    async def update_session(
        self, *, options: RealtimeOptionsT | None = None, tools: ToolsInput | None = None
    ) -> None: ...
    async def create_response(self, *, options: RealtimeOptionsT | None = None) -> str: ...
    async def say(self, text: str) -> str: ...
    async def interrupt(self, *, played_ms: int | None = None) -> None: ...
    async def handoff(self, agent: RawRealtimeAgent[RealtimeOptionsT], *, tools: ToolsInput | None = None) -> None: ...
    async def close(self) -> None: ...


class RealtimeEvent:
    kind: RealtimeKind  # NewType over str; RealtimeKindLiteral lists the known kinds
    content: Content | None  # one payload, or None for a signal
    service_event_type: str | None  # the provider's event name, verbatim
    additional_properties: Mapping[str, Any]  # the kind's neutral attributes and documented provider extras
    raw_representation: Any | None  # never serialized


class RealtimeClientProtocol(Protocol[RealtimeOptionsT]):
    async def start_session(
        self, *, options: RealtimeOptionsT | None = None, tools: Sequence[ToolTypes] | None = None
    ) -> RealtimeClientSessionProtocol[RealtimeOptionsT]: ...


class RealtimeClientSessionProtocol(Protocol[RealtimeOptionsT]):  # async context manager
    options: RealtimeOptionsT

    async def send(self, event: RealtimeEvent) -> None: ...
    def receive(self) -> AsyncIterable[RealtimeEvent]: ...  # one reader only
    async def update_session(self, options: RealtimeOptionsT, tools: Sequence[ToolTypes] | None) -> None: ...
    async def create_response(self, options: RealtimeOptionsT | None = None) -> str: ...
    async def say(self, text: str) -> str: ...
    async def cancel_response(self) -> None: ...
    async def truncate(self, item_id: str, content_index: int, audio_end_ms: int) -> None: ...
    async def close(self) -> None: ...


class RealtimeOptions(TypedDict, total=False):
    instructions: str
    voice: str  # the Azure option types widen it to str | AzureVoice
    tool_choice: ToolMode | Literal["auto", "required", "none"]
    temperature: float
    max_tokens: int
    metadata: Mapping[str, str]
    input_audio_format: str  # "audio/pcm;rate=24000", "audio/pcmu" or "audio/pcma"
    output_audio_format: str
    output_modalities: Sequence[str]
    turn_detection: RealtimeTurnDetection | None  # type, create_response, interrupt_response, threshold, timings
    transcription: RealtimeTranscription | None  # model, language
    noise_reduction: RealtimeNoiseReduction | None  # type
```

- `RawRealtimeAgent` runs no middleware and emits no telemetry; its tool loop still runs tools. It adds an
  `InMemoryHistoryProvider` when a session is passed and no context provider loads history, as `Agent` does.
- `run(...)` has no `**kwargs`, `compaction_strategy` or `tokenizer`. Per-run `options` merge over `default_options`,
  and per-run `tools` merge into the run's realtime session. `start_conversation(...)` applies `options` and `tools` to
  the initial session. `middleware=` joins the agent's own list, sorted by category. `AgentRunInputs` passed to
  `run(...)`, `start_conversation(...)` or `send(...)` go through `normalize_messages(...)`, as `Agent.run(...)` input
  does.
- `function_invocation_kwargs` is copied when a run or the conversation starts and goes to every tool call, as in chat.
  `client_kwargs` exists because `SupportsAgentRun.run` declares it. No v1 realtime client takes per-run arguments, so
  `run(...)` ignores a non-empty mapping with a warning. The caller owns both mappings, and nothing mutates them.
- `run(...)` fails when its own response request claims an error, or when that request's acknowledgement times out,
  instead of waiting for a `response.done` that won't come. Cancelling a run whose response never finishes, directly or
  through the caller's `asyncio.timeout()`, runs the close sequence. Leaving a `run(stream=True)` stream early closes
  its realtime session and receive loop.
- `RealtimeOptions` replaces ADR 0035's `RealtimeSessionOptions`, and its `create_response` lives in turn detection.
  `None` turns a feature off. `OpenAIRealtimeOptions`, `VoiceLiveOptions` and `FoundryVoiceAgentOptions` extend it.
  `AzureVoice`, `AzureTurnDetection`, `AzureTranscription` and `AzureNoiseReduction` add Azure's types and fields, and
  Azure-only settings such as echo cancellation, animation, avatar and interim response exist only on the Azure types.
  The Azure types widen `voice` to `str | AzureVoice`, as `AnthropicChatOptions` widens `instructions`. The agent, the
  conversation and the client protocols are generic over the options type, as `RawAgent` is, so a
  `RealtimeAgent[VoiceLiveOptions]` accepts an `AzureVoice`.
- Clients translate the keys, nest audio settings under `audio.input` and `audio.output` for GA-protocol sessions, and
  drop keys they can't honor with a warning, such as AG-UI's `store`. They forward nothing raw. An audio format the
  provider can't take raises before connecting.
- `__aenter__` runs the conversation middleware chain, which opens the provider session. Opening runs `before_run`,
  applies the options and tools with what `before_run` added, replays history, sends initial `messages` as context and
  starts the single receive loop. It never requests a response. `close()` is idempotent and runs the
  [close sequence](#close).
- The update queue is bounded, with a capacity set at start. On overflow the conversation fails with
  `RealtimeBackpressureException` and closes; it never drops or overwrites events. Terminal state lives outside the
  queue, so close and failure always wake iterators.

```text
AgentFrameworkException
└── RealtimeException                         base for realtime failures
    ├── RealtimeControlException              the provider rejected a control call, with its code and param
    │   ├── RealtimeControlTimeoutException   no acknowledgement in time; the provider may still have applied it
    │   └── RealtimeActiveResponseException   a response was active, or the provider dropped the request for one
    ├── RealtimeUnsupportedException          the provider can't do this with the current session
    ├── RealtimeConversationClosedException   send(...) or a control call after close started
    └── RealtimeBackpressureException         the bounded update queue overflowed
```

Arguments that can never work raise `ValueError` before anything is sent: a handoff target on another client or
outside the topology, and app input for a call id the conversation isn't tracking.

## Control calls

| Control | Contract |
| --- | --- |
| `update_session(*, options=None, tools=None)` | `options` merge over the realtime session's settings. `tools`, when given, replace the declared and executable tools together; `[]` clears them. Returns on `session.updated`. Changes neither the agent's defaults nor the agent session. |
| `create_response(*, options=None) -> str` | Per-response options. Returns the response id when the response's own `response.created` arrives. |
| `say(text) -> str` | Verbatim speech. With an Azure voice, Voice Live and Foundry send `pre_generated_assistant_message`. OpenAI, Azure OpenAI, and Foundry with an OpenAI voice request a response with the instruction "Say exactly the following, word for word, and nothing else: ...", which is best effort. Voice Live with an OpenAI voice raises `RealtimeUnsupportedException`. |
| `interrupt(*, played_ms=None)` | Idempotent. Cancels the active response, waits for its `response.done`, and treats the provider's "no active response" error as done. With no active response it returns at once. With `played_ms`, also cuts the unheard audio ([barge-in](#barge-in)). |
| `handoff(agent, *, tools=None)` | See [Handoffs](#handoffs). |
| `send(..., create_response=True)` | Text and message input requests a response unless `create_response=False`. Audio leaves responses to turn detection. App tool results and approvals go to their [batch](#tool-loop). |

- **Ordering.** Control calls are serialized in call order, and each waits for its acknowledgement before the next
  starts. The acknowledgements are `session.updated`, the call's own `response.created`, `conversation.item.truncated`
  and a cancelled response's `response.done`. `send(...)` never waits behind an acknowledgement, and writes never
  interleave. A response requested after `update_session(...)` returns uses the new session, whatever requested it. A
  response in flight doesn't change, and a turn-detection response racing the update has no guarantee.
- **Timeout.** One acknowledgement timeout per conversation, set at start, defaults to about 10 seconds. An update with
  nothing left to send after dropped keys returns at once.
- **Active response.** `create_response(...)` and `say(...)` raise `RealtimeActiveResponseException` while a response
  is active. Nothing is queued or cancelled implicitly.
- **Error claiming.** Requests carry a client `event_id`, and responses an id in `response.metadata`. While a control
  call waits, an error that carries its id, `null` or a server id fails it. An error that echoes another request's id
  doesn't. Claiming runs before deduplication, and only unclaimed back-to-back identical errors count once. Every other
  error becomes `error` content.
- **Lost races.** While `create_response(...)` or `say(...)` waits, a `response.created` without its `metadata` id
  fails the call with `RealtimeActiveResponseException`. A pre-generated `say(...)` takes the first `response.created`
  after its send instead, because Voice Live returns `metadata: null` for those responses.
- **Voice.** Where the provider refuses a voice change, the client drops `voice` before sending, with a warning. Where
  the provider ignores one, the client warns when `session.updated` echoes the old voice.
- **Tools.** A call resolves against the executable set when its `function_call` arrives. Replacing tools doesn't cancel
  running calls, and a call for a tool missing from the current set gets an error result.
- **Raw commands.** `send(RealtimeEvent)` goes out verbatim and untracked. Out-of-band responses
  (`conversation: "none"`), input-buffer commit and clear, item delete, `output_audio_buffer.clear` and untyped session
  fields stay raw. A raw `session.update` that changes tools or instructions leaves the executable tool set out of step
  with the provider's, so those changes go through `update_session(...)`.
- **Samples** start control calls from the update loop as tasks, so the loop keeps reading updates.

### Barge-in

- The app stops its playback on `speech_started` and calls `interrupt(played_ms=...)`. Turn detection has already
  cancelled the response (`interrupt_response`).
- `played_ms` counts the response's audio across its assistant messages in order. The conversation truncates the
  message where playback stopped, clamped below its generated length, and doesn't cut a message heard to within 40 ms of
  its end.
- Whenever `interrupt(...)` or Voice Live's `auto_truncate` cuts a message, the conversation truncates the response's
  later assistant messages at 0 ms.
- `auto_truncate` replaces `played_ms`; the two aren't combined.
- `interrupt(...)` waits for each truncate's acknowledgement.

## Event kinds

- Provider clients set a kind on every `RealtimeEvent`, and core dispatches on it. `service_event_type` stays verbatim:
  clients map Voice Live's preview event names to the same kinds as the GA names, without renaming them.
- Every provider server event becomes a `RealtimeEvent`, and every `RealtimeEvent` one `AgentResponseUpdate`. Payload
  kinds use existing content types, signals use `Content.from_realtime_event(kind=...)`, and everything else gets
  `kind="passthrough"`.
- The kind type is open, `RealtimeKindLiteral` plus `NewType("RealtimeKind", str)`, so updates from a newer runtime
  deserialize. Only core adds kinds; provider packages, third-party ones included, map the rest to `passthrough`.
- Names are snake_case and describe meaning, with `input_` and `output_` for caller and agent media, a noun plus a past
  participle for lifecycle, content type names where the payload has one, and the .NET ADR's names where they overlap.

| Kind | Update | Notes |
| --- | --- | --- |
| `session_created` | realtime-event content | Realtime session id, model, expiry. The only place the session id appears. |
| `session_updated` | realtime-event content | Acknowledges a session update. |
| `speech_started`, `speech_stopped` | realtime-event content, `role="user"` | `audio_start_ms` or `audio_end_ms`, item id. The barge-in cue; there's no `barge_in` kind. |
| `response_created` | realtime-event content, `role="assistant"` | Response id. |
| `response_done` | realtime-event content, `role="assistant"` | Provider `status`, `reason` and `error`. The update sets `finish_reason`. |
| `item_added` | realtime-event content | Item id, previous item id, role. The provider's conversation order. |
| `item_truncated` | realtime-event content, `role="assistant"` | Item id, content index, `audio_end_ms`, whoever truncated. |
| `input_transcription` | `text`, `role="user"` | Caller transcript. |
| `input_transcription_failed` | realtime-event content, `role="user"` | Item id, error code and message. Not `error` content. |
| `output_text` | `text`, `role="assistant"` | Agent transcript, spoken or written. |
| `output_audio` | `data` | Media type from the session's output format; response id, item id, content index. |
| `function_call` | `function_call` | The assembled call, once its arguments finish. Not `informational_only`. |
| `function_result` | `function_result`, `role="tool"` | One for every settled call. |
| `function_approval_request` | `function_approval_request` | Local approvals, and hosted ones with `hosted=True`. |
| `handoff_completed` | realtime-event content | `source` and `target`. |
| `usage` | `usage` | Once per response, from `response.done`. |
| `error` | `error` | Unclaimed provider `error` events, and failed responses. |
| `passthrough` | realtime-event content | Everything else. |

These pass through: audio-done events, output-item and content-part events, input-buffer commits, clears and timeouts,
item done and deleted events, function-call argument deltas, `rate_limits.updated`, `conversation.created`, hosted MCP
call events, and Voice Live's `warning`, animation, timestamp, avatar, video, viseme and `foundry_agent_call` events.

- **Roles and authors.** An update takes the role of the item it's about, and `message_id` is the provider's item id.
  Only assistant and tool updates carry `author_name` and `agent_id`, from the agent that was active when their response
  was created.
- **Transcripts.** Deltas are text with `merge_mode="append"`. The final carries the full text with
  `merge_mode="replace"` and `status="completed"`, built with `Content.from_text(status=...)`, and text merges keep the
  latest status. A future speech-to-text pipeline's interim snapshots would be `replace` without `completed`.
  `run(...)`'s `AgentResponse.messages` hold only the agent's assistant and tool messages, so caller transcripts and
  realtime-event content stay out of them, though both still stream.
- **`finish_reason`.** `completed` maps to `"stop"`, or `"tool_calls"` when the output holds calls. `incomplete` with
  `content_filter` maps to `"content_filter"`, and with `max_output_tokens` to `"length"`. `cancelled` and `failed` map
  to `None`. A failed response also emits `error` content with its `response_id`.
- **Errors.** An unclaimed `error` event becomes `error` content with `message`, `error_code` and, as metadata, the
  provider's `type`, `param` and client `event_id`. Iteration continues; a dead socket or receive-loop failure raises
  from iteration instead.
- **Acknowledgements.** `session.updated` and `response.created` always stream. An error a control call claims streams
  only as a passthrough update.
- **Usage.** `usage` content carries the standard keys and the audio, text and image keys ([Telemetry](#telemetry)).
  Input-transcription usage stays in the caller transcript's provider metadata.
- **Realtime-event content** carries `kind`, the verbatim `service_event_type` and the neutral attributes each kind
  documents as a `TypedDict`. Documented provider extras go in the owning package's namespace, `openai` or `foundry`, as
  ADR 0048 sets out. Passthrough events carry the wire payload as JSON in that namespace. No event copies the full wire
  event, and `created_at` is set only from a provider timestamp. Outbound raw commands are `passthrough`.

## Tool loop

- **Batches.** The conversation collects a response's calls as they arrive. At `response_done`, it runs them as one
  batch through `_execute_function_calls` in a background task beside the receive loop, with chat's rules. Approvals,
  declaration-only calls and `UserInputRequiredException` requests pause the batch until the app answers. The rest run
  concurrently, or in model order with `allow_concurrent_invocation=False`, and function middleware and error shaping
  apply.
- **Results.** Clients send `function_result.result` as the `function_call_output` item's `output` string, never as
  `items` or a raw value. They drop images, audio and files with a warning.
- **Follow-ups.** Once every call in a batch has a result, the loop sends the results in model order and requests one
  follow-up response, after any active response finishes. Follow-ups due together merge. Batches from different
  responses run concurrently. `MiddlewareTermination` sends the results without a follow-up. `run(...)` ends at the
  first response that finishes without tool calls, or when `MiddlewareTermination` stops the loop. Responses that tools
  or middleware request through `say(...)` or `create_response(...)`, such as a filler line, don't end it.
- **Ordering.** Anything a tool awaits completes before its batch's results go out. `ctx.add_tools()` and
  `ctx.remove_tools()` reach the realtime session through `update_session(tools=...)` before the results. That update
  replaces the live tool list's contents in place, so both mechanisms see the same list. A rejected update follows the
  claimed-error rules, and the results and follow-up still go out.
- **Context.** The loop sets `FunctionInvocationContext.realtime_conversation`, in `run(...)` too; elsewhere it's
  `None`. It isn't in the tools' runtime keyword arguments.
- **Configuration and limits.** `function_invocation_configuration` on the agent configures the loop, and function
  middleware comes from the agent's `middleware=`. In a live conversation each tool chain is one request, restarting
  when the caller speaks or the app sends input. `max_iterations` counts follow-ups; the other limits count across the
  chain. When a limit trips, the loop still sends the results and requests the follow-up with `tool_choice: "none"`.
  Unknown calls always get an error result. `run(...)` keeps chat's per-run limits.
- **Slow tools.** There are no timeout or filler settings. Function middleware covers both, by wrapping `call_next()` in
  `asyncio.timeout()` or by saying a filler line and catching `RealtimeActiveResponseException`.
- **Settlement.** Calls from a response that ended `cancelled`, `incomplete` or `failed` don't run; each gets an error
  result, sent on a best-effort basis, and no follow-up. Barge-in never cancels running tools. Close stops new batches,
  cancels running tools and waits for them, settles every unfinished call with an error result that isn't sent, and
  clears pending approvals from the agent session. A settled error's `exception` says why, such as `cancelled` or
  `conversation_closed`. `MiddlewareFailure` ends the conversation the same way, and iteration raises it.
- **Paused runs.** A `run(...)` that ends because its batch paused for an approval, an app-run tool or a user-input
  request leaves those calls pending. It returns them in its `AgentResponse` and keeps the approvals in the agent
  session, so the next run resumes them by replaying the call into its new realtime session. Close settles calls only
  when a live conversation ends or a run fails or is cancelled.
- **App input.** The app answers through `send(...)` with `Content.from_function_result(...)`,
  `Content.from_function_approval_response(...)` or a `Message(role="tool")`, which the conversation routes into its
  batch. In a live conversation an approval request streams as `function_approval_request`, the call stays open while
  the caller talks, and the response matches through the agent session.
- **MCP tools.** `RawAgent`'s MCP lifecycle moves into a helper shared with `RawRealtimeAgent`. A tool connects through
  `async with agent` or on first use and stays open for the agent's lifetime; Agent Framework never closes a tool the
  app connected. The conversation copies MCP tools' functions into its executable set at start and on
  `update_session(tools=...)`; server-side tool-list changes wait for the next update.
- **Hosted tools.** Static helpers on the provider clients declare them, and the loop never runs them. Their call events
  pass through. Hosted approval requests become `function_approval_request` content with `hosted=True`; the app's
  response goes to the provider as `mcp_approval_response`, and the loop then requests a follow-up.

## History

- **Contents.** History holds the provider conversation's authored turns as text: each caller audio item as its final
  caller transcript (`role="user"`), each assistant message item as its final agent transcript, app input as sent
  (including `create_response=False` context), function calls with their settled results, and approval requests and
  responses. It holds no realtime-event content, `error` content, usage, session updates, out-of-band responses or
  hosted-tool events. Each conversation item is one message with the provider's item id as `message_id`.
- **Text-less caller turns.** A caller turn whose transcription failed, wasn't configured, or was still pending after
  close's wait is a `role="user"` message with no contents and a namespaced marker saying why. Partial text before a
  failure is kept as `status="incomplete"`. Replay skips text-less turns. With history providers but no transcription,
  the conversation logs one warning at start and never switches transcription on.
- **Cut-off turns.** After a truncation, history keeps the final agent transcript cut at the word boundary nearest
  `audio_end_ms / generated audio length`, as `status="incomplete"`, and leaves out a turn cut at 0 ms. A response cut
  short without a truncation keeps the text generated so far, as `status="incomplete"`. The full generated text isn't
  kept, and a truncation that arrives after its exchange was reported changes nothing.
- **Order.** History follows `item_added` order (`previous_item_id`), falling back to the order of each item's first
  event. A caller turn keeps its place before the reply even when its transcript arrives later; text fills in by item
  id. Each tool result goes directly after its call, and an exchange waits for its running tools before it's reported.
- **Attribution.** A live conversation writes to the session passed to `start_conversation(session=...)`, through the
  starting agent's context providers. Assistant and tool messages carry the `author_name` of the agent that was active
  when their response was created; the response holding a handoff call belongs to the old agent.
- **Exchanges.** An exchange is input from the caller or the app with the reply and tool chain it drives, up to the next
  input. Consecutive inputs belong to one exchange, and a greeting before any input is an exchange with no input. Each
  gets one `after_run`, where `input_messages` holds its caller turns and app input, and `response.messages` its agent
  turns and tool messages. An exchange is reported once the next has started and its items are settled, in order, in a
  background task. Providers run in reverse registration order, as in chat. `before_run` runs once at start, and its
  context goes with the first report only, while every exchange's context shares the start context's `metadata`. Each
  exchange's `AgentResponse` carries its summed usage and the last response's id and finish reason.
- **Context provider failures.** A failing `after_run` streams a non-fatal `error` update naming the provider. That
  provider's exchanges stay queued and retry with the next report and at close; other providers continue. If it still
  fails at close, `close()` raises after cleanup (an exception group for several). Providers must tolerate seeing an
  exchange twice.
- **Replay.** Opening a realtime session with an agent session replays the loaded history as conversation items: user
  and system text, agent turns as assistant text (`output_text` on GA-protocol services, `text` on Voice Live), each
  call followed by its result, and cut-off turns as their heard text. Agent Framework doesn't cap replay.
- **`run(...)`.** Each run is one exchange. Text input is stored as given, and audio input as its caller transcript.
  The run's session closes through the close sequence before `after_run`. Context providers get the response without
  audio. A failed run reports nothing.
- **Reading.** `RealtimeConversation.history` is a read-only snapshot of the call so far, including exchanges not yet
  reported, and holds the final record after close.
- **Continuation.** `service_session_id` stays unset for every v1 provider. A provider's conversation id travels in
  `session_created` metadata; a Foundry voice agent's `conversation_id` goes in its `foundry` namespace.

### Close

Every close path (`close()`, context exit, hang-up, a provider or receive-loop failure, `MiddlewareFailure`, task
cancellation) runs this sequence:

1. Stop accepting input.
2. If the socket is open, wait for pending caller transcripts, up to about 3 seconds. Skip caller turns whose response
   ended `content_filter`.
3. Close the socket.
4. Settle unfinished tool calls ([Tool loop](#tool-loop)).
5. Report the remaining exchanges.
6. Run conversation middleware's close hooks.
7. Return, or raise context provider and close-hook failures after cleanup.

## Handoffs

- **`handoff(agent, *, tools=None)`** has `update_session(...)`'s contract and makes `agent` the active agent.
  `active_agent` starts as the agent that started the conversation. `tools` adds tools until the next handoff. On a
  rejection or timeout it raises, and the previous agent stays active. The target must use the conversation's client
  instance.
- **`handoff_completed`.** On success, `handoff(...)` emits one update carrying `source` and `target`, named as
  `HandoffSentEvent` names agents (`name`, else `id`), before it returns. Nothing is emitted at start.
- **What changes.** `handoff(...)` recomputes instructions, tools, voice and `tool_choice` as `start_conversation(...)`
  computed them, with the target in place of the starting agent:
  - instructions are the target's, with the call-wide instructions appended (those in a built handoff agent's
    `default_options`, in the start options, and from context providers);
  - tools are the target's, plus the start tools, context-provider tools and `handoff(tools=...)`;
  - voice and `tool_choice` are the target's, unless the start options set one, which then holds for the call.

  Nothing else carries over from the previous agent, and every other setting stays as the call started. Where the
  provider refuses the voice change, the client drops the voice with a warning and the handoff still succeeds.
- **What stays.** History, context providers and tool-loop limits stay with the agent that started the conversation;
  `before_run` doesn't run again, and each handoff re-applies its instructions and tools. A tool chain that spans a
  handoff counts as one chain. Each call runs through the function middleware of the agent active when it arrived. A
  target's MCP tools connect before the session update.
- **`RealtimeHandoffBuilder`** in `agent-framework-orchestrations` shares an internal topology base with
  `HandoffBuilder`, which stays chat-only: `participants(...)`, `with_start_agent(...)` (required),
  `add_handoff(source, targets, *, description=...)`, the default all-to-all mesh and their validation are identical.
  Chat's turn-based options (checkpointing, termination conditions, autonomous mode, return-to-previous, output
  selection) don't exist on it, and a built agent can't be a participant.
- **The built agent** takes `name`, `description`, `context_providers`, call-wide `default_options` (whose instructions
  are appended to every participant's), `function_invocation_configuration` and `middleware`. Participants are realtime
  agents on one client instance and supply instructions, tools, voice, `tool_choice`, function middleware and MCP tools;
  `build()` warns about participant settings that won't apply. `build()` returns a realtime agent with `RealtimeAgent`'s
  layers, whose conversations start with the start agent active and its handoff tools installed. `async with` it enters
  every participant. On its conversations, `handoff(...)` accepts only participants and adds the target's handoff tools.
- **Handoff tools** are named `handoff_to_<name>`, take no arguments, never need approval and return
  `{"handoff_to": "<name>"}`. The description comes from `add_handoff(description=...)`, else the target's
  `description`, else "Handoff to the <name> agent.". The tool awaits `handoff(...)`. If the handoff fails, the tool
  returns a result telling the model the handoff failed and to carry on as the current agent. The builder injects no
  instructions. Of two handoffs in one response, the first to reach `handoff(...)` wins and the others fail.
- **Lifetime.** A handoff lasts for its realtime session; each `start_conversation(...)` and `run(...)` starts with the
  start agent.
- **Foundry voice agents.** Only the start agent can be a `FoundryVoiceAgent`; other participants use its client. Its
  instructions, tools, voice and tool choice come from the captured agent definition, at start and on a handoff back.
  Definitions published by Semantic Kernel (tool names such as `<Plugin>-<function>` and `Handoff-handoff_to_<name>`)
  must be republished to match Agent Framework's tool names.

## Middleware

- **Categories.** Agent middleware wraps `run(...)` only. `start_conversation(...)` skips bare agent middleware with a
  warning and raises `MiddlewareException` for a `MiddlewareBundle` with an agent member. Chat middleware has no seam:
  `categorize_middleware(supported_categories=...)` skips bare chat middleware with a warning and raises for a bundle
  with a chat member, so the agent-hooks bundle raises. Function middleware is unchanged. A chat `Agent` skips
  conversation middleware with a warning.
- **`ConversationMiddleware`** has `process(context, call_next)`. A plain function is conversation middleware when its
  parameter is annotated `ConversationContext` (`MiddlewareType.CONVERSATION`). The chain runs once whenever a realtime
  session opens, in `start_conversation(...)` and inside each `run(...)`.
- **`ConversationContext`** holds the starting agent, `session`, `options`, `tools` and `metadata`, which middleware can
  change before `call_next()`, and `conversation` once the session is open.
- **Hooks.** Middleware registers send hooks (every value passed to `send(...)`, after normalization), update hooks
  (every update yielded, audio included) and close hooks (once, after history is reported, with the exception that
  ended the conversation or `None`). Control calls and the tool loop's own results get no hooks.
- **Outcomes.** A send or update hook returns a replacement, or `None` to keep the item. `MiddlewareTermination` in a
  hook drops the item; in `process()` before `call_next()`, it refuses the session, and `start_conversation(...)` or
  `run(...)` raises it. Any other exception ends the conversation fail-closed, and close-hook failures raise from
  `close()` after cleanup.
- **Order.** Hooks run in onion order. The outermost middleware sees inputs first and updates last. History and
  telemetry record inputs after the send hooks and updates before the update hooks. The tool loop and control calls act
  on provider events, so dropping an update doesn't stop a call. `run(...)` builds its `AgentResponse` from what the
  update hooks deliver.
- **Ownership.** Conversation middleware comes from the agent that started the conversation and applies for the whole
  call. `RealtimeHandoffBuilder(middleware=...)` applies to the built agent; its function middleware wraps every call,
  outside the active participant's own. Participants' agent and conversation middleware don't apply, and `build()`
  warns about them.

## Telemetry

- **Location.** The `RealtimeAgent` telemetry layer instruments `run(...)` and `start_conversation(...)` from the event
  kinds. Provider clients have no telemetry layers and no `Raw*` client classes; they supply the provider name, server
  address and model. A provider client used without an agent emits nothing. `FoundryVoiceAgent` sits over
  `RawFoundryVoiceAgent`.

```text
invoke_agent {owner}                 INTERNAL: the conversation span
├── realtime_inference {model}       CLIENT: one per response
├── execute_tool {tool}              INTERNAL: one per call that ran
│   └── realtime.update_session      a control call made inside a tool
├── realtime.create_response | realtime.say | realtime.update_session | realtime.interrupt
└── realtime.handoff                 only for handoff(...) called outside a tool
```

- **Conversation span.** `invoke_agent {owner}` opens at `start_conversation(...)` under the current span and ends at
  close; `run(...)` gets the same tree. It names the agent that started the conversation (a built handoff agent names
  itself), carries chat `invoke_agent`'s attributes plus `gen_ai.realtime_session.id`, and sums usage over every
  response. Every task the conversation starts (receive loop, tool batches, exchange reports) runs in a copy of the
  context captured inside it, so `realtime_inference` and `execute_tool` spans are its children and siblings of each
  other.
- **Control spans.** Each public `update_session(...)`, `create_response(...)`, `say(...)` and `interrupt(...)` gets an
  INTERNAL `realtime.<name>` span with the realtime session id, provider and any created response id. It records a
  claimed error with the provider's code as `error.type` and ERROR status, except the `response_cancel_not_active`
  that `interrupt(...)` ignores. Tool-loop follow-ups get none.
- **Agents.** `realtime_inference` spans carry `gen_ai.agent.name` and `gen_ai.agent.id` of the agent active when the
  response was created, and `execute_tool` spans those of the agent active when the call arrived. A handoff tool's span
  uses `gen_ai.transfer.mode=pass_control` and `gen_ai.transfer.target.name`, and its `handoff(...)` opens no span. A
  handoff's own session update opens no control span. A `handoff(...)` outside a tool gets `realtime.handoff` with
  source and target attributes. Spans naming a `FoundryVoiceAgent` carry `gen_ai.agent.version` when it pins one.
- **Conventions.** Realtime follows semantic-conventions-genai at commit `750e42c`, unreleased, in Agent Framework's
  latest mode; under the v1.36 baseline it emits the same spans, with mode-dependent attributes following the mode. The
  [per-provider mapping table](https://github.com/adamdougal/agent-framework/blob/research/realtime-telemetry-mapping/docs/research/realtime-telemetry-mapping.md#mapping-table)
  is normative, with these corrections: reasoning tokens sit inside `text_tokens` (plus an undocumented
  `reasoning_tokens`), `gpt-transcribe` reports `duration`, Voice Live reports no transcription usage, failed Voice Live
  responses report `server_error`, and content-filter blocks never carry a category. Where the table and this section
  differ, this section wins.
- **Identity.** Provider names are `openai`, `azure.ai.openai` with Azure routing, and `azure.ai.foundry` for Voice
  Live and Foundry voice agents. `gen_ai.conversation.id` follows chat's rule (a host-supplied id, else a string
  `service_session_id`), captured at start. `gen_ai.realtime_session.id` comes from `session_created`. The event logger
  emits `gen_ai.client.realtime_session.started` and `.ended` for every realtime session, INFO or WARN with
  `error.type` when the session dies.
- **Responses.** Span finish reasons are `stop`, `tool_call`, `content_filter` or `length`; the provider's verbatim
  `turn_detected` or `client_cancelled`, else `interrupted`, for cancelled responses and a response still running at
  close; and `error`, with `error.type` and ERROR status, for failed responses and sessions that die mid-response.
  Content filtering and interruptions leave the status unset. There's no `gen_ai.response.status`. Empty cancelled
  responses are recorded like any other.
- **Usage.** `UsageDetails` gains `audio_input_token_count`, `text_output_token_count`,
  `image_cache_read_input_token_count` and their siblings, one per convention attribute, mapped in latest mode to
  `gen_ai.usage.{text,audio,image}.{input,output}_tokens` and `.cache_read.input_tokens`. Transcription usage isn't
  recorded. Each response records `gen_ai.client.operation.duration` and `gen_ai.client.token.usage` with
  `gen_ai.operation.name=realtime_inference`; there are no realtime-specific metrics.
- **Errors.** A session that dies marks the conversation span ERROR with `error.type` and any close code; a normal close
  leaves it unset, and a `close()` that raises marks it ERROR. Unclaimed provider errors and context provider failures
  become `agent_framework.realtime.error` events (message only with sensitive data on) and never change a span's
  status. Each `item_truncated` becomes an `agent_framework.realtime.item_truncated` event with the response id, item
  id and `audio_end_ms`. Exchange reports get no spans.
- **Cancelled tools.** `FunctionTool.invoke` records a cancelled call as ERROR with `error.type` `cancelled`, or
  `conversation_closed` when closing the conversation cancelled it. Calls that never ran get no span.
- **Content.** Telemetry never records audio. With sensitive data on, `realtime_inference` spans record the agent
  transcript as `transcription` parts plus text and `tool_call` parts, the app input and any caller transcript final
  before the span ends, and `gen_ai.system_instructions` as of the response's creation. The live conversation span
  records no messages; `run(...)`'s records input and output as chat does.
- **Azure SDKs.** Each Azure client warns once when its SDK's instrumentor is active, naming the duplicate spans and how
  to turn it off. `VoiceLiveInstrumentor` adds a span per outbound event, including each audio append (1,176 spans in a
  30-second call). `FoundryVoiceAgentClient` injects `traceparent` and `tracestate`, never baggage, from the
  conversation span when it connects, unless the app's `AIProjectInstrumentor` already does.

## Provider clients

| Provider | Classes | Install | Transport |
| --- | --- | --- | --- |
| OpenAI, Azure OpenAI | `OpenAIRealtimeClient`, `OpenAIRealtimeOptions` | `agent-framework-openai[realtime]` | `openai` SDK |
| Azure Voice Live | `VoiceLiveClient`, `VoiceLiveOptions` | `agent-framework-foundry[realtime]` | `azure-ai-voicelive` |
| Foundry voice agents | `FoundryVoiceAgent`, `FoundryVoiceAgentClient`, `FoundryVoiceAgentOptions` | `agent-framework-foundry[realtime]` | `azure-ai-projects` (`.beta.voice_agents.realtime`) |

- **Shared base session.** `OpenAIRealtimeSession`, public in `agent-framework-openai`, implements the typed control
  methods, kinds, string tool outputs, replay, error claiming and the barge-in cut for services that speak the OpenAI
  Realtime protocol. It maps wire JSON over an `OpenAIRealtimeConnection` (`send`, `receive`, `close`) that each client
  opens through its service's SDK, with no other SDK models and compression off. The connection maps transport errors
  and closes to the realtime exceptions. Voice Live's and Foundry's sessions subclass it privately.
- **Azure OpenAI** is routing on `OpenAIRealtimeClient`: `azure_endpoint`, `api_key` or `credential` (the
  `cognitiveservices` scope), the deployment as `model`, and the `AZURE_OPENAI_*` fallbacks. It connects through
  `AsyncOpenAI(websocket_base_url=".../openai/v1")` without an `api_version`, and raises when given an
  `AsyncAzureOpenAI` client. Model variables are `OPENAI_REALTIME_MODEL` and `AZURE_OPENAI_REALTIME_MODEL`, falling back
  to `*_MODEL`.
- **Voice Live.** `VoiceLiveClient` pins `api-version=2026-07-15`, whose session is flat, with `response.audio.*` events
  and no `session.type`. It accepts later dated versions of that shape and raises for `v1`. It maps the preview event
  names to the kinds. A managed model needs `model=`; BYOM takes `profile=`, `model=<deployment>` and optionally
  `foundry_resource_override`, sent through the SDK's `query`. It authenticates with `api_key` or `credential`, adapting
  token-provider callables and sync credentials, with `https://ai.azure.com/.default` as the default scope. It sends its
  first `session.update` before any other event, drops per-response `instructions` with a warning, tracks the active
  response itself, and offers `get_mcp_tool()` and `get_foundry_agent_tool()`. It reads `AZURE_VOICELIVE_ENDPOINT`,
  `AZURE_VOICELIVE_API_KEY`, `AZURE_VOICELIVE_MODEL` and `AZURE_VOICELIVE_API_VERSION`.
- **Foundry voice agents.** `FoundryVoiceAgent` takes `project_client`, or `project_endpoint`, `agent_name`,
  `agent_version` and a `credential` for the `https://ai.azure.com/.default` scope. It reads the same `FOUNDRY_*`
  variables as `FoundryAgent`, and other participants use `foundry_agent.client`. The client sends `agent_version` only
  as the `x-agent-version-override` header, and connects through `AIProjectClient.beta.voice_agents.realtime.connect()`
  with `api-version=v1` and `Foundry-Features: VoiceAgents=V1Preview`. Before sending anything it waits for the agent
  definition's unsolicited `session.updated`, raising if it doesn't arrive within the acknowledgement timeout, and keeps
  the definition read-only. It restores the definition's voice as an `AzureVoice` from `voice` and `voice_type`.
  `tools=` supplies implementations matched by name; a definition function without one gets an error result and a
  warning at start. Added tools are declared alongside the definition's tools, re-sent as published; with nothing added,
  no `tools` key is sent. Both `conversation.item.created` and `.added` map to `item_added`.
- **Packages.** `agent-framework-openai[realtime]` installs `openai[realtime]` (`websockets>=13,<16`).
  `agent-framework-foundry[realtime]` adds `azure-ai-voicelive[aiohttp]>=1.3.1,<2` and raises `azure-ai-projects` to
  `>=2.7,<2.8`. Without the extra, the Azure clients raise an install hint. `agent_framework.openai` and
  `agent_framework.foundry` re-export the classes. Fields the base session writes go in the `openai` metadata namespace,
  and Voice Live's and Foundry's extras in `foundry`, because
  [spec 005](005-python-content-identity-and-protocol-metadata.md#metadata-namespaces) names a namespace after its
  owning package.

## Provider capabilities

From live probes on 2026-11-04 and the prototype on 2026-11-06. The OpenAI column is inferred from Azure OpenAI, which
speaks the same GA protocol, until live tests verify it.

| Capability | OpenAI (inferred) | Azure OpenAI | Voice Live `2026-07-15` | Foundry voice agent (preview) |
| --- | --- | --- | --- | --- |
| Voice change after the first audio | No | No (`cannot_update_voice`); dropped with a warning | Azure voices: yes. From an OpenAI voice: ignored silently; the client warns | Azure to Azure: yes. Azure to OpenAI: rejected; dropped before sending |
| Verbatim speech (`say(...)`) | Best-effort prompt | Best-effort prompt | Azure voice: `pre_generated_assistant_message`. OpenAI voice: raises | Azure voice: `pre_generated_assistant_message`. OpenAI voice: best-effort prompt (not probed) |
| Per-response overrides | Yes | Yes | `instructions` ignored; dropped with a warning | Yes |
| `metadata` echo on `response.created` | Yes | Yes | Yes, except `null` for pre-generated responses | Yes (pre-generated not probed) |
| `event_id` echo on errors | As Azure OpenAI | Yes, except rejected tool schemas, `cannot_update_voice` and bad truncate offsets | Never; some errors arrive three times | Never |
| Errors for invalid events | As Azure OpenAI | `conversation_already_has_active_response`, `response_cancel_not_active` | None: dropped silently; events before the first `session.update` are rejected | None: dropped silently |
| Transcript read back after a truncate | Empty | Empty | Azure voice: the cut text. OpenAI voice: empty | The cut text |
| `auto_truncate` | Rejected | Rejected | Supported, off by default | Not probed |
| Cancel reasons | `turn_detected`, `client_cancelled` | `turn_detected`, `client_cancelled` | As OpenAI, but with an Azure TTS voice a client cancel can report `turn_detected` | With an Azure TTS voice, a client cancel can report `turn_detected` |
| Empty cancelled responses | Not observed | Not observed | A short VAD silence (the probed default of 200 ms) splits one utterance into turns whose responses are cancelled at once; 500 ms didn't split | Not probed |
| Content filtering | `incomplete` with `content_filter`, no category, never `error` | As OpenAI | BYOM as Azure OpenAI; managed models never content-filtered | Not probed |
| `audio_tokens` in usage | Model tokens | Model tokens | Cascaded and BYOM convert Azure Speech audio (10 per second in, 20 out); BYOM with `azure-speech` counts input twice | Converted Azure Speech audio |

On every provider, an `interrupt(...)` sent on `speech_started` races the server's own cancel, so a barge-in can also
report `client_cancelled`. With an Azure TTS voice, `response.done` can follow a call's arguments by 0.3 to 2.5 s, and
`session.updated` arrives 53 to 376 ms after a session update.

## Changes to existing packages

Under `REALTIME` (or `FOUNDRY_VOICE_AGENTS` for the Foundry voice agent classes):

- `agent-framework-core`: the `realtime_event` content type and `Content.from_realtime_event(kind=...)`, accepted by
  `ContentType` and `Content.from_dict`; the kind types; `FunctionInvocationContext.realtime_conversation`;
  `ConversationMiddleware`, `ConversationContext`, `MiddlewareType.CONVERSATION` and the conversation category in
  `categorize_middleware`; `RawRealtimeAgent`, `RealtimeAgent`, `RealtimeConversation`, `RealtimeEvent`, the
  client/session protocols, the options types and the realtime exceptions; the `realtime.*` span names and the
  `agent_framework.realtime.error` and `agent_framework.realtime.item_truncated` events.
- `agent-framework-orchestrations`: `RealtimeHandoffBuilder` and its built agent.
- `agent-framework-openai`: `OpenAIRealtimeClient`, `OpenAIRealtimeOptions`, `OpenAIRealtimeSession`,
  `OpenAIRealtimeConnection` and the `[realtime]` extra.
- `agent-framework-foundry`: `VoiceLiveClient`, `VoiceLiveOptions`, the `Azure*` option types and the `[realtime]`
  extra; `FoundryVoiceAgent`, `RawFoundryVoiceAgent`, `FoundryVoiceAgentClient` and `FoundryVoiceAgentOptions` under
  `FOUNDRY_VOICE_AGENTS`.

On stable APIs, all additive: `Content.from_text(status=...)` and text merges that keep the latest status; the audio,
text and image `UsageDetails` keys and their telemetry mapping; `FunctionTool.invoke` recording cancellation as ERROR
(its own PR, first); the internal MCP helper shared by `RawAgent` and `RawRealtimeAgent`; the internal topology base
shared by `HandoffBuilder` and `RealtimeHandoffBuilder`; and `ExperimentalFeature.REALTIME` and
`ExperimentalFeature.FOUNDRY_VOICE_AGENTS`.

## Version compatibility

- Updates with kinds a runtime doesn't know still deserialize, because the kind type is an open string, and
  `realtime_event` content round-trips through `Content.from_dict`.
- The `REALTIME` family graduates together; the Foundry voice agent classes graduate separately, with the service.
- Moving Voice Live to a GA-shaped API version, or the conventions past `750e42c`, changes no public API.

## Validation

- Unit tests with fake realtime sessions cover the conversation lifecycle, bounded-queue backpressure, single-reader
  receive, control acknowledgements, error claiming before deduplication, lost races, the barge-in cut, the close
  sequence, every kind mapped from GA and Voice Live preview payloads, and `realtime_event` serialization.
- Tool-loop tests cover batching, follow-ups, ordering against session updates, settlement, paused runs, approvals and
  app-run tools through the shared executor.
- History tests cover spoken order with late transcripts, cut-off and text-less turns, per-exchange reporting, provider
  retries and replay. Handoff, middleware and telemetry tests cover the active agent, the builder and the span tree.
- Regression tests show the MCP helper and the topology base don't change chat behavior.
- Live integration tests, marked `flaky` and `integration` with environment-based skips, cover each provider and verify
  the inferred OpenAI column. Dependency probes cover both `[realtime]` extras on every supported Python version.

