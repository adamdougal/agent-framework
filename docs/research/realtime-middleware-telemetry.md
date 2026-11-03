# Middleware and telemetry for full-duplex realtime traffic

Research for [Middleware and telemetry for full-duplex realtime traffic](https://github.com/adamdougal/agent-framework/issues/5),
on the map [Python realtime agents in Agent Framework](https://github.com/adamdougal/agent-framework/issues/1).
Researched 2026-11-03. Every source is pinned to a commit, tag or release (see [Sources](#sources)).

## Question

How do Agent Framework's middleware and telemetry layers work today, and what would it take for full-duplex realtime
traffic (audio in and out, transcripts, tool calls, handoffs) to flow through them? What do the OpenTelemetry GenAI
semantic conventions say about realtime or voice sessions?

## Answer

- **AF middleware is call-scoped, not conversation-scoped.** Agent middleware wraps one `run()`, chat middleware wraps
  one model call on a chat client, and function middleware wraps one tool call. Each chain runs once per call, data
  flows through a context object, and streaming works by attaching hooks to a `ResponseStream` that ends in a final
  response. Nothing intercepts `send()`, received updates or `close()` on a live conversation, which is what the open
  [ADR 0035 review thread][pr-6831-r3953440213] asks about.
- **Only function middleware maps one-to-one onto realtime.** A realtime tool call is still one tool call. ADR 0035's
  shared function-invocation executor would give realtime tool calls function middleware, `execute_tool` spans and the
  tool-duration metric as they are. A realtime agent has no chat client, so chat middleware (and the chat-seam half of
  the experimental agent-hooks bundle) has nowhere to run. AF already has a way to make that fail loudly for bundles.
- **AF telemetry assumes a bounded call.** Streaming `invoke_agent` and `chat` spans end in a cleanup hook that awaits
  the final response, and child spans are parented by activating the span around each pull of the stream. A
  conversation with its own receive loop and no final response needs explicit span parenting and its own span
  lifecycle.
- **AF telemetry doesn't handle audio.** It has no audio token attributes. With sensitive data on, audio `data`
  content would be written as base64 into `gen_ai.input.messages` and `gen_ai.output.messages` (the accelerator turns
  content recording on by default). An experiment shows a cancelled tool call is recorded exactly like a success, so
  barge-in or hang-up abandoning a tool would be invisible.
- **Released OTel GenAI conventions say nothing about realtime.** The last release containing them is v1.41.1. They
  have moved to the unreleased `semantic-conventions-genai` repository, whose main branch adds audio token attributes
  and an agent-transfer (handoff) tool span. A realtime proposal there, approved but unmerged, defines a
  `realtime_inference` CLIENT span per provider generation, an opt-in `user_speech` span bounded by voice-activity
  events, `gen_ai.client.realtime_session.started`/`.ended` events, `gen_ai.realtime_session.id` and a `transcription`
  message part. It deliberately has no session span or turn span, and leaves metrics and tool events that happen during
  a generation to open issues.
- **Other frameworks split two ways.** LiveKit and Pipecat keep a long-lived session or conversation span with turn
  spans under it. .NET Microsoft.Extensions.AI instruments the realtime client with short per-response spans and no
  session span. The OpenAI Agents SDK relies on OpenAI's server-side trace plus a typed event stream, and runs output
  guardrails on debounced transcripts, interrupting the response when one trips.
- **The accelerator builds all of its voice telemetry itself** from Semantic Kernel's event stream: SK's realtime
  clients emit no telemetry, and only SK kernel functions emit `execute_tool` spans. Session lifecycle, per-response
  spans with usage and content-filter finish reasons, tool spans (including abandoned ones), handoffs, transcripts,
  errors and trace-context propagation to the provider are framework-shaped. The WebSocket and call root, connection and
  call-duration metrics, the `cc.*` business counters and call recording stay in the app.

So the contract ticket has four choices to make: a span topology (a session span, or semconv-style events plus
per-response spans), a middleware entry point (function middleware only, agent middleware around `start_conversation()`,
a new conversation middleware category, or read-only observers), a semconv target, and a policy for audio content and
cancellation. The options and their trade-offs are in [Options](#options).

## Findings

### 1. Agent Framework middleware today

**Types and contexts.** Python AF has three middleware categories. Each has a context object and a
`process(context, call_next)` contract in which "all data flows through the context" ([middleware types][af-mw-types]):

| Category | Wraps | Context | Notable fields |
| --- | --- | --- | --- |
| Agent | one `agent.run()` | `AgentContext` ([source][af-mw-agentcontext]) | `messages` (the normalised run input), `session`, `tools`, `options`, `stream`, `metadata`, `result` (`AgentResponse`, or `ResponseStream` when streaming), and stream hooks: update and result transforms, update and result gates, buffering, cleanup hooks |
| Chat | one `get_response()` model call on a chat client | `ChatContext` ([source][af-mw-chatcontext]) | `messages`, `options`, `stream`, `session`, `result` (`ChatResponse` or `ResponseStream`), and the same stream hooks |
| Function | one tool invocation | `FunctionInvocationContext` ([source][af-mw-funccontext]) | `function`, `arguments`, `session`, `metadata` (including `call_id`), `result`, and a live, mutable `tools` list for progressive tool exposure |

**Pipelines.** Each pipeline builds its chain and runs it once per call; `MiddlewareTermination` stops the chain early
with an optional substitute result. After the agent and chat chains unwind, the pipeline copies the hooks, gates and
cleanup hooks registered on the context onto the returned `ResponseStream` ([agent pipeline][af-mw-agentpipeline],
[chat pipeline][af-mw-chatpipeline]). The chat pipeline rejects a streaming result that isn't a `ResponseStream`
([source][af-mw-chat-stream-check]). The function pipeline lets `MiddlewareTermination` propagate to stop the tool loop
([source][af-mw-funcpipeline]); ordinary exceptions become tool-error results, and `MiddlewareFailure` aborts the run
fail-closed ([semantics][af-mw-failure]).

**Composition.** `Agent` is `AgentMiddlewareLayer` over `AgentTelemetryLayer` over `RawAgent`
([source][af-agent-class]). A chat client such as `OpenAIChatClient` is `FunctionInvocationLayer` over
`ChatMiddlewareLayer` over `ChatTelemetryLayer` over the raw client ([source][af-openai-client]).
`AgentMiddlewareLayer.run()` runs the agent chain and forwards chat and function middleware to the chat client through
`client_kwargs["middleware"]` ([source][af-mw-forward]); streaming runs are wrapped with `ResponseStream.from_awaitable`
([source][af-mw-agent-stream]). Function middleware runs per tool call inside `_auto_invoke_function`, and the
`execute_tool` span opens in the innermost handler, `FunctionTool.invoke`, so time spent in function middleware falls
outside the tool span ([auto-invoke][af-tools-autoinvoke], [invoke span][af-tools-invoke-span]). The executor is
already a private function bound per call with `partial` ([executor][af-tools-executor], [binding][af-tools-binding]):
that's the seam [ADR 0035 proposes to extract][adr35-fi].

**Bundles and seams.** `MiddlewareBundle` groups middleware that only works as a unit, such as agent-hooks.
`categorize_middleware(supported_categories=...)` lets a seam that can't install a category skip bare middleware with
a warning, and raise for bundle members ([bundle][af-mw-bundle], [categorize][af-mw-categorize]). The experimental
agent-hooks feature rides all three seams (`input`/`output` on the agent seam, `pre_model_call`/`post_model_call` on the
chat seam, `pre_tool_call`/`post_tool_call` on the function seam). It supports streaming only "fail-closed by
buffering", consuming the whole stream before releasing any of it ([agent hooks][af-hooks]).

**Related decisions.** [ADR 0013][adr-0013] (accepted) folded streaming into one `run(..., stream=)` and
`get_response(..., stream=)` method, so every layer branches on `stream` and returns a `ResponseStream`.
[ADR 0016][adr-0016] (accepted) chose run-scoped `before_run`/`after_run` hooks for context providers.
[ADR 0007][adr-0007] (middleware) and [ADR 0003][adr-0003] (OpenTelemetry) are proposed and .NET-centric. Since ADR
0035's review, `ResponseStream` has gained an idempotent `close()` and `async with` support, so abandoning a bounded
stream early now runs its cleanup hooks ([context manager][af-rs-aexit], [close][af-rs-close];
[Python: fix(openai): close the chat-completions SDK stream when the consumer stops early][af-pr-8773],
[Python: Pass stream errors to nested streams when closing them][af-pr-9476]). That settles early exit for half-duplex
`run(stream=True)`, but not for conversations.

**Assumptions that break for a long-lived duplex conversation.**

| Assumption today | Evidence | Why it breaks |
| --- | --- | --- |
| Input arrives as one batch per call | `AgentContext.messages` is normalised once from the `run()` input ([source][af-mw-forward]) | Conversation input arrives over time through `send()`: text, audio chunks and raw events ([ADR 0035][adr35-conv]) |
| Each call has one result with a terminal value | `context.result` is an `AgentResponse` or a `ResponseStream` with a finaliser; result hooks and gates run at finalisation ([source][af-mw-agentcontext]) | The conversation's stream has no final `AgentResponse`; it ends only on close or failure ([ADR 0035][adr35-conv]) |
| Post-processing happens after `call_next()` returns | Middleware observes `context.result` after `call_next()` ([source][af-mw-types]) | If `call_next()` returns when the conversation opens, nothing after it sees any traffic. If it returns at close, every middleware frame stays on the stack for the whole call |
| Enforcement can buffer | Result gates and agent-hooks buffer the stream before releasing it ([agent hooks][af-hooks], [gate warning][af-mw-gate-warning]) | Holding audio until a response ends defeats realtime. The OpenAI Agents SDK interrupts the response when a debounced transcript guardrail trips instead ([source][oai-guardrails]) |
| `call_next()` can run again | `AgentLoopMiddleware` calls `call_next()` in a loop ([source][af-loop]) | Re-entering a live session isn't a retry, so existing agent middleware can't be applied to conversations blindly |
| There is a chat seam | Chat middleware and agent-hooks' model-call points run in `ChatMiddlewareLayer` ([source][af-hooks]) | A realtime agent has no chat client, so model-call interception has to be redefined (per response or per event) or chat middleware rejected ([categorize][af-mw-categorize]) |
| Context travels with the consumer | Telemetry sets context variables and activates spans around each pull, in the consumer's context ([source][af-obs-agent-pull]) | ADR 0035 gives the conversation a single receive loop of its own ([ADR 0035][adr35-conv]); work done there (tool calls, per-response spans) doesn't run inside the consumer's pulls |

### 2. Agent Framework telemetry today

**Spans.** `invoke_agent {agent}` (INTERNAL) comes from `AgentTelemetryLayer` ([source][af-obs-agentlayer]),
`chat {model}` from `ChatTelemetryLayer` ([source][af-obs-chatlayer]), and `execute_tool {tool}` from
`FunctionTool.invoke` ([span helper][af-obs-funcspan], [invoke][af-tools-invoke-span]); there are also workflow and MCP
CLIENT spans. Attributes come from the `OtelAttr` enum: operation, provider, model, agent id, name and description,
`gen_ai.conversation.id`, usage (input, output, cache and reasoning tokens), tool name, id, type, arguments and result,
and input and output messages ([source][af-obs-otelattr]).

**Streaming spans.** A streaming call starts a span that isn't made current, activates it around each iterator pull so
child spans parent correctly, and ends it in a cleanup hook. That hook awaits `get_final_response()` before setting the
response id, finish reason, usage and output messages. A weakref finaliser ends the span if the stream is never
consumed ([chat][af-obs-chat-stream], [agent][af-obs-agent-stream], [helpers][af-obs-activate]). Inner chat spans add
their usage to a context variable that the agent span applies at the end ([source][af-obs-usage-accum]). So a span
lives exactly as long as its bounded stream, and its useful attributes need a terminal response.

**Metrics.** The chat layer records `gen_ai.client.operation.duration` and `gen_ai.client.token.usage`
([histograms][af-obs-histograms], [recording][af-obs-capture-response]). `AgentTelemetryLayer` creates the same
histograms but passes neither to `_capture_response`, so `invoke_agent` records no metrics
([constructor][af-obs-agent-init], [streaming capture][af-obs-agent-capture],
[non-streaming capture][af-obs-agent-capture-2]). Tools record `agent_framework.function.invocation.duration`, with
`error.type` on failure ([source][af-tools-invoke-span]).

**Audio and other media.** `observability.py` never mentions audio. `USAGE_DETAIL_TO_OTEL_ATTR` maps no audio usage
keys ([source][af-obs-usage-map]), even though the OpenAI chat-completions client already reports `prompt/audio_tokens`
and `completion/audio_tokens` ([source][af-openai-audio-usage]). With the latest conventions and sensitive data on, a
`data` content item is serialised as a `blob` part carrying the base64 payload and `modality: "audio"`
([source][af-obs-part]). Realtime audio routed through that code would put raw audio into span attributes, and the
accelerator turns content recording on by default (`config.yaml:387`, `config.py:285`).

**Content and configuration.** Instrumentation is on by default. Message content needs `ENABLE_SENSITIVE_DATA` or
`enable_sensitive_telemetry()`, and tool arguments and results need both sensitive data and the latest conventions
([settings][af-obs-sensitive]). `configure_otel_providers()` wires up exporters ([source][af-obs-configure]).
`create_metric_views()` keeps only `agent_framework*` and `gen_ai*` instruments and drops everything else
([source][af-obs-views]), so app metrics such as `cc.*` would vanish if an app passed it unchanged. AF already emits
OpenTelemetry events through an event logger, for the v1.36 message events ([logger][af-obs-logger],
[emit][af-obs-events]); realtime session start and end events could reuse it.

**Semantic-conventions version.** AF supports the v1.36.0 baseline and "latest" (anything above v1.36.0) through
`OTEL_SEMCONV_STABILITY_OPT_IN=gen_ai_latest_experimental`, and defaults to latest ([source][af-obs-semconv]). The
newest attribute it emits, `gen_ai.usage.reasoning.output_tokens`, first appears in v1.41.0
([v1.41.0 registry][semconv-1410-registry]), so "latest" means v1.41.x in practice. AF doesn't emit
`gen_ai.response.time_to_first_chunk`, also new in v1.41.0. Python core depends on `opentelemetry-api>=1.39.0`
([pyproject][af-core-pyproject]).

**Experiment: cancelled tool calls.** I ran one cancelled and one failing tool call through `FunctionTool.invoke` with
an in-memory span exporter and metric reader, on `agent-framework-core==1.24.0` from PyPI (its `invoke` span block is
identical to the pinned commit). The script is in [Appendix: experiment](#appendix-experiment).

| Call | `execute_tool` span status | `error.type` on the span | `agent_framework.function.invocation.duration` dimensions |
| --- | --- | --- | --- |
| Cancelled after 0.2 s with `task.cancel()` | UNSET | none | no `error.type`, so it looks like a success |
| Raised `RuntimeError` | ERROR | `RuntimeError` | `error.type=RuntimeError` |

`FunctionTool.invoke` catches `Exception`, and `asyncio.CancelledError` isn't one, so its `finally` block records the
duration with success dimensions ([source][af-tools-invoke-span]). In a realtime call, barge-in and hang-up cancel
in-flight tools. The accelerator records those as `outcome=abandoned` with an ERROR span
(`call_span_manager.py:397-414`).

### 3. OpenTelemetry GenAI semantic conventions

**Where they live.** The GenAI conventions were part of
[open-telemetry/semantic-conventions][semconv-releases] up to v1.41.1 (released 2026-05-11). From v1.42.0 (2026-06-12)
those pages say the conventions have moved to [open-telemetry/semantic-conventions-genai][genai-repo]
([v1.42.0 notice][semconv-moved]). That repository hasn't released anything yet; its README's schema URL still reads
"TODO" ([README][genai-readme]). The core repository's latest release is v1.45.0 (2026-11-02). Every GenAI convention
has Development status.

**Released (v1.41.1).** Nothing on realtime, voice, sessions, turns or interruptions, and no audio token attributes.
`gen_ai.output.type` already has a `speech` value ([registry][semconv-1411-registry]).

**Unreleased main branch of semantic-conventions-genai** (commit `95aff9c`):

- Per-modality usage attributes, including `gen_ai.usage.audio.input_tokens` and `gen_ai.usage.audio.output_tokens`,
  which count towards the totals ([registry][genai-audio-usage], [changelog][genai-cl-440e]).
- Token counters split by `gen_ai.token.modality` (`text`, `image`, `audio`, `unknown`) that replace the
  `gen_ai.client.token.usage` histogram AF records today ([changelog][genai-cl-374b], [changelog][genai-cl-374e]); a
  new `gen_ai.client.inference.duration` metric and renamed time-to-chunk metrics ([changelog][genai-cl-521]); and
  `gen_ai.usage.cache_creation.input_tokens` renamed to `gen_ai.usage.cache_write.input_tokens`
  ([changelog][genai-cl-440b]).
- An agent-transfer refinement of `execute_tool` for tool calls that hand work to another agent, with
  `gen_ai.transfer.mode` (`return_to_caller` or `pass_control`) and `gen_ai.transfer.target.name`
  ([span][genai-transfer-span], [attributes][genai-transfer-attrs], [changelog][genai-cl-447]). An in-session handoff
  triggered by a tool call would be a `pass_control` transfer.

**The realtime proposal.** [Add semantic conventions for realtime GenAI sessions][genai-pr-realtime] was opened on
2026-07-23. By 2026-11-03 it had three approving reviews, but it isn't merged and has merge conflicts. At head
`f80be17` it defines:

- **A `realtime_inference` span** (CLIENT, named `realtime_inference {model}`), one per provider-reported generation.
  Start (a server event or the first output chunk) and end (completion or interruption) are best effort, and the span
  isn't reported if either boundary can't be determined. A provider that ends a generation at a tool call gets two
  spans. It isn't created by default for long-running transcription or translation. Finish reasons carry the
  provider's interruption reason, or `error` for an abnormal end. Input and output messages leave out tool events that
  happen during the generation. Attributes include `gen_ai.realtime_session.id`, the model, usage including audio
  tokens, and the opt-in content attributes ([spec][pr394-span], [finish reasons][pr394-finish],
  [messages][pr394-messages]).
- **A `user_speech` span** (INTERNAL, opt-in) for one user utterance, recorded only when its start and end are
  reliable: from voice-activity events such as OpenAI's `input_audio_buffer.speech_started` and `speech_stopped`, or
  from client push-to-talk. It carries no token usage ([spec][pr394-user-speech]).
- **`gen_ai.client.realtime_session.started` and `.ended` events** for the session lifecycle; `.ended` carries
  `error.type` and WARN severity on failure ([spec][pr394-events], [model][pr394-events-model]).
- **`gen_ai.realtime_session.id`**: the provider's session id or a client per-connection id. Instrumentations "MUST
  NOT use `gen_ai.conversation.id` as a realtime session identifier" ([spec][pr394-session-id]).
- **A `transcription` message part** for input and output audio transcripts, alongside `blob` parts whose `content` is
  base64 ([schema][pr394-transcription]).
- Its OpenAI reference scenario says the session "is not an operation", and that `execute_tool` isn't instrumentable at
  the model client because tools run in app code ([scenario][pr394-openai-scenario]). A framework that runs the tools,
  as AF does, is where `execute_tool` spans belong.

**Deliberately deferred**, as open issues in semantic-conventions-genai:

- [Define session and turn modeling for realtime GenAI APIs][genai-issue-567]: a long-lived session span or a pair of
  lifecycle events? An optional logical turn span? How should user input be associated with the responses to it?
- [Define telemetry for continuous realtime generation and usage attribution][genai-issue-568]: continuous models, and
  usage that arrives after a generation has ended.
- [Define duration and token usage metrics for realtime inference][genai-issue-569]: there are no realtime metrics yet,
  and the generic duration metric is itself being reconsidered.
- [Model intermediate events during realtime inference][genai-issue-633]: tool calls and results while one generation
  stays open.
- [Additional configuration for recording binary content on attributes?][genai-issue-515]: whether audio and video
  need their own opt-in. The latest maintainer view is one content opt-in, plus declarative-config knobs and SDK
  attribute limits.

**Instrumentation libraries.** OpenTelemetry's Python OpenAI instrumentation has no realtime support;
[Realtime support for OpenAI Agents SDK][contrib-issue-4470] is an open request.

### 4. How other realtime frameworks instrument sessions

| Framework (version) | Session-level signal | Per response or turn | Tools | Handoffs | Interception model |
| --- | --- | --- | --- | --- | --- |
| .NET Microsoft.Extensions.AI (`dotnet/extensions` at `47dd770`) | none from the OpenTelemetry layer; the function-invoking layer wraps the receive loop in a non-standard `orchestrate_tools` span unless an `invoke_agent` span is current | a near-instant `realtime {model}` CLIENT span per `response.done`, with `gen_ai.operation.name=chat`; a span per sent message only when sensitive data is on | `execute_tool {name}` from a function-invocation processor shared with the chat client | not modelled | a delegating client pipeline (`UseFunctionInvocation`, `UseOpenTelemetry`, `UseLogging`) |
| OpenAI Agents SDK 0.24.0 | OpenAI's server-side Realtime trace (`tracing: "auto"` sent in `session.update`) | server-side | `tool_start` and `tool_end` session events | `handoff` session event | typed session events; transcript guardrails that interrupt |
| LiveKit Agents 1.8.10 | `agent_session` root span | `user_turn` and `agent_turn` spans (`agent_turn` has `invoke_agent` semantics) with `realtime_inference` children (or `llm_node` and `tts_node` in pipelines); `user_speaking` and `agent_speaking` spans | `function_tool` span | `update_agent` span | an event emitter plus overridable pipeline nodes |
| Pipecat 1.15.0 | `conversation` span (tracing is opt-in) | `turn` spans with `turn.was_interrupted`, durations and user-to-bot latency; realtime `llm_setup`, `llm_request` and `llm_response` spans | function-call attributes on the realtime spans | not modelled in core tracing | frame observers |

- **Microsoft.Extensions.AI (.NET).** Added in [Realtime Client Proposal][meai-pr-7285], it follows semantic
  conventions v1.41 "where applicable" and notes the spec defines no realtime operation ([remarks][meai-remarks]).
  There's no session span: each `response.done` creates a near-instant `realtime {model}` CLIENT span with usage, time
  to first chunk and custom `gen_ai.realtime.*` attributes (voice, output modalities, session kind, received
  modalities) ([response span][meai-response-span], [activity][meai-activity], [constants][meai-consts]). `SendAsync`
  creates a span per content-bearing client message only when sensitive data is on ([send][meai-send]). The duration
  stopwatch, the time to first chunk and the output-message and modality accumulators all start when the receive loop
  starts and are never reset, so per-response duration and output messages are cumulative over the session
  ([source][meai-receive]). Tool calls go through a function-invocation processor shared with the chat client, which
  emits `execute_tool {name}` spans ([processor][meai-fi-processor]); the realtime function-invoking session wraps its
  receive loop in an `orchestrate_tools` span ([source][meai-fi-session]). That shared processor is the .NET
  counterpart of the executor ADR 0035 wants to extract, and the .NET AF realtime ADR draft plans to reuse this whole
  pipeline ([.NET ADR draft][dotnet-adr]).
- **OpenAI Agents SDK.** It has two independent tracing paths: the Realtime API's server-side trace (configured with
  `workflow_name`, `group_id` and `metadata`, and sent in `session.update` once `session.created` arrives) and the
  SDK's own client traces. Nothing links the two as parent and child, and `RealtimeSession` doesn't produce the trace
  hierarchy the `Runner` does ([architecture note][oai-tracing-note], [config][oai-config],
  [sending the config][oai-send-tracing]). Apps observe typed session events (`agent_start`, `agent_end`, `handoff`,
  `tool_start`, `tool_end`, `tool_approval_required`, `audio`, `audio_interrupted`, `guardrail_tripped`, `error`
  and others) ([events][oai-events]). Output guardrails run on the accumulated transcript every `debounce_text_length`
  characters (100 by default) and interrupt the response when one trips ([settings][oai-guardrail-settings],
  [run][oai-guardrails]).
- **LiveKit Agents.** A long-lived `agent_session` span is the root ([source][lk-session-span]). `user_turn` spans are
  pinned to the session root context because "a turn may be created from any task" ([source][lk-user-turn]).
  `agent_turn` spans carry `invoke_agent` semantics, with inference and tool spans underneath
  ([source][lk-agent-turn], [tool span][lk-function-tool]). Realtime generations get `realtime_inference` spans
  ([source][lk-realtime-inference]), and realtime metrics are tagged `gen_ai.operation.name=generate_content`
  ([source][lk-realtime-metrics]). An agent change gets an `update_agent` span with the previous and next agent labels
  ([source][lk-update-agent]). Speaking-state spans and an interruption-source attribute (`audio_activity`,
  `user_turn` or `programmatic`) cover barge-in ([speaking][lk-speaking], [interruption][lk-interruption]). Metrics
  include `lk.agents.turn.e2e_latency` and other turn latencies, `lk.agents.usage.*` counters with audio tokens, and
  `gen_ai.*` metrics ([metrics][lk-metrics], [names][lk-metric-names]). Content attributes carry an `lk.pii.` segment so
  a collector can strip them ([convention][lk-pii]). Apps subscribe to typed session events and override pipeline nodes
  such as `stt_node`, `llm_node`, `tts_node`, `transcription_node`, `realtime_audio_output_node` and
  `on_user_turn_completed` ([events][lk-events], [nodes][lk-nodes]).
- **Pipecat.** Tracing is opt-in (`enable_tracing=False`), and turn tracking is on by default ([worker][pc-worker]).
  `TurnTraceObserver` creates a `conversation` span and a `turn` span per turn, with `turn.number`,
  `turn.was_interrupted`, `turn.duration_seconds` and `turn.user_bot_latency_seconds` ([source][pc-turns]). The OpenAI
  Realtime service emits `llm_setup` (on `session.created`), `llm_request` (on response creation) and `llm_response`
  (on `response.done`) spans under the turn ([decorator][pc-decorator], [service][pc-openai]), with `gen_ai.usage.*`
  attributes including audio tokens ([usage][pc-usage]).

### 5. The accelerator's bar

The accelerator (a private repository; local checkout at `d42fbcf` on `feat/content-filter-handling`; paths below are
relative to `src/ai_contact_centre_solution_accelerator/` unless they start with `docs/`) builds its voice telemetry
itself. Semantic Kernel's realtime clients have no OpenTelemetry instrumentation; the only SK spans in the voice path
are the `execute_tool {plugin-function}` spans SK emits when the realtime client invokes a kernel function
([realtime client][sk-realtime], [function tracer][sk-function-tracer], [kernel function][sk-kernel-function]). The app
turns SK's event stream into typed session events (`core/events.py:8-97`) and fans them out to listeners;
`TelemetryListener` maps them onto a stateful `CallSpanManager` (`core/listeners.py:297-350`,
`observability/call_span_manager.py`), and `routes/call.py` owns the call lifecycle (`routes/call.py:140-232`). The
accelerator's chat path already uses AF's `invoke_agent`, `chat`, `execute_tool` and workflow spans
(`docs/architecture.md:658-697`), so the voice path is the gap.

| Signal | Shape | Driven by | Who should own it |
| --- | --- | --- | --- |
| `ws.session` | FastAPI SERVER span, renamed, with `session.id` (`docs/architecture.md:624-629`) | WebSocket request | App (transport) |
| `chat.completions {deployment}` | CLIENT root span per call with `gen_ai.provider.name=azure.ai.openai`, `gen_ai.operation.name=chat.completions`, model, `server.address` and aggregate usage, plus the Foundry agent's name and version (`observability/call_span_manager.py:40-94`, `157-160`) | Call start and end | The app's root today; AF could supply a conversation span or session events under it |
| `aoai.realtime.session` | CLIENT span for the provider session, with the error recorded (`observability/call_span_manager.py:343-356`) | Provider connect and close | Framework (provider session lifecycle) |
| `agent.session`, the `agent_handoff` event and `cc.handoff_count{from,to}` | INTERNAL span per active agent, plus an event on the root (`observability/call_span_manager.py:243-269`, `observability/metrics.py:30-53`) | Agent switch | Span and event: the framework, if AF owns in-session handoff. Counter: the app |
| `agent.response_turn` | INTERNAL span per response, with per-response usage and `gen_ai.response.finish_reasons=["content_filter"]` when blocked (`observability/call_span_manager.py:271-307`) | `response.created` to `response.done` | Framework (roughly `realtime_inference`) |
| `cc.content_filter_count{channel}` | Counter (`observability/metrics.py:73-87`) | A content-filtered `response.done` | App, derivable if AF exposes the finish reason |
| `execute_tool {name}` and `cc.function_call_latency{tool.name,outcome}` | CLIENT span per tool, with arguments and result when content recording is on; tools still open at cleanup get ERROR and `outcome=abandoned`; `handoff_to_*` calls are excluded (`observability/call_span_manager.py:313-337`, `397-414`; `core/listeners.py:326-345`; `observability/metrics.py:90-105`) | Function call and result | Framework span and duration (AF runs the tools, but needs cancellation semantics). The outcome label: the app, or AF's `error.type` |
| `gen_ai.user.message` and `gen_ai.assistant.message` events | On the root, with `message.turn`, `message.agent`, and content when enabled (`observability/call_span_manager.py:174-195`, `observability/span_helpers.py:93-116`) | Transcripts | Framework, with content opt-in (for example as `transcription` parts). Turn numbering is an open question |
| `gen_ai.content.prompt` and `gen_ai.content.completion` events | The system prompt, and the last 20 messages of the transcript, for the Foundry tracing view (`observability/call_span_manager.py:197-241`) | Call start and end | App (portal-specific) |
| `realtime.error` event, ERROR status and `cc.error_count{component,error_type}` | (`observability/call_span_manager.py:162-172`, `385-395`; `observability/metrics.py:56-71`) | Provider error or exception | Framework records errors on spans and events. Counter: the app |
| `ws.disconnect`, `cc.active_connections` and `cc.call_duration{outcome}` | (`observability/call_span_manager.py:358-381`; `observability/metrics.py:7-44`) | WebSocket lifecycle | App (transport) |
| W3C trace context on the Foundry voice-agent handshake | `traceparent` and `tracestate` headers, so Foundry's server-side spans join the call's trace (`core/foundry_voice_client.py:55-80`, `docs/architecture.md:550-554`) | Provider connect | Framework (the realtime client's connect) |
| Call recording | Stereo WAV built from audio events (`docs/architecture.md:601-615`) | Audio in and out | App |

The accelerator's span ADR (`docs/adrs/2026-02-11-Define-Span-Hierarchy-And-Attribute-Conventions.md`) adds three
constraints. It wants one trace per call, so traces render as trees in Azure AI Foundry tracing and Application
Insights (lines 14-21 and 40), and it rejected per-turn root spans correlated by `session.id` because joining traces is
awkward, handoff context splits across traces and Foundry's trace view expects a tree (lines 269-275). It prefers the
framework's built-in instrumentation to duplicating it (line 18). And it sets span attributes after creation, because
Azure Monitor's rate-limited sampler drops attributes passed at creation (`docs/architecture.md:699-705`).

## Options

### Telemetry: where realtime spans and events come from

| Option | Shape | For | Against |
| --- | --- | --- | --- |
| **T1. Conversation span with per-response children** | AF opens a span (an `invoke_agent {agent}` or a custom name) in `__aenter__` and ends it in `__aexit__`. Children: per-response spans, `execute_tool`, handoff spans. Aggregate usage goes on the root | One trace per call, which is the accelerator's design driver; mirrors LiveKit and Pipecat; a natural parent for tools and handoffs | Exporters receive a span only once it ends ([OpenTelemetry SDK spec][otel-spec-processor]), so the root appears only when the call ends and is lost if the process dies; a minutes-long `invoke_agent` stretches that convention; the realtime proposal avoids a session span and the question is open upstream; an app root would sit above it, duplicating aggregate attributes |
| **T2. The semconv proposal's shape** | Session start and end events; one `realtime_inference` CLIENT span per generation; opt-in `user_speech`; `execute_tool` from the shared executor; a transfer span for handoffs; correlation by `gen_ai.realtime_session.id` | Tracks the emerging standard; short spans export promptly and survive crashes; vendor-neutral | Built on an unmerged proposal with Development status; no single trace per call unless the spans are parented under a root (T3), and the accelerator rejected traces joined on a session id; turn modelling, metrics and tool events during a generation are still undefined |
| **T3. Hybrid: T2 parented under the caller's context** | AF captures the current context in `start_conversation()` and explicitly parents every span it emits under it, including spans from the receive task. The app keeps its own call root (the accelerator's `chat.completions {deployment}`) | The app keeps the call root and its business attributes; AF supplies standard children; no long-lived AF span | Two owners in one trace; AF must parent spans explicitly instead of relying on per-pull activation; still built on an unmerged proposal |
| **T4. Instrument the realtime client, not the agent** | A telemetry layer on the realtime client and session (like `ChatTelemetryLayer`), with per-response spans in the style of Microsoft.Extensions.AI | Concept parity with .NET; works for any agent; provider details live there | No agent context (agent name, handoffs); tools run outside the client; Microsoft.Extensions.AI's session-cumulative measurements show the pitfalls |

Whichever topology wins, the contract also has to settle:

- **Metrics.** Reuse `gen_ai.client.operation.duration` and `gen_ai.client.token.usage` per response (as
  Microsoft.Extensions.AI does, although semantic-conventions-genai main replaces the token histogram), add
  `agent_framework.realtime.*` instruments (which survive `create_metric_views()`), or record none until
  [Define duration and token usage metrics for realtime inference][genai-issue-569] lands.
- **Audio content.** Never serialise audio blobs, even with sensitive data on, and record `transcription` parts and
  byte or duration counts instead; or follow the single content opt-in and rely on attribute limits. AF's current
  `data`-to-`blob` path does the second by accident.
- **Cancellation.** Record cancelled tool calls and interrupted responses with an `error.type` or a finish reason, so
  barge-in and hang-up are visible.
- **Trace-context propagation.** Realtime clients could inject `traceparent` and `tracestate` on the WebSocket
  handshake, as the accelerator does for Foundry voice agents.
- **Semconv target.** Implement against the v1.41-era names behind AF's existing opt-in, or against
  semantic-conventions-genai main behind a new opt-in token.

### Middleware: how realtime traffic enters it

| Option | Shape | For | Against |
| --- | --- | --- | --- |
| **M1. Function middleware only** (ADR 0035 today, which [defers dedicated realtime middleware][adr35-deferred]) | Tool calls go through the shared executor; conversations get no agent or chat middleware; chat middleware on a realtime agent is rejected through `supported_categories` | Smallest change; function semantics map one-to-one; `execute_tool` spans and the tool metric come for free; bundles fail loudly instead of half-installing | Leaves the review thread's sends, updates and close unanswered; hosts get no policy hook on inbound audio or text or on outbound updates; agent-hooks can't be installed on a realtime agent |
| **M2. Agent middleware around `start_conversation()`** | The agent chain runs once when the conversation opens, with the conversation as `context.result`. Middleware registers per-send transforms, per-update transforms and gates, and close hooks on the context, the way today's `stream_*` hooks work | Reuses the existing type and registration; one policy path for `run()` and conversations; each per-event hook is one call, with no new chain per event | `AgentContext` assumes `messages` and a terminal result; buffering gates can't apply; existing agent middleware (loops, retries, approvals) would misbehave unless conversations are opt-in, for example via a marker or a `context.kind` |
| **M3. A dedicated conversation middleware category** | For example a `ConversationMiddleware` with a `ConversationContext`, where every inbound send and outbound update passes through a chain, and termination means "drop the event" or "interrupt the response" | Explicit duplex semantics; chat-era middleware can't be misapplied; supports guardrails that interrupt when they trip, as the OpenAI Agents SDK does | A fourth category and a bigger API; overhead on every audio chunk unless filtered by content type; its order relative to agent and function middleware must be specified |
| **M4. Observers only** | Read-only typed callbacks for conversation events (a LiveKit-style emitter, like the accelerator's listeners) | Simplest; matches the accelerator's existing listener fan-out; no ordering issues on the hot path | Can't transform or block anything; duplicates what `async for update in conversation` already gives the app |

## Implications for the map

- **[Middleware and telemetry contract for realtime agents][fork-9] is unblocked.** It has to decide: the span topology
  (T1 to T4); the middleware entry point (M1 to M4), including what `MiddlewareTermination` and `MiddlewareFailure` mean
  in a conversation; what happens to chat middleware and bundles on a realtime agent; the semconv target and opt-in
  flag; the audio-content policy; cancellation semantics for tools and interrupted responses; metric names; whether
  realtime clients propagate trace context; and how the accelerator's signals map onto AF's (the table in
  [the accelerator's bar](#5-the-accelerators-bar)).
- **Handoff telemetry follows the handoff decision.** If AF owns in-session handoffs
  ([Handoff model for realtime agents][fork-7]), AF should emit them, as a `pass_control` transfer span or an
  agent-switch span or event; otherwise the app keeps `agent.session` and `agent_handoff`. That makes the handoff
  ticket an input to the contract ticket.
- **Mapping providers onto spans needs provider facts.** Usage (including audio tokens), `response.done` status
  reasons (content filter, cancelled, turn detected), session ids and error events differ between OpenAI Realtime,
  Azure OpenAI Realtime and Voice Live.
  [Azure realtime protocols: Azure OpenAI, Voice Live and Foundry voice agents][fork-4] covers part of this.
- **The prototype can test two risks.** [Prototype the accelerator's voice loop on the extended API][fork-10] can check
  span parenting from the receive task and the cost of per-update hooks on audio chunks.
- **Stays in the app** (out of scope for AF): WebSocket and transport spans, `cc.active_connections`,
  `cc.call_duration`, the names and labels of the `cc.*` business counters, the Foundry-portal prompt and completion
  events, sampler workarounds, and call recording.
- **Pre-existing AF issues found along the way** (not realtime-specific, worth raising separately): cancelled tool
  calls look like successes in telemetry; `AgentTelemetryLayer` creates histograms it never records; `data` content is
  serialised inline as base64 into message attributes; and AF's metric and attribute names will need migrating when
  semantic-conventions-genai releases (the token histogram is replaced and `cache_creation` is renamed).
- **Side note for [Write the extended ADR and open the upstream PR][fork-11]:** ADR number 0035 is already used
  upstream, by `0035-dotnet-agent-hooks-enforcement.md` and `0035-foundry-hosting-resilient-long-running-agents.md`
  ([decisions folder][af-decisions]), so the successor ADR needs a new number.

## Open questions

1. Will the realtime proposal merge as it stands, and when will semantic-conventions-genai make its first release? The
   choice between T1 and T2 or T3 partly depends on that, and on
   [Define session and turn modeling for realtime GenAI APIs][genai-issue-567].
2. Should half-duplex `run()` (over a fresh realtime session) emit `invoke_agent` with a `realtime_inference` child, so
   bounded and live use look alike?
3. Should AF model a turn (user speech followed by one or more responses) at all? LiveKit, Pipecat and the accelerator
   (`message.turn`) do; the proposal doesn't.
4. What do Azure OpenAI Realtime and Voice Live report for usage, session ids and content-filter status, and are their
   events enough to bound `realtime_inference` and `user_speech` reliably?
5. How does a `function_approval_request` pause a live response, and is that middleware's job or the conversation's?
   This overlaps the map's "Tool execution in realtime" fog.
6. If chat-seam enforcement (agent-hooks' `pre_model_call` and `post_model_call`) matters to realtime users, what's the
   realtime equivalent: per `response.create`, per transcript chunk, or nothing?

## Appendix: experiment

Run with `agent-framework-core==1.24.0` and `opentelemetry-sdk` in a scratch virtual environment.

```python
import asyncio

from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

span_exporter = InMemorySpanExporter()
tracer_provider = TracerProvider()
tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))
trace.set_tracer_provider(tracer_provider)
metric_reader = InMemoryMetricReader()
metrics.set_meter_provider(MeterProvider(metric_readers=[metric_reader]))

from agent_framework import tool


@tool(approval_mode="never_require")
async def slow_lookup(order_id: str) -> str:
    """Look up an order slowly."""
    await asyncio.sleep(5)
    return f"order {order_id} found"


@tool(approval_mode="never_require")
async def failing_lookup(order_id: str) -> str:
    """Look up an order and fail."""
    raise RuntimeError("backend down")


async def main() -> None:
    cancelled_call = asyncio.create_task(slow_lookup.invoke(arguments={"order_id": "42"}, tool_call_id="call_cancelled"))
    await asyncio.sleep(0.2)
    cancelled_call.cancel()
    try:
        await cancelled_call
    except asyncio.CancelledError:
        pass
    try:
        await failing_lookup.invoke(arguments={"order_id": "43"}, tool_call_id="call_failed")
    except RuntimeError:
        pass
    for finished_span in span_exporter.get_finished_spans():
        print(finished_span.name, finished_span.status.status_code.name, dict(finished_span.attributes))
    for resource_metrics in metric_reader.get_metrics_data().resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                for data_point in metric.data.data_points:
                    print(metric.name, dict(data_point.attributes))


asyncio.run(main())
```

Output (abridged; `gen_ai.tool.description` and some digits omitted):

```text
execute_tool slow_lookup UNSET {gen_ai.operation.name: execute_tool, gen_ai.tool.name: 'slow_lookup', gen_ai.tool.call.id: 'call_cancelled', gen_ai.tool.type: 'function', agent_framework.function.invocation.duration: 0.2001...}
execute_tool failing_lookup ERROR {gen_ai.operation.name: execute_tool, gen_ai.tool.name: 'failing_lookup', gen_ai.tool.call.id: 'call_failed', gen_ai.tool.type: 'function', error.type: 'RuntimeError', ...}
agent_framework.function.invocation.duration {gen_ai.operation.name: execute_tool, gen_ai.tool.name: 'slow_lookup', gen_ai.tool.type: 'function', agent_framework.function.name: 'slow_lookup'}
agent_framework.function.invocation.duration {gen_ai.operation.name: execute_tool, gen_ai.tool.name: 'failing_lookup', gen_ai.tool.type: 'function', agent_framework.function.name: 'failing_lookup', error.type: 'RuntimeError'}
```

## Sources

| Source | Pinned at |
| --- | --- |
| Agent Framework (`microsoft/agent-framework`) | commit [`f4bdd1d`][af-commit] (main, 2026-11-03); Python core 1.24.0 |
| ADR 0035, Python realtime agents ([Python: Add realtime agent ADR][pr-6831]) | commit [`591b7ea`][adr35] on `eavanvalkenburg/agent-framework`, branch `python-realtime-agent-adr`; review threads [full-duplex traffic][pr-6831-r3953440213] and [audio-only traffic][pr-6831-r3667773087] |
| .NET realtime agent ADR draft | commit [`864fdc3`][dotnet-adr] on `rogerbarreto/agent-framework-public` |
| OpenTelemetry semantic conventions | releases v1.36.0 to v1.45.0; GenAI model at [v1.41.1][semconv-1411-registry] |
| OpenTelemetry GenAI semantic conventions (`open-telemetry/semantic-conventions-genai`) | main at commit [`95aff9c`][genai-readme]; realtime proposal at head [`f80be17`][pr394-span] |
| OpenTelemetry specification | [v1.56.0][otel-spec-processor] |
| Microsoft.Extensions.AI (`dotnet/extensions`) | commit [`47dd770`][meai-remarks] |
| OpenAI Agents SDK (`openai/openai-agents-python`) | commit [`e355d69`][oai-tracing-note] (release 0.24.0 is the latest) |
| LiveKit Agents (`livekit/agents`) | commit [`8f62cab`][lk-session-span] (release 1.8.10 is the latest) |
| Pipecat (`pipecat-ai/pipecat`) | commit [`e03fa8d`][pc-worker] (release 1.15.0 is the latest) |
| Semantic Kernel Python (`microsoft/semantic-kernel`) | tag [`python-1.45.0`][sk-realtime] |
| AI Contact Centre accelerator (private) | local commit `d42fbcf`, branch `feat/content-filter-handling` |

[af-commit]: https://github.com/microsoft/agent-framework/tree/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f
[af-mw-agentcontext]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L227-L366
[af-mw-funccontext]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L434-L631
[af-mw-chatcontext]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L634-L851
[af-mw-types]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L854-L1081
[af-mw-failure]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L82-L147
[af-mw-bundle]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L1098-L1169
[af-mw-gate-warning]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L1367-L1384
[af-mw-agentpipeline]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L1420-L1503
[af-mw-funcpipeline]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L1552-L1589
[af-mw-chatpipeline]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L1625-L1720
[af-mw-chat-stream-check]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L1650-L1651
[af-mw-forward]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L2008-L2100
[af-mw-agent-stream]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L2104-L2119
[af-mw-categorize]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L2251-L2341
[af-agent-class]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_agents.py#L2032-L2037
[af-openai-client]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/openai/agent_framework_openai/_chat_client.py#L4687-L4693
[af-openai-audio-usage]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/openai/agent_framework_openai/_chat_completion_client.py#L997-L1006
[af-tools-invoke-span]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_tools.py#L1254-L1356
[af-tools-autoinvoke]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_tools.py#L2375-L2445
[af-tools-executor]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_tools.py#L2875-L2902
[af-tools-binding]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_tools.py#L6460-L6469
[af-hooks]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_agent_hooks.py#L5-L80
[af-loop]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_harness/_loop.py#L548-L556
[af-rs-aexit]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_types.py#L4541-L4552
[af-rs-close]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_types.py#L4891-L4921
[af-obs-otelattr]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L224-L367
[af-obs-usage-map]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L382-L404
[af-obs-views]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L956-L971
[af-obs-semconv]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L1179-L1194
[af-obs-sensitive]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L1204-L1220
[af-obs-configure]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L1605-L1660
[af-obs-histograms]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L1883-L1898
[af-obs-chatlayer]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L1909-L2264
[af-obs-chat-stream]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L2032-L2188
[af-obs-agentlayer]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L2339-L2700
[af-obs-agent-init]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L2342-L2355
[af-obs-agent-stream]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L2401-L2536
[af-obs-agent-pull]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L2507-L2536
[af-obs-agent-capture]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L2473-L2483
[af-obs-agent-capture-2]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L2565-L2581
[af-obs-funcspan]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L2703-L2745
[af-obs-activate]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L2814-L2888
[af-obs-usage-accum]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L3515-L3541
[af-obs-part]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L3472-L3512
[af-obs-capture-response]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L3626-L3643
[af-obs-logger]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L119
[af-obs-events]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L3360-L3372
[af-core-pyproject]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/pyproject.toml#L30
[af-decisions]: https://github.com/microsoft/agent-framework/tree/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/docs/decisions
[adr-0003]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/docs/decisions/0003-agent-opentelemetry-instrumentation.md
[adr-0007]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/docs/decisions/0007-agent-filtering-middleware.md
[adr-0013]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/docs/decisions/0013-python-get-response-simplification.md#L146-L151
[adr-0016]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/docs/decisions/0016-python-context-middleware.md#L1037-L1049
[af-pr-8773]: https://github.com/microsoft/agent-framework/pull/8773
[af-pr-9476]: https://github.com/microsoft/agent-framework/pull/9476
[pr-6831]: https://github.com/microsoft/agent-framework/pull/6831
[pr-6831-r3953440213]: https://github.com/microsoft/agent-framework/pull/6831#discussion_r3953440213
[pr-6831-r3667773087]: https://github.com/microsoft/agent-framework/pull/6831#discussion_r3667773087
[adr35]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md
[adr35-conv]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L178-L351
[adr35-fi]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L522-L543
[adr35-deferred]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L570-L577
[dotnet-adr]: https://github.com/rogerbarreto/agent-framework-public/blob/864fdc3b714c0df329cad6366bb9b233755928c3/docs/decisions/0030-realtime-ai-agent.md#L113-L114
[semconv-releases]: https://github.com/open-telemetry/semantic-conventions/releases
[semconv-moved]: https://github.com/open-telemetry/semantic-conventions/blob/v1.42.0/docs/gen-ai/README.md
[semconv-1411-registry]: https://github.com/open-telemetry/semantic-conventions/blob/v1.41.1/model/gen-ai/registry.yaml#L457
[semconv-1410-registry]: https://github.com/open-telemetry/semantic-conventions/blob/v1.41.0/model/gen-ai/registry.yaml
[genai-repo]: https://github.com/open-telemetry/semantic-conventions-genai
[genai-readme]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/README.md
[genai-audio-usage]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/model/gen-ai/registry.yaml#L333-L380
[genai-transfer-attrs]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/model/gen-ai/registry.yaml#L497-L514
[genai-transfer-span]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/model/gen-ai/spans.yaml#L1025-L1055
[genai-cl-374b]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/changelog.d/374.breaking.md
[genai-cl-374e]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/changelog.d/374.enhancement.md
[genai-cl-440e]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/changelog.d/440.enhancement.md
[genai-cl-440b]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/changelog.d/440.breaking.md
[genai-cl-447]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/changelog.d/447.enhancement.md
[genai-cl-521]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/changelog.d/521.breaking.md
[genai-pr-realtime]: https://github.com/open-telemetry/semantic-conventions-genai/pull/394
[pr394-span]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/docs/gen-ai/gen-ai-realtime.md#L18-L58
[pr394-finish]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/docs/gen-ai/gen-ai-realtime.md#L177-L190
[pr394-messages]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/docs/gen-ai/gen-ai-realtime.md#L241-L251
[pr394-user-speech]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/docs/gen-ai/gen-ai-realtime.md#L376-L405
[pr394-events]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/docs/gen-ai/gen-ai-realtime.md#L540-L655
[pr394-events-model]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/model/gen-ai/events.yaml#L1-L54
[pr394-session-id]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/docs/gen-ai/gen-ai-realtime.md#L152-L168
[pr394-transcription]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/model/gen-ai/gen-ai-input-messages.json#L505-L530
[pr394-openai-scenario]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/reference/scenarios/openai-realtime/README.md
[genai-issue-515]: https://github.com/open-telemetry/semantic-conventions-genai/issues/515
[genai-issue-567]: https://github.com/open-telemetry/semantic-conventions-genai/issues/567
[genai-issue-568]: https://github.com/open-telemetry/semantic-conventions-genai/issues/568
[genai-issue-569]: https://github.com/open-telemetry/semantic-conventions-genai/issues/569
[genai-issue-633]: https://github.com/open-telemetry/semantic-conventions-genai/issues/633
[contrib-issue-4470]: https://github.com/open-telemetry/opentelemetry-python-contrib/issues/4470
[otel-spec-processor]: https://github.com/open-telemetry/opentelemetry-specification/blob/v1.56.0/specification/trace/sdk.md#L1060-L1068
[meai-pr-7285]: https://github.com/dotnet/extensions/pull/7285
[meai-remarks]: https://github.com/dotnet/extensions/blob/47dd770c802d4db8d106d1b81e4e52edd26cbc18/src/Libraries/Microsoft.Extensions.AI/Realtime/OpenTelemetryRealtimeClientSession.cs#L24-L81
[meai-send]: https://github.com/dotnet/extensions/blob/47dd770c802d4db8d106d1b81e4e52edd26cbc18/src/Libraries/Microsoft.Extensions.AI/Realtime/OpenTelemetryRealtimeClientSession.cs#L175-L193
[meai-receive]: https://github.com/dotnet/extensions/blob/47dd770c802d4db8d106d1b81e4e52edd26cbc18/src/Libraries/Microsoft.Extensions.AI/Realtime/OpenTelemetryRealtimeClientSession.cs#L195-L300
[meai-response-span]: https://github.com/dotnet/extensions/blob/47dd770c802d4db8d106d1b81e4e52edd26cbc18/src/Libraries/Microsoft.Extensions.AI/Realtime/OpenTelemetryRealtimeClientSession.cs#L274-L284
[meai-activity]: https://github.com/dotnet/extensions/blob/47dd770c802d4db8d106d1b81e4e52edd26cbc18/src/Libraries/Microsoft.Extensions.AI/Realtime/OpenTelemetryRealtimeClientSession.cs#L661-L737
[meai-consts]: https://github.com/dotnet/extensions/blob/47dd770c802d4db8d106d1b81e4e52edd26cbc18/src/Libraries/Microsoft.Extensions.AI/OpenTelemetryConsts.cs#L181-L217
[meai-fi-session]: https://github.com/dotnet/extensions/blob/47dd770c802d4db8d106d1b81e4e52edd26cbc18/src/Libraries/Microsoft.Extensions.AI/Realtime/FunctionInvokingRealtimeClientSession.cs#L146-L152
[meai-fi-processor]: https://github.com/dotnet/extensions/blob/47dd770c802d4db8d106d1b81e4e52edd26cbc18/src/Libraries/Microsoft.Extensions.AI/Common/FunctionInvocationProcessor.cs#L160-L175
[oai-tracing-note]: https://github.com/openai/openai-agents-python/blob/e355d69152d4f4730dfb03b63dbce53e8d9f5d8c/.agents/references/realtime-tracing.md
[oai-config]: https://github.com/openai/openai-agents-python/blob/e355d69152d4f4730dfb03b63dbce53e8d9f5d8c/src/agents/realtime/config.py#L269-L295
[oai-guardrail-settings]: https://github.com/openai/openai-agents-python/blob/e355d69152d4f4730dfb03b63dbce53e8d9f5d8c/src/agents/realtime/config.py#L248-L256
[oai-send-tracing]: https://github.com/openai/openai-agents-python/blob/e355d69152d4f4730dfb03b63dbce53e8d9f5d8c/src/agents/realtime/openai_realtime.py#L704-L716
[oai-events]: https://github.com/openai/openai-agents-python/blob/e355d69152d4f4730dfb03b63dbce53e8d9f5d8c/src/agents/realtime/events.py#L15-L253
[oai-guardrails]: https://github.com/openai/openai-agents-python/blob/e355d69152d4f4730dfb03b63dbce53e8d9f5d8c/src/agents/realtime/session.py#L1682-L1780
[lk-session-span]: https://github.com/livekit/agents/blob/8f62cabfa2230421a3ff08058364c389338c17d8/livekit-agents/livekit/agents/voice/agent_session.py#L996-L1000
[lk-user-turn]: https://github.com/livekit/agents/blob/8f62cabfa2230421a3ff08058364c389338c17d8/livekit-agents/livekit/agents/voice/audio_recognition.py#L2100-L2104
[lk-agent-turn]: https://github.com/livekit/agents/blob/8f62cabfa2230421a3ff08058364c389338c17d8/livekit-agents/livekit/agents/voice/agent_activity.py#L280-L292
[lk-realtime-inference]: https://github.com/livekit/agents/blob/8f62cabfa2230421a3ff08058364c389338c17d8/livekit-agents/livekit/agents/voice/agent_activity.py#L4355-L4360
[lk-realtime-metrics]: https://github.com/livekit/agents/blob/8f62cabfa2230421a3ff08058364c389338c17d8/livekit-agents/livekit/agents/telemetry/utils.py#L114-L128
[lk-update-agent]: https://github.com/livekit/agents/blob/8f62cabfa2230421a3ff08058364c389338c17d8/livekit-agents/livekit/agents/voice/agent_session.py#L1917-L1925
[lk-speaking]: https://github.com/livekit/agents/blob/8f62cabfa2230421a3ff08058364c389338c17d8/livekit-agents/livekit/agents/voice/agent_session.py#L2218-L2284
[lk-interruption]: https://github.com/livekit/agents/blob/8f62cabfa2230421a3ff08058364c389338c17d8/livekit-agents/livekit/agents/telemetry/trace_types.py#L438-L441
[lk-metrics]: https://github.com/livekit/agents/blob/8f62cabfa2230421a3ff08058364c389338c17d8/livekit-agents/livekit/agents/telemetry/otel_metrics.py#L23-L90
[lk-metric-names]: https://github.com/livekit/agents/blob/8f62cabfa2230421a3ff08058364c389338c17d8/livekit-agents/livekit/agents/telemetry/trace_types.py#L417-L421
[lk-pii]: https://github.com/livekit/agents/blob/8f62cabfa2230421a3ff08058364c389338c17d8/livekit-agents/livekit/agents/telemetry/trace_types.py#L1-L8
[lk-events]: https://github.com/livekit/agents/blob/8f62cabfa2230421a3ff08058364c389338c17d8/livekit-agents/livekit/agents/voice/events.py#L309-L325
[lk-nodes]: https://github.com/livekit/agents/blob/8f62cabfa2230421a3ff08058364c389338c17d8/livekit-agents/livekit/agents/voice/agent.py#L336-L512
[lk-function-tool]: https://github.com/livekit/agents/blob/8f62cabfa2230421a3ff08058364c389338c17d8/livekit-agents/livekit/agents/voice/generation.py#L1010
[pc-worker]: https://github.com/pipecat-ai/pipecat/blob/e03fa8dba962ade3cee2095763ad1ac87c2c3bd6/src/pipecat/pipeline/worker.py#L300-L302
[pc-turns]: https://github.com/pipecat-ai/pipecat/blob/e03fa8dba962ade3cee2095763ad1ac87c2c3bd6/src/pipecat/utils/tracing/turn_trace_observer.py#L120-L226
[pc-decorator]: https://github.com/pipecat-ai/pipecat/blob/e03fa8dba962ade3cee2095763ad1ac87c2c3bd6/src/pipecat/utils/tracing/service_decorators.py#L1413-L1440
[pc-openai]: https://github.com/pipecat-ai/pipecat/blob/e03fa8dba962ade3cee2095763ad1ac87c2c3bd6/src/pipecat/services/openai/realtime/llm.py#L965-L966
[pc-usage]: https://github.com/pipecat-ai/pipecat/blob/e03fa8dba962ade3cee2095763ad1ac87c2c3bd6/src/pipecat/utils/tracing/service_decorators.py#L170-L244
[sk-realtime]: https://github.com/microsoft/semantic-kernel/blob/python-1.45.0/python/semantic_kernel/connectors/ai/open_ai/services/_open_ai_realtime.py#L506
[sk-function-tracer]: https://github.com/microsoft/semantic-kernel/blob/python-1.45.0/python/semantic_kernel/utils/telemetry/model_diagnostics/function_tracer.py#L22-L65
[sk-kernel-function]: https://github.com/microsoft/semantic-kernel/blob/python-1.45.0/python/semantic_kernel/functions/kernel_function.py#L264
[fork-4]: https://github.com/adamdougal/agent-framework/issues/4
[fork-7]: https://github.com/adamdougal/agent-framework/issues/7
[fork-9]: https://github.com/adamdougal/agent-framework/issues/9
[fork-10]: https://github.com/adamdougal/agent-framework/issues/10
[fork-11]: https://github.com/adamdougal/agent-framework/issues/11
