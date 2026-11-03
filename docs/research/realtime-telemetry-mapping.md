# Realtime telemetry mapping per provider

Research for [Realtime telemetry mapping per provider][ticket], on the map
[Python realtime agents in Agent Framework][map]. Researched 2026-11-03. Every source is pinned to a tag, commit or
page date (see [Sources](#sources)).

It builds on two earlier tickets and doesn't repeat them:

- [Middleware and telemetry for full-duplex realtime traffic][t5] ([findings][f5]): Agent Framework's telemetry
  layers and the OpenTelemetry realtime proposal.
- [Azure realtime protocols: Azure OpenAI, Voice Live and Foundry voice agents][t4] ([findings][f4]): connection and
  schema differences between the providers.

## Question

How do OpenAI Realtime, Azure OpenAI Realtime and Voice Live report session ids, usage (including audio and cached
tokens), response status and finish reasons (content filter, cancelled, turn detected) and errors, and how does each
map onto the OpenTelemetry GenAI attributes Agent Framework emits?

## Answer

- **One mapper covers all three providers.** Every signal sits on events whose names and shapes the three share:
  - the session id on `session.created`;
  - the response id, status, status reason, usage and failure error on the `response` object of `response.done`;
  - recoverable errors on an `error` event (`type`, `code`, `message`, `param`, `event_id`).

  Voice Live's preview event names only affect the audio, text and transcript streaming events, which matter for time
  to first chunk and transcripts. Voice Live adds a `warning` event and, from API version `2026-07-15`, output
  `reasoning_tokens`.
- **Status plus reason gives every finish reason the ticket asks about.**
  - `completed` → `stop`, or `tool_call` when the output holds function calls.
  - `incomplete` with `content_filter` → `content_filter`; `incomplete` with `max_output_tokens` → `length`.
  - `cancelled` with `turn_detected` or `client_cancelled` → that reason. The realtime semconv proposal says to report
    the provider's interruption reason.
  - `failed` with `status_details.error` → `error`, with `error.type` set to the provider's error code.
  - The raw status fits `gen_ai.response.status`, which unreleased semantic-conventions-genai defines for
    `fetch_response`.
- **Two traps.**
  - A content-filter block isn't an error. There's no `error` event and no category or severity, and nothing says
    whether the caller's speech or the reply was blocked. The accelerator saw this on Azure OpenAI and Voice Live.
  - A barge-in after the model has finished generating still reports `completed`. Over a WebSocket, generation
    usually outruns playback, so only the client knows the caller cut the audio off. Agent Framework (AF) has to
    record that itself.
- **Usage arrives once per response, split by modality.**
  - All three report input, output and total tokens; input text, audio and cached tokens; and output text and audio
    tokens. OpenAI and Azure OpenAI add image tokens and cached tokens by modality. Voice Live's SDK models those
    too, but Learn doesn't document them.
  - AF maps only the totals, cache reads and reasoning tokens. Audio and text counts have no AF usage key and no
    attribute in the released conventions (v1.41.1). Unreleased semantic-conventions-genai defines
    `gen_ai.usage.audio.input_tokens` and its siblings.
  - Input transcription is billed at the transcription model's rate and reported on a separate event (OpenAI and
    Azure OpenAI only). It has no home in the conventions.
- **Ids and provider names.**
  - `session.id` (`sess_…`) → `gen_ai.realtime_session.id`, from the proposal. The proposal forbids using
    `gen_ai.conversation.id` for it.
  - `gen_ai.conversation.id` should stay AF's agent-session id. OpenAI's `response.conversation_id` (`conv_…`) lives
    only as long as the session; a Foundry voice agent's `conversation_id` is stored and can be read back.
  - `openai` and `azure.ai.openai` are well-known provider names. Voice Live has none: AF's Foundry clients say
    `azure.ai.foundry`, and the Voice Live SDK's instrumentor says `microsoft.foundry`.
- **Existing mappings disagree, so AF has to document its own.** The Voice Live SDK's instrumentor and .NET
  Microsoft.Extensions.AI put the status in `gen_ai.response.finish_reasons`, so a filtered reply reads
  `["incomplete"]`. The accelerator records only `content_filter` and token totals, and marks its call span ERROR on
  every `error` event, including benign races that Pipecat ignores.

The full table is in [Mapping table](#mapping-table).

## Findings

### 1. Where the signals live

All three providers report telemetry on the same few server events:

| Signal | Event | Fields |
| --- | --- | --- |
| Session id, model and expiry | `session.created` (first event on connect), `session.updated` | `session.id`, `session.model`, `session.expires_at` |
| Generation start | `response.created` | `response.id`; `response.status` is `in_progress` |
| Generation end | `response.done` | `response.id`, `.status`, `.status_details`, `.usage`, `.conversation_id`, `.output`, `.metadata` |
| Errors (most are recoverable) | `error` | `error.type`, `.code`, `.message`, `.param`, `.event_id` |
| Transcription usage and failures | `conversation.item.input_audio_transcription.completed` and `.failed` | `usage`; `error` and `item_id` |
| Turn detection | `input_audio_buffer.speech_started` and `.speech_stopped` | `audio_start_ms`, `audio_end_ms`, `item_id` |
| Rate limits | `rate_limits.updated`, at the start of each response | `rate_limits[]` |

Sources: the OpenAI server-event reference ([OpenAI events][oai-events]), Azure's classic GA reference
([Azure OpenAI events][aoai-ref-classic]) and the Voice Live `2026-07-15` reference ([Voice Live reference][vl-ref]).
OpenAI says `response.done` is "always emitted, no matter the final state" ([`response.done`][oai-response-done]), and
Azure's classic reference says the same ([Azure `response.done`][aoai-ref-classic-done]). Voice Live's reference
doesn't say, but the accelerator receives it for content-filtered responses (`docs/architecture.md:561-563`).

**How usage relates to billing.**

- Realtime API costs accrue per response, from its input and output tokens, except input transcription
  ([OpenAI costs][oai-cost]).
- Each response's input includes the conversation so far: "output from previous turns … will become the input for
  later turns" ([SDK usage type][oai-sdk-usage]). Summing input tokens across responses therefore gives billed tokens,
  not the size of the conversation.
- OpenAI counts 1 token per 100 ms of user audio and 1 per 50 ms of assistant audio ([OpenAI per-response
  costs][oai-cost-per-response]). Voice Live estimates about 10 tokens a second in and 20 out for Azure OpenAI models,
  and 12.5 in for Phi models ([Voice Live tokens][vl-overview-tokens]).

### 2. OpenAI Realtime

**Session id and model.**

- `session.created` is the first server event. The reference types its `session` as `RealtimeSessionCreateResponse`,
  whose `id` "looks like `sess_1234567890abcdef`", alongside `expires_at` and `model`. The example's model is a dated
  snapshot, `gpt-realtime-2025-08-28` ([`session.created`][oai-session-created]).
- The `openai` SDK types the same field as `RealtimeSessionCreateRequest | RealtimeTranscriptionSessionCreateRequest`,
  neither of which declares `id` ([SDK event][oai-sdk-session-created]). Only the REST `RealtimeSessionCreateResponse`
  declares `id`, `object` and `expires_at` ([SDK REST type][oai-sdk-session-response]). That holds in v2.25.0 (AF's
  floor) and v3.42.1 (the latest).
- The SDK's models allow extra fields ([`_models.py`][oai-sdk-models-extra]) and its WebSocket parser doesn't validate
  ([`parse_event`][oai-sdk-parse]), so `getattr(event.session, "id")` returns the id
  ([experiment](#appendix-sdk-parsing-experiment)). The accelerator reads it that way (`core/orchestration.py:532-535`).
- A session lasts at most 60 minutes ([session lifecycle][oai-conversations-lifecycle]).

**Conversation id and metadata.**

- `conversation.created` reports a `conv_…` id right after the session is created
  ([`conversation.created`][oai-conversation-created]).
- Every response carries `conversation_id`, which is null for responses created with `conversation: "none"`
  ([SDK response][oai-sdk-response], [out-of-band responses][oai-out-of-band]).
- `response.metadata` echoes the metadata the client set on `response.create` ([SDK response][oai-sdk-response]).

**Status and finish reasons.** `response.status` is `completed`, `cancelled`, `failed`, `incomplete` or `in_progress`.
`status_details` holds ([SDK status][oai-sdk-status], [`response.done`][oai-response-done]):

- `type`, which repeats the status;
- `reason`, for a response that didn't complete:
  - `cancelled`: `turn_detected` ("the server VAD detected a new start of speech") or `client_cancelled`;
  - `incomplete`: `max_output_tokens` or `content_filter` ("the server-side safety filter activated and cut off the
    response");
- `error` (`type`, `code`) when the status is `failed`. v3.42.1 declares `message` too; v2.25.0 doesn't
  ([v2.25.0 status][oai-sdk-status-225]), though it still exposes it as an extra field
  ([experiment](#appendix-sdk-parsing-experiment)).

**Interruption.**

- With voice activity detection on, the server cancels an in-progress response when the caller speaks, and sends
  `response.done` with `status: cancelled`.
- Over a WebSocket the client "manages audio playback", so it must stop playback and send `conversation.item.truncate`
  itself. Over WebRTC and SIP the server truncates ([interruption and truncation][oai-interruption]).
- A response that has already finished generating has nothing left to cancel. Its `completed` status says nothing
  about whether the caller heard all of it.

**Usage** is on `response.done` → `response.usage` ([SDK usage][oai-sdk-usage], [input details][oai-sdk-input-details],
[output details][oai-sdk-output-details], [`response.done`][oai-response-done]):

- `input_tokens` ("including text and audio tokens"), `output_tokens` and `total_tokens`;
- `input_token_details`: `text_tokens`, `audio_tokens`, `image_tokens`, `cached_tokens` (a subset of the input
  tokens) and `cached_tokens_details` (`text_tokens`, `audio_tokens`, `image_tokens`);
- `output_token_details`: `text_tokens` and `audio_tokens`. There's no reasoning-token field, although reasoning-capable
  models such as `gpt-realtime-2` take a `reasoning` session setting ([`session.created`][oai-session-created]).

`response.created` carries `usage: null` ([`response.created`][oai-response-created]).

**Transcription usage.** `conversation.item.input_audio_transcription.completed` carries `usage`, "billed according to
the ASR model's pricing rather than the realtime model's pricing" ([SDK event][oai-sdk-transcription-completed]). It
takes one of two shapes:

- `type: "tokens"`: input, output and total tokens, with input text and audio tokens;
- `type: "duration"`: `seconds`. `gpt-transcribe` bills this way ([transcription costs][oai-cost-transcription]).

**Errors.**

- The `error` event carries `type` (for example `invalid_request_error` or `server_error`), an optional `code`,
  `message`, `param`, and the `event_id` of the client event that caused it. "Most errors are recoverable and the
  session will stay open" ([`error`][oai-error], [SDK error][oai-sdk-error]).
- The reference has no catalogue of error codes. The guide's example is `invalid_request_error` with `invalid_value`
  ([error handling][oai-error-handling]).
- Transcription failures arrive on `conversation.item.input_audio_transcription.failed`, with the same error fields and
  the `item_id` ([SDK event][oai-sdk-transcription-failed]).
- `rate_limits.updated` reports `requests` and `tokens` limits at the start of each response
  ([`rate_limits.updated`][oai-rate-limits]).

### 3. Azure OpenAI Realtime

- **Same shapes as OpenAI.** Learn's current reference says Azure OpenAI Realtime "follows the OpenAI Realtime API
  specification" and lists one deviation: the transcription `model` is a deployment name ([reference][aoai-ref]).
  Everything in section 2 applies.
- **The classic GA reference is incomplete.**
  - Its `response.done` example matches OpenAI's field for field, including `conversation_id`,
    `cached_tokens_details` and `image_tokens` ([Azure `response.done`][aoai-ref-classic-done]).
  - It lists 38 server events and leaves out `error`, `session.created` and `session.updated`
    ([server events][aoai-ref-classic]). The service sends them: the accelerator handles `error` and
    `session.created` (`core/orchestration.py:532-545`).
  - It documents a transcription error object (`transcription_error`, `audio_unintelligible`) and transcription usage
    of type `tokens` only ([transcription objects][aoai-ref-classic-transcription]).
- **Content filtering.**
  - Azure's default guardrail for audio models blocks hate, violence, sexual and self-harm content at medium severity
    in prompts and completions, jailbreaks in prompts, and protected text and code in completions
    ([default policies][default-safety]).
  - On the wire it shows up only as `response.done` with `status: incomplete` and
    `status_details.reason: content_filter`, "after part of the reply has played. There's no `error` event, and
    nothing says whether the caller's speech or the reply was blocked". Typed text sent to the session wasn't blocked
    in the accelerator's tests (`docs/architecture.md:561-563`).
  - Chat and Responses calls behave differently. A blocked prompt fails with HTTP 400 `content_filter`, and successful
    responses carry per-category annotations (`prompt_filter_results` and `content_filter_results`, or `content_filters`)
    ([Responses guardrails][aoai-responses-guardrails]). AF turns that 400 into `OpenAIContentFilterException`
    ([AF][af-oai-content-filter]). Realtime has neither, so there's no exception to raise and no category to record.
- **Provider name.** AF already labels Azure-routed OpenAI clients `azure.ai.openai` ([AF][af-oai-provider]), the
  conventions' well-known value ([provider names][semconv-1411-provider]).
- **Model.** What Azure OpenAI puts in `session.model` (the deployment or the model name) isn't documented.

### 4. Azure Voice Live

**Event names.** In `2026-07-15`, every telemetry-bearing event keeps its OpenAI name: `session.created`,
`response.created`, `response.done`, `error`, the transcription events, `input_audio_buffer.speech_started` and
`.speech_stopped`, and `rate_limits.updated` ([reference][vl-ref]). Only the audio, text and transcript streaming
events (`response.audio.*`, `response.text.*` and `response.audio_transcript.*`) use preview names
([Azure protocols findings][f4]).

**Session id and model.** `session.created` carries `session.id` (`sess_…`) and `model`. The example's model is the
requested ID (`gpt-realtime`), not a dated snapshot ([`session.created`][vl-session-created]).
`RealtimeResponseSession` documents `id`, `expires_at` and `model` ([`RealtimeResponseSession`][vl-response-session]).

**Response and status.**

- `RealtimeResponse` documents `id`, `status`, `status_details`, `usage` and an optional `conversation_id`, plus
  `voice`, `modalities`, `output_audio_format`, `temperature` and `max_response_output_tokens`
  ([`RealtimeResponse`][vl-realtimeresponse]).
- Statuses are `in_progress`, `completed`, `cancelled`, `incomplete` ("interrupted") and `failed`
  ([`RealtimeResponseStatus`][vl-realtimeresponsestatus]).
- Learn documents `RealtimeResponseStatusDetails` with a `type` only, in both `2025-10-01` and `2026-07-15`
  ([`2026-07-15`][vl-status-details], [`2025-10-01`][vl-ref-1001-status-details]).
- The generated models of the `azure-ai-voicelive` 1.3.1 SDK describe the full shape, as a union discriminated on
  `type` ([base][vl-sdk-status-details]):
  - `cancelled`, with `reason` `turn_detected`, `client_cancelled` or another string ([SDK][vl-sdk-cancelled]);
  - `incomplete`, with `reason` `max_output_tokens`, `content_filter` or another string ([SDK][vl-sdk-incomplete]);
  - `failed`, with an untyped `error` ([SDK][vl-sdk-failed]).
- The accelerator receives `content_filter` there from Voice Live and Voice Live with bring-your-own-model (BYOM)
  (`docs/architecture.md:561-563`).

**Usage.**

- Learn's `RealtimeUsage` has `total_tokens`, `input_tokens`, `output_tokens`, and input and output `TokenDetails`:
  `cached_tokens`, `text_tokens`, `audio_tokens` and, from `2026-07-15`, `reasoning_tokens` ("Applies to output token
  details only") ([`RealtimeUsage`][vl-realtimeusage], [`TokenDetails`][vl-tokendetails]). `2025-10-01` has no
  `reasoning_tokens` ([`2025-10-01` `TokenDetails`][vl-ref-1001-tokendetails]).
- The SDK declares `image_tokens` and `cached_tokens_details` (text, audio and image) as required on input details,
  and `reasoning_tokens` as optional on output details ([input][vl-sdk-input-details], [cached][vl-sdk-cached-details],
  [output][vl-sdk-output-details], [usage][vl-sdk-token-usage]).
- `response.created`'s example carries zeroed usage where OpenAI sends `null`
  ([`response.created`][vl-response-created]).
- Voice Live bills text, native audio and Azure Speech audio at a pro, standard or lite rate depending on the model,
  plus cached audio and text inputs ([pricing][vl-overview-pricing], [tokens][vl-overview-tokens]). Nothing documents
  how cascaded sessions (a text model with Azure speech-to-text and text-to-speech) or BYOM sessions fill
  `audio_tokens`.
- `conversation.item.input_audio_transcription.completed` documents no `usage`
  ([transcription completed][vl-transcription-completed]).

**Errors and warnings.**

- `error` has the OpenAI fields ([`error`][vl-error], [SDK][vl-sdk-error]), and transcription failures arrive on
  `conversation.item.input_audio_transcription.failed` ([transcription failed][vl-transcription-failed]).
- `warning` (`message`, plus optional `code` and `param`) is Voice Live only. Warnings "are informational and the
  session continues normally" ([`warning`][vl-warning], [SDK][vl-sdk-warning]).
- Content filtering is always on, and only bringing your own model changes it ([FAQ][vl-faq]).

**Parsing with the `openai` SDK.** The accelerator parses Voice Live events with the `openai` SDK's GA types
(`core/voicelive_patches.py:305-360`). That works for these events, including the undeclared `reasoning_tokens`
([experiment](#appendix-sdk-parsing-experiment)). But an event the SDK doesn't know, such as `warning`, comes back as a
`ConversationCreatedEvent`, the first member of the SDK's event union ([SDK union][oai-sdk-union]), with its real
`type` string intact. Code must dispatch on `type`, not on the class.

**Provider name.** No well-known `gen_ai.provider.name` covers Voice Live
([v1.41.1][semconv-1411-provider], [semantic-conventions-genai][genai-provider]). AF's Foundry clients use
`azure.ai.foundry` ([AF][af-foundry-provider]). The SDK's instrumentor uses `microsoft.foundry`, with
`gen_ai.system=az.ai.voicelive` ([SDK][vl-sdk-utils]).

### 5. Foundry voice agents (extension point)

- They speak the GA protocol ([Azure protocols findings][f4]), so section 2's shapes apply.
- `session.created` adds a top-level `conversation_id`. Learn says to capture it "for correlation and, when storage is
  enabled, read-back" and "Don't confuse it with a realtime session ID" ([events][fva-migrate-events],
  [monitoring][fva-migrate-monitoring]). Stored conversations expose `usage` after the session ends
  ([pricing][fva-pricing]).
- Foundry traces the voice pipeline on the server: one trace per session, a span per turn with model, tool,
  speech-recognition, speech-synthesis and voice-activity children, per-turn token usage, and
  `microsoft.foundry.voice.estimated_cost.*` estimates ([observability][fva-observability], [pricing][fva-pricing]).
  Client spans join that trace through `traceparent` ([middleware and telemetry findings][f5]).

### 6. What Agent Framework emits today

At commit `f4bdd1d`:

- **Usage.**
  - `UsageDetails` standardises input, output, total, cache-write, cache-read and reasoning counts, and accepts other
    integer keys ([`UsageDetails`][af-usage-details]).
  - Only mapped keys reach spans: the input and output totals, cache reads and writes, and reasoning
    ([map][af-usage-map]). The cache and reasoning attributes need the latest-experimental conventions
    ([gate][af-latest-gate]). Total tokens are never emitted.
  - The chat-completions client already collects `prompt/audio_tokens` and `completion/audio_tokens`
    ([client][af-cc-usage]), and telemetry drops them.
- **Finish reasons.**
  - `FinishReason` is an open string with four known values: `stop`, `length`, `tool_calls` and `content_filter`
    ([`FinishReason`][af-finish-reason]).
  - Spans get the raw value as a one-item JSON list ([response attributes][af-response-attrs]). Only output messages
    translate `tool_calls` to the conventions' `tool_call` ([map][af-finish-map], [use][af-finish-map-use]), so AF's
    spans say `["tool_calls"]` while its messages say `tool_call`.
  - The Responses client is a ready precedent. It maps `incomplete_details.reason` `content_filter` → `content_filter`
    and `max_output_tokens` → `length`, and a completed response → `tool_calls` if it holds function calls, otherwise
    `stop` ([Responses client][af-oai-finish]). Realtime `status_details.reason` needs the same logic, plus the two
    cancellation reasons.
- **Ids.** `gen_ai.response.id` comes from the response. `gen_ai.conversation.id` comes from an app-supplied telemetry
  id, the agent's `_get_otel_conversation_id`, or the agent session's `service_session_id`
  ([context variable][af-conv-id], [`invoke_agent`][af-invoke-conv-id]).
- **Errors.** `error.type` is the exception's class name ([`capture_exception`][af-capture-exception],
  [attribute map][af-attr-map]), whereas the conventions prefer the provider's error code
  ([v1.41.1][semconv-1411-error-type]). Realtime errors arrive as events, not exceptions.
- **Metrics.** `gen_ai.client.token.usage` and `gen_ai.client.operation.duration` carry the operation, provider,
  request and response model, and server address and port ([recording][af-capture-response],
  [attributes][af-metric-attrs]).

### 7. OpenTelemetry targets

**Released conventions (v1.41.1), AF's "latest":**

- Usage attributes cover input, output, cache read, cache creation and reasoning tokens, with no modality split
  ([usage][semconv-1411-usage]).
- `gen_ai.response.finish_reasons` is a string array ([registry][semconv-1411-finish]). The output-message
  `FinishReason` lists `stop`, `length`, `content_filter`, `tool_call` and `error`, and allows any other string
  ([schema][semconv-1411-finish-schema]).
- `error.type` "SHOULD match the error code returned by the Generative AI provider or the client library, the
  canonical name of exception that occurred, or another low-cardinality error identifier"
  ([spans][semconv-1411-error-type]).
- `gen_ai.output.type` has a `speech` value ([registry][semconv-1411-output-type]).

**Unreleased semantic-conventions-genai `main` (`95aff9c`):**

- Per-modality usage: `gen_ai.usage.{text,image,audio}.input_tokens`, `.output_tokens` and
  `.cache_read.input_tokens`. Each is a subset of the totals; the totals include every modality and cached tokens;
  billed counts win when a provider reports two ([usage][genai-usage]). `gen_ai.token.modality` splits the token
  metrics, recording any unexplained remainder as `unknown` ([modality][genai-modality]).
- `gen_ai.response.status` (`queued`, `in_progress`, `completed`, `incomplete`, `failed`, `cancelled`), "distinct from
  `gen_ai.response.finish_reasons`", added with the `fetch_response` operation ([status][genai-response-status],
  [gen-ai: add fetch_response operation and span][genai-pr-353]).

**The realtime proposal** ([Add semantic conventions for realtime GenAI sessions][genai-pr-394], head `f80be17`,
approved but unmerged):

- The `realtime_inference` span lists `gen_ai.realtime_session.id`, `gen_ai.conversation.id`, the response id and
  model, finish reasons, the totals, and per-modality usage including `gen_ai.usage.audio.cache_read.input_tokens`
  ([attributes][pr394-span-attrs]). It doesn't list `gen_ai.response.status`.
- "For interrupted generations, instrumentations SHOULD report the provider-reported interruption reason when
  available", and `error` when an expected reason is missing or the generation ended abnormally
  ([finish reasons][pr394-finish]).
- `error.type` goes on the span, on `user_speech` and on the `realtime_session.ended` event
  ([span][pr394-span-attrs], [user speech][pr394-user-speech], [session ended][pr394-ended]).
- `gen_ai.realtime_session.id` uses OpenAI's `session.id` as its example, and instrumentations "MUST NOT use
  `gen_ai.conversation.id` as a realtime session identifier" ([session id][pr394-session-id]).
- Transcription isn't recorded as `realtime_inference` by default, and `user_speech` has no usage attributes
  ([span][pr394-span], [user speech][pr394-user-speech]). Input-transcription usage has nowhere to go.

### 8. Prior art

| Implementation | Session id | Finish reasons | Usage | Errors |
| --- | --- | --- | --- | --- |
| `azure-ai-voicelive` 1.3.1 `VoiceLiveInstrumentor` (opt-in) | `gen_ai.voice.session_id` on the `connect` span | the status, so `["incomplete"]` for a filtered reply | input and output totals, plus the token histogram | a `gen_ai.voice.error` span event with `error.code` and `error.message`; span status unchanged |
| .NET Microsoft.Extensions.AI realtime telemetry | none | the status | totals, cache reads, reasoning, and custom `gen_ai.usage.input_audio_tokens`, `input_text_tokens`, `output_audio_tokens` and `output_text_tokens` | `error.type` = provider code or `RealtimeError`; ERROR status |
| Pipecat 1.15.0, OpenAI Realtime service | not checked | not checked | totals, cached, input and output audio, and cached audio | ignores `response_cancel_not_active` and `conversation_already_has_active_response`; treats other errors as fatal |
| The accelerator (Semantic Kernel) | logged only | `["content_filter"]` only | totals only | every `error` event marks the call span ERROR, with `error.type="RealtimeAPIError"` |

- **`VoiceLiveInstrumentor`** ([Learn][vl-telemetry]) traces `connect`, `send` and `recv` spans. It sets
  `gen_ai.provider.name=microsoft.foundry` and `gen_ai.system=az.ai.voicelive`, against conventions schema 1.34.0
  ([constants][vl-sdk-utils]). On `response.done` it records the response id, `conversation_id`, and the status as the
  finish reason ([source][vl-instr-done]); usage is totals only ([source][vl-instr-usage]). Errors and rate limits
  become span events ([source][vl-instr-events]). Its interruption count counts the client's `response.cancel` sends,
  so server `turn_detected` cancellations don't count ([source][vl-instr-interrupt]); its turn count counts
  `response.done` events ([source][vl-instr-turn]).
- **Microsoft.Extensions.AI** puts the status in finish reasons, records `error.type` from the response error, tags
  modality usage with its own attribute names ([span tags][meai-tags], [names][meai-consts]), and splits its token
  histogram with custom `gen_ai.token.type` values `input_audio`, `input_text`, `output_audio` and `output_text`
  ([metrics][meai-metrics], [values][meai-token-types]).
- **Pipecat** maps `response.done` usage, including cached audio tokens, and logs the two benign error codes at debug
  level ([source][pc-errors], [usage][pc-usage]).

### 9. The accelerator today

Paths are relative to `src/ai_contact_centre_solution_accelerator/` unless they start with `docs/`.

- **Session id.** Read from `session.created` (`core/orchestration.py:532-535`), logged (`core/listeners.py:107-108`)
  and used as the recording file name (`core/listeners.py:161-166`, `core/listeners.py:289`). It isn't on any span.
- **Usage.** `response.usage` passes through (`core/orchestration.py:577-584`). `CallSpanManager.on_response_done`
  reads only `input_tokens` and `output_tokens`, sets them on the response span and sums them onto the call span
  (`observability/call_span_manager.py:282-307`).
- **Finish reasons.** Only `status_details.reason == "content_filter"` is checked (`core/orchestration.py:79-81`). A
  blocked response gets `gen_ai.response.finish_reasons=["content_filter"]`, keeps its status, and increments
  `cc.content_filter_count{channel=voice}` (`observability/call_span_manager.py:287-290`,
  `observability/metrics.py:74-87`). Cancelled, length-limited and failed responses look like successes, and no
  response id is recorded.
- **Errors.**
  - Every `error` event becomes `ServiceErrorEvent(error=str)` (`core/orchestration.py:540-545`). The listener marks the
    call span ERROR and adds a `realtime.error` event with `error.type="RealtimeAPIError"`, and `cc.error_count`
    counts it (`core/listeners.py:312-313`, `observability/call_span_manager.py:162-172`).
  - That includes a case the code itself expects: the content-filter fallback's `response.create` rejected while
    another response is active (`core/orchestration.py:541-543`).
  - Transcription failures are only logged (`core/orchestration.py:561-563`, `core/listeners.py:124-125`).
- **Provider and operation.** Every call says `gen_ai.provider.name=azure.ai.openai` and
  `gen_ai.operation.name=chat.completions`, Voice Live included (`observability/span_helpers.py:19`,
  `observability/span_helpers.py:119-133`).

## Mapping table

Shorthand: `S` is `session` on `session.created` (and `session.updated`), `R` is `response` on `response.done`, and
`U` is `R.usage`. "Same" means the same field as OpenAI.

Targets:

- **AF**: an attribute AF emits today.
- **v1.41**: the released conventions.
- **genai**: semantic-conventions-genai `main`, unreleased.
- **proposal**: the realtime proposal, unmerged.
- **AF-defined**: no convention exists, so AF would define it.

Numbers in brackets point to the [quirks and gaps](#quirks-and-gaps) below.

### Identity

| Signal | OpenAI Realtime | Azure OpenAI Realtime | Voice Live `2026-07-15` | AF today | Target |
| --- | --- | --- | --- | --- | --- |
| Realtime session id | `S.id` (`sess_…`) [1] | Same [2] | `S.id` (`sess_…`) | none | `gen_ai.realtime_session.id` (proposal) |
| Conversation id | `R.conversation_id` (`conv_…`; null for out-of-band responses); `conversation.created` | Same | `R.conversation_id` (optional) | `gen_ai.conversation.id` from the agent session | `gen_ai.conversation.id` = AF's agent-session id (AF) [3] |
| Response id | `R.id` (`resp_…`), also on `response.created` | Same | Same | `gen_ai.response.id` | Same (AF) |
| Requested model | `model` query parameter | Deployment name in `model` | `model` query parameter; with BYOM, a deployment plus `profile` | `gen_ai.request.model` | Same (AF) |
| Response model | `S.model` (dated, such as `gpt-realtime-2025-08-28`) | `S.model` (value undocumented) | `S.model` (the requested ID) | `gen_ai.response.model` | Same (AF), set from the session because `response.done` has no model |
| Provider | none | none | none | `openai`; `azure.ai.openai` when routed to Azure | `openai`, `azure.ai.openai` (v1.41); Voice Live has no value [4] |

### Usage (per response, on `response.done`)

| Signal | OpenAI Realtime | Azure OpenAI Realtime | Voice Live `2026-07-15` | AF today | Target |
| --- | --- | --- | --- | --- | --- |
| Input total | `U.input_tokens` | Same | Same | `gen_ai.usage.input_tokens` | Same (v1.41) |
| Output total | `U.output_tokens` | Same | Same | `gen_ai.usage.output_tokens` | Same (v1.41) |
| Total | `U.total_tokens` | Same | Same | `total_token_count`, not emitted | none |
| Cached input | `U.input_token_details.cached_tokens` | Same | Same | `gen_ai.usage.cache_read.input_tokens` (latest only) | Same (v1.41) |
| Cached input by modality | `U.input_token_details.cached_tokens_details.{text,audio,image}_tokens` | Same | SDK only [5] | none | `gen_ai.usage.{text,audio,image}.cache_read.input_tokens` (genai) |
| Audio input | `U.input_token_details.audio_tokens` | Same | Same | none [6] | `gen_ai.usage.audio.input_tokens` (genai) |
| Text input | `U.input_token_details.text_tokens` | Same | Same | none | `gen_ai.usage.text.input_tokens` (genai) |
| Image input | `U.input_token_details.image_tokens` | Same | SDK only [5] | none | `gen_ai.usage.image.input_tokens` (genai) |
| Audio output | `U.output_token_details.audio_tokens` | Same | Same | none [6] | `gen_ai.usage.audio.output_tokens` (genai) |
| Text output | `U.output_token_details.text_tokens` | Same | Same | none | `gen_ai.usage.text.output_tokens` (genai) |
| Reasoning output | not reported | not reported | `U.output_token_details.reasoning_tokens` (`2026-07-15` only) | `gen_ai.usage.reasoning.output_tokens` | Same (v1.41) |
| Input transcription | `usage` on `conversation.item.input_audio_transcription.completed` (`tokens` or `duration`) | Same; only `tokens` documented | not reported | none | AF-defined [7] |

### Status and finish reasons (on `response.done`)

| Outcome | OpenAI Realtime | Azure OpenAI Realtime | Voice Live `2026-07-15` | AF today | Target |
| --- | --- | --- | --- | --- | --- |
| Status | `R.status` | Same | Same | none | `gen_ai.response.status` (genai, defined for `fetch_response`) [8] |
| Completed | `completed`; `status_details` is null | Same | Same | `stop` or `tool_calls` (chat) | `finish_reasons=["stop"]`, or `["tool_call"]` when `R.output` holds `function_call` items |
| Content filter | `incomplete`; `status_details.reason` is `content_filter` | Same, with no category or severity [9] | Same on the wire and in the SDK; Learn documents only `type` [5] | `content_filter` (chat) | `["content_filter"]`; span status stays unset |
| Max output tokens | `incomplete`; `max_output_tokens` | Same | Same (SDK) | `length` (chat) | `["length"]` |
| Turn detected | `cancelled`; `turn_detected` | Same | Same (SDK) | none | `["turn_detected"]` (proposal: report the provider's interruption reason) |
| Client cancelled | `cancelled`; `client_cancelled` | Same | Same (SDK) | none | `["client_cancelled"]` |
| Failed | `failed`; `status_details.error.{type,code,message}` | Same | `failed`; `error`, untyped in the SDK | none | `["error"]`, `error.type` = the provider's `code`, else its `type`, and ERROR status (v1.41) |
| Playback cut off | not reported: the response is `completed` and the client sends `conversation.item.truncate` [10] | Same | Same | none | AF-defined [10] |

### Errors and other signals

| Signal | OpenAI Realtime | Azure OpenAI Realtime | Voice Live `2026-07-15` | AF today | Target |
| --- | --- | --- | --- | --- | --- |
| Session error | `error` → `error.{type,code,message,param,event_id}` | Same [2] | Same | exceptions only: `error.type` = class name | a span event or log, with `error.type` = provider code on the span it affects; `error.type` on `realtime_session.ended` only when the session dies (proposal) [11] |
| Transcription failure | `conversation.item.input_audio_transcription.failed` → `error.{type,code,message,param}` and `item_id` | Same (such as `transcription_error`, `audio_unintelligible`) | Same | none | `error.type` on `user_speech` (proposal, opt-in), or a span event |
| Warning | none | none | `warning` → `warning.{message,code,param}` | none | a log or span event (AF-defined) |
| Rate limits | `rate_limits.updated` → `rate_limits[]` | Same | Same | none | none; pass through |
| Caller speech | `input_audio_buffer.speech_started` (`audio_start_ms`) and `.speech_stopped` (`audio_end_ms`) | Same | Same | none | `user_speech` span (proposal, opt-in) |
| Output modality | `R.output_modalities` | Same | `R.modalities` | none (`OtelAttr` defines `gen_ai.output.type` but nothing sets it) | `gen_ai.output.type` = `speech` or `text` (v1.41) |

### Quirks and gaps

1. The `openai` SDK types `S` without `id` in every version AF allows; read it as an extra field.
2. Azure's current reference defers to OpenAI's. The classic reference omits the `error` and `session.*` events,
   though the service sends them.
3. OpenAI's `conv_…` lasts only as long as the session. The conventions say to set `gen_ai.conversation.id` "when the
   client framework being instrumented manages conversation history" ([proposal][pr394-conv-id]), which is AF's
   agent session. A Foundry voice agent's stored `conversation_id` could fill the same role.
4. Voice Live's candidates are `azure.ai.foundry` (AF's Foundry clients), `microsoft.foundry` (the Voice Live SDK's
   instrumentor) or a new value. `azure.ai.openai`, which the accelerator uses today, would be wrong.
5. Learn's Voice Live reference leaves out `status_details.reason` and `.error`, `image_tokens` and
   `cached_tokens_details`. The 1.3.1 SDK models all of them, and the accelerator has received `content_filter`.
6. AF's chat-completions client already drops `prompt/audio_tokens` and `completion/audio_tokens` from telemetry.
7. Input transcription runs on a different model at a different rate, so its usage mustn't be added to the response's
   `gen_ai.usage.*`. Neither the released conventions nor the proposal have a place for it.
8. The proposal's `realtime_inference` span doesn't list `gen_ai.response.status`, so emitting it there is AF's call.
9. Azure chat and Responses calls report filter categories and fail on blocked prompts; realtime does neither.
10. A `completed` response can still be cut off during playback, and only the client knows `audio_end_ms`. Voice
    Live's `auto_truncate` might let the server know (unverified).
11. Most `error` events are recoverable, and some are benign races: `response_cancel_not_active` and
    `conversation_already_has_active_response` (Pipecat ignores both).

## Implications for the map

- **[Middleware and telemetry contract for realtime agents][t9]** can adopt the mapping table as its normative mapping.
  These choices remain:
  1. **Finish-reason vocabulary.** The conventions' values plus the provider's interruption reasons, as the proposal
     says (recommended), or folding `turn_detected` and `client_cancelled` into `error`. Also whether to fix AF's
     `tool_calls` span value.
  2. **Status.** Whether to emit `gen_ai.response.status` alongside finish reasons.
  3. **Error policy.** Which `error` events mark which span ERROR: the affected response or tool, and the session only
     when the connection dies. Which codes are benign. Whether `error.type` is the provider's code (recommended).
  4. **Usage keys.** Adding modality keys to `UsageDetails` and mapping them to semantic-conventions-genai names
     means targeting unreleased names behind the latest-experimental opt-in. That is the semconv-target decision
     again.
  5. **Transcription usage.** An AF-defined attribute or metric, or raw pass-through only.
  6. **Voice Live's provider name.**
  7. **Conversation id.** AF's agent session (recommended), or the provider's ids.
  8. **Cut-off playback.** An AF-defined attribute or event when the conversation truncates a response. This ties to
     [Live conversation controls: session updates, explicit responses, interruption][t12].
  9. **Double instrumentation.** If the Voice Live client wraps `azure-ai-voicelive`, its opt-in instrumentor would
     add a second span tree that disagrees with AF's. This ties to
     [Azure provider clients: design and package placement][t8].
- **[Realtime event taxonomy: first-class signals vs passthrough][t14]** should take this ticket as an input:
  - "Response done" needs the response id, status, reason, usage with its modality breakdown, and the failure error.
  - "Session created" needs the session id, model and expiry.
  - "Error" needs `type`, `code`, `message`, `param` and `event_id`.
  - `warning`, `rate_limits.updated` and Voice Live's extra events can pass through raw.
  - Dispatch on `type`, not on `openai` SDK classes.
- **[Probe realtime wire behaviour across providers][t13]** could also settle [open questions](#open-questions) 1 to 6
  and 9.
- **[Prototype the accelerator's voice loop on the extended API][t10]** should check that the accelerator's
  per-response usage, content-filter handling, `cc.content_filter_count` and error spans can be rebuilt from AF's
  updates and spans.
- **New ticket (task): Per-modality token usage in Agent Framework telemetry.** Should `UsageDetails` gain standard
  audio, text, image and cached-by-modality keys, mapped to semantic-conventions-genai's `gen_ai.usage.*` names behind
  the latest-experimental opt-in, so realtime usage and the chat-completions client's audio tokens reach telemetry?
  Blocked by [Middleware and telemetry contract for realtime agents][t9], which picks the semconv target.
- **Out of scope:**
  - GPT-Live, OpenAI's newer voice API. It's a different protocol, billed by the second, with a
    `session.usage.updated` event ([GPT-Live][oai-gpt-live], [GPT-Live costs][oai-gpt-live-cost]).
  - Billing reconciliation and cost estimates. Foundry already estimates cost on the server for voice agents.
  - Contributing realtime status or transcription usage to the OpenTelemetry conventions. The ADR can note the gaps.
  - Rate-limit telemetry beyond pass-through.
- **Pre-existing AF issues** to raise separately: chat-completions audio tokens are dropped from telemetry, and spans
  say `tool_calls` while messages say `tool_call`.

## Open questions

1. What do Azure OpenAI and Voice Live put in `session.model`?
2. Does Voice Live send `status_details.reason` for `cancelled` responses and `status_details.error` for `failed`
   ones, as its SDK models? Does it send `cached_tokens_details` and `image_tokens`?
3. What do cascaded and BYOM Voice Live sessions report in `audio_tokens`? Does any event report Azure Speech usage?
4. Does Azure OpenAI send `duration` transcription usage, for example with `gpt-transcribe` deployments? Does Voice
   Live ever send transcription usage?
5. Does an Azure content-filter block ever carry more than the reason, such as a category? Does a jailbreak in the
   caller's speech ever surface as an `error` instead?
6. Which benign error codes do Azure OpenAI and Voice Live send, and with what `type`?
7. With Voice Live's `auto_truncate` on, does the server send `conversation.item.truncated`, so a cut-off reply is
   visible without client bookkeeping?
8. Will the realtime proposal adopt `gen_ai.response.status`, or define a place for transcription usage, before AF
   ships?
9. Where do OpenAI and Azure OpenAI count the reasoning tokens of reasoning-capable models such as `gpt-realtime-2`:
   inside `text_tokens`, or nowhere?

## Appendix: SDK parsing experiment

I fed provider-shaped events through the `openai` SDK's WebSocket parser (`AsyncRealtimeConnection.parse_event`) in
scratch virtual environments with `openai==2.25.0` and `openai==3.39.0`; the package index available here stopped at
3.39.0. Both versions printed the same results. Between 2.25.0 and 3.42.1, the types this experiment touches differ
only in the declared `message` on `status_details.error`, and the event union keeps the same order, so the results
carry over to 3.42.1. The payloads follow the documented examples, with `status_details` and `reasoning_tokens` shaped
as the Voice Live SDK models them; they aren't captured traffic.

```python
import json

import openai
from openai.resources.realtime.realtime import AsyncRealtimeConnection

session_created = {
    "type": "session.created",
    "event_id": "event_1",
    "session": {
        "type": "realtime",
        "object": "realtime.session",
        "id": "sess_C9G5QPteg4UIbotdKLoYQ",
        "model": "gpt-realtime-2025-08-28",
        "expires_at": 1756324625,
    },
}

# Voice Live 2026-07-15 shape: flat session, no session.type.
voice_live_session_created = {
    "type": "session.created",
    "event_id": "event_2",
    "session": {
        "id": "sess_ABC123DEF456",
        "object": "realtime.session",
        "model": "gpt-realtime",
        "voice": {"type": "azure-standard", "name": "en-US-Ava:DragonHDLatestNeural"},
    },
}

voice_live_content_filtered_done = {
    "type": "response.done",
    "event_id": "event_3",
    "response": {
        "id": "resp_ABC123",
        "object": "realtime.response",
        "status": "incomplete",
        "status_details": {"type": "incomplete", "reason": "content_filter"},
        "output": [],
        "usage": {
            "total_tokens": 87,
            "input_tokens": 52,
            "output_tokens": 35,
            "input_token_details": {"cached_tokens": 0, "text_tokens": 45, "audio_tokens": 7},
            "output_token_details": {"text_tokens": 15, "audio_tokens": 20, "reasoning_tokens": 4},
        },
    },
}

failed_done = {
    "type": "response.done",
    "event_id": "event_4",
    "response": {
        "id": "resp_failed",
        "status": "failed",
        "status_details": {
            "type": "failed",
            "error": {"type": "server_error", "code": "internal_error", "message": "boom"},
        },
    },
}

voice_live_warning = {
    "type": "warning",
    "event_id": "event_5",
    "warning": {"code": "some_code", "message": "informational"},
}

parse = AsyncRealtimeConnection.parse_event

print("openai", openai.__version__)

parsed_session = parse(None, json.dumps(session_created))
print("session.created:", type(parsed_session).__name__, type(parsed_session.session).__name__)
print("  declared fields include id:", "id" in type(parsed_session.session).model_fields)
print("  getattr(session, 'id'):", getattr(parsed_session.session, "id", None))
print("  session.model:", parsed_session.session.model)

parsed_voice_live_session = parse(None, json.dumps(voice_live_session_created))
print(
    "voice live session.created:",
    type(parsed_voice_live_session.session).__name__,
    getattr(parsed_voice_live_session.session, "id", None),
)

parsed_done = parse(None, json.dumps(voice_live_content_filtered_done))
response = parsed_done.response
print("response.done:", type(parsed_done).__name__)
print("  status:", response.status, "| reason:", response.status_details.reason)
print("  usage audio in/out:", response.usage.input_token_details.audio_tokens, response.usage.output_token_details.audio_tokens)
print("  undeclared reasoning_tokens:", getattr(response.usage.output_token_details, "reasoning_tokens", None))

parsed_failed = parse(None, json.dumps(failed_done))
failed_error = parsed_failed.response.status_details.error
print("failed:", failed_error.type, failed_error.code, getattr(failed_error, "message", None))

parsed_warning = parse(None, json.dumps(voice_live_warning))
print("warning:", type(parsed_warning).__name__, getattr(parsed_warning, "type", None), getattr(parsed_warning, "warning", None))
```

Output with `openai==2.25.0` (3.39.0 differs only in the version line):

```text
openai 2.25.0
session.created: SessionCreatedEvent RealtimeSessionCreateRequest
  declared fields include id: False
  getattr(session, 'id'): sess_C9G5QPteg4UIbotdKLoYQ
  session.model: gpt-realtime-2025-08-28
voice live session.created: RealtimeSessionCreateRequest sess_ABC123DEF456
response.done: ResponseDoneEvent
  status: incomplete | reason: content_filter
  usage audio in/out: 7 20
  undeclared reasoning_tokens: 4
failed: server_error internal_error boom
warning: ConversationCreatedEvent warning {'code': 'some_code', 'message': 'informational'}
```

## Sources

| Source | Pinned at |
| --- | --- |
| Agent Framework (`microsoft/agent-framework`) | commit [`f4bdd1d`][af-commit] (main, 2026-11-03) |
| `openai` Python SDK | tags [`v3.42.1`][oai-tag-3421] (latest, 2026-11-01) and [`v2.25.0`][oai-tag-225] (AF's floor); the experiment ran on 2.25.0 and 3.39.0 |
| OpenAI API reference and guides | developers.openai.com, read 2026-11-03 |
| Microsoft Learn | read 2026-11-03; page dates: Azure OpenAI reference 2026-05-13, classic GA reference 2026-01-26, Voice Live `2026-07-15` reference 2026-10-27, Voice Live `2025-10-01` reference 2026-09-24, Voice Live overview 2026-10-26, Voice Live FAQ 2026-03-31, Voice Live telemetry 2026-04-28, default safety policies 2026-10-08, Responses how-to 2026-08-18, Foundry voice-agent migration 2026-10-25, pricing 2026-10-06, observability 2026-09-30 |
| `azure-ai-voicelive` | tag [`azure-ai-voicelive_1.3.1`][vl-sdk-tag] (the wheel's files match the tag) |
| OpenTelemetry semantic conventions | [`v1.41.1`][semconv-1411-registry] |
| OpenTelemetry GenAI semantic conventions (`semantic-conventions-genai`) | main at [`95aff9c`][genai-registry]; realtime proposal at head [`f80be17`][pr394-span] |
| Microsoft.Extensions.AI (`dotnet/extensions`) | commit [`47dd770`][meai-tags] |
| Pipecat (`pipecat-ai/pipecat`) | commit [`e03fa8d`][pc-errors] (release 1.15.0) |
| AI Contact Centre accelerator (private) | local commit `d42fbcf`, branch `feat/content-filter-handling` |

[ticket]: https://github.com/adamdougal/agent-framework/issues/15
[map]: https://github.com/adamdougal/agent-framework/issues/1
[t4]: https://github.com/adamdougal/agent-framework/issues/4
[t5]: https://github.com/adamdougal/agent-framework/issues/5
[t8]: https://github.com/adamdougal/agent-framework/issues/8
[t9]: https://github.com/adamdougal/agent-framework/issues/9
[t10]: https://github.com/adamdougal/agent-framework/issues/10
[t12]: https://github.com/adamdougal/agent-framework/issues/12
[t13]: https://github.com/adamdougal/agent-framework/issues/13
[t14]: https://github.com/adamdougal/agent-framework/issues/14
[f4]: https://github.com/adamdougal/agent-framework/blob/research/azure-realtime-protocols/docs/research/azure-realtime-protocols.md
[f5]: https://github.com/adamdougal/agent-framework/blob/research/realtime-middleware-telemetry/docs/research/realtime-middleware-telemetry.md

[oai-events]: https://developers.openai.com/api/reference/resources/realtime/server-events
[oai-session-created]: https://developers.openai.com/api/reference/resources/realtime/server-events#session.created
[oai-conversation-created]: https://developers.openai.com/api/reference/resources/realtime/server-events#conversation.created
[oai-response-created]: https://developers.openai.com/api/reference/resources/realtime/server-events#response.created
[oai-response-done]: https://developers.openai.com/api/reference/resources/realtime/server-events#response.done
[oai-error]: https://developers.openai.com/api/reference/resources/realtime/server-events#error
[oai-rate-limits]: https://developers.openai.com/api/reference/resources/realtime/server-events#rate_limits.updated
[oai-cost]: https://developers.openai.com/api/docs/guides/voice-latency-cost#realtime-api-costs
[oai-cost-per-response]: https://developers.openai.com/api/docs/guides/voice-latency-cost#per-response-costs
[oai-cost-transcription]: https://developers.openai.com/api/docs/guides/voice-latency-cost#input-transcription-costs
[oai-conversations-lifecycle]: https://developers.openai.com/api/docs/guides/realtime-conversations#session-lifecycle-events
[oai-out-of-band]: https://developers.openai.com/api/docs/guides/realtime-conversations#create-responses-outside-the-default-conversation
[oai-error-handling]: https://developers.openai.com/api/docs/guides/realtime-conversations#error-handling
[oai-interruption]: https://developers.openai.com/api/docs/guides/realtime-conversations#interruption-and-truncation
[oai-gpt-live]: https://developers.openai.com/api/docs/guides/live
[oai-gpt-live-cost]: https://developers.openai.com/api/docs/guides/voice-latency-cost#gpt-live-usage-and-costs

[oai-tag-3421]: https://github.com/openai/openai-python/tree/v3.42.1
[oai-tag-225]: https://github.com/openai/openai-python/tree/v2.25.0
[oai-sdk-response]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/types/realtime/realtime_response.py#L38-L101
[oai-sdk-status]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/types/realtime/realtime_response_status.py#L11-L49
[oai-sdk-status-225]: https://github.com/openai/openai-python/blob/v2.25.0/src/openai/types/realtime/realtime_response_status.py#L11-L21
[oai-sdk-usage]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/types/realtime/realtime_response_usage.py#L12-L49
[oai-sdk-input-details]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/types/realtime/realtime_response_usage_input_token_details.py#L10-L42
[oai-sdk-output-details]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/types/realtime/realtime_response_usage_output_token_details.py#L10-L17
[oai-sdk-session-created]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/types/realtime/session_created_event.py#L12-L30
[oai-sdk-session-response]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/types/realtime/realtime_session_create_response.py#L422-L437
[oai-sdk-error]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/types/realtime/realtime_error.py#L10-L26
[oai-sdk-transcription-completed]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/types/realtime/conversation_item_input_audio_transcription_completed_event.py#L19-L96
[oai-sdk-transcription-failed]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/types/realtime/conversation_item_input_audio_transcription_failed_event.py#L11-L47
[oai-sdk-union]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/types/realtime/realtime_server_event.py#L135-L185
[oai-sdk-parse]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/resources/realtime/realtime.py#L425-L433
[oai-sdk-models-extra]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/_models.py#L150

[aoai-ref]: https://learn.microsoft.com/azure/foundry/openai/realtime-audio-reference
[aoai-ref-classic]: https://learn.microsoft.com/azure/foundry-classic/openai/realtime-audio-reference-ga#server-events
[aoai-ref-classic-done]: https://learn.microsoft.com/azure/foundry-classic/openai/realtime-audio-reference-ga#responsedone
[aoai-ref-classic-transcription]: https://learn.microsoft.com/azure/foundry-classic/openai/realtime-audio-reference-ga#transcription-error-object
[aoai-responses-guardrails]: https://learn.microsoft.com/azure/foundry/openai/how-to/responses#handle-guardrails-and-content-filtering
[default-safety]: https://learn.microsoft.com/azure/foundry/guardrails/default-safety-policies#audio-models

[vl-ref]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15
[vl-session-created]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15#sessioncreated
[vl-response-created]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15#responsecreated
[vl-transcription-completed]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15#conversationiteminput_audio_transcriptioncompleted
[vl-transcription-failed]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15#conversationiteminput_audio_transcriptionfailed
[vl-error]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15#error
[vl-warning]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15#warning
[vl-realtimeresponse]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15#realtimeresponse
[vl-realtimeresponsestatus]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15#realtimeresponsestatus
[vl-realtimeusage]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15#realtimeusage
[vl-tokendetails]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15#tokendetails
[vl-status-details]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15#realtimeresponsestatusdetails
[vl-response-session]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15#realtimeresponsesession
[vl-ref-1001-status-details]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2025-10-01#realtimeresponsestatusdetails
[vl-ref-1001-tokendetails]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2025-10-01#tokendetails
[vl-overview-pricing]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live#pricing
[vl-overview-tokens]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live#token-usage-and-cost-estimation
[vl-faq]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-faq#can-you-modify-or-disable-the-content-filtering-in-voice-live-api-
[vl-telemetry]: https://learn.microsoft.com/azure/ai-services/speech-service/how-to-voice-live-telemetry?pivots=programming-language-python#what-gets-traced

[fva-migrate-events]: https://learn.microsoft.com/azure/foundry/agents/how-to/migrate-from-voice-live#adapt-audio-and-event-handling
[fva-migrate-monitoring]: https://learn.microsoft.com/azure/foundry/agents/how-to/migrate-from-voice-live#configure-history-recordings-and-monitoring
[fva-pricing]: https://learn.microsoft.com/azure/foundry/agents/concepts/voice-agent-pricing#see-usage-before-the-invoice
[fva-observability]: https://learn.microsoft.com/azure/foundry/agents/concepts/voice-agent-observability#trace-a-voice-based-agent

[vl-sdk-tag]: https://github.com/Azure/azure-sdk-for-python/tree/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive
[vl-sdk-status-details]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/models/_models.py#L3997-L4030
[vl-sdk-cancelled]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/models/_models.py#L4031-L4067
[vl-sdk-failed]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/models/_models.py#L4250-L4282
[vl-sdk-incomplete]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/models/_models.py#L4499-L4535
[vl-sdk-token-usage]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/models/_models.py#L8270-L8321
[vl-sdk-input-details]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/models/_models.py#L3060-L3109
[vl-sdk-cached-details]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/models/_models.py#L1749-L1786
[vl-sdk-output-details]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/models/_models.py#L3466-L3503
[vl-sdk-error]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/models/_models.py#L5638-L5723
[vl-sdk-warning]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/models/_models.py#L8039-L8077
[vl-sdk-utils]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/telemetry/_utils.py#L22-L97
[vl-instr-done]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/telemetry/_voicelive_instrumentor.py#L1313-L1374
[vl-instr-usage]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/telemetry/_voicelive_instrumentor.py#L937-L964
[vl-instr-events]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/telemetry/_voicelive_instrumentor.py#L1481-L1541
[vl-instr-interrupt]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/telemetry/_voicelive_instrumentor.py#L778-L781
[vl-instr-turn]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/telemetry/_voicelive_instrumentor.py#L917-L921

[af-commit]: https://github.com/microsoft/agent-framework/tree/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f
[af-usage-details]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_types.py#L586-L607
[af-finish-reason]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_types.py#L2356-L2366
[af-finish-map]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L376-L381
[af-usage-map]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L382-L397
[af-latest-gate]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L399-L404
[af-conv-id]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L134-L153
[af-invoke-conv-id]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L2383-L2389
[af-attr-map]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L3127-L3162
[af-capture-exception]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L3221-L3228
[af-finish-map-use]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L3390-L3391
[af-response-attrs]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L3571-L3588
[af-metric-attrs]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L3591-L3604
[af-capture-response]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/observability.py#L3626-L3643
[af-oai-provider]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/openai/agent_framework_openai/_chat_client.py#L842-L843
[af-oai-content-filter]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/openai/agent_framework_openai/_chat_client.py#L870-L884
[af-oai-finish]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/openai/agent_framework_openai/_chat_client.py#L3489-L3503
[af-cc-usage]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/openai/agent_framework_openai/_chat_completion_client.py#L988-L1014
[af-foundry-provider]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/foundry/agent_framework_foundry/_chat_client.py#L167

[semconv-1411-registry]: https://github.com/open-telemetry/semantic-conventions/blob/v1.41.1/model/gen-ai/registry.yaml
[semconv-1411-provider]: https://github.com/open-telemetry/semantic-conventions/blob/v1.41.1/model/gen-ai/registry.yaml#L8-L100
[semconv-1411-finish]: https://github.com/open-telemetry/semantic-conventions/blob/v1.41.1/model/gen-ai/registry.yaml#L177-L183
[semconv-1411-usage]: https://github.com/open-telemetry/semantic-conventions/blob/v1.41.1/model/gen-ai/registry.yaml#L192-L226
[semconv-1411-output-type]: https://github.com/open-telemetry/semantic-conventions/blob/v1.41.1/model/gen-ai/registry.yaml#L441-L474
[semconv-1411-finish-schema]: https://github.com/open-telemetry/semantic-conventions/blob/v1.41.1/docs/gen-ai/gen-ai-output-messages.json#L101-L112
[semconv-1411-error-type]: https://github.com/open-telemetry/semantic-conventions/blob/v1.41.1/docs/gen-ai/gen-ai-spans.md#L126-L128

[genai-registry]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/model/gen-ai/registry.yaml
[genai-provider]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/model/gen-ai/registry.yaml#L3-L97
[genai-response-status]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/model/gen-ai/registry.yaml#L230-L269
[genai-usage]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/model/gen-ai/registry.yaml#L282-L403
[genai-modality]: https://github.com/open-telemetry/semantic-conventions-genai/blob/95aff9cdf605bbc7d7e8f7c4745a693bd0c4eea8/model/gen-ai/registry.yaml#L404-L435
[genai-pr-353]: https://github.com/open-telemetry/semantic-conventions-genai/pull/353
[genai-pr-394]: https://github.com/open-telemetry/semantic-conventions-genai/pull/394
[pr394-span]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/docs/gen-ai/gen-ai-realtime.md#L18-L58
[pr394-span-attrs]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/docs/gen-ai/gen-ai-realtime.md#L66-L99
[pr394-conv-id]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/docs/gen-ai/gen-ai-realtime.md#L127-L145
[pr394-session-id]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/docs/gen-ai/gen-ai-realtime.md#L154-L168
[pr394-finish]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/docs/gen-ai/gen-ai-realtime.md#L177-L190
[pr394-user-speech]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/docs/gen-ai/gen-ai-realtime.md#L376-L419
[pr394-ended]: https://github.com/open-telemetry/semantic-conventions-genai/blob/f80be17d1516649ea15a55c615f5ce8a7cad2850/docs/gen-ai/gen-ai-realtime.md#L641-L668

[meai-tags]: https://github.com/dotnet/extensions/blob/47dd770c802d4db8d106d1b81e4e52edd26cbc18/src/Libraries/Microsoft.Extensions.AI/Realtime/OpenTelemetryRealtimeClientSession.cs#L833-L887
[meai-metrics]: https://github.com/dotnet/extensions/blob/47dd770c802d4db8d106d1b81e4e52edd26cbc18/src/Libraries/Microsoft.Extensions.AI/Realtime/OpenTelemetryRealtimeClientSession.cs#L759-L806
[meai-consts]: https://github.com/dotnet/extensions/blob/47dd770c802d4db8d106d1b81e4e52edd26cbc18/src/Libraries/Microsoft.Extensions.AI/OpenTelemetryConsts.cs#L172-L176
[meai-token-types]: https://github.com/dotnet/extensions/blob/47dd770c802d4db8d106d1b81e4e52edd26cbc18/src/Libraries/Microsoft.Extensions.AI/OpenTelemetryConsts.cs#L28-L31

[pc-errors]: https://github.com/pipecat-ai/pipecat/blob/e03fa8dba962ade3cee2095763ad1ac87c2c3bd6/src/pipecat/services/openai/realtime/llm.py#L953-L962
[pc-usage]: https://github.com/pipecat-ai/pipecat/blob/e03fa8dba962ade3cee2095763ad1ac87c2c3bd6/src/pipecat/services/openai/realtime/llm.py#L1090-L1135
