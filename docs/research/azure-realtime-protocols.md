# Azure realtime protocols: Azure OpenAI, Voice Live and Foundry voice agents

Findings for [Azure realtime protocols: Azure OpenAI, Voice Live and Foundry voice agents][ticket] on the map
[Python realtime agents in Agent Framework][map]. Researched on 2026-11-03.

Pinned sources:

- Microsoft Learn pages and the OpenAI Realtime API reference as published on 2026-11-03.
- `openai` Python SDK [v2.25.0][oai-tag-225] (Agent Framework's lower bound), [v3.39.0][oai-tag-339] and
  [v3.42.1][oai-tag-3421] (latest on PyPI).
- `azure-ai-voicelive` [1.3.1][vl-tag] and `azure-ai-projects` [2.7.0][proj-tag-27] / [2.10.0][proj-tag-210].
- Semantic Kernel Python [1.45.0][sk-azure-rt], the version the accelerator locks.
- Agent Framework `main` at [`f4bdd1d`][af-sha].
- The accelerator's private repo (`src/ai_contact_centre_solution_accelerator/...`), local checkout at `d42fbcf`. Its
  files are cited by path, and only for wire-level behaviour it observed.

## Question

How do Azure OpenAI Realtime, Azure Voice Live and Foundry voice agents differ from the OpenAI Realtime protocol that
ADR 0035's `OpenAIRealtimeClient` targets, and where do Azure clients live in Agent Framework today?

## Answer

1. **Azure OpenAI Realtime is the OpenAI GA protocol; only the connection differs.** Learn says it "follows the OpenAI
   Realtime API specification" and documents one deviation: the transcription `model` is a deployment name
   ([AOAI reference][aoai-ref]).
   - GA route: `wss://<resource>.openai.azure.com/openai/v1/realtime?model=<deployment>`, with no `api-version`, using
     an API key or an Entra token ([AOAI WebSockets][aoai-ws]). The preview surface was deprecated on 2026-04-30
     ([migration guide][aoai-mig]).
   - So Azure OpenAI needs Azure routing on `OpenAIRealtimeClient`, as `OpenAIChatClient` has today, not a class of its
     own.
2. **Don't use the `openai` SDK's `AsyncAzureOpenAI` for realtime.** In every SDK version Agent Framework allows,
   `AsyncAzureOpenAI.realtime.connect()` adds `api-version` and `deployment` to the URL
   ([`azure.py`][oai-azure-py]). Semantic Kernel 1.45.0 bypasses it for that reason
   ([`azure_realtime.py`][sk-azure-rt]). Use `AsyncOpenAI(websocket_base_url="wss://…/openai/v1", api_key=<key or token>)`
   as Learn does ([AOAI WebSockets][aoai-ws]).
3. **Voice Live is not GA-protocol compatible.** Every API version up to GA `2026-07-15` keeps the OpenAI preview shape:
   a flat session, `response.audio.*`, `response.text.*` and `response.audio_transcript.*`, and no `session.type`
   ([VL 2026-07-15][vl-ref]).
   - Azure extensions sit on top: Azure semantic VAD and end-of-utterance detection, deep noise suppression, server echo
     cancellation, Azure voice objects, visemes, avatar, `pre_generated_assistant_message`, and more than 20 extra
     events.
   - It has its own endpoint (`/voice-live/realtime?api-version=…&model=…`) and model namespace.
   - It needs its own client, or a preview-to-neutral mapper.
4. **Foundry voice agents (preview) are GA protocol with Voice Live features inside.**
   - A `kind: voice` agent version holds the model, instructions, greeting, audio settings and tools.
   - Clients connect to `…/api/projects/<project>/agents/<agent>/endpoint/protocols/voice?api-version=v1` with an
     Entra token (`https://ai.azure.com/.default`; no API keys), plus `Foundry-Features: VoiceAgents=V1Preview` and the
     `realtime` subprotocol ([migration guide][fva-migrate]).
   - Instructions and tools are defaults that a session can override. `function` tools run in the app; MCP and toolbox
     tools run in the service ([configure][fva-config]).
   - The SDK is `azure-ai-projects[voice]>=2.7.0`, but `agent-framework-foundry` caps `azure-ai-projects` below 2.8.0.
5. **Placement today.**
   - Azure OpenAI has no classes of its own. `agent-framework-openai`'s generic clients route to it through
     `azure_endpoint` and `credential`; the `AzureOpenAI*` classes and `agent-framework-azure-ai` are gone.
   - Foundry lives in `agent-framework-foundry` (`FoundryChatClient`, `FoundryAgent`,
     `RawFoundryAgentChatClient(RawOpenAIChatClient)`), exposed through `agent_framework.foundry`. That namespace also
     re-exports beta packages that wrap Foundry Tools SDKs.
   - Natural homes:
     - Azure OpenAI: `agent-framework-openai`
     - Foundry voice agents: `agent-framework-foundry`, `FoundryAgent`-style
     - Voice Live: `agent_framework.foundry`, either in `agent-framework-foundry` or a new beta package
6. **Wrap `azure-ai-voicelive`, or speak raw websocket?**
   - The SDK is GA, typed and offers raw-dict sends.
   - Against it, it changed its default API version in three of its last four releases, two of them flagged as
     breaking. It also lags the service, can't reach the Foundry voice-agent route and types events differently from
     `openai`.
   - Raw websocket already works in the accelerator. The evidence leans raw, with options passed through.
     [Azure provider clients: design and package placement][t8] decides.
7. **Two cross-provider facts.**
   - OpenAI and Azure OpenAI can't change `voice` after the first audio ([OpenAI reference][oai-ref]). Voice Live can
     ([VL 2026-07-15][vl-ref]), and so could Foundry voice agents in the accelerator's probes, within a voice family.
   - Every surface needs string tool results. The accelerator's "Azure hangs" came from a Semantic Kernel serialisation
     bug, not from the protocol.

## Findings

### 1. Baseline: the OpenAI GA Realtime protocol

- Connection: `wss://api.openai.com/v1/realtime?model=…` with a bearer token ([OpenAI reference][oai-ref]).
- `session.update` carries `session.type: "realtime"`. Audio settings nest under `audio.input` (`format`,
  `noise_reduction` with `near_field` / `far_field`, `transcription`, and `turn_detection` with `server_vad` /
  `semantic_vad`) and `audio.output` (`format`, `voice`, `speed`). The session also holds `output_modalities`,
  `instructions`, `tools`, `tool_choice`, `max_output_tokens`, `reasoning`, `tracing` and `truncation`
  ([OpenAI reference][oai-ref]).
- "Voice cannot be changed during the session once the model has responded with audio at least once." `session.update`
  may change any field except `voice` and `model` ([OpenAI reference][oai-ref]).
- `function_call_output.output` is a `string`. A `response.done` that didn't complete reports
  `status_details.reason`: `turn_detected`, `client_cancelled`, `max_output_tokens` or `content_filter`
  ([OpenAI reference][oai-ref]).
- ADR 0035 puts `RawOpenAIRealtimeClient` / `OpenAIRealtimeClient` in `agent-framework-openai` and keeps core
  provider-free. Its illustrative `RealtimeSessionOptions` is flat: `voice: str`, `input_audio_format: str`,
  `output_audio_format: str`, `input_audio_transcription`, `turn_detection`. That is closer to the preview and Voice Live
  shape than to GA ([ADR 0035][adr0035], "Direct realtime model path" and "Appendix: Proposed Python realtime client
  protocol").

### 2. Azure OpenAI Realtime

**GA and preview.** The preview API was deprecated on 2026-04-30 ([migration guide][aoai-mig]). Moving to GA changes
these things ([migration guide][aoai-mig]):

- The URL becomes `/openai/v1/realtime` with no `api-version`. Sending `api-version` on the GA URL returns 401.
- `session.update` requires `session.type`.
- `response.text.delta`, `response.audio.delta` and `response.audio_transcript.delta` become `response.output_text.*`,
  `response.output_audio.*` and `response.output_audio_transcript.*`.
- Assistant content types become `output_text` and `output_audio`.
- The `OpenAI-Beta` header is dropped.
- `conversation.item.added` and `conversation.item.done` are new events.

The how-to also says: "Don't use date-based API versions or the api-version query parameter"
([how-to][aoai-howto]).

**Endpoint and naming.**

- URL: `wss://<resource>.openai.azure.com/openai/v1/realtime?model=<deployment-name>` ([AOAI WebSockets][aoai-ws]).
- `model` is the deployment name (`AZURE_OPENAI_DEPLOYMENT_NAME` in Learn's quickstart).
- Deployable realtime models run from `gpt-realtime` (2025-08-28) to `gpt-realtime-2.1` and `gpt-realtime-2.1-mini`
  (2026-07-07), plus translate and transcription models ([AOAI WebSockets][aoai-ws]).
- Sessions last at most 60 minutes ([how-to][aoai-howto]).

**Auth.**

- Microsoft Entra (recommended) sends a bearer token. API keys go in an `api-key` header or query parameter. The user
  needs the `Cognitive Services OpenAI User` role ([AOAI WebSockets][aoai-ws]).
- Learn's Python quickstart builds `AsyncOpenAI(websocket_base_url="wss://<resource>.openai.azure.com/openai/v1",
  api_key=...)`. It passes either an Entra token for `https://ai.azure.com/.default` or the API key, so both travel as a
  bearer token ([AOAI WebSockets][aoai-ws]).
- The v1 auth doc says "Tokens must be issued with scope `https://ai.azure.com/.default`" ([Entra ID][entra]). Agent
  Framework's chat clients request `https://cognitiveservices.azure.com/.default` ([`_shared.py`][af-shared-scope]), and
  the accelerator uses that scope for its realtime clients (`core/client_factory.py`).

**Schema and events compared with OpenAI GA.**

- Learn documents one deviation: the transcription `model` must be a deployment name ([AOAI reference][aoai-ref]).
- The GA event reference lists 11 client and 38 server events, all with OpenAI GA names, including MCP and
  `rate_limits.updated` ([classic GA reference][aoai-ref-classic]).
- Sessions accept MCP tools ([how-to][aoai-howto]).
- The accelerator spots content-filter cut-offs through `response.done` `status_details.reason == "content_filter"`
  (`core/orchestration.py`), the field the OpenAI GA spec defines.
- One inconsistency to ignore: the how-to's `session.update` example still shows preview-style flat fields
  ([how-to][aoai-howto]). The GA reference and quickstart are authoritative.

**The `openai` Python SDK against Azure.**

- `AsyncAzureOpenAI.realtime.connect(model=…)` calls `_configure_realtime` ([`azure.py`][oai-azure-py],
  [`realtime.py`][oai-realtime-py]). That builds `wss://<endpoint>/openai/realtime?api-version=<api_version>&deployment=<deployment>`.
  It authenticates with an `api-key` header or an Entra token. With `websocket_base_url` set, it appends `/realtime` but
  still adds `api-version` and `deployment`.
- `api_version` is a required constructor argument ([`azure.py`][oai-azure-py]). Versions 2.25.0, 3.39.0 and 3.42.1 all
  behave this way.
- For non-Azure clients, the URL is `websocket_base_url + "/realtime?model=…"`, and the API key goes in the
  `Authorization` header as a bearer token ([`realtime.py`][oai-realtime-py]). That is how Learn's quickstart reaches the GA route.
- Semantic Kernel 1.45.0's `AzureRealtimeWebsocket.create_session` skips the SDK's URL builder. Its comment explains
  why: "The Azure GA Realtime endpoint (/openai/v1/realtime) does not accept the api-version query parameter. The
  openai SDK always adds it". It builds `wss://<resource>/openai/v1/realtime?model=<deployment>` itself, then sends an
  `api-key` header or an Entra bearer token ([`azure_realtime.py`][sk-azure-rt]).
- In Agent Framework, Chat Completions and embedding clients already switch to `AsyncOpenAI` for Azure `/openai/v1`
  base URLs, and normalise token providers into the async callable `AsyncOpenAI` needs ([`_shared.py`][af-shared-bridge]).
  The Responses client (`OpenAIChatClient`) instead uses `AsyncAzureOpenAI` with base URL `…/openai/v1/` and default
  `api_version="preview"`. Reusing its SDK client for realtime would put `api-version=preview&deployment=…` on the URL
  ([`_shared.py`][af-shared-load], [`_chat_client.py`][af-chat-azure]).
- The SDK warns: "The Azure API shape differs from the core API shape which means that the static types for responses
  / params won't always be correct" ([openai README][oai-readme]).
- Realtime needs the `openai[realtime]` extra, which brings `websockets>=13,<16` ([PyPI][pypi-openai]).

**Accelerator evidence.**

- Its `client_type: realtime` path (`core/client_factory.py`, `config.yaml`) runs on that Semantic Kernel code, so it
  connects to the GA route. Its `api_version: "2025-08-28"` is the `gpt-realtime` model version, not a documented API
  version. It only feeds the `AsyncAzureOpenAI` constructor and never reaches the realtime URL.
- `core/realtime_patches.py` turns every tool result into a string, citing [Python: Bug: realtime service - plugins with
  tools that return non-string responses (i.e. dicts) cause realtime API to stop responding][sk-13003]. The issue shows
  Semantic Kernel never built the `RealtimeFunctionResultEvent` for a dict result. With no `function_call_output` sent,
  the model waited. Every surface requires a string `output` ([OpenAI reference][oai-ref], [VL 2026-07-15][vl-ref],
  [migration guide][fva-migrate]), so this is a framework requirement, not an Azure difference.
- `speak()` (`core/realtime_patches.py`) asks the model to "Say exactly …", because the GA protocol can't speak given
  text word for word. Voice Live can (section 3).

### 3. Azure Voice Live

**Status and versions.**

- Voice Live went GA in November 2025.
- API versions:
  - `2025-10-01` (GA)
  - `2026-01-01-preview`
  - `2026-04-10` (GA)
  - `2026-06-01-preview`
  - `2026-07-15` (GA, and the SDKs' default since July 2026)

  ([release notes][speech-rn], [VL 2026-07-15][vl-ref]).
- "Smart turn detection and WebRTC remain in preview" ([VL 2026-07-15][vl-ref]).

**Endpoint, auth and limits.**

- Endpoint: `wss://<resource>.services.ai.azure.com/voice-live/realtime?api-version=2026-07-15&model=<model>`. Older
  resources use the `cognitiveservices.azure.com` domain ([VL 2026-07-15][vl-ref]).
- Foundry resources are recommended. Speech-only resources don't support Agent Service integration or BYOM.
- Entra tokens go in an `Authorization` header (or as a query parameter in browsers). Use the `https://ai.azure.com/.default`
  scope, or the legacy `https://cognitiveservices.azure.com/.default`, with the `Cognitive Services User` and
  `Foundry User` roles ([VL 2026-07-15][vl-ref]). API keys go in an `api-key` header or query parameter.
- Quota, shared with voice agents: 60 minutes or less per session, 100 new connections a minute (S0), and up to 120,000
  tokens a minute ([quotas][speech-quotas]).
- The accelerator saw an Azure OpenAI `api-version` on the Voice Live endpoint return HTTP 404 (`core/voicelive_patches.py`).

**Models.**

- Model IDs Voice Live hosts itself, with no deployment of your own ([overview][vl-overview]):
  - `gpt-realtime-2.1`, `gpt-realtime-1.5` and `gpt-realtime`, plus `-mini`, `-datazone` and `-regional` variants
  - text models run as a cascade with Azure STT/TTS: GPT-4o, GPT-4.1, GPT-5.x and GPT-6.x
  - `phi4-mm-realtime` (preview)
  - `azure-realtime`, with its own native voices
- Bring your own model (BYOM) adds `profile=` to the URL:
  - `byom-azure-openai-realtime`
  - `byom-foundry-openai-responses`
  - `byom-azure-openai-chat-completion`
  - `byom-foundry-anthropic-messages` (preview)

  `model=` then names your deployment, and `foundry-resource-override=` can point at another resource ([BYOM][vl-byom]).
  The accelerator uses `byom-azure-openai-realtime`.

**How close it is to OpenAI Realtime.** Learn says it's "designed for compatibility with the Azure OpenAI Realtime API",
events "mostly match", and Azure features are "optional and additive" ([overview][vl-overview], [how-to][vl-howto]). On
the wire, though, it follows the preview shape:

- The session is flat: `modalities`, a `voice` object, `input_audio_format` / `output_audio_format` strings,
  `input_audio_sampling_rate`, `input_audio_transcription`, `turn_detection` and `max_response_output_tokens`. There is
  no `session.type`.
- Server events use preview names: `response.text.*`, `response.audio.*` and `response.audio_transcript.*`.

This holds for `2026-07-15` ([VL 2026-07-15][vl-ref]) and `2025-10-01` ([VL 2025-10-01][vl-ref-1001]). The 1.3.1 SDK's
`ServerEventType` enum agrees ([`_enums.py`][vl-enums]). The accelerator's three Voice Live patches
(`core/voicelive_patches.py`) exist because of this mismatch:

- the URL and api-version
- renaming `response.audio.*` to GA names; otherwise Semantic Kernel and the GA `openai` client "silently" dropped every
  audio delta
- sending `session.update` as a raw dict. Voice Live `2025-10-01` rejected the `session.type` that the SDK model injects
  ("'session.type' unexpected (extra fields not permitted)") and closed the connection after five rejections.

**Behaviour that differs from OpenAI.**

- `session.update` "can be sent at any time to modify settings such as voice …". Only the model is fixed
  ([VL 2026-07-15][vl-ref]). The accelerator's in-session handoffs change voice mid-call on Voice Live.
- Content filtering is always on and can't be changed; BYOM is the route to custom filtering ([FAQ][vl-faq]).
- `function_call_output.output` is a string ([VL 2026-07-15][vl-ref]).

**Azure-specific session fields (`2026-07-15`)**, from [VL 2026-07-15][vl-ref] unless noted:

| Field | Values and options |
| --- | --- |
| `turn_detection` | `server_vad`, extended with `speech_duration_ms`, `end_of_utterance_detection`, `interrupt_response` and `auto_truncate`. `semantic_vad` (OpenAI's, `gpt-realtime` and `-mini` only, with `eagerness`). `azure_semantic_vad` and `azure_semantic_vad_multilingual`: `threshold`, `prefix_padding_ms`, `silence_duration_ms`, `speech_duration_ms`, `remove_filler_words`, `languages`, `end_of_utterance_detection` (`semantic_detection_v1[_en\|_multilingual]`, `threshold_level`, `timeout_ms`), `create_response`, `interrupt_response`, `auto_truncate`. `appended_text_after_truncation` is JSON-only ([auto-truncation][vl-trunc]). |
| `input_audio_noise_reduction` | `near_field` / `far_field` (OpenAI's, `gpt-realtime` and `-mini` only); `azure_deep_noise_suppression`; `azure_deep_noise_suppression_headset`; `azure_personal_noise_suppression`. |
| `input_audio_echo_cancellation` | `server_echo_cancellation`, with `reference_source` (`server` or `client`) and `channels` (1 or 2), for client-supplied playback reference ([how-to][vl-howto]). |
| Audio formats | Input `pcm16` (16 or 24 kHz), `g711_ulaw`, `g711_alaw`. Output `pcm16` (24 kHz), `pcm16_8000hz`, `pcm16_16000hz` and G.711. |
| `voice` | An object, typed `openai`, `azure-standard`, `azure-custom` (`endpoint_id`), `azure-personal` (`model`) or `azure-realtime-native`. Azure types take `temperature`, `rate`, `pitch`, `volume`, `style`, `locale`, `prefer_locales`, `custom_lexicon_url` and `custom_text_normalization_url`. Covers HD voices (`en-US-Ava:DragonHDLatestNeural`) and MAI-Voice-2-Flash (preview) ([how-to][vl-howto]). |
| `input_audio_transcription` | Model *names*, not deployments: `azure-speech`, `mai-transcribe`, `mai-transcribe-2`, `mai-transcribe-2-streaming`, `whisper-1`, `gpt-4o-transcribe`, `gpt-4o-mini-transcribe` and `gpt-4o-transcribe-diarize` (the OpenAI ones only with `gpt-realtime` and `-mini`). Plus `language`, `phrase_list`, `custom_speech` and `prompt` ([how-to][vl-howto]). |
| `animation`, `output_audio_timestamp_types` | `outputs: ["blendshapes", "viseme_id"]`; `["word"]` word timestamps ([how-to][vl-howto]). |
| `avatar`, `modalities` | Video or photo avatar over WebRTC or websocket; `modalities` can include `animation` and `avatar`. |
| `interim-response` | Static or LLM-generated filler, triggered by `latency` or `tool`. |
| Others | `parallel_tool_calls`, `reasoning_effort`, `metadata`, `temperature` (0.6 to 1.2), `max_response_output_tokens`. |
| `tools` | `function`; `mcp` (with `require_approval`); `foundry_agent`, which consults a Foundry agent as a tool. |
| `response.create` | `pre_generated_assistant_message`: speaks the given text verbatim and adds it to history. |

**Events added or renamed (`2026-07-15`)** ([VL 2026-07-15][vl-ref], [migration guide][aoai-mig]):

- Renamed relative to GA: `response.text.*` ↔ `response.output_text.*`, `response.audio.*` ↔ `response.output_audio.*`,
  and `response.audio_transcript.*` ↔ `response.output_audio_transcript.*`.
- New client events: `session.avatar.connect`, `input_text.delta` and `input_text.done` (streamed text input), and
  `output_audio_buffer.clear` (avatar mode only).
- New server events:
  - `warning`
  - `session.avatar.connecting` and `session.avatar.switch_to_speaking` / `switch_to_idle`
  - `response.animation_blendshapes.*`, `response.animation_viseme.*`, `response.audio_timestamp.*` and
    `response.video.delta`
  - `response.foundry_agent_call*`, `response.web_search_call.*`, `response.file_search_call.*`
  - `response.audio_transcript.annotation.added` and `response.invocation.delta`
- Missing from the reference and the SDK enum: the GA-only `conversation.item.added` / `.done` and
  `input_audio_buffer.timeout_triggered`. The SDK removed `output_audio_buffer.started` / `stopped` from its GA release
  ([CHANGELOG][vl-changelog]).

**The `azure-ai-voicelive` Python SDK.**

- **Releases.** Seventeen since 1.0.0b1 (2025-08-29): 1.0.0 on 2025-10-01, 1.2.0 on 2026-05-22, 1.3.0 on 2026-08-03
  and 1.3.1 on 2026-10-23. It's classified "Production/Stable" and lives in `sdk/voicelive/azure-ai-voicelive`
  ([PyPI][pypi-vl], [CHANGELOG][vl-changelog]).
- **Dependencies.** `azure-core>=1.37.0`, `isodate` and `typing-extensions`. `aiohttp` is declared as an optional
  `[aiohttp]` extra, but `azure.ai.voicelive.aio` raises `ImportError` without it ([PyPI][pypi-vl],
  [`aio/_patch.py`][vl-patch-import]).
- **API shape.** `azure.ai.voicelive.aio.connect(...)` returns an async context manager that yields a
  `VoiceLiveConnection` ([`aio/_patch.py`][vl-patch-connect]):
  - Parameters: `credential` (`AzureKeyCredential` or `AsyncTokenCredential`), `endpoint`,
    `api_version="2026-07-15"`, `model`, the agent-integration arguments, `query`, `headers`,
    `connection_options` and `credential_scopes`.
  - The connection mirrors the `openai` client: `.session.update`, `.response.create` / `.cancel`,
    `.input_audio_buffer.*`, `.conversation.item.*` and `.output_audio_buffer.clear`.
  - `send()` takes a typed event or a raw `Mapping`, and iterating the connection yields typed `ServerEvent`s.
  - It builds `<endpoint>/voice-live/realtime?api-version=…&x-ms-client-sdk=…&model=…`, then authenticates with an
    `api-key` header or a bearer token. The default scope is `https://ai.azure.com/.default`.
- **Maintenance.** From the [CHANGELOG][vl-changelog]:
  - Three of the last four releases changed the default API version: 1.2.0 to `2026-04-10` (under Other Changes),
    1.3.0b1 to `2026-06-01-preview` and 1.3.0 to `2026-07-15` (both under Breaking Changes).
  - Fields were renamed: `RequestImageContentPart.url` became `image_url`, and `AvatarConfig.type` became
    `avatar_type`.
  - 1.2.0 removed `FoundryAgentTool` and friends.
  - The 1.3.0 GA release dropped preview features (WebRTC, smart end-of-turn).
  - It lags the service: Learn says `appended_text_after_truncation` has no SDK support yet and shows a raw dict
    instead ([auto-truncation][vl-trunc]).
  - 1.2.0 added an opt-in `VoiceLiveInstrumentor` for OpenTelemetry.
- **Earlier attempt.** [Python: Add realtime voice agents with OpenAI, Azure OpenAI and Voice Live clients][pr3821]
  wrapped this SDK in a new `agent-framework-azure-voice-live` package. An automated review noted the client stored an
  `api_version` it never passed to the SDK ([review comment][pr3821-apiversion]).

**Wrap the SDK, or speak raw websocket?**

| Aspect | Wrap `azure-ai-voicelive` | Raw websocket |
| --- | --- | --- |
| New dependencies | `azure-core`, `isodate`, `aiohttp` | None: `websockets` already comes with `openai[realtime]`, and `aiohttp` is already in `agent-framework-foundry` ([foundry `pyproject.toml`][af-foundry-pyproject]) |
| Version caps | Must bound an SDK whose minor releases ship breaking changes | Transport only |
| API version | The SDK default moves under us unless pinned | Agent Framework pins it explicitly |
| Feature lag | Typed models trail the service; the raw-dict send covers gaps | None, if options pass unknown keys through |
| Schema ownership | The SDK owns it | Agent Framework owns TypedDicts, which can drift. The accelerator's end-of-utterance model uses `threshold`/`timeout`, but the reference says `threshold_level`/`timeout_ms` ([VL 2025-10-01][vl-ref-1001]); it's unused today |
| Event typing | `ServerEvent` models, separate from `openai`'s, so a second mapper | Raw JSON, so one neutral mapper plus a preview-to-GA rename table |
| Coverage | Voice Live only; can't reach the Foundry voice-agent route | One transport covers Voice Live and Foundry voice agents, as the accelerator shows |
| Telemetry | `VoiceLiveInstrumentor` (opt-in) | The Agent Framework telemetry layer only |
| Auth | Built-in `AzureKeyCredential` / `AsyncTokenCredential` | Reuse the credential-to-token helpers in `agent-framework-openai` ([`_shared.py`][af-shared-cred]) |

**Not to be confused with Foundry voice agents: Voice Live with Foundry Agent Service.** That older integration adds
`agent-name` and `agent-project-name` to the Voice Live URL ([VL 2026-07-15][vl-ref], [`aio/_patch.py`][vl-patch-connect]).
Voice Live then wraps a *text* agent, which runs its own tools, and `instructions` isn't supported ([how-to][vl-howto]).
It went GA in April 2026 ([release notes][speech-rn]), and Learn now has a guide for migrating off it to Foundry voice
agents ([migration guide][fva-migrate]). The accelerator doesn't use it.

### 4. Foundry voice agents

**What they are.**

- A Foundry agent whose `kind` is `voice`. Each create or update makes an immutable version ([configure][fva-config]).
- The definition holds:
  - `model_type` (`managed` or `self_deployed`) and `model`
  - `instructions`, `greeting` and `structured_inputs`
  - `audio.input` / `audio.output` and `output_modalities`
  - `tools`, `tool_choice`, `parallel_tool_calls` and `interim_response`
  - `store` (off by default), plus avatar settings
- Speech-to-speech or cascaded processing follows from the chosen model ([configure][fva-config],
  [overview][fva-overview]).

**Status.** "Voice-based agents and some related … capabilities are in preview" ([overview][fva-overview]). The preview
opt-ins are:

- the `Foundry-Features: VoiceAgents=V1Preview` header on REST calls
- `allow_preview=True` on `AIProjectClient` for `.agents` calls with voice definitions
- `api-version=v1`

([migration guide][fva-migrate], [configure][fva-config]).

**Endpoint and auth.**

- Route: `wss://<account>.services.ai.azure.com/api/projects/<project>/agents/<agent-name>/endpoint/protocols/voice?api-version=v1`.
- `x-agent-version-override` pins a version, and a `store` query parameter overrides storage for one session.
- Auth is an Entra token for `https://ai.azure.com/.default`. "Resource API keys aren't a substitute for agent
  authentication." ([migration guide][fva-migrate], [configure][fva-config])
- The SDK's handshake sends a bearer token in the `Authorization` header and `Foundry-Features: VoiceAgents=V1Preview`, over
  the `realtime` subprotocol. It may add `agent_session_id`, `structured_input` and `transport` ([`aio/_realtime.py`][proj-rt-url]).
- The accelerator's spike connected with a plain websocket, no SDK (`docs/superpowers/specs/2026-10-21-foundry-voice-agents-design.md`).

**Protocol.**

- Event names are GA: `response.output_audio.*`, `response.output_audio_transcript.*` and `response.output_text.*`.
  Input, response-control and function events keep their names ([migration guide][fva-migrate]).
- `session.update` uses the GA nested layout: `session.type: "realtime"`, `audio.input.format` as
  `{"type": "audio/pcm", "rate": 24000}`, and so on ([migration guide][fva-migrate]).
- Voice Live settings move into that layout ([migration guide][fva-migrate]):

  | Voice Live | Foundry voice agent |
  | --- | --- |
  | `voice.name` | `audio.output.voice` (documented as a string) |
  | `voice.type` | `audio.output.voice_type` |
  | `voice.temperature` | `audio.output.voice_temperature` |
  | `input_audio_transcription` | `audio.input.transcription` |
  | `turn_detection` | `audio.input.turn_detection` |
  | `input_audio_noise_reduction` | `audio.input.noise_reduction` |
  | `input_audio_echo_cancellation` | `audio.input.echo_cancellation` |
  | `modalities` | `output_modalities` |

- The SDK's `VoiceAgentSessionUpdateConfig` has no `model`. It accepts `instructions`, `tools`, `tool_choice`, `audio`,
  `interim_response`, `greeting`, `reasoning` and more (`azure.ai.projects.models`, 2.10.0).
- What the accelerator's probes found (`core/foundry_voice.py`, `core/foundry_voice_client.py` and the design doc):
  - A flat Voice Live update is rejected (`'session.voice' unexpected`).
  - `audio.output.voice` accepted the Voice Live voice *object* (`name`, `type`, `temperature`, `rate`).
  - The turn-detection type can't change mid-session (`turn_detection_type_change_not_allowed`).
  - `animation` is accepted, but no viseme events arrive.
  - Switching from an Azure voice to an OpenAI voice is rejected.
  - In one preview quirk, `session.created` reported `marin` while the configured Azure voice spoke.

**Instructions and tools.**

- On the server: "The agent owns the model, instructions, voice, turn detection, and noise suppression on the server,
  so the client doesn't send a `session.update` event" ([quickstart][fva-quick]).
- The definition's values are session defaults, which a client may override in supported fields ([configure][fva-config]).
- The accelerator replaced instructions, tools and voice several times within one session, and the changes applied
  only to that session (spike).

**Tool execution** ([configure][fva-config]):

- `function` tools run in your client. `mcp`, `toolbox` and `system` tools (`end_conversation`) run in the service.
  `response_scheduling` decides when a service tool's result is spoken.
- The client loop is the GA one: on `response.function_call_arguments.done`, execute and send `conversation.item.create`
  with a string-valued `function_call_output`, then `response.create` ([migration guide][fva-migrate]).
- The accelerator measured app-run tools at about 0.8 to 0.9 seconds to first audio. A consulted subagent couldn't use
  app-run tools (design doc).

**Other surface.**

- `greeting` (`template` or `llm_generated`) handles speech at session start ([configure][fva-config]).
- The SDK models `pre_generated_assistant_message` on `VoiceAgentResponseCreateParams`
  (`azure.ai.projects.models`, 2.10.0); whether the service honours it is unverified.
- `session.created` carries `conversation_id` ([quickstart][fva-quick]).
- Foundry traces the voice pipeline ([migration guide][fva-migrate]). The accelerator injects W3C `traceparent` into the
  handshake so those spans join the call's trace (`core/foundry_voice_client.py`).

**Python SDK.**

- Install `azure-ai-projects[voice]>=2.7.0` ([migration guide][fva-migrate]). Version 2.7.0 (2026-09-18) added preview
  voice agents, including sync and async `.beta.voice_agents.realtime.connect()` ([CHANGELOG][proj-changelog]).
- The `voice` extra adds `websockets>=13` and `aiohttp`. The package itself requires `openai>=3.0.0`
  ([`pyproject.toml`][proj-pyproject], [PyPI][pypi-proj]).
- The async client is hand-written over aiohttp. It returns typed `RealtimeServerEvent*` / `VoiceAgent*` models with a
  dict fallback, and its audio deltas are already-decoded bytes ([`aio/_realtime.py`][proj-rt-url],
  [migration guide][fva-migrate]).
- The preview surface churns: 2.8.0 and 2.9.0 both shipped breaking changes, each noting "All breaking changes affect
  preview APIs" ([CHANGELOG][proj-changelog]).
- Agent Framework pins `azure-ai-projects>=2.2.0,<2.8.0` in `agent-framework-foundry`
  ([`pyproject.toml`][af-foundry-pyproject]), while 2.10.0 is out ([PyPI][pypi-proj]). Using the SDK client means a
  2.7 floor and lifting the cap.

### 5. Agent Framework placement today

**What ADR 0021 prescribes** ([ADR 0021][adr0021]):

- A lightweight core.
- Provider-leading names.
- An `agent-framework-openai` package that depends only on core, `openai` and `packaging`.
- One `model` parameter that "naturally maps … to an Azure OpenAI deployment name, making it straightforward to use
  `OpenAIChatClient` with either OpenAI or Azure OpenAI backends".
- Azure wrappers deprecated into `azure-ai`.
- `FoundryChatClient` built on `RawFoundryChatClient(RawOpenAIChatClient)`.
- `FoundryAgent` (option B) for agents that already exist in Foundry, with `RawFoundryAgentChatClient` as the extension
  point.
- Its namespace intent: `agent_framework.openai` for OpenAI, and `agent_framework.azure` for "Foundry, Azure AI, and
  other Azure-specific classes".

**What `main` holds today** ([`f4bdd1d`][af-sha]):

- **`agent-framework-openai` 1.18.0 (released)** ships `OpenAIChatClient`, `OpenAIChatCompletionClient`,
  `OpenAIEmbeddingClient` and their `Raw*` versions. Its only dependencies are core and `openai>=2.25.0,<4`
  ([`pyproject.toml`][af-openai-pyproject]). Azure routing ([`_chat_client.py`][af-chat-azure],
  [`_shared.py`][af-shared-load], [README][af-openai-readme]):
  - `azure_endpoint`, `credential` or an `AsyncAzureOpenAI` client selects Azure. Without an OpenAI key, the client
    falls back to the `AZURE_OPENAI_*` variables. Explicit Azure inputs win.
  - `credential` becomes a token provider for the `cognitiveservices` scope; `azure-identity` is imported lazily.
  - Chat Completions and embedding clients send `/openai/v1` base URLs through `AsyncOpenAI`. Everything else, including
    the Responses client at `…/openai/v1/`, goes through `AsyncAzureOpenAI`.

  Two upstream PRs settled this: [[BREAKING] Python: fix OpenAI Azure routing and provider samples][pr4925] and
  [Python: [BREAKING] Remove deprecated Python OpenAI/Azure AI surfaces][pr4990]. The second removed the `AzureOpenAI*`
  classes.
- **`agent-framework-azure-ai`** is deprecated. Its classes "were renamed, sometimes changed, and moved to
  `agent-framework-foundry`" ([PACKAGE_STATUS][af-pkg-status], [Python: [BREAKING] Python: move Azure AI embeddings to
  Foundry][pr5056]).
- **`agent-framework-foundry` 1.16.0 (released)** ships ([`_chat_client.py`][af-foundry-chat],
  [`_agent.py`][af-foundry-agent], [`pyproject.toml`][af-foundry-pyproject]):
  - `FoundryChatClient` and `RawFoundryChatClient`, which use the Responses API through
    `AIProjectClient.get_openai_client()`
  - `FoundryAgent`, `RawFoundryAgent` and `RawFoundryAgentChatClient`, which connect to existing prompt or hosted
    agents by `agent_name` and `agent_version`, with `allow_preview`
  - `FoundryEmbeddingClient`, `FoundryMemoryProvider` and `FoundryEvals`

  It depends on `agent-framework-openai`, `aiohttp` and `azure-ai-projects<2.8.0`.
- **Namespaces drifted from ADR 0021.**
  - `agent_framework.foundry` re-exports `agent-framework-foundry`, plus `AnthropicFoundryClient` from the Anthropic
    package, Foundry hosting and Foundry Local. It also re-exports `agent-framework-azure-contentunderstanding`, a
    separate beta package that wraps the `azure-ai-contentunderstanding` SDK ([`foundry/__init__.py`][af-ns-foundry],
    [contentunderstanding `pyproject.toml`][af-cu-pyproject]).
  - `agent_framework.azure` now holds only AI Search, Cosmos and Durable ([`azure/__init__.py`][af-ns-azure]).
- **No realtime code** exists in `python/packages` today.

**Where realtime clients would naturally sit** (analysis):

- **Azure OpenAI Realtime** belongs on ADR 0035's `OpenAIRealtimeClient` as Azure routing, in `agent-framework-openai`.
  That matches how chat works since [Python: [BREAKING] Python: Provider-leading client design & OpenAI package
  extraction][pr4818]. It needs a GA URL builder (`AsyncOpenAI` with `websocket_base_url=".../openai/v1"`), not
  `AsyncAzureOpenAI`'s legacy route.
- **Foundry voice agents** belong in `agent-framework-foundry`, mirroring `FoundryAgent` and
  `RawFoundryAgentChatClient`: project endpoint, credential, `agent_name`, `agent_version`, `allow_preview`. The protocol
  is GA, so the client can reuse the GA mapping from `agent-framework-openai`, which is already a dependency.
- **Azure Voice Live** belongs under `agent_framework.foundry`. It could live inside `agent-framework-foundry` (raw
  websocket needs no new dependencies) or in a new beta package (if it wraps `azure-ai-voicelive`; Content
  Understanding is the precedent). It doesn't belong in `agent-framework-openai`: it's a different service with a
  different schema, and ADR 0021 keeps that package free of Azure SDK dependencies.
- [Python: Add realtime voice agents with OpenAI, Azure OpenAI and Voice Live clients][pr3821] predates ADR 0021. It put
  an `AzureOpenAIRealtimeClient` in core's `azure` namespace, which conflicts with today's Azure routing.

## Delta table

| Aspect | OpenAI GA | Azure OpenAI (GA) | Azure Voice Live (`2026-07-15`) | Foundry voice agents (preview) |
| --- | --- | --- | --- | --- |
| Status | GA | GA; preview deprecated 2026-04-30 | GA service (Nov 2025); API `2026-07-15` GA; smart EOT and WebRTC preview | Preview |
| WebSocket URL | `wss://api.openai.com/v1/realtime?model=` | `wss://<res>.openai.azure.com/openai/v1/realtime?model=<deployment>` | `wss://<res>.services.ai.azure.com/voice-live/realtime?api-version=…&model=<id>` | `wss://<acct>.services.ai.azure.com/api/projects/<p>/agents/<agent>/endpoint/protocols/voice?api-version=v1` |
| API version | None | None; sending one gives 401 | Required, dated | `v1` plus the preview header |
| Model selection | Model name | Deployment name | Hosted model ID; BYOM `profile=` with a deployment | In the definition (`managed` model or `self_deployed` deployment); pin with `x-agent-version-override` |
| API key | Sent as a bearer token | `api-key` header or query (Learn's SDK sample sends it as a bearer token) | `api-key` header or query | Not accepted |
| Entra | n/a | Bearer token; scope `ai.azure.com` (docs) or `cognitiveservices` (Agent Framework chat, accelerator) | Bearer token in the `Authorization` header, or an `Authorization` query parameter; `ai.azure.com` or legacy `cognitiveservices` | Bearer token; `ai.azure.com` only |
| Extra handshake | None | None (no `OpenAI-Beta`) | None | `Foundry-Features: VoiceAgents=V1Preview`; subprotocol `realtime` |
| Session layout | Nested `audio.input` / `audio.output`, `session.type` | Same as OpenAI | Flat preview fields; `session.type` rejected (accelerator, `2025-10-01`) | Nested GA layout with Voice Live extensions; `session.type` required; no `model` |
| Audio, text and transcript events | `response.output_audio.*`, `output_text.*`, `output_audio_transcript.*` | Same as OpenAI | `response.audio.*`, `response.text.*`, `response.audio_transcript.*` | GA names |
| Voice | Built-in name or custom `{id}`; can't change after the first audio | Same as OpenAI (custom voices on Azure unverified) | Object (`openai` / `azure-standard` / `azure-custom` / `azure-personal` / `azure-realtime-native`) with rate, temperature, pitch, volume, style, locale, lexicon, `endpoint_id`; changeable mid-session | Definition: `voice` string plus `voice_type` and fields. `session.update` took the Voice Live object; switches within a family work, Azure to OpenAI is rejected (accelerator) |
| Turn detection | `server_vad`, `semantic_vad` | Same as OpenAI | Adds `azure_semantic_vad[_multilingual]`, EOU, `remove_filler_words`, `auto_truncate`, `appended_text_after_truncation` | Adds `azure_semantic_vad[_en\|_multilingual]` (EOU, filler-word removal); type fixed for the session (accelerator) |
| Noise suppression | `near_field`, `far_field` | Same as OpenAI | Adds `azure_deep_noise_suppression[_headset]` and `azure_personal_noise_suppression` | `near_field`, `far_field`, `azure_deep_noise_suppression` (plus headset and personal in SDK 2.10.0) |
| Echo cancellation | None | None | `server_echo_cancellation` (server or client reference) | `echo_cancellation` (server or client reference) |
| Transcription `model` | Model name | Deployment name | Model name: `azure-speech`, `mai-transcribe*`, `whisper-1`, `gpt-4o-*`; `phrase_list`, `custom_speech` | Model name; `phrase_list`, `custom_speech` |
| Visemes, timestamps, avatar | No | No | Yes | Avatar and timestamps in the definition; visemes accepted but not emitted (accelerator) |
| Verbatim speech | No (prompt the model) | No | `pre_generated_assistant_message` | `greeting`; SDK models `pre_generated_assistant_message` (unverified) |
| Instructions and tools | Client session | Client session | Client session | Agent definition; client may override per session |
| Tool execution | Client functions; server MCP | Same as OpenAI | Client functions; server MCP and `foundry_agent` | Client functions; service MCP, toolbox, system |
| Function result | `output` string | String | String | String |
| Content filter | `status_details.reason = content_filter` | Same field; Azure filtering | Always on, can't be disabled (BYOM for custom) | Guardrails configurable on the agent |
| Extra events | n/a | None documented | `warning`, `animation_*`, `audio_timestamp.*`, avatar and video, `foundry_agent_call*`, web and file search, `input_text.*` | Voice Live animation, avatar and timestamp events under GA naming (SDK event map) |
| Max session | Not checked | 60 minutes | 60 minutes or less (quota) | Shares the Voice Live quota (60 minutes or less) |
| Python SDK | `openai[realtime]` | `openai[realtime]` via `AsyncOpenAI(websocket_base_url=…/openai/v1)` | `azure-ai-voicelive` 1.3.1 (GA), or raw websocket | `azure-ai-projects[voice]>=2.7.0` `.beta.voice_agents.realtime` (preview), or raw websocket |

## Implications for the map

1. **For [Azure provider clients: design and package placement][t8]:**
   - Azure OpenAI gets Azure routing on `OpenAIRealtimeClient`, not a new class. Watch the URL: use `AsyncOpenAI` with
     `websocket_base_url`, not `AsyncAzureOpenAI`.
   - Voice Live needs its own client under `agent_framework.foundry`, plus a decision on SDK versus raw websocket. The
     evidence leans raw.
   - Foundry voice agents can be first-class at low cost, as a GA-protocol variant in `agent-framework-foundry`
     modelled on `FoundryAgent`.
   - The core protocol needs these extension points whatever is decided:
     - a connection factory covering URL and query, headers, subprotocol and credential scope
     - a session-payload hook
     - an event-name normalisation table
     - raw-event pass-through on `send(RealtimeEvent)`
     - provider options that pass unknown keys through
2. **ADR 0035's options can't express Azure settings.** `RealtimeSessionOptions` (`voice: str`, string formats) has no
   room for Azure voice objects or Voice Live audio settings. Per-provider TypedDict options, as chat has, plus a GA
   mapper that nests the audio settings, would cover it.
3. **The event model** needs per-provider capabilities. [Accelerator voice-path capability checklist vs ADR 0035][t2]
   proposed two tickets for it: "Realtime event taxonomy" and "Live conversation controls".
   - Voice Live's preview event names need normalising to GA. The content-filter field is common to all four providers.
     Voice Live adds `warning`, animation and timestamp events.
   - Verbatim speech exists only on Voice Live and Foundry voice agents. Voice can change mid-session only on Voice Live
     and Foundry voice agents (within a family).
4. **The handoff decision** ([Handoff model for realtime agents][t7]) can't promise a voice change on OpenAI or Azure
   OpenAI after the first audio. On Foundry voice agents the turn-detection type is fixed for the session.
5. **Tool execution** (the proposed "Realtime tool execution contract" ticket): every provider needs string results.
   Server-executed tools also need surfacing, without Agent Framework executing them: MCP on all four providers, and
   `foundry_agent`, toolbox and system tools on some. MCP approvals use `mcp_approval_request` and
   `mcp_approval_response`.
6. **Telemetry** ([Middleware and telemetry for full-duplex realtime traffic][t5]): Foundry voice agents join the
   caller's trace when the handshake carries W3C `traceparent`. Both Azure SDKs ship their own OpenTelemetry
   instrumentors, which could double up if Agent Framework wraps them.
7. **A small wire probe would settle open questions 2 to 4** before [Azure provider clients: design and package
   placement][t8] fixes its mappers. It only needs an Azure OpenAI resource, a Voice Live resource and a Foundry
   project.

## Open questions

1. Is the legacy route the `openai` SDK builds (`/openai/realtime?api-version=…&deployment=…`) still served for GA
   models? This only matters if Agent Framework wanted to reuse `AsyncAzureOpenAI`; Learn and Semantic Kernel both
   avoid it.
2. Does Azure OpenAI realtime accept `https://cognitiveservices.azure.com/.default` tokens, Agent Framework's chat
   scope, as well as `https://ai.azure.com/.default`?
3. Does Voice Live `2026-07-15` still reject `session.type` and the GA nested layout? Will Voice Live ever emit GA
   event names? No document says either way.
4. Foundry voice agents:
   - Is `audio.output.voice` a string (docs) or an object (accelerator probe) in `session.update`?
   - Does the service honour `pre_generated_assistant_message`?
   - Which fields can a session override?
   - What changes at GA (the `Foundry-Features` header, `v1`)?
5. Do OpenAI-only GA features, such as custom voices `{id}`, `prompt`, `tracing` and `truncation`, work on Azure
   OpenAI realtime?
6. Design smells in the accelerator to raise separately:
   - Its end-of-utterance model uses `threshold`/`timeout`, but the documented fields are `threshold_level`/`timeout_ms`;
     it's not sent today.
   - It sends `eagerness` with `azure_semantic_vad`, where docs list `eagerness` for `semantic_vad` only.
   - Its Azure OpenAI `api_version: "2025-08-28"` is a model version labelled as the "Realtime GA value". Semantic
     Kernel never sends it on the realtime URL.

[ticket]: https://github.com/adamdougal/agent-framework/issues/4
[map]: https://github.com/adamdougal/agent-framework/issues/1
[t2]: https://github.com/adamdougal/agent-framework/issues/2
[t5]: https://github.com/adamdougal/agent-framework/issues/5
[t7]: https://github.com/adamdougal/agent-framework/issues/7
[t8]: https://github.com/adamdougal/agent-framework/issues/8
[adr0035]: https://github.com/eavanvalkenburg/agent-framework/blob/python-realtime-agent-adr/docs/decisions/0035-python-realtime-agent.md
[oai-ref]: https://developers.openai.com/api/reference/resources/realtime
[aoai-mig]: https://learn.microsoft.com/azure/foundry/openai/how-to/realtime-audio-preview-api-migration-guide
[aoai-ws]: https://learn.microsoft.com/azure/foundry/openai/how-to/realtime-audio-websockets
[aoai-howto]: https://learn.microsoft.com/azure/foundry/openai/how-to/realtime-audio
[aoai-ref]: https://learn.microsoft.com/azure/foundry/openai/realtime-audio-reference
[aoai-ref-classic]: https://learn.microsoft.com/azure/foundry-classic/openai/realtime-audio-reference-ga
[entra]: https://learn.microsoft.com/azure/foundry/foundry-models/how-to/configure-entra-id
[vl-overview]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live
[vl-howto]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-how-to
[vl-ref]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15
[vl-ref-1001]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2025-10-01
[vl-faq]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-faq
[vl-byom]: https://learn.microsoft.com/azure/ai-services/speech-service/how-to-bring-your-own-model
[vl-trunc]: https://learn.microsoft.com/azure/ai-services/speech-service/how-to-voice-live-auto-truncation
[speech-rn]: https://learn.microsoft.com/azure/ai-services/speech-service/releasenotes
[speech-quotas]: https://learn.microsoft.com/azure/ai-services/speech-service/speech-services-quotas-and-limits
[fva-migrate]: https://learn.microsoft.com/azure/foundry/agents/how-to/migrate-from-voice-live
[fva-config]: https://learn.microsoft.com/azure/foundry/agents/how-to/configure-voice-agent
[fva-quick]: https://learn.microsoft.com/azure/foundry/agents/quickstarts/prompt-voice-agent
[fva-overview]: https://learn.microsoft.com/azure/foundry/agents/overview#how-voice-based-agents-work
[oai-tag-225]: https://github.com/openai/openai-python/tree/v2.25.0
[oai-tag-339]: https://github.com/openai/openai-python/tree/v3.39.0
[oai-tag-3421]: https://github.com/openai/openai-python/tree/v3.42.1
[oai-azure-py]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/lib/azure.py#L905-L940
[oai-realtime-py]: https://github.com/openai/openai-python/blob/v3.42.1/src/openai/resources/realtime/realtime.py#L662-L751
[oai-readme]: https://github.com/openai/openai-python/blob/v3.42.1/README.md#microsoft-azure-openai
[pypi-openai]: https://pypi.org/project/openai/3.42.1/
[vl-tag]: https://github.com/Azure/azure-sdk-for-python/tree/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive
[vl-patch-import]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/aio/_patch.py#L22-L28
[vl-patch-connect]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/aio/_patch.py#L678-L1012
[vl-enums]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/azure/ai/voicelive/models/_enums.py#L403-L520
[vl-changelog]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive/CHANGELOG.md
[pypi-vl]: https://pypi.org/project/azure-ai-voicelive/#history
[proj-tag-27]: https://github.com/Azure/azure-sdk-for-python/tree/azure-ai-projects_2.7.0/sdk/ai/azure-ai-projects
[proj-tag-210]: https://github.com/Azure/azure-sdk-for-python/tree/azure-ai-projects_2.10.0/sdk/ai/azure-ai-projects
[proj-rt-url]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-projects_2.10.0/sdk/ai/azure-ai-projects/azure/ai/projects/aio/_realtime.py#L272-L288
[proj-pyproject]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-projects_2.10.0/sdk/ai/azure-ai-projects/pyproject.toml#L45-L49
[proj-changelog]: https://github.com/Azure/azure-sdk-for-python/blob/azure-ai-projects_2.10.0/sdk/ai/azure-ai-projects/CHANGELOG.md
[pypi-proj]: https://pypi.org/project/azure-ai-projects/#history
[sk-13003]: https://github.com/microsoft/semantic-kernel/issues/13003
[sk-azure-rt]: https://github.com/microsoft/semantic-kernel/blob/python-1.45.0/python/semantic_kernel/connectors/ai/open_ai/services/azure_realtime.py#L151-L191
[pr3821]: https://github.com/microsoft/agent-framework/pull/3821
[pr3821-apiversion]: https://github.com/microsoft/agent-framework/pull/3821#discussion_r2790024547
[pr4818]: https://github.com/microsoft/agent-framework/pull/4818
[pr4925]: https://github.com/microsoft/agent-framework/pull/4925
[pr4990]: https://github.com/microsoft/agent-framework/pull/4990
[pr5056]: https://github.com/microsoft/agent-framework/pull/5056
[af-sha]: https://github.com/microsoft/agent-framework/tree/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f
[adr0021]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/docs/decisions/0021-provider-leading-clients.md
[af-openai-pyproject]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/openai/pyproject.toml#L25-L28
[af-shared-scope]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/openai/agent_framework_openai/_shared.py#L31
[af-shared-load]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/openai/agent_framework_openai/_shared.py#L246-L423
[af-shared-bridge]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/openai/agent_framework_openai/_shared.py#L394-L441
[af-shared-cred]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/openai/agent_framework_openai/_shared.py#L444-L470
[af-chat-azure]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/openai/agent_framework_openai/_chat_client.py#L685-L733
[af-openai-readme]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/openai/README.md#L81-L131
[af-pkg-status]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/PACKAGE_STATUS.md#L75
[af-foundry-pyproject]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/foundry/pyproject.toml#L25-L32
[af-foundry-chat]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/foundry/agent_framework_foundry/_chat_client.py#L149
[af-foundry-agent]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/foundry/agent_framework_foundry/_agent.py#L154-L287
[af-ns-foundry]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/foundry/__init__.py
[af-ns-azure]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/azure/__init__.py
[af-cu-pyproject]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/azure-contentunderstanding/pyproject.toml
