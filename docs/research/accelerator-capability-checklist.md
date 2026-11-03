# Accelerator voice-path capability checklist vs ADR 0035

Research for [Accelerator voice-path capability checklist vs ADR 0035](https://github.com/adamdougal/agent-framework/issues/2)
on the map [Python realtime agents in Agent Framework](https://github.com/adamdougal/agent-framework/issues/1).

## Question

Which capabilities does the AI Contact Centre accelerator's voice path rely on, and for each one, does ADR 0035 (final
revision) already cover it, partly cover it, or miss it?

## Answer

The voice path relies on **44 capabilities**. Against ADR 0035:

| Classification | Count | Of which |
| --- | --- | --- |
| Covered | 22 | 16 first-class, 6 only through raw provider-event passthrough |
| Partial | 11 | mostly passthrough-only: transcripts, content-filter status, fallback speech, Azure voices |
| Missing | 9 | 4 are Azure provider work (Azure OpenAI, Voice Live, Foundry session, Foundry publish); 5 are in ADR 0035's own scope |
| Out of scope | 2 | SK plugin classes (app migration), ACS call answering (telephony bridging) |

The gaps that decide whether the accelerator can drop Semantic Kernel (SK):

1. **Mid-call session updates are missing.** Every handoff sends `session.update` with new instructions, tools and voice,
   and switches which functions the tool loop runs. ADR 0035 offers only a raw outbound `RealtimeEvent`, which reaches
   the provider but not the agent's own tool set. Handoffs themselves are deferred by the ADR.
2. **The tool loop has no contract beyond "reuse the shared executor".** Missing: string-only results (Azure Realtime
   stops responding on non-string output), ordering (a handoff's `session.update` must land before the tool result and
   the next `response.create`), whether tools block the receive loop, and MCP tools (the ADR's `BaseAgent`-based agent
   doesn't inherit `RawAgent`'s MCP handling).
3. **Azure providers are absent.** Voice Live is the accelerator's default and needs its own URL, session payload,
   event-name normalisation, voice object and `pre_generated_assistant_message`. Foundry voice agents need a different
   handshake and session layout.
4. **The event model works, but only through provider event names for the signals the app acts on.** Barge-in,
   input vs output transcripts, transcript finality and content-filter status are all reachable only by matching
   `service_event_type`, and the agent-transcript name differs between OpenAI GA and Voice Live. AF already has
   `finish_reason="content_filter"`; the ADR's mapping doesn't set it.
5. **Explicit responses are half there.** The greeting is first-class (`create_response=True`); fallback speech needs a
   raw `response.create` with a provider-specific payload whose outbound encoding the ADR doesn't define.

ADR 0035 already covers the plumbing well: conversation lifecycle, serialised writes, audio in and out, server VAD,
transcription config, `interrupt()`, tool execution through the shared executor, argument aggregation, usage, errors,
receive-loop failure and the greeting.

## Sources and method

- **Accelerator:** `ai-contact-centre-solution-accelerator`, local checkout at `d42fbcf` (branch
  `feat/content-filter-handling`); `src/`, `config.yaml` and `docs/` are identical to its `main` at `a72f030`, where
  the content-filter work landed squashed. The accelerator repository is private, so citations are `path:line`. Paths
  without a prefix are under `src/ai_contact_centre_solution_accelerator/`.
- **ADR 0035:** final revision, eavanvalkenburg's branch at commit
  [`591b7ea7e`](https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md)
  (blob `6cbccd9b`, verified identical to the map's saved copy). ADR line links (`Lnnn`) point at that permalink.
  Review threads are on [Python: Add realtime agent ADR](https://github.com/microsoft/agent-framework/pull/6831).
- **Semantic Kernel:** 1.45.0, the version in the accelerator's `uv.lock`; the installed files were verified identical
  to tag [`python-1.45.0`](https://github.com/microsoft/semantic-kernel/tree/python-1.45.0/python). Read for behaviour
  the accelerator gets from SK without writing it (tool auto-invocation, `response.create` after results).
- **Agent Framework Python:** this repository at
  [`f4bdd1dc9`](https://github.com/microsoft/agent-framework/tree/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python), for
  primitives that exist today (`finish_reason`, MCP tools, progressive tool exposure).
- **Method:** read the whole voice path (`core/`, `routes/call.py`, `routes/call_listeners.py`, `routes/incoming.py`,
  `agents/`, `tools/`, `observability/`, `config.yaml`, `config.py`, voice ADRs, `docs/architecture.md`), listed every
  provider event, setting and side effect it depends on, then searched the ADR text for each one.

### Classification

- **Covered:** ADR 0035 already provides it; the accelerator could do the same thing on the proposed API.
- **Partial:** the ADR provides part of it, or provides it with an open shape that the accelerator would trip on.
- **Missing:** the ADR doesn't provide it and the accelerator can't drop SK without it.
- **Out of scope:** per the map, it stays in the app or in the separate migration effort.
- **Route:** *first-class* means a named, provider-neutral API element (method, option key, `Content` type or update
  field). *Passthrough* means only reachable through `service_event_type` matching, `additional_properties`,
  `raw_representation`, provider extension options or a raw outbound `RealtimeEvent`.

## Findings

### Gap table

| # | Capability | Accelerator evidence | ADR 0035 coverage | Class | Route |
| --- | --- | --- | --- | --- | --- |
| **Session and transport** | | | | | |
| 1 | One realtime session per call: opened at call start, closed when the caller's websocket closes; receive loop runs as a task | `routes/call.py:183-225`; `core/orchestration.py:360-406` | Async context manager owns the single receive loop and cleanup; `close()` is idempotent ([ADR L246-260][a246], [L345-351][a345]) | Covered | First-class |
| 2 | Two tasks write to one socket: the audio pump, and the receive loop (tool results, session updates, fallback speech) | `routes/call.py:112-130`; SK [`_open_ai_realtime.py:513-515`][sk513]; `core/orchestration.py:586` | "Serialize outbound writes ... a simple async lock" ([L252-254][a252]) | Covered | First-class |
| 3 | Event delivery paced by the consumer: listeners are awaited inline, so a slow client socket slows the loop and nothing is dropped | `core/listeners.py:56-67`; `routes/call.py:183-185` | Bounded, configurable queue; overflow fails the conversation with a backpressure error ([L255-258][a255]). Behaviour differs, see Open questions | Covered | First-class |
| 4 | Initial session config on connect: instructions (plus silent-handoff suffix), tools, voice, turn detection, transcription, Voice Live audio processing | `core/orchestration.py:384-395`; `core/client_factory.py:78-160` | `__aenter__` "applies initial session options ... installs tools/session settings" ([L340-342][a340]); `RealtimeSessionOptions` ([L857-866][a857]); `default_options` example ([L692-700][a692]) | Covered | First-class |
| 5 | Azure OpenAI Realtime: GA `wss://<resource>/openai/v1/realtime?model=<deployment>`, Entra ID token only | `core/client_factory.py:55-75`; SK [`azure_realtime.py:151-192`][skaz] bypasses the OpenAI SDK because it adds `api-version` (L159-161) | Only OpenAI, in `agent-framework-openai` ([L443-444][a443], [L564-568][a564]); Azure not mentioned | Missing | — |
| 6 | Azure Voice Live: `/voice-live/realtime?api-version=2025-10-01&model=`, BYOM `profile` and `foundry-resource-override` params, raw `session.update` without `session.type`, preview event names (`response.audio.*`) rewritten to GA, `conversation.item.create` shaping | `core/voicelive_patches.py:49-64, 232-304, 306-364, 436-476, 495-535`; `core/client_factory.py:60-68` | Not mentioned | Missing | — |
| 7 | Foundry voice-agent session: `/agents/<name>/endpoint/protocols/voice?api-version=v1&x-agent-version-override=<v>`, `Foundry-Features: VoiceAgents=V1Preview`, `https://ai.azure.com/.default` token, `realtime` subprotocol, W3C trace context in the handshake, nested GA session layout, turn detection never re-sent | `core/foundry_voice_client.py:29-32, 55-114`; `core/foundry_voice.py:11-61`; `core/client_factory.py:44-53` | Not mentioned | Missing | — |
| 8 | Publish the entry agent as a Foundry voice agent, rendered from the session settings a call sends, and pin calls to that version | `core/foundry_voice_agent.py:69-104, 112-151`; `core/orchestration.py:267-272` | Not mentioned | Missing | — |
| **Audio** | | | | | |
| 9 | Caller audio in: ACS and browser PCM16 24 kHz mono base64 frames to `input_audio_buffer.append` (today forwarded still base64) | `routes/call.py:81-110`; `routes/acs_media.py:36-55`; `routes/incoming.py:78-89` | `conversation.send(Content.from_data(..., media_type="audio/pcm"))` ([L355-366][a355], [L794-800][a794]) | Covered | First-class |
| 10 | Agent audio out to ACS `AudioData`, the browser and the recording | `core/orchestration.py:409-427, 475-478`; `routes/call_listeners.py:30-36` | Audio as `Content.from_data` updates ([L483-490][a483]); the ACS sample parses `content.uri` by hand ([L775-791][a775]) because `Content` has no public bytes accessor (AF only has private [`_get_data_bytes`][afbytes]) | Covered | First-class |
| 11 | Audio format and rate: PCM16 at 24 kHz both ways, left at the service default; recording assumes 24 kHz; Foundry definition pins `{"type": "audio/pcm", "rate": 24000}`; Voice Live also takes 16 kHz input | `core/client_factory.py:94-160` (no format fields); `core/listeners.py:163`; `core/foundry_voice_agent.py:33, 80, 107`; `core/voicelive_patches.py:168`; `routes/static/index.html:930, 945` | `input_audio_format` / `output_audio_format` as strings ([L862-863][a862]); audio content is `audio/pcm` with no rate ([L799][a799]) | Partial | First-class (format only; rate not modelled) |
| **Turn detection** | | | | | |
| 12 | Server VAD: `server_vad`, threshold 0.8, silence 800 ms, `create_response` | `config.yaml:169-173`; `core/client_factory.py:101-106, 122-127` | `turn_detection: Mapping[str, Any] \| None` ([L865][a865]) with these exact values in its sample ([L697][a697]) | Covered | First-class |
| 13 | Azure semantic VAD: `azure_semantic_vad`, `eagerness`, `remove_filler_words` (`end_of_utterance_detection` modelled but unused) | `core/client_factory.py:114-120`; `core/voicelive_patches.py:67-79, 114-138`; `config.yaml:117-123` | Provider-specific values in the `turn_detection` mapping; provider extension fields allowed ([L940-941][a940]) | Covered | Passthrough |
| 14 | Input transcription config: `gpt-4o-transcribe` or `azure-speech`, `language="en"`, Voice Live `phrase_list` | `config.yaml:89`; `core/client_factory.py:97-100, 151-154`; `core/voicelive_patches.py:89-104` | `input_audio_transcription: Mapping[str, Any]` ([L864][a864]) | Covered | First-class (Azure values passthrough) |
| **Barge-in** | | | | | |
| 15 | Stop playback on `input_audio_buffer.speech_started`: send `StopAudio` to ACS or the browser, with `audio_start_ms` | `core/orchestration.py:547-549`; `routes/call_listeners.py:70-71`; `routes/static/index.html:1065` | Realtime-event content; the ADR's own ACS sample does exactly this by matching `service_event_type == "input_audio_buffer.speech_started"` ([L803-809][a803]), with `audio_start_ms` in `additional_properties` ([L984-990][a984]). Event kinds are only coarse categories such as `lifecycle` ([L1108-1109][a1108]) | Covered | Passthrough |
| 16 | Cancel the in-flight response on barge-in | Left to the service's turn-detection interruption: never sends `response.cancel` and leaves `interrupt_response` unset (`core/client_factory.py:94-160`; `core/voicelive_patches.py:133`) | `interrupt()` ([L229-231][a229]); `interrupt_response` fits the `turn_detection` mapping | Covered | First-class |
| 17 | Discard the unheard rest of the response (`conversation.item.truncate` with item id, content index, `audio_end_ms`) | Not done: `src/` never truncates, it only stops playback (`core/orchestration.py:547-549`) | "Must not ignore the problem"; an optional sink-to-conversation callback or event, shape open ([L368-372][a368]) | Partial | Undecided |
| **Transcripts** | | | | | |
| 18 | Caller transcript: final text from `conversation.item.input_audio_transcription.completed`, with `audio_start_ms`, to the UI, history and telemetry | `core/orchestration.py:551-559`; `core/listeners.py:83-87, 314-316` | Transcripts become `Content.from_text` ([L483-490][a483]); the update's role comes from an unspecified `_role_from_realtime_event` ([L1130-1151][a1130]). Nothing marks "caller transcript, final" | Partial | Passthrough |
| 19 | Agent transcript: one final text per response (`response.output_audio_transcript.done`, Voice Live `response.audio_transcript.done`), labelled with the active agent; deltas ignored | `core/orchestration.py:522-523, 565-572`; `core/voicelive_patches.py:59-64` | Says deltas and `.done` "require different handling" ([L1042-1043][a1042]) but not how an update marks finality; author fixed to `agent.name` ([L1130-1151][a1130]); its sample prints every `update.text` ([L717-718][a717]), which duplicates text if `.done` also maps to text (SK avoids this, [`_open_ai_realtime.py:326-328`][sk326]) | Partial | Passthrough |
| 20 | Transcription failure (`...input_audio_transcription.failed`) to the log and UI | `core/orchestration.py:561-563`; `core/listeners.py:124-125` | Every server event becomes an update ([L500-502][a500]); errors map to `Content.from_error` ([L489][a483]). Telling it from a service error needs `service_event_type` | Covered | Passthrough |
| **Tools** | | | | | |
| 21 | Auto-invoke the active agent's tools, send `function_call_output`, then ask for the next response | SK [`_open_ai_realtime.py:442-517`][sk442] (invoke at 506, result and `response.create` at 513-515); `core/orchestration.py:314-323`; `agents/realtime_agent.py:39-41` | Reuse the shared function-invocation executor: middleware, approvals, limits, error shaping ([L522-540][a522]); "direct realtime tool loop invokes existing function machinery" ([L1185][a1185]) | Covered | First-class (timing: row 23) |
| 22 | Tool results serialised to a string, because Azure Realtime stops responding on non-string output | `core/realtime_patches.py:51-77`, citing [Python: Bug: realtime service - plugins with tools that return non-string responses (i.e. dicts) cause realtime API to stop responding](https://github.com/microsoft/semantic-kernel/issues/13003). AF's [`FunctionTool.parse_result`][afparse] returns `list[Content]` | Only "function results -> `Content.from_function_result`" ([L488][a483]) | Missing | — |
| 23 | Tool-loop ordering and concurrency: a handoff tool's `session.update` must land before its result and the next `response.create`; tools run inline in the receive loop, so audio and barge-in events wait behind a slow tool; SK sends `response.create` after every result | `core/orchestration.py:245-252`; `docs/architecture.md:830-837`; SK [`_open_ai_realtime.py:942-963`][sk942] (inline) and [513-515][sk513] | Silent on where the loop runs, what it blocks and when it requests the next response | Missing | — |
| 24 | Streamed tool-call arguments assembled per `call_id` (the app de-duplicates SK's per-delta events) | `core/orchestration.py:460-461, 480-500`; SK [`_open_ai_realtime.py:334-346`][sk334] | Arguments "need event-level correlation before they become a complete `Content.from_function_call(...)`" ([L1050-1051][a1050]) | Covered | First-class |
| 25 | Tool calls and results visible to the app with `call_id`, arguments and result: UI events, `execute_tool` spans, `cc.function_call_latency` | `core/orchestration.py:480-520`; `routes/call_listeners.py:48-69`; `core/listeners.py:326-349`; `observability/call_span_manager.py:313-337` | Calls are in the stream ([L487][a483], [L1185][a1185]); results the local loop produces aren't said to appear in the live stream, only listed as a content type ([L488][a483]); function middleware runs in the shared executor ([L522-540][a522]) | Partial | First-class for calls |
| 26 | MCP tools (HTTP and stdio) connected once at app start, shared across calls, assigned per agent | `core/mcp_loader.py:14-98`; `core/orchestration.py:176-196`; `main.py:46-68` | Not mentioned. AF's MCP handling lives in `RawAgent` ([`_agents.py:1040-1114, 1653-1699`][afmcp]); the ADR derives `RawRealtimeAgent` from `BaseAgent`, not `RawAgent` ([L425-430][a425]) | Missing | — |
| 27 | SK `@kernel_function` plugin classes loaded from config, with plugin names in UI and eval events | `tools/loader.py:50-76, 147-175`; `tools/example_tools.py:40-102`; `core/orchestration.py:496, 516`; `eval/src/eval/conversation_simulator.py:295-313` | Tools are AF tools ([L692-700][a692]). The chat path already bridges these plugins (`chat/sk_tools.py:104-230`; `chat/agents.py:17-54`) | Out of scope | — |
| **Session updates and handoffs** | | | | | |
| 28 | Mid-call `session.update` with new instructions, tools and voice, plus switching which functions the tool loop will run | `core/orchestration.py:333-343`; SK `update_session` [`_open_ai_realtime.py:364-407`][sk364] | Only as a raw outbound command ([L1060-1062][a1060], [L1116-1117][a1108]): it reaches the provider, not the agent's tool set. Progressive tool exposure ([L533][a522]; [`FunctionInvocationContext.add_tools`][afprog], effective "on the next model iteration") has no rule that pushes the change to the live session | Missing | Passthrough for the provider half only |
| 29 | Voice selection: OpenAI voice names, or the Voice Live voice object (`name`, `azure-standard`/`azure-custom`, `rate`, `temperature`, `endpoint_id`, `custom_lexicon_url`); per-agent voice on handoff | `core/client_factory.py:137-144`; `core/voicelive_patches.py:141-158`; `core/orchestration.py:295-312`; `config.yaml:125-131, 163-164, 218, 241` | `voice: str` ([L861][a857]) | Partial | First-class for names; Azure object passthrough |
| 30 | Handoff orchestration: config graph, `handoff_to_<agent>` tools, silent handoffs (prompt suffix, result "OK"), handoff calls hidden from UI and telemetry, `AgentSwitch` events, active-agent attribution | `core/orchestration.py:198-265, 457-472, 482-483, 504-505`; `agents/agent_factory.py:70-97`; `core/listeners.py:326-339`; `config.yaml:268-299, 347-372` | Deferred "until that orchestration is independently generalized beyond `Agent`" ([L405-409][a405], [L67-69][a67]) | Missing | — |
| **Explicit responses** | | | | | |
| 31 | The agent speaks first (greeting) | `routes/call.py:194` (`create_response=True`); SK [`_open_ai_realtime.py:432-433`][sk432] | `__aenter__` requests a response only with `create_response=True` ([L343-344][a343]); also a `RealtimeSessionOptions` key ([L866][a857]) | Covered | First-class |
| 32 | Fallback speech after a content-filter block: Voice Live and Foundry use `response.create` with `pre_generated_assistant_message` (verbatim, no model call); Azure OpenAI uses `response.create` with an `instructions` override; at most one fallback, re-armed on `error` because the service rejects `response.create` while a response is active | `core/voicelive_patches.py:479-493`; `core/realtime_patches.py:80-87`; `core/orchestration.py:113-115, 540-545, 577-586`; `docs/architecture.md:559-577` | "Use `conversation.send(RealtimeEvent(...))` for explicit provider response commands" ([L333-335][a333]); how an outbound `RealtimeEvent` carries a `response` payload isn't defined ([L869-876][a869], [L1116-1117][a1108]) | Partial | Passthrough |
| **Response lifecycle, content filter, usage, errors** | | | | | |
| 33 | Response boundaries (`response.created`, `response.done`) and session events (`session.created` id, `session.updated`) | `core/orchestration.py:532-538, 574-584`; `core/listeners.py:106-109, 320-323` | Control events become realtime-event content ([L296-313][a296]); `response_id` is copied onto the update ([L1130-1151][a1130]) | Covered | Passthrough |
| 34 | Content-filter status: `response.done` with `status_details.reason == "content_filter"` and no `error` event, driving the fallback, a WARNING log, `gen_ai.response.finish_reasons` and `cc.content_filter_count` | `core/orchestration.py:79-81, 577-586`; `observability/call_span_manager.py:282-290`; `observability/metrics.py:74-87`; `docs/architecture.md:561-564` | Never mentions content filtering; status is generic `additional_properties` ([L1113][a1108]); the mapping sets no `finish_reason` ([L1130-1151][a1130]), though `AgentResponseUpdate.finish_reason` already accepts [`"content_filter"`][affinish] | Partial | Passthrough |
| 35 | Token usage per response, summed per call | `core/orchestration.py:582-583`; `observability/call_span_manager.py:292-303` | Usage maps to `Content.from_usage` ([L490][a483]); best effort ([L438-441][a438]) | Covered | First-class |
| 36 | Non-fatal service `error` events (the session continues): UI, log, span event, `cc.error_count`, a canned assistant line in history | `core/orchestration.py:540-545`; `core/listeners.py:90-91, 312-313`; `observability/call_span_manager.py:162-172` | Errors map to `Content.from_error` ([L489][a483]) | Covered | First-class |
| 37 | Receive-loop failure reported to the UI and spans; teardown on disconnect | `core/orchestration.py:591-596`; `core/listeners.py:324-325`; `observability/call_span_manager.py:385-395`; `routes/call.py:206-225` | Failures surface to the iterator; terminal state is kept outside the bounded queue ([L259-260][a246], [L331-332][a331], [L345-351][a345]) | Covered | First-class |
| **Telemetry, history, recording, config** | | | | | |
| 38 | Call telemetry: spans `chat.completions <deployment>`, `aoai.realtime.session`, `agent.session`, `agent.response_turn`, `execute_tool`, `invoke_agent` (handoff), `ws.disconnect`; message, prompt and completion events; metrics `cc.handoff_count`, `cc.function_call_latency`, `cc.error_count`, `cc.content_filter_count`, `cc.active_connections`, `cc.call_duration` | `observability/call_span_manager.py:39-414`; `observability/metrics.py:7-105`; `core/listeners.py:297-369`; `docs/adrs/2026-02-11-Define-Span-Hierarchy-And-Attribute-Conventions.md:47-75` | Record provider, model and session metadata and timing, not raw audio ([L438-441][a438]); dedicated realtime middleware deferred ([L570-577][a570]); the [full-duplex middleware and telemetry thread](https://github.com/microsoft/agent-framework/pull/6831#discussion_r3953440213) is unanswered | Partial | Open |
| 39 | App-side history of final caller and agent transcripts in order, used for the post-call completion event; seeding a session from history is supported but unused | `core/listeners.py:75-93`; `routes/call.py:165, 194, 210`; `observability/call_span_manager.py:224-237`; `core/orchestration.py:360-395` | An authored-turn ledger is finalised through context providers on close ([L263-266][a263], [L315-324][a315]); seeding from messages ([L340-342][a340], [L1021-1022][a1021]). What an authored turn is in an audio-only call isn't defined | Partial | First-class ledger, undefined content |
| 40 | Two-channel call recording (caller left, agent right, 24 kHz) | `routes/call.py:60-70, 100`; `core/listeners.py:149-294` | Inbound audio is the app's own before `send`; outbound arrives as audio content ([L355-366][a355]) | Covered | First-class (app-level) |
| 41 | Agent definitions in `config.yaml` shared with the chat path: name, description, instructions, voice, plugins, MCP servers, handoffs | `config.yaml:189-299`; `agents/agent_factory.py:19-97`; `chat/agents.py:57-83` | `RealtimeAgent(client=..., instructions=..., tools=..., default_options=...)` fits one definition ([L692-700][a692]); several definitions sharing one session is deferred ([L405-409][a405]) | Partial | First-class for one agent |
| **Voice Live extras and transport** | | | | | |
| 42 | Voice Live audio processing: `azure_deep_noise_suppression`, `server_echo_cancellation` | `config.yaml:108-115`; `core/client_factory.py:129-135` | Provider extension fields ([L940-941][a940]); needs a Voice Live client (row 6) | Covered | Passthrough |
| 43 | Visemes for avatars (`animation.outputs=["viseme_id"]`): off by default, events never consumed, rejected in Foundry mode | `config.yaml:132-133`; `core/client_factory.py:146-148`; `config.py:346-352`; no viseme case in `core/orchestration.py:525-589` | Extension field, and the events arrive as realtime-event content ([L940-941][a940], [L500-502][a500]) | Covered | Passthrough |
| 44 | ACS call answering with bidirectional, mixed-channel 24 kHz PCM media streaming | `routes/incoming.py:36-96` | ACS bridges stay samples or optional integrations ([L355-366][a355]) | Out of scope | — |

### Event model: what the app acts on

`core/events.py:7-117` defines 14 app events; `stream_session_events` (`core/orchestration.py:457-596`) produces 13 of
them from SK events and the route adds `AudioInputEvent`. Mapped onto ADR 0035's projection:

| App event | Provider event | ADR 0035 projection | How the app would find it |
| --- | --- | --- | --- |
| `AudioOutputEvent` | `response.output_audio.delta` (Voice Live: `response.audio.delta`) | `Content.from_data(audio/...)` | First-class: `has_top_level_media_type("audio")` |
| `StopAudioEvent` | `input_audio_buffer.speech_started` | realtime-event content, kind `lifecycle` | `service_event_type` string |
| `TranscriptionEvent(user)` | `conversation.item.input_audio_transcription.completed` | `Content.from_text`, role unspecified | `service_event_type` string |
| `TranscriptionEvent(agent)` | `response.output_audio_transcript.done` (Voice Live: `response.audio_transcript.done`) | `Content.from_text` or realtime-event content, unspecified | `service_event_type` string, which differs by provider |
| `TranscriptionErrorEvent` | `conversation.item.input_audio_transcription.failed` | error or realtime-event content, unspecified | `service_event_type` string |
| `FunctionCallEvent` | `response.function_call_arguments.*` | `Content.from_function_call` (assembled) | First-class |
| `FunctionResultEvent` | none (produced locally) | not said to be emitted | Unspecified |
| `ResponseCreatedEvent` | `response.created` | realtime-event content | `service_event_type` string |
| `ResponseDoneEvent(usage, is_content_filtered)` | `response.done` | split into usage content and realtime-event content ([L1160-1161][a1160]) | Usage first-class; filter status via `additional_properties` |
| `ServiceErrorEvent` | `error` | `Content.from_error` | First-class |
| `SessionCreatedEvent`, `SessionUpdatedEvent` | `session.created`, `session.updated` | realtime-event content | `service_event_type` string |
| `AgentSwitchEvent` | none (handoff) | none | Missing with handoffs |
| `AgentErrorEvent` | none (receive-loop exception) | iterator raises | First-class |
| `AudioInputEvent` | none (app's own input) | n/a | App-level |

So the "no event loss" driver ([L78-79][a78]) holds: everything is reachable. What's missing is meaning. Four of the
signals the app acts on most (barge-in, both transcripts, content filter) need string matching on provider event
names, and the agent-transcript name differs between OpenAI GA and Voice Live `2025-10-01`
(`core/voicelive_patches.py:51-64`).
Unless provider clients normalise `service_event_type`, app code written against passthrough is provider-specific,
which is exactly what the shared surface is meant to avoid. The .NET draft goes further than ADR 0035 here: it surfaces
"VAD speech-started and speech-stopped, barge-in, response-created and response-done, input-transcription, error" as
"a typed realtime event content"
([.NET ADR 0030 draft, decision Q4](https://github.com/rogerbarreto/agent-framework-public/blob/features/realtime-agent/docs/decisions/0030-realtime-ai-agent.md)).

Two small, high-value fixes fall out of the table:

- Map `response.done` status to `AgentResponseUpdate.finish_reason`, which already accepts `"content_filter"`
  ([`_types.py:2356`][affinish]). The accelerator's span already writes `gen_ai.response.finish_reasons`
  (`observability/call_span_manager.py:282-290`), so telemetry would line up with chat.
- Mark transcript updates with their speaker and finality, so `update.text` doesn't double-print and history can
  take final text only.

### Conversation controls

| Control | Accelerator | ADR 0035 | Gap |
| --- | --- | --- | --- |
| Start with a response | `create_response=True` on start (`routes/call.py:194`) | `create_response=True` ([L343-344][a343]) | None. The name also exists inside `turn_detection` with another meaning ([L697][a697]) |
| Update the session mid-call | `update_session(settings, kernel)` swaps instructions, tools, voice and the kernel the tool loop invokes (`core/orchestration.py:333-343`) | Raw `RealtimeEvent` command ([L1060-1062][a1060]) | No first-class op; a raw event leaves the agent's tool set and instructions stale |
| Ask for a response with per-response settings | `response.create` with `instructions` (Azure OpenAI) or `pre_generated_assistant_message` (Voice Live) (`core/realtime_patches.py:80-87`; `core/voicelive_patches.py:479-493`) | Raw `RealtimeEvent` command ([L333-335][a333]) | No defined outbound payload; no "say this verbatim" abstraction; no rule for a response that is already active |
| Interrupt | Not sent; server-side interruption | `interrupt()` ([L229-231][a229]) | None for the accelerator |
| Truncate unheard audio | Not done | Required in principle, shape open ([L368-372][a368]) | ACS gives the app no playback position, so `audio_end_ms` must be estimated from audio sent; see Open questions |

The in-session handoff is the accelerator's defining feature. Its correctness depends on ordering that SK provides
implicitly: the handoff runs as the tool function, awaits `update_session`, and only then does SK send the result and
`response.create` (`core/orchestration.py:245-252`; `docs/architecture.md:830-837`). ADR 0035 already lists
progressive tool exposure as a capability of the shared executor ([L533][a522]). In AF a tool can call
`ctx.add_tools(...)` / `ctx.remove_tools(...)`, and "mutations take effect on the next model iteration"
([`_middleware.py:470-475`][afprog]). In a realtime session the next model iteration is the `response.create` that
follows the tool result. A rule that the conversation pushes `session.update` before sending a result whenever the tool
set, instructions or voice changed would give handoffs their ordering without a handoff-specific API.

### Tool execution

What the accelerator gets from SK today, none of it written in the app:

1. On `response.function_call_arguments.done`, SK splits the `plugin-function` name, invokes the kernel function
   (failing closed if no function choice behaviour is set), sends `function_call_output` and then `response.create`
   ([`_open_ai_realtime.py:442-517`][sk442]).
2. That all happens inside the receive generator ([`_open_ai_realtime.py:942-963`][sk942]; the Voice Live override
   keeps this, `core/voicelive_patches.py:362`), so nothing else is read from the socket while a tool runs.
3. `response.create` follows every result ([`_open_ai_realtime.py:513-515`][sk513]), not once per response.
4. The app's only tool code is the string sanitiser (`core/realtime_patches.py:51-77`) and MCP plugin wiring
   (`core/mcp_loader.py:14-98`; `core/orchestration.py:176-196`).

ADR 0035 decides the right thing at the top (reuse the shared executor, [L522-540][a522]) but leaves the realtime loop
around it unspecified. The extended ADR needs to say:

- how a `list[Content]` result ([`FunctionTool.parse_result`][afparse]) becomes the string `output` the realtime APIs
  take, so the Azure hang can't recur;
- whether tools run on the receive loop or beside it. Beside it keeps barge-in responsive during a slow tool, but then
  ordering must be explicit: result after any session change, and one `response.create` per completed set of calls;
- how MCP tools connect and are shared. `RawAgent` connects them in `__aenter__` and per run
  ([`_agents.py:1040-1114, 1653-1699`][afmcp]); a `BaseAgent`-derived realtime agent gets none of that, and the
  accelerator needs one connection per server for the whole process, not per call;
- whether approvals are in scope for voice. The accelerator doesn't use them ("the voice path has no built-in approval
  step", `docs/production-readiness.md:230`), so they aren't needed to drop SK.

### Azure providers and Foundry voice agents

ADR 0035 names one provider, OpenAI, in `agent-framework-openai` ([L443-444][a443], [L564-568][a564]). eavanvalkenburg's
Dec 2025 draft listed "Day 1 support for both OpenAI Realtime API, Azure Voice Live API" as a decision driver
([`00XX-realtime-agents.md`](https://github.com/eavanvalkenburg/agent-framework/blob/voice_agents/docs/decisions/00XX-realtime-agents.md),
Decision Drivers); the final revision dropped it. The accelerator defaults to Voice Live (`config.yaml:90`) and needs,
per provider:

| Need | Azure OpenAI | Voice Live | Foundry voice agent |
| --- | --- | --- | --- |
| Endpoint | `/openai/v1/realtime?model=` without `api-version` ([SK `azure_realtime.py:159-161`][skaz]) | `/voice-live/realtime?api-version=2025-10-01&model=` plus BYOM params (`core/voicelive_patches.py:49, 239, 278-283`) | `/agents/<name>/endpoint/protocols/voice?api-version=v1&x-agent-version-override=` (`core/foundry_voice_client.py:111-114`) |
| Auth | Entra ID, `cognitiveservices` scope (`core/client_factory.py:56-58`) | same | Entra ID, `ai.azure.com` scope, `Foundry-Features` header, `realtime` subprotocol (`core/foundry_voice.py:11-14`; `core/foundry_voice_client.py:66-79`) |
| `session.update` shape | GA nested `audio.input` / `audio.output` (SK maps flat settings, [`open_ai_realtime_execution_settings.py:82-121`][skset]) | flat, and must not carry `session.type` (`core/voicelive_patches.py:436-476`) | GA nested with `session.type`, no `turn_detection` (`core/foundry_voice.py:36-61`; `core/foundry_voice_client.py:85-95`) |
| Event names | GA | preview `response.audio.*` (`core/voicelive_patches.py:51-64`) | GA (`core/foundry_voice_client.py:38-40`) |
| Voice | name string | object with type, rate, temperature, custom voice, lexicon (`core/voicelive_patches.py:141-158`) | Voice Live object under `audio.output.voice` (`core/foundry_voice.py:39-44`) |
| Verbatim speech | no; `instructions` override (`core/realtime_patches.py:80-87`) | `pre_generated_assistant_message` (`core/voicelive_patches.py:479-493`) | as Voice Live |
| Extra settings | none | noise suppression, echo cancellation, `azure_semantic_vad`, `azure-speech` with `phrase_list`, 16 kHz input, visemes | as Voice Live, minus visemes (`config.py:346-352`) |

The accelerator reaches Foundry by subclassing its Voice Live client and overriding three seams: the handshake (URL,
headers, subprotocol, trace context), the `session.update` payload, and event parsing
(`core/foundry_voice_client.py:55-109`). If AF's Voice Live client exposes the same seams, a Foundry voice agent is a
small subclass; that is a concrete candidate for the "extension point" the map asks for. Publishing the agent definition
(`core/foundry_voice_agent.py:69-151`) uses the Foundry SDK and could stay in the app, but it needs the session
config rendered without opening a session (`core/orchestration.py:267-272`).

### Telemetry the accelerator emits

The call telemetry in the accelerator's span ADR is built in the app from the session events
(`core/listeners.py:297-369`; `observability/call_span_manager.py:97-414`); SK's own GenAI diagnostics are switched
on as well (`observability/bootstrap.py:231`). To rebuild it without SK, the realtime agent's stream (or middleware)
must expose, per call:

- **Response turns:** `response.created` and `response.done` with `response_id`, per-response usage and finish reason,
  for `agent.response_turn` (`observability/call_span_manager.py:271-307`).
- **Tool calls:** call start (name, arguments, `call_id`) and result (`call_id`, result), for `execute_tool` and
  `cc.function_call_latency` (`observability/call_span_manager.py:313-337`). AF's own `execute_tool` spans from the
  shared executor could replace these, as the chat path already does (`chat/sk_tools.py:49-75`).
- **Handoffs:** source and target agent at the moment the session switches, for `invoke_agent`, `agent.session` and
  `cc.handoff_count` (`core/orchestration.py:345-358`; `observability/call_span_manager.py:243-269`).
- **Messages:** final caller and agent transcripts with the active agent, for `gen_ai.user.message` and
  `gen_ai.assistant.message` events (`observability/call_span_manager.py:174-195`).
- **Errors and content filter:** service errors, receive-loop failures, and content-filter blocks counted as blocks, not
  errors (`observability/call_span_manager.py:162-172, 282-290, 385-395`).
- **Trace context out:** the call span's W3C context injected into the Foundry handshake
  (`core/foundry_voice_client.py:65-74`); ADR 0035 doesn't mention propagating trace context to the provider.

These are inputs for [Middleware and telemetry contract for realtime agents](https://github.com/adamdougal/agent-framework/issues/9).

### History, recording and config

- **History.** The app keeps its own `ChatHistory` of final transcripts only (`core/listeners.py:75-93`) and writes the
  last 20 messages into a completion event at hang-up (`observability/call_span_manager.py:224-237`). Nothing persists
  beyond the call. ADR 0035's authored-turn ledger ([L263-266][a263]) is the right home, but it doesn't say whether a
  voice turn is its final transcript, whether turns are ordered as spoken, or whether a barged-in agent turn keeps
  the words the caller never heard. A prior AF attempt hit the ordering problem: Copilot's review of
  [Python: Add realtime voice agents with OpenAI, Azure OpenAI and Voice Live clients](https://github.com/microsoft/agent-framework/pull/3821)
  flagged that batching transcripts by role "reorders multi-turn conversations"
  ([review thread](https://github.com/microsoft/agent-framework/pull/3821#discussion_r2790024634)).
- **Recording.** Needs raw audio both ways and nothing else from the framework (`core/listeners.py:149-294`). ADR
  0035's audio content is enough. Getting the bytes back means parsing `content.uri` until `Content` has a public
  accessor (the ADR sample does this, [L775-784][a775]).
- **Config.** One `config.yaml` drives both voice and chat agents (`agents/agent_factory.py:19-97`;
  `chat/agents.py:57-83`), as the accelerator's own ADR requires ("Share definitions, not runtime implementations",
  `docs/adrs/2026-03-12-Adopt-MAF-For-Text-Chat-While-Retaining-SK-For-Voice.md:81-92`). A `RealtimeAgent` can be
  built from the same fields; the handoff graph and per-agent voices need the handoff design.

### SK features the accelerator uses that ADR 0035 doesn't mention

ADR 0035's SK comparison ([L610-639][a610]) mentions SK's realtime client, events, `audio_output_callback`,
`ChatHistory` seeding, `FunctionChoiceBehavior.Auto()` and `TurnDetection`. The voice path also depends on these,
which the ADR never mentions:

| SK feature | Accelerator evidence | What AF would need |
| --- | --- | --- |
| `RealtimeClientBase.update_session(settings=..., kernel=...)`: new settings and a new tool kernel on a live session | `core/orchestration.py:339`; SK [`_open_ai_realtime.py:364-407`][sk364] | First-class session update that also swaps the local tool set (row 28) |
| Auto-invocation inside the realtime client, followed by `function_call_output` and `response.create` | SK [`_open_ai_realtime.py:442-517`][sk442] | A specified realtime tool loop (rows 21-23) |
| Tool schemas for `session.update` built from a kernel (`prepare_settings_for_function_calling`, `update_settings_from_function_call_configuration`) | `core/orchestration.py:314-331` | Provider clients map AF tool schemas to realtime `tools`; unstated in the ADR |
| SK handoff types (`OrchestrationHandoffs`, `AgentHandoffs`, `HANDOFF_PLUGIN_NAME`) reused for a realtime orchestration SK doesn't ship | `core/orchestration.py:18, 191-243`; `agents/agent_factory.py:70-97` | The handoff model |
| Dynamic kernel functions (`KernelFunctionFromMethod.model_construct`) for `handoff_to_<agent>` | `core/orchestration.py:198-243` | Plain `FunctionTool` closures; no ADR change |
| `Kernel.clone()` per agent, with MCP plugins added after cloning so connections are shared | `core/orchestration.py:176-196` | MCP tool sharing across conversations (row 26) |
| SK MCP plugins (`MCPStreamableHttpPlugin`, `MCPStdioPlugin`) and `KernelPlugin.from_object` naming | `core/mcp_loader.py:14-61` | AF `MCPStreamableHTTPTool` / `MCPStdioTool` ([`_mcp.py:3698, 3957`][afmcpcls]) in a realtime agent |
| Deep-copied base settings overlaid per agent (`PromptExecutionSettings.model_copy`) | `core/orchestration.py:295-312` | Per-agent overlays on conversation options for updates |
| Subclass seams on the websocket client: `create_session`, `receive`, `send` / `_send`, `_send_session_update` / `_session_payload`, `_parse_event`, `get_prompt_execution_settings_class` | `core/realtime_patches.py:75-87`; `core/voicelive_patches.py:245-535`; `core/foundry_voice_client.py:55-109` | Extension seams on AF's Azure clients |
| `_send` taking raw dicts for payloads the SDK models lack (`response.create` overrides, Voice Live `session.update`) | `core/realtime_patches.py:80-87`; `core/voicelive_patches.py:466-493` | A defined raw outbound payload on `RealtimeEvent` |
| SK GenAI OpenTelemetry diagnostics switched on | `observability/bootstrap.py:231` | AF observability, already configured for chat (`observability/bootstrap.py:242-251`) |

### Capabilities the voice path doesn't use

No AF work is needed for these to drop SK: sending text into the session; half-duplex `run()`; WebRTC (SK supports
it; the app uses websockets); SK's `audio_output_callback` (passed `None`, `core/orchestration.py:464`); seeding
history at start (supported, never passed, `routes/call.py:194`); explicit `response.cancel`; truncation; tool
approvals (`docs/production-readiness.md:230`); connection retry and backoff (recommended in
`docs/production-readiness.md:145`, not implemented); a handoff hop limit (chat only,
`docs/production-readiness.md:200`; `chat/handoff.py:28-52`); consuming viseme events; transcription-only sessions.

### Accelerator smells found on the way

Not AF gaps; worth a separate accelerator issue:

1. **A receive-loop error silences the call instead of ending it.** `stream_session_events` catches every exception
   and yields "A tool call failed. The assistant will continue." (`core/orchestration.py:591-596`), then the event
   forwarder returns while `receive_audio_loop` keeps sending caller audio to a session nobody reads
   (`routes/call.py:183-206`, `112-130`).
2. **No truncation after barge-in,** so the model's context keeps words the caller never heard (row 17).
3. **`voicelive.semantic_vad.end_of_utterance` is validated but never sent** (`config.py:136-145`;
   `core/client_factory.py:114-120`).
4. **`_pending_function_calls` is written but never read** (`core/orchestration.py:111, 405, 442-453`).
5. **Tools block the receive loop** (SK behaviour, [`_open_ai_realtime.py:942-963`][sk942]), so audio and barge-in
   wait behind a slow tool or MCP call.
6. **Likely: the recording includes audio the caller never heard.** Chunks are placed by arrival time, kept in order
   per direction (`core/listeners.py:149-160, 245-246`), and output audio arrives faster than it plays, so audio ACS
   dropped after `StopAudio` still lands on the agent channel. Not verified with a recording.

## Implications for the map

### Fog that can graduate

The fog item "Gaps the capability checklist surfaces in ADR 0035's event model and conversation controls" splits
cleanly into two decisions, and the evidence above is enough to start both. The adjacent fog items "Tool execution in
realtime" and "Persisting transcripts into agent sessions" are also concrete now.

| Proposed ticket | Question | Type | Blocked by |
| --- | --- | --- | --- |
| Realtime event taxonomy: first-class signals vs passthrough | Which provider signals get provider-neutral kinds or standard update fields (speech started and stopped, caller and agent transcripts with finality, response created and done with status, `finish_reason="content_filter"`, usage, errors), and do provider clients normalise `service_event_type` across OpenAI GA and Voice Live names? | grilling | [Azure realtime protocols: Azure OpenAI, Voice Live and Foundry voice agents](https://github.com/adamdougal/agent-framework/issues/4) |
| Live conversation controls: session updates, explicit responses, interruption | Which first-class `RealtimeConversation` operations does the extended ADR add (update instructions, tools and voice mid-call while keeping the local tool set in step; request a response with per-response instructions or verbatim text; interrupt and truncate), and which stay raw `RealtimeEvent` commands with a defined payload? | grilling | [Azure realtime protocols: Azure OpenAI, Voice Live and Foundry voice agents](https://github.com/adamdougal/agent-framework/issues/4) |
| Realtime tool execution contract | How does the realtime tool loop serialise results to strings, order results against session changes and `response.create`, run alongside the receive loop, and connect and share MCP tools in a `BaseAgent`-derived agent; are approvals out of scope for voice v1? | grilling | none |
| Voice turns in agent-session history | What becomes an authored turn in an audio-only conversation (final transcripts, spoken order, active-agent attribution, text cut at barge-in), and how does it reach context providers on close? | grilling | Realtime event taxonomy (proposed above) |

The coordinator may prefer to fold "Live conversation controls" into
[Handoff model for realtime agents](https://github.com/adamdougal/agent-framework/issues/7), since session updates are
the handoff primitive. Keeping it separate lets the fallback-speech and truncation decisions move without waiting for
the handoff debate.

### Suggested out of scope

- Consuming viseme events and other avatar features: a config toggle with no consumer (row 43); passthrough is enough.
- SK plugin classes and plugin-name attribution in UI and eval events: app migration (row 27).
- Publishing Foundry voice-agent definitions: app code on the Foundry SDK (`core/foundry_voice_agent.py:112-151`). AF
  only needs to render the session config, which
  [Azure provider clients: design and package placement](https://github.com/adamdougal/agent-framework/issues/8)
  can decide.
- Connection retry and backoff, and a voice handoff hop limit: not used today.

### Blocked and downstream tickets

- [Azure provider clients: design and package placement](https://github.com/adamdougal/agent-framework/issues/8)
  (blocked by this ticket): rows 5-8, 13, 14, 29, 32 and 42, and the provider table above. Three findings shape the
  design: Voice Live's `voice` is an object, not the ADR's `voice: str`; Voice Live still emits preview event names;
  and the accelerator's Foundry client is a subclass overriding three seams (handshake, session payload, event
  parsing), a concrete shape for the extension point.
- [Prototype the accelerator's voice loop on the extended API](https://github.com/adamdougal/agent-framework/issues/10)
  (blocked by this ticket): each of its five slices hits a gap. Voice Live needs a client (row 6). Barge-in works
  through passthrough (row 15). One tool call needs string results and ordering (rows 22-23). One in-session handoff
  needs a session update that swaps the tool set (row 28). Content-filter fallback needs `response.done` status and a
  raw `response.create` payload (rows 32, 34). The prototype should test the proposed controls and tool contract rather
  than wait for them.
- [Handoff model for realtime agents](https://github.com/adamdougal/agent-framework/issues/7): needs rows 28-30, the
  ordering in row 23, and agent attribution of updates (ADR 0035 fixes `author_name` to one agent,
  [L1130-1151][a1130]). Progressive tool exposure is a candidate mechanism (see Conversation controls).
- [Middleware and telemetry contract for realtime agents](https://github.com/adamdougal/agent-framework/issues/9): the
  signal list under "Telemetry the accelerator emits", plus trace-context propagation into provider handshakes.
- [Answer ADR 0035's open API-contract review threads](https://github.com/adamdougal/agent-framework/issues/6): any new
  conversation controls and event kinds belong inside the experimental boundary that thread asks about.
- [Write the extended ADR and open the upstream PR](https://github.com/adamdougal/agent-framework/issues/11): the ADR
  needs new sections for conversation controls, the realtime tool loop, the event taxonomy and Azure providers.

## Open questions

1. **Queue overflow on a phone call.** ADR 0035 fails the conversation when the update queue fills ([L255-258][a255]).
   Today a slow consumer just slows the loop. Is fail-fast right for telephony, where a transient websocket stall would
   drop the call, and what capacity should the default be at roughly 50 audio chunks a second?
2. **Who normalises event names?** Should provider clients rewrite Voice Live's preview names, as the accelerator does
   (`core/voicelive_patches.py:306-364`), or should `service_event_type` stay raw and first-class kinds absorb the
   difference?
3. **When does the tool loop request the next response?** After each result, as SK does, or once all calls from a
   response have results? The service rejects `response.create` while a response is active
   (`core/orchestration.py:541-542`), so per-result requests can fail with several calls in one response. Not verified
   against the service.
4. **How is `audio_end_ms` known for ACS?** ACS doesn't report playback position to the app, so truncation needs an
   estimate from audio sent minus audio not yet played.
5. **Where does content-filter detection live?** In the provider client (mapping `status_details` to `finish_reason`)
   or in the agent's projection?
6. **`voice` shape.** Keep `voice: str` for OpenAI with an Azure voice object in provider options, or widen the type?
7. **`create_response` naming.** The session option ([L866][a857]) and `turn_detection.create_response`
   ([L697][a697]) mean different things under one name.
8. **Foundry voice-agent preview.** The `Foundry-Features: VoiceAgents=V1Preview` header (`core/foundry_voice.py:11`)
   marks a preview API; how much should AF commit to before it's GA?

<!-- ADR 0035, final revision -->
[a67]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L67-L69
[a78]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L78-L79
[a229]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L229-L231
[a246]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L246-L260
[a252]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L252-L254
[a255]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L255-L258
[a263]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L263-L266
[a296]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L296-L313
[a315]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L315-L324
[a331]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L331-L332
[a333]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L333-L335
[a340]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L340-L342
[a343]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L343-L344
[a345]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L345-L351
[a355]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L355-L366
[a368]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L368-L372
[a405]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L405-L409
[a425]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L425-L430
[a438]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L438-L441
[a443]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L443-L444
[a483]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L483-L490
[a500]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L500-L502
[a522]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L522-L540
[a564]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L564-L568
[a570]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L570-L577
[a610]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L610-L639
[a692]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L692-L700
[a697]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L697
[a717]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L717-L718
[a775]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L775-L791
[a794]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L794-L800
[a799]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L799
[a803]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L803-L809
[a857]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L857-L866
[a862]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L862-L863
[a864]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L864
[a865]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L865
[a869]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L869-L876
[a940]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L940-L941
[a984]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L984-L990
[a1021]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L1021-L1022
[a1042]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L1042-L1043
[a1050]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L1050-L1051
[a1060]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L1060-L1062
[a1108]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L1108-L1117
[a1130]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L1130-L1151
[a1160]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L1160-L1161
[a1185]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md#L1185

<!-- Semantic Kernel python-1.45.0 -->
[sk326]: https://github.com/microsoft/semantic-kernel/blob/python-1.45.0/python/semantic_kernel/connectors/ai/open_ai/services/_open_ai_realtime.py#L326-L328
[sk334]: https://github.com/microsoft/semantic-kernel/blob/python-1.45.0/python/semantic_kernel/connectors/ai/open_ai/services/_open_ai_realtime.py#L334-L346
[sk364]: https://github.com/microsoft/semantic-kernel/blob/python-1.45.0/python/semantic_kernel/connectors/ai/open_ai/services/_open_ai_realtime.py#L364-L407
[sk432]: https://github.com/microsoft/semantic-kernel/blob/python-1.45.0/python/semantic_kernel/connectors/ai/open_ai/services/_open_ai_realtime.py#L432-L433
[sk442]: https://github.com/microsoft/semantic-kernel/blob/python-1.45.0/python/semantic_kernel/connectors/ai/open_ai/services/_open_ai_realtime.py#L442-L517
[sk513]: https://github.com/microsoft/semantic-kernel/blob/python-1.45.0/python/semantic_kernel/connectors/ai/open_ai/services/_open_ai_realtime.py#L513-L515
[sk942]: https://github.com/microsoft/semantic-kernel/blob/python-1.45.0/python/semantic_kernel/connectors/ai/open_ai/services/_open_ai_realtime.py#L942-L963
[skaz]: https://github.com/microsoft/semantic-kernel/blob/python-1.45.0/python/semantic_kernel/connectors/ai/open_ai/services/azure_realtime.py#L151-L192
[skset]: https://github.com/microsoft/semantic-kernel/blob/python-1.45.0/python/semantic_kernel/connectors/ai/open_ai/prompt_execution_settings/open_ai_realtime_execution_settings.py#L82-L121

<!-- Agent Framework Python at f4bdd1dc9 -->
[afbytes]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_types.py#L288-L330
[affinish]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_types.py#L2356-L2366
[afmcp]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_agents.py#L1040-L1114
[afmcpcls]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_mcp.py#L3698
[afparse]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_tools.py#L1406-L1446
[afprog]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_middleware.py#L470-L475
