# Findings: the accelerator's voice loop on the extended realtime API

Prototype for [Prototype the accelerator's voice loop on the extended API](https://github.com/adamdougal/agent-framework/issues/10).
Run live on 2026-11-06 against the accelerator's Azure resources: Voice Live `2026-07-15` (managed `gpt-realtime-2.1`,
and BYOM `byom-azure-openai-realtime` on the resource's `gpt-realtime-2.1` deployment), Azure OpenAI (GA route,
`gpt-realtime-2.1`) and the published Foundry voice agent `receptionist-voice` (`v1` preview). Agent Framework at
upstream `f4bdd1dc9`; `openai` 3.36.1, `azure-ai-voicelive` 1.3.1, `azure-ai-projects` 2.7.0.

## Answer

**Yes.** The accelerator's voice loop runs on the extended API with no Semantic Kernel and no wire event name in the
app. On Voice Live BYOM, a call greets, runs a tool, hands off silently from the receptionist (Ava) to billing (Sonia)
in-session, runs billing's tool, and answers a content-filtered jailbreak with the fallback line through `say()`. The
accelerator's `cc.*` signals rebuild from updates, and every response and tool call nests under one conversation span
under the app's root.

Across 14 live scenarios, 50 checks passed, 1 failed and 15 were recorded as observations. The failure is the known
`FunctionTool.invoke` cancellation gap that [Middleware and telemetry contract for realtime agents](https://github.com/adamdougal/agent-framework/issues/9)
already plans to fix upstream.

The design held. Seven rules broke or needed sharpening once real providers answered (below), each with a contained fix
the prototype applied.

## Results

| Scenario | Provider | Checks | Covers |
| --- | --- | --- | --- |
| `core-loop` | Voice Live BYOM | 18 pass, 1 observed | The ticket's question; #7 topology, voices, suffix, hidden handoff calls; #14 no wire names; #9 trace shape and signals; #17 late transcripts, completion event; #8 BYOM through `query` |
| `manual-handoff` | Voice Live | 4 pass | #7 app-driven `handoff()`, non-participant rejection; #9 `realtime.handoff` span |
| `handoff-rejected` | Voice Live | 5 pass, 1 observed | #7 failed handoff keeps the old agent; #8 error claiming and the lost race |
| `two-handoffs` | Voice Live BYOM | 1 pass | #7 first handoff wins (the model emitted both calls itself) |
| `foundry` | Foundry voice agent | 4 pass, 4 observed | #8 definition capture, definition tools survive; #7 handoff back restores the definition; #9 `traceparent` |
| `barge-in` | Voice Live BYOM | 3 pass | #17 heard text only; #12 `interrupt(played_ms=)`; #9 `item_truncated` event |
| `aoai` | Azure OpenAI | 1 pass, 3 observed | #8 voice lock on handoff, API key as bearer, two-message barge-in; #13 tool gap |
| `slow-tool` | Voice Live BYOM | 3 pass, 1 observed | #16 barge-in during a slow tool; #17 result after its call; #13 tool gap |
| `hang-up-tool` | Voice Live BYOM | 4 pass, **1 fail** | #16 hang-up settles the running call; #9 abandoned tools |
| `pending-transcript` | Voice Live | 2 pass | #17 hang-up with a pending caller transcript; #13 close wait |
| `shared-mcp` | Voice Live | 2 pass | #16 app-wide MCP tools shared by concurrent calls |
| `guardrail` | Voice Live | 1 pass, 1 observed | #9 guardrail that interrupts and drops a response; update-hook cost |
| `instrumentors` | Voice Live | 1 pass, 2 observed | #9 duplicate SDK spans and the warning |
| `vad-cancelled` | Voice Live | 1 pass, 2 observed | #9 empty cancelled responses under VAD |

Evidence for every check, with histories, span trees and filtered wire logs, is in `results/<scenario>.json`.

## What broke

1. **`say()` on Voice Live fails #8's lost-race rule every time.** Voice Live (managed and BYOM) returns
   `metadata: null` on `response.created` for a `pre_generated_assistant_message`, though it echoes metadata on other
   responses. The rule read the missing echo as "the provider dropped the request", so `say()` raised while the line was
   spoken. The prototype takes the first `response.created` after a pre-generated request, which is #12's original fallback.
   **Cost:** on Voice Live, `say()` can't tell whether it lost a race with turn detection. → [Azure provider clients: design and package placement](https://github.com/adamdougal/agent-framework/issues/8)
2. **Deduplicating identical errors swallowed a retried handoff's rejection.** After a rejected handoff, the model called
   the handoff tool again within a second. Its identical `invalid_session_update_message` was dropped as a repeat, so
   the retry waited out the 10 s timeout in silence. Fix: a waiting control claims an error first, and only unclaimed
   errors are deduplicated. Voice Live's triple repeats didn't occur in these runs (0 deduplicated). → #8
3. **`interrupt(played_ms=)` needs slack.** Apps compute `played_ms` from playback clocks and round it. A greeting the
   caller heard in full (10375.9 ms generated, `played_ms=10375`) was cut 1 ms short and stored as `incomplete`. The
   prototype treats anything within 40 ms of the end as fully heard. → #8 and [Live conversation controls: session updates, explicit responses, interruption](https://github.com/adamdougal/agent-framework/issues/12)
4. **Handing back to a Foundry voice agent was rejected until its voice was rebuilt.** The definition carries the voice
   as a name plus a sibling `voice_type`. Re-sending the name alone fails with `invalid_value`. `FoundryVoiceAgent` must
   restore it as an `AzureVoice` object (and `say()` must read the type to know the voice is Azure). → #8, [Handoff model for realtime agents](https://github.com/adamdougal/agent-framework/issues/7)
5. **A Foundry definition published by Semantic Kernel doesn't match Agent Framework tools by name.** The accelerator's
   `receptionist-voice` declares `ReceptionistPlugin-get_current_time` and `Handoff-handoff_to_billing`. #8's name match
   finds no implementations, and `RealtimeHandoffBuilder` adds `handoff_to_billing` beside the definition's
   `Handoff-handoff_to_billing`, so the model sees two handoff tools for one target, one with no implementation. Apps
   migrating from SK must republish their definitions. The builder could warn when a Foundry definition already declares
   tools whose names end in a handoff tool's name. → #7, #8
6. **`azure-ai-projects` 2.7.0 crashes on `connect(agent_version=...)`.** It forwards the keyword into aiohttp's
   `ws_connect`. Sending `x-agent-version-override` as a header, as #8 decided, avoids it. A fourth SDK issue to file.
   → #8
7. **The cancelled tool call's `execute_tool` span stays UNSET** (the one failed check). This confirms #9's
   `FunctionTool.invoke` fix is needed before abandoned tools show as `error.type=conversation_closed` in traces. Updates
   and history already carry it. → #9

## What felt wrong

- **A rejected handoff makes the model retry blindly.** The model saw only `Error: Function failed.` (Agent Framework
  hides exception text by default), retried the handoff twice more, each failing in about 50 ms, then told the caller to
  contact billing directly. The handoff tool should return a result that says the handoff failed and the current agent
  should carry on, rather than raise. → #7
- **A control awaited inside the update loop stalls the reader.** The content-filter fallback runs `say()`, which waits for
  `response.created` while the app isn't reading updates, so audio stops flowing to the caller for that round trip. The
  port starts it as a task. The ADR's samples should do the same.
- **Controls queue behind `interrupt()`.** Because controls are serialised, a handoff queued behind the browser's
  `interrupt()` (cancel plus an acknowledged truncate) took 1.6 s, against about 250 ms otherwise. Truncates could skip
  the acknowledgement wait, as their errors only matter for history.
- **A content-filtered caller turn never gets a final transcript.** On Voice Live BYOM, the jailbreak turn received
  transcription deltas, but after its `input_audio_buffer.committed` neither `…transcription.completed` nor `.failed`
  arrived (2/2 runs). Close's wait ran its full 3 s and history kept the partial text as `incomplete`. Close's wait
  should stop waiting for a turn whose response ended `content_filter`. → [Voice turns in agent-session history](https://github.com/adamdougal/agent-framework/issues/17)
- **`FunctionInvocationContext` can't carry the conversation without a core change.** `_auto_invoke_function` builds
  the context itself, so the prototype sets `realtime_conversation` from an internal function middleware it always
  installs. #16's core change is needed, and tool code needs a `type: ignore` until it lands.

## What held

- **Controls (#12):** updates acknowledged in 48 to 403 ms; rejected updates claimed in about 50 ms (no `event_id` on
  Voice Live errors); `create_response()` during a response raised locally at once; claimed errors streamed only as
  passthrough.
- **Kinds (#14):** the ported loop reads barge-in, transcripts, the content filter (`finish_reason="content_filter"`),
  usage and errors by kind. A regex over `accelerator.py` finds no wire event name.
- **Tools (#16):** one batch per response, then one follow-up; `ctx.realtime_conversation.handoff()` landed before the
  result and follow-up, so the follow-up was credited to billing; an 8 s tool kept running through barge-in, and its
  follow-up waited for turn detection's response; hang-up cancelled it and settled the call with `conversation_closed`
  in history and updates; one orders MCP connection served two concurrent calls.
- **History (#17):** late caller transcripts (500 to 640 ms after the reply was created) stayed ordered before the
  reply; barge-in kept the heard 70 of 939 characters, marked `incomplete`; the slow tool's result stayed straight after
  its call; hang-up kept a transcript that arrived 0.66 s later, or, with no wait, a text-less turn marked
  `transcript_pending`; exchanges reached `InMemoryHistoryProvider` one `after_run` at a time during the call.
- **Handoffs (#7):** per-agent Azure voices switched in-session on Voice Live and Foundry; the suffix, written once on
  the builder, appeared once after each swap; Azure OpenAI dropped the voice and billing spoke in the receptionist's;
  `gpt-realtime-2.1` emitted two handoff calls in one response and the first won; app-driven `handoff()` got its own span
  and rejected a non-participant before sending.
- **Telemetry (#9):** `ws.session` → `invoke_agent contact_centre` → sibling `realtime_inference` and `execute_tool`
  spans, including tool batches; the handoff tool's span carried `gen_ai.transfer.*`; a guardrail hook interrupted a
  response and dropped its 23 remaining updates; hooks cost 12 µs per update with one and 21 µs with 21, audio included.
- **Providers (#8):** Voice Live through `azure-ai-voicelive` `send(Mapping)` and `recv_bytes()`, BYOM through `query`;
  Foundry through `azure-ai-projects` `recv()` and `as_dict()`, with the definition captured 22 to 42 ms after
  `session.created`; Azure OpenAI through `openai` `send_raw()` and `recv_bytes()` on the GA route.

## Wire facts measured

| Fact | Result |
| --- | --- |
| Function call → `response_done` with an Azure TTS voice (Voice Live BYOM) | 1 to 603 ms across 9 calls in 3 runs; #13's upper range of 2.5 s wasn't reached |
| Same with the model's own voice (Azure OpenAI) | 0 to 15 ms |
| Close's wait for a pending caller transcript | 0.66 s; the 3 s cap held, except for content-filtered turns |
| `VoiceLiveInstrumentor` spans in a 30 s call | 1,176, of which 1,170 were `send input_audio_buffer.append`. It needs `azure-core-tracing-opentelemetry` and `settings.tracing_implementation` to emit at all |
| `AIProjectInstrumentor` 2.7.0 on realtime | Doesn't patch `send` or `recv` |
| Voice Live's default turn detection | `server_vad`, 500 ms silence, threshold 0.5: no split. At 200 ms, a long caller turn produced 2 empty responses cancelled with `turn_detected`, each recorded as a `realtime_inference` span |
| Content filtering | BYOM only; managed Voice Live models refused normally (as #13 found), so the accelerator's default setup never triggers its fallback |

## Not settled

- **Barge-in on a two-message reply** (#8's "cut later messages at 0"): `gpt-realtime-2.1` on Azure OpenAI never split
  a reply in 4 attempts while a call was unanswered. The rule is implemented but untested live.
- **An Azure OpenAI API key sent as a bearer token:** both Azure OpenAI resources have `disableLocalAuth`. The client
  sends `api_key` as `Authorization: Bearer`, which works for Entra tokens.
- **Foundry's server-side spans joining the trace through `traceparent`:** the handshake accepted it; the join isn't
  observable without Foundry tracing in Application Insights.
- **Not prototyped:** `run()`, approvals, app-run tools, context-provider instructions and tools, and OpenAI (no key).

## Core changes the prototype made in memory

1. `Content.from_realtime_event(kind=...)` and a `realtime_event` content type. The constructor accepts the type at
   runtime, but `ContentType` and `Content.from_dict` reject it, so a serialised update wouldn't round-trip.
2. `FunctionInvocationContext.realtime_conversation`, set by an internal function middleware (see above).
3. `Content.from_text(status=)`. `Content()` already takes `status`, and text merges keep it.

Unrelated to realtime: Agent Framework's MCP client needs `tenacity`, which `agent-framework-core` doesn't install.
