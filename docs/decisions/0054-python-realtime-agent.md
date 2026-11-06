---
status: proposed
contact: adamdougal
date: 2026-11-06
deciders: eavanvalkenburg
consulted:
informed:
---

# Python realtime agents for direct realtime models

## Context and Problem Statement

Python Agent Framework has mature request/response agent and chat-client abstractions, but no realtime agent API for
live audio and text. Realtime work spans two architectures. In a **direct realtime model** (phase 1), one provider
session takes audio or text in and streams audio, transcripts, tool calls and control events out over a live
connection. In a **sandwich pipeline** (phase 2), speech-to-text feeds a normal agent and text-to-speech renders its
reply. Both look like "a voice agent" to a caller, but they differ in turn detection, barge-in, tool timing and failure
modes, so one conversation model must not hide those differences.

This ADR continues ADR 0035, proposed in [Python: Add realtime agent ADR](https://github.com/microsoft/agent-framework/pull/6831)
([ADR text](https://github.com/eavanvalkenburg/agent-framework/blob/python-realtime-agent-adr/docs/decisions/0035-python-realtime-agent.md))
and closed until someone picked the work up; upstream has since given 0035 to another ADR. It keeps ADR 0035's design,
answers the review threads left open, and adds what a production voice app needs: mid-call session updates, a defined
tool loop, voice history, in-session handoffs, middleware and telemetry, and clients for Azure OpenAI, Voice Live and
Foundry voice agents. A contact-centre solution accelerator's voice path, on Semantic Kernel today, sets the minimum;
ADR 0035 fully covers 22 of its 44 voice capabilities
([checklist](https://github.com/adamdougal/agent-framework/blob/research/accelerator-capability-checklist/docs/research/accelerator-capability-checklist.md)).
The design serves Agent Framework users in general. This ADR records the decisions, their reasons and their costs;
[spec 006](../specs/006-python-realtime-agents.md) holds the exact contract.

Related decisions and designs:

- The [.NET realtime ADR draft](https://github.com/rogerbarreto/agent-framework-public/blob/features/realtime-agent/docs/decisions/0030-realtime-ai-agent.md)
  puts a `RealtimeAgent` over `IRealtimeClient`, with an `AgentConversation`. Python calls its live conversation
  `RealtimeConversation`, to avoid confusion with `AgentSession`.
- [Semantic Kernel ADR 0065](https://github.com/microsoft/semantic-kernel/blob/main/docs/decisions/0065-realtime-api-clients.md)
  chooses an event-centred realtime client and leaves microphone and speaker handling to apps.
- [microsoft/agent-framework#3821](https://github.com/microsoft/agent-framework/pull/3821) closed after reviewers
  objected that its `run` broke `BaseAgent` substitutability.
- [ADR 0021](0021-provider-leading-clients.md) keeps provider SDKs out of core and reaches Azure OpenAI through the
  generic OpenAI clients. [ADR 0033](0033-python-agent-session-identity.md) places identity values by lifecycle.
  [ADR 0048](0048-python-content-identity-and-protocol-metadata.md) groups provider metadata in namespaces.
- Upstream, [Python: Realtime Agent Abstractions](https://github.com/microsoft/agent-framework/issues/6803) tracks this
  work, under the epic microsoft/agent-framework#6715. The original request is microsoft/agent-framework#728.

### Terminology

- **Realtime session**: the provider's live session for one connection, holding the active instructions, tools, voice
  and turn detection. An **agent session** (`AgentSession`) is Agent Framework's persisted state, which outlives it.
- **Live conversation**: full-duplex interaction in which caller input and agent output flow at the same time until
  it's closed. A **half-duplex run** sends input and waits for the complete response.
- **Control call**: an operation the app or a tool issues to steer a live conversation: a session update, an explicit
  response, verbatim speech, an interruption or a handoff.
- **Active agent**: the agent whose instructions, tools and voice are live on a realtime session. An **in-session
  handoff** changes it without reconnecting.
- **First-class signal**: a realtime event whose meaning Agent Framework defines, such as the caller starting to speak.
  A **passthrough event** keeps the provider's own name and has no Agent Framework meaning.
- **Exchange**: input from the caller or the app, with the agent's reply and any tool chain it drives, up to the next
  input. History reaches context providers one exchange at a time.
- **Conversation span**: the `invoke_agent` span covering one live conversation or one `run()`.

## Decision Drivers

- **Agent composition.** Realtime agents compose with agent middleware, telemetry, context providers and the hosts and
  workflows that accept `SupportsAgentRun`.
- **Direct now, sandwich later**, behind one API, without implying they behave the same.
- **Full-duplex fidelity**, with `run(...)` and `run(..., stream=True)` still available for one-turn flows.
- **Provider-neutral signals.** Apps react to barge-in, transcripts, content filtering, usage and errors without
  matching wire event names.
- **In-session control.** Apps and tools change instructions, tools and voice mid-call, with defined ordering against
  tool results and responses, and hand the caller between agents without reconnecting.
- **Function invocation reuse.** Realtime tool calls reuse the tested executor instead of a parallel tool loop.
- **Honest history.** History records what entered the provider's conversation, in spoken order, and only what the
  caller heard.
- **One policy, one trace.** Guardrails and telemetry cover live conversations and `run()` alike.
- **Boundaries.** Provider code stays out of core, live resources stay out of `AgentSession`, and device drivers stay
  in samples.
- **Additive and experimental.** Stable APIs change only additively, and new APIs ship behind feature flags.

## Considered Options

- **Option 1: Add `RealtimeAgent` as the realtime-specific agent type**
- **Option 2: Extend `Agent` to also be the realtime agent type**

## Decision Outcome

Chosen option: **Option 1: Add `RealtimeAgent` as the realtime-specific agent type**, because it keeps the live
lifecycle explicit and sets the phase 1 API for direct realtime models, which a phase 2 pipeline can sit behind later.

```python
agent = RealtimeAgent(
    client=OpenAIRealtimeClient(model="gpt-realtime"),
    instructions="You are a helpful voice assistant.",
    tools=[get_weather],
    default_options={"voice": "alloy"},
)

async with agent.start_conversation() as conversation:
    await conversation.send("Hi there, I'm based in Amsterdam.")
    async for update in conversation:
        for content in update.contents:
            if content.type == "text" and content.status == "completed":
                print(f"{update.role}: {content.text}")
```

The sample prints final transcripts only. ADR 0035's sample printed every `update.text`, which prints each transcript
twice, once as deltas and once as the final text.

### Agent and live conversation

`RawRealtimeAgent` derives from `BaseAgent`, not the chat `RawAgent`, whose chat options and loop realtime would
otherwise inherit. `RealtimeAgent` adds realtime middleware and telemetry layers. `start_conversation(...)` returns
`RealtimeConversation`, an async context manager that owns the socket, the single receive loop and a bounded update
queue, and yields `AgentResponseUpdate` values; it never lives in `AgentSession`. A realtime agent starts the
conversation, and its active agent can change through handoffs. `run(...)` is a half-duplex run over an ephemeral
realtime session, so the agent satisfies `SupportsAgentRun`.

Both entry points take `options=`, `tools=` and `middleware=` as named parameters. Hosts such as AG-UI, the MCP agent
tool, Foundry hosting, declarative workflows and group chat pass `options=` or `tools=`, though `SupportsAgentRun`
declares neither. That protocol gap affects every protocol-only agent, `CopilotStudioAgent` included, and stays out of
scope. Option names reuse Agent Framework's normalized names, and clients drop the keys they can't honor, with a
warning, because realtime `session.update` rejects unknown fields. A catch-all `**kwargs` was rejected, because it hides
the contract. See [API shapes](../specs/006-python-realtime-agents.md#api-shapes).

### Acknowledged control calls

`update_session(...)`, `create_response(...)`, `say(...)`, `interrupt(played_ms=...)` and `handoff(...)` are typed
methods on the conversation and on the provider session, so provider clients own the wire translation. Everything else
stays a raw provider event sent through `send(...)`. Control calls are serialized, and each waits for the provider's
acknowledgement, so a response requested after `update_session(...)` returns uses the new session, and rejections reach
the caller, such as a handoff tool. The cost is one round trip per call, 53 to 376 ms for `session.updated`.
Fire-and-forget updates were rejected, because their ordering would rest on undocumented in-order processing.

Many Azure errors carry no client `event_id`, so the call that's waiting claims them. Barge-in cuts every unheard
message of the response. `interrupt(...)` waits for its truncate acknowledgements. Not waiting would stop it holding up
the next control call. In the prototype, a handoff queued behind it took 1.6 s instead of about 250 ms. But an
unacknowledged truncate's error would then fail the next waiting call, because it carries no `event_id` on Voice Live,
or on Azure OpenAI for bad offsets. See [Control calls](../specs/006-python-realtime-agents.md#control-calls).

### Event kinds

Provider clients tag every event with an Agent Framework kind, and keep `service_event_type` verbatim. Payloads use
existing content types, pure signals become `realtime_event` content, and everything else passes through under its
wire name. Kinds replace ADR 0035's `event_type` categories, so apps never match wire names. Under ADR 0035, the
accelerator's four most-used signals (barge-in, both transcripts, content filtering) needed provider-specific string
matching. Final transcripts carry `status="completed"`, and content filtering arrives as
`finish_reason="content_filter"`. Renaming Voice Live's preview events to GA names was rejected, because it would make
OpenAI's wire protocol the neutral API that phase 2 and other providers must imitate. Provider-defined kinds were
rejected, so the set stays one that core understands. See
[Event kinds](../specs/006-python-realtime-agents.md#event-kinds).

### Tool loop

Each response's tool calls run as one batch through the existing `_execute_function_calls` once `response_done`
arrives, in a background task beside the receive loop, followed by one follow-up response. That keeps chat's batch
rules (approvals, declaration-only tools, concurrency, function middleware) at little cost, because `response.create`
can't go out while a response is active anyway. The cost shows with Azure TTS voices, where `response.done` can follow a
call by up to 2.5 s. Starting each call as it arrives, as the OpenAI Agents SDK, LiveKit and Pipecat do, was rejected
for losing those rules, and so was a follow-up per result, which fails when one response holds several calls.

Tools reach the conversation through `FunctionInvocationContext.realtime_conversation`, so a handoff tool's session
update takes effect before its result. Results are always strings. The "Azure Realtime hangs on non-string results"
that Semantic Kernel apps work around isn't a protocol difference. Semantic Kernel sent no output for dict results, so
the model waited. Barge-in never cancels running tools, because most barge-ins are backchannels and a half-done payment
is worse than a reported one. Close cancels and settles them, and a paused `run(...)` keeps its calls for the next run.
Approvals and local MCP tools are in scope, with `Agent`'s MCP lifecycle. See
[Tool loop](../specs/006-python-realtime-agents.md#tool-loop).

### Voice history

History holds the provider conversation's authored turns as text, in spoken order, with only what the caller heard of a
cut-off turn. A live conversation reports history to context providers one exchange at a time during the call, through
the same `after_run` a run uses, and every close path reports the rest. This replaces ADR 0035's authored-turn ledger
finalized at close. Reporting per exchange keeps chat's inputs-then-outputs order, keeps all but the last exchange if
the process dies, and keeps close fast. Storing audio, ordering by arrival (late transcripts land after their replies)
and dropping text-less caller turns (replies look unprompted) were rejected. See
[History](../specs/006-python-realtime-agents.md#history).

### In-session handoffs

`RealtimeConversation.handoff(...)` swaps instructions, tools, voice and tool choice in the same realtime session and
emits `handoff_completed`. `RealtimeHandoffBuilder` in `agent-framework-orchestrations` shares `HandoffBuilder`'s
topology API and builds a realtime agent that owns the call; history, context and limits stay with it. `HandoffBuilder`
stays chat-only. It clones chat agents, finds handoffs in finished responses and runs one bounded turn per participant,
so a realtime agent inside it would reconnect every turn, while every framework surveyed hands off by updating the live
session. Generalizing `HandoffBuilder` was rejected, because it can only give per-turn reconnects and would make
`build()`'s return type depend on its participants. Agent-declared handoffs were rejected, because they add a topology
concept chat `Agent` lacks. A handoff lasts for its realtime session, and a voice the provider can't change never fails
a handoff. See [Handoffs](../specs/006-python-realtime-agents.md#handoffs).

### Middleware and telemetry

Full-duplex traffic enters middleware and telemetry at these points:

| Traffic | Middleware | Telemetry |
| --- | --- | --- |
| A whole `run(...)` | agent middleware | the conversation span |
| A realtime session opening, live or in `run(...)` | conversation middleware's `process()` | the conversation span, and the session-started event |
| Inbound `send(...)` | conversation middleware's send hooks | input on `realtime_inference` spans (sensitive data on) |
| Outbound updates | conversation middleware's update hooks | `realtime_inference` spans, events and metrics |
| Tool calls | function middleware | `execute_tool` spans |
| Control calls | none | `realtime.*` control spans |
| Close | conversation middleware's close hooks | the end of the conversation span, and the session-ended event |

Telemetry never records audio, even with sensitive data on, and records transcripts instead. Middleware sees audio as
`data` content and must not assume an update has text.

Conversation middleware (`ConversationMiddleware`) is a new category, so one guardrail covers live calls and `run()`.
Agent middleware can't wrap a live call safely, because it reads a final result and may re-enter `call_next()`.
Telemetry lives at the agent layer, in core. Each call is one `invoke_agent` span, with a `realtime_inference` span per
response and its tool and control spans beneath it, following semantic-conventions-genai at `750e42c`, which is
unreleased. Client-level spans, the Microsoft.Extensions.AI model, were rejected, because they have no agent context. A
cancelled tool call now counts as a failure. See [Middleware](../specs/006-python-realtime-agents.md#middleware) and
[Telemetry](../specs/006-python-realtime-agents.md#telemetry).

### Provider clients

Azure OpenAI is Azure routing on `OpenAIRealtimeClient`, as chat has routed Azure since ADR 0021. Voice Live gets
`VoiceLiveClient`, pinned to the documented `2026-07-15`. Foundry voice agents get a first-class `FoundryVoiceAgent`,
because a handoff topology needs the start agent's published definition. Both Azure clients live in
`agent-framework-foundry` behind a `[realtime]` extra. All of them share `OpenAIRealtimeSession`, a public GA-protocol
base session in `agent-framework-openai`, which maps wire JSON over each service's SDK used only as a transport. The
SDKs add little weight, carry their handshake hardening, and fix only the URL path
([evidence](https://github.com/adamdougal/agent-framework/blob/research/azure-realtime-transports/docs/research/azure-realtime-transports.md)),
so a raw websocket was rejected. Providers differ by service and voice type, and a capability table documents the
differences rather than a typed capability API. See
[Provider clients](../specs/006-python-realtime-agents.md#provider-clients) and
[Provider capabilities](../specs/006-python-realtime-agents.md#provider-capabilities).

### Experimental features and release

One `ExperimentalFeature.REALTIME` covers the family, so it graduates together. The Foundry voice agent classes use
`FOUNDRY_VOICE_AGENTS`, so the family needn't wait for a preview service. This ADR proposes shipping the family as
experimental in 1.x rather than waiting for the next major version; changes to stable APIs are additive (see
[Changes to existing packages](../specs/006-python-realtime-agents.md#changes-to-existing-packages)). The maintainers
make the final call.

### Phase 2 and deferred decisions

In phase 2, a `RealtimePipeline` composes speech-to-text, an inner `SupportsAgentRun` and text-to-speech behind the same
`RealtimeConversation` API, with tools on the inner agent. It ships only after a concrete provider or sample validates
the speech-to-text and text-to-speech abstractions.

Each of these can be added later without breaking anything: warm connection reuse, WebRTC and SIP, device drivers,
cancelling tools on barge-in, per-tool timeouts and filler speech, first-class out-of-band responses, resuming
provider-side conversations, remembering the active agent across realtime sessions, structured inputs and a per-session
`store` override for Foundry voice agents, a typed capability API, and further telemetry such as `user_speech` spans and
realtime metrics. Handoffs that change model or provider, transcription-only and translation sessions, Voice Live's
agent-integration mode, and publishing Foundry agent definitions are out of scope. There's no `FoundryRealtimeClient` in
v1, because `OpenAIRealtimeClient(azure_endpoint=...)` already reaches a Foundry resource's Azure OpenAI endpoint, and
nobody has verified that the project-scoped route serves realtime.

### Consequences

- Good, because callers get one live conversation concept, and provider code stays outside core.
- Good, because apps react to barge-in, transcripts, content filtering, usage and errors without matching wire event
  names. The prototype's port of the accelerator's voice loop contains none.
- Good, because acknowledged control calls make a handoff's session update take effect before its tool result and
  follow-up, and rejections reach the caller.
- Good, because handoffs happen in-session, and history records what the caller heard, in spoken order, during the call.
- Good, because one conversation middleware policy covers live calls and `run()`, and each call is one trace.
- Neutral, because the docs must spell out how providers differ, or the shared API hides it.
- Neutral, because telemetry follows unreleased conventions pinned at a commit, which may still change.
- Bad, because live resources bring lifecycle concerns that chat agents don't have, and sandwich pipelines need more
  cancellation and backpressure plumbing.
- Bad, because every control call costs a round trip, and control calls queue behind `interrupt(...)`'s truncate
  acknowledgements.
- Bad, because tool batches wait for `response_done`, up to 2.5 s after a call with Azure TTS voices.
- Bad, because a handoff lasts only for its realtime session, so a host calling `run(...)` per turn restarts at the
  start agent each turn. Some providers keep the old voice, and of two handoffs in one response only the first happens.
- Bad, because history estimates the heard part of a cut-off turn proportionally, every `run(...)` replays the whole
  history, and a context provider may see an exchange twice after a retry.
- Bad, because exporters receive the conversation span only at close, so a crash loses it. History and telemetry don't
  see changes that conversation middleware's update hooks make. .NET instruments realtime at the client and Python at
  the agent, so their traces differ.
- Bad, because on Voice Live a pre-generated `say(...)` can't detect a lost race, and Foundry definitions published by
  Semantic Kernel need republishing.
- Bad, because the `[realtime]` extras constrain dependencies: `websockets` below 16, and `azure-ai-projects` 2.7 with
  `openai>=3`.

## Validation

1. Review of this ADR and [spec 006](../specs/006-python-realtime-agents.md) validates the design before
   implementation.
2. A [prototype](https://github.com/adamdougal/agent-framework/tree/prototype/realtime-voice-loop/python/packages/realtime-prototype)
   ran the accelerator's voice loop on this design against Voice Live (managed and BYOM), Azure OpenAI and a Foundry
   voice agent: 14 live scenarios, with 50 checks passed, 1 failed and 15 observations
   ([findings](https://github.com/adamdougal/agent-framework/blob/prototype/realtime-voice-loop/python/packages/realtime-prototype/FINDINGS.md)).
   The failure is the `FunctionTool.invoke` cancellation gap this design fixes. `run()`, approvals, app-run tools,
   context-provider instructions and tools, barge-in on a two-message reply, and OpenAI weren't exercised.
3. The implementation's unit, regression and live integration tests follow
   [the spec's validation section](../specs/006-python-realtime-agents.md#validation).

## Pros and Cons of the Options

### Option 1: Add `RealtimeAgent` as the realtime-specific agent type

- Good, because the live lifecycle stays explicit, and `Agent` stays focused on request/response chat clients.
- Good, because `start_conversation(...)` and realtime options aren't grafted onto every `Agent`.
- Good, because direct realtime can ship first, with sandwich pipelines behind the same API later.
- Neutral, because users must learn when to use `Agent` and when `RealtimeAgent`, and chat orchestrations that need a
  concrete `Agent`, such as `HandoffBuilder`, need realtime counterparts.
- Bad, because the shared abstraction must stay narrow, and integrations that special-case `Agent` may need to
  generalize.

### Option 2: Extend `Agent` to also be the realtime agent type

- Good, because users learn one agent type.
- Bad, because `client=` would mean three different lifecycles: chat, live realtime and composed pipelines.
- Bad, because every `Agent` would carry realtime-only APIs, and `Agent[ChatOptions]` typing would have to cover
  realtime options.
- Bad, because live resource lifetime is easy to miss behind the chat-agent constructor.

## More Information

### What changed from ADR 0035

| ADR 0035 | This ADR |
| --- | --- |
| A conversation "owned by a RealtimeAgent" | Started by a realtime agent, whose active agent can change |
| An argless `interrupt()` and untyped commands | Acknowledged control calls with error claiming, and `interrupt(played_ms=...)` |
| `run(...)` keywords and experimental scope open | Explicit `options=`, `tools=`, `middleware=`; one `REALTIME` flag |
| `RealtimeSessionOptions` | `RealtimeOptions`, with `create_response` inside turn detection |
| `event_type` categories | Agent Framework kinds, with `service_event_type` verbatim |
| A refactored shared executor | `_execute_function_calls` reused, with realtime loop rules |
| An authored-turn ledger finalized at close | Per-exchange reporting during the call |
| Handoffs deferred | In-session `handoff(...)` and `RealtimeHandoffBuilder` |
| Realtime middleware deferred; `RawOpenAIRealtimeClient` | Conversation middleware; telemetry at the agent layer |
| OpenAI first | OpenAI, Azure OpenAI, Voice Live and Foundry voice agents, with a capability table |

ADR 0035's comparisons with Semantic Kernel and .NET, its protocol sketch and its data-model appendix remain in its
[original text](https://github.com/eavanvalkenburg/agent-framework/blob/python-realtime-agent-adr/docs/decisions/0035-python-realtime-agent.md).

### Review threads on ADR 0035

| Thread | Answered in |
| --- | --- |
| [`run(...)` keywords that hosts pass](https://github.com/microsoft/agent-framework/pull/6831#discussion_r3953440204) | [Agent and live conversation](#agent-and-live-conversation) |
| [Experimental scope](https://github.com/microsoft/agent-framework/pull/6831#discussion_r3953440210) | [Experimental features and release](#experimental-features-and-release) |
| [Full-duplex middleware and telemetry](https://github.com/microsoft/agent-framework/pull/6831#discussion_r3953440213) | [Middleware and telemetry](#middleware-and-telemetry), the interception points |
| [Audio-only middleware and telemetry](https://github.com/microsoft/agent-framework/pull/6831#discussion_r3667773087) | [Middleware and telemetry](#middleware-and-telemetry), the audio rule |
| [Handoff compatibility](https://github.com/microsoft/agent-framework/pull/6831#discussion_r3852979100) | [In-session handoffs](#in-session-handoffs) |

### Open questions

- **Naming alignment with .NET.** .NET's draft calls its live conversation `AgentConversation` and has no counterparts
  yet for `handoff(...)`, `handoff_completed` or `RealtimeHandoffBuilder`.
- **Release.** Shipping in 1.x rather than the next major version is the maintainers' call.
- **Settings.** The acknowledgement timeout and the update queue's capacity are set at start. Their parameter
  names, and the queue's default capacity, are left to the implementation.

### Evidence

Research findings, on throwaway branches of a fork:
[capability checklist](https://github.com/adamdougal/agent-framework/blob/research/accelerator-capability-checklist/docs/research/accelerator-capability-checklist.md),
[Azure protocols](https://github.com/adamdougal/agent-framework/blob/research/azure-realtime-protocols/docs/research/azure-realtime-protocols.md),
[Azure transports](https://github.com/adamdougal/agent-framework/blob/research/azure-realtime-transports/docs/research/azure-realtime-transports.md),
[handoff survey](https://github.com/adamdougal/agent-framework/blob/research/realtime-handoff-survey/docs/research/realtime-handoff-survey.md),
[middleware and telemetry](https://github.com/adamdougal/agent-framework/blob/research/realtime-middleware-telemetry/docs/research/realtime-middleware-telemetry.md),
[telemetry mapping](https://github.com/adamdougal/agent-framework/blob/research/realtime-telemetry-mapping/docs/research/realtime-telemetry-mapping.md)
and [live wire probes](https://github.com/adamdougal/agent-framework/blob/research/realtime-wire-probes/docs/research/realtime-wire-probes.md).

### Existing telemetry gaps outside this design

These affect chat today and should be raised separately: spans emit `finish_reasons` as a string, say `tool_calls`
where the conventions say `tool_call`, still emit `gen_ai.system`, and say `cache_creation` where the conventions say
`cache_write`; sensitive-data capture writes `data` content as base64; the Chat Completions client drops audio token
counts; `AgentTelemetryLayer` creates histograms it never records; and sessions with local history have no
`gen_ai.conversation.id`.
