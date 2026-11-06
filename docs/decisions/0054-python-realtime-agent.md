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

Python Agent Framework has mature request/response agent and chat-client abstractions:
`Agent`, `BaseAgent`, `BaseChatClient`, `SupportsChatGetResponse`, `AgentSession`, `AgentResponse`,
`AgentResponseUpdate`, `Content`, `ResponseStream`, middleware, tools, telemetry, and history providers. It does not
yet have a realtime agent API for live audio/text interaction.

Realtime agent work needs to account for two related but different architectures:

1. **Direct realtime model (phase 1)**: one model/provider session accepts audio/text input and emits audio,
   text/transcripts, function calls, and control events over a live bidirectional connection. OpenAI Realtime-style APIs
   are the first target.
2. **Sandwich setup (phase 2)**: speech-to-text (STT) converts live audio into text, a normal Agent Framework agent
   reasons and calls tools over that text, and text-to-speech (TTS) renders the agent output back to audio.

Both can look like "a voice agent" to a caller, but they do not have the same semantics. Direct realtime models may own
server-side VAD, barge-in, session continuation, and tool-call timing. Sandwich pipelines have three independently
failing components, more explicit cancellation boundaries, and tool calls usually happen in the inner text agent. The
Python architecture needs one caller-facing realtime conversation model where that is honest, without hiding the
capability differences.

This ADR continues ADR 0035, which
[Python: Add realtime agent ADR](https://github.com/microsoft/agent-framework/pull/6831) proposed
([ADR text](https://github.com/eavanvalkenburg/agent-framework/blob/python-realtime-agent-adr/docs/decisions/0035-python-realtime-agent.md))
and then closed until someone picked the work up. Upstream has since given the number 0035 to another ADR, so the design
continues as ADR 0054. It keeps ADR 0035's design, answers the review threads that were open when its PR closed, and
extends it with what a production voice application needs: mid-call session updates, a defined tool loop, voice history,
in-session handoffs, a middleware and telemetry contract, and clients for Azure OpenAI, Azure Voice Live and Foundry
voice agents. [What changed from ADR 0035](#what-changed-from-adr-0035) lists the differences.

The voice path of a contact-centre solution accelerator, which runs on Semantic Kernel today, sets the minimum
requirements: three providers, barge-in, tool calls, in-session handoffs between agents with their own voices,
content-filter fallback speech and one trace per call. ADR 0035 fully covers 22 of the 44 voice capabilities that path
relies on ([capability checklist](https://github.com/adamdougal/agent-framework/blob/research/accelerator-capability-checklist/docs/research/accelerator-capability-checklist.md)).
The design serves Agent Framework users in general, so the accelerator is a lower bound, not the scope.

The decisions rest on research into the providers' protocols and transports, other frameworks' handoffs and the
telemetry conventions; on live wire probes against Azure OpenAI, Voice Live and a Foundry voice agent; and on a
prototype that ran the accelerator's voice loop on this design against those services.
[More Information](#more-information) links each.

Related prior decisions and designs:

- The .NET realtime ADR proposes a `RealtimeAgent` over `IRealtimeClient` as a subclass of `AIAgent`, with an
  `AgentConversation` type for full-duplex use. Python uses the name `RealtimeConversation` to avoid confusion with
  `AgentSession`.
- Python does not have a direct `AIAgent` equivalent. The compatibility target for half-duplex scenarios is the
  structural `SupportsAgentRun` protocol.
- Semantic Kernel ADR 0065 chooses an event-centered realtime client model and explicitly avoids owning microphone and
  speaker device handling in the runtime.
- [Python: Add realtime voice agents with OpenAI, Azure OpenAI and Voice Live clients](https://github.com/microsoft/agent-framework/pull/3821)
  closed after reviewers objected that its `run` broke `BaseAgent` substitutability. Here `run(...)` stays a
  half-duplex run, and live use goes through `start_conversation(...)`.
- [ADR 0021](0021-provider-leading-clients.md) keeps core free of provider SDKs and reaches Azure OpenAI through the
  generic OpenAI clients. [ADR 0033](0033-python-agent-session-identity.md) places identity values by lifecycle.
  [ADR 0048](0048-python-content-identity-and-protocol-metadata.md) groups provider metadata in per-provider namespaces.
- Upstream, [Python: Realtime Agent Abstractions](https://github.com/microsoft/agent-framework/issues/6803) tracks this
  work, under the epic microsoft/agent-framework#6715. The original request is microsoft/agent-framework#728.

### Terminology

General:

- **Realtime interaction**: Live communication in which input and output can overlap in time. This is different from a
  normal request/response chat call, even when the output is streamed.
- **Direct realtime model**: A provider-native live model session that accepts audio/text input and emits audio, text,
  transcripts, tool calls, and control events over one bidirectional connection.
- **Sandwich pipeline**: A composed flow where speech-to-text produces text for an agent and text-to-speech renders the
  agent output back to audio.
- **Speech-to-text (STT)**: A component that turns incoming audio into text or transcript events.
- **Text-to-speech (TTS)**: A component that turns outgoing text into audio.
- **Realtime agent**: An agent holding a live, two-way speech conversation with a speech-to-speech model.
- **Provider**: The realtime service behind a realtime agent: OpenAI Realtime, Azure OpenAI Realtime or Azure Voice
  Live.
- **Foundry voice agent**: An agent published in Microsoft Foundry as a voice agent and reached at its own Voice Live
  endpoint. Its tools still run in the app.
- **Agent definition**: A Foundry voice agent's published instructions, voice, tools and tool choice. They apply to each
  realtime session until it overrides them.
- **Managed model**: A model the Azure voice service provides, so no deployment of your own is needed. Voice Live can
  instead use your own Azure OpenAI deployment ("bring your own model", BYOM).
- **Host**: A component that runs an agent on behalf of an external protocol or UI, such as AG-UI, MCP, A2A, Foundry
  hosting or DevUI.

Sessions and conversations:

- **Realtime session**: The provider's live session for one connection, holding the active instructions, tools, voice
  and turn detection.
- **Agent session**: Agent Framework's persisted conversation state (`AgentSession`), which outlives any realtime
  session.
- **Half-duplex run**: One bounded turn, which sends input and then waits for the complete response.
- **Live conversation**: Full-duplex interaction in which caller input and agent output flow at the same time until
  it's closed.
- **Durable session state**: Serializable state used to continue future calls. It must not hold live sockets, receive
  tasks, platform audio resources, or other live realtime objects.
- **Turn detection / VAD**: Voice activity detection that decides when the caller started or stopped speaking and
  whether a model response should be created automatically.
- **Barge-in**: The caller speaking over the agent, which must stop playback and discard the unheard rest of the
  response.

Controls and handoffs:

- **Control call**: An operation the app or a tool issues to steer a live conversation: a session update, an explicit
  response, verbatim speech, an interruption or a handoff. A raw provider event sent through `send()` is a *command*,
  not a control call.
- **Session update**: Changing a live realtime session's instructions, tools, voice or other settings mid-call, without
  reconnecting.
- **Verbatim speech**: The agent speaking app-supplied text word for word instead of generating a reply.
- **Active agent**: The agent whose instructions, tools and voice are live on a realtime session. Responses created
  while it's active are credited to it, and a handoff changes it.
- **Handoff**: Moving the caller from the active agent to another agent. An **in-session handoff** keeps the same
  realtime session and swaps instructions, tools, voice and tool choice, so the caller hears no reconnect. In a contact
  centre, *transfer* means moving the call to a person or another line, so this ADR doesn't call handoffs transfers.
- **Participant**: A realtime agent in a handoff topology. It supplies the settings that define who's speaking: its
  instructions, tools, voice and tool choice. The call's history, context and limits belong to the agent that started
  it.

Signals:

- **First-class signal**: A realtime event whose meaning Agent Framework defines, so every provider reports it the same
  way, such as the caller starting to speak or a response finishing.
- **Passthrough event**: A provider event that Agent Framework gives no meaning to, delivered under the provider's own
  event name.
- **Caller transcript**: The provider's text of what the caller said. It streams in pieces, then arrives once in full
  when final.
- **Agent transcript**: The text of what the agent said, whether it spoke it or wrote it. It streams in pieces, then
  arrives once in full when final.

Tools:

- **Tool batch**: The tool calls from one model response, run together once that response finishes.
- **Follow-up response**: The response the tool loop requests after sending a batch's results, so the model can act on
  them.
- **Tool chain**: The follow-up responses driven by tool calls after one caller turn or app input. The tool loop's
  limits apply per chain.
- **Settled call**: A tool call that has its final result, which is either what the tool returned or an error recorded
  because the call couldn't run or finish.
- **Hosted tool**: A tool the provider runs in its own service, such as a provider-run MCP server. Agent Framework
  declares it but never runs it.

History:

- **Authored turn**: A caller, agent or tool turn that entered the provider's conversation. History records authored
  turns as text; signals and passthrough events never enter it.
- **Exchange**: Input from the caller or the app, together with the agent's reply and any tool chain it drives, up to
  the next input. A live conversation reports history one exchange at a time, as `run()` reports one run.
- **Cut-off turn**: An agent turn the caller talked over. History keeps only the part the caller heard, marked
  incomplete.

Middleware and telemetry:

- **Conversation middleware**: Middleware that applies to a realtime session as a whole, live or inside `run()`. It sees
  the session's inbound sends, outbound updates and close.
- **Conversation span**: The `invoke_agent` span covering one live conversation or one `run()`. The conversation's
  responses, tool calls and control calls are its child spans.

## Decision Drivers

- **Agent composition.** Realtime agents should still compose with existing agent middleware, telemetry, context
  providers, and protocol-based workflows/utilities where their contracts accept `SupportsAgentRun`.
- **Direct and sandwich coverage.** The architecture should handle both direct realtime model sessions and
  STT-agent-TTS pipelines in a later phase.
- **Different semantics stay visible.** A shared API must not imply identical latency, interruption, continuation,
  or tool-call behavior across direct and sandwich implementations, or across providers and voice types.
- **Full-duplex fidelity.** Live send and receive must both be possible without flattening realtime into a simple
  request/response chat call.
- **Half-duplex compatibility.** Callers should still be able to use `run(...)` and `run(..., stream=True)` for simple
  one-turn flows, including under hosts that pass `options` and `tools`.
- **No event loss.** Every provider or pipeline signal must be representable at the Agent Framework layer, with raw
  provider objects available as an escape hatch.
- **Provider-neutral signals.** Apps react to barge-in, transcripts, content filtering, usage and errors without
  matching any provider's wire event names.
- **In-session control.** Apps and tools change instructions, tools and voice mid-call, with defined ordering against
  tool results and the next response, and hand the caller between agents without reconnecting.
- **Function invocation reuse.** Direct realtime model tool calls should reuse the existing tested function-invocation
  behavior rather than duplicate approval, middleware, declaration-only, progressive-tool, budget, and error logic.
- **Honest history.** History records what entered the provider's conversation, in spoken order, and only what the
  caller heard.
- **One policy, one trace.** Guardrails and telemetry cover live conversations and `run()` alike, and each call is one
  trace.
- **Provider separation.** Core should contain provider-neutral abstractions only; provider implementations belong in
  provider packages.
- **Live resource boundaries.** Live sockets, audio streams, and receive tasks are disposable runtime resources and do
  not belong in serializable `AgentSession` state.
- **Practical media integration.** Local microphone/speaker loops and hosted media streams such as Azure Communication
  Services Call Automation should be easy to wire without users rebuilding the same bridge code each time.
- **Audio device neutrality.** Framework core should define audio bridge seams, but concrete microphone/speaker drivers
  should live in samples or optional integrations.
- **Additive and experimental.** Changes to stable APIs are additive, and new public APIs ship behind experimental
  feature flags.

## Considered Options

- **Option 1: Add `RealtimeAgent` as the realtime-specific agent type**
- **Option 2: Extend `Agent` to also be the realtime agent type**

## Pros and Cons of the Options

### Option 1: Add `RealtimeAgent` as the realtime-specific agent type

Add `RawRealtimeAgent` / `RealtimeAgent` as first-class agent types for realtime. They should satisfy
`SupportsAgentRun` for half-duplex compatibility and reuse the same base/middleware/telemetry stack where practical, but
their constructor and methods can be explicit about live realtime behavior.
Illustrative direct-model and sandwich setup snippets are in the appendix.

- Good, because it covers the direct realtime model first without forking the future sandwich pipeline concept.
- Good, because the public type advertises that this agent starts live realtime sessions.
- Good, because `Agent` can remain focused on chat-style request/response clients.
- Good, because `start_conversation(...)` and realtime-specific options do not have to be grafted onto every `Agent`.
- Good, because it keeps live resource lifetime explicit.
- Good, because direct providers can preserve provider events while sandwich pipelines can emit pipeline events.
- Good, because it allows direct realtime first and leaves sandwich architecture for phase 2.
- Neutral, because users must learn when to use `Agent` versus `RealtimeAgent`.
- Neutral, because chat orchestrations that need a concrete `Agent`, such as `HandoffBuilder`, need realtime
  counterparts.
- Bad, because the common abstraction has to be narrow and carefully policed; otherwise it becomes a leaky lowest-common
  denominator.
- Bad, because framework integrations that special-case the concrete `Agent` type may need to generalize to the agent
  protocol/base class.

### Option 2: Extend `Agent` to also be the realtime agent type

Reuse `Agent` as the only public agent type. `Agent` would accept either a chat client, a direct realtime client, or a
sandwich realtime pipeline, and would grow realtime-specific APIs such as `start_conversation(...)`.
Illustrative direct-model and sandwich setup snippets are in the appendix.

- Good, because users have one public agent type to learn.
- Good, because existing `Agent(...)` samples and mental model remain the starting point.
- Neutral, because `Agent` would need to accept multiple client protocols with different lifecycle semantics.
- Bad, because it overloads the meaning of `client=`. Chat clients are request/response, realtime clients own live
  full-duplex sessions, and sandwich pipelines are composed systems.
- Bad, because `Agent` would need realtime-only APIs such as `start_conversation(...)` even when used with normal chat
  clients.
- Bad, because type checking and overloads become harder when `Agent[ChatOptions]` and realtime options have different
  execution shapes.
- Bad, because live resource lifetime is easier to miss when hidden behind the existing chat-agent constructor.

## Decision Outcome

Chosen option: **Option 1: Add `RealtimeAgent` as the realtime-specific agent type**, because it keeps the live realtime
lifecycle explicit while establishing the phase 1 API for direct realtime model sessions. The same API should be able to
host a sandwich STT-agent-TTS pipeline later, but that is phase 2.

Direct realtime model setup should look like this:

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
        ...
```

In phase 2, sandwich setups should use a concrete `RealtimePipeline` class that composes speech-to-text, an inner
`SupportsAgentRun`, and text-to-speech. The constructor order should follow the audio-to-audio flow:

```python
pipeline = RealtimePipeline(
    speech_to_text=speech_to_text_client,
    agent=text_agent,
    text_to_speech=text_to_speech_client,
)

agent = RealtimeAgent(client=pipeline)
```

This requires adding provider-neutral speech-to-text and text-to-speech abstractions in addition to the
`RealtimePipeline` class itself. Those abstractions are explicitly **not phase 1** unless a concrete STT/TTS provider or
sample is selected to validate them with the first implementation.

The appendices compare the design with Semantic Kernel Python realtime and show canonical code shapes, so the main
decision stays readable while the proposed API stays concrete.

The selected architecture has these parts:

- [Agent-level live conversation](#agent-level-live-conversation)
- [Live conversation controls](#live-conversation-controls)
- [Audio I/O and hosted media bridges](#audio-io-and-hosted-media-bridges)
- [Direct realtime model path](#direct-realtime-model-path), with its [options](#options)
- [Event taxonomy](#event-taxonomy)
- [Tool execution](#tool-execution)
- [Session, identity and voice history](#session-identity-and-voice-history)
- [Handoffs](#handoffs)
- [Middleware and telemetry](#middleware-and-telemetry)
- [Provider clients and packages](#provider-clients-and-packages), and the
  [provider capability table](#provider-capabilities)
- [Experimental features and release](#experimental-features-and-release), and the
  [changes to existing packages](#changes-to-existing-packages)
- [Half-duplex behavior](#half-duplex-behavior)
- [Phase 2: Sandwich STT-agent-TTS path](#phase-2-sandwich-stt-agent-tts-path)
- [Deferred decisions](#deferred-decisions)

### Agent-level live conversation

Introduce an experimental `RealtimeConversation` as Agent Framework's live full-duplex API. It is an async context
manager / async disposable object, not an `AgentSession`. A realtime agent starts it with `start_conversation(...)`. The
agent that starts it keeps the call's history, context providers, conversation middleware and tool-loop limits for the
whole call. The conversation's **active agent**, whose instructions, tools and voice are live, can change through
[handoffs](#handoffs).

`RealtimeConversation` owns the live connection and exposes operations for:

- sending user text, messages, audio content, app-supplied tool results and approvals, or explicit raw realtime events;
- steering the realtime session with control calls: `update_session(...)`, `create_response(...)`, `say(...)`,
  `interrupt(...)` and `handoff(...)`;
- receiving a single ordered stream of `AgentResponseUpdate` values through `async for update in conversation`, with
  `ResponseStream` used only when a final `AgentResponse` can be defined;
- reading who is speaking (`active_agent`) and the call's history so far (`history`);
- closing the live resources.

The type must make its lifecycle clear. Callers must close it or use `async with`.

`RealtimeConversation` is also the convenience boundary. It may accept `Message`, `Content`, strings, and audio helper
inputs and translate them into provider-level realtime events. Provider sessions should not own these agent-level
conveniences.

`RealtimeConversation.send(...)` normalization must be deterministic:

| Input to `send(...)` | Normalization |
| --- | --- |
| `str`, `Content`, `Message`, or sequences of those | Reuse the same `AgentRunInputs` / `normalize_messages(...)` path used by `agent.run(...)`, then convert normalized `Message` values to provider conversation events. Text and message input requests a response unless the caller passes `create_response=False`. Audio content goes to the provider's input audio buffer, and turn detection decides when to respond. |
| `Content.from_function_result(...)`, `Content.from_function_approval_response(...)`, or a `Message(role="tool")` | The conversation routes it into the tool batch waiting for it ([Tool execution](#tool-execution)). `create_response` doesn't apply, because the tool loop owns the follow-up response. |
| `RealtimeEvent` | The conversation sends it verbatim to the provider session as a raw command, and doesn't track it. |

Realtime should not invent a second input-normalization stack. The provider-specific mapper starts from normalized
`Message` objects and converts their contents to the matching provider realtime events, for example input text, input
audio, or function result events.

Illustrative shape, where `ToolsInput` stands for the tool types `Agent.run(tools=...)` accepts:

```python
class RealtimeConversation:
    """Live full-duplex conversation started by a realtime agent."""

    @property
    def updates(self) -> AsyncIterable[AgentResponseUpdate]:
        """The single ordered stream of agent-level realtime updates."""
        ...

    def __aiter__(self) -> AsyncIterator[AgentResponseUpdate]:
        """Iterate over the same stream as updates."""
        ...

    @property
    def active_agent(self) -> RawRealtimeAgent:
        """The agent whose instructions, tools and voice are live."""
        ...

    @property
    def history(self) -> Sequence[Message]:
        """Read-only snapshot of the call's authored turns so far."""
        ...

    async def send(self, value: AgentRunInputs | RealtimeEvent, *, create_response: bool = True) -> None:
        """Send input, an app-supplied tool result or approval, or a raw provider command."""
        ...

    async def update_session(self, *, options: RealtimeOptions | None = None, tools: ToolsInput | None = None) -> None:
        """Change the live realtime session and wait for the provider to acknowledge it."""
        ...

    async def create_response(self, *, options: RealtimeOptions | None = None) -> str:
        """Request a response and return its id once the provider has created it."""
        ...

    async def say(self, text: str) -> str:
        """Speak text verbatim and return the response id."""
        ...

    async def interrupt(self, *, played_ms: int | None = None) -> None:
        """Cancel the active response and cut the audio the caller didn't hear."""
        ...

    async def handoff(self, agent: RawRealtimeAgent, *, tools: ToolsInput | None = None) -> None:
        """Make another realtime agent the active agent in the same realtime session."""
        ...

    async def close(self) -> None:
        """Close the live conversation and release provider resources."""
        ...

    async def __aenter__(self) -> "RealtimeConversation":
        """Start the live provider session and receive loop."""
        ...

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: Any) -> None:
        """Run the close sequence."""
        ...
```

`RealtimeConversation` has these responsibilities:

- **Own the single receive loop**. The provider session is single-reader. `RealtimeConversation` is the only component
  that calls `RealtimeClientSessionProtocol.receive(...)`.
- **Project events to updates**. It maps each provider `RealtimeEvent` to one `AgentResponseUpdate` by the event's kind
  ([Event taxonomy](#event-taxonomy)) and exposes only agent-level updates to callers.
- **Order outbound writes**. Writes to the provider never interleave. Control calls are also serialized. Each waits for
  the provider's acknowledgement before the next one starts ([Live conversation controls](#live-conversation-controls)).
  `send(...)` never waits behind a control's acknowledgement, so audio keeps flowing during a session update.
- **Bound pending updates**. The projection queue between the receive loop and caller must have a finite, configurable
  capacity. The receive loop must not block indefinitely or grow memory without bound when a caller is slow. If the
  queue fills, the conversation fails with an explicit backpressure error, initiates provider cleanup, and makes that
  failure observable to the iterator without requiring another queue slot. Events must not be silently dropped or
  overwritten.
- **Run the tool loop** beside the receive loop ([Tool execution](#tool-execution)).
- **Own lifecycle and cleanup**. It is an async context manager. Exiting it runs the [close sequence](#close), which
  closes the provider session, cancels the receive task, drains/terminates queues, and propagates receive-loop failures.
- **Report history per exchange**. It reports authored user, assistant and tool turns to the starting agent's context
  providers one exchange at a time during the call, and whatever is left on close
  ([Session, identity and voice history](#session-identity-and-voice-history)). Signals, VAD events, rate-limit events,
  raw audio-buffer events and provider lifecycle events stay in the update stream and raw representations; they never
  become durable chat messages.

`RealtimeConversation` should not:

- expose provider `RealtimeEvent` as its primary output stream;
- own platform microphone/speaker drivers;
- be serializable or stored in `AgentSession`;
- run a second tool loop for sandwich pipelines when the inner `SupportsAgentRun` already owns tool invocation.

The output stream should not be inverted to stream `RealtimeEvent` values with optional `AgentResponseUpdate` or
`Content` inside them. `RealtimeEvent` is the provider/session boundary. `AgentResponseUpdate` is the agent-facing
stream item. If callers iterate a `RealtimeConversation`, they should receive the same kind of update they receive from
`Agent/RealtimeAgent.run(..., stream=True)`, not a lower-level provider event envelope.

Keeping `RealtimeEvent` below the agent layer matters because:

- middleware, telemetry, workflows, and agent consumers already understand `AgentResponseUpdate`;
- `RealtimeEvent` contains provider or pipeline protocol details that are useful for mapping and diagnostics but should
  not be the primary application programming model;
- putting `AgentResponseUpdate` inside `RealtimeEvent` would make the lower-level provider event depend on the higher
  agent layer, reversing the dependency direction;
- `RealtimeEvent.raw_representation` already provides the escape hatch for callers that need the raw provider event;
- the same `RealtimeEvent` model is used for outbound commands, which are not agent output updates.

The projection direction is therefore one-way:

```text
provider SDK event -> RealtimeEvent -> AgentResponseUpdate -> AgentResponse (for half-duplex finalization)
```

Content-free realtime events are still visible to callers. They are projected to `AgentResponseUpdate` with a
`Content.from_realtime_event(...)` item:

```text
RealtimeEvent(
    kind="speech_started",
    content=None,
    service_event_type="input_audio_buffer.speech_started",
)
    -> AgentResponseUpdate(
        role="user",
        contents=[Content.from_realtime_event(kind="speech_started", ...)],
        raw_representation=<the RealtimeEvent>,
    )
```

This keeps the user-facing stream uniform, because callers always receive `AgentResponseUpdate`. For normal payloads the
update contains text/audio/function/error content. For signals the update contains realtime-event content. Callers that
care about provider details can inspect the realtime-event content metadata or the update's `raw_representation`.

Response and history aggregation must be realtime-specific rather than calling `AgentResponse.from_updates(...)` over
the unfiltered update stream:

- provider clients tag every event with a kind, so core tells authored content from signals without reading wire names;
- bounded `run(...)` finalization correlates the run's responses and reconstructs only the agent's output, which is its
  assistant and tool messages. Caller transcripts stay out of `AgentResponse.messages`;
- the conversation reports history per exchange, built from final transcripts, the provider's item order and tool
  traffic, instead of reconstructing it from every emitted update;
- realtime-event content (signals and passthrough events) remains visible in the update stream and raw representations
  but never enters `AgentResponse.messages` or durable history.

Continuous conversation and scoped response requests are different:

- `conversation.updates` is the open-ended full-duplex update stream. It may run until the conversation is closed and
  does not necessarily have a final `AgentResponse`.
- `async for update in conversation` is shorthand for iterating `conversation.updates`.
- Temporary idleness does not end iteration. The open-ended stream terminates only after explicit close/context exit or
  a terminal provider, receive-loop, or backpressure failure; a provider response-complete event ends only that
  response.
- The app requests responses with `create_response(...)` or `say(...)`. `send(RealtimeEvent(...))` is for
  provider commands that have no first-class control.
- `RealtimeAgent.run(..., stream=True)` owns the bounded response/`ResponseStream` projection for half-duplex scenarios.

Entering and exiting the async context should be deterministic:

- `__aenter__` runs the conversation middleware chain ([Middleware and telemetry](#middleware-and-telemetry)), which
  opens the provider session. Opening runs the context providers' `before_run`, applies the initial options and tools
  together with what `before_run` added, replays the agent session's history as conversation items, sends any initial
  messages as context, and starts the single receive loop.
- `__aenter__` never requests a model response. The app requests one with `create_response(...)`, speaks a greeting with
  `say(...)`, or sends text.
- `__aexit__` and `close()` run the [close sequence](#close): stop accepting input, wait briefly for pending caller
  transcripts, close the provider session, settle unfinished tool calls, report the remaining exchanges and run the
  close hooks. Receive-loop errors propagate unless already superseded by the context exception.
- `close()` should be idempotent and should perform the same cleanup as `__aexit__`.
- Terminal state and errors must be recorded independently of the bounded update queue so explicit close and exceptional
  shutdown can always wake iterators and complete even when that queue is full.

### Live conversation controls

A live conversation has four first-class controls, plus [`handoff(...)`](#handoffs) and a `create_response` keyword on
`send(...)`. Each control is also a typed method on the provider session, so provider clients own the wire translation.
Everything else stays a raw provider command.

| Control | Contract |
| --- | --- |
| `update_session(*, options=None, tools=None)` | Same types as `start_conversation(...)`. `options` merge over the realtime session's current settings. `tools`, when given, replace the provider's declared tools and the conversation's executable tools together; `[]` clears them. Returns when `session.updated` arrives, and raises on a claimed error or the timeout. It changes only the realtime session, not the agent's defaults or the agent session. |
| `create_response(*, options=None) -> str` | Per-response options use Agent Framework's normalized names (`instructions`, `tool_choice`, `max_tokens`, `metadata` and so on). Provider clients drop the keys they can't honor, with a warning. Returns the response id once the response's own `response.created` arrives. |
| `say(text) -> str` | Verbatim speech. With an Azure voice, Voice Live and Foundry voice agents send `pre_generated_assistant_message`, which they speak word for word and record. OpenAI and Azure OpenAI have no event for verbatim speech, so their clients request a response with the per-response instruction "Say exactly the following, word for word, and nothing else: ...", documented as best effort. With an OpenAI voice, a Foundry voice agent uses that prompt too, because Foundry honors per-response instructions (inferred, not probed). Voice Live raises instead, because it ignores per-response instructions and would return a pre-generated message as unrecorded text. Returns like `create_response(...)`. |
| `interrupt(*, played_ms=None)` | Idempotent. Cancels the active response, if there is one, and ignores the provider's "no active response" error. With `played_ms`, it also cuts the audio the caller didn't hear ([barge-in](#barge-in)). Without it, it only cancels. |
| `send(..., create_response=True)` | Text and message input requests a response by default, so the simple example above works. `create_response=False` adds context silently. Audio input leaves responses to turn detection. |

Rules:

- **Ordering.** Control calls are serialized and written in call order. Any response requested after
  `update_session(...)` returns uses the new session, whether the app, the tool loop or turn detection requested it. A
  response already in flight doesn't change. A turn-detection response racing the update has no guarantee.
- **Acknowledgements.** `update_session(...)` waits for `session.updated`, which every provider sends for every update,
  no-op updates included, within 53 to 376 ms in the probes. `create_response(...)` and `say(...)` wait for their own
  `response.created`. A truncate waits for `conversation.item.truncated`. Waiting keeps the ordering guarantee from
  resting on undocumented in-order processing, and lets the caller, such as a handoff tool, handle a rejection. The
  cost is one round trip per control call.
- **Error claiming.** Requests carry a client `event_id`, and responses carry an id in `response.metadata`, which
  `response.created` echoes. Many errors carry no client id, though. `session.updated` never echoes it, Voice Live and
  Foundry put `null` or a server id on every error, and Azure OpenAI omits it for rejected tool schemas,
  `cannot_update_voice` and bad truncate offsets. Because control calls are serialized, at most one awaits an
  acknowledgement, so:
  - an error that arrives while a control waits fails that control if it carries the control's id, `null` or a server
    id. An error that echoes another request's id doesn't;
  - a waiting control claims an error before duplicates are discarded. Only unclaimed errors are deduplicated, counting
    back-to-back identical errors once, because Voice Live repeats some errors three times;
  - every other error becomes `error` content ([Event taxonomy](#event-taxonomy)).
- **Lost races.** No provider queues a second response. Azure OpenAI rejects it, and Voice Live and Foundry drop it
  silently. While `create_response(...)` or `say(...)` waits, a `response.created` without its `metadata` id means the
  provider dropped the request, so the call fails at once. The exception is Voice Live, which returns `metadata: null`
  on `response.created` for a `pre_generated_assistant_message`. A pre-generated `say(...)` therefore takes the first
  `response.created` after its send, and can't detect a lost race on Voice Live.
- **Active response.** `create_response(...)` and `say(...)` raise if a response is active. The conversation
  neither queues nor cancels anything implicitly. To talk over a response, call `interrupt(...)` first.
- **Voice changes.** Where the provider refuses a mid-call voice change, the client drops `voice` before sending, with a
  warning, and applies the rest. Where the provider ignores the change silently, the client warns when `session.updated`
  echoes the old voice. The [capability table](#provider-capabilities) says which providers do which; clients don't
  declare it in code.
- **Tools across a swap.** A tool call resolves against the executable set when its `function_call` arrives. Replacing
  the tools doesn't cancel running calls; a handoff tool itself comes from the old set. A call for a tool missing from
  the current set gets an error result.
- **Timeout.** One conversation-level acknowledgement timeout covers all control calls. The app sets it at start, and
  it defaults to about 10 seconds. The provider may still have applied a control call that timed out. Callers can still
  wrap a call in `asyncio.timeout()`.
- **No-op updates.** An update with nothing left to send after dropped keys returns at once, without a round trip.

The control errors join Agent Framework's exception hierarchy:

```text
AgentFrameworkException
└── RealtimeException                         base for realtime failures
    ├── RealtimeControlException              the provider rejected a control call, with its code and param
    │   ├── RealtimeControlTimeoutException   no acknowledgement in time; the provider may still have applied it
    │   └── RealtimeActiveResponseException   a response was active, or the provider dropped the request for one
    ├── RealtimeUnsupportedException          the provider can't do this with the current session, such as
    │                                         say(...) on Voice Live with an OpenAI voice
    ├── RealtimeConversationClosedException   send(...) or a control call after close started
    └── RealtimeBackpressureException         the bounded update queue overflowed
```

Arguments that can never work, such as a handoff target on another client, raise `ValueError` before anything is sent.

#### Barge-in

Agent Framework doesn't handle barge-in automatically. Turn detection already cancels the provider's response when the
caller speaks (`interrupt_response`). On the `speech_started` update, the app stops its own playback and calls
`interrupt(played_ms=...)` with how much of the response's audio the caller heard:

- `played_ms` counts the response's audio across all its assistant messages, in order. The conversation tracks each
  message's item id, content index and generated audio length.
- The conversation truncates the message where playback stopped, at the heard offset, clamped below the generated
  length.
- A message the caller heard to within about 40 ms of its end counts as fully heard, and the conversation doesn't cut
  it. Apps compute `played_ms` from playback clocks and round it. A full-length cut also misbehaves. Azure OpenAI wipes
  the transcript, and Voice Live rejects the cut because it reports a duration 1 ms short.
- Whenever `interrupt(...)` or Voice Live's automatic truncation cuts a message, the conversation truncates the
  response's later assistant messages at 0 ms. That covers models such as `gpt-realtime-2.1` that split one reply into
  two messages, and an Azure TTS preamble cancelled before its audio was sent.
- Voice Live's server-side `auto_truncate` is an alternative to `played_ms`, not something to combine with it. It's off
  by default, and Azure OpenAI rejects it.

`interrupt(...)` waits for each truncate's acknowledgement, even though that holds up the next control call. In the
prototype, a handoff queued behind an `interrupt(...)` took 1.6 s, against about 250 ms otherwise. Not waiting would
break error claiming, because truncate errors carry no client `event_id` on Voice Live, nor on Azure OpenAI for bad
offsets. The next waiting control call would claim an unacknowledged truncate's error and fail, for example a handoff
that had succeeded.

#### Raw provider commands

`send(RealtimeEvent)` sends the provider's own JSON event verbatim, with no event-name normalization and no tracking by
the conversation. A raw `session.update` that changes tools or instructions leaves the conversation's executable tool
set inconsistent with the provider's, so use `update_session(...)` for those. These stay raw:

- out-of-band responses (`conversation: "none"`);
- input-buffer commit and clear;
- item delete;
- `output_audio_buffer.clear` (WebRTC only);
- session fields Agent Framework hasn't typed;
- anything else not listed above.

Out-of-band responses can become first-class later without breaking anything.

#### Provider session methods

`RealtimeClientSessionProtocol` gains typed methods: `update_session(options, tools)`, `create_response(options)`,
`say(text)`, `cancel_response()` and `truncate(item_id, content_index, audio_end_ms)`. A shared base session supplies
the GA-protocol implementations built on `send(...)` ([Provider clients and packages](#provider-clients-and-packages)),
so Voice Live and third-party clients override only what differs. `RealtimeConversation` stays a thin layer over them
for the executable tool registry, audio-item tracking and serialization.

#### Calling controls from the update loop

A control waits for the provider, so an update loop that awaits one stops reading updates, and audio stops flowing to
the caller, for that round trip. Samples start controls such as a fallback `say(...)` as tasks instead of awaiting them
inside the update loop.

### Audio I/O and hosted media bridges

`RealtimeConversation` should start with one send path. Text, messages, content, audio content, and explicit realtime
events all go through `conversation.send(...)`. The conversation emits audio as `AgentResponseUpdate` content. Audio
formats are media types, both in options and on content: `audio/pcm;rate=24000`, `audio/pcmu` and `audio/pcma`.

Samples should prove audio bridge shapes before the framework defines shared source/sink protocols. Concrete device
drivers and ACS-specific web apps should remain samples or optional integrations. The first implementation should still
make these common loops possible through `RealtimeConversation`:

- local microphone audio, sent with `conversation.send(Content.from_data(...))`;
- conversation audio content, played on a local speaker;
- ACS websocket audio data, sent with `conversation.send(Content.from_data(...))`;
- conversation audio content, forwarded as ACS websocket audio data;
- `speech_started` updates, which stop local or hosted playback before the app calls `interrupt(played_ms=...)`.

Direct realtime models keep generated audio and its transcript in the provider's conversation even when the caller never
heard it. Audio sinks report playback progress through `interrupt(played_ms=...)`, which truncates the unheard part
([Barge-in](#barge-in)). Without it, later turns can reason over unheard words, and history keeps text the caller never
heard, marked incomplete.

A dedicated low-latency audio callback can be added later if the content-update path is not smooth enough for local
speaker playback, WebRTC tracks, or ACS websocket forwarding.

Do not add a broad audio framework in the first implementation. Shared audio source/sink protocols wait until at least
two concrete examples prove the same shape.

### Direct realtime model path

Add provider-neutral direct realtime client/session protocols in core. The shape should be small:

- a realtime client creates a live realtime session;
- a realtime session sends `RealtimeEvent` commands and carries out the typed control methods;
- a realtime session yields provider events, each tagged with an Agent Framework kind, which the agent maps to
  `AgentResponseUpdate`;
- raw provider events remain reachable through `raw_representation`.

The provider-level input and output shape is deliberately not `AgentResponseUpdate`:

- client input is `RealtimeEvent` commands, typed control calls and provider options;
- client output is `AsyncIterable[RealtimeEvent]`;
- `RealtimeConversation` accepts user conveniences such as `str`, `Message`, and `Content`, normalizes them with the
  existing `AgentRunInputs` / `normalize_messages(...)` path, and maps them to provider events;
- audio-only input and output use `Content.from_data(..., media_type="audio/...")` at the agent layer and
  `RealtimeEvent.content` at the provider/session boundary.

Add `RawRealtimeAgent` / `RealtimeAgent` over this client/session shape:

- `run(..., stream=False)` opens an ephemeral realtime session, sends one turn, drains until the run's terminal response
  event, and returns `AgentResponse`;
- `run(..., stream=True)` does the same but yields `AgentResponseUpdate` values through `ResponseStream`;
- `start_conversation(...)` returns `RealtimeConversation` for full-duplex live use.

`RealtimeAgent` must satisfy `SupportsAgentRun` so it can participate in half-duplex agent scenarios whose contracts
accept the structural protocol, including protocol-based workflows, hosts, and agent-like utilities. Handoffs don't go
through the chat `HandoffBuilder`, whose participants must be concrete `Agent` instances and which runs one bounded turn
per run; realtime agents hand off in-session, with their own `RealtimeHandoffBuilder` ([Handoffs](#handoffs)).
`RealtimeAgent` does not need to be a subclass of `Agent`. Shared behavior should come from `BaseAgent`, the structural
protocol, and middleware/telemetry concepts that are compatible with the realtime `run(...)` shape.

The implementation should mirror the existing Raw/Layered pattern without inheriting the chat-specific `RawAgent`:

```python
class RawRealtimeAgent(BaseAgent):
    ...


class RealtimeAgent(<realtime middleware layer>, <realtime telemetry layer>, RawRealtimeAgent):
    ...
```

`RawRealtimeAgent` should derive from `BaseAgent`, not `RawAgent`. `RawAgent` is the chat-client agent implementation:
it assumes `SupportsChatGetResponse`, chat options, chat response parsing, and the chat function-calling loop. Reusing
it would make realtime inherit request/response chat assumptions that the realtime protocol is explicitly trying to
avoid. The one piece of `RawAgent` realtime needs, its MCP tool lifecycle, moves into an internal helper both agents
share ([MCP and hosted tools](#mcp-and-hosted-tools)).

`BaseAgent` is the right base because it provides identity, context provider storage, middleware storage, session
factories, tool-as-agent support, and provider hooks without imposing a chat-client transport.

`AgentMiddlewareLayer` and `AgentTelemetryLayer` are shaped around the chat `Agent.run(...)` signature, including chat
options, tool forwarding, compaction, tokenizer, and chat/function middleware forwarding, and a live conversation has no
single final result for them to wrap. The realtime layers therefore reuse their pieces without inheriting them. The
realtime middleware layer runs agent middleware around `run(...)` and conversation middleware whenever a realtime
session opens; the realtime telemetry layer opens the conversation span
([Middleware and telemetry](#middleware-and-telemetry)). `RawRealtimeAgent` runs no middleware and emits no telemetry,
as `RawAgent` runs no agent middleware. Its tool loop still runs tools.

#### Run and conversation keywords

Illustrative shape, with `ToolsInput` as above:

```python
class RawRealtimeAgent(BaseAgent):
    def __init__(
        self,
        client: RealtimeClientProtocol,
        *,
        instructions: str | None = None,
        tools: ToolsInput | None = None,
        default_options: RealtimeOptions | None = None,
        function_invocation_configuration: FunctionInvocationConfiguration | None = None,
        id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        context_providers: Sequence[ContextProvider] | None = None,
        middleware: Sequence[MiddlewareTypes] | None = None,
    ) -> None: ...

    def run(
        self,
        messages: AgentRunInputs | None = None,
        *,
        stream: bool = False,
        session: AgentSession | None = None,
        options: RealtimeOptions | None = None,
        tools: ToolsInput | None = None,
        middleware: Sequence[MiddlewareTypes] | None = None,
        function_invocation_kwargs: Mapping[str, Any] | None = None,
        client_kwargs: Mapping[str, Any] | None = None,
    ) -> Awaitable[AgentResponse] | ResponseStream[AgentResponseUpdate, AgentResponse]: ...

    def start_conversation(
        self,
        messages: AgentRunInputs | None = None,
        *,
        session: AgentSession | None = None,
        options: RealtimeOptions | None = None,
        tools: ToolsInput | None = None,
        middleware: Sequence[MiddlewareTypes] | None = None,
        function_invocation_kwargs: Mapping[str, Any] | None = None,
    ) -> RealtimeConversation: ...
```

- **`run(...)` takes `options=` and `tools=` explicitly**, mirroring `Agent.run(...)`, with no catch-all `**kwargs`.
  Per-run `options` merge over `default_options`, and per-run `tools` merge into that run's ephemeral session.
  `SupportsAgentRun.run` declares only `messages`, `stream`, `session`, `function_invocation_kwargs` and
  `client_kwargs`, but hosts pass more: AG-UI passes `tools=` and `options={"metadata", "store"}`, and the MCP agent
  tool, Foundry hosting, declarative workflows and group chat pass `options=`. Accepting both keeps the claim that hosts
  can run a realtime agent.
- **`start_conversation(...)` takes the same `options` and `tools`**, applied to the initial session. Mid-call changes
  use `update_session(...)`.
- **`middleware=`** on both calls joins the agent's own list, sorted by category ([Middleware](#middleware)).
- **`function_invocation_kwargs`** seeds the tools' runtime keyword arguments, per run for `run(...)` and for the whole
  call for `start_conversation(...)`.
- `run(...)` doesn't mirror `compaction_strategy` or `tokenizer`, which are chat-only and which no host passes.
- **A known issue stays open.** `SupportsAgentRun` doesn't declare the keywords hosts pass, so any protocol-only agent
  can fail under those hosts, as `CopilotStudioAgent` can today. Fixing the protocol is outside this ADR.

#### Options

Core defines `RealtimeOptions`, a `TypedDict` of what every provider shares, and each provider client extends it:
`OpenAIRealtimeOptions`, `VoiceLiveOptions` and `FoundryVoiceAgentOptions`. It renames ADR 0035's
`RealtimeSessionOptions`, because the options also hold response-level keys, as `ChatOptions` does.

- Options use Agent Framework's normalized names where the concept exists in realtime: `instructions`, `temperature`,
  `tool_choice`, `max_tokens` and `metadata`.
- Each client translates these neutral, flat keys:
  - `voice: str` takes an OpenAI-style voice name that every client understands. The Azure option types widen it to also
    take an `AzureVoice` object (`type`, `name`, `rate`, `style` and so on), as `AnthropicChatOptions` widens
    `instructions`.
  - `input_audio_format` and `output_audio_format` take media types: `audio/pcm;rate=24000`, `audio/pcmu` and
    `audio/pcma`. Each client translates them to its wire format.
  - `turn_detection`, `transcription` and `noise_reduction` take `RealtimeTurnDetection`, `RealtimeTranscription` and
    `RealtimeNoiseReduction`, which carry an open `type` string and only the fields every provider shares. `None` turns
    the feature off. ADR 0035's top-level `create_response` moves into turn detection, where providers define it.
    `AzureTurnDetection`, `AzureTranscription` and `AzureNoiseReduction` add Azure's types and fields.
  - `output_modalities` selects audio or text output.
  - Clients nest the audio settings under `audio.input` and `audio.output` for GA-protocol sessions.
- **Azure-only settings**, such as echo cancellation, animation, avatar and interim responses, exist only on the Azure
  option types.
- **Clients drop keys they can't honor, with a warning**, such as AG-UI's `store`. They forward nothing raw, because
  realtime `session.update` rejects unknown fields, whereas chat clients forward unknown keys. A field Agent Framework
  hasn't typed needs a raw `session.update` through `send(...)`.
- **Formats raise.** An audio format the provider can't take raises before connecting. A dropped key only changes
  behavior, but a dropped format garbles the audio.

### Event taxonomy

Provider clients tag every `RealtimeEvent` with an Agent Framework **kind**, and core dispatches on it, so apps never
match wire event names. Kinds replace ADR 0035's `event_type` categories (`audio`, `text`, `service`, `lifecycle` and so
on).

- **Updates.** Kinds that carry a payload use the content types and update fields Agent Framework already has. Pure
  signals become `Content.from_realtime_event(kind=...)`. Everything else gets `kind="passthrough"` and travels under
  its wire name. This is the .NET ADR's decision Q4 shape.
- **`service_event_type` stays verbatim.** Clients don't rename Voice Live's preview events to GA names; they map them
  to the same kinds. Renaming would have made OpenAI's wire protocol the neutral API that phase 2 pipelines and other
  providers must imitate.
- **What earns a kind.** Core machinery must read it (the tool loop, control acknowledgements and truncation tracking,
  `run(...)` finalization, history, telemetry), or a typical voice app acts on it (barge-in, transcripts, content
  filter, usage, errors). Either way, it isn't unique to one provider.
- **Who adds kinds.** The kind type follows Agent Framework's open-string pattern, a `Literal` of known kinds plus a
  `NewType` over `str`, like `FinishReason`. Updates from a newer runtime therefore still deserialize. Only core adds
  kinds. Provider packages, third-party ones included, map everything else to `passthrough`.
- **Naming.** Kinds follow these rules:
  - snake_case, named for meaning, not wire structure: `speech_started`, not `input_audio_buffer.speech_started`;
  - `input_` and `output_` for caller and agent media;
  - a noun plus a past participle for lifecycle: `session_created`, `response_done`;
  - Agent Framework's content type names where the payload has one: `function_call`, `usage`, `error`;
  - the .NET ADR's names where they overlap: `speech_started`, `speech_stopped`, `response_created`, `response_done`,
    `input_transcription`, `error`.

| Kind | Update | Notes |
| --- | --- | --- |
| `session_created` | realtime-event content | Realtime session id, model and expiry. The only place the session id appears. |
| `session_updated` | realtime-event content | Acknowledges a session update. |
| `speech_started`, `speech_stopped` | realtime-event content, `role="user"` | `audio_start_ms` or `audio_end_ms`, and the item id. The barge-in cue. Only the app knows whether audio is still playing, so there's no `barge_in` kind. |
| `response_created` | realtime-event content, `role="assistant"` | Response id. |
| `response_done` | realtime-event content, `role="assistant"` | The provider's `status`, `reason` and `error`. The update sets `finish_reason`. |
| `item_added` | realtime-event content | Item id, previous item id and role, which give the provider's conversation order for history. |
| `item_truncated` | realtime-event content, `role="assistant"` | Item id, content index and `audio_end_ms`, whoever triggered the truncation. The cut-off-playback signal. |
| `input_transcription` | `text`, `role="user"` | Caller transcript. |
| `input_transcription_failed` | realtime-event content, `role="user"` | Item id, error code and message. Not `error` content, because the model heard the caller and only the text is missing. |
| `output_text` | `text`, `role="assistant"` | Agent transcript. Spoken and written agent text aren't told apart. |
| `output_audio` | `data` | Media type from the session's output format, with the rate: `audio/pcm;rate=24000`, `audio/pcmu` or `audio/pcma`. |
| `function_call` | `function_call` | The assembled call, once its arguments finish. Not `informational_only`, because its result follows, as in chat. |
| `function_result` | `function_result`, `role="tool"` | The conversation emits one for every settled call ([Tool execution](#tool-execution)). |
| `function_approval_request` | `function_approval_request` | Approval requests for local tools, and for hosted tools with `hosted=True`. |
| `handoff_completed` | realtime-event content | `source` and `target`. The conversation emits it ([Handoffs](#handoffs)). |
| `usage` | `usage` | Once per response, from `response.done`. |
| `error` | `error` | Unclaimed provider `error` events, and failed responses. |
| `passthrough` | realtime-event content | Everything else. |

These pass through:

- audio-done events;
- output-item and content-part events;
- input-buffer commits, clears and timeouts;
- item done and deleted events;
- function-call argument deltas;
- `rate_limits.updated` and `conversation.created`;
- hosted MCP call events. They can map to `mcp_server_tool_call` content later without breaking anything;
- Voice Live's `warning`, animation, timestamp, avatar, video and `foundry_agent_call` events, including visemes.

Rules:

- **Roles and authors.** An update takes the role of the item it's about, and `message_id` is the provider's item id.
  Only assistant and tool updates carry an agent's `author_name` and `agent_id`, taken from the agent that was active
  when their response was created.
- **Transcript finality.**
  - Deltas are text with `merge_mode="append"`.
  - The final event carries the full text with `merge_mode="replace"` and `status="completed"`. This needs an additive
    core change: `Content.from_text(status=...)`, and text merges that keep the latest status.
  - A phase 2 STT pipeline's interim snapshots would be `replace` without `completed`.
  - ADR 0035's sample printed every `update.text`, which prints each transcript twice. Samples print deltas or finals.
- **Caller transcripts** stay out of `run(...)`'s `AgentResponse.messages`, which hold agent output. Durable history is
  covered under [Session, identity and voice history](#session-identity-and-voice-history).
- **Response outcome.** `response_done` always carries the provider's status and reason, so apps and telemetry can
  report interruptions. The update's `finish_reason` keeps chat's vocabulary:
  - `completed` maps to `"stop"`, or to `"tool_calls"` when the output holds function calls;
  - `incomplete` with `content_filter` maps to `"content_filter"`, and with `max_output_tokens` to `"length"`;
  - `cancelled` and `failed` map to `None`, because Agent Framework doesn't add realtime-only values such as
    `"turn_detected"` to a field chat shares;
  - a failed response also emits `error` content (role `assistant`, with its `response_id`) from `status_details.error`.
- **Errors.**
  - An `error` event that no control call claims becomes `error` content: `message`, `error_code` from the provider's
    code, and the provider's `type`, `param` and client `event_id` as metadata.
  - Iteration continues. A dead socket or receive-loop failure raises from iteration, so in-stream errors are
    recoverable by definition, and there's no `fatal` or `recoverable` flag.
- **Control acknowledgements and rejections.**
  - `session.updated` and `response.created` always stream, even when a control call waited for them.
  - An error a control call claims goes to that call. The call raises it, or `interrupt(...)` ignores it if it's
    `response_cancel_not_active`. It streams only as a passthrough update, so app error handlers don't fire twice or
    on benign races.
- **Usage.** `usage` content carries the standard keys (input, output, total, cache read, and reasoning where reported)
  and the audio, text and image keys ([Responses and usage](#responses-and-usage)). Input-transcription usage is never
  `usage` content, because it's billed at another model's rate and would inflate the response's totals. It travels in
  the caller transcript's provider metadata.
- **Realtime-event content.**
  - First-class kinds carry `kind`, the verbatim `service_event_type`, and neutral attributes documented per kind as
    `TypedDict`s.
  - Provider extras appear only if the provider package documents them, in its metadata namespace
    ([ADR 0048](0048-python-content-identity-and-protocol-metadata.md)). The conversation never copies the
    full wire event, because `session_created` would then put the instructions and tool schemas into every serialized
    update a host forwards to a browser.
  - Passthrough events carry the wire payload as JSON in the provider's namespace, so visemes and warnings survive
    serialization.
  - The SDK object stays in `raw_representation`.
- **Provider contract.**
  - Every kind documents the neutral attributes provider clients must supply on the `RealtimeEvent`, content kinds
    included. For example, `output_audio` needs the response id, item id and content index for truncation.
  - Every provider server event becomes a `RealtimeEvent`, and every `RealtimeEvent` becomes an `AgentResponseUpdate`,
    with normal content or realtime-event content.
  - Outbound raw commands carry `kind="passthrough"`.
  - `created_at` stays unset unless the provider sends a timestamp.
- **Signals the conversation produces** use the same mechanism: `function_result`, `function_approval_request` and
  `handoff_completed`. Phase 2 pipelines emit the same kinds where the meaning matches, such as speech start and stop,
  transcripts and response lifecycle, and passthrough for the rest.

### Tool execution

Direct realtime model tool calls reuse the existing function-invocation implementation. The current
`FunctionInvocationLayer.get_response(...)` wrapper is chat-client-roundtrip oriented, but the batch executor under it,
`_execute_function_calls`, is provider-neutral and already handles:

- normalized `FunctionInvocationConfiguration`;
- function middleware pipelines;
- tool approval requests and responses;
- declaration-only tools;
- argument validation;
- progressive tool exposure through `FunctionInvocationContext.tools`;
- `UserInputRequiredException`;
- max iterations / max function calls / consecutive error limits;
- detailed versus generic tool error shaping.

Realtime reuses that executor with realtime loop rules on top, rather than refactoring a new shared executor out of it.
For sandwich pipelines, function invocation belongs to the inner text agent; there is no second outer realtime tool
loop.

#### Execution

- **One batch per response.** The conversation collects a response's calls as they arrive, resolving each against the
  executable set at that point. When the response's `response_done` arrives, it runs them as one batch through the
  executor, in a background task beside the receive loop. Running tools inside the receive loop would stop audio and
  barge-in, and would deadlock a tool that awaits `update_session(...)`, which waits for the receive loop to deliver
  `session.updated`.
- **The executor keeps chat's rules.** Approval-required calls pause the batch, declaration-only calls wait for the app,
  and the rest run concurrently, or in model order with `allow_concurrent_invocation=False`. Function middleware and
  error shaping apply.
- **Results are strings.** Provider clients always send `function_result.result` as the `function_call_output` item's
  `output` string, never as `items` or a raw value. Agent Framework already produces a string, because `parse_result`
  serializes objects to JSON and `Content.from_function_result(...)` joins text items. Clients drop images, audio and
  files with a warning, as the Chat Completions client does. The shared GA base session implements this, so Voice Live
  and third-party clients get it automatically. The "Azure Realtime hangs on non-string results" that Semantic Kernel
  apps work around isn't a protocol difference. `output` is a string on every provider, and Semantic Kernel sent no
  output at all for dict results, so the model waited.
- **One follow-up response per batch.**
  - A batch settles when every call from its response has a result, including results the app supplies and calls
    resumed after approval.
  - The loop then sends the results in model order and requests one follow-up response.
  - If a response is active at that point, from turn detection or a tool's `say(...)`, the loop waits for it to finish.
    Follow-ups due at the same time merge into one.
  - Batches from different responses run concurrently and settle independently.
  - If function middleware raises `MiddlewareTermination`, the loop sends the results without a follow-up.
  - `run(...)` ends at the first response that finishes without tool calls, or when middleware terminates the loop.
- **Ordering guarantee.**
  - Anything a tool awaits, such as `update_session(...)` or `handoff(...)`, completes before the loop sends that
    batch's results and follow-up.
  - Tool-set changes made with `ctx.add_tools()` / `ctx.remove_tools()` reach the realtime session through
    `update_session(tools=...)` before the batch's results, so the follow-up sees the new tools, as chat's "from the
    next iteration" rule requires.
  - `update_session(tools=...)` replaces the live tool list's contents in place, so both mechanisms see the same list.
  - If the provider rejects that update, the rejection follows the rules for claimed errors
    ([Event taxonomy](#event-taxonomy)), and the loop still sends the results and follow-up.

Batching at `response_done` keeps chat's batch rules at little cost, because `response.create` can't go out while the
response is active anyway, and tool calls usually end their response. The cost shows with Azure TTS voices, where
`response.done` can trail a call's arguments by 0.3 to 2.5 s when the model speaks a preamble, against 0 to 15 ms with
the model's own voice. The OpenAI Agents SDK, LiveKit and Pipecat start each call as it arrives instead.
Microsoft.Extensions.AI batches calls but blocks its receive loop while they run. One follow-up per batch fixes a bug in
the closed microsoft/agent-framework#3821, which sent `response.create` after every result and failed when one response
held several calls.

#### Tools and the conversation

- **`FunctionInvocationContext.realtime_conversation: RealtimeConversation | None`.** It's experimental, under the
  `REALTIME` flag. The realtime loop sets it, during `run(...)` too; everywhere else it's `None`. Tool bodies and
  function middleware call `update_session(...)`, `say(...)`, `create_response(...)`, `interrupt(...)` and
  `handoff(...)` through it. A handoff tool awaits the switch before returning. It isn't passed in the tools' runtime
  keyword arguments, where it would reach every tool that accepts `**kwargs` and could clash with the app's keys.
- **Configuration.** `RealtimeAgent(function_invocation_configuration=...)` holds the loop's settings, because the loop
  belongs to the agent's conversation, not to the client. Function middleware comes from the agent's `middleware=` list,
  sorted as `Agent` sorts it.
- **Limits.** In a live conversation, a "request" is a tool chain, which restarts when the caller starts speaking or the
  app sends input.
  - `max_iterations` counts follow-ups. `max_function_calls`, `max_duration_seconds` (including approval waits) and
    `max_consecutive_errors_per_request` count across the chain.
  - When a limit trips, the loop still sends the results, but requests the follow-up with `tool_choice: "none"`.
  - Unknown calls always get an error result, so `terminate_on_unknown_calls` doesn't apply to realtime.
  - `run(...)` keeps chat's per-run meaning.
- **Slow tools.** There are no timeout or filler-speech settings. Two function-middleware patterns cover them, shown in
  the docs and a sample:
  - wrapping `call_next()` in `asyncio.timeout()`, so the `TimeoutError` becomes an ordinary error result;
  - saying a filler line before a slow tool, catching `RealtimeActiveResponseException` if the caller is talking.

#### Cancellation and settlement

- **Responses that didn't complete.** Calls from a response that ended `cancelled`, `incomplete` or `failed` don't run.
  The loop settles each with an error result and sends it as the call's output on a best-effort basis, ignoring any
  error the provider returns for that send. It requests no follow-up, because the caller's new turn drives the next
  response.
- **Barge-in never cancels running tools.** Neither `speech_started` nor `interrupt(...)` touches them, and their
  results go out when ready; the follow-up waits for the response that turn detection started. Most barge-ins are
  backchannels, cancelling a payment halfway is worse than reporting it, and cooperative cancellation can't stop a tool
  running in a worker thread. The OpenAI Agents SDK and LiveKit keep tools running too; Pipecat cancels them by default.
- **Close** (hang-up, `close()`, context exit or a terminal failure):
  - stops starting batches;
  - cancels running tool tasks and waits for them to stop, as the chat loop does when it aborts a batch;
  - settles every unfinished call (running, awaiting approval or awaiting an app result) with an error result. The
    loop doesn't send that result. It goes into history, and onto the update stream if anything still reads it;
  - clears pending approvals from the agent session.
- **A paused `run(...)` isn't abandoned.** When a run ends because its batch paused for an approval or an app-run tool,
  closing the run's realtime session leaves those calls pending. The run returns them in its `AgentResponse` and keeps
  the pending approvals in the agent session, as chat does, so the next run can resume them. Close settles calls only
  when a live conversation ends, or when a run fails or is cancelled.
- **`MiddlewareFailure`** ends the conversation fail-closed. The loop cancels sibling calls and settles unfinished calls
  as on close, and iteration raises the failure. Ordinary exceptions become error results.
- **No dangling calls.** Every call ends settled, apart from the calls a paused `run(...)` hands back, which the next
  run settles. So neither the realtime session nor stored history keeps an abandoned call without a result, and history
  stays replayable into chat clients, which need each result after its call. A settled
  error's `exception` says why, for example `cancelled` or `conversation_closed`, so apps, telemetry and history can
  tell an abandoned call from a real result.

#### App-supplied input

- **Pauses.** Calls to app-run (declaration-only) tools, approval requests and `UserInputRequiredException` requests
  pause their batch.
- **Answering through `send(...)`.** The app answers with `Content.from_function_result(...)`,
  `Content.from_function_approval_response(...)` or a `Message(role="tool")`, and the conversation routes each answer
  into its batch. `create_response` doesn't apply to these inputs, because the loop owns the follow-up. Input for a call
  id the conversation isn't tracking (unknown, already settled or cancelled) raises `ValueError`. Raw outputs remain
  possible through `send(RealtimeEvent)`, without tracking.
- **Approvals are in scope**, in live conversations and in `run(...)`:
  - In a live conversation, the request streams as a `function_approval_request` update, and the call stays open while
    the caller keeps talking. The conversation matches the approval response through its agent session, as in chat.
  - In `run(...)`, approval requests and calls to app-run tools end the run, as in chat. The next run executes the
    approved call, or receives the app's result, and replays the call into its new realtime session.

#### MCP and hosted tools

- **Local MCP tools get `Agent`'s lifecycle.**
  - `RawAgent`'s MCP handling moves into an internal helper shared by `RawAgent` and `RawRealtimeAgent`, without
    changing chat behavior.
  - A tool connects through `async with agent` or on first use, and stays open for the agent's lifetime. Agent Framework
    never closes a tool the app connected itself.
  - `MCPTool`'s dedicated lifecycle task and locks make one connection safe to share across agents and concurrent calls.
  - The conversation copies each MCP tool's functions into its executable set and its declared tools at start, and
    again on `update_session(tools=...)`. A change to the server's tool list doesn't trigger a session update. The
    conversation keeps using its copy until the next `update_session(tools=...)`.
- **Hosted tools** (provider-run MCP, and Voice Live's `foundry_agent` and system tools):
  - Static helpers on the provider clients declare them, like `OpenAIChatClient.get_mcp_tool()`.
  - The loop never runs them, and their call events stay passthrough.
  - Hosted approval requests map to `function_approval_request` content with `hosted=True`. The app's approval response
    goes to the provider as `mcp_approval_response`, as the Responses chat client does. Providers send these requests
    after `response.done` and speak the result only after another `response.create`, so the loop requests a follow-up
    after sending the approval.

### Session, identity and voice history

`RealtimeConversation` is live runtime state. `AgentSession` is durable serialized state. Do not store sockets, tasks,
audio streams, or provider session objects in `AgentSession`.

Use `AgentSession` first for durable state and compatibility checks. Add `RealtimeAgentSession` only when a concrete
provider needs typed durable realtime state that cannot be represented clearly with `AgentSession.service_session_id` or
namespaced serializable state. Following [ADR 0033](0033-python-agent-session-identity.md), each value goes where its
lifecycle puts it:

- future-call continuation goes in `AgentSession.service_session_id` or namespaced session state;
- single response/event identity goes in response/update/event metadata;
- unfinished work resume would use continuation tokens, if needed later;
- per-run correlation goes in run/telemetry context, not `AgentSession`;
- live socket state stays in `RealtimeConversation`, never `AgentSession`.

No v1 provider can resume a realtime conversation in a new realtime session, so `service_session_id` stays unset and no
`RealtimeAgentSession` is needed:

- OpenAI and Azure OpenAI conversation ids last only as long as the connection.
- Foundry voice agents' stored conversations (`store=true`) are read-back records that their docs say not to resume.
- A provider's conversation id travels in `session_created` metadata, for correlation only. A Foundry voice agent's
  `conversation_id` goes in its `foundry` namespace.

Direct realtime model sessions and sandwich pipeline sessions are not interchangeable. Implementations should detect
obviously incompatible session state before making remote calls when practical.

History holds the **authored turns** of the provider's conversation, as text: caller and agent transcripts in
conversation order, app input, and tool traffic with each result straight after its call. History credits each agent
turn to the agent that was active when its response was created, and keeps only what the caller heard of a cut-off
turn. A live
conversation reports history to context providers one exchange at a time during the call, through the same `after_run`
hook a run uses, and every close path reports whatever is left. This replaces ADR 0035's authored-turn ledger finalized
at close.

#### What history holds

- **Text only.** History stores voice turns as transcripts, never as audio. Audio stays on the update stream, and call
  recording stays in the app. Ten minutes of 24 kHz PCM16 audio is about 29 MB, and `InMemoryHistoryProvider` serializes
  messages into `session.state`. A caller transcript comes from a separate transcription model and can differ from what
  the speech model heard.
- **Authored turns** are what entered the provider's conversation:
  - caller turns: each caller audio item, as its final caller transcript (`role="user"`);
  - agent turns: each assistant message item, as its final agent transcript, including greetings and `say(...)` lines;
  - app input sent through `send(...)`, stored as sent, including `create_response=False` context;
  - function calls with their settled results, including error results and handoff calls;
  - approval requests and responses.
- **History leaves out** realtime-event content (signals and passthrough events), `error` content, usage, session
  updates and instruction changes, out-of-band responses, and hosted-tool events. Agent Framework adds no canned error
  lines, and apps can add their own.
- **One message per conversation item**, with the provider's item id as `message_id`. Stored transcripts keep
  the status they streamed with: `completed`, or `incomplete` as described below. Chat clients don't send a text's
  `status` back to models.

#### Caller turns without text

- A caller turn has no text when its transcription failed (`input_transcription_failed`), when transcription isn't
  configured, or when its transcript was still pending after close's wait.
- These turns stay in history, as a `role="user"` message with no contents, its item id as `message_id`, and a
  namespaced marker saying why. Dropping them would make the reply look unprompted, and placeholder text would put Agent
  Framework's words in the caller's mouth.
- If transcript deltas arrived before a failure, history keeps that text, marked `status="incomplete"`.
- Replay skips text-less messages. The Chat Completions client already emits nothing for a message with no contents.
- If the conversation has history providers but no transcription is configured, it logs one warning at start. It never
  switches transcription on, because that bills a second model.

#### Cut-off turns

- **Truncated turns keep only the heard part.** The truncation can come from `interrupt(played_ms=...)` or from Voice
  Live's `auto_truncate`. History keeps the final agent transcript cut at the word boundary nearest
  `audio_end_ms / generated audio length`, marked `status="incomplete"`. The estimate is proportional, so it can be a
  few words off. If the caller heard nothing (`audio_end_ms=0`), history leaves the turn out.
- **Responses cut short without a truncation** keep the transcript generated so far, marked `status="incomplete"`. This
  covers responses cancelled when the app doesn't track playback, and responses a content filter ended as `incomplete`.
- **History drops the unheard text.** OpenAI deletes the server-side transcript of truncated audio so that the model
  doesn't reason over text the caller never heard, and Voice Live documents the same behavior.
- **History ignores late truncations.** A truncation that arrives after its exchange was reported doesn't change it.

#### Order

- **History follows the provider's conversation order**, which the `item_added` updates give. GA's
  `conversation.item.added` and Voice Live's `conversation.item.created` both carry `previous_item_id`.
- The provider adds a caller item when its audio is committed, before the reply. So the caller turn comes before the
  reply even when its transcript arrives later, and the conversation fills in the text by item id when the final
  transcript arrives. Transcription runs
  asynchronously with response creation, so arrival order would put late caller transcripts after the replies they
  caused, and grouping transcripts by role, as microsoft/agent-framework#3821 did, reorders multi-turn calls.
- Where a provider doesn't report item order, history uses the order in which each item's first event arrives.
- **Each tool result goes directly after its call**, even if the caller spoke while the tool ran. It's the one
  deliberate deviation from the provider's order, where the output is appended at the end, and it keeps history
  replayable into Chat Completions, which requires results to follow their calls. The conversation reports an exchange
  whose tool is still running once the tool's result arrives, and later exchanges wait behind it.

#### Attribution

- **One agent session per conversation.** A live conversation writes all its history to the session passed to
  `start_conversation(session=...)`, through the context providers of the agent that started it, whatever handoffs
  happen.
- **Credit at response creation.** Assistant and tool messages carry the `author_name` of the agent that was active when
  their response was created. Caller turns carry none. History credits the response holding a handoff call to the old
  agent, and the follow-up to the new one, because the session update takes effect first.
- **`author_name` is the only attribution**, because `Message` has no agent id. The Chat Completions client replays it
  as `name`, and orchestration's history cleaning keeps it.

#### Reporting to context providers

- **One `after_run` per exchange**, as `run(...)` does per run. `input_messages` holds the exchange's caller turns and
  app input, and `response.messages` holds its agent turns and tool messages. `HistoryProvider.after_run` stores inputs,
  then outputs, so the call stays in order and `store_inputs` and `store_outputs` stay meaningful. Consecutive inputs
  belong to one exchange, and a greeting before any input is an exchange with no input.
- **Reported during the call.** The conversation reports an exchange once the next one has started and all its items are
  settled, meaning their transcripts are final or text-less and their calls are settled. Reports run in order, in a
  background task that never blocks the receive loop. Providers run in reverse registration order for each exchange, as
  in chat.
- **`before_run` runs once**, at `start_conversation(...)`. Its context (context messages, instructions and tools) goes
  with the first exchange's report only, so `store_context_messages` doesn't repeat it. Every exchange's context shares
  the start context's `metadata`.
- **Each exchange's `AgentResponse`** carries its responses' summed usage, plus the last response's id and finish
  reason.

Reporting during the call keeps all but the last exchange if the process dies, and keeps close fast, because memory
providers make model or network calls in every `after_run`. A single report at close would instead have to reorder the
call by role or put caller turns in `response.messages`. Agent Framework already persists history per model call for
long chat runs (`PerServiceCallHistoryPersistingMiddleware`).

#### Close

Every close path reports what's left: `close()`, context exit, hang-up, a provider or receive-loop failure,
`MiddlewareFailure` and task cancellation. A dropped call's turns really happened, and close settles every unfinished
tool call. The sequence:

1. Stop accepting input.
2. If the socket is still open, wait for pending caller transcripts. The wait ends as soon as they're all in, with a cap
   of about 3 seconds, because probes saw late transcripts up to 2.3 s after the caller's turn was committed. It skips
   caller turns whose response ended `content_filter`, because Voice Live with BYOM never finalizes their transcripts.
3. Close the socket.
4. Settle unfinished tool calls ([Tool execution](#cancellation-and-settlement)).
5. Report the remaining exchanges.
6. Run conversation middleware's close hooks.
7. Return, or raise as described under provider failures.

A transcript still missing after the wait becomes a text-less caller turn.

#### Provider failures

- A context provider whose `after_run` raises mid-call doesn't end the call. The failure streams as a non-fatal `error`
  update naming the provider.
- Only that provider waits. Its unreported exchanges stay queued in order, and the conversation retries them with the
  next report and again on close. Other providers keep receiving exchanges.
- If it still fails on close, `close()` raises after everything else is cleaned up, with an exception group if several
  providers fail.
- A provider may see an exchange twice if it partly wrote before raising. Chat never retries, so providers should
  tolerate a retry.

#### Replay

- When `start_conversation(...)` or `run(...)` opens a realtime session with an agent session, the conversation replays
  the history that `before_run` loads as conversation items:
  - user and system text;
  - agent turns, as assistant text. No provider can create assistant audio items, and the content type is `output_text`
    on GA-protocol services and `text` on Voice Live. In probes, replaying agent turns as text never pushed replies to
    text only;
  - function calls, each followed by its result;
  - cut-off turns, as their heard text.
- Replay skips text-less caller turns. Approval controls follow chat. `HistoryProvider.before_run` drops resolved ones,
  and pending ones drive the approval resume ([App-supplied input](#app-supplied-input)).
- Instructions and tools from `before_run` go into the initial session update, merged with the start options and tools.
- Agent Framework doesn't cap replay. Context providers, such as `CompactionProvider` and compaction strategies, keep
  history short, as in chat. OpenAI's session `truncation` setting trims server-side; Voice Live has no such setting.
- `RawRealtimeAgent` adds an `InMemoryHistoryProvider` when a session is passed and no provider loads history, as
  `Agent` does.

#### History in `run(...)`

- Each `run(...)` is one exchange with one `after_run`, as in chat.
- History stores text input as given, and audio input as its caller transcript, or as a text-less caller turn if none
  arrives. This differs deliberately from chat, which stores inputs verbatim.
- The run's fresh session closes through the close sequence, including the bounded transcript wait, so the input's
  transcript is in place before `after_run`. A run that paused for an approval or an app-run tool keeps those calls
  pending ([Cancellation and settlement](#cancellation-and-settlement)).
- `AgentResponse.messages` keeps the agent's audio for the caller, but context providers get a copy without audio.
- A failed `run(...)` reports nothing, as in chat, so a host can retry without storing the input twice.

#### Reading history during the call

`RealtimeConversation.history` is a read-only snapshot of the call's messages so far. It follows the order above,
carries the markers above, and includes exchanges not yet reported. After close it holds the final record, so an app can
build an end-of-call summary from it.

### Handoffs

Handoffs come in two layers:

- In core, `RealtimeConversation` tracks an **active agent** and gets `handoff(...)`, which switches the realtime
  session to another realtime agent's instructions, tools, voice and tool choice through a session update, then emits
  `handoff_completed`.
- In `agent-framework-orchestrations`, `RealtimeHandoffBuilder` shares `HandoffBuilder`'s topology API and builds a
  realtime agent that owns the call. `HandoffBuilder` stays chat-only.

A handoff lasts for the realtime session it happens in.

```python
receptionist = RealtimeAgent(
    client=voice_live_client,
    name="receptionist",
    description="Greets the caller and finds out what they need.",
    instructions="...",
    default_options={"voice": AzureVoice(type="azure-standard", name="en-US-AvaMultilingualNeural")},
)
billing = RealtimeAgent(
    client=voice_live_client,
    name="billing",
    description="Answers questions about bills and payments.",
    instructions="...",
    tools=[get_balance],
    default_options={"voice": AzureVoice(type="azure-standard", name="en-GB-SoniaNeural")},
)

contact_centre = (
    RealtimeHandoffBuilder(
        name="contact_centre",
        default_options={"instructions": "Speak as one assistant. Never mention handoffs."},
    )
    .participants([receptionist, billing])
    .with_start_agent(receptionist)
    .add_handoff(receptionist, [billing])
    .add_handoff(billing, [receptionist])
    .build()
)

async with contact_centre.start_conversation() as conversation:
    ...
```

#### Why not `HandoffBuilder`

ADR 0035 deferred realtime handoffs until `HandoffBuilder` was generalized beyond `Agent`. Generalizing it can't give
in-session handoffs, because `HandoffBuilder` assumes bounded per-turn runs. It clones a chat `Agent`, injects tools
through `default_options`, finds the handoff in a finished `AgentResponse`, runs one executor and agent session per
participant, waits on `request_info` between turns, and shares text-only history. A realtime agent inside it would
reconnect every turn. The OpenAI Agents SDK's `RealtimeSession`, LiveKit's `AgentSession` and Semantic Kernel-based
realtime orchestrations all hand off in whatever owns the provider's realtime session, by updating that session.

Only the topology half is shared. The return type, participants sharing one realtime session, the mechanics, the
turn-based options, history and events all differ. Folding them into the stable `HandoffBuilder` would make `build()`'s
return type depend on its participants and leave five options raising. This design also rejects agent-declared handoffs,
such as `RealtimeAgent(handoffs=...)` in the style of the OpenAI Agents SDK. They'd add a topology concept that chat
`Agent` lacks, and cycles such as billing and receptionist handing to each other need agents changed after construction.
The original request, microsoft/agent-framework#728, asked for the session to be "updated upon agent handoffs when used
in an orchestration pattern".

#### `handoff(...)` and the active agent

- **`await conversation.handoff(agent, *, tools=None)`**, plus a read-only `conversation.active_agent`, which starts as
  the agent that started the conversation.
  - `tools` adds tools until the next handoff. The builder passes the target's handoff tools through it, so the builder
    needs no private API.
  - A handoff tool reaches it through `ctx.realtime_conversation`. App code can call it directly, for example to
    escalate.
- **It has `update_session(...)`'s contract.** It's serialized with the other control calls and returns once
  `session.updated` arrives. Responses requested afterwards use the target; a response already in flight doesn't change.
  On a rejection or timeout it raises, and the previous agent stays active. The provider may still have applied a
  handoff whose acknowledgement timed out.
- **One client.** The target must use the conversation's client instance, or `handoff(...)` raises `ValueError` before
  sending anything. A realtime session can't change provider or model.
- **`handoff_completed`.** On success, `handoff(...)` emits one update with realtime-event content of kind
  `handoff_completed`, carrying `source` and `target`, each named as `HandoffSentEvent` names agents, by `name`, else
  `id`. `handoff(...)` emits it before returning, so it precedes the handoff tool's result. The conversation emits
  nothing at start, because `active_agent` and `author_name` already say who's speaking.
- **Attribution.** Assistant and tool messages carry the `author_name` of the agent that was active when their response
  was created, and updates also carry its `agent_id`.

#### What a handoff changes

A handoff changes who's speaking, not how the call works. `handoff(...)` recomputes four settings exactly as
`start_conversation(...)` computed them, with the target in place of the starting agent:

- Instructions are the target's, with the call-wide instructions appended, as chat appends run and provider
  instructions. Call-wide instructions are those in a built handoff agent's `default_options`, in the start options, and
  from context providers.
- Tools are the target's, plus `start_conversation(tools=...)`, context-provider tools and `handoff(tools=...)`. Running
  calls continue, and a late call to a removed tool gets an error result.
- Voice and `tool_choice` are the target's, unless the start options set one, which then holds for the whole call.

Nothing carries over from the previous agent. A target with no `tool_choice` gets the start value or the provider
default, so a receptionist's `"required"` can't leak into billing. Applying a target's whole `default_options` instead
would leak the previous agent's other keys, because resetting them needs every provider's defaults. Everything else
stays as the call started: audio formats, turn detection, transcription, noise reduction, temperature and token limits.

**A voice change never fails a handoff.** Where the provider refuses the change, the client drops the voice with a
warning ([Live conversation controls](#live-conversation-controls)), and the new agent speaks in the old voice. That
happens on Azure OpenAI (and, inferred, OpenAI) after the first audio, on Voice Live from an OpenAI voice, and on
Foundry from an Azure to an OpenAI voice. Failing the handoff would keep the caller with the wrong agent over a cosmetic
difference, and reconnecting is out of scope. Per-agent voices need Azure voices on Voice Live or a Foundry voice agent
([capabilities](#provider-capabilities)).

#### What stays with the conversation

- **History and context providers** stay with the agent that started the conversation. `before_run` doesn't run again;
  each handoff re-applies its instructions and tools.
- **Tool-loop limits** come from that agent's `function_invocation_configuration`, for the whole call. A tool chain that
  spans a handoff counts as one chain.
- **Function middleware follows the active agent.** Each call runs through the middleware of the agent that was active
  when the call arrived, as each chat handoff participant runs with its own. The source agent's middleware can block its
  handoff tool, and billing's middleware guards billing's tools.
- **MCP tools.** A target's MCP tools connect before the session update if they aren't connected yet. Entering the agent
  beforehand avoids the wait.

#### `RealtimeHandoffBuilder`

- **It's its own class in `agent-framework-orchestrations`**, sharing a topology base with `HandoffBuilder`.
  - `participants(...)`, `with_start_agent(...)` (required), `add_handoff(source, targets, *, description=...)`, the
    default all-to-all mesh and their validation are identical, so an app's topology code differs only in the class
    name.
  - The base is an internal refactor of `HandoffBuilder`, with no behavior change.
  - Chat's turn-based options don't carry over, because they have no meaning on a live call: `with_checkpointing`,
    `with_termination_condition`, `with_autonomous_mode`, `enable_return_to_previous`, `output_from` and
    `intermediate_output_from`.
- **Participants** are realtime agents on one client instance. A built agent can't be a participant.
- **The built agent owns the call.** The builder takes:
  - `name` and `description`, as `HandoffBuilder` does;
  - `context_providers`, as `Workflow.as_agent()` does;
  - call-wide `default_options`, such as audio formats, turn detection and transcription. The built agent appends
    their instructions to every participant's, so the app writes a shared rule once;
  - `function_invocation_configuration`;
  - `middleware` ([Middleware and telemetry](#middleware-and-telemetry)).
- **Participants supply who's speaking**, with their four settings, function middleware and MCP tools. `build()` warns
  about participant settings that won't apply, the start participant's included.
- **`build()` returns a realtime agent** with `RealtimeAgent`'s layers. It has `start_conversation(...)`, `run(...)`,
  sessions, `name`, `id` and `description`, so anything that hosts a realtime agent hosts a handoff orchestration. Its
  conversations start with the start agent active and its handoff tools installed. `async with` it enters every
  participant, so no handoff waits for an MCP connection.
- **On its conversations, `handoff(...)` follows the topology.** It accepts only participants, raising `ValueError`
  before sending for anything else, and adds the target's handoff tools itself, however the handoff is triggered.
- **Handoff tools copy chat's.** Each is named `handoff_to_<name>`, using the target's `name`, else its `id`. It takes
  no arguments, never needs approval, and returns `{"handoff_to": "<name>"}`. Its description comes from
  `add_handoff(description=...)`, else the target's `description`, else "Handoff to the <name> agent."
- **The tool body awaits `handoff(...)`**, so the switch takes effect before the result and the follow-up response go
  out. If the handoff fails, the tool catches the error and returns a result telling the model that the handoff failed
  and to carry on as the current agent. A raised error would reach the model as an opaque "Function failed", because
  Agent Framework hides exception text by default. In the prototype, the model then retried the handoff twice and told
  the caller to call billing directly.
- **Handoff tools are ordinary function tools.** They stream and enter history like any call, and function middleware
  can block them. Apps that hide them filter on the `handoff_to_` prefix and show `handoff_completed` instead.
- **The builder injects no instructions**, as in chat. Silent handoffs stay a matter of prompting.
- **Of two handoffs in one response, only the first happens.** The first call to reach `handoff(...)` wins, and the
  others fail and return the failure result. Chat performs neither of two handoffs, but a voice switch can't be taken
  back.

#### Session lifetime

- **A handoff lasts for its realtime session.** Each `start_conversation(...)` and each `run(...)` starts with the start
  agent.
- A host that calls `run(...)` once per turn therefore starts each turn back at the start agent. That agent sees the
  earlier handoff in the replayed history and can hand off again.
- Remembering the active agent across realtime sessions can be added later as an opt-in without breaking anything.

#### Foundry voice agents in a topology

- **Only the start agent can be a Foundry voice agent.** Each Foundry voice agent has its own endpoint, and every other
  participant must share the start agent's client.
- **Its four settings come from its published agent definition**, which the client captures at start
  ([Foundry voice agents](#foundry-voice-agents)). The built agent needs them twice: at start, to add the start agent's
  handoff tools without dropping the definition's own tools, and on a handoff back to it.
- **Definitions published by Semantic Kernel need republishing.** Semantic Kernel names functions
  `<Plugin>-<function>`, so its definitions' tool names don't match Agent Framework's tool implementations, and their
  handoff tools (`Handoff-handoff_to_<name>`) duplicate the ones the builder adds.

### Middleware and telemetry

Realtime agents get a fourth middleware category and a telemetry layer that treats each live conversation as one agent
invocation:

- **Conversation middleware** runs whenever a realtime session opens, live or inside `run(...)`. Its hooks see the
  session's inbound sends, outbound updates and close, so one policy covers live and bounded use.
- Agent middleware keeps wrapping `run(...)` only, function middleware keeps wrapping each tool call, and chat
  middleware has no seam.
- Telemetry follows the realtime conventions in semantic-conventions-genai. Each conversation is one `invoke_agent`
  span, with a `realtime_inference` span per response and its tool and control calls beneath it.
- Telemetry never records audio, and counts cancelled tool calls as failures.

This replaces ADR 0035's deferral of dedicated realtime middleware.

#### Interception points

Full-duplex traffic enters middleware and telemetry at these points:

| Traffic | Middleware | Telemetry |
| --- | --- | --- |
| A whole `run(...)` | agent middleware | the conversation span, over the run's temporary realtime session |
| A realtime session opening, live or in `run(...)` | conversation middleware's `process()` | the conversation span, and the session-started event |
| Inbound `send(...)` | conversation middleware's send hooks | input messages on `realtime_inference` spans (sensitive data on) |
| Outbound updates | conversation middleware's update hooks | `realtime_inference` spans, events and metrics |
| Tool calls | function middleware | `execute_tool` spans |
| Control calls | none | `realtime.*` control spans |
| Close | conversation middleware's close hooks | the end of the conversation span, and the session-ended event |

For audio-only traffic, telemetry never records audio, even with sensitive data on, and records transcripts instead.
Middleware sees audio as `data` content and must not assume an update has text.

#### Middleware

- **Categories on a realtime agent.**
  - Agent middleware wraps `run(...)` only. `start_conversation(...)` skips it with a warning, and raises
    `MiddlewareException` for a `MiddlewareBundle` with an agent member. Middleware written for `run(...)` reads
    `context.messages` and a final `context.result`, and some re-enters `call_next()` (`AgentLoopMiddleware`), which
    would open a second realtime session.
  - Chat middleware has no seam, because a realtime agent makes no chat model call. Agent Framework's seam rule,
    `categorize_middleware(supported_categories=...)`, applies. The agent skips bare chat middleware with a warning, and
    raises for a bundle with a chat member. The agent-hooks bundle therefore raises, because its model-call hooks live
    on the chat seam.
  - Function middleware is unchanged. It comes from the agent's `middleware=` list and follows the active agent.
  - Conversation middleware is new.
- **`ConversationMiddleware`**, under the `REALTIME` flag:
  - Its method is `process(context, call_next)`. Agent Framework recognizes a plain function as conversation middleware
    by its `ConversationContext` parameter annotation, as it recognizes the other categories by theirs
    (`MiddlewareType.CONVERSATION`). A chat `Agent` skips conversation middleware with a warning, by the same seam rule.
  - Its chain runs once whenever a realtime session opens, in `start_conversation(...)` and inside each `run(...)`.
  - `ConversationContext` holds the agent that started the conversation, plus `session`, `options`, `tools` and
    `metadata`, which middleware can change before `call_next()`, as on `AgentContext`. Once the session is open, it
    also holds `conversation`.
- **Hooks.** Middleware registers three hook lists on the context, named like `AgentContext`'s stream hooks:
  - **Send hooks** see every value the app passes to `send(...)`, after normalization: each `Message` (text, audio
    chunks, app-supplied tool results and approvals), or a raw `RealtimeEvent`.
  - **Update hooks** see every update the conversation yields, audio included.
  - **Close hooks** run once, at the end of the [close sequence](#close), after the conversation has reported history
    and `conversation.history` is final. They get the exception that ended the conversation, or `None`.
  - Control calls and the tool loop's own results get no hooks. They're commands the app or its tools issue on purpose,
    and what they produce, such as a `say(...)` line, streams through the update hooks.
- **Outcomes.**
  - A send or update hook returns a replacement, or `None` to leave the item unchanged, as `ResponseStream` transforms
    do.
  - Raising `MiddlewareTermination` in a hook drops that item, so the conversation doesn't send the input or deliver
    the update.
  - Any other exception ends the conversation fail-closed, as `MiddlewareFailure` does in the tool loop. The close
    sequence runs, then `send(...)` or iteration raises the exception. Close-hook failures raise from `close()` after
    cleanup, alongside history-provider failures.
  - Raising `MiddlewareTermination` in `process()` before `call_next()` refuses the realtime session, so
    `start_conversation(...)` or `run(...)` raises it.
- **Order.** Hooks run in onion order, as agent middleware's stream hooks do. The outermost middleware sees inputs first
  and updates last.
- **What each layer sees.**
  - History and telemetry record what reached or came from the provider: inputs after the send hooks, and updates before
    the update hooks.
  - The tool loop and the control calls act on the provider's events, so dropping a `function_call` update doesn't stop
    the call.
  - `run(...)` builds its `AgentResponse` from what the update hooks deliver.
  - A guardrail that trips calls `context.conversation.interrupt(...)` and drops the rest of that response's updates.
    In the prototype, hooks cost 12 µs per update with one hook and 21 µs with 21, audio included.
- **Handoffs.**
  - Conversation middleware belongs to the conversation. It comes from the agent that started it, a plain realtime agent
    or a built handoff agent, and applies for the whole call, like history and context providers.
  - `RealtimeHandoffBuilder` takes `middleware=` for the built agent. Its agent and conversation middleware apply as on
    any realtime agent. Its function middleware wraps every tool call, outside the active participant's own.
  - Participants' own agent and conversation middleware don't apply, and `build()` warns about them, as it does for
    other participant settings.
- **Per call.** `run(...)` and `start_conversation(...)` take `middleware=`, which joins the agent's own list, sorted by
  category. Agent middleware wraps that `run(...)`, conversation middleware wraps that realtime session, and function
  middleware wraps its tool calls. `start_conversation(...)` treats agent middleware passed to it as above.
- **`RawRealtimeAgent` runs no middleware**, as `RawAgent` runs no agent middleware. Its tool loop still runs tools.

Agent middleware can't safely wrap a live conversation, because it reads a final result and may re-enter `call_next()`.
Observers alone couldn't enforce a guardrail. The hooks touch history, the tool loop and the controls, so defining them
now costs less than adding them later. In prior art, the OpenAI Agents SDK interrupts a response when a transcript
guardrail trips, LiveKit and Pipecat offer pipeline nodes and observers, and Microsoft.Extensions.AI wraps the realtime
client.

#### Where telemetry lives

- **At the agent layer, in core.** `RawRealtimeAgent` emits nothing. `RealtimeAgent`'s telemetry layer wraps `run(...)`
  and `start_conversation(...)`, opens the conversation span, and instruments the conversation from the event kinds, so
  every provider, third-party ones included, gets the same spans, events and metrics. The agent layer also knows the
  active agent, the tools and the controls, which client-level spans don't.
- **Provider clients get no telemetry layers**, so there are no `Raw*` client classes, only `OpenAIRealtimeClient`,
  `VoiceLiveClient` and `FoundryVoiceAgentClient`. They supply the provider name, server address and model. This
  replaces ADR 0035's `RawOpenAIRealtimeClient` / `OpenAIRealtimeClient` example. A provider client used without an
  agent emits no telemetry.
- `FoundryVoiceAgent` sits over a `RawFoundryVoiceAgent`, as `FoundryAgent` sits over `RawFoundryAgent`. The built
  handoff agent gets `RealtimeAgent`'s layers.
- .NET instruments realtime at the client, through Microsoft.Extensions.AI. Its per-response spans have no agent
  context, and their measurements accumulate over the session.

#### Span tree

```text
invoke_agent {owner}                 INTERNAL: the conversation span
├── realtime_inference {model}       CLIENT: one per response
├── execute_tool {tool}              INTERNAL: one per call that ran
│   └── realtime.update_session      a control call made inside a tool
├── realtime.create_response | realtime.say | realtime.update_session | realtime.interrupt
└── realtime.handoff                 only for handoff(...) called outside a tool
```

- **The conversation span** opens at `start_conversation(...)` under whatever span is current, and ends at close.
  `run(...)` gets the same tree over its temporary session.
  - It's `invoke_agent {owner}`, named for the agent that started the conversation. A built handoff agent names itself,
    not a participant.
  - Its attributes are chat `invoke_agent`'s, plus `gen_ai.realtime_session.id` once `session_created` arrives, and the
    usage summed over every response.
  - Every call becomes one trace without app work, as LiveKit and Pipecat do with their session spans. The conventions
    define no session or turn span, and leave an `execute_tool` span's parent to "the active context in that layer".
- **Background tasks.** Every task the conversation starts (the receive loop, tool batches and exchange reports) runs in
  a copy of the context captured inside the conversation span at start.
  - `realtime_inference` and `execute_tool` spans are its children, and siblings of each other, as `chat` and
    `execute_tool` spans are in chat. Parenting a tool's span under the response that requested it would parent it
    under a span that has usually ended, because batches run after `response_done`.
  - Spans the app opens inside a tool nest under its `execute_tool` span. Context providers' own spans nest under the
    conversation span.
  - Context variables the caller set, such as a host's conversation id, carry over.
- **Control spans.**
  - Each public `update_session(...)`, `create_response(...)`, `say(...)` and `interrupt(...)` call gets a short
    INTERNAL span, named in Agent Framework's style as `realtime.update_session` and so on, like `executor.process`.
  - It carries the realtime session id, the provider, and the id of any response the call created.
  - The span records any error the call claims, benign races included, with the provider's code as `error.type` and
    ERROR status. The `response_cancel_not_active` that `interrupt(...)` ignores isn't an error. These spans also show
    the 53 to 376 ms that `session.updated` takes, which is the latency every handoff adds.
  - The tool loop's follow-up requests get no control span, because their `realtime_inference` spans show them.
- **Agents and handoffs.**
  - Each `realtime_inference` span carries `gen_ai.agent.name` and `gen_ai.agent.id` for the agent that was active when
    its response was created. Each `execute_tool` span carries them for the agent that was active when its call arrived.
  - There's no span per active-agent period. Responses in flight and tool calls straddle handoffs, so such spans
    couldn't contain them.
  - A handoff tool's `execute_tool` span uses the conventions' transfer refinement: `gen_ai.transfer.mode=pass_control`
    and `gen_ai.transfer.target.name`. As the conventions ask, the `handoff(...)` inside it opens no span of its own.
  - A `handoff(...)` called outside a tool call can't use the refinement, so it gets a `realtime.handoff` span with
    Agent Framework-defined source and target attributes. A handoff's own session update opens no separate control span.
  - Any span that names a `FoundryVoiceAgent` also carries `gen_ai.agent.version` when the agent pins one.

#### Conventions and identity

- **Target.** Realtime follows
  [semantic-conventions-genai](https://github.com/open-telemetry/semantic-conventions-genai) at commit `750e42c`, the
  merge of
  [Add semantic conventions for realtime GenAI sessions](https://github.com/open-telemetry/semantic-conventions-genai/pull/394),
  in Agent Framework's default latest mode. The repository has never released (its `schema_url` is `1.42.0-dev`), and
  the released OpenTelemetry GenAI conventions say nothing about realtime, so this ADR pins the commit to keep later
  changes visible.
  - Under the v1.36 baseline, realtime emits the same spans. Attributes that differ by mode (message capture, cache and
    reasoning usage, tool arguments) follow the mode, as chat's do.
  - Realtime reuses chat's attribute helpers where they fit.
- **Mapping.** The [per-provider mapping table](https://github.com/adamdougal/agent-framework/blob/research/realtime-telemetry-mapping/docs/research/realtime-telemetry-mapping.md#mapping-table)
  is normative, with these corrections from the live probes:
  - reasoning tokens are counted inside `text_tokens`, and also reported in an undocumented `reasoning_tokens`;
  - `gpt-transcribe` reports transcription usage as `duration`;
  - Voice Live reports no transcription usage;
  - every failed Voice Live response reports `server_error`;
  - content-filter blocks never carry a category.
- **Provider names.** `OpenAIRealtimeClient` reports `openai`, or `azure.ai.openai` with Azure routing, as chat does.
  `VoiceLiveClient` and `FoundryVoiceAgentClient` report `azure.ai.foundry`, like the rest of `agent-framework-foundry`,
  because the registry has no entry for either service. `server.address` and the model tell the two services apart.
- **Ids.**
  - `gen_ai.conversation.id` follows chat's rule, using an id a host supplies, or else a string `service_session_id`. v1
    realtime sets none unless a host supplies one, and the conversation captures it at start. The conventions forbid
    using it for the realtime session id.
  - `gen_ai.realtime_session.id` is the provider's session id, from `session_created`.
  - A Foundry voice agent's stored `conversation_id` stays in `foundry` metadata.
- **Session events.** `gen_ai.client.realtime_session.started` and `.ended` go out through Agent Framework's event
  logger for every realtime session, live or inside `run(...)`. They're INFO, or WARN with `error.type` when the session
  dies. Exporters receive the started event at once, unlike the conversation span.

#### Responses and usage

- **Finish reasons on spans follow the conventions**, so they can differ from the update's `finish_reason`, which keeps
  chat's vocabulary:
  - `stop`, `tool_call`, `content_filter` or `length` for responses that finish;
  - `turn_detected` or `client_cancelled` for cancelled responses, taken verbatim from the provider. A response with no
    reason gets `interrupted`, including one still running when the conversation closes;
  - `error` for failed responses, and for a realtime session that dies mid-response. These spans get the provider's code
    as `error.type` and ERROR status.
  - A content-filter block and an interruption aren't errors, so their span status stays unset.
- **No `gen_ai.response.status`.** The conventions define it only for `fetch_response`. Apps read the raw status from
  `response_done`.
- **Cancel reasons stay verbatim, even where they mislead.** With Azure voices, a client cancel can report
  `turn_detected` and a barge-in `client_cancelled`. Agent Framework can't correct this reliably, because it doesn't
  track raw cancels, and an `interrupt(...)` sent on `speech_started` races the server's own cancel. The
  [capability table](#provider-capabilities) says so, and `realtime.interrupt` spans show the app's own cancels.
- **Telemetry records empty cancelled responses.** Voice Live's server VAD can split one utterance into several turns
  when its silence duration is short, such as the 200 ms the probed sessions defaulted to. Each turn starts a response
  that the provider cancels at once, often with zero usage. Each is still a generation the provider started, and seeing
  them is how an app spots an over-eager VAD. The docs point to `silence_duration_ms` and semantic VAD.
- **Usage keys.**
  - `UsageDetails` gains standard audio, text and image keys in Agent Framework's naming, one per convention attribute:
    `audio_input_token_count`, `text_output_token_count`, `image_cache_read_input_token_count` and their siblings. In
    latest mode they map to `gen_ai.usage.{text,audio,image}.{input,output}_tokens` and `.cache_read.input_tokens`.
  - Realtime clients fill them. Chat clients can adopt them later.
  - Voice Live and Foundry fill them with what the provider reports, like any provider. The capability table documents
    that cascaded and BYOM sessions convert Azure Speech seconds into `audio_tokens`, and that BYOM with `azure-speech`
    transcription counts input audio twice. Their other extras stay in `voice_live` and `foundry` metadata.
  - Transcription usage stays out of telemetry, in the caller transcript's metadata.
- **Metrics.** Each realtime response records chat's `gen_ai.client.operation.duration` and
  `gen_ai.client.token.usage`, with `gen_ai.operation.name=realtime_inference`, so existing dashboards keep working.
  Tools keep `agent_framework.function.invocation.duration`. Agent Framework defines no realtime-specific metrics; apps
  count handoffs, filtered responses and interruptions from updates.

#### Errors and cancellation

- **A session that dies** (a socket or receive-loop failure, backpressure overflow, `MiddlewareFailure`) marks the
  conversation span ERROR with `error.type`. It also records the provider's close code when there is one; both Azure
  transports drop close reasons but keep the codes.
- **A normal close**, hang-up included, leaves the conversation span's status unset. If `close()` raises because history
  providers still fail, the span is ERROR, as chat's `invoke_agent` is when `after_run` raises.
- **Unclaimed provider errors never change a span's status.** Most are recoverable, and some are benign races. Each
  becomes an `agent_framework.realtime.error` event on the conversation span, with the provider's code as `error.type`,
  or its `type` if there's no code. The event includes the message only when sensitive data is on. History-provider
  failures during the call get the same event, naming the provider.
- **Transcription failures and Voice Live warnings** stay on the update stream only.
- **Cut-off playback.** Each `item_truncated` becomes an `agent_framework.realtime.item_truncated` event on the
  conversation span, with the response id, item id and `audio_end_ms`. By then the response's span has usually ended
  with `stop`, and its finish reason stays as reported.
- **Cancelled tool calls count as failures.**
  - `FunctionTool.invoke` catches only `Exception`, so today a cancelled call ends its span with status unset and
    records its duration as a success.
  - Cancellation becomes ERROR, with `error.type` set to `cancelled`, or to `conversation_closed` when closing the
    conversation cancelled the call. The conversation passes the reason as the cancel message. Chat runs abandoned
    mid-tool get the fix too.
  - The fix ships first, as its own PR.
  - Calls that never ran get no `execute_tool` span. Their settlement shows in updates and history.
- **Exchange reports get no spans**, as context providers get none in chat.

#### Content capture

- **Telemetry never records audio**, even with sensitive data on, whether as `blob` parts, attributes or events. Agent
  Framework's message serializer would otherwise write audio as base64, and the conventions' handling of binary content
  is unresolved. Call recording stays in the app.
- **With sensitive data on**, spans record content as follows:
  - each `realtime_inference` span records its output as the agent transcript in `transcription` parts, plus text and
    `tool_call` parts;
  - its input is the app input sent for it, plus the caller transcript if it's final before the span ends. The span
    leaves out a late transcript rather than staying open for it;
  - it records `gen_ai.system_instructions` as they were when the response was created, so handoffs are visible;
  - the live conversation span records no messages, because a call's transcript has no size limit;
    `conversation.history` and the history providers are the record. `run(...)`'s `invoke_agent` span records input and
    output as chat does;
  - `execute_tool` arguments and results, and the rules for baseline versus latest mode, follow chat.

#### Azure SDK instrumentors and trace propagation

- **Instrumentors.** Both Azure SDKs ship opt-in OpenTelemetry instrumentors that would add a second span tree.
  `VoiceLiveInstrumentor` wraps the SDK's `send`, so it adds a span for every outbound event, including one per audio
  append. In the prototype, it emitted 1,176 spans in a 30-second call, 1,170 of them for audio appends. It doesn't wrap
  `recv_bytes()`, which Agent Framework receives through. `AIProjectInstrumentor` wraps realtime `send`, `recv` and
  `close` from `azure-ai-projects` 2.9. Each Azure client logs one warning when its SDK's instrumentor is active, naming
  the duplicate spans, including the span per audio append, and how to turn the instrumentor off. The docs say the same.
- **`traceparent`.** `FoundryVoiceAgentClient` injects `traceparent` and `tracestate` from the trace context active when
  it connects, which is the conversation span, so Foundry's server-side spans join the call's trace. It never sends
  baggage, which can carry app data, and it skips the injection when the app's `AIProjectInstrumentor` already does it.
  No other provider documents joining a client trace; they can follow later.

### Provider clients and packages

Azure OpenAI is Azure routing on `OpenAIRealtimeClient`. Voice Live gets `VoiceLiveClient`, pinned to the documented,
preview-shaped `2026-07-15` API version. Foundry voice agents are first-class as `FoundryVoiceAgent`, under their own
preview flag. Both Azure clients live in `agent-framework-foundry` behind a `[realtime]` extra, and
`agent_framework.foundry` exposes them. Every client builds on one public GA-protocol base session in
`agent-framework-openai`, which maps wire JSON over a connection each client opens through its service's SDK, used only
as a transport. This replaces ADR 0035's provider scope, which named only OpenAI.

| Provider | Classes | Install | Transport |
| --- | --- | --- | --- |
| OpenAI, Azure OpenAI | `OpenAIRealtimeClient`, `OpenAIRealtimeOptions` | `agent-framework-openai[realtime]` | `openai` SDK |
| Azure Voice Live | `VoiceLiveClient`, `VoiceLiveOptions` | `agent-framework-foundry[realtime]` | `azure-ai-voicelive` |
| Foundry voice agents | `FoundryVoiceAgent`, `FoundryVoiceAgentClient`, `FoundryVoiceAgentOptions` | `agent-framework-foundry[realtime]` | `azure-ai-projects` (`.beta.voice_agents.realtime`) |

Gemini Live or another provider with a different protocol remains a useful later portability proof. The kinds and the
typed control methods are its seams.

#### Azure OpenAI

- **Azure routing on `OpenAIRealtimeClient`, mirroring `OpenAIChatClient`.** It takes `azure_endpoint`, `api_key` and
  `credential`; Entra tokens use the `cognitiveservices` scope. `model` is the deployment name. The `AZURE_OPENAI_*`
  fallbacks apply when no OpenAI key is set.
- The model variables are `OPENAI_REALTIME_MODEL` and `AZURE_OPENAI_REALTIME_MODEL`, falling back to `*_MODEL` like
  every other client.
- It differs from chat in two ways:
  - There's no `api_version`. The client connects to the GA route through
    `AsyncOpenAI(websocket_base_url=".../openai/v1")`, which rejects one.
  - Passing an `AsyncAzureOpenAI` as `async_client` raises an error that points to `azure_endpoint`, because its
    `realtime.connect()` always builds the legacy URL with an `api-version`.
- Chat has routed Azure OpenAI through the generic clients since [ADR 0021](0021-provider-leading-clients.md), and
  microsoft/agent-framework#4990 removed the `AzureOpenAI*` classes, so realtime gets no Azure-specific class either.

#### The shared base session

- **`OpenAIRealtimeSession`, public in `agent-framework-openai`,** is the extension point for services that speak the
  OpenAI Realtime protocol. It extends the core realtime session base the way `RawFoundryChatClient` extends
  `RawOpenAIChatClient`. It implements the typed control methods, the event kinds, string tool outputs, `output_text`
  replay, [error claiming](#live-conversation-controls) and the [barge-in cut](#barge-in). Voice Live's and Foundry's
  sessions subclass it privately and override only what differs.
- **It maps wire JSON, not SDK models.** Each client opens an `OpenAIRealtimeConnection`, a protocol with `send`,
  `receive` and `close`:
  - OpenAI and Azure OpenAI through the `openai` SDK;
  - Voice Live through `azure-ai-voicelive`, with `send(Mapping)` and `recv_bytes()`;
  - Foundry through `azure-ai-projects`, with `recv()` plus `as_dict()`.

  The clients use no other typed SDK models, and keep compression off. The connection maps each transport's errors and
  closes to the realtime exceptions. Both Azure SDKs drop close reasons, but close codes survive.
- **It lives outside core.** ADR 0035 keeps direct provider code outside core, and ADR 0021 keeps core lightweight.
  `agent-framework-foundry` already depends on `agent-framework-openai`, and wire JSON is the only format all the
  services share.

The clients use the service SDKs as transports rather than a raw websocket. Offline experiments with
`azure-ai-voicelive` 1.3.1 and `azure-ai-projects` 2.10.0 showed that:

- their dependencies are already in `agent-framework-foundry`'s closure, and Voice Live adds a single 161 KB wheel;
- `query` and `headers` override everything, so only the path is fixed. `send(Mapping)` is byte-identical,
  `recv_bytes()` returns exact frames, and `as_dict()` matched the wire JSON in every probe;
- Agent Framework pins `api_version`, no breaking change since `azure-ai-voicelive` 1.0.0 has touched the transport, and
  Foundry's `.beta` realtime module changed only additively from 2.7 to 2.10;
- they leave the handshake's hardening to the service teams. Since 1.3.1, the Voice Live SDK rejects redirects; in a
  local test, raw `websockets` and `aiohttp` both forwarded `api-key` across a cross-origin redirect.

They also give the same operations Agent Framework already uses on the `openai` transport, and follow its practice of
reaching Azure services through official SDKs. Other frameworks differ. LiveKit uses the Voice Live SDK, and Pipecat a
raw websocket. The evidence is in
[Azure realtime transports](https://github.com/adamdougal/agent-framework/blob/research/azure-realtime-transports/docs/research/azure-realtime-transports.md).

#### Voice Live

- **`VoiceLiveClient` pins `api-version=2026-07-15`,** the documented version. It keeps the OpenAI preview shape: a flat
  session, `response.audio.*` events and no `session.type`.
  - `api_version` accepts later dated versions of the same shape. It raises for the undocumented, GA-shaped `v1`.
  - Its session overrides payload building and maps the preview event names to the kinds, keeping `service_event_type`
    verbatim. For example, `conversation.item.created` maps to `item_added`, and `conversation.item.truncated`
    (including from `auto_truncate`) maps to `item_truncated`.
  - A GA-shaped version can follow once it's documented. The kinds hide wire names, so moving changes no public API.
- **Managed models and BYOM.** A managed model needs only `model=`. BYOM takes `profile=`, `model=<deployment>` and an
  optional `foundry_resource_override`. The client sends them through the SDK's `query`, because the SDK silently
  ignores a `profile=` keyword and rejects `foundry_resource_override=` outside agent mode.
- **It authenticates with `api_key` or `credential`.** Entra tokens default to `https://ai.azure.com/.default`, the
  documented default and the SDK's own, and the `cognitiveservices` scope also works. A small adapter covers
  token-provider callables and sync credentials, which the SDK doesn't accept. Agent Framework's User-Agent goes through
  `headers`.
- It follows Voice Live's wire rules:
  - The client sends its first `session.update` before anything else, even with no options. Voice Live rejects every
    earlier event, and closes the connection after five.
  - It drops per-response `instructions` with a warning, because Voice Live ignores them.
  - `say(...)` sends `pre_generated_assistant_message` with an Azure voice, and raises `RealtimeUnsupportedException`
    with an OpenAI voice. The voice type comes from the latest `session.updated`.
  - Voice Live drops invalid events silently and never echoes `event_id`, so the client tracks the active response
    itself.
- `get_mcp_tool()` and `get_foundry_agent_tool()` declare hosted tools.
- It reads `AZURE_VOICELIVE_ENDPOINT` and `AZURE_VOICELIVE_API_KEY`, the names in the SDK's samples, plus
  `AZURE_VOICELIVE_MODEL` and `AZURE_VOICELIVE_API_VERSION`.

#### Foundry voice agents

- **`FoundryVoiceAgent` is first-class, mirroring `FoundryAgent`.** The start agent of a handoff topology needs its
  definition's settings at start and on a handoff back, and other participants share its client but bring their own
  settings, so it needs a type of its own.
  - `FoundryVoiceAgent` takes either `project_client`, or `project_endpoint`, `agent_name`, `agent_version` and
    `credential`. It reads the same `FOUNDRY_PROJECT_ENDPOINT`, `FOUNDRY_AGENT_NAME` and `FOUNDRY_AGENT_VERSION`
    variables as `FoundryAgent`.
  - The client sends `agent_version` only as the `x-agent-version-override` header. `azure-ai-projects` 2.7.0 crashes on
    `connect(agent_version=...)`, because it forwards the keyword into aiohttp's `ws_connect`.
  - `credential` uses `https://ai.azure.com/.default`, the only scope Foundry accepts.
  - Other participants use `foundry_agent.client`.
- **The agent definition owns instructions, voice, tool choice and tools.**
  - Foundry applies it in an unsolicited `session.updated` 28 to 52 ms after `session.created`. The client waits for it
    before sending anything, so it never mistakes that update for an acknowledgement. It raises if the definition
    doesn't arrive within the acknowledgement timeout, and keeps the definition read-only.
  - Then the handoff rules apply. The conversation appends other instructions, and start options can override voice
    and tool choice.
  - The definition carries its voice as a name plus a sibling `voice_type`, and Foundry rejects the name alone. So
    `FoundryVoiceAgent` restores the voice as an `AzureVoice` built from `voice` and `voice_type`, and `say(...)` reads
    the type to know the voice is Azure.
- **`tools=` supplies implementations only,** matched by name, as in `FoundryAgent`. A definition function with no
  implementation gets an error result, plus a warning at start.
- **Added tools** come from handoffs, `start_conversation(tools=...)` and context providers. The client declares them
  alongside the definition's tools, which it re-sends as published so that the definition's MCP, toolbox and system
  tools survive. When nothing is added, the client sends no `tools` key.
- It connects through `AIProjectClient.beta.voice_agents.realtime.connect()`, always with `api-version=v1` and
  `Foundry-Features: VoiceAgents=V1Preview`. There's no `allow_preview` parameter, because no non-preview route exists.
- Foundry acknowledges client items with `conversation.item.created` and server items with `conversation.item.added`.
  Both map to `item_added`.
- Structured inputs and the per-session `store` override aren't in v1. Each can be added later without breaking
  anything.

#### Packages, extras and namespaces

- **Extras.**
  - `agent-framework-openai[realtime]` installs `openai[realtime]` and takes its `websockets>=13,<16` bound. The
    workspace lock moves from `websockets` 16.1.1 to 15.x, which `uvicorn[standard]` and `google-genai` accept.
  - `agent-framework-foundry[realtime]` adds `azure-ai-voicelive[aiohttp]>=1.3.1,<2`, because 1.3.1 brings the redirect
    fix. It also raises `azure-ai-projects` to `>=2.7,<2.8`, which needs `openai>=3`.
  - Plain installs don't change. Without the extra, both Azure clients raise an install hint when called.
- **Namespaces.** `agent_framework.openai` and `agent_framework.foundry` re-export the new classes. Under
  [ADR 0048](0048-python-content-identity-and-protocol-metadata.md), fields the base session writes go in the `openai`
  metadata namespace, and service extras in `voice_live` and `foundry`, as the Foundry chat client already does.
- **No capability API in v1.** The rules vary by provider and by voice type, and clients already enforce them by
  dropping, warning or raising. The [capability table](#provider-capabilities) documents them. A typed capability object
  can be added later without breaking anything.
- **No `FoundryRealtimeClient` in v1** for realtime models deployed in a Foundry project.
  `OpenAIRealtimeClient(azure_endpoint=...)` already reaches a Foundry resource's Azure OpenAI endpoint, and whether the
  project-scoped route serves realtime at all is unverified.

### Provider capabilities

Providers differ by service and by voice type. Clients enforce the differences, and this table documents them. It comes
from live probes on 2026-11-04 and the prototype on 2026-11-06 against Azure OpenAI (GA, `gpt-realtime-2.1` and
`gpt-realtime`), Voice Live `2026-07-15` (managed, cascaded and BYOM) and a published Foundry voice agent. **The probes
didn't cover OpenAI Realtime, so its column is inferred from Azure OpenAI, which speaks the same GA protocol, until live
tests verify it.**

| Capability | OpenAI (inferred) | Azure OpenAI | Voice Live `2026-07-15` | Foundry voice agent (preview) |
| --- | --- | --- | --- | --- |
| Voice change after the first audio | No | No: `cannot_update_voice`. The client drops `voice` with a warning. | Azure voices: yes. From an OpenAI voice: ignored silently (`session.updated` echoes the old voice), and the client warns. | Azure to Azure: yes. Azure to OpenAI: rejected even before any audio, so the client drops `voice` before sending. |
| Verbatim speech (`say(...)`) | Best-effort prompt | Best-effort prompt | Azure voice: `pre_generated_assistant_message`, spoken word for word and recorded. OpenAI voice: unsupported; `say(...)` raises. | Azure voice: `pre_generated_assistant_message`. OpenAI voice: best-effort prompt (not probed). |
| Per-response overrides on `response.create` | Yes | Yes | `instructions` ignored, so the client drops them with a warning | Yes |
| `metadata` echo on `response.created` | Yes | Yes | Yes, except `null` for pre-generated responses, so `say(...)` can't detect a lost race | Yes (pre-generated responses not probed) |
| Client `event_id` echo on errors | As Azure OpenAI | Yes, except for rejected tool schemas, `cannot_update_voice` and bad truncate offsets | Never: `null` or a server id. Some errors arrive three times. | Never: `null` or a server id |
| Errors for invalid events (`response.create` during a response, `response.cancel` with none) | As Azure OpenAI | `conversation_already_has_active_response` and `response_cancel_not_active` | None: such events are dropped silently, as are unknown event types. Every event before the first `session.update` is rejected. | None: dropped silently |
| Transcript read back after a truncate (`conversation.item.retrieve`) | Empty | Empty | Azure voice: the cut, word-aligned text. OpenAI voice: empty. | The cut, word-aligned text |
| Server-side truncation on barge-in (`auto_truncate`) | Rejected | Rejected | Supported, off by default. Cuts at the detected speech start; a later client truncate wins. | Not probed |
| Cancel reasons (`status_details.reason`) | `turn_detected` or `client_cancelled` | `turn_detected` or `client_cancelled` | Same values, but with an Azure TTS voice a client cancel can report `turn_detected` | With an Azure TTS voice, a client cancel can report `turn_detected` |
| Empty cancelled responses under server VAD | Not observed | Not observed | A short silence duration splits one utterance into several turns, each starting a response that's cancelled at once, often with zero usage. The probed sessions' default of 200 ms split every multi-sentence clip; 500 ms didn't split in the prototype. | Not probed |
| Content filtering | `incomplete` with `content_filter`; no category; never an `error` event | `incomplete` with `content_filter`; no category; never an `error` event | BYOM: as Azure OpenAI. Managed models were never content-filtered: they refused in their own words, so a content-filter fallback never triggers. | Not probed |
| `audio_tokens` in usage | The model's audio tokens | The model's audio tokens | Cascaded and BYOM sessions convert Azure Speech audio into tokens (10 per second of speech in, 20 per second synthesized out). BYOM realtime with `azure-speech` transcription counts input audio twice. | Converted from Azure Speech audio, as on Voice Live |

On every provider, an `interrupt(...)` sent on `speech_started` races the server's own cancel, so a barge-in can also
report `client_cancelled`. Spans report cancel reasons verbatim ([Responses and usage](#responses-and-usage)).

### Experimental features and release

- **One `ExperimentalFeature.REALTIME`** goes on every new public realtime API, in core and in the provider packages:
  `RealtimeAgent`, `RawRealtimeAgent`, `RealtimeConversation`, `RealtimeEvent` and its kinds, the client/session
  protocols, the realtime options types, `Content.from_realtime_event(...)`,
  `FunctionInvocationContext.realtime_conversation`, conversation middleware, the realtime exceptions,
  `RealtimeHandoffBuilder` and the provider clients. This follows Agent Framework's convention of one feature id per
  family (for example `VECTOR_STORES`, `EVALS` and `HARNESS`), so the family graduates together.
- **`FOUNDRY_VOICE_AGENTS`** replaces `REALTIME` on `FoundryVoiceAgent`, `RawFoundryVoiceAgent`,
  `FoundryVoiceAgentClient` and `FoundryVoiceAgentOptions`, so the realtime family can graduate without waiting for a
  preview service. `agent-framework-foundry` already splits `FOUNDRY_PREVIEW_TOOLS` from `FOUNDRY_TOOLS` by service
  stage, and warnings fire once per id.
- **Release.** This ADR proposes shipping the family as experimental in 1.x rather than waiting for the next major
  version. Every change to a stable API is additive, and this ADR lists each below. The maintainers make the final call.

### Changes to existing packages

Under the `REALTIME` flag:

- `agent-framework-core`:
  - the `realtime_event` content type, `Content.from_realtime_event(kind=...)` and the kind type. `ContentType` and
    `Content.from_dict` accept the new type, so serialized updates round-trip;
  - `FunctionInvocationContext.realtime_conversation`;
  - `ConversationMiddleware`, `ConversationContext`, `MiddlewareType.CONVERSATION`, and the conversation category in
    `categorize_middleware`;
  - `RawRealtimeAgent` (with `function_invocation_configuration=` and the automatic `InMemoryHistoryProvider`),
    `RealtimeAgent`, `RealtimeConversation` (with `history`, `active_agent` and `handoff(...)`), `RealtimeEvent`, the
    client/session protocols, `RealtimeOptions` and its turn-detection, transcription and noise-reduction types, and
    the realtime exceptions;
  - `middleware=` on `run(...)` and `start_conversation(...)`;
  - the kinds, including `item_truncated` and `handoff_completed`, and `function_result` updates for every settled
    call;
  - the realtime span and event names: the `realtime.*` control spans, and the `agent_framework.realtime.error` and
    `agent_framework.realtime.item_truncated` events.
- `agent-framework-orchestrations`: `RealtimeHandoffBuilder` and its built agent, including `middleware=`.
- `agent-framework-openai`: `OpenAIRealtimeClient`, `OpenAIRealtimeOptions`, `OpenAIRealtimeSession`,
  `OpenAIRealtimeConnection` and the `[realtime]` extra.
- `agent-framework-foundry`: `VoiceLiveClient`, `VoiceLiveOptions`, `AzureVoice`, `AzureTurnDetection`,
  `AzureTranscription`, `AzureNoiseReduction` and the `[realtime]` extra; and, under `FOUNDRY_VOICE_AGENTS`,
  `FoundryVoiceAgent`, `RawFoundryVoiceAgent`, `FoundryVoiceAgentClient` and `FoundryVoiceAgentOptions`.

On stable APIs, all additive:

- `Content.from_text(status=...)`, and text merges that keep the latest status.
- Standard audio, text and image `UsageDetails` keys and their telemetry mapping.
- `FunctionTool.invoke` records a cancelled call as ERROR with `error.type`, instead of as a success. This fixes chat's
  telemetry too, and ships first as its own PR.
- `RawAgent`'s MCP handling moves into an internal helper shared with `RawRealtimeAgent`, with no behavior change.
- `HandoffBuilder`'s topology moves into an internal base shared with `RealtimeHandoffBuilder`, with no behavior change.
- `ExperimentalFeature.REALTIME` and `ExperimentalFeature.FOUNDRY_VOICE_AGENTS`.

### Half-duplex behavior

The first direct realtime implementation should open a fresh ephemeral realtime session per `run(...)`. It sends the
input, requests or commits a response, drains until the terminal response event, and closes the realtime session. Warm
connection reuse is a later optimization because it complicates cancellation, session compatibility, and resource
ownership.

- **The run's terminal response** is the first response that finishes without tool calls. Tool batches and follow-ups in
  between run as in a live conversation, with chat's per-run limits.
- **Approval requests and calls to app-run tools end the run**, as in chat. The next run resumes them by replaying the
  call into its new realtime session.
- **Every run starts fresh.** It replays the agent session's whole history into its new realtime session and starts with
  the start agent active, so a handoff lasts only for that run.
- Agent middleware wraps the run, and conversation middleware wraps the run's realtime session.
- `run(..., stream=True)` yields every update a live conversation would. AG-UI, A2A and Foundry hosting skip unknown
  content types. DevUI prints a warning for each one, which is a host fix for later.

Bounded `run(...)` needs a terminal-error contract in addition to terminal response events. Some realtime providers emit
recoverable `error` events and keep the session open. If an error is correlated to the current input or response
request, the bounded run should fail or finalize with an error instead of waiting forever for a response-done event that
will not arrive. Error claiming provides the correlation. An error that the run's own response request claims fails the
run, and the acknowledgement timeout covers a request the provider never answers. A timeout/cancellation fallback is
also required for missing terminal events.

Streaming `run(..., stream=True)` must also clean up deterministically if the caller stops iteration early. The
implementation must provide an explicit close/cancel path or a stream wrapper that closes the ephemeral realtime session
and receive task when iteration is abandoned. Breaking out after one update must not leave a live socket behind.

Sandwich `run(...)` follows the same half-duplex projection. It processes one input turn through STT if needed, runs the
inner agent, optionally synthesizes audio, emits updates, and closes live resources.

### Phase 2: Sandwich STT-agent-TTS path

Add `RealtimePipeline` in phase 2 for sandwich setups. It composes a speech-to-text component, an inner
`SupportsAgentRun`, and a text-to-speech component behind the same `RealtimeConversation` API. This also requires
adding provider-neutral STT and TTS abstractions that are small enough to support both local samples and hosted media
integrations.

The direct realtime model path should land first. `RealtimePipeline`, STT, and TTS abstractions should land only after a
concrete provider or sample validates their shape. Otherwise this ADR records the intended phase 2 architecture and
defers the concrete API.

The sandwich implementation should:

- accept live audio input through `RealtimeConversation`;
- stream or batch audio into the speech-to-text abstraction;
- feed recognized text into the inner agent;
- stream the inner agent's text updates into the text-to-speech abstraction;
- emit synthesized audio as `Content.from_data(..., media_type="audio/...")`;
- report STT/TTS/pipeline lifecycle events with the same kinds where the meaning matches, and as passthrough events
  otherwise;
- use the inner agent's normal function invocation behavior instead of creating a second outer tool loop by default.

Tool handling in `RealtimePipeline` stays with the inner agent:

- tools are configured on the inner `SupportsAgentRun` agent, not on `RealtimePipeline`;
- recognized text from STT is passed to the inner agent through its normal `run(...)` / streaming path;
- function calls, approvals, progressive tool exposure, middleware, and tool errors are handled by the inner agent
  exactly as they are for normal text interactions;
- the pipeline observes the inner agent's streamed `AgentResponseUpdate` values and forwards text content to TTS;
- the pipeline emits synthesized audio and pipeline lifecycle events, but it does not inspect or execute tool calls
  itself;
- if a future scenario needs realtime-model-style tool handling outside the inner agent, that should be a separate
  decision, not an implicit responsibility of `RealtimePipeline`.

Concrete provider implementations can live in provider packages or samples, but the provider-neutral STT/TTS protocol
shape and `RealtimePipeline` class are part of the architecture.

### Deferred decisions

Still deferred from ADR 0035:

- Warm connection reuse for half-duplex `run(...)`.
- WebRTC/browser-oriented transport APIs, and SIP.
- Concrete `RealtimePipeline`, STT, and TTS implementations, and the final public STT and TTS protocol names.
- Built-in microphone and speaker handlers.

Each of these can be added later without breaking anything:

- **Tools.** Cancelling running tools on barge-in, a tool-call cancel API, a way for a tool to skip the follow-up
  response, sending images, audio or files from tool results to the model, session updates when an MCP server's tool
  list changes, and per-tool timeouts and automatic filler speech, which function middleware covers today.
- **Controls and events.** First-class out-of-band responses, raw session-field passthrough, mapping hosted MCP call
  events to `mcp_server_tool_call` content, and first-class viseme and avatar events, which pass through today.
- **History.** Per-turn retrieval by context providers during a live call, keeping the full generated text of cut-off
  turns, word-timed truncation, a history-changed update kind, and resuming provider-side conversations through
  `service_session_id`.
- **Handoffs.** Remembering the active agent across realtime sessions, as an opt-in.
- **Providers.** Voice Live's GA-shaped API version once it's documented, a `FoundryRealtimeClient`, Foundry voice
  agents' structured inputs and per-session `store` override, a typed capability API, connection retry and backoff, and
  providers with other protocols, such as Gemini Live.
- **Telemetry.** `user_speech` spans, turn spans, realtime-specific metrics, transcription usage, telemetry for provider
  clients used without an agent, `traceparent` for providers other than Foundry voice agents, hooks on control calls,
  and realtime agent-hooks, which raise on a realtime agent until a conversation-middleware member joins the bundle.

Out of scope:

- Handoffs that change model or provider mid-call, which need a new realtime session.
- Chat's turn-based handoff options (checkpointing, termination conditions, autonomous mode, return-to-previous and
  output selection) and nested handoff topologies.
- Delegation patterns and node-graph flows beyond handoffs, such as LiveKit's `AgentTask` or Pipecat Flows.
- Transcription-only "listener" sessions and translation sessions.
- Voice Live's agent-integration mode, for which Microsoft Learn recommends Foundry voice agents instead.
- Publishing Foundry voice-agent definitions, which is app code on the Foundry SDK. Agent Framework only connects to
  published agents.
- Telephony and app transport bridging, such as Azure Communication Services media streaming, which samples show and
  apps own.
- Correcting providers' cancel reasons, which Agent Framework can't do reliably.
- Fixing `SupportsAgentRun`'s missing keywords, which affects every protocol-only agent.

### Consequences

- Good, because callers get one Agent Framework live conversation concept for realtime use.
- Good, because direct realtime providers can establish the event/output handling before phase 2 sandwich pipelines
  reuse it.
- Good, because direct provider code remains outside core.
- Good, because function invocation reuses existing tested behavior instead of inventing a parallel tool loop.
- Good, because `AgentSession` remains durable state and `RealtimeConversation` owns live resources.
- Good, because apps react to barge-in, transcripts, content filtering, usage and errors without matching wire event
  names. The prototype's port of the accelerator's voice loop contains none.
- Good, because control calls are acknowledged, so a handoff's session update takes effect before its tool result and
  the follow-up response, and rejections reach the caller.
- Good, because handoffs happen in-session, with per-agent voices where the provider allows them, and topology code
  matches `HandoffBuilder`'s.
- Good, because history records what entered the provider's conversation and what the caller heard, in spoken order, and
  reaches context providers during the call.
- Good, because one conversation middleware policy covers live calls and `run(...)`, and each call is one trace.
- Good, because Azure OpenAI, Voice Live and Foundry voice agents are covered, and the shared base session is a public
  extension point for other services that speak the OpenAI Realtime protocol.
- Neutral, because the shared API must be carefully documented to avoid hiding capability differences. The
  capability table does this for providers.
- Neutral, because realtime telemetry follows OpenTelemetry conventions that are unreleased and pinned at a commit, so
  they may still change.
- Bad, because the design introduces new live-resource lifecycle concerns that existing chat agents do not have.
- Bad, because sandwich pipelines require more cancellation and backpressure plumbing than direct realtime model
  sessions.
- Bad, because every control call costs a round trip (53 to 376 ms for a session update), and control calls queue behind
  each other, including behind `interrupt(...)`'s truncate acknowledgements.
- Bad, because tool batches wait for `response_done`, which can trail a call's arguments by up to 2.5 s with Azure TTS
  voices.
- Bad, because a handoff lasts only for its realtime session, so a host that calls `run(...)` once per turn restarts
  each turn at the start agent.
- Bad, because some providers keep the old voice after a handoff, and of two handoffs in one response only the first
  happens.
- Bad, because history estimates the heard part of a cut-off turn proportionally, so it can be a few words off.
- Bad, because every `run(...)` replays the whole history into its fresh realtime session.
- Bad, because a context provider may see an exchange twice after a retry.
- Bad, because exporters receive the conversation span only at close. A crash loses it and orphans its children, and
  dashboards show `invoke_agent` spans that last minutes.
- Bad, because history and telemetry record what reached or came from the provider, so they don't see changes that
  conversation middleware's hooks make.
- Bad, because .NET instruments realtime at the client while Python instruments it at the agent, so the two languages'
  traces differ.
- Bad, because on Voice Live a pre-generated `say(...)` can't detect a lost race with turn detection.
- Bad, because Foundry voice-agent definitions published by Semantic Kernel need republishing.
- Bad, because the `[realtime]` extras constrain dependencies: `websockets` below 16 with `openai[realtime]`, and
  `azure-ai-projects` 2.7, which needs `openai>=3`, for Foundry voice agents.

## Validation

Validation should happen in stages:

1. ADR review validates the architecture before implementation.
2. A prototype has already run the accelerator's voice loop on this design against Voice Live (managed and BYOM), Azure
   OpenAI and a Foundry voice agent: 14 live scenarios, with 50 checks passed, 1 failed and 15 recorded as observations.
   The failure is the `FunctionTool.invoke` cancellation gap this ADR fixes. `run(...)`, approvals, app-run tools,
   context-provider instructions and tools, barge-in on a two-message reply, and OpenAI weren't exercised.
3. Unit tests with fake realtime sessions validate direct realtime model behavior, half-duplex finalization, streaming
   finalization, event preservation, bounded-queue backpressure, slow-consumer overflow, cleanup with a full queue,
   receive-loop errors, and single-reader behavior.
4. Unit tests validate `Content.from_realtime_event(...)` serialization and round-tripping, the kinds each provider maps
   from GA and Voice Live preview wire payloads, and realtime-specific response finalization, including exclusion of
   signals from `AgentResponse.messages`.
5. Unit tests validate control acknowledgements, error claiming before deduplication, lost races, the barge-in cut
   (including the fully-heard tolerance and later messages cut at 0 ms), and the close sequence.
6. Regression tests validate that the MCP helper extraction and the shared `HandoffBuilder` topology base don't change
   chat behavior.
7. Realtime tool tests validate batching, follow-ups, ordering against session updates, settlement on cancellation and
   close, and function call, function result, approval and user-input flows through the shared executor.
8. History tests validate spoken order with late transcripts, cut-off turns, text-less caller turns, per-exchange
   reporting, context-provider failures and retries, and replay.
9. Handoff, middleware and telemetry tests validate the active agent, `handoff_completed`, the builder's topology and
   failure result, conversation middleware hooks and outcomes, and the span tree.
10. Phase 2 sandwich tests, once a first implementation exists, validate STT-agent-TTS event flow, cancellation, and TTS
    output mapping.
11. Provider tests validate event mappers with fake SDK payloads. Live integration tests, marked `flaky` and
    `integration` with environment-based skips, cover each provider, including the OpenAI column of the capability
    table, which is inferred today. Dependency probes cover both `[realtime]` extras, including `azure-ai-voicelive` on
    every Python version Agent Framework supports.

## Appendix: Comparison with Semantic Kernel Python realtime

Semantic Kernel's Python realtime design, as captured in ADR 0065, is primarily a **client abstraction**. It centers on
a realtime client that can create/update/close a session, send events, and receive an async stream of `RealtimeEvent`
objects. Its event model treats both content and control signals as events, with provider details retained through
service event type/raw event fields. It also deliberately leaves microphone and speaker handling to application code or
samples.

This proposal keeps those lessons but maps them into Agent Framework's agent model:

- **Client versus agent.** Semantic Kernel exposes a realtime client API directly. Agent Framework should expose a
  `RealtimeAgent` and `RealtimeConversation`, with provider clients below that API, so realtime composes with agent
  middleware, sessions, protocol-based workflows, hosts, telemetry, and tools. Handoff orchestration uses
  `RealtimeHandoffBuilder` rather than the chat `HandoffBuilder`.
- **Event shape.** Semantic Kernel returns `RealtimeEvent` subclasses directly, and apps match service event names.
  Agent Framework should return `AgentResponseUpdate` values whose `contents` are normal `Content` items where possible,
  plus a small realtime-event content type for signals, with an Agent Framework kind on every event. Raw provider events
  remain available through `raw_representation`.
- **Tool invocation.** Semantic Kernel integrates realtime events with Kernel functions. Agent Framework should reuse
  the existing function-invocation executor so approval flow, function middleware, declaration-only tools, progressive
  tools, user-input requests, and tool error shaping stay consistent with chat agents.
- **Direct and sandwich setups.** Semantic Kernel ADR 0065 focuses on direct realtime API clients. Agent Framework
  should implement direct realtime first and reserve STT-agent-TTS sandwich pipelines for phase 2 behind the same
  `RealtimeConversation` API, while documenting that their latency, interruption, and continuation semantics differ.
- **Session lifetime.** Semantic Kernel's realtime client owns realtime session operations. Agent Framework should keep
  live sockets/tasks in `RealtimeConversation` and keep `AgentSession` for durable, serializable continuation state
  only.
- **Audio path.** Semantic Kernel exposes `audio_output_callback` so audio can be forwarded before normal event handling
  for smoother playback. Agent Framework should start with audio as normal conversation updates and small
  source/sink-style bridge helpers. A dedicated low-latency audio callback can be added later if the normal update path
  proves insufficient for local devices or hosted media streams.

### Semantic Kernel local websocket shape with function calling

```python
from semantic_kernel import Kernel
from semantic_kernel.connectors.ai import FunctionChoiceBehavior
from semantic_kernel.connectors.ai.open_ai import (
    AzureRealtimeExecutionSettings,
    AzureRealtimeWebsocket,
    ListenEvents,
    TurnDetection,
)
from semantic_kernel.contents import ChatHistory, RealtimeTextEvent

kernel = Kernel()
kernel.add_functions(plugin_name="helpers", functions=[goodbye, get_weather, get_date_time])

# Sample helpers own local microphone/speaker integration.
realtime_client = AzureRealtimeWebsocket()
audio_player = AudioPlayerWebsocket()
audio_recorder = AudioRecorderWebsocket(realtime_client=realtime_client)

settings = AzureRealtimeExecutionSettings(
    instructions="You are a helpful voice assistant.",
    voice="alloy",
    turn_detection=TurnDetection(type="server_vad", create_response=True, silence_duration_ms=800, threshold=0.8),
    function_choice_behavior=FunctionChoiceBehavior.Auto(),
)

chat_history = ChatHistory()
chat_history.add_user_message("Hi there, I'm based in Amsterdam.")

async with (
    audio_recorder,
    realtime_client(settings=settings, chat_history=chat_history, kernel=kernel, create_response=True),
    audio_player,
):
    async for event in realtime_client.receive(audio_output_callback=audio_player.client_callback):
        match event:
            case RealtimeTextEvent():
                print(event.text.text, end="")
            case _:
                if event.service_type == ListenEvents.RESPONSE_CREATED:
                    print("\nAssistant transcript: ", end="")
```

### Proposed Agent Framework local websocket shape with function calling

The sample prints final transcripts only. Printing every text update would print each transcript twice, once as
deltas and once as the final text.

```python
from agent_framework.openai import OpenAIRealtimeClient
from agent_framework.realtime import RealtimeAgent

agent = RealtimeAgent(
    client=OpenAIRealtimeClient(model="gpt-realtime"),
    instructions="You are a helpful voice assistant.",
    tools=[get_weather],
    default_options={
        "voice": "alloy",
        "turn_detection": {"type": "server_vad", "create_response": True, "silence_duration_ms": 800, "threshold": 0.8},
        "transcription": {"model": "gpt-4o-transcribe"},
    },
)

# Sample/optional helpers own local microphone/speaker integration.
audio_player = RealtimeAudioPlayer()
audio_recorder = RealtimeAudioRecorder()

async with (
    audio_player,
    agent.start_conversation() as conversation,
    audio_recorder.stream_to(conversation),
):
    await conversation.send("Hi there, I'm based in Amsterdam.")
    async for update in conversation:
        for content in update.contents:
            if content.has_top_level_media_type("audio"):
                await audio_player.write(content)
            elif content.type == "text" and content.status == "completed":
                print(f"{update.role}: {content.text}")
```

### Proposed Agent Framework phase 2 sandwich shape

```python
from agent_framework import Agent
from agent_framework.openai import OpenAIChatClient
from agent_framework.realtime import RealtimeAgent, RealtimePipeline

text_agent = Agent(
    client=OpenAIChatClient(model="gpt-4.1"),
    instructions="You are a helpful assistant.",
    tools=[get_weather],
)

agent = RealtimeAgent(
    client=RealtimePipeline(
        speech_to_text=speech_to_text_client,
        agent=text_agent,
        text_to_speech=text_to_speech_client,
    ),
)
```

### Semantic Kernel Call Automation bridge shape

```python
async def from_realtime_to_acs(audio: ndarray) -> None:
    await websocket.send(json.dumps({
        "kind": "AudioData",
        "audioData": {"data": base64.b64encode(audio.tobytes()).decode("utf-8")},
    }))


async def from_acs_to_realtime(client: RealtimeClientBase) -> None:
    while True:
        data = json.loads(await websocket.receive())
        if data["kind"] == "AudioData":
            await client.send(RealtimeAudioEvent(audio=AudioContent(data=data["audioData"]["data"])))


async with client(settings=settings, create_response=True, kernel=kernel):
    receive_task = asyncio.create_task(handle_realtime_messages(client, audio_output_callback=from_realtime_to_acs))
    await from_acs_to_realtime(client)
    receive_task.cancel()
```

### Proposed Agent Framework Call Automation bridge shape

On barge-in, the bridge stops ACS playback and interrupts the conversation with how much audio the caller heard.
`PlaybackClock` stands for the app's own record of the response audio ACS has played. Control calls wait for the
provider, so the bridge starts them as tasks and keeps reading updates.

```python
import base64

from agent_framework import Content
from agent_framework.realtime import RealtimeConversation


def get_base64_audio(content: Content) -> str:
    if content.type != "data" or content.uri is None:
        raise ValueError("Expected audio data content.")

    prefix, separator, encoded = content.uri.partition(",")
    if not separator or not prefix.startswith("data:audio/") or not prefix.endswith(";base64"):
        raise ValueError("Expected a base64 audio data URI.")

    base64.b64decode(encoded, validate=True)
    return encoded


async def from_realtime_to_acs(websocket: WebSocket, content: Content) -> None:
    await websocket.send(json.dumps({
        "kind": "AudioData",
        "audioData": {"data": get_base64_audio(content)},
    }))


async def from_acs_to_realtime(websocket: WebSocket, conversation: RealtimeConversation) -> None:
    while True:
        data = json.loads(await websocket.receive())
        if data["kind"] == "AudioData":
            await conversation.send(
                Content.from_data(base64.b64decode(data["audioData"]["data"]), media_type="audio/pcm;rate=24000")
            )


async def handle_agent_updates(
    websocket: WebSocket, conversation: RealtimeConversation, playback: PlaybackClock
) -> None:
    async with asyncio.TaskGroup() as controls:
        async for update in conversation:
            for content in update.contents:
                if content.has_top_level_media_type("audio"):
                    await from_realtime_to_acs(websocket, content)
                    playback.add(content)
                elif content.type == "realtime_event" and content.kind == "speech_started":
                    await websocket.send(json.dumps({"Kind": "StopAudio", "AudioData": None, "StopAudio": {}}))
                    controls.create_task(conversation.interrupt(played_ms=playback.played_ms()))


@app.websocket("/ws")
async def ws() -> None:
    websocket = current_websocket()
    async with agent.start_conversation() as conversation, asyncio.TaskGroup() as bridge:
        # If either direction fails or ACS disconnects, the task group cancels the other and raises.
        bridge.create_task(handle_agent_updates(websocket, conversation, PlaybackClock()))
        bridge.create_task(from_acs_to_realtime(websocket, conversation))
```

### Rejected option shape: extending `Agent`

This shows the alternative rejected in the decision outcome.

```python
from agent_framework import Agent
from agent_framework.openai import OpenAIRealtimeClient

agent = Agent(
    client=OpenAIRealtimeClient(model="gpt-realtime"),
    instructions="You are a helpful voice assistant.",
    tools=[get_weather],
)

async with agent.start_conversation() as conversation:
    await conversation.send(Content.from_data(audio_chunk, media_type="audio/pcm;rate=24000"))
    async for update in conversation:
        ...
```

## Appendix: Proposed Python realtime client protocol

The direct realtime model path should use a small provider-neutral protocol. The agent owns mapping between this
protocol and `RealtimeConversation` / `AgentResponseUpdate`; provider packages own mapping between the protocol and
provider wire events.

Illustrative protocol shape:

```python
from collections.abc import AsyncIterable, Mapping, Sequence
from typing import Any, Literal, NewType, Protocol, runtime_checkable
from typing_extensions import TypedDict

from agent_framework import Content, ToolMode, ToolTypes

RealtimeKindLiteral = Literal[
    "session_created",
    "session_updated",
    "speech_started",
    "speech_stopped",
    "response_created",
    "response_done",
    "item_added",
    "item_truncated",
    "input_transcription",
    "input_transcription_failed",
    "output_text",
    "output_audio",
    "function_call",
    "function_result",
    "function_approval_request",
    "handoff_completed",
    "usage",
    "error",
    "passthrough",
]
RealtimeKind = NewType("RealtimeKind", str)


class RealtimeTurnDetection(TypedDict, total=False):
    """Turn detection fields every provider shares; provider types add their own."""

    type: str
    create_response: bool
    interrupt_response: bool
    threshold: float
    prefix_padding_ms: int
    silence_duration_ms: int


class RealtimeTranscription(TypedDict, total=False):
    model: str
    language: str


class RealtimeNoiseReduction(TypedDict, total=False):
    type: str


class RealtimeOptions(TypedDict, total=False):
    """Provider-neutral realtime options that each provider client translates."""

    instructions: str
    voice: str
    tool_choice: ToolMode | Literal["auto", "required", "none"]
    temperature: float
    max_tokens: int
    metadata: Mapping[str, str]
    input_audio_format: str
    output_audio_format: str
    output_modalities: Sequence[str]
    turn_detection: RealtimeTurnDetection | None
    transcription: RealtimeTranscription | None
    noise_reduction: RealtimeNoiseReduction | None


class RealtimeEvent:
    """Provider-neutral event to send to, or receive from, a realtime session."""

    kind: RealtimeKind
    content: Content | None
    service_event_type: str | None
    additional_properties: Mapping[str, Any]
    raw_representation: Any | None


@runtime_checkable
class RealtimeClientSessionProtocol(Protocol):
    """Live provider session for one realtime connection."""

    options: RealtimeOptions

    async def send(self, event: RealtimeEvent) -> None:
        """Send one raw realtime command to the provider."""
        ...

    def receive(self) -> AsyncIterable[RealtimeEvent]:
        """Yield provider events tagged with kinds. Only one active receiver is allowed per session."""
        ...

    async def update_session(self, options: RealtimeOptions, tools: Sequence[ToolTypes] | None) -> None:
        """Update the live session and wait for the provider's acknowledgement."""
        ...

    async def create_response(self, options: RealtimeOptions | None = None) -> str:
        """Request a response and return its id once the provider has created it."""
        ...

    async def say(self, text: str) -> str:
        """Request verbatim speech and return the response id."""
        ...

    async def cancel_response(self) -> None:
        """Cancel the active response, if there is one."""
        ...

    async def truncate(self, item_id: str, content_index: int, audio_end_ms: int) -> None:
        """Truncate an assistant audio item and wait for the provider's acknowledgement."""
        ...

    async def close(self) -> None:
        """Close the live provider session."""
        ...

    async def __aenter__(self) -> "RealtimeClientSessionProtocol":
        """Enter the live provider session context."""
        ...

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: Any) -> None:
        """Exit and close the live provider session context."""
        ...


@runtime_checkable
class RealtimeClientProtocol(Protocol):
    """Provider client capable of creating realtime sessions."""

    async def start_session(
        self,
        *,
        options: RealtimeOptions | None = None,
        tools: Sequence[ToolTypes] | None = None,
    ) -> RealtimeClientSessionProtocol:
        """Create a live realtime provider session."""
        ...
```

Notes:

- `RealtimeAgent` exposes a higher-level `start_conversation(...)` method that accepts agent run options and returns
  `RealtimeConversation`; provider clients expose `start_session(...)`.
- `RealtimeConversation`, not `RealtimeClientSessionProtocol`, owns convenience inputs such as strings, `Message`,
  `Content`, local audio helpers, and hosted media bridge helpers. It translates them into provider-level
  `RealtimeEvent` values.
- `RealtimeClientSessionProtocol.receive(...)` intentionally returns a plain `AsyncIterable[RealtimeEvent]`, not
  `ResponseStream`, because it is the provider-level live event feed and has no provider-neutral final response.
- `ResponseStream` belongs at the agent layer:
  - `RealtimeAgent.run(..., stream=True)` returns `ResponseStream[AgentResponseUpdate, AgentResponse]`.
  - A continuously open conversation may expose an async stream of updates without requiring callers to finalize it into
    one response.
  - A scoped `RealtimeConversation` response helper can return `ResponseStream[AgentResponseUpdate, AgentResponse]`
    later if the API proves necessary.
- `receive(...)` is single-reader. `RealtimeConversation` should own that read loop and project provider events into
  agent-level updates.
- `RealtimeOptions` is a `TypedDict`. Provider-specific options extend it in their provider packages
  (`OpenAIRealtimeOptions`, `VoiceLiveOptions`, `FoundryVoiceAgentOptions`) instead of through untyped extension fields,
  and clients drop the keys they can't honor, with a warning.
- `OpenAIRealtimeSession` implements the typed control methods for services that speak the OpenAI Realtime protocol. A
  third-party service with that protocol subclasses it; a service with another protocol implements the protocol
  directly.
- Provider SDK objects stay in `raw_representation`.

## Appendix: Realtime data model roles

The realtime design uses existing Agent Framework message/content models where possible and adds a realtime event model
only for live-session events. These types have different jobs:

| Type | Role | Lifetime | Examples | Serialized? |
| --- | --- | --- | --- | --- |
| `Content` | One semantic payload item. | Reusable across chat, agent, realtime, and tool flows. | text delta, audio bytes, function call, function result, error, usage, realtime-event content. | Yes, except `raw_representation`. |
| `Message` | A role-authored collection of content items. | Conversation/history unit for half-duplex runs, seeded context and voice history. | user text message, caller transcript, assistant response message, tool result message. | Yes, except `raw_representation`. |
| `RealtimeEvent` | One live realtime protocol/pipeline signal, tagged with a kind. | Realtime session send/receive unit. | `response.created`, `response.output_audio.delta`, `input_audio_buffer.speech_started`, pipeline transcript-ready event. | Mostly yes; raw provider object excluded. |
| `AgentResponseUpdate` | One caller-facing streamed agent update. | Output stream item from `run(..., stream=True)` or `RealtimeConversation`. | audio update, transcript update, function-call update, speech-started update. | Yes, except `raw_representation`. |

### `Content`

`Content` remains the framework's semantic payload type. Realtime should not introduce new content classes for data the
framework already understands:

- text and transcripts use `Content.from_text(...)`, with `status="completed"` on final transcripts;
- audio uses `Content.from_data(..., media_type="audio/...")`;
- tool requests use `Content.from_function_call(...)`;
- tool results use `Content.from_function_result(...)`;
- errors use `Content.from_error(...)`;
- usage uses `Content.from_usage(...)`.

Only realtime signals that do not naturally fit an existing content type need a new content shape:
`Content.from_realtime_event(kind=...)`. That content carries the kind, the verbatim service event type and serializable
neutral attributes, and keeps the raw provider event in `raw_representation`.

`Content.from_realtime_event(...)` is not a replacement for `RealtimeEvent`. It is the semantic `Content` payload used
when a realtime signal needs to appear in an `AgentResponseUpdate.contents` list. `RealtimeEvent` is the realtime
session's event envelope; realtime-event content is the agent-response payload created from that envelope when there is
no text/audio/function/error content to carry.

Do not make `Content.from_realtime_event(...)` a wrapper that optionally carries text, audio, function-call, or
function-result content. That creates two envelopes for the same event and gives users two ways to find the same
payload. Payload-bearing events should use normal `Content` directly in `RealtimeEvent.content`; pure signals should use
`Content.from_realtime_event(...)` only when projected into an `AgentResponseUpdate`.

Example:

```python
event = RealtimeEvent(
    kind="speech_started",
    content=None,
    service_event_type="input_audio_buffer.speech_started",
    additional_properties={"audio_start_ms": 1200, "item_id": "item_123"},
    raw_representation=openai_event,
)

update = AgentResponseUpdate(
    role="user",
    contents=[
        Content.from_realtime_event(
            kind=event.kind,
            service_event_type=event.service_event_type,
            additional_properties=event.additional_properties,
            raw_representation=event.raw_representation,
        )
    ],
    message_id="item_123",
    raw_representation=event,
)
```

In short:

- `RealtimeEvent` says "the live realtime session emitted this event, and this is its kind."
- `Content.from_realtime_event(...)` says "this event has no normal semantic payload, so carry the signal as a content
  item in the agent update."

### `Message`

`Message` is still the durable conversation/history unit. It groups one or more `Content` items under a role such as
`user`, `assistant`, or `tool`.

Realtime uses `Message` in three places:

- **Half-duplex input/output**: `RealtimeAgent.run(...)` accepts normal `Message`/string/`Content` inputs and returns an
  `AgentResponse` containing normal `Message` objects.
- **Session seeding / history replay**: a direct realtime model session can be created from existing `Message` history
  so an ephemeral socket can reconstruct context ([Replay](#replay)).
- **Voice history**: authored turns become messages, one per provider conversation item
  ([Session, identity and voice history](#session-identity-and-voice-history)).

`Message` should not represent every provider event. A VAD event, response-created signal, or audio buffer update is not
a durable authored chat message; it should remain a realtime event or an `AgentResponseUpdate` with realtime-event
content.

### `RealtimeEvent`

`RealtimeEvent` is the live wire-level abstraction for direct realtime providers and pipeline internals. It is not the
main user-facing response type. It exists so provider clients can report every service signal without forcing all
signals into `Message`.

On the receive side, `RealtimeEvent` is closest to `ChatResponseUpdate`. It is a provider-level streamed item that the
agent projects into `AgentResponseUpdate`. It is not called `RealtimeUpdate` because the same type also represents
outbound live-session commands. "Update" would imply response-only/output-only semantics, which is wrong for operations
like response create, response cancel, audio buffer clear, session update, and interruption.

Returning only `Content` from the provider session is not enough because realtime services emit events where the event
itself carries important meaning beyond the payload:

- multiple events can carry the same semantic content type but require different handling, for example
  `response.output_audio.delta`, `response.output_audio.done`, and `response.output_audio_transcript.done`;
- signals often have no semantic payload but still matter, for example speech started/stopped, response created,
  response done, rate-limit updates, cancellation, and errors;
- event-level identifiers such as response id, item id, output index, content index, timestamps, and provider event type
  are needed for ordering, correlation, interruption, and final aggregation;
- some send operations are commands rather than content, for example response creation, cancellation, buffer clear, or
  session update;
- tool-call arguments may arrive across multiple service events and need event-level correlation before they become a
  complete `Content.from_function_call(...)`;
- hosted media and sandwich pipelines also need lifecycle events that are not authored messages and are not naturally
  text/audio/function content.

`Content` should answer "what semantic payload is present?" `RealtimeEvent` should answer "what happened in the live
session, and what provider/pipeline metadata came with it?"

What `RealtimeEvent` adds over `Message` / `AgentResponseUpdate` is narrow, and should stay narrow:

- **Outbound commands**: `Message` and `AgentResponseUpdate` are not a good fit for raw commands. `RealtimeEvent` can
  represent live-session operations without pretending they are authored conversation messages or agent output updates.
- **Provider/session boundary**: provider clients should not have to manufacture `AgentResponseUpdate` values. That is
  the agent projection layer's job, just like chat clients return `ChatResponseUpdate` and agents map those to
  `AgentResponseUpdate`.
- **Pre-projection correlation**: raw provider events often need event type, response id, item id, output/content
  indexes, timestamps, and raw SDK objects before the agent decides how to project them. `RealtimeEvent` keeps that
  protocol metadata together until projection.

If `RealtimeEvent` stops being the provider/session boundary and becomes only another way to carry `Content`, delete it.
It should not exist as a third user-facing response model.

#### Sub-option: one content item or a list of content items

There are two reasonable shapes for the payload field.

Option A: `RealtimeEvent.content: Content | None`

- Good, because each realtime event has one clear payload or no payload.
- Good, because it avoids making `RealtimeEvent`, `Message`, and `AgentResponseUpdate` all own `list[Content]`.
- Good, because it forces provider mappers to split multi-payload provider events into simpler framework events.
- Good, because it matches the current Semantic Kernel Python shape: `RealtimeAudioEvent.audio`,
  `RealtimeTextEvent.text`, `RealtimeFunctionCallEvent.function_call`, and related event subclasses each carry one
  typed payload, while base `RealtimeEvent` carries service/control event metadata.
- Neutral, because a raw provider event that contains multiple payloads may become multiple `RealtimeEvent` instances
  that share the same `raw_representation`.
- Bad, because it can lose the fact that one raw provider event contained multiple semantic payloads unless that
  relationship is recorded in metadata.
- Bad, because wrapping to `AgentResponseUpdate` always has to put the single content into a list.

Option B: `RealtimeEvent.contents: Sequence[Content]`

- Good, because it mirrors `Message.contents`, `ChatResponseUpdate.contents`, and `AgentResponseUpdate.contents`.
- Good, because one raw provider event with multiple semantic payloads can remain one framework event.
- Good, because wrapping into `AgentResponseUpdate` is mechanically simple: copy `event.contents`.
- Neutral, because many realtime events will still have zero or one content item.
- Bad, because it creates another envelope with a content list, which overlaps with `Message` and `AgentResponseUpdate`.
- Bad, because users may have to learn whether a payload is in the event list, the update list, or a final message list.
- Bad, because it encourages stuffing unrelated payloads into one event instead of keeping event semantics precise.

Decision: use **Option A: `RealtimeEvent.content: Content | None`**. This matches the Semantic Kernel Python realtime
model and keeps realtime event handling simple, because one live event has one payload or no payload. Provider mappers
should split raw provider events with multiple semantic payloads into multiple `RealtimeEvent` instances that share the
same `raw_representation`.

It should carry:

- `kind`: the Agent Framework kind ([Event taxonomy](#event-taxonomy));
- `content`: one optional `Content` item when the event has a semantic payload;
- `service_event_type`: the provider or pipeline event name, verbatim, such as `response.output_audio.delta` or
  `input_audio_buffer.speech_started`;
- `additional_properties`: the kind's neutral attributes, such as response id, item id, content index or timestamps,
  plus any documented provider extras in the provider's namespace;
- `raw_representation`: the provider SDK event object or pipeline object, excluded from serialization.

For sending, `RealtimeEvent` is the escape hatch for operations that have no typed control, such as clearing an audio
buffer, out-of-band responses, or session fields Agent Framework hasn't typed.

For receiving, provider clients map raw service events into `RealtimeEvent`; `RealtimeAgent` then maps those into
`AgentResponseUpdate` values for callers.

### Wrapping `RealtimeEvent` into agent responses

There is one wrapping path. The conversation wraps each received `RealtimeEvent` into one `AgentResponseUpdate`.
`Content.from_realtime_event(...)` is not a second wrapping mechanism; it is only the fallback content item used when
the event has no existing semantic content. The update preserves the event as its raw representation and exposes the
event's payload as normal update content:

```python
def map_realtime_event_to_agent_update(event: RealtimeEvent, active_agent: RawRealtimeAgent) -> AgentResponseUpdate:
    content = event.content or Content.from_realtime_event(
        kind=event.kind,
        service_event_type=event.service_event_type,
        additional_properties=event.additional_properties,
        raw_representation=event.raw_representation,
    )
    role = _role_from_realtime_event(event)
    authored_by_agent = role in ("assistant", "tool")

    return AgentResponseUpdate(
        contents=[content],
        role=role,
        author_name=active_agent.name if authored_by_agent else None,
        agent_id=active_agent.id if authored_by_agent else None,
        response_id=event.additional_properties.get("response_id"),
        message_id=event.additional_properties.get("item_id"),
        finish_reason=_finish_reason_from_response_done(event) if event.kind == "response_done" else None,
        raw_representation=event,
    )
```

Here `active_agent` is the agent that was active when the event's response was created, and
`_finish_reason_from_response_done` applies the response-outcome rules in [Event taxonomy](#event-taxonomy).

The wrapping rules are:

- Always create an `AgentResponseUpdate` with `raw_representation` set to the `RealtimeEvent`.
- If `event.content` is present, put that one `Content` item in `AgentResponseUpdate.contents`.
- If `event.content` is absent, create one `Content.from_realtime_event(...)` item and put it in
  `AgentResponseUpdate.contents`.
- If one raw provider event carries multiple semantic payloads, split it into multiple `RealtimeEvent` instances that
  share the same `raw_representation` rather than adding a list of contents to `RealtimeEvent`.
- Do not wrap normal text/audio/function/error payloads inside `Content.from_realtime_event(...)`.
- The `RealtimeEvent.raw_representation` points to the provider SDK object or pipeline object.
- Copy identifiers into the standard `response_id` / `message_id` fields when they match existing meanings, and keep
  provider extras in the provider's metadata namespace.
- Only assistant and tool updates carry an agent's `author_name` and `agent_id`.
- Do not manufacture a durable `Message` for every `RealtimeEvent`. Messages are only created when finalizing a
  half-duplex `AgentResponse` or when reporting authored turns to history.

For half-duplex `run(...)`, `RealtimeAgent` accumulates the streamed updates until the terminal response event and then
builds a normal `AgentResponse`. That final response should contain authored assistant/tool messages reconstructed from
content-bearing updates, while signals remain available through the response `raw_representation` list and through any
realtime-event content that was emitted during streaming. Caller transcripts stay out of `AgentResponse.messages`.

### Mapping rules

The mapping should be explicit and boring:

| Input/output situation | Preferred model |
| --- | --- |
| User sends text to a realtime conversation | `Message` or `Content.from_text(...)`, converted to provider input-text events. |
| User sends audio bytes | `conversation.send(Content.from_data(...))`, converted to provider input-audio events. |
| Caller needs a control operation | A typed control call, such as `update_session(...)` or `interrupt(...)`. |
| Caller needs a provider operation with no typed control | `RealtimeEvent` with `kind="passthrough"`. |
| Provider emits audio | `RealtimeEvent(kind="output_audio", content=Content.from_data(...))`, which becomes `AgentResponseUpdate(contents=[...])`. |
| Provider emits a transcript | `RealtimeEvent(kind="output_text" or "input_transcription", content=Content.from_text(...))`, which becomes `AgentResponseUpdate(contents=[...])`. |
| Provider emits a function call | `RealtimeEvent(kind="function_call", content=Content.from_function_call(...))`; the realtime tool loop invokes the existing function machinery. |
| Provider emits a signal | `RealtimeEvent(kind=..., content=None)`, which becomes `AgentResponseUpdate(contents=[Content.from_realtime_event(...)])`. |
| Provider emits an event Agent Framework gives no meaning to | `RealtimeEvent(kind="passthrough", content=None)`, with the wire payload in the provider's namespace. |
| Half-duplex final response | Accumulate the agent's content-bearing updates into a normal `AgentResponse(messages=[Message(...)])`. |

This separation keeps `Message` useful for durable conversation history, keeps `Content` as the common semantic payload
model, and keeps `RealtimeEvent` focused on live protocol/pipeline events that may not be authored messages.

## Appendix: Comparison with .NET `IRealtimeClient`

| Concern | .NET `IRealtimeClient` / `IRealtimeClientSession` | Proposed Python protocol |
| --- | --- | --- |
| Client role | `IRealtimeClient` creates `IRealtimeClientSession` instances. | `RealtimeClientProtocol.start_session(...)` creates `RealtimeClientSessionProtocol` instances. |
| Session lifetime | `IRealtimeClientSession` is `IAsyncDisposable`. | `RealtimeClientSessionProtocol` is an async context manager with `close()`. |
| Send operation | `SendAsync(RealtimeClientMessage, ...)`. | `send(RealtimeEvent)` for raw commands, plus typed control methods. `Message` / `Content` conveniences live on `RealtimeConversation`. |
| Receive operation | `GetStreamingResponseAsync(...)` yields `RealtimeServerMessage`; single-reader constraint is called out. | `receive(...)` yields `RealtimeEvent` tagged with kinds; single-reader constraint is explicit and normally owned by `RealtimeConversation`. |
| Event model | Decision Q4: payload events map to existing content types, and pure signals to realtime event content. | The same shape, with an open set of kinds that only core extends, a `passthrough` kind for everything else, and verbatim service event names. |
| Options | `RealtimeSessionOptions` configures voice, VAD, formats, tools, and provider settings. | `RealtimeOptions` carries the same conceptual fields as a `TypedDict`, extended per provider package. |
| Audio | MEAI events can carry audio; .NET ADR projects audio to `DataContent` at agent level. | Audio flows as normal `Content.from_data(...)` payloads on `RealtimeEvent`; a callback can be added later if needed. |
| Break-glass access | `GetService(...)` and raw representations expose MEAI/provider SDK objects. | `raw_representation` exposes provider SDK objects on events/updates. |
| Agent projection | .NET `RealtimeAgent : AIAgent` wraps `IRealtimeClient`; `AgentConversation` is agent-level. | Python has no `AIAgent`; `RealtimeAgent` wraps `RealtimeClientProtocol`, exposes `RealtimeConversation`, and must satisfy `SupportsAgentRun` for half-duplex compatibility. |
| Function invocation | .NET expects reuse of `RealtimeClientBuilder` middleware, including function invocation. | Python reuses the existing function-invocation batch executor for direct realtime tool calls. |
| Middleware and telemetry | Client-level, through Microsoft.Extensions.AI's realtime client pipeline. | Conversation middleware, and telemetry at the agent layer with one conversation span per call. |
| Handoffs | Claimed through reuse of `AIAgent`, which gives a reconnect per turn rather than an in-session handoff. | In-session `RealtimeConversation.handoff(...)` and `RealtimeHandoffBuilder`. |
| Sandwich pipeline | Not the main `IRealtimeClient` focus in the .NET ADR. | Phase 2 through `RealtimePipeline(STT, SupportsAgentRun, TTS)` using the same `RealtimeConversation` API. |

The proposed Python protocol intentionally stays close to .NET in the client/session split, live async lifetime,
single-reader receive stream, options concept, raw-representation escape hatch, event model, and agent-level
conversation projection. It differs where Python needs a more idiomatic or practical shape:

- Python uses structural `Protocol` types instead of C# interfaces.
- Python uses async context managers instead of `IAsyncDisposable`.
- Python includes `Message`/`Content` send conveniences to reduce boilerplate in samples and hosting bridges.
- Python instruments realtime at the agent layer, where the active agent, tools and controls are known.
- Python hands off in-session, which the .NET ADR's reuse of `AIAgent` can't.
- Python reserves `RealtimePipeline` as a phase 2 sandwich composition target, while .NET's ADR focuses on direct
  realtime clients.

## More Information

### What changed from ADR 0035

| ADR 0035 | This ADR |
| --- | --- |
| A conversation "owned by a RealtimeAgent" | Started by a realtime agent, whose active agent can change through handoffs |
| An argless `interrupt()`; untyped `RealtimeEvent` commands for everything else | Acknowledged controls with error claiming: `update_session(...)`, `create_response(...)`, `say(...)`, `interrupt(played_ms=...)`, `handoff(...)` |
| A playback-progress path left open | `interrupt(played_ms=...)` cuts every unheard message |
| `run(...)` keywords unspecified | Explicit `options=`, `tools=` and `middleware=` on `run(...)` and `start_conversation(...)` |
| Experimental scope open | One `REALTIME` flag for the family, and `FOUNDRY_VOICE_AGENTS` for Foundry voice agents |
| `RealtimeSessionOptions`, with a top-level `create_response` | `RealtimeOptions`, extended per provider, with `create_response` inside turn detection |
| `event_type` categories (`audio`, `text`, `service`, `lifecycle`) | Agent Framework kinds, with `service_event_type` kept verbatim |
| A sample that printed every `update.text` | Samples print transcript deltas or finals |
| "Refactor into a shared executor" | Reuse `_execute_function_calls`, with realtime loop rules on top |
| An authored-turn ledger finalized at close | Per-exchange reporting during the call, in spoken order, with cut-off turns |
| Realtime handoffs deferred until `HandoffBuilder` is generalized | In-session `handoff(...)` and `RealtimeHandoffBuilder`; `HandoffBuilder` stays chat-only |
| Dedicated realtime middleware deferred | Conversation middleware, and one conversation span per call |
| `RawOpenAIRealtimeClient` / `OpenAIRealtimeClient` | Provider clients without telemetry layers; telemetry at the agent layer |
| OpenAI first, other providers later | OpenAI and Azure OpenAI, Voice Live, and Foundry voice agents, with a capability table |

### Review threads on ADR 0035

| Thread | Answered in |
| --- | --- |
| [`run(...)` keywords that hosts pass](https://github.com/microsoft/agent-framework/pull/6831#discussion_r3953440204) | [Run and conversation keywords](#run-and-conversation-keywords) |
| [Experimental scope for the whole realtime family](https://github.com/microsoft/agent-framework/pull/6831#discussion_r3953440210) | [Experimental features and release](#experimental-features-and-release) |
| [How full-duplex traffic enters middleware and telemetry](https://github.com/microsoft/agent-framework/pull/6831#discussion_r3953440213) | [Interception points](#interception-points) |
| [Middleware and telemetry for audio-only traffic](https://github.com/microsoft/agent-framework/pull/6831#discussion_r3667773087) | [Interception points](#interception-points), and [Content capture](#content-capture) |
| [Handoff compatibility through `SupportsAgentRun`](https://github.com/microsoft/agent-framework/pull/6831#discussion_r3852979100) | [Handoffs](#handoffs) |

### Open questions for reviewers

- **Naming alignment with .NET.** .NET's draft names its live conversation type `AgentConversation`, where Python uses
  `RealtimeConversation`, and has no counterparts yet for `handoff(...)`, `handoff_completed` or
  `RealtimeHandoffBuilder`. If the maintainers want cross-language consistency, the names should be settled before
  implementation.
- **Release.** Shipping as experimental in 1.x rather than waiting for the next major version is the maintainers' call.

### Evidence

Research findings, on throwaway branches of a fork:

- [Accelerator voice-path capability checklist vs ADR 0035](https://github.com/adamdougal/agent-framework/blob/research/accelerator-capability-checklist/docs/research/accelerator-capability-checklist.md)
- [Azure realtime protocols: Azure OpenAI, Voice Live and Foundry voice agents](https://github.com/adamdougal/agent-framework/blob/research/azure-realtime-protocols/docs/research/azure-realtime-protocols.md)
- [Azure realtime transports](https://github.com/adamdougal/agent-framework/blob/research/azure-realtime-transports/docs/research/azure-realtime-transports.md)
- [How realtime frameworks handle in-session handoffs](https://github.com/adamdougal/agent-framework/blob/research/realtime-handoff-survey/docs/research/realtime-handoff-survey.md)
- [Middleware and telemetry for full-duplex realtime traffic](https://github.com/adamdougal/agent-framework/blob/research/realtime-middleware-telemetry/docs/research/realtime-middleware-telemetry.md)
- [Realtime telemetry mapping per provider](https://github.com/adamdougal/agent-framework/blob/research/realtime-telemetry-mapping/docs/research/realtime-telemetry-mapping.md)
- [Realtime wire behaviour across providers: live probes](https://github.com/adamdougal/agent-framework/blob/research/realtime-wire-probes/docs/research/realtime-wire-probes.md),
  with the request, response, API version and date for each answer

The [prototype](https://github.com/adamdougal/agent-framework/tree/prototype/realtime-voice-loop/python/packages/realtime-prototype)
ran the accelerator's voice loop on this design, never to be merged. Its
[findings](https://github.com/adamdougal/agent-framework/blob/prototype/realtime-voice-loop/python/packages/realtime-prototype/FINDINGS.md)
record every scenario, with the evidence for each check.

### Existing telemetry gaps outside this design

These affect chat today. Realtime shares the helpers that would fix them, so they should be raised separately:

- The semantic-conventions-genai repository's Agent Framework conformance scenario records four gaps: spans emit
  `finish_reasons` as a single string rather than an array, say `tool_calls` where the conventions say `tool_call`,
  still emit the removed `gen_ai.system`, and emit `cache_creation` where the conventions now say `cache_write`.
- With sensitive data on, `data` content is written as base64 `blob` parts.
- The Chat Completions client's audio token counts are dropped from telemetry.
- `AgentTelemetryLayer` creates metric histograms it never records.
- Sessions with local history have no `gen_ai.conversation.id`.

### Related designs

- [.NET realtime ADR draft](https://github.com/rogerbarreto/agent-framework-public/blob/features/realtime-agent/docs/decisions/0030-realtime-ai-agent.md)
- [Semantic Kernel realtime client ADR](https://github.com/microsoft/semantic-kernel/blob/main/docs/decisions/0065-realtime-api-clients.md)
- Python session identity ADR: [`0033-python-agent-session-identity.md`](0033-python-agent-session-identity.md)
- Provider package separation ADR: [`0021-provider-leading-clients.md`](0021-provider-leading-clients.md)
- Content identity and protocol metadata ADR:
  [`0048-python-content-identity-and-protocol-metadata.md`](0048-python-content-identity-and-protocol-metadata.md)
- [semantic-conventions-genai's realtime conventions](https://github.com/open-telemetry/semantic-conventions-genai/pull/394)
