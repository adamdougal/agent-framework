# How realtime frameworks handle in-session handoffs

Findings for [How realtime frameworks handle in-session handoffs](https://github.com/adamdougal/agent-framework/issues/3),
a research ticket on the map [Python realtime agents in Agent Framework](https://github.com/adamdougal/agent-framework/issues/1).
Sources were read on 2026-11-03 at the versions below. Unversioned documentation pages carry that retrieval date.

| Source | Version read |
| --- | --- |
| OpenAI Agents SDK for Python | `v0.24.0` at [`17d994bd`](https://github.com/openai/openai-agents-python/tree/17d994bd3045dae71acda676a4ef4762ed8ecd94) |
| OpenAI Python SDK (generated API types) | `v3.42.1` at [`190dd5ae`](https://github.com/openai/openai-python/tree/190dd5aee1053dd2093d2a5c3937f604860310a3) |
| LiveKit Agents (Python) | `livekit-agents@1.8.10` at [`ff962a01`](https://github.com/livekit/agents/tree/ff962a019b6500e08f8f1fcd7f55673361bd9e91) |
| Pipecat, including Flows (now in core) | `v1.15.0` at [`fda1d5e0`](https://github.com/pipecat-ai/pipecat/tree/fda1d5e0cde9625c8dcef7b653ed731c2ce63672) |
| pipecat-flows (deprecated standalone package) | `v1.4.0` at [`96223f4e`](https://github.com/pipecat-ai/pipecat-flows/tree/96223f4e9ea6f84651e379978be6560cf3ae50c8) |
| Agent Framework (Python and .NET) | `main` at [`f4bdd1dc`](https://github.com/microsoft/agent-framework/tree/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f) |
| ADR 0035, Python realtime agents (final revision) | [`591b7ea7`](https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md) |
| ADR 0030 draft, .NET realtime agent | [`864fdc3b`](https://github.com/rogerbarreto/agent-framework-public/blob/864fdc3b714c0df329cad6366bb9b233755928c3/docs/decisions/0030-realtime-ai-agent.md) |
| Adam's Semantic Kernel branch `dougal/realtime-agents` | `668a9c7a` (local only, not pushed). Base: [`microsoft/semantic-kernel@3b61c786`](https://github.com/microsoft/semantic-kernel/tree/3b61c786c9b4f9a8224f39f647859fd51af4c902) |
| AI Contact Centre accelerator | `main` at `a72f030e` of the private `mcaps-microsoft/ai-contact-center-solution-accelerator`. `src/` matches the local `feat/content-filter-handling` at `d42fbcf8`. Cited by path and line because the repo is private. |

## Question

How do existing realtime agent frameworks model handoffs between agents during one live realtime session, and what
constraints does Agent Framework's own handoff orchestration impose on doing the same?

## Answer

- **Every in-session handoff lives with whatever owns the live provider session, not in a request/response
  orchestrator.**
  - OpenAI Agents SDK: agents declare `handoffs` and the `RealtimeSession` carries them out.
  - LiveKit: `AgentSession.update_agent()` switches agent, or a tool returns an `Agent`.
  - The accelerator and the SK branch: a `RealtimeHandoffOrchestration` wraps one realtime client.
  - Pipecat: no speech-to-speech handoff. Its docs say Flows doesn't support speech-to-speech models.
- **The mechanism is the same everywhere.**
  - The handoff is a function tool named `transfer_to_<agent>` or `handoff_to_<agent>`.
  - On the call, the target's instructions and tools go to the same socket with `session.update`. Then the tool output
    is sent and the next response is requested.
  - Order matters: the update must land before the response that answers the handoff. The OpenAI SDK sends the update
    first. The accelerator switches inside the tool body for exactly this reason. The SK branch switches after the tool
    output, so it has the race.
- **Voice is the hard constraint.**
  - OpenAI and Azure OpenAI don't allow a voice change after the session's first audio output.
  - The OpenAI SDK keeps voice session-wide.
  - LiveKit's handoff never changes the voice of a reused session. A per-agent voice needs another model instance,
    which means a new provider session, or a per-agent TTS.
  - The accelerator gives agents different Azure voices and switches them only on Voice Live. Voice Live's own
    reference says both that voice can and can't be changed, so this needs a test.
- **History follows the session or the agent.**
  - In-session handoffs keep the provider's server-side conversation. The OpenAI SDK offers no `input_filter` for
    realtime handoffs, and the accelerator replays nothing.
  - LiveKit gives each agent its own context, fresh by default. It then diff-syncs the provider conversation to that
    context.
- **Agent Framework's `HandoffBuilder` can't deliver in-session handoffs by relaxing its `Agent` type check.** It
  rests on bounded, per-turn runs:
  - it clones a chat `Agent` from its client and `default_options`;
  - it injects handoff tools through `default_options`;
  - it finds the handoff in a finished `AgentResponse`;
  - each participant runs in its own workflow executor with its own `AgentSession`;
  - it asks for user input with `request_info`;
  - it shares history between agents as text-only messages.

  A realtime agent pushed through it would open a new realtime session on every turn, through ADR 0035's half-duplex
  `run()`. That's the opposite of an in-session handoff. The .NET ADR 0030 draft's claim of handoff compatibility has
  the same limit.
- **Parts of it can be reused by a realtime-specific mechanism:**
  - the topology API: `participants`, `add_handoff`, `with_start_agent`;
  - the `handoff_to_<id>` tool name, which the accelerator already uses;
  - the function-middleware interception of handoff calls, provided realtime tool calls run through Agent Framework's
    function-invocation layer, as ADR 0035 proposes;
  - the `handoff_sent` workflow event.

## Findings

Each framework is described with the same headings: where the handoff lives, what triggers it, how the switch happens,
what happens to history, what happens to in-flight audio and responses, and how the handoff shows up in events and
telemetry.

### OpenAI Agents SDK (Python), `agents.realtime`

- **Where it lives.** It's split between agent and session.
  - `RealtimeAgent` declares `handoffs: list[RealtimeAgent | Handoff]`
    ([agent.py L66-L72][oai-agent-handoffs]).
  - `RealtimeRunner` creates a `RealtimeSession` from a starting agent ([runner.py L18-L79][oai-runner]).
  - The session executes handoffs and tracks the current agent ([session.py L1311-L1379][oai-session-handoff]).
  - There's also an explicit API, `RealtimeSession.update_agent(agent)`, which sends a `session.update` built from the
    new agent ([session.py L388-L401][oai-update-agent]).
- **What triggers it.** A function call from the model.
  - Each handoff becomes a session function tool, named and described by the `Handoff`
    ([openai_realtime.py L1877-L1911][oai-tools-to-session]).
  - The default name is `transfer_to_<agent name>` ([handoffs/\_\_init\_\_.py L214-L218][oai-default-tool-name]).
  - `realtime_handoff(...)` customises the name, description, input type, `on_handoff` callback and `is_enabled`
    ([realtime/handoffs.py L108-L208][oai-realtime-handoff]).
- **How it switches.** On a handoff call the session runs `on_invoke_handoff` and builds new model settings from the
  target agent. Then it sends `session.update` *before* the tool output. That tool output is sent with
  `start_response=True` ([session.py L1345-L1378][oai-session-handoff-switch], [session.py L931-L945][oai-pending-output]).
  - The settings come from the session's base settings plus the agent's `prompt`, instructions, tools and handoffs
    ([session.py L2116-L2160][oai-settings-from-agent]).
  - Voice isn't per agent: it's read from the session-level model settings
    ([openai_realtime.py L1799-L1802][oai-voice-setting]).
  - The docs say voice "cannot change after the session has already produced spoken audio"
    ([guide.md L39-L44][oai-guide-agent-config]). The `RealtimeAgent` docstring says the same
    ([agent.py L27-L43][oai-agent-doc]).
- **History.** Nothing is replayed or filtered. The handoff branch sends only a session update and the tool output, so
  the server-side conversation carries over unchanged ([session.py L1345-L1378][oai-session-handoff-switch]). The docs
  say realtime handoffs "do **not** support the regular handoff `input_filter`" ([guide.md L297-L321][oai-guide-handoffs]).
  The code passes `input_filter=None` ([realtime/handoffs.py L131][oai-realtime-handoff-note],
  [L205][oai-realtime-handoff-filter]).
- **In-flight audio and responses.** The handoff doesn't cancel anything.
  - The response that carried the handoff call plays out.
  - The follow-up `response.create` goes through a sequencer that tracks the ongoing response
    ([openai_realtime.py L985-L1003][oai-send-tool-output], [L231-L245][oai-sequencer]).
  - When that turn ends, the SDK credits it to the agent captured at `turn_started`, because the handoff has already
    moved `_current_agent` ([session.py L614-L640][oai-turn-ended]).
  - Barge-in goes through the general interruption and playback-tracking path, not a handoff-specific one
    ([guide.md L252-L258][oai-guide-interruptions]).
- **Events and telemetry.**
  - The session emits `RealtimeHandoffEvent(from_agent, to_agent)` ([events.py L46-L59][oai-handoff-event]) and
    `agent_start`/`agent_end` ([guide.md L212-L227][oai-guide-events]).
  - Tracing is configured as the Realtime API's server-side `tracing` setting (workflow name, group id, metadata)
    ([config.py L240-L280][oai-tracing-config]).
- **Limitations.**
  - Voice is fixed once audio has been produced.
  - Handoffs can't filter history.
  - The model is session-wide: "all RealtimeAgents will be handled by the same model within a `RealtimeSession`"
    ([agent.py L27-L43][oai-agent-doc]).

The voice rule comes from the API itself:

- OpenAI: "The client may send this event at any time to update any field except for `voice` and `model`. `voice` can be
  updated only if there have been no other audio outputs yet" ([session_update_event.py L15-L25][openai-session-update]).
- Azure OpenAI: its GA realtime reference states the same rule ([Realtime client events reference, notes and
  constraints][aoai-realtime-constraints]).

### LiveKit Agents (Python)

- **Where it lives.** It's split between session and agent.
  - `AgentSession` owns the active `Agent` and the transport. It switches agents with `update_agent(agent)`
    ([agent_session.py L1788-L1805][lk-update-agent]) and `_update_activity`
    ([agent_session.py L1872-L2013][lk-update-activity]).
  - An `Agent` carries its own instructions, tools and `chat_ctx`. It can also carry its own `stt`, `llm` (including a
    `RealtimeModel`) and `tts` ([agent.py L47-L73][lk-agent-init]).
  - Agents have lifecycle hooks `on_enter` and `on_exit` ([agent.py L336-L342][lk-agent-hooks]).
- **What triggers it.** A function tool that returns an `Agent`, or `(Agent, result)`, or an explicit `update_agent`
  call ([docs: agents and handoffs][lk-docs]).
  - In the realtime path, a returned agent sets `_handoff_required` and calls `session.update_agent(...)`
    ([agent_activity.py L4863-L4874][lk-rt-tool-handoff]).
  - If several tools return agents, LiveKit logs an error and ignores the switch
    ([agent_activity.py L4857-L4861][lk-rt-multi-handoff]).
- **How it switches.**
  1. The old activity stops taking new user turns straight away ([agent_session.py L1791-L1795][lk-update-agent]).
  2. It drains: runs `on_exit` and waits for all queued speech tasks to complete
     ([agent_activity.py L1385-L1423][lk-drain], [L1446][lk-drain-speech]).
  3. It hands the provider session to the new activity only if all of these hold
     ([agent_activity.py L1125-L1173][lk-detach]):
     - the new agent uses the *same* `RealtimeModel` instance;
     - the model can change chat context, or the two contexts are equivalent;
     - the model can change instructions, or they're equal;
     - the model can change tools, or they're equal;
     - both agents resolve server-side turn detection the same way.

     The OpenAI realtime model can change chat context, instructions and tools mid-session
     ([\_realtime/openai.py L505-L518][lk-oai-capabilities]).
  4. On reuse it calls `interrupt()` and `clear_audio()`, then pushes the new instructions, chat context and tools
     ([agent_activity.py L1249-L1302][lk-start-session], [llm/realtime.py L305-L328][lk-update-session]).
  5. Otherwise it creates a new provider session with `self.llm.session(...)`, so the caller's audio stays connected
     while the model connection is replaced ([agent_activity.py L1260-L1268][lk-start-session-new]).

  On reuse, LiveKit pushes instructions, chat context and tools, never voice ([agent_activity.py L1296-L1302][lk-start-session]).
  Voice is a model option, so a per-agent voice means a per-agent model instance and therefore a new provider session.
  LiveKit's own tracing example gives one agent `openai.realtime.RealtimeModel(voice="alloy")`
  ([otel_trace.py L126-L150][lk-otel-example]). A per-agent `tts` is the other route.
- **History.** It's per agent and explicit. The docs say: "By default, each new agent or task starts with a fresh
  conversation history ... unless you explicitly pass conversation history using `chat_ctx`"
  ([docs: context preservation][lk-docs-context]).
  - When the provider session is reused, LiveKit diff-syncs the provider conversation to the new agent's `chat_ctx`
    with `conversation.item.delete` and `conversation.item.create` ([\_realtime/openai.py L1503-L1520][lk-update-chat-ctx],
    [L1555-L1615][lk-chat-ctx-diff]). Items not in the new context are removed from the provider conversation.
  - The full transcript stays in `session.history`, and an `AgentHandoff` item records the switch
    ([chat_context.py L494-L499][lk-handoff-item], [agent_session.py L1973-L1987][lk-handoff-item-insert]).
- **In-flight audio and responses.**
  - A bare handoff (a tool that returns only an agent) expects no reply
    ([generation.py L1241-L1243][lk-bare-handoff]).
  - If the tool also returns a result, the draining old agent replies with `tool_choice="none"`
    ([agent_activity.py L4944-L4958][lk-draining-reply]).
  - LiveKit waits for queued speech before sending tool outputs, because "most realtime models don't support generating
    multiple responses at the same time" ([agent_activity.py L4876-L4887][lk-wait-speech]).
  - A handoff whose turn is interrupted is answered as an error, "the agent handoff was interrupted and did not happen"
    ([generation.py L1254-L1265][lk-interrupted-handoff]).
  - The new agent usually speaks first from `on_enter` with `session.generate_reply()` ([docs][lk-docs]).
- **Events and telemetry.**
  - An OpenTelemetry span `update_agent` carries `lk.previous_agent_label` and `lk.agent_label`. The old agent's
    `on_exit` and the new agent's `on_enter` nest under it ([agent_session.py L1912-L1925][lk-handoff-span],
    [trace_types.py L11][lk-attr-agent], [L446][lk-attr-prev]).
  - A `conversation_item_added` event carries the `AgentHandoff` item ([agent_session.py L1983-L1987][lk-handoff-item-insert]).
- **Limitations.**
  - A per-agent voice or model forces a new provider session.
  - Reuse depends on model capabilities.
  - Context handling is the developer's job, and an agent with an empty `chat_ctx` wipes the provider conversation
    when the session is reused.
- **Also in LiveKit.** `AgentTask` is an inline sub-agent that pauses the caller and returns a result
  ([agent.py L878-L900][lk-agent-task]). That's delegation, not a handoff.

### Pipecat and Pipecat Flows

- **Pipecat has no handoff mechanism for speech-to-speech models.** Its pipeline is a chain of frame processors. Its
  two handoff-like features, Flows and multi-worker handoffs, are both built for cascaded STT → LLM → TTS pipelines.
- **Flows (moved into core as `pipecat.flows` since `pipecat-ai` 1.5.0)** ([pipecat-flows README L5-L9][pf-readme])
  models a conversation as nodes.
  - A function handler returns `(result, next_node)`. The result is sent with `run_llm=False`. Once the context holds
    the result and no calls are in flight, the transition runs ([manager.py L491-L536][pc-flows-edge],
    [L545-L556][pc-flows-check]).
  - The transition sets the role as `LLMUpdateSettingsFrame(system_instruction=...)`. It then appends or replaces
    messages, sets tools with `LLMSetToolsFrame`, and runs the LLM with `LLMRunFrame`
    ([manager.py L673-L790][pc-flows-set-node], [L805-L914][pc-flows-update-context]).
  - The context and tools frames are uninterruptible, so barge-in can't leave the LLM on the previous node's settings
    ([manager.py L908-L914][pc-flows-uninterruptible], [CHANGELOG L3654-L3659][pc-changelog-flows]).
  - The docs exclude speech-to-speech: "Flows needs a text LLM with function calling in a cascaded STT → LLM → TTS
    pipeline ... Speech-to-speech models such as Gemini Live, OpenAI Realtime, Ultravox, and Nova Sonic aren't supported,
    because Flows moves between nodes by rewriting the LLM's context and tools mid-session and those APIs don't expose
    that" ([Pipecat Flows introduction][pc-flows-docs]).
- **The OpenAI realtime service could take most of a node switch on one socket.**
  - It turns `LLMSetToolsFrame` into `session.update` ([llm.py L700-L705][pc-rt-settools]).
  - It applies settings deltas such as `system_instruction` with `session.update` ([llm.py L843-L852][pc-rt-settings]).
  - It adds appended messages as conversation items ([llm.py L752-L775][pc-rt-append]).
  - It doesn't delete server-side items when the context is replaced; it only stores the new context
    ([llm.py L729-L750][pc-rt-context]). Its only reset is `reset_conversation()`, which disconnects and reconnects
    ([llm.py L1260-L1273][pc-rt-reset]).
  - It holds a second `response.create` until the active response is done ([llm.py L1276-L1286][pc-rt-hold]).
- **Other Pipecat patterns.**
  - The multi-worker framework hands off between LLM workers: "Two or more LLM workers swap 'who is talking'"
    ([multi-worker README L36][pc-mw-readme]). This uses `LLMWorker.activate_worker(...)`, which drains the current
    worker ([llm_worker.py L245-L295][pc-activate-worker]). Workers raise `on_activated`/`on_deactivated` events
    ([base_worker.py L246-L247][pc-worker-events]). Among the multi-worker examples at this SHA, only the
    `llm-with-backend` ones use a realtime service; the handoff examples use cascaded pipelines.
  - `LLMWithBackend` pairs a speech-to-speech frontend, such as OpenAI Realtime, with a tool-using backend worker
    ([llm-with-backend README L3][pc-backend-readme], [L26][pc-backend-rt]). That's delegation, not a handoff.
- **Events and telemetry for Flows.** `FlowManager` registers no event handlers and creates no spans in `manager.py` at
  this SHA. Transitions show up only in debug logs and the frames themselves.

### .NET ADR 0030 draft (Realtime Agent over `IRealtimeClient`)

- **Claim.** Reusing `AIAgent` keeps "multi-agent orchestration, handoffs" composable. Option 1 is rejected because "it
  loses direct composition with ... orchestration, and handoffs" ([ADR 0030 L23-L35][adr30-drivers],
  [L173-L176][adr30-option1]).
- **What that buys.**
  - The base `AIAgent` surface is half-duplex. Each `RunAsync`/`RunStreamingAsync` "open[s] a fresh ephemeral
    `IRealtimeClientSession` per run" and replays history into the new socket ([ADR 0030 L64-L76][adr30-halfduplex]).
  - The full-duplex surface is a separate `AgentConversation` from `StartConversationAsync`
    ([ADR 0030 L78-L93][adr30-fullduplex]).
  - The ADR says nothing about how a handoff would act on a live `AgentConversation`.
- **Agent Framework .NET's handoff executor.**
  - It accepts any `AIAgent`, but injects handoff tools as declaration-only functions in `ChatClientAgentRunOptions`
    with `AllowMultipleToolCalls = false` ([HandoffAgentExecutor.cs L236-L273][dotnet-handoff-tools]).
  - It runs each turn with `RunStreamingAsync(messages, session, options)` ([HandoffAgentExecutor.cs L586-L589][dotnet-handoff-run]).
  - So the realtime agent must honour `ChatClientAgentRunOptions` tools.
- **Conclusion.** Any handoff through it is a reconnect per turn and per handoff, not an in-session handoff.

### Adam's Semantic Kernel branch (`dougal/realtime-agents` at `668a9c7a`, local only)

- **Where it lives.** An orchestration: `RealtimeHandoffOrchestration` holds `members: list[RealtimeAgent]`,
  `OrchestrationHandoffs` and one `realtime_client`. "All agents share the same realtime session"
  (`python/semantic_kernel/agents/realtime/realtime_handoff_orchestration.py` L53-L122).
- **What triggers it.** Generated `transfer_to_<agent>` kernel functions, added to a clone of each agent's kernel (same
  file, L209-L263).
- **How it switches.**
  - The handoff function only records `_handoff_pending` (L265-L273).
  - The receive loop yields the function result and *then* calls `_switch_to_agent`. That sends `update_session(settings,
    kernel, create_response=True)` with the target's instructions, `voice=agent.voice` and function-calling settings
    (L275-L302, L314-L336, L461-L468).
- **The race.** SK's OpenAI realtime connector has, by then, already sent the function output and a `response.create`
  ([\_open\_ai\_realtime.py L454-L467][sk-function-call]). So the reply to the handoff is requested under the old agent,
  and the switch then asks for a second response ([\_open\_ai\_realtime.py L336-L405][sk-update-session]). The
  accelerator avoids this (next section).
- **Voice.** The sample gives each agent its own OpenAI voice: `alloy`, `echo`, `shimmer`
  (`python/samples/concepts/realtime/realtime_multi_agent_handoff.py` L134, L154, L174). OpenAI doesn't allow a voice
  change once audio has been produced (see the voice rule under the OpenAI Agents SDK).
- **History.** Kept in the shared provider session.
- **Events and telemetry.** `on_function_call` and `on_function_result` callbacks, plus `is_handoff_pending()` and
  `get_pending_handoff_target()` (L48-L50, L536-L550). No spans or handoff event.

### Accelerator `RealtimeHandoffOrchestration`

Paths are under `src/ai_contact_centre_solution_accelerator/`, at `a72f030e`.

- **Where it lives.** A `RealtimeHandoffOrchestration` with members, `OrchestrationHandoffs` and one realtime client.
  "All agents share the same realtime session" (`core/orchestration.py` L84-L121).
  - It's constructed once per call (`routes/call.py` L176-L181).
  - For Foundry voice agents, only the entry agent is published. "Handoffs switch the session to the other agents with
    `session.update`" (`core/foundry_voice_agent.py` L1-L7).
- **What triggers it.** `handoff_to_<agent>` kernel functions, added to a cloned kernel next to the agent's MCP plugins
  (`core/orchestration.py` L176-L243).
  - With silent handoffs on (the default), the tool description says "Call this function WITHOUT saying anything first"
    (L205-L211).
  - Every agent's instructions get a "unified assistant" suffix that forbids transfer announcements and re-greetings
    (`config.py` L245-L264, appended at `core/orchestration.py` L292-L293).
- **How it switches.** The switch runs *inside* the handoff function, because "Semantic Kernel sends the function result
  and asks the model to reply as soon as it returns. The target agent's session must be in place by then, or the reply
  is generated with the previous agent's instructions, tools and voice" (`core/orchestration.py` L245-L265).
  - `_switch_to_agent` sends `update_session(settings, kernel)` and sets `current_agent` only after it succeeds (L333-L343).
  - In production `start()` gets base settings (`routes/call.py` L194), so the voice changes only when the base
    settings' voice is an object with a `name`: Voice Live's `AzureVoiceLiveVoiceConfig` (`core/orchestration.py`
    L274-L325, `core/client_factory.py` L78-L91, L137-L141). The Azure OpenAI Realtime settings carry no such object,
    so the voice is left alone there. Only without base settings does it build
    `OpenAIRealtimeExecutionSettings(voice=agent.voice)`.
  - `config.yaml` gives billing and support their own Azure voices: `en-GB-SoniaNeural` and `en-GB-AlfieNeural`
    (`config.yaml` L186, L218, L241).
- **History.** Kept in the provider session, with nothing replayed.
- **In-flight audio and responses.** No handoff-specific handling. The prompt keeps the model from speaking before the
  handoff call, and the receive loop tracks pending function calls (L429-L455).
- **Events and telemetry.**
  - The switch runs in an `invoke_agent` span with `handoff.from` and `handoff.to` attributes (L345-L358).
  - `stream_session_events` emits `AgentSwitchEvent` when the current agent changes (L457-L472).
  - It hides `handoff_to_*` call and result events from the UI (L480-L483, L503-L505).
- **The chat channel already uses Agent Framework.** `chat/handoff.py` builds Agent Framework's `HandoffBuilder` from the
  same `config.handoffs`, with a hop-limit termination condition (`chat/handoff.py` L67-L105, `config.py` L266-L272).
  One handoff topology drives two different engines today.

### Agent Framework Python `HandoffBuilder` (`agent_framework_orchestrations`)

**What it assumes about participants**

- **Concrete `Agent` only.** `participants(...)` takes `Sequence[Agent]` and raises `TypeError` for anything else. The
  reason given is "Handoff workflows require Agent because they rely on cloning, tool injection, and middleware
  capabilities" ([\_handoff.py L737-L787][af-participants], docstring [L643-L668][af-builder-doc]).
- **Workflow executors with bounded runs.** Each participant becomes a `HandoffAgentExecutor(AgentExecutor)`
  ([\_handoff.py L243-L297][af-executor-init], [L1182-L1223][af-resolve-executors]).
  - Every executor has its own `AgentSession` ([\_agent\_executor.py L349][af-executor-session]).
  - It calls `agent.run(cache, stream=..., session=...)` once per activation ([\_agent\_executor.py L837-L846][af-executor-run],
    [L904-L913][af-executor-run-stream]).
  - The graph is fully connected with fan-out edges, so the active agent can broadcast to the others
    ([\_handoff.py L1105-L1116][af-fanout]).
- **Turn-based user input.** After each response without a handoff, the executor asks for the next user turn with
  `ctx.request_info(HandoffAgentUserRequest(response), list[Message])`, unless autonomous mode is on
  ([\_handoff.py L489-L501][af-request-info]).
- **Chat-history guarantee.** `build()` rejects any participant without `require_per_service_call_history_persistence=True`,
  because the short-circuited handoff calls never reach the service ([\_handoff.py L1058-L1075][af-build-validate]).

**How handoff tools are injected and detected**

- **Clone.** `_clone_chat_agent` rebuilds `Agent(client=agent.client, ..., default_options=cloned_options)`. It sets
  `allow_multiple_tool_calls = False` so that only one handoff can be called at a time ([\_handoff.py L330-L365][af-clone]).
- **Inject.** `_apply_auto_tools` appends one `@tool(name="handoff_to_<id>", approval_mode="never_require")` per target to
  `default_options["tools"]` ([\_handoff.py L367-L412][af-apply-tools]). Names come from `get_handoff_tool_name`
  ([L130-L137][af-tool-name]).
- **Intercept.** `_AutoHandoffMiddleware`, a `FunctionMiddleware`, catches those calls. It records `(call_id, target)`,
  sets the synthetic result `{"handoff_to": target}` and raises `MiddlewareTermination` ([\_handoff.py L142-L175][af-middleware]).
  - It's appended to the clone's middleware list. When the agent has context providers, it's contributed through an
    extra context provider instead, so it runs after the providers' own middleware ([L178-L195][af-provider],
    [L299-L328][af-prepare]).
- **Detect.** `_is_handoff_requested` needs the matching function result in the *last message of a finished
  `AgentResponse`* ([\_handoff.py L556-L604][af-detect]).
- **Route.** The executor sends `AgentExecutorRequest(messages=[], should_respond=True)` to the target executor and emits
  `WorkflowEvent("handoff_sent", HandoffSentEvent(source, target))` ([\_handoff.py L459-L481][af-route],
  [L81-L86][af-sent-event]).
- **Share history.** Each response is cleaned and broadcast to every executor
  ([\_handoff.py L448-L457][af-broadcast]). The cleaner drops tool-control content and keeps assistant messages
  text-only ([\_orchestrator\_helpers.py L22-L66][af-clean]), so assistant audio wouldn't survive; only a transcript
  would.

**What would have to change for a realtime agent to participate**

| Assumption | Where | Why it fails for a live realtime session |
| --- | --- | --- |
| Participant is a chat `Agent` that can be cloned from `client` and `default_options` | [L330-L365][af-clone], [L737-L787][af-participants] | ADR 0035's `RealtimeAgent` derives from `BaseAgent`, not `Agent` or `RawAgent` ([ADR 0035 L405-L427][adr35-handoff]). Its tools go to the provider through `session.update`, not chat options. |
| Handoff is found in a finished `AgentResponse` | [L556-L604][af-detect] | A live conversation has no bounded response. The signal would have to be an event on the conversation's tool-call path. |
| One executor and one `AgentSession` per participant, one `agent.run` per activation | [\_agent\_executor.py L349][af-executor-session], [L837-L846][af-executor-run] | ADR 0035's `run()` "opens an ephemeral realtime session" per call ([ADR 0035 L400-L403][adr35-run]). Each turn and each handoff would reconnect. |
| User input arrives as `request_info` turns | [L489-L501][af-request-info] | Realtime input is continuous audio with server-side turn detection and barge-in. |
| History moves as cleaned, text-only messages broadcast to all executors | [L448-L457][af-broadcast], [helpers L22-L66][af-clean] | With an in-session handoff the provider already holds the history. Re-sending it would duplicate it, and audio isn't kept. |
| `require_per_service_call_history_persistence` on every participant | [L1058-L1075][af-build-validate] | It's a chat-history consistency rule with no direct realtime equivalent. |

What carries over without change, if realtime tool calls go through Agent Framework's function-invocation layer as
ADR 0035 proposes ([ADR 0035 L618-L630][adr35-sk-compare]):

- the interception idea in `_AutoHandoffMiddleware`;
- the `handoff_to_<id>` naming;
- `HandoffConfiguration` (target and description);
- the `handoff_sent` event;
- the topology API.

ADR 0035 already rules handoffs out of phase 1: "handoff participants must be concrete `Agent` instances ... Realtime
handoff support is deferred until that orchestration is independently generalized beyond `Agent`"
([ADR 0035 L405-L409][adr35-handoff], [L66-L69][adr35-driver]). That followed the review note that
`SupportsAgentRun` "is not currently sufficient for the promised handoff/delegation compatibility"
([review comment](https://github.com/microsoft/agent-framework/pull/6831#discussion_r3852979100),
[reply](https://github.com/microsoft/agent-framework/pull/6831#discussion_r3922199696)).

ADR 0035 also defines `RealtimeConversation` as "owned by a RealtimeAgent". Its surface is `send`, `interrupt`, `close`
and one update stream. It has no typed session update; that's only a raw `RealtimeEvent` command
([ADR 0035 L211-L240][adr35-conversation], [L1058-L1062][adr35-commands]). An in-session handoff changes which agent's
configuration is live on one conversation, so both the ownership wording and the missing typed update matter.

**Generalising `HandoffBuilder` versus a realtime-specific mechanism**

- **Generalising `HandoffBuilder`** (accepting `SupportsAgentRun` or a realtime agent) needs four things:
  - a clone-or-wrap seam that doesn't assume chat options;
  - tool injection through a seam the realtime agent honours;
  - handoff detection that doesn't depend on `AgentResponse`;
  - acceptance of per-turn half-duplex runs.

  It would give realtime agents the workflow features (checkpointing, `request_info`, termination conditions), but
  never an in-session handoff.
- **A realtime-specific mechanism** needs:
  - one owner of the live conversation that swaps instructions, tools and voice on it;
  - interception of handoff tool calls inside the conversation's tool loop;
  - a guarantee that the session update reaches the provider before the tool output that triggers the next response;
  - a handoff event.

  It can reuse the topology vocabulary and tool naming above, so one `config.handoffs` could still drive both the chat
  `HandoffBuilder` and the voice channel, as it does in the accelerator.

## Comparison table

| | OpenAI Agents SDK | LiveKit Agents | Pipecat / Flows | .NET ADR 0030 draft | SK branch | Accelerator | AF `HandoffBuilder` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **Where handoff lives** | Declared on `RealtimeAgent.handoffs`, executed by `RealtimeSession` | `AgentSession.update_agent`; tool returns an `Agent` | Flows node transitions (cascaded pipelines only); worker handoff between pipelines | Reuses AF .NET handoff workflows over `AIAgent` | `RealtimeHandoffOrchestration` | `RealtimeHandoffOrchestration` | Workflow of `HandoffAgentExecutor`s |
| **Trigger** | `transfer_to_<agent>` tool call; `update_agent()` | Tool returns an agent; `update_agent()` | Function returns `(result, next_node)`; `activate_worker()` | Declaration-only handoff tool call per run | `transfer_to_<agent>` kernel function | `handoff_to_<agent>` kernel function | `handoff_to_<id>` tool, caught by middleware |
| **Instructions and tools switch** | `session.update`, sent before tool output and `response.create` | `update_instructions`, `update_chat_ctx`, `update_tools` on a reused session, or a new session | `LLMUpdateSettingsFrame`, messages frame, `LLMSetToolsFrame`, then `LLMRunFrame` | New ephemeral session per run | `update_session` *after* SK has already sent tool output and `response.create` (race) | `update_session` inside the tool body, before the result returns | The next executor runs its own cloned `Agent` |
| **Voice switch** | No: voice is session-wide and fixed after first audio | Only with another model instance (new provider session) or a per-agent TTS | Not applicable to speech-to-speech | Not covered | Sends `voice=` (OpenAI doesn't allow it after first audio) | Voice Live only, via `voice.name` (unverified) | Not applicable |
| **Session continuity** | Same socket | Same socket if the same model instance and capabilities allow, else reconnect (caller stays connected) | OpenAI realtime service: same socket for settings and tools; reset reconnects | Reconnect per turn | Same socket | Same socket | Not applicable (one bounded run per turn) |
| **History** | Server conversation kept; no `input_filter` | Per-agent `chat_ctx`, fresh by default; provider conversation diff-synced | `APPEND` or `RESET` strategies (cascaded) | Replayed into each new socket | Kept on the server | Kept on the server | Cleaned, text-only messages broadcast to all executors |
| **In-flight audio and responses** | Not cancelled; response creation sequenced | Old agent drains; reused session interrupted and cleared; interrupted handoff fails | Transition frames uninterruptible | Not covered | Not handled; double `response.create` | Prompt rule against speaking before handoff | Not applicable |
| **Events and telemetry** | `handoff` event, `agent_start`/`agent_end`; server-side tracing config | `update_agent` span with previous and new agent labels; `AgentHandoff` chat item | Worker `on_activated`/`on_deactivated`; no Flows events | Not covered | Function callbacks; pending-handoff getters | `invoke_agent` span with `handoff.from`/`handoff.to`; `AgentSwitchEvent` | `handoff_sent` workflow event; feature-usage mark |
| **Key limitation** | Voice fixed; one model; no history filter | Voice or model change means reconnect; context is the developer's job | Flows doesn't support speech-to-speech models | No in-session handoff | Ordering race; per-agent OpenAI voices not allowed after first audio | Switching depends on SK internals; Voice Live voice behaviour unverified | Bounded request/response runs only |

## Implications for the map

### Design options for Agent Framework

These are options, not a recommendation.

1. **Generalise `HandoffBuilder` to realtime participants** (half-duplex runs per turn).
   - Gains: one handoff API; workflow features such as checkpointing, `request_info` and termination conditions.
   - Costs: no in-session handoff; a reconnect per turn and per handoff; no barge-in or full duplex. The same limit as
     the .NET ADR 0030 draft. It also needs the clone, inject and detect seams in the earlier table.
2. **Agent-declared handoffs carried out by the live conversation** (the OpenAI Agents SDK model).
   `RealtimeAgent(handoffs=[...])`; `RealtimeConversation` swaps the active agent with `session.update`, then sends the
   tool output and creates the next response.
   - Gains: in-session; small API; matches the OpenAI Agents SDK's model.
   - Costs: a second handoff concept beside `HandoffBuilder`; the topology lives on the agents; ADR 0035's "conversation
     owned by a `RealtimeAgent`" becomes "owned by the *starting* agent"; voice stays session-wide on OpenAI and Azure
     OpenAI.
3. **A realtime handoff orchestration in `agent-framework-orchestrations`** (the accelerator and SK model). For example,
   a realtime builder that mirrors `HandoffBuilder`'s `participants`/`add_handoff`/`with_start_agent`, owns one
   `RealtimeConversation`, and emits a `handoff_sent`-style event.
   - Gains: in-session; the same vocabulary and config shape as the chat path (the accelerator already feeds one
     `config.handoffs` to both); agents stay simple configuration holders.
   - Costs: it isn't a `Workflow`, so no checkpointing or `request_info`; more surface area; it needs the conversation
     to expose a typed session update and an interception point for tool calls.
4. **Primitives only.** A typed `RealtimeConversation` session update (instructions, tools, voice) that guarantees it
   lands before the next response, a documented hook to intercept tool calls (function middleware), and an
   agent-switched event. Applications build their own orchestration.
   - Gains: smallest core change; flexible; also enables non-handoff uses such as mode changes and greetings.
   - Costs: every application has to get the ordering rule right itself (the SK branch gets it wrong; the accelerator's
     docstring spells out the fix); handoffs are less discoverable; telemetry is inconsistent between applications.
5. **Session-level `update_agent` with reuse or reconnect** (the LiveKit model). Reuse the provider session when only
   instructions and tools change; reconnect, keeping the caller connected, when voice or model change.
   - Gains: per-agent voices work on providers that refuse voice changes.
   - Costs: the most machinery; a reconnect needs history replay and a policy for it; overlaps with ADR 0035's deferred
     warm-connection work.

Cross-cutting choices, whichever option wins:

- **Ordering guarantee.** The session update must land before the tool output that triggers the next response.
- **Voice policy.** Session-wide, provider-dependent, or reconnect.
- **History policy.** One provider conversation, or per-agent context.
- **Event and span shape.** For example, reuse `HandoffSentEvent(source, target)` and add `handoff.from`/`handoff.to`
  span attributes as the accelerator does.
- **Parallel calls.** What happens if a response holds more than one handoff call. `HandoffBuilder` disables parallel
  tool calls; LiveKit ignores the switch.

### For the map's tickets

- **[Handoff model for realtime agents](https://github.com/adamdougal/agent-framework/issues/7)** can be unblocked. This
  survey gives it the five options above and the constraints to settle:
  - where the handoff lives;
  - how it relates to `HandoffBuilder`;
  - conversation ownership;
  - the ordering guarantee;
  - the voice and history policies;
  - the event and span shape.
- **[Prototype the accelerator's voice loop on the extended API](https://github.com/adamdougal/agent-framework/issues/10)**
  should exercise the chosen ordering rule: switch before the tool output that triggers the next response. It should
  also exercise a voice change on Voice Live, because the accelerator relies on it.
- **The extended ADR** needs a typed mid-conversation session update. Whichever option wins, it should cover what
  in-session handoffs need, not only raw `RealtimeEvent` commands. It also needs to reword "conversation owned by a
  `RealtimeAgent`" if the active agent can change.
- **Out of scope.**
  - Handoffs that change model or provider, which need a new provider session.
  - LiveKit-style `AgentTask` sub-agents and Pipecat-style frontend/backend delegation, which aren't handoffs.
  - Pipecat Flows-style node graphs.

  The accelerator uses none of these.

## Open questions

1. **Voice Live.**
   - Its 2026-07-15 reference says `session.update` "can be sent at any time to modify settings such as voice". The same
     page's session component says "Once the voice is used in the session for the model's audio response, it can't be
     changed" ([Voice Live 2026-07-15 API reference][voicelive-ref]).
   - Which is true for Azure standard voices such as `en-GB-SoniaNeural`, and for a Foundry voice agent?
   - The accelerator's per-agent voices depend on the answer.
2. **Foundry voice agents.** Does a mid-session `session.update` reliably override a *published* Foundry voice agent's
   instructions and tools? The accelerator assumes so (`core/foundry_voice_agent.py` L1-L7). Is that a supported
   contract or an accident of the preview?
3. **Interception in a realtime tool loop.** If realtime tool calls reuse Agent Framework's function-invocation layer,
   does a `MiddlewareTermination` short-circuit (as in `_AutoHandoffMiddleware`) produce a function output that the
   conversation sends after the session update? Or does the realtime tool loop need its own hook?
4. **Persistence.** After an in-session handoff, whose `AgentSession` holds the transcript: one per conversation or one
   per agent? How does that meet the map's open "persisting transcripts into agent sessions" fog?
5. **Recovery.** Can a realtime handoff orchestration offer anything like workflow checkpointing, or is recovery limited
   to replaying history into a new provider session?

<!-- Reference links -->
[oai-agent-doc]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/agent.py#L27-L43
[oai-agent-handoffs]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/agent.py#L66-L72
[oai-runner]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/runner.py#L18-L79
[oai-update-agent]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/session.py#L388-L401
[oai-session-handoff]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/session.py#L1311-L1379
[oai-session-handoff-switch]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/session.py#L1345-L1378
[oai-pending-output]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/session.py#L931-L945
[oai-settings-from-agent]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/session.py#L2116-L2160
[oai-turn-ended]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/session.py#L614-L640
[oai-tools-to-session]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/openai_realtime.py#L1877-L1911
[oai-voice-setting]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/openai_realtime.py#L1799-L1802
[oai-send-tool-output]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/openai_realtime.py#L985-L1003
[oai-sequencer]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/openai_realtime.py#L231-L245
[oai-default-tool-name]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/handoffs/__init__.py#L214-L218
[oai-realtime-handoff]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/handoffs.py#L108-L208
[oai-realtime-handoff-note]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/handoffs.py#L131
[oai-realtime-handoff-filter]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/handoffs.py#L205
[oai-handoff-event]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/events.py#L46-L59
[oai-tracing-config]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/src/agents/realtime/config.py#L240-L280
[oai-guide-agent-config]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/docs/realtime/guide.md?plain=1#L39-L44
[oai-guide-events]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/docs/realtime/guide.md?plain=1#L212-L227
[oai-guide-interruptions]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/docs/realtime/guide.md?plain=1#L252-L258
[oai-guide-handoffs]: https://github.com/openai/openai-agents-python/blob/17d994bd3045dae71acda676a4ef4762ed8ecd94/docs/realtime/guide.md?plain=1#L297-L321
[openai-session-update]: https://github.com/openai/openai-python/blob/190dd5aee1053dd2093d2a5c3937f604860310a3/src/openai/types/realtime/session_update_event.py#L15-L25
[aoai-realtime-constraints]: https://learn.microsoft.com/azure/foundry-classic/openai/realtime-audio-reference-ga#notes-and-constraints
[voicelive-ref]: https://learn.microsoft.com/azure/ai-services/speech-service/voice-live-api-reference-2026-07-15
[lk-agent-init]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent.py#L47-L73
[lk-agent-hooks]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent.py#L336-L342
[lk-agent-task]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent.py#L878-L900
[lk-update-agent]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent_session.py#L1788-L1805
[lk-update-activity]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent_session.py#L1872-L2013
[lk-handoff-span]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent_session.py#L1912-L1925
[lk-handoff-item-insert]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent_session.py#L1973-L1987
[lk-detach]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent_activity.py#L1125-L1173
[lk-start-session]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent_activity.py#L1249-L1302
[lk-start-session-new]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent_activity.py#L1260-L1268
[lk-drain]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent_activity.py#L1385-L1423
[lk-drain-speech]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent_activity.py#L1446
[lk-rt-multi-handoff]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent_activity.py#L4857-L4861
[lk-rt-tool-handoff]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent_activity.py#L4863-L4874
[lk-wait-speech]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent_activity.py#L4876-L4887
[lk-draining-reply]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/agent_activity.py#L4944-L4958
[lk-bare-handoff]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/generation.py#L1241-L1243
[lk-interrupted-handoff]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/voice/generation.py#L1254-L1265
[lk-update-session]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/llm/realtime.py#L305-L328
[lk-oai-capabilities]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/llm/_realtime/openai.py#L505-L518
[lk-update-chat-ctx]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/llm/_realtime/openai.py#L1503-L1520
[lk-chat-ctx-diff]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/llm/_realtime/openai.py#L1555-L1615
[lk-handoff-item]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/llm/chat_context.py#L494-L499
[lk-attr-agent]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/telemetry/trace_types.py#L11
[lk-attr-prev]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/livekit-agents/livekit/agents/telemetry/trace_types.py#L446
[lk-otel-example]: https://github.com/livekit/agents/blob/ff962a019b6500e08f8f1fcd7f55673361bd9e91/examples/voice_agents/otel_trace.py#L126-L150
[lk-docs]: https://docs.livekit.io/agents/logic/agents-handoffs/
[lk-docs-context]: https://docs.livekit.io/agents/logic/agents-handoffs/#context-preservation
[pf-readme]: https://github.com/pipecat-ai/pipecat-flows/blob/96223f4e9ea6f84651e379978be6560cf3ae50c8/README.md?plain=1#L5-L9
[pc-flows-edge]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/src/pipecat/flows/manager.py#L491-L536
[pc-flows-check]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/src/pipecat/flows/manager.py#L545-L556
[pc-flows-set-node]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/src/pipecat/flows/manager.py#L673-L790
[pc-flows-update-context]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/src/pipecat/flows/manager.py#L805-L914
[pc-flows-uninterruptible]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/src/pipecat/flows/manager.py#L908-L914
[pc-changelog-flows]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/CHANGELOG.md?plain=1#L3654-L3659
[pc-flows-docs]: https://docs.pipecat.ai/pipecat/flows/introduction
[pc-rt-settools]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/src/pipecat/services/openai/realtime/llm.py#L700-L705
[pc-rt-context]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/src/pipecat/services/openai/realtime/llm.py#L729-L750
[pc-rt-append]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/src/pipecat/services/openai/realtime/llm.py#L752-L775
[pc-rt-settings]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/src/pipecat/services/openai/realtime/llm.py#L843-L852
[pc-rt-reset]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/src/pipecat/services/openai/realtime/llm.py#L1260-L1273
[pc-rt-hold]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/src/pipecat/services/openai/realtime/llm.py#L1276-L1286
[pc-mw-readme]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/examples/multi-worker/README.md?plain=1#L36
[pc-activate-worker]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/src/pipecat/workers/llm/llm_worker.py#L245-L295
[pc-worker-events]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/src/pipecat/workers/base_worker.py#L246-L247
[pc-backend-readme]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/examples/multi-worker/llm-with-backend/README.md?plain=1#L3
[pc-backend-rt]: https://github.com/pipecat-ai/pipecat/blob/fda1d5e0cde9625c8dcef7b653ed731c2ce63672/examples/multi-worker/llm-with-backend/README.md?plain=1#L26
[adr30-drivers]: https://github.com/rogerbarreto/agent-framework-public/blob/864fdc3b714c0df329cad6366bb9b233755928c3/docs/decisions/0030-realtime-ai-agent.md?plain=1#L23-L35
[adr30-halfduplex]: https://github.com/rogerbarreto/agent-framework-public/blob/864fdc3b714c0df329cad6366bb9b233755928c3/docs/decisions/0030-realtime-ai-agent.md?plain=1#L64-L76
[adr30-fullduplex]: https://github.com/rogerbarreto/agent-framework-public/blob/864fdc3b714c0df329cad6366bb9b233755928c3/docs/decisions/0030-realtime-ai-agent.md?plain=1#L78-L93
[adr30-option1]: https://github.com/rogerbarreto/agent-framework-public/blob/864fdc3b714c0df329cad6366bb9b233755928c3/docs/decisions/0030-realtime-ai-agent.md?plain=1#L173-L176
[dotnet-handoff-tools]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/dotnet/src/Microsoft.Agents.AI.Workflows/Specialized/HandoffAgentExecutor.cs#L236-L273
[dotnet-handoff-run]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/dotnet/src/Microsoft.Agents.AI.Workflows/Specialized/HandoffAgentExecutor.cs#L586-L589
[sk-update-session]: https://github.com/microsoft/semantic-kernel/blob/3b61c786c9b4f9a8224f39f647859fd51af4c902/python/semantic_kernel/connectors/ai/open_ai/services/_open_ai_realtime.py#L336-L405
[sk-function-call]: https://github.com/microsoft/semantic-kernel/blob/3b61c786c9b4f9a8224f39f647859fd51af4c902/python/semantic_kernel/connectors/ai/open_ai/services/_open_ai_realtime.py#L454-L467
[af-sent-event]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L81-L86
[af-tool-name]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L130-L137
[af-middleware]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L142-L175
[af-provider]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L178-L195
[af-executor-init]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L243-L297
[af-prepare]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L299-L328
[af-clone]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L330-L365
[af-apply-tools]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L367-L412
[af-broadcast]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L448-L457
[af-route]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L459-L481
[af-request-info]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L489-L501
[af-detect]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L556-L604
[af-builder-doc]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L643-L668
[af-participants]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L737-L787
[af-build-validate]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L1058-L1075
[af-fanout]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L1105-L1116
[af-resolve-executors]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_handoff.py#L1182-L1223
[af-clean]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/orchestrations/agent_framework_orchestrations/_orchestrator_helpers.py#L22-L66
[af-executor-session]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_workflows/_agent_executor.py#L349
[af-executor-run]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_workflows/_agent_executor.py#L837-L846
[af-executor-run-stream]: https://github.com/microsoft/agent-framework/blob/f4bdd1dc98ae5aa38ce2eee2fdfc43bd6b30c44f/python/packages/core/agent_framework/_workflows/_agent_executor.py#L904-L913
[adr35-driver]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md?plain=1#L66-L69
[adr35-conversation]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md?plain=1#L211-L240
[adr35-run]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md?plain=1#L400-L403
[adr35-handoff]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md?plain=1#L405-L427
[adr35-sk-compare]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md?plain=1#L618-L630
[adr35-commands]: https://github.com/eavanvalkenburg/agent-framework/blob/591b7ea7e50aabef3cf0fca55b21a622d309d5f9/docs/decisions/0035-python-realtime-agent.md?plain=1#L1058-L1062
