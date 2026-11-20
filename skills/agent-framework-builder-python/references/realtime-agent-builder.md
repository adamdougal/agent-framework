# Realtime agent builder

Use this path when callers talk with an agent live: speech flows both ways at once, on a speech-to-speech model whose
provider detects turns and lets the caller interrupt. Realtime agents are experimental, and Foundry voice agents are in
preview.

## Realtime agent or chat agent

- Choose `RealtimeAgent` for a live voice call.
- Choose a chat `Agent` for text, for bounded turns, or when a workflow, a hosting adapter, or `as_tool()` must run the
  agent. Put the application's own speech-to-text and text-to-speech around it when it needs voice. A voice session
  per text turn is slower and costlier than a chat model.
- Use the agent rather than a provider client's session directly. The session alone has no tool loop, history,
  middleware, handoffs, or telemetry.

## Provider selection

| Need | Distribution and API |
| --- | --- |
| OpenAI Realtime | `agent-framework-openai[realtime]`, `OpenAIRealtimeClient` |
| Direct access to your own Azure OpenAI realtime deployment | `agent-framework-openai[realtime]`, `OpenAIRealtimeClient` with Azure settings |
| Azure Voice Live: Azure voices and audio processing, on a managed model or your own deployment | `agent-framework-foundry[realtime]`, `VoiceLiveClient` |
| An agent published in Foundry as a voice agent, whose definition owns its instructions, voice, and tools | `agent-framework-foundry[realtime]`, `FoundryVoiceAgent` |

Two provider differences change the choice:

- Only an Azure text-to-speech voice, on Voice Live or a Foundry voice agent, speaks application-supplied text word for
  word with `say()`, such as a required disclosure.
- OpenAI and Azure OpenAI keep a call's first voice once the agent has spoken. For handoff participants with their own
  voices, use Azure voices on Voice Live, or one voice family on a Foundry voice agent.

Check the
[OpenAI](https://github.com/microsoft/agent-framework/blob/main/python/packages/openai/README.md#realtime-agents-experimental)
and [Foundry](https://github.com/microsoft/agent-framework/blob/main/python/packages/foundry/README.md#voice-live-experimental)
package READMEs for other provider differences before relying on one.

## Live call design

- **Audio.** The application owns audio devices, the transport to a browser or phone line, and playback. When the
  caller talks over the agent, stop playback and report what the caller heard with `interrupt(played_ms=...)`, so
  history keeps only the heard text.
- **Mid-call changes.** Use `update_session()` when one agent changes its instructions, voice, or tools, and
  `RealtimeHandoffBuilder` for distinct roles. Both keep the same realtime session, so the call doesn't reconnect.
  `HandoffBuilder` is chat-only. The built handoff agent owns the call's history and context providers; participants
  bring their instructions, tools, and voice, and share one client, so a call keeps one provider and model. See the
  [handoff sample](https://github.com/microsoft/agent-framework/blob/main/python/samples/02-agents/providers/foundry/voice_live_handoff.py).
- **History.** Pass an `AgentSession` to keep calls. History is text, reaches context providers when the call closes,
  and is replayed into the next call on that session. A process that dies mid-call reports nothing, so record from the
  update stream when each exchange must be stored as it happens. See the
  [history sample](https://github.com/microsoft/agent-framework/blob/main/python/samples/02-agents/providers/foundry/voice_live_history.py).
- **Context.** Context providers add context once, when the call starts. Supply context that changes during the call
  through tools or `update_session()`.
- **Transcription.** History and guardrails see what the caller said only when input transcription is on.
- **Guardrails.** Use conversation middleware: its hooks see what the application sends, each update it reads, and the
  close. Agent and chat middleware don't apply to a live call; function middleware still wraps each tool the agent
  runs. Send hooks see caller speech only as audio, so they can filter text the application sends but not words the
  caller speaks. Audio can reach the application before its transcript, so a transcript check stops the rest of a
  reply, not audio already played. History stores replies as the provider sent them, before update hooks change them.
  See the
  [guardrail sample](https://github.com/microsoft/agent-framework/blob/main/python/samples/02-agents/providers/foundry/voice_live_guardrail.py).
- **MCP.** Provider-run MCP tools can't ask for approval in a live call, so run an MCP server that needs approvals as a
  local MCP tool.

## Validation

Drive a call with text before wiring audio; the samples send text and save the spoken reply, so they need no
microphone. Then exercise barge-in, tool calls, handoffs, and history at close where the application uses them.

## Learn more

- [OpenAI realtime sample](https://github.com/microsoft/agent-framework/blob/main/python/samples/02-agents/providers/openai/realtime_client_with_function_tools.py)
- [Voice Live samples](https://github.com/microsoft/agent-framework/blob/main/python/samples/02-agents/providers/foundry/README.md#voiceliveclient-samples)
- [Foundry voice agent sample](https://github.com/microsoft/agent-framework/blob/main/python/samples/02-agents/providers/foundry/foundry_voice_agent.py)
