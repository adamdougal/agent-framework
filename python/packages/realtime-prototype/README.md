# PROTOTYPE: the accelerator's voice loop on the extended realtime API

> **Throwaway. Never merged.** Lives only on the `prototype/realtime-voice-loop` branch of
> [adamdougal/agent-framework](https://github.com/adamdougal/agent-framework), as the primary source for
> [Prototype the accelerator's voice loop on the extended API](https://github.com/adamdougal/agent-framework/issues/10).

**Question:** can the AI Contact Centre accelerator's core voice loop (Voice Live, barge-in, one tool call, one
in-session handoff, content-filter fallback speech) run on a rough prototype of the extended ADR 0035 API instead of
Semantic Kernel, and what breaks or feels wrong?

**Answer and findings:** [FINDINGS.md](FINDINGS.md).

## What's here

| Module | Stands in for |
| --- | --- |
| `_core.py` | Core additions: event kinds, `RealtimeEvent`, errors, options, conversation middleware, and the in-memory core patches listed in `CORE_CHANGES` |
| `_conversation.py` | `RealtimeConversation`: receive loop, serialised controls, tool batches, history ledger, per-exchange reporting, `handoff()`, spans, close sequence |
| `_agent.py` | `RealtimeAgent`, `FoundryVoiceAgent` (no `run()`: no #10 check needs it) |
| `_ga_session.py` | `agent-framework-openai`: the GA base session and `OpenAIRealtimeClient` with Azure routing |
| `_azure.py` | `agent-framework-foundry[realtime]`: `VoiceLiveClient` (`2026-07-15`) and `FoundryVoiceAgentClient` |
| `_handoff.py` | `agent-framework-orchestrations`: `RealtimeHandoffBuilder` |
| `accelerator.py` | The accelerator's agents, voices, topology, tools, suffix, fallback and app loop, ported from SK |
| `scenarios.py` | One live scenario per check in #10's Inputs; results in `results/` |
| `server.py`, `static/index.html` | The debugger: manual calls and the scenarios in a browser |
| `harness.py`, `make_clips.py`, `orders_mcp.py` | Scripted caller and speaker, TTS caller clips, a tiny orders MCP server |

## Run it

From `python/` (the repo pins uv 0.13.5; `uvx --from uv==0.13.5 uv ...` if yours is older):

```bash
uv sync --package agent-framework-realtime-prototype
cd packages/realtime-prototype
cp .env.example .env   # fill in your Azure endpoints; auth is the Azure CLI login (az login)
```

- **Debugger:** `uv run python -m agent_framework_realtime_prototype.server`, then open http://localhost:8765.
  Pick a provider, start a call with your microphone or with TTS clips only, and drive the app controls. The
  Scenarios tab runs each scenario live with its checks.
- **All scenarios:** `uv run python -m agent_framework_realtime_prototype.scenarios` (or name some, such as `core-loop`).
- **Clips:** `uv run python -m agent_framework_realtime_prototype.make_clips` regenerates missing caller clips.

The content-filter scenario sends a jailbreak clip, which raises an Azure AI Content Safety Prompt Shields alert on the
subscription.
