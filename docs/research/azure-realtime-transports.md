# Azure realtime transports: raw websocket or SDK for Voice Live and Foundry voice agents

Findings for [Azure provider clients: design and package placement](https://github.com/adamdougal/agent-framework/issues/8),
on the map [Python realtime agents in Agent Framework](https://github.com/adamdougal/agent-framework/issues/1).
Gathered on 2026-11-05 with offline experiments.

This file collects evidence and makes no decision. The ticket decided to use both SDKs as transports only.

**Pinned sources**

- `azure-ai-voicelive` 1.3.1, the latest on PyPI (2026-10-23). The 1.0.0, 1.1.0, 1.2.0 and 1.3.0 wheels were used for
  diffs. Installed-package paths are cited as `aio/_patch.py:N`, meaning
  `azure/ai/voicelive/aio/_patch.py` in the 1.3.1 wheel. The same tree is at
  `https://github.com/Azure/azure-sdk-for-python/tree/azure-ai-voicelive_1.3.1/sdk/voicelive/azure-ai-voicelive`.
- `azure-ai-projects` 2.10.0, the latest (2026-10-30). The 2.7.0, 2.8.0 and 2.9.0 wheels were used for diffs. Paths are
  cited as `projects aio/_realtime.py:N`, meaning `azure/ai/projects/aio/_realtime.py` in the 2.10.0 wheel.
- `aiohttp` 3.14.5 (the version AF's `uv.lock` resolves), `websockets` 17.3 and `openai` 3.42.1, installed in the
  throwaway venvs.
- Agent Framework `main` at `f4bdd1dc9` (2026-11-03). Paths are relative to `python/` unless they start with `docs/`.
- Prior research: `origin/research/azure-realtime-protocols` (`183a71d5b`) and `origin/research/realtime-wire-probes`
  (`62f34492e`).

**Method.** All experiments were offline. A local `aiohttp` websocket server ran on `127.0.0.1` (plain `ws://`, plus
TLS with a self-signed certificate for `azure-ai-projects`, which only accepts `wss://`). Credentials were dummies and no
Azure endpoint was contacted. The scripts and their output are kept locally and not published; the appendix's file names identify them.

---

## 1. Evidence table: Voice Live, raw websocket vs `azure-ai-voicelive`

| # | Argument | Status | Evidence |
| --- | --- | --- | --- |
| 1 | *Prior, for raw:* the SDK brings new dependencies (`azure-core`, `isodate`, `aiohttp`). | **Void** for a client in `agent-framework-foundry` or in any package that depends on it. **Partly holds** for a standalone package that depends only on core and openai. | The SDK requires `isodate>=0.6.1`, `azure-core>=1.37.0`, `typing-extensions>=4.6.0`, plus `aiohttp>=3.9,<4` through `[aiohttp]` (PyPI metadata). `agent-framework-foundry` already has all of them: `azure-ai-projects>=2.2.0` requires `azure-core>=1.37.0` and `isodate`, `azure-ai-inference` 1.0.0b9 requires `azure-core` and `isodate`, and `aiohttp` is a direct dependency (`packages/foundry/pyproject.toml:28`). `uv pip compile` of `agent-framework-foundry==1.16.0`, with and without `azure-ai-voicelive[aiohttp]==1.3.1`, adds exactly one distribution: `azure-ai-voicelive`, a 161 KB pure-Python wheel (`resolve/`). **Correction to the prior table:** raw isn't dependency-free. `agent-framework-openai` depends on plain `openai`, without `[realtime]` (`packages/openai/pyproject.toml:27`, `uv.lock`), so `websockets` isn't in AF's runtime closure today. `openai[realtime]` pins `websockets>=13,<16` in every version checked (2.25.0, 3.36.1 and 3.46.0). |
| 2 | *Prior, for raw:* the SDK lags the service. | **Void at the transport level.** | `query` is merged after the SDK's own parameters (`aio/_patch.py:867`), so `profile`, `foundry-resource-override`, `api-version` and `x-ms-client-sdk` can all be set or overridden (verified). `headers` are merged over the auth headers (`:791`), and `credential_scopes` is configurable (`:698`). `connection_options.vendor_options` forwards any `ws_connect` keyword; a `realtime` subprotocol negotiated through it (verified). The one fixed element is the path suffix `/voice-live/realtime` (`:874`). Typed options lag the same way in both designs, and both can send raw dicts. Two pitfalls: the named `foundry_resource_override=` keyword raises `ValueError` outside agent mode (`:100-124`, verified), and unknown keywords such as `profile=` are silently ignored (verified), so BYOM must go through `query`. |
| 3 | *Prior, for raw:* the SDK needs a second mapper for its typed events. | **Void.** | `recv_bytes()` returns each text or binary frame byte for byte (`:524-557`, verified). `send(Mapping)` sends `json.dumps(dict(event))` unchanged (`:583`). The local server received byte-identical JSON, with no `session.type` injected and no validation. `as_dict()` equalled the wire JSON for every probe case (see A.17): known event, unknown type, extra fields, unknown nested `item.type`, wrong field types and audio deltas. Unknown types deserialise to the base `ServerEvent`, and iteration doesn't raise. Caveats: attribute access to unknown fields raises `AttributeError` (mapping access works), and non-object JSON makes `recv()` raise an uncaught `AttributeError`. |
| 4 | *Prior, for raw:* the default API version moves. | **Void** if AF always passes `api_version`. | `api_version` becomes the `api-version` query parameter (`:843`), and `query` can still override it. Across the 1.0.0, 1.1.0, 1.2.0, 1.3.0 and 1.3.1 wheels, the transport API didn't change apart from defaults and additive changes: `connect`, `send`, `recv_bytes`, `close`, header and query precedence, the path and the default scope (A.18). The default was `2025-10-01` in 1.0.0 and 1.1.0, `2026-04-10` in 1.2.0, and `2026-07-15` in 1.3.0 and 1.3.1. Clients that don't pin are exposed: LiveKit calls `connect()` without `api_version` (E.4). |
| 5 | *Prior, for raw:* the SDK can't reach the Foundry voice-agent route. | **Holds**, with a smaller consequence than the prior research implied. | The path suffix is hard-coded (`:874`). An endpoint path prefix is kept, so `…/agents/<a>/endpoint/protocols/voice` can't be built, even though the subprotocol and headers can be supplied. Foundry voice agents do have an SDK client, in a package AF already depends on (table 2). So the real choice is two aiohttp-based SDK transports, or one transport owned by AF. |
| 6 | *Prior, for raw:* the SDK needs version caps because its minor releases break. | **Weakened.** | Every breaking change since 1.0.0 is in the typed models or the default API version (section B). The only transport behaviour change is 1.3.1's redirect rejection, a security fix. AF policy still requires a lower and upper bound on every integration dependency (`scripts/dependencies/README.md:31-34`). That's routine work: `azure-ai-projects` was bumped from `<2.3` to `<2.4`, `<2.7` and `<2.8` in 2026. |
| 7 | *Prior, for the SDK:* `VoiceLiveInstrumentor` provides telemetry. | **Weakened.** | It monkeypatches `__aenter__`, `__aexit__`, `send`, `recv` and `close`, but not `recv_bytes` (`telemetry/_voicelive_instrumentor.py:1546-1580`). It's a no-op unless `AZURE_EXPERIMENTAL_ENABLE_GENAI_TRACING=true` (`:174`). Under transport-only use it sees no received events. |
| 8 | *Prior, for the SDK:* auth is built in. | **Weakened.** | The SDK accepts `AzureKeyCredential` (sends an `api-key` header) or a token credential (a `Bearer` token), and fetches the token once on connect (`:811-826`). It doesn't accept a callable token provider. AF's openai clients take `Callable[[], str \| Awaitable[str]]` (`packages/openai/agent_framework_openai/_shared.py:49,444-469`), so the SDK needs a small adapter that implements `get_token`, `close`, `__aenter__` and `__aexit__`; a duck-typed one was detected as async (verified). Raw code would reuse AF's existing helpers. |
| 9 | *New, for the SDK:* redirect and credential hardening. | **New.** | Since 1.3.1, the SDK rejects every redirect before credentials can be forwarded (`:669-675,796-798`; CHANGELOG 1.3.1). Locally, raw `websockets` 17.3 and raw `aiohttp` 3.14.5 both followed a cross-origin 307 and **forwarded the `api-key` header**; they strip only `Authorization` (`ws_redirect_probe.out`, `aiohttp_redirect_probe.out`; `websockets/asyncio/client.py:507-516`). A raw transport that supports API keys must handle redirects itself. |
| 10 | *New, for raw:* the SDK's transport has rough edges. | **New.** All verified locally with aiohttp 3.14.5. | **Close and iteration:** the close reason is always `""` (`:542`), and iterating swallows closes (`:500`), so iterators never see the close code. **Options:** the documented `compression=True` fails ("Compress wbits must between 9 and 15") and leaks the `ClientSession` (`vl_compression_probe.out`); `compression=15` works. `handshake_timeout` and unknown option keys are ignored (`:764,772`). **Logging:** `recv_bytes()` logs every received frame, audio and transcripts included, at DEBUG (`:535,538`). The `openai` SDK logs only byte counts (`openai/resources/realtime/realtime.py:399`). **Typed helpers:** `response.create(response=Mapping)` and `conversation.item.create(item=Mapping)` raise `TypeError` for unknown keys, and the second does even for a valid `function_call_output` (`:211,337`). LiveKit users hit this (livekit/agents#4693). |
| 11 | *New, neutral:* the realtime clients would use two websocket stacks. | **New.** | The SDK uses aiohttp, with a new `ClientSession` per connection, a 30 s heartbeat and a 4 MiB limit (`:787-800`). AF's agreed OpenAI transport is the `openai` SDK over `websockets`. Their exceptions differ: `azure.ai.voicelive.aio.ConnectionClosed(code, "")` and `ConnectionError` (an `AzureError`), versus `websockets.exceptions.ConnectionClosed*`. The connection seam needs per-provider exception mapping either way. |
| 12 | *New, for the SDK:* it matches the agreed OpenAI pattern. | **New.** | `VoiceLiveConnection` exposes the three primitives AF already uses on `openai`'s `AsyncRealtimeConnection`: `send(Mapping)`, `recv_bytes()` and `close(code, reason)`. The same base-session code could drive either connection, given the per-provider exception mapping in row 11. One difference: `openai` 3.x's `send(dict)` runs `async_maybe_transform` and has a separate `send_raw()` (`openai/_websocket_connection.py:474`), while the Voice Live SDK's `send(Mapping)` is plain `json.dumps`. |
| 13 | *New, for the SDK:* AF precedent. | **New.** | AF's Azure integrations almost all use official SDKs, either Azure ones or protocol SDKs such as `openai` and `pymongo`. Two exceptions call Microsoft endpoints directly: Purview calls Microsoft Graph over httpx, and `FoundryToolbox` speaks MCP to a Foundry endpoint (section D). |
| 14 | *New, neutral:* the ecosystem is split. | **New.** | The SDK is used by LiveKit (merged 2026-09-24), microsoft/agent-framework#3071 (by szhaomsft) and microsoft/agent-framework#3821. A raw websocket is used by Pipecat (merged 2026-10-03) and the accelerator. The .NET ADR draft treats Voice Live as OpenAI-wire-compatible rather than using `Azure.AI.VoiceLive`. No AF maintainer has commented on SDK vs raw (section E). |
| 15 | *New, neutral:* performance cost. | **New.** | The typed `recv()` path adds about 12 µs per 100 ms audio delta (about 19 µs against 6.5 µs for `json.loads`), and nothing through `recv_bytes()`. Importing `azure.ai.voicelive.aio` on top of aiohttp and azure-core adds about 30 ms (section G). |

## 2. Evidence table: Foundry voice agents, raw websocket vs `azure-ai-projects[voice]`

| # | Argument | Status | Evidence |
| --- | --- | --- | --- |
| F1 | *For raw:* new dependencies. | **Void** for the async client. | `azure-ai-projects` is already a dependency. The `[voice]` extra adds `websockets>=13`, which only the sync client uses (`azure/ai/projects/_realtime.py:766`), and `aiohttp`, already a direct dependency. A `uv pip compile` diff showed it adds only `websockets` (`resolve/`). |
| F2 | *For raw:* the SDK needs a floor of `>=2.7`, which brings `openai>=3`. | **Holds** if the package floor is raised. **Weakened** by a runtime gate. | `azure-ai-projects>=2.5.0` requires `openai>=3.0.0`; 2.2.0 to 2.4.0 require `openai>=2.8.0` (PyPI). Today `agent-framework-foundry==1.16.0` plus `openai<3` resolves to `azure-ai-projects` 2.4.0 and `openai` 2.54.0. Adding `azure-ai-projects>=2.7.0` makes the set unsatisfiable (`resolve/`, verified). `openai` 3.0.0 shipped on 2026-08-12. AF's lowest-direct release probes (`scripts/dependencies/README.md:31-34`) would also move. **Weakened because:** AF already gates a feature at call time on a newer `azure-ai-projects` without raising the floor (`FoundryChatClient.get_computer_tool()` needs `>=2.3.0`, `packages/foundry/agent_framework_foundry/_chat_client.py:1008-1024`). The 2.7.x line already satisfies the current `<2.8` cap, so the cap needn't be lifted to use the client. AF's `uv.lock` already resolves 2.7.0. |
| F3 | *For raw:* churn in the preview SDK. | **Holds weakly.** | The client lives in `.beta`, and the 2.8.0 and 2.9.0 CHANGELOGs both say "All breaking changes affect preview APIs". But the async realtime module changed only additively from 2.7.0 to 2.10.0: a logger rename in 2.8, an additive `transport=` keyword in 2.9, and no change in 2.10 (wheel diffs). None of the preview APIs broken in 2.8 or 2.9 are used in AF (grep, no matches). |
| F4 | *For raw:* raw receive. | **Holds.** | There's no public raw-bytes API. `recv()` returns a typed model (a `Mapping`) or, for unknown types, a plain dict (`projects aio/_realtime.py:635-682`). `as_dict()` equalled the wire JSON in every probe case, extra fields and unknown nested types included (`proj_deser_probe.out`). The raw bytes are reachable only through the private `_connection`; `CODING_STANDARD.md:123` says private members of third-party dependencies SHOULD NOT be used without a documented reason. |
| F5 | *For the SDK:* it owns the handshake. | **New.** | The SDK builds the route (`:272-288`), the `Foundry-Features: VoiceAgents=V1Preview` header (`:67,826`), the forced `realtime` subprotocol (`:838-845`), `api-version=v1` from the client config, `x-ms-client-sdk` (`:808`) and the token scope (`https://ai.azure.com/.default` by default). `extra_query` and `extra_headers` pass through; `traceparent`, `x-agent-version-override` and `User-Agent` were verified. It enforces `wss://` (`:805`) and checks a `connection_url` override against the endpoint's host and port (`:314-337`). |
| F6 | *For the SDK:* it reuses AF's `AIProjectClient`. | **New, with a caveat.** | `FoundryAgent` already builds an `AIProjectClient` (`packages/foundry/agent_framework_foundry/_agent.py:270`), and `.beta.voice_agents.realtime.connect(agent_name=…)` reuses its endpoint, credential, API version and scopes. Caveat: the client's `user_agent` and its per-retry feature-usage policy don't reach the websocket handshake, which uses a module constant (`:73,834`). AF must pass `User-Agent` through `extra_headers`, which works (verified). |
| F7 | *Neutral:* error and close behaviour. | **New.** | A failed handshake raises the built-in `ConnectionError`, caused by `WSServerHandshakeError` (status available, no body). Closes raise `ConnectionResetError`. A close code other than 1000 or 1001 propagates out of `async for`, while a normal close ends it (verified). The close reason is dropped. There's no heartbeat default. |
| F8 | *For the SDK:* the accelerator's stated intent. | **New.** | The accelerator avoided the SDK only because Semantic Kernel caps `azure-ai-projects` below 2.5. Its ADR says to "revisit … (then use the SDK's voice client)" (section F). |
| F9 | *Neutral:* performance cost. | **New.** | Building the model adds about 11 µs per 100 ms delta (16.4 µs against 5.8 µs for `json.loads`; `proj_bench.out`). |
| F10 | *Neutral:* telemetry. | **New.** | From 2.9.0, `AIProjectInstrumentor` can patch `send`, `recv` and `close` on the realtime connection classes when an app enables it (`telemetry/_voice_agents_instrumentor.py:994-1010`). AF doesn't use it today (grep). |

## 3. The strongest single reason each way

This section frames the evidence and makes no decision.

**How much code the SDK replaces.** The accelerator's raw handshakes are small:

- Voice Live: about 24 lines (`core/voicelive_patches.py:278-301`), covering the query, URL, token and `ws_connect`.
- Foundry voice agents: about 19 lines (`core/foundry_voice_client.py:68-80,111-114`).

What a raw AF transport would still need on top: redirect handling for `api-key` (row 9), a timeout, exception mapping,
and a user agent.

**Voice Live:**

- *For the SDK:* once used as a transport, the SDK offers exactly the primitives AF's agreed seam needs. `send(Mapping)`
  is byte-identical and `recv_bytes()` returns exact frames. Every prior argument against it that the experiments could
  test turned out void: dependencies, transport lag, a second mapper, and the moving default API version. The SDK also
  supplies service-team maintenance and hardening AF would otherwise write itself, such as the 1.3.1 redirect rejection.
- *For raw:* AF wouldn't use the SDK's main asset, its typed models. In exchange it would take on a separate dependency
  and release line for about 25 lines of handshake. That dependency's transport has verified defects: the close reason
  is dropped, `compression=True` is broken and leaks, and DEBUG logging dumps full payloads. It also can't serve the
  Foundry route, which one AF transport could.

**Foundry voice agents:**

- *For the SDK:* it adds no new async dependency and reuses the `AIProjectClient` AF already builds. The service SDK
  owns the preview handshake: `Foundry-Features`, the subprotocol, `api-version` and the scope. The accelerator's own ADR
  says it would use this client if it could.
- *For raw:* the client is a `.beta` API in a package whose minor releases break preview APIs. It has no public
  raw-bytes receive, so AF would need `as_dict()` on typed models or a private attribute. It needs `>=2.7`, which means
  `openai>=3`, unless gated at runtime.

---

## A. `azure-ai-voicelive` 1.3.1 source

### A.1 Package shape

- **Async only.** The public API is `azure.ai.voicelive.aio`. The top-level `_patch.py` exports nothing, so there's no sync
  `connect`.
- **aiohttp is required in practice.** `aio/_patch.py:23-28` raises `ImportError` when `aiohttp` is missing, even though
  the dependency is the optional `[aiohttp]` extra.
- **Typed, pure Python.** The package ships `py.typed` and a 161 KB wheel. Its classifiers say Production/Stable and
  list Python 3.10 to 3.14. Whether it works on 3.15, which AF advertises, wasn't verified.
- **Release history.** There have been 17 releases since 1.0.0b1 (2025-08-29); 1.3.1 shipped on 2026-10-23.

### A.2 `connect()`

The signature is at `aio/_patch.py:895-1017`:

```python
def connect(*, credential: AzureKeyCredential | AsyncTokenCredential, endpoint: str,
            api_version: str = "2026-07-15", model: str | None = None,
            agent_name=None, project_name=None, agent_version=None, conversation_id=None,
            authentication_identity_client_id=None, foundry_resource_override=None,
            query: Mapping[str, Any] | None = None, headers: Mapping[str, Any] | None = None,
            connection_options: WebsocketConnectionOptions | None = None,
            credential_scopes: str | Sequence[str] | None = None, **kwargs) -> AbstractAsyncContextManager[VoiceLiveConnection]
```

- `connect()` returns `_VoiceLiveConnectionManager` (`:678`); the connection opens in `__aenter__` (`:776-809`).
- `credential` is required.
- Unknown keywords are ignored ("Unknown values are ignored"). `connect(profile="byom-…")` was accepted and dropped.

### A.3 URL building (`_prepare_url`, `:828-876`)

- **Scheme.** `https` maps to `wss` and `http` to `ws`; any other scheme is kept, so `wss://` endpoints work.
- **Path.** The endpoint's path is kept, then `/voice-live/realtime` is appended (`:874`).
- **Query, in order:**
  1. `api-version` and `x-ms-client-sdk` (`:843-844`)
  2. `model`
  3. the agent-mode parameters
  4. `params.update(query)` (`:867`)
  5. any query parameters already on the endpoint URL, only if not yet set (`:869-872`)
- **BYOM.** `profile` and `foundry-resource-override` work through `query`. A run with
  `query={"profile": "byom-azure-openai-realtime", "foundry-resource-override": "other-resource"}` reached the server
  intact (`vl_transport_probe.out` §1).
- **Overrides.** `query` can also override `api-version` and `x-ms-client-sdk` (`vl_transport_probe.out` §7).
- **The `foundry_resource_override=` keyword** belongs to agent-integration mode. Passed without
  `agent_name`/`project_name`, it raises `ValueError` (`:100-124`; `vl_transport_probe.out` §6).

### A.4 Auth (`_get_auth_headers`, `:811-826`)

- **API key.** `AzureKeyCredential` becomes an `api-key` header (`:819`).
- **Entra token.** An `AsyncTokenCredential` gives `await get_token(*scopes)`, sent as an `Authorization` `Bearer`
  token (`:822`). Any other object gets a synchronous `get_token(...)` (`:824`), which blocks the event loop.
- **Scope.** The default is `["https://ai.azure.com/.default"]` (`:698`). `credential_scopes` (a string or a sequence)
  overrides it; `https://cognitiveservices.azure.com/.default` was verified, and the wire probes showed Voice Live
  accepts both scopes.
- **When the token is fetched.** Once, inside `__aenter__`, and never refreshed; tokens matter only at the handshake.
  Verified: zero calls before entering, one after (`vl_transport_probe.out` §3).
- **Override.** Because `headers` are merged over the auth headers (`:791`), a caller could supply its own
  `Authorization`. An `AzureKeyCredential` would still add `api-key`.

### A.5 Custom headers, User-Agent and feature-usage telemetry

- **Custom headers.** `headers` reach the handshake unchanged. `traceparent`, `x-ms-custom` and a custom `User-Agent`
  were verified (`vl_transport_probe.out` §1).
- **User-Agent.** Since 1.3.1, the SDK sets `User-Agent: azsdk-python-ai-voicelive/1.3.1 Python/… (<platform>)` only when
  the caller supplied none (`:792-793`). Before 1.3.1 it didn't set one at all.
- **AF telemetry.** AF's user agent comes from `get_user_agent()` and `prepend_agent_framework_to_user_agent`
  (`packages/core/agent_framework/_telemetry.py:182`). Its feature token is added only for approved origins
  (`packages/openai/agent_framework_openai/_feature_usage.py`; the foundry origins are in
  `packages/foundry/agent_framework_foundry/_feature_usage.py:33-37`). Under the SDK, AF would compute the header at
  connect time and pass it in `headers`. A raw transport does the same thing.

### A.6 The `x-ms-client-sdk` query parameter

- Added in 1.3.1. It's always set to the SDK's user agent, including the OS and platform string (`:844`), and can be
  overridden through `query`.
- The CHANGELOG says the SDK "now identifies Voice Live WebSocket connections" with it.
- How the service uses it is **unverified**. Raw connections from the accelerator and the wire probes connected without it.

### A.7 Connection options (`WebsocketConnectionOptions`, `:615-666`; mapping at `:709-774`)

- **Defaults** (`:787-788`): `max_msg_size=4 MiB` and `heartbeat=30`. aiohttp then closes the connection if no pong
  arrives within half the heartbeat.
- **Compression** is off by default (aiohttp's `compress=0`). `compression=True` maps to `compress=-1` (`:732`), which
  aiohttp 3.14.5 rejects and which leaks the `ClientSession`. An integer such as `15` works (`vl_compression_probe.out`).
- `receive_timeout` and `close_timeout` map to `ClientWSTimeout`.
- `handshake_timeout` is "intentionally ignored" (`:764`), so callers need `asyncio.wait_for`; LiveKit does this.
- Any other unknown keys are ignored (`:772`). The 1.0.0 CHANGELOG's aliases, such as `max_size` and `ping_interval`,
  aren't honoured in 1.3.1. `max_size` was verified as ignored, and the code drops every other leftover key.
- `vendor_options` passes keywords straight to `ws_connect`. A subprotocol was verified through
  `protocols=("realtime",)` (`vl_vendor_probe.out`). Proxies need `vendor_options={"proxy": …}`, because the
  `ClientSession` it creates uses aiohttp's default `trust_env=False`. By contrast, `websockets` 17.3 defaults to
  `proxy=True`, which honours environment proxies.

### A.8 `send()` (`:559-593`)

- A `ClientEvent` model is serialised with `as_dict()`. Any other `Mapping` is sent as
  `json.dumps(dict(event), default=_json_default)` (`:583`), followed by `send_str` (`:590`).
- There's no validation and no injection: a flat Voice Live `session.update` with unknown keys, and an unknown
  `brand.new.client_event`, reached the server byte-identical to `json.dumps(...)` (`vl_transport_probe.out` §1).
- `session.update(session=Mapping)` (`:153-176`) wraps the mapping in a model through the positional path. It keeps
  unknown keys and adds no `session.type`. The verified output was `{"session": {...}, "type": "session.update"}`.
- **Pitfalls in the typed helpers.** `response.create(response=Mapping)` runs `ResponseCreateParams(**dict(response))`
  (`:211`), and `conversation.item.create(item=Mapping)` runs `ConversationRequestItem(**dict(item))` (`:337`). Both
  raise `TypeError` for keys the model doesn't declare. The base `ConversationRequestItem` doesn't declare `call_id`,
  so a valid `function_call_output` mapping fails (verified).

### A.9 Receiving (`:490-557`)

- `recv()` (`:504-522`) runs `json.loads`, then `ServerEvent._deserialize(payload, [])`. It's typed.
- `recv_bytes()` (`:524-557`) is raw. Text frames come back as UTF-8 bytes and binary frames as bytes. CLOSE, ERROR and
  CLOSED frames raise `ConnectionClosed`, and any other frame type returns `b""` with a warning.
- `__aiter__` (`:490-502`) yields typed events and ends quietly on `aiohttp.ClientError`, `ConnectionClosed` or
  `RuntimeError`.
- **Unknown event types don't raise;** they come back as the base `ServerEvent`.

### A.10 Errors and closes

- `ConnectionError` subclasses `AzureError`, and `ConnectionClosed(code, reason)` subclasses it (`:129-139`).
- **Handshake 4xx.** A failed handshake raises `azure.ai.voicelive.aio.ConnectionError("Failed to establish WebSocket
  connection: 401, message='Invalid response status', url=…")`. Its `__cause__` is `aiohttp.WSServerHandshakeError`,
  which carries the status and headers but not the body (`vl_transport_probe.out` §4). The error message includes the
  full URL with the query.
- **Redirects** are rejected with `ConnectionError("… WebSocket redirects are not supported.")` (§5).
- **Server closes.** `recv_bytes()` raises `ConnectionClosed(code=1008, reason='')` even when the server sent a reason
  (`max_config_attempts_exceeded` in the test). `reason` is hard-coded to `""` (`:542`). Whether Voice Live puts text
  in its close frames is **unverified**.
- **Parse failures** raise `ConnectionError`, which `__aiter__` doesn't swallow. JSON that isn't an object raises an
  uncaught `AttributeError`.
- **Send failures** raise `ConnectionError("Failed to send event: …")`.

### A.11 Transport and background tasks

- It's aiohttp only: each connect creates a new `aiohttp.ClientSession` with a redirect-rejecting trace config
  (`:796-798`), and `close()` closes both the websocket and the session (`:595-612`).
- A session or connector can't be injected.
- The SDK starts no tasks. aiohttp's heartbeat runs on loop timers and short-lived ping tasks
  (`aiohttp/client_ws.py:147-207`).

### A.12 Nothing happens on connect beyond the handshake

`__aenter__` sends nothing; the local server received only what the test sent. Voice Live rejects any client event
sent before the first `session.update` (wire probes), so AF's session must send that update itself whichever transport
it uses.

### A.13 Sync and async

Async only, as A.1 notes.

### A.14 Logging

- `log.debug("Connecting to %s", url)` (`:785`) logs the URL, which holds no secret: the SDK never puts a key in the
  query.
- `recv_bytes()` logs **every frame's full content** at DEBUG (`:535,538`), including base64 audio and transcripts.

### A.15 Telemetry instrumentor

- `VoiceLiveInstrumentor` is opt-in and gated by `AZURE_EXPERIMENTAL_ENABLE_GENAI_TRACING`
  (`telemetry/_voicelive_instrumentor.py:174`).
- It monkeypatches `__aenter__`, `__aexit__`, `send`, `recv` and `close` globally (`:1546-1620`), but not `recv_bytes`.

### A.16 BYOM and `api_version="v1"`

- Both are transport-level and both work through the SDK (URL-level tests).
- The wire probes found that Voice Live's undocumented `api-version=v1` speaks the GA shape. The SDK's typed
  `ServerEventType` enum is preview-shaped, which matters only to typed consumers.

### A.17 Deserialisation probes (`vl_deser_probe.out`)

| Input | Result |
| --- | --- |
| `session.created`, flat Voice Live session with unknown `turn_detection` knobs, `greeting`, `handoff` and `expires_at` | `ServerEventSessionCreated`. `as_dict()` equals the wire JSON before and after attribute access. `.session.expires_at` is a `datetime`, and `.session.turn_detection` is `AzureSemanticVad` with the unknown key kept. |
| `response.audio.delta` with 4,800 bytes of base64 | `ServerEventResponseAudioDelta`. `as_dict()` equals the wire and keeps the base64 string; `.delta` decodes lazily to `bytes`. |
| `response.done` with extra fields (`reasoning_tokens`, `phase`, top-level `x_extra`) | `as_dict()` equals the wire, and nested models keep the extra keys. |
| Unknown type `session.handoff.started` | Base `ServerEvent`; `as_dict()` equals the wire. |
| Unknown GA name `response.output_audio.delta` | Base `ServerEvent`; `as_dict()` equals the wire. `.delta` raises `AttributeError`, but `event["delta"]` works. |
| `conversation.item.created` with unknown `item.type` | `ResponseItem` base; `as_dict()` equals the wire. |
| Wrong types (`delta: 123`, `content_index: "x"`) | No validation. Attributes return the raw values, and `as_dict()` equals the wire. |
| No `type` key | Base `ServerEvent`, `type` is `None`. |
| JSON array | `AttributeError: 'list' object has no attribute 'get'` |

### A.18 Stability of the transport API, 1.0.0 to 1.3.1 (wheel diffs)

**Unchanged in every version:**

- the bodies of `recv_bytes()` and `send(Mapping | ClientEvent)`
- `headers` taking precedence over the auth headers
- `params.update(query)`
- the path suffix
- the default scope `https://ai.azure.com/.default`
- the `max_msg_size` and heartbeat defaults

**What changed:**

- **`recv()`:** switched from `ServerEvent.deserialize(payload)` to `ServerEvent._deserialize(payload, [])`.
- **1.2.0:** `credential_scopes` became an explicit keyword (it was a `kwargs` pop before), agent-mode parameters
  arrived, and a dead `ProxyPolicy` that pointed at `localhost:8888` was removed. That policy existed in 1.0.0 and
  1.1.0 but was never used for the websocket.
- **1.3.1:** added redirect rejection, `User-Agent` and `x-ms-client-sdk`.
- **Default `api_version`:** see table row 4.

---

## B. `azure-ai-voicelive` CHANGELOG since 1.0.0, classified for transport-only use

"Transport-only" means `connect()` with an explicit `api_version`, `send(dict)`, `recv_bytes()` and `close()`.

| Release | Breaking or behaviour-changing items (CHANGELOG section) | Effect on transport-only use |
| --- | --- | --- |
| 1.0.0 (2025-10-01) | Breaking: `OAIVoice`→`OpenAIVoiceName`; `ToolChoiceObject`/`ToolChoiceFunctionObject` renamed; `ResponseSession` no longer inherits `RequestSession`; `AgentConfig` removed; `EOUDetection`→`EouDetection`; content-part discriminators use enums. | None (typed models) |
| 1.1.0b1, 1.1.0 | No breaking changes (`AgentConfig` re-added). | None |
| 1.2.0b1, 1.2.0b2 | No breaking changes. | None |
| 1.2.0b3 | Bug fix: `OutputAudioFormat` values changed from hyphens to underscores. Other: `azure-core` floor raised to 1.37.0. | None. The floor matches `azure-ai-projects>=2.0`. |
| 1.2.0b4 | Breaking: Foundry agent-tool classes and enum values removed. | None |
| 1.2.0b5 | Other: default API version became `2026-01-01-preview`. | None when pinned |
| 1.2.0 (2026-05-22) | Breaking: Foundry agent-tool classes removed; `OutputAudioFormat` values; `AvatarConfig.type`→`avatar_type`. Other: default `2026-04-10`. | None when pinned |
| 1.3.0b1 (2026-05-28) | Breaking: `RequestImageContentPart.url`→`image_url`; default `2026-06-01-preview`. | None when pinned |
| 1.3.0 (2026-08-03) | Breaking: default `2026-07-15`. Bug fix: the `image_url` rename. Other: WebRTC, smart end-of-turn and `output_audio_buffer.started`/`stopped` removed. | None when pinned. Removed events still arrive raw if the service sends them. |
| 1.3.1 (2026-10-23) | Bug fix: redirects rejected. Other: `User-Agent` and `x-ms-client-sdk` identification. | Yes, but benign: a 3xx now fails the connect, and one query parameter and one header were added (both overridable). |

Source: `vl-CHANGELOG.md`, fetched from the `azure-ai-voicelive_1.3.1` tag.

---

## C. `azure-ai-projects` voice-agent realtime client (2.10.0)

### C.1 Package, extras and constraints

**Requirements of 2.10.0** (PyPI):

- `isodate>=0.6.1`
- `azure-core>=1.37.0`
- `typing-extensions>=4.11`
- `azure-identity>=1.15.0`
- `openai>=3.0.0` (true since 2.5.0)
- `azure-storage-blob>=12.15.0`

**The `[voice]` extra** (since 2.7.0) adds:

- `websockets>=13.0`, for the sync client (`azure/ai/projects/_realtime.py:766`, `websockets.sync.client.connect`)
- `aiohttp>=3.9,<4`, for the async client, imported lazily (`projects aio/_realtime.py:792`)

**Resolution experiments** (`resolve/`):

- `agent-framework-foundry==1.16.0` resolves to `azure-ai-projects` 2.7.0 and `openai` 3.42.1.
- Adding `azure-ai-projects[voice]>=2.7.0` adds only `websockets` (17.3).
- `agent-framework-foundry==1.16.0` plus `openai<3` resolves to `azure-ai-projects` 2.4.0 and `openai` 2.54.0.
- Adding `azure-ai-projects>=2.7.0` to that set is **unsatisfiable**.

**AF's cap history:**

- `<2.3`, then `<2.4` (`c21806764`, 2026-07-21)
- `<2.7` (microsoft/agent-framework#8188, "support Projects 2.6 by isolating Lab", `[BREAKING]`)
- `<2.8` (microsoft/agent-framework#8751, 2026-09-28)

The 2.7.x line works without lifting the cap.

### C.2 `connect()` signature

At `projects aio/_realtime.py:894-957`:

```python
client.beta.voice_agents.realtime.connect(*, agent_name: str, agent_session_id: str | None = None,
    transport: str | VoiceAgentTransport | None = None, structured_inputs: Mapping[str, Any] | None = None,
    connection_url: str | None = None, api_version: str | None = None, credential_scopes: list[str] | None = None,
    extra_query: Mapping[str, str] | None = None, extra_headers: Mapping[str, str] | None = None,
    **kwargs) -> AsyncBetaRealtimeConnectionManager
```

The API version, scopes, endpoint and credential come from the `AIProjectClient` configuration. The defaults are
`api_version="v1"` and scopes `["https://ai.azure.com/.default"]` (`aio/_configuration.py:46,57`). Any `**kwargs` go to
`ws_connect`, for example `ssl=False` in the tests.

### C.3 Handshake (`enter`, `:781-857`)

- **URL.** `wss://<project endpoint>/agents/<quoted name>/endpoint/protocols/voice` (`:272-288`). Only `wss://` is
  allowed (`:805`). A `connection_url` override must match the endpoint's host and port (`:314-337`).
- **Query.** `api-version` and `x-ms-client-sdk` (`:808`), then optional `agent_session_id`, `structured_input` (as JSON)
  and `extra_query` (`:816`), then `transport`.
- **Headers.** `Authorization` with a `Bearer` token from `await credential.get_token(*scopes)` (`:823`), fetched once
  per connect. `Foundry-Features: VoiceAgents=V1Preview` (`:826`), overridable through `extra_headers` (`:828`).
  `User-Agent` is set only when absent, matched case-insensitively (`:834`). There's no API-key support.
- **Subprotocol.** `protocols=("realtime",)` is forced; a caller's `protocols` is discarded (`:838-845`).
- **Local verification** (`proj_transport_probe.out` §1):
  - the server saw the path `/api/projects/p1/agents/my agent/endpoint/protocols/voice`
  - and the query `api-version=v1`, `x-ms-client-sdk`, `store=false`
  - and the headers `Authorization`, `Foundry-Features`, `traceparent`, `x-agent-version-override`, a custom
    `User-Agent` and `Sec-WebSocket-Protocol: realtime`
  - the token was requested for `https://ai.azure.com/.default`
- **What doesn't reach the handshake.** The `AIProjectClient`'s `user_agent=` and its per-retry policies (AF's
  feature-usage policy) don't apply.

### C.4 Send and receive

- **Send.** `send()` takes a model, a `Mapping` (`json.dumps(event, cls=SdkJSONEncoder)`, `:698`) or a raw JSON string,
  which is validated and then sent verbatim (`:684-706`). A raw dict reached the server byte-identical (verified).
- **Typed helpers.** They accept mappings leniently: `conversation.item.create(item=Mapping)` kept unknown keys, which
  differs from `azure-ai-voicelive`.
- **Receive.** `recv()` (`:635-682`) skips PING and PONG, `json.loads` each frame, and looks the type up in
  `_SERVER_EVENT_TYPES` (`:138`). Known types become a model (`event_cls(payload)`); unknown types come back as a
  **plain dict**.
- **Fidelity.** Models are `Mapping`s, and `as_dict()` equalled the wire JSON for a GA `response.done` with extra
  fields, a `session.updated` carrying a Voice Live voice object, an unknown nested `item.type` and malformed values
  (`proj_deser_probe.out`). `.delta` decodes base64 to bytes.
- **No raw-bytes API.**
- **Logging.** Only the event type and byte count are logged (`:673`).

### C.5 Errors and closes

- A failed handshake raises the built-in `ConnectionError("Failed to open the realtime WebSocket connection to voice
  agent '<name>' at '<url>': 403, …")`, caused by `WSServerHandshakeError` with the status (`:847-856`; verified).
  `ValueError` and `RuntimeError` propagate unwrapped.
- A close with code 1000 or 1001 raises `ConnectionResetError`, which ends `async for` quietly.
- Any other code raises `ConnectionResetError` from `_AbnormalWebSocketClosure(code)`, which `async for` re-raises
  (`:618-664`). A server close with 1011 was verified to raise.
- The close reason is dropped. There's no heartbeat by default; aiohttp's `heartbeat=None`.

### C.6 Changes in 2.8, 2.9 and 2.10 that touch the voice client

- **Async realtime module diff** (wheels):
  - 2.7.0 to 2.8.0: logger name only.
  - 2.8.0 to 2.9.0: an additive `transport=` keyword.
  - 2.9.0 to 2.10.0: identical.
  - The `_SERVER_EVENT_TYPES` map didn't change.
- **2.8.0 breaking changes:** Agent Optimization, data-generation and evaluator-generation preview APIs. None touch
  voice.
- **2.9.0 breaking changes:**
  - Teams Phone telephony renames.
  - `VoiceAgentRealtimeResponseBase` and `VoiceResponseBase` removed, with their properties moved onto
    `VoiceAgentRealtimeResponse` and `VoiceResponse`. `RealtimeServerEventResponseDone.response` is typed
    `VoiceAgentRealtimeResponse`, so this affects typed consumers only, not `as_dict()` or dicts.
- **2.9.0 additions:** experimental realtime tracing in `AIProjectInstrumentor`, and a sample that logs through the
  `azure.ai.projects.realtime` logger.
- **2.10.0:** additions only, such as soundscape, a profanity filter and `AZURE_SPEECH_STREAMING`. No breaking changes.
- **AF usage of the changed APIs:** a grep of the AF repo for the renamed or removed 2.8 and 2.9 APIs found no matches.

Source: `proj-CHANGELOG.md`, fetched from the `azure-ai-projects_2.10.0` tag.

---

## D. AF precedent: official SDK or direct HTTP?

**Through official SDKs** (main):

| Package | SDK | Citation |
| --- | --- | --- |
| `azure-ai-search` | `azure-search-documents` | `_context_provider.py:34-35` |
| `azure-cosmos` | `azure-cosmos` | `_checkpoint_storage.py:22-23` |
| `azure-contentunderstanding` | `azure-ai-contentunderstanding` | `_context_provider.py:32-33` |
| `foundry` | `azure-ai-projects` `AIProjectClient` | `_chat_client.py:27,249`; `_agent.py:38,270` |
| `foundry` (embeddings) | `azure-ai-inference` | `_embedding_client.py:27` |
| `foundry-hosting` | `azure-ai-agentserver-*` | `pyproject.toml` |
| `azure-cosmos-memory` | `azure-cosmos-agent-memory` | `pyproject.toml` |
| `azure-documentdb` | `pymongo` (protocol SDK for an Azure service) | `pyproject.toml` |
| `openai` (Azure OpenAI) | `openai` SDK | — |
| `foundry` (Foundry Responses) | `openai` SDK obtained from `AIProjectClient.get_openai_client()` | `_chat_client.py:257`; `_agent.py:280` |

So "official SDK" in practice includes protocol SDKs from other vendors.

**Direct calls to Microsoft or Azure endpoints:**

- **`agent-framework-purview`** calls Microsoft Graph's `dataSecurityAndGovernance` endpoints with raw `httpx` and an
  azure-core credential (`purview/agent_framework_purview/_client.py:13,63-65,108,131,150`), not `msgraph-sdk`. The
  package docs give no reason. Whether `msgraph-sdk` covers these endpoints wasn't checked.
- **`agent-framework-foundry-hosting`'s `FoundryToolbox`** speaks MCP to a Foundry endpoint,
  `{project}/toolboxes/{name}/mcp?api-version=v1`. It uses AF's own MCP client with an `httpx2` auth hook that adds a
  bearer token and `Foundry-Features` (`foundry_hosting/agent_framework_foundry_hosting/_toolbox.py:15,48,91,109-160`).
  That's a protocol client plus Azure auth, rather than `azure-ai-projects`. Its docstring explains the wrapper (`:161-185`).

**What `agent-framework-foundry` uses `aiohttp` and `httpx` for:**

- Neither is imported anywhere under `packages/foundry`, code or tests.
- **aiohttp** was added in microsoft/agent-framework#6567 (2026-06-18) because `azure.ai.projects.aio.AIProjectClient` needs azure-core's async
  transport, and azure-core only exposes aiohttp through its `aio` extra. A minimal install failed with
  `ModuleNotFoundError: No module named 'aiohttp'` (microsoft/agent-framework#6566). `azure-ai-search` and `azure-contentunderstanding` also
  declare aiohttp without importing it, probably for the same reason (inferred, not verified).
- **httpx** arrived in the 1.17.0 release bump (`4507512f9`, 2026-09-03), when foundry tests imported it
  (`tests/foundry/test_foundry_agent.py:15` at that commit). Current foundry code and tests don't import it, so it looks
  **unused** today: a design smell to raise separately.

**AF dependency policy:**

- Every integration needs a lower and an upper bound, probed with `lowest-direct` and `highest` resolution
  (`scripts/dependencies/README.md:31-34`).
- `CODING_STANDARD.md:123` says private members of third-party dependencies SHOULD NOT be used without a documented
  reason.
- No AF document says "always wrap the official SDK". ADR 0011 builds its create and get agent APIs on the underlying
  SDK client.

---

## E. Upstream and ecosystem signals

### E.1 Agent Framework

**microsoft/agent-framework#3821**, "Python: Add realtime voice agents with OpenAI, Azure OpenAI and Voice Live clients":

- Opened by adamdougal on 2026-02-10; closed unmerged on 2026-06-01 ("no recent activity").
- It used the SDK for every provider:
  - Voice Live: `azure-ai-voicelive[aiohttp]>=1.0.0` with typed iteration (`async for event in self._connection`)
  - OpenAI: `openai`'s `realtime.connect`
  - Azure OpenAI: `AsyncAzureOpenAI.realtime.connect`
- The only human reviewer, alliscode, commented on design (`BaseAgent` inheritance, duplication), not on transport.
- The bot noted an `api_version` stored but never passed (`discussion_r2790024547`).

**microsoft/agent-framework#3071**, "Python: add voice live agent":

- Opened by szhaomsft on 2026-01-04; closed on 2026-02-24.
- Its dependencies were `azure-ai-voicelive`, `azure-identity` and `azure-core`, with typed iteration over the
  connection.
- The maintainer, eavanvalkenburg, closed it to "first decide how we want to abstract this as a whole" (a broader
  design), not over the SDK.

**microsoft/agent-framework#6831** (ADR 0035, "Python: Add realtime agent ADR"):

- Opened by eavanvalkenburg on 2026-06-30; closed on 2026-09-09, "until we pick it up".
- Its 25 review comments, from TaoChenOSU, moonbox3, bots and the author's own replies, cover I/O shape, close and
  finalisation, truncation, errors, backpressure, handoffs and middleware. None discuss provider SDKs or raw transport.
- The ADR assumes provider-SDK events: "provider SDK event → RealtimeEvent", and `raw_representation` "points to the
  provider SDK object". It says only "Core must not take an OpenAI SDK dependency".

**Other AF material:**

- microsoft/agent-framework#728 (the original RealtimeAgent issue) has no SDK discussion.
- The WIP ADR `00XX-realtime-agents.md` on `eavanvalkenburg/agent-framework@voice_agents` asks for "Day 1 support for …
  Azure Voice Live", with no transport preference.

### E.2 .NET Agent Framework and Microsoft.Extensions.AI

- **No Voice Live implementation in .NET AF.** AF `main` has no Voice Live or `IRealtimeClient` code under `dotnet/`
  (grep).
- **The .NET ADR draft, `0030-realtime-ai-agent.md`** (`rogerbarreto/agent-framework-public@features/realtime-agent`,
  commit `864fdc3b7`, 2026-06-25; tracked by microsoft/agent-framework#6716, open):
  - Only `Microsoft.Extensions.AI.OpenAI` ships a released `IRealtimeClient` adapter.
  - "OpenAI-wire-compatible providers (Azure VoiceLive, xAI) reuse the OpenAI realtime wire format", which implies the
    OpenAI adapter rather than `Azure.AI.VoiceLive`.
  - The wire probes show Voice Live `2026-07-15` rejects the GA session shape. Whether MEAI's OpenAI adapter works
    against Voice Live is **unverified**.
- **`Azure.AI.VoiceLive`.** NuGet's latest is 1.2.1. `azure-sdk-for-net` `main` has
  `sdk/voicelive/Azure.AI.VoiceLive/src/Azure.AI.VoiceLive.csproj` at `1.3.0-beta.1`, referencing only `Azure.Core`. It
  has no MEAI dependency, so it can't implement `IRealtimeClient`.

### E.3 Semantic Kernel Python

- **No Voice Live connector.** Upstream `main` (`df1cb33d0`, 2026-11-05) has `connectors/ai/open_ai/services/` with
  `open_ai_realtime.py`, `azure_realtime.py` and `_open_ai_realtime.py` only. GitHub code search finds no
  `voicelive`/`voice-live` in the repo, and no PR mentions Voice Live.
- **SK's Azure realtime** uses the `openai` SDK (prior research).
- **Adam's local branch `dougal/realtime-agents`** adds two commits on top of `3b61c786c`: "Initial implementation of
  realtime agents" and "Add function calling" (2026-01-22). They add `agents/realtime` (`RealtimeAgent`,
  `RealtimeHandoffOrchestration`) and no Voice Live code (`git grep`).

### E.4 Third-party frameworks

**LiveKit Agents** uses the SDK:

- `livekit-plugins-azure` has a `[realtime]` extra with `azure-ai-voicelive[aiohttp]>=1.2.0` (no upper bound) and
  `azure-identity`.
- `realtime/realtime_model.py` imports `azure.ai.voicelive.aio.connect` and the typed models, iterates typed events, and
  calls `connect(endpoint=…, credential=…, model=…)` **without `api_version`**. It has no BYOM `profile` support.
- It was added in livekit/agents#4693 (merged 2026-09-24).
- In that PR, the author explained Voice Live "is a completely different service with its own SDK and protocol". A user
  reported `conversation.item.create()` failing with raw dicts, as in A.8.
- LiveKit's own OpenAI realtime uses a raw aiohttp `ws_connect`
  (`livekit-agents/livekit/agents/llm/_realtime/openai.py:1113`).

**Pipecat** uses a raw websocket:

- `AzureVoiceLiveLLMService` (pipecat-ai/pipecat#5718, merged 2026-10-03) uses `websockets.asyncio.client.connect` with
  `api-key` or `Bearer` auth from its own token provider, and `DEFAULT_API_VERSION = "2026-07-15"`
  (`services/azure/voice_live/llm.py:73,761-779`).
- This matches Pipecat's raw-websocket OpenAI realtime. The PR has no SDK discussion.

---

## F. The accelerator (`~/workspace/ai-contact-centre-solution-accelerator`, branch `docs/readme-demo-video`, commit `c8032aa`)

**Voice Live** (`core/voicelive_patches.py`):

- It subclasses Semantic Kernel's Azure realtime client.
- It opens a raw `websockets` connection to `<endpoint>/voice-live/realtime?api-version=2025-10-01&model=…`, adding
  `profile` and `foundry-resource-override` for BYOM, with `Authorization: Bearer` and SK's user agent
  (`:245-304`). It then wraps the socket in `openai`'s `AsyncRealtimeConnection`.
- It renames `response.audio.*` to the GA names (`:306-…`) and sends `session.update` as a raw dict, so the openai SDK
  doesn't inject `session.type` (`:436-476`).
- The code doesn't explain why it avoided `azure-ai-voicelive`; the design is driven by staying inside SK.
- The accelerator's framework ADR (`docs/adrs/2026-01-28-Use-Semantic-Kernel-For-Agentic-Framework.md:152-162`) weighed
  the "Azure Voice Live SDK" as a framework alternative. Pros: GA, native Voice Live and ACS support. Cons: "significant
  effort to implement all required features", tying the accelerator to Voice Live, and a small community. It didn't
  weigh the SDK as a transport.

**Foundry voice agents** (`core/foundry_voice_client.py:1-6,62-113`):

- It opens a raw `websockets` connection with the `realtime` subprotocol and `Bearer` plus `Foundry-Features` headers,
  injects W3C `traceparent` (trace context only, no baggage), and pins `x-agent-version-override` as a query parameter.
- **Stated rationale:** "Why a subclass: the SDK's voice-agent client needs `azure-ai-projects>=2.7`, but Semantic
  Kernel caps it below 2.5 … Remove it when Semantic Kernel lifts the cap (then use the SDK's voice client), or when
  voice moves to Agent Framework."
- The same rationale appears in the ADR (`docs/adrs/2026-10-21-Support-Foundry-Voice-Agents-For-Voice.md:23,30,49`) and
  the design doc (`docs/superpowers/specs/2026-10-21-foundry-voice-agents-design.md:88,196`), which adds that
  "`agent-framework-foundry` caps it below 2.8. The lock is on 2.4."

---

## G. Microbenchmark (`vl_bench.out`, `proj_bench.out`)

Python 3.12.12 on arm64, best of 5 × 20,000 iterations, per event, for a PCM16 24 kHz audio delta.

| Payload | `json.loads` | `json.loads` + `b64decode` | Voice Live SDK `recv()` path | + `.delta` (bytes) | + `as_dict()` |
| --- | --- | --- | --- | --- | --- |
| 20 ms (960 B) | 2.5 µs | 4.0 µs | 14.2 µs | 18.5 µs | 18.9 µs |
| 100 ms (4,800 B) | 6.5 µs | 13.2 µs | 19.0 µs | 28.4 µs | 23.4 µs |
| 500 ms (24,000 B) | 27.3 µs | 61.4 µs | 39.5 µs | 78.2 µs | 43.6 µs |

- **`azure-ai-projects`** (100 ms delta): `json.loads` 5.8 µs; `recv()` model construction 16.4 µs; plus `as_dict()`,
  21.0 µs.
- **Overhead.** It's about 10 to 12 µs per event, under 1 ms a second at 50 deltas a second, and zero through
  `recv_bytes()`.
- **Import cost.** `azure.ai.voicelive.aio` adds about 30 ms to an import of aiohttp and azure-core.

---

## Corrections to the prior research

1. **"`websockets` already comes with `openai[realtime]`."** AF depends on plain `openai`, so `websockets` isn't in
   AF's runtime closure. `openai[realtime]` would also cap it below 16, while AF's lock currently has 16.1.1 through
   other packages.
2. **"New dependencies: `azure-core`, `isodate`, `aiohttp`."** All three are already in `agent-framework-foundry`'s
   closure.
3. **"Using the SDK client means a 2.7 floor and lifting the cap."** The 2.7.x line satisfies the existing `<2.8` cap,
   and a runtime gate avoids the floor.
4. **"The SDK … types events differently from `openai`, so a second mapper."** `recv_bytes()` makes that moot.

## Unverified or open

- **Service behaviour (no live calls):**
  - whether Voice Live sends close-frame reasons, redirects, or reads `x-ms-client-sdk`
  - whether aiohttp's 30 s heartbeat with a 15 s pong timeout ever trips against Voice Live
- **SDK support:** whether `azure-ai-voicelive` runs on Python 3.15.
- **Earlier aiohttp:** whether `compression=True` worked with older aiohttp versions.
- **.NET:** whether MEAI's OpenAI realtime adapter works against Voice Live.
- **Purview:** why it uses raw httpx for Graph.
- **`agent-framework-foundry`:** whether its `httpx` dependency is needed by anything at runtime (no importer was found).

## Appendix: experiment files (kept locally, not published)

- **Venvs:** `venv-vl` (`azure-ai-voicelive[aiohttp]==1.3.1`) and `venv-proj` (`azure-ai-projects[voice]==2.10.0`).
  `wheels/` holds historical versions for the diffs.
- **Voice Live probes:**
  - `vl_deser_probe.py` / `.out`: A.17
  - `vl_transport_probe.py` / `.out`: URL, headers, raw send, `recv_bytes`, closes, auth, 4xx, redirects, keywords
  - `vl_vendor_probe.py` / `.out`: subprotocol through `vendor_options`
  - `vl_compression_probe.py` / `.out`
- **Foundry probes:** `proj_transport_probe.py` / `.out` (TLS server; regenerate its key with
  `openssl req -x509 -newkey rsa:2048 -nodes -keyout local-key.pem -out local-cert.pem -days 2 -subj "/CN=127.0.0.1" -addext "subjectAltName=IP:127.0.0.1"`)
  and `proj_deser_probe.py` / `.out`.
- **Redirect probes:** `ws_redirect_probe.py` / `.out` and `aiohttp_redirect_probe.py` / `.out`.
- **Benchmarks:** `vl_bench.py` / `.out` and `proj_bench.py` / `.out`.
- **Resolution:** `resolve/*.in` and `*.txt` (`uv pip compile` results).
- **Downloaded sources:** `vl-CHANGELOG.md`, `proj-CHANGELOG.md`, and `gh/` (PR comment dumps, ADR copies, LiveKit and
  Pipecat sources).
