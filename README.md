# pi-mqtt-swarm

Coordinate a swarm of [pi](https://pi.dev) coding agents over **MQTT**.

Each pi TUI instance loads this extension and joins a swarm. A central
orchestrator (any MQTT client) can discover agents, stream work into them,
interrupt them, watch their output and status, change their model, reset their
context, or reload them — all while every agent keeps its full interactive TUI.

Agents also share a **message board** so cross-cutting facts ("I renamed folder
`x` to `y`") propagate to peers that need them.

---

## Why

pi has no built-in sub-agents or orchestration. This extension provides a thin
MQTT control/▶data plane so you can run a fleet of real, watchable pi TUIs and
drive them centrally. MQTT handles delivery, fan-out, retained state and
liveness; the extension handles idle-gated queueing and the agent-facing tools.

## Install

Requires an MQTT broker (e.g. [Mosquitto](https://mosquitto.org/)).

```bash
# As a pi package (user scope)
pi install git:github.com/luumo-factory/pi-mqtt-swarm

# Or load locally during development
pi -e /path/to/pi-mqtt-swarm/index.ts --swarm-name coder-1
```

When installed as a package, declare the broker/name via env or the
`--swarm-name` flag at launch.

## Launch an agent

```bash
PI_SWARM_BROKER=mqtt://127.0.0.1:1883 pi --swarm-name coder-1
PI_SWARM_BROKER=mqtt://127.0.0.1:1883 pi --swarm-name reviewer
```

| Setting | Source | Default | Purpose |
|---------|--------|---------|---------|
| name | `--swarm-name` flag / `PI_SWARM_NAME` / pi `--name` | `agent-<pid>` | Human label; also names the session. Slugified into the agent id used in topics. Resolution priority: `--swarm-name` > `PI_SWARM_NAME` > pi session name (`--name`) > default. |
| broker | `PI_SWARM_BROKER` / `MQTT_URL` | `mqtt://127.0.0.1:1883` | MQTT broker URL. |
| namespace | `PI_SWARM_NS` | `swarm` | Root of all topics. |
| group | `PI_SWARM_GROUP` | `red` | Initial colour-coded group; selects the board topic the agent binds to. Changeable at runtime via the `set_group` control action. |

> Give each agent a **unique** name — the id (slug of the name) is used in
> topic paths, so duplicate names collide.

## Topic map

`NS` = namespace (default `swarm`), `ID` = slugified agent id.

`NS` = namespace (default `swarm`), `ID` = slugified agent id. There are two
planes: a **work/data plane** (`in` / `interrupt` / `out`) and a dedicated
**control plane** (`control/in` / `control/out`).

| Topic | Dir | Retained | Payload |
|-------|-----|----------|---------|
| `NS/registry/ID` | agent → all | ✅ + LWT | `{ id, name, status, model, availableModels, extensions, tools, pid, cwd, startedAt, ts }` |
| `NS/agents/ID/in` | orch → agent | – | `{ text }` or raw string — **queued, delivered when idle** |
| `NS/agents/ID/interrupt` | orch → agent | – | `{ text }` or raw string — urgent message **injected immediately** (steers the turn) |
| `NS/agents/ID/out` | agent → orch | – | work event stream (locally-typed user input `{ type:"user_input", text, source }`, agent/turn summaries, session reset/reload) |
| `NS/agents/ID/control/in` | orch → agent | – | `{ action, ... }` control commands (see below) |
| `NS/agents/ID/control/out` | agent → orch | – | control replies (acks, results, model/extension/tool state) |
| `NS/board` | any → all | ✅ | `{ seq, from:{id,name}, text, urgent, ts }` — default `red` group board |
| `NS/board/<group>` | any → all | ✅ | per-group board (`orange`…`pink`); each agent binds to its group's board only |

> **Two kinds of "interrupt":** `NS/agents/ID/interrupt` *injects* an urgent
> message into the running turn (steering). The control action `abort` (below)
> *cancels* the running turn entirely (`ctx.abort()`).

`status` is one of `online` \| `busy` \| `idle` \| `offline`. The broker
publishes `offline` automatically via MQTT Last-Will if an agent dies.

`model` is the currently active model `{ provider, id, name }`. `availableModels`
is the list of models this agent can actually switch to (those with valid
credentials), each `{ provider, id, name }`.

`extensions` lists the agent's loaded extensions (those contributing tools or
commands), each `{ id, source, scope, origin, tools, commands, active }`. `active`
reflects whether all of that extension's tools are currently active. `tools` is
`{ active, available }` (tool-name arrays) for fine-grained control. (pi has no
runtime extension on/off switch, so enabling/disabling an extension toggles the
tools it registered.)

### Control actions (`NS/agents/ID/control/in`)

```jsonc
{ "action": "ping" }                                  // re-publish registry + pong
{ "action": "status" }                                // full status on control/out

// Model
{ "action": "set_model", "provider": "anthropic",     // switch model (explicit)
  "modelId": "claude-sonnet-4-5" }
{ "action": "set_model", "query": "gpt-4o" }          // switch model (fuzzy)

// Group (colour-coded board membership)
{ "action": "set_group", "group": "blue" }            // re-bind to that group's board topic

// Interrupt the running turn
{ "action": "abort" }                                 // alias: "interrupt"

// Extensions / tools
{ "action": "list_extensions" }
{ "action": "disable_extension", "extension": "plan-mode" }   // path/basename/source match
{ "action": "enable_extension",  "extension": "plan-mode" }
{ "action": "disable_tools", "tools": ["read_board"] }
{ "action": "enable_tools",  "tools": ["read_board"] }
{ "action": "set_active_tools", "tools": ["read","bash","edit","write"] }

// Session lifecycle
{ "action": "reset" }                                 // fresh conversation/context
{ "action": "reload" }                                // reload extensions/skills/etc
{ "action": "quit" }                                  // graceful shutdown, like /quit (alias: "shutdown")

// Rename this agent (alias: "set_name")
{ "action": "rename", "name": "coder-2" }             // rename + reslug id/topics (default)
{ "action": "rename", "name": "Coder Two",            // rename display name only,
  "reslug": false }                                   //   keep existing id/topics
```

Replies are published to `NS/agents/ID/control/out`.

A `rename` updates both the swarm `name` and the pi session name. By default it
re-derives the agent **id** from the new name, which moves all `NS/agents/ID/*`
topics: the agent clears its old retained registry, reconnects (so its Last-Will
binds to the new id), and re-subscribes. Because the topics move, the agent first
emits a `rename_ack` (with the predicted `newId` + `newTopics`) on the **current**
`control/out`, then publishes the final `rename_result` on the **new**
`control/out` — resubscribe accordingly. Pass `"reslug": false` to change only the
display name while keeping the id/topics stable.

## Spawn console

The **console** (`console.ts`) is a separate, long-running process — not a pi
extension — that lets an orchestrator spawn and shut down headless agents over
MQTT. It listens on a dedicated spawn channel, forks `pi --mode rpc` processes
(always loading the swarm extension so the new agent joins the swarm), tracks
them, and can list/kill them. Headless agents stay alive on an open stdin and are
driven entirely over MQTT by the extension.

```bash
# Run it (Node 24+ runs the .ts file directly)
node console.ts --name host-1 --broker mqtt://127.0.0.1:1883
# or
npm run console -- --name host-1
```

Config (CLI flag wins over env): `--broker`/`PI_SWARM_BROKER`, `--ns`/`PI_SWARM_NS`,
`--name`/`PI_SWARM_CONSOLE_NAME`, `--pi`/`PI_BIN` (default `pi`),
`--extension`/`PI_SWARM_EXTENSION` (default `index.ts` beside the console).

| Topic | Dir | Retained | Payload |
|-------|-----|----------|---------|
| `NS/console/in` | orch → console | – | `{ action, ... }` spawn/list/kill/ping |
| `NS/console/out` | console → orch | – | replies + events (`spawn_result`, `agents`, `kill_result`, `exited`, `pong`, `error`) |
| `NS/console/registry/CID` | console → all | ✅ + LWT | `{ type:"console", id, name, host, pid, agents:[...], ... }` |

### Console actions (`NS/console/in`)

```jsonc
// Spawn a headless agent. All fields except action are optional.
{ "action": "spawn",
  "console": "host-1",                        // target console id; omitted -> every console acts
  "name": "coder-3",                          // --name (also the swarm id); omitted -> agent-<pid>
  "model": "anthropic/claude-sonnet-4-5",     // --model
  "extensions": ["./my-ext.ts", "npm:foo"],   // extra -e extensions (swarm ext auto-added)
  "cwd": "/path/to/project",                  // working directory
  "env": { "FOO": "bar" },                    // extra environment
  "noSession": true,                          // --no-session (ephemeral)
  "approve": true,                            // --approve (trust project for the run)
  "includeSwarmExtension": false,             // opt out of auto-loading the swarm ext
  "reqId": "abc" }                            // echoed back in spawn_result

{ "action": "list" }                          // -> { type:"agents", agents:[...] }
{ "action": "kill", "target": "coder-3" }     // SIGTERM by id / name / pid
{ "action": "kill", "target": "all", "force": true }   // SIGKILL every agent
{ "action": "kill", "target": "coder-3", "signal": "SIGINT" }
{ "action": "ping" }                          // -> { type:"pong", agents: <count> }
```

When several consoles run on different hosts they share `NS/console/in`, so
`spawn` and `kill` may include a `console` field naming the target console id
(from its `NS/console/registry/CID`); only the matching console acts, while
unaddressed `list`/`ping` are answered by every console for discovery.

The console spawns **multiple** agents concurrently (one child process each,
tracked by swarm id) and rejects a spawn whose id is already running. `kill`
sends the chosen signal (default `SIGTERM`, escalating to `SIGKILL` after 5s) and
emits an `exited` event when the child actually stops. On its own shutdown
(`SIGINT`/`SIGTERM`) the console terminates all spawned agents and clears its
retained registry.

```bash
# Spawn two agents
mosquitto_pub -h 127.0.0.1 -t 'swarm/console/in' \
  -m '{"action":"spawn","name":"coder-1","model":"sonnet"}'
mosquitto_pub -h 127.0.0.1 -t 'swarm/console/in' \
  -m '{"action":"spawn","name":"reviewer","extensions":["./review-ext.ts"]}'

# List / kill via MQTT
mosquitto_pub -h 127.0.0.1 -t 'swarm/console/in' -m '{"action":"list"}'
mosquitto_pub -h 127.0.0.1 -t 'swarm/console/in' -m '{"action":"kill","target":"coder-1"}'
mosquitto_pub -h 127.0.0.1 -t 'swarm/console/in' -m '{"action":"kill","target":"all"}'

# Watch console replies/events
mosquitto_sub -h 127.0.0.1 -t 'swarm/console/out' -v
```

## Message delivery model

All inbound work funnels through one queue:

- **Normal** (`/in`, and peer `NS/board` posts) is buffered and flushed when the
  agent goes **idle** (`agent_end`). Multiple queued messages are coalesced into
  one turn.
- **Urgent** (`/interrupt`, or board posts with `urgent: true`) is delivered
  immediately — steered into a running turn or starting a new one if idle.
- **Slash commands** — a single-line message whose first non-whitespace
  character is `/` is detected on either channel and **interpreted by the
  extension itself**, then mapped to the matching pi API call. Injected text is
  *not* handed to pi to parse: `pi.sendUserMessage()` routes through
  `prompt({ expandPromptTemplates: false })`, which deliberately skips command
  handling, and built-ins (`/quit`, `/new`, `/compact`, …) are handled by the
  TUI input layer rather than the agent session — so delivering `/quit` as a
  message would just send the literal text to the model. Supported commands:
  `/quit` `/exit` `/shutdown` → `ctx.shutdown()`; `/compact` → `ctx.compact()`;
  `/stop` `/abort` `/cancel` `/interrupt` → `ctx.abort()`; `/status` →
  re-publish registration; `/reset` `/new` `/clear` → new session; `/reload` →
  reload runtime. Anything unrecognized falls through to normal delivery as
  prose. **Note:** `newSession()`/`reload()` are only exposed on the *command*
  context pi builds for command handlers, not on the event/tool context the
  extension holds, so the programmatic `/reset` and `/reload` paths no-op (with
  a TUI notice) unless a future pi exposes those on the base context — run
  `/swarm-reset` / `/swarm-reload` in the TUI instead.

This is the "pull on idle, separate interrupt channel" design: MQTT does the
inter-process delivery and offline queueing (persistent session, QoS 1); the
extension decides *when* messages reach the model.

## Agent tools

The extension registers two tools and injects a system-prompt block telling the
agent it works in a multi-agent environment:

- `read_board(limit?)` — read recent peer broadcasts before assuming shared state.
- `post_to_board(text, urgent?)` — broadcast a change other agents must know about.

## Example: orchestrator snippets

```bash
# Watch everything
mosquitto_sub -h 127.0.0.1 -t 'swarm/#' -v

# Discover agents (retained registrations)
mosquitto_sub -h 127.0.0.1 -t 'swarm/registry/#' -v

# Watch control replies
mosquitto_sub -h 127.0.0.1 -t 'swarm/agents/coder-1/control/out' -v

# Send work (queued until idle)
mosquitto_pub -h 127.0.0.1 -t 'swarm/agents/coder-1/in' \
  -m '{"text":"Refactor src/auth into smaller modules"}'

# Inject an urgent message into the running turn
mosquitto_pub -h 127.0.0.1 -t 'swarm/agents/coder-1/interrupt' \
  -m '{"text":"Stop — the API contract just changed"}'

# Cancel the running turn entirely
mosquitto_pub -h 127.0.0.1 -t 'swarm/agents/coder-1/control/in' -m '{"action":"abort"}'

# Change model
mosquitto_pub -h 127.0.0.1 -t 'swarm/agents/coder-1/control/in' \
  -m '{"action":"set_model","query":"sonnet"}'

# Disable / enable an extension (by path, basename, or source substring)
mosquitto_pub -h 127.0.0.1 -t 'swarm/agents/coder-1/control/in' \
  -m '{"action":"disable_extension","extension":"plan-mode"}'

# Reset context / reload
mosquitto_pub -h 127.0.0.1 -t 'swarm/agents/coder-1/control/in' -m '{"action":"reset"}'
mosquitto_pub -h 127.0.0.1 -t 'swarm/agents/coder-1/control/in' -m '{"action":"reload"}'

# Broadcast a fact to the whole swarm
mosquitto_pub -h 127.0.0.1 -t 'swarm/board' \
  -m '{"seq":1,"from":{"id":"orch","name":"orchestrator"},"text":"Renamed app/ to web/","urgent":true,"ts":0}'
```

## Slash commands (in-TUI)

- `/swarm-status` — re-publish this agent's registration.
- `/swarm-reset` — fresh conversation context.
- `/swarm-reload` — reload extensions/skills/prompts/themes.

## Notes & limitations

- Board history is kept in-memory per agent; late joiners don't see older posts.
  An orchestrator can persist `NS/board` and replay if needed.
- Broadcasting canonical shared facts via a single authority (the orchestrator)
  avoids feedback loops; agents ignore their own board echoes by id.
- Anonymous local broker is assumed; add auth/TLS for anything networked.

## License

MIT © Luumo Factory
