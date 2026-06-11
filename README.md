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
pi -e /path/to/pi-mqtt-swarm/src/index.ts --swarm-name coder-1
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
| name | `--swarm-name` flag / `PI_SWARM_NAME` | `agent-<pid>` | Human label; also names the session. Slugified into the agent id used in topics. |
| broker | `PI_SWARM_BROKER` / `MQTT_URL` | `mqtt://127.0.0.1:1883` | MQTT broker URL. |
| namespace | `PI_SWARM_NS` | `swarm` | Root of all topics. |

> Give each agent a **unique** name — the id (slug of the name) is used in
> topic paths, so duplicate names collide.

## Topic map

`NS` = namespace (default `swarm`), `ID` = slugified agent id.

| Topic | Dir | Retained | Payload |
|-------|-----|----------|---------|
| `NS/registry/ID` | agent → all | ✅ + LWT | `{ id, name, status, model, pid, cwd, startedAt, ts }` |
| `NS/agents/ID/in` | orch → agent | – | `{ text }` or raw string — **queued, delivered when idle** |
| `NS/agents/ID/interrupt` | orch → agent | – | `{ text }` or raw string — **delivered immediately** |
| `NS/agents/ID/control` | orch → agent | – | `{ action, ... }` (see below) |
| `NS/agents/ID/out` | agent → orch | – | event stream (turn/agent summaries, acks, results) |
| `NS/board` | any → all | – | `{ seq, from:{id,name}, text, urgent, ts }` |

`status` is one of `online` \| `busy` \| `idle` \| `offline`. The broker
publishes `offline` automatically via MQTT Last-Will if an agent dies.

### Control actions (`NS/agents/ID/control`)

```jsonc
{ "action": "ping" }                                  // re-publish registry + pong
{ "action": "set_model", "provider": "anthropic",     // switch model (explicit)
  "modelId": "claude-sonnet-4-5" }
{ "action": "set_model", "query": "gpt-4o" }          // switch model (fuzzy)
{ "action": "reset" }                                 // fresh conversation/context
{ "action": "reload" }                                // reload extensions/skills/etc
```

Results/acks are published to `NS/agents/ID/out`.

## Message delivery model

All inbound work funnels through one queue:

- **Normal** (`/in`, and peer `NS/board` posts) is buffered and flushed when the
  agent goes **idle** (`agent_end`). Multiple queued messages are coalesced into
  one turn.
- **Urgent** (`/interrupt`, or board posts with `urgent: true`) is delivered
  immediately — steered into a running turn or starting a new one if idle.

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

# Send work (queued until idle)
mosquitto_pub -h 127.0.0.1 -t 'swarm/agents/coder-1/in' \
  -m '{"text":"Refactor src/auth into smaller modules"}'

# Interrupt now
mosquitto_pub -h 127.0.0.1 -t 'swarm/agents/coder-1/interrupt' \
  -m '{"text":"Stop — the API contract just changed"}'

# Change model
mosquitto_pub -h 127.0.0.1 -t 'swarm/agents/coder-1/control' \
  -m '{"action":"set_model","query":"sonnet"}'

# Reset context / reload
mosquitto_pub -h 127.0.0.1 -t 'swarm/agents/coder-1/control' -m '{"action":"reset"}'
mosquitto_pub -h 127.0.0.1 -t 'swarm/agents/coder-1/control' -m '{"action":"reload"}'

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
