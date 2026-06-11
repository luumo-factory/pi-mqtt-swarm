#!/usr/bin/env node
/**
 * pi-mqtt-swarm console
 * =====================
 * A lightweight, long-running process that listens on a dedicated MQTT spawn
 * channel and launches headless pi agents on demand. It is the "host" side of
 * the swarm: orchestrators publish spawn requests, the console forks `pi`
 * processes (always wired up with the swarm extension so the new agent joins
 * the swarm), tracks them, and can list/kill them.
 *
 * Headless agents run in pi's RPC mode (`pi --mode rpc`): the process stays
 * alive on an open stdin, and the swarm extension drives it entirely over MQTT
 * (inbound work, control, board). The console never needs to feed RPC commands.
 *
 * Run it:
 *   node src/console.ts                       # uses env defaults
 *   node src/console.ts --name host-1 --broker mqtt://127.0.0.1:1883
 *   PI_SWARM_BROKER=... PI_SWARM_NS=... node src/console.ts
 *
 * Configuration (env + CLI flag; flag wins):
 *   --broker <url>     PI_SWARM_BROKER / MQTT_URL  (default mqtt://127.0.0.1:1883)
 *   --ns <root>        PI_SWARM_NS                  (default "swarm")
 *   --name <name>      PI_SWARM_CONSOLE_NAME        (default console-<host>-<pid>)
 *   --pi <bin>         PI_BIN                       (default "pi")
 *   --extension <path> PI_SWARM_EXTENSION           (default ./index.ts beside this file)
 *
 * Topic map (NS = namespace):
 *   NS/console/in              inbound console commands { action, ... }
 *   NS/console/out             outbound replies + events (spawned/exited/...)
 *   NS/console/registry/CID    (retained) console presence + live agent list + LWT
 *
 * Console actions (NS/console/in):
 *   { action: "spawn", name?, model?, extensions?: string[], cwd?, env?,
 *     noSession?, approve?, includeSwarmExtension?, reqId? }
 *   { action: "list" }
 *   { action: "kill", target: <name|id|pid|"all">, signal?, force? }
 *   { action: "ping" }
 */

import mqtt, { type MqttClient } from "mqtt";
import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { hostname } from "node:os";

// ---------------------------------------------------------------------------
// Config / CLI
// ---------------------------------------------------------------------------

function parseArgs(argv: string[]): Record<string, string> {
	const out: Record<string, string> = {};
	for (let i = 0; i < argv.length; i++) {
		const a = argv[i];
		if (!a.startsWith("--")) continue;
		const key = a.slice(2);
		const next = argv[i + 1];
		if (next === undefined || next.startsWith("--")) {
			out[key] = "true";
		} else {
			out[key] = next;
			i++;
		}
	}
	return out;
}

const ARGS = parseArgs(process.argv.slice(2));

const NS = ARGS.ns ?? process.env.PI_SWARM_NS ?? "swarm";
const BROKER = ARGS.broker ?? process.env.PI_SWARM_BROKER ?? process.env.MQTT_URL ?? "mqtt://127.0.0.1:1883";
const PI_BIN = ARGS.pi ?? process.env.PI_BIN ?? "pi";

// Default to the swarm extension that sits next to this file, so spawned agents
// always join the swarm. Overridable for non-standard installs.
const DEFAULT_EXTENSION =
	ARGS.extension ?? process.env.PI_SWARM_EXTENSION ?? fileURLToPath(new URL("./index.ts", import.meta.url));

function slug(s: string): string {
	return (
		s
			.toLowerCase()
			.trim()
			.replace(/[^a-z0-9._-]+/g, "-")
			.replace(/^-+|-+$/g, "") || "agent"
	);
}

const CONSOLE_NAME = ARGS.name ?? process.env.PI_SWARM_CONSOLE_NAME ?? `console-${hostname()}-${process.pid}`;
const CONSOLE_ID = slug(CONSOLE_NAME);

const T = {
	in: `${NS}/console/in`,
	out: `${NS}/console/out`,
	registry: `${NS}/console/registry/${CONSOLE_ID}`,
};

// ---------------------------------------------------------------------------
// Runtime state
// ---------------------------------------------------------------------------

type Agent = {
	id: string; // slug(name) — matches the agent's own swarm id
	name: string;
	pid: number;
	child: ChildProcess;
	model: string | null;
	extensions: string[];
	cwd: string;
	startedAt: number;
};

const agents = new Map<string, Agent>(); // keyed by agent id
const startedAt = Date.now();
let client: MqttClient | null = null;

function log(...args: unknown[]) {
	console.error(`[swarm-console ${CONSOLE_NAME}]`, ...args);
}

const pub = (topic: string, payload: unknown, opts?: mqtt.IClientPublishOptions) => {
	try {
		client?.publish(topic, typeof payload === "string" ? payload : JSON.stringify(payload), opts ?? { qos: 1 });
	} catch {
		/* broker may be momentarily down */
	}
};

const reply = (payload: Record<string, unknown>) => pub(T.out, { console: CONSOLE_ID, ts: Date.now(), ...payload });

function agentView(a: Agent) {
	return {
		id: a.id,
		name: a.name,
		pid: a.pid,
		model: a.model,
		extensions: a.extensions,
		cwd: a.cwd,
		startedAt: a.startedAt,
	};
}

const publishRegistry = () => {
	pub(
		T.registry,
		{
			type: "console",
			id: CONSOLE_ID,
			name: CONSOLE_NAME,
			host: hostname(),
			pid: process.pid,
			broker: BROKER,
			ns: NS,
			piBin: PI_BIN,
			agents: [...agents.values()].map(agentView),
			startedAt,
			ts: Date.now(),
		},
		{ qos: 1, retain: true },
	);
};

// ---------------------------------------------------------------------------
// Spawning
// ---------------------------------------------------------------------------

function buildArgs(req: any): { args: string[]; extensions: string[]; name?: string; model: string | null } {
	const includeSwarm = req.includeSwarmExtension !== false;
	const requested: string[] = Array.isArray(req.extensions)
		? req.extensions.map(String)
		: req.extension
			? [String(req.extension)]
			: [];

	// Always load the swarm extension first (unless explicitly disabled) so the
	// new agent joins the swarm. Avoid duplicating it if the caller listed it.
	const extensions = [...requested];
	if (includeSwarm && !extensions.includes(DEFAULT_EXTENSION)) extensions.unshift(DEFAULT_EXTENSION);

	const name: string | undefined = req.name ? String(req.name) : undefined;
	const model: string | null = req.model ? String(req.model) : null;

	const args = ["--mode", "rpc"];
	for (const ext of extensions) args.push("--extension", ext);
	if (name) args.push("--name", name);
	if (model) args.push("--model", model);
	if (req.noSession) args.push("--no-session");
	if (req.approve) args.push("--approve");

	return { args, extensions, name, model };
}

function handleSpawn(req: any) {
	const { args, extensions, name: requestedName, model } = buildArgs(req);
	let name: string | undefined = requestedName;
	let id: string | null = name ? slug(name) : null;

	// If name is provided and it's a duplicate, append a hyphen and an incrementing integer.
	if (id && agents.has(id)) {
		let counter = 1;
		let newName = `${name}-${counter}`;
		let newId = slug(newName);

		while (agents.has(newId)) {
			counter++;
			newName = `${name}-${counter}`;
			newId = slug(newName);
		}
		name = newName;
		id = newId;

		// Update the args to use the new name
		const nameIndex = args.indexOf("--name");
		if (nameIndex !== -1) {
			args[nameIndex + 1] = name;
		} else {
			args.push("--name", name);
		}
	}

	const cwd = req.cwd ? String(req.cwd) : process.cwd();
	const env = { ...process.env, ...(req.env && typeof req.env === "object" ? req.env : {}) };

	let child: ChildProcess;
	try {
		child = spawn(PI_BIN, args, {
			cwd,
			env,
			// stdin kept open (pipe) so RPC mode does not see EOF and exit;
			// stdout/stderr piped so we can surface startup errors.
			stdio: ["pipe", "pipe", "pipe"],
		});
	} catch (err: any) {
		reply({ type: "spawn_result", ok: false, error: `spawn failed: ${err?.message ?? err}`, reqId: req.reqId });
		return;
	}

	const pid = child.pid ?? -1;
	// If no explicit name was given, the agent defaults to agent-<its pid>; mirror
	// that here so our tracking id matches the agent's own swarm id.
	const finalName = name ?? `agent-${pid}`;
	const finalId = id ?? slug(finalName);

	const agent: Agent = {
		id: finalId,
		name: finalName,
		pid,
		child,
		model,
		extensions,
		cwd,
		startedAt: Date.now(),
	};
	agents.set(finalId, agent);

	child.stdout?.on("data", (b) => log(`[${finalId}:out]`, b.toString().trimEnd()));
	child.stderr?.on("data", (b) => log(`[${finalId}:err]`, b.toString().trimEnd()));

	child.on("error", (err) => {
		log(`agent ${finalId} process error:`, err.message);
		reply({ type: "agent_error", id: finalId, name: finalName, error: err.message });
	});

	child.on("exit", (code, signal) => {
		agents.delete(finalId);
		log(`agent ${finalId} (pid ${pid}) exited code=${code} signal=${signal}`);
		reply({ type: "exited", id: finalId, name: finalName, pid, code, signal });
		publishRegistry();
	});

	log(`spawned agent ${finalId} (pid ${pid}): ${PI_BIN} ${args.join(" ")}`);
	reply({ type: "spawn_result", ok: true, reqId: req.reqId, agent: agentView(agent) });
	publishRegistry();
}

function resolveTargets(target: unknown): Agent[] {
	if (target === "all" || target === undefined || target === null) return [...agents.values()];
	const t = String(target);
	const byId = agents.get(t);
	if (byId) return [byId];
	const bySlug = agents.get(slug(t));
	if (bySlug) return [bySlug];
	return [...agents.values()].filter((a) => a.name === t || String(a.pid) === t);
}

function handleKill(req: any) {
	const targets = resolveTargets(req.target);
	if (targets.length === 0) {
		reply({ type: "kill_result", ok: false, error: `no matching agent: ${req.target}`, reqId: req.reqId });
		return;
	}
	const signal: NodeJS.Signals = req.force ? "SIGKILL" : ((req.signal as NodeJS.Signals) ?? "SIGTERM");
	const killed: string[] = [];
	for (const a of targets) {
		try {
			a.child.kill(signal);
			killed.push(a.id);
			// Escalate to SIGKILL if it doesn't exit promptly (unless already SIGKILL).
			if (signal !== "SIGKILL") {
				const ref = a;
				setTimeout(() => {
					if (agents.has(ref.id)) {
						try {
							ref.child.kill("SIGKILL");
						} catch {
							/* ignore */
						}
					}
				}, 5000).unref?.();
			}
		} catch (err: any) {
			log(`failed to kill ${a.id}:`, err?.message ?? err);
		}
	}
	reply({ type: "kill_result", ok: true, reqId: req.reqId, signal, killed });
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

// Whether a command is addressed to this console. Several consoles share the
// NS/console/in topic, so spawn/kill may carry a `console` field naming the
// target host; when present and not matching us, we ignore the command. list/
// ping are unaddressed so every console answers (for discovery).
function addressedToMe(msg: any): boolean {
	const target = msg?.console;
	if (target === undefined || target === null || target === "") return true;
	const t = String(target);
	return t === CONSOLE_ID || slug(t) === CONSOLE_ID;
}

function handleCommand(msg: any) {
	const action = msg?.action;
	switch (action) {
		case "spawn":
			if (!addressedToMe(msg)) return;
			handleSpawn(msg);
			return;
		case "list":
			reply({ type: "agents", reqId: msg.reqId, agents: [...agents.values()].map(agentView) });
			return;
		case "kill":
			if (!addressedToMe(msg)) return;
			handleKill(msg);
			return;
		case "ping":
			reply({ type: "pong", reqId: msg.reqId, agents: agents.size });
			publishRegistry();
			return;
		default:
			reply({ type: "error", reqId: msg?.reqId, error: `unknown console action: ${action}` });
	}
}

// ---------------------------------------------------------------------------
// MQTT
// ---------------------------------------------------------------------------

function connect() {
	client = mqtt.connect(BROKER, {
		clientId: `pi-console-${CONSOLE_ID}-${process.pid}`,
		clean: true,
		reconnectPeriod: 2000,
		will: {
			topic: T.registry,
			payload: JSON.stringify({ type: "console", id: CONSOLE_ID, name: CONSOLE_NAME, status: "offline", ts: Date.now() }),
			qos: 1,
			retain: true,
		},
	});

	client.on("connect", () => {
		client!.subscribe(T.in, { qos: 1 });
		publishRegistry();
		log(`connected to ${BROKER}; listening on ${T.in}`);
	});

	client.on("message", (_topic, raw) => {
		let msg: any;
		try {
			msg = JSON.parse(raw.toString());
		} catch {
			log("ignoring non-JSON console message");
			return;
		}
		try {
			handleCommand(msg);
		} catch (err: any) {
			log("command handler error:", err?.message ?? err);
			reply({ type: "error", reqId: msg?.reqId, error: String(err?.message ?? err) });
		}
	});

	client.on("error", (err) => log("mqtt error:", err.message));
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

let shuttingDown = false;
function shutdown(signal: string) {
	if (shuttingDown) return;
	shuttingDown = true;
	log(`received ${signal}; shutting down (${agents.size} agent(s))`);
	for (const a of agents.values()) {
		try {
			a.child.kill("SIGTERM");
		} catch {
			/* ignore */
		}
	}
	// Clear retained registry and disconnect, then exit.
	pub(T.registry, "", { qos: 1, retain: true });
	try {
		client?.end(false, {}, () => process.exit(0));
	} catch {
		process.exit(0);
	}
	setTimeout(() => process.exit(0), 3000).unref?.();
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

log(`starting; ns=${NS} broker=${BROKER} pi=${PI_BIN}`);
log(`swarm extension: ${DEFAULT_EXTENSION}`);
connect();
