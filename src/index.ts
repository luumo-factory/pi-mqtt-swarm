/**
 * pi-mqtt-swarm
 * =============
 * Turns a pi TUI instance into a member of an MQTT-coordinated agent swarm.
 *
 * A central orchestrator can:
 *   - discover agents (retained registration/overview topic)
 *   - stream work into each agent (idle-gated queue) and interrupt urgently
 *   - watch agent output and status centrally
 *   - change an agent's model, reset its context, or reload it remotely
 *
 * Agents can:
 *   - read and post to a shared multi-agent message board, so cross-cutting
 *     facts ("I renamed folder X to Y") propagate to peers.
 *
 * Configuration (env + CLI flag):
 *   --swarm-name <name>     Human label for this agent (also names the session).
 *   PI_SWARM_NAME           Same as --swarm-name (flag wins).
 *   PI_SWARM_BROKER / MQTT_URL   Broker URL (default mqtt://127.0.0.1:1883).
 *   PI_SWARM_NS             Topic namespace root (default "swarm").
 *
 * Topic map (NS = namespace, ID = slugified agent id):
 *   NS/registry/ID            (retained) registration + live overview + LWT
 *   NS/agents/ID/in           inbound normal work  -> queued, delivered when idle
 *   NS/agents/ID/interrupt    inbound urgent work  -> delivered immediately
 *   NS/agents/ID/control      inbound control { action: set_model|reset|reload|ping }
 *   NS/agents/ID/out          outbound events (locally-typed user input, turn/agent summaries, results, acks)
 *   NS/board                  shared broadcast board (all agents subscribe)
 */

import mqtt, { type MqttClient } from "mqtt";
import { Type } from "typebox";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const NS = process.env.PI_SWARM_NS ?? "swarm";
const BROKER = process.env.PI_SWARM_BROKER ?? process.env.MQTT_URL ?? "mqtt://127.0.0.1:1883";
const BOARD_HISTORY_MAX = 100;

function slug(s: string): string {
	return (
		s
			.toLowerCase()
			.trim()
			.replace(/[^a-z0-9._-]+/g, "-")
			.replace(/^-+|-+$/g, "") || "agent"
	);
}

// Extract the text of the last assistant message from an agent_end batch.
function lastAssistantText(messages: any[]): string | null {
	for (let i = messages.length - 1; i >= 0; i--) {
		const m = messages[i];
		if (m?.role !== "assistant") continue;
		if (typeof m.content === "string") return m.content;
		if (Array.isArray(m.content)) {
			const text = m.content
				.filter((c: any) => c?.type === "text" && typeof c.text === "string")
				.map((c: any) => c.text)
				.join("");
			if (text) return text;
		}
	}
	return null;
}

function topicsFor(ns: string, id: string) {
	return {
		registry: `${ns}/registry/${id}`,
		// Work / data plane
		in: `${ns}/agents/${id}/in`,
		interrupt: `${ns}/agents/${id}/interrupt`,
		out: `${ns}/agents/${id}/out`,
		// Control plane (dedicated in/out pair)
		controlIn: `${ns}/agents/${id}/control/in`,
		controlOut: `${ns}/agents/${id}/control/out`,
		board: `${ns}/board`,
	};
}

type ExtInfo = {
	id: string;
	source: string;
	scope?: string;
	origin?: string;
	tools: string[];
	commands: string[];
	active: boolean;
};

type ModelInfo = { provider: string; id: string; name?: string } | null;
type BoardPost = { seq: number; from: { id: string; name: string }; text: string; urgent: boolean; ts: number };

export default function (pi: ExtensionAPI) {
	// CLI flag: pi --swarm-name coder-1
	pi.registerFlag("swarm-name", {
		description: "Human label for this agent in the MQTT swarm (also names the session)",
		type: "string",
	});

	// Identity is resolved lazily in session_start, because CLI flag values are
	// not yet applied while the extension factory runs.
	let NAME = `agent-${process.pid}`;
	let ID = slug(NAME);
	let T = topicsFor(NS, ID);
	let identityResolved = false;

	const resolveIdentity = () => {
		if (identityResolved) return;
		// Priority: explicit --swarm-name flag > PI_SWARM_NAME env > the pi
		// session name (set via `pi --name xxxx`) > default agent-<pid>.
		const rawName =
			(pi.getFlag("swarm-name") as string | undefined) ??
			process.env.PI_SWARM_NAME ??
			pi.getSessionName?.() ??
			undefined;
		if (rawName) {
			NAME = rawName;
			ID = slug(rawName);
			T = topicsFor(NS, ID);
		}
		identityResolved = true;
	};

	// -----------------------------------------------------------------------
	// Runtime state
	// -----------------------------------------------------------------------
	let client: MqttClient | null = null;
	let busy = false; // true between agent_start and agent_end
	let model: ModelInfo = null;
	let modelRegistry: any = null; // captured from latest session ctx
	let availableModels: { provider: string; id: string; name?: string }[] = [];
	let lastCtx: any = null; // most recent context, used for ctx.abort()
	const queue: string[] = []; // normal inbound, awaiting idle
	const board: BoardPost[] = []; // local mirror of board history
	let boardSeq = 0;
	const startedAt = Date.now();

	// -----------------------------------------------------------------------
	// MQTT helpers
	// -----------------------------------------------------------------------
	const pub = (topic: string, payload: unknown, opts?: mqtt.IClientPublishOptions) => {
		try {
			client?.publish(topic, typeof payload === "string" ? payload : JSON.stringify(payload), opts ?? { qos: 1 });
		} catch {
			/* broker may be momentarily down; QoS1 persistent session will catch up */
		}
	};

	// Group registered tools + commands by their owning extension. pi exposes
	// runtime control at tool granularity, so an extension is considered "active"
	// when all of the tools it registered are currently active.
	const listExtensions = (): ExtInfo[] => {
		const active = new Set(pi.getActiveTools());
		const groups = new Map<string, ExtInfo>();
		const get = (si: any): ExtInfo => {
			const key = si?.path ?? si?.source ?? "unknown";
			let g = groups.get(key);
			if (!g) {
				g = { id: key, source: si?.source ?? "extension", scope: si?.scope, origin: si?.origin, tools: [], commands: [], active: true };
				groups.set(key, g);
			}
			return g;
		};
		for (const t of pi.getAllTools()) {
			const si = (t as any).sourceInfo;
			if (!si || si.source === "builtin" || si.source === "sdk") continue;
			get(si).tools.push(t.name);
		}
		for (const c of pi.getCommands()) {
			if ((c as any).source !== "extension") continue;
			get((c as any).sourceInfo).commands.push(c.name);
		}
		return [...groups.values()].map((g) => ({
			...g,
			active: g.tools.length === 0 ? true : g.tools.every((n) => active.has(n)),
		}));
	};

	const toolSummary = () => ({ active: pi.getActiveTools(), available: pi.getAllTools().map((t) => t.name) });

	const publishRegistry = (status: "online" | "busy" | "idle" | "offline") => {
		pub(
			T.registry,
			{
				id: ID,
				name: NAME,
				status,
				model,
				availableModels,
				extensions: listExtensions(),
				tools: toolSummary(),
				pid: process.pid,
				cwd: process.cwd(),
				startedAt,
				ts: Date.now(),
			},
			{ qos: 1, retain: true },
		);
	};

	// Cache the models this agent can actually use (those with valid credentials).
	const refreshAvailableModels = async () => {
		try {
			const list = (await modelRegistry?.getAvailable?.()) ?? [];
			availableModels = list.map((m: any) => ({ provider: m.provider, id: m.id, name: m.name }));
		} catch {
			/* leave previous list in place on failure */
		}
	};

	const updateStatusLine = (ctx: any) => {
		if (!ctx?.hasUI) return;
		const m = model?.name ?? model?.id ?? "no-model";
		ctx.ui.setStatus?.("swarm", `swarm:${NAME}${busy ? " ⋅ busy" : ""} ⋅ ${m}`);
	};

	// -----------------------------------------------------------------------
	// Unified delivery queue (point 6: all messages queued together)
	// -----------------------------------------------------------------------
	// Everything destined for the LLM funnels through here. Normal traffic is
	// buffered and flushed when the agent is idle; urgent traffic bypasses.
	// Whether the agent is actually streaming *right now*. Our `busy` flag only
	// flips on agent_start/agent_end, so it's briefly stale — e.g. just after we
	// trigger a turn (before agent_start fires) or while the operator is typing
	// directly into the TUI. ctx.isIdle() reflects pi's real state, so prefer it
	// when we have a context, falling back to the flag otherwise.
	const agentStreaming = () => {
		if (typeof lastCtx?.isIdle === "function") {
			try {
				return !lastCtx.isIdle();
			} catch {
				/* fall back to the flag */
			}
		}
		return busy;
	};

	// Send a user message, choosing immediate vs. queued delivery from the
	// agent's real state. If pi still reports "already processing" (a race we
	// lost between the check and the send), retry with the queued behavior so the
	// message is never dropped and the error never surfaces in the TUI.
	const deliver = (content: string, queued: "steer" | "followUp") => {
		const opts = agentStreaming() ? { deliverAs: queued } : undefined;
		try {
			pi.sendUserMessage(content, opts);
		} catch {
			if (!opts) {
				try {
					pi.sendUserMessage(content, { deliverAs: queued });
				} catch {
					/* give up: agent state is unrecoverable for this message */
				}
			}
		}
	};

	const flush = () => {
		if (agentStreaming() || queue.length === 0) return;
		const batch = queue.splice(0, queue.length);
		const text =
			batch.length === 1
				? batch[0]
				: `You have ${batch.length} queued swarm messages:\n` + batch.map((m, i) => `${i + 1}. ${m}`).join("\n");
		deliver(text, "followUp"); // idle -> triggers a fresh turn
	};

	// A slash command is a single-line message whose first non-whitespace
	// character is "/". pi only recognizes a command when the "/" leads the
	// input, so such messages must be delivered verbatim — never wrapped (e.g.
	// "[URGENT] …") or batched into a numbered list, or pi would treat them as
	// plain prose for the LLM instead of executing the command.
	const isSlashCommand = (text: string) => {
		const t = text.trim();
		return t.startsWith("/") && !/[\r\n]/.test(t);
	};

	// Deliver a slash command verbatim. Mid-stream it's queued as a followUp
	// (commands can't be steered); idle, it runs immediately and triggers a turn.
	const deliverCommand = (text: string) => deliver(text.trim(), "followUp");

	const enqueue = (text: string, urgent: boolean) => {
		// Slash commands bypass wrapping/batching so pi runs them as commands.
		if (isSlashCommand(text)) {
			deliverCommand(text);
			return;
		}
		if (urgent) {
			// Deliver now; steer if mid-stream, otherwise it triggers a turn.
			deliver(`[URGENT] ${text}`, "steer");
			return;
		}
		queue.push(text);
		if (!agentStreaming()) flush();
	};

	// Invoke one of our own slash commands as a user message (documented pattern
	// for reaching command-only context like newSession/reload from elsewhere).
	const invokeCommand = (name: string) => deliverCommand(`/${name}`);

	// -----------------------------------------------------------------------
	// Board helpers
	// -----------------------------------------------------------------------
	const recordBoard = (post: BoardPost) => {
		board.push(post);
		if (board.length > BOARD_HISTORY_MAX) board.splice(0, board.length - BOARD_HISTORY_MAX);
	};

	// -----------------------------------------------------------------------
	// Control plane: replies go to T.controlOut
	// -----------------------------------------------------------------------
	const reply = (payload: Record<string, unknown>) => pub(T.controlOut, { id: ID, ts: Date.now(), ...payload });

	// Resolve `extension` arg (path / basename / source substring) to the matching
	// extension groups, and return the union of tools they registered.
	const resolveExtensionTools = (arg: string): { matched: ExtInfo[]; tools: string[] } => {
		const q = String(arg).toLowerCase();
		const matched = listExtensions().filter(
			(e) =>
				e.id.toLowerCase().includes(q) ||
				e.source.toLowerCase().includes(q) ||
				e.id.split(/[\\/]/).pop()?.toLowerCase().includes(q),
		);
		const tools = [...new Set(matched.flatMap((e) => e.tools))];
		return { matched, tools };
	};

	const setActive = (names: string[]) => pi.setActiveTools([...new Set(names)]);

	// Tear down and re-establish the MQTT connection under the current identity.
	// Used by rename when the slugified id (and therefore topics + Last-Will)
	// changes, so subscriptions and the will follow the new id.
	const reconnect = () => {
		try {
			client?.end(true);
		} catch {
			/* ignore */
		}
		client = null;
		if (lastCtx) connect(lastCtx);
	};

	// Rename this agent. Always updates the human-facing NAME and the pi session
	// name. When the slugified id changes (the default), topics move too: the
	// stale retained registry entry is cleared and the client reconnects so
	// subscriptions + Last-Will bind to the new id. Pass reslug=false to keep the
	// id/topics stable and only change the display name.
	const renameAgent = (rawName: string, reslug = true) => {
		const trimmed = String(rawName ?? "").trim();
		if (!trimmed) return { ok: false as const, error: "empty name" };

		const previous = { name: NAME, id: ID };
		NAME = trimmed;
		pi.setSessionName?.(NAME);

		const newId = reslug ? slug(trimmed) : ID;
		const idChanged = newId !== ID;
		if (idChanged) {
			const oldRegistry = T.registry;
			ID = newId;
			T = topicsFor(NS, ID);
			// Delete the stale retained registry entry under the old id (empty
			// retained payload), then reconnect so the new id is fully wired up.
			try {
				client?.publish(oldRegistry, "", { qos: 1, retain: true });
			} catch {
				/* ignore */
			}
			reconnect();
		} else {
			publishRegistry(busy ? "busy" : "online");
		}
		if (lastCtx) updateStatusLine(lastCtx);
		return { ok: true as const, name: NAME, id: ID, idChanged, previous };
	};

	const handleControl = async (msg: any) => {
		const action = msg?.action;
		switch (action) {
			case "ping":
				publishRegistry(busy ? "busy" : "online");
				reply({ type: "pong" });
				return;

			case "status":
				publishRegistry(busy ? "busy" : "online");
				reply({ type: "status", status: busy ? "busy" : "idle", model, extensions: listExtensions(), tools: toolSummary() });
				return;

			case "set_model": {
				if (!modelRegistry) {
					reply({ type: "set_model_result", ok: false, error: "no model registry" });
					return;
				}
				let target: any = null;
				if (msg.provider && msg.modelId) {
					target = modelRegistry.find(msg.provider, msg.modelId);
				} else if (msg.query) {
					const q = String(msg.query).toLowerCase();
					const available = (await modelRegistry.getAvailable?.()) ?? [];
					target =
						available.find((m: any) => `${m.provider}/${m.id}`.toLowerCase() === q) ??
						available.find((m: any) => m.id?.toLowerCase().includes(q) || m.name?.toLowerCase().includes(q));
				}
				if (!target) {
					reply({ type: "set_model_result", ok: false, error: "model not found", request: msg });
					return;
				}
				const ok = await pi.setModel(target);
				reply({
					type: "set_model_result",
					ok,
					model: ok ? { provider: target.provider, id: target.id, name: target.name } : null,
					error: ok ? undefined : "no API key for model",
				});
				return;
			}

			case "list_extensions":
				reply({ type: "extensions", extensions: listExtensions(), tools: toolSummary() });
				return;

			case "enable_extension":
			case "disable_extension": {
				const enabling = action === "enable_extension";
				const { matched, tools } = resolveExtensionTools(msg.extension ?? "");
				if (matched.length === 0) {
					reply({ type: "extension_toggle", ok: false, error: `no extension matched: ${msg.extension}` });
					return;
				}
				const current = pi.getActiveTools();
				setActive(enabling ? [...current, ...tools] : current.filter((n) => !tools.includes(n)));
				publishRegistry(busy ? "busy" : "online");
				reply({
					type: "extension_toggle",
					ok: true,
					enabled: enabling,
					matched: matched.map((e) => e.id),
					toolsAffected: tools,
					extensions: listExtensions(),
				});
				return;
			}

			case "enable_tools":
			case "disable_tools":
			case "set_active_tools": {
				const names: string[] = Array.isArray(msg.tools) ? msg.tools : [];
				const current = pi.getActiveTools();
				const next =
					action === "set_active_tools" ? names : action === "enable_tools" ? [...current, ...names] : current.filter((n) => !names.includes(n));
				setActive(next);
				publishRegistry(busy ? "busy" : "online");
				reply({ type: "tools", action, tools: toolSummary() });
				return;
			}

			case "abort":
			case "interrupt": {
				const wasBusy = busy;
				try {
					lastCtx?.abort?.();
				} catch {
					/* ignore */
				}
				reply({ type: "abort", ok: true, wasBusy });
				return;
			}

			case "rename":
			case "set_name": {
				const wantName = String(msg.name ?? msg.text ?? "").trim();
				const reslug = msg.reslug !== false;
				if (!wantName) {
					reply({ type: "rename_result", ok: false, error: "empty name" });
					return;
				}
				const predictedId = reslug ? slug(wantName) : ID;
				// Pre-ack on the *current* control/out. A reslug moves topics, so
				// announce the new id/topics here before we switch over — the final
				// rename_result is published on the new control/out.
				reply({
					type: "rename_ack",
					request: { name: wantName, reslug },
					newId: predictedId,
					newTopics: topicsFor(NS, predictedId),
				});
				const result = renameAgent(wantName, reslug);
				reply({ type: "rename_result", ...result });
				return;
			}

			case "reset":
				reply({ type: "ack", action: "reset" });
				invokeCommand("swarm-reset");
				return;

			case "reload":
				reply({ type: "ack", action: "reload" });
				invokeCommand("swarm-reload");
				return;

			case "quit":
			case "shutdown": {
				// Graceful shutdown, equivalent to the in-TUI /quit command.
				reply({ type: "ack", action: "quit" });
				if (typeof lastCtx?.shutdown === "function") {
					lastCtx.shutdown();
				} else {
					// No context yet: clear our retained registry and exit directly.
					try {
						client?.publish(T.registry, JSON.stringify({ id: ID, name: NAME, status: "offline", ts: Date.now() }), {
							qos: 1,
							retain: true,
						});
					} catch {
						/* ignore */
					}
					setTimeout(() => process.exit(0), 100).unref?.();
				}
				return;
			}

			default:
				reply({ type: "error", error: `unknown control action: ${action}` });
		}
	};

	// -----------------------------------------------------------------------
	// MQTT connection (re)established on every session_start
	// -----------------------------------------------------------------------
	const connect = (ctx: any) => {
		client = mqtt.connect(BROKER, {
			clientId: `pi-${ID}-${process.pid}`,
			clean: false, // persistent session: broker queues QoS1 work while we're away
			reconnectPeriod: 2000,
			will: {
				topic: T.registry,
				payload: JSON.stringify({ id: ID, name: NAME, status: "offline", ts: Date.now() }),
				qos: 1,
				retain: true,
			},
		});

		client.on("connect", () => {
			client!.subscribe([T.in, T.interrupt, T.controlIn, T.board], { qos: 1 });
			publishRegistry(busy ? "busy" : "online");
			updateStatusLine(ctx);
		});

		client.on("message", (topic, raw) => {
			let msg: any;
			const s = raw.toString();
			try {
				msg = JSON.parse(s);
			} catch {
				msg = { text: s };
			}

			if (topic === T.board) {
				if (!msg || typeof msg.seq !== "number") return;
				if (msg.from?.id === ID) return; // ignore our own broadcast echo
				recordBoard(msg as BoardPost);
				enqueue(
					`[BOARD] ${msg.from?.name ?? "peer"} broadcast: ${msg.text}`,
					Boolean(msg.urgent),
				);
				return;
			}

			if (topic === T.controlIn) {
				void handleControl(msg);
				return;
			}

			const text: string = msg.text ?? msg.message ?? s;
			if (topic === T.interrupt) {
				enqueue(text, true);
			} else {
				enqueue(text, false);
			}
		});

		client.on("error", () => {
			/* swallow; auto-reconnect handles it */
		});
	};

	// -----------------------------------------------------------------------
	// Tools (point 7)
	// -----------------------------------------------------------------------
	pi.registerTool({
		name: "read_board",
		label: "Read swarm board",
		description:
			"Read recent broadcasts from other agents in this multi-agent swarm (shared facts, decisions, environment changes).",
		promptSnippet: "Read the shared swarm message board for cross-agent updates.",
		promptGuidelines: [
			"Use read_board before relying on assumptions about the project layout, file paths, or shared state that another agent may have changed.",
		],
		parameters: Type.Object({
			limit: Type.Optional(Type.Number({ description: "Max recent posts to return (default 20)" })),
		}),
		async execute(_id, params) {
			const n = Math.max(1, Math.min(BOARD_HISTORY_MAX, params.limit ?? 20));
			const recent = board.slice(-n);
			const text = recent.length
				? recent
						.map((p) => `#${p.seq} ${new Date(p.ts).toISOString()} <${p.from.name}>${p.urgent ? " [URGENT]" : ""}: ${p.text}`)
						.join("\n")
				: "(board is empty)";
			return { content: [{ type: "text", text }], details: { count: recent.length } };
		},
	});

	pi.registerTool({
		name: "post_to_board",
		label: "Post to swarm board",
		description:
			"Broadcast a fact to all other agents in the swarm. Use for changes that affect shared state others depend on (renamed/moved paths, schema changes, completed migrations, claimed work).",
		promptSnippet: "Broadcast an update to the swarm that may affect other agents' work.",
		promptGuidelines: [
			"Use post_to_board whenever you make a change other agents must know about, e.g. renaming a folder, moving files, changing an API contract, or claiming a task.",
			"Keep post_to_board messages short, factual, and actionable; set urgent only when peers must react before continuing.",
		],
		parameters: Type.Object({
			text: Type.String({ description: "The fact/update to broadcast" }),
			urgent: Type.Optional(Type.Boolean({ description: "Interrupt other agents immediately instead of queueing" })),
		}),
		async execute(_id, params) {
			const post: BoardPost = {
				seq: ++boardSeq,
				from: { id: ID, name: NAME },
				text: params.text,
				urgent: Boolean(params.urgent),
				ts: Date.now(),
			};
			pub(T.board, post, { qos: 1 });
			recordBoard(post); // keep in our own history for read_board continuity
			return { content: [{ type: "text", text: `Broadcast to swarm board (#${post.seq}).` }], details: { seq: post.seq } };
		},
	});

	// -----------------------------------------------------------------------
	// Commands (entrypoints for context reset + runtime reload)
	// -----------------------------------------------------------------------
	pi.registerCommand("swarm-reset", {
		description: "Reset this agent's conversation context (start a fresh session)",
		handler: async (_args, ctx) => {
			pub(T.out, { type: "session_reset", id: ID, ts: Date.now() });
			await ctx.newSession();
		},
	});

	pi.registerCommand("swarm-reload", {
		description: "Reload extensions/skills/prompts/themes for this agent",
		handler: async (_args, ctx) => {
			pub(T.out, { type: "reloading", id: ID, ts: Date.now() });
			await ctx.reload();
			return;
		},
	});

	pi.registerCommand("swarm-status", {
		description: "Re-publish this agent's swarm registration/status",
		handler: async (_args, ctx) => {
			publishRegistry(busy ? "busy" : "online");
			if (ctx.hasUI) ctx.ui.notify(`swarm ${NAME} (${ID}) -> ${BROKER}`, "info");
		},
	});

	// -----------------------------------------------------------------------
	// System prompt: tell the agent it's in a swarm (point 7)
	// -----------------------------------------------------------------------
	pi.on("before_agent_start", async (event: any) => {
		const swarmContext = [
			"## Multi-agent swarm",
			`You are "${NAME}" (id: ${ID}), one of several pi agents working in parallel on a shared environment, coordinated over MQTT.`,
			"Other agents may change shared state (rename or move files/folders, change APIs/schemas, run migrations) while you work.",
			"- Call read_board to check recent cross-agent updates before assuming the project layout or shared state.",
			"- Call post_to_board to broadcast any change you make that could affect other agents' work; mark it urgent only if they must react immediately.",
			"- Messages prefixed with [BOARD] are broadcasts from peers; messages prefixed with [URGENT] require prompt attention.",
		].join("\n");
		return { systemPrompt: `${event.systemPrompt}\n\n${swarmContext}` };
	});

	// -----------------------------------------------------------------------
	// Lifecycle + status (points 3/4) and output mirroring (point 3)
	// -----------------------------------------------------------------------
	pi.on("session_start", async (_event: any, ctx: any) => {
		resolveIdentity();
		lastCtx = ctx;
		modelRegistry = ctx.modelRegistry ?? null;
		const m = ctx.model;
		model = m ? { provider: m.provider, id: m.id, name: m.name } : model;
		if (pi.getSessionName?.() == null) pi.setSessionName?.(NAME);
		await refreshAvailableModels(); // populate before the first registry publish
		connect(ctx);
		updateStatusLine(ctx);
	});

	pi.on("session_shutdown", async () => {
		publishRegistry("offline");
		try {
			client?.end(false);
		} catch {
			/* ignore */
		}
		client = null;
	});

	pi.on("model_select", async (event: any, ctx: any) => {
		const m = event.model;
		model = m ? { provider: m.provider, id: m.id, name: m.name } : model;
		publishRegistry(busy ? "busy" : "online");
		updateStatusLine(ctx);
		pub(T.controlOut, { id: ID, type: "model_select", model, source: event.source, ts: Date.now() });
	});

	// Mirror locally-typed (and RPC) user input over MQTT, so the orchestrator
	// sees what an operator entered directly into the TUI. Skip "extension"
	// source: those messages originate from inbound MQTT work we injected via
	// sendUserMessage, and re-publishing them would echo back to the swarm.
	pi.on("input", async (event: any) => {
		if (event?.source === "extension") return { action: "continue" };
		const text = typeof event?.text === "string" ? event.text : "";
		if (text) {
			pub(T.out, {
				type: "user_input",
				id: ID,
				text,
				source: event?.source ?? "interactive",
				streamingBehavior: event?.streamingBehavior ?? null,
				images: Array.isArray(event?.images) ? event.images.length : 0,
				ts: Date.now(),
			});
		}
		return { action: "continue" };
	});

	pi.on("agent_start", async (_event: any, ctx: any) => {
		lastCtx = ctx;
		busy = true;
		publishRegistry("busy");
		updateStatusLine(ctx);
		pub(T.out, { type: "agent_start", id: ID, ts: Date.now() });
	});

	pi.on("agent_end", async (event: any, ctx: any) => {
		busy = false;
		const lastText = lastAssistantText(event?.messages ?? []);
		pub(T.out, {
			type: "agent_end",
			id: ID,
			text: lastText,
			messageCount: event?.messages?.length ?? 0,
			ts: Date.now(),
		});
		publishRegistry("idle");
		updateStatusLine(ctx);
		flush(); // deliver anything that arrived while busy
	});

	pi.on("turn_end", async (event: any) => {
		pub(T.out, {
			type: "turn_end",
			id: ID,
			turnIndex: event?.turnIndex,
			tools: (event?.toolResults ?? []).map((r: any) => r.toolName),
			ts: Date.now(),
		});
	});
}
