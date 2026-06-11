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
 *   NS/agents/ID/out          outbound events (turn/agent summaries, results, acks)
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
		in: `${ns}/agents/${id}/in`,
		interrupt: `${ns}/agents/${id}/interrupt`,
		control: `${ns}/agents/${id}/control`,
		out: `${ns}/agents/${id}/out`,
		board: `${ns}/board`,
	};
}

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
		const rawName = (pi.getFlag("swarm-name") as string | undefined) ?? process.env.PI_SWARM_NAME;
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

	const publishRegistry = (status: "online" | "busy" | "idle" | "offline") => {
		pub(
			T.registry,
			{
				id: ID,
				name: NAME,
				status,
				model,
				pid: process.pid,
				cwd: process.cwd(),
				startedAt,
				ts: Date.now(),
			},
			{ qos: 1, retain: true },
		);
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
	const flush = () => {
		if (busy || queue.length === 0) return;
		const batch = queue.splice(0, queue.length);
		const text =
			batch.length === 1
				? batch[0]
				: `You have ${batch.length} queued swarm messages:\n` + batch.map((m, i) => `${i + 1}. ${m}`).join("\n");
		pi.sendUserMessage(text); // idle -> triggers a fresh turn
	};

	const enqueue = (text: string, urgent: boolean) => {
		if (urgent) {
			// Deliver now; steer if mid-stream, otherwise it triggers a turn.
			pi.sendUserMessage(`[URGENT] ${text}`, busy ? { deliverAs: "steer" } : undefined);
			return;
		}
		queue.push(text);
		if (!busy) flush();
	};

	// Invoke one of our own slash commands as a user message (documented pattern
	// for reaching command-only context like newSession/reload from elsewhere).
	const invokeCommand = (name: string) => {
		pi.sendUserMessage(`/${name}`, busy ? { deliverAs: "followUp" } : undefined);
	};

	// -----------------------------------------------------------------------
	// Board helpers
	// -----------------------------------------------------------------------
	const recordBoard = (post: BoardPost) => {
		board.push(post);
		if (board.length > BOARD_HISTORY_MAX) board.splice(0, board.length - BOARD_HISTORY_MAX);
	};

	// -----------------------------------------------------------------------
	// Control actions (points 8/9/10)
	// -----------------------------------------------------------------------
	const handleControl = async (msg: any) => {
		const action = msg?.action;
		switch (action) {
			case "ping":
				publishRegistry(busy ? "busy" : "online");
				pub(T.out, { type: "pong", id: ID, ts: Date.now() });
				return;

			case "set_model": {
				if (!modelRegistry) {
					pub(T.out, { type: "set_model_result", ok: false, error: "no model registry", ts: Date.now() });
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
					pub(T.out, { type: "set_model_result", ok: false, error: "model not found", request: msg, ts: Date.now() });
					return;
				}
				const ok = await pi.setModel(target);
				pub(T.out, {
					type: "set_model_result",
					ok,
					model: ok ? { provider: target.provider, id: target.id, name: target.name } : null,
					error: ok ? undefined : "no API key for model",
					ts: Date.now(),
				});
				return;
			}

			case "reset":
				pub(T.out, { type: "ack", action: "reset", ts: Date.now() });
				invokeCommand("swarm-reset");
				return;

			case "reload":
				pub(T.out, { type: "ack", action: "reload", ts: Date.now() });
				invokeCommand("swarm-reload");
				return;

			default:
				pub(T.out, { type: "error", error: `unknown control action: ${action}`, ts: Date.now() });
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
			client!.subscribe([T.in, T.interrupt, T.control, T.board], { qos: 1 });
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

			if (topic === T.control) {
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
		modelRegistry = ctx.modelRegistry ?? null;
		const m = ctx.model;
		model = m ? { provider: m.provider, id: m.id, name: m.name } : model;
		if (pi.getSessionName?.() == null) pi.setSessionName?.(NAME);
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
		pub(T.out, { type: "model_select", model, source: event.source, ts: Date.now() });
	});

	pi.on("agent_start", async (_event: any, ctx: any) => {
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
