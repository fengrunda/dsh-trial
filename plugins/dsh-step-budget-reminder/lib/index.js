/**
 * dsh-step-budget-reminder — soft reminders + hard tool block at 2× (G1).
 *
 * Soft: notice at budget and budget*secondFactor (default 1.5).
 * Hard: at budget*hardFactor (default 2) inject paused_limit notice and
 * block subsequent tool calls (except write/edit so the agent can land summary).
 * @module dsh-step-budget-reminder
 */
import {
	DEFAULT_HARD_FACTOR,
	DEFAULT_SECOND_FACTOR,
	StepCounter,
	parseBudget,
} from "./budget.js";

const name = "step-budget-reminder";

const PLUGIN_SOURCE = {
	kind: "plugin",
	plugin: "step-budget-reminder",
};

function firstReminder(steps, budget) {
	return `[step-budget] 已到步数预算（${steps}/${budget}）。请收尾：跑相关测试、写 summary、提交；做不完就在 summary 里写清剩余工作。`;
}

function secondReminder(steps, budget) {
	return `[step-budget] 已超预算 1.5 倍（${steps}/${budget}）。请立即收尾：跑相关测试、写 summary、提交；做不完就写清剩余工作。`;
}

function hardReminder(steps, budget) {
	return `[step-budget] 已到硬阈值（${steps}/${budget}，约 2×）。停止探索性工具调用；立即写 summary，机器块 status=paused_limit，notes 写清已用步数与剩余工作。broker 会交监理五选一，不会机械 closeout。`;
}

function createNotice(kind, steps, budget, secondFactor) {
	let text;
	let summary;
	if (kind === "first") {
		text = firstReminder(steps, budget);
		summary = `step-budget reached (${steps}/${budget})`;
	} else if (kind === "second") {
		text = secondReminder(steps, budget);
		summary = `step-budget ${secondFactor}x exceeded (${steps}/${budget})`;
	} else {
		text = hardReminder(steps, budget);
		summary = `step-budget hard cap (${steps}/${budget}) → paused_limit`;
	}
	return Object.freeze({
		id: crypto.randomUUID(),
		role: "user",
		content: [{ type: "text", text }],
		source: { ...PLUGIN_SOURCE, form: "notice", summary },
	});
}

function appendContext(ours, theirs) {
	return [...(theirs ?? []), ours];
}

/** Tools still allowed after hard: land the paused_limit summary. */
function isWrapUpTool(exec) {
	const n = String(exec?.name || exec?.toolName || "").toLowerCase();
	return n === "write" || n === "edit" || n === "design_pack_read";
}

function apply(ctx, config) {
	const cfg = config !== null && typeof config === "object" ? config : {};
	const budget = parseBudget(process.env, cfg);
	const secondFactor =
		typeof cfg.secondFactor === "number" &&
		Number.isFinite(cfg.secondFactor) &&
		cfg.secondFactor > 1
			? cfg.secondFactor
			: DEFAULT_SECOND_FACTOR;
	const hardFactor =
		typeof cfg.hardFactor === "number" && Number.isFinite(cfg.hardFactor)
			? cfg.hardFactor
			: DEFAULT_HARD_FACTOR;

	const counters = new WeakMap();
	const pending = new WeakMap();
	const hardAgents = new WeakSet();

	function isRoot(agent) {
		return Boolean(agent) && !agent.parentAgent;
	}

	function counterFor(agent) {
		let counter = counters.get(agent);
		if (!counter) {
			counter = new StepCounter(budget, secondFactor, hardFactor);
			counters.set(agent, counter);
		}
		return counter;
	}

	ctx.on("agent/pre-step", ({ agent, messages }, next) => {
		if (budget > 0 && isRoot(agent)) {
			const counter = counterFor(agent);
			const kind = counter.step();
			if (kind) {
				pending.set(
					agent,
					createNotice(kind, counter.steps, budget, secondFactor),
				);
				if (kind === "hard") hardAgents.add(agent);
			}
			if (counter.hardReached) hardAgents.add(agent);
		}
		return next();
	});

	ctx.on("tools/pre-execute", async (exec, next) => {
		if (!isRoot(exec.agent) || !hardAgents.has(exec.agent)) {
			return next();
		}
		if (isWrapUpTool(exec)) {
			return next();
		}
		const notice = createNotice("hard", counterFor(exec.agent).steps, budget, secondFactor);
		return {
			kind: "block",
			feedback:
				"step-budget hard: 停止该工具。请只写 summary（status=paused_limit）后停止。",
			additionalContexts: [notice],
		};
	});

	ctx.on("tools/post-execute", async (exec, _result, next) => {
		const downstream = await next();
		const reminder = isRoot(exec.agent) ? pending.get(exec.agent) : void 0;
		if (!reminder) return downstream;
		pending.delete(exec.agent);
		if (downstream.kind === "block") {
			return {
				kind: "block",
				feedback: downstream.feedback,
				additionalContexts: appendContext(
					reminder,
					downstream.additionalContexts,
				),
			};
		}
		return {
			...downstream,
			additionalContexts: appendContext(reminder, downstream.additionalContexts),
		};
	});
}

export {
	DEFAULT_HARD_FACTOR,
	DEFAULT_SECOND_FACTOR,
	apply,
	createNotice,
	name,
};
