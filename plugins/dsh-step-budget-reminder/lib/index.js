/**
 * dsh-step-budget-reminder — a soft step-budget nudge for the ROOT agent.
 *
 * When a step budget is configured, the root agent's loop gets one notice when
 * it reaches the budget and one more when it reaches `budget * secondFactor`
 * (rounded up). The notice is appended to the *latest* position — the tail of
 * the current step's `additionalContexts` — so history is never rewritten and
 * the cached prefix stays stable. Nothing here vetoes a tool call or stops the
 * loop: this plugin only talks.
 *
 * Budget source precedence: `config.budget` > `DSH_STEP_BUDGET` (read at apply
 * time) > none. With no budget the plugin registers listeners that do nothing.
 * @module dsh-step-budget-reminder
 */
import { DEFAULT_SECOND_FACTOR, StepCounter, parseBudget } from "./budget.js";

const name = "step-budget-reminder";

/**
 * The `{kind:'plugin'}` source stamped on every notice this plugin injects —
 * the label is load-bearing (an unlabeled context would render as a user
 * prompt in derived history).
 */
const PLUGIN_SOURCE = {
	kind: "plugin",
	plugin: "step-budget-reminder",
};

/** First reminder, emitted at the budget. */
function firstReminder(steps, budget) {
	return `[step-budget] 已到步数预算（${steps}/${budget}）。请收尾：跑相关测试、写 summary、提交；做不完就在 summary 里写清剩余工作。`;
}

/** Escalated reminder, emitted at `budget * secondFactor` (rounded up). */
function secondReminder(steps, budget) {
	return `[step-budget] 已超预算 1.5 倍（${steps}/${budget}）。请立即收尾：跑相关测试、写 summary、提交；做不完就写清剩余工作。`;
}

/**
 * Build the notice message. Constructed inline (not via a dsh-llm helper)
 * because a link-installed plugin cannot resolve `@deepseek-ai/dsh-llm`.
 */
function createNotice(kind, steps, budget, secondFactor) {
	const text =
		kind === "first"
			? firstReminder(steps, budget)
			: secondReminder(steps, budget);
	const summary =
		kind === "first"
			? `step-budget reached (${steps}/${budget})`
			: `step-budget ${secondFactor}x exceeded (${steps}/${budget})`;
	return Object.freeze({
		id: crypto.randomUUID(),
		role: "user",
		content: [{ type: "text", text }],
		source: { ...PLUGIN_SOURCE, form: "notice", summary },
	});
}

/** Append our notice at the END, preserving every downstream context. */
function appendContext(ours, theirs) {
	return [...(theirs ?? []), ours];
}

/**
 * Install the listeners.
 * @param ctx - plugin context; listeners are scoped to it and disposed with it.
 * @param config - `{ budget?: number, secondFactor?: number }`.
 */
function apply(ctx, config = {}) {
	const budget = parseBudget(process.env, config);
	const secondFactor =
		typeof config.secondFactor === "number" &&
		Number.isFinite(config.secondFactor) &&
		config.secondFactor > 1
			? config.secondFactor
			: DEFAULT_SECOND_FACTOR;

	/** Per-agent counters; only the root agent is tracked. */
	const counters = new WeakMap();
	/** Per-agent reminder awaiting the next post-execute delivery. */
	const pending = new WeakMap();

	/** Root agents are those without a live parent Agent (`parentAgent`). */
	function isRoot(agent) {
		return Boolean(agent) && !agent.parentAgent;
	}

	function counterFor(agent) {
		let counter = counters.get(agent);
		if (!counter) {
			counter = new StepCounter(budget, secondFactor);
			counters.set(agent, counter);
		}
		return counter;
	}

	ctx.on("agent/pre-step", ({ agent, messages }, next) => {
		if (budget > 0 && isRoot(agent)) {
			const counter = counterFor(agent);
			const kind = counter.step();
			if (kind) {
				pending.set(agent, createNotice(kind, counter.steps, budget, secondFactor));
			}
		}
		return next();
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

export { DEFAULT_SECOND_FACTOR, apply, createNotice, name };
