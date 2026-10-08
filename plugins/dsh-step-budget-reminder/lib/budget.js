/**
 * Pure step-budget logic for dsh-step-budget-reminder: budget resolution,
 * threshold computation, and threshold crossing detection. No cordis, no I/O —
 * everything here is directly unit-testable.
 * @module dsh-step-budget-reminder/budget
 */

/** Default multiplier for the second (escalated) reminder. */
export const DEFAULT_SECOND_FACTOR = 1.5;

/**
 * Parse a single budget value: a positive integer, otherwise "no budget" (0).
 * @param value - raw candidate (`config.budget` or an environment string).
 * @returns the parsed budget, or 0 when absent/invalid/non-positive.
 */
export function parseBudgetValue(value) {
	if (typeof value === "number") {
		return Number.isInteger(value) && value > 0 ? value : 0;
	}
	if (typeof value === "string") {
		const trimmed = value.trim();
		if (!/^\d+$/.test(trimmed)) return 0;
		const parsed = Number.parseInt(trimmed, 10);
		return Number.isInteger(parsed) && parsed > 0 ? parsed : 0;
	}
	return 0;
}

/**
 * Resolve the effective budget: `config.budget` wins, then the
 * `DSH_STEP_BUDGET` environment variable at apply time, then none.
 * @param env - environment source (defaults to `process.env`).
 * @param config - plugin config; only `budget` is read.
 * @returns the effective budget; 0 means "no budget — stay inert".
 */
export function parseBudget(env, config) {
	const fromConfig = parseBudgetValue(config?.budget);
	if (fromConfig > 0) return fromConfig;
	return parseBudgetValue(env?.DSH_STEP_BUDGET);
}

/**
 * Compute the two reminder thresholds: the budget itself and the escalated
 * multiple, rounded up so it is always an integer step count.
 * @param budget - effective budget (> 0).
 * @param factor - second-threshold multiplier (default 1.5).
 * @returns `[first, second]`, or an empty list when there is no budget.
 */
export function thresholds(budget, factor = DEFAULT_SECOND_FACTOR) {
	if (!Number.isInteger(budget) || budget <= 0) return [];
	const effectiveFactor =
		typeof factor === "number" && Number.isFinite(factor) && factor > 1
			? factor
			: DEFAULT_SECOND_FACTOR;
	const second = Math.ceil(budget * effectiveFactor);
	return second > budget ? [budget, second] : [budget];
}

/**
 * Per-agent step counter that reports each threshold exactly once, in order.
 * A budget of 0 yields {@link StepCounter.NONE} forever (the plugin is inert).
 */
export class StepCounter {
	/** Sentinel returned when no reminder is due this step. */
	static NONE = null;

	#marks;
	#delivered = new Set();
	#steps = 0;

	/**
	 * @param budget - effective budget; 0 disables the counter.
	 * @param factor - second-threshold multiplier (default 1.5).
	 */
	constructor(budget, factor = DEFAULT_SECOND_FACTOR) {
		this.#marks = thresholds(budget, factor).map((at, index) => ({
			at,
			kind: index === 0 ? "first" : "second",
		}));
	}

	/** Steps seen so far. */
	get steps() {
		return this.#steps;
	}

	/** Thresholds this counter can still report, in ascending order. */
	get pendingMarks() {
		return this.#marks.filter((mark) => !this.#delivered.has(mark.kind));
	}

	/**
	 * Register one loop step.
	 * @returns `'first'`, `'second'`, or `null` when nothing is due.
	 */
	step() {
		this.#steps += 1;
		const mark = this.#marks.find(
			(candidate) =>
				!this.#delivered.has(candidate.kind) && this.#steps >= candidate.at,
		);
		if (!mark) return StepCounter.NONE;
		this.#delivered.add(mark.kind);
		return mark.kind;
	}
}
