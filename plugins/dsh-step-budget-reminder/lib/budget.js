/**
 * Pure step-budget logic for dsh-step-budget-reminder.
 * Soft reminders at budget and budget*secondFactor; hard mark at budget*hardFactor.
 * @module dsh-step-budget-reminder/budget
 */

export const DEFAULT_SECOND_FACTOR = 1.5;
/** Default hard threshold multiplier (G1): 2× budget. */
export const DEFAULT_HARD_FACTOR = 2;

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

export function parseBudget(env, config) {
	const fromConfig = parseBudgetValue(config?.budget);
	if (fromConfig > 0) return fromConfig;
	return parseBudgetValue(env?.DSH_STEP_BUDGET);
}

/**
 * Soft thresholds only (first + second). Hard is separate.
 * @returns `[first, second?]`
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

/** Hard step count (ceil), or 0 when disabled. */
export function hardThreshold(budget, hardFactor = DEFAULT_HARD_FACTOR) {
	if (!Number.isInteger(budget) || budget <= 0) return 0;
	const f =
		typeof hardFactor === "number" && Number.isFinite(hardFactor) && hardFactor > 1
			? hardFactor
			: DEFAULT_HARD_FACTOR;
	return Math.ceil(budget * f);
}

export class StepCounter {
	static NONE = null;

	#marks;
	#hardAt;
	#delivered = new Set();
	#steps = 0;
	#hardFired = false;

	/**
	 * @param budget
	 * @param factor - soft second multiplier
	 * @param hardFactor - hard multiplier (default 2); <=1 disables hard
	 */
	constructor(budget, factor = DEFAULT_SECOND_FACTOR, hardFactor = DEFAULT_HARD_FACTOR) {
		this.#marks = thresholds(budget, factor).map((at, index) => ({
			at,
			kind: index === 0 ? "first" : "second",
		}));
		this.#hardAt =
			typeof hardFactor === "number" && hardFactor > 1
				? hardThreshold(budget, hardFactor)
				: 0;
	}

	get steps() {
		return this.#steps;
	}

	get hardReached() {
		return this.#hardFired;
	}

	get pendingMarks() {
		return this.#marks.filter((mark) => !this.#delivered.has(mark.kind));
	}

	/**
	 * @returns `'first' | 'second' | 'hard' | null`
	 * hard is reported once when steps >= hardAt (after soft marks for the same step).
	 */
	step() {
		this.#steps += 1;
		const mark = this.#marks.find(
			(candidate) =>
				!this.#delivered.has(candidate.kind) && this.#steps >= candidate.at,
		);
		if (mark) {
			this.#delivered.add(mark.kind);
			return mark.kind;
		}
		if (this.#hardAt > 0 && !this.#hardFired && this.#steps >= this.#hardAt) {
			this.#hardFired = true;
			return "hard";
		}
		return StepCounter.NONE;
	}
}
