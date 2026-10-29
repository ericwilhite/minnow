/**
 * Declared reach for a generated campaign.
 *
 * A generated suite enforces its laws only in the states its histories actually enter. A run can
 * pass any number of seeds and still check nothing where a law matters: the generator never
 * produced a matching UPDATE, every fault hit a missing row, every live delivery was a full
 * re-run. Counting passing runs does not show that; seed 1349451771 in regression-seeds.json is
 * the case where it did not.
 *
 * A campaign declares the states its laws depend on. Each history (a seed's plan, a fast-check
 * run) starts with `beginHistory()` and calls `reach(state)` when it enters one. After the
 * campaign, `assertReached()` fails for every declared state fewer than `minimum` histories
 * entered. A history counts once per state however often it enters it.
 *
 * A `MINNOW_SEED` replay checks one history, not the campaign, so the assertion stands aside
 * then; so does a filtered run in which no history ran. The idea and the name are TanStack DB's
 * (`packages/db/tests/oracle-reach.ts`). Like ./seeds.ts, this module is excluded from the
 * published tarball.
 */
export interface Reach<State extends string> {
  /** Starts one generated history. */
  beginHistory(): void;
  /** The current history entered a declared state. */
  reach(state: State): void;
  /** Records one whole history from the states it entered, ignoring undeclared ones. */
  recordHistory(entered: Iterable<string>): void;
  /** How many histories entered each declared state. */
  counts(): ReadonlyMap<State, number>;
  /** Fails for each declared state that too few histories entered. */
  assertReached(): void;
}

export function createReach<State extends string>(
  campaign: string,
  states: readonly State[],
  { minimum = 1 }: { minimum?: number } = {},
): Reach<State> {
  const counts = new Map<State, number>(states.map((state) => [state, 0]));
  let current = new Set<State>();
  let histories = 0;
  return {
    beginHistory() {
      current = new Set();
      histories += 1;
    },
    reach(state) {
      const count = counts.get(state);
      if (count === undefined) throw new Error(`${campaign}: undeclared reach state ${state}`);
      if (histories === 0) throw new Error(`${campaign}: reach(${state}) before beginHistory()`);
      if (current.has(state)) return;
      current.add(state);
      counts.set(state, count + 1);
    },
    recordHistory(entered) {
      this.beginHistory();
      for (const state of entered) {
        if (counts.has(state as State)) this.reach(state as State);
      }
    },
    counts: () => counts,
    assertReached() {
      const replaying = (process.env.MINNOW_SEED ?? "").trim() !== "";
      if (replaying || histories === 0) return;
      const missed = [...counts]
        .filter(([, count]) => count < minimum)
        .map(([state, count]) => `${state} (${String(count)})`);
      if (missed.length > 0) {
        throw new Error(
          `${campaign}: of ${String(histories)} generated histories, fewer than ` +
            `${String(minimum)} reached these states, so their laws were not enforced there: ` +
            missed.join(", "),
        );
      }
    },
  };
}
