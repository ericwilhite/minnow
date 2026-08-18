/**
 * Seed control for the generative suites.
 *
 * The differential harnesses generate their corpora from a seeded RNG. With the seed hard-coded,
 * every run asks exactly the same thousand questions forever: that is a fine regression net and a
 * useless bug finder, because the frontier never moves. With the seed free, a run finds new
 * shapes but stops being reproducible, which is worse.
 *
 * The way out is the one Turso's simulator and SQLite's fuzzers both take. A committed run is
 * deterministic — the checked-in seeds, plus every seed that has ever failed. A soak run varies
 * the seed and prints the one that broke, and that seed is then committed to
 * `regression-seeds.json`, where it joins the deterministic set permanently. The explored space
 * only ever grows, and it grows by exactly the amount the soak discovered.
 *
 * Set `MINNOW_SEED` to replay one specific seed:
 *
 *     MINNOW_SEED=123456 npx vitest run packages/core/src/engine/sql-conformance.test.ts
 */
import regressions from "../../regression-seeds.json" with { type: "json" };

const registry = regressions as { readonly suites: Record<string, readonly number[]> };

/** Reads `MINNOW_SEED`, if this process was given one. Non-numeric values are a mistake, not a hint. */
function overrideSeed(): number | undefined {
  const raw = process.env.MINNOW_SEED;
  if (raw === undefined || raw.trim() === "") return undefined;
  const seed = Number(raw);
  if (!Number.isFinite(seed)) throw new Error(`MINNOW_SEED must be a number, got: ${raw}`);
  return Math.trunc(seed);
}

/**
 * The seeds a suite runs. `MINNOW_SEED` replaces the set entirely, so a soak failure replays on
 * its own without the rest of the corpus obscuring it. Otherwise the suite runs its own defaults
 * plus every regression seed recorded for it, de-duplicated and in a stable order.
 */
export function seedsFor(suite: string, defaults: readonly number[]): number[] {
  const override = overrideSeed();
  if (override !== undefined) return [override];
  return [...new Set([...defaults, ...(registry.suites[suite] ?? [])])];
}

/** The single seed a suite runs when it generates one corpus rather than a set of them. */
export function seedFor(suite: string, fallback: number): number {
  return seedsFor(suite, [fallback])[0] ?? fallback;
}

/** Every regression seed on file, for the soak runner's report and for the coverage assertion. */
export function regressionSeeds(suite: string): readonly number[] {
  return registry.suites[suite] ?? [];
}
