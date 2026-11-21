/**
 * The interaction-plan simulator's generated plans, shadow model and judgments over next/,
 * through the public owner path (simulator-driver.ts). A read refused by a listed parity gap is
 * skipped; a write refused by one stops its plan; a plan blocked by an open finding is counted
 * against it. Every other plan must run to the end. Replay a seed with MINNOW_SEED.
 */
import { afterAll, describe, expect, it } from "vitest";
import {
  generateInteractionPlan,
  runInteractionPlan,
  type InteractionPlanOptions,
} from "../../testing/interaction-simulator.js";
import { seedsFor } from "../../testing/seeds.js";
import { NEXT_OPEN_FINDINGS, NEXT_PARITY_GAPS, openFinding } from "./next-parity-gaps.js";
import { createNextDriver } from "./simulator-driver.js";

const SETS: ReadonlyArray<[string, InteractionPlanOptions, readonly number[]]> = [
  [
    "base",
    { length: 120, connections: 3, tables: 2, keySpace: 16 },
    seedsFor("interaction-simulator-next-base", [1, 2, 3, 4]),
  ],
  [
    "constraints",
    { length: 120, connections: 3, tables: 3, keySpace: 16, constraints: true },
    seedsFor("interaction-simulator-next-constraints", [1, 2, 3, 4]),
  ],
  [
    "rekeying",
    { length: 120, connections: 3, tables: 2, keySpace: 16, rekeying: true },
    seedsFor("interaction-simulator-next-rekeying", [1, 2, 3, 4]),
  ],
  [
    "schema changes",
    { length: 120, connections: 2, tables: 2, keySpace: 16, schemaChanges: true },
    seedsFor("interaction-simulator-next-schema", [1, 2, 3, 4]),
  ],
  [
    "full text",
    { length: 80, connections: 2, tables: 2, keySpace: 16, fullText: true },
    seedsFor("interaction-simulator-next-full-text", [1, 2, 3, 4]),
  ],
];
const gaps = new Set(NEXT_PARITY_GAPS.map((gap) => gap.id));
const blocked = new Map<string, number>();
const families = new Set<string>();

// Every open finding still blocks some plan: a fixed one fails here until its entry is removed
// (a single replayed seed cannot see them all).
afterAll(() => {
  if (process.env.MINNOW_SEED !== undefined || families.size < SETS.length) return;
  for (const finding of NEXT_OPEN_FINDINGS)
    expect(
      blocked.get(finding.id) ?? 0,
      `open finding ${finding.id} blocked no plan`,
    ).toBeGreaterThan(0);
});

describe.each(SETS)("interaction simulator over next/: %s", (name, options, seeds) => {
  it.each(seeds)(
    "seed %i",
    async (seed) => {
      const plan = generateInteractionPlan(seed, options);
      // Idle transactions roll back after 2 s here (default 30 s), so a plan blocked by an
      // open finding ends promptly; nothing judged depends on the timeout's length.
      const driver = await createNextDriver({ transactionIdleTimeoutMs: 2000 });
      try {
        const result = await runInteractionPlan(plan, driver);
        for (const gap of Object.keys(result.gapsSkipped ?? {})) expect(gaps.has(gap)).toBe(true);
        if (result.stoppedByGap !== undefined) expect(gaps.has(result.stoppedByGap.gap)).toBe(true);
        else {
          // Every step judged, or skipped as a read refused by a listed gap.
          const skipped = Object.values(result.gapsSkipped ?? {}).reduce((n, k) => n + k, 0);
          expect(result.interactions + skipped).toBe(plan.interactions.length);
        }
        families.add(name);
      } catch (error) {
        // Blocked by an open finding: counted, not passed. Anything else fails here.
        families.add(name);
        const finding = openFinding(error);
        if (finding === null) throw error;
        blocked.set(finding.id, (blocked.get(finding.id) ?? 0) + 1);
      }
    },
    120_000,
  );
});
