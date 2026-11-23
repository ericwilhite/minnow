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
  type InteractionPlan,
  type InteractionPlanOptions,
  type InteractionReachState,
} from "../../testing/interaction-simulator.js";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { NEXT_OPEN_FINDINGS, NEXT_PARITY_GAPS, openFinding } from "./next-parity-gaps.js";
import { createNextDriver } from "./simulator-driver.js";

const COMMON: readonly InteractionReachState[] = [
  "update-matched",
  "delete-matched",
  "write-refused",
  "limit-truncated",
  "rollback-discarded",
  "concurrent-read",
  "reopen-with-rows",
  "read-bypassed-queued-write",
];
/**
 * The states each family's seeds must reach; faults, crashes and index-path explains do not run
 * here. Rekeying plans are judged against PostgreSQL's behaviour: next/ keeps no key forwards
 * (decision D14, the driver's `keyForwarding: false`), so a retired key reads nothing.
 */
const DECLARED: Readonly<Record<string, readonly InteractionReachState[]>> = {
  base: COMMON,
  constraints: [...COMMON, "unique-refused"],
  rekeying: [...COMMON, "rekey-moved", "rekey-refused", "retired-key-read"],
  "schema changes": [...COMMON, "alter-populated"],
  "full text": COMMON,
};

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
  const reach = createReach(`interaction simulator over next/: ${name}`, DECLARED[name] ?? []);
  afterAll(() => {
    if (process.env.MINNOW_SEED === undefined) reach.assertReached();
  });
  it.each(seeds)(
    "seed %i",
    async (seed) => {
      const plan = generateInteractionPlan(seed, options);
      // Idle transactions roll back after 2 s here (default 30 s), so a plan blocked by an
      // open finding ends promptly; nothing judged depends on the timeout's length.
      const driver = await createNextDriver({ transactionIdleTimeoutMs: 2000 });
      try {
        const result = await runInteractionPlan(plan, driver);
        reach.recordHistory(result.reached);
        families.add(name);
        for (const gap of Object.keys(result.gapsSkipped ?? {})) expect(gaps.has(gap)).toBe(true);
        if (result.stoppedByGap !== undefined) expect(gaps.has(result.stoppedByGap.gap)).toBe(true);
        else {
          // Every step judged, or skipped as a read refused by a listed gap.
          const skipped = Object.values(result.gapsSkipped ?? {}).reduce((n, k) => n + k, 0);
          expect(result.interactions + skipped).toBe(plan.interactions.length);
        }
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

/**
 * Both foreign-key sides, pinned (the generated plans rarely delete a RESTRICT-referenced parent):
 * an insert naming no parent is refused as the referencing side, and a delete of a referenced
 * parent as the referenced side. Each must be judged by its own side (PostgreSQL's 23503 "is not
 * present in table" vs "is still referenced from table"; next/'s typed `problem`).
 */
it("judges each foreign-key side by its own refusal (pinned plan)", async () => {
  const parent = { name: "p", columns: [{ name: "c0", type: "integer" as const, nullable: true }] };
  const child = {
    name: "c",
    columns: [
      {
        name: "f",
        type: "integer" as const,
        nullable: true,
        references: { table: "p", onDelete: "restrict" as const },
      },
    ],
  };
  const plan: InteractionPlan = {
    version: 1,
    seed: 0,
    connections: 1,
    interactions: [
      { kind: "createTable", connection: 0, table: parent, expectExisting: false },
      { kind: "createTable", connection: 0, table: child, expectExisting: false },
      { kind: "insert", connection: 0, table: "p", rows: [{ id: 1, c0: 1 }], viaParameters: false },
      { kind: "insert", connection: 0, table: "c", rows: [{ id: 1, f: 1 }], viaParameters: false },
      // The referencing side: no parent row 9.
      { kind: "insert", connection: 0, table: "c", rows: [{ id: 2, f: 9 }], viaParameters: true },
      // The referenced side: child 1 still references parent 1 under RESTRICT.
      {
        kind: "delete",
        connection: 0,
        table: "p",
        predicate: { kind: "compare", column: "id", op: "=", value: 1 },
      },
    ],
  };
  const driver = await createNextDriver();
  const result = await runInteractionPlan(plan, driver);
  expect(result.interactions).toBe(plan.interactions.length);
  expect(result.reached).toContain("write-refused");
});
