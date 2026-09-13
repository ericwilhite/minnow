/**
 * The interaction-plan simulator, run in-process over every block store the engine ships.
 *
 * A fixed seed on every commit, the recorded regression seeds after it, and the soak runner
 * exploring new ones. A failure names the interaction, the property, the SQL leading up to it,
 * and the seed to replay with MINNOW_SEED.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, OpfsBlockStore } from "../storage/index.js";
import { heavyTestTimeout } from "../engine/storage-test-helpers.js";
import {
  createDatabaseDriver,
  evaluate,
  generateInteractionPlan,
  parseInteractionPlan,
  renderPredicate,
  runInteractionPlan,
  type DriverStoreSource,
  type Interaction,
} from "./interaction-simulator.js";
import { MemoryOpfs } from "./opfs-shim.js";
import { seedsFor } from "./seeds.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

/**
 * Memory has no durable identity to share, so its connections share one instance. IndexedDB and
 * OPFS open a separate adapter instance per connection over one database, as separate tabs do.
 */
const stores: ReadonlyArray<{ name: string; source: () => DriverStoreSource }> = [
  { name: "memory", source: () => new MemoryBlockStore() },
  {
    name: "indexeddb",
    source: () => {
      const indexedDB = new IDBFactory();
      const name = crypto.randomUUID();
      return () => IndexedDbBlockStore.open({ name, indexedDB });
    },
  },
  {
    name: "opfs",
    source: () => {
      const root = new MemoryOpfs().root;
      const name = crypto.randomUUID();
      return () => OpfsBlockStore.open({ name, root });
    },
  },
];

describe("interaction plans", () => {
  it("generates the same plan from the same seed and round-trips through JSON", () => {
    const first = generateInteractionPlan(0x1a7e, { length: 200, connections: 4 });
    const second = generateInteractionPlan(0x1a7e, { length: 200, connections: 4 });
    expect(second).toEqual(first);
    expect(parseInteractionPlan(JSON.stringify(first))).toEqual(first);
    const kinds = new Set(first.interactions.map((interaction) => interaction.kind));
    // Every interaction family must appear, or a property is silently never checked.
    for (const kind of [
      "createTable",
      "insert",
      "update",
      "delete",
      "select",
      "partition",
      "unionAll",
      "createIndex",
      "dropTable",
      "transaction",
      "concurrent",
      "fault",
      "reopen",
      "maintenance",
      "checkpoint",
    ] satisfies Array<Interaction["kind"]>) {
      expect(kinds, kind).toContain(kind);
    }
  });

  it.each([
    "null",
    JSON.stringify({ version: 2, seed: 1, connections: 1, interactions: [] }),
    JSON.stringify({ version: 1, seed: 1, connections: 0, interactions: [] }),
    JSON.stringify({ version: 1, seed: 1, connections: 1, interactions: [{ kind: "nope" }] }),
    JSON.stringify({
      version: 1,
      seed: 1,
      connections: 1,
      interactions: [
        {
          kind: "select",
          connection: 3,
          table: "t",
          predicate: { kind: "literal", value: true },
          limit: null,
          descending: false,
        },
      ],
    }),
    JSON.stringify({
      version: 1,
      seed: 1,
      connections: 2,
      interactions: [
        {
          kind: "concurrent",
          table: "t",
          readers: [],
          operations: [
            { connection: 0, mutation: { kind: "deleteKey", id: 1 } },
            { connection: 1, mutation: { kind: "deleteKey", id: 1 } },
          ],
        },
      ],
    }),
  ])("rejects malformed plans: %s", (source) => {
    expect(() => parseInteractionPlan(source)).toThrow();
  });

  it("evaluates predicates with SQL three-valued logic", () => {
    const row = { id: 1, a: null, b: 3, c: "x" };
    const isNull = { kind: "isNull", column: "a", negated: false } as const;
    const compareNull = { kind: "compare", column: "a", op: "=", value: 1 } as const;
    const gt = { kind: "compare", column: "b", op: ">", value: 2 } as const;
    expect(evaluate(isNull, row)).toBe(true);
    expect(evaluate(compareNull, row)).toBeNull();
    expect(evaluate({ kind: "not", inner: compareNull }, row)).toBeNull();
    expect(evaluate({ kind: "and", left: compareNull, right: gt }, row)).toBeNull();
    expect(
      evaluate({ kind: "and", left: compareNull, right: { kind: "literal", value: false } }, row),
    ).toBe(false);
    expect(evaluate({ kind: "or", left: compareNull, right: gt }, row)).toBe(true);
    expect(
      evaluate({ kind: "or", left: compareNull, right: { kind: "literal", value: false } }, row),
    ).toBeNull();
    expect(
      renderPredicate({
        kind: "and",
        left: gt,
        right: { kind: "compare", column: "c", op: "<>", value: "it's" },
      }),
    ).toBe(`("b" > 2 AND "c" <> 'it''s')`);
  });
});

describe.each(stores)("interaction simulator over $name", ({ source }) => {
  it.each(seedsFor("interaction-simulator", [0x5eed, 0xc0ffee]))("seed %i", async (seed) => {
    const store = source();
    try {
      const plan = generateInteractionPlan(seed, {
        length: 160,
        connections: 3,
        tables: 2,
        keySpace: 20,
      });
      const result = await runInteractionPlan(
        plan,
        createDatabaseDriver(store, { databaseOptions: { rowsPerBlock: 8 } }),
      );
      expect(result.interactions).toBe(plan.interactions.length);
      expect(result.checkpoints).toBeGreaterThan(0);
      expect(result.acceptedWrites).toBeGreaterThan(20);
      expect(result.expectedFailures).toBeGreaterThan(0);
      expect(result.faultsInjected).toBeGreaterThan(0);
    } finally {
      if (typeof store !== "function") store.close();
    }
  });
});
