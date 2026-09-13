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
  type InteractionPlan,
  type SimulatedConnection,
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

/**
 * The two refusals a SQL transaction may legitimately meet. Both discard the whole transaction,
 * so the plan step ends with the committed state rather than reporting an engine defect: the
 * runner would otherwise call a lost commit race or an idle rollback a bug, which is what a
 * crash fault in another tab (a minute of silence on its in-flight call) once looked like.
 */
describe("a refused SQL transaction", () => {
  const plan: InteractionPlan = {
    version: 1,
    seed: 1,
    connections: 2,
    interactions: [
      {
        kind: "createTable",
        connection: 0,
        table: { name: "t0", columns: [{ name: "c0", type: "integer", nullable: true }] },
        expectExisting: false,
      },
      {
        kind: "insert",
        connection: 0,
        table: "t0",
        rows: [{ id: 1, c0: 10 }],
        viaParameters: false,
      },
      {
        kind: "transaction",
        connection: 1,
        observer: 0,
        table: "t0",
        statements: [{ kind: "insert", rows: [{ id: 2, c0: 20 }] }],
        outcome: "commit",
      },
      {
        kind: "select",
        connection: 0,
        table: "t0",
        predicate: { kind: "literal", value: true },
        limit: null,
        descending: false,
      },
    ],
  };

  it("counts a lost commit race and keeps going", async () => {
    const store = new MemoryBlockStore();
    try {
      const driver = createDatabaseDriver(store);
      const connections: SimulatedConnection[] = [];
      const result = await runInteractionPlan(plan, {
        ...driver,
        open: async (index) => {
          const connection = await driver.open(index);
          connections[index] = connection;
          return {
            ...connection,
            execute: async (sql, params) => {
              // Another connection publishes a data commit while this transaction has writes
              // staged. The values do not change, so the shadow model still describes the table.
              if (sql === "COMMIT") {
                await connections[0]?.execute(`UPDATE "t0" SET "c0" = "c0" WHERE "id" = 1`);
              }
              return connection.execute(sql, params);
            },
          };
        },
      });
      expect(result.rejectedConflicts).toBe(1);
      expect(result.acceptedWrites).toBe(1);
    } finally {
      store.close();
    }
  });

  it("acknowledges an idle rollback and keeps going", async () => {
    const store = new MemoryBlockStore();
    try {
      // One millisecond of idleness is all this transaction is allowed, and the connection waits
      // twenty after BEGIN, so its first staged statement meets a transaction already rolled back.
      const driver = createDatabaseDriver(store, {
        databaseOptions: { transactionIdleTimeoutMs: 1 },
      });
      const result = await runInteractionPlan(plan, {
        ...driver,
        open: async (index) => {
          const connection = await driver.open(index);
          return {
            ...connection,
            execute: async (sql, params) => {
              const executed = await connection.execute(sql, params);
              if (sql === "BEGIN") await new Promise((resolve) => setTimeout(resolve, 20));
              return executed;
            },
          };
        },
      });
      expect(result.expectedFailures).toBe(1);
      expect(result.rejectedConflicts).toBe(0);
    } finally {
      store.close();
    }
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
