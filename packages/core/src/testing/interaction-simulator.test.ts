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
import { markStoreUncoordinatedForTests } from "../engine/write-coordinator.js";
import { heavyTestTimeout } from "../engine/storage-test-helpers.js";
import {
  createDatabaseDriver,
  evaluate,
  generateInteractionPlan,
  InteractionFailure,
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
      "queuedWrite",
      "concurrent",
      "fault",
      "reopen",
      "maintenance",
      "checkpoint",
    ] satisfies Array<Interaction["kind"]>) {
      expect(kinds, kind).toContain(kind);
    }
  });

  it("adds rekeying from its own stream, leaving every plan without it unchanged", async () => {
    const options = { length: 200, connections: 4 } as const;
    const plain = generateInteractionPlan(0x1a7e, options);
    const rekeying = generateInteractionPlan(0x1a7e, { ...options, rekeying: true });
    expect(parseInteractionPlan(JSON.stringify(rekeying))).toEqual(rekeying);
    expect(plain.interactions.some((interaction) => interaction.kind === "rekey")).toBe(false);
    expect(
      rekeying.interactions.filter((interaction) => interaction.kind === "rekey").length,
    ).toBeGreaterThan(10);
    expect(
      rekeying.interactions.some(
        (interaction) =>
          interaction.kind === "createTable" &&
          interaction.table.columns.some((column) => column.references === "self"),
      ),
    ).toBe(true);
    // A plan without rekeying is exactly what the generator produced before rekeying existed,
    // so every recorded seed still replays the plan that failed. The digest was taken from
    // that generator; a change to it here means the recorded seeds now test something else.
    const digest = await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(JSON.stringify(plain)),
    );
    expect(
      [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join(""),
    ).toBe("8d289aca9f86156e4f8768d50c249e89bfe2fa2256b414c5577beaa96a1578bc");
  });

  it("adds constraints, schema changes, full text, and bursts from their own streams", () => {
    const options = { length: 300, connections: 4, tables: 3, keySpace: 24 } as const;
    const plain = generateInteractionPlan(0x5c4e, options);
    const extended = generateInteractionPlan(0x5c4e, {
      ...options,
      constraints: true,
      schemaChanges: true,
      fullText: true,
      burstCommits: 50,
    });
    expect(parseInteractionPlan(JSON.stringify(extended))).toEqual(extended);
    const kinds = new Set(extended.interactions.map((interaction) => interaction.kind));
    for (const kind of [
      "upsertUnique",
      "lookup",
      "alterTable",
      "dropIndex",
      "match",
      "burst",
    ] satisfies Array<Interaction["kind"]>) {
      expect(kinds, kind).toContain(kind);
      expect(plain.interactions.some((interaction) => interaction.kind === kind)).toBe(false);
    }
    const tables = extended.interactions.flatMap((interaction) =>
      interaction.kind === "createTable" ? [interaction.table] : [],
    );
    expect(tables.some((table) => table.columns.some((column) => column.unique))).toBe(true);
    expect(tables.some((table) => table.appendOnly === true)).toBe(true);
    expect(
      tables.some((table) => table.columns.some((column) => typeof column.references === "object")),
    ).toBe(true);
    // Each option draws from its own stream, so turning one on leaves the others' steps alone:
    // the primary stream's steps are what a plan without any of them holds.
    const primary = (plan: InteractionPlan) =>
      plan.interactions.filter((interaction) => interaction.kind === "partition").length;
    expect(primary(generateInteractionPlan(0x5c4e, { ...options, fullText: true }))).toBe(
      primary(plain),
    );
    // A burst always appears, even when the stream never drew one.
    const short = generateInteractionPlan(7, { length: 5, burstCommits: 10 });
    expect(short.interactions.filter((interaction) => interaction.kind === "burst")).toHaveLength(
      1,
    );
  });

  it("adds and drops columns only on tables nothing has written, unless populated", () => {
    const writes = new Set<Interaction["kind"]>([
      "insert",
      "update",
      "delete",
      "transaction",
      "concurrent",
      "fault",
      "upsertUnique",
      "burst",
    ]);
    const unwrittenOnly = (plan: InteractionPlan): boolean => {
      const written = new Set<string>();
      return plan.interactions.every((interaction) => {
        if (
          interaction.kind === "alterTable" &&
          !interaction.expectRefused &&
          interaction.change.kind !== "renameColumn" &&
          written.has(interaction.table)
        ) {
          return false;
        }
        if (
          writes.has(interaction.kind) &&
          "table" in interaction &&
          typeof interaction.table === "string"
        ) {
          written.add(interaction.table);
        }
        return true;
      });
    };
    const options = { length: 600, connections: 4, tables: 3, keySpace: 24 } as const;
    for (const seed of [1, 2, 3]) {
      expect(
        unwrittenOnly(generateInteractionPlan(seed, { ...options, schemaChanges: true })),
      ).toBe(true);
    }
    expect(
      [1, 2, 3].some(
        (seed) =>
          !unwrittenOnly(generateInteractionPlan(seed, { ...options, schemaChanges: "populated" })),
      ),
    ).toBe(true);
  });

  it("appends an effective fresh insert for every configured fault point", () => {
    const points = [
      "beforeBlockWrite",
      "afterBlockWrite",
      "beforeTransactionCommit",
      "afterTransactionCommit",
      "crash",
    ] as const;
    const keySpace = 20;
    const plan = generateInteractionPlan(1_349_451_771, {
      length: 160,
      connections: 3,
      tables: 2,
      keySpace,
      faultPoints: points,
    });
    const probes = plan.interactions.slice(-(points.length + 1), -1);

    expect(probes.map((interaction) => interaction.kind)).toEqual(points.map(() => "fault"));
    expect(
      probes.map((interaction) => (interaction.kind === "fault" ? interaction.point : undefined)),
    ).toEqual(points);
    expect(
      probes.map((interaction) =>
        interaction.kind === "fault" && interaction.mutation.kind === "insert"
          ? interaction.mutation.row.id
          : undefined,
      ),
    ).toEqual([21, 22, 23, 24, 25]);
    expect(plan.interactions.at(-1)).toEqual({ kind: "checkpoint" });
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

it("wraps a raw reopen failure with its interaction and SQL trace", async () => {
  const storageError = Object.assign(new Error("Segment header checksum differs"), {
    name: "StorageCorruptionError",
    backend: "opfs",
    location: "segments/0007",
  });
  const reopen = { kind: "reopen", connection: 0 } as const;
  const plan: InteractionPlan = {
    version: 1,
    seed: 24_301,
    connections: 1,
    interactions: [reopen],
  };

  let failure: unknown;
  try {
    await runInteractionPlan(plan, {
      open: () =>
        Promise.resolve({
          execute: () => Promise.resolve({ kind: "noop" }),
          query: () => Promise.resolve({ columns: [], rows: [] }),
          reopen: () => Promise.reject(storageError),
        }),
    });
  } catch (error) {
    failure = error;
  }

  expect(failure).toBeInstanceOf(InteractionFailure);
  const interactionFailure = failure as InteractionFailure;
  expect(interactionFailure.index).toBe(0);
  expect(interactionFailure.interaction).toEqual(reopen);
  expect(interactionFailure.trace).toEqual(["[0] -- reopen"]);
  expect(interactionFailure.cause).toBe(storageError);
  expect(interactionFailure.message).toContain(
    "reopen failed: StorageCorruptionError: Segment header checksum differs",
  );
  expect(interactionFailure.message).toContain("at interaction 0 (reopen)");
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
    // The interloper on connection 0 must land under the transaction: an uncoordinated writer.
    markStoreUncoordinatedForTests(store);
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

describe("expected failure identities", () => {
  type ExecuteArgs = Parameters<SimulatedConnection["execute"]>;
  type QueryArgs = Parameters<SimulatedConnection["query"]>;
  interface Overrides {
    execute?: (
      connection: SimulatedConnection,
      ...args: ExecuteArgs
    ) => ReturnType<SimulatedConnection["execute"]>;
    query?: (
      connection: SimulatedConnection,
      ...args: QueryArgs
    ) => ReturnType<SimulatedConnection["query"]>;
  }

  const table = {
    name: "t0",
    columns: [{ name: "c0", type: "integer", nullable: false }],
  } as const;
  const create = (expectExisting: boolean): Interaction => ({
    kind: "createTable",
    connection: 0,
    table,
    expectExisting,
  });
  const insert = (): Interaction => ({
    kind: "insert",
    connection: 0,
    table: "t0",
    rows: [{ id: 1, c0: 10 }],
    viaParameters: false,
  });
  const plan = (...interactions: Interaction[]): InteractionPlan => ({
    version: 1,
    seed: 3,
    connections: 1,
    interactions,
  });

  const expectRejected = async (
    interactionPlan: InteractionPlan,
    overrides: Overrides,
    expected: RegExp,
  ): Promise<void> => {
    const store = new MemoryBlockStore();
    try {
      const driver = createDatabaseDriver(store);
      await expect(
        runInteractionPlan(interactionPlan, {
          ...driver,
          open: async (index) => {
            const connection = await driver.open(index);
            const execute = overrides.execute;
            const query = overrides.query;
            return {
              ...connection,
              execute: (...args) =>
                execute === undefined ? connection.execute(...args) : execute(connection, ...args),
              query: (...args) =>
                query === undefined ? connection.query(...args) : query(connection, ...args),
            };
          },
        }),
      ).rejects.toThrow(expected);
    } finally {
      store.close();
    }
  };

  it("rejects an unrelated error on double CREATE", async () => {
    let creates = 0;
    await expectRejected(
      plan(create(false), create(true)),
      {
        execute: (connection, sql, params) => {
          if (sql.startsWith("CREATE TABLE") && ++creates === 2) {
            return Promise.reject(new Error("catalog storage unavailable"));
          }
          return connection.execute(sql, params);
        },
      },
      /creating an existing table failed with the wrong error.*catalog storage unavailable/s,
    );
  });

  it("rejects an unrelated error on a missing DROP", async () => {
    let drops = 0;
    await expectRejected(
      plan(
        create(false),
        { kind: "dropTable", connection: 0, table: "t0", expectMissing: false },
        { kind: "dropTable", connection: 0, table: "t0", expectMissing: true },
      ),
      {
        execute: (connection, sql, params) => {
          if (sql.startsWith("DROP TABLE") && ++drops === 2) {
            return Promise.reject(new Error("catalog storage unavailable"));
          }
          return connection.execute(sql, params);
        },
      },
      /dropping a missing table failed with the wrong error.*catalog storage unavailable/s,
    );
  });

  it("requires an UnknownTableError after DROP", async () => {
    await expectRejected(
      plan(create(false), { kind: "dropTable", connection: 0, table: "t0", expectMissing: false }),
      {
        query: (connection, sql, params) =>
          sql.startsWith('SELECT * FROM "t0"')
            ? Promise.reject(new Error("catalog read unavailable"))
            : connection.query(sql, params),
      },
      /querying a dropped table failed with the wrong error.*catalog read unavailable/s,
    );
  });

  it("rejects an unrelated error containing unique on a duplicate INSERT", async () => {
    let inserts = 0;
    await expectRejected(
      plan(create(false), insert(), insert()),
      {
        execute: (connection, sql, params) => {
          if (sql.startsWith("INSERT INTO") && ++inserts === 2) {
            return Promise.reject(new Error("unique storage service unavailable"));
          }
          return connection.execute(sql, params);
        },
      },
      /duplicate insert failed with the wrong error.*unique storage service unavailable/s,
    );
  });

  it("requires UniqueConstraintError for a duplicate inside a transaction", async () => {
    let inserts = 0;
    await expectRejected(
      plan(create(false), insert(), {
        kind: "transaction",
        connection: 0,
        observer: 0,
        table: "t0",
        statements: [{ kind: "insert", rows: [{ id: 1, c0: 10 }] }],
        outcome: "rollback",
      }),
      {
        execute: (connection, sql, params) => {
          if (sql.startsWith("INSERT INTO") && ++inserts === 2) {
            return Promise.reject(new Error("transaction storage unavailable"));
          }
          return connection.execute(sql, params);
        },
      },
      /duplicate insert inside a transaction failed with the wrong error.*storage unavailable/s,
    );
  });

  it("requires UniqueConstraintError for a duplicate fault step", async () => {
    let inserts = 0;
    await expectRejected(
      plan(create(false), insert(), {
        kind: "fault",
        connection: 0,
        table: "t0",
        mutation: { kind: "insert", row: { id: 1, c0: 10 } },
        point: "beforeBlockWrite",
      }),
      {
        execute: (connection, sql, params) => {
          if (sql.startsWith("INSERT INTO") && ++inserts === 2) {
            return Promise.reject(new Error("fault driver unavailable"));
          }
          return connection.execute(sql, params);
        },
      },
      /duplicate insert during a fault step failed with the wrong error.*driver unavailable/s,
    );
  });

  it("rejects unrelated conflict text as a lost transaction race", async () => {
    await expectRejected(
      plan(create(false), {
        kind: "transaction",
        connection: 0,
        observer: 0,
        table: "t0",
        statements: [{ kind: "insert", rows: [{ id: 2, c0: 20 }] }],
        outcome: "commit",
      }),
      {
        execute: async (connection, sql, params) => {
          if (sql === "COMMIT") {
            await connection.execute("ROLLBACK");
            throw new Error("conflict observer unavailable");
          }
          return connection.execute(sql, params);
        },
      },
      /COMMIT failed.*conflict observer unavailable/s,
    );
  });

  it("rejects unrelated transaction-expired text as an idle rollback", async () => {
    let transactionOpen = false;
    await expectRejected(
      plan(create(false), {
        kind: "transaction",
        connection: 0,
        observer: 0,
        table: "t0",
        statements: [{ kind: "insert", rows: [{ id: 2, c0: 20 }] }],
        outcome: "rollback",
      }),
      {
        execute: async (connection, sql, params) => {
          if (sql === "BEGIN") {
            const result = await connection.execute(sql, params);
            transactionOpen = true;
            return result;
          }
          if (transactionOpen && sql.startsWith("INSERT INTO")) {
            throw new Error("transaction expired metrics unavailable");
          }
          return connection.execute(sql, params);
        },
      },
      /INSERT inside a transaction failed.*transaction expired metrics unavailable/s,
    );
  });
});

describe("fault outcome oracle", () => {
  const faultPlan = (point: "beforeBlockWrite" | "crash"): InteractionPlan => ({
    version: 1,
    seed: 2,
    connections: 1,
    interactions: [
      {
        kind: "createTable",
        connection: 0,
        table: { name: "t0", columns: [{ name: "c0", type: "integer", nullable: false }] },
        expectExisting: false,
      },
      {
        kind: "fault",
        connection: 0,
        table: "t0",
        mutation: { kind: "insert", row: { id: 1, c0: 10 } },
        point,
      },
    ],
  });

  const rejectingDriver = (
    store: MemoryBlockStore,
    error: Error,
    point: "beforeBlockWrite" | "crash",
    recoveryError?: Error,
  ) => {
    const driver = createDatabaseDriver(store);
    let crashed = false;
    return {
      ...driver,
      ...(point === "beforeBlockWrite"
        ? {
            faults: {
              arm: () => undefined,
              disarm: () => undefined,
              fired: () => true,
            },
          }
        : {}),
      open: async (index: number) => {
        const connection = await driver.open(index);
        return {
          ...connection,
          execute: (sql: string, params?: ReadonlyArray<number | string | boolean | null>) =>
            sql.startsWith("INSERT INTO") ? Promise.reject(error) : connection.execute(sql, params),
          query: (sql: string, params?: ReadonlyArray<number | string | boolean | null>) =>
            crashed && recoveryError !== undefined
              ? Promise.reject(recoveryError)
              : connection.query(sql, params),
          ...(point === "crash"
            ? {
                crash: async () => {
                  crashed = true;
                },
              }
            : {}),
        };
      },
    };
  };

  it("rejects a generic closed error from a crash driver", async () => {
    const store = new MemoryBlockStore();
    try {
      await expect(
        runInteractionPlan(
          faultPlan("crash"),
          rejectingDriver(store, new Error("Table is closed for maintenance"), "crash"),
        ),
      ).rejects.toThrow(/crash surfaced an unexpected error.*Table is closed/s);
    } finally {
      store.close();
    }
  });

  it("rejects a conflict from a sequential injected-fault step", async () => {
    const store = new MemoryBlockStore();
    const error = new Error("Manifest changed without a competing writer");
    error.name = "WriteConflictError";
    try {
      await expect(
        runInteractionPlan(
          faultPlan("beforeBlockWrite"),
          rejectingDriver(store, error, "beforeBlockWrite"),
        ),
      ).rejects.toThrow(/beforeBlockWrite surfaced an unexpected error.*WriteConflictError/s);
    } finally {
      store.close();
    }
  });

  it("rejects an unrelated error merely containing the word injected", async () => {
    const store = new MemoryBlockStore();
    try {
      await expect(
        runInteractionPlan(
          faultPlan("beforeBlockWrite"),
          rejectingDriver(
            store,
            new Error("the storage provider injected latency into this request"),
            "beforeBlockWrite",
          ),
        ),
      ).rejects.toThrow(/beforeBlockWrite surfaced an unexpected error.*injected latency/s);
    } finally {
      store.close();
    }
  });

  it("accepts the explicit unknown-outcome identity for a crashed publishing call", async () => {
    const store = new MemoryBlockStore();
    try {
      const error = new Error("The worker ended before the commit reply");
      error.name = "DatabaseWorkerOutcomeUnknownError";
      const result = await runInteractionPlan(
        faultPlan("crash"),
        rejectingDriver(store, error, "crash"),
      );
      expect(result.faultsInjected).toBe(1);
      expect(result.interactions).toBe(2);
    } finally {
      store.close();
    }
  });

  it("does not call unrelated StorageUnresponsive text an accepted browser wedge", async () => {
    const store = new MemoryBlockStore();
    try {
      const outcome = new Error("The worker ended before the commit reply");
      outcome.name = "DatabaseWorkerOutcomeUnknownError";
      await expect(
        runInteractionPlan(
          faultPlan("crash"),
          rejectingDriver(
            store,
            outcome,
            "crash",
            new Error("StorageUnresponsive is only a diagnostic label here"),
          ),
        ),
      ).rejects.toThrow(/SELECT failed.*StorageUnresponsive is only a diagnostic label/s);
    } finally {
      store.close();
    }
  });

  it("accepts an exact StorageUnresponsiveError only during deliberate crash recovery", async () => {
    const store = new MemoryBlockStore();
    try {
      const outcome = new Error("The worker ended before the commit reply");
      outcome.name = "DatabaseWorkerOutcomeUnknownError";
      const recovery = new Error("The IndexedDB connection answered nothing");
      recovery.name = "StorageUnresponsiveError";
      const result = await runInteractionPlan(
        faultPlan("crash"),
        rejectingDriver(store, outcome, "crash", recovery),
      );
      expect(result.transientsAccepted).toBe(1);
      expect(result.stoppedBy).toBe(
        "StorageUnresponsiveError: The IndexedDB connection answered nothing",
      );
      expect(result.interactions).toBe(1);
    } finally {
      store.close();
    }
  });
});

describe("simulator result-shape oracle", () => {
  it.each([
    "missing nullable field",
    "undefined nullable field",
    "extra field",
    "reordered columns",
    "missing columns",
    "duplicate columns",
  ])("rejects %s instead of accepting a plausible row set", async (defect) => {
    const store = new MemoryBlockStore();
    const driver = createDatabaseDriver(store, {
      databaseOptions: { autoCompact: false, autoCollect: false },
    });
    const plan: InteractionPlan = {
      version: 1,
      seed: 1,
      connections: 1,
      interactions: [
        {
          kind: "createTable",
          connection: 0,
          expectExisting: false,
          table: { name: "t0", columns: [{ name: "c0", type: "integer", nullable: true }] },
        },
        {
          kind: "insert",
          connection: 0,
          table: "t0",
          rows: [{ id: 1, c0: null }],
          viaParameters: false,
        },
        { kind: "checkpoint" },
      ],
    };
    try {
      await expect(
        runInteractionPlan(plan, {
          open: async (index) => {
            const connection = await driver.open(index);
            return {
              ...connection,
              query: async (sql) => {
                const result = await connection.query(sql);
                if (defect === "reordered columns")
                  return { ...result, columns: [...result.columns].reverse() };
                if (defect === "missing columns") return { ...result, columns: [] };
                if (defect === "duplicate columns")
                  return { ...result, columns: [...result.columns, ...result.columns] };
                return {
                  ...result,
                  rows: result.rows.map((original) => {
                    const row = { ...original };
                    if (defect === "missing nullable field") delete row.c0;
                    if (defect === "undefined nullable field") row.c0 = undefined;
                    if (defect === "extra field") row.unrequested = 1;
                    return row;
                  }),
                };
              },
            };
          },
        }),
      ).rejects.toThrow(/malformed query row|query result.*columns/);
    } finally {
      store.close();
    }
  });
});

describe.each(stores)("interaction simulator with rekeying over $name", ({ source }) => {
  it.each(seedsFor("interaction-simulator-rekeying", [0x4e4b, 0xbeef]))("seed %i", async (seed) => {
    const store = source();
    try {
      const plan = generateInteractionPlan(seed, {
        length: 200,
        connections: 3,
        tables: 2,
        keySpace: 16,
        rekeying: true,
      });
      expect(plan.interactions.some((interaction) => interaction.kind === "rekey")).toBe(true);
      const result = await runInteractionPlan(
        plan,
        createDatabaseDriver(store, { databaseOptions: { rowsPerBlock: 8 } }),
      );
      expect(result.interactions).toBe(plan.interactions.length);
      expect(result.acceptedWrites).toBeGreaterThan(20);
      expect(result.rejectedConflicts).toBe(0);
    } finally {
      if (typeof store !== "function") store.close();
    }
  });
});

describe.each(stores)(
  "interaction simulator with constraints and schema changes over $name",
  ({ source }) => {
    it.each(seedsFor("interaction-simulator-schema", [0x5c4e, 0xd00d]))("seed %i", async (seed) => {
      const store = source();
      try {
        const plan = generateInteractionPlan(seed, {
          length: 200,
          connections: 3,
          tables: 3,
          keySpace: 16,
          constraints: true,
          schemaChanges: true,
          fullText: true,
          burstCommits: 60,
        });
        const result = await runInteractionPlan(
          plan,
          createDatabaseDriver(store, {
            databaseOptions: { rowsPerBlock: 8, ftsAutoIndexRows: 8 },
          }),
        );
        expect(result.interactions).toBe(plan.interactions.length);
        expect(result.acceptedWrites).toBeGreaterThan(20);
        expect(result.expectedFailures).toBeGreaterThan(0);
        expect(result.rejectedConflicts).toBe(0);
        expect(result.burstCommits).toBeGreaterThanOrEqual(30);
        // The lookups are shaped for the secondary-index path; some must have taken it.
        expect(result.indexPathQueries).toBeGreaterThan(0);
      } finally {
        if (typeof store !== "function") store.close();
      }
    });
  },
);

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
      // Every connection takes its turn as the database's writer, so concurrent rounds and
      // transactions across connections never lose a commit race.
      expect(result.rejectedConflicts).toBe(0);
      // The default plan ends with an effective insert for each of the four storage fault points;
      // the in-process driver intentionally skips only the separate browser crash point.
      expect(result.faultsInjected).toBeGreaterThanOrEqual(4);
    } finally {
      if (typeof store !== "function") store.close();
    }
  });
});
