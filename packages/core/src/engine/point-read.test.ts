/**
 * The keyed point-read fast path answers a single-table key-covering equality conjunction
 * without the vector pipeline, so its one obligation is exactness: for every statement it
 * serves, the answer must be byte-identical to the ordinary executor's, and for everything
 * else it must fall back rather than guess. These tests run each statement twice — fast path
 * enabled and disabled — and compare complete results, including the shapes that must fall
 * back (mutation histories, views, type coercion, NULL parameters) and the canonical errors
 * the ordinary path owns.
 */
import { afterEach, describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase, type QueryOptions } from "./database.js";
import type { QueryValue } from "./query.js";
import { pointReadTestHooks } from "./point-read.js";

afterEach(() => {
  pointReadTestHooks.disabled = false;
});

/** Runs the statement under both executors and requires identical complete results. */
async function differential(
  database: MinnowDatabase,
  sql: string,
  params: QueryValue[] = [],
  options: Omit<QueryOptions, "params"> = {},
): Promise<{ served: boolean; rows: Array<Record<string, QueryValue>> }> {
  pointReadTestHooks.disabled = false;
  const servedBefore = pointReadTestHooks.served;
  const fast = await database.query(sql, { ...options, params, memoize: false });
  const served = pointReadTestHooks.served > servedBefore;
  pointReadTestHooks.disabled = true;
  const ordinary = await database.query(sql, { ...options, params, memoize: false });
  pointReadTestHooks.disabled = false;
  expect(fast.columns).toEqual(ordinary.columns);
  expect(fast.columnDomains).toEqual(ordinary.columnDomains);
  expect(fast.rows).toEqual(ordinary.rows);
  return { served, rows: fast.rows };
}

/** Both executors must reject the statement with the same error message. */
async function differentialError(
  database: MinnowDatabase,
  sql: string,
  params: QueryValue[] = [],
): Promise<void> {
  pointReadTestHooks.disabled = false;
  const fast = await database.query(sql, { params, memoize: false }).then(
    () => undefined,
    (error: unknown) => String(error),
  );
  pointReadTestHooks.disabled = true;
  const ordinary = await database.query(sql, { params, memoize: false }).then(
    () => undefined,
    (error: unknown) => String(error),
  );
  pointReadTestHooks.disabled = false;
  expect(fast).toBeDefined();
  expect(fast).toBe(ordinary);
}

async function compositeDatabase(options: { rowsPerBlock?: number } = {}): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore(), options);
  await database.execute(
    "CREATE TABLE orders (tenant TEXT NOT NULL, id INTEGER NOT NULL, payload TEXT NOT NULL, active BOOLEAN NOT NULL, placed TIMESTAMP, note TEXT, PRIMARY KEY (tenant, id))",
  );
  await database.insertBatch(
    "orders",
    Array.from({ length: 500 }, (_, index) => ({
      tenant: `tenant-${String(index % 7)}`,
      id: index + 1,
      payload: `payload-${String(index + 1)}`,
      active: index % 3 === 0,
      placed: index % 5 === 0 ? null : new Date(Date.UTC(2026, 0, 1 + (index % 28))),
      note: index % 4 === 0 ? null : `note-${String(index % 11)}`,
    })),
  );
  return database;
}

async function scalarDatabase(): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore());
  await database.createTable({
    name: "users",
    uniqueKey: "email",
    columns: [
      { name: "email", type: "string" },
      { name: "score", type: "number" },
      { name: "joined", type: "datetime", nullable: true },
    ],
  });
  await database.insertBatch(
    "users",
    Array.from({ length: 200 }, (_, index) => ({
      email: `user-${String(index)}@example.com`,
      score: (index * 37) % 100,
      joined: index % 6 === 0 ? null : new Date(Date.UTC(2025, index % 12, 1)),
    })),
  );
  return database;
}

describe("point-read fast path", () => {
  it("serves a composite-key lookup identically to the ordinary executor", async () => {
    const database = await compositeDatabase();
    const { served, rows } = await differential(
      database,
      "SELECT payload, active, placed FROM orders WHERE tenant = ? AND id = ?",
      ["tenant-2", 3],
    );
    expect(served).toBe(true);
    expect(rows).toHaveLength(1);
    await database.close();
  });

  it("serves hits and misses across every parameter position", async () => {
    const database = await compositeDatabase();
    for (const [tenant, id] of [
      ["tenant-0", 1],
      ["tenant-1", 2],
      ["tenant-6", 499],
      ["tenant-0", 500],
      ["missing", 3],
      ["tenant-2", 100_000],
    ] as const) {
      const { served } = await differential(
        database,
        "SELECT payload FROM orders WHERE tenant = ? AND id = ?",
        [tenant, id],
      );
      expect(served).toBe(true);
    }
    await database.close();
  });

  it("serves scalar string keys through the dictionary path", async () => {
    const database = await scalarDatabase();
    const hit = await differential(
      database,
      "SELECT email, score, joined FROM users WHERE email = ?",
      ["user-42@example.com"],
    );
    expect(hit.served).toBe(true);
    expect(hit.rows).toHaveLength(1);
    const miss = await differential(database, "SELECT score FROM users WHERE email = ?", [
      "nobody@example.com",
    ]);
    expect(miss.served).toBe(true);
    expect(miss.rows).toHaveLength(0);
    await database.close();
  });

  it("matches the ordinary executor across multi-block segments", async () => {
    const database = await compositeDatabase({ rowsPerBlock: 64 });
    for (const id of [1, 63, 64, 65, 128, 400, 500]) {
      const { served, rows } = await differential(
        database,
        "SELECT payload, note FROM orders WHERE tenant = ? AND id = ?",
        [`tenant-${String((id - 1) % 7)}`, id],
      );
      expect(served).toBe(true);
      expect(rows).toHaveLength(1);
    }
    await database.close();
  });

  it("handles literal predicates, reversed operands, aliases, and extra equalities", async () => {
    const database = await compositeDatabase();
    const shapes: Array<[string, QueryValue[]]> = [
      ["SELECT payload FROM orders WHERE tenant = 'tenant-2' AND id = 3", []],
      ["SELECT payload FROM orders WHERE 'tenant-2' = tenant AND 3 = id", []],
      ["SELECT o.payload AS p FROM orders AS o WHERE o.tenant = ? AND o.id = ?", ["tenant-2", 3]],
      ["SELECT o.payload FROM orders AS o WHERE o.tenant = ? AND id = ?", ["tenant-2", 3]],
      [
        "SELECT payload FROM orders WHERE tenant = ? AND id = ? AND active = ?",
        ["tenant-0", 1, true],
      ],
      [
        "SELECT payload FROM orders WHERE tenant = ? AND id = ? AND active = ?",
        ["tenant-0", 1, false],
      ],
      [
        "SELECT payload FROM orders WHERE tenant = ? AND id = ? AND payload = ?",
        ["tenant-2", 3, "payload-3"],
      ],
      ["SELECT id, id AS twice FROM orders WHERE tenant = ? AND id = ?", ["tenant-2", 3]],
      ["SELECT payload FROM orders WHERE id = ? AND id = ? AND tenant = ?", [3, 3, "tenant-2"]],
      ["SELECT payload FROM orders WHERE id = ? AND id = ? AND tenant = ?", [3, 4, "tenant-2"]],
    ];
    let servedCount = 0;
    for (const [sql, params] of shapes) {
      const { served } = await differential(database, sql, params);
      if (served) servedCount += 1;
    }
    expect(servedCount).toBe(shapes.length);
    await database.close();
  });

  it("compares datetime equality by instant", async () => {
    const database = await scalarDatabase();
    const { served } = await differential(
      database,
      "SELECT email FROM users WHERE email = ? AND joined = ?",
      ["user-1@example.com", new Date(Date.UTC(2025, 1, 1))],
    );
    expect(served).toBe(true);
    await database.close();
  });

  it("falls back on NULL parameters and reproduces cross-type comparison errors", async () => {
    const database = await compositeDatabase();
    for (const params of [
      [null, 3],
      ["tenant-2", null],
    ] satisfies QueryValue[][]) {
      const { served, rows } = await differential(
        database,
        "SELECT payload FROM orders WHERE tenant = ? AND id = ?",
        params,
      );
      expect(served).toBe(false);
      expect(rows).toEqual([]);
    }
    // The engine rejects cross-type comparisons; the fast path must fall back so the
    // ordinary executor reports the same error, never silently answering an empty result.
    await differentialError(
      database,
      "SELECT payload FROM orders WHERE tenant = ? AND id = ?",
      [3, 3],
    );
    await differentialError(database, "SELECT payload FROM orders WHERE tenant = ? AND id = ?", [
      "tenant-2",
      "3",
    ]);
    await differentialError(
      database,
      "SELECT payload FROM orders WHERE tenant = ? AND id = ? AND active = ?",
      ["tenant-0", 1, 1],
    );
    await database.close();
  });

  it("falls back once a table carries update or delete deltas, and returns after compaction", async () => {
    const database = await compositeDatabase();
    await database.execute("UPDATE orders SET payload = 'patched' WHERE tenant = ? AND id = ?", [
      "tenant-2",
      3,
    ]);
    const afterUpdate = await differential(
      database,
      "SELECT payload FROM orders WHERE tenant = ? AND id = ?",
      ["tenant-2", 3],
    );
    expect(afterUpdate.served).toBe(false);
    expect(afterUpdate.rows).toEqual([{ payload: "patched" }]);
    await database.execute("DELETE FROM orders WHERE tenant = ? AND id = ?", ["tenant-0", 1]);
    const afterDelete = await differential(
      database,
      "SELECT payload FROM orders WHERE tenant = ? AND id = ?",
      ["tenant-0", 1],
    );
    expect(afterDelete.served).toBe(false);
    expect(afterDelete.rows).toEqual([]);
    await database.compactTable("orders");
    const afterCompaction = await differential(
      database,
      "SELECT payload FROM orders WHERE tenant = ? AND id = ?",
      ["tenant-2", 3],
    );
    expect(afterCompaction.served).toBe(true);
    expect(afterCompaction.rows).toEqual([{ payload: "patched" }]);
    await database.close();
  });

  it("observes new rows immediately after an insert", async () => {
    const database = await compositeDatabase();
    await database.insert("orders", {
      tenant: "tenant-new",
      id: 9_999,
      payload: "fresh",
      active: true,
      placed: null,
      note: null,
    });
    const { served, rows } = await differential(
      database,
      "SELECT payload FROM orders WHERE tenant = ? AND id = ?",
      ["tenant-new", 9_999],
    );
    expect(served).toBe(true);
    expect(rows).toEqual([{ payload: "fresh" }]);
    await database.close();
  });

  it("falls back for views, keyless tables, and ineligible statement shapes", async () => {
    const database = await compositeDatabase();
    await database.createView(
      "order_view",
      "SELECT tenant, id, payload FROM orders WHERE active = TRUE",
    );
    await database.execute("CREATE TABLE keyless (a INTEGER, b TEXT)");
    await database.insertBatch("keyless", [
      { a: 1, b: "x" },
      { a: 1, b: "y" },
    ]);
    const ineligible: Array<[string, QueryValue[]]> = [
      ["SELECT payload FROM order_view WHERE tenant = ? AND id = ?", ["tenant-2", 3]],
      ["SELECT b FROM keyless WHERE a = ?", [1]],
      ["SELECT payload FROM orders WHERE tenant = ? AND id > ?", ["tenant-2", 2]],
      ["SELECT payload FROM orders WHERE tenant = ? OR id = ?", ["tenant-2", 3]],
      ["SELECT COUNT(*) AS n FROM orders WHERE tenant = ? AND id = ?", ["tenant-2", 3]],
      ["SELECT payload FROM orders WHERE tenant = ? AND id = ? LIMIT 1", ["tenant-2", 3]],
      ["SELECT payload FROM orders WHERE tenant = ? AND id = ? ORDER BY id", ["tenant-2", 3]],
      ["SELECT DISTINCT payload FROM orders WHERE tenant = ? AND id = ?", ["tenant-2", 3]],
      ["SELECT * FROM orders WHERE tenant = ? AND id = ?", ["tenant-2", 3]],
      ["SELECT UPPER(payload) AS p FROM orders WHERE tenant = ? AND id = ?", ["tenant-2", 3]],
      ["SELECT payload FROM orders WHERE tenant = ?", ["tenant-2"]],
      ["SELECT payload FROM orders WHERE id = ?", [3]],
    ];
    for (const [sql, params] of ineligible) {
      const { served } = await differential(database, sql, params);
      expect(served).toBe(false);
    }
    await database.close();
  });

  it("keeps the ordinary executor's canonical errors", async () => {
    const database = await compositeDatabase();
    await differentialError(database, "SELECT nope FROM orders WHERE tenant = ? AND id = ?", [
      "tenant-2",
      3,
    ]);
    await differentialError(database, "SELECT payload FROM missing WHERE a = ? AND b = ?", [1, 2]);
    await differentialError(database, "SELECT payload FROM orders WHERE tenant = ? AND id = ?", [
      "tenant-2",
    ]);
    await database.close();
  });

  it("falls back after ALTER TABLE ADD COLUMN leaves older segments without the column", async () => {
    const database = await compositeDatabase();
    await database.execute("ALTER TABLE orders ADD COLUMN extra TEXT");
    const older = await differential(
      database,
      "SELECT payload, extra FROM orders WHERE tenant = ? AND id = ?",
      ["tenant-2", 3],
    );
    expect(older.rows).toEqual([{ payload: "payload-3", extra: null }]);
    await database.insert("orders", {
      tenant: "tenant-new",
      id: 10_000,
      payload: "widened",
      active: false,
      placed: null,
      note: null,
      extra: "present",
    });
    const newer = await differential(
      database,
      "SELECT payload, extra FROM orders WHERE tenant = ? AND id = ?",
      ["tenant-new", 10_000],
    );
    expect(newer.rows).toEqual([{ payload: "widened", extra: "present" }]);
    await database.close();
  });

  it("reads its own committed writes inside and outside statement transactions", async () => {
    const database = await compositeDatabase();
    await database.execute("BEGIN");
    await database.execute(
      "INSERT INTO orders (tenant, id, payload, active, placed, note) VALUES (?, ?, ?, ?, ?, ?)",
      ["tenant-tx", 20_000, "staged", true, null, null],
    );
    // Inside the scope the session executor answers; the fast path must not intercept it.
    const staged = await database.query("SELECT payload FROM orders WHERE tenant = ? AND id = ?", {
      params: ["tenant-tx", 20_000],
      memoize: false,
    });
    expect(staged.rows).toEqual([{ payload: "staged" }]);
    await database.execute("ROLLBACK");
    const rolledBack = await differential(
      database,
      "SELECT payload FROM orders WHERE tenant = ? AND id = ?",
      ["tenant-tx", 20_000],
    );
    expect(rolledBack.rows).toEqual([]);
    await database.close();
  });

  it("stays exact under randomized lookups against a randomized table", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 128 });
    await database.execute(
      "CREATE TABLE random_data (bucket TEXT NOT NULL, seq INTEGER NOT NULL, value DOUBLE PRECISION NOT NULL, flag BOOLEAN NOT NULL, PRIMARY KEY (bucket, seq))",
    );
    let seed = 0x2f6e2b1;
    const nextRandom = (): number => {
      // Deterministic xorshift so a failure reproduces.
      seed ^= seed << 13;
      seed ^= seed >>> 17;
      seed ^= seed << 5;
      return (seed >>> 0) / 0xffffffff;
    };
    const rows = Array.from({ length: 1_000 }, (_, index) => ({
      bucket: `b-${String(Math.floor(nextRandom() * 20))}`,
      seq: index,
      value: Math.round(nextRandom() * 1_000) / 4,
      flag: nextRandom() < 0.5,
    }));
    await database.insertBatch("random_data", rows);
    for (let index = 0; index < 200; index += 1) {
      const row = rows[Math.floor(nextRandom() * rows.length)];
      if (row === undefined) continue;
      const bucket = nextRandom() < 0.8 ? row.bucket : `b-${String(Math.floor(nextRandom() * 25))}`;
      const seq = nextRandom() < 0.8 ? row.seq : Math.floor(nextRandom() * 1_500);
      const { served } = await differential(
        database,
        "SELECT bucket, seq, value, flag FROM random_data WHERE bucket = ? AND seq = ?",
        [bucket, seq],
      );
      expect(served).toBe(true);
    }
    await database.close();
  });
});
