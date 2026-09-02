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

const MOODS = ["sad", "ok", "happy"] as const;

/** A table where every non-key projection carries a logical PostgreSQL domain. */
async function domainDatabase(options: { rowsPerBlock?: number } = {}): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore(), options);
  await database.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
  await database.execute(
    "CREATE TABLE payments (id INTEGER NOT NULL PRIMARY KEY, amount NUMERIC(10, 2), " +
      "loose NUMERIC, ident UUID, document JSONB, booked DATE, at TIME, span INTERVAL, " +
      "feeling mood, choices INTEGER[], note TEXT)",
  );
  await database.insertBatch(
    "payments",
    Array.from({ length: 240 }, (_, index) => ({
      id: index + 1,
      amount: index % 9 === 0 ? null : `${String(index)}.5`,
      loose: index % 7 === 0 ? null : `${String(index)}.125`,
      ident: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
      document: `{"seq":${String(index)},"tag":"p-${String(index)}"}`,
      booked: `2026-01-${String((index % 28) + 1).padStart(2, "0")}`,
      at: index % 5 === 0 ? null : "12:34:56",
      span: "1 month 2 days",
      feeling: MOODS[index % 3] ?? "sad",
      choices: `[${String(index)},${String(index + 1)}]`,
      note: `note-${String(index)}`,
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
    // A string beside the numeric key reads as a number, as PostgreSQL types an untyped
    // literal by its context; fast path and ordinary executor must answer the same row.
    const coerced = await differential(
      database,
      "SELECT payload FROM orders WHERE tenant = ? AND id = ?",
      ["tenant-2", "3"],
    );
    expect(coerced.rows).toHaveLength(1);
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
      ["SELECT UPPER(payload) AS p FROM orders WHERE tenant = ? AND id = ?", ["tenant-2", 3]],
      ["SELECT payload FROM orders WHERE tenant = ?", ["tenant-2"]],
      ["SELECT payload FROM orders WHERE id = ?", [3]],
    ];
    for (const [sql, params] of ineligible) {
      const { served } = await differential(database, sql, params);
      expect(served).toBe(false);
    }
    // The commonest point lookup spelling: the wildcard expands from the catalog at serving
    // time, in declaration order, with the same columns the ordinary executor projects.
    const wildcard = await differential(
      database,
      "SELECT * FROM orders WHERE tenant = ? AND id = ?",
      ["tenant-2", 3],
    );
    expect(wildcard.served).toBe(true);
    expect(wildcard.rows).toHaveLength(1);
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

  it("serves every logical-domain projection identically to the ordinary executor", async () => {
    const database = await domainDatabase();
    // id 12 has every column non-null; id 10 has amount NULL; id 8 has loose NULL;
    // id 6 has at NULL.
    for (const id of [12, 10, 8, 6]) {
      const { served, rows } = await differential(
        database,
        "SELECT amount, loose, ident, document, booked, at, span, feeling, choices, note " +
          "FROM payments WHERE id = ?",
        [id],
      );
      expect(served).toBe(true);
      expect(rows).toHaveLength(1);
    }
    const miss = await differential(database, "SELECT amount FROM payments WHERE id = ?", [9_999]);
    expect(miss.served).toBe(true);
    expect(miss.rows).toEqual([]);
    await database.close();
  });

  it("externalizes served domain values and reports their column domains", async () => {
    const database = await domainDatabase();
    const servedBefore = pointReadTestHooks.served;
    const result = await database.query(
      "SELECT amount, loose, ident, document, booked, at, span, feeling, choices " +
        "FROM payments WHERE id = ?",
      { params: [12], memoize: false },
    );
    expect(pointReadTestHooks.served).toBe(servedBefore + 1);
    // index 11: amount "11.5" pads to the declared scale, loose NUMERIC stays canonical.
    expect(result.rows).toEqual([
      {
        amount: "11.50",
        loose: "11.125",
        ident: "00000000-0000-4000-8000-000000000011",
        document: '{"seq":11,"tag":"p-11"}',
        booked: "2026-01-12",
        at: "12:34:56",
        span: "1 mons 2 days 0 usecs",
        feeling: "happy",
        choices: "[11,12]",
      },
    ]);
    expect(result.columnDomains).toEqual([
      { kind: "numeric", precision: 10, scale: 2 },
      { kind: "numeric" },
      { kind: "uuid" },
      { kind: "jsonb" },
      { kind: "date" },
      { kind: "time" },
      { kind: "interval" },
      { kind: "enum", name: "mood", values: ["sad", "ok", "happy"] },
      { kind: "array", element: "INTEGER" },
    ]);
    await database.close();
  });

  it("pads declared-scale NUMERIC exactly like the ordinary executor", async () => {
    const database = await domainDatabase();
    await database.execute(
      "INSERT INTO payments (id, amount, loose) VALUES (1001, 3, 3), (1002, 0.07, 0.07), " +
        "(1003, -12.5, -12.5), (1004, 1.25, 1.25)",
    );
    const expected: Record<number, { amount: string; loose: string }> = {
      1001: { amount: "3.00", loose: "3" },
      1002: { amount: "0.07", loose: "0.07" },
      1003: { amount: "-12.50", loose: "-12.5" },
      1004: { amount: "1.25", loose: "1.25" },
    };
    for (const [id, values] of Object.entries(expected)) {
      const { served, rows } = await differential(
        database,
        "SELECT amount, loose FROM payments WHERE id = ?",
        [Number(id)],
      );
      expect(served).toBe(true);
      expect(rows).toEqual([values]);
    }
    await database.close();
  });

  it("serves mixed plain and domain projections, aliases included", async () => {
    const database = await domainDatabase();
    const { served, rows } = await differential(
      database,
      "SELECT p.note, p.amount AS a, p.id, p.feeling FROM payments AS p WHERE p.id = ?",
      [13],
    );
    expect(served).toBe(true);
    expect(rows).toEqual([{ note: "note-12", a: "12.50", id: 13, feeling: "sad" }]);
    await database.close();
  });

  it("matches the ordinary executor for domain projections across multi-block segments", async () => {
    const database = await domainDatabase({ rowsPerBlock: 64 });
    for (const id of [1, 64, 65, 128, 200, 240]) {
      const { served, rows } = await differential(
        database,
        "SELECT amount, ident, feeling, booked FROM payments WHERE id = ?",
        [id],
      );
      expect(served).toBe(true);
      expect(rows).toHaveLength(1);
    }
    await database.close();
  });

  it("still falls back when a domain column appears in an equality predicate", async () => {
    const database = await domainDatabase();
    const byExtraEquality = await differential(
      database,
      "SELECT amount FROM payments WHERE id = ? AND amount = ?",
      [12, 11.5],
    );
    expect(byExtraEquality.served).toBe(false);
    expect(byExtraEquality.rows).toEqual([{ amount: "11.50" }]);
    await database.execute(
      "CREATE TABLE uuid_keyed (ident UUID NOT NULL PRIMARY KEY, label TEXT NOT NULL)",
    );
    await database.execute(
      "INSERT INTO uuid_keyed VALUES ('00000000-0000-4000-8000-000000000001', 'one')",
    );
    const byDomainKey = await differential(
      database,
      "SELECT label FROM uuid_keyed WHERE ident = ?",
      ["00000000-0000-4000-8000-000000000001"],
    );
    expect(byDomainKey.served).toBe(false);
    expect(byDomainKey.rows).toEqual([{ label: "one" }]);
    await database.close();
  });

  it("falls back on plain text stored in the protected NUL namespace", async () => {
    const database = await domainDatabase();
    const notes: Array<[number, string]> = [
      [2001, "\u0000minnow-domain:text:sneaky"],
      [2002, "\u0000plain-but-nul"],
    ];
    for (const [id, note] of notes) {
      await database.insert("payments", {
        id,
        amount: null,
        loose: null,
        ident: null,
        document: null,
        booked: null,
        at: null,
        span: null,
        feeling: null,
        choices: null,
        note,
      });
    }
    for (const [id, note] of notes) {
      const { served, rows } = await differential(
        database,
        "SELECT note, amount FROM payments WHERE id = ?",
        [id],
      );
      expect(served).toBe(false);
      expect(rows).toEqual([{ note, amount: null }]);
    }
    await database.close();
  });

  it("falls back for domain projections once update deltas exist, and returns after compaction", async () => {
    const database = await domainDatabase();
    await database.execute("UPDATE payments SET amount = 99.9 WHERE id = ?", [12]);
    const withDelta = await differential(
      database,
      "SELECT amount FROM payments WHERE id = ?",
      [12],
    );
    expect(withDelta.served).toBe(false);
    expect(withDelta.rows).toEqual([{ amount: "99.90" }]);
    await database.compactTable("payments");
    const compacted = await differential(
      database,
      "SELECT amount FROM payments WHERE id = ?",
      [12],
    );
    expect(compacted.served).toBe(true);
    expect(compacted.rows).toEqual([{ amount: "99.90" }]);
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
