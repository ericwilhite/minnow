/**
 * Index/base divergence oracle. A seeded stream of inserts, keyed and predicate updates and
 * deletes, upserts through the key and through a UNIQUE index arbiter, rekeys (scalar and
 * composite keys), folds, reopens, DROP/CREATE INDEX and ADD/DROP COLUMN runs against one table
 * carrying a single-column index, a composite index, a UNIQUE index and a DESC composite index.
 * After every step each query shape runs three ways — literal predicates (the index path, which
 * `explain` confirms is taken), the same predicates over expressions no index can serve (a forced
 * scan), and a JavaScript model — and all three must agree on rows and order.
 *
 * `MINNOW_SEED=<n>` replays one seed; `MINNOW_ORACLE_STEPS` widens a run.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { mulberry32, seedsFor } from "../testing/seeds.js";
import { MinnowDatabase } from "./database.js";
import { UniqueConstraintError } from "./errors.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

type V = number | string | null;
interface Row {
  id: number;
  a: number | null;
  b: string | null;
  c: number | null;
  d: string;
  e?: number | null;
}

/** A map entry the model guarantees is present. */
function must<T>(value: T | undefined, what: string): T {
  if (value === undefined) throw new Error(`${what} is missing`);
  return value;
}

const int = (r: () => number, n: number): number => Math.floor(r() * n);
const pick = <T>(r: () => number, xs: readonly T[]): T => xs[int(r, xs.length)] as T;
const lit = (v: V): string =>
  v === null ? "NULL" : typeof v === "number" ? String(v) : `'${v.replace(/'/g, "''")}'`;

const WORDS = ["ant", "bee", "cat", "dog", "eel", "fox", "gnu", "hen", "kit", "owl", "pig", "yak"];

function cmp(x: V, y: V): number {
  if (typeof x === "number" && typeof y === "number") return x - y;
  const a = String(x);
  const b = String(y);
  return a < b ? -1 : a > b ? 1 : 0;
}

/** PostgreSQL order: NULLS LAST ascending, NULLS FIRST descending; ties by id the same way. */
function order(rows: Row[], col: keyof Row, desc: boolean): Row[] {
  return [...rows].sort((l, rr) => {
    const x = l[col] ?? null;
    const y = rr[col] ?? null;
    let o: number;
    if (x === null && y === null) o = 0;
    else if (x === null) o = 1;
    else if (y === null) o = -1;
    else o = cmp(x, y);
    if (desc) o = -o;
    return o !== 0 ? o : desc ? rr.id - l.id : l.id - rr.id;
  });
}

interface Shape {
  readonly name: string;
  /** Literal predicates on bare columns: the spelling an index serves. */
  readonly indexed: string;
  /** The same predicates over expressions, which no index can serve. */
  readonly scan: string;
  readonly ordered: boolean;
  readonly expect: (rows: Row[]) => Row[];
}

function shapes(r: () => number, hasE: boolean): Shape[] {
  const v = int(r, 12);
  const v2 = v + 1 + int(r, 5);
  const w = int(r, 12);
  const s = pick(r, WORDS);
  const p = s.slice(0, 1 + int(r, 2));
  const ins = [int(r, 12), int(r, 12), int(r, 12)];
  const dv = `d${String(int(r, 40))}`;
  const out: Shape[] = [
    {
      name: "a = v",
      indexed: `WHERE a = ${String(v)}`,
      scan: `WHERE a + 0 = ${String(v)}`,
      ordered: false,
      expect: (rows) => rows.filter((x) => x.a === v),
    },
    {
      name: "a IN",
      indexed: `WHERE a IN (${ins.join(",")})`,
      scan: `WHERE a + 0 IN (${ins.join(",")})`,
      ordered: false,
      expect: (rows) => rows.filter((x) => x.a !== null && ins.includes(x.a)),
    },
    {
      name: "a range",
      indexed: `WHERE a >= ${String(v)} AND a < ${String(v2)}`,
      scan: `WHERE a + 0 >= ${String(v)} AND a + 0 < ${String(v2)}`,
      ordered: false,
      expect: (rows) => rows.filter((x) => x.a !== null && x.a >= v && x.a < v2),
    },
    {
      name: "a = v AND b = s",
      indexed: `WHERE a = ${String(v)} AND b = '${s}'`,
      scan: `WHERE a + 0 = ${String(v)} AND b || '' = '${s}'`,
      ordered: false,
      expect: (rows) => rows.filter((x) => x.a === v && x.b === s),
    },
    {
      name: "a = v AND b > p",
      indexed: `WHERE a = ${String(v)} AND b > '${p}'`,
      scan: `WHERE a + 0 = ${String(v)} AND b || '' > '${p}'`,
      ordered: false,
      expect: (rows) => rows.filter((x) => x.a === v && x.b !== null && x.b > p),
    },
    {
      name: "a = v AND b IS NULL",
      indexed: `WHERE a = ${String(v)} AND b IS NULL`,
      scan: `WHERE a + 0 = ${String(v)} AND b IS NULL`,
      ordered: false,
      expect: (rows) => rows.filter((x) => x.a === v && x.b === null),
    },
    {
      name: "d = unique",
      indexed: `WHERE d = '${dv}'`,
      scan: `WHERE d || '' = '${dv}'`,
      ordered: false,
      expect: (rows) => rows.filter((x) => x.d === dv),
    },
    {
      name: "b LIKE prefix",
      indexed: `WHERE b LIKE '${p}%'`,
      scan: `WHERE b || '' LIKE '${p}%'`,
      ordered: false,
      expect: (rows) => rows.filter((x) => x.b?.startsWith(p) === true),
    },
    {
      name: "c > v ORDER BY c DESC LIMIT 5",
      indexed: `WHERE c > ${String(w)} ORDER BY c DESC, id DESC LIMIT 5`,
      scan: `WHERE c + 0 > ${String(w)} ORDER BY c + 0 DESC, id DESC LIMIT 5`,
      ordered: true,
      expect: (rows) =>
        order(
          rows.filter((x) => x.c !== null && x.c > w),
          "c",
          true,
        ).slice(0, 5),
    },
    {
      name: "ORDER BY a LIMIT 7",
      indexed: `ORDER BY a, id LIMIT 7`,
      scan: `ORDER BY a + 0, id LIMIT 7`,
      ordered: true,
      expect: (rows) => order(rows, "a", false).slice(0, 7),
    },
    {
      name: "ORDER BY a DESC LIMIT 7",
      indexed: `ORDER BY a DESC, id DESC LIMIT 7`,
      scan: `ORDER BY a + 0 DESC, id DESC LIMIT 7`,
      ordered: true,
      expect: (rows) => order(rows, "a", true).slice(0, 7),
    },
    {
      name: "a = v OR c = w",
      indexed: `WHERE a = ${String(v)} OR c = ${String(w)}`,
      scan: `WHERE a + 0 = ${String(v)} OR c + 0 = ${String(w)}`,
      ordered: false,
      expect: (rows) => rows.filter((x) => x.a === v || x.c === w),
    },
  ];
  if (hasE) {
    out.push(
      {
        name: "e = 7",
        indexed: `WHERE e = 7`,
        scan: `WHERE e + 0 = 7`,
        ordered: false,
        expect: (rows) => rows.filter((x) => (x.e ?? null) === 7),
      },
      {
        name: "e = 3 AND a = v",
        indexed: `WHERE e = 3 AND a = ${String(v)}`,
        scan: `WHERE e + 0 = 3 AND a + 0 = ${String(v)}`,
        ordered: false,
        expect: (rows) => rows.filter((x) => (x.e ?? null) === 3 && x.a === v),
      },
    );
  }
  return out;
}

const INDEXES = {
  ia: "CREATE INDEX ia ON t(a)",
  iab: "CREATE INDEX iab ON t(a, b)",
  ud: "CREATE UNIQUE INDEX ud ON t(d)",
  icb: "CREATE INDEX icb ON t(c DESC, b ASC)",
} as const;
type IndexName = keyof typeof INDEXES;
const INDEX_NAMES = Object.keys(INDEXES) as IndexName[];

const describeError = (error: unknown): string =>
  error instanceof Error ? error.message : JSON.stringify(error);

interface StoreKind {
  readonly name: string;
  open(): Promise<BlockStore>;
  reopen(store: BlockStore): Promise<BlockStore>;
}

function memoryStore(): StoreKind {
  return {
    name: "memory",
    open: async () => new MemoryBlockStore(),
    reopen: async (store) => store,
  };
}

function indexedDbStore(): StoreKind {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  return {
    name: "indexeddb",
    open: () => IndexedDbBlockStore.open({ name, indexedDB: factory }),
    reopen: async (store) => {
      (store as IndexedDbBlockStore).close();
      return IndexedDbBlockStore.open({ name, indexedDB: factory });
    },
  };
}

interface Outcome {
  readonly indexedPlans: number;
  readonly checks: number;
  readonly problems: string[];
}

async function runStream(
  seed: number,
  storeKind: StoreKind,
  steps: number,
  composite: boolean,
): Promise<Outcome> {
  const r = mulberry32(seed);
  let store = await storeKind.open();
  const open = (): MinnowDatabase =>
    new MinnowDatabase(store, { rowsPerBlock: 8, compaction: { partitionRows: 16 } });
  let db = open();
  const model = new Map<number, Row>();
  const present = new Set<string>();
  /** Keys a rekey retired: the engine keeps them reserved against inserts while the forward lives. */
  const retired = new Set<number>();
  const problems: string[] = [];
  let hasE = false;
  let indexedPlans = 0;
  let checks = 0;
  await db.execute(
    composite
      ? "CREATE TABLE t (id INTEGER NOT NULL, k2 TEXT NOT NULL, a INTEGER, b TEXT, c INTEGER, d TEXT NOT NULL, PRIMARY KEY (id, k2))"
      : "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER, d TEXT NOT NULL)",
  );
  /** The second key component of each live row (composite tables only); never reused, since a retired tuple stays reserved. */
  const k2 = new Map<number, string>();
  let k2Serial = 0;
  const k2Of = (id: number): string => {
    let value = k2.get(id);
    if (value === undefined) {
      value = `v${String(k2Serial++)}`;
      k2.set(id, value);
    }
    return value;
  };
  const keyWhere = (id: number): string =>
    composite ? `id = ${String(id)} AND k2 = '${k2Of(id)}'` : `id = ${String(id)}`;
  const conflictTarget = composite ? "(id, k2)" : "(id)";
  for (const k of INDEX_NAMES) {
    await db.execute(INDEXES[k]);
    present.add(k);
  }

  const uniqueD = (d: string, exceptId: number | null): boolean =>
    !present.has("ud") || ![...model.values()].some((row) => row.d === d && row.id !== exceptId);
  const freshId = (): number => {
    for (let tries = 0; ; tries += 1) {
      const id = tries < 8 ? int(r, 48) : int(r, 10_000);
      if (!model.has(id) && !retired.has(id)) return id;
    }
  };
  const someId = (): number | undefined =>
    model.size === 0 ? undefined : pick(r, [...model.keys()]);
  const randRow = (id: number): Row => ({
    id,
    a: r() < 0.15 ? null : int(r, 12),
    b: r() < 0.2 ? null : pick(r, WORDS),
    c: r() < 0.15 ? null : int(r, 12),
    d: `d${String(int(r, 40))}`,
    ...(hasE ? { e: r() < 0.3 ? 7 : int(r, 5) } : {}),
  });
  const cols = (): string[] => ["id", "a", "b", "c", "d", ...(hasE ? ["e"] : [])];
  const insertSql = (rows: Row[]): string =>
    `INSERT INTO t (${[...cols(), ...(composite ? ["k2"] : [])].join(",")}) VALUES ${rows
      .map(
        (x) =>
          `(${cols()
            .map((cn) => lit((x as unknown as Record<string, V>)[cn] ?? null))
            .join(",")}${composite ? `, '${k2Of(x.id)}'` : ""})`,
      )
      .join(",")}`;

  const expectUnique = async (sql: string, shouldFail: boolean, what: string): Promise<void> => {
    let failed: unknown;
    try {
      await db.execute(sql);
    } catch (error) {
      failed = error;
    }
    if (shouldFail) {
      if (!(failed instanceof UniqueConstraintError)) {
        problems.push(`seed ${String(seed)}: expected a unique error for ${what}: ${sql}`);
      }
    } else if (failed !== undefined) {
      throw new Error(
        `seed ${String(seed)}: ${what} failed unexpectedly: ${sql} -> ${describeError(failed)}`,
      );
    }
  };

  const verify = async (stepLabel: string): Promise<void> => {
    const all = [...model.values()];
    for (const shape of shapes(r, hasE)) {
      const select = `SELECT ${cols().join(",")} FROM t `;
      const want = shape.expect(all).map((x) => ({ ...x, ...(hasE ? { e: x.e ?? 7 } : {}) }));
      const [viaIndex, viaScan] = await Promise.all([
        db.query(select + shape.indexed, { memoize: false }),
        db.query(select + shape.scan, { memoize: false }),
      ]);
      checks += 1;
      const norm = (rows: ReadonlyArray<Record<string, unknown>>): Row[] => {
        const out = rows.map((x) => ({ ...(x as unknown as Row) }));
        return shape.ordered ? out : out.sort((l, rr) => l.id - rr.id);
      };
      const wantN = shape.ordered ? want : [...want].sort((l, rr) => l.id - rr.id);
      const got = norm(viaIndex.rows);
      const scan = norm(viaScan.rows);
      if (
        JSON.stringify(got) !== JSON.stringify(wantN) ||
        JSON.stringify(scan) !== JSON.stringify(wantN)
      ) {
        const plan = await db.explain(select + shape.indexed);
        problems.push(
          `seed ${String(seed)} ${storeKind.name}${composite ? " composite" : ""} after ${stepLabel}: ` +
            `shape "${shape.name}" (${shape.indexed})\n  index: ${JSON.stringify(got)}\n` +
            `  scan : ${JSON.stringify(scan)}\n  model: ${JSON.stringify(wantN)}\n  plan: ${plan.replace(/\n/g, " | ")}`,
        );
        return;
      }
    }
    if (r() < 0.1) {
      const plan = await db.explain(`SELECT id FROM t WHERE a = 3`);
      if (plan.includes("secondary index")) indexedPlans += 1;
    }
  };

  for (let step = 0; step < steps && problems.length === 0; step += 1) {
    const roll = r();
    let label: string;
    if (roll < 0.2) {
      const n = 1 + int(r, 3);
      const rows: Row[] = [];
      const ids = new Set<number>();
      for (let i = 0; i < n; i += 1) {
        const id = r() < 0.1 ? (someId() ?? freshId()) : freshId();
        if (!ids.has(id)) {
          ids.add(id);
          rows.push(randRow(id));
        }
      }
      const ds = new Set<string>();
      let fails = false;
      for (const x of rows) {
        if (
          model.has(x.id) ||
          retired.has(x.id) ||
          !uniqueD(x.d, null) ||
          (present.has("ud") && ds.has(x.d))
        ) {
          fails = true;
        }
        ds.add(x.d);
      }
      label = `insert ${JSON.stringify(rows)} fails=${String(fails)}`;
      await expectUnique(insertSql(rows), fails, "insert");
      if (!fails) for (const x of rows) model.set(x.id, x);
    } else if (roll < 0.35) {
      const id = someId();
      if (id === undefined) continue;
      const cur = must(model.get(id), "row");
      const next = randRow(id);
      const which = pick(r, [
        ["a"],
        ["b"],
        ["c"],
        ["d"],
        ["a", "b"],
        ["a", "c", "d"],
        ["b", "c"],
        ...(hasE ? [["e"], ["e", "a"]] : []),
      ] as string[][]);
      const sets = which
        .map((cn) => `${cn} = ${lit((next as unknown as Record<string, V>)[cn] ?? null)}`)
        .join(", ");
      const fails = which.includes("d") && !uniqueD(next.d, id);
      label = `update ${String(id)} SET ${sets} fails=${String(fails)}`;
      await expectUnique(`UPDATE t SET ${sets} WHERE ${keyWhere(id)}`, fails, "update");
      if (!fails) {
        model.set(id, {
          ...cur,
          ...Object.fromEntries(
            which.map((cn) => [cn, (next as unknown as Record<string, V>)[cn] ?? null]),
          ),
        });
      }
    } else if (roll < 0.42) {
      const v = int(r, 12);
      const by = r() < 0.5 ? "a" : "c";
      label = `UPDATE t SET c = c + 1, b = 'zzz' WHERE ${by} = ${String(v)}`;
      await db.execute(
        `UPDATE t SET c = CASE WHEN c IS NULL THEN NULL ELSE c + 1 END, b = 'zzz' WHERE ${by} = ${String(v)}`,
      );
      for (const row of model.values()) {
        if (row[by] === v) {
          row.c = row.c === null ? null : row.c + 1;
          row.b = "zzz";
        }
      }
    } else if (roll < 0.5) {
      const id = someId();
      if (id === undefined) continue;
      label = `delete ${String(id)}`;
      await db.execute(`DELETE FROM t WHERE ${keyWhere(id)}`);
      model.delete(id);
      k2.delete(id);
    } else if (roll < 0.55) {
      const v = int(r, 12);
      label = `DELETE WHERE a = ${String(v)}`;
      await db.execute(`DELETE FROM t WHERE a = ${String(v)}`);
      for (const [id, row] of model) {
        if (row.a === v) {
          model.delete(id);
          k2.delete(id);
        }
      }
    } else if (roll < 0.62) {
      const id = r() < 0.6 ? (someId() ?? freshId()) : freshId();
      const x = randRow(id);
      const fails = !uniqueD(x.d, id);
      const c = cols();
      label = `upsert ${JSON.stringify(x)} fails=${String(fails)}`;
      await expectUnique(
        `${insertSql([x])} ON CONFLICT ${conflictTarget} DO UPDATE SET ${c
          .filter((cn) => cn !== "id")
          .map((cn) => `${cn} = EXCLUDED.${cn}`)
          .join(", ")}`,
        fails,
        "upsert",
      );
      if (!fails) model.set(id, x);
    } else if (roll < 0.67 && present.has("ud")) {
      // Upsert through the UNIQUE index arbiter: a colliding d updates the row holding it.
      const id = freshId();
      const x = randRow(id);
      const holder = [...model.values()].find((row) => row.d === x.d);
      label = `upsert ON CONFLICT (d) ${JSON.stringify(x)} holder=${String(holder?.id)}`;
      await db.execute(
        `${insertSql([x])} ON CONFLICT (d) DO UPDATE SET a = EXCLUDED.a, c = EXCLUDED.c`,
      );
      if (holder === undefined) model.set(id, x);
      else {
        k2.delete(id);
        model.set(holder.id, { ...holder, a: x.a, c: x.c });
      }
    } else if (roll < 0.72) {
      const id = someId();
      if (id === undefined) continue;
      const to = freshId();
      if (composite && r() < 0.5) {
        const nk = `v${String(k2Serial++)}`;
        label = `rekey k2 of ${String(id)} -> ${nk}`;
        await db.execute(`UPDATE t SET k2 = '${nk}' WHERE ${keyWhere(id)}`);
        k2.set(id, nk);
      } else {
        label = `rekey ${String(id)} -> ${String(to)}`;
        await db.execute(`UPDATE t SET id = ${String(to)} WHERE ${keyWhere(id)}`);
        const row = must(model.get(id), "row");
        model.delete(id);
        model.set(to, { ...row, id: to });
        retired.add(id);
        retired.delete(to);
        if (composite) {
          k2.set(to, k2Of(id));
          k2.delete(id);
        }
      }
    } else if (roll < 0.8) {
      label = "compactTable";
      await db.compactTable("t");
    } else if (roll < 0.85) {
      label = "reopen";
      await db.close();
      store = await storeKind.reopen(store);
      db = open();
    } else if (roll < 0.92) {
      const k = pick(r, INDEX_NAMES);
      if (present.has(k)) {
        label = `DROP INDEX ${k}`;
        await db.execute(`DROP INDEX ${k}`);
        present.delete(k);
      } else {
        label = INDEXES[k];
        try {
          await db.execute(INDEXES[k]);
          present.add(k);
        } catch (error) {
          if (k !== "ud" || !(error instanceof UniqueConstraintError)) throw error;
          const ds = [...model.values()].map((x) => x.d);
          if (new Set(ds).size === ds.length) {
            throw new Error(
              `seed ${String(seed)}: CREATE UNIQUE INDEX refused with no duplicates`,
              {
                cause: error,
              },
            );
          }
          label += " (refused: duplicates)";
        }
      }
    } else if (!hasE) {
      label = "ADD COLUMN e INTEGER DEFAULT 7 + CREATE INDEX ie";
      await db.execute("ALTER TABLE t ADD COLUMN e INTEGER DEFAULT 7");
      await db.execute("CREATE INDEX ie ON t(e)");
      hasE = true;
      for (const row of model.values()) row.e = 7;
    } else {
      label = "DROP INDEX ie + DROP COLUMN e";
      await db.execute("DROP INDEX ie");
      await db.execute("ALTER TABLE t DROP COLUMN e");
      hasE = false;
      for (const row of model.values()) delete row.e;
    }
    await verify(`step ${String(step)}: ${label}`);
  }
  await db.close();
  return { indexedPlans, checks, problems };
}

const STEPS = Number(process.env.MINNOW_ORACLE_STEPS ?? 60);

describe("index reads agree with forced scans and the model", () => {
  it.each(seedsFor("index-oracle", [0x1d3a, 0x2b7c]))(
    "memory store, scalar and composite keys (seed %s)",
    async (seed) => {
      const scalar = await runStream(seed, memoryStore(), STEPS, false);
      const composite = await runStream(seed + 1, memoryStore(), STEPS, true);
      expect([...scalar.problems, ...composite.problems]).toEqual([]);
      expect(scalar.checks + composite.checks).toBeGreaterThan(0);
      expect(scalar.indexedPlans + composite.indexedPlans).toBeGreaterThan(0);
    },
  );

  it("fake IndexedDB store, scalar key", async () => {
    const [seed] = seedsFor("index-oracle", [0x1d3a]);
    const outcome = await runStream(seed ?? 0x1d3a, indexedDbStore(), Math.min(STEPS, 40), false);
    expect(outcome.problems).toEqual([]);
    expect(outcome.checks).toBeGreaterThan(0);
  });
});

describe("CREATE UNIQUE INDEX over existing duplicates", () => {
  async function duplicates(): Promise<MinnowDatabase> {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, d TEXT NOT NULL)");
    await database.execute(
      "INSERT INTO t VALUES (1, 1, 'x'), (2, 1, 'y'), (3, 2, 'x'), (4, 2, 'x')",
    );
    return database;
  }

  it("fails with UniqueConstraintError naming the table, column and value, through SQL", async () => {
    const database = await duplicates();
    try {
      const error = await database.execute("CREATE UNIQUE INDEX ud ON t (d)").then(
        () => undefined,
        (caught: unknown) => caught,
      );
      expect(error).toBeInstanceOf(UniqueConstraintError);
      const unique = error as UniqueConstraintError;
      expect(unique.tableName).toBe("t");
      expect(unique.columnName).toBe("d");
      expect(unique.value).toBe("x");
      expect(unique.message).toBe("Duplicate value for t.d: x");
      // Nothing is left behind: the name is free, and a key that is unique builds.
      await database.execute("CREATE UNIQUE INDEX ud ON t (d, id)");
    } finally {
      await database.close();
    }
  });

  it("names a composite index's column list, through createIndex()", async () => {
    const database = await duplicates();
    try {
      const error = await database
        .createIndex(
          "uad",
          "t",
          [
            { name: "a", direction: "asc" },
            { name: "d", direction: "asc" },
          ],
          { unique: true },
        )
        .then(
          () => undefined,
          (caught: unknown) => caught,
        );
      expect(error).toBeInstanceOf(UniqueConstraintError);
      expect((error as UniqueConstraintError).tableName).toBe("t");
      expect((error as UniqueConstraintError).columnName).toBe("(a, d)");
      expect((error as UniqueConstraintError).value).toBe('[2,"x"]');
      // Without the repeated pair, (a, d) is unique as (1,'x'), (1,'y'), (2,'x') and builds;
      // the single column d still repeats 'x'.
      await database.execute("DELETE FROM t WHERE id = 4");
      await database.execute("CREATE UNIQUE INDEX uad ON t (a, d)");
      const single = await database.execute("CREATE UNIQUE INDEX IF NOT EXISTS ud ON t (d)").then(
        () => undefined,
        (caught: unknown) => caught,
      );
      expect(single).toBeInstanceOf(UniqueConstraintError);
      expect((single as UniqueConstraintError).columnName).toBe("d");
    } finally {
      await database.close();
    }
  });
});
