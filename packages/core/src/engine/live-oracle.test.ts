/**
 * Live-query oracle. After every commit the last result each subscription delivered must equal
 * a fresh execution of the same statement, with incremental maintenance on and off. The shapes
 * cover keyed lookups, filters with ORDER BY, LIMIT/OFFSET windows, plain and grouped aggregates
 * (COUNT, SUM, AVG, MIN, MAX, COUNT(column), HAVING), inner, left and aggregated joins to a
 * lookup table that is itself rekeyed, LIKE, DISTINCT, an IN-subquery, parameters, a window
 * function, `SELECT *`, CASE, an expression ORDER BY, a cascade-deleted child table and a
 * trigger-written audit table. The commits come from this connection and a peer: keyed and
 * predicate updates and deletes, upserts, rekeys of both tables, FK cascades, write scopes,
 * compaction, and ADD/DROP COLUMN on the watched table.
 *
 * `MINNOW_SEED=<n>` replays one seed; `MINNOW_ORACLE_STEPS` widens a run.
 */
import { describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { mulberry32, seedsFor } from "../testing/seeds.js";
import { MinnowDatabase } from "./database.js";
import type { LiveQueryInput, LiveQuerySubscription } from "./live.js";
import type { QueryResult } from "./query.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

const int = (r: () => number, n: number): number => Math.floor(r() * n);
const pick = <T>(r: () => number, xs: readonly T[]): T => xs[int(r, xs.length)] as T;
const STATUS = ["open", "paid", "void"];
const NOTES = ["alpha", "beta", "amber", null, "ash"];

const SHAPES: Array<{ name: string; input: LiveQueryInput }> = [
  { name: "keyed", input: "SELECT id, status, total FROM o WHERE id = 5" },
  { name: "keyed-in", input: "SELECT id, total FROM o WHERE id IN (1,2,3) ORDER BY id" },
  {
    name: "filter-order",
    input: "SELECT id, cust, status, total FROM o WHERE status = 'open' ORDER BY id",
  },
  { name: "window-desc", input: "SELECT id, total FROM o ORDER BY total DESC, id LIMIT 4" },
  { name: "window-offset", input: "SELECT id, total FROM o ORDER BY total, id LIMIT 3 OFFSET 2" },
  {
    name: "window-filter",
    input: "SELECT id, total FROM o WHERE status <> 'void' ORDER BY total DESC, id LIMIT 3",
  },
  { name: "agg", input: "SELECT COUNT(*) AS n, SUM(total) AS s, AVG(total) AS av FROM o" },
  {
    name: "agg-group",
    input: "SELECT status, COUNT(*) AS n, SUM(total) AS s FROM o GROUP BY status ORDER BY status",
  },
  {
    name: "agg-having",
    input: "SELECT cust, SUM(total) AS s FROM o GROUP BY cust HAVING SUM(total) > 20 ORDER BY cust",
  },
  {
    name: "agg-minmax",
    input: "SELECT MAX(total) AS mx, MIN(total) AS mn, COUNT(note) AS notes FROM o",
  },
  { name: "agg-filtered", input: "SELECT COUNT(*) AS n, SUM(total) AS s FROM o WHERE total > 10" },
  {
    name: "join",
    input:
      "SELECT o.id, c.name, o.total FROM o JOIN c ON c.id = o.cust WHERE o.status <> 'void' ORDER BY o.id",
  },
  {
    name: "left-join",
    input: "SELECT o.id, c.name FROM o LEFT JOIN c ON c.id = o.cust ORDER BY o.id",
  },
  {
    name: "join-agg",
    input:
      "SELECT c.name, COUNT(*) AS n FROM o JOIN c ON c.id = o.cust GROUP BY c.name ORDER BY c.name",
  },
  { name: "like", input: "SELECT id, note FROM o WHERE total > 10 AND note LIKE 'a%' ORDER BY id" },
  { name: "distinct", input: "SELECT DISTINCT status FROM o ORDER BY status" },
  {
    name: "in-subquery",
    input: "SELECT id FROM o WHERE cust IN (SELECT id FROM c WHERE tier = 'gold') ORDER BY id",
  },
  {
    name: "params",
    input: {
      kind: "sql-query",
      sql: "SELECT id, total FROM o WHERE cust = $1 ORDER BY id",
      params: [2],
    },
  },
  {
    name: "window-fn",
    input: "SELECT id, ROW_NUMBER() OVER (ORDER BY total DESC, id) AS rn FROM o ORDER BY id",
  },
  { name: "star", input: "SELECT * FROM o ORDER BY id" },
  { name: "lookup", input: "SELECT id, name, tier FROM c ORDER BY id" },
  {
    name: "expr-order",
    input: "SELECT id, total * 2 AS dbl FROM o WHERE total * 2 > 10 ORDER BY dbl DESC, id LIMIT 5",
  },
  { name: "null-cust", input: "SELECT id FROM o WHERE cust IS NULL ORDER BY id" },
  {
    name: "case",
    input: "SELECT id, CASE WHEN total > 15 THEN 'big' ELSE 'small' END AS size FROM o ORDER BY id",
  },
  { name: "cascade-child", input: "SELECT id, oid, w FROM g ORDER BY id" },
  { name: "cascade-child-agg", input: "SELECT COUNT(*) AS n, SUM(w) AS s FROM g" },
  {
    name: "cascade-join",
    input: "SELECT g.id, o.status FROM g JOIN o ON o.id = g.oid ORDER BY g.id",
  },
  {
    name: "trigger-audit",
    input: "SELECT oid, COUNT(*) AS n FROM audit GROUP BY oid ORDER BY oid",
  },
  { name: "trigger-audit-sum", input: "SELECT SUM(w) AS s, COUNT(*) AS n FROM audit" },
];

interface Tracked {
  name: string;
  input: LiveQueryInput;
  sql: string;
  params: readonly unknown[];
  latest?: QueryResult;
  deliveries: number;
  errors: unknown[];
  sub?: LiveQuerySubscription;
}

async function runStream(seed: number, steps: number, incremental: boolean): Promise<string[]> {
  const r = mulberry32(seed);
  const store = new MemoryBlockStore();
  const db = new MinnowDatabase(store, { rowsPerBlock: 8 });
  const peer = new MinnowDatabase(store, { rowsPerBlock: 8 });
  const problems: string[] = [];
  const live = db.liveQueries({ incremental });
  const quoted = (n: string | null): string => (n === null ? "NULL" : `'${n}'`);
  try {
    await db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, name TEXT NOT NULL, tier TEXT)");
    await db.execute(
      "CREATE TABLE o (id INTEGER PRIMARY KEY, cust INTEGER REFERENCES c(id), status TEXT NOT NULL, total INTEGER NOT NULL, note TEXT)",
    );
    await db.execute(
      "CREATE TABLE g (id INTEGER PRIMARY KEY, oid INTEGER NOT NULL REFERENCES o(id) ON DELETE CASCADE, w INTEGER NOT NULL)",
    );
    await db.execute("CREATE TABLE audit (oid INTEGER, w INTEGER)");
    await db.execute(
      "CREATE TRIGGER g_del AFTER DELETE ON g BEGIN INSERT INTO audit (oid, w) VALUES (OLD.oid, OLD.w); END",
    );
    await db.execute(
      "CREATE TRIGGER o_upd AFTER UPDATE ON o BEGIN INSERT INTO audit (oid, w) VALUES (NEW.id, NEW.total); END",
    );
    for (let i = 1; i <= 5; i += 1) {
      await db.execute(
        `INSERT INTO c VALUES (${String(i)}, 'c${String(i)}', ${i % 2 === 0 ? "'gold'" : "'silver'"})`,
      );
    }
    for (let i = 1; i <= 8; i += 1) {
      await db.execute(
        `INSERT INTO o VALUES (${String(i)}, ${String(1 + (i % 5))}, '${pick(r, STATUS)}', ${String(int(r, 30))}, ${i % 3 === 0 ? "NULL" : "'alpha'"})`,
      );
    }
    let gNext = 1;
    const custIds = new Set<number>([1, 2, 3, 4, 5]);
    const retiredO = new Set<number>();
    const retiredC = new Set<number>();
    let hasFlag = false;

    const tracked: Tracked[] = SHAPES.map((s) => ({
      name: s.name,
      input: s.input,
      deliveries: 0,
      errors: [],
      sql: typeof s.input === "string" ? s.input : s.input.kind === "sql-query" ? s.input.sql : "",
      params: typeof s.input === "string" ? [] : s.input.kind === "sql-query" ? s.input.params : [],
    }));
    for (const t of tracked) {
      t.sub = await live.subscribe(t.input, {
        onChange: (result) => {
          t.latest = result;
          t.deliveries += 1;
        },
        onError: (error) => t.errors.push(error),
      });
    }
    const fresh = async (t: Tracked): Promise<QueryResult> =>
      db.query(t.sql, { memoize: false, params: t.params as never });
    const canon = (x: QueryResult | undefined): string =>
      JSON.stringify(x === undefined ? null : { columns: x.columns, rows: x.rows });

    const previous = new Map<string, string>();
    for (const t of tracked) previous.set(t.name, canon(await fresh(t)));
    for (const t of tracked) {
      if (canon(t.latest) !== previous.get(t.name)) {
        problems.push(`seed ${String(seed)} initial ${t.name}: live=${canon(t.latest)}`);
      }
    }

    const oIds = async (): Promise<number[]> =>
      (await db.query("SELECT id FROM o ORDER BY id", { memoize: false })).rows.map(
        (x) => x.id as number,
      );
    const freshO = async (): Promise<number> => {
      const have = new Set(await oIds());
      for (let tries = 0; ; tries += 1) {
        const id = tries < 8 ? int(r, 40) : int(r, 100_000);
        if (!have.has(id) && !retiredO.has(id)) return id;
      }
    };
    const custValue = (): string => (r() < 0.15 ? "NULL" : String(pick(r, [...custIds])));

    for (let step = 0; step < steps && problems.length === 0; step += 1) {
      const conn = r() < 0.3 ? peer : db;
      const roll = r();
      let label = "";
      try {
        if (roll < 0.2) {
          const id = await freshO();
          label = `insert o ${String(id)}`;
          await conn.execute(
            `INSERT INTO o (id, cust, status, total, note) VALUES (${String(id)}, ${custValue()}, '${pick(r, STATUS)}', ${String(int(r, 30))}, ${quoted(pick(r, NOTES))})`,
          );
        } else if (roll < 0.4) {
          const ids = await oIds();
          if (ids.length === 0) continue;
          const id = pick(r, ids);
          const set = pick(r, [
            `status = '${pick(r, STATUS)}'`,
            `total = ${String(int(r, 30))}`,
            `note = ${quoted(pick(r, NOTES))}`,
            `cust = ${custValue()}`,
            `total = total + 7, status = '${pick(r, STATUS)}'`,
          ]);
          label = `update o ${String(id)} SET ${set}`;
          await conn.execute(`UPDATE o SET ${set} WHERE id = ${String(id)}`);
        } else if (roll < 0.47) {
          const st = pick(r, STATUS);
          label = `UPDATE o SET total = total + 3 WHERE status = '${st}'`;
          await conn.execute(`UPDATE o SET total = total + 3 WHERE status = '${st}'`);
        } else if (roll < 0.55) {
          const ids = await oIds();
          if (ids.length === 0) continue;
          const id = pick(r, ids);
          label = `delete o ${String(id)}`;
          await conn.execute(`DELETE FROM o WHERE id = ${String(id)}`);
        } else if (roll < 0.6) {
          const v = int(r, 30);
          label = `DELETE FROM o WHERE total < ${String(v)} AND status = 'void'`;
          await conn.execute(`DELETE FROM o WHERE total < ${String(v)} AND status = 'void'`);
        } else if (roll < 0.67) {
          const ids = await oIds();
          const id = ids.length > 0 && r() < 0.6 ? pick(r, ids) : await freshO();
          label = `upsert o ${String(id)}`;
          await conn.execute(
            `INSERT INTO o (id, cust, status, total, note) VALUES (${String(id)}, ${custValue()}, '${pick(r, STATUS)}', ${String(int(r, 30))}, 'amber') ON CONFLICT (id) DO UPDATE SET total = EXCLUDED.total, status = EXCLUDED.status, cust = EXCLUDED.cust`,
          );
        } else if (roll < 0.74) {
          const ids = await oIds();
          if (ids.length === 0) continue;
          const id = pick(r, ids);
          const to = await freshO();
          label = `rekey o ${String(id)} -> ${String(to)}`;
          await conn.execute(`UPDATE o SET id = ${String(to)} WHERE id = ${String(id)}`);
          retiredO.add(id);
        } else if (roll < 0.79) {
          const from = pick(r, [...custIds]);
          let to = 6 + int(r, 20);
          while (custIds.has(to) || retiredC.has(to)) to = 6 + int(r, 100_000);
          label = `rekey c ${String(from)} -> ${String(to)}`;
          await conn.execute(`UPDATE c SET id = ${String(to)} WHERE id = ${String(from)}`);
          custIds.delete(from);
          custIds.add(to);
          retiredC.add(from);
        } else if (roll < 0.84) {
          const id = pick(r, [...custIds]);
          label = `update c ${String(id)}`;
          await conn.execute(
            `UPDATE c SET name = 'n${String(int(r, 100))}', tier = '${pick(r, ["gold", "silver"])}' WHERE id = ${String(id)}`,
          );
        } else if (roll < 0.89) {
          label = "compactTable o";
          await conn.compactTable("o");
        } else if (roll < 0.93) {
          const ids = await oIds();
          if (ids.length < 2) continue;
          const [x, y] = [pick(r, ids), pick(r, ids)];
          label = "write scope: insert, update, delete";
          const id = await freshO();
          await conn.write(async (scope) => {
            await scope.insertBatch("o", [
              { id, cust: null, status: "open", total: 12, note: "ash" },
            ]);
            await scope.updateBatch("o", { keys: [x], changes: { total: [99] } });
            if (y !== x) await scope.deleteBatch("o", { keys: [y] });
          });
        } else if (roll < 0.97) {
          const ids = await oIds();
          if (ids.length === 0) continue;
          const oid = pick(r, ids);
          const id = gNext++;
          label = `insert g ${String(id)} -> o ${String(oid)}`;
          await conn.execute(
            `INSERT INTO g (id, oid, w) VALUES (${String(id)}, ${String(oid)}, ${String(int(r, 20))})`,
          );
        } else if (!hasFlag) {
          label = "ALTER TABLE o ADD COLUMN flag INTEGER DEFAULT 0";
          await conn.execute("ALTER TABLE o ADD COLUMN flag INTEGER DEFAULT 0");
          hasFlag = true;
        } else {
          label = "ALTER TABLE o DROP COLUMN flag";
          await conn.execute("ALTER TABLE o DROP COLUMN flag");
          hasFlag = false;
        }
      } catch (error) {
        label += ` (refused: ${String(error).slice(0, 80)})`;
        if (!/UniqueConstraint|Duplicate|FOREIGN KEY|retired|reserved/i.test(String(error))) {
          problems.push(`seed ${String(seed)} step ${String(step)} unexpected refusal: ${label}`);
        }
      }
      await live.refresh();
      for (const t of tracked) {
        const now = canon(await fresh(t));
        const was = previous.get(t.name);
        previous.set(t.name, now);
        if (t.errors.length > 0) {
          problems.push(
            `seed ${String(seed)} step ${String(step)} (${label}) ${t.name}: onError ${String(t.errors[0]).slice(0, 200)}`,
          );
          t.errors.length = 0;
        }
        if (canon(t.latest) !== now) {
          problems.push(
            `seed ${String(seed)} incremental=${String(incremental)} step ${String(step)} (${label}) ${t.name} [${t.sql}]:\n` +
              `   live =${canon(t.latest)}\n   fresh=${now}\n   prev =${was ?? ""}`,
          );
        }
      }
    }
    for (const t of tracked) t.sub?.close();
  } finally {
    live.close();
    await peer.close();
    await db.close();
  }
  return problems;
}

const STEPS = Number(process.env.MINNOW_ORACLE_STEPS ?? 50);

describe("live results equal re-execution after every commit", () => {
  it.each(seedsFor("live-oracle", [0x11fe, 0x2a9d]))(
    "with incremental maintenance on and off (seed %s)",
    async (seed) => {
      expect(await runStream(seed, STEPS, true)).toEqual([]);
      expect(await runStream(seed, STEPS, false)).toEqual([]);
    },
  );
});
