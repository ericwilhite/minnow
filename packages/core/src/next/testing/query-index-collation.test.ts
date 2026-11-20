/** Index equality, IN and composite equality prefixes are byte comparisons. A predicate under a
 * collation the executor does not support must stay in place, so the indexed route reports the
 * same typed refusal as a forced scan instead of answering by byte keys. Under byte collations
 * the indexed route, the forced scan and PGlite agree. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, QUERY_INDEX_COLLATION_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { operators, withoutIndexes } from "./query-index.test-support.js";

async function outcome(run: () => Promise<unknown>) {
  try {
    return { error: null, rows: await run() };
  } catch (error) {
    let current: unknown = error;
    for (let depth = 0; depth < 4 && current instanceof Error; depth++) {
      const value = current as Error & { sqlState?: string; code?: string; cause?: unknown };
      const state = value.sqlState ?? value.code;
      if (state && /^[0-9A-Z]{5}$/u.test(state)) return { error: state, rows: null };
      current = value.cause;
    }
    throw error;
  }
}

/** RETURNING has no defined order; SELECT results are already ordered. */
function sorted(rows: readonly unknown[]): unknown[] {
  return rows
    .map((row) => JSON.stringify(row))
    .sort()
    .map((row) => JSON.parse(row) as unknown);
}

/** The executor's byte collations (exec/kernels/compare.ts, exec/kernels/compile.ts IN). */
const BYTE = new Set<string | null>([null, "C", "default"]);
const VALUES = ["a", "A", "b", "B", "ab", "é", "e", "z"];

const history = fc.record({
  shape: fc.constantFrom("unique", "primary", "composite"),
  form: fc.constantFrom("column", "key", "in", "prefix"),
  collation: fc.constantFrom(null, "C", "default", "POSIX", "ucs_basic", "und-x-icu"),
  statement: fc.constantFrom("select", "update"),
  rows: fc.integer({ min: 3, max: VALUES.length }),
  key: fc.integer({ min: 0, max: VALUES.length }),
  placement: fc.constantFrom("memory", "stored", "reopen"),
});
type History = typeof history extends fc.Arbitrary<infer T> ? T : never;

function statementOf(h: History) {
  const k = h.shape === "composite" ? "a" : "k";
  const quoted = h.collation === null ? "" : ` COLLATE "${h.collation}"`;
  const predicate =
    h.form === "key"
      ? `${k} = $1${quoted}`
      : h.form === "in"
        ? `${k}${quoted} IN ($1, 'zz')`
        : h.form === "prefix" && h.shape === "composite"
          ? `${k}${quoted} = $1 AND b >= 0`
          : `${k}${quoted} = $1`;
  const columns = h.shape === "composite" ? "a, b, v" : "k, v";
  return {
    table: h.shape === "composite" ? "qcomp" : "qcoll",
    sql:
      h.statement === "select"
        ? `SELECT ${columns} FROM ${h.shape === "composite" ? "qcomp" : "qcoll"} WHERE ${predicate} ORDER BY ${columns}`
        : `UPDATE ${h.shape === "composite" ? "qcomp" : "qcoll"} SET v = v + 100 WHERE ${predicate} RETURNING ${columns}`,
    all: `SELECT ${columns} FROM ${h.shape === "composite" ? "qcomp" : "qcoll"} ORDER BY ${columns}`,
  };
}

for (const seed of seedsFor("next-query-index-collation", [49711, 49712]))
  it(`keeps non-byte collation predicates out of index keys seed${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("query index collation", QUERY_INDEX_COLLATION_REACH);
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(history, async (h) => {
          reach.beginHistory();
          const core = new StorageCore(h.placement === "memory" ? 256 : 4),
            governor = new Governor(32 << 20);
          Object.assign(core.harness, { governor });
          await core.open();
          let scan = false;
          const context = core.context.bind(core);
          core.context = () => {
            const c = context();
            return { ...c, catalog: scan ? withoutIndexes(c.catalog) : c.catalog };
          };
          try {
            const values = VALUES.slice(0, h.rows);
            const setup =
              h.shape === "composite"
                ? [
                    "CREATE TABLE qcomp(a TEXT, b INT, v INT, PRIMARY KEY(a, b))",
                    `INSERT INTO qcomp VALUES ${values.map((v, i) => `('${v}',${String(i)},${String(i)}),('${v}',${String(i + 10)},${String(i)})`).join(",")}`,
                  ]
                : [
                    h.shape === "primary"
                      ? "CREATE TABLE qcoll(k TEXT PRIMARY KEY, v INT)"
                      : "CREATE TABLE qcoll(k TEXT UNIQUE, v INT)",
                    `INSERT INTO qcoll VALUES ${values.map((v, i) => `('${v}',${String(i)})`).join(",")}`,
                  ];
            await pg.exec("DROP TABLE IF EXISTS qcoll, qcomp");
            for (const sql of setup) {
              await pg.exec(sql);
              await core.execute(sql);
            }
            if (h.placement !== "memory") {
              await core.harness.path.quiesce();
              await core.harness.path.recycleLog();
            }
            if (h.placement === "reopen") await core.reopen();
            const s = statementOf(h),
              params = [VALUES[h.key] ?? "zz"],
              byte = BYTE.has(h.collation);
            const results = [];
            for (const forced of [true, false]) {
              scan = forced;
              const lowered = core.lowerer.lower(s.sql, core.context());
              const ops = operators(
                lowered.kind === "query"
                  ? lowered.plan.root
                  : lowered.kind === "modify"
                    ? lowered.plan.root
                    : (() => {
                        throw Error("plan required");
                      })(),
              );
              const indexed = ops.includes("keyLookup") || ops.includes("indexRange");
              // A predicate under a non-byte collation may never become index keys. Under a
              // byte collation the planner may choose the index (composite IN does not).
              if (forced || !byte)
                expect(indexed, "a non-byte collation predicate became index keys").toBe(false);
              else if (indexed) reach.reach("index served");
              const actual = await outcome(async () => {
                const result = await core.execute(s.sql, params, AbortSignal.timeout(30000));
                const rows = "batches" in result ? result : result.returning;
                if (!rows) throw Error("rows required");
                return sorted(oracleRows(rows));
              });
              results.push(actual);
              if (h.statement === "update" && actual.error === null && forced) {
                // Undo the forced-scan update so the indexed run starts from the same rows.
                await core.execute(
                  s.sql.replace("v = v + 100", "v = v - 100").replace(/ RETURNING .*/u, ""),
                  params,
                );
              }
              reach.reach(forced ? "forced scan" : "index route considered");
            }
            const [scanned, indexed] = results;
            expect(indexed, "indexed route matches the forced scan").toEqual(scanned);
            if (byte) {
              const expected = await outcome(async () =>
                sorted((await pg.query(s.sql, params)).rows),
              );
              expect(indexed, "byte collation matches PostgreSQL").toEqual(expected);
              reach.reach("byte collation index");
              reach.reach(
                (indexed?.rows as unknown[] | null)?.length ? "matching key" : "no matching key",
              );
            } else {
              expect(indexed?.error).toBe("0A000");
              reach.reach("non-byte collation refused");
            }
            const all = await core.execute(s.all);
            if (!("batches" in all)) throw Error("rows required");
            expect(oracleRows(all), "no effects beyond PostgreSQL's").toEqual(
              (await pg.query(s.all)).rows,
            );
            reach.reach(h.collation ?? "no COLLATE");
            reach.reach(
              h.form === "prefix" && h.shape === "composite"
                ? "composite equality prefix"
                : h.form === "in"
                  ? "IN list"
                  : h.form === "key"
                    ? "collated key side"
                    : "collated column side",
            );
            reach.reach(h.shape);
            reach.reach(h.statement);
            reach.reach(h.placement);
          } finally {
            await core.close();
            expect(governor.usedBytes).toBe(0);
            expect(governor.loanedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("zero ownership");
          }
        }),
        {
          seed,
          numRuns: 24,
          endOnFailure: true,
          examples: [
            [
              {
                shape: "unique",
                form: "column",
                collation: "und-x-icu",
                statement: "select",
                rows: 4,
                key: 2,
                placement: "memory",
              },
            ],
            [
              {
                shape: "primary",
                form: "in",
                collation: "POSIX",
                statement: "update",
                rows: 5,
                key: 1,
                placement: "stored",
              },
            ],
            [
              {
                shape: "composite",
                form: "prefix",
                collation: "ucs_basic",
                statement: "select",
                rows: 6,
                key: 3,
                placement: "reopen",
              },
            ],
            [
              {
                shape: "composite",
                form: "prefix",
                collation: "C",
                statement: "update",
                rows: 4,
                key: 0,
                placement: "memory",
              },
            ],
            [
              {
                shape: "unique",
                form: "key",
                collation: "default",
                statement: "select",
                rows: 3,
                key: 8,
                placement: "stored",
              },
            ],
            [
              {
                shape: "primary",
                form: "column",
                collation: null,
                statement: "select",
                rows: 8,
                key: 5,
                placement: "memory",
              },
            ],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 300000);
