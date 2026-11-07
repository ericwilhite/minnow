/** PG decides admission, not which strength its error happens to name (B6 final review P3). */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import type { CompiledQuery, RowLocking } from "../plan/model.js";
import { MemoryBlockStore } from "../storage/index.js";
import { createReach, ROW_LOCK_DIAGNOSTIC_STATES } from "../testing/reach.js";
import { MinnowDatabase } from "./database.js";
import { clonePlanTree, compileQuery, expandRowLockCteViews, expandViewSources } from "./query.js";
import { expandedRowLock } from "./row-locking.js";

// PostgreSQL LockClauseStrength order. Expected intent is declared from the generated SQL,
// independently of the planner and PG's diagnostic (which can name a weaker outer clause).
const LOCKS = ["KEY SHARE", "SHARE", "NO KEY UPDATE", "UPDATE"] as const;
const OUTER = [undefined, ...LOCKS] as const;
const MESSAGE = "cannot execute SELECT in a read-only transaction";
// These statements have no ORDER BY: compare exact row multisets, including multiplicity.
const rowsKey = (rows: unknown[]): string =>
  JSON.stringify(rows.map((row) => JSON.stringify(row)).sort());
const VIEWS = new Map(
  LOCKS.flatMap((lock, index) => [
    [`v${String(index)}`, `SELECT * FROM t FOR ${lock}`],
    [`n${String(index)}`, `SELECT * FROM v${String(index)}`],
  ]),
);
VIEWS.set("constant_view", "SELECT 1 AS id");
VIEWS.set("plain_view", "SELECT * FROM t");
VIEWS.set("nested_plain_view", "SELECT * FROM plain_view");
const DDL = [
  "CREATE TABLE t (id integer PRIMARY KEY)",
  "INSERT INTO t VALUES (1), (2)",
  ...[...VIEWS].map(([name, body]) => `CREATE VIEW ${name} AS ${body}`),
];

interface Shape {
  sql: string;
  lock: RowLocking | undefined;
  states: string[];
}

function shapes(): Shape[] {
  const result: Shape[] = [];
  for (const outer of OUTER) {
    const suffix = outer === undefined ? "" : ` FOR ${outer}`;
    for (const [from, lock, states] of [
      ["t", outer, ["early"]],
      ["(SELECT * FROM t) s", outer, ["early"]],
      ["constant_view", outer, ["early", "expanded view"]],
    ] as const)
      result.push({ sql: `SELECT * FROM ${from}${suffix}`, lock, states: [...states] });
    result.push(
      { sql: `SELECT 1 AS id${suffix}`, lock: undefined, states: ["early"] },
      {
        sql: `WITH c AS (SELECT * FROM t) SELECT * FROM c${suffix}`,
        lock: undefined,
        states: ["cte boundary"],
      },
    );
    for (const view of ["plain_view", "nested_plain_view"])
      result.push({
        sql: `SELECT * FROM ${view} WHERE id = 1${suffix}`,
        lock: outer,
        states: ["unlocked view", "expanded view", "point read"],
      });
    for (const [index, inner] of LOCKS.entries()) {
      const strongest = outer !== undefined && LOCKS.indexOf(outer) > index ? outer : inner;
      const states =
        outer === undefined || outer === inner
          ? []
          : [LOCKS.indexOf(outer) < index ? "weaker outer" : "stronger outer"];
      for (const depth of ["v", "n"]) {
        const view = `${depth}${String(index)}`;
        const sourceStates = ["expanded view", ...(depth === "n" ? ["nested"] : [])];
        for (const [from, where] of [
          [view, ""],
          [`(SELECT * FROM ${view}) s`, ""],
          [view, " WHERE id = 1"],
        ] as const)
          result.push({
            sql: `SELECT * FROM ${from}${where}${suffix}`,
            lock: strongest,
            states: [...sourceStates, ...states, ...(where === "" ? [] : ["point read"])],
          });
        result.push(
          {
            sql: `WITH c AS (SELECT * FROM ${view}) SELECT * FROM c${suffix}`,
            lock: inner,
            states: [...sourceStates, "retained cte", "cte boundary"],
          },
          {
            sql: `WITH c AS (SELECT * FROM ${view}), d AS (SELECT * FROM c) SELECT * FROM t${suffix}`,
            lock: strongest,
            states: [...sourceStates, ...states, "retained cte"],
          },
        );
      }
      result.push(
        {
          sql: `WITH c AS (SELECT * FROM t FOR ${inner}), d AS (SELECT * FROM c) SELECT * FROM t${suffix}`,
          lock: strongest,
          states: [...states, "retained cte"],
        },
        {
          sql: `WITH c AS (SELECT * FROM v${String(index)}) SELECT 1 AS id${suffix}`,
          lock: undefined,
          states: ["unused cte"],
        },
      );
    }
  }
  return result;
}

/** Exercise the shared expanded intent without deriving it from an error's command tag. */
function intent(sql: string): RowLocking | undefined {
  const plan = clonePlanTree(compileQuery(sql));
  const bodies = new Map<string, CompiledQuery>();
  const resolve = (name: string): CompiledQuery | undefined => {
    const body = VIEWS.get(name);
    if (body === undefined) return undefined;
    let compiled = bodies.get(name);
    if (compiled === undefined) {
      compiled = compileQuery(body);
      bodies.set(name, compiled);
    }
    return compiled;
  };
  expandViewSources(plan, resolve);
  return expandedRowLock(plan, bodies.values(), (body) => expandRowLockCteViews(body, resolve));
}

const ENTRIES: ReadonlyArray<
  readonly [string, (db: MinnowDatabase, sql: string) => Promise<Array<Record<string, unknown>>>]
> = [
  [
    "execute",
    async (db, sql) => {
      const result = await db.execute(sql);
      if (result.kind !== "rows") throw new Error("expected SELECT rows");
      return result.result.rows;
    },
  ],
  ["query", async (db, sql) => (await db.query(sql)).rows],
  [
    "typed",
    (db, sql) => db.run<Record<string, unknown>>({ kind: "typed-query", plan: compileQuery(sql) }),
  ],
  [
    "cloned",
    (db, sql) =>
      db.run<Record<string, unknown>>({
        kind: "typed-query",
        plan: clonePlanTree(compileQuery(sql)),
      }),
  ],
  [
    "serialized",
    (db, sql) =>
      db.run<Record<string, unknown>>({
        kind: "typed-query",
        plan: structuredClone(compileQuery(sql)),
      }),
  ],
  [
    "cursor",
    async (db, sql) => {
      const rows: Array<Record<string, unknown>> = [];
      for await (const batch of db.queryCursor(sql)) rows.push(...batch.rows);
      return rows;
    },
  ],
];

it("keeps locking-read diagnostics strength-free while expanded intent stays strongest", async () => {
  const reach = createReach<string>("locking SELECT diagnostics", ROW_LOCK_DIAGNOSTIC_STATES);
  const cases = shapes();
  expect(cases).toHaveLength(275);
  const failures: string[] = [];
  let pg = new PGlite();
  const databases = ENTRIES.map(() => new MinnowDatabase(new MemoryBlockStore()));
  try {
    for (const sql of DDL) {
      await pg.exec(sql);
      for (const db of databases) await db.execute(sql);
    }
    for (const [index, shape] of cases.entries()) {
      if (index > 0 && index % 200 === 0) {
        await pg.close();
        pg = new PGlite();
        for (const sql of DDL) await pg.exec(sql);
      }
      expect(intent(shape.sql), shape.sql).toBe(shape.lock);
      for (const readOnly of [true, false]) {
        const begin = `BEGIN READ ${readOnly ? "ONLY" : "WRITE"}`;
        await pg.exec(begin);
        let expected = "rows";
        let expectedRows: unknown[] = [];
        try {
          expectedRows = (await pg.query(shape.sql)).rows;
        } catch (error) {
          expected = String((error as { code: unknown }).code);
        } finally {
          await pg.exec("ROLLBACK");
        }
        expect(expected, shape.sql).toBe(readOnly && shape.lock !== undefined ? "25006" : "rows");
        for (const [entry, [name, run]] of ENTRIES.entries()) {
          const db = databases[entry];
          if (db === undefined) throw new Error("missing entry database");
          reach.beginHistory();
          await db.execute(begin);
          let actual = "rows";
          let rows: unknown[] = [];
          try {
            rows = await run(db, shape.sql);
          } catch (error) {
            // Do not normalize Minnow's message: every strength-bearing mutant must fail.
            actual =
              error instanceof TypeError && error.message === MESSAGE ? "25006" : String(error);
          } finally {
            await db.execute("ROLLBACK");
          }
          if (actual !== expected)
            failures.push(`${name}: ${begin}; ${shape.sql}\nPG ${expected}; Minnow ${actual}`);
          else if (actual === "rows" && rowsKey(rows) !== rowsKey(expectedRows))
            failures.push(
              `${name}: ${begin}; ${shape.sql}\nPG ${JSON.stringify(expectedRows)}; Minnow ${JSON.stringify(rows)}`,
            );
          else {
            reach.reach(name);
            reach.reach(
              readOnly
                ? expected === "rows"
                  ? "read only allowed"
                  : "read only refused"
                : "read write allowed",
            );
            for (const state of shape.states) reach.reach(state);
            if (shape.lock !== undefined) reach.reach(`strongest ${shape.lock}`);
          }
        }
      }
    }
    expect(failures.join("\n")).toBe("");
    reach.assertReached();
  } finally {
    await pg.close();
    for (const db of databases) await db.close();
  }
}, 120_000);
