/** Owner admission over public lowering metadata, compared with PostgreSQL (PGlite). */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import {
  createReach,
  OWNER_ROW_LOCK_STATES,
  OWNER_MODE_ORDER_STATES,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { createLowerer } from "../lower/create.js";
import type { RunContext } from "../owner/engine.js";
import { withTestStatements } from "./owner-standins.js";
import { cleanups, harness } from "./owner-harness.js";

async function outcome(action: () => Promise<unknown>) {
  try {
    await action();
    return "ok";
  } catch (error) {
    const e = error as { sqlState?: string; code?: string };
    return e.sqlState ?? e.code;
  }
}
const lockSQL = {
  direct: "SELECT * FROM t FOR $LOCK",
  point: "SELECT * FROM t WHERE id = 1 FOR $LOCK",
  empty: "SELECT * FROM t WHERE false FOR $LOCK",
  nested: "SELECT * FROM (SELECT * FROM t) q FOR $LOCK",
  "nested-body": "SELECT * FROM (SELECT * FROM t FOR $LOCK) q",
  view: "SELECT * FROM v FOR $LOCK",
  "view-body": "SELECT * FROM v",
  "cte-body": "WITH x AS (SELECT * FROM t FOR $LOCK) SELECT * FROM x",
  "cte-reference": "WITH x AS (SELECT * FROM t) SELECT * FROM x FOR $LOCK",
  "unused-cte": "WITH x AS (SELECT * FROM t FOR $LOCK) SELECT 1 AS n",
  "cte-graph-one":
    "WITH x AS (SELECT * FROM t FOR $LOCK), unused AS (SELECT * FROM x) SELECT 1 AS n",
  "cte-graph-two":
    "WITH x AS (SELECT * FROM t FOR $LOCK), y AS (SELECT * FROM x), unused AS (SELECT * FROM y) SELECT 1 AS n",
  constants: "SELECT * FROM (SELECT 1 AS n) q FOR $LOCK",
} as const;
const lockKinds = Object.keys(lockSQL) as Array<keyof typeof lockSQL>;

const context: RunContext = {
  signal: new AbortController().signal,
  windowBytes: 65536,
  request: null,
  frameRows: null,
};

// Preserve the committed seed keys from the pending campaigns for regression replay.
it.each(seedsFor("next-owner-pending-locking", [0x10c6]))(
  "generated effective row locking matches PG (seed %i)",
  async (seed) => {
    const pg = new PGlite();
    const reach = createReach("next-owner-pending-locking", OWNER_ROW_LOCK_STATES, {
      minimum: 2,
    });
    try {
      await pg.exec("CREATE TABLE t(id INT PRIMARY KEY); INSERT INTO t VALUES(1)");
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            cached: fc.boolean(),
            strength: fc.constantFrom("UPDATE", "NO KEY UPDATE", "SHARE", "KEY SHARE"),
            kind: fc.constantFrom(...lockKinds),
          }),
          async ({ strength, kind, cached }) => {
            const planner = withTestStatements(createLowerer());
            const lower = vi.spyOn(planner, "lower");
            const lowerParameterized = vi.spyOn(planner, "lowerParameterized");
            const h = harness({ planner });
            const executions = vi.fn();
            const prepare = h.executor.prepare.bind(h.executor);
            const preparations = vi.spyOn(h.executor, "prepare").mockImplementation((plan) => {
              const prepared = prepare(plan);
              return {
                ...prepared,
                execute: (request) => {
                  executions();
                  return prepared.execute(request);
                },
              };
            });
            const c = await h.connect();
            try {
              await c.query("CREATE TABLE t(id INT PRIMARY KEY)");
              await c.query("INSERT INTO t VALUES(1)");
              await pg.exec("DROP VIEW IF EXISTS v");
              const view = `CREATE VIEW v AS SELECT * FROM t${kind === "view-body" ? ` FOR ${strength}` : ""}`;
              if (kind === "view" || kind === "view-body") {
                await pg.exec(view);
                await c.query(view);
              }
              const sql = lockSQL[kind].replaceAll("$LOCK", strength);
              if (cached) {
                // A different literal must reuse the locking template on the point path.
                await c.query(kind === "point" ? sql.replace("id = 1", "id = 2") : sql);
              }
              await pg.exec("BEGIN READ ONLY");
              await c.query("BEGIN READ ONLY");
              lower.mockClear();
              lowerParameterized.mockClear();
              executions.mockClear();
              preparations.mockClear();
              const expected = await outcome(() => pg.query(sql));
              expect(["ok", "25006"]).toContain(expected);
              expect(await outcome(() => c.query(sql)), sql).toBe(expected);
              if (cached) {
                expect(lower, sql).not.toHaveBeenCalled();
                expect(lowerParameterized, sql).not.toHaveBeenCalled();
              }
              const entered = new Set<string>([kind, strength]);
              if (expected === "25006") {
                expect(executions, sql).not.toHaveBeenCalled();
                entered.add(cached ? "cached locking refusal" : "cold locking refusal");
              } else expect(executions, sql).toHaveBeenCalled();
              if (kind === "point") {
                // Exercise both admission entry points with real lowered SQL, including a
                // template hit: neither may enter a cached executor before refusing the lock.
                for (const sync of [false, true]) {
                  const session = h.engine.openSession("locking", 1);
                  try {
                    await session.begin(true, context.signal);
                    const plan = h.engine.plan(sql);
                    expect(plan.lowered).toMatchObject({ kind: "query", pointRead: true });
                    executions.mockClear();
                    preparations.mockClear();
                    expect(
                      await outcome(async () => {
                        if (sync) session.runNow(plan, [], context);
                        else await session.run(plan, [], context).done;
                      }),
                      sql,
                    ).toBe(expected);
                    expect(executions, sql).not.toHaveBeenCalled();
                    expect(session.transaction).toBe("failed");
                    entered.add(sync ? "synchronous point refusal" : "normal point refusal");
                  } finally {
                    session.close();
                  }
                }
              }
              reach.recordHistory(entered);
            } finally {
              await pg.exec("ROLLBACK");
              h.server.stepDown("closing");
              await h.engine.close();
              for (const close of cleanups.splice(0).reverse()) close();
              preparations.mockRestore();
              lower.mockRestore();
              lowerParameterized.mockRestore();
            }
          },
        ),
        { seed, numRuns: 120 },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  },
  120_000,
);

it.each(seedsFor("next-owner-pending-ordered-modes", [0x0de2]))(
  "generated ordered mode instructions match PG (seed %i)",
  async (seed) => {
    const pg = new PGlite();
    const reach = createReach("next-owner-pending-ordered-modes", OWNER_MODE_ORDER_STATES, {
      minimum: 2,
    });
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            initialReadOnly: fc.boolean(),
            depth: fc.integer({ min: 0, max: 2 }),
            reverse: fc.boolean(),
            command: fc.constantFrom("BEGIN", "START TRANSACTION", "SET TRANSACTION"),
          }),
          async ({ initialReadOnly, depth, reverse, command }) => {
            const h = harness();
            const c = await h.connect();
            try {
              if (command === "SET TRANSACTION") {
                const begin = `BEGIN READ ${initialReadOnly ? "ONLY" : "WRITE"}`;
                await pg.exec(begin);
                await c.query(begin);
                for (let d = 0; d < depth; d++) {
                  await pg.exec(`SAVEPOINT s${String(d)}`);
                  await c.query(`SAVEPOINT s${String(d)}`);
                }
              }
              const sql = command + (reverse ? " READ WRITE, READ ONLY" : " READ ONLY, READ WRITE");
              const expected = await outcome(() => pg.query(sql));
              expect(["ok", "25001"]).toContain(expected);
              expect(await outcome(() => c.query(sql)), sql).toBe(expected);
              const entered = new Set<string>([
                command,
                reverse ? "readWrite then readOnly" : "readOnly then readWrite",
              ]);
              if (command === "SET TRANSACTION") {
                entered.add(
                  depth === 0
                    ? "root SET"
                    : depth === 1
                      ? "one savepoint SET"
                      : "nested savepoint SET",
                );
                entered.add(initialReadOnly ? "read-only SET entry" : "read-write SET entry");
                if (!initialReadOnly && depth > 0)
                  entered.add(
                    reverse
                      ? "readWrite then readOnly under writable savepoint"
                      : "readOnly then readWrite under writable savepoint",
                  );
                if (initialReadOnly && depth > 0) {
                  expect(expected).toBe("25001");
                  entered.add(
                    reverse
                      ? "readWrite then readOnly under read-only savepoint"
                      : "readOnly then readWrite under read-only savepoint",
                  );
                }
              }
              reach.recordHistory(entered);
            } finally {
              await pg.exec("ROLLBACK");
              h.server.stepDown("closing");
              await h.engine.close();
              for (const close of cleanups.splice(0).reverse()) close();
            }
          },
        ),
        { seed, numRuns: 120 },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  },
  120_000,
);
