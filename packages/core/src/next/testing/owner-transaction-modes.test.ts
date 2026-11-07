/** PG is the authority for each access/isolation/deferrability transition, including savepoints. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { OWNER_TRANSACTION_MODE_STATES, createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { cleanups, harness } from "./owner-harness.js";

async function subject() {
  const h = harness();
  const client = await h.connect();
  await client.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
  await client.query("INSERT INTO t VALUES (1)");
  return { ...h, client };
}
async function finish(h: Awaited<ReturnType<typeof subject>>) {
  h.server.stepDown("closing");
  await h.engine.close();
  for (const close of cleanups.splice(0).reverse()) close();
}
async function code(action: () => Promise<unknown>): Promise<string> {
  try {
    await action();
    return "ok";
  } catch (error) {
    const e = error as { code?: string; sqlState?: string };
    return e.sqlState ?? e.code ?? String(error);
  }
}
const sorted = (rows: ReadonlyArray<readonly unknown[]>) =>
  [...rows].sort((a, b) => Number(a[0]) - Number(b[0]));

describe("PG transaction mode regressions", () => {
  it("does not escape READ ONLY through a savepoint, and rollback-to restores access", async () => {
    const h = await subject();
    const c = h.client;
    try {
      await c.query("BEGIN READ ONLY");
      await c.query("SAVEPOINT s");
      await expect(c.query("SET TRANSACTION READ WRITE")).rejects.toMatchObject({
        sqlState: "25001",
      });
      await c.query("ROLLBACK");
      await c.query("BEGIN");
      await c.query("SAVEPOINT s");
      await c.query("SET TRANSACTION READ ONLY");
      expect(h.writePath.writerBusy).toBe(false);
      await c.query("ROLLBACK TO s");
      expect(h.writePath.writerBusy).toBe(true);
      await c.query("INSERT INTO t VALUES (2)");
      await c.query("COMMIT");
      expect(sorted((await c.query("SELECT id FROM t")).rows())).toEqual([[1], [2]]);
    } finally {
      await finish(h);
    }
  });
  it("READ ONLY after a write keeps the prefix, its schema and its statement view", async () => {
    const h = await subject();
    const c = h.client;
    try {
      await c.query("BEGIN");
      await c.query("INSERT INTO t VALUES (2)");
      await c.query("CREATE TABLE u (id INT PRIMARY KEY)");
      await c.query("SET TRANSACTION READ ONLY");
      expect(h.writePath.writerBusy).toBe(true);
      expect((await c.query("SELECT id FROM t WHERE id = 2")).rows()).toEqual([[2]]);
      await c.query("SAVEPOINT s");
      await c.query("SELECT 1");
      await c.query("ROLLBACK TO s");
      await c.query("COMMIT");
      expect(sorted((await c.query("SELECT id FROM t")).rows())).toEqual([[1], [2]]);
      expect((await c.query("SELECT * FROM u")).rows()).toEqual([]);
    } finally {
      await finish(h);
    }
  });
  it("snapshot restrictions survive rollback-to and a failed READ ONLY write", async () => {
    const h = await subject();
    const c = h.client;
    try {
      await c.query("BEGIN READ ONLY, ISOLATION LEVEL READ COMMITTED");
      await c.query("SAVEPOINT s");
      await expect(c.query("INSERT INTO t VALUES (2)")).rejects.toMatchObject({
        sqlState: "25006",
      });
      await c.query("ROLLBACK TO s");
      await c.query("RELEASE s");
      await expect(c.query("SET TRANSACTION READ WRITE")).rejects.toMatchObject({
        sqlState: "25001",
      });
      await c.query("ROLLBACK");
      await c.query("BEGIN READ ONLY, ISOLATION LEVEL READ COMMITTED");
      await c.query("SAVEPOINT s");
      const before = h.server.stats.synchronous;
      await c.query("SELECT id FROM t WHERE id = 1");
      expect(h.server.stats.synchronous).toBe(before + 1);
      await c.query("ROLLBACK TO s");
      await c.query("RELEASE s");
      await c.query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
      await expect(
        c.query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
      ).rejects.toMatchObject({ sqlState: "25001" });
      await c.query("ROLLBACK");
    } finally {
      await finish(h);
    }
  });
  it("a released child permits READ WRITE again before a snapshot; SHOW does not freeze modes", async () => {
    const h = await subject();
    const c = h.client;
    try {
      await c.query("BEGIN");
      await c.query("SAVEPOINT s");
      await c.query("SET TRANSACTION READ ONLY");
      await c.query("RELEASE s");
      await c.query("SHOW transaction_isolation");
      await c.query("SET TRANSACTION READ WRITE");
      expect(h.writePath.writerBusy).toBe(true);
      await c.query("ROLLBACK");
      await c.query("SET TRANSACTION READ ONLY");
      await c.query("INSERT INTO t VALUES (2)");
    } finally {
      await finish(h);
    }
  });

  it("applies isolation metadata per session and resets it at the transaction end", async () => {
    const h = await subject();
    const c = h.client;
    try {
      await c.query("BEGIN ISOLATION LEVEL READ COMMITTED, READ ONLY, DEFERRABLE");
      expect((await c.query("SHOW transaction_isolation")).rows()).toEqual([["read committed"]]);
      expect((await c.query("SHOW transaction_isolation", [], { session: 1 })).rows()).toEqual([
        ["repeatable read"],
      ]);
      await c.query("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED, NOT DEFERRABLE");
      expect((await c.query("SHOW transaction_isolation")).rows()).toEqual([["read uncommitted"]]);
      await c.query("ROLLBACK");
      expect((await c.query("SHOW transaction_isolation")).rows()).toEqual([["repeatable read"]]);
      // Outside a transaction PostgreSQL warns and changes nothing (the protocol has no notices).
      await c.query("SET TRANSACTION READ ONLY, ISOLATION LEVEL READ COMMITTED");
      expect((await c.query("SHOW transaction_isolation")).rows()).toEqual([["repeatable read"]]);
      await c.query("INSERT INTO t VALUES (2)");
    } finally {
      await finish(h);
    }
  });
});

it.each(seedsFor("next-owner-transaction-modes", [0x7a2501]))(
  "generated PG per-attribute matrix (seed %i)",
  async (seed) => {
    const pg = new PGlite();
    await pg.exec("CREATE TABLE t(id INT PRIMARY KEY); INSERT INTO t VALUES(1)");
    const reach = createReach("next-owner-transaction-modes", OWNER_TRANSACTION_MODE_STATES, {
      minimum: 2,
    });
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            initialReadOnly: fc.boolean(),
            initialDeferrable: fc.boolean(),
            queryShape: fc.constantFrom("constant", "scan", "point"),
            duplicateModes: fc.boolean(),
            isolation: fc.constantFrom("READ COMMITTED", "READ UNCOMMITTED", "REPEATABLE READ"),
            depth: fc.integer({ min: 0, max: 2 }),
            first: fc.constantFrom("none", "query", "modify", "ddl", "show"),
            end: fc.constantFrom("COMMIT", "ROLLBACK"),
          }),
          async ({
            initialReadOnly,
            initialDeferrable,
            queryShape,
            duplicateModes,
            isolation,
            depth,
            first,
            end,
          }) => {
            const entered = new Set<string>();
            const other = isolation === "READ COMMITTED" ? "REPEATABLE READ" : "READ COMMITTED";
            for (const change of [
              "READ ONLY",
              "READ WRITE",
              `ISOLATION LEVEL ${isolation}`,
              `ISOLATION LEVEL ${other}`,
              "DEFERRABLE",
              "NOT DEFERRABLE",
            ]) {
              const h = await subject();
              const c = h.client;
              try {
                const begin = `BEGIN ISOLATION LEVEL ${isolation}, ${duplicateModes ? `READ ${initialReadOnly ? "WRITE" : "ONLY"}, ` : ""}READ ${initialReadOnly ? "ONLY" : "WRITE"}, ${initialDeferrable ? "" : "NOT "}DEFERRABLE`;
                await pg.exec(begin);
                await c.query(begin);
                entered.add(initialReadOnly ? "read-only begin" : "read-write begin");
                if (duplicateModes)
                  entered.add(initialReadOnly ? "last read-only wins" : "last read-write wins");
                for (let d = 0; d < depth; d++) {
                  await pg.exec(`SAVEPOINT s${String(d)}`);
                  await c.query(`SAVEPOINT s${String(d)}`);
                }
                const work =
                  first === "query"
                    ? queryShape === "constant"
                      ? "SELECT 1"
                      : queryShape === "scan"
                        ? "SELECT id FROM t"
                        : "SELECT id FROM t WHERE id = 1"
                    : first === "modify"
                      ? "INSERT INTO t VALUES(2)"
                      : first === "ddl"
                        ? "CREATE TABLE u(id INT PRIMARY KEY)"
                        : first === "show"
                          ? "SHOW transaction_isolation"
                          : null;
                let workCode = "ok";
                if (work !== null) {
                  workCode = await code(() => pg.query(work));
                  expect(await code(() => c.query(work))).toBe(workCode);
                  entered.add(first + " work");
                  if (first === "query") entered.add(queryShape + " snapshot");
                }
                const sql = "SET TRANSACTION " + change;
                const expected = await code(() => pg.query(sql));
                expect(
                  await code(() => c.query(sql)),
                  `${begin}; ${String(work)}; depth=${String(depth)}; ${sql}`,
                ).toBe(expected);
                if (expected === "ok") {
                  entered.add(
                    change === "READ ONLY"
                      ? "set read-only"
                      : change === "READ WRITE"
                        ? "set read-write"
                        : change.startsWith("ISOLATION")
                          ? "isolation metadata"
                          : change === "DEFERRABLE"
                            ? "deferrable"
                            : "not deferrable",
                  );
                  if (
                    change === "READ ONLY" &&
                    !initialReadOnly &&
                    workCode === "ok" &&
                    (first === "modify" || first === "ddl")
                  ) {
                    const probe =
                      first === "modify" ? "SELECT id FROM t WHERE id=2" : "SELECT id FROM u";
                    const expectedPrefix = (
                      await pg.query<Record<string, unknown>>(probe)
                    ).rows.map((row) => Object.values(row));
                    let actualPrefix: ReadonlyArray<readonly unknown[]> = [];
                    expect(
                      await code(async () => {
                        actualPrefix = (await c.query(probe)).rows();
                      }),
                    ).toBe("ok");
                    expect(actualPrefix).toEqual(expectedPrefix);
                    entered.add("write prefix retained");
                  }
                  if (depth > 0) {
                    // Alternate restoration and release without changing the snapshot boundary.
                    const rollback = change === "READ ONLY";
                    const close = `${rollback ? "ROLLBACK TO" : "RELEASE"} s0`;
                    await pg.exec(close);
                    await c.query(close);
                    entered.add(rollback ? "rollback modes" : "release modes");
                    if (rollback) {
                      await pg.exec("RELEASE s0");
                      await c.query("RELEASE s0");
                      const probe = `SET TRANSACTION ISOLATION LEVEL ${other}`;
                      const afterRollback = await code(() => pg.query(probe));
                      expect(await code(() => c.query(probe))).toBe(afterRollback);
                      if (afterRollback === "25001") entered.add("rollback snapshot boundary");
                    }
                  }
                } else {
                  entered.add(expected === "25P02" ? "read-only refusal" : "mode refusal");
                  expect(await code(() => c.query("SELECT 1"))).toBe(
                    await code(() => pg.query("SELECT 1")),
                  );
                  if (depth > 0 && workCode === "25006") {
                    await pg.exec("ROLLBACK TO s0; RELEASE s0");
                    await c.query("ROLLBACK TO s0");
                    await c.query("RELEASE s0");
                    const probe = "SET TRANSACTION READ WRITE";
                    expect(await code(() => c.query(probe))).toBe(
                      await code(() => pg.query(probe)),
                    );
                    entered.add("failed write snapshot");
                  }
                }
                await pg.exec(end);
                await c.query(end);
                entered.add(end === "COMMIT" ? "commit reset" : "rollback reset");
                const expectedRows = (await pg.query<{ id: number }>("SELECT id FROM t")).rows.map(
                  (row) => [row.id],
                );
                expect(sorted((await c.query("SELECT id FROM t")).rows())).toEqual(
                  sorted(expectedRows),
                );
                expect((await c.query("SHOW transaction_isolation")).rows()).toEqual([
                  ["repeatable read"],
                ]);
                expect(h.writePath.writerBusy).toBe(false);
              } finally {
                await pg.exec("ROLLBACK; DROP TABLE IF EXISTS u; DELETE FROM t WHERE id=2");
                await finish(h);
              }
            }
            reach.recordHistory(entered);
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
