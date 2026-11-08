/** Sequence mutations are dynamic effects; lock intent must come from public lowering metadata. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, OWNER_SEQUENCE_EFFECT_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { guardedSequences } from "../owner/transaction-modes.js";
import type { PlannedStatement, RunContext } from "../owner/engine.js";
import { cleanups, harness } from "./owner-harness.js";

const context: RunContext = {
  signal: new AbortController().signal,
  windowBytes: 65536,
  request: null,
  frameRows: null,
};
const normalized = (rows: ReadonlyArray<readonly unknown[]>) =>
  rows.map((row) =>
    row.map((value) => {
      if (value === null) return null;
      if (typeof value !== "number" && typeof value !== "bigint" && typeof value !== "string")
        throw new TypeError("Expected a scalar sequence result");
      return String(value);
    }),
  );
const expressions = [
  { kind: "currval", sql: "SELECT currval('s')", params: [] },
  {
    kind: "nonexecuted inverse parameter",
    sql: "SELECT CASE WHEN $1 THEN 7 ELSE nextval('s') END",
    params: [true],
  },
  { kind: "nextval", sql: "SELECT nextval('s')", params: [] },
  { kind: "nonexecuted filter", sql: "SELECT nextval('s') WHERE false", params: [] },
  {
    kind: "nonexecuted case",
    sql: "SELECT CASE WHEN true THEN 7 ELSE nextval('s') END",
    params: [],
  },
  { kind: "executed case", sql: "SELECT CASE WHEN false THEN 7 ELSE nextval('s') END", params: [] },
  {
    kind: "nonexecuted parameter",
    sql: "SELECT CASE WHEN $1 THEN nextval('s') ELSE 7 END",
    params: [false],
  },
  {
    kind: "executed parameter",
    sql: "SELECT CASE WHEN $1 THEN nextval('s') ELSE 7 END",
    params: [true],
  },
] as const;

it.each(seedsFor("next-owner-sequence-effects", [0x5e9e]))(
  "generated PG sequence effects (seed %i)",
  async (seed) => {
    const pg = new PGlite();
    const reach = createReach("next-owner-sequence-effects", OWNER_SEQUENCE_EFFECT_STATES, {
      minimum: 2,
    });
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            cases: fc.shuffledSubarray([...expressions], {
              minLength: expressions.length,
              maxLength: expressions.length,
            }),
            readOnly: fc.boolean(),
            savepoint: fc.boolean(),
            prepared: fc.boolean(),
          }),
          async ({ cases, readOnly, savepoint, prepared }) => {
            const h = harness();
            const c = await h.connect();
            const preparations = vi.spyOn(h.executor, "prepare");
            const entered = new Set<string>();
            let pgPrepared = false;
            try {
              await pg.exec("DROP SEQUENCE IF EXISTS s; CREATE SEQUENCE s");
              await c.query("CREATE SEQUENCE s");
              await pg.query("SELECT nextval('s')");
              await c.query("SELECT nextval('s')");
              for (const item of cases) {
                const params: readonly boolean[] = item.params;
                const begin = `BEGIN READ ${readOnly ? "ONLY" : "WRITE"}`;
                await pg.exec(begin);
                await c.query(begin);
                if (savepoint) {
                  await pg.exec("SAVEPOINT s");
                  await c.query("SAVEPOINT s");
                }
                let preparedId: number | null = null;
                if (prepared) {
                  await pg.exec("PREPARE seq_probe AS " + item.sql);
                  pgPrepared = true;
                  preparedId = (await c.prepare(item.sql)).id;
                }
                const pgSQL = prepared
                  ? `EXECUTE seq_probe${params.length === 0 ? "" : `(${params[0] ? "true" : "false"})`}`
                  : item.sql;
                let expected: unknown[][] | undefined;
                let expectedCode = "ok";
                try {
                  expected = (
                    await pg.query<Record<string, unknown>>(pgSQL, prepared ? [] : [...params])
                  ).rows.map((row) => Object.values(row));
                } catch (error) {
                  expectedCode = (error as { code: string }).code;
                }
                let actual: ReadonlyArray<readonly unknown[]> | undefined;
                let actualCode = "ok";
                try {
                  actual = (
                    await (preparedId === null
                      ? c.query(item.sql, [...params])
                      : c.execute(preparedId, [...params]))
                  ).rows();
                } catch (error) {
                  actualCode = (error as { sqlState: string }).sqlState;
                }
                expect(actualCode, item.sql).toBe(expectedCode);
                if (expected !== undefined)
                  expect(normalized(actual ?? []), item.sql).toEqual(normalized(expected));
                if (item.kind === "currval") {
                  const last = preparations.mock.results.at(-1);
                  if (last?.type !== "return") throw new Error("currval did not prepare");
                  expect(last.value.usesSequences).toBe(true);
                  entered.add("sequence metadata");
                }
                entered.add(item.kind);
                entered.add(readOnly ? "read-only effects" : "read-write effects");
                entered.add(prepared ? "prepared effects" : "direct effects");
                if (savepoint) entered.add("savepoint effects");
                await pg.exec("ROLLBACK");
                await c.query("ROLLBACK");
                if (prepared) {
                  await pg.exec("DEALLOCATE seq_probe");
                  pgPrepared = false;
                }
                // Refused calls consume no value, including conditional calls on the error path.
                const next = (await pg.query<{ n: unknown }>("SELECT nextval('s') AS n")).rows.map(
                  (row) => [row.n],
                );
                expect(normalized((await c.query("SELECT nextval('s') AS n")).rows())).toEqual(
                  normalized(next),
                );
              }
              reach.recordHistory(entered);
            } finally {
              await pg.exec("ROLLBACK");
              if (pgPrepared) await pg.exec("DEALLOCATE seq_probe");
              h.server.stepDown("closing");
              await h.engine.close();
              for (const close of cleanups.splice(0).reverse()) close();
              preparations.mockRestore();
            }
          },
        ),
        { seed, numRuns: 100 },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  },
  120_000,
);

it("the sequence-access guard checks next and set at call time, before delegating", () => {
  let readOnly = false;
  const access = { next: vi.fn(() => 11), set: vi.fn(() => 12) };
  const guarded = guardedSequences(access, () => readOnly);
  const out = { repr: "int64" as const, values: new BigInt64Array(1) };
  expect(guarded.next(1, 1, out, 0)).toBe(11);
  expect(guarded.set(1, 2n, true)).toBe(12);
  readOnly = true;
  expect(() => guarded.next(1, 1, out, 0)).toThrow(expect.objectContaining({ sqlState: "25006" }));
  expect(() => guarded.set(1, 3n, false)).toThrow(expect.objectContaining({ sqlState: "25006" }));
  expect(access.next).toHaveBeenCalledTimes(1);
  expect(access.set).toHaveBeenCalledTimes(1);
});

it.each(["update", "noKeyUpdate", "share", "keyShare"] as const)(
  "owner honors public effective %s metadata on normal and point paths",
  async (rowLocking) => {
    const h = harness();
    const c = await h.connect();
    try {
      await c.query("CREATE TABLE t(id INT PRIMARY KEY)");
      for (const point of [false, true]) {
        const session = h.engine.openSession("metadata", 1);
        try {
          await session.begin(true, context.signal);
          const plan = h.engine.plan(point ? "SELECT id FROM t WHERE id=1" : "SELECT id FROM t");
          const locked: PlannedStatement = {
            ...plan,
            lowered: { ...plan.lowered, rowLocking },
          };
          if (point)
            expect(() => session.runNow(locked, [], context)).toThrow(
              expect.objectContaining({ sqlState: "25006" }),
            );
          else
            await expect(session.run(locked, [], context).done).rejects.toMatchObject({
              sqlState: "25006",
            });
          expect(session.transaction).toBe("failed");
        } finally {
          session.close();
        }
      }
    } finally {
      h.server.stepDown("closing");
      await h.engine.close();
      for (const close of cleanups.splice(0).reverse()) close();
    }
  },
);

it("owner consumes ordered public mode items without collapsing access transitions", async () => {
  const h = harness();
  try {
    for (const reversed of [false, true]) {
      const session = h.engine.openSession("ordered", 1);
      try {
        await session.begin(false, context.signal);
        await session.run(h.engine.plan("SAVEPOINT s"), [], context).done;
        const plan = h.engine.plan(
          reversed ? "SET TRANSACTION READ ONLY" : "SET TRANSACTION READ WRITE",
        );
        const values = reversed
          ? (["readWrite", "readOnly"] as const)
          : (["readOnly", "readWrite"] as const);
        const lowered = {
          ...plan.lowered,
          modeItems: values.map((value) => ({ kind: "access" as const, value })),
        };
        const run = session.run({ ...plan, lowered }, [], context);
        if (reversed) {
          await run.done;
          expect(session.readOnly).toBe(true);
        } else await expect(run.done).rejects.toMatchObject({ sqlState: "25001" });
      } finally {
        session.close();
      }
    }
  } finally {
    h.server.stepDown("closing");
    await h.engine.close();
    for (const close of cleanups.splice(0).reverse()) close();
  }
});

it("public none locking metadata leaves ordinary reads allowed", async () => {
  const h = harness();
  const c = await h.connect();
  try {
    await c.query("CREATE TABLE t(id INT PRIMARY KEY)");
    for (const point of [false, true]) {
      const session = h.engine.openSession("no-lock", 1);
      try {
        await session.begin(true, context.signal);
        const plan = h.engine.plan(point ? "SELECT id FROM t WHERE id=1" : "SELECT id FROM t");
        const unlocked: PlannedStatement = {
          ...plan,
          lowered: { ...plan.lowered, rowLocking: "none" },
        };
        if (point) {
          const run = session.runNow(unlocked, [], context);
          expect(run).not.toBeNull();
          if (run !== null && "continued" in run) await run.continued.done;
        } else await session.run(unlocked, [], context).done;
        expect(session.transaction).toBe("open");
      } finally {
        session.close();
      }
    }
  } finally {
    h.server.stepDown("closing");
    await h.engine.close();
    for (const close of cleanups.splice(0).reverse()) close();
  }
});
