/** PostgreSQL evaluates each candidate's row constraints/RETURNING before requesting
 * the next candidate. A sealed input retains a terminal SQL value error after its prefix.
 * Statement-end unique/FK checks remain deferred under the product contract. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { currentEngine } from "../../testing/differential-current.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  MODIFY_SOURCE_ERROR_ORDER_REACH,
  MODIFY_RETURNING_ERROR_ORDER_REACH,
  MODIFY_SOURCE_FAULT_ORDER_REACH,
  MODIFY_WINDOW_ERROR_ORDER_REACH,
  MODIFY_RETURNING_SCOPE_REACH,
} from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { ModifySpool } from "../exec/modify-spool.js";
import { WindowError } from "../exec/window-frames.js";
import { ExecutionError } from "../exec/errors.js";
import { TableWriterImpl } from "../write/transaction.js";
import { ChunkReader } from "../page/reader.js";
import { startModification } from "./modify-native-test-support.js";
import { oracleRows } from "./differential-adapter.js";
const kinds = ["insert", "nothing", "upsert", "update"] as const;
type Kind = (typeof kinds)[number];
async function state(run: () => Promise<unknown>): Promise<string | null> {
  try {
    await run();
    return null;
  } catch (error) {
    if (!(error instanceof Error)) throw error;
    if ("sqlState" in error && typeof error.sqlState === "string") return error.sqlState;
    if ("code" in error && typeof error.code === "string") return error.code;
    // Shipped errors predate native SQLSTATE classes; retain their independent outcome.
    if (/CHECK .*failed/.test(error.message)) return "23514";
    if (error.message.includes("cannot be null")) return "23502";
    if (/division by zero/i.test(error.message)) return "22012";
    throw error;
  }
}
async function setup(
  core: StorageCore,
  pg: PGlite,
  shipped: Awaited<ReturnType<typeof currentEngine>>,
  kind: Kind,
  count: number,
  check = true,
) {
  const all = async (sql: string) => {
    await pg.exec(sql);
    await shipped.run({ kind: "sql", sql });
    await core.execute(sql);
  };
  await all("DROP TABLE IF EXISTS dml_error");
  await all(
    `CREATE TABLE dml_error(id INTEGER PRIMARY KEY,a INTEGER NOT NULL${check ? " CHECK(a<0)" : ""})`,
  );
  if (kind !== "insert")
    await all(
      `INSERT INTO dml_error VALUES ${Array.from({ length: count }, (_, i) => `(${String(i)},-100)`).join(",")}`,
    );
  return async () => {
    const sql = "SELECT * FROM dml_error ORDER BY id",
      actual = await core.execute(sql);
    if (actual.kind !== "rows" || !("batches" in actual)) throw new Error("Expected rows");
    const expected = (await pg.query(sql)).rows;
    expect(oracleRows(actual)).toEqual(expected);
    expect((await shipped.run({ kind: "sql", sql })).rows).toEqual(expected);
    expect(core.spill.bytesInUse).toBe(0);
  };
}
function insertSql(kind: Kind, source: string): string {
  return `INSERT INTO dml_error ${source}${kind === "nothing" ? " ON CONFLICT DO NOTHING" : kind === "upsert" ? " ON CONFLICT(id) DO UPDATE SET a=EXCLUDED.a" : ""}`;
}
it.each(seedsFor("next-modify-source-error-order", [0xb5f5, 0xb5f6]))(
  "keeps terminal source errors behind earlier candidate rules seed %i",
  async (seed) => {
    const reach = createReach("DML source error order", MODIFY_SOURCE_ERROR_ORDER_REACH),
      core = await new StorageCore(256).open(),
      pg = new PGlite(),
      shipped = await currentEngine("memory");
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...kinds),
          fc.constantFrom(0, 1, 255, 256, 257),
          fc.integer({ min: 1, max: 4 }),
          fc.constantFrom("check", "null", "none"),
          fc.boolean(),
          async (kind, prefix, gap, rule, returning) => {
            reach.beginHistory();
            const bad = prefix + gap,
              count = bad + 1,
              compare = await setup(core, pg, shipped, kind, count);
            const value = `CASE WHEN ${rule === "none" ? "FALSE" : `id=${String(prefix)}`} THEN ${rule === "null" ? "NULL" : "100"} WHEN id=${String(bad)} THEN 100/(id-id) ELSE -100 END`;
            const source = `SELECT k,CASE WHEN ${rule === "null" ? `k=${String(prefix)}` : "FALSE"} THEN NULL ELSE 100/x END FROM (VALUES ${Array.from({ length: count }, (_, i) => `(${String(i)},${String(i === bad ? 0 : i === prefix && rule === "check" ? 1 : -1)})`).join(",")})input(k,x)`;
            const sql =
              (kind === "update" ? `UPDATE dml_error SET a=${value}` : insertSql(kind, source)) +
              (returning ? " RETURNING id,a" : "");
            const expected = rule === "check" ? "23514" : rule === "null" ? "23502" : "22012";
            expect(await state(() => pg.exec(sql))).toBe(expected);
            expect(await state(() => core.execute(sql))).toBe(expected);
            // Shipped's nullable arithmetic and batch-wide constraints can choose a
            // different error. It independently proves refusal/rollback, not PG identity.
            const oldState = await state(() => shipped.run({ kind: "sql", sql }));
            // Existing shipped DO NOTHING skips every conflicting proposal before row rules;
            // its nullable arithmetic also makes the later division a skipped NULL proposal.
            if (kind === "nothing") expect(oldState).toBeNull();
            else expect(oldState).not.toBeNull();
            await compare();
            reach.reach(kind);
            reach.reach(rule);
            reach.reach(returning ? "RETURNING" : "no RETURNING");
            reach.reach(prefix >= 255 ? "multiple input pages" : "short prefix");
            reach.reach("atomic rollback");
            reach.reach("zero spill");
          },
        ),
        {
          seed,
          numRuns: 20,
          examples: [
            ["insert", 0, 1, "check", false],
            ["nothing", 1, 2, "null", true],
            ["upsert", 255, 2, "none", false],
            ["update", 257, 1, "check", true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await shipped.close();
      await pg.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  },
  120000,
);
it.each(seedsFor("next-modify-returning-error-order", [0xb5f7]))(
  "does not move a later rule ahead of an earlier RETURNING error seed %i",
  async (seed) => {
    const reach = createReach("DML RETURNING error order", MODIFY_RETURNING_ERROR_ORDER_REACH),
      core = await new StorageCore(256).open(),
      pg = new PGlite(),
      shipped = await currentEngine("memory");
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("insert", "upsert", "update"),
          fc.constantFrom(0, 1, 255, 256),
          fc.integer({ min: 1, max: 4 }),
          fc.boolean(),
          async (form, prefix, gap, check) => {
            reach.beginHistory();
            const kind = form,
              bad = prefix + gap,
              count = bad + 1,
              compare = await setup(core, pg, shipped, kind, count, check);
            const invalid = check ? "1" : "NULL";
            const value = `CASE WHEN id=${String(prefix)} THEN -1 WHEN id=${String(bad)} THEN ${invalid} ELSE -2 END`;
            const sql =
              (kind === "update"
                ? `UPDATE dml_error SET a=${value}`
                : insertSql(
                    kind,
                    `SELECT k,CASE WHEN k=${String(prefix)} THEN -1 WHEN k=${String(bad)} THEN ${invalid} ELSE -2 END FROM (VALUES ${Array.from({ length: count }, (_, i) => `(${String(i)})`).join(",")})input(k)`,
                  )) + " RETURNING 100/(a+1) AS r";
            expect(await state(() => pg.exec(sql))).toBe("22012");
            // Authority cutover: shipped validates the full write batch before RETURNING and
            // raises the later CHECK. Preserve that evidence; native follows PG row order.
            expect(await state(() => shipped.run({ kind: "sql", sql }))).toBe(
              check ? "23514" : "23502",
            );
            expect(await state(() => core.execute(sql))).toBe("22012");
            await compare();
            reach.reach(kind);
            reach.reach(check ? "CHECK binding" : "NOT NULL without CHECK");
            reach.reach(
              Math.floor(prefix / 256) === Math.floor(bad / 256) ? "same batch" : "across batches",
            );
            reach.reach("PG row order");
            reach.reach("shipped batch-order difference");
            reach.reach("atomic rollback");
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            ["insert", 0, 1, true],
            ["update", 255, 1, false],
            ["upsert", 256, 2, true],
            ["insert", 0, 1, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await shipped.close();
      await pg.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  },
  120000,
);

it.each(seedsFor("next-modify-source-fault-order", [0xb5f8]))(
  "never replays source cancellation, unsupported, storage or admission faults seed %i",
  async (seed) => {
    const reach = createReach("DML source fault order", MODIFY_SOURCE_FAULT_ORDER_REACH),
      core = await new StorageCore().open();
    try {
      await core.execute(
        "CREATE TABLE dml_error(id INTEGER PRIMARY KEY,a INTEGER NOT NULL CHECK(a<0))",
      );
      await fc.assert(
        fc.asyncProperty(fc.constantFrom("57014", "0A000", "XX000", "53200"), async (code) => {
          reach.beginHistory();
          const fault = new ExecutionError(
            "injected source boundary fault",
            "MINNOW_SOURCE_FAULT",
            code,
          );
          // eslint-disable-next-line @typescript-eslint/unbound-method -- The saved method is always invoked with the original receiver via call.
          const original = ModifySpool.prototype.push;
          let calls = 0;
          const source = vi.spyOn(ModifySpool.prototype, "push").mockImplementation(function (
              this: ModifySpool,
              ...args
            ) {
              if (++calls >= 2) throw fault;
              return original.call(this, ...args);
            }),
            writer = vi.spyOn(TableWriterImpl.prototype, "insert");
          try {
            // The source fault is outside SQL candidate evaluation. Its preceding invalid
            // row must never reach writer/CHECK and replace the infrastructure error.
            expect(
              await state(() => core.execute("INSERT INTO dml_error VALUES(1,1),(2,-1)")),
            ).toBe(code);
            expect(writer).not.toHaveBeenCalled();
            expect(calls).toBeGreaterThanOrEqual(2);
          } finally {
            source.mockRestore();
            writer.mockRestore();
          }
          const result = await core.execute("SELECT * FROM dml_error");
          if (result.kind !== "rows" || !("batches" in result)) throw new Error("Expected rows");
          expect(oracleRows(result)).toEqual([]);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach(code);
          reach.reach("no writer continuation");
          reach.reach("zero spill");
        }),
        { seed, numRuns: 8, examples: [["57014"], ["0A000"], ["XX000"], ["53200"]] },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  },
  60000,
);

it.each(seedsFor("next-modify-window-error-order", [0xb5f9]))(
  "keeps a typed terminal window source failure after its candidate prefix seed %i",
  async (seed) => {
    const reach = createReach("DML window error metadata", MODIFY_WINDOW_ERROR_ORDER_REACH),
      core = await new StorageCore().open();
    try {
      await core.execute("CREATE TABLE window_error(id INTEGER PRIMARY KEY,a INTEGER CHECK(a<0))");
      await fc.assert(
        fc.asyncProperty(fc.boolean(), async (check) => {
          reach.beginHistory();
          const error = new WindowError("argument of nth_value must be greater than zero", "22016");
          // This is the source-boundary contract, not new SQL window-argument lowering support.
          // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with its original receiver via call.
          const original = ModifySpool.prototype.push;
          let calls = 0;
          const source = vi.spyOn(ModifySpool.prototype, "push").mockImplementation(function (
            this: ModifySpool,
            ...args
          ) {
            if (++calls >= 2) throw error;
            return original.call(this, ...args);
          });
          try {
            expect(
              await state(() =>
                core.execute(
                  `INSERT INTO window_error VALUES(1,${check ? "100" : "-100"}),(2,-100)`,
                ),
              ),
            ).toBe(check ? "23514" : "22016");
          } finally {
            source.mockRestore();
          }
          const rows = await core.execute("SELECT * FROM window_error");
          if (rows.kind !== "rows" || !("batches" in rows)) throw new Error("Expected rows");
          expect(oracleRows(rows)).toEqual([]);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach(check ? "earlier CHECK" : "window argument error");
          reach.reach("atomic rollback");
          reach.reach("zero spill");
        }),
        { seed, numRuns: 6, examples: [[true], [false]] },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  },
  60000,
);
it.each(seedsFor("next-modify-returning-scope", [0xb5fa]))(
  "requires admitted complete-row validation for raw RETURNING callers seed %i",
  async (seed) => {
    const reach = createReach("RETURNING row validation scope", MODIFY_RETURNING_SCOPE_REACH),
      core = await new StorageCore().open();
    try {
      await core.execute("CREATE TABLE scope_error(id INTEGER PRIMARY KEY,a INTEGER NOT NULL)");
      await fc.assert(
        fc.asyncProperty(fc.boolean(), async (bound) => {
          reach.beginHistory();
          const sql = "INSERT INTO scope_error VALUES(1,-1),(2,NULL) RETURNING 100/(a+1)",
            lowered = core.lowerer.lower(sql, core.context());
          if (lowered.kind !== "modify") throw new Error("Expected modify");
          const fixture = bound ? await startModification(core, sql, "atExecuted") : null,
            tx = fixture?.tx ?? (await core.harness.path.begin());
          const execution =
            fixture?.execution ??
            core.executor.prepare(lowered.plan).execute({
              snapshot: null,
              statement: tx.statement({
                rules: () => ({ check: () => null, generate: () => undefined }),
              }),
              parameters: [],
              statementTime: 0n,
              governor: core.harness.governor,
              spill: core.spill,
              queryId: 1000031,
              reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
              sequences: null,
              sequenceSession: null,
              returning: "atExecuted",
            });
          try {
            expect(await state(() => execution.executed)).toBe(bound ? "22012" : "0A000");
            await execution.delivered.catch(() => undefined);
          } finally {
            execution.cancel("fixture done");
            await Promise.allSettled([execution.executed, execution.delivered]);
            tx.rollback();
          }
          reach.reach(bound ? "bound native error order" : "raw missing capability refusal");
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("zero spill");
        }),
        { seed, numRuns: 6, examples: [[false], [true]] },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  },
  60000,
);
