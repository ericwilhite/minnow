/**
 * A multi-row write beside another connection's read is written at once, or once the read ends:
 * never refused part way.
 *
 * The memtable charge grew only from free memory, relieved, then refused: beside a held 3 MiB
 * read at 32 MiB, `INSERT INTO t SELECT g, $1 FROM generate_series(1, 6) g` with 1 MiB rows was
 * refused part way ("write path memtable … (2115724 bytes, 0 available)"). Its steps now wait in
 * the governor for what relief cannot free, carrying the statement's holdings
 * (write/engine.ts `#waitForRows`, `Statement.waitHoldings`): refused typed only when they could
 * never be served beside them.
 *
 * Law (GOVERNOR-WAIT-POLICY point 5, next/memory/README.md): a statement that fits once a finite
 * holder releases waits for it and is served; PostgreSQL statement atomicity for any refusal.
 */
import fc from "fast-check";
import { afterEach, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { createReach, MULTI_ROW_BESIDE_READ_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const MiB = 1 << 20;
const PENDING = Symbol("pending");
const within = <T>(work: Promise<T>, ms: number) =>
  Promise.race([
    work,
    new Promise<typeof PENDING>((resolve) => {
      setTimeout(() => {
        resolve(PENDING);
      }, ms);
    }),
  ]);

afterEach(() => {
  Governor.stallAfterMs = null;
});

async function history(
  rows: number,
  width: number,
  read: number,
  memtableRows: number | undefined,
  reach?: ReturnType<typeof createReach>,
): Promise<void> {
  Governor.stallAfterMs = 5_000;
  const core = new StorageCore(memtableRows);
  Object.defineProperty(core.harness, "governor", { value: new Governor(32 * MiB) });
  await core.open();
  let end: (() => Promise<void>) | null = null;
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    if (read > 0) {
      await core.execute("CREATE TABLE r(id INT PRIMARY KEY, v TEXT)");
      await core.execute("INSERT INTO r VALUES (1, $1)", ["b".repeat(read)]);
      const lowered = core.lowerer.lower("SELECT id, v FROM r", core.context());
      if (lowered.kind !== "query") throw new Error("query plan required");
      const other = core.start(core.executor.prepare(lowered.plan));
      await new Promise((resolve) => setTimeout(resolve, 100));
      end = async () => {
        for (let frame = await other.execution.results.next(); frame !== null;) {
          frame.release();
          frame = await other.execution.results.next();
        }
        await other.execution.delivered;
        other.release();
      };
      reach?.reach("beside a held read");
    }
    const sql = `INSERT INTO t SELECT g, $1 FROM generate_series(1, ${String(rows)}) g`;
    const statement = core.execute(sql, ["a".repeat(width)]).then(
      () => "written" as const,
      (error: unknown) => error,
    );
    const context = `${sql} (${String(width)}-byte rows) beside a ${String(read)}-byte read, memtables of ${String(memtableRows ?? "default")} rows`;
    const early = await within(statement, 1_000);
    if (early !== PENDING) expect(early, `${context}: never refused`).toBe("written");
    if (end !== null) {
      await end();
      end = null;
    }
    expect(await statement, `${context}: written once the read ends`).toBe("written");
    reach?.reach(early === PENDING ? "waited for the read" : "written at once");
    const ids = await core.execute("SELECT id FROM t ORDER BY id");
    if (!("batches" in ids)) throw new Error("rows");
    expect(oracleRows(ids).map((row) => Number(row.id))).toEqual(
      Array.from({ length: rows }, (_, i) => i + 1),
    );
    expect(core.harness.path.failure).toBeNull();
    if (memtableRows === 2) reach?.reach("two-row memtables");
  } finally {
    if (end !== null) await end();
    await core.close();
  }
}

it("waits for a held 3 MiB read, then writes six 1 MiB rows (two-row memtables)", async () => {
  await history(6, MiB, 3 * MiB, 2);
}, 60_000);

it("cancels a multi-row write while it waits beside a read, writing nothing", async () => {
  Governor.stallAfterMs = 5_000;
  const core = new StorageCore(2);
  Object.defineProperty(core.harness, "governor", { value: new Governor(32 * MiB) });
  await core.open();
  let end: (() => Promise<void>) | null = null;
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    await core.execute("CREATE TABLE r(id INT PRIMARY KEY, v TEXT)");
    await core.execute("INSERT INTO r VALUES (1, $1)", ["b".repeat(3 * MiB)]);
    const lowered = core.lowerer.lower("SELECT id, v FROM r", core.context());
    if (lowered.kind !== "query") throw new Error("query plan required");
    const other = core.start(core.executor.prepare(lowered.plan));
    await new Promise((resolve) => setTimeout(resolve, 100));
    end = async () => {
      for (let frame = await other.execution.results.next(); frame !== null;) {
        frame.release();
        frame = await other.execution.results.next();
      }
      await other.execution.delivered;
      other.release();
    };
    const cancel = new AbortController();
    const statement = core
      .execute(
        "INSERT INTO t SELECT g, $1 FROM generate_series(1, 6) g",
        ["a".repeat(MiB)],
        cancel.signal,
      )
      .then(
        () => "written" as const,
        (error: unknown) => error,
      );
    expect(await within(statement, 1_000), "it waits beside the read").toBe(PENDING);
    cancel.abort(new Error("cancelled by the test"));
    const outcome = await within(statement, 2_000);
    expect(outcome, "cancellation ends the wait").not.toBe(PENDING);
    expect(outcome).not.toBe("written");
    await end();
    end = null;
    const ids = await core.execute("SELECT id FROM t ORDER BY id");
    if (!("batches" in ids)) throw new Error("rows");
    expect(oracleRows(ids), "nothing written").toEqual([]);
    await core.execute("INSERT INTO t VALUES (100, 'after')");
    expect(core.harness.path.failure).toBeNull();
  } finally {
    if (end !== null) await end();
    await core.close();
  }
}, 60_000);

for (const seed of seedsFor("next-multi-row-beside-read", [0x5f01, 0x5f02]))
  it(`multi-row writes beside a read are written, never refused part way (seed ${String(seed)})`, async () => {
    const reach = createReach("multi-row beside read", MULTI_ROW_BESIDE_READ_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 10 }),
        fc.constantFrom(524_288, MiB, 1_500_000),
        fc.constantFrom(0, MiB, 2 * MiB, 3 * MiB, 3_500_000),
        fc.constantFrom(2, undefined),
        async (rows, width, read, memtableRows) => {
          reach.beginHistory();
          await history(rows, width, read, memtableRows, reach);
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [6, MiB, 3 * MiB, 2],
          [10, MiB, 3 * MiB, 2],
          [10, 1_500_000, 3_500_000, undefined],
        ],
      },
    );
    reach.assertReached();
  }, 600_000);
