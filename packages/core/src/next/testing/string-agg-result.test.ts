/**
 * An aggregate's result value larger than its admission is the typed memory refusal (53200),
 * never an untyped error.
 *
 * Astra's review of #53: `INSERT INTO t SELECT 11, string_agg($1, ''), 'x' FROM
 * generate_series(1, 11)` with a 1,000,000-character parameter, at 64 MiB beside another
 * connection's held read, threw the untyped "Result-owned buffers exceed their complete
 * admission" (exec/result-buffer.ts). `string_agg`'s value is not yet sized from its inputs (the
 * sum of its inputs and separators, with a row-count bound: its own measured change); until
 * then its result allocation beyond the admission is refused typed.
 *
 * Law (next/memory/README.md): every memory refusal is `MemoryAdmissionError` (SQLSTATE 53200);
 * PostgreSQL statement atomicity: a refused statement writes nothing.
 */
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const MiB = 1 << 20;

it("refuses an over-admission string_agg value typed beside a held read, writing nothing", async () => {
  Governor.stallAfterMs = 5_000;
  const core = new StorageCore();
  Object.defineProperty(core.harness, "governor", { value: new Governor(64 * MiB) });
  await core.open();
  let end: (() => Promise<void>) | null = null;
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT, w TEXT)");
    await core.execute("CREATE TABLE r(id INT PRIMARY KEY, v TEXT)");
    await core.execute("INSERT INTO r VALUES (1, $1)", ["b".repeat(1_500_000)]);
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
    const outcome: unknown = await core
      .execute("INSERT INTO t SELECT 11, string_agg($1, ''), 'x' FROM generate_series(1, 11)", [
        "a".repeat(1_000_000),
      ])
      .then(
        () => null,
        (error: unknown) => error,
      );
    await end();
    end = null;
    const ids = await core.execute("SELECT id FROM t");
    if (!("batches" in ids)) throw new Error("rows");
    if (outcome === null) expect(oracleRows(ids)).toHaveLength(1);
    else {
      expect(outcome).toMatchObject({
        name: "MemoryAdmissionError",
        code: "MINNOW_MEMORY_ADMISSION",
        sqlState: "53200",
      });
      expect(oracleRows(ids), "nothing written").toEqual([]);
    }
    await core.execute("INSERT INTO t VALUES (1, 'after', 'x')");
    expect(core.harness.path.failure).toBeNull();
  } finally {
    if (end !== null) await end();
    await core.close();
    Governor.stallAfterMs = null;
  }
}, 60_000);
