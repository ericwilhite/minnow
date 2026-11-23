/**
 * An INSERT of an aggregate wider than the memtable can take is decided, never spun.
 *
 * Astra's probe in the review of #58: at 64 MiB, `INSERT INTO t SELECT 11, string_agg($1, ''), 'x'
 * FROM generate_series(1, 11)` with a 1,000,000-character `$1` never finished on main 47e7bf7e9:
 * neither a 10 s abort signal nor the 3 s stall check fired. It is #57's row-wait spin
 * (`WritePathImpl.#waitForRows` giving its rows back to its own admission's reclaim): with that
 * fix disabled, the retry guard reports "write path memtable wait: 65 retries without progress".
 * With it, the statement is refused typed in milliseconds.
 *
 * Law (GOVERNOR-WAIT-POLICY, next/memory/README.md): every request is served, or refused with
 * 53200 before its effect; none waits forever. PostgreSQL statement atomicity for the refusal.
 */
import { afterEach, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

afterEach(() => {
  Governor.stallAfterMs = null;
});

it("decides an aggregate INSERT at 64 MiB: written or refused typed, never spun", async () => {
  Governor.stallAfterMs = 3_000;
  const core = new StorageCore();
  Object.defineProperty(core.harness, "governor", { value: new Governor(64 * 2 ** 20) });
  await core.open();
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT, w TEXT)");
    const outcome: unknown = await core
      .execute("INSERT INTO t SELECT 11, string_agg($1, ''), 'x' FROM generate_series(1, 11)", [
        "a".repeat(1_000_000),
      ])
      .then(
        () => "written",
        (error: unknown) => error,
      );
    if (outcome !== "written")
      expect(outcome, "refused typed, never a spin or stall").toMatchObject({
        name: "MemoryAdmissionError",
        sqlState: "53200",
      });
    const rows = await core.execute("SELECT id FROM t");
    if (!("batches" in rows)) throw new Error("rows");
    expect(oracleRows(rows).length, "statement atomicity").toBe(outcome === "written" ? 1 : 0);
    expect(core.harness.path.failure).toBeNull();
  } finally {
    await core.close();
  }
}, 60_000);
