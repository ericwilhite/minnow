/**
 * A read by key fetches its own row, not the memtable rows stored around it.
 *
 * Law (next/memory/README.md, GOVERNOR-WAIT-POLICY point 5): a read whose need fits the cap
 * returns its exact rows; a refusal is a typed `MemoryAdmissionError` (53200), never an untyped
 * error. A lookup's sizing measures the rows it selects (page/read-bounds.ts
 * `ReadSource.locators`), so execution must decode only those rows: before the fix, the memory
 * scan decoded a selected row's chunk from its first row, materialized a wide neighbour that
 * sizing never funded, and failed with `RangeError: Admitted memory output did not fit`
 * (`wide-insert-admission`, seeds 23297 and 23298).
 *
 * The need of each read here is at most 3 × 1 MiB + one 4 MiB read step + 8 MiB, far below
 * 32 MiB, so every read must return its row.
 */
import { afterEach, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const MiB = 1 << 20;
const CAP = 32 * MiB;

afterEach(() => {
  Governor.stallAfterMs = null;
});

it.each([
  { wide: 200_000, form: "values" },
  { wide: 200_000, form: "select" },
  { wide: MiB, form: "values" },
  { wide: MiB, form: "select" },
] as const)(
  "reads each row by key beside $wide-character neighbours ($form)",
  async ({ wide, form }) => {
    Governor.stallAfterMs = 2_000;
    const core = new StorageCore();
    Object.defineProperty(core.harness, "governor", { value: new Governor(CAP) });
    await core.open();
    try {
      await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
      // The small rows sit before, between and after the wide ones in one memtable chunk.
      const values = new Map<number, string>([
        [1, "old"],
        [2, "w".repeat(wide)],
        [3, "s".repeat(4096)],
        [4, "x".repeat(wide)],
        [5, "tail"],
      ]);
      for (const [id, value] of values)
        await core.execute(
          form === "values"
            ? `INSERT INTO t VALUES (${String(id)}, $1)`
            : `INSERT INTO t SELECT ${String(id)}, $1`,
          [value],
        );
      for (const [id, value] of values) {
        const result = await core.execute(`SELECT v FROM t WHERE id = ${String(id)}`);
        if (!("batches" in result)) throw new Error("rows expected");
        const rows = oracleRows(result);
        expect(rows.length, `id ${String(id)}`).toBe(1);
        expect(rows[0]?.v === value, `id ${String(id)}: its value`).toBe(true);
      }
      expect(core.harness.path.failure).toBeNull();
    } finally {
      await core.close();
    }
  },
);
