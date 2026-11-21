/**
 * The public write routes stay within the write batch limit (MAX_WRITE_BATCH_ROWS, 65,536 rows):
 * SQL INSERT, UPDATE, DELETE and ON CONFLICT push executor batches, at most BATCH_ROWS each, to the
 * write path, whatever the statement's total. Checked at its boundary: statements touching
 * 2 × 65,536 + 1 rows succeed, and every batch the writer receives is at most BATCH_ROWS.
 */
import { afterEach, expect, it, vi } from "vitest";
import { MAX_WRITE_BATCH_ROWS } from "../access/modify.js";
import { BATCH_ROWS } from "../vector/vector.js";
import { TableWriterImpl } from "../write/transaction.js";
import { StorageCore } from "./storage-core.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { need } from "../write/bytes.js";

afterEach(() => {
  vi.restoreAllMocks();
});

it("keeps every SQL write batch within BATCH_ROWS for statements past the batch limit", async () => {
  const total = 2 * MAX_WRITE_BATCH_ROWS + 1;
  let largest = 0;
  for (const method of ["insert", "update", "delete", "conflicts"] as const) {
    // eslint-disable-next-line @typescript-eslint/unbound-method -- called with apply on the writer
    const original = TableWriterImpl.prototype[method] as (...args: unknown[]) => unknown;
    vi.spyOn(TableWriterImpl.prototype, method).mockImplementation(function (
      this: TableWriterImpl,
      ...args: unknown[]
    ) {
      largest = Math.max(largest, (args[0] as { length: number }).length);
      return original.apply(this, args) as never;
    });
  }
  const core = await new StorageCore(8192).open();
  await core.execute("CREATE TABLE w(id INTEGER PRIMARY KEY, v INTEGER)");
  await core.execute(`INSERT INTO w SELECT g, g FROM generate_series(1, ${String(total)}) g`);
  await core.execute("UPDATE w SET v = v + 1");
  await core.execute(
    `INSERT INTO w SELECT g, 0 FROM generate_series(1, ${String(total)}) g ON CONFLICT (id) DO UPDATE SET v = w.v + 1`,
  );
  await core.execute("DELETE FROM w WHERE id % 2 = 0");
  const result = await core.execute("SELECT count(*) AS n, min(v) AS lo, max(v) AS hi FROM w");
  if (!("batches" in result)) throw new Error("count rows");
  const first = result.batches[0];
  if (first === undefined) throw new Error("no count batch");
  const at = (column: number) => Number(valueAt(need(first.columns[column]), 0));
  // Odd ids remain; each was inserted (v = id), updated (+1) and upserted (+1).
  expect([at(0), at(1), at(2)]).toEqual([MAX_WRITE_BATCH_ROWS + 1, 3, total + 2]);
  expect(largest).toBeGreaterThan(0);
  expect(largest, "a SQL write batch above BATCH_ROWS").toBeLessThanOrEqual(BATCH_ROWS);
}, 300_000);
