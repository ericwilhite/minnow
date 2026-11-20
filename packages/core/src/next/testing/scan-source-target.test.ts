/**
 * Every scan sizes its batch target from the bounded demand of its source (exec/plan.ts
 * sourceTarget): a retained resident source of known rows takes indexReadTarget, not the
 * cap-wide batchTargetFor. #16 split the scan recipe and #17's change reached only the
 * page-scan copy; one shared recipe now serves both forms.
 */
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { indexReadTarget } from "../exec/key-order.js";
import { QueryRuntime } from "../exec/run.js";
import { batchTargetFor, Governor } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import { BATCH_ROWS } from "../vector/vector.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const ROWS = 20;
const CAP = 4 << 20;
let core: StorageCore;

beforeAll(async () => {
  // A small table that stays in the memtable: its query retains it as a resident source.
  core = await new StorageCore(8192).open();
  await core.execute("CREATE TABLE r (id INTEGER PRIMARY KEY, v INTEGER, s TEXT)");
  await core.writeBatch(
    "insertBatch",
    "r",
    Array.from({ length: ROWS }, (_, i) => ({ id: i + 1, v: i, s: `s${String(i)}` })),
  );
});
afterAll(async () => {
  await core.close();
});

/** Runs `sql` under a CAP governor; returns its rows and the scan operator's admitted minimum. */
async function scanMinimum(sql: string): Promise<{ rows: unknown[]; minimum: number }> {
  const governor = new Governor(CAP);
  const nativePrepare = core.executor.prepare.bind(core.executor);
  const prepare = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
    const prepared = nativePrepare(plan);
    return { ...prepared, execute: (request) => prepared.execute({ ...request, governor }) };
  });
  const minimums: number[] = [];
  // eslint-disable-next-line @typescript-eslint/unbound-method -- captured once; wrapper preserves receiver
  const admit = QueryRuntime.prototype.admit;
  const admits = vi.spyOn(QueryRuntime.prototype, "admit").mockImplementation(function (
    this: QueryRuntime,
    label,
    minimum,
    ...rest
  ) {
    if (label.startsWith("scan#")) minimums.push(minimum);
    return admit.call(this, label, minimum, ...rest);
  });
  try {
    const result = await core.execute(sql);
    if (result.kind !== "rows" || !("batches" in result)) throw new Error(`No rows: ${sql}`);
    expect(minimums, `${sql}: one scan admission`).toHaveLength(1);
    return { rows: oracleRows(result), minimum: minimums[0] ?? -1 };
  } finally {
    admits.mockRestore();
    prepare.mockRestore();
  }
}

it("sizes a plain scan of a retained resident source from its rows, not the cap", async () => {
  const types: PhysicalType[] = [{ kind: "int32" }, { kind: "text" }];
  const resident = indexReadTarget(types, CAP, ROWS);
  expect(resident).toBeLessThan(batchTargetFor(CAP, 4));
  // Unfiltered: the minimum is exactly the resident target plus the fixed selection lists.
  const plain = await scanMinimum("SELECT id, s FROM r");
  expect(plain.rows).toHaveLength(ROWS);
  expect(plain.minimum).toBe(resident + BATCH_ROWS * 16);
  // Filtered (not a sort): the same target, plus the filter kernel's own buffers.
  const filtered = await scanMinimum("SELECT id, s FROM r WHERE v > 3");
  expect(filtered.rows).toHaveLength(ROWS - 4);
  expect(filtered.minimum, "the filter kernel adds its own buffers").toBeGreaterThan(plain.minimum);
  // The cap-wide target alone would exceed this whole minimum.
  expect(filtered.minimum).toBeLessThan(batchTargetFor(CAP, 4));
});
