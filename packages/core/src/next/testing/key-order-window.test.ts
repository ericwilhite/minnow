/**
 * Primary-key ORDER BY with LIMIT/OFFSET: PostgreSQL owns the rows, and the read bound is
 * sized only over the groups the window names. A window of at most one cursor batch reads
 * its locators first (exec/key-order.ts windowSegments); every other group stays unread.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { createReach, KEY_ORDER_WINDOW_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { WINDOW_SIZING_LABEL, windowSegments } from "../exec/key-order.js";
import { packLocator } from "../vector/locator.js";
import { MetadataReader } from "../format/metadata-reader.js";
import { Governor } from "../memory/governor.js";
import { ViewIndex } from "../write/view.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

/** 16 groups of 256 consecutive ids, written in shuffled group order with merges off. */
const GROUP_ROWS = 256,
  GROUPS = 16,
  ROWS = GROUP_ROWS * GROUPS;
let core: StorageCore;
let pg: PGlite;

beforeAll(async () => {
  core = await new StorageCore(GROUP_ROWS, { merges: { mode: "off" } }).open();
  pg = new PGlite();
  await pg.waitReady;
  const ddl = "CREATE TABLE t (id BIGINT PRIMARY KEY, k INTEGER, v DOUBLE PRECISION, s TEXT)";
  await core.execute(ddl);
  await pg.exec(ddl);
  const random = mulberry32(0x5d0);
  const order = Array.from({ length: GROUPS }, (_, g) => g);
  for (let i = order.length - 1; i > 0; i--) {
    const j = Math.floor(random() * (i + 1));
    [order[i], order[j]] = [order[j] ?? 0, order[i] ?? 0];
  }
  for (const group of order) {
    const rows = Array.from({ length: GROUP_ROWS }, (_, r) => {
      const id = group * GROUP_ROWS + r + 1;
      return { id, k: id % 7, v: id % 11 === 0 ? null : id / 4, s: `s${String(id % 13)}` };
    });
    await core.writeBatch("insertBatch", "t", rows);
    await core.harness.path.quiesce();
    await pg.exec(
      `INSERT INTO t VALUES ${rows
        .map(
          (r) => `(${String(r.id)},${String(r.k)},${r.v === null ? "NULL" : String(r.v)},'${r.s}')`,
        )
        .join(",")}`,
    );
  }
});
beforeAll(async () => {
  // A resident one-chunk table (100 rows stay in the memtable): its LIMIT 1 uses the
  // single-use resident range.
  const ddl = "CREATE TABLE r (id BIGINT PRIMARY KEY, v DOUBLE PRECISION, s TEXT)";
  await core.execute(ddl);
  await pg.exec(ddl);
  const rows = Array.from({ length: 100 }, (_, i) => ({ id: i + 1, v: i / 2, s: `r${String(i)}` }));
  await core.writeBatch("insertBatch", "r", rows);
  await pg.exec(
    `INSERT INTO r VALUES ${rows.map((r) => `(${String(r.id)},${String(r.v)},'${r.s}')`).join(",")}`,
  );
});
afterAll(async () => {
  await core.close();
  await pg.close();
});

const canonical = (rows: ReadonlyArray<Record<string, unknown>>) =>
  rows.map((r) => [Number(r.id), r.v === null ? null : Number(r.v), r.s]);

/**
 * Runs `sql` on both engines; returns the group roots the next/ query read, and whether each
 * root was read by more than one reader (sizing and fetch use different readers: a group the
 * fetch reads that sizing never saw would be read once).
 */
async function run(
  sql: string,
): Promise<{ roots: number; scanRows: number; sizedWhatItRead: boolean }> {
  const expected = await pg.query<Record<string, unknown>>(sql);
  await core.execute(sql); // warm the shared metadata caches: count only this query's reads
  const readers = new Map<string, Set<MetadataReader>>();
  // eslint-disable-next-line @typescript-eslint/unbound-method -- captured once; wrapper preserves receiver
  const groupRoot = MetadataReader.prototype.groupRoot;
  const roots = vi.spyOn(MetadataReader.prototype, "groupRoot").mockImplementation(function (
    this: MetadataReader,
    ref,
    budget,
  ) {
    const key = JSON.stringify(ref);
    readers.set(key, (readers.get(key) ?? new Set()).add(this));
    return groupRoot.call(this, ref, budget);
  });
  try {
    const result = await core.execute(sql);
    if (result.kind !== "rows" || !("batches" in result)) throw new Error(`No rows: ${sql}`);
    expect(canonical(oracleRows(result)), sql).toEqual(canonical(expected.rows));
    const scan = core.lastProfile?.operators.find((op) => op.op === "scan");
    return {
      roots: roots.mock.calls.length,
      scanRows: scan?.rowsOut ?? -1,
      sizedWhatItRead: [...readers.values()].every((set) => set.size >= 2),
    };
  } finally {
    roots.mockRestore();
  }
}

/** Groups holding the window's rows: each group is a run of 256 consecutive ids. */
function groupsOf(offset: number, limit: number, descending: boolean): number {
  const groups = new Set<number>();
  for (let k = offset; k < Math.min(ROWS, offset + limit); k++) {
    const id = descending ? ROWS - k : k + 1;
    groups.add(Math.floor((id - 1) / GROUP_ROWS));
  }
  return groups.size;
}

it("reads only the group a deep-offset window names (pinned W1M shape)", async () => {
  const { roots, scanRows, sizedWhatItRead } = await run(
    "SELECT id, v, s FROM t ORDER BY id LIMIT 100 OFFSET 86",
  );
  expect(scanRows, "the ordered key path ran").toBe(100);
  // Sizing reads the window's one group root and the fetch reads it again: never the other 15.
  expect(roots).toBeLessThanOrEqual(2 * groupsOf(86, 100, false));
  expect(sizedWhatItRead, "every fetched group was sized").toBe(true);
});

for (const seed of seedsFor("next-key-order-window", [0x5d01, 0x5d02]))
  it(`sizes only the groups each window fetches, with PostgreSQL's rows (seed ${String(seed)})`, async () => {
    const reach = createReach("key order window", KEY_ORDER_WINDOW_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(1, 5, 100, 300, 2048, 2049),
        fc.constantFrom(0, 86, 255, 256, 3000, 4095, 4096, 10000),
        fc.boolean(),
        async (limit, offset, descending) => {
          reach.beginHistory();
          const sql = `SELECT id, v, s FROM t ORDER BY id ${descending ? "DESC" : "ASC"} LIMIT ${String(limit)} OFFSET ${String(offset)}`;
          const { roots, scanRows, sizedWhatItRead } = await run(sql);
          const fetched = groupsOf(offset, limit, descending);
          const window = Math.max(0, Math.min(limit, ROWS - offset));
          if (limit <= 2048 && limit > 1) {
            expect(scanRows, "the ordered key path ran").toBe(window);
            // Groups touched and sized: at most twice the groups fetched (sizing, then fetch).
            expect(roots, sql).toBeLessThanOrEqual(2 * fetched);
            expect(sizedWhatItRead, `${sql}: every fetched group was sized`).toBe(true);
            reach.reach("work bound");
            reach.reach(
              fetched === 0 ? "offset past end" : fetched === 1 ? "one group" : "spans groups",
            );
          } else reach.reach(limit === 1 ? "one-row limit" : "over one batch");
          if (descending) reach.reach("descending");
          if (offset > 0 && window > 0) reach.reach("deep offset");
          reach.reach("PG exact");
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          [100, 86, false],
          [100, 4095, true],
          [5, 10000, false],
          [300, 3000, false],
          [2049, 0, true],
          [1, 256, false],
        ],
      },
    );
    reach.assertReached();
  }, 120_000);

it("keeps PostgreSQL's order and rows for ties and DESC on other keys", async () => {
  // Not the primary key: the generic recipe, with ties broken by id for a total order.
  for (const sql of [
    "SELECT id, v, s FROM t ORDER BY k, id LIMIT 50 OFFSET 600",
    "SELECT id, v, s FROM t ORDER BY k DESC, id DESC LIMIT 50 OFFSET 4090",
    "SELECT id, v, s FROM t ORDER BY v NULLS FIRST, id LIMIT 20 OFFSET 0",
  ])
    await run(sql);
});

it("sizes a window by its own cursor exactly when it fits one cursor batch", async () => {
  // Lead design decision under the governor law: size before admission, release, then admit.
  const counts = { plans: 0, resident: 0, sizing: 0, execution: 0, closes: 0 };
  // eslint-disable-next-line @typescript-eslint/unbound-method -- captured once; wrappers preserve receivers
  const planRange = ViewIndex.prototype.planRange;
  // eslint-disable-next-line @typescript-eslint/unbound-method -- captured once; wrappers preserve receivers
  const prepareResident = ViewIndex.prototype.prepareResidentRange;
  const plans = vi.spyOn(ViewIndex.prototype, "planRange").mockImplementation(function (
    this: ViewIndex,
    range,
    direction,
  ) {
    counts.plans++;
    const plan = planRange.call(this, range, direction);
    return {
      minimumBytes: plan.minimumBytes,
      open(options) {
        if (options.budget.label === WINDOW_SIZING_LABEL) counts.sizing++;
        else counts.execution++;
        const cursor = plan.open(options);
        return {
          next: (out) => cursor.next(out),
          skip: (count) => cursor.skip(count),
          close() {
            counts.closes++;
            cursor.close();
          },
        };
      },
    };
  });
  const resident = vi
    .spyOn(ViewIndex.prototype, "prepareResidentRange")
    .mockImplementation(function (this: ViewIndex, range, direction) {
      counts.resident++;
      return prepareResident.call(this, range, direction);
    });
  const nativePrepare = core.executor.prepare.bind(core.executor);
  let governor: Governor | null = null;
  const prepare = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
    const prepared = nativePrepare(plan);
    return {
      ...prepared,
      execute: (request) => prepared.execute({ ...request, ...(governor ? { governor } : {}) }),
    };
  });
  const check = async (sql: string, expected: typeof counts) => {
    Object.assign(counts, { plans: 0, resident: 0, sizing: 0, execution: 0, closes: 0 });
    const want = await pg.query<Record<string, unknown>>(sql);
    const result = await core.execute(sql);
    if (result.kind !== "rows" || !("batches" in result)) throw new Error(`No rows: ${sql}`);
    expect(canonical(oracleRows(result)), sql).toEqual(canonical(want.rows));
    expect(counts, sql).toEqual(expected);
  };
  try {
    // Sized: a window of one cursor batch (2 plans: sizing, then execution; both closed).
    const two = { plans: 2, resident: 0, sizing: 1, execution: 1, closes: 2 };
    const one = { plans: 1, resident: 0, sizing: 0, execution: 1, closes: 1 };
    await check("SELECT id, v, s FROM t ORDER BY id LIMIT 100 OFFSET 86", two);
    await check("SELECT id, v, s FROM t ORDER BY id DESC LIMIT 2048 OFFSET 5", two);
    // LIMIT 1 asks for a resident range first; a stored table has none, so it is sized.
    await check("SELECT id, v, s FROM t ORDER BY id LIMIT 1 OFFSET 300", { ...two, resident: 1 });
    // Not sized: more than one cursor batch, and no LIMIT at all.
    await check("SELECT id, v, s FROM t ORDER BY id LIMIT 2049 OFFSET 0", one);
    await check("SELECT id, v, s FROM t ORDER BY id OFFSET 4000", one);
    // Not sized: LIMIT 1 over a resident one-chunk table keeps its single-use resident range.
    await check("SELECT id, v, s FROM r ORDER BY id LIMIT 1 OFFSET 7", {
      plans: 0,
      resident: 1,
      sizing: 0,
      execution: 0,
      closes: 0,
    });
    // A cap whose cursor path does not fit: the range is planned, then declined, and no cursor
    // (sizing or execution) opens; the generic sort answers. The sizing scratch (range minimum
    // plus 8 bytes per locator) is below the cursor path's own minimum, so it never exceeds a
    // cap the cursor path fits.
    governor = new Governor(6 << 20);
    await check("SELECT id, v, s FROM t ORDER BY id LIMIT 100 OFFSET 86", {
      plans: 1,
      resident: 0,
      sizing: 0,
      execution: 0,
      closes: 0,
    });
  } finally {
    governor = null;
    prepare.mockRestore();
    plans.mockRestore();
    resident.mockRestore();
  }
});

it("reads the pinned snapshot when a writer commits between sizing and execution", async () => {
  const ddl = "CREATE TABLE w (id BIGINT PRIMARY KEY, v DOUBLE PRECISION, s TEXT)";
  await core.execute(ddl);
  await pg.exec(ddl);
  // More rows than one chunk, so the ordered key path (not the small-input sort) runs.
  for (let g = 0; g < 9; g++) {
    const rows = Array.from({ length: GROUP_ROWS }, (_, r) => {
      const id = 1000 + g * GROUP_ROWS + r;
      return { id, v: r / 2, s: `w${String(id % 5)}` };
    });
    await core.writeBatch("insertBatch", "w", rows);
    await core.harness.path.quiesce();
    await pg.exec(
      `INSERT INTO w VALUES ${rows.map((r) => `(${String(r.id)},${String(r.v)},'${r.s}')`).join(",")}`,
    );
  }
  const sql = "SELECT id, v, s FROM w ORDER BY id LIMIT 100 OFFSET 40";
  const before = await pg.query<Record<string, unknown>>(sql);
  // New rows sort first: under a newer snapshot they would shift the whole window.
  const inserted = Array.from({ length: GROUP_ROWS }, (_, r) => ({ id: r + 1, v: -r, s: "new" }));
  let committed = false;
  // eslint-disable-next-line @typescript-eslint/unbound-method -- captured once; wrappers preserve receivers
  const planRange = ViewIndex.prototype.planRange;
  const plans = vi.spyOn(ViewIndex.prototype, "planRange").mockImplementation(function (
    this: ViewIndex,
    range,
    direction,
  ) {
    const plan = planRange.call(this, range, direction);
    return {
      minimumBytes: plan.minimumBytes,
      open(options) {
        const cursor = plan.open(options);
        if (options.budget.label !== WINDOW_SIZING_LABEL) return cursor;
        return {
          skip: (count) => cursor.skip(count),
          async next(out) {
            const n = await cursor.next(out);
            if (!committed) {
              // A concurrent writer commits a new group between the window read and execution.
              committed = true;
              await core.writeBatch("insertBatch", "w", inserted);
              await core.harness.path.quiesce();
            }
            return n;
          },
          close: () => cursor.close(),
        };
      },
    };
  });
  const readers = new Map<string, Set<MetadataReader>>();
  // eslint-disable-next-line @typescript-eslint/unbound-method -- captured once; wrapper preserves receiver
  const groupRoot = MetadataReader.prototype.groupRoot;
  const roots = vi.spyOn(MetadataReader.prototype, "groupRoot").mockImplementation(function (
    this: MetadataReader,
    ref,
    budget,
  ) {
    const key = JSON.stringify(ref);
    readers.set(key, (readers.get(key) ?? new Set()).add(this));
    return groupRoot.call(this, ref, budget);
  });
  try {
    const result = await core.execute(sql);
    if (result.kind !== "rows" || !("batches" in result)) throw new Error("rows");
    expect(committed, "the writer committed during the query").toBe(true);
    expect(canonical(oracleRows(result)), "rows of the query's pinned snapshot").toEqual(
      canonical(before.rows),
    );
    expect(
      [...readers.values()].every((set) => set.size >= 2),
      "every fetched group was sized",
    ).toBe(true);
  } finally {
    roots.mockRestore();
    plans.mockRestore();
  }
  await pg.exec(
    `INSERT INTO w VALUES ${inserted.map((r) => `(${String(r.id)},${String(r.v)},'${r.s}')`).join(",")}`,
  );
  // A later query sees the commit, still exactly PostgreSQL's rows.
  await run(sql);
});

it("charges the window's segment list until sizing releases it (review repro)", async () => {
  // 2048 locators in 2048 distinct segments: the list is as large as its bound.
  const governor = new Governor(16 << 20);
  const rows = 2048;
  let served = 0;
  const plan = {
    minimumBytes: 4096,
    open: () => ({
      skip: (count: number) => count,
      next(out: Float64Array) {
        const n = Math.min(out.length, rows - served);
        for (let k = 0; k < n; k++) out[k] = packLocator(rows - 1 - (served + k), 0);
        served += n;
        return n;
      },
      close() {
        // Nothing pending.
      },
    }),
  };
  const window = await windowSegments(plan, 0, rows, governor, new AbortController().signal);
  expect(window.segments.length).toBe(rows);
  expect(Array.from(window.segments.subarray(0, 3))).toEqual([0, 1, 2]);
  // The list is charged (4 bytes a locator) while sizing may still read it; the scratch is not.
  expect(governor.usedBytes).toBeGreaterThanOrEqual(rows * Uint32Array.BYTES_PER_ELEMENT);
  expect(governor.usedBytes).toBeLessThan(rows * Float64Array.BYTES_PER_ELEMENT);
  window.release();
  window.release();
  expect(governor.usedBytes).toBe(0);
});
