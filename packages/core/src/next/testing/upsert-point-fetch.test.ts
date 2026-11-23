/**
 * ON CONFLICT DO UPDATE reads each conflicting row with one reusable point fetcher per statement
 * (exec/point-fetch.ts), whose reader memo (access/reader-memo.ts) keeps the published group root
 * and column readers of the memtable it last read.
 *
 * Law (exec/modify-upsert.ts, access/reader-memo.ts): the statement still arbitrates one proposal
 * at a time; a fetch's work does not grow with the statement's rows, so rows × 2 costs work × 2;
 * the memo's memory is owned by the statement, so it is fully released at statement end, on
 * abort and on error; a memo its budget refuses is declined and the statement reads without it.
 *
 * Work is counted, not timed: the fetcher's reads, the memo's opens and reuses, and the
 * `AbortSignal.any` composites the statement builds. A per-row reopen shows as opens growing with
 * rows; a per-read composite on the statement's long-lived signal (quadratic in Node, whose
 * cleanup walks every earlier dependent) shows as composites growing with rows.
 */
import { afterEach, expect, it, vi } from "vitest";
import { valueAt } from "../exec/kernels/test-support.js";
import { PointFetcher } from "../exec/point-fetch.js";
import { need } from "../write/bytes.js";
import { StorageCore } from "./storage-core.js";

const MEMTABLE_ROWS = 8192;
/** Reads per fetched row of `(id, v)`: one sizing hint and one chunk read per column. */
const READS_PER_FETCH = 4;

afterEach(() => {
  vi.restoreAllMocks();
});

/** The statement's fetcher, captured at its first fetch. `each` runs before every fetch. */
function captureFetcher(each?: (fetcher: PointFetcher, call: number) => void): () => PointFetcher {
  let fetcher: PointFetcher | undefined,
    calls = 0;
  // eslint-disable-next-line @typescript-eslint/unbound-method -- called with its receiver below
  const fetch = PointFetcher.prototype.fetch;
  vi.spyOn(PointFetcher.prototype, "fetch").mockImplementation(function (
    this: PointFetcher,
    ...args
  ) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias -- the spied method's receiver is the fetcher under test
    fetcher ??= this;
    each?.(this, ++calls);
    return fetch.apply(this, args);
  });
  return () => {
    if (fetcher === undefined) throw new Error("the statement fetched no conflicting row");
    return fetcher;
  };
}

async function table(rows: number): Promise<StorageCore> {
  const core = await new StorageCore(MEMTABLE_ROWS).open();
  await core.execute("CREATE TABLE s(id INTEGER PRIMARY KEY, v INTEGER)");
  await core.execute(`INSERT INTO s SELECT g, g FROM generate_series(1, ${String(rows)}) g`);
  return core;
}

async function sumOfV(core: StorageCore): Promise<number> {
  const result = await core.execute("SELECT sum(v) AS total FROM s");
  if (!("batches" in result)) throw new Error("sum rows");
  return Number(valueAt(need(need(result.batches[0]).columns[0]), 0));
}

const upsert = (rows: number, set = "s.v + 1") =>
  `INSERT INTO s SELECT g, 0 FROM generate_series(1, ${String(rows)}) g ON CONFLICT (id) DO UPDATE SET v = ${set}`;

it("does the same work per conflicting row at N and 2N rows", async () => {
  const measured = [];
  for (const rows of [2 * MEMTABLE_ROWS, 4 * MEMTABLE_ROWS]) {
    const core = await table(rows);
    try {
      const fetcher = captureFetcher();
      let composites = 0;
      const any = AbortSignal.any.bind(AbortSignal);
      vi.spyOn(AbortSignal, "any").mockImplementation((signals) => {
        composites++;
        return any(signals);
      });
      await core.execute(upsert(rows));
      vi.restoreAllMocks();
      const { memoryFetches, declines, memo } = fetcher();
      // Every row was arbitrated and updated once: v = id + 1.
      expect(await sumOfV(core)).toBe((rows * (rows + 1)) / 2 + rows);
      expect(memoryFetches, "one fetch per conflicting row").toBe(rows);
      expect(declines).toBe(0);
      const reads = memo.opens + memo.reuses;
      expect(reads, "no fetch reads more than its rows' columns").toBeLessThanOrEqual(
        READS_PER_FETCH * rows,
      );
      expect(memo.reuses, "fetches reuse the memo's readers").toBeGreaterThanOrEqual(
        (READS_PER_FETCH / 2) * rows,
      );
      // One entry per published memtable root (and at most one replaced on re-publication).
      expect(memo.opens, "the memo opens per published root, not per row").toBeLessThanOrEqual(
        rows / MEMTABLE_ROWS + 2,
      );
      expect(composites, "composite signals are per memo entry, not per read").toBeLessThanOrEqual(
        2 * memo.opens + 8,
      );
      expect(memo.closed, "the statement closed its memo").toBe(true);
      expect(memo.held, "the memo gives back everything at statement end").toBe(0);
      measured.push({ rows, reads, opens: memo.opens, composites });
    } finally {
      await core.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  }
  const [n, twice] = measured;
  if (n === undefined || twice === undefined) throw new Error("two sizes");
  // Rows × 2 → reads × 2, up to the opens that replace a published root.
  expect(Math.abs(twice.reads - 2 * n.reads)).toBeLessThanOrEqual(twice.opens + n.opens);
}, 120_000);

it("releases the memo when a row's update fails mid-statement", async () => {
  const rows = 2 * MEMTABLE_ROWS,
    failing = MEMTABLE_ROWS + 7;
  const core = await table(rows);
  try {
    const fetcher = captureFetcher();
    // Division by zero at one conflicting row, after the memo holds a published root.
    await expect(
      core.execute(upsert(rows, `s.v + 1 / (s.id - ${String(failing)})`)),
    ).rejects.toThrow();
    const { memo, memoryFetches } = fetcher();
    expect(memoryFetches).toBe(failing);
    expect(memo.opens, "the memo was in use when the statement failed").toBeGreaterThan(0);
    expect(memo.closed, "the statement closed its memo").toBe(true);
    expect(memo.held, "an error gives back everything the memo held").toBe(0);
    // The statement rolled back: every v is as inserted.
    vi.restoreAllMocks();
    expect(await sumOfV(core)).toBe((rows * (rows + 1)) / 2);
  } finally {
    await core.close();
  }
  expect(core.harness.governor.usedBytes).toBe(0);
}, 60_000);

it("releases the memo when the statement is aborted between fetches", async () => {
  const rows = 2 * MEMTABLE_ROWS,
    abortAt = MEMTABLE_ROWS + 11;
  const core = await table(rows);
  try {
    const controller = new AbortController();
    let heldAtAbort = 0;
    const fetcher = captureFetcher((current, call) => {
      if (call !== abortAt) return;
      heldAtAbort = current.memo.held;
      controller.abort(new Error("cancelled by the test"));
    });
    await expect(core.execute(upsert(rows), [], controller.signal)).rejects.toThrow();
    const { memo } = fetcher();
    expect(heldAtAbort, "the memo held a published root when aborted").toBeGreaterThan(0);
    expect(memo.closed, "the statement closed its memo").toBe(true);
    expect(memo.held, "an abort gives back everything the memo held").toBe(0);
    vi.restoreAllMocks();
    expect(await sumOfV(core)).toBe((rows * (rows + 1)) / 2);
  } finally {
    await core.close();
  }
  expect(core.harness.governor.usedBytes).toBe(0);
}, 60_000);

it("declines a memo its budget refuses and reads every row without it", async () => {
  const rows = 2 * MEMTABLE_ROWS;
  const core = await table(rows);
  try {
    let refused = 0;
    // Fault injection: from the first fetch on, the memo's budget refuses every byte.
    const fetcher = captureFetcher((current, call) => {
      if (call !== 1) return;
      Object.defineProperty(current.memo, "budget", {
        value: {
          allocate: () => {
            refused++;
            return false;
          },
          free: () => undefined,
        },
      });
    });
    await core.execute(upsert(rows));
    const { memo, declines, memoryFetches } = fetcher();
    expect(refused, "the memo asked its budget").toBeGreaterThan(0);
    expect(declines, "one decline closes the memo for the statement").toBe(1);
    expect(memo.closed, "the statement closed its memo").toBe(true);
    expect(memo.opens).toBe(1);
    expect(memoryFetches).toBe(rows);
    vi.restoreAllMocks();
    expect(await sumOfV(core)).toBe((rows * (rows + 1)) / 2 + rows);
  } finally {
    await core.close();
  }
  expect(core.harness.governor.usedBytes).toBe(0);
}, 60_000);
