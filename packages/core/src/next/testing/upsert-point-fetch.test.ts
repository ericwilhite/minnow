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

/**
 * The statement's fetcher, captured at its first fetch. `each` runs before every fetch. No flush
 * publishes before the statement's first fetch (`table` holds them): the statement's snapshot sees
 * every conflicting row in a memtable. At that fetch the flushes are let go and the write path is
 * quiesced, so every memtable holding a conflicting row (all are frozen) is published under the
 * statement's older snapshot, which reads it re-pointed to its group (write/memtable.ts
 * "Lifecycle"). Every fetch then reads a published memtable through the memo.
 */
function captureFetcher(
  core: Table,
  each?: (fetcher: PointFetcher, call: number) => void,
): () => PointFetcher {
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
    const call = ++calls;
    const run = () => {
      each?.(this, call);
      return fetch.apply(this, args);
    };
    if (call !== 1) return run();
    core.publish();
    return core.harness.path.quiesce().then(run);
  });
  return () => {
    if (fetcher === undefined) throw new Error("the statement fetched no conflicting row");
    return fetcher;
  };
}

type Table = StorageCore & { readonly publish: () => void };

/**
 * `rows` conflicting rows (v = id) and one more, so every memtable holding them is frozen. Their
 * flushes publish only once `publish` is called (engine.ts `beforeFlushPublish`).
 */
async function table(rows: number): Promise<Table> {
  let publish: () => void = () => undefined;
  const held = new Promise<void>((resolve) => {
    publish = resolve;
  });
  const core = await new StorageCore(MEMTABLE_ROWS, { beforeFlushPublish: () => held }).open();
  try {
    await core.execute("CREATE TABLE s(id INTEGER PRIMARY KEY, v INTEGER)");
    await core.execute(`INSERT INTO s SELECT g, g FROM generate_series(1, ${String(rows + 1)}) g`);
  } catch (error) {
    // Let held flushes go, or close waits on them until the test times out.
    publish();
    await core.close();
    throw error;
  }
  return Object.assign(core, { publish });
}
/** The sum of v as inserted: ids 1 … rows + 1. */
const inserted = (rows: number) => ((rows + 1) * (rows + 2)) / 2;

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
      const fetcher = captureFetcher(core);
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
      expect(await sumOfV(core)).toBe(inserted(rows) + rows);
      expect(memoryFetches, "one fetch per conflicting row").toBe(rows);
      expect(declines).toBe(0);
      // Every row is read from a published memtable (`captureFetcher`): each read goes through
      // the memo, which opens once per memtable root and reuses it for every other read.
      const reads = memo.opens + memo.reuses;
      expect(reads, "each fetch reads its row's columns once each").toBe(READS_PER_FETCH * rows);
      expect(memo.opens, "the memo opens per published root, not per row").toBe(
        rows / MEMTABLE_ROWS,
      );
      expect(composites, "composite signals are per memo entry, not per read").toBeLessThanOrEqual(
        2 * memo.opens + 8,
      );
      expect(memo.closed, "the statement closed its memo").toBe(true);
      expect(memo.held, "the memo gives back everything at statement end").toBe(0);
      measured.push({ rows, reads, opens: memo.opens, composites });
    } finally {
      core.publish(); // a failure before the first fetch still holds the flushes
      await core.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  }
  const [n, twice] = measured;
  if (n === undefined || twice === undefined) throw new Error("two sizes");
  // Rows × 2 → reads × 2, and opens × 2.
  expect(twice.reads).toBe(2 * n.reads);
  expect(twice.opens).toBe(2 * n.opens);
}, 120_000);

it("releases the memo when a row's update fails mid-statement", async () => {
  const rows = 2 * MEMTABLE_ROWS,
    failing = MEMTABLE_ROWS + 7;
  const core = await table(rows);
  try {
    const fetcher = captureFetcher(core);
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
    expect(await sumOfV(core)).toBe(inserted(rows));
  } finally {
    core.publish(); // a failure before the first fetch still holds the flushes
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
    const fetcher = captureFetcher(core, (current, call) => {
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
    expect(await sumOfV(core)).toBe(inserted(rows));
  } finally {
    core.publish(); // a failure before the first fetch still holds the flushes
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
    const fetcher = captureFetcher(core, (current, call) => {
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
    expect(await sumOfV(core)).toBe(inserted(rows) + rows);
  } finally {
    core.publish(); // a failure before the first fetch still holds the flushes
    await core.close();
  }
  expect(core.harness.governor.usedBytes).toBe(0);
}, 60_000);
