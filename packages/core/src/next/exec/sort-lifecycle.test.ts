/**
 * The sort operators' lifecycle under ruling R23 (review B7-p2b-1): operator-owned scratch
 * (two operators interleaved on one worker), finish work in task-yielding slices (a zero-delay
 * timer runs before finish completes), the abort signal checked before finish, at every slice and
 * after backpressure, and compare mode's decoded operands charged to the grant. Also the review's
 * resource probes: an oversized row refused cleanly, and a wide spill that stays bounded.
 */
import { afterAll, describe, expect, it, vi } from "vitest";
import { createReach } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import type { Batch } from "../vector/vector.js";
import {
  ArraySource,
  CollectSink,
  ResolvedBlockSink,
  INT,
  TEXT,
  batchesOf,
  rowsOf,
  type Row,
} from "./join-fixtures.js";
import {
  TaskClock,
  YIELD_INTERVAL_MS,
  beginTask,
  drive,
  taskClock,
  type BatchSource,
  type Operator,
  type PushResult,
} from "./pipeline.js";
import {
  PREFIX_WORDS,
  RadixScratch,
  TUPLE_WORDS,
  mergeSortJob,
  msdSort,
  msdStackWords,
  type SortColumn,
} from "./sort-keys.js";
import * as sortKeys from "./sort-keys.js";
import { Slice } from "./slices.js";

const original = sortKeys.decodeOperands;
import { GroupTopKOperator, SortOperator, sortMinimumBytes } from "./sort.js";

const JSONB: PhysicalType = { kind: "json", binary: true };
/** Inputs a 1 MiB pool would merge at once if only 8 KiB pages counted (2 × 8 KiB each). */
const MAX_INPUTS_AT_PAGE_BYTES = 64;
const asc = (column: number): SortColumn => ({
  column,
  direction: "asc",
  nulls: "last",
  collation: null,
});

/** A governor holding the sort's minimum plus `slack` (spills early), or a roomy one. */
async function context(types: readonly PhysicalType[], slack?: number, signal?: AbortSignal) {
  const minimum = sortMinimumBytes(types);
  const cap =
    slack === undefined
      ? 512 << 20
      : Math.max(minimum + slack + (1 << 20), Math.ceil(((minimum + slack) * 8) / 7));
  const governor = new Governor(cap);
  const spill = new MemorySpillService();
  const grant = await governor.admit({ label: "sort", minimumBytes: minimum, spillable: true });
  return { governor, spill, context: { grant, spill, queryId: 5, ...(signal ? { signal } : {}) } };
}

async function pushAll(operator: Operator, types: readonly PhysicalType[], rows: readonly Row[]) {
  for (const batch of batchesOf(types, rows))
    if (operator.push(batch) === "blocked") await operator.ready();
}

/**
 * Pushes batch i to `a`, then batch i to `b` while `a` may still be suspended in that push (a
 * spill or compaction it awaits), then waits for both: scratch shared between instances is
 * overwritten before `a` resumes, every time.
 */
async function lockstep(a: Operator, aBatches: Batch[], b: Operator, bBatches: Batch[]) {
  let blocked = 0;
  for (let i = 0; i < Math.max(aBatches.length, bBatches.length); i++) {
    const x = aBatches[i];
    const y = bBatches[i];
    const ra = x === undefined ? "ready" : a.push(x);
    const rb = y === undefined ? "ready" : b.push(y);
    if (ra === "blocked") {
      blocked++;
      await a.ready();
    }
    if (rb === "blocked") await b.ready();
  }
  await Promise.all([a.finish(), b.finish()]);
  return blocked;
}

describe("R23.1: two interleaved operators keep their own scratch", () => {
  it("spilled group top-k sorts keep private partition lengths (review probe)", async () => {
    const types = [TEXT, INT];
    const make = async (label: string) => {
      const { context: ctx, governor, spill } = await context(types, 0);
      const sink = new CollectSink();
      const op = new GroupTopKOperator(
        { types, partitionBy: [0], orderBy: [asc(1)], k: 1 },
        ctx,
        sink,
      );
      const rows: Row[] = Array.from({ length: 30_000 }, (_, i) => [label, 30_000 - i]);
      return { op, sink, governor, spill, label, batches: [...batchesOf(types, rows)] };
    };
    const a = await make("a");
    const b = await make("a much longer partition");
    expect(await lockstep(a.op, a.batches, b.op, b.batches)).toBeGreaterThan(0);
    for (const x of [a, b]) {
      await x.op.closed;
      expect(x.op.profile.runs).toBeGreaterThan(0);
      expect(x.sink.rows).toEqual([[x.label, 1, 1n]]);
      expect(x.governor.usedBytes).toBe(0);
      expect(x.spill.bytesInUse).toBe(0);
    }
  }, 60_000);

  it("top-N sorts keep private threshold survivors", async () => {
    // Top-N lists the rows that beat its threshold in scratch kept across a spill's await. The
    // grant admits the early compactions (which set the threshold); later rows are wide, so the
    // store fills and spills mid-batch with the list live. A's rows all beat the threshold
    // (descending keys); B keeps every other row, so its list differs from A's.
    const types = [INT, TEXT];
    const limit = 1000;
    const make = async (key: (i: number) => number) => {
      const { context: ctx, governor } = await context(types, 8 << 20);
      const sink = new CollectSink();
      const op = new SortOperator(
        // A text key besides the (unique) int: byte keys (wide ones) through the survivor list.
        { types, keys: [asc(0), asc(1)], limit, offset: 0, withTies: false },
        ctx,
        sink,
      );
      const rows: Row[] = Array.from({ length: 40_000 }, (_, i) => [
        key(i),
        `p${String(i)}-${"z".repeat(i < 10_000 ? 300 : 3000)}`,
      ]);
      const expected = rows
        .map((row) => row[0] as number)
        .sort((x, y) => x - y)
        .slice(0, limit);
      return {
        op,
        sink,
        governor,
        expected,
        batches: [...batchesOf(types, rows, { batchRows: 700 })],
      };
    };
    const a = await make((i) => 40_000 - i);
    const b = await make((i) => (i % 2 === 0 ? 40_000 - i : 10_000_000 + i));
    const blocked = await lockstep(a.op, a.batches, b.op, b.batches);
    expect(blocked).toBeGreaterThan(0);
    expect(b.op.profile.rejected).toBeGreaterThan(0);
    for (const x of [a, b]) {
      expect(x.op.profile.compactions).toBeGreaterThan(0);
      expect(x.op.profile.runs).toBeGreaterThan(0);
      await x.op.closed;
      expect(x.sink.rows.map((row) => row[0])).toEqual(x.expected);
      expect(x.governor.usedBytes).toBe(0);
    }
    // 25-34 s on a loaded host (load ~25): 40,000 rows of 3 KB spill through an 8 MiB grant.
  }, 180_000);
});

describe("R23.1: operator-owned scratch is charged", () => {
  it("a sorter charges its batch scratch and radix histograms when it is built", async () => {
    // The spill pool (1 MiB), the batch-row scratch (10 bytes a row) and the two radix
    // histograms are held from construction; the rest of the minimum is the encoders' and pages'.
    const { context: ctx, governor } = await context([INT]);
    const before = ctx.grant.allocated;
    const op = new SortOperator(
      { types: [INT], keys: [asc(0)], limit: null, offset: 0, withTies: false },
      ctx,
      new CollectSink(),
    );
    expect(ctx.grant.allocated - before).toBeGreaterThanOrEqual(
      (1 << 20) + 2048 * 10 + RadixScratch.BYTES,
    );
    op.abort(new Error("done"));
    await op.closed;
    expect(governor.usedBytes).toBe(0);
  });
});

describe("R23.2/R23.3: finish runs in task-yielding slices", () => {
  const cases: Array<{
    name: string;
    types: PhysicalType[];
    make: (ctx: Awaited<ReturnType<typeof context>>["context"], sink: Operator) => Operator;
    rows: () => Row[];
    slack?: number;
  }> = [
    {
      name: "resident sort (review probe)",
      types: [INT],
      make: (ctx, sink) =>
        new SortOperator(
          { types: [INT], keys: [asc(0)], limit: null, offset: 0, withTies: false },
          ctx,
          sink,
        ),
      rows: () => Array.from({ length: 200_000 }, (_, i) => [200_000 - i]),
    },
    {
      name: "resident top-N",
      types: [INT],
      make: (ctx, sink) =>
        new SortOperator(
          { types: [INT], keys: [asc(0)], limit: 150_000, offset: 0, withTies: true },
          ctx,
          sink,
        ),
      rows: () => Array.from({ length: 200_000 }, (_, i) => [(i * 7919) % 200_000]),
    },
    {
      name: "spilled sort (merge)",
      types: [INT, TEXT],
      make: (ctx, sink) =>
        new SortOperator(
          { types: [INT, TEXT], keys: [asc(0)], limit: null, offset: 0, withTies: false },
          ctx,
          sink,
        ),
      rows: () => Array.from({ length: 60_000 }, (_, i) => [(i * 7919) % 60_000, "x".repeat(40)]),
      slack: 0,
    },
    {
      // OFFSET skips nearly every merged row: only the merge's own slices can yield.
      name: "spilled sort with a large OFFSET (merge only)",
      types: [INT, TEXT],
      make: (ctx, sink) =>
        new SortOperator(
          { types: [INT, TEXT], keys: [asc(0)], limit: null, offset: 199_990, withTies: false },
          ctx,
          sink,
        ),
      rows: () => Array.from({ length: 200_000 }, (_, i) => [(i * 7919) % 200_000, "x".repeat(20)]),
      slack: 4 << 20,
    },
    {
      name: "resident group top-k",
      types: [INT, INT],
      make: (ctx, sink) =>
        new GroupTopKOperator(
          { types: [INT, INT], partitionBy: [0], orderBy: [asc(1)], k: 2 },
          ctx,
          sink,
        ),
      rows: () => Array.from({ length: 200_000 }, (_, i) => [i % 5000, 200_000 - i]),
    },
    {
      name: "compare-mode sort (JSONB)",
      types: [JSONB],
      make: (ctx, sink) =>
        new SortOperator(
          { types: [JSONB], keys: [asc(0)], limit: null, offset: 0, withTies: false },
          ctx,
          sink,
        ),
      rows: () =>
        Array.from({ length: 30_000 }, (_, i) => [JSON.stringify({ k: (i * 7919) % 30_000 })]),
    },
  ];
  for (const c of cases)
    it(`a zero-delay timer runs before finish completes: ${c.name}, downstream blocked with a resolved ready (R23.4)`, async () => {
      const { context: ctx } = await context(c.types, c.slack);
      const sink = new ResolvedBlockSink();
      const op = c.make(ctx, sink);
      await pushAll(op, c.types, c.rows());
      let serviced = false;
      const timer = setTimeout(() => {
        serviced = true;
      }, 0);
      await op.finish();
      clearTimeout(timer);
      expect(serviced).toBe(true);
      expect(sink.finished).toBe(true);
    }, 60_000);
  for (const c of cases)
    it(`a zero-delay timer runs before finish completes: ${c.name}`, async () => {
      const { context: ctx } = await context(c.types, c.slack);
      const sink = new CollectSink();
      const op = c.make(ctx, sink);
      const rows = c.rows();
      await pushAll(op, c.types, rows);
      let serviced = false;
      const timer = setTimeout(() => {
        serviced = true;
      }, 0);
      await op.finish();
      clearTimeout(timer);
      expect(serviced).toBe(true);
      expect(sink.finished).toBe(true);
    }, 60_000);
});

describe("R23.2: the merge itself yields", () => {
  it("a timer set at the merge's first page read runs before finish completes", async () => {
    // OFFSET skips nearly every merged row, so nothing downstream yields; the runs exist before
    // finish, so the first read starts the merge (its passes and the final one).
    const types = [INT, TEXT];
    const { context: ctx } = await context(types, 4 << 20);
    // Set from the read hook: a holder, since the closure's writes are invisible to narrowing.
    const state: { timer: ReturnType<typeof setTimeout> | null; serviced: boolean } = {
      timer: null,
      serviced: false,
    };
    const service = ctx.spill;
    const create = service.create.bind(service);
    service.create = async (queryId: number) => {
      const file = await create(queryId);
      const read = file.read.bind(file);
      file.read = (ref) => {
        state.timer ??= setTimeout(() => {
          state.serviced = true;
        }, 0);
        return read(ref);
      };
      return file;
    };
    const sink = new CollectSink();
    const op = new SortOperator(
      { types, keys: [asc(0)], limit: null, offset: 199_990, withTies: false },
      ctx,
      sink,
    );
    await pushAll(
      op,
      types,
      Array.from({ length: 200_000 }, (_, i) => [(i * 7919) % 200_000, "x".repeat(20)]),
    );
    expect(op.profile.runs).toBeGreaterThan(1);
    await op.finish();
    if (state.timer !== null) clearTimeout(state.timer);
    expect(state.timer).not.toBeNull();
    expect(state.serviced).toBe(true);
    expect(sink.rows.map((row) => row[0])).toEqual([...Array(10).keys()].map((i) => 199_990 + i));
  }, 60_000);
});

describe("R23.2/R23.3: the abort signal", () => {
  for (const spilled of [false, true])
    for (const group of [false, true])
      it(`an abort before finish rejects (${group ? "group top-k" : "sort"}, ${spilled ? "spilled" : "resident"})`, async () => {
        const controller = new AbortController();
        const types = [INT, TEXT];
        const {
          context: ctx,
          governor,
          spill,
        } = await context(types, spilled ? 0 : undefined, controller.signal);
        const sink = new CollectSink();
        const op = group
          ? new GroupTopKOperator({ types, partitionBy: [0], orderBy: [asc(1)], k: 1 }, ctx, sink)
          : new SortOperator(
              { types, keys: [asc(0)], limit: 1, offset: 0, withTies: true },
              ctx,
              sink,
            );
        const rows: Row[] = Array.from({ length: spilled ? 30_000 : 3 }, (_, i) => [
          i % 3,
          "y".repeat(spilled ? 60 : 1),
        ]);
        await pushAll(op, types, rows);
        expect(op.profile.runs > 0).toBe(spilled);
        controller.abort(new Error("cancel requested"));
        await expect(Promise.resolve().then(() => op.finish())).rejects.toThrow("cancel requested");
        expect(sink.rows).toHaveLength(0);
        await op.closed;
        expect(governor.usedBytes).toBe(0);
        expect(spill.bytesInUse).toBe(0);
      }, 60_000);

  it("a resumption after downstream backpressure checks the signal", async () => {
    // A few batches, so the emission fits one slice: only the check after `ready()` sees it.
    const controller = new AbortController();
    let pushes = 0;
    const sink: Operator & { rows: Row[] } = {
      rows: [],
      push(batch: Batch): PushResult {
        pushes++;
        this.rows.push(...rowsOf(batch));
        controller.abort(new Error("cancelled while blocked"));
        return "blocked";
      },
      ready: () => new Promise((resolve) => setTimeout(resolve, 0)),
      finish: () => undefined,
      abort: () => undefined,
    };
    const types = [INT];
    const { context: ctx, governor } = await context(types, undefined, controller.signal);
    const op = new SortOperator(
      { types, keys: [asc(0)], limit: null, offset: 0, withTies: false },
      ctx,
      sink,
    );
    await pushAll(
      op,
      types,
      Array.from({ length: 8000 }, (_, i) => [8000 - i]),
    );
    await expect(Promise.resolve().then(() => op.finish())).rejects.toThrow(
      "cancelled while blocked",
    );
    expect(pushes).toBe(1);
    await op.closed;
    expect(governor.usedBytes).toBe(0);
  });
});

describe("compare mode's decoded operands are charged", () => {
  const types = [INT, JSONB];
  const rows: Row[] = Array.from({ length: 3000 }, (_, i) => [
    i,
    JSON.stringify({ k: (i * 7919) % 3000, pad: "q".repeat(200 + (i % 300)) }),
  ]);
  const keys = [asc(1), asc(0)];
  const expected = [...rows]
    .sort((a, b) => {
      const x = (JSON.parse(a[1] as string) as { k: number }).k;
      const y = (JSON.parse(b[1] as string) as { k: number }).k;
      return x - y;
    })
    .map((row) => row[0]);

  it("the governor's peak includes the operands held for a resident sort", async () => {
    const { context: ctx, governor } = await context(types);
    const sink = new CollectSink();
    const op = new SortOperator(
      { types, keys, limit: null, offset: 0, withTies: false },
      ctx,
      sink,
    );
    await drive(new ArraySource(batchesOf(types, rows)), op);
    const profile = op.profile;
    expect(profile.runs).toBe(0);
    expect(profile.operandBytes).toBeGreaterThan(0);
    // The rows themselves are ~1.1 MB; the operands are charged on top of them.
    expect(governor.peakBytes).toBeGreaterThanOrEqual(profile.operandBytes + 1_000_000);
    expect(sink.rows.map((row) => row[0])).toEqual(expected);
    expect(governor.usedBytes).toBe(0);
  });

  it("rows whose operands do not fit spill, and merge in order", async () => {
    // A grant that holds the rows but not their decoded trees: the operands force the runs.
    const { context: ctx, governor, spill } = await context(types, 2 << 20);
    const sink = new CollectSink(3);
    const op = new SortOperator(
      { types, keys, limit: null, offset: 0, withTies: false },
      ctx,
      sink,
    );
    await drive(new ArraySource(batchesOf(types, rows, { batchRows: 97 })), op);
    await op.closed;
    expect(op.profile.runs).toBeGreaterThan(1);
    expect(sink.rows.map((row) => row[0])).toEqual(expected);
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  }, 60_000);
});

describe("compare mode's merge holds every input's operands within the pool", () => {
  it("a tight grant: many runs merge in passes whose fan-in counts page operands", async () => {
    // Rows of a few hundred bytes decode to trees of ~24 bytes per byte: a fan-in counted by
    // page bytes alone would hold far more than the pool and the grant have.
    const types = [INT, JSONB];
    const rows: Row[] = Array.from({ length: 3000 }, (_, i) => [
      i,
      JSON.stringify({ k: (i * 7919) % 3000, pad: "w".repeat(i % 900) }),
    ]);
    const { context: ctx, governor, spill } = await context(types, 0);
    const sink = new CollectSink(2);
    const op = new SortOperator(
      { types, keys: [asc(1), asc(0)], limit: null, offset: 0, withTies: false },
      ctx,
      sink,
    );
    await drive(new ArraySource(batchesOf(types, rows, { batchRows: 64 })), op);
    await op.closed;
    expect(op.profile.runs).toBeGreaterThan(MAX_INPUTS_AT_PAGE_BYTES);
    expect(op.profile.mergePasses).toBeGreaterThan(1);
    const expected = [...rows]
      .sort((x, y) => {
        const p = (JSON.parse(x[1] as string) as { k: number }).k;
        const q = (JSON.parse(y[1] as string) as { k: number }).k;
        return p - q || (x[0] as number) - (y[0] as number);
      })
      .map((row) => row[0]);
    expect(sink.rows.map((row) => row[0])).toEqual(expected);
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  }, 60_000);
});

describe("wide sort keys under a tight grant", () => {
  for (const limit of [null, 50])
    it(`a batch whose key bytes exceed the reserve is encoded in parts (limit ${String(limit)})`, async () => {
      // 700 rows of ~3 KB text keys: one batch of key bytes is far past the 1.4 MB reserve, so
      // a step for the whole batch would be refused; parts, down to one row, are not.
      const types = [TEXT, INT];
      const { context: ctx, governor, spill } = await context(types, 0);
      const sink = new CollectSink();
      const op = new SortOperator(
        { types, keys: [asc(0), asc(1)], limit, offset: 0, withTies: false },
        ctx,
        sink,
      );
      const rows: Row[] = Array.from({ length: 4200 }, (_, i) => [
        `${String((i * 7919) % 4200).padStart(5, "0")}${"k".repeat(3000)}`,
        i,
      ]);
      await drive(new ArraySource(batchesOf(types, rows, { batchRows: 700 })), op);
      await op.closed;
      const expected = [...rows]
        .sort((x, y) => ((x[0] as string) < (y[0] as string) ? -1 : 1))
        .map((row) => row[1])
        .slice(0, limit ?? undefined);
      expect(sink.rows.map((row) => row[1])).toEqual(expected);
      expect(governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
    }, 60_000);
});

describe("review B7-p2b-2 #2: slices bound work, not rows", () => {
  // A controlled clock: each comparison costs one unit; the deadline is four units.
  it("refining equal prefixes yields within a few comparisons (reviewer: 1,664 units)", () => {
    let now = 0;
    const slice = new Slice(4, new TaskClock(() => now));
    const n = 400;
    const tuples = new Uint32Array(n * TUPLE_WORDS);
    for (let i = 0; i < n; i++) tuples[i * TUPLE_WORDS + PREFIX_WORDS] = i;
    const job = msdSort(
      tuples,
      new Uint32Array(n * TUPLE_WORDS),
      n,
      (a, b) => {
        now++;
        return b - a;
      },
      slice,
      new RadixScratch(),
    );
    expect(job.next().done).toBe(false);
    expect(now).toBeLessThanOrEqual(16);
    let steps = 0;
    while (job.next().done !== true) steps++;
    expect(steps).toBeGreaterThan(10);
    for (let i = 0; i < n; i++) expect(tuples[i * TUPLE_WORDS + PREFIX_WORDS]).toBe(n - 1 - i);
  });

  it("a merge sort yields within a few comparisons (reviewer: 5,120 units)", () => {
    let now = 0;
    const slice = new Slice(4, new TaskClock(() => now));
    const ids = Int32Array.from({ length: 1024 }, (_, i) => 1024 - i);
    const job = mergeSortJob(
      ids,
      new Int32Array(1024),
      (a, b) => {
        now++;
        return a - b;
      },
      slice,
    );
    job.next();
    expect(now).toBeLessThanOrEqual(10);
    while (job.next().done !== true);
    expect([...ids]).toEqual(Array.from({ length: 1024 }, (_, i) => i + 1));
  });

  it("400 keys sharing a 64 KiB prefix: finish's first synchronous segment stays short", async () => {
    // The reviewer's operator case: 598 ms in one segment before the fix.
    const types = [TEXT, INT];
    const { context: ctx, governor } = await context(types);
    const sink = new CollectSink();
    const op = new SortOperator(
      { types, keys: [asc(0)], limit: null, offset: 0, withTies: false },
      ctx,
      sink,
    );
    const prefix = "x".repeat(65_536);
    const rows: Row[] = Array.from({ length: 400 }, (_, i) => [
      prefix + String(400 - i).padStart(4, "0"),
      i,
    ]);
    for (const batch of batchesOf(types, rows, { batchRows: 1 }))
      if (op.push(batch) === "blocked") await op.ready();
    let serviced = false;
    const timer = setTimeout(() => {
      serviced = true;
    }, 0);
    const start = performance.now();
    const result = op.finish();
    const first = performance.now() - start;
    await result;
    clearTimeout(timer);
    expect(serviced).toBe(true);
    expect(first).toBeLessThan(150);
    expect(sink.rows.map((row) => row[1])).toEqual(rows.map((_, i) => 399 - i));
    expect(governor.usedBytes).toBe(0);
  }, 60_000);

  it("a batch of wide values arrives in byte-bounded parts with task yields between", async () => {
    const types = [TEXT, INT];
    const { context: ctx } = await context(types);
    const sink = new CollectSink();
    const op = new SortOperator(
      { types, keys: [asc(0)], limit: null, offset: 0, withTies: false },
      ctx,
      sink,
    );
    const rows: Row[] = Array.from({ length: 400 }, (_, i) => ["y".repeat(65_536) + String(i), i]);
    const [batch] = [...batchesOf(types, rows, { batchRows: 400 })];
    if (batch === undefined) throw new Error("no batch");
    let serviced = false;
    const timer = setTimeout(() => {
      serviced = true;
    }, 0);
    expect(op.push(batch)).toBe("blocked");
    await op.ready();
    clearTimeout(timer);
    expect(serviced).toBe(true);
    await op.finish();
    expect(sink.rows).toHaveLength(400);
  }, 60_000);
});

describe("review B7-p2b-2 #3: suspended sorts hold charged histograms", () => {
  it("two sorts suspended mid-finish both show their radix histograms in their grants", async () => {
    const make = async () => {
      const { context: ctx } = await context([INT]);
      const sink = new CollectSink();
      const op = new SortOperator(
        { types: [INT], keys: [asc(0)], limit: null, offset: 0, withTies: false },
        ctx,
        sink,
      );
      await pushAll(
        op,
        [INT],
        Array.from({ length: 300_000 }, (_, i) => [(i * 7919) % 300_000]),
      );
      return { op, ctx, sink };
    };
    const sorts = [await make(), await make()];
    let settled = 0;
    const finishing = sorts.map((x) => Promise.resolve(x.op.finish()).then(() => settled++));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(settled).toBe(0);
    for (const x of sorts)
      expect(x.ctx.grant.allocated).toBeGreaterThanOrEqual(RadixScratch.BYTES + 300_000 * 4);
    await Promise.all(finishing);
    for (const x of sorts) expect(x.sink.rows).toHaveLength(300_000);
  }, 60_000);
});

describe("top-N under a full grant compacts instead of spilling", () => {
  // The grant holds the kept rows about three times over (the store's, and the compaction's
  // copy while its arenas grow): below that a top-N cannot compact and must spill. Narrow rows
  // fit that inside the sort's minimum.
  for (const [name, width, keyed, limit, slack] of [
    ["a top-100, narrow rows, direct keys, minimum grant", 40, false, 100, 0],
    ["a top-1,000, wide rows, direct keys", 2000, false, 1000, 12 << 20],
    // Byte keys store the key bytes too: twice the reserve.
    ["a top-1,000, wide rows, byte keys", 2000, true, 1000, 24 << 20],
  ] as const)
    it(`never spills: ${name}`, async () => {
      const types = [INT, TEXT];
      const { context: ctx, governor, spill } = await context(types, slack);
      const sink = new CollectSink();
      const keys = keyed ? [asc(0), asc(1)] : [asc(0)];
      const op = new SortOperator({ types, keys, limit, offset: 0, withTies: false }, ctx, sink);
      const n = 60_000;
      const rows: Row[] = Array.from({ length: n }, (_, i) => [
        (i * 7919) % n,
        `${String(i)}-${"w".repeat(width)}`,
      ]);
      const start = performance.now();
      await drive(new ArraySource(batchesOf(types, rows, { batchRows: 500 })), op);
      const ms = performance.now() - start;
      await op.closed;
      process.stderr.write(
        `top-N ${name}: ${ms.toFixed(0)} ms, ${String(op.profile.runs)} runs, ${String(op.profile.compactions)} compactions\n`,
      );
      expect(op.profile.runs).toBe(0);
      expect(op.profile.compactions).toBeGreaterThan(0);
      expect(sink.rows.map((row) => row[0])).toEqual(Array.from({ length: limit }, (_, i) => i));
      expect(governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
    }, 120_000);
});

describe("top-N whose grant cannot hold the compaction reserve", () => {
  it("spills about as before (byte keys, 12 MiB)", async () => {
    // A reserve of ~12 MB does not fit beside the rows: compacting early, then letting the store
    // fill, ends in runs as without the reserve (a9ef41a: 24; this: 25; a reserve that blocked
    // appends regardless would write ~700).
    const types = [INT, TEXT];
    const { context: ctx, governor, spill } = await context(types, 12 << 20);
    const sink = new CollectSink();
    const op = new SortOperator(
      { types, keys: [asc(0), asc(1)], limit: 1000, offset: 0, withTies: false },
      ctx,
      sink,
    );
    const n = 60_000;
    const rows: Row[] = Array.from({ length: n }, (_, i) => [
      (i * 7919) % n,
      `${String(i)}-${"w".repeat(2000)}`,
    ]);
    await drive(new ArraySource(batchesOf(types, rows, { batchRows: 500 })), op);
    await op.closed;
    expect(op.profile.runs).toBeGreaterThan(0);
    expect(op.profile.runs).toBeLessThanOrEqual(30);
    expect(sink.rows.map((row) => row[0])).toEqual(Array.from({ length: 1000 }, (_, i) => i));
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  }, 120_000);
});

describe("review B7-p2c-1 #1: group top-k thresholds are charged", () => {
  const types = [INT, TEXT];
  const rows: Row[] = Array.from({ length: 66_000 }, (_, i) => [
    i % 1000,
    "x".repeat(1024) + String(66_000 - i).padStart(6, "0"),
  ]);
  /** Counts the bytes of threshold copies (Uint8Array slices made by #setGroupThresholds). */
  const copied = async (run: () => unknown): Promise<number> => {
    let bytes = 0;
    // TypedArray's slice, shadowed on Uint8Array.prototype for the run, then unshadowed.
    const typed = Object.getPrototypeOf(Uint8Array.prototype) as object;
    const original = Object.getOwnPropertyDescriptor(typed, "slice")?.value as (
      this: Uint8Array,
      start?: number,
      end?: number,
    ) => Uint8Array;
    Object.defineProperty(Uint8Array.prototype, "slice", {
      configurable: true,
      writable: true,
      value: function (this: Uint8Array, start?: number, end?: number): Uint8Array {
        const result = original.call(this, start, end);
        if (new Error().stack?.includes("setGroupThresholds") === true) bytes += result.length;
        return result;
      },
    });
    try {
      await run();
    } finally {
      Reflect.deleteProperty(Uint8Array.prototype, "slice");
    }
    return bytes;
  };
  const best = (out: readonly Row[]) => {
    expect(out).toHaveLength(1000);
    for (const row of out) {
      const p = row[0] as number;
      // The best (smallest) key of partition p: the last of its rows (66,000 − i descending).
      const last = 66_000 - 1 - ((66_000 - 1 - p) % 1000);
      expect(row[1]).toBe("x".repeat(1024) + String(66_000 - last).padStart(6, "0"));
    }
  };

  it("every copied threshold byte is charged (review probe: 1,038,000 bytes)", async () => {
    const { context: ctx, governor } = await context(types);
    const sink = new CollectSink();
    const op = new GroupTopKOperator(
      { types, partitionBy: [0], orderBy: [asc(1)], k: 1 },
      ctx,
      sink,
    );
    const bytes = await copied(() => drive(new ArraySource(batchesOf(types, rows)), op));
    expect(bytes).toBe(1_038_000);
    expect(op.profile.thresholdBytes).toBe(bytes);
    best(sink.rows);
    await op.closed;
    expect(governor.usedBytes).toBe(0);
  }, 60_000);

  it("a refused threshold drops the optimization and keeps every row", async () => {
    const { context: ctx, governor } = await context(types);
    // The grant refuses the threshold copies only (1,038 bytes each: the partition and order
    // key), not the partition set's growth: the operator must carry on without them.
    const grant = ctx.grant;
    const refusing = new Proxy(grant, {
      get(target, property) {
        if (property === "allocate")
          return (bytes: number) =>
            bytes === 1038 && new Error().stack?.includes("setGroupThresholds") === true
              ? false
              : target.allocate(bytes);
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function"
          ? (value as (...a: unknown[]) => unknown).bind(target)
          : value;
      },
    });
    const sink = new CollectSink();
    const op = new GroupTopKOperator(
      { types, partitionBy: [0], orderBy: [asc(1)], k: 1 },
      { ...ctx, grant: refusing },
      sink,
    );
    const bytes = await copied(() => drive(new ArraySource(batchesOf(types, rows)), op));
    expect(bytes).toBe(0);
    expect(op.profile.thresholdBytes).toBe(0);
    expect(op.profile.compactions).toBeGreaterThan(0);
    best(sink.rows);
    await op.closed;
    expect(governor.usedBytes).toBe(0);
  }, 60_000);
});

describe("review B7-p2c-1 #2: the MSD stack is bounded and charged", () => {
  // The reviewer's high-fanout keys: a shared first two bytes, then 65,536 two-row buckets.
  const n = 131_072;
  const fill = (t: Uint32Array) => {
    const S = TUPLE_WORDS;
    for (let i = 0; i < n; i++) {
      const v = (i >>> 1) * 256 + (i & 1);
      t[i * S] = 0x0100_0000 | (v >>> 8);
      t[i * S + 1] = (v << 24) >>> 0;
      t[i * S + PREFIX_WORDS] = i;
    }
  };
  it("two jobs in lockstep sort within a stack of n / 49 ranges (reviewer: 196,608 live numbers)", () => {
    const S = TUPLE_WORDS;
    const a = new Uint32Array(n * S);
    const b = new Uint32Array(n * S);
    fill(a);
    fill(b);
    const words = msdStackWords(n);
    expect(words).toBeLessThan(8100);
    const ga = msdSort(
      a,
      new Uint32Array(n * S),
      n,
      null,
      new Slice(0),
      new RadixScratch(),
      new Int32Array(words),
    );
    const gb = msdSort(
      b,
      new Uint32Array(n * S),
      n,
      null,
      new Slice(0),
      new RadixScratch(),
      new Int32Array(words),
    );
    let da = false;
    let db = false;
    while (!da || !db) {
      if (!da) da = ga.next().done === true;
      if (!db) db = gb.next().done === true;
    }
    for (let i = 0; i < n; i++) {
      expect(a[i * S + PREFIX_WORDS]).toBe(i);
      expect(b[i * S + PREFIX_WORDS]).toBe(i);
    }
  }, 60_000);

  it("the high-fanout keys sort under a full grant", async () => {
    const types = [INT];
    const { context: ctx, governor, spill } = await context(types, 0);
    const sink = new CollectSink();
    const op = new SortOperator(
      { types, keys: [asc(0)], limit: null, offset: 0, withTies: false },
      ctx,
      sink,
    );
    const keys = Array.from({ length: n }, (_, i) => -2_147_483_648 + (i >>> 1) * 256 + (i & 1));
    const shuffled: Row[] = keys.map((_, i) => [keys[(i * 7919) % n] ?? 0]);
    await drive(new ArraySource(batchesOf(types, shuffled)), op);
    await op.closed;
    // The first out-of-place row (a 131,072-row diff would take minutes to render).
    expect(sink.rows).toHaveLength(n);
    const wrong = sink.rows.findIndex((row, i) => row[0] !== keys[i]);
    expect(wrong).toBe(-1);
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  }, 60_000);
});

describe("review B7-p2b-1 resource probes", () => {
  for (const group of [false, true]) {
    const make = (
      types: PhysicalType[],
      ctx: Awaited<ReturnType<typeof context>>["context"],
      sink: Operator,
    ) =>
      group
        ? new GroupTopKOperator({ types, partitionBy: [0], orderBy: [asc(0)], k: 1 }, ctx, sink)
        : new SortOperator(
            { types, keys: [asc(0)], limit: 1, offset: 0, withTies: true },
            ctx,
            sink,
          );

    it(`an oversized row is refused beyond the memory reserve (group=${String(group)})`, async () => {
      const types = [INT, TEXT];
      const { context: ctx, governor, spill } = await context(types, 0);
      const sink = new CollectSink();
      const op = make(types, ctx, sink);
      const rows: Row[] = Array.from({ length: 8 }, () => [1, "x".repeat(2 << 20)]);
      await expect(
        Promise.resolve().then(() =>
          drive(new ArraySource(batchesOf(types, rows, { batchRows: 1 })), op),
        ),
      ).rejects.toThrow("exceeds the memory reserve");
      expect(sink.rows).toHaveLength(0);
      await op.closed;
      expect(spill.bytesInUse).toBe(0);
      expect(governor.usedBytes).toBe(0);
    }, 30_000);

    it(`a wide spill stays bounded (group=${String(group)})`, async () => {
      const types = [INT, TEXT];
      const { context: ctx, governor, spill } = await context(types, 0);
      const sink = new CollectSink();
      const op = make(types, ctx, sink);
      const rows: Row[] = Array.from({ length: 100 }, () => [1, "x".repeat(128 << 10)]);
      await drive(new ArraySource(batchesOf(types, rows, { batchRows: 1 })), op);
      expect(sink.rows).toHaveLength(group ? 1 : 100);
      await op.closed;
      expect(spill.bytesInUse).toBe(0);
      expect(governor.usedBytes).toBe(0);
    }, 60_000);
  }
});

describe("the merge sort checks its deadline inside run tails (review B7-p2b-2 #2)", () => {
  it("a long tail copy reads the clock without any comparison", () => {
    // Sorted input: every merge exhausts its left run by comparisons, then copies the right
    // run's tail with none. 2^19 ids take 16 merge levels (no copy back at the end), so the
    // only clock readings without a comparison between them are the tails' checks.
    const n = 1 << 19;
    const ids = Int32Array.from({ length: n }, (_, i) => i);
    let readings = 0;
    let stretch = 0;
    let longest = 0;
    const slice = new Slice(
      Infinity,
      new TaskClock(() => {
        readings++;
        stretch++;
        longest = Math.max(longest, stretch);
        return readings;
      }),
    );
    const job = mergeSortJob(
      ids,
      new Int32Array(n),
      (a, b) => {
        stretch = 0;
        return a - b;
      },
      slice,
    );
    while (job.next().done !== true);
    expect(Array.from(ids.subarray(0, 4))).toEqual([0, 1, 2, 3]);
    // The last merge copies a 2^18-row tail: a check every 8,192 copies is 32 readings.
    expect(longest).toBeGreaterThanOrEqual(16);
  });
});

describe("the store-wide operand decode checks its deadline per chunk (review B7-p2b-2 #3)", () => {
  it("an ICU sort that falls back to compare mode at its sort decodes in sliced chunks", async () => {
    // 6,000 distinct ~400-character values under an ICU collation, with a grant that holds the
    // rows and their distinct set but not the ranks' strings: the sort falls back to compare
    // mode and decodes every stored row's operand (DECODE_BYTES at a time) before sorting.
    const types = [TEXT, INT];
    const rows: Row[] = Array.from({ length: 6000 }, (_, i) => [
      `é${"v".repeat(400)}${String((i * 7919) % 6000)}`,
      i,
    ]);
    const minimum = sortMinimumBytes(types);
    const governor = new Governor(minimum + 12 * (1 << 20));
    const grant = await governor.admit({ label: "sort", minimumBytes: minimum, spillable: true });
    const sink = new CollectSink();
    const op = new SortOperator(
      {
        types,
        keys: [{ column: 0, direction: "asc", nulls: "last", collation: "en-US-x-icu" }],
        limit: null,
        offset: 0,
        withTies: false,
      },
      { grant, spill: new MemorySpillService(), queryId: 7 },
      sink,
    );
    // Per decode call, whether it decoded more than one row (a chunk of DECODE_BYTES).
    const events: boolean[] = [];
    const decode = vi.spyOn(sortKeys, "decodeOperands");
    decode.mockImplementation((...args) => {
      events.push(args[4] - args[3] > 1);
      return original(...args);
    });
    const due = vi.spyOn(Slice.prototype, "due");
    const checks: number[] = [];
    const decodes: number[] = [];
    try {
      await drive(new ArraySource(batchesOf(types, rows)), op);
    } finally {
      // (Read before the restore, which clears them.)
      checks.push(...due.mock.invocationCallOrder);
      decodes.push(...decode.mock.invocationCallOrder);
      decode.mockRestore();
      due.mockRestore();
    }
    // A multi-row decode followed by a check before the next decode.
    let checked = 0;
    let multi = 0;
    for (let i = 0; i < decodes.length; i++) {
      const at = decodes[i] ?? 0;
      const next = decodes[i + 1] ?? Infinity;
      if (events[i] !== true) continue;
      multi++;
      if (checks.some((c) => c > at && c < next)) checked++;
    }
    expect(op.profile.mode).toBe("compare");
    expect(multi).toBeGreaterThanOrEqual(10);
    expect(checked).toBeGreaterThanOrEqual(multi - 2);
    expect(sink.rows.length).toBe(6000);
    await op.closed;
    expect(governor.usedBytes).toBe(0);
  }, 60_000);
});

describe("R23.5: one task clock per worker", () => {
  // The worker's clock reads a fake time here (restored after): every operator's deadline is a
  // Slice on it, as in production.
  function withTime(run: (advance: (ms: number) => number) => void): void {
    const real = taskClock.now;
    let t = 0;
    taskClock.now = () => t;
    try {
      beginTask();
      run((ms) => (t += ms));
    } finally {
      taskClock.now = real;
      beginTask();
    }
  }

  it("two breakers alternating in one task yield by ~4 ms in total, not 4 ms each", () => {
    withTime((advance) => {
      // A breaker's entry: 3 ms of work in 1 ms steps, checking its deadline after each.
      const entry = (slice: Slice): boolean => {
        for (let ms = 0; ms < 3; ms++) {
          advance(1);
          if (slice.due()) return true;
        }
        return false;
      };
      const a = new Slice(YIELD_INTERVAL_MS);
      let yieldedAt = -1;
      if (entry(a)) yieldedAt = 3;
      // The second breaker is made just before its first entry (a downstream stage set up as
      // rows reach it); per-operator clocks would start it fresh here.
      const b = new Slice(YIELD_INTERVAL_MS);
      let t = 3;
      for (let round = 0; yieldedAt < 0 && round < 4; round++) {
        const slice = round % 2 === 0 ? b : a;
        for (let ms = 0; ms < 3; ms++) {
          t += 1;
          advance(1);
          if (slice.due()) {
            yieldedAt = t;
            break;
          }
        }
      }
      expect(yieldedAt).toBeGreaterThan(0);
      expect(yieldedAt).toBeLessThanOrEqual(YIELD_INTERVAL_MS);
    });
  });

  it("drive() does not start a task: a pipeline started late in a task yields after its first batch", () => {
    withTime((advance) => {
      // 3 ms of the task went to other work before this pipeline started.
      advance(3);
      let pushes = 0;
      let left = 10;
      const source: BatchSource = {
        next: () => {
          advance(1);
          return left-- > 0
            ? { batch: { length: 1, columns: [], selection: null }, transfer: null as never }
            : null;
        },
        abort: () => undefined,
      };
      const sink: Operator = {
        push: () => {
          pushes++;
          return "ready";
        },
        ready: () => Promise.resolve(),
        finish: () => undefined,
        abort: () => undefined,
      };
      const run = drive(source, sink);
      // The task's 4 ms ran out after the first batch: the driver yields (a Promise).
      expect(run).toBeInstanceOf(Promise);
      expect(pushes).toBe(1);
      void run;
    });
  });
});

const admissionReach = createReach("next-sort-scratch-admission", [
  "sort",
  "group",
  "pool refused",
  "scratch refused",
  "admitted",
]);
afterAll(() => admissionReach.assertReached());
describe("R19: sort scratch admission", () => {
  for (const kind of ["sort", "group"] as const)
    for (const refuseAt of [1, 2, 0])
      it(`${kind} constructs radix only after admission, refusal ${String(refuseAt)}`, async () => {
        const { context: ctx, governor, spill } = await context([INT]);
        const native = globalThis.Int32Array;
        const allocate = ctx.grant.allocate.bind(ctx.grant);
        let calls = 0;
        const observed: number[] = [];
        const allocation = vi.spyOn(ctx.grant, "allocate").mockImplementation((bytes) => {
          calls++;
          return calls === refuseAt ? false : allocate(bytes);
        });
        vi.stubGlobal(
          "Int32Array",
          new Proxy(native, {
            construct(target, args, newTarget) {
              const value = Reflect.construct(target, args, newTarget) as Int32Array;
              if (value.length === 65_537) observed.push(calls);
              return value;
            },
          }),
        );
        let op: SortOperator | GroupTopKOperator | undefined;
        try {
          const make = () =>
            kind === "sort"
              ? new SortOperator(
                  { types: [INT], keys: [asc(0)], limit: null, offset: 0, withTies: false },
                  ctx,
                  new CollectSink(),
                )
              : new GroupTopKOperator(
                  { types: [INT], partitionBy: [0], orderBy: [asc(0)], k: 1 },
                  ctx,
                  new CollectSink(),
                );
          if (refuseAt === 0) {
            op = make();
            expect(observed).toEqual([2, 2]);
            await drive(new ArraySource([]), op);
            await op.closed;
          } else {
            expect(make).toThrow(RangeError);
            expect(observed).toEqual([]);
            // Scratch refusal must unwind the pool reservation already made by this constructor.
            expect(ctx.grant.allocated).toBe(0);
          }
          admissionReach.beginHistory();
          admissionReach.reach(kind);
          admissionReach.reach(
            refuseAt === 0 ? "admitted" : refuseAt === 1 ? "pool refused" : "scratch refused",
          );
        } finally {
          vi.unstubAllGlobals();
          allocation.mockRestore();
          op?.abort(new Error("admission test cleanup"));
          await op?.closed.catch(() => undefined);
          ctx.grant.close();
        }
        expect(governor.usedBytes).toBe(0);
        expect(spill.bytesInUse).toBe(0);
      });
});
