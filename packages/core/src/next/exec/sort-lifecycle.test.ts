/**
 * The sort operators' lifecycle under ruling R23 (review B7-p2b-1): operator-owned scratch
 * (two operators interleaved on one worker), finish work in task-yielding slices (a zero-delay
 * timer runs before finish completes), the abort signal checked before finish, at every slice and
 * after backpressure, and compare mode's decoded operands charged to the grant. Also the review's
 * resource probes: an oversized row refused cleanly, and a wide spill that stays bounded.
 */
import { describe, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import type { Batch } from "../vector/vector.js";
import {
  ArraySource,
  CollectSink,
  INT,
  TEXT,
  batchesOf,
  rowsOf,
  type Row,
} from "./join-fixtures.js";
import { drive, type Operator, type PushResult } from "./pipeline.js";
import { RadixScratch, type SortColumn } from "./sort-keys.js";
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
  }, 60_000);
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
