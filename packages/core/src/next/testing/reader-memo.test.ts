/**
 * A reader memo (access/reader-memo.ts) never serves readers of a root its segment no longer
 * reads, and gives back everything it holds.
 *
 * Law (access/reader-memo.ts; write/memtable.ts "Lifecycle"; access/snapshot.ts "What a snapshot
 * is"): a memory segment reads its resident chunks until its memtable is published, then its
 * published group root; that root can change again (a staged memtable re-staged or folded into a
 * new group); a pinned snapshot keeps every blob it reaches readable until release, whatever
 * merges and flushes run meanwhile. The memo is reused only for the same segment and the same
 * root by full blob identity, is closed by the snapshot's release, is declined (and the read
 * still served without it) when its budget refuses, and holds zero bytes once closed.
 *
 * Reads go through the segment's own bounded contract (`readHint`, then `chunk` per column), as
 * the ON CONFLICT fetch does (exec/point-fetch.ts). The root-change case drives `MemoryReads`
 * over a source whose published root switches between two real published groups, because a
 * re-staging inside one statement cannot be scheduled from a test without the engine's internals.
 */
import { expect, it } from "vitest";
import { MemoDeclinedError, ReaderMemo } from "../access/reader-memo.js";
import type { ByteBudget } from "../access/space.js";
import {
  SnapshotReleasedError,
  type MemoryReadContext,
  type MemoryReadHint,
  type MemoryReadRequest,
  type MemorySegment,
  type Snapshot,
} from "../access/snapshot.js";
import type { BlobRef } from "../format/manifest.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { MemoryOutputs } from "../exec/memory-output.js";
import { countingBudget } from "../storage/space-test-support.js";
import { need } from "../write/bytes.js";
import type { PhysicalType } from "../types.js";
import { MemoryReads, type MemoryReadSource } from "../write/memory-read.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  tableInput,
  texts,
} from "../write/test-support.js";

const ROWS = 8;
const TEXT: PhysicalType = { kind: "text" };

type PointTarget = Pick<MemorySegment, "readHint" | "chunk">;

/** `(id, v)` of one row, read as the point fetch reads it. */
async function readRow(
  target: PointTarget,
  columns: readonly number[],
  position: number,
  context: MemoryReadContext,
): Promise<unknown[]> {
  const request: MemoryReadRequest = {
    columns,
    chunk: 0,
    startRow: position,
    maxRows: 1,
    targetBytes: 4096,
  };
  const hint: MemoryReadHint = await target.readHint(request, context);
  expect(hint.rowCount).toBe(1);
  const outputs = new MemoryOutputs([INT, TEXT], context.budget);
  try {
    const out = outputs.prepare(hint.outputs);
    const values: unknown[] = [];
    for (let c = 0; c < columns.length; c++) {
      const result = await target.chunk(columns[c] ?? -1, 0, need(out[c]), position, {
        hint,
        context,
      });
      expect(result.rowCount).toBe(1);
      values.push(valueAt(result.vector, 0));
    }
    return values;
  } finally {
    outputs.close();
  }
}

const row = (id: number, prefix = "r") => [id, `${prefix}${String(id)}`];

interface Fixture {
  readonly h: Harness;
  readonly id: number;
  readonly columns: readonly number[];
  readonly insert: (from: number, count: number, prefix?: string) => Promise<void>;
}

async function fixture(): Promise<Fixture> {
  const h = new Harness("strict", "memory", ROWS);
  await h.open();
  let id = 0;
  await autocommit(h.path, async (s) => {
    id =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false), column("v", TEXT)]),
        })
      ).id ?? 0;
  });
  const columns = h.path.catalog.table(id)?.columns.map((c) => c.id) ?? [];
  return {
    h,
    id,
    columns,
    insert: (from, count, prefix = "r") =>
      autocommit(h.path, (s) =>
        s
          .table(id)
          .insert(
            batch(
              ints(Array.from({ length: count }, (_, k) => from + k)),
              texts(Array.from({ length: count }, (_, k) => `${prefix}${String(from + k)}`)),
            ),
            null,
          ),
      ).then(() => undefined),
  };
}

async function memorySegments(snapshot: Snapshot, id: number): Promise<MemorySegment[]> {
  const table = await snapshot.table(id, { budget: countingBudget(Infinity) });
  return table.segments.filter((s): s is MemorySegment => s.kind === "memory");
}

function memoFor(budget: ByteBudget = countingBudget(Infinity)) {
  return new ReaderMemo(budget, new AbortController().signal);
}

it("reads resident rows directly, then opens one entry once the memtable is published", async () => {
  const { h, id, columns, insert } = await fixture();
  let snapshot: Snapshot | undefined;
  try {
    await insert(0, ROWS);
    snapshot = h.path.pin();
    const [segment] = await memorySegments(snapshot, id);
    if (segment === undefined) throw new Error("memory segment");
    const memo = memoFor(),
      budget = countingBudget(Infinity);
    const context = { budget, pages: h.space, signal: memo.signal, readers: memo };
    expect(await readRow(segment, columns, 0, context)).toEqual(row(0));
    expect(memo.opens, "a resident row needs no published reader").toBe(0);
    // Publication between two reads: the segment now reads its published group.
    await insert(ROWS, 1);
    await h.path.quiesce();
    expect(await readRow(segment, columns, 1, context)).toEqual(row(1));
    expect(memo.opens).toBe(1);
    for (let k = 2; k < ROWS; k++)
      expect(await readRow(segment, columns, k, context)).toEqual(row(k));
    expect(memo.opens, "later reads of the same root reuse its readers").toBe(1);
    expect(memo.reuses).toBeGreaterThanOrEqual(2 * (ROWS - 2));
    expect(memo.held).toBeGreaterThan(0);
    expect(budget.used, "each read gives back its own charges").toBe(0);
    memo.close();
    expect(memo.held).toBe(0);
  } finally {
    snapshot?.release();
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});

it("replaces the entry when the segment's published root changes", async () => {
  const { h, id, columns, insert } = await fixture();
  let snapshot: Snapshot | undefined;
  try {
    // Two full memtables (ids 0..7 and 8..15), pinned while the second is resident, then both
    // published.
    await insert(0, ROWS, "a");
    await insert(ROWS, ROWS, "b");
    snapshot = h.path.pin();
    await insert(2 * ROWS, 1);
    await h.path.quiesce();
    const segments = await memorySegments(snapshot, id);
    expect(segments.map((s) => s.rowCount)).toEqual([ROWS, ROWS]);
    const roots = segments.map(
      (s) =>
        (s as unknown as { memtable: { published: { group: { root: BlobRef } } | null } }).memtable
          .published?.group.root,
    );
    const [a, b] = roots;
    if (a === undefined || b === undefined) throw new Error("both memtables are published");
    const view = (
      segments[0] as unknown as {
        view: {
          column: MemoryReadSource["column"];
          snapshot: { checkRead(): void; watchCursor(cursor: { close(): void }): () => void };
        };
      }
    ).view;
    let root = a;
    const reads = new MemoryReads({
      rowCount: ROWS,
      column: (columnId) => view.column(columnId),
      resident: () => null,
      publishedRoot: () => root,
      publishedMissing: () => {
        throw new Error("both groups store every column");
      },
      check: () => view.snapshot.checkRead(),
      watch: (close) => view.snapshot.watchCursor({ close }),
    });
    const target: PointTarget = {
      readHint: (request, context) => reads.hint(request, context),
      chunk: (columnId, chunk, out, start = 0, step) => {
        if (step === undefined) throw new Error("bounded reads only");
        return reads.chunk(columnId, chunk, out, start, step);
      },
    };
    const memo = memoFor(),
      budget = countingBudget(Infinity);
    const context = { budget, pages: h.space, signal: memo.signal, readers: memo };
    expect(await readRow(target, columns, 1, context)).toEqual(row(1, "a"));
    expect(await readRow(target, columns, 2, context)).toEqual(row(2, "a"));
    expect(memo.opens).toBe(1);
    root = b;
    expect(await readRow(target, columns, 1, context), "never the old root's readers").toEqual(
      row(ROWS + 1, "b"),
    );
    expect(memo.opens).toBe(2);
    root = a;
    expect(await readRow(target, columns, 1, context)).toEqual(row(1, "a"));
    expect(memo.opens).toBe(3);
    expect(memo.invalidations).toBe(2);
    memo.close();
    expect(memo.held).toBe(0);
    expect(budget.used).toBe(0);
  } finally {
    snapshot?.release();
    await h.close();
  }
});

it("keeps serving the pinned root while later flushes and index merges run", async () => {
  const h = new Harness("strict", "memory", ROWS, undefined, {
    merges: { mode: "manual", fanIn: 2, splitEntries: 16, sliceEntries: 32 },
  });
  await h.open();
  let snapshot: Snapshot | undefined;
  try {
    let id = 0;
    await autocommit(h.path, async (s) => {
      id =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false), column("v", TEXT)]),
          })
        ).id ?? 0;
    });
    const columns = h.path.catalog.table(id)?.columns.map((c) => c.id) ?? [];
    const insert = (from: number, count: number) =>
      autocommit(h.path, (s) =>
        s
          .table(id)
          .insert(
            batch(
              ints(Array.from({ length: count }, (_, k) => from + k)),
              texts(Array.from({ length: count }, (_, k) => `r${String(from + k)}`)),
            ),
            null,
          ),
      );
    await insert(0, ROWS);
    snapshot = h.path.pin();
    await insert(ROWS, 1);
    await h.path.quiesce();
    const [segment] = await memorySegments(snapshot, id);
    if (segment === undefined) throw new Error("memory segment");
    const memo = memoFor(),
      budget = countingBudget(Infinity);
    const context = { budget, pages: h.space, signal: memo.signal, readers: memo };
    expect(await readRow(segment, columns, 0, context)).toEqual(row(0));
    const merged = h.path.maintenance.merges;
    for (let k = 1; k < ROWS; k++) {
      // More memtables published, their index runs merged, between two reads.
      await insert(ROWS * (k + 1), ROWS);
      await h.path.recycleLog();
      await h.path.quiesce();
      await h.path.runMaintenance();
      expect(await readRow(segment, columns, k, context)).toEqual(row(k));
    }
    expect(h.path.maintenance.merges, "index runs were merged between reads").toBeGreaterThan(
      merged,
    );
    expect(memo.opens, "the pinned root never changed").toBe(1);
    memo.close();
    expect(memo.held).toBe(0);
    expect(budget.used).toBe(0);
  } finally {
    snapshot?.release();
    await h.close();
  }
}, 60_000);

it("closes the entry when the snapshot is released", async () => {
  const { h, id, columns, insert } = await fixture();
  let snapshot: Snapshot | undefined;
  try {
    await insert(0, ROWS);
    snapshot = h.path.pin();
    await insert(ROWS, 1);
    await h.path.quiesce();
    const [segment] = await memorySegments(snapshot, id);
    if (segment === undefined) throw new Error("memory segment");
    const memo = memoFor(),
      budget = countingBudget(Infinity);
    const context = { budget, pages: h.space, signal: memo.signal, readers: memo };
    expect(await readRow(segment, columns, 3, context)).toEqual(row(3));
    expect(memo.held).toBeGreaterThan(0);
    snapshot.release();
    snapshot = undefined;
    expect(memo.held, "the release closed the entry").toBe(0);
    expect(memo.invalidations).toBe(1);
    await expect(readRow(segment, columns, 4, context)).rejects.toBeInstanceOf(
      SnapshotReleasedError,
    );
    expect(memo.held).toBe(0);
    expect(budget.used).toBe(0);
    memo.close();
  } finally {
    snapshot?.release();
    await h.close();
  }
});

it("declines a memo its budget refuses, holding nothing, and reads without it", async () => {
  const { h, id, columns, insert } = await fixture();
  let snapshot: Snapshot | undefined;
  try {
    await insert(0, ROWS);
    snapshot = h.path.pin();
    await insert(ROWS, 1);
    await h.path.quiesce();
    const [segment] = await memorySegments(snapshot, id);
    if (segment === undefined) throw new Error("memory segment");
    const memo = memoFor(countingBudget(0)),
      budget = countingBudget(Infinity);
    await expect(
      readRow(segment, columns, 5, {
        budget,
        pages: h.space,
        signal: memo.signal,
        readers: memo,
      }),
    ).rejects.toBeInstanceOf(MemoDeclinedError);
    expect(memo.held).toBe(0);
    expect(budget.used).toBe(0);
    expect(
      await readRow(segment, columns, 5, { budget, pages: h.space, signal: memo.signal }),
    ).toEqual(row(5));
    expect(budget.used).toBe(0);
    memo.close();
  } finally {
    snapshot?.release();
    await h.close();
  }
});

it("gives back everything when the memo closes while a read is in flight", async () => {
  const { h, id, columns, insert } = await fixture();
  let snapshot: Snapshot | undefined;
  try {
    await insert(0, ROWS);
    snapshot = h.path.pin();
    await insert(ROWS, 1);
    await h.path.quiesce();
    const [segment] = await memorySegments(snapshot, id);
    if (segment === undefined) throw new Error("memory segment");
    const controller = new AbortController();
    const memo = new ReaderMemo(countingBudget(Infinity), controller.signal),
      budget = countingBudget(Infinity);
    const context = { budget, pages: h.space, signal: memo.signal, readers: memo };
    expect(await readRow(segment, columns, 0, context)).toEqual(row(0));
    // The statement aborts while the next read is suspended: its signal, then the memo's close.
    const pending = readRow(segment, columns, 6, context);
    controller.abort(new Error("statement aborted"));
    memo.close();
    await pending.then(
      (values) => expect(values).toEqual(row(6)),
      (error: unknown) => expect(error).toBeInstanceOf(Error),
    );
    expect(memo.held, "nothing is left held after the read drains").toBe(0);
    expect(budget.used).toBe(0);
  } finally {
    snapshot?.release();
    await h.close();
  }
});
