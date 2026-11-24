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
import { expect, it, vi } from "vitest";
import { MemoDeclinedError, ReaderMemo } from "../access/reader-memo.js";
import type { ByteBudget } from "../access/space.js";
import {
  MEMORY_CHUNK_ROWS,
  SnapshotReleasedError,
  type MemoryReadContext,
  type MemoryReadHint,
  type MemoryReadRequest,
  type MemorySegment,
  type Snapshot,
} from "../access/snapshot.js";
import type { BlobRef } from "../format/manifest.js";
import { parseCharge } from "../format/manifest-codec.js";
import { metadataCachesFor } from "../format/metadata-reader.js";
import { ChunkReader } from "../page/reader.js";
import { StoreCorruptionError } from "../storage/contract.js";
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
  const chunk = Math.floor(position / MEMORY_CHUNK_ROWS),
    row = position % MEMORY_CHUNK_ROWS;
  const request: MemoryReadRequest = {
    columns,
    chunk,
    startRow: row,
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
      const result = await target.chunk(columns[c] ?? -1, chunk, need(out[c]), row, {
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

// ── Review of #62 (Gemini Neon, 7012263e) ─────────────────────────────────────────────────────

/** A published memtable of `rows` rows of `(id, v TEXT)` with `width`-byte values, pinned. */
async function publishedText(rows: number, width: number) {
  const h = new Harness("strict", "memory", rows);
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
  const value = (k: number) => `${String(k)}:`.padEnd(width, "x");
  const insert = (from: number, count: number) =>
    autocommit(h.path, (s) =>
      s
        .table(id)
        .insert(
          batch(
            ints(Array.from({ length: count }, (_, k) => from + k)),
            texts(Array.from({ length: count }, (_, k) => value(from + k))),
          ),
          null,
        ),
    );
  for (let from = 0; from < rows; from += 512) await insert(from, Math.min(512, rows - from));
  const snapshot = h.path.pin();
  await insert(rows, 1);
  await h.path.quiesce();
  const [segment] = await memorySegments(snapshot, id);
  if (segment === undefined) throw new Error("memory segment");
  expect(segment.rowCount).toBe(rows);
  const columns = h.path.catalog.table(id)?.columns.map((c) => c.id) ?? [];
  const root = (
    segment as unknown as { memtable: { published: { group: { root: BlobRef } } | null } }
  ).memtable.published?.group.root;
  if (root === undefined) throw new Error("the memtable is published");
  return { h, snapshot, segment, columns, root, value };
}

/** A space whose reads wait at a gate once armed, and honour their signal while they wait. */
function gatedPages(space: Harness["space"]) {
  let armed = false,
    arrived: () => void = () => undefined,
    open: () => void = () => undefined;
  let reached = new Promise<void>((resolve) => (arrived = resolve));
  let gate = new Promise<void>((resolve) => (open = resolve));
  let signals: AbortSignal[] = [];
  const pages: Pick<Harness["space"], "peek" | "read"> = {
    // Cold: every blob is read, so a read can be in flight.
    peek: () => null,
    read: async (refs, options) => {
      if (armed) {
        armed = false;
        if (options.signal !== undefined) signals.push(options.signal);
        arrived();
        await new Promise<void>((resolve, reject) => {
          void gate.then(resolve);
          options.signal?.addEventListener(
            "abort",
            () => {
              reject(new DOMException("The gated read was cancelled", "AbortError"));
            },
            { once: true },
          );
        });
      }
      return space.read(refs, options);
    },
  };
  return {
    pages,
    arm() {
      armed = true;
      signals = [];
      reached = new Promise<void>((resolve) => (arrived = resolve));
      gate = new Promise<void>((resolve) => (open = resolve));
      return reached;
    },
    release: () => open(),
    signals: () => signals,
  };
}

it("keeps the snapshot's pins until a memo read in flight has drained", async () => {
  const { h, snapshot, segment, columns, value } = await publishedText(64, 16);
  let released = false;
  try {
    const gated = gatedPages(h.space);
    const memo = memoFor(),
      budget = countingBudget(Infinity);
    const context = { budget, pages: gated.pages, signal: memo.signal, readers: memo };
    const pins = h.space.stats.pinCount;
    const reached = gated.arm();
    const pending = readRow(segment, columns, 3, context);
    await reached;
    // Released while the memo's read is suspended in storage I/O.
    snapshot.release();
    released = true;
    expect(h.space.stats.pinCount, "pins held while the read is in flight").toBe(pins);
    expect(
      gated.signals().every((signal) => signal.aborted),
      "the read is cancelled",
    ).toBe(true);
    gated.release();
    await expect(pending).rejects.toThrow();
    expect(h.space.stats.pinCount, "released once the read drained").toBeLessThan(pins);
    expect(memo.held).toBe(0);
    expect(budget.used).toBe(0);
    void value;
    memo.close();
  } finally {
    if (!released) snapshot.release();
    await h.close();
  }
});

it("cancels a memo read in flight when the memo closes, and holds nothing after", async () => {
  const { h, snapshot, segment, columns } = await publishedText(64, 16);
  try {
    const gated = gatedPages(h.space);
    const memo = memoFor(),
      budget = countingBudget(Infinity);
    const context = { budget, pages: gated.pages, signal: memo.signal, readers: memo };
    const reached = gated.arm();
    const pending = readRow(segment, columns, 5, context);
    await reached;
    memo.close();
    expect(
      gated.signals().every((signal) => signal.aborted),
      "the read is cancelled",
    ).toBe(true);
    gated.release();
    await expect(pending).rejects.toThrow();
    expect(memo.held).toBe(0);
    expect(budget.used).toBe(0);
  } finally {
    snapshot.release();
    await h.close();
  }
});

it("keeps at most one page per column across a memtable's rows", async () => {
  // 4,096 rows of 200-byte text: many pages per column.
  const { h, snapshot, segment, columns, value } = await publishedText(4096, 200);
  try {
    const memo = memoFor(),
      budget = countingBudget(Infinity);
    const context = { budget, pages: h.space, signal: memo.signal, readers: memo };
    expect(await readRow(segment, columns, 0, context)).toEqual([0, value(0)]);
    const first = memo.held;
    let most = first;
    for (let k = 1; k < 4096; k += 7) {
      expect(await readRow(segment, columns, k, context)).toEqual([k, value(k)]);
      most = Math.max(most, memo.held);
    }
    expect(memo.opens).toBe(1);
    // Holding every page read so far grows with the rows (18x before); one page per column
    // stays within a page of the first read's holdings.
    expect(most, "the memo's holdings stay at one page per column").toBeLessThan(2 * first);
    memo.close();
    expect(memo.held).toBe(0);
    expect(budget.used).toBe(0);
  } finally {
    snapshot.release();
    await h.close();
  }
}, 60_000);

it("loads a kept page once for consecutive rows of it", async () => {
  const { h, snapshot, segment, columns, value } = await publishedText(256, 16);
  try {
    const memo = memoFor(),
      budget = countingBudget(Infinity);
    const context = { budget, pages: h.space, signal: memo.signal, readers: memo };
    expect(await readRow(segment, columns, 0, context)).toEqual([0, value(0)]);
    const loads = vi.spyOn(ChunkReader.prototype, "load");
    try {
      for (let k = 1; k <= 8; k++)
        expect(await readRow(segment, columns, k, context)).toEqual([k, value(k)]);
      // Rows 1-8 sit in the pages row 0 loaded: sizing and reading them loads nothing again.
      expect(loads, "no page is loaded again for rows of the kept pages").not.toHaveBeenCalled();
    } finally {
      loads.mockRestore();
    }
    memo.close();
  } finally {
    snapshot.release();
    await h.close();
  }
});

it("reports a real failure after a declined optional probe as itself, not as a decline", async () => {
  const { h, snapshot, segment, columns, root } = await publishedText(4096, 200);
  try {
    // A cold owner root cache: the memo's root read builds the optional validated view, whose
    // construction peak (format/manifest-codec.ts `tryDecodeGroupRootView`) the budget refuses.
    const caches = metadataCachesFor(h.space);
    caches?.groupRoots?.clear();
    const viewPeak = 2 * parseCharge(root.length) + root.length + 1024;
    let probed = 0;
    const memo = memoFor({
      allocate: (bytes) => {
        if (bytes !== viewPeak) return true;
        probed++;
        return false;
      },
      free: () => undefined,
    });
    const budget = countingBudget(Infinity);
    const context = { budget, pages: h.space, signal: memo.signal, readers: memo };
    expect(await readRow(segment, columns, 0, context)).toHaveLength(2);
    expect(probed, "the optional view was refused, the ordinary decode used").toBe(1);
    expect(memo.opens).toBe(1);
    // A later read of the same entry fails in storage (a page that must be read, and is damaged).
    const peek = vi.spyOn(h.space, "peek").mockReturnValue(null);
    const read = vi
      .spyOn(h.space, "read")
      .mockRejectedValue(new StoreCorruptionError("Checksum mismatch (test)"));
    try {
      await expect(readRow(segment, columns, 4000, context)).rejects.toBeInstanceOf(
        StoreCorruptionError,
      );
    } finally {
      peek.mockRestore();
      read.mockRestore();
    }
    memo.close();
  } finally {
    snapshot.release();
    await h.close();
  }
}, 60_000);

it("refuses a memo read under another signal, or as a one-pass read", async () => {
  const { h, snapshot, segment, columns } = await publishedText(64, 16);
  try {
    const memo = memoFor(),
      budget = countingBudget(Infinity);
    await expect(
      readRow(segment, columns, 1, {
        budget,
        pages: h.space,
        signal: new AbortController().signal,
        readers: memo,
      }),
    ).rejects.toThrow("A memo read must use its memo's signal");
    await expect(
      readRow(segment, columns, 1, {
        budget,
        pages: h.space,
        signal: memo.signal,
        readers: memo,
        once: true,
      }),
    ).rejects.toThrow("A one-pass read cannot keep readers in a memo");
    expect(budget.used).toBe(0);
    memo.close();
  } finally {
    snapshot.release();
    await h.close();
  }
});
