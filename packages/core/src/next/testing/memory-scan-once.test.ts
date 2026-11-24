/**
 * A table scan's memory reads keep the owner's caching rules (reviews of #61, P3).
 *
 * Law:
 * - exec/table-scan.ts `TableScanOptions.directoryOnce`: "Non-retaining sources borrow hits
 *   without admitting directory misses". A group's chunk readers open with `once: true`; a memory
 *   segment re-pointed to its published group reads through `MemoryReads`, which resolves the
 *   owner's caches, so the scan must say `once` there too (access/snapshot.ts
 *   `MemoryReadContext.once`), and that read must not admit the root blob to the page cache
 *   either (storage/extents.ts `ReadOptions.once`). A retaining scan of the same rows admits
 *   them, which shows the fixture reaches the caches at all.
 * - format/manifest-codec.ts `OptionalParseBudget`: optional probes must not increase a sizing
 *   pass's mandatory requirement. A cached root probe allocates optional room through the scan's
 *   memory, so that memory must pass `optionalAllocate` on to its owner.
 */
import { expect, it, vi } from "vitest";
import type { MemorySegment, Snapshot, TableSnapshot } from "../access/snapshot.js";
import type { ByteBudget, SpaceReader } from "../access/space.js";
import { OutputArena } from "../exec/kernels/vectors.js";
import { chunkReads } from "../exec/scan.js";
import { TableScan } from "../exec/table-scan.js";
import type { BlobRef } from "../format/manifest.js";
import { metadataCachesFor } from "../format/metadata-reader.js";
import { countingBudget } from "../storage/space-test-support.js";
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

const ROWS = 64;

/** A sizing pass's budget, as page/read-bounds.ts `SizingBudget`: optional room is not required. */
class Sizing implements ByteBudget {
  allocated = 0;
  required = 0;
  allocate(bytes: number): boolean {
    this.allocated += bytes;
    this.required = Math.max(this.required, this.allocated);
    return true;
  }
  optionalAllocate(bytes: number): boolean {
    this.allocated += bytes;
    return true;
  }
  free(bytes: number): void {
    this.allocated -= bytes;
  }
}

interface Published {
  readonly h: Harness;
  readonly snapshot: Snapshot;
  readonly table: TableSnapshot;
  readonly root: BlobRef;
}

/** A pinned snapshot whose one memory segment reads a published memtable of `ROWS` rows. */
async function published(work: (fixture: Published) => Promise<void>): Promise<void> {
  const h = new Harness("strict", "memory", ROWS);
  await h.open();
  let snapshot: Snapshot | undefined;
  try {
    let id = 0;
    await autocommit(h.path, async (s) => {
      id =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false), column("v", { kind: "text" })]),
          })
        ).id ?? 0;
    });
    const rows = (from: number, count: number) =>
      batch(
        ints(Array.from({ length: count }, (_, k) => from + k)),
        texts(Array.from({ length: count }, (_, k) => `v${String(from + k)}`)),
      );
    await autocommit(h.path, (s) => s.table(id).insert(rows(0, ROWS), null));
    // Pinned while resident, then published: the snapshot reads the memtable re-pointed.
    snapshot = h.path.pin();
    await autocommit(h.path, (s) => s.table(id).insert(rows(ROWS, 1), null));
    await h.path.quiesce();
    const full = await snapshot.table(id, { budget: countingBudget(Infinity) });
    const memory = full.segments.filter((s): s is MemorySegment => s.kind === "memory");
    expect(memory.map((s) => s.rowCount)).toEqual([ROWS]);
    const root = (
      memory[0] as unknown as { memtable: { published: { group: { root: BlobRef } } | null } }
    ).memtable.published?.group.root;
    if (root === undefined) throw new Error("the memtable is published");
    await work({ h, snapshot, table: { ...full, segments: memory, liveRowCount: ROWS }, root });
  } finally {
    snapshot?.release();
    await h.close();
  }
}

/** Reads every row with a bounded scan; returns the rows read. */
async function scanAll(
  table: TableSnapshot,
  pages: SpaceReader,
  budget: ByteBudget,
  once: boolean,
): Promise<number> {
  const arena = new OutputArena(countingBudget(Infinity), 2048);
  const directories = metadataCachesFor(pages)?.directories;
  const scan = new TableScan({
    table,
    pages,
    reads: chunkReads,
    budget,
    columns: table.columns.map((c) => c.id),
    filter: null,
    emitLocator: false,
    boundedMemory: true,
    ...(directories === undefined ? {} : { directories, directoryOnce: once }),
    context: { arena, params: { slots: [] }, input: [] },
  });
  let read = 0;
  try {
    for (let item = await scan.next(); item !== null; item = await scan.next()) {
      read += item.batch.selection?.count ?? item.batch.length;
      item.transfer?.release();
    }
  } finally {
    scan.close();
    arena.close();
  }
  return read;
}

it.each([true, false])("a scan with directoryOnce=%s admits only when it retains", (once) =>
  published(async ({ h, table }) => {
    const caches = metadataCachesFor(h.space);
    const roots = caches?.groupRoots,
      directories = caches?.directories;
    if (roots === undefined || directories === undefined) throw new Error("owner caches");
    roots.clear();
    directories.clear();
    const budget = countingBudget(Infinity);
    expect(await scanAll(table, h.space, budget, once)).toBe(ROWS);
    if (once) {
      expect(roots.entryCount, "a one-pass scan admits no group root").toBe(0);
      expect(directories.entryCount, "nor any column directory").toBe(0);
    } else {
      expect(roots.entryCount, "a retaining scan admits the group root").toBeGreaterThan(0);
      expect(directories.entryCount, "and its column directories").toBeGreaterThan(0);
    }
    expect(budget.used).toBe(0);
  }),
);

it("a one-pass scan reads its memory segment's root blob without admitting it", () =>
  published(async ({ h, table, root }) => {
    const same = (ref: BlobRef) =>
      ref.offset === root.offset &&
      ref.length === root.length &&
      ref.checksum === root.checksum &&
      ref.extent.offset === root.extent.offset;
    const accesses: Array<boolean | undefined> = [];
    const peek = h.space.peek.bind(h.space),
      read = h.space.read.bind(h.space);
    vi.spyOn(h.space, "peek").mockImplementation((ref, options) => {
      if (same(ref)) accesses.push(options?.once);
      return peek(ref, options);
    });
    vi.spyOn(h.space, "read").mockImplementation((refs, options) => {
      if (refs.some(same)) accesses.push(options.once);
      return read(refs, options);
    });
    try {
      expect(await scanAll(table, h.space, countingBudget(Infinity), true)).toBe(ROWS);
    } finally {
      vi.restoreAllMocks();
    }
    expect(accesses.length, "the scan read the published root").toBeGreaterThan(0);
    expect(accesses, "every access to it is one-pass").toEqual(accesses.map(() => true));
  }));

it("a scan's optional root probe raises no mandatory requirement", () =>
  published(async ({ h, table }) => {
    const uncached: SpaceReader = {
      peek: (ref, options) => h.space.peek(ref, options),
      read: (refs, options) => h.space.read(refs, options),
    };
    const sized = async (pages: SpaceReader) => {
      const budget = new Sizing();
      expect(await scanAll(table, pages, budget, false)).toBe(ROWS);
      expect(budget.allocated).toBe(0);
      return budget.required;
    };
    const baseline = await sized(uncached);
    // Cold (admits the owner's root view), then warm (borrows it).
    for (const round of ["cold", "warm"])
      expect(
        await sized(h.space),
        `a ${round} cached scan requires no more than the uncached one`,
      ).toBeLessThanOrEqual(baseline);
  }));
