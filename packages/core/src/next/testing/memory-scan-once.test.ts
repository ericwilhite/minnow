/**
 * A non-retaining table scan admits nothing it parses to the owner's metadata caches, for its
 * memory segments as for its groups (review of #61, P3).
 *
 * Law (exec/table-scan.ts `TableScanOptions.directoryOnce`): "Non-retaining sources borrow hits
 * without admitting directory misses". A group's chunk readers open with `once: true`; a memory
 * segment re-pointed to its published group reads through `MemoryReads`, which now resolves the
 * owner's caches, so the scan must say `once` there too (access/snapshot.ts
 * `MemoryReadContext.once`). A retaining scan of the same rows admits them, which shows the
 * fixture reaches the caches at all.
 */
import { expect, it } from "vitest";
import type { Snapshot } from "../access/snapshot.js";
import { OutputArena } from "../exec/kernels/vectors.js";
import { chunkReads } from "../exec/scan.js";
import { TableScan } from "../exec/table-scan.js";
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

it.each([true, false])("a scan with directoryOnce=%s admits only when it retains", async (once) => {
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
    const caches = metadataCachesFor(h.space);
    const roots = caches?.groupRoots,
      directories = caches?.directories;
    if (roots === undefined || directories === undefined) throw new Error("owner caches");
    roots.clear();
    directories.clear();
    const table = await snapshot.table(id, { budget: countingBudget(Infinity) });
    const memory = table.segments.filter((s) => s.kind === "memory");
    expect(memory.map((s) => s.rowCount)).toEqual([ROWS]);
    const budget = countingBudget(Infinity),
      arena = new OutputArena(budget, 2048);
    const scan = new TableScan({
      table: { ...table, segments: memory, liveRowCount: ROWS },
      pages: h.space,
      reads: chunkReads,
      budget,
      columns: table.columns.map((c) => c.id),
      filter: null,
      emitLocator: false,
      boundedMemory: true,
      directories,
      directoryOnce: once,
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
    expect(read).toBe(ROWS);
    if (once) {
      expect(roots.entryCount, "a one-pass scan admits no group root").toBe(0);
      expect(directories.entryCount, "nor any column directory").toBe(0);
    } else {
      expect(roots.entryCount, "a retaining scan admits the group root").toBeGreaterThan(0);
      expect(directories.entryCount, "and its column directories").toBeGreaterThan(0);
    }
    expect(budget.used).toBe(0);
  } finally {
    snapshot?.release();
    await h.close();
  }
});
