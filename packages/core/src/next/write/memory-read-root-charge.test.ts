/**
 * A memory read through the owner's metadata caches needs no more than the same read without
 * them (review of #61, P1).
 *
 * Law: a cache may make a read cheaper, never make it need more (format/manifest-codec.ts
 * `OptionalParseBudget`: "Optional probes must not increase a sizing pass's mandatory admission
 * requirement"). The owner caches group roots as validated views (`tryDecodeGroupRootView`),
 * whose charge is the ordinary decode's `parseCharge(L)` plus the encoded copy and packed arrays.
 * A memory read holds its group root while its column reader opens and loads, so holding the
 * cached view there raised the read's mandatory minimum: a sizing pass at the uncached read's
 * minimum plus 1 KiB was refused (`required` above the uncached one).
 *
 * The baseline is the same read with the caches out of reach: its pages are a wrapper the owner's
 * caches do not resolve (as before #61). The cached read runs warm (its root view admitted by an
 * earlier read), on a wide table, so the view's overhead is well above the 1 KiB margin.
 */
import { expect, it } from "vitest";
import type { MemorySegment, MemoryReadContext, Snapshot } from "../access/snapshot.js";
import type { ByteBudget, SpaceReader } from "../access/space.js";
import { metadataCachesFor } from "../format/metadata-reader.js";
import { Governor } from "../memory/governor.js";
import { boundSource } from "../page/read-bounds.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  tableInput,
  texts,
} from "./test-support.js";

const COLUMNS = 200;
const ROWS = 256;

/** A sizing pass's budget, as page/read-bounds.ts `SizingBudget`: optional room is not required. */
class Sizing implements ByteBudget {
  allocated = 0;
  required = 0;
  constructor(readonly capacity = Number.MAX_SAFE_INTEGER) {}
  allocate(bytes: number): boolean {
    const needed = this.allocated + bytes;
    this.required = Math.max(this.required, needed);
    if (needed > this.capacity) return false;
    this.allocated = needed;
    return true;
  }
  optionalAllocate(bytes: number): boolean {
    if (this.allocated + bytes > this.capacity) return false;
    this.allocated += bytes;
    return true;
  }
  free(bytes: number): void {
    this.allocated -= bytes;
  }
}

async function publishedWideTable(
  h: Harness,
): Promise<{ id: number; columns: number[]; snapshot: Snapshot }> {
  let id = 0;
  await autocommit(h.path, async (s) => {
    id =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("wide", [
            column("id", INT, false),
            ...Array.from({ length: COLUMNS }, (_, c) => column(`c${String(c)}`, { kind: "text" })),
          ]),
        })
      ).id ?? 0;
  });
  const rows = (from: number, count: number) =>
    batch(
      ints(Array.from({ length: count }, (_, k) => from + k)),
      ...Array.from({ length: COLUMNS }, (_, c) =>
        texts(Array.from({ length: count }, (_, k) => `v${String(c)}:${String(from + k)}`)),
      ),
    );
  await autocommit(h.path, (s) => s.table(id).insert(rows(0, ROWS), null));
  // Pinned while the memtable is resident: the snapshot keeps reading it as a memory segment,
  // re-pointed to its published group (write/memtable.ts "Lifecycle").
  const snapshot = h.path.pin();
  // A further row freezes the full memtable; quiesce publishes it.
  await autocommit(h.path, (s) => s.table(id).insert(rows(ROWS, 1), null));
  await h.path.quiesce();
  const table = h.path.catalog.table(id);
  if (table === undefined) throw new Error("missing table");
  return { id, columns: table.columns.map((c) => c.id), snapshot };
}

function memorySegment(snapshot: Snapshot, id: number): MemorySegment {
  const table = snapshot.table(id, { budget: new Sizing() });
  if (table instanceof Promise) throw new Error("resident table fixture unexpectedly asynchronous");
  const segment = table.segments.find((s) => s.kind === "memory");
  if (segment?.kind !== "memory") throw new Error("missing memory segment");
  return segment;
}

it("sizes a cached memory read at the uncached read's minimum", async () => {
  const h = new Harness("strict", "memory", ROWS);
  await h.open();
  let snapshot: Snapshot | undefined;
  try {
    const published = await publishedWideTable(h);
    const { id, columns } = published;
    snapshot = published.snapshot;
    const segment = memorySegment(snapshot, id);
    expect(metadataCachesFor(h.space)?.groupRoots, "the owner caches group roots").toBeDefined();
    const uncached: SpaceReader = {
      peek: (ref, options) => h.space.peek(ref, options),
      read: (refs, options) => h.space.read(refs, options),
    };
    const signal = new AbortController().signal;
    // One text column of row 0, the shape of a point read.
    const request = {
      columns: [columns[1] ?? -1],
      chunk: 0,
      startRow: 0,
      maxRows: 1,
      targetBytes: 4096,
    };
    const size = async (pages: SpaceReader, budget: Sizing) => {
      const context: MemoryReadContext = { budget, pages, signal };
      const hint = await segment.readHint(request, context);
      expect(hint.rowCount).toBe(1);
      expect(budget.allocated, "a hint holds nothing after it returns").toBe(0);
      return budget.required;
    };
    expect(segment.rowCount).toBe(ROWS);
    expect(segment.deletions(), "the fixture reads a whole published memtable").toBeNull();
    const baseline = await size(uncached, new Sizing());
    // Cold, then warm (the owner's root view admitted): directory-cache hits may make the read
    // need less; nothing may make it need more.
    for (const round of ["cold", "warm"])
      expect(
        await size(h.space, new Sizing()),
        `a ${round} cached read requires no more than the uncached read`,
      ).toBeLessThanOrEqual(baseline);
    expect(
      await size(h.space, new Sizing(baseline + 1024)),
      "the uncached minimum plus 1 KiB admits the cached read",
    ).toBeLessThanOrEqual(baseline);

    // The executor's sizing of the same one-row read (page/read-bounds.ts `boundSource`).
    const source = {
      id: 1,
      tableId: id,
      selectedRow: true as const,
      columns: [columns[1] ?? -1],
      types: [{ kind: "text" as const }],
      windowBytes: 0,
      stagingBytes: 4096,
      rows: 1,
    };
    const view = (pages: SpaceReader) => ({
      pages,
      table: (tableId: number, options: Parameters<Snapshot["table"]>[1]) =>
        snapshot?.table(tableId, options) ?? Promise.reject(new Error("released")),
    });
    const bound = async (pages: SpaceReader, cap: number) =>
      (await boundSource(view(pages), source, new Governor(cap), signal, 4096)).bytes;
    const plain = await bound(uncached, 1 << 30);
    expect(await bound(h.space, 1 << 30), "cached sizing needs no more").toBeLessThanOrEqual(plain);
    expect(await bound(h.space, plain + 1024), "and fits the uncached bound plus 1 KiB").toBe(
      await bound(uncached, plain + 1024),
    );
  } finally {
    snapshot?.release();
    await h.close();
  }
}, 60_000);
