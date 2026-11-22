/**
 * A group's deletion bitmap is read by two kinds of reader, and each must pay for it from memory
 * it was admitted, so that pressure never becomes a failure:
 * - A flush folding deletions reads the old bitmap through the metadata budget: a refusal is
 *   metadata pressure, which the flush unwinds and retries (#metadataAsync). It used to charge the
 *   write path's rows grant, and a refusal there (ReadDeniedError) failed the whole write path.
 * - A run reader (opened by a merge, a flush or a lookup) reads through the metadata budget too.
 * - A scan reads it through its own budget, and the scan's read bound (page/read-bounds.ts)
 *   includes those bytes. It used to read through the view's budget, which for a prepared
 *   source does not allocate, so a cold bitmap read failed with an untyped ReadDeniedError.
 */
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { boundSource } from "../page/read-bounds.js";
import { MetadataBudget, MetadataPressure } from "../write/budget.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

/** A table whose first group has a stored deletion bitmap, then a cold reopen. */
async function tableWithBitmap(core: StorageCore): Promise<void> {
  await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)");
  const insert = async (from: number, to: number) => {
    const rows = Array.from({ length: to - from }, (_, k) => `(${String(from + k)}, ${String(k)})`);
    await core.execute(`INSERT INTO t VALUES ${rows.join(", ")}`);
  };
  await insert(0, 64);
  await core.harness.path.quiesce();
  // The first deletion's flush writes the group's bitmap; the second's reads it back.
  await core.execute("DELETE FROM t WHERE id = 3");
  await insert(64, 128);
  await core.harness.path.quiesce();
  await core.execute("DELETE FROM t WHERE id = 5");
  await insert(128, 192);
}

it("retries a flush whose deletion-bitmap read is refused, never failing the write path", async () => {
  const core = new StorageCore(64, { merges: { mode: "manual" } });
  // The flush reads the stored bitmap while the write path's rows grant is exhausted (as under
  // a small cap with wide rows): a read charged to that grant is refused every time. A read
  // charged to the metadata budget meets metadata pressure once, which the flush must retry.
  let refused = 0,
    metadataPressure = 0;
  core.harness.onSpace = (space) => {
    // The read must happen: a cache hit (`peek`) would skip it, and whether the open left the
    // bitmap resident varies by machine. The targeted read always goes to storage.
    const peek = space.peek.bind(space);
    space.peek = (ref, options) => {
      const stack = new Error().stack ?? "";
      return stack.includes("#bitmap") && stack.includes("#writeFlush") ? null : peek(ref, options);
    };
    const read = space.read.bind(space);
    space.read = (refs, options) => {
      const stack = new Error().stack ?? "";
      if (!stack.includes("#bitmap") || !stack.includes("#writeFlush")) return read(refs, options);
      const budget = options.budget;
      const metadata = budget instanceof MetadataBudget;
      if (metadata && metadataPressure > 0) return read(refs, options);
      return read(refs, {
        ...options,
        budget: {
          allocate: (bytes: number) => {
            refused++;
            if (metadata) {
              metadataPressure++;
              throw new MetadataPressure(bytes, bytes);
            }
            return false;
          },
          free: (bytes: number) => {
            budget.free(bytes);
          },
        },
      });
    };
  };
  await core.open();
  try {
    await tableWithBitmap(core);
    await core.harness.reopen();
    // The flush of the pending deletion reads the stored bitmap, cold, under that pressure.
    await core.execute("INSERT INTO t VALUES (1000, 0)");
    await core.harness.path.quiesce();
    expect(refused).toBeGreaterThan(0);
    expect(metadataPressure).toBe(1);
    expect(core.harness.path.failure).toBeNull();
    const rows = await core.execute("SELECT count(*) AS n FROM t");
    if (!("batches" in rows)) throw new Error("rows");
    expect(oracleRows(rows).map((row) => Number(row.n))).toEqual([192 + 1 - 2]);
  } finally {
    await core.close();
  }
});

it("bounds a scan's bitmap read and reads it through the scan's own budget", async () => {
  const core = new StorageCore(64, { merges: { mode: "manual" } });
  await core.open();
  try {
    await tableWithBitmap(core);
    await core.harness.path.quiesce();
    await core.harness.reopen();
    // A cold scan: the bitmap is read from storage through the scan's budget.
    const rows = await core.execute("SELECT id FROM t WHERE v >= 0 ORDER BY id");
    if (!("batches" in rows)) throw new Error("rows");
    const ids = oracleRows(rows).map((row) => Number(row.id));
    expect(ids.length).toBe(192 - 2);
    expect(ids.includes(3) || ids.includes(5)).toBe(false);
    // The bound includes the bitmap's stored bytes, derived from the stored ref, not from the
    // engine's estimate: the same view with its bitmaps hidden bounds that much less.
    const snapshot = core.harness.path.pin();
    try {
      const table = core.harness.path.catalog.tableByName("t");
      if (table === undefined) throw new Error("table");
      const pinned = snapshot.table(table.id, {
        budget: { allocate: () => false, free: () => undefined },
      });
      const index = pinned.segments.findIndex(
        (segment) => segment.kind === "group" && segment.group.deletions !== null,
      );
      const stored = pinned.segments[index];
      if (stored?.kind !== "group" || stored.group.deletions === null)
        throw new Error("no stored bitmap");
      const bitmapBytes = stored.group.deletions.length;
      // Only that group is bounded, so the bound differs by exactly its bitmap.
      const source = {
        id: 1,
        tableId: table.id,
        columns: table.columns.map((column) => column.id),
        types: table.columns.map((column) => column.type),
        windowBytes: 0,
        stagingBytes: 0,
        rows: null,
        segments: Uint32Array.of(index),
      };
      const governor = new Governor(256 << 20),
        signal = new AbortController().signal;
      const withBitmap = await boundSource(snapshot, source, governor, signal, 1 << 20);
      // The same view with the bitmap hidden: proxies keep every method and private field.
      const hideBitmap = <T extends object>(segment: T): T =>
        new Proxy(segment, {
          get(target, key) {
            const value: unknown = Reflect.get(target, key, target);
            if (key === "group") return { ...(value as object), deletions: null };
            return typeof value === "function" ? (value as () => unknown).bind(target) : value;
          },
        });
      const hidden = new Proxy(snapshot, {
        get(target, key) {
          if (key === "table")
            return (id: number, options: Parameters<typeof snapshot.table>[1]) => {
              const real = target.table(id, options);
              return new Proxy(real, {
                get(t, k) {
                  if (k === "segments") return t.segments.map(hideBitmap);
                  const value: unknown = Reflect.get(t, k, t);
                  return typeof value === "function" ? (value as () => unknown).bind(t) : value;
                },
              });
            };
          const value: unknown = Reflect.get(target, key, target);
          return typeof value === "function" ? (value as () => unknown).bind(target) : value;
        },
      });
      const without = await boundSource(hidden, source, governor, signal, 1 << 20);
      expect(withBitmap.bytes - without.bytes).toBe(bitmapBytes);
      expect(withBitmap.io).toBeGreaterThanOrEqual(bitmapBytes);
    } finally {
      snapshot.release();
    }
  } finally {
    await core.close();
  }
});

it("completes a merge whose run-reader read is refused: it waits out the pressure", async () => {
  const core = new StorageCore(16, { merges: { mode: "manual", fanIn: 2 } });
  // While the merge opens its input runs, the rows grant is exhausted (every read charged to it
  // is refused) and the metadata budget refuses once. Charged to the rows grant, the refusal
  // used to abandon the merge (no failure, but no merge either); charged to the metadata budget,
  // it is waited out and the merge completes.
  let refused = 0,
    metadataPressure = 0,
    armed = false;
  core.harness.onSpace = (space) => {
    // The reads must happen: a cache hit (`peek`) would skip them, and whether the open left
    // the runs resident varies by machine. The targeted reads always go to storage.
    const peek = space.peek.bind(space);
    space.peek = (ref, options) => {
      const stack = new Error().stack ?? "";
      return armed && stack.includes("RunReader") && stack.includes("#mergePinned")
        ? null
        : peek(ref, options);
    };
    const read = space.read.bind(space);
    space.read = (refs, options) => {
      const stack = new Error().stack ?? "";
      if (!armed || !stack.includes("RunReader") || !stack.includes("#mergePinned"))
        return read(refs, options);
      const budget = options.budget;
      const metadata = budget instanceof MetadataBudget;
      if (metadata && metadataPressure > 0) return read(refs, options);
      return read(refs, {
        ...options,
        budget: {
          allocate: (bytes: number) => {
            refused++;
            if (metadata) {
              metadataPressure++;
              throw new MetadataPressure(bytes, bytes);
            }
            return false;
          },
          free: (bytes: number) => {
            budget.free(bytes);
          },
        },
      });
    };
  };
  await core.open();
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, n INT)");
    await core.execute("CREATE INDEX t_n ON t(n)");
    for (let batch = 0; batch < 6; batch++) {
      const rows = Array.from({ length: 16 }, (_, k) => {
        const id = batch * 16 + k;
        return `(${String(id)}, ${String(id % 7)})`;
      });
      await core.execute(`INSERT INTO t VALUES ${rows.join(", ")}`);
      await core.harness.path.quiesce();
    }
    // Cold: the merge reads its input runs from storage.
    await core.harness.reopen();
    armed = true;
    await core.harness.path.runMaintenance();
    armed = false;
    expect(core.harness.path.failure).toBeNull();
    expect(core.harness.path.maintenance.merges).toBeGreaterThan(0);
    expect(refused).toBeGreaterThan(0);
    expect(metadataPressure).toBe(1);
    const rows = await core.execute("SELECT count(*) AS n FROM t WHERE n = 3");
    if (!("batches" in rows)) throw new Error("rows");
    const expected = Array.from({ length: 96 }, (_, id) => id).filter((id) => id % 7 === 3).length;
    expect(oracleRows(rows).map((row) => Number(row.n))).toEqual([expected]);
  } finally {
    await core.close();
  }
});
