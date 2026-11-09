/** access/modify.ts: oversized caller batches use bounded engine windows in one statement. */
import { expect, it, vi } from "vitest";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { Memtable } from "./memtable.js";
import { MEMORY_CHUNK_ROWS } from "./columns.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  TEXT,
  ints,
  texts,
  tableInput,
  rowsOf,
} from "./test-support.js";

for (const backend of ["memory", "indexeddb"] as const)
  it(`bounds copies of a 50k-row input over ${backend}`, async () => {
    const h = new Harness("strict", "memory");
    if (backend === "indexeddb")
      Object.assign(h, {
        backend: createIndexedDbBackend({
          database: () => simIndexedDb(h.disk),
          name: "input-window",
          locks: new SimLocks(),
          memory: h.governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
        }),
      });
    let path = await h.open();
    let table = 0;
    await autocommit(path, async (s) => {
      table =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false), column("v", TEXT)]),
          })
        ).id ?? 0;
    });
    const count = 50000,
      text = "x".repeat(1300);
    const input = batch(
      ints(Array.from({ length: count }, (_, k) => k)),
      texts(Array.from({ length: count }, () => text)),
    );
    // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked below with explicit this
    const append = Memtable.prototype.append;
    let calls = 0;
    let copying = false;
    const oversized: number[] = [];
    const bytes = globalThis.Uint8Array;
    vi.stubGlobal(
      "Uint8Array",
      new Proxy(bytes, {
        construct(target, args) {
          const result = Reflect.construct(target, args) as Uint8Array;
          // A constructor over an existing ArrayBuffer is a view, not a copied allocation.
          if (copying && !(args[0] instanceof ArrayBuffer) && result.byteLength >= 8 * 1024 * 1024)
            oversized.push(result.byteLength);
          return result;
        },
      }),
    );
    // Slice allocates through the intrinsic species constructor, bypassing the global proxy.
    // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with explicit this
    const slice = bytes.prototype.slice;
    const sliced = vi.spyOn(bytes.prototype, "slice").mockImplementation(function (
      this: Uint8Array,
      ...args
    ) {
      const result = slice.apply(this, args);
      if (copying && result.byteLength >= 8 * 1024 * 1024) oversized.push(result.byteLength);
      return result;
    });
    const spy = vi.spyOn(Memtable.prototype, "append").mockImplementation(function (
      this: Memtable,
      columns,
      rows,
      from,
      length,
    ) {
      // Observe the actual ownership boundary, not a proposed batch-size setting.
      expect(columns[0]).toBe(input.columns[0]);
      expect(columns[1]).toBe(input.columns[1]);
      expect(length).toBeLessThanOrEqual(MEMORY_CHUNK_ROWS);
      expect(length * (4 + 4 + text.length)).toBeLessThan(8 * 1024 * 1024);
      calls++;
      copying = true;
      try {
        const result = append.call(this, columns, rows, from, length);
        expect(oversized, "actual window copies stay below 8 MiB").toEqual([]);
        return result;
      } finally {
        copying = false;
      }
    });
    try {
      await autocommit(path, (s) => s.table(table).insert(input, null));
      expect(calls).toBe(Math.ceil(count / MEMORY_CHUNK_ROWS));
      expect(oversized, "actual copy-window constructors stay below 8 MiB").toEqual([]);
      spy.mockRestore();
      await h.reopen();
      path = h.path;
      const view = path.pin();
      try {
        const rows = await rowsOf(view, table);
        expect(rows.length).toBe(count);
        expect(rows.every((row, k) => row[0] === k && row[1] === text)).toBe(true);
      } finally {
        view.release();
      }
    } finally {
      spy.mockRestore();
      sliced.mockRestore();
      vi.unstubAllGlobals();
      await h.close();
    }
    expect(h.governor.usedBytes).toBe(0);
  }, 120000);
