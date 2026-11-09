/** --expose-gc witness: an OPEN cursor cannot retain an evicted navigation index or its bytes. */
import { strict as assert } from "node:assert";
import { setImmediate as nextTask } from "node:timers/promises";
import { Harness } from "../packages/core/dist/next/write/test-support.js";
import { RunBuilder, runRef } from "../packages/core/dist/next/write/runs.js";
import { StreamingRangeCursor } from "../packages/core/dist/next/write/range-cursor.js";
import { DirectoryNavigation } from "../packages/core/dist/next/write/range-navigation.js";
import type { ViewIndex } from "../packages/core/dist/next/write/view.js";
import type { BlobRef } from "../packages/core/dist/next/format/manifest.js";
import type { KeyRange } from "../packages/core/dist/next/access/key-index.js";
export interface NavigationLifetimeCase {
  rows: number;
  mode: "desc" | "bounded asc" | "bounded desc";
}
function witnesses(h: Harness, refs: readonly BlobRef[]) {
  return refs.flatMap((ref) => {
    const navigation = h.space.parsed?.(ref);
    return navigation instanceof DirectoryNavigation
      ? [navigation.positions.buffer, navigation.source.buffer].map((b) => ({
          ref: new WeakRef(b),
          bytes: b.byteLength,
        }))
      : [];
  });
}
async function one(s: NavigationLifetimeCase) {
  const h = new Harness("strict", "memory");
  await h.open();
  const writer = h.space.beginWrite("navigation GC witness"),
    builder = new RunBuilder();
  const key = (n: number) => {
    const b = new Uint8Array(64);
    new DataView(b.buffer).setUint32(0, n, false);
    return b;
  };
  for (let i = 0; i < s.rows; i++) builder.add(key(i), i + 1, false);
  const encoded = builder.finish();
  if (encoded === null) throw new Error("missing run");
  const ref = runRef(encoded, (await writer.write(encoded.parts)).blobs),
    start = s.rows >>> 1;
  const range: KeyRange =
    s.mode === "desc"
      ? { lower: null, upper: null }
      : {
          lower: { kind: "atLeast", key: key(start) },
          upper: { kind: "through", key: key(start + 255) },
        };
  const index = {
    def: { id: 1, unique: true },
    table: {
      snapshot: {
        check: () => undefined,
        watchCursor: () => () => undefined,
        bound: 1,
        shared: { space: h.space, governor: h.governor },
      },
      tableView: { memtables: [], runs: new Map([[1, [ref]]]) },
      locator: (row: number) => row,
    },
  } as unknown as ViewIndex;
  const direction = s.mode === "bounded asc" ? "asc" : "desc",
    cursor = new StreamingRangeCursor(index, range, direction),
    out = new Float64Array(1);
  try {
    assert.equal(await cursor.next(out), 1);
    const expected = s.mode === "desc" ? s.rows : direction === "asc" ? start + 1 : start + 256;
    assert.equal(out[0], expected);
    const weak = witnesses(h, [ref.entries, ref.fences]);
    assert.ok(weak.length >= 2);
    assert.equal(h.space.stats.cache.pinnedBytes, 0);
    const evictable = h.space.stats.cache.residentBytes + h.space.stats.cache.poolBytes;
    const pressure = h.governor.tryAdmit({
      label: "evict while cursor stays OPEN",
      minimumBytes:
        h.governor.capBytes - h.governor.reserveBytes - h.governor.usedBytes + evictable,
      spillable: false,
    });
    assert.ok(pressure);
    pressure.close();
    assert.equal(h.space.parsed?.(ref.entries), null);
    assert.equal(h.space.parsed?.(ref.fences), null);
    // WeakRef creation retains its target until the current job ends; never dereference mid-GC.
    await nextTask();
    for (let n = 0; n < 3; n++) {
      globalThis.gc?.();
      await nextTask();
    }
    const retained = weak
      .filter((w) => w.ref.deref() !== undefined)
      .reduce((sum, w) => sum + w.bytes, 0);
    assert.equal(retained, 0, "open cursor retained evicted navigation payload");
    // Cross several block boundaries after eviction, preserving order and the live cursor.
    for (let n = 1; n <= 128; n++) {
      assert.equal(await cursor.next(out), 1);
      assert.equal(out[0], expected + (direction === "asc" ? n : -n));
    }
    return { ...s, witnesses: weak.length, retainedBytes: retained };
  } finally {
    cursor.close();
    await writer.abort();
    await h.close();
  }
}
if (globalThis.gc === undefined) throw new Error("Run with --expose-gc");
const plan = JSON.parse(process.argv[2] ?? "[]") as NavigationLifetimeCase[];
const result = [];
for (const s of plan) result.push(await one(s));
console.log(JSON.stringify(result));
