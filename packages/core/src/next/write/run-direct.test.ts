/* eslint-disable no-restricted-imports -- Node-only test compares complete native byte arrays without per-byte assertion objects. */
import { Buffer } from "node:buffer";
/** Stored-form law: the bulk writer emits exactly the ordinary RunEncoder's native bytes. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_DIRECT_RUN_STATES } from "../../testing/reach.js";
import { KeyDelta, type DeltaVisitor, type SingleEntries } from "./key-hash.js";
import { RunBuilder, type EncodedRun } from "./runs.js";
import * as copying from "./bytes.js";
import {
  Harness,
  autocommit,
  batch,
  ints,
  column,
  INT,
  tableInput,
  rules,
  rowsOf,
} from "./test-support.js";

// Optional methods let the same work-law test replay on the pre-optimization writer.
interface Builder {
  addRange: RunBuilder["addRange"];
  finish(): EncodedRun | null;
  close?(): void;
  addUniqueRange?: RunBuilder["addUniqueRange"];
  addSingleBatch?: (
    data: SingleEntries,
    from: number,
    to: number,
    bound: number,
    keep: ((row: number, tombstone: boolean) => boolean) | null,
  ) => number;
}
async function encode(
  delta: KeyDelta,
  bulk: boolean,
  drop = false,
  bound = 10,
  keep: ((row: number, tombstone: boolean) => boolean) | null = null,
  options: { releaseOwner?: boolean; fail?: Error; drained?: () => void } = {},
) {
  const builder: Builder = new RunBuilder(drop, bulk);
  const release = delta.retain();
  const stats = { rows: 0, batches: 0, implicit: 0, permuted: 0 };
  let ownerReleased = false;
  const after = () => {
    if (options.releaseOwner && !ownerReleased) {
      delta.releaseCharge();
      ownerReleased = true;
    }
    if (options.fail !== undefined) throw options.fail;
  };
  try {
    const visit: DeltaVisitor = (arena, start, end, row, tombstone, hash, bloomHash, single) => {
      stats.rows++;
      if (bulk && single && !tombstone && builder.addUniqueRange !== undefined)
        builder.addUniqueRange(arena, start, end, row, hash, bloomHash);
      else builder.addRange(arena, start, end, row, tombstone, hash, bloomHash);
      after();
    };
    if (bulk && builder.addSingleBatch !== undefined)
      visit.singles = (data, from, to, maximum, predicate) => {
        stats.batches++;
        if (data.order === null) stats.implicit++;
        else stats.permuted++;
        const end = builder.addSingleBatch?.(data, from, to, maximum, predicate) ?? from;
        after();
        return end;
      };
    await delta.forEachSorted(bound, visit, keep);
    options.drained?.();
    return { encoded: builder.finish(), stats };
  } finally {
    builder.close?.();
    release();
  }
}
function sameRun(actual: EncodedRun | null, expected: EncodedRun | null): void {
  if (actual === null || expected === null) {
    expect(actual).toBe(expected);
    return;
  }
  expect(actual.entryCount).toBe(expected.entryCount);
  expect(actual.minKey).toEqual(expected.minKey);
  expect(actual.maxKey).toEqual(expected.maxKey);
  expect(actual.parts.length).toBe(expected.parts.length);
  for (let i = 0; i < actual.parts.length; i++) {
    const a = actual.parts[i] ?? new Uint8Array(0),
      b = expected.parts[i] ?? new Uint8Array(0);
    expect(a.length).toBe(b.length);
    expect(
      Buffer.from(a.buffer, a.byteOffset, a.byteLength).equals(
        Buffer.from(b.buffer, b.byteOffset, b.byteLength),
      ),
      `complete native part ${String(i)}`,
    ).toBe(true);
  }
}

function key(index: number, length = 5): Uint8Array {
  const out = new Uint8Array(length).fill(0x81);
  if (length > 0) out[0] = (index >>> 24) & 255;
  if (length > 1) out[1] = (index >>> 16) & 255;
  if (length > 2) out[2] = (index >>> 8) & 255;
  if (length > 3) out[3] = index & 255;
  return out;
}
for (const seed of seedsFor("next-write-direct-run", [0xb5d1]))
  it(`keeps native bytes through bulk, mixed and bounded histories (${String(seed)})`, async () => {
    const reach = createReach("direct run bytes", WRITE_DIRECT_RUN_STATES);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(
          "single",
          "unordered",
          "chains",
          "filter",
          "partition",
          "long",
          "empty key",
          "mixed widths",
        ),
        fc.integer({ min: 8, max: 64 }),
        fc.constantFrom(1, 5, 127, 128, 129),
        fc.constantFrom(0, 2 ** 31, 2 ** 32, 2 ** 48 - 40000),
        fc.boolean(),
        async (shape, size, width, base, drop) => {
          reach.beginHistory();
          reach.reach(shape);
          const count =
            shape === "partition"
              ? 8192
              : shape === "empty key"
                ? 1
                : shape === "mixed widths"
                  ? 512
                  : size;
          const length =
            shape === "partition"
              ? 129
              : shape === "long"
                ? 2048
                : shape === "empty key"
                  ? 0
                  : width;
          const delta = new KeyDelta();
          try {
            for (let i = 0; i < count; i++) {
              const bytes = key(
                shape === "unordered" ? count - i : i,
                shape === "mixed widths" ? ([5, 127, 128, 129, 2048][i % 5] ?? 5) : length,
              );
              const row = base + i * 4;
              delta.add(bytes, 0, bytes.length, row, 1, false);
              if (shape === "chains" && i % 13 === 7) {
                delta.add(bytes, 0, bytes.length, row, 2, true);
                delta.add(bytes, 0, bytes.length, row + 1, 3, false);
              }
            }
            const keep = shape === "filter" ? (row: number) => row % 8 === 0 : null;
            const expected = await encode(delta, false, drop, 3, keep);
            const actual = await encode(delta, true, drop, 3, keep);
            sameRun(actual.encoded, expected.encoded);
            if (base >= 2 ** 31) reach.reach("wide row id");
            if (length >= 128) reach.reach("two-byte length");
            if (actual.stats.batches > 0) reach.reach("batch callback");
            if (actual.stats.implicit > 0) reach.reach("implicit order batch");
            if (actual.stats.permuted > 0) reach.reach("permuted order batch");
            if (shape === "unordered" && length >= 5)
              expect(actual.stats.permuted).toBeGreaterThan(0);
            if (actual.stats.rows > 0) reach.reach("per-row fallback");
            if (shape === "chains" && length >= 5) reach.reach("partial fallback");
            if (shape === "filter" && length >= 5) {
              expect(actual.stats.rows).toBe(0);
              expect(actual.stats.batches).toBeGreaterThan(0);
              reach.reach("filtered batch");
            }
          } finally {
            delta.releaseCharge();
          }
        },
      ),
      { seed, numRuns: 300 },
    );
    reach.assertReached();
  }, 120_000);

it.each([false, true])(
  "replaces per-row calls and scratch key copies with bounded final-block writes (predicate=%s)",
  async (predicate) => {
    const delta = new KeyDelta();
    for (let i = 0; i < 2048; i++) {
      const bytes = key(i);
      delta.add(bytes, 0, bytes.length, i + 1, 1, false);
    }
    const keep = predicate
      ? (row: number, tombstone: boolean) => !tombstone && row >= 1 && row <= 2048
      : null;
    const expected = await encode(delta, false, false, 10, keep);
    const copy = vi.spyOn(copying, "copyRange");
    try {
      const actual = await encode(delta, true, false, 10, keep);
      expect(actual.stats.rows).toBe(0);
      expect(actual.stats.batches).toBeGreaterThan(0);
      expect(copy.mock.calls.length).toBeLessThan(2048 * 1.1);
      expect(actual.encoded?.entryCount).toBe(2048);
      sameRun(actual.encoded, expected.encoded);
    } finally {
      copy.mockRestore();
      delta.releaseCharge();
    }
  },
);

it.each([false, true])(
  "retains charged input through finish or abort (failure=%s)",
  async (fail) => {
    let paid = 0;
    const delta = new KeyDelta({
      allocate: (n) => {
        paid += n;
        return true;
      },
      free: (n) => {
        paid -= n;
      },
    });
    for (let i = 0; i < 256; i++) {
      const bytes = key(i);
      delta.add(bytes, 0, bytes.length, i + 1, 1, false);
    }
    const expected = await encode(delta, false);
    const stopped = new Error("bulk visitor stopped");
    const result = encode(delta, true, false, 10, null, {
      releaseOwner: true,
      ...(fail ? { fail: stopped } : {}),
      drained: () => expect(paid).toBeGreaterThan(0),
    });
    if (fail) await expect(result).rejects.toBe(stopped);
    else sameRun((await result).encoded, expected.encoded);
    expect(paid).toBe(0);
    delta.releaseCharge();
    expect(paid).toBe(0);
  },
);

it("ordinary staged transactions reach the predicated bulk writer without per-row handoffs", async () => {
  const h = new Harness("strict", "memory", 8, undefined, { merges: { mode: "off" } });
  const path = await h.open();
  const bulk = vi.spyOn(RunBuilder.prototype, "addSingleBatch");
  const scalar = vi.spyOn(RunBuilder.prototype, "addRange");
  const unique = vi.spyOn(RunBuilder.prototype, "addUniqueRange");
  try {
    let table = 0;
    await autocommit(path, async (s) => {
      table =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    const tx = await path.begin();
    for (let from = 0; from < 64; from += 8) {
      const statement = tx.statement({ rules });
      await statement
        .table(table)
        .insert(batch(ints(Array.from({ length: 8 }, (_, i) => from + i))), null);
      await statement.end();
    }
    expect(path.maintenance.stages).toBeGreaterThan(0);
    await (
      await tx.commit(null)
    ).durable;
    await path.quiesce();
    expect(bulk.mock.calls.some((args) => typeof args[4] === "function")).toBe(true);
    expect(scalar).not.toHaveBeenCalled();
    expect(unique).not.toHaveBeenCalled();
    const view = path.pin();
    try {
      expect(await rowsOf(view, table)).toEqual(Array.from({ length: 64 }, (_, i) => [i]));
    } finally {
      view.release();
    }
  } finally {
    bulk.mockRestore();
    scalar.mockRestore();
    unique.mockRestore();
    await h.close();
  }
});
