/**
 * Review B2-p2f (rebuild/format-p2f 8988105). #1: `translationMap` reserved its map before the
 * interner ran and recorded it only after every callback succeeded, so a throwing interner leaked
 * the reservation past `close`. Now the reservation is provisional: any exception gives it back,
 * and the map is recorded only on success. The reviewer's other probes are kept as tests.
 */
import { describe, expect, it } from "vitest";
import type { PhysicalType } from "../types.js";
import { ChunkReader } from "./reader.js";
import { defined, targetFor, UNLIMITED, valueAt, writeGroup } from "./test-support.js";

const encoder = new TextEncoder();
const values = Array.from({ length: 4000 }, (_, k) =>
  k % 13 === 0 ? null : encoder.encode(`v${String(k % 7)}`),
);

describe("review B2-p2f #1: a failed translation map gives its reservation back", () => {
  it("throws at every callback position, retries, closes: nothing outstanding (the probe, inverted)", async () => {
    const { space, group } = await writeGroup([{ type: { kind: "text" }, values }]);
    let held = 0;
    const budget = {
      allocate: (bytes: number) => ((held += bytes), true),
      free: (bytes: number) => void (held -= bytes),
    };
    const reader = await ChunkReader.open(space, defined(group.columns[0]), { budget });
    await reader.loadDictionary();
    const entries = defined(reader.directory.dictionary ?? undefined).rowCount;
    expect(entries).toBe(7);
    const before = held;
    for (let position = 1; position <= entries; position++) {
      let calls = 0;
      const failing = (): number => {
        if (++calls === position) throw new Error(`interner fails at entry ${String(position)}`);
        return calls;
      };
      expect(() => reader.translationMap(failing)).toThrow(
        `interner fails at entry ${String(position)}`,
      );
      expect(held, `after a failure at entry ${String(position)}`).toBe(before);
      // Nothing was recorded for it: the next call builds afresh (and fails the same way).
      calls = 0;
      expect(() => reader.translationMap(failing)).toThrow();
      expect(held).toBe(before);
    }
    // A retry with a working interner builds, charges and records the map once.
    let calls = 0;
    const working = (): number => ++calls;
    const map = defined(reader.translationMap(working) ?? undefined);
    expect(held - before).toBe(entries * 4);
    expect(reader.translationMap(working)).toBe(map);
    expect(calls).toBe(entries);
    reader.close();
    expect(held).toBe(0);
  });
});

describe("review B2-p2f: what the reviewer found sound stays so", () => {
  it("a cached dictionary serves readers of other types; maps are per reader and interner (the probe)", async () => {
    const { space, group } = await writeGroup([{ type: { kind: "text" }, values }]);
    const chunk = defined(group.columns[0]);
    const a = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
    const av = await a.loadDictionary();
    const hits = space.parsedHits;
    const b = await ChunkReader.open(
      space,
      { ...chunk, type: { kind: "bytes" } },
      { budget: UNLIMITED },
    );
    const bv = await b.loadDictionary();
    expect(space.parsedHits).toBe(hits + 1);
    expect(av?.type.kind).toBe("text");
    expect(bv?.type.kind).toBe("bytes");
    expect(bv).not.toBe(av);
    expect(bv?.data).toEqual(av?.data);
    const f = (): number => 3;
    const g = (): number => 5;
    expect([...defined(a.translationMap(f) ?? undefined)]).toEqual(Array<number>(7).fill(3));
    expect([...defined(a.translationMap(g) ?? undefined)]).toEqual(Array<number>(7).fill(5));
    expect(b.translationMap(f)).not.toBe(a.translationMap(f));
    a.close();
    b.close();
  });

  it("partial consecutive batches across different page boundaries, with an out-of-line value (the probe)", async () => {
    const n = 6000;
    const columns: Array<{ type: PhysicalType; values: Array<number | Uint8Array | null> }> = [
      {
        type: { kind: "int32", bits: 32 },
        values: Array.from({ length: n }, (_, k) => (k % 17 === 0 ? null : k)),
      },
      {
        type: { kind: "text" },
        values: Array.from({ length: n }, (_, k) =>
          k % 19 === 0 ? null : encoder.encode(`word-${String(k % 11)}`),
        ),
      },
      {
        type: { kind: "bytes" },
        values: Array.from({ length: n }, (_, k) =>
          k % 23 === 0 ? null : new Uint8Array(k === 1700 ? 70_000 : 20 + (k % 31)).fill(k % 251),
        ),
      },
    ];
    const { space, group } = await writeGroup(columns, { pageBytes: 1024, rawPageBytes: 1024 });
    const readers = await Promise.all(
      group.columns.map(async (chunk) => ChunkReader.open(space, chunk, { budget: UNLIMITED })),
    );
    expect(
      new Set(readers.map((reader) => reader.directory.pages[0]?.rowCount)).size,
    ).toBeGreaterThan(1);
    for (let from = 0; from < n; from += 777) {
      const to = Math.min(n, from + 777);
      for (const [column, reader] of readers.entries()) {
        const [first, end] = reader.pagesInRange(from, to);
        await reader.load(Array.from({ length: end - first }, (_, k) => first + k));
        for (let page = end; page < reader.directory.pages.length; page++)
          expect(reader.isLoaded(page)).toBe(false);
        await reader.loadValues(from, to);
        const target = targetFor(reader, to - from, reader.varlenBytes(from, to));
        reader.decodeRange(from, to, target, 0);
        for (let k = from; k < to; k++)
          expect(valueAt(target, k - from)).toEqual(columns[column]?.values[k]);
        reader.release(Array.from({ length: Math.max(0, first) }, (_, page) => page));
      }
    }
    for (const reader of readers) reader.close();
  }, 60_000);

  it("pruning and selection never need the excluded pages loaded (the probe)", async () => {
    const ints = Array.from({ length: 5000 }, (_, k) => (k % 17 === 0 ? null : k));
    const { space, group } = await writeGroup(
      [{ type: { kind: "int32", bits: 32 }, values: ints }],
      { pageBytes: 256, rawPageBytes: 256 },
    );
    const reader = await ChunkReader.open(space, defined(group.columns[0]), { budget: UNLIMITED });
    const compiled = defined(
      reader.compilePredicate({ op: "between", low: 1000, high: 1100 }) ?? undefined,
    );
    const wanted = reader.pagesMatching(compiled);
    expect(wanted.length).toBeLessThan(reader.directory.pages.length);
    await reader.load(wanted);
    const out = new Uint16Array(5000);
    expect([...out.subarray(0, reader.select(compiled, 0, 5000, out))]).toEqual(
      ints.flatMap((value, k) => (value !== null && value >= 1000 && value <= 1100 ? [k] : [])),
    );
    for (let page = 0; page < reader.directory.pages.length; page++)
      expect(reader.isLoaded(page)).toBe(wanted.includes(page));
    reader.close();
  });
});
