/**
 * Review B2-p2g (rebuild/format-p2f 65e7bb4, rebuild/format-p2g 3d8daac). #1: `translationMap`
 * gave a throwing interner's reservation back, but not one whose interner re-entered it (the
 * inner map was overwritten by the outer, its 28 bytes never freed) or closed the reader (the map
 * was then published into a closed reader). Now reentry with the same interner is refused, a
 * close from the interner discards the map, and the reservation goes back whenever no map is
 * recorded. The generated property drives random callback schedules against the budget.
 *
 * #2: a one-pass reader's cache hits went through `peek`, which promoted them, so a scan pushed
 * the hot set out; it now peeks with `once` (no use, no move) for pages, parts and directories.
 * #3: a one-pass miss's private copy could be attached as a descriptor to a concurrent ordinary
 * entry of the same blob (two buffers held, one charged); now a one-pass reader attaches nothing,
 * and the space refuses a descriptor whose bytes are not the resident entry's own (the cache
 * model's foreign-attach dimension, storage/page-cache-model.test.ts). #4 is in once.test.ts.
 */
import fc from "fast-check";
import { afterAll, describe, expect, it, vi } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { BlobRef, ExtentReader, ReadOptions } from "../storage/extents.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { ENTRY_OVERHEAD } from "../storage/page-cache.js";
import { countingMemory, openOver } from "../storage/space-test-support.js";
import { ChunkReader, ReaderClosedError, TranslationReentryError } from "./reader.js";
import { defined, vectorOf, writeGroup } from "./test-support.js";
import { GroupWriter } from "./writer.js";

const encoder = new TextEncoder();
const text = { kind: "text" } as const;

async function dictionaryReader(distinct = 7) {
  const { space, group } = await writeGroup([
    {
      type: text,
      values: Array.from({ length: 4000 }, (_, k) => encoder.encode(`v${String(k % distinct)}`)),
    },
  ]);
  let held = 0;
  const budget = {
    allocate: (bytes: number) => ((held += bytes), true),
    free: (bytes: number) => void (held -= bytes),
  };
  const reader = await ChunkReader.open(space, defined(group.columns[0]), { budget });
  await reader.loadDictionary();
  return {
    reader,
    held: () => held,
    entries: defined(reader.directory.dictionary ?? undefined).rowCount,
  };
}

describe("review B2-p2g #1: translation maps survive reentrant and closing interners", () => {
  it("refuses an interner re-entering with itself; the outer map is kept once (the probe, inverted)", async () => {
    const { reader, held, entries } = await dictionaryReader();
    const before = held();
    let refused: unknown = null;
    const intern = (): number => {
      if (refused === null)
        try {
          reader.translationMap(intern);
        } catch (error) {
          refused = error;
        }
      return 1;
    };
    const map = defined(reader.translationMap(intern) ?? undefined);
    expect(refused).toBeInstanceOf(TranslationReentryError);
    expect(held() - before).toBe(entries * 4);
    expect(reader.translationMap(intern)).toBe(map);
    reader.close();
    expect(held()).toBe(0);
  });

  it("an uncaught reentry fails the outer call and gives everything back", async () => {
    const { reader, held } = await dictionaryReader();
    const before = held();
    const intern = (): number => {
      reader.translationMap(intern);
      return 1;
    };
    expect(() => reader.translationMap(intern)).toThrow(TranslationReentryError);
    expect(held()).toBe(before);
    reader.close();
    expect(held()).toBe(0);
  });

  it("a close from the interner discards the map and throws (the probe, inverted)", async () => {
    const { reader, held } = await dictionaryReader();
    expect(() =>
      reader.translationMap(() => {
        reader.close();
        return 1;
      }),
    ).toThrow(ReaderClosedError);
    expect(held()).toBe(0);
    reader.close();
    expect(held()).toBe(0);
  });

  it("an allocation failure gives the reservation back and a retry succeeds (the probe)", async () => {
    const { reader, held } = await dictionaryReader();
    const before = held();
    const original = globalThis.Int32Array;
    vi.stubGlobal(
      "Int32Array",
      new Proxy(original, {
        construct() {
          throw new RangeError("injected allocation");
        },
      }),
    );
    try {
      expect(() => reader.translationMap(() => 1)).toThrow("injected allocation");
      expect(held()).toBe(before);
    } finally {
      vi.unstubAllGlobals();
    }
    reader.translationMap(() => 1);
    reader.close();
    expect(held()).toBe(0);
  });
});

const SEEDS = seedsFor("next-translation-callbacks", [0x9e7, 0x2b]);
const reach = createReach("next-translation-callbacks", [
  "an interner re-entering with itself (refused)",
  "an interner building another interner's map",
  "an interner closing the reader",
  "an interner throwing",
  "a map built two levels deep",
]);
afterAll(() => {
  reach.assertReached();
});

/** What an interner does when it is called for entry `at` (once per build). */
type Action =
  | { readonly kind: "throw" }
  | { readonly kind: "close" }
  | { readonly kind: "self"; readonly caught: boolean }
  | { readonly kind: "other"; readonly target: number; readonly caught: boolean };

describe.each(SEEDS)("translation maps under random callback schedules (seed %i)", (seed) => {
  it("charge exactly the recorded maps while open, and nothing after close", async () => {
    const action: fc.Arbitrary<Action> = fc.oneof(
      fc.constant({ kind: "throw" as const }),
      fc.constant({ kind: "close" as const }),
      fc.record({ kind: fc.constant("self" as const), caught: fc.boolean() }),
      fc.record({
        kind: fc.constant("other" as const),
        target: fc.nat({ max: 3 }),
        caught: fc.boolean(),
      }),
    );
    const interner = fc.record({
      at: fc.nat({ max: 7 }),
      action: fc.option(action, { nil: undefined }),
    });
    const calls = fc.array(fc.nat({ max: 3 }), { minLength: 1, maxLength: 6 });
    await fc.assert(
      fc.asyncProperty(
        fc.array(interner, { minLength: 4, maxLength: 4 }),
        calls,
        async (shapes, order) => {
          const { reader, held, entries } = await dictionaryReader();
          const base = held();
          reach.beginHistory();
          let depth = 0;
          const state = { closed: false };
          /** Maps a call returned, by interner: exactly these are recorded and charged. */
          const maps = new Map<number, Int32Array>();
          const interners: Array<(bytes: Uint8Array, start: number, end: number) => number> = [];
          const call = (index: number): void => {
            depth++;
            try {
              if (depth === 2) reach.reach("a map built two levels deep");
              const map = reader.translationMap(defined(interners[index]));
              if (map !== null) maps.set(index, map);
            } finally {
              depth--;
            }
          };
          for (const [index, shape] of shapes.entries()) {
            let calls = 0;
            let acted = false;
            interners.push((_bytes, start) => {
              if (calls++ === shape.at % entries && !acted && shape.action !== undefined) {
                acted = true;
                const act = shape.action;
                switch (act.kind) {
                  case "throw":
                    reach.reach("an interner throwing");
                    throw new Error("interner failed");
                  case "close":
                    reach.reach("an interner closing the reader");
                    reader.close();
                    state.closed = true;
                    maps.clear();
                    break;
                  case "self":
                  case "other": {
                    const target = act.kind === "self" ? index : act.target;
                    reach.reach(
                      target === index
                        ? "an interner re-entering with itself (refused)"
                        : "an interner building another interner's map",
                    );
                    try {
                      call(target);
                    } catch (error) {
                      if (!act.caught) throw error;
                    }
                  }
                }
              }
              return start;
            });
          }
          for (const index of order) {
            try {
              call(index);
            } catch {
              // Throws, refusals and closes are the schedule's; the budget is the property.
            }
            if (state.closed) expect(held()).toBe(0);
            else expect(held() - base).toBe(maps.size * entries * 4);
          }
          // A recorded map comes back as is, without running its interner again.
          if (!state.closed)
            for (const [index, map] of maps)
              expect(reader.translationMap(defined(interners[index]))).toBe(map);
          if (!state.closed) expect(held() - base).toBe(maps.size * entries * 4);
          reader.close();
          expect(held()).toBe(0);
        },
      ),
      { seed, numRuns: 200 },
    );
  });
});

/** A column on the real extent space and page cache: its chunk, page refs and directory ref. */
async function onDisk(
  type: { kind: "int32"; bits: 32 } | { kind: "text" } = { kind: "int32", bits: 32 },
) {
  const memory = countingMemory(Number.POSITIVE_INFINITY);
  const { space, budget } = await openOver(new MemoryBackend(), { memory });
  const writer = new GroupWriter({
    columns: [{ columnId: 1, type }],
    writer: space.beginWrite("p2g"),
    inlineBytes: 0,
    pageBytes: 1024,
    rawPageBytes: 1024,
  });
  const values =
    type.kind === "text"
      ? Array.from({ length: 4000 }, (_, k) => encoder.encode(`v${String(k % 7)}`))
      : Array.from({ length: 10_000 }, (_, k) => (k * 7919) % 100_000);
  await writer.append([vectorOf(type, values)], values.length);
  const chunk = defined((await writer.finish()).columns[0]);
  const probe = await ChunkReader.open(space, chunk, { budget, once: true });
  const blob = (entry: { offset: number; length: number; checksum: number }): BlobRef => ({
    extent: chunk.data.extent,
    offset: chunk.data.offset + entry.offset,
    length: entry.length,
    checksum: entry.checksum,
  });
  const refs = probe.directory.pages.map(blob);
  const dictionary = probe.directory.dictionary === null ? null : blob(probe.directory.dictionary);
  probe.close();
  const cache = defined(
    (memory.cache ?? undefined) as { evict(bytes: number): number } | undefined,
  );
  return { space, budget, memory, cache, chunk, refs, dictionary };
}

describe("review B2-p2g #2: a one-pass reader's hits promote nothing", () => {
  it("a scanned probation page goes before the protected hot one (the probe, inverted)", async () => {
    const { space, budget, cache, chunk, refs } = await onDisk();
    const [scanned, hot] = [defined(refs[0]), defined(refs[1])];
    (await space.read([hot], { budget })).release();
    (await space.read([hot], { budget })).release(); // used twice: protected
    (await space.read([scanned], { budget })).release(); // used once: probation
    const reader = await ChunkReader.open(space, chunk, { budget, once: true });
    expect(reader.load([0])).toBeUndefined(); // the synchronous hit path (peek)
    reader.close();
    expect(space.parsed?.(scanned) ?? null).toBeNull(); // and no descriptor attached on the hit
    cache.evict(1);
    expect([space.peek(scanned) === null, space.peek(hot) === null]).toEqual([true, false]);
    expect(budget.used).toBe(0);
  });

  it("a cached directory blob a one-pass reader opens from stays in probation", async () => {
    const { space, budget, cache, chunk, refs } = await onDisk();
    const hot = defined(refs[1]);
    (await space.read([hot], { budget })).release();
    (await space.read([hot], { budget })).release(); // protected
    (await ChunkReader.open(space, chunk, { budget })).close(); // the directory blob: probation
    for (let use = 0; use < 2; use++)
      (await ChunkReader.open(space, chunk, { budget, once: true })).close();
    cache.evict(1); // probation first: the directory, never the hot page
    expect(space.peek(chunk.directory) === null).toBe(true);
    expect(space.peek(hot) === null).toBe(false);
  });
});

describe("review B2-p2g #3: no descriptor of a one-pass reader's private bytes", () => {
  /** Admits an ordinary copy of `target` while the one-pass read of it is in flight. */
  function racing(space: ExtentReader, target: BlobRef, budget: ReadOptions["budget"]) {
    let privateBytes: Uint8Array | undefined;
    const reader: ExtentReader = {
      peek: (ref, options) => space.peek(ref, options),
      parsed: (ref) => space.parsed?.(ref) ?? null,
      attach: (ref, parsed, bytes, of) => space.attach?.(ref, parsed, bytes, of) ?? false,
      read: async (refs, options) => {
        const lease = await space.read(refs, options);
        if (refs[0]?.offset === target.offset && options.once === true) {
          privateBytes = lease.bytes[0];
          (await space.read(refs, { budget })).release();
        }
        return lease;
      },
    };
    return { reader, privateBytes: () => privateBytes };
  }

  it("a page: the ordinary entry carries no descriptor, and is charged only itself (the probe, inverted)", async () => {
    const { space, budget, memory, chunk, refs } = await onDisk();
    const ref = defined(refs[0]);
    const { reader: wrapped, privateBytes } = racing(space, ref, budget);
    const before = memory.used;
    const reader = await ChunkReader.open(wrapped, chunk, { budget, once: true });
    await reader.load([0]);
    reader.close();
    expect(privateBytes()).toBeDefined();
    expect(privateBytes()).not.toBe(space.peek(ref, { once: true }));
    expect(space.parsed?.(ref) ?? null).toBeNull();
    expect(budget.used).toBe(0);
    // The scan's released page buffer may wait in the one-pass pool, charged as the pool's
    // reservation (increment 10's ruling); nothing else stays.
    expect(memory.used - before - space.stats.cache.poolBytes).toBe(ref.length + ENTRY_OVERHEAD);
  });

  it("a dictionary page: the same", async () => {
    const { space, budget, memory, chunk, dictionary } = await onDisk({ kind: "text" });
    const ref = defined(dictionary ?? undefined);
    const { reader: wrapped, privateBytes } = racing(space, ref, budget);
    const before = memory.used;
    const reader = await ChunkReader.open(wrapped, chunk, { budget, once: true });
    await reader.loadDictionary();
    reader.close();
    expect(privateBytes()).toBeDefined();
    expect(space.parsed?.(ref) ?? null).toBeNull();
    expect(budget.used).toBe(0);
    expect(memory.used - before).toBe(ref.length + ENTRY_OVERHEAD);
  });

  it("the space refuses a descriptor of an equal copy of a resident blob", async () => {
    const { space, budget, refs } = await onDisk();
    const ref = defined(refs[0]);
    (await space.read([ref], { budget })).release();
    const own = defined(space.peek(ref, { once: true }) ?? undefined);
    expect(space.attach?.(ref, {}, 768, own.slice())).toBe(false);
    expect(space.attach?.(ref, {}, 768, own)).toBe(true);
  });
});
