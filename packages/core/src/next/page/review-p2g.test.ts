/**
 * Review B2-p2g (rebuild/format-p2f 65e7bb4, rebuild/format-p2g 3d8daac). #1: `translationMap`
 * gave a throwing interner's reservation back, but not one whose interner re-entered it (the
 * inner map was overwritten by the outer, its 28 bytes never freed) or closed the reader (the map
 * was then published into a closed reader). Now reentry with the same interner is refused, a
 * close from the interner discards the map, and the reservation goes back whenever no map is
 * recorded. The generated property drives random callback schedules against the budget.
 */
import fc from "fast-check";
import { afterAll, describe, expect, it, vi } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ChunkReader, ReaderClosedError, TranslationReentryError } from "./reader.js";
import { defined, writeGroup } from "./test-support.js";

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
