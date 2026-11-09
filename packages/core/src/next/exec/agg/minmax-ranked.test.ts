/**
 * MIN / MAX over dictionary-encoded text and bytea by dictionary rank (states.ts
 * `MinMaxAccumulator`, the ranked path) against the same rows as plain vectors (the per-row byte
 * comparison, judged against PostgreSQL in aggregate-pglite.test.ts). Law: MIN/MAX order is the
 * values' order (`compareBytes`, the C collation), never dictionary-code order, across groups,
 * dictionaries and plain batches alike.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { mulberry32, seedsFor } from "../../../testing/seeds.js";
import type { Vector } from "../../vector/vector.js";
import { TEXT, UNCHARGED, dictionary, texts, valueAt } from "../kernels/test-support.js";
import { IDENTITY } from "../kernels/vectors.js";
import { accumulatorFor } from "./states.js";

const SEEDS = seedsFor("next-agg-minmax-ranked", [0x3a, 0x3b]);

/** Values sharing prefixes, the empty string, bytes past 0x7f and long ones. */
const POOL = [
  "",
  "a",
  "ab",
  "abc",
  "abd",
  "b",
  "B",
  "ba",
  "z",
  "é",
  "ée",
  "€",
  "\u{1f600}",
  "zz",
  "a".repeat(40),
  "a".repeat(39) + "b",
  "m",
  "mm",
  " ",
  "~",
];

describe("MIN/MAX by dictionary rank equals MIN/MAX by value (generated)", () => {
  it.each(SEEDS)(
    "dictionaries in first-seen order, changing, duplicated, mixed with plain batches (seed %i)",
    (seed) => {
      const reach = createReach("next-agg-minmax-ranked", [
        "ranked batch",
        "dictionary change",
        "plain batch between ranked ones",
        "duplicate dictionary entries",
        "value outside the dictionary",
        "dictionary reused across batches",
      ] as const);
      fc.assert(
        fc.property(
          fc.constantFrom("min", "max"),
          fc.constantFrom(1, 7, 300),
          fc.integer({ min: 1, max: 30 }),
          fc.integer(),
          (fn, groups, batches, inner) => {
            reach.beginHistory();
            const random = mulberry32(inner);
            const ranked = accumulatorFor(fn, TEXT, TEXT, UNCHARGED);
            const plain = accumulatorFor(fn, TEXT, TEXT, UNCHARGED);
            const batched = accumulatorFor(fn, TEXT, TEXT, UNCHARGED, {
              batchDictionaryMinMax: true,
            });
            ranked.grow(groups);
            plain.grow(groups);
            batched.grow(groups);
            const gids = new Int32Array(2048);
            let dictionaryEntries: string[] | null = null;
            // One entries vector and id per dictionary, kept across batches (as a chunk's pages).
            let dictionaryFlat = texts([""]);
            let dictionaryId = -1;
            let rankedWith = -1;
            let lastRanked = false;
            let plainSince = false;
            for (let b = 0; b < batches; b++) {
              const count = 64 + Math.floor(random() * 1985);
              for (let r = 0; r < count; r++) gids[r] = Math.floor(random() * groups);
              // A new dictionary now and then: a random subset of the pool, shuffled (codes are
              // first-seen order, not value order), sometimes with a duplicate entry.
              if (dictionaryEntries === null || random() < 0.3) {
                const size = 1 + Math.floor(random() * Math.min(POOL.length, count / 4));
                const entries = [...POOL].sort(() => random() - 0.5).slice(0, size);
                if (random() < 0.2 && entries.length > 1) {
                  entries.push(entries[0] ?? "");
                  reach.reach("duplicate dictionary entries");
                }
                if (dictionaryEntries !== null && lastRanked) reach.reach("dictionary change");
                dictionaryEntries = entries;
                dictionaryFlat = texts(entries);
                dictionaryId = -1;
              }
              const entries = dictionaryEntries;
              const asPlain = random() < 0.25;
              const values: Array<string | null> = [];
              const codes: Array<number | null> = [];
              for (let r = 0; r < count; r++) {
                if (random() < 0.05) {
                  values.push(null);
                  codes.push(null);
                  continue;
                }
                if (asPlain) {
                  // Plain batches may hold values no dictionary has.
                  const value = POOL[Math.floor(random() * POOL.length)] ?? "";
                  if (!entries.includes(value)) reach.reach("value outside the dictionary");
                  values.push(value);
                  codes.push(null);
                } else {
                  const code = Math.floor(random() * entries.length);
                  values.push(entries[code] ?? "");
                  codes.push(code);
                }
              }
              const flat = texts(values);
              let vector: Vector = flat;
              if (!asPlain) {
                const made = dictionary(dictionaryFlat, codes);
                if (dictionaryId < 0 && made.form === "dictionary")
                  dictionaryId = made.dictionaryId;
                vector = { ...made, dictionaryId } as Vector;
              }
              if (!asPlain && 4 * entries.length <= count) {
                reach.reach("ranked batch");
                if (rankedWith === dictionaryId) reach.reach("dictionary reused across batches");
                rankedWith = dictionaryId;
                if (plainSince) reach.reach("plain batch between ranked ones");
                plainSince = false;
                lastRanked = true;
              } else if (asPlain && lastRanked) plainSince = true;
              ranked.update(vector, gids, IDENTITY, count, groups === 1);
              plain.update(flat, gids, IDENTITY, count, groups === 1);
              batched.update(vector, gids, IDENTITY, count, groups === 1);
            }
            const ours = ranked.emit(0, groups);
            const theirs = plain.emit(0, groups);
            const batchResult = batched.emit(0, groups);
            for (let g = 0; g < groups; g++)
              expect(valueAt(ours, g), `group ${String(g)}`).toEqual(valueAt(theirs, g));
            for (let g = 0; g < groups; g++)
              expect(valueAt(batchResult, g), `batch candidate group ${String(g)}`).toEqual(
                valueAt(theirs, g),
              );
            batched.close();
            ranked.close();
            plain.close();
          },
        ),
        { seed, numRuns: 60 },
      );
      reach.assertReached();
    },
    180_000,
  );
});
