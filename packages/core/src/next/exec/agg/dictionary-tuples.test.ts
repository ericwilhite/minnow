/** GROUP BY equality and first-seen group order, without decoding row text. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach } from "../../../testing/reach.js";
import type { Vector } from "../../vector/vector.js";
import { TEXT, dictionary, texts, valueAt } from "../kernels/test-support.js";
import { GroupTable } from "./groups.js";

it.each(seedsFor("dictionary-native-tuples", [0xb491, 0xb492]))(
  "keeps exact tuples through dictionary and direct/hash changes (seed %i)",
  (seed) => {
    const reach = createReach("dictionary tuple", [
      "native",
      "duplicates",
      "selection",
      "NULL",
      "flat",
      "hash",
      "borrowed",
      "release",
      "unused entries",
      "append",
      "new-short",
      "reappend",
    ]);
    fc.assert(
      fc.property(
        fc.array(fc.integer({ min: 0, max: 10000 }), { minLength: 128, maxLength: 128 }),
        (base) => {
          reach.beginHistory();
          const identities: number[] = [];
          let held = 0,
            controlHeld = 0;
          const table = new GroupTable([TEXT, TEXT, TEXT], {
              allocate(n) {
                held += n;
                return true;
              },
              free(n) {
                held -= n;
              },
            }),
            control = new GroupTable(
              [TEXT, TEXT, TEXT],
              {
                allocate(n) {
                  controlHeld += n;
                  return true;
                },
                free(n) {
                  controlHeld -= n;
                },
              },
              2048,
              false,
            ),
            controlGids = new Int32Array(128),
            gids = new Int32Array(128),
            model = new Map<string, number>(),
            first: string[][] = [];
          try {
            for (const mode of [
              "native",
              "append",
              "new-short",
              "reappend",
              "selection",
              "NULL",
              "flat",
              "hash",
            ] as const) {
              const card = mode === "hash" ? 70 : 3;
              const entries = Array.from({ length: card }, (_, i) => `value ${String(i)}`);
              entries.push(entries[0] ?? "", "UNUSED".repeat(1000));
              const vectors: Vector[] = Array.from({ length: 3 }, (_, c) => {
                const ordered = c % 2 === 0 ? [...entries] : [...entries].reverse();
                if (mode === "append" || mode === "reappend")
                  ordered.push(...Array.from({ length: 8 }, (_, i) => `${mode} ${String(i)}`));
                const usable = ordered
                  .map((_, i) => i)
                  .filter((i) => ordered[i] !== "UNUSED".repeat(1000));
                const codes = base.map((v, i) =>
                  mode === "NULL" && i % 11 === 0
                    ? null
                    : (usable[(v + c * i) % usable.length] ?? 0),
                );
                // Two codes for one byte value exercise canonical equality, not code identity.
                codes[0] = ordered.indexOf("value 0");
                codes[1] = ordered.lastIndexOf("value 0");
                if (mode === "flat")
                  return texts(codes.map((code) => (code === null ? null : (ordered[code] ?? ""))));
                const vector = dictionary(texts(ordered), codes);
                if (vector.form !== "dictionary") throw new Error("Missing dictionary fixture");
                if (mode === "append" || mode === "reappend")
                  return { ...vector, dictionaryId: identities[c] ?? vector.dictionaryId };
                if (mode === "native" || mode === "new-short") identities[c] = vector.dictionaryId;
                return vector;
              });
              const chosen =
                mode === "selection"
                  ? {
                      rows: Uint16Array.from(Array.from({ length: 64 }, (_, i) => 2 * i)),
                      count: 64,
                    }
                  : null;
              const active =
                chosen === null
                  ? Array.from({ length: 128 }, (_, i) => i)
                  : Array.from(chosen.rows);
              table.assign(vectors, 128, chosen, gids);
              control.assign(vectors, 128, chosen, controlGids);
              // Interning inactive dictionary entries would spend bytes absent from the
              // ordinary selected-entry path; both tables must own the same exact key data.
              expect(held).toBe(controlHeld);
              expect(Array.from(gids)).toEqual(Array.from(controlGids));
              reach.reach("unused entries");
              for (const row of active) {
                const values = vectors.map((v) => valueAt(v, row)),
                  key = JSON.stringify(values);
                let id = model.get(key);
                if (id === undefined) {
                  id = model.size;
                  model.set(key, id);
                  first.push(values as string[]);
                }
                expect(gids[row]).toBe(id);
              }
              expect(table.groupCount).toBe(model.size);
              if (mode === "native") {
                expect(table.dictionaryRows).toBeGreaterThan(0);
                reach.reach("native");
                reach.reach("duplicates");
              } else if (mode === "hash") {
                expect(table.directMapped).toBe(false);
                reach.reach("hash");
              } else reach.reach(mode);
              for (const v of vectors) {
                const flat = v.form === "dictionary" ? v.dictionary : v.form === "flat" ? v : null;
                if (flat?.data.layout === "varlen") flat.data.bytes.fill(255);
                if (v.form === "dictionary") v.codes.fill(0);
              }
              reach.reach("borrowed");
            }
            for (let c = 0; c < 3; c++) {
              const out = table.emitKey(c, 0, table.groupCount);
              for (let g = 0; g < model.size; g++) expect(valueAt(out, g)).toBe(first[g]?.[c]);
            }
          } finally {
            table.close();
            control.close();
          }
          expect(held).toBe(0);
          expect(controlHeld).toBe(0);
          reach.reach("release");
        },
      ),
      { seed, numRuns: 30 },
    );
    reach.assertReached();
  },
);
