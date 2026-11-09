/** GROUP BY equality depends on bytes, never on the dictionary's cache capacity. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach } from "../../../testing/reach.js";
import type { DictionaryVector } from "../../vector/vector.js";
import { TEXT, dictionary, texts, valueAt } from "../kernels/test-support.js";
import { GroupTable } from "./groups.js";

it.each(seedsFor("group-dictionary-append", [0xb493, 0xb494]))(
  "maps appended entries after both growth and cache-capacity reuse (seed %i)",
  (seed) => {
    const reach = createReach("group dictionary append", [
      "growth",
      "reused capacity",
      "borrowed",
      "released",
    ]);
    fc.assert(
      fc.property(
        fc.integer({ min: 4, max: 12 }),
        fc.integer({ min: -10000, max: 10000 }),
        (size, salt) => {
          reach.beginHistory();
          for (const reuse of [false, true]) {
            let held = 0;
            const table = new GroupTable([TEXT], {
                allocate(n) {
                  held += n;
                  return true;
                },
                free(n) {
                  held -= n;
                },
              }),
              ids = new Int32Array(32),
              model = new Map<string, number>();
            const entries = (tag: string, n: number) =>
              Array.from({ length: n }, (_, i) => `${tag}:${String(salt)}:${String(i)}`);
            function push(values: string[], identity?: number): DictionaryVector {
              const raw = dictionary(
                texts(values),
                values.map((_, i) => i),
              );
              if (raw.form !== "dictionary") throw new Error("Missing dictionary fixture");
              const vector = {
                ...raw,
                ...(identity === undefined ? {} : { dictionaryId: identity }),
              };
              table.assign([vector], values.length, null, ids);
              for (let i = 0; i < values.length; i++) {
                const value = values[i];
                if (value === undefined) throw new Error("Missing value");
                let id = model.get(value);
                if (id === undefined) {
                  id = model.size;
                  model.set(value, id);
                }
                expect(ids[i], `class of ${value}`).toBe(id);
              }
              if (vector.dictionary.data.layout === "varlen")
                vector.dictionary.data.bytes.fill(255);
              reach.reach("borrowed");
              return vector;
            }
            try {
              if (reuse) push(entries("old", 16));
              const prefix = entries("base", 2),
                short = push(prefix);
              push([...prefix, ...entries("fresh", size)], short.dictionaryId);
              // A different dictionary identity must reset translations without changing SQL classes.
              push([...entries("fresh", size)].reverse());
              expect(table.groupCount).toBe(model.size);
              const out = table.emitKey(0, 0, table.groupCount);
              for (const [value, id] of model) expect(valueAt(out, id)).toBe(value);
              reach.reach(reuse ? "reused capacity" : "growth");
            } finally {
              table.close();
            }
            expect(held).toBe(0);
            reach.reach("released");
          }
        },
      ),
      { seed, numRuns: 30 },
    );
    reach.assertReached();
  },
);
