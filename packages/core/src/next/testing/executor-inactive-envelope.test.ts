/** LIMIT 0 cannot enlarge a live sibling's minimum through an unopened aggregate. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { StreamDictionaries } from "../owner/frame.js";
import { Governor } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import { StorageCore } from "./storage-core.js";

it.each(seedsFor("next-inactive-aggregate-envelope", [0xb4df, 0xb4e0]))(
  "sizes only active aggregate envelopes beside a live EXCEPT input (seed %i)",
  async (seed) => {
    const reach = createReach("inactive aggregate envelope", [
      "sorted zero",
      "unordered zero",
      "parameter",
      "constant",
      "live sibling",
      "PG exact",
      "zero",
    ]);
    const core = await new StorageCore().open(),
      pg = new PGlite();
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1_030_000, max: 1_040_000 }),
          fc.boolean(),
          fc.boolean(),
          async (length, ordered, parameter) => {
            reach.beginHistory();
            const sql = `SELECT 'live'::text AS v EXCEPT (SELECT min('${"x".repeat(length)}') AS v${ordered ? " ORDER BY v" : ""} LIMIT ${parameter ? "$1" : "0"})`,
              args = parameter ? [0] : [];
            const expected = (await pg.query(sql, args)).rows;
            const governor = new Governor(8 << 20),
              snapshot = core.harness.path.pin(),
              prepared = core.prepare(sql);
            const execution = prepared.execute({
              snapshot,
              statement: null,
              parameters: args,
              statementTime: 0n,
              governor,
              spill: core.spill,
              queryId: 1,
              reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
              sequences: null,
              sequenceSession: null,
              returning: "atExecuted",
            });
            const rows: Array<{ v: string }> = [],
              dictionaries = new StreamDictionaries();
            try {
              for (;;) {
                const frame = await execution.results.next();
                if (!frame) break;
                try {
                  // Frames may preserve dictionary/constant forms (ResultFrame contract).
                  dictionaries.apply(frame.payload);
                  const value = frame.payload.columns[0];
                  if (!value) throw new Error("Missing text sibling output");
                  for (let row = 0; row < frame.payload.rowCount; row++) {
                    const entry =
                      value.form === "dictionary"
                        ? dictionaries.locate(value.dictionary, value.codes[row] ?? 0)
                        : null;
                    const flat =
                      entry?.entries ?? (value.form === "constant" ? value.value : value);
                    const index = entry?.index ?? (value.form === "constant" ? 0 : row);
                    if (flat.form !== "flat" || flat.data.layout !== "varlen")
                      throw new Error("Expected text sibling output");
                    rows.push({
                      v: new TextDecoder().decode(
                        flat.data.bytes.subarray(
                          flat.data.offsets[index],
                          flat.data.offsets[index + 1],
                        ),
                      ),
                    });
                  }
                } finally {
                  frame.release();
                }
              }
              await execution.executed;
              await execution.delivered;
              expect(rows).toEqual(expected);
              reach.reach(ordered ? "sorted zero" : "unordered zero");
              reach.reach(parameter ? "parameter" : "constant");
              reach.reach("live sibling");
              reach.reach("PG exact");
            } finally {
              execution.cancel();
              await Promise.allSettled([execution.executed, execution.delivered]);
              snapshot.release();
            }
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("zero");
          },
        ),
        {
          seed,
          numRuns: 6,
          examples: [
            [1_040_000, false, false],
            [1_040_000, true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
      await core.close();
    }
  },
  60000,
);
