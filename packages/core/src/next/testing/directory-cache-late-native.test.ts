/** The late payload scan receives the same owner cache before its IO wrappers.
 * Its once policy survives that forwarding; rows are checked independently with PG.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, DIRECTORY_CACHE_LATE_REACH } from "../../testing/reach.js";
import type { Rel } from "../ir/ir.js";
import { lateSortShape } from "../exec/late-sort.js";
import { IndexOrderSource } from "../exec/key-order.js";
import { metadataCachesFor } from "../format/metadata-reader.js";
import * as directoryCode from "../page/directory.js";
import { ChunkReader } from "../page/reader.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});
const columns = ["id", "v", "t", ...Array.from({ length: 13 }, (_, n) => `c${String(n)}`)],
  fullSql = "SELECT * FROM late_directory_input ORDER BY v NULLS FIRST, id";
function selectedLateShape(rel: Rel): boolean {
  if (rel.op === "sort" && lateSortShape(rel) !== null) return true;
  return "input" in rel && selectedLateShape(rel.input);
}
for (const seed of seedsFor("next-directory-cache-late", [0xb509, 0xb50a]))
  it(`reuses payload directories through once late-fetch scans seed ${String(seed)}`, async () => {
    const reach = createReach("late-fetch directory cache", DIRECTORY_CACHE_LATE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.integer(),
        fc.boolean(),
        fc.boolean(),
        async (dense, salt, nullable, indexed) => {
          reach.beginHistory();
          const sql = indexed
              ? "SELECT * FROM late_directory_input ORDER BY id DESC LIMIT 7"
              : "SELECT * FROM late_directory_input ORDER BY v DESC NULLS LAST, id DESC LIMIT 7",
            count = dense ? 512 + ((salt >>> 0) % 129) : 64 + ((salt >>> 0) % 65),
            rows = Array.from({ length: count }, (_, id) =>
              Object.fromEntries<number | string | null>(
                columns.map((name, column): [string, number | string | null] => [
                  name,
                  column === 0
                    ? id
                    : column === 2
                      ? `key-${String(id % 13)}`
                      : nullable && (id + column) % 7 === 0
                        ? null
                        : (Math.imul(id ^ salt, column + 31) >>> 0) % 1000,
                ]),
              ),
            ),
            values = rows
              .map(
                (row) =>
                  `(${columns
                    .map((column) => {
                      const value = row[column];
                      if (value === undefined)
                        throw new Error("Missing late payload fixture column");
                      return value === null
                        ? "NULL"
                        : typeof value === "string"
                          ? `'${value}'`
                          : String(value);
                    })
                    .join(",")})`,
              )
              .join(","),
            wanted = (
              await pg.query(
                `WITH late_directory_input(${columns.join(",")}) AS (VALUES ${values}) ${sql}`,
              )
            ).rows,
            fullWanted = (
              await pg.query(
                `WITH late_directory_input(${columns.join(",")}) AS (VALUES ${values}) ${fullSql}`,
              )
            ).rows,
            core = await new StorageCore(4096).open(),
            parse = vi.spyOn(directoryCode, "parseDirectory"),
            opens = vi.spyOn(ChunkReader, "open"),
            ordered = vi.spyOn(IndexOrderSource.prototype, "next");
          try {
            await core.execute(
              `CREATE TABLE late_directory_input (${columns
                .map(
                  (column, at) =>
                    `${column} ${at === 2 ? "TEXT" : "INTEGER"}${at === 0 ? " PRIMARY KEY" : ""}`,
                )
                .join(",")})`,
            );
            await core.writeBatch("insertBatch", "late_directory_input", rows);
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            expect(selectedLateShape(core.prepare(sql).plan.root)).toBe(true);
            reach.reach("late shape selected");
            const query = async () => {
              ordered.mockClear();
              const result = await core.execute(sql);
              if (!("batches" in result)) throw new Error("Expected late payload rows");
              expect(oracleRows(result)).toEqual(wanted);
              expect(opens.mock.calls.some((call) => call[2].once === true)).toBe(true);
              expect(ordered.mock.calls.length > 0, "physical ordered-index path").toBe(indexed);
              reach.reach(indexed ? "ordered index payload" : "late sort payload");
            };
            parse.mockClear();
            opens.mockClear();
            await query();
            expect(parse).toHaveBeenCalled();
            const cache = metadataCachesFor(core.harness.space)?.directories;
            for (const result of parse.mock.results)
              if (result.type === "return" && result.value.sketch)
                reach.reach(result.value.sketch[1] === 0 ? "dense sketch" : "sparse sketch");
            // One-pass payload misses deliberately stay cold. A separate retaining
            // scan establishes entries this late-fetch plan is allowed to borrow.
            const primer = await core.execute(fullSql);
            if (!("batches" in primer)) throw new Error("Expected full payload primer rows");
            expect(oracleRows(primer)).toEqual(fullWanted);
            reach.reach("retaining payload primer");
            parse.mockClear();
            opens.mockClear();
            await query();
            expect(parse.mock.calls.length, "warm late payload directory parses").toBe(0);
            expect(cache).toBeDefined();
            expect(cache?.residentBytes).toBeGreaterThan(0);
            expect(cache?.evictableBytes).toBe(cache?.residentBytes);
            reach.reach("warm payload performs no validation");
            reach.reach("once payload opens");
            reach.reach(nullable ? "nullable payload" : "all-valid payload");
            reach.reach("complete PostgreSQL rows");
          } finally {
            parse.mockClear();
            parse.mockRestore();
            opens.mockClear();
            opens.mockRestore();
            ordered.mockClear();
            ordered.mockRestore();
            await core.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("all credit returned");
        },
      ),
      {
        seed,
        numRuns: 40,
        examples: [
          [true, 17, true, false],
          [false, -31, false, true],
        ],
      },
    );
    reach.assertReached();
  }, 180_000);
