/** Catalogue EXISTS decorrelation must fund complete int8 key growth, not refuse below cap. */
import fixture from "./executor-catalogue.json" with { type: "json" };
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, SEMI_ANTI_KEY_GROWTH_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { ParameterValue } from "../exec/executor.js";
import { bindParameter } from "../exec/parameters.js";
import { ColumnStore } from "../exec/column-store.js";
import { BATCH_ROWS } from "../vector/vector.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

/** The old row adapter's DatabaseRow type excludes bigint. Use exact native
 * vectors through the ordinary writer; every temporary input buffer is charged. */
async function insertExact(core: StorageCore, rows: ReadonlyArray<Record<string, ParameterValue>>) {
  const table = core.harness.path.catalog.tableByName("events_wide");
  if (!table) throw Error("Missing fixture table");
  const grant = await core.harness.governor.admit({
    label: "exact int8 input",
    minimumBytes: 1 << 20,
    spillable: true,
  });
  try {
    for (let from = 0; from < rows.length; from += BATCH_ROWS) {
      const input = rows.slice(from, from + BATCH_ROWS);
      const store = new ColumnStore(
        table.columns.map((column) => column.type),
        grant,
        { chunkRows: BATCH_ROWS },
      );
      try {
        for (const row of input) {
          const before = grant.allocated;
          const vectors = table.columns.map((column, slot) =>
            bindParameter(column.type, row[column.name] ?? null, slot, grant),
          );
          const bindings = grant.allocated - before;
          try {
            expect(store.append(vectors, Uint16Array.of(0), 0, 1)).toBe(1);
          } finally {
            grant.free(bindings);
          }
        }
        await core.insertVectors(table, {
          length: input.length,
          selection: null,
          columns: store.vectors(),
        });
      } finally {
        store.close();
      }
    }
  } finally {
    expect(grant.allocated).toBe(0);
    grant.close();
  }
}

const queries = ["join_semi_exists", "join_anti_not_exists"].map((id) => {
  const query = fixture.queries.find((entry) => entry.id === id);
  if (!query) throw Error(`Missing catalogue query ${id}`);
  return query;
});
const reach = createReach("semi/anti complete key growth", SEMI_ANTI_KEY_GROWTH_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});
for (const query of queries)
  for (const seed of seedsFor("next-semi-anti-key-growth", [0xb4f5, 0xb4f6]))
    it(`executes ${query.id} across key resizes (${String(seed)})`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            rows: fc.integer({ min: 18000, max: 19000 }),
            wide: fc.boolean(),
            nullable: fc.boolean(),
            reverse: fc.boolean(),
          }),
          async ({ rows, wide, nullable, reverse }) => {
            reach.beginHistory();
            const core = await new StorageCore().open();
            const ddl =
              "CREATE TABLE events_wide(id BIGINT PRIMARY KEY, i20 BIGINT, b00 BOOLEAN, i05 INTEGER)";
            const base = wide ? 2n ** 60n : 0n;
            const data = Array.from({ length: rows }, (_, index) => {
              const i = reverse ? rows - index - 1 : index;
              return {
                id: base + BigInt(i + 1),
                i20: nullable && i % 19 === 0 ? null : base + BigInt(((i * 17) % (rows + 97)) + 1),
                b00: i % 7 === 0 ? null : i % 7 !== 1,
                i05: i % 7 === 0 ? null : i % 7 === 1 ? 1 : 0,
              };
            });
            try {
              await core.execute(ddl);
              await insertExact(core, data);
              await pg.exec(
                `DROP TABLE IF EXISTS events_wide; ${ddl}; INSERT INTO events_wide SELECT ${String(base)} + i + 1, ${nullable ? "CASE WHEN i%19=0 THEN NULL ELSE " : ""}${String(base)} + ((i*17)%${String(rows + 97)}) + 1${nullable ? " END" : ""}, CASE WHEN i%7=0 THEN NULL ELSE i%7<>1 END, CASE WHEN i%7=0 THEN NULL WHEN i%7=1 THEN 1 ELSE 0 END FROM generate_series(0,${String(rows - 1)}) i`,
              );
              expect(data.filter((row) => row.b00).length).toBeGreaterThan(8192);
              reach.reach("more than 8192 right keys");
              {
                const expected = (await pg.query<{ n: number | string }>(query.sql)).rows.map(
                  (row) => Number(row.n),
                );
                const result = await core.execute(query.sql);
                if (!("batches" in result)) throw Error("Expected SELECT rows");
                expect(oracleRows(result).map((row) => Number(row.n))).toEqual(expected);
                reach.reach(query.id === "join_semi_exists" ? "catalogue semi" : "catalogue anti");
                reach.reach("PG exact");
                expect(core.spill.bytesInUse).toBe(0);
              }
              if (wide) reach.reach("wide correlation");
              if (nullable) reach.reach("NULL correlation");
            } finally {
              await core.close();
              expect(core.harness.governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("zero resources");
            }
          },
        ),
        {
          seed,
          numRuns: 3,
          examples: [
            [{ rows: 18000, wide: false, nullable: false, reverse: false }],
            [{ rows: 18000, wide: true, nullable: true, reverse: true }],
          ],
        },
      );
    }, 180000);
