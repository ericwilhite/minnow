/** pg_enum labels are bounded by UTF8 bytes, not JavaScript code units. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_ENUM_LABELS_REACH, createReach } from "../../testing/reach.js";
import { LoweredDatabase } from "./lowering-harness.js";
import { ENUM_LABEL_LIMIT } from "../types.js";
import { Catalog } from "../write/catalog.js";
import { lowerDdl } from "../lower/ddl.js";
for (const seed of seedsFor("next-catalog-enum-labels", [0xb792, 0xb793]))
  it(`matches enum label byte limits seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("enum label UTF8", CATALOG_ENUM_LABELS_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("z", "汉", "😀"),
          fc.integer({ min: 0, max: 70 }),
          async (unit, width) => {
            reach.beginHistory();
            await pg.exec("DROP TYPE IF EXISTS label_mood");
            const label = unit.repeat(width),
              sql = `CREATE TYPE label_mood AS ENUM ('${label}')`,
              next = new LoweredDatabase();
            let expected: unknown = null,
              actual: unknown = null;
            try {
              await pg.exec(sql);
            } catch (error) {
              if (error && typeof error === "object" && "code" in error) expected = error.code;
              else throw error;
            }
            try {
              next.execute(sql);
            } catch (error) {
              if (error && typeof error === "object" && "sqlState" in error)
                actual = error.sqlState;
              else throw error;
            }
            expect(actual).toBe(expected);
            const bytes = new TextEncoder().encode(label).length;
            reach.reach(expected === null ? "valid label" : "wide label");
            reach.reach(unit === "z" ? "ASCII" : unit === "汉" ? "BMP" : "astral");
            if (bytes === 63) reach.reach("63 bytes");
            if (bytes === 64) reach.reach("64 bytes");
          },
        ),
        {
          seed,
          numRuns: 30,
          examples: [
            ["z", 63],
            ["z", 64],
            ["汉", 21],
            ["汉", 22],
            ["😀", 15],
            ["😀", 16],
          ],
        },
      );
      // The native two-byte sort-rank contract is an explicit limit, not a PG limit.
      fc.assert(
        fc.property(
          fc.integer({ min: ENUM_LABEL_LIMIT - 1, max: ENUM_LABEL_LIMIT + 1 }),
          (count) => {
            reach.beginHistory();
            const labels = Array.from({ length: count }, (_, i) => `v${String(i)}`);
            // Use the compiled/catalog seam: SQL's independent token cap fires first.
            const statement = { kind: "create-enum" as const, name: "many_labels", values: labels };
            if (count > ENUM_LABEL_LIMIT) {
              expect(() => lowerDdl(statement, Catalog.empty())).toThrow(
                expect.objectContaining({ sqlState: "0A000" }),
              );
              reach.reach("rank limit refused");
            } else {
              const lowered = lowerDdl(statement, Catalog.empty());
              if (!lowered.change) throw new Error("Missing enum catalog change");
              const catalog = Catalog.empty().change(lowered.change, 1).catalog;
              expect(Catalog.decode(catalog.encode()).types[0]?.labels).toEqual(labels);
              reach.reach("rank limit fits");
            }
          },
        ),
        {
          seed,
          numRuns: 3,
          examples: [[ENUM_LABEL_LIMIT - 1], [ENUM_LABEL_LIMIT], [ENUM_LABEL_LIMIT + 1]],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
