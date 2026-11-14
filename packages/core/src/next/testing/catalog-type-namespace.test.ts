/** PG implicit pg_catalog lookup wins over public enum names, even with quoted spelling. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_TYPE_NAMESPACE_REACH, createReach } from "../../testing/reach.js";
import { PG_CATALOG_TYPE_NAMES } from "../lower/catalog-type-names.js";
import { LoweredDatabase } from "./lowering-harness.js";
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
const literal = (text: string) => `'${text.replaceAll("'", "''")}'`;
const at = (name: string) => PG_CATALOG_TYPE_NAMES.findIndex((value) => value === name);
for (const seed of seedsFor("next-catalog-type-namespace", [0xb796, 0xb797]))
  it(`refuses ambiguous catalog shadows without blocking custom enums seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("catalog type namespace", CATALOG_TYPE_NAMESPACE_REACH);
    try {
      const authority = (
        await pg.query<{ typname: string; typtype: string }>(
          `SELECT typname,typtype FROM pg_type WHERE typnamespace='pg_catalog'::regnamespace ORDER BY typname COLLATE "C"`,
        )
      ).rows;
      expect(authority.map((row) => row.typname)).toEqual(PG_CATALOG_TYPE_NAMES);
      const kinds = new Map(authority.map((row) => [row.typname, row.typtype]));
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.integer({ min: 0, max: PG_CATALOG_TYPE_NAMES.length - 1 }),
          fc.integer({ min: 0, max: 1000 }),
          async (builtin, which, suffix) => {
            reach.beginHistory();
            const name = builtin
              ? PG_CATALOG_TYPE_NAMES[which]
              : which % 3 === 0
                ? "integer"
                : which % 3 === 1
                  ? "INTEGER"
                  : `b6.${String(suffix)}`;
            if (!name) throw new Error("Missing catalog name");
            const create = `CREATE TYPE ${quote(name)} AS ENUM ('x')`,
              native = new LoweredDatabase();
            try {
              await pg.exec(create);
              if (builtin) {
                const names = await pg.query<{ resolved: number; system: number; own: number }>(
                  `SELECT to_regtype(${literal(quote(name))})::oid AS resolved,to_regtype(${literal("pg_catalog." + quote(name))})::oid AS system,to_regtype(${literal("public." + quote(name))})::oid AS own`,
                );
                expect(names.rows[0]?.resolved).toBe(names.rows[0]?.system);
                expect(names.rows[0]?.resolved).not.toBe(names.rows[0]?.own);
                expect(() => native.execute(create)).toThrow(
                  expect.objectContaining({ sqlState: "0A000", construct: "enumType" }),
                );
                reach.reach("implicit pg_catalog first");
                reach.reach(
                  name.startsWith("_")
                    ? "array builtin"
                    : kinds.get(name) === "c"
                      ? "catalog composite"
                      : kinds.get(name) === "p"
                        ? "pseudo builtin"
                        : "scalar builtin",
                );
              } else {
                native.execute(create);
                const query = `SELECT 'x'::${quote(name)} AS v`;
                expect(native.execute(query)).toEqual((await pg.query(query)).rows);
                reach.reach(name.startsWith("b6.") ? "novel custom" : "quoted alias");
              }
            } finally {
              await pg.exec(`DROP TYPE IF EXISTS public.${quote(name)}`);
            }
          },
        ),
        {
          seed,
          numRuns: 24,
          examples: [
            [true, at("money"), 0],
            [true, at("_text"), 0],
            [true, at("pg_class"), 0],
            [true, at("anyelement"), 0],
            [false, 0, 0],
            [false, 1, 0],
            [false, 2, 0],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
