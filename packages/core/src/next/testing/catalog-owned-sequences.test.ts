/** PostgreSQL chooses collision-free, UTF8-clipped names for owned sequences. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_OWNED_NAMES_REACH, createReach } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
const names = [
  ["t", "id"],
  ["t".repeat(40), "c".repeat(40)],
  ["表".repeat(12), "列".repeat(12)],
  ["Case.table", "a.b"],
] as const;
for (const seed of seedsFor("next-catalog-owned-names", [0xb794, 0xb795]))
  it(`allocates owned sequence names seed ${String(seed)}`, async () => {
    const reach = createReach("owned sequence names", CATALOG_OWNED_NAMES_REACH);
    const pg = new PGlite();
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 3 }),
          fc.integer({ min: 0, max: 2 }),
          async (which, collisions) => {
            reach.beginHistory();
            const pair = names[which];
            if (!pair) throw new Error("Missing names");
            const [table, column] = pair,
              create = `CREATE TABLE ${quote(table)} (${quote(column)} SERIAL PRIMARY KEY)`;
            const core = await new StorageCore().open(),
              blockers: string[] = [];
            const actual = async () =>
              (
                await pg.query<{ name: string }>(
                  "SELECT relname AS name FROM pg_class WHERE oid=pg_get_serial_sequence($1,$2)::regclass",
                  [quote(table), column],
                )
              ).rows[0]?.name;
            try {
              for (let i = 0; i < collisions; i++) {
                await pg.exec(create);
                const name = await actual();
                if (!name) throw new Error("Missing PG sequence");
                await pg.exec(`DROP TABLE ${quote(table)}`);
                const sql = `CREATE SEQUENCE ${quote(name)}`;
                await pg.exec(sql);
                await core.execute(sql);
                blockers.push(name);
              }
              await pg.exec(create);
              await core.execute(create);
              const expected = await actual();
              expect(
                core.harness.path.catalog.sequences.find((s) => s.ownedBy !== null)?.name,
              ).toBe(expected);
              expect(new TextEncoder().encode(expected).length).toBeLessThanOrEqual(63);
              reach.reach(collisions ? "collision" : "no collision");
              reach.reach(
                which === 1 ? "long ASCII" : which === 2 ? "long Unicode" : "quoted names",
              );
            } finally {
              await core.close();
              await pg.exec(`DROP TABLE IF EXISTS ${quote(table)}`);
              for (const name of blockers) await pg.exec(`DROP SEQUENCE ${quote(name)}`);
            }
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [0, 0],
            [1, 1],
            [2, 2],
            [3, 1],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
