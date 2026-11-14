/** The internal RI namespace is an explicit 1.0 cutover refusal, not PG OID emulation. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { createReach, TRIGGER_RESERVED_NAME_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { StorageCore } from "./storage-core.js";

const quote = (s: string) => `"${s.replaceAll('"', '""')}"`;
for (const seed of seedsFor("next-trigger-reserved-name", [0xb7c8, 0xb7c9]))
  it(`reserves the resolved RI prefix without changing nearby names seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("trigger reserved names", TRIGGER_RESERVED_NAME_REACH);
    try {
      await pg.exec(
        "CREATE TABLE src(n INT);CREATE FUNCTION mark() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
      );
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("RI_ConstraintTrigger_", "RI_ConstraintTrigger", "ri_constrainttrigger_"),
          fc.constantFrom("a", "Case", "汉"),
          fc.boolean(),
          async (prefix, suffix, asciiQuoted) => {
            reach.beginHistory();
            // Both front ends require quotes for UTF8 identifiers; keep generated SQL valid.
            const quoted = asciiQuoted || suffix === "汉",
              name = prefix + suffix,
              token = quoted ? quote(name) : name,
              resolved = quoted ? name : name.toLowerCase(),
              reserved = resolved.startsWith("RI_ConstraintTrigger_"),
              native = await new StorageCore().open(),
              shipped = new MinnowDatabase(new MemoryBlockStore());
            let pgCreated = false;
            try {
              for (const db of [native, shipped]) {
                await db.execute("CREATE TABLE src(n INT)");
                await db.execute("CREATE TABLE audit(n INT)");
              }
              const sql = `CREATE TRIGGER ${token} AFTER INSERT ON src BEGIN INSERT INTO audit(n) VALUES(NEW.n); END`;
              // Both authorities accept the name. Only 1.0 reserves this exact resolved prefix.
              await pg.exec(
                `CREATE TRIGGER ${token} AFTER INSERT ON src FOR EACH ROW EXECUTE FUNCTION mark()`,
              );
              pgCreated = true;
              await shipped.execute(sql);
              if (reserved) {
                expect(() => native.lowerer.lower(sql, native.context())).toThrow(
                  expect.objectContaining({ sqlState: "42939" }),
                );
                await expect(native.execute(sql)).rejects.toMatchObject({ sqlState: "42939" });
                expect(native.harness.path.catalog.triggers).toEqual([]);
                reach.reach("reserved refused");
              } else {
                await native.execute(sql);
                expect(native.harness.path.catalog.triggers[0]?.name).toBe(resolved);
                await native.execute(`DROP TRIGGER ${token}`);
                reach.reach(quoted ? "nearby quoted allowed" : "folded allowed");
              }
              if (suffix === "汉") reach.reach("UTF8 suffix");
              reach.reach("shipped accepts");
              reach.reach("PG accepts");
            } finally {
              if (pgCreated) await pg.exec(`DROP TRIGGER ${quote(resolved)} ON src`);
              await native.close();
              await shipped.close();
            }
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            ["RI_ConstraintTrigger_", "a", true],
            ["RI_ConstraintTrigger_", "Case", false],
            ["RI_ConstraintTrigger", "汉", true],
            ["ri_constrainttrigger_", "a", true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
