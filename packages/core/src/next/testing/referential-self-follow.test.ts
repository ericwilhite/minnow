/** Follow's captured identity is one target, even when later RI entries name its new
 * physical versions. Shipped execution and the original-target contract supply authority. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { currentEngine } from "../../testing/differential-current.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_SELF_FOLLOW_REACH } from "../../testing/reach.js";
import { batch, ints } from "../write/test-support.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { referentialFixture } from "./referential.test-support.js";
for (const seed of seedsFor("next-native-self-follow", [0xb67d, 0xb67e]))
  it(`finishes self follow without revisiting original targets seed ${String(seed)}`, async () => {
    const reach = createReach("native self follow targets", NATIVE_SELF_FOLLOW_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 8 }),
        fc.integer({ min: 1, max: 12 }),
        fc.boolean(),
        fc.integer({ min: 1, max: 20 }),
        async (count, step, published, factor) => {
          reach.beginHistory();
          const shipped = await currentEngine("memory");
          const shift = 1 + (step % (count - 1)),
            setup = [
              "CREATE TABLE p(id INT PRIMARY KEY REFERENCES p(id),v INT)",
              `INSERT INTO p VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String((i + 1) * factor)})`).join(",")}`,
            ],
            f = await referentialFixture(setup, published, null, 64 << 20);
          const seen = new Set<number>(),
            write = f.rows.write.bind(f.rows);
          let applying = false;
          const spy = vi
            .spyOn(f.rows, "write")
            .mockImplementation(async (command, table, event, images, id, hooks, signal) => {
              if (applying && event === "update" && id !== null) {
                const identity = await f.targets.resolve(table, id);
                if (!identity) throw new Error("missing follow identity");
                expect(
                  seen.has(identity.originalId),
                  "follow cannot revisit the original child target",
                ).toBe(false);
                seen.add(identity.originalId);
              }
              return write(command, table, event, images, id, hooks, signal);
            });
          try {
            for (const sql of setup) await shipped.run({ kind: "sql", sql });
            await shipped.run({
              kind: "sql",
              sql: `UPDATE p SET id=((id+${String(shift)}-1)%${String(count)})+1`,
            });
            const input = await f.capture(
              "p",
              Array.from({ length: count }, (_, i) => i),
            );
            for (const [i, row] of input.refs.entries())
              await f.update(
                input.tableId,
                row.id,
                batch(ints([((i + shift) % count) + 1]), ints([(i + 1) * factor])),
                row.image,
              );
            applying = true;
            await f.end();
            applying = false;
            expect(seen.size).toBe(count);
            await f.retire();
            const result = await f.core.execute("SELECT id,v FROM p ORDER BY id");
            if (!("batches" in result)) throw new Error("rows");
            const old = await shipped.run({ kind: "sql", sql: "SELECT id,v FROM p ORDER BY id" });
            expect(
              result.batches.flatMap((b) =>
                Array.from({ length: b.length }, (_, i) => b.columns.map((v) => valueAt(v, i))),
              ),
            ).toEqual(old.rows?.map((row) => [row.id, row.v]));
            reach.reach(count === 2 ? "swap" : "rotation");
            reach.reach(published ? "published" : "resident");
            reach.reach("one follow per original target");
            reach.reach("shipped data");
          } finally {
            applying = false;
            spy.mockRestore();
            await f.close();
            await shipped.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [2, 1, false, 10],
          [3, 2, true, 10],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
