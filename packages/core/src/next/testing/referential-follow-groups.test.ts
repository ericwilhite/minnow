/** Omitted-update follow is a complete original-target uniqueness group. Explicit CASCADE
 * retains PostgreSQL per-action behavior. A failed group precedes subsequent user AFTER work. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { currentEngine } from "../../testing/differential-current.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  NATIVE_FOLLOW_GROUP_REACH,
  NATIVE_FOLLOW_GROUP_PRESSURE_REACH,
} from "../../testing/reach.js";
import { NativeChildInputs } from "../owner/child-inputs.js";
import { batch, ints } from "../write/test-support.js";
import { referentialFixture } from "./referential.test-support.js";
function state(error: unknown): string {
  if (error instanceof Error && error.name === "UniqueConstraintError") return "23505";
  if (typeof error === "object" && error !== null) {
    if ("sqlState" in error && typeof error.sqlState === "string") return error.sqlState;
    if ("code" in error && typeof error.code === "string") return error.code;
  }
  throw new Error("Unexpected follow error", { cause: error });
}
for (const seed of seedsFor("next-native-follow-groups", [0xb66b, 0xb66c]))
  it(`checks whole original follow groups before returning to AFTER seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      shipped = await currentEngine("memory"),
      reach = createReach("native follow groups", NATIVE_FOLLOW_GROUP_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.integer({ min: 2, max: 6 }),
          fc.integer({ min: 1, max: 9 }),
          fc.boolean(),
          fc.boolean(),
          async (explicit, count, step, bad, published) => {
            reach.beginHistory();
            const shift = 1 + (step % (count - 1)),
              next = (id: number) => (bad ? 100 + id : ((id + shift - 1) % count) + 1),
              setup = [
                "CREATE TABLE p(id INT PRIMARY KEY,v INT)",
                `CREATE TABLE c(id INT PRIMARY KEY,pid INT UNIQUE REFERENCES p(id)${explicit ? " ON UPDATE CASCADE" : ""},g INT GENERATED ALWAYS AS(CASE WHEN pid>100 THEN 1 ELSE pid END) STORED,CONSTRAINT child_value_unique UNIQUE(g))`,
                `INSERT INTO p VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)})`).join(",")}`,
                `INSERT INTO c(id,pid) VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)})`).join(",")}`,
              ],
              sql = bad
                ? "UPDATE p SET id=id+100"
                : `UPDATE p SET id=((id+${String(shift)}-1)%${String(count)})+1`,
              f = await referentialFixture(setup, published, bad && !explicit ? 1 : null);
            const seals = vi.spyOn(NativeChildInputs.prototype, "seal"),
              write = f.rows.write.bind(f.rows),
              observer = vi
                .spyOn(f.rows, "write")
                .mockImplementation((command, table, event, images, id, hooks, signal) => {
                  if (!explicit && table === 2) {
                    expect(seals.mock.calls.length).toBe(count);
                    reach.reach("whole group OLD before writes");
                  }
                  return write(command, table, event, images, id, hooks, signal);
                });
            try {
              let expected: string | null = null;
              if (explicit) {
                await pg.exec("DROP TABLE IF EXISTS c,p CASCADE");
                for (const s of setup) await pg.exec(s);
                try {
                  await pg.exec(sql);
                } catch (e) {
                  expected = state(e);
                }
                reach.reach("explicit CASCADE PG");
              } else {
                await shipped.run({ kind: "sql", sql: "DROP TABLE IF EXISTS c" });
                await shipped.run({ kind: "sql", sql: "DROP TABLE IF EXISTS p" });
                for (const s of setup) await shipped.run({ kind: "sql", sql: s });
                try {
                  await shipped.run({ kind: "sql", sql });
                } catch (e) {
                  expected = state(e);
                }
                reach.reach("omitted follow shipped");
              }
              let actual: string | null = null,
                fault: unknown = null;
              try {
                const input = await f.capture(
                  "p",
                  Array.from({ length: count }, (_, i) => i),
                );
                for (const [i, row] of input.refs.entries())
                  await f.update(
                    input.tableId,
                    row.id,
                    batch(ints([next(i + 1)]), ints([i + 1])),
                    row.image,
                  );
                await f.end();
              } catch (e) {
                actual = state(e);
                fault = e;
              }
              expect(actual).toBe(expected);
              expect(actual).toBe(explicit || bad ? "23505" : null);
              if (bad) {
                const child = f.statement.catalog.tableByName("c"),
                  index = child?.indexes.find((i) => i.name === "child_value_unique");
                if (!child || !index) throw new Error("missing child constraint");
                expect(fault).toMatchObject({
                  tableId: child.id,
                  indexId: index.id,
                  sqlState: "23505",
                });
                reach.reach("child constraint identity");
                reach.reach("failure before later AFTER");
              }
              if (actual) await f.rollback();
              await f.retire();
              const result = await f.core.execute("SELECT id,pid FROM c ORDER BY id");
              if (!("batches" in result)) throw new Error("missing child rows");
              const { valueAt } = await import("../exec/kernels/test-support.js");
              expect(
                result.batches.flatMap((b) =>
                  Array.from({ length: b.length }, (_, i) => b.columns.map((v) => valueAt(v, i))),
                ),
              ).toEqual(
                Array.from({ length: count }, (_, i) => [i + 1, actual ? i + 1 : next(i + 1)]),
              );
              reach.reach(published ? "published" : "resident");
              reach.reach(count === 2 ? "swap" : "rotation");
              reach.reach(bad ? "nonunique completed group" : "unique completed group");
            } finally {
              observer.mockRestore();
              seals.mockRestore();
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 24,
          examples: [
            [false, 2, 1, false, false],
            [false, 3, 2, false, true],
            [false, 3, 1, true, false],
            [true, 2, 1, false, true],
            [true, 3, 2, true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
      await shipped.close();
    }
  }, 120000);

for (const seed of seedsFor("next-native-follow-group-pressure", [0xb66f, 0xb670]))
  it(`spills follow rotations with charged fixed headers seed ${String(seed)}`, async () => {
    const reach = createReach("follow group pressure", NATIVE_FOLLOW_GROUP_PRESSURE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 16, max: 64 }), fc.boolean(), async (count, published) => {
        reach.beginHistory();
        const setup = [
            "CREATE TABLE p(id INT PRIMARY KEY)",
            "CREATE TABLE c(id INT PRIMARY KEY,pid INT UNIQUE REFERENCES p(id))",
            `INSERT INTO p VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)})`).join(",")}`,
            `INSERT INTO c VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)})`).join(",")}`,
          ],
          f = await referentialFixture(setup, published, null, 64 << 20),
          gov = f.core.harness.governor,
          competitor = await gov.admit({
            label: "follow competing credit",
            minimumBytes: Math.max(0, gov.capBytes - gov.reserveBytes - gov.usedBytes - (1 << 20)),
            spillable: false,
          });
        expect(competitor.allocate(competitor.held)).toBe(true);
        const creates = vi.spyOn(f.spill, "create");
        try {
          const input = await f.capture(
            "p",
            Array.from({ length: count }, (_, i) => i),
          );
          for (const [i, row] of input.refs.entries())
            await f.update(input.tableId, row.id, batch(ints([((i + 1) % count) + 1])), row.image);
          const retained = f.charge.allocated;
          await f.end();
          expect(f.source.allocated).toBe(0);
          expect(f.source.loaned).toBe(0);
          expect(f.charge.allocated).toBeLessThanOrEqual(retained);
          expect(creates.mock.calls.length).toBeLessThan(12);
          expect(gov.peakBytes).toBeLessThanOrEqual(gov.capBytes);
          competitor.close();
          await f.retire();
          const result = await f.core.execute("SELECT id,pid FROM c ORDER BY id");
          if (!("batches" in result)) throw new Error("missing rows");
          const { valueAt } = await import("../exec/kernels/test-support.js");
          expect(
            result.batches.flatMap((b) =>
              Array.from({ length: b.length }, (_, i) => b.columns.map((v) => valueAt(v, i))),
            ),
          ).toEqual(Array.from({ length: count }, (_, i) => [i + 1, ((i + 1) % count) + 1]));
          reach.reach(published ? "published" : "resident");
          reach.reach("rotation");
          reach.reach("64MiB competing credit");
          reach.reach("bounded files");
          reach.reach("fixed charged metadata");
        } finally {
          creates.mockRestore();
          competitor.close();
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      {
        seed,
        numRuns: 4,
        examples: [
          [64, true],
          [16, false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
