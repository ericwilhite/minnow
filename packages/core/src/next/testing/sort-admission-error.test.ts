/** Direct positional aliases are immutable reads (ir/ir.ts). A sort may retain one
 * physical copy, but every logical output and order must match PostgreSQL under pressure. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { createReach, SORT_ADMISSION_ERROR_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { SortOperator } from "../exec/sort.js";
import { Governor } from "../memory/governor.js";
import { StepLoans } from "../memory/step-loans.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const reach = createReach("sort alias progress", SORT_ADMISSION_ERROR_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});
for (const seed of seedsFor("next-sort-admission-error", [0xb761, 0xb762]))
  it(`sorts repeated SQL aliases exactly under pressure (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          aliases: fc.integer({ min: 5, max: 6 }),
          chars: fc.integer({ min: 360000, max: 360010 }),
          desc: fc.boolean(),
          many: fc.boolean(),
          split: fc.boolean(),
          topn: fc.boolean(),
          markers: fc.boolean(),
          markerAt: fc.integer({ min: 0, max: 6 }),
        }),
        async ({ aliases, chars, desc, many, split, topn, markers, markerAt }) => {
          reach.beginHistory();
          const core = await new StorageCore().open();
          const governor = new Governor(128 * 1024 * 1024);
          const resident = await governor.admit({
            label: "busy resident",
            minimumBytes: 0,
            spillable: true,
          });
          const scopes: Array<{ mockRestore(): void }> = [];
          const requests: Array<{ bytes: number; capacity: number; live: number }> = [];
          let pushed = false;
          let spilledBytes: (() => number) | undefined;
          let textSlots = 0;
          try {
            await core.execute(
              "CREATE TABLE wide_text(id INTEGER PRIMARY KEY, v0 TEXT, v1 TEXT, v2 TEXT, v3 TEXT, v4 TEXT)",
            );
            const value = "汉".repeat(chars);
            const rows = many
              ? [
                  { id: 1, v0: value, v1: split ? "a" + value : null },
                  { id: 2, v0: null, v1: split ? "only-second" : null },
                  { id: 3, v0: value + "z", v1: null },
                ]
              : [{ id: 1, v0: value, v1: split ? "a" + value : null }];
            await core.writeBatch("insertBatch", "wide_text", rows);
            await pg.exec(
              "DROP TABLE IF EXISTS wide_text; CREATE TABLE wide_text(id INTEGER PRIMARY KEY, v0 TEXT, v1 TEXT)",
            );
            for (const row of rows)
              await pg.query("INSERT INTO wide_text VALUES ($1,$2,$3)", [row.id, row.v0, row.v1]);
            const prepare = core.executor.prepare.bind(core.executor);
            scopes.push(
              vi.spyOn(core.executor, "prepare").mockImplementation((input) => {
                const prepared = prepare(input);
                return {
                  ...prepared,
                  execute: (request) => prepared.execute({ ...request, governor }),
                };
              }),
            );
            // eslint-disable-next-line @typescript-eslint/unbound-method -- applied with the original receiver.
            const push = SortOperator.prototype.push;
            scopes.push(
              vi.spyOn(SortOperator.prototype, "push").mockImplementation(function (
                this: SortOperator,
                batch,
                transfer,
              ) {
                spilledBytes = () => this.profile.spilledBytes;
                textSlots = Math.max(
                  textSlots,
                  batch.columns.filter((column) => column.type.kind === "text").length,
                );
                if (!pushed) {
                  pushed = true;
                  expect(
                    resident.allocate(
                      governor.capBytes - governor.reserveBytes - governor.usedBytes,
                    ),
                  ).toBe(true);
                  reach.reach("ordinary room exhausted");
                }
                return push.call(this, batch, transfer);
              }),
            );
            // eslint-disable-next-line @typescript-eslint/unbound-method -- applied with the original receiver.
            const borrow = StepLoans.prototype.borrow;
            scopes.push(
              vi.spyOn(StepLoans.prototype, "borrow").mockImplementation(function (
                this: StepLoans,
                bytes,
                signal,
              ) {
                if (this.label.includes("/sort#"))
                  requests.push({ bytes, capacity: this.capacity, live: this.budget.allocated });
                return borrow.call(this, bytes, signal);
              }),
            );
            const fields = Array.from(
              { length: aliases },
              (_, i) => `${split && i % 2 === 1 ? "v1" : "v0"} AS a${String(i)}`,
            );
            if (markers)
              fields.splice(markerAt % (aliases + 1), 0, "id + 17 AS marker", "id + 29 AS other");
            const sql = `SELECT ${fields.join(",")} FROM wide_text ORDER BY a${String((markerAt + 1) % aliases)} ${desc ? "DESC" : "ASC"}${many ? ", id" : ""}${topn ? " LIMIT 2" : ""}`;
            const expected = (await pg.query<Record<string, unknown>>(sql)).rows;
            const result = await core.execute(sql);
            if (!("batches" in result)) throw new Error("Expected SELECT rows");
            const actual = oracleRows(result);
            expect(actual).toHaveLength(expected.length);
            expect(result.columns.map((column) => column.name)).toEqual(
              Object.keys(expected[0] ?? {}),
            );
            for (const [index, row] of expected.entries()) {
              const observed = actual[index];
              for (const name of Object.keys(row))
                expect(
                  Object.is(observed?.[name], row[name]),
                  `row ${String(index)}, field ${name}`,
                ).toBe(true);
            }
            expect(textSlots).toBe(split ? 2 : 1);
            expect(pushed).toBe(true);
            if (!topn) expect(spilledBytes?.()).toBeGreaterThan(0);
            expect(requests.length).toBeGreaterThan(0);
            expect(requests.every((request) => request.bytes <= request.capacity)).toBe(true);
            expect(requests.every((request) => request.live === 0)).toBe(true);
            reach.reach("repeated SQL projection");
            reach.reach(topn ? "bounded topN succeeds" : "bounded spill and merge succeed");
            if (markers) reach.reach("computed fields preserved");
            if (split) reach.reach("separate variable sources preserved");
            reach.reach(aliases === 5 ? "five aliases" : "six aliases");
            reach.reach(desc ? "descending alias key" : "ascending alias key");
            reach.reach(markerAt % (aliases + 1) === 0 ? "leading marker" : "interleaved marker");
            if (many) reach.reach("NULL and unequal text rows");
            // Fixed-width aliases need no variable-value multiplier. Release the old
            // competitor before admitting the next complete execution, then pressure it again.
            resident.free(resident.allocated);
            resident.release(resident.held);
            pushed = false;
            await core.execute("CREATE TABLE tiny(id INTEGER PRIMARY KEY,v TEXT)");
            await core.writeBatch(
              "insertBatch",
              "tiny",
              Array.from({ length: 512 }, (_, i) => ({ id: i + 1, v: "x" })),
            );
            await pg.exec(
              "DROP TABLE IF EXISTS tiny; CREATE TABLE tiny(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO tiny SELECT i,'x' FROM generate_series(1,512)i",
            );
            const fixedSql = `SELECT ${Array.from({ length: 256 + aliases }, (_, i) => `id AS k${String(i)}`).join(",")} FROM tiny ORDER BY id`;
            const fixedExpected = (await pg.query<Record<string, unknown>>(fixedSql)).rows;
            const fixedResult = await core.execute(fixedSql);
            if (!("batches" in fixedResult)) throw new Error("Expected fixed SELECT rows");
            expect(oracleRows(fixedResult)).toEqual(fixedExpected);
            reach.reach("fixed-width alias rows exact");
            resident.free(resident.allocated);
            resident.release(resident.held);
            pushed = false;
            const smallSql = `SELECT ${Array.from({ length: 256 + aliases }, (_, i) => `v AS k${String(i)}`).join(",")} FROM tiny WHERE id%512=1 ORDER BY v`;
            const smallExpected = (await pg.query<Record<string, unknown>>(smallSql)).rows;
            const smallResult = await core.execute(smallSql);
            if (!("batches" in smallResult)) throw new Error("Expected small SELECT rows");
            expect(oracleRows(smallResult)).toEqual(smallExpected);
            reach.reach("tiny variable aliases remain admissible");
          } finally {
            for (const scope of scopes.reverse()) scope.mockRestore();
            resident.close();
            await core.close();
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("cleanup returns all resources");
          }
        },
      ),
      {
        seed,
        numRuns: 5,
        examples: [
          [
            {
              aliases: 5,
              chars: 360000,
              desc: false,
              many: false,
              split: false,
              topn: false,
              markers: false,
              markerAt: 0,
            },
          ],
          [
            {
              aliases: 6,
              chars: 360000,
              desc: true,
              many: true,
              split: true,
              topn: false,
              markers: true,
              markerAt: 3,
            },
          ],
          [
            {
              aliases: 6,
              chars: 360000,
              desc: false,
              many: true,
              split: true,
              topn: true,
              markers: true,
              markerAt: 1,
            },
          ],
        ],
      },
    );
  }, 120000);
