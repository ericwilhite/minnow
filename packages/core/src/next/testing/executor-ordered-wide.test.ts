/** PG ORDER BY preserves all logical fields; pins must not make a fitting spill step refuse. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ORDERED_WIDE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { QueryRuntime } from "../exec/run.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- receiver preserved
const reserve = QueryRuntime.prototype.reserve;
// eslint-disable-next-line @typescript-eslint/unbound-method -- receiver preserved
const restore = QueryRuntime.prototype.restorePhase;
afterEach(() => vi.restoreAllMocks());
for (const seed of seedsFor("ordered-wide", [47047, 47048]))
  it(`sorts and delivers wide logical keys under pressure seed${String(seed)}`, async () => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    const reach = createReach("ordered wide", ORDERED_WIDE_REACH);
    try {
      await pg.waitReady;
      for (const width of [32000, 800000]) {
        const table = `t${String(width)}`,
          a = "x".repeat(width);
        await core.execute(`CREATE TABLE ${table}(a TEXT,i INTEGER)`);
        await core.writeBatch(
          "insertBatch",
          table,
          [1, 2].map((i) => ({ a, i })),
        );
        await pg.exec(`CREATE TABLE ${table}(a TEXT,i INTEGER)`);
        await pg.query(`INSERT INTO ${table} VALUES($1,1),($1,2)`, [a]);
      }
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(1, 4, 12, 16),
          fc.oneof(
            fc.record({ aliases: fc.constant(true), width: fc.constantFrom(32000, 800000) }),
            fc.record({ aliases: fc.constant(false), width: fc.constant(32000) }),
          ),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (count, { width, aliases }, pin, descending, limited) => {
            reach.beginHistory();
            const keys = Array.from({ length: count }, (_, i) => `k${String(i)}`);
            const sql = `SELECT ${keys.join(",")},count(*) n FROM (SELECT ${keys.map((k, i) => `${aliases ? "a" : `CASE WHEN i=1 THEN a ELSE 'k${String(i)}' END`} ${k}`).join(",")} FROM t${String(width)})q GROUP BY ${keys.join(",")} ORDER BY k0 ${descending ? "DESC" : "ASC"}${limited ? " LIMIT 1" : ""}`;
            const expected = (await pg.query(sql)).rows;
            const governor = new Governor((width === 800000 ? 256 : 64) << 20);
            let holder = await governor.admit({
                label: "competitor",
                minimumBytes: 0,
                spillable: true,
              }),
              filled = false;
            const fill = () => {
              expect(
                holder.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
              ).toBe(true);
              filled = true;
            };
            const mocks: Array<{ mockRestore(): void }> = [];
            try {
              mocks.push(
                vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
                  this: QueryRuntime,
                  ...args
                ) {
                  await reserve.apply(this, args);
                  if (this.request.governor === governor && pin && !filled) fill();
                }),
              );
              mocks.push(
                vi.spyOn(QueryRuntime.prototype, "restorePhase").mockImplementation(async function (
                  this: QueryRuntime,
                  ...args
                ) {
                  if (this.request.governor !== governor) return restore.apply(this, args);
                  holder.close();
                  await restore.apply(this, args);
                  holder = await governor.admit({
                    label: "repin",
                    minimumBytes: 0,
                    spillable: true,
                  });
                  if (pin) fill();
                }),
              );
              const prepare = core.executor.prepare.bind(core.executor);
              mocks.push(
                vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                  const q = prepare(plan);
                  return {
                    ...q,
                    execute: (r) =>
                      q.execute({ ...r, governor, signal: AbortSignal.timeout(30000) }),
                  };
                }),
              );
              const result = await core.execute(sql);
              if (!("batches" in result)) throw new Error("Missing rows");
              expect(oracleRows(result)).toEqual(expected);
              if (aliases && count > 1) {
                const [a, b] = result.batches[0]?.columns ?? [];
                if (
                  a?.form !== "flat" ||
                  b?.form !== "flat" ||
                  a.data.layout !== "varlen" ||
                  b.data.layout !== "varlen"
                )
                  throw new Error("flat delivered aliases required");
                const before = b.data.bytes[0];
                a.data.bytes[0] = (a.data.bytes[0] ?? 0) ^ 1;
                expect(b.data.bytes[0]).toBe(before);
                expect(a.data.bytes.buffer).not.toBe(b.data.bytes.buffer);
              }
              reach.reach(aliases ? "aliases" : "distinct bindings");
              if (count === 16) reach.reach("16 keys");
              if (count === 12) reach.reach("12 keys");
              if (pin) reach.reach("pinned");
              if (limited) reach.reach("top-N");
              else reach.reach("full sort");
              reach.reach("PG exact");
            } finally {
              for (const mock of mocks.reverse()) mock.mockRestore();
              holder.close();
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("zero");
            }
          },
        ),
        {
          seed,
          numRuns: 12,
          endOnFailure: true,
          examples: [
            [16, { width: 800000, aliases: true }, true, false, true],
            [16, { width: 32000, aliases: false }, true, true, true],
            [12, { width: 800000, aliases: true }, true, true, false],
            [12, { width: 32000, aliases: false }, true, false, false],
            [4, { width: 32000, aliases: true }, false, false, true],
            [16, { width: 32000, aliases: false }, false, true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 120000);
