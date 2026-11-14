/** Row-local array errors preserve the cursor's earlier rows, with or without pressure. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SEQUENCE_PREFIX_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { QueryRuntime } from "../exec/run.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- supplied receiver
const reserve = QueryRuntime.prototype.reserve;
// eslint-disable-next-line @typescript-eslint/unbound-method -- supplied receiver
const restore = QueryRuntime.prototype.restorePhase;
const TEXT = { kind: "text" } as const,
  ARRAY = { kind: "array", element: TEXT } as const;
for (const seed of seedsFor("sequence-prefix", [47061, 47062]))
  it(`matches array error prefixes at the cursor boundary seed${String(seed)}`, async () => {
    const core = await new StorageCore(2).open(),
      pg = new PGlite(),
      reach = createReach("sequence prefix", SEQUENCE_PREFIX_REACH);
    let history = 0;
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 3 }),
          fc.boolean(),
          fc.boolean(),
          fc.constantFrom(16, 20),
          fc.boolean(),
          async (before, prepend, pinned, mib, grouped) => {
            reach.beginHistory();
            const name = `prefix_${String(history++)}`,
              count = before + 1,
              lower = prepend ? -2147483648 : 2147483646,
              governor = new Governor(mib << 20);
            await core.createTable({
              name,
              columns: [
                { name: "i", type: "number" },
                { name: "a", type: "string", sqlDomain: { kind: "array", element: "text" } },
              ],
            });
            await pg.exec(`CREATE TABLE ${name}(i double precision,a text[])`);
            const utf8 = new TextEncoder(),
              parts = Array.from({ length: count }, (_, i) =>
                encodeArrayValue(TEXT, {
                  dimensions: [1],
                  lowerBounds: [i === before ? lower : 1],
                  elements: [utf8.encode("v")],
                }),
              ),
              offsets = new Int32Array(count + 1);
            let size = 0;
            parts.forEach((p, i) => {
              offsets[i] = size;
              size += p.length;
            });
            offsets[count] = size;
            const bytes = new Uint8Array(size);
            parts.forEach((p, i) => bytes.set(p, offsets[i] ?? 0));
            const table = core.harness.path.catalog.tableByName(name);
            if (!table) throw new Error("Missing table");
            await core.insertVectors(table, {
              length: count,
              selection: null,
              columns: [
                {
                  form: "flat",
                  type: { kind: "float64" },
                  length: count,
                  validity: null,
                  data: {
                    layout: "f64",
                    values: Float64Array.from({ length: count }, (_, i) => i),
                  },
                },
                {
                  form: "flat",
                  type: ARRAY,
                  length: count,
                  validity: null,
                  data: { layout: "varlen", bytes, offsets },
                },
              ],
            });
            for (let i = 0; i < count; i++)
              await pg.query(`INSERT INTO ${name} VALUES($1,$2::text[])`, [
                i,
                `[${String(i === before ? lower : 1)}:${String(i === before ? lower : 1)}]={v}`,
              ]);
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            const aggregate = grouped && !prepend;
            const sql = aggregate
              ? `SELECT i,array_agg('x'::text||a ORDER BY i) a FROM(SELECT i,a FROM ${name} ORDER BY i OFFSET 0)q GROUP BY i`
              : `SELECT i,${prepend ? "'x'::text||a" : "a||'x'::text"} a FROM(SELECT i,a FROM ${name} ORDER BY i OFFSET 0)q`;
            const expected: Array<Record<string, unknown>> = [];
            let expectedError = "";
            await pg.exec("BEGIN");
            try {
              await pg.exec(`DECLARE c CURSOR FOR ${sql}`);
              for (let i = 0; i < before; i++)
                expected.push(...(await pg.query<Record<string, unknown>>("FETCH 1 FROM c")).rows);
              try {
                await pg.query("FETCH 1 FROM c");
              } catch (e) {
                expectedError = (e as { code: string }).code;
              }
            } finally {
              await pg.exec("ROLLBACK");
            }
            expect(expectedError).toBe(prepend ? "22003" : "54000");
            let holder = await governor.admit({
                label: "competitor",
                minimumBytes: 0,
                spillable: true,
              }),
              filled = false;
            const mocks: Array<{ mockRestore(): void }> = [];
            const fill = () => {
              expect(
                holder.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
              ).toBe(true);
              filled = true;
            };
            try {
              mocks.push(
                vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
                  this: QueryRuntime,
                  ...args
                ) {
                  await reserve.apply(this, args);
                  if (this.request.governor === governor && pinned && !filled) fill();
                }),
              );
              mocks.push(
                vi.spyOn(QueryRuntime.prototype, "restorePhase").mockImplementation(async function (
                  this: QueryRuntime,
                  ...args
                ) {
                  if (this.request.governor !== governor) return restore.apply(this, args);
                  holder.close();
                  holder = await governor.admit({
                    label: "repin",
                    minimumBytes: 0,
                    spillable: true,
                  });
                  await restore.apply(this, args);
                  if (pinned) fill();
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
              const prepared = core.prepare(sql),
                run = core.start(prepared),
                settled = Promise.allSettled([run.execution.executed, run.execution.delivered]),
                actual: Array<Record<string, unknown>> = [];
              let error: unknown;
              try {
                for (;;) {
                  const frame = await run.execution.results.next();
                  if (!frame) break;
                  try {
                    const columns = frame.payload.columns.map((v, i) => {
                      if (v.form !== "flat") throw new Error("Flat frame required");
                      const type = prepared.columns[i]?.type;
                      if (!type) throw new Error("Missing type");
                      return { ...v, type };
                    });
                    actual.push(
                      ...oracleRows({
                        columns: prepared.columns,
                        batches: [{ length: frame.payload.rowCount, selection: null, columns }],
                      }),
                    );
                  } finally {
                    frame.release();
                  }
                }
              } catch (e) {
                error = e;
              } finally {
                await settled;
                run.release();
              }

              expect(error).toMatchObject({ sqlState: expectedError });
              expect(actual).toEqual(expected);
              reach.reach(prepend ? "22003" : "54000");
              if (aggregate) reach.reach("grouped prefix");
              reach.reach(pinned ? "pinned" : "ordinary");
              reach.reach(before ? "prefix" : "first row");
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
          numRuns: 8,
          endOnFailure: true,
          examples: [
            [2, false, true, 16, false],
            [2, false, true, 16, true],
            [2, false, false, 16, false],
            [1, true, true, 20, false],
            [0, true, false, 16, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 120000);
