/** File-backed expanding output: whole admission, cancellation and client ownership. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SEQUENCE_PRESSURE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { QueryRuntime } from "../exec/run.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- supplied receiver
const reserve = QueryRuntime.prototype.reserve;
// eslint-disable-next-line @typescript-eslint/unbound-method -- supplied receiver
const restore = QueryRuntime.prototype.restorePhase;
afterEach(() => vi.restoreAllMocks());
for (const seed of seedsFor("sequence-pressure", [47053, 47054]))
  it(`funds growing delivery and cleans every pressure phase seed${String(seed)}`, async () => {
    const core = await new StorageCore().open(),
      pg = new PGlite(),
      reach = createReach("sequence pressure", SEQUENCE_PRESSURE_REACH);
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("string", "array", "concat"),
          fc.constantFrom(100000, 200000),
          fc.constantFrom(4, 20),
          fc.constantFrom(12, 16, 20),
          fc.constantFrom("ordinary", "pinned", "cancel"),
          fc.boolean(),
          async (kind, width, count, cap, mode, aliases) => {
            reach.beginHistory();
            const parameter = "汉".repeat(Math.floor(width / 3)),
              governor = new Governor(cap << 20),
              controller = new AbortController();
            let holder = await governor.admit({
                label: "competitor",
                minimumBytes: 0,
                spillable: true,
              }),
              filled = false,
              phases = 0;
            const mocks: Array<{ mockRestore(): void }> = [];
            const fill = () => {
              expect(
                holder.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
              ).toBe(true);
              filled = true;
            };
            const base =
              kind === "concat"
                ? "SELECT $1::text||$1::text||$1::text a"
                : `SELECT ${kind === "string" ? "string_agg($1::text,'-' ORDER BY i)" : "array_agg($1::text ORDER BY i)"} a FROM generate_series(1,${String(count)})g(i)`;
            const sql = aliases ? `SELECT a x,a y FROM(${base})q` : base;
            const expected = (await pg.query(sql, [parameter])).rows;
            try {
              mocks.push(
                vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
                  this: QueryRuntime,
                  ...args
                ) {
                  await reserve.apply(this, args);
                  if (this.request.governor === governor && mode !== "ordinary" && !filled) fill();
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
                  phases++;
                  if (mode === "cancel")
                    queueMicrotask(() => controller.abort(new Error("cancel sequence phase")));
                  await restore.apply(this, args);
                  if (mode !== "ordinary") fill();
                }),
              );
              const prepare = core.executor.prepare.bind(core.executor);
              mocks.push(
                vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                  const q = prepare(plan);
                  return {
                    ...q,
                    execute: (r) =>
                      q.execute({
                        ...r,
                        governor,
                        signal: AbortSignal.any([controller.signal, AbortSignal.timeout(30000)]),
                      }),
                  };
                }),
              );
              if (mode === "cancel") {
                await expect(core.execute(sql, [parameter])).rejects.toBeDefined();
                expect(controller.signal.aborted).toBe(true);
                reach.reach("cancel");
              } else {
                const result = await core.execute(sql, [parameter]);
                if (!("batches" in result)) throw new Error("Rows required");
                expect(oracleRows(result)).toEqual(expected);
                expect(JSON.stringify(expected).length).toBeGreaterThan(parameter.length);
                reach.reach("expansion");
                reach.reach("PG exact");
                if (aliases) {
                  const batch = result.batches[0],
                    a = batch?.columns[0],
                    b = batch?.columns[1];
                  if (
                    a?.form !== "flat" ||
                    b?.form !== "flat" ||
                    a.data.layout !== "varlen" ||
                    b.data.layout !== "varlen"
                  )
                    throw new Error("Expected independently mutable outputs");
                  expect(a.data.bytes.buffer).not.toBe(b.data.bytes.buffer);
                  const before = b.data.bytes[0];
                  a.data.bytes[0] = (a.data.bytes[0] ?? 0) ^ 1;
                  expect(b.data.bytes[0]).toBe(before);
                  reach.reach("independent aliases");
                }
              }
              expect(phases).toBeGreaterThan(0);
              reach.reach("handoff");
              if (mode === "pinned") reach.reach("pinned");
              reach.reach(kind);
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
            ["string", 200000, 20, 16, "pinned", true],
            ["array", 200000, 20, 16, "pinned", true],
            ["concat", 200000, 4, 12, "pinned", true],
            ["string", 100000, 4, 12, "cancel", false],
            ["array", 100000, 4, 12, "cancel", false],
            ["concat", 100000, 4, 12, "cancel", false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 120000);
