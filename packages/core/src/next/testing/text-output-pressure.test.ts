/** Complete native TEXT output, not the input width or FTS document cap, funds delivery. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TEXT_OUTPUT_PRESSURE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { QueryRuntime } from "../exec/run.js";
import { arrayHeader } from "../exec/array-data.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { outputVector } from "./text-output-cases.test.fixture.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with supplied receiver
const reserve = QueryRuntime.prototype.reserve;
// eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with supplied receiver
const restore = QueryRuntime.prototype.restorePhase;
const ARRAY = { kind: "array", element: { kind: "text" } } as const;
for (const seed of seedsFor("text-output-pressure", [47069, 47070]))
  it(`funds full native TEXT and retires phases seed${String(seed)}`, async () => {
    const core = await new StorageCore().open(),
      pg = new PGlite(),
      reach = createReach("text output pressure", TEXT_OUTPUT_PRESSURE_REACH);
    let history = 0;
    try {
      await pg.waitReady;
      await core.execute("CREATE TABLE wide_array(i double precision,a text[])");
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("array", "numeric", "bytes"),
          fc.integer({ min: 80000, max: 120000 }),
          fc.constantFrom(12, 16, 20),
          fc.constantFrom("ordinary", "pinned", "cancel"),
          fc.boolean(),
          async (kind, size, cap, mode, aliases) => {
            reach.beginHistory();
            const governor = new Governor(cap << 20),
              controller = new AbortController(),
              mocks: Array<{ mockRestore(): void }> = [];
            let holder = await governor.admit({
                label: "competitor",
                minimumBytes: 0,
                spillable: true,
              }),
              filled = false,
              phases = 0,
              mutated = false;
            const bytes = kind === "bytes" ? new Uint8Array(size).fill(39) : null;
            let base: string,
              authority: string,
              args: ReadonlyArray<string | Uint8Array> = [];
            if (kind === "array") {
              const count = aliases ? Math.min(size, 350000) : size,
                header = arrayHeader(ARRAY.element, [count], [1], true),
                encoded = new Uint8Array(header.length + Math.ceil(count / 8));
              encoded.set(header);
              encoded.fill(255, header.length);
              if (count % 8) encoded[encoded.length - 1] = (1 << (count % 8)) - 1;
              const table = core.harness.path.catalog.tableByName("wide_array");
              if (!table) throw new Error("Missing table");
              const id = history++;
              await core.insertVectors(table, {
                length: 1,
                selection: null,
                columns: [outputVector({ kind: "float64" }, id), outputVector(ARRAY, encoded)],
              });
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
              base = `SELECT a::text a FROM wide_array WHERE i=${String(id)}`;
              authority = `SELECT array_fill(NULL::text,ARRAY[${String(count)}])::text a`;
            } else {
              base = "SELECT $1::text a";
              authority = `SELECT $1::${kind === "bytes" ? "bytea" : "numeric"}::text a`;
              args = [bytes ?? "0." + "0".repeat(16383)];
              const lower = core.lowerer.lower.bind(core.lowerer);
              mocks.push(
                vi.spyOn(core.lowerer, "lower").mockImplementation((sql, c) =>
                  lower(sql, {
                    ...c,
                    parameterTypes: [
                      kind === "bytes"
                        ? { kind: "bytes" }
                        : { kind: "decimal", precision: null, scale: null },
                    ],
                  }),
                ),
              );
            }
            const sql = aliases ? `SELECT a x,a y FROM(${base})q` : base,
              pgSql = aliases ? `SELECT a x,a y FROM(${authority})q` : authority,
              expected = (await pg.query(pgSql, [...args])).rows;
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
                  ...a
                ) {
                  await reserve.apply(this, a);
                  if (this.request.governor === governor) {
                    if (bytes && !mutated) {
                      bytes.fill(99);
                      mutated = true;
                    }
                    if (mode !== "ordinary" && !filled) fill();
                  }
                }),
              );
              mocks.push(
                vi.spyOn(QueryRuntime.prototype, "restorePhase").mockImplementation(async function (
                  this: QueryRuntime,
                  ...a
                ) {
                  if (this.request.governor !== governor) return restore.apply(this, a);
                  holder.close();
                  holder = await governor.admit({
                    label: "repin",
                    minimumBytes: 0,
                    spillable: true,
                  });
                  phases++;
                  if (mode === "cancel")
                    queueMicrotask(() => controller.abort(new Error("cancel text output")));
                  await restore.apply(this, a);
                  if (mode !== "ordinary") fill();
                }),
              );
              const prepare = core.executor.prepare.bind(core.executor);
              mocks.push(
                vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                  const p = prepare(plan);
                  return {
                    ...p,
                    execute: (r) =>
                      p.execute({
                        ...r,
                        governor,
                        signal: AbortSignal.any([controller.signal, AbortSignal.timeout(30000)]),
                      }),
                  };
                }),
              );
              if (mode === "cancel") {
                await expect(core.execute(sql, args)).rejects.toBeDefined();
                expect(controller.signal.aborted).toBe(true);
                reach.reach("cancel");
              } else {
                const result = await core.execute(sql, args);
                if (!("batches" in result)) throw new Error("Rows required");
                expect(oracleRows(result)).toEqual(expected);
                reach.reach("PG exact");
                if (aliases) {
                  const a = result.batches[0]?.columns[0],
                    b = result.batches[0]?.columns[1];
                  if (
                    a?.form !== "flat" ||
                    b?.form !== "flat" ||
                    a.data.layout !== "varlen" ||
                    b.data.layout !== "varlen"
                  )
                    throw new Error("Text buffers required");
                  expect(a.data.bytes.buffer).not.toBe(b.data.bytes.buffer);
                  const before = b.data.bytes[0];
                  a.data.bytes[0] = (a.data.bytes[0] ?? 0) ^ 1;
                  expect(b.data.bytes[0]).toBe(before);
                  reach.reach("independent aliases");
                }
              }
              reach.reach(kind);
              if (kind === "bytes") {
                expect(mutated).toBe(true);
                reach.reach("owned byte input");
              }
              if (kind === "array" && size === 2000000 && !aliases) reach.reach("ten MB result");
              if (mode === "pinned") reach.reach("pinned");
              if (phases) reach.reach("handoff");
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
          examples: [
            ["array", 2000000, 16, "pinned", false],
            ["array", 100000, 12, "ordinary", true],
            ["array", 100000, 16, "cancel", true],
            ["numeric", 80000, 12, "pinned", true],
            ["bytes", 120000, 12, "pinned", true],
          ],
          endOnFailure: true,
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 120000);
