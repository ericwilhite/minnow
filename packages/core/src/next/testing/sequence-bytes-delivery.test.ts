/** Bytea uses the execution's admitted copy, never a caller buffer after admission. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SEQUENCE_BYTES_REACH } from "../../testing/reach.js";
import { QueryRuntime } from "../exec/run.js";
import { Governor } from "../memory/governor.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- supplied receiver
const reserve = QueryRuntime.prototype.reserve;
for (const seed of seedsFor("sequence-bytes", [47057, 47058]))
  it(`preserves owned byte parameters and aliases seed${String(seed)}`, async () => {
    const core = await new StorageCore().open(),
      pg = new PGlite(),
      reach = createReach("sequence byte delivery", SEQUENCE_BYTES_REACH);
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(0, 32, 200000),
          fc.integer({ min: 0, max: 255 }),
          fc.integer({ min: 2, max: 8 }),
          async (width, tail, count) => {
            reach.beginHistory();
            const governor = new Governor(16 << 20),
              value = new Uint8Array(width).fill(37),
              delimiter = new Uint8Array(width ? 3 : 0).fill(37);
            if (width) value[width - 1] = tail;
            const sql = `SELECT a x,a y FROM(SELECT string_agg($1::bytea,$2::bytea ORDER BY i) a FROM generate_series(1,${String(count)})g(i))q`;
            const expected = (await pg.query(sql, [value, delimiter])).rows;
            // BYTEA SQL casts/DDL remain a separate front-end limitation; the native
            // lowerer parameter contract supports the exact physical byte type.
            const lower = core.lowerer.lower.bind(core.lowerer);
            const typed = vi
              .spyOn(core.lowerer, "lower")
              .mockImplementation((text, context) =>
                lower(text, { ...context, parameterTypes: [{ kind: "bytes" }, { kind: "bytes" }] }),
              );
            let changed = false;
            const admit = vi
              .spyOn(QueryRuntime.prototype, "reserve")
              .mockImplementation(async function (this: QueryRuntime, ...args) {
                await reserve.apply(this, args);
                if (this.request.governor === governor) {
                  value.fill(99);
                  delimiter.fill(100);
                  changed = true;
                }
              });
            const prepare = core.executor.prepare.bind(core.executor),
              spy = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                const p = prepare(plan);
                return { ...p, execute: (r) => p.execute({ ...r, governor }) };
              });
            try {
              const result = await core.execute(sql.replaceAll("::bytea", ""), [value, delimiter]);
              if (!("batches" in result)) throw new Error("Rows required");
              const actual = oracleRows(result);
              expect(actual).toEqual(expected);
              expect(changed).toBe(true);
              const x = result.batches[0]?.columns[0],
                y = result.batches[0]?.columns[1];
              if (
                x?.form !== "flat" ||
                y?.form !== "flat" ||
                x.data.layout !== "varlen" ||
                y.data.layout !== "varlen"
              )
                throw new Error("Bytea buffers required");
              // The adapter structured-clones the whole frame graph, preserving any
              // sharing. Inspect those backing buffers, not oracleValue's independent slices.
              expect(x.data.bytes.buffer).not.toBe(y.data.bytes.buffer);
              if (x.data.bytes.length) {
                const prior = y.data.bytes[0];
                x.data.bytes[0] = (x.data.bytes[0] ?? 0) ^ 1;
                expect(y.data.bytes[0]).toBe(prior);
              }
              reach.reach(width === 0 ? "empty" : width === 200000 ? "large" : "shared prefix");
              reach.reach("caller mutation");
              reach.reach("PG exact");
            } finally {
              spy.mockRestore();
              typed.mockRestore();
              admit.mockRestore();
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
            [0, 0, 2],
            [200000, 17, 8],
            [32, 37, 4],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 120000);
