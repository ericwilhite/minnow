/** Choosing an optional index recipe must include inputs in its atomic admission. */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { createExecutor } from "../exec/create.js";
import { prepareParameters } from "../exec/parameters.js";
import { indexMinimums, orderedSort } from "../exec/key-order.js";
import { Governor, reserveFor, MemoryAdmissionError } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import type { PreparedPlan, ParameterValue } from "../exec/executor.js";
import type { Snapshot } from "../access/snapshot.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

it.each(seedsFor("next-key-order-parameter-admission", [0xe109, 0xe10a]))(
  "retains a fitting generic recipe when index buffers plus inputs exceed the cap (seed %i)",
  async (seed) => {
    const core = await new StorageCore(8192).open(),
      pg = new PGlite();
    const reach = createReach("index parameter admission", [
      "narrow",
      "wide",
      "one-parameter",
      "two-parameters",
      "null",
      "null-edge",
      "near-cap",
      "fallback",
      "released",
      "zero-unreferenced-input",
    ]);
    const names = ["id", ...Array.from({ length: 16 }, (_, i) => `c${String(i)}`)];
    const input = Array.from({ length: 20 }, (_, k) => {
      const id = (k * 7) % 20;
      return Object.fromEntries(names.map((n, j) => [n, id * 100 + (j === 0 ? -id * 99 : j)]));
    });
    let queryId = 700000;
    await pg.waitReady;
    try {
      const ddl = `CREATE TABLE t(${names.map((n) => `${n} INTEGER${n === "id" ? " PRIMARY KEY" : ""}`).join(",")})`;
      await core.execute(ddl);
      await pg.exec(ddl);
      await core.writeBatch("insertBatch", "t", input);
      for (const row of input)
        await pg.query(
          `INSERT INTO t VALUES(${names.map((_, i) => `$${String(i + 1)}`).join(",")})`,
          names.map((n) => row[n]),
        );
      const run = async (prepared: PreparedPlan, args: readonly ParameterValue[], cap: number) => {
        const governor = new Governor(cap),
          pin = core.harness.path.pin();
        let opens = 0;
        const snapshot: Snapshot = new Proxy(pin, {
          get(target, name) {
            if (name === "index")
              return (...a: Parameters<Snapshot["index"]>) => {
                opens++;
                return target.index(...a);
              };
            const value: unknown = Reflect.get(target, name, target);
            return typeof value === "function" ? (value.bind(target) as unknown) : value;
          },
        });
        const execution = prepared.execute({
          parameters: args,
          snapshot,
          statement: null,
          statementTime: 0n,
          reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
          governor,
          spill: core.spill,
          queryId: ++queryId,
          sequences: null,
          sequenceSession: null,
          returning: "atExecuted",
        });
        const rows: Array<Record<string, unknown>> = [];
        try {
          for (;;) {
            const frame = await execution.results.next();
            if (frame === null) break;
            try {
              rows.push(
                ...oracleRows({
                  columns: prepared.columns,
                  batches: [
                    {
                      length: frame.payload.rowCount,
                      selection: null,
                      columns: frame.payload.columns.map((v, i) => {
                        const type = prepared.columns[i]?.type;
                        if (!type || v.form !== "flat") throw new Error("frame type");
                        return { ...v, type };
                      }),
                    },
                  ],
                }),
              );
            } finally {
              frame.release();
            }
          }
          await execution.executed;
          await execution.delivered;
          return rows;
        } catch (error) {
          execution.cancel(error);
          await Promise.allSettled([execution.executed, execution.delivered]);
          throw error;
        } finally {
          pin.release();
          expect(opens, "a non-fitting optional strategy keeps its generic recipe").toBe(0);
          expect(governor.usedBytes).toBe(0);
          expect(core.spill.bytesInUse).toBe(0);
        }
      };
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.integer({ min: 0, max: 31 }),
          fc.boolean(),
          fc.boolean(),
          async (wide, two, nullLimit, slack, descending, unused) => {
            reach.beginHistory();
            const sql = `SELECT ${wide ? "*" : "id"} FROM t ORDER BY id ${descending ? "DESC" : "ASC"} LIMIT $1 OFFSET ${two ? "$2" : "1"}`;
            const args = two ? [nullLimit ? null : 3, 1] : [nullLimit ? null : 3];
            const plan = core.prepare(sql).plan,
              ordered = orderedSort(plan);
            if (!ordered) throw new Error("ordered fixture");
            const shape = {
              source: ordered.scan,
              fetch: ordered.project,
              restore: ordered.rel,
              tableId: ordered.scan.tableId,
              columns: ordered.columns,
              types: ordered.project.output.map((f) => f.type),
              descending,
            };
            const total = (cap: number) =>
              indexMinimums(shape, cap).reduce((n, e) => n + e.minimum, 0);
            const binding = await prepareParameters(plan, args);
            let lo = 2 << 20,
              hi = 64 << 20;
            while (lo < hi) {
              const mid = Math.floor((lo + hi) / 2);
              if (total(mid) + binding.bytes <= mid - reserveFor(mid)) hi = mid;
              else lo = mid + 1;
            }
            const cap = lo - 1 - slack,
              room = cap - reserveFor(cap) - total(cap);
            expect(room).toBeGreaterThanOrEqual(0);
            expect(room).toBeLessThan(binding.bytes);
            if (unused) {
              // Direct IR may retain an unused, typed input after a projection is pruned.
              // Zero still needs the ordinary input admission; no payload is materialized.
              const retained = {
                ...plan,
                parameters: [...plan.parameters, { kind: "text" } as const],
                placeholderCount: plan.placeholderCount + 1,
              };
              const oversized = [0, ...args.slice(1), "x".repeat(cap)];
              for (const keyOrder of [false, true])
                await expect(
                  run(createExecutor({ keyOrder }).prepare(retained), oversized, cap),
                ).rejects.toBeInstanceOf(MemoryAdmissionError);
              reach.reach("zero-unreferenced-input");
              reach.reach("released");
              return;
            }
            const expected = (await pg.query(sql, args)).rows;
            expect(await run(createExecutor({ keyOrder: false }).prepare(plan), args, cap)).toEqual(
              expected,
            );
            expect(await run(createExecutor({ keyOrder: true }).prepare(plan), args, cap)).toEqual(
              expected,
            );
            reach.reach("near-cap");
            reach.reach(wide ? "wide" : "narrow");
            reach.reach(two ? "two-parameters" : "one-parameter");
            if (nullLimit) reach.reach("null");
            if (nullLimit && binding.bytes - room <= 12) reach.reach("null-edge");
            reach.reach("fallback");
            reach.reach("released");
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [true, true, false, 0, false, false],
            [false, false, true, 31, true, false],
            [false, false, true, 0, true, false],
            [true, true, false, 0, false, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  },
);
