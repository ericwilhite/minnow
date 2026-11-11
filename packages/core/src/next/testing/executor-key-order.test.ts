/** SQL order and pinned index cursor lifetime: indexRange R17.5 / access/key-index.ts. */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import type { Snapshot } from "../access/snapshot.js";
import type { IndexReader, IndexCursor } from "../access/key-index.js";
import { OutputArena } from "../exec/kernels/vectors.js";
import { packLocator } from "../vector/locator.js";
import { currentNullBoundRefusal } from "./divergences.js";
import { createExecutor } from "../exec/create.js";

const names = ["id", ...Array.from({ length: 16 }, (_, i) => `p${String(i)}`)];
const schema = `id INTEGER PRIMARY KEY,${names
  .slice(1)
  .map((n, i) => `${n} ${i === 0 ? "TEXT" : "INTEGER"}`)
  .join(",")}`;
const input: Array<Record<string, number | string | null>> = Array.from(
  { length: 2305 },
  (_, i) => {
    const id = (i * 73) % 2305;
    return Object.fromEntries(
      names.map((n, j) => [
        n,
        j === 0
          ? id
          : j === 1
            ? `value-${String(id % 7)}`
            : (id + j) % 23 === 0
              ? null
              : id * 100 + j,
      ]),
    );
  },
);
const bounds = [0, 1, 7, 100, 2048, null, -1, 9223372036854775808n] as const;

it.each(seedsFor("next-executor-key-order", [0xe101, 0xe102]))(
  "uses ordered cursors and stops at the requested prefix (seed %i)",
  async (seed) => {
    const textKey = seed % 2 === 0;
    const fixtureRows: typeof input = textKey
      ? input.map((row) => ({ ...row, id: String(row.id).padStart(4, "0") }))
      : input;
    const definition = textKey ? schema.replace("id INTEGER", "id TEXT") : schema;
    const core = await new StorageCore(8192).open(),
      pg = new PGlite(),
      current = await currentEngine("memory");
    const reach = createReach("key order", [
      textKey ? "text-key" : "integer-key",
      "wide-payload",
      "projected-payload",
      "memory",
      "stored",
      "mixed",
      "empty",
      "deleted",
      "ascending",
      "descending",
      "parameter",
      "constant",
      "zero",
      "offset",
      "null",
      "negative",
      "overflow",
      "cursor",
      "admitted SQL cursor",
      "early-stop",
      "closed",
      "explicit-IR",
      "lowered-sort",
      "null-refusal-registered",
    ]);
    const stats = { opens: 0, closes: 0, rows: 0, capacity: 0, admitted: 0 };
    let explicit = false;
    const executor = createExecutor();
    const diagnostic = createExecutor({ keyOrder: true });
    core.executor.prepare = (plan) => {
      let physical = plan;
      if (
        explicit &&
        plan.root.op === "sort" &&
        plan.root.input.op === "project" &&
        plan.root.input.input.op === "scan"
      ) {
        const sort = plan.root,
          project = plan.root.input,
          scan = plan.root.input.input;
        const indexId = core.harness.path.catalog.table(scan.tableId)?.primaryKey;
        if (indexId === null || indexId === undefined) throw new Error("fixture PK");
        physical = {
          ...plan,
          root: {
            id: sort.id,
            op: "limit",
            output: sort.output,
            limit: sort.limit,
            offset: sort.offset,
            input: {
              ...project,
              input: {
                id: scan.id,
                op: "indexRange",
                output: scan.output,
                tableId: scan.tableId,
                indexId,
                lower: null,
                upper: null,
                descending: sort.keys[0]?.direction === "desc",
                columns: scan.columns,
                emitLocator: false,
              },
            },
          },
        };
      }
      const prepared = (explicit ? diagnostic : executor).prepare(physical);
      return {
        ...prepared,
        execute: (request) => {
          const pin = request.snapshot;
          if (!pin) throw new Error("query pin");
          const snapshot: Snapshot = new Proxy(pin, {
            get(target, name) {
              if (name === "index")
                return (...args: Parameters<Snapshot["index"]>): IndexReader => {
                  const reader = target.index(...args);
                  const observe = (cursor: IndexCursor): IndexCursor => {
                    stats.opens++;
                    return {
                      skip: cursor.skip.bind(cursor),
                      next(out) {
                        stats.capacity = Math.max(stats.capacity, out.length);
                        const done = (n: number) => {
                          stats.rows += n;
                          return n;
                        };
                        const result = cursor.next(out);
                        return result instanceof Promise ? result.then(done) : done(result);
                      },
                      close() {
                        stats.closes++;
                        cursor.close();
                      },
                    };
                  };
                  return {
                    def: reader.def,
                    probe: reader.probe.bind(reader),
                    range: (range, direction) => observe(reader.range(range, direction)),
                    planRange: (range, direction) => {
                      const plan = reader.planRange?.(range, direction);
                      if (!plan) throw new Error("Native admitted index plan required");
                      return {
                        minimumBytes: plan.minimumBytes,
                        open(options) {
                          expect(
                            options.budget.held - options.budget.allocated,
                          ).toBeGreaterThanOrEqual(plan.minimumBytes);
                          stats.admitted++;
                          return observe(plan.open(options));
                        },
                      };
                    },
                  };
                };
              const value: unknown = Reflect.get(target, name, target);
              return typeof value === "function" ? (value.bind(target) as unknown) : value;
            },
          });
          return prepared.execute({ ...request, snapshot, statement: null });
        },
      };
    };
    await pg.waitReady;
    try {
      for (const table of ["m", "s", "x", "e", "d"]) {
        const sql = `CREATE TABLE ${table}(${definition})`;
        await core.execute(sql);
        await pg.exec(sql);
        await current.run({ kind: "sql", sql });
        if (table === "e") continue;
        await current.run({ kind: "insertBatch", table, rows: fixtureRows });
        for (let at = 0; at < fixtureRows.length; at += 64) {
          const part = fixtureRows.slice(at, at + 64);
          await pg.query(
            `INSERT INTO ${table} VALUES ${part.map((_, k) => `(${names.map((_, j) => `$${String(k * names.length + j + 1)}`).join(",")})`).join(",")}`,
            part.flatMap((row) => names.map((n) => row[n])),
          );
        }
      }
      await core.writeBatch("insertBatch", "s", fixtureRows);
      await core.writeBatch("insertBatch", "d", fixtureRows);
      await core.writeBatch("insertBatch", "x", fixtureRows.slice(0, 1152));
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      await core.writeBatch("insertBatch", "m", fixtureRows);
      await core.writeBatch("insertBatch", "x", fixtureRows.slice(1152));
      const def = core.harness.path.catalog.tableByName("d");
      if (!def) throw new Error("delete fixture");
      const grant = await core.harness.governor.admit({
        label: "key-order/fixture",
        minimumBytes: 1 << 20,
        spillable: true,
      });
      const arena = new OutputArena(grant, 512);
      try {
        await core.write(async (statement) => {
          const table = await statement.view.table(def.id, { budget: grant });
          const v = arena.flat({ kind: "locator", tableId: def.id }).vector;
          if (v.data.layout !== "f64") throw new Error("locator layout");
          let at = 0,
            count = 0;
          for (const segment of table.segments)
            for (let position = 0; position < segment.rowCount; position++, at++)
              if (Number(fixtureRows[at]?.id) % 13 === 0)
                v.data.values[count++] = packLocator(segment.index, position);
          v.length = count;
          expect(
            await statement
              .table(def.id)
              .delete({ length: count, selection: null, columns: [v] }, null),
          ).toBe(count);
        });
        const pin = core.harness.path.pin();
        try {
          for (const [name, expected] of [
            ["m", ["memory"]],
            ["s", ["group"]],
            ["x", ["group", "memory"]],
            ["d", ["group"]],
          ] as const) {
            const fixture = core.harness.path.catalog.tableByName(name);
            if (!fixture) throw new Error("fixture table");
            const table = pin.table(fixture.id, { budget: grant });
            expect(
              new Set(table.segments.filter((s) => s.rowCount > 0).map((s) => s.kind)),
            ).toEqual(new Set(expected));
            if (name === "d") expect(table.liveRowCount).toBeLessThan(fixtureRows.length);
          }
        } finally {
          pin.release();
        }
      } finally {
        arena.close();
        grant.close();
      }
      await pg.exec("DELETE FROM d WHERE CAST(id AS INTEGER) % 13 = 0");
      await current.run({ kind: "sql", sql: "DELETE FROM d WHERE CAST(id AS INTEGER) % 13 = 0" });
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 4 }),
          fc.integer({ min: 0, max: bounds.length - 1 }),
          fc.integer({ min: 0, max: bounds.length - 1 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (state, li, oi, descending, parameter, useIR, projection) => {
            const small = projection && !useIR;
            explicit = useIR;
            reach.beginHistory();
            Object.assign(stats, { opens: 0, closes: 0, rows: 0, capacity: 0, admitted: 0 });
            const table = ["m", "s", "x", "e", "d"][state],
              limit = bounds[li],
              offset = bounds[oi];
            if (!table || limit === undefined || offset === undefined) throw new Error("dimension");
            const bound = parameter || li >= 6 || oi >= 6 || limit === null || offset === null;
            const sql = `SELECT ${small ? "p0" : "*"} FROM ${table} ORDER BY id ${descending ? "DESC" : "ASC"} LIMIT ${bound ? "$1" : String(limit)} OFFSET ${bound ? "$2" : String(offset)}`;
            const args = bound ? [limit, offset] : [];
            let code: unknown, expected: unknown;
            try {
              expected = (await pg.query(sql, args)).rows;
            } catch (e) {
              code = (e as { code: unknown }).code;
            }
            if (code !== undefined) {
              await expect(core.execute(sql, args)).rejects.toMatchObject({ sqlState: code });
              expect(stats.opens).toBe(0);
              if (code === "22003") reach.reach("overflow");
              else reach.reach("negative");
            } else {
              const result = await core.execute(sql, args);
              if (!("batches" in result)) throw new Error("query result");
              const actual = oracleRows(result);
              if (!useIR && stats.admitted > 0) reach.reach("admitted SQL cursor");
              expect(actual).toEqual(expected);
              if (actual.length > 0) {
                reach.reach(small ? "projected-payload" : "wide-payload");
                reach.reach(textKey ? "text-key" : "integer-key");
              }
              let legacyError: unknown;
              try {
                const legacy = await current.run({
                  kind: "sql",
                  sql,
                  params: args.map((v) => (typeof v === "bigint" ? String(v) : v)),
                });
                expect(legacy.rows).toEqual(expected);
              } catch (error) {
                legacyError = error;
              }
              if (legacyError !== undefined) {
                const normalized = args.map(
                  (value, i) => value ?? (i === 0 ? fixtureRows.length : 0),
                );
                const retry = await current.run({
                  kind: "sql",
                  sql,
                  params: normalized.map((value) =>
                    typeof value === "bigint" ? String(value) : value,
                  ),
                });
                expect(retry.rows).toEqual(expected);
                const plan = core.prepare(sql).plan;
                expect(currentNullBoundRefusal(plan, args, legacyError, true)).toBe(
                  "current-null-bound-refusal",
                );
                expect(currentNullBoundRefusal(plan, normalized, legacyError, true)).toBeNull();
                expect(currentNullBoundRefusal(plan, args, legacyError, false)).toBeNull();
                expect(
                  currentNullBoundRefusal(plan, args, new TypeError("other failure"), true),
                ).toBeNull();
                reach.reach("null-refusal-registered");
              }
              if (limit === 0) {
                expect(stats.opens).toBe(0);
                reach.reach("zero");
              } else {
                expect(stats.opens).toBe(1);
                expect(stats.closes).toBe(1);
                reach.reach("cursor");
                reach.reach(explicit ? "explicit-IR" : "lowered-sort");
                reach.reach("closed");
                if (typeof limit === "number" && typeof offset === "number") {
                  expect(stats.rows).toBeLessThanOrEqual(limit + offset);
                  expect(stats.capacity).toBeLessThanOrEqual(Math.max(limit + offset, 1));
                  if (limit + offset < fixtureRows.length && state !== 3) reach.reach("early-stop");
                }
              }
              if (offset !== null && typeof offset === "number" && offset > 0)
                reach.reach("offset");
              if (limit === null || offset === null) reach.reach("null");
            }
            reach.reach(
              (["memory", "stored", "mixed", "empty", "deleted"] as const)[state] ?? "empty",
            );
            reach.reach(descending ? "descending" : "ascending");
            reach.reach(bound ? "parameter" : "constant");
            expect(core.spill.bytesInUse).toBe(0);
          },
        ),
        {
          seed,
          numRuns: 20,
          examples: [
            [0, 3, 2, false, false, false, false],
            [1, 3, 2, true, true, true, false],
            [2, 2, 0, false, true, false, false],
            [3, 2, 0, false, false, true, false],
            [0, 0, 0, false, false, false, false],
            [0, 5, 5, false, true, true, false],
            [0, 0, 6, true, true, false, false],
            [0, 0, 7, true, true, true, false],
            [4, 3, 1, true, true, true, false],
            [0, 3, 2, false, true, false, true],
            [0, 1, 2, false, true, false, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
      await current.close();
    }
  },
  120000,
);
