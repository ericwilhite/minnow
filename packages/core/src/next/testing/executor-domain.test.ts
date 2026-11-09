/** End-to-end exact statistics certification; PostgreSQL/current judge both strategies. */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import { createExecutor } from "../exec/create.js";
import { DomainCountSumOperator } from "../exec/agg/domain-count-sum.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
import { OutputArena } from "../exec/kernels/vectors.js";
import { packLocator } from "../vector/locator.js";

const modes = [
  "stored",
  "memory",
  "mixed",
  "wide",
  "computed",
  "filtered",
  "deleted",
  "empty",
  "all-null",
  "zero",
  "parameter",
] as const;
it.each(seedsFor("next-executor-domain", [0xb4a3, 0xb4a4]))(
  "uses certified stored domains and exact general fallbacks (seed %i)",
  async (seed) => {
    const core = await new StorageCore(8192).open(),
      pg = new PGlite(),
      current = await currentEngine("memory");
    const reach = createReach("executor domain", [
      ...modes,
      "PG",
      "current",
      "direct",
      "certificate",
      "general",
      "released",
      "default",
      "nulls-first",
      "nulls-last",
      "offset",
      "zero-offset",
    ]);
    const bounds = vi.spyOn(DomainCountSumOperator.prototype, "bounds"),
      push = vi.spyOn(DomainCountSumOperator.prototype, "push");
    let history = 0;
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: modes.length - 1 }),
          fc.integer({ min: -4096, max: 4096 }),
          fc.integer({ min: 32, max: 128 }),
          fc.boolean(),
          fc.integer({ min: 0, max: 5 }),
          async (state, base, count, nullsFirst, offset) => {
            reach.beginHistory();
            const mode = modes[state];
            if (!mode) throw new Error("mode");
            const name = `d${String(history++)}`,
              ddl = `CREATE TABLE ${name}(id INTEGER PRIMARY KEY,g INTEGER,v DOUBLE PRECISION)`;
            await core.execute(ddl);
            await pg.exec(ddl);
            await current.run({ kind: "sql", sql: ddl });
            let rows =
              mode === "empty"
                ? []
                : Array.from({ length: count }, (_, id) => ({
                    id,
                    g: mode === "all-null" || id % 11 === 0 ? null : base + ((id * 17) % 13),
                    v: id % 19 === 0 ? null : ((id % 17) - 8) * 0.125,
                  }));
            if (mode === "wide" && rows[1] && rows[2]) {
              rows[1].g = -2147483648;
              rows[2].g = 2147483647;
            }
            if (rows.length) {
              await current.run({ kind: "insertBatch", table: name, rows });
              await pg.query(
                `INSERT INTO ${name} VALUES ${rows.map((_, k) => `($${String(3 * k + 1)},$${String(3 * k + 2)},$${String(3 * k + 3)})`).join(",")}`,
                rows.flatMap((r) => [r.id, r.g, r.v]),
              );
              const cut = mode === "mixed" ? count >>> 1 : count;
              await core.writeBatch("insertBatch", name, rows.slice(0, cut));
              if (mode !== "memory") {
                await core.harness.path.recycleLog();
                await core.harness.path.quiesce();
              }
              if (mode === "mixed") await core.writeBatch("insertBatch", name, rows.slice(cut));
            }
            if (mode === "deleted") {
              const definition = core.harness.path.catalog.tableByName(name);
              if (!definition) throw new Error("delete table");
              const grant = await core.harness.governor.admit({
                label: "domain/delete-fixture",
                minimumBytes: 1 << 20,
                spillable: true,
              });
              const arena = new OutputArena(grant, 2048);
              try {
                await core.write(async (statement) => {
                  const table = await statement.view.table(definition.id, { budget: grant });
                  const vector = arena.flat({ kind: "locator", tableId: definition.id }).vector;
                  if (vector.data.layout !== "f64") throw new Error("locator");
                  let ordinal = 0,
                    n = 0;
                  for (const segment of table.segments)
                    for (let position = 0; position < segment.rowCount; position++, ordinal++)
                      if (rows[ordinal]?.g === base)
                        vector.data.values[n++] = packLocator(segment.index, position);
                  vector.length = n;
                  expect(
                    await statement
                      .table(definition.id)
                      .delete({ length: n, columns: [vector], selection: null }, null),
                  ).toBe(n);
                });
              } finally {
                arena.close();
                grant.close();
              }
              const sql = `DELETE FROM ${name} WHERE g=${String(base)}`;
              await pg.exec(sql);
              await current.run({ kind: "sql", sql });
              rows = rows.filter((r) => r.g !== base);
            }
            const key = mode === "computed" ? "g+0" : "g",
              limit = mode === "zero" ? "0" : mode === "parameter" ? "$1" : "10";
            const sql = `SELECT ${key} AS g,COUNT(*) AS n,SUM(v) AS s FROM ${name}${mode === "filtered" ? ` WHERE g>=${String(base + 3)}` : ""} GROUP BY ${key} ORDER BY n DESC,g ASC NULLS ${nullsFirst ? "FIRST" : "LAST"} LIMIT ${limit} OFFSET ${String(offset)}`;
            const params = mode === "parameter" ? [10] : [];
            const expected = (await pg.query<Record<string, unknown>>(sql, params)).rows;
            const legacy = await current.run({ kind: "sql", sql, params });
            expect(legacy.kind).toBe("rows");
            expect(legacy.rows).toEqual(expected);
            reach.reach("PG");
            reach.reach("current");
            for (const enabled of [false, true, undefined]) {
              const executor = createExecutor(
                enabled === undefined ? {} : { domainCountSum: enabled },
              );
              core.executor.prepare = executor.prepare.bind(executor);
              bounds.mockClear();
              push.mockClear();
              const result = await core.execute(sql, params);
              if (!("batches" in result)) throw new Error("rows");
              expect(oracleRows(result)).toEqual(expected);
              const direct = push.mock.contexts.some(
                (value) => value instanceof DomainCountSumOperator && value.directRows > 0,
              );
              const wanted = enabled !== false && (mode === "stored" || mode === "deleted");
              expect(
                direct,
                "known stored compact domains must use direct state; all other shapes keep exact fallback",
              ).toBe(wanted);
              if (wanted) {
                const certificate = bounds.mock.calls[0];
                if (!certificate) throw new Error("missing complete domain certificate");
                const keys = rows.flatMap((r) => (r.g === null ? [] : [r.g]));
                expect(
                  certificate[0],
                  "certificate must cover visible negative/offset keys",
                ).toBeLessThanOrEqual(Math.min(...keys));
                expect(certificate[1]).toBeGreaterThanOrEqual(Math.max(...keys));
                expect(certificate[2]).toBe(rows.length);
                reach.reach("direct");
                reach.reach("certificate");
              } else reach.reach("general");
              if (enabled === undefined) reach.reach("default");
            }
            reach.reach(nullsFirst ? "nulls-first" : "nulls-last");
            reach.reach(offset > 0 ? "offset" : "zero-offset");
            reach.reach(mode);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("released");
          },
        ),
        {
          seed,
          // Keep 16 random histories after all explicit mode/null-order examples.
          numRuns: 16 + 2 * modes.length,
          examples: modes.flatMap(
            (_, mode) =>
              [
                [mode, -17, 64, true, 0],
                [mode, -17, 64, false, 3],
              ] as Array<[number, number, number, boolean, number]>,
          ),
        },
      );
      reach.assertReached();
    } finally {
      bounds.mockRestore();
      push.mockRestore();
      await current.close();
      await pg.close();
      await core.close();
      expect(core.harness.governor.usedBytes).toBe(0);
    }
  },
  120000,
);
