/** FK actions call the same charged rules and keep OLD intact; failures roll back the statement. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ROW_RULES_FK_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { compileTableRules } from "../exec/rules.js";
import { tableRuleMinimumBytes, type BoundTableRules } from "../exec/row-rules.js";
import { bindParameter } from "../exec/parameters.js";
import type { Statement } from "../access/modify.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { ints, locators, batch } from "../write/test-support.js";
import * as reads from "../write/rows.js";
import type { FlatVector } from "../vector/vector.js";
const originalRead = reads.readRows;
const state = async (work: () => unknown) => {
  try {
    await work();
    return null;
  } catch (e) {
    if (e && typeof e === "object") {
      if ("sqlState" in e) return e.sqlState;
      if ("code" in e) return e.code;
    }
    throw e;
  }
};
for (const seed of seedsFor("next-row-rules-fk", [0xb7b2, 0xb7b3]))
  it(`retains OLD through generated SET NULL and rollback seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("FK row rules", ROW_RULES_FK_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(fc.boolean(), fc.boolean(), async (reject, checkpoint) => {
          reach.beginHistory();
          const core = await new StorageCore().open();
          const captured: FlatVector[] = [];
          const spy = vi.spyOn(reads, "readRows").mockImplementation((table, ids) => {
            const remember = (cols: FlatVector[]) => {
              if (table.columns.some((c) => c.name === "g")) {
                const g = cols[2];
                if (g) captured.push(g);
              }
              return cols;
            };
            const result = originalRead(table, ids);
            return result instanceof Promise ? result.then(remember) : remember(result);
          });
          try {
            for (const sql of [
              "CREATE TABLE p(id INT PRIMARY KEY)",
              `CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) ON DELETE SET NULL,g INT GENERATED ALWAYS AS (coalesce(pid,0)) STORED,CHECK(g>=${reject ? "1" : "0"}))`,
            ]) {
              await pg.exec(sql);
              await core.execute(sql);
            }
            const run = async (
              work: (
                s: Statement,
                grant: import("../memory/governor.js").Grant,
                bound: (id: number) => BoundTableRules,
              ) => Promise<void>,
            ) => {
              const catalog = core.harness.path.catalog;
              const programs = new Map(
                catalog.tables.map((t) => [
                  t.id,
                  compileTableRules(t, catalog.version, core.lowerer.lowerTableRules(t, catalog)),
                ]),
              );
              const minimum = [...programs.values()].reduce(
                (sum, p) => sum + tableRuleMinimumBytes(p),
                65536,
              );
              const grant = await core.harness.governor.admit({
                label: "test/FK-rules",
                minimumBytes: minimum,
                spillable: true,
              });
              const tx = await core.harness.path.begin();
              const bindings = new Map<number, BoundTableRules>();
              const rules = (id: number) => {
                let r = bindings.get(id);
                if (!r) {
                  const t = catalog.table(id);
                  if (!t) throw new Error("missing rule table");
                  r = compileTableRules(
                    t,
                    catalog.version,
                    core.lowerer.lowerTableRules(t, catalog),
                    { charge: grant },
                  );
                  bindings.set(id, r);
                }
                return r;
              };
              const s = tx.statement({ rules });
              try {
                await work(s, grant, rules);
                await s.end();
                await (
                  await tx.commit(null)
                ).durable;
              } catch (e) {
                s.rollback();
                tx.rollback();
                throw e;
              } finally {
                for (const b of bindings.values()) b.close();
                expect(grant.allocated).toBe(0);
                grant.close();
              }
            };
            await run(async (s, grant, rules) => {
              for (const [name, values] of [
                ["p", [1]],
                ["c", [1, 1, null]],
              ] as const) {
                const table = s.catalog.tableByName(name);
                if (!table) throw new Error("missing target");
                const before = grant.allocated;
                const cols = table.columns.map((c, i) =>
                  bindParameter(c.type, values[i] ?? null, i, grant),
                );
                const bytes = grant.allocated - before;
                try {
                  rules(table.id).generate(
                    { length: 1, columns: cols, selection: null },
                    table.columns.map((c) => c.id),
                  );
                  await s
                    .table(table.id)
                    .insert({ length: 1, columns: cols, selection: null }, null);
                } finally {
                  grant.free(bytes);
                }
              }
            });
            await pg.exec("INSERT INTO p VALUES(1);INSERT INTO c(id,pid) VALUES(1,1)");
            if (checkpoint) {
              await core.harness.path.recycleLog();
              await core.reopen();
              reach.reach("published source");
            } else reach.reach("resident source");
            const authority = await state(() => pg.exec("DELETE FROM p WHERE id=1"));
            const actual = await state(() =>
              run(async (s, grant) => {
                const p = s.catalog.tableByName("p");
                if (p?.primaryKey === undefined || p.primaryKey === null)
                  throw new Error("missing PK");
                const encoded = new KeyBatch(grant);
                try {
                  encodeKeyBatch(
                    "equality",
                    [{ vector: ints([1]), direction: "asc", nulls: "first" }],
                    { length: 1, selection: null },
                    encoded,
                  );
                  const out = {
                    probes: new Int32Array(1),
                    locators: new Float64Array(1),
                    count: 0,
                  };
                  await s.view.index(p.id, p.primaryKey).probe(encoded, out);
                  expect(out.count).toBe(1);
                  await s
                    .table(p.id)
                    .delete(
                      batch(locators(Array.from(out.locators.subarray(0, out.count)), p.id)),
                      null,
                    );
                } finally {
                  encoded.close();
                }
              }),
            );
            expect(actual).toBe(authority);
            expect(authority).toBe(reject ? "23514" : null);
            expect(captured.length).toBeGreaterThan(0);
            for (const v of captured) {
              if (v.data.layout !== "i32") throw new Error("wrong OLD layout");
              expect(v.data.values[0]).toBe(1);
            }
            reach.reach("OLD wrapper preserved");
            reach.reach(reject ? "CHECK rollback" : "generated recomputed");
            const result = await core.execute("SELECT id,pid,g FROM c ORDER BY id");
            if (!("batches" in result)) throw new Error("missing rows");
            expect(oracleRows(result)).toEqual(
              (await pg.query("SELECT id,pid,g FROM c ORDER BY id")).rows,
            );
            const parents = await core.execute("SELECT id FROM p");
            if (!("batches" in parents)) throw new Error("missing parent rows");
            expect(oracleRows(parents)).toEqual((await pg.query("SELECT id FROM p")).rows);
            reach.reach("statement atomicity");
          } finally {
            spy.mockRestore();
            await core.close();
            await pg.exec("DROP TABLE IF EXISTS c,p");
          }
        }),
        {
          seed,
          numRuns: 8,
          examples: [
            [false, false],
            [true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
