/** The general cursor/fetch recipe is funded by DML's complete producer minimum.
 * Query roots retain the diagnostic opt-in; LIMIT zero cannot bypass prepare-time refusal. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, MODIFY_INDEX_GATE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { currentEngine } from "../../testing/differential-current.js";
import type { PhysicalPlan, Rel } from "../ir/ir.js";
import { createExecutor } from "../exec/create.js";
import { UnsupportedPlanError } from "../errors.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
function findIndex(rel: Rel): Extract<Rel, { op: "keyLookup" | "indexRange" }> | null {
  if (rel.op === "keyLookup" || rel.op === "indexRange") return rel;
  if ("input" in rel) return findIndex(rel.input);
  if ("left" in rel) return findIndex(rel.left) ?? findIndex(rel.right);
  if ("inputs" in rel)
    for (const input of rel.inputs) {
      const found = findIndex(input);
      if (found) return found;
    }
  return null;
}
for (const seed of seedsFor("next-modify-index-gate", [0xb609, 0xb60a]))
  it(`keeps query index admission diagnostic while executing DML inputs seed ${String(seed)}`, async () => {
    const reach = createReach("DML-only index admission", MODIFY_INDEX_GATE_REACH),
      core = await new StorageCore(8).open(),
      pg = new PGlite(),
      shipped = await currentEngine("memory");
    const context = core.context.bind(core);
    core.context = () => ({
      ...context(),
      statistics: { table: () => ({ rowCount: 1000000, column: () => null }) },
    });
    const all = async (sql: string) => {
      await core.execute(sql);
      await pg.exec(sql);
      await shipped.run({ kind: "sql", sql });
    };
    const bag = (rows: readonly unknown[]) =>
      [...rows].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("update", "delete", "insert", "upsert"),
          fc.constantFrom("keyLookup", "indexRange"),
          fc.integer({ min: 6, max: 12 }),
          fc.integer({ min: 1, max: 3 }),
          fc.integer({ min: 1, max: 7 }),
          fc.boolean(),
          async (form, route, count, first, delta, zero) => {
            reach.beginHistory();
            await all("DROP TABLE IF EXISTS dst");
            await all("DROP TABLE IF EXISTS src");
            await all("CREATE TABLE src(id INTEGER PRIMARY KEY,v INTEGER)");
            await all("CREATE INDEX src_v ON src(v)");
            await all("CREATE TABLE dst(id INTEGER PRIMARY KEY,v INTEGER)");
            await all(
              `INSERT INTO src VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)})`).join(",")}`,
            );
            if (form === "upsert") await all("INSERT INTO dst SELECT * FROM src");
            const predicate =
              route === "keyLookup"
                ? `id IN(${String(first)},${String(first + 1)},${String(first)})`
                : `v>=${String(first)} AND v<${String(first + 2)}`;
            const sql =
              form === "update"
                ? `UPDATE src SET v=v+${String(delta)} WHERE ${predicate} RETURNING id,v`
                : form === "delete"
                  ? `DELETE FROM src WHERE ${predicate} RETURNING id,v`
                  : `INSERT INTO dst SELECT id,v+${String(delta)} FROM src WHERE ${predicate}${form === "upsert" ? " ON CONFLICT(id) DO UPDATE SET v=EXCLUDED.v" : ""} RETURNING id,v`;
            const lowered = core.lowerer.lower(sql, core.context());
            if (lowered.kind !== "modify" || lowered.plan.root.op !== "modify")
              throw new Error("Expected native modify");
            const input = findIndex(lowered.plan.root.input);
            expect(input?.op).toBe(route);
            if (!input) throw new Error("Missing planned index input");
            const output = input.output.slice(0, input.columns.length),
              selected: Rel = { ...input, emitLocator: false, output },
              select: PhysicalPlan = {
                ...lowered.plan,
                columns: output,
                root: {
                  op: "limit",
                  id: 1000000,
                  input: selected,
                  output,
                  limit: { kind: "const", value: zero ? 0 : 3 },
                  offset: null,
                },
              };
            for (const options of [{}, { keyOrder: false }] as const) {
              const executor = createExecutor(options);
              let refused = false;
              try {
                executor.prepare(select);
              } catch (error) {
                expect(error).toBeInstanceOf(UnsupportedPlanError);
                expect((error as UnsupportedPlanError).construct).toBe(route);
                refused = true;
              }
              expect(refused, "SELECT index cursor must remain diagnostic-only").toBe(true);
              expect(() => executor.prepare(lowered.plan)).not.toThrow();
            }
            // Branching input needs its own simultaneous-source admission proof. A
            // MODIFY ancestor alone cannot authorize the cursor under a join/set input.
            const empty: Rel = {
              id: 1000001,
              op: "values",
              output: lowered.plan.root.input.output,
              rows: [],
            };
            const joined: Rel = {
              id: 1000002,
              op: "join",
              kind: "semi",
              algorithm: "nestedLoop",
              left: lowered.plan.root.input,
              right: empty,
              output: empty.output,
              leftKeys: [],
              rightKeys: [],
              nullSafe: [],
              residual: null,
              publishes: [],
            };
            const except: Rel = {
              id: 1000003,
              op: "setOp",
              kind: "except",
              all: false,
              inputs: [lowered.plan.root.input, empty],
              output: empty.output,
            };
            for (const branch of [
              joined,
              { ...joined, left: empty, right: lowered.plan.root.input },
              ...(form === "insert" || form === "upsert" ? [except] : []),
            ]) {
              const nested: PhysicalPlan = {
                ...lowered.plan,
                root: { ...lowered.plan.root, input: branch },
              };
              for (const keyOrder of [undefined, false]) {
                let refused = false;
                try {
                  createExecutor(keyOrder === undefined ? {} : { keyOrder }).prepare(nested);
                } catch (error) {
                  expect(error).toBeInstanceOf(UnsupportedPlanError);
                  expect((error as UnsupportedPlanError).construct).toBe(route);
                  refused = true;
                }
                expect(refused, "nested branch index must remain diagnostic-only").toBe(true);
              }
              expect(() => createExecutor({ keyOrder: true }).prepare(nested)).not.toThrow();
              reach.reach(branch.op === "join" ? "nested join refused" : "nested set refused");
            }
            expect(() => createExecutor({ keyOrder: true }).prepare(select)).not.toThrow();
            const actual = await core.execute(sql),
              expected = await pg.query(sql),
              old = await shipped.run({ kind: "sql", sql });
            if (!("returning" in actual)) throw new Error("Expected RETURNING");
            expect(bag(oracleRows(actual.returning))).toEqual(bag(expected.rows));
            expect(bag(oracleRows(actual.returning))).toEqual(bag(old.rows ?? []));
            for (const name of ["src", "dst"]) {
              const query = `SELECT * FROM ${name} ORDER BY id`,
                rows = await core.execute(query);
              if (rows.kind !== "rows" || !("batches" in rows)) throw new Error("Expected rows");
              expect(oracleRows(rows)).toEqual((await pg.query(query)).rows);
              expect(oracleRows(rows)).toEqual(
                (await shipped.run({ kind: "sql", sql: query })).rows,
              );
            }
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach(form);
            reach.reach(route);
            reach.reach(zero ? "zero SELECT limit" : "positive SELECT limit");
            reach.reach("query unset refused");
            reach.reach("query false refused");
            reach.reach("query opt-in prepared");
            reach.reach("DML unset admitted");
            reach.reach("DML false admitted");
            reach.reach("PG and shipped rows");
            reach.reach("zero spill");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            ["update", "keyLookup", 6, 1, 2, true],
            ["delete", "indexRange", 8, 2, 3, false],
            ["insert", "indexRange", 7, 1, 1, true],
            ["upsert", "keyLookup", 12, 3, 2, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
      await shipped.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  }, 120000);
