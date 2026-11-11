import type { Rel, ScalarExpr } from "../ir/ir.js";
import { BIG, vectorOf } from "../exec/join-fixtures.js";
/** Compiler recognition must preserve the complete ordered SQL result against PostgreSQL. */
import { PGlite } from "@electric-sql/pglite";
import { beforeAll, afterAll, it, expect, vi } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createReach, WINDOW_TOPK_SQL_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { rankingBounds } from "../exec/window-topk-plan.js";
import { WindowOperator } from "../exec/window.js";
import { createExecutor } from "../exec/create.js";
const reach = createReach("bounded ranking SQL", WINDOW_TOPK_SQL_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});
function scalar(v: unknown): string {
  if (v === null) return "NULL";
  if (Object.is(v, -0)) return "-0";
  if (
    typeof v === "number" ||
    typeof v === "string" ||
    typeof v === "bigint" ||
    typeof v === "boolean" ||
    v === undefined
  )
    return String(v);
  throw new TypeError("expected scalar SQL value");
}
const canon = (rows: ReadonlyArray<readonly unknown[]>) => rows.map((row) => row.map(scalar));
for (const seed of seedsFor("next-window-topk-sql", [0xb720, 0xb721]))
  it(`rank bounds execute the original SQL (${String(seed)})`, async () => {
    const random = mulberry32(seed),
      core = await new StorageCore(64).open();
    try {
      await core.execute("CREATE TABLE wt(p text,k float8,id int)");
      await pg.exec("DROP TABLE IF EXISTS wt;CREATE TABLE wt(p text,k float8,id int)");
      const data = Array.from({ length: 23 }, (_, id) => ({
        p: id % 4 === 0 ? null : `p${String(id % 3)}`,
        k: id % 7 === 0 ? NaN : id % 5 === 0 ? Infinity : id % 11 === 0 ? -Infinity : id % 4,
        id,
      }));
      await core.writeBatch("insertBatch", "wt", data);
      const params: unknown[] = [];
      await pg.query(
        `INSERT INTO wt VALUES ${data
          .map(
            (row) =>
              `(${[row.p, row.k, row.id]
                .map((v) => {
                  params.push(v);
                  return "$" + String(params.length);
                })
                .join(",")})`,
          )
          .join(",")}`,
        params,
      );
      for (const fn of ["rank", "row_number"] as const)
        for (const frame of ["", " ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"])
          for (const empty of [false, true]) {
            reach.beginHistory();
            const k = 2 + Math.floor(random() * 4),
              desc = random() < 0.5;
            const reverse = empty,
              strict = fn === "row_number";
            const predicate = reverse
              ? `${String(k)} ${strict ? ">" : ">="} r`
              : `r ${strict ? "<" : "<="} ${String(k)}`;
            const sql = `SELECT p,k,id,r FROM (SELECT p,k,id,${fn}() OVER(PARTITION BY p ORDER BY k ${desc ? "DESC" : "ASC"} NULLS LAST${fn === "row_number" ? ",id" : ""}${frame}) r FROM wt ${empty ? "WHERE false" : ""}) t WHERE ${predicate} ORDER BY p NULLS LAST,r,id`;
            const expected = (await pg.query(sql, [], { rowMode: "array" })).rows;
            const bounds = rankingBounds(core.prepare(sql).plan.root);
            // PostgreSQL windowfuncs.c SupportRequestOptimizeWindowClause ignores rank frames;
            // reviewed lower/windows.ts canonicalizes them to null before this optimization.
            expect(bounds.size).toBe(1);
            const finish = Object.getOwnPropertyDescriptor(WindowOperator.prototype, "finish")
              ?.value as WindowOperator["finish"];
            for (const enabled of [false, true]) {
              let observed: number | undefined;
              const spy = vi.spyOn(WindowOperator.prototype, "finish").mockImplementation(function (
                this: WindowOperator,
              ) {
                observed = this.spec.topK;
                return finish.call(this);
              });
              const executor = createExecutor({ windowTopK: enabled });
              core.executor.prepare = executor.prepare.bind(executor);
              try {
                const result = await core.execute(sql);
                if (!("batches" in result)) throw new Error("not query");
                expect(canon(oracleRows(result).map((row) => Object.values(row)))).toEqual(
                  canon(expected as unknown[][]),
                );
                expect(observed).toBe(enabled ? k - (strict ? 1 : 0) : undefined);
              } finally {
                spy.mockRestore();
              }
              if (!enabled) reach.reach("unbounded control");
            }
            reach.reach(fn);
            reach.reach("aliases");
            reach.reach("ordered PG rows");
            if (frame) reach.reach("rank frame canonicalization");
            if (empty) {
              reach.reach("empty");
              reach.reach("reverse comparison");
            }
            if (strict) reach.reach("strict bound");
          }
      // The SQL optimizer may relocate computed SELECT expressions, and lowering may
      // split clauses. Exercise the conservative physical-IR guards themselves too.
      let window: Extract<Rel, { op: "window" }> | undefined;
      const locate = (rel: Rel): void => {
        if (rel.op === "window") window = rel;
        if ("input" in rel) locate(rel.input);
      };
      locate(
        core.prepare("SELECT p,k,id,rank() OVER(PARTITION BY p ORDER BY k) r FROM wt").plan.root,
      );
      if (window === undefined) throw new Error("missing window");
      const w: Extract<Rel, { op: "window" }> = window;
      for (const state of ["computed guard", "multiple functions", "shared consumer"] as const) {
        reach.beginHistory();
        const exprs: ScalarExpr[] = w.output.map((column, index) => ({
          op: "column",
          index,
          type: column.type,
        }));
        if (state === "computed guard") {
          const first = exprs[0];
          if (first === undefined) throw new Error("missing field");
          exprs[0] = { op: "cast", input: first, type: first.type };
        }
        const child: Rel =
          state === "multiple functions"
            ? {
                ...w,
                functions: [...w.functions, { fn: "row_number", args: [], frame: null, type: BIG }],
                output: [...w.output, { name: "extra", type: BIG, nullable: false }],
              }
            : w;
        const project: Rel = { op: "project", id: 99991, input: child, exprs, output: w.output };
        const literal = vectorOf(BIG, [2 + (seed % 3)]);
        if (literal.form !== "flat") throw new Error("expected flat literal");
        const filter: Rel = {
          op: "filter",
          id: 99992,
          input: project,
          output: project.output,
          predicate: {
            op: "compare",
            cmp: "<=",
            left: { op: "column", index: w.input.output.length, type: BIG },
            right: { op: "literal", type: BIG, value: literal },
            collation: null,
            type: { kind: "bool" },
          },
        };
        const root: Rel =
          state === "shared consumer"
            ? {
                op: "setOp",
                kind: "union",
                all: true,
                id: 99993,
                inputs: [filter, w],
                output: w.output,
              }
            : filter;
        expect(rankingBounds(root).size).toBe(0);
        reach.reach(state);
      }
    } finally {
      await core.close();
    }
  }, 60000);
