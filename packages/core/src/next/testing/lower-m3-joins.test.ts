/** SQL -> actual storage/executor, independently compared with PostgreSQL and the shipped engine. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { currentEngine } from "../../testing/differential-current.js";
import {
  compareEngines,
  type EngineUnderTest,
  type Operation,
  type Workload,
} from "../../testing/differential-harness.js";
import { createReach, LOWER_M3_JOINS_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { nextEngine } from "./differential-adapter.js";
import { KEYLESS_FULL_AUTHORITY } from "./divergences.js";
import { StorageCore } from "./storage-core.js";

async function postgres(): Promise<EngineUnderTest> {
  const pg = new PGlite();
  await pg.waitReady;
  return {
    async run(operation) {
      if (operation.kind === "schema") {
        await pg.exec(
          `CREATE TABLE ${operation.schema.name} (id DOUBLE PRECISION PRIMARY KEY, k DOUBLE PRECISION, v DOUBLE PRECISION, label TEXT)`,
        );
        return { kind: "schema" };
      }
      if (operation.kind === "insertBatch") {
        const params: unknown[] = [];
        const values = operation.rows.map((row) => {
          const start = params.length;
          params.push(row.id, row.k, row.v, row.label);
          return `($${String(start + 1)},$${String(start + 2)},$${String(start + 3)},$${String(start + 4)})`;
        });
        if (values.length)
          await pg.query(`INSERT INTO ${operation.table} VALUES ${values.join(",")}`, params);
        return { kind: "insertBatch", counts: { rowCount: operation.rows.length } };
      }
      if (operation.kind === "reopen") return { kind: "reopen" };
      if (operation.kind !== "sql") throw new Error("unexpected PostgreSQL operation");
      let sql = operation.sql;
      const keyless =
        /^SELECT p.id AS pid, b.id AS bid FROM (p_\d+) p FULL JOIN (b_\d+) b ON p.v > b.v ORDER BY pid NULLS FIRST, bid NULLS FIRST$/u.exec(
          sql,
        );
      if (keyless !== null) {
        await expect(pg.query(sql)).rejects.toMatchObject({
          code: KEYLESS_FULL_AUTHORITY.sqlState,
        });
        const [, p = "", b = ""] = keyless;
        sql = `SELECT p.id AS pid,b.id AS bid FROM ${p} p LEFT JOIN ${b} b ON p.v>b.v UNION ALL SELECT NULL AS pid,b.id AS bid FROM ${b} b WHERE NOT EXISTS(SELECT 1 FROM ${p} p WHERE p.v>b.v) ORDER BY pid NULLS FIRST,bid NULLS FIRST`;
      }
      const result = await pg.query<Record<string, unknown>>(sql);
      return {
        kind: "rows",
        columns: result.fields.map((field) => field.name),
        domains: result.fields.map(() => null),
        rows: result.rows,
      };
    },
    close: () => pg.close(),
  };
}

const reach = createReach("next lower M3 joins", LOWER_M3_JOINS_REACH, { minimum: 2 });
for (const seed of seedsFor("next-lower-m3-joins", [0x3a71, 0x3a72]))
  it(`executes generated join SQL against both independent engines, seed ${String(seed)}`, async () => {
    const random = mulberry32(seed);
    const operations: Operation[] = [];
    const states = new Map<Operation, string>();
    for (let history = 0; history < 8; history++) {
      const names = ["p", "b", "d"].map((name) => `${name}_${String(history)}`);
      const p = names[0] ?? "",
        b = names[1] ?? "",
        d = names[2] ?? "";
      for (let side = 0; side < names.length; side++) {
        const name = names[side] ?? "";
        operations.push({
          kind: "schema",
          schema: {
            name,
            uniqueKey: "id",
            columns: [
              { name: "id", type: "number" },
              { name: "k", type: "number", nullable: true },
              { name: "v", type: "number", nullable: true },
              { name: "label", type: "string", nullable: true },
            ],
          },
        });
        const n =
          history === 0 || (history === 1 && side === 1) || (history === 2 && side === 0)
            ? 0
            : 7 + Math.floor(random() * 9);
        const rows = Array.from({ length: n }, (_, id) => ({
          id,
          k: id % 4 === 0 ? null : id % 3,
          v: id % 3 === 0 ? null : Math.floor(random() * 7) - 3,
          label: id % 5 === 0 ? null : `v${String(id % 3)}`,
        }));
        if (rows.length) operations.push({ kind: "insertBatch", table: name, rows });
      }
      const pair = (join: string, on = "p.k = b.k") =>
        `SELECT p.id AS pid, b.id AS bid FROM ${p} p ${join} ${b} b${join === "CROSS JOIN" ? "" : ` ON ${on}`} ORDER BY pid NULLS FIRST, bid NULLS FIRST`;
      const queries: Array<[string, string]> = [
        ["inner", pair("INNER JOIN")],
        [
          "USING",
          `SELECT k, p.id AS pid, b.id AS bid FROM ${p} p FULL JOIN ${b} b USING(k) ORDER BY k NULLS FIRST,pid NULLS FIRST,bid NULLS FIRST`,
        ],
        [
          "USING chain",
          `SELECT k, p.id AS pid, b.id AS bid, d.id AS did FROM ${p} p FULL JOIN ${b} b USING(k) FULL JOIN ${d} d USING(k) ORDER BY k NULLS FIRST,pid NULLS FIRST,bid NULLS FIRST,did NULLS FIRST`,
        ],
        [
          "NATURAL",
          `SELECT id,k,v,label FROM ${p} p NATURAL FULL JOIN ${b} b ORDER BY id NULLS FIRST,k NULLS FIRST,v NULLS FIRST,label NULLS FIRST`,
        ],

        ["left", pair("LEFT JOIN")],
        ["right", pair("RIGHT JOIN")],
        ["full", pair("FULL JOIN")],
        ["cross", pair("CROSS JOIN")],
        ["residual", pair("INNER JOIN", "p.k = b.k AND b.v > p.v")],
        ["outer residual", pair("LEFT JOIN", "p.k = b.k AND b.v > p.v")],
        ["full residual", pair("FULL JOIN", "p.k = b.k AND b.v > p.v")],
        ["keyless full", pair("FULL JOIN", "p.v > b.v")],
        ["null safe", pair("INNER JOIN", "p.k IS NOT DISTINCT FROM b.k")],
        [
          "outer WHERE",
          `SELECT p.id AS pid, b.id AS bid FROM ${p} p LEFT JOIN ${b} b ON p.k=b.k WHERE b.v IS NULL ORDER BY pid, bid NULLS FIRST`,
        ],
        [
          "semi",
          `SELECT p.id FROM ${p} p WHERE EXISTS (SELECT 1 FROM ${b} b WHERE b.k=p.k AND b.v>p.v) ORDER BY p.id`,
        ],
        [
          "anti",
          `SELECT p.id FROM ${p} p WHERE NOT EXISTS (SELECT 1 FROM ${b} b WHERE b.k=p.k AND b.v>p.v) ORDER BY p.id`,
        ],
        ["IN", `SELECT p.id FROM ${p} p WHERE p.k IN (SELECT b.k FROM ${b} b) ORDER BY p.id`],
        [
          "NOT IN",
          `SELECT p.id FROM ${p} p WHERE p.k NOT IN (SELECT b.k FROM ${b} b) ORDER BY p.id`,
        ],
        [
          "correlated NOT IN",
          `SELECT p.id FROM ${p} p WHERE p.k NOT IN (SELECT b.k FROM ${b} b WHERE b.v>p.v) ORDER BY p.id`,
        ],
        ["mark", `SELECT p.id, p.k IN (SELECT b.k FROM ${b} b) AS found FROM ${p} p ORDER BY p.id`],
        [
          "EXISTS mark",
          `SELECT p.id, EXISTS (SELECT 1 FROM ${b} b WHERE b.k=p.k) AS found FROM ${p} p ORDER BY p.id`,
        ],
        [
          "uncorrelated EXISTS",
          `SELECT p.id FROM ${p} p WHERE EXISTS (SELECT 1 FROM ${b} b WHERE b.v>0) ORDER BY p.id`,
        ],
        [
          "uncorrelated anti",
          `SELECT p.id FROM ${p} p WHERE NOT EXISTS (SELECT 1 FROM ${b} b ORDER BY b.id LIMIT 1) ORDER BY p.id`,
        ],
        [
          "limited NOT IN",
          `SELECT p.id FROM ${p} p WHERE p.k NOT IN (SELECT b.k FROM ${b} b ORDER BY b.id LIMIT 3) ORDER BY p.id`,
        ],
        [
          "OR EXISTS",
          `SELECT p.id FROM ${p} p WHERE p.v<0 OR EXISTS (SELECT 1 FROM ${b} b WHERE b.v>0) ORDER BY p.id`,
        ],
        [
          "star",
          `SELECT p.id AS pid, b.id AS bid, d.id AS did FROM ${p} p JOIN ${b} b ON p.k=b.k JOIN ${d} d ON d.k=b.k WHERE p.v<d.v ORDER BY pid,bid,did`,
        ],
      ];
      for (const join of ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN"]) {
        queries.push([
          "key-only",
          pair(join).replace("p.id AS pid, b.id AS bid", "p.k AS pid,b.k AS bid"),
        ]);
        queries.push([
          "zero-column",
          pair(join).replace("p.id AS pid, b.id AS bid", "1 AS pid,1 AS bid"),
        ]);
      }
      const addQueries = () => {
        for (const [state, sql] of queries) {
          const operation: Operation = {
            kind: "sql",
            sql,
            totalOrder: "projected unique source-row identifiers",
          };
          operations.push(operation);
          states.set(operation, state);
        }
      };
      addQueries();
      if (history === 3) {
        operations.push({ kind: "reopen" });
        addQueries();
      }
    }
    const workload: Workload = { name: `lower M3 joins ${String(seed)}`, operations };
    const core = await new StorageCore(8192).open();
    const candidate = await nextEngine(async () => core)("memory");
    const run = candidate.run.bind(candidate);
    candidate.run = async (operation) => {
      if (operation.kind === "reopen") {
        await core.harness.path.recycleLog();
        await core.harness.path.quiesce();
      }
      const result = await run(operation);
      return result;
    };
    const pg = await postgres(),
      current = await currentEngine("memory");
    const currentRun = current.run.bind(current);
    // The shipped parser accepts FULL only as the sole join. Parenthesizing the first join
    // gives the same SQL bag, including NULL-key misses, through that older execution path.
    current.run = (operation) => {
      if (
        operation.kind === "sql" &&
        operation.sql.includes("FULL JOIN") &&
        operation.sql.includes("AS did")
      ) {
        const match = /FROM (p_\d+) p FULL JOIN (b_\d+) b USING\(k\) FULL JOIN (d_\d+) d/u.exec(
          operation.sql,
        );
        if (match !== null)
          return currentRun({
            ...operation,
            sql: `SELECT k,t.pid,t.bid,d.id AS did FROM (SELECT k,p.id AS pid,b.id AS bid FROM ${match[1] ?? ""} p FULL JOIN ${match[2] ?? ""} b USING(k)) t FULL JOIN ${match[3] ?? ""} d USING(k) ORDER BY k NULLS FIRST,pid NULLS FIRST,bid NULLS FIRST,did NULLS FIRST`,
          });
      }
      return currentRun(
        operation.kind === "sql" && operation.sql.includes("NATURAL FULL JOIN")
          ? {
              ...operation,
              sql: operation.sql
                .replace("NATURAL FULL JOIN", "FULL JOIN")
                .replace(/ b ORDER BY/u, " b USING(id,k,v,label) ORDER BY"),
            }
          : operation,
      );
    };
    let checked = 0;
    reach.beginHistory();
    try {
      await compareEngines(workload, [pg, candidate, current], (outcome) => {
        if (!outcome.ok) throw new Error("matching refusals do not prove join execution");
        const operation = operations[checked++];
        if (operation === undefined) throw new Error("missing operation");
        const state = states.get(operation);
        if (state !== undefined) reach.reach(state as (typeof LOWER_M3_JOINS_REACH)[number]);
        if (operation.kind === "sql") {
          if (operation.sql.includes("p_0")) {
            reach.reach("empty probe");
            reach.reach("empty build");
          }
          if (operation.sql.includes("p_3")) {
            reach.reach("NULL");
            reach.reach("duplicates");
          }
          if (core.lastProfile?.operators.some((profile) => profile.pagesRead > 0))
            reach.reach("flushed");
        }
        if (operation.kind === "reopen") reach.reach("reopened");
      });
      expect(checked).toBe(operations.length);
      reach.reach("complete");
    } finally {
      await pg.close();
      await current.close();
      await candidate.close();
    }
  }, 180_000);

it("reaches every declared join SQL dimension", () => reach.assertReached());
