/** PostgreSQL is the authority; the READ ONLY admission adapter is B5's contract, not its owner implementation. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createReach, ROW_LOCK_INTENT_STATES } from "../../testing/reach.js";
import { LoweredDatabase } from "./lowering-harness.js";
import { pgOutcome, nextOutcome } from "./pglite-oracle.js";

const LOCKS = [
  ["KEY SHARE", "keyShare"],
  ["SHARE", "share"],
  ["NO KEY UPDATE", "noKeyUpdate"],
  ["UPDATE", "update"],
] as const;
type Intent = "none" | (typeof LOCKS)[number][1];
const pg = new PGlite();
const next = new LoweredDatabase();
const ddl = [
  "CREATE TABLE t (id integer PRIMARY KEY)",
  "CREATE TABLE u (id integer PRIMARY KEY)",
  "INSERT INTO t VALUES (1),(2)",
  "INSERT INTO u VALUES (1)",
  "CREATE VIEW vp AS SELECT * FROM t",
  "CREATE VIEW vc AS SELECT 1 AS id",
  "CREATE VIEW vx AS WITH c AS (SELECT * FROM t) SELECT * FROM c",
  ...LOCKS.flatMap(([lock], i) => [
    `CREATE VIEW vl${String(i)} AS SELECT * FROM t FOR ${lock}`,
    `CREATE VIEW vn${String(i)} AS SELECT * FROM vl${String(i)}`,
  ]),
];
beforeAll(async () => {
  for (const sql of ddl) {
    await pg.exec(sql);
    next.execute(sql);
  }
});
afterAll(async () => {
  await pg.close();
});

function intent(sql: string): Intent | undefined {
  return (next.lower(sql) as { readonly rowLocking?: Intent }).rowLocking;
}

describe("effective row-lock intent for the owner", () => {
  it("matches READ ONLY rows versus 25006 across targets, CTEs and expanded views", async () => {
    const reach = createReach<string>("row-lock intent", ROW_LOCK_INTENT_STATES);
    const failures: string[] = [];
    for (const [lock, expected] of LOCKS) {
      const shapes: ReadonlyArray<readonly [string, string, Intent]> = [
        ["base relation", `SELECT * FROM t FOR ${lock}`, expected],
        ["base relation", `SELECT * FROM t WHERE id = 1 FOR ${lock}`, expected],
        ["base relation", `SELECT * FROM t WHERE false FOR ${lock}`, expected],
        ["FROM propagation", `SELECT * FROM (SELECT * FROM t) s FOR ${lock}`, expected],
        [
          "FROM propagation",
          `SELECT * FROM (SELECT * FROM (SELECT * FROM t) a) s FOR ${lock}`,
          expected,
        ],
        ["constant source", `SELECT 1 AS id FOR ${lock}`, "none"],
        ["constant source", `SELECT * FROM (SELECT 1 AS id) s FOR ${lock}`, "none"],
        ["constant source", `SELECT * FROM generate_series(1,2) s FOR ${lock}`, "none"],
        ["CTE boundary", `WITH c AS (SELECT * FROM t) SELECT * FROM c FOR ${lock}`, "none"],
        [
          "CTE boundary",
          `WITH c AS MATERIALIZED (SELECT * FROM t) SELECT * FROM c FOR ${lock}`,
          "none",
        ],
        [
          "CTE boundary",
          `WITH c AS (SELECT * FROM t) SELECT * FROM (SELECT * FROM c) s FOR ${lock}`,
          "none",
        ],
        ["CTE body", `WITH c AS (SELECT * FROM t FOR ${lock}) SELECT * FROM c`, expected],
        [
          "CTE body",
          `WITH c AS (SELECT * FROM t FOR ${lock}) SELECT * FROM (SELECT * FROM c) s`,
          expected,
        ],
        ["unused CTE", `WITH c AS (SELECT * FROM t FOR ${lock}) SELECT 1 AS id`, "none"],
        ["unused CTE", `WITH c AS (SELECT * FROM vl3) SELECT 1 AS id`, "none"],
        ["OF target", `SELECT t.id FROM t, (SELECT 1 AS n) s FOR ${lock} OF s`, "none"],
        ["OF target", `SELECT t.id FROM t, (SELECT 1 AS n) s FOR ${lock} OF t`, expected],
        ["OF target", `SELECT * FROM (SELECT * FROM t) s FOR ${lock} OF s`, expected],
        ["view target", `SELECT * FROM vp FOR ${lock}`, expected],
        // A view is itself an RTE_RELATION: PG refuses its outer lock even for a constant/CTE body.
        ["view target", `SELECT * FROM vc FOR ${lock}`, expected],
        ["view target", `SELECT * FROM vx FOR ${lock}`, expected],
      ];
      for (let i = 0; i < LOCKS.length; i++) {
        const own = LOCKS[i]?.[1];
        if (own === undefined) throw new Error("lock fixture");
        const ownClause = LOCKS[i]?.[0];
        if (ownClause === undefined) throw new Error("lock fixture");
        await check(
          "unused stronger CTE",
          `WITH unused AS (SELECT * FROM t FOR ${ownClause}), c AS (SELECT * FROM t FOR ${lock}) SELECT * FROM c`,
          expected,
        );
        await check(
          "CTE body",
          `WITH c AS (SELECT * FROM t FOR ${ownClause}) SELECT * FROM c FOR ${lock}`,
          own,
        );
        await check(
          "CTE body",
          `WITH c AS (SELECT * FROM vl${String(i)}) SELECT * FROM c FOR ${lock}`,
          own,
        );
        for (const [state, sql, wanted] of [
          ["view clause", `SELECT * FROM vl${String(i)}`, own],
          ["nested view", `SELECT * FROM vn${String(i)}`, own],
          ["view clause", `WITH c AS (SELECT * FROM vl${String(i)}) SELECT * FROM c`, own],
          [
            "strongest",
            `SELECT * FROM vl${String(i)} FOR ${lock}`,
            LOCKS[
              Math.max(
                i,
                LOCKS.findIndex(([, x]) => x === expected),
              )
            ]?.[1] ?? "none",
          ],
        ] as const) {
          await check(state, sql, wanted);
        }
      }
      for (const [state, sql, wanted] of shapes) await check(state, sql, wanted);
      for (const [other, otherIntent] of LOCKS) {
        const index = Math.max(
          LOCKS.findIndex(([, x]) => x === expected),
          LOCKS.findIndex(([, x]) => x === otherIntent),
        );
        await check(
          "strongest",
          `SELECT * FROM t FOR ${lock} FOR ${other}`,
          LOCKS[index]?.[1] ?? "none",
        );
      }
    }
    expect(failures.join("\n")).toBe("");
    reach.assertReached();
    async function check(state: string, sql: string, wanted: Intent): Promise<void> {
      reach.beginHistory();
      await pg.exec("BEGIN READ ONLY");
      const expected = await pgOutcome(pg, sql);
      await pg.exec("ROLLBACK");
      if ("error" in expected) expect(expected.error, sql).toBe("25006");
      const lock = intent(sql);
      const actual =
        lock === undefined
          ? { error: "missing intent" }
          : lock === "none"
            ? nextOutcome(next, sql)
            : { error: "25006" };
      if (JSON.stringify(expected) !== JSON.stringify(actual) || lock !== wanted)
        failures.push(
          `${sql}\nPG ${JSON.stringify(expected)}; intent ${String(lock)} wanted ${wanted}; next ${JSON.stringify(actual)}`,
        );
      else reach.reach(state);
    }
  });

  it("announces no lock on utilities/batch writes and retains query locks in modify/CTAS", () => {
    for (const sql of ["SELECT 1", "BEGIN", "SET TRANSACTION READ ONLY", "SHOW timezone"])
      expect(intent(sql), sql).toBe("none");
    expect(
      next.lowerer.lowerWrite({ table: "u", op: "insert", names: ["id"] }, next.context()),
    ).toMatchObject({ rowLocking: "none" });
    expect(next.lower("SELECT * FROM t WHERE id = 1 FOR UPDATE")).toMatchObject({
      kind: "query",
      pointRead: true,
      rowLocking: "update",
    });
    expect(intent("INSERT INTO u SELECT id FROM t FOR SHARE")).toBe("share");
    expect(intent("CREATE TABLE copied AS SELECT * FROM t FOR UPDATE")).toBe("update");
  });
});
