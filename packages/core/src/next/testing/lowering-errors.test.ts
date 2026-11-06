/**
 * Every failure of `Lowerer.lower` carries PostgreSQL's SQLSTATE (lower/errors.ts): syntax errors
 * (42601), unknown relations (42P01), columns (42703), functions (42883) and types (42704), and
 * the other compile-error classes the front end raises. Each case runs in PGlite and through
 * lowering (pglite-oracle.ts), and the codes must match.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createReach, FRAME_OFFSET_STATES, HAVING_NAME_STATES } from "../../testing/reach.js";
import { PlanningError } from "../lower/errors.js";
import { check, nextOutcome, pgOutcome } from "./pglite-oracle.js";
import { LoweredDatabase } from "./lowering-harness.js";

let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);

afterAll(async () => {
  await pg.close();
});

const SETUP = [
  "CREATE TABLE a (id integer, v integer)",
  "CREATE TABLE b (id integer, v integer)",
  "CREATE VIEW va AS SELECT id FROM a",
  "CREATE SEQUENCE s",
];

describe("lowering's errors carry PostgreSQL's SQLSTATE", () => {
  it("syntax errors are 42601", async () => {
    await check(pg, SETUP, [
      { sql: "SELEC 1" },
      { sql: "SELECT FROM WHERE" },
      { sql: "SELECT (1" },
      { sql: "SELECT 1 +" },
      { sql: "SELECT 'abc" },
      { sql: "INSERT INTO a VALUES (1, 2" },
      { sql: "CREATE TABLE (id int)" },
      { sql: "SELECT id FROM a UNION SELECT id, v FROM b" },
    ]);
  });

  it("unknown relations are 42P01, wherever they are named", async () => {
    await check(pg, SETUP, [
      { sql: "SELECT * FROM missing" },
      { sql: "SELECT id FROM missing" },
      { sql: "SELECT * FROM a JOIN missing m ON m.id = a.id" },
      { sql: "SELECT x.id FROM a" },
      { sql: "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.id AND z.v = 1)" },
      { sql: "INSERT INTO missing VALUES (1)" },
      { sql: "UPDATE missing SET id = 1" },
      { sql: "DELETE FROM missing" },
      { sql: "DROP TABLE missing" },
      { sql: "DROP VIEW missing" },
      { sql: "CREATE INDEX i ON missing (id)" },
      { sql: "ALTER TABLE missing ADD COLUMN c integer" },
      { sql: "SELECT nextval('missing')" },
    ]);
  });

  it("unknown columns, functions and types: 42703, 42883, 42704", async () => {
    await check(pg, SETUP, [
      { sql: "SELECT nope FROM a" },
      { sql: "SELECT a.nope FROM a" },
      { sql: "UPDATE a SET nope = 1" },
      { sql: "INSERT INTO a (nope) VALUES (1)" },
      { sql: "SELECT id FROM a ORDER BY nope" },
      { sql: "CREATE INDEX i ON a (nope)" },
      { sql: "SELECT nofunc(1)" },
      { sql: "SELECT abs('x', 1)" },
      { sql: "SELECT length(1, 2, 3)" },
      { sql: "SELECT sum(id, id) FROM a" },
      { sql: "SELECT CAST(1 AS notatype)" },
      { sql: "SELECT 1::notatype" },
      { sql: "CREATE TABLE t (c notatype)" },
    ]);
  });

  it("the other compile-error classes", async () => {
    await check(pg, SETUP, [
      { sql: "SELECT id FROM a, b" },
      { sql: "CREATE TABLE t (c integer, c integer)" },
      { sql: "CREATE TABLE a (id integer)" },
      { sql: "CREATE VIEW va AS SELECT 1" },
      { sql: "CREATE SEQUENCE s" },
      { sql: "SELECT id FROM a GROUP BY v" },
      { sql: "SELECT sum(sum(id)) FROM a" },
      { sql: "SELECT id FROM a WHERE sum(id) > 1" },
      { sql: "SELECT 'x'::integer" },
      { sql: "SELECT id FROM a WHERE id = 'x'" },
    ]);
  });

  it("keeps the parser's position and the original error, and refuses with 0A000", () => {
    const next = new LoweredDatabase();
    for (const sql of SETUP) next.execute(sql);
    const failure = (sql: string): PlanningError => {
      try {
        next.lower(sql);
      } catch (error) {
        if (error instanceof PlanningError) return error;
        throw error;
      }
      throw new Error(`${sql} lowered`);
    };
    const syntax = failure("SELEC 1");
    expect([syntax.sqlState, syntax.position]).toEqual(["42601", { offset: 0, length: 5 }]);
    expect((syntax.cause as Error).name).toBe("SqlCompileError");
    // Shapes the front end refuses though PostgreSQL accepts them: feature not supported.
    expect(
      failure(
        "SELECT id FROM a GROUP BY id HAVING max(v) > (SELECT max(v) FROM b WHERE b.id = a.id)",
      ).sqlState,
    ).toBe("0A000");
    expect(failure("SELECT id FROM a WHERE id IN (SELECT a.id FROM b)").sqlState).toBe("0A000");
  });
});

describe("transaction modes (ruling R11.2, review B6-p2b-2 findings 1 and 2)", () => {
  /** PostgreSQL's access mode after `begin` (on / off), or its SQLSTATE. */
  const pgMode = async (begin: string): Promise<string> => {
    try {
      await pg.exec(begin);
      const shown = await pg.query<{ transaction_read_only: string }>("SHOW transaction_read_only");
      return shown.rows[0]?.transaction_read_only ?? "missing";
    } catch (error) {
      return String((error as { code?: string }).code);
    } finally {
      await pg.exec("ROLLBACK");
    }
  };
  const ourMode = (next: LoweredDatabase, begin: string): string => {
    try {
      const lowered = next.lower(begin);
      if (lowered.kind !== "transactionControl") return lowered.kind;
      return lowered.readOnly ? "on" : "off";
    } catch (error) {
      return error instanceof PlanningError ? error.sqlState : String(error);
    }
  };

  it("BEGIN and START TRANSACTION apply the last access mode written, as PostgreSQL", async () => {
    const next = new LoweredDatabase();
    for (const begin of [
      "BEGIN",
      "BEGIN READ ONLY",
      "BEGIN READ WRITE",
      "BEGIN READ ONLY READ WRITE",
      "BEGIN READ ONLY, READ WRITE",
      "BEGIN READ WRITE, READ ONLY",
      "BEGIN TRANSACTION READ ONLY",
      "BEGIN WORK ISOLATION LEVEL READ COMMITTED READ ONLY",
      "START TRANSACTION READ ONLY",
      "START TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY, NOT DEFERRABLE",
      "BEGIN -- READ ONLY\n",
      "BEGIN /* READ ONLY */ READ WRITE",
      "BEGIN 'READ ONLY'",
      "BEGIN ONLY READ",
      "BEGIN READ",
      "BEGIN READ ONLY,",
      "BEGIN , READ ONLY",
    ])
      expect(ourMode(next, begin), begin).toBe(await pgMode(begin));
  });

  it("SET TRANSACTION is a typed instruction carrying its modes, never a no-op", async () => {
    const next = new LoweredDatabase();
    for (const set of [
      "SET TRANSACTION READ ONLY",
      "SET TRANSACTION READ WRITE",
      "SET TRANSACTION READ ONLY, READ WRITE",
      "SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY",
    ]) {
      const lowered = next.lower(set);
      if (lowered.kind !== "transactionControl") throw new Error(`${set}: ${lowered.kind}`);
      expect(lowered.action, set).toBe("setTransaction");
      expect(lowered.controlsTransaction, set).toBe(true);
      // PostgreSQL's mode after BEGIN; SET TRANSACTION … inside the transaction.
      await pg.exec("BEGIN");
      await pg.exec(set);
      const shown = await pg.query<{ transaction_read_only: string }>("SHOW transaction_read_only");
      await pg.exec("ROLLBACK");
      expect(lowered.modes.readOnly ? "on" : "off", set).toBe(shown.rows[0]?.transaction_read_only);
    }
    const modes = next.lower("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    expect(modes.kind === "transactionControl" ? modes.modes : null).toEqual({
      readOnly: null,
      isolation: "repeatable read",
      deferrable: null,
    });
    // Forms that cannot be honoured are refused, not acknowledged.
    expect(ourMode(next, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")).toBe("0A000");
    expect(ourMode(next, "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")).toBe("0A000");
    expect(ourMode(next, "SET TRANSACTION")).toBe("42601");
  });
});

describe("HAVING names and frame offsets (review B6-p2b-3 finding 3)", () => {
  /** A fresh schema in both with `setup`; the lowered database. */
  const fixture = async (setup: readonly string[]): Promise<LoweredDatabase> => {
    await pg.exec("DROP SCHEMA IF EXISTS hf CASCADE; CREATE SCHEMA hf; SET search_path TO hf");
    const next = new LoweredDatabase();
    for (const sql of setup) {
      await pg.exec(sql);
      next.execute(sql);
    }
    return next;
  };
  const DATA = [
    "CREATE TABLE a (id integer, v integer)",
    "CREATE TABLE b (id integer, w integer)",
    "CREATE TABLE e (id integer, v integer)",
    "INSERT INTO a VALUES (1, 10), (2, 20), (2, 5)",
    "INSERT INTO b VALUES (1, 7)",
  ];

  it("a HAVING name is checked as PostgreSQL checks it: unknown 42703 before ungrouped 42803", async () => {
    const next = await fixture(DATA);
    const reach = createReach("HAVING names", HAVING_NAME_STATES);
    const names = ["id", "v", "nope", "NOPE", "a.id", "a.v", "a.nope", "z.v", "b.w"];
    const conditions = [
      (n: string) => `${n} > 0`,
      (n: string) => `0 < ${n}`,
      (n: string) => `${n} IS NOT NULL`,
      (n: string) => `sum(${n}) > 0`,
    ];
    const sources = ["a", "a JOIN b ON b.id = a.id"];
    const failures: string[] = [];
    for (const from of sources)
      for (const name of names)
        for (const condition of conditions) {
          reach.beginHistory();
          const sql = `SELECT a.id FROM ${from} GROUP BY a.id HAVING ${condition(name)} ORDER BY a.id`;
          const expected = await pgOutcome(pg, sql);
          const actual = nextOutcome(next, sql);
          if (JSON.stringify(actual) !== JSON.stringify(expected)) {
            failures.push(
              `${sql}\n  pglite: ${JSON.stringify(expected)}\n  next: ${JSON.stringify(actual)}`,
            );
            continue;
          }
          const state = "error" in expected ? expected.error : "accepted";
          reach.reach(
            state === "accepted"
              ? "accepted"
              : state === "42703"
                ? "unknown column"
                : state === "42803"
                  ? "ungrouped column"
                  : "unknown table",
          );
        }
    expect(failures.join("\n")).toBe("");
    reach.assertReached();
  });

  it("an invalid frame offset is 22004 or 22013, as PostgreSQL", async () => {
    const next = await fixture(DATA);
    const reach = createReach("frame offsets", FRAME_OFFSET_STATES);
    const offsets = [
      "-1",
      "- 2",
      "NULL",
      "0",
      "1",
      "'1'",
      "INTERVAL '1 day'",
      // Review B6-p2b-4: a sign is not a value. Signed zeros are zero; ROWS / GROUPS round a
      // numeric literal half away from zero before the sign check (-0.4 is 0, -0.5 is -1).
      "-0",
      "- 0",
      "-0.0",
      "-0e4",
      "+1",
      "0.4",
      "-0.4",
      "-0.5",
      "1.5",
    ];
    const bounds = [
      (o: string) => `BETWEEN ${o} PRECEDING AND CURRENT ROW`,
      (o: string) => `BETWEEN CURRENT ROW AND ${o} FOLLOWING`,
      (o: string) => `${o} PRECEDING`,
    ];
    const failures: string[] = [];
    const divergences: string[] = [];
    const precedence: string[] = [];
    for (const table of ["a", "e"])
      for (const unit of ["ROWS", "RANGE", "GROUPS"])
        for (const offset of offsets)
          for (const bound of bounds) {
            reach.beginHistory();
            const sql = `SELECT id, count(*) OVER (ORDER BY v ${unit} ${bound(offset)}) AS n FROM ${table} ORDER BY id, n`;
            const expected = await pgOutcome(pg, sql);
            const actual = nextOutcome(next, sql);
            if (JSON.stringify(actual) === JSON.stringify(expected)) {
              const state = "error" in expected ? expected.error : "accepted";
              if (state === "accepted" && /^- ?0/u.test(offset))
                reach.reach("signed zero accepted");
              if (state === "accepted" && unit !== "RANGE" && offset.includes("."))
                reach.reach("rounded offset accepted");
              if (state === "0A000") reach.reach("numeric offset on an integer key");
              if (state === "accepted") reach.reach("accepted");
              else if (state === "22004") reach.reach("null offset");
              else if (state === "22013") reach.reach("negative offset");
              continue;
            }
            // A refusal (0A000) of SQL PostgreSQL runs: interval and string offsets.
            if ("rows" in expected && "error" in actual && actual.error === "0A000") {
              reach.reach("refused");
              continue;
            }
            // A negative decimal RANGE offset on an integer key has two faults: PostgreSQL reports
            // the type (0A000) first; the parser, which does not know the key, the sign (22013).
            if (
              unit === "RANGE" &&
              (offset === "-0.4" || offset === "-0.5") &&
              "error" in expected &&
              expected.error === "0A000" &&
              "error" in actual &&
              actual.error === "22013"
            ) {
              precedence.push(sql);
              continue;
            }
            // PostgreSQL checks a negative RANGE offset when a row first compares; lowering
            // raises it at once, so over no rows it fails where PostgreSQL returns none.
            if (
              unit === "RANGE" &&
              table === "e" &&
              "rows" in expected &&
              "error" in actual &&
              actual.error === "22013"
            ) {
              divergences.push(sql);
              continue;
            }
            failures.push(
              `${sql}\n  pglite: ${JSON.stringify(expected)}\n  next: ${JSON.stringify(actual)}`,
            );
          }
    expect(failures.join("\n")).toBe("");
    // Exactly the negative RANGE offsets over the empty table: two spellings, three bounds.
    expect(divergences.length).toBe(6);
    // Exactly -0.4 and -0.5 on the integer key (RANGE does not round), three bounds, both tables.
    expect(precedence.length).toBe(12);
    reach.assertReached();
  });
});
