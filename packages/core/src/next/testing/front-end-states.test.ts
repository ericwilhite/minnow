/**
 * The SQLSTATE translation of front-end failures (lower/errors.ts) depends on message text, so
 * the messages are pinned twice:
 *
 * 1. A snapshot (front-end-states.json) of every TypeError / SqlCompileError message template in
 *    the front end's sources that a FRONT_END_STATES pattern maps, with its code. A reworded or
 *    added message changes the set and fails here until its code is reviewed. Regenerate with
 *    MINNOW_UPDATE_FRONT_END_STATES=1 after checking each changed line.
 * 2. For every pattern, SQL that reaches it, whose code must be PostgreSQL's (PGlite), or, for a
 *    refusal (0A000), SQL that PostgreSQL accepts.
 */
import { PGlite } from "@electric-sql/pglite";
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { compileQuery } from "../../engine/query.js";
import {
  FRONT_END_STATES,
  frontEndError,
  frontEndSqlState,
  PlanningError,
} from "../lower/errors.js";
import { LoweredDatabase } from "./lowering-harness.js";
import { pgOutcome } from "./pglite-oracle.js";

const ENGINE = join(import.meta.dirname, "../../engine");
const SNAPSHOT = join(import.meta.dirname, "front-end-states.json");

/** Message templates (`${…}` as `X`) of every error the front end's sources construct. */
function frontEndMessages(): Array<{ file: string; message: string; state: string }> {
  const found: Array<{ file: string; message: string; state: string }> = [];
  const constructed = /new (?:TypeError|SqlCompileError)\(\s*(["'`])((?:\\.|(?!\1)[\s\S])*?)\1/gu;
  for (const file of readdirSync(ENGINE).sort()) {
    if (!file.endsWith(".ts") || file.endsWith(".test.ts")) continue;
    const text = readFileSync(join(ENGINE, file), "utf8");
    for (const match of text.matchAll(constructed)) {
      const message = (match[2] ?? "").replace(/\$\{[^}]*\}/gu, "X");
      const mapped = FRONT_END_STATES.find(([pattern]) => pattern.test(message));
      if (mapped !== undefined) found.push({ file, message, state: mapped[1] });
    }
  }
  const unique = new Map(found.map((entry) => [JSON.stringify(entry), entry]));
  return [...unique.values()];
}

describe("front-end messages the SQLSTATE map depends on", () => {
  it("are exactly the reviewed snapshot", () => {
    const current = frontEndMessages();
    if (process.env.MINNOW_UPDATE_FRONT_END_STATES === "1")
      writeFileSync(SNAPSHOT, `${JSON.stringify(current, null, 2)}\n`);
    const pinned = JSON.parse(readFileSync(SNAPSHOT, "utf8")) as typeof current;
    expect(current).toEqual(pinned);
  });
});

let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);

afterAll(async () => {
  await pg.close();
});

const SETUP = ["CREATE TABLE a (id integer, v integer)", "CREATE TABLE b (id integer, v integer)"];

/**
 * One row per pattern (several where a pattern covers distinct messages): the SQL, and the
 * front-end message prefix it must fail with. The code is PGlite's, except "accepted": a
 * refusal of SQL PostgreSQL runs (0A000).
 */
const CASES: ReadonlyArray<readonly [sql: string, message: string, source?: "published"]> = [
  ["SELECT '[]'::json -> 1.5", "-> array positions are integers"],
  ["SELECT '{}'::json ->> true", "->> keys are member names or array positions"],
  ["SELECT * FROM missing", "SELECT * requires known columns for: "],
  ["SELECT z.* FROM a", "Unknown table for z.*: "],
  [
    "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.id AND z.v = 1)",
    "Correlated EXISTS references an unavailable outer source: ",
  ],
  ["SELECT coalesce()", "COALESCE requires "],
  ["SELECT nullif(1)", "NULLIF requires "],
  ["SELECT greatest()", "GREATEST requires "],
  ["SELECT nofunc(1)", "Unsupported function: "],
  ["SELECT length()", "LENGTH requires exactly one argument"],
  ["SELECT round(1, 2, 3)", "ROUND requires one or two arguments"],
  ["SELECT concat()", "CONCAT takes at least 1 arguments"],
  ["SELECT left('a')", "LEFT takes exactly 2 arguments"],
  ["SELECT sum() FROM a", "SUM requires exactly one argument"],
  ["SELECT substring()", "SUBSTRING requires a string argument"],
  ["SELECT date_trunc('month')", "DATE_TRUNC requires a unit and a datetime argument"],
  ["SELECT CAST(1 AS notatype)", "Unsupported CAST target: "],
  ["CREATE TABLE t (c integer, c integer)", "CREATE TABLE column names must be unique"],
  ["SELECT id FROM a GROUP BY v", "Selected column must appear in GROUP BY"],
  [
    "SELECT id FROM a GROUP BY id HAVING v > 0",
    "HAVING conditions must use aggregates, literals, or GROUP BY expressions",
  ],
  [
    "SELECT count(*) OVER (ORDER BY id, v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM a",
    "Offset RANGE frames require exactly one ORDER BY expression",
  ],
  [
    "SELECT count(*) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM a",
    "Window frame bounds are reversed",
  ],
  [
    "SELECT count(*) OVER (GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM a",
    "GROUPS frames require ORDER BY inside",
  ],
  // Review B6-p2b-3 finding 3: an unknown HAVING name, and invalid frame offsets.
  ["SELECT id FROM a GROUP BY id HAVING nope > 0", 'column "nope" does not exist'],
  ["SELECT id FROM a GROUP BY id HAVING a.nope > 0", "column a.nope does not exist"],
  ["SELECT a.id FROM a GROUP BY a.id HAVING z.v > 0", 'missing FROM-clause entry for table "z"'],
  [
    "SELECT a.id FROM a JOIN b ON b.id = a.id GROUP BY a.id HAVING id > 0",
    'column reference "id" is ambiguous',
  ],
  [
    "SELECT count(*) OVER (ORDER BY id ROWS BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) FROM a",
    "argument of ROWS must be type bigint, not type interval",
  ],
  [
    "SELECT count(*) OVER (ORDER BY id ROWS BETWEEN -1 PRECEDING AND CURRENT ROW) FROM a",
    "frame starting offset must not be negative",
    "published",
  ],
  [
    "SELECT count(*) OVER (ORDER BY id GROUPS BETWEEN CURRENT ROW AND -2 FOLLOWING) FROM a",
    "frame ending offset must not be negative",
    "published",
  ],
  [
    "SELECT count(*) OVER (ORDER BY id RANGE BETWEEN NULL PRECEDING AND CURRENT ROW) FROM a",
    "frame starting offset must not be null",
    "published",
  ],
  [
    "SELECT count(*) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND NULL FOLLOWING) FROM a",
    "frame ending offset must not be null",
    "published",
  ],
  [
    "SELECT count(*) OVER (ORDER BY id ROWS BETWEEN 1e400 PRECEDING AND CURRENT ROW) FROM a",
    "bigint out of range",
  ],
  // Refusals of SQL PostgreSQL runs.
  [
    "SELECT id FROM a GROUP BY id HAVING max(v) > (SELECT max(v) FROM b WHERE b.id = a.id)",
    "Correlated subqueries are supported only as top-level WHERE conditions",
  ],
  ["SELECT id FROM a WHERE id IN (SELECT a.id FROM b)", "Correlated IN subqueries support only"],
  [
    "SELECT a.id FROM a LEFT JOIN b ON b.id = a.id FULL JOIN b c ON c.id = a.id",
    "FULL JOIN is only supported as the sole join",
    "published",
  ],
  [
    "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
    "SET SESSION CHARACTERISTICS AS TRANSACTION is not supported",
  ],
  ["BEGIN ISOLATION LEVEL SERIALIZABLE", "BEGIN ISOLATION LEVEL SERIALIZABLE is not supported"],
  [
    "SELECT count(*) OVER (ORDER BY id RANGE BETWEEN '1' PRECEDING AND CURRENT ROW) FROM a",
    "Window frame offsets other than numeric literals are not supported",
  ],
];

describe("every pattern's code against PostgreSQL", () => {
  it("gives PostgreSQL's SQLSTATE, or 0A000 where PostgreSQL accepts the SQL", async () => {
    const next = new LoweredDatabase();
    for (const sql of SETUP) {
      next.execute(sql);
      await pg.exec(sql);
    }
    const reached = new Set<string>();
    for (const [sql, message, source] of CASES) {
      let failure: PlanningError | null = null;
      try {
        // The published compiler retains these refusals; typed lowering defers offset
        // validation to execution and supports the formerly refused FULL join shape.
        if (source === "published") compileQuery(sql);
        else next.lower(sql);
      } catch (error) {
        const mapped = source === "published" ? frontEndError(error) : error;
        if (!(mapped instanceof PlanningError)) throw mapped;
        failure = mapped;
      }
      if (failure === null) throw new Error(`${sql} lowered`);
      const cause = failure.cause instanceof Error ? failure.cause.message : failure.message;
      expect(cause, sql).toContain(message);
      for (const [pattern] of FRONT_END_STATES)
        if (pattern.test(cause)) reached.add(pattern.source);
      const expected = await pgOutcome(pg, sql);
      expect(failure.sqlState, sql).toBe("error" in expected ? expected.error : "0A000");
      if (!("error" in expected)) expect(failure.sqlState, `${sql}: a refusal`).toBe("0A000");
      expect(frontEndSqlState(cause, failure.position !== undefined)).toBe(failure.sqlState);
    }
    // Every pattern of the map is exercised by SQL above, but those whose messages only the
    // current engine's execution paths raise: lowering resolves those names itself first.
    expect(
      FRONT_END_STATES.map(([pattern]) => pattern.source).filter((source) => !reached.has(source)),
    ).toEqual([String.raw`^Unknown table alias: `, String.raw`^Ambiguous or missing column: `]);
  });
});
