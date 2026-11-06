/**
 * Window frame offsets in the released engine against PostgreSQL (reviews B6-p2b-4 and -5): a
 * sign is not a value, zero and the sign come from the literal's digits, ROWS / GROUPS round to
 * bigint and check its range. Each offset × unit × bound runs in PGlite and in MinnowDatabase:
 * where PostgreSQL runs it, Minnow returns the same rows; where PostgreSQL raises 22013, 22004 or
 * 22003, Minnow raises PostgreSQL's message.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { createReach } from "../testing/reach.js";
import { MinnowDatabase } from "./database.js";

let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);

afterAll(async () => {
  await pg.close();
});

const SETUP = [
  "CREATE TABLE a (id INTEGER, v INTEGER)",
  "INSERT INTO a VALUES (1, 10), (2, 20), (2, 5)",
];

const OFFSETS = [
  "0",
  "1",
  "-1",
  "NULL",
  "-0",
  "- 0",
  "-0.0",
  "-0e4",
  "+1",
  "0.4",
  "-0.4",
  "-0.5",
  "1.5",
  "1e-400",
  "-1e-400",
  "1e400",
  "-0e-400",
  "9223372036854775808",
];

/** PostgreSQL's errors and the released engine's messages for them. */
const MESSAGES: Readonly<Record<string, RegExp>> = {
  "22013": /^frame (starting|ending) offset must not be negative$/u,
  "22004": /^frame (starting|ending) offset must not be null$/u,
  "22003": /^bigint out of range$/u,
};

const STATES = ["rows", "22013", "22004", "22003"] as const;

describe("frame offsets in the released engine", () => {
  it("match PostgreSQL's rows and errors for ROWS and GROUPS", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    for (const sql of SETUP) {
      await database.execute(sql);
      await pg.exec(sql);
    }
    const reach = createReach("released frame offsets", STATES);
    const mismatches: string[] = [];
    try {
      for (const unit of ["ROWS", "GROUPS"])
        for (const offset of OFFSETS)
          for (const bound of [
            `BETWEEN ${offset} PRECEDING AND CURRENT ROW`,
            `BETWEEN CURRENT ROW AND ${offset} FOLLOWING`,
          ]) {
            reach.beginHistory();
            const sql = `SELECT id, count(*) OVER (ORDER BY v ${unit} ${bound}) AS n FROM a ORDER BY id, n`;
            let expected: { rows: unknown } | { code: string };
            try {
              expected = { rows: (await pg.query(sql)).rows };
            } catch (error) {
              expected = { code: String((error as { code?: string }).code) };
            }
            let actual: { rows: unknown } | { message: string };
            try {
              actual = { rows: (await database.query(sql)).rows };
            } catch (error) {
              actual = { message: error instanceof Error ? error.message : String(error) };
            }
            if ("rows" in expected) {
              if ("rows" in actual && JSON.stringify(actual.rows) === JSON.stringify(expected.rows))
                reach.reach("rows");
              else mismatches.push(`${sql}\n  pglite: rows\n  minnow: ${JSON.stringify(actual)}`);
              continue;
            }
            const message = MESSAGES[expected.code];
            if (message === undefined) throw new Error(`${sql}: unexpected ${expected.code}`);
            if ("message" in actual && message.test(actual.message))
              reach.reach(expected.code as (typeof STATES)[number]);
            else
              mismatches.push(
                `${sql}\n  pglite: ${expected.code}\n  minnow: ${JSON.stringify(actual)}`,
              );
          }
    } finally {
      await database.close();
    }
    expect(mismatches.join("\n")).toBe("");
    reach.assertReached();
  });
});
