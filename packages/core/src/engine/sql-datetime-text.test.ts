/**
 * The datetime text formatters against PostgreSQL itself. Each value reaches PGlite as numbers
 * — epoch milliseconds, interval fields — never as text this module wrote, so a formatter that
 * wrote the wrong year or sign could not agree with PostgreSQL by reading its own output back.
 * Every value then goes the other way: Minnow's input readers take the text and return the
 * value it came from.
 */
import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { mulberry32 } from "../testing/seeds.js";
import { MinnowDatabase } from "./database.js";
import {
  postgresIntervalText,
  postgresTimeText,
  postgresTimestampJsonText,
  postgresTimestampText,
} from "./sql-datetime-text.js";
import { intervalDomainParts, intervalDomainValue, timeDomainValue } from "./sql-domains.js";
import { parseSqlTimestampText } from "./sql-semantics.js";

const DAY = 86_400_000;

/** Instants across PostgreSQL's range: BC years, years below 1000, past 9999, every fraction. */
function sampleInstants(): number[] {
  const rng = mulberry32(20_261_016);
  const fixed = [
    Date.UTC(2024, 0, 2, 10, 0, 0, 0),
    Date.UTC(2024, 0, 2, 10, 0, 0, 500),
    Date.UTC(2024, 0, 2, 10, 0, 0, 50),
    Date.UTC(2024, 0, 2, 10, 0, 0, 5),
    Date.UTC(2024, 0, 2, 10, 0, 0, 123),
    Date.UTC(2024, 1, 29, 23, 59, 59, 999),
    0,
    -1,
    -DAY,
    new Date("0099-03-04T05:06:07.000Z").getTime(),
    new Date("0001-01-01T00:00:00.000Z").getTime(),
    new Date("0001-01-01T00:00:00.000Z").getTime() - 1,
    new Date("-000001-12-31T12:00:00.000Z").getTime(),
    new Date("-004713-11-24T00:00:00.000Z").getTime(),
    new Date("+010000-01-01T00:00:00.000Z").getTime(),
    new Date("+099999-12-31T23:59:59.999Z").getTime(),
  ];
  const random = Array.from({ length: 400 }, () => {
    // PostgreSQL's range begins in 4713 BC; reach past 9999 AD but not past its own limit.
    const span = new Date("+100000-01-01T00:00:00.000Z").getTime();
    const start = new Date("-004713-11-25T00:00:00.000Z").getTime();
    const instant = Math.floor(start + rng() * (span - start));
    // Whole seconds and whole milliseconds both occur, as stored data has them.
    return rng() < 0.3 ? instant - (((instant % 1000) + 1000) % 1000) : instant;
  });
  return [...fixed, ...random];
}

/** Intervals with every sign mix PostgreSQL's output distinguishes. */
function sampleIntervals(): Array<[number, number, number]> {
  const rng = mulberry32(1_016);
  const fixed: Array<[number, number, number]> = [
    [0, 0, 0],
    [0, 1, 7_200_000_000],
    [1, 14, 43_200_000_000],
    [0, 0, -1_000_000],
    [14, 0, 0],
    [-14, 0, 0],
    [0, -3, 14_400_000_000],
    [0, 3, -14_400_000_000],
    [1, -1, 0],
    [-1, 1, -1],
    [0, 0, 90_000_000_000],
    [0, 0, 360_000_000_000_000],
    [12, 0, 500_000],
    [-12, -1, -1_250_000],
    [0, 0, 1],
    [0, 0, -999_999],
  ];
  const pick = (scale: number): number =>
    rng() < 0.3 ? 0 : Math.round((rng() * 2 - 1) * scale) || 0;
  const random = Array.from({ length: 300 }, (): [number, number, number] => [
    pick(400),
    pick(1_000),
    pick(400 * 3_600_000_000),
  ]);
  return [...fixed, ...random];
}

describe("datetime text against PostgreSQL", () => {
  it("writes timestamps, timestamps with time zone, and their JSON as PostgreSQL does", async () => {
    const pg = await PGlite.create();
    try {
      await pg.exec("SET TIME ZONE 'UTC'");
      const instants = sampleInstants();
      const mismatches: string[] = [];
      for (const instant of instants) {
        // Whole days plus the milliseconds into the day: exact, where to_timestamp's float8
        // seconds would round a far-future instant's fraction.
        const days = Math.floor(instant / DAY);
        const result = await pg.query<{ zoned: string; local: string; zj: string; lj: string }>(
          `SELECT t::text AS zoned, (t AT TIME ZONE 'UTC')::text AS local, to_json(t)::text AS zj, to_json(t AT TIME ZONE 'UTC')::text AS lj FROM (SELECT TIMESTAMPTZ 'epoch' + make_interval(days => $1::int) + $2::int * INTERVAL '1 millisecond' AS t) s`,
          [days, instant - days * DAY],
        );
        const row = result.rows[0];
        const ours = {
          zoned: postgresTimestampText(instant, true),
          local: postgresTimestampText(instant, false),
          zj: JSON.stringify(postgresTimestampJsonText(instant, true)),
          lj: JSON.stringify(postgresTimestampJsonText(instant, false)),
        };
        if (JSON.stringify(ours) !== JSON.stringify(row)) {
          mismatches.push(`${String(instant)}: ${JSON.stringify(ours)} vs ${JSON.stringify(row)}`);
        }
        // The text reads back as the same instant, with or without its zone.
        for (const text of [ours.zoned, ours.local, ours.zj.slice(1, -1), ours.lj.slice(1, -1)]) {
          const read = parseSqlTimestampText(text)?.getTime();
          if (read !== instant) mismatches.push(`${text} reads back as ${String(read)}`);
        }
      }
      expect(mismatches.slice(0, 10)).toEqual([]);
      expect(instants.length).toBeGreaterThan(400);
    } finally {
      await pg.close();
    }
  });

  it("writes intervals as PostgreSQL does and reads that text back", async () => {
    const pg = await PGlite.create();
    try {
      const intervals = sampleIntervals();
      const mismatches: string[] = [];
      for (const [months, days, microseconds] of intervals) {
        const result = await pg.query<{ text: string }>(
          "SELECT make_interval(0, $1::int, 0, $2::int, 0, 0, $3::numeric / 1000000)::text AS text",
          [months, days, String(microseconds)],
        );
        const expected = result.rows[0]?.text;
        const ours = postgresIntervalText(months, days, microseconds);
        if (ours !== expected) {
          mismatches.push(
            `${JSON.stringify([months, days, microseconds])}: ${ours} vs ${String(expected)}`,
          );
        }
        const read = intervalDomainParts(intervalDomainValue(ours));
        if (JSON.stringify(read) !== JSON.stringify([months, days, microseconds])) {
          mismatches.push(`${ours} reads back as ${JSON.stringify(read)}`);
        }
      }
      expect(mismatches.slice(0, 10)).toEqual([]);
    } finally {
      await pg.close();
    }
  });

  it("reads PostgreSQL's own interval spellings", async () => {
    const pg = await PGlite.create();
    try {
      const spellings = [
        "1 day 2 hours",
        "1 year 2 months 3 days 04:05:06.5",
        "-3 days 4 hours",
        "1.5 days",
        "1.5 months",
        "1.25 years",
        "2 weeks 1.5 days",
        "90 minutes 30 seconds",
        "100 hours",
        "1 day -00:00:01",
        "-1 day +00:00:01",
        "@ 1 day 2 hours ago",
        "3 mons 1 usec",
        "250 ms",
        "1 decade 2 centuries",
        "-00:00:00.000001",
        "12:34",
      ];
      for (const spelling of spellings) {
        const result = await pg.query<{ text: string }>("SELECT $1::interval::text AS text", [
          spelling,
        ]);
        const parts = intervalDomainParts(intervalDomainValue(spelling));
        expect(parts, spelling).toBeDefined();
        const [months = 0, days = 0, microseconds = 0] = parts ?? [];
        expect(postgresIntervalText(months, days, microseconds), spelling).toBe(
          result.rows[0]?.text,
        );
      }
      expect(() => intervalDomainValue("1 day trailing")).toThrow("Invalid INTERVAL");
      expect(() => intervalDomainValue("1 fortnight")).toThrow("Unknown INTERVAL unit");
      expect(() => intervalDomainValue("10:75")).toThrow("Invalid INTERVAL");
    } finally {
      await pg.close();
    }
  });

  it("keeps full-text documents of TIME and INTERVAL columns on the text postings store", async () => {
    // Postings persist the terms of each document, so full-text search reads TIME and INTERVAL
    // values as it always has even though SQL text now follows PostgreSQL. Repeated values make
    // the scan read a dictionary-encoded vector, the other path that tokenizes documents.
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 64 });
    try {
      await database.execute("CREATE TABLE spans (id INTEGER PRIMARY KEY, span INTERVAL, at TIME)");
      const values = Array.from(
        { length: 200 },
        (_, index) =>
          `(${String(index)}, '${index % 2 === 0 ? "1 day 2 hours" : "3 months"}', '10:00:00.500')`,
      );
      await database.execute(`INSERT INTO spans VALUES ${values.join(", ")}`);
      const count = async (sql: string): Promise<unknown> => (await database.query(sql)).rows[0]?.n;
      expect(await count("SELECT COUNT(*) AS n FROM spans WHERE MATCH(span) AGAINST 'usecs'")).toBe(
        200,
      );
      expect(await count("SELECT COUNT(*) AS n FROM spans WHERE MATCH(span) AGAINST 'mons'")).toBe(
        200,
      );
      expect(await count("SELECT COUNT(*) AS n FROM spans WHERE MATCH(at) AGAINST '500'")).toBe(
        200,
      );
      expect(
        (await database.query("SELECT span, at FROM spans WHERE id < 2 ORDER BY id")).rows,
      ).toEqual([
        { span: "1 day 02:00:00", at: "10:00:00.5" },
        { span: "3 mons", at: "10:00:00.5" },
      ]);
    } finally {
      await database.close();
    }
  });

  it("writes TIME as PostgreSQL does, without trailing fractional zeros", async () => {
    const pg = await PGlite.create();
    try {
      for (const time of ["10:00:00", "10:00:00.5", "10:00:00.500", "23:59:59.120000", "00:00"]) {
        const result = await pg.query<{ text: string }>("SELECT $1::time::text AS text", [time]);
        const canonical = String(timeDomainValue(time)).replace(/^.*time:/, "");
        expect(postgresTimeText(canonical), time).toBe(result.rows[0]?.text);
      }
    } finally {
      await pg.close();
    }
  });
});
