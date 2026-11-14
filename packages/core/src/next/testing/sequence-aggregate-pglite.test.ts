/** PG aggregate/concatenation semantics at SQL lowering -> real executor -> adapter rows. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SEQUENCE_SQL_REACH } from "../../testing/reach.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
const literal = (v: string | null) =>
  v === null ? "NULL::text" : `'${v.replaceAll("'", "''")}'::text`;
afterEach(() => vi.unstubAllEnvs());
for (const seed of seedsFor("sequence-sql", [47049, 47050]))
  it(`matches typed sequence aggregates and concatenation seed${String(seed)}`, async () => {
    // PGlite's timestamp-without-zone driver uses the process time zone; D1 is UTC.
    vi.stubEnv("TZ", "UTC");
    const core = await new StorageCore().open(),
      pg = new PGlite(),
      reach = createReach("sequence SQL", SEQUENCE_SQL_REACH);
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.array(
            fc.record({
              v: fc.constantFrom<string | null>(null, "", "a", "b", "汉", "Ä", "a,b", "a'c"),
              d: fc.constantFrom<string | null>(null, "", "-", "汉"),
              k: fc.integer({ min: 0, max: 2 }),
            }),
            { maxLength: 24 },
          ),
          fc.constantFrom(
            "string",
            "text",
            "integer",
            "numeric",
            "numeric fixed",
            "boolean",
            "date",
            "timestamp",
          ),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (rows, kind, distinct, ordered, grouped, filter) => {
            reach.beginHistory();
            const values = rows.length
              ? rows
                  .map((r, i) => `(${String(i)},${String(r.k)},${literal(r.v)},${literal(r.d)})`)
                  .join(",")
              : "(0,0,NULL::text,NULL::text)";
            const expr =
              kind === "integer"
                ? "CASE WHEN v IS NULL THEN NULL ELSE i END"
                : kind === "numeric fixed"
                  ? "CASE WHEN v IS NULL THEN NULL ELSE i::numeric(6,2) END"
                  : kind === "numeric"
                    ? "CASE WHEN v IS NULL THEN NULL ELSE i::numeric/10 END"
                    : kind === "boolean"
                      ? "CASE WHEN v IS NULL THEN NULL ELSE i%2=0 END"
                      : kind === "date"
                        ? "CASE WHEN v IS NULL THEN NULL ELSE $1::date END"
                        : kind === "timestamp"
                          ? "CASE WHEN v IS NULL THEN NULL ELSE $1::timestamp END"
                          : "v";
            const ordering = ordered
              ? ` ORDER BY ${distinct ? (kind === "string" ? "v,d" : expr) : "i"}`
              : "";
            const aggregate =
              kind === "string"
                ? `string_agg(${distinct ? "DISTINCT " : ""}v,d${ordering})`
                : `array_agg(${distinct ? "DISTINCT " : ""}${expr}${ordering})`;
            const baseSql = `SELECT ${grouped ? "k," : ""}${aggregate}${filter ? " FILTER(WHERE i%2=0)" : ""} a FROM (VALUES${values})t(i,k,v,d)${rows.length ? "" : " WHERE false"}${grouped ? " GROUP BY k ORDER BY k" : ""}`;
            const sql =
              kind === "numeric fixed"
                ? `SELECT ${grouped ? "k," : ""}a||1.2345::numeric a FROM(${baseSql})q${grouped ? " ORDER BY k" : ""}`
                : baseSql;
            const args =
              kind === "date"
                ? ["2001-01-01"]
                : kind === "timestamp"
                  ? ["2001-01-01 12:34:56.123"]
                  : [];
            const expected = (await pg.query(sql, args)).rows,
              result = await core.execute(sql, args);
            if (!("batches" in result)) throw new Error("Missing rows");
            const normalize = (v: unknown): unknown =>
              v instanceof Date
                ? kind === "date"
                  ? v.toISOString().slice(0, 10)
                  : v.toISOString()
                : Array.isArray(v)
                  ? v.map(normalize)
                  : typeof v === "object" && v !== null
                    ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, normalize(x)]))
                    : v;
            expect(normalize(oracleRows(result))).toEqual(normalize(expected));
            reach.reach(kind === "string" ? "string" : "typed array");
            if (kind === "numeric fixed") reach.reach("element modifiers");
            if (rows.some((r) => r.v === null)) reach.reach("NULL element");
            if (!rows.length) reach.reach("empty input");
            if (distinct) reach.reach("DISTINCT");
            if (ordered) reach.reach("ORDER BY");
            if (grouped) reach.reach("groups");
            if (filter) reach.reach("FILTER");
            reach.reach("PG exact");
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("zero");
          },
        ),
        {
          seed,
          numRuns: 36,
          endOnFailure: true,
          examples: [
            [[], "string", false, false, false, false],
            [[], "text", false, false, false, false],
            [
              [
                { v: null, d: "-", k: 0 },
                { v: "汉", d: null, k: 0 },
                { v: "a", d: "!", k: 1 },
              ],
              "string",
              false,
              true,
              true,
              true,
            ],
            [
              [
                { v: null, d: null, k: 0 },
                { v: "汉", d: "-", k: 0 },
              ],
              "text",
              false,
              true,
              false,
              false,
            ],
            [
              [
                { v: "b", d: "-", k: 0 },
                { v: "a", d: "!", k: 0 },
                { v: "a", d: null, k: 0 },
              ],
              "string",
              true,
              true,
              false,
              false,
            ],
            [
              [
                { v: "a", d: null, k: 0 },
                { v: null, d: "-", k: 0 },
              ],
              "integer",
              true,
              true,
              false,
              true,
            ],
            [
              [
                { v: "a", d: null, k: 0 },
                { v: null, d: "-", k: 0 },
              ],
              "numeric",
              false,
              false,
              true,
              false,
            ],
            [[{ v: "a", d: null, k: 0 }], "numeric fixed", false, true, false, false],
            [[{ v: "a", d: null, k: 0 }], "boolean", false, false, false, false],
            [[{ v: "a", d: null, k: 0 }], "date", false, false, false, false],
            [[{ v: "a", d: null, k: 0 }], "timestamp", false, false, false, false],
          ],
        },
      );
      for (const sql of [
        "SELECT 'a'::text||'汉'::text a,NULL::text||'x' b,'x'||NULL::text c",
        "SELECT a||a a,a||9 b,9||a c,a||NULL d,a||NULL::integer e FROM(SELECT array_agg(i ORDER BY i) a FROM generate_series(1,3)g(i))q",
        "SELECT array_agg(i::numeric(5,2))||1.2345::numeric a FROM generate_series(1,2)g(i)",
        "SELECT 1.2345::numeric||array_agg(i::numeric(5,2)) a FROM generate_series(1,2)g(i)",
        "SELECT array_agg(a) a FROM(SELECT array_agg(i ORDER BY i) a FROM generate_series(1,3)g(i))q",
      ]) {
        const result = await core.execute(sql);
        if (!("batches" in result)) throw new Error("Rows");
        expect(oracleRows(result)).toEqual((await pg.query(sql)).rows);
      }
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 120000);
