/** PostgreSQL CTAS grammar; long inactive SELECT literals must not enter CTAS matching. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { ctasDataClause } from "../lower/ctas.js";

it.each(seedsFor("next-ctas-data-clause", [0xb4e9, 0xb4ea]))(
  "classifies CTAS clauses against PostgreSQL (seed %i)",
  async (seed) => {
    const pg = new PGlite(),
      core = await new StorageCore().open();
    const reach = createReach("CTAS classification", [
      "WITH DATA",
      "WITH NO DATA",
      "quoted AS",
      "default AS",
      "column AS",
      "comment AS",
      "candidate false positive",
      "not CTAS",
      "case",
      "semicolon",
      "PG exact",
    ]);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.boolean(),
          fc.integer({ min: 0, max: 4 }),
          fc.boolean(),
          fc.boolean(),
          async (ctas, noData, kind, lower, semicolon) => {
            reach.beginHistory();
            const name = kind === 0 ? '"copy AS label"' : "copy";
            let sql = ctas
              ? `CREATE TABLE ${name} AS SELECT 1 AS v WITH ${noData ? "NO " : ""}DATA`
              : kind === 0
                ? 'CREATE TABLE "copy AS label" (v integer) WITH NO DATA'
                : kind === 1
                  ? "CREATE TABLE copy(v text DEFAULT 'AS') WITH NO DATA"
                  : kind === 2
                    ? 'CREATE TABLE copy("AS" integer) WITH NO DATA'
                    : kind === 3
                      ? "CREATE TABLE copy(v integer) WITH DATA"
                      : "CREATE TABLE copy(v integer /* AS */) WITH NO DATA";
            if (lower) {
              sql = sql.replace(/CREATE TABLE|SELECT|WITH|NO|DATA|AS|DEFAULT/gu, (word) =>
                word.toLowerCase(),
              );
              reach.reach("case");
            }
            if (semicolon) {
              sql += " ; \n";
              reach.reach("semicolon");
            }
            await pg.exec(
              'DROP TABLE IF EXISTS copy;DROP TABLE IF EXISTS "copy AS label";DROP TABLE IF EXISTS "copy as label"',
            );
            let state: unknown;
            try {
              await pg.exec(sql);
            } catch (error) {
              state = (error as { code: unknown }).code;
            }
            if (ctas) {
              expect(state).toBeUndefined();
              const result = core.lowerer.lower(sql, core.context());
              expect(result.kind).toBe("ddl");
              if (result.kind !== "ddl") throw new Error("Expected CTAS DDL");
              expect(result.followUp === null).toBe(noData);
              expect(ctasDataClause(sql)?.noData).toBe(noData);
              reach.reach(noData ? "WITH NO DATA" : "WITH DATA");
            } else {
              expect(state).toBe("42601");
              // Candidate matching may find AS inside names/literals/comments; the
              // parsed statement kind must still reject the CTAS-only suffix.
              if (kind !== 3) {
                expect(ctasDataClause(sql)).not.toBeNull();
                reach.reach("candidate false positive");
              }
              let actual: unknown;
              try {
                core.lowerer.lower(sql, core.context());
              } catch (error) {
                actual = error;
              }
              expect(actual).toMatchObject({ sqlState: state });
              reach.reach("not CTAS");
              if (kind === 0) reach.reach("quoted AS");
              if (kind === 1) reach.reach("default AS");
              if (kind === 2) reach.reach("column AS");
              if (kind === 4) reach.reach("comment AS");
            }
            reach.reach("PG exact");
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            [false, true, 0, false, false],
            [false, true, 1, false, true],
            [false, true, 2, false, false],
            [false, true, 4, false, false],
            [true, false, 3, true, true],
            [true, true, 0, false, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
      await core.close();
    }
  },
  30000,
);

it.each(seedsFor("next-ctas-scanner-budget", [0xb4eb, 0xb4ec]))(
  "bounds backward character work without timing (seed %i)",
  (seed) => {
    const reach = createReach("CTAS scan budget", [
      "spaces",
      "tabs",
      "no clause",
      "NO DATA",
      "DATA",
      "budget",
      "qualified header",
      "column header",
      "nested comment header",
    ]);
    fc.assert(
      fc.property(
        fc.integer({ min: 1, max: 1024 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 0, max: 3 }),
        (length, noData, clause, tabs, headerShape) => {
          reach.beginHistory();
          const pad = (tabs ? "\t" : " ").repeat(length);
          const header =
            headerShape === 1
              ? `"s${pad}.AS"."t""AS"`
              : headerShape === 2
                ? `copy ("a${pad}(,)", "b""AS")`
                : headerShape === 3
                  ? `/*${"/*".repeat(length)} AS ${"*/".repeat(length)}*/ copy`
                  : "copy";
          const sql = `CREATE TABLE ${header} AS SELECT '${pad}AS WITH NO DATA' AS v${clause ? `${pad}WITH${pad}${noData ? `NO${pad}` : ""}DATA` : ""}${pad};${pad}`;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- Preserved receiver below.
          const original = String.prototype.charCodeAt;
          let reads = 0;
          String.prototype.charCodeAt = function (this: string, index: number) {
            if (++reads > 2 * sql.length + 128)
              throw new Error("CTAS classification exceeded its linear character budget");
            return original.call(this, index);
          };
          let result: ReturnType<typeof ctasDataClause>;
          try {
            result = ctasDataClause(sql);
          } finally {
            String.prototype.charCodeAt = original;
          }
          expect(result === null).toBe(!clause);
          if (result) expect(result.noData).toBe(noData);
          reach.reach(tabs ? "tabs" : "spaces");
          reach.reach(clause ? (noData ? "NO DATA" : "DATA") : "no clause");
          reach.reach("budget");
          if (headerShape === 1) reach.reach("qualified header");
          if (headerShape === 2) reach.reach("column header");
          if (headerShape === 3) reach.reach("nested comment header");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [1024, true, true, false, 0],
          [1024, false, true, true, 0],
          [1024, false, false, false, 0],
          [1024, true, true, false, 1],
          [1024, true, true, false, 2],
          [1024, true, true, false, 3],
        ],
      },
    );
    reach.assertReached();
  },
);
