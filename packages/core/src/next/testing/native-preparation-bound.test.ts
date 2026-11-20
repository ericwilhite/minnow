/** The static preparation bounds (owner/native-preparation-bound.ts) cover what the real lowerer
 * and rule compiler retain, over generated trigger bodies and table rules: long expression
 * chains, CASE nesting, IN lists, ASCII and non-BMP string literals, OLD/NEW bindings, base-table
 * subqueries with `*`, wide targets with DEFAULT/generated text, and CHECK constraints. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_PREPARATION_BOUND_REACH } from "../../testing/reach.js";
import { compileTableRules } from "../exec/rules.js";
import {
  bodyPreparationBound,
  bodyRetainedBytes,
  rulePreparationBound,
  ruleRetainedBytes,
  scanSql,
} from "../owner/native-preparation-bound.js";
import { StorageCore } from "./storage-core.js";

const shape = fc.record({
  kind: fc.constantFrom("insert" as const, "update" as const, "delete" as const),
  chain: fc.integer({ min: 0, max: 300 }),
  caseDepth: fc.integer({ min: 0, max: 12 }),
  list: fc.integer({ min: 0, max: 80 }),
  literal: fc.integer({ min: 0, max: 4000 }),
  wideChar: fc.constantFrom("a", "é", "\u{1F600}"),
  subquery: fc.constantFrom("none" as const, "column" as const, "star" as const),
  width: fc.integer({ min: 1, max: 60 }),
  defaultTerms: fc.integer({ min: 0, max: 20 }),
  generated: fc.integer({ min: 0, max: 4 }),
  checks: fc.integer({ min: 0, max: 4 }),
  bodies: fc.integer({ min: 1, max: 3 }),
});
type Shape = typeof shape extends fc.Arbitrary<infer T> ? T : never;

function setup(h: Shape): string[] {
  const def = (i: number) => `DEFAULT ${String(i)}${" + 1".repeat(h.defaultTerms)}`;
  const extra = Array.from({ length: h.width }, (_, i) => `,c${String(i)} INT ${def(i)}`).join("");
  const gen = Array.from(
    { length: h.generated },
    (_, i) => `,g${String(i)} INT GENERATED ALWAYS AS (c0${" + 1".repeat(i + 1)}) STORED`,
  ).join("");
  const checks = Array.from(
    { length: h.checks },
    (_, i) => `,CONSTRAINT k${String(i)} CHECK (c0${" + 1".repeat(i * 5)} > -1000000)`,
  ).join("");
  return [
    "CREATE TABLE p(id INT PRIMARY KEY, label TEXT)",
    `CREATE TABLE log(id INT, note TEXT${extra}${gen}${checks})`,
    `CREATE TABLE t(id INT PRIMARY KEY, note TEXT${extra}${gen}${checks})`,
    `CREATE TABLE u(k INT PRIMARY KEY, w INT${extra})`,
    "CREATE TABLE one(k INT PRIMARY KEY)",
  ];
}

function body(h: Shape, n: number): string {
  let value = `OLD.id${" + 1".repeat(h.chain)}`;
  for (let d = 0; d < h.caseDepth; d++)
    value = `CASE WHEN NEW.id > ${String(d)} THEN ${value} ELSE OLD.id - ${String(d)} END`;
  const text = `'${h.wideChar.repeat(h.literal)}'`;
  const list = h.list
    ? ` AND id IN (${Array.from({ length: h.list }, (_, i) => String(i)).join(",")})`
    : "";
  const sub =
    h.subquery === "column"
      ? " AND id IN (SELECT k FROM u WHERE w > OLD.id)"
      : h.subquery === "star"
        ? " AND id <= (SELECT count(*) FROM u WHERE w * 2 > OLD.id)"
        : "";
  if (h.kind === "insert")
    return `INSERT INTO log(id, note) VALUES (${value} + ${String(n)}, upper(${text}) || NEW.label);`;
  if (h.kind === "update")
    return `UPDATE t SET note = coalesce(${text}, NEW.label), c0 = ${value} WHERE id = OLD.id${list}${sub};`;
  return `DELETE FROM t WHERE id = ${value}${list}${sub};`;
}

let worst = 0;
for (const seed of seedsFor("next-native-preparation-bound", [49721, 49722]))
  it(`static preparation bounds cover real lowering seed${String(seed)}`, async () => {
    const reach = createReach("native preparation bound", NATIVE_PREPARATION_BOUND_REACH);
    await fc.assert(
      fc.asyncProperty(shape, async (h) => {
        reach.beginHistory();
        const core = new StorageCore(256);
        await core.open();
        try {
          for (const sql of setup(h)) await core.execute(sql);
          const bodies = Array.from({ length: h.bodies }, (_, n) => body(h, n)).join(" ");
          await core.execute(`CREATE TRIGGER tr AFTER UPDATE ON p BEGIN ${bodies} END`);
          const catalog = core.harness.path.catalog;
          for (const definition of catalog.triggers)
            for (const stored of definition.statements) {
              const lowered = core.lowerer.lower(stored.sql, {
                ...core.context(),
                parameterTypes: stored.bindings.map((binding) => binding.type),
              });
              if (lowered.kind !== "modify") throw new Error("modify body required");
              const actual = bodyRetainedBytes(lowered.plan, stored.bindings.length),
                bound = bodyPreparationBound(stored, catalog);
              expect(actual, stored.sql.slice(0, 120)).toBeLessThanOrEqual(bound);
              worst = Math.max(worst, actual / bound);
            }
          for (const table of catalog.tables) {
            const expr = core.lowerer.lowerTableRules(table, catalog),
              program = compileTableRules(table, catalog.version, expr);
            const defaults = table.columns
              .filter((column) => column.generatedSql === null)
              .map((column) => ({
                tableId: table.id,
                columnId: column.id,
                expr: expr.defaults?.find((d) => d.columnId === column.id)?.expr ?? null,
              }));
            const actual = ruleRetainedBytes({ expr, program, defaults }),
              bound = rulePreparationBound(table);
            expect(actual, table.name).toBeLessThanOrEqual(bound);
            worst = Math.max(worst, actual / bound);
          }
          reach.reach(h.kind);
          if (h.chain >= 150) reach.reach("long chain");
          if (h.caseDepth >= 6) reach.reach("nested CASE");
          if (h.list >= 40) reach.reach("long IN list");
          if (h.literal >= 2000)
            reach.reach(h.wideChar === "a" ? "long ASCII literal" : "long non-ASCII literal");
          if (h.subquery !== "none")
            reach.reach(h.subquery === "star" ? "subquery star" : "subquery");
          if (h.width >= 40) reach.reach("wide table");
          if (h.defaultTerms >= 10) reach.reach("long defaults");
          if (h.generated > 0) reach.reach("generated columns");
          if (h.checks > 0) reach.reach("checks");
          if (h.bodies > 1) reach.reach("several bodies");
        } finally {
          await core.close();
        }
      }),
      {
        seed,
        numRuns: 20,
        examples: [
          [
            {
              kind: "update",
              chain: 300,
              caseDepth: 12,
              list: 80,
              literal: 4000,
              wideChar: "\u{1F600}",
              subquery: "column",
              width: 60,
              defaultTerms: 20,
              generated: 4,
              checks: 4,
              bodies: 3,
            },
          ],
          [
            {
              kind: "insert",
              chain: 0,
              caseDepth: 0,
              list: 0,
              literal: 0,
              wideChar: "a",
              subquery: "none",
              width: 1,
              defaultTerms: 0,
              generated: 0,
              checks: 0,
              bodies: 1,
            },
          ],
          [
            {
              kind: "delete",
              chain: 200,
              caseDepth: 6,
              list: 50,
              literal: 2500,
              wideChar: "é",
              subquery: "star",
              width: 45,
              defaultTerms: 12,
              generated: 2,
              checks: 1,
              bodies: 2,
            },
          ],
          [
            {
              kind: "insert",
              chain: 160,
              caseDepth: 8,
              list: 0,
              literal: 3000,
              wideChar: "a",
              subquery: "none",
              width: 50,
              defaultTerms: 15,
              generated: 3,
              checks: 2,
              bodies: 2,
            },
          ],
        ],
      },
    );
    reach.assertReached();
    // Directional: how much of each bound the largest preparation used.
    expect(worst).toBeGreaterThan(0);
  }, 300000);

it("counts tokens without building them", () => {
  const names: string[] = [];
  const scan = scanSql(
    `UPDATE "My ""T""" SET a = 'x''y' || 1.5e3 -- c\n WHERE b >= $1 /* d */ AND c IN (SELECT * FROM u)`,
    (name) => names.push(name),
  );
  expect(scan.stars).toBe(1);
  expect(names).toEqual([
    "update",
    'My "T"',
    "set",
    "a",
    "where",
    "b",
    "$1",
    "and",
    "c",
    "in",
    "select",
    "from",
    "u",
  ]);
  // || and >= count as two tokens each, which only raises a bound.
  expect(scan.tokens).toBe(23);
});
