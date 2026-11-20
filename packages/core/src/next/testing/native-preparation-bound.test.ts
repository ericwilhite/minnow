/** The static preparation bounds (owner/native-preparation-bound.ts) cover what the real lowerer
 * and rule compiler retain, over generated trigger bodies and table rules: long expression
 * chains, CASE nesting, IN lists, ASCII and non-BMP string literals, OLD/NEW bindings, base-table
 * subqueries with `*`, wide targets with DEFAULT/generated text, and CHECK constraints. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_PREPARATION_BOUND_REACH } from "../../testing/reach.js";
import { compileTableRules } from "../exec/rules.js";
import {
  bodyPreparation,
  bodyPreparationBound,
  bodyRetainedBytes,
  rulePreparationBound,
  ruleRetainedBytes,
  scanSql,
} from "../owner/native-preparation-bound.js";
import { StorageCore } from "./storage-core.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";

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
  // Row count × explicit DEFAULT cells × stored default size, and DEFAULT VALUES.
  rows: fc.integer({ min: 1, max: 10 }),
  defaultCells: fc.integer({ min: 0, max: 2 }),
  defaultText: fc.integer({ min: 0, max: 20000 }),
  defaultValues: fc.boolean(),
  // Parser copy sites: one source subtree lowered several times (native-preparation-bound.ts).
  copies: fc.constantFrom(
    "none" as const,
    "simpleCase" as const,
    "nestedCase" as const,
    "between" as const,
    "symmetric" as const,
    "rowEq" as const,
    "rowLt" as const,
    "rowIn" as const,
    "variance" as const,
    "groupRefs" as const,
    "usingJoin" as const,
  ),
  operand: fc.integer({ min: 0, max: 60 }),
  arms: fc.integer({ min: 1, max: 30 }),
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
    `CREATE TABLE log(id INT, note TEXT DEFAULT '${"d".repeat(h.defaultText)}'${extra}${gen}${checks})`,
    `CREATE TABLE t(id INT PRIMARY KEY, note TEXT DEFAULT '${"d".repeat(h.defaultText)}'${extra}${gen}${checks})`,
    `CREATE TABLE u(k INT PRIMARY KEY, w INT${extra})`,
    "CREATE TABLE one(k INT PRIMARY KEY)",
  ];
}

/** A predicate (or value) whose operand the parser copies, scaled by operand size and arms. */
function copied(h: Shape): { readonly value: string | null; readonly predicate: string } {
  const x = `(OLD.id${" + 1".repeat(h.operand)})`;
  const arms = (operand: string) =>
    `CASE ${operand} ${Array.from({ length: h.arms }, (_, i) => `WHEN ${String(i)} THEN ${String(i + 1)}`).join(" ")} ELSE 0 END`;
  const items = Array.from({ length: h.arms }, (_, i) => `(${String(i)}, ${x})`).join(", ");
  switch (h.copies) {
    case "simpleCase":
      return { value: arms(x), predicate: "" };
    case "nestedCase":
      return { value: arms(arms(x)), predicate: "" };
    case "between":
      return { value: null, predicate: ` AND ${x} BETWEEN 0 AND 100` };
    case "symmetric":
      return { value: null, predicate: ` AND ${x} BETWEEN SYMMETRIC 100 AND ${x}` };
    case "rowEq":
      return { value: null, predicate: ` AND (id, ${x}) = (OLD.id, ${x})` };
    case "rowLt":
      return { value: null, predicate: ` AND (id, ${x}) < (OLD.id + 1, ${x})` };
    case "rowIn":
      return { value: null, predicate: ` AND (id, ${x}) IN (${items})` };
    case "variance":
      return {
        value: null,
        predicate: ` AND id IN (SELECT CAST(variance(w + ${x}) AS INT) FROM u GROUP BY k)`,
      };
    case "groupRefs":
      return {
        value: null,
        predicate: ` AND id IN (SELECT k + ${x} AS kk FROM u GROUP BY 1 ORDER BY kk)`,
      };
    case "usingJoin":
      return {
        value: null,
        predicate: ` AND id IN (SELECT u.k FROM u JOIN one USING (k) WHERE u.w > ${x})`,
      };
    default:
      return { value: null, predicate: "" };
  }
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
  if (h.kind === "insert") {
    if (h.defaultValues && n === 0) return "INSERT INTO log DEFAULT VALUES;";
    const rows = Array.from(
      { length: h.rows },
      (_, r) =>
        `(${copied(h).value ?? value} + ${String(n + r)}, ${h.defaultCells >= 1 ? "DEFAULT" : `upper(${text}) || NEW.label`}, ${h.defaultCells >= 2 ? "DEFAULT" : String(r)})`,
    );
    return `INSERT INTO log(id, note, c0) VALUES ${rows.join(", ")};`;
  }
  const copy = copied(h);
  if (h.kind === "update")
    return `UPDATE t SET note = coalesce(${text}, NEW.label), c0 = ${copy.value ?? value} WHERE id = OLD.id${list}${sub}${copy.predicate};`;
  return `DELETE FROM t WHERE id = ${value}${list}${sub}${copy.predicate};`;
}

const COPY_SITES = [
  "simpleCase",
  "nestedCase",
  "between",
  "symmetric",
  "rowEq",
  "rowLt",
  "rowIn",
  "variance",
  "groupRefs",
  "usingJoin",
] as const;
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
          if (h.kind === "insert" && h.rows >= 5) reach.reach("multi-row VALUES");
          if (h.defaultCells > 0 && h.kind === "insert") reach.reach("DEFAULT cells");
          if (h.kind === "insert" && h.defaultCells > 0 && h.defaultText >= 10000)
            reach.reach("large repeated default");
          if (h.kind === "insert" && h.defaultValues) reach.reach("DEFAULT VALUES");
          if (h.copies !== "none" && (h.kind !== "insert" || copied(h).value !== null))
            reach.reach(`copies ${h.copies}`);
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
              rows: 1,
              defaultCells: 1,
              defaultText: 12000,
              defaultValues: false,
              copies: "none",
              operand: 0,
              arms: 1,
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
              rows: 1,
              defaultCells: 0,
              defaultText: 0,
              defaultValues: false,
              copies: "none",
              operand: 0,
              arms: 1,
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
              rows: 1,
              defaultCells: 0,
              defaultText: 0,
              defaultValues: false,
              copies: "none",
              operand: 0,
              arms: 1,
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
              rows: 6,
              defaultCells: 2,
              defaultText: 500,
              defaultValues: true,
              copies: "none",
              operand: 0,
              arms: 1,
            },
          ],
          // Every parser copy site, at a large operand and many arms.
          ...(COPY_SITES.map((copies) => [
            {
              kind: "update" as const,
              chain: 0,
              caseDepth: 0,
              list: 0,
              literal: 0,
              wideChar: "a" as const,
              subquery: "none" as const,
              width: 2,
              defaultTerms: 0,
              generated: 0,
              checks: 0,
              bodies: 1,
              rows: 1,
              defaultCells: 0,
              defaultText: 0,
              defaultValues: false,
              copies,
              operand: 60,
              arms: copies === "nestedCase" ? 12 : 30,
            },
          ]) as Array<[Shape]>),
          // Review scenario: ten DEFAULT rows of a 100,000-character text default.
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
              rows: 10,
              defaultCells: 1,
              defaultText: 100000,
              defaultValues: false,
              copies: "none",
              operand: 0,
              arms: 1,
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

it("refuses grouping-set bodies before lowering them", async () => {
  const owner = await publicOwner({ capBytes: 64 << 20 }),
    db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
  let lowered = 0;
  try {
    for (const sql of [
      "CREATE TABLE p(id INT PRIMARY KEY)",
      "CREATE TABLE t(k INT PRIMARY KEY, v INT)",
      "CREATE TABLE u(k INT PRIMARY KEY, w INT)",
      "INSERT INTO p VALUES (1)",
      "INSERT INTO t VALUES (1, 1)",
      "CREATE TRIGGER r AFTER UPDATE ON p BEGIN UPDATE t SET v = 2 WHERE k IN (SELECT k FROM u GROUP BY ROLLUP (k, w)); END",
    ])
      await db.execute(sql);
    const catalog = owner.core.harness.path.catalog,
      body = catalog.triggers[0]?.statements[0];
    if (!body) throw new Error("Missing body");
    expect(bodyPreparation(body, catalog).groupingSets).toBe(true);
    const lower = owner.core.lowerer.lower.bind(owner.core.lowerer);
    vi.spyOn(owner.core.lowerer, "lower").mockImplementation((text, context) => {
      if (text === body.sql) lowered++;
      return lower(text, context);
    });
    const error = await db.execute("UPDATE p SET id = id").then(
      () => null,
      (e: unknown) => e as { sqlState?: string; message?: string },
    );
    expect(error?.sqlState).toBe("0A000");
    expect(error?.message).toMatch(/GROUPING SETS, ROLLUP or CUBE/u);
    expect(lowered).toBe(0);
    expect((await db.query("SELECT * FROM t")).rows()).toEqual([{ k: 1, v: 1 }]);
  } finally {
    vi.restoreAllMocks();
    await db.close();
    await owner.close();
  }
});
