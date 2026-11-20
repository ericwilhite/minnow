/** Metered preparation is sound: for every generated trigger body and table rule set, what the
 * real parser, optimizer, lowerer and rule compiler retain stays within the base share
 * (owner/native-preparation-bound.ts) plus every copy the copy meter (engine/copy-meter.ts)
 * charged while lowering it. Generated: long chains, CASE nesting, IN lists, ASCII and non-BMP
 * literals, OLD/NEW bindings, base-table subqueries with `*`, wide targets with DEFAULT,
 * generated and CHECK text, row count x DEFAULT cells x default size, every parser copy site
 * including negated forms (row NOT IN, NOT BETWEEN), nesting, grouping sets, and quoted keyword
 * identifiers (`"cube"`, `"rollup"`). A copy that bypasses the meter fails this check. */
// eslint-disable-next-line no-restricted-imports -- Test-only GC access for the peak check.
import { setFlagsFromString } from "node:v8";
// eslint-disable-next-line no-restricted-imports -- Test-only GC access for the peak check.
import { runInNewContext } from "node:vm";
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_PREPARATION_BOUND_REACH } from "../../testing/reach.js";
import { compileTableRules } from "../exec/rules.js";
import {
  bodyRetainedBytes,
  rulePreparationBound,
  ruleRetainedBytes,
  scanSql,
  PREPARATION_TEXT_BYTES,
  PREPARATION_TOKEN_BYTES,
} from "../owner/native-preparation-bound.js";
import { lowerBodyMetered } from "../owner/native-command.js";
import { withCopyMeter, type CopyMeter } from "../../engine/copy-meter.js";
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
    "rowNotIn" as const,
    "notBetween" as const,
    "betweenInCase" as const,
    "quotedKeywords" as const,
    "groupingSet" as const,
    "rollup" as const,
    "rowNotInLiteral" as const,
    "rollupWide" as const,
    "cubeWide" as const,
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
    'CREATE TABLE "cube"(k INT PRIMARY KEY, "rollup" INT)',
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
    case "rowNotInLiteral":
      return {
        value: null,
        predicate: ` AND (note, '${"x".repeat(h.literal)}') NOT IN (${Array.from({ length: h.arms }, (_, i) => `('a${String(i)}', 'b')`).join(", ")})`,
      };
    case "rollupWide":
      return {
        value: null,
        predicate: ` AND id IN (SELECT k FROM u WHERE w > ${x} GROUP BY ROLLUP (${Array.from({ length: h.arms }, (_, i) => (i % 2 === 0 ? "k" : "w")).join(", ")}))`,
      };
    case "cubeWide":
      return {
        value: null,
        predicate: ` AND id IN (SELECT k FROM u WHERE w > ${x} GROUP BY CUBE (${Array.from({ length: Math.min(5, h.arms) }, (_, i) => (i % 2 === 0 ? "k" : "w")).join(", ")}))`,
      };
    case "rowNotIn":
      return { value: null, predicate: ` AND (id, ${x}) NOT IN (${items})` };
    case "notBetween":
      return { value: null, predicate: ` AND ${x} NOT BETWEEN 100 AND ${x}` };
    case "betweenInCase":
      return {
        value: `CASE (${x} BETWEEN 0 AND ${x}) ${Array.from({ length: h.arms }, (_, i) => `WHEN ${i % 2 === 0 ? "true" : "false"} THEN ${String(i)}`).join(" ")} ELSE 0 END`,
        predicate: "",
      };
    case "quotedKeywords":
      return {
        value: null,
        predicate: ` AND id IN (SELECT "rollup" FROM "cube" WHERE "rollup" > ${x})`,
      };
    case "groupingSet":
      return {
        value: null,
        predicate: ` AND id IN (SELECT k FROM u WHERE w > ${x} GROUP BY GROUPING SETS ((k)))`,
      };
    case "rollup":
      return {
        value: null,
        predicate: ` AND id IN (SELECT k FROM u WHERE w > ${x} GROUP BY ROLLUP (k, w))`,
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
  "rowNotIn",
  "notBetween",
  "betweenInCase",
  "quotedKeywords",
  "groupingSet",
  "rollup",
  "rowNotInLiteral",
  "rollupWide",
  "cubeWide",
] as const;
/** An unlimited meter that only sums its charges. */
function counting(): CopyMeter & { charged: number } {
  const meter = {
    charged: 0,
    charge(bytes: number) {
      meter.charged += bytes;
    },
  };
  return meter;
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
              const meter = counting();
              const lowered = lowerBodyMetered(
                { planner: core.lowerer, context: core.context() },
                catalog,
                stored,
                meter,
              );
              if (lowered.kind !== "modify") throw new Error("modify body required");
              const actual = bodyRetainedBytes(lowered.plan, stored.bindings.length),
                bound = meter.charged;
              expect(actual, stored.sql.slice(0, 120)).toBeLessThanOrEqual(bound);
              worst = Math.max(worst, actual / bound);
            }
          for (const table of catalog.tables) {
            const meter = counting();
            meter.charge(rulePreparationBound(table));
            const expr = withCopyMeter(meter, () => core.lowerer.lowerTableRules(table, catalog)),
              program = compileTableRules(table, catalog.version, expr);
            const defaults = table.columns
              .filter((column) => column.generatedSql === null)
              .map((column) => ({
                tableId: table.id,
                columnId: column.id,
                expr: expr.defaults?.find((d) => d.columnId === column.id)?.expr ?? null,
              }));
            const actual = ruleRetainedBytes({ expr, program, defaults }),
              bound = meter.charged;
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
        numRuns: 40,
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
          // Review scenario: (note, 100,000 x) NOT IN 100 two-column rows, in a DELETE body.
          [
            {
              kind: "delete",
              chain: 0,
              caseDepth: 0,
              list: 0,
              literal: 100000,
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
              copies: "rowNotInLiteral",
              operand: 0,
              arms: 100,
            },
          ],
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

it("runs single-set grouping and quoted keyword bodies like PostgreSQL", async () => {
  const owner = await publicOwner({ capBytes: 64 << 20 }),
    db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() }),
    pg = new PGlite();
  const schema = [
    "CREATE TABLE p(id INT PRIMARY KEY)",
    "CREATE TABLE t(k INT PRIMARY KEY, v INT)",
    "CREATE TABLE u(k INT PRIMARY KEY, w INT)",
    'CREATE TABLE "cube"(k INT, "rollup" INT)',
    "INSERT INTO p VALUES (1), (2)",
    "INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)",
    "INSERT INTO u VALUES (1, 10), (3, 30)",
  ];
  const bodies = [
    "UPDATE t SET v = v + 100 WHERE k IN (SELECT k FROM u GROUP BY GROUPING SETS ((k)))",
    'INSERT INTO "cube" VALUES (OLD.id, OLD.id * 10)',
  ];
  const state = [
    "SELECT * FROM t ORDER BY k",
    'SELECT * FROM "cube" ORDER BY k, "rollup"',
    "SELECT * FROM p ORDER BY id",
  ];
  try {
    for (const sql of schema) {
      await db.execute(sql);
      await pg.exec(sql);
    }
    for (const [i, body] of bodies.entries()) {
      await db.execute(`CREATE TRIGGER g${String(i)} AFTER UPDATE ON p BEGIN ${body}; END`);
      await pg.exec(
        `CREATE FUNCTION g${String(i)}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${body}; RETURN NEW; END $$;
         CREATE TRIGGER g${String(i)} AFTER UPDATE ON p FOR EACH ROW EXECUTE FUNCTION g${String(i)}()`,
      );
    }
    const mutation = "UPDATE p SET id = id + 10";
    expect((await db.execute(mutation)).affectedRows).toBe(2);
    await pg.exec(mutation);
    for (const sql of state)
      expect((await db.query(sql)).rows(), sql).toEqual((await pg.query(sql)).rows);
    // A multi-set ROLLUP keeps the executor's existing unsupported refusal and changes nothing.
    await db.execute(
      "CREATE TRIGGER r AFTER UPDATE ON p BEGIN UPDATE t SET v = 0 WHERE k IN (SELECT k FROM u GROUP BY ROLLUP (k, w)); END",
    );
    const error = await db.execute(mutation).then(
      () => null,
      (e: unknown) => e as { sqlState?: string; message?: string },
    );
    expect(error?.sqlState).toBe("0A000");
    expect(error?.message).toMatch(/set operation other than binary EXCEPT/u);
    for (const sql of state)
      expect((await db.query(sql)).rows(), sql).toEqual((await pg.query(sql)).rows);
  } finally {
    await db.close();
    await owner.close();
    await pg.close();
  }
});

it("charges a wide ROLLUP before building its prefixes (peak check)", async () => {
  // Live heap at the meter's abort must not exceed what the meter charged: an allocation built
  // before its charge shows up here even though no plan graph survives.
  setFlagsFromString("--expose-gc");
  const gc = runInNewContext("gc") as () => void;
  const core = new StorageCore(256);
  await core.open();
  try {
    for (const sql of [
      "CREATE TABLE p(id INT PRIMARY KEY)",
      "CREATE TABLE t(id INT PRIMARY KEY, note TEXT)",
      "CREATE TABLE u(k INT PRIMARY KEY, w INT)",
      "CREATE TRIGGER r AFTER UPDATE ON p BEGIN UPDATE t SET note = 'x' WHERE id = OLD.id; END",
    ])
      await core.execute(sql);
    const columns = Array.from({ length: 2000 }, () => "k").join(", ");
    const sql = `DELETE FROM t WHERE id IN (SELECT k FROM u GROUP BY ROLLUP (${columns}))`;
    // The statement's own base share (its parse and linear lowering) is charged first, as native
    // preparation charges a body's base share before lowering it.
    const scan = scanSql(sql),
      base = PREPARATION_TOKEN_BYTES * scan.tokens + PREPARATION_TEXT_BYTES * scan.characters,
      budget = base + (4 << 20);
    let charged = base;
    const meter: CopyMeter = {
      charge(bytes) {
        if (charged + bytes > budget) throw new RangeError("meter budget");
        charged += bytes;
      },
    };
    gc();
    const before = process.memoryUsage().heapUsed;
    let live = 0;
    try {
      withCopyMeter(meter, () => core.lowerer.lower(sql, core.context()));
    } catch (error) {
      live = process.memoryUsage().heapUsed - before;
      expect(String(error)).toMatch(/meter budget/u);
    }
    // The unmetered prefixes alone were about 16 MB for this body.
    expect(live).toBeLessThanOrEqual(charged + (2 << 20));
  } finally {
    await core.close();
  }
});
