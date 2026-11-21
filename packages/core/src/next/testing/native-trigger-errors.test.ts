/** Error precedence at real root/row-rule/RI/body boundaries, not a synthetic action adapter. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_TRIGGER_ERRORS_REACH } from "../../testing/reach.js";
import { nativeTriggerSession } from "./native-trigger-session.test-support.js";
const cases = [
  "before-input",
  "after-input",
  "before-generated",
  "after-generated",
  "check-before-next-row",
  "root-unique",
  "ri-unique",
  "ri-check",
  "child-before",
  "parent-before-child-after",
  "early-parent-after",
  "before-restrict",
  "body-unique",
] as const;
type Kind = (typeof cases)[number];
const expected: Record<Kind, string> = {
  "before-input": "23514",
  "after-input": "22003",
  "before-generated": "23514",
  "after-generated": "22012",
  "check-before-next-row": "23514",
  "root-unique": "23505",
  "ri-unique": "23505",
  "ri-check": "23514",
  "child-before": "22012",
  "parent-before-child-after": "22012",
  "early-parent-after": "22012",
  "before-restrict": "22012",
  "body-unique": "23505",
};
async function state(work: () => Promise<unknown>) {
  try {
    await work();
    return null;
  } catch (e) {
    if (typeof e === "object" && e !== null) {
      if ("sqlState" in e && typeof e.sqlState === "string") return e.sqlState;
      if ("code" in e && typeof e.code === "string") return e.code;
    }
    throw e;
  }
}
function script(kind: Kind, rows: number, reverse: boolean) {
  const generated = kind.endsWith("generated"),
    guardCheck = ["before-input", "after-input", "before-generated", "after-generated"].includes(
      kind,
    ),
    rootCheck = kind === "check-before-next-row",
    childCheck = ["ri-check", "child-before", "early-parent-after"].includes(kind),
    childUnique = kind === "ri-unique",
    deleting = kind === "before-restrict",
    event = deleting ? "DELETE" : "UPDATE",
    source = deleting ? "OLD" : "NEW";
  const schema = [
    `CREATE TABLE p(id INT PRIMARY KEY,n INT${rootCheck ? " CONSTRAINT p_positive CHECK(n>0)" : ""}${generated ? ",g INT GENERATED ALWAYS AS(100/n) STORED" : ""})`,
    `CREATE TABLE c(id INT PRIMARY KEY,pid INT${childUnique ? " DEFAULT 99 UNIQUE" : ""} REFERENCES p(id) ON UPDATE ${childUnique ? "SET DEFAULT" : "CASCADE"} ON DELETE ${deleting ? "RESTRICT" : "CASCADE"}${childCheck ? " CONSTRAINT c_small CHECK(pid<10)" : ""})`,
    `CREATE TABLE guard_table(id INT PRIMARY KEY,v INT${guardCheck ? " CONSTRAINT guard_positive CHECK(v>0)" : ""})`,
    "CREATE TABLE audit(n INT)",
    "CREATE TABLE mirror(n INT)",
    "CREATE TABLE target_unique(id INT PRIMARY KEY,v INT UNIQUE)",
  ];
  const seed = [
    `INSERT INTO p(id,n) VALUES ${Array.from({ length: rows }, (_, i) => `(${String(i + 1)},1)`).join(",")},(99,1)`,
    `INSERT INTO c VALUES ${Array.from({ length: rows }, (_, i) => `(${String(i + 101)},${String(i + 1)})`).join(",")}${childUnique ? ",(999,99)" : ""}`,
    "INSERT INTO guard_table VALUES(1,1)",
    "INSERT INTO target_unique VALUES(1,2),(2,1)",
  ];
  const before = kind.startsWith("before-") || rootCheck;
  const parentExpr = guardCheck
    ? "-1"
    : rootCheck
      ? "1/(NEW.id-2)+2"
      : deleting
        ? "1/(OLD.id-1)"
        : kind === "root-unique"
          ? "1/(NEW.id-99)"
          : kind === "child-before"
            ? "2147483647+NEW.id"
            : "1/(NEW.id-11)";
  const specs = [
    {
      name: kind === "early-parent-after" ? "A_bad" : "zz_bad",
      table: "p",
      event,
      timing: before ? "BEFORE" : "AFTER",
      commands: [
        `INSERT INTO audit VALUES(${source}.id)`,
        kind === "body-unique"
          ? "UPDATE target_unique SET v=1 WHERE id=NEW.id"
          : `UPDATE guard_table SET v=${parentExpr} WHERE id=1`,
      ],
    },
    {
      name: "audit_echo",
      table: "audit",
      event: "INSERT",
      timing: "AFTER",
      commands: ["INSERT INTO mirror VALUES(NEW.n)"],
    },
    {
      name: "child_before",
      table: "c",
      event,
      timing: "BEFORE",
      commands: [
        `INSERT INTO audit VALUES(${source}.id)`,
        ...(kind === "child-before" ? ["UPDATE guard_table SET v=1/(NEW.pid-11) WHERE id=1"] : []),
      ],
    },
    ...(kind === "body-unique"
      ? [
          {
            name: "target_after",
            table: "target_unique",
            event: "UPDATE",
            timing: "AFTER",
            commands: ["UPDATE guard_table SET v=1/(NEW.id-1) WHERE id=1"],
          },
        ]
      : []),
    ...(kind === "parent-before-child-after"
      ? [
          {
            name: "child_bad",
            table: "c",
            event,
            timing: "AFTER",
            commands: ["UPDATE guard_table SET v=2147483647+NEW.pid WHERE id=1"],
          },
        ]
      : []),
  ];
  if (reverse) specs.reverse();
  const sql = deleting
    ? "DELETE FROM p WHERE id<99"
    : kind.endsWith("input")
      ? "UPDATE p SET n=2147483647+(id-1) WHERE id<99"
      : generated
        ? "UPDATE p SET n=id-1 WHERE id<99"
        : rootCheck
          ? "UPDATE p SET n=-1 WHERE id<99"
          : kind === "root-unique"
            ? "UPDATE p SET id=id+98 WHERE id<99"
            : kind === "body-unique"
              ? "UPDATE p SET n=n+1 WHERE id<99"
              : "UPDATE p SET id=id+10 WHERE id<99";
  return { schema, seed, specs, sql };
}
const queries = [
  "SELECT * FROM p ORDER BY id",
  "SELECT * FROM c ORDER BY id",
  "SELECT * FROM guard_table ORDER BY id",
  "SELECT * FROM audit ORDER BY n",
  "SELECT * FROM mirror ORDER BY n",
  "SELECT * FROM target_unique ORDER BY id",
];
for (const seed of seedsFor("next-native-trigger-errors", [0xb7e8, 0xb7e9]))
  it(`matches PG native trigger error precedence seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("native trigger errors", NATIVE_TRIGGER_ERRORS_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: cases.length - 1 }).map((index) => {
            const kind = cases[index];
            if (!kind) throw new Error("Invalid error-case index");
            return kind;
          }),
          fc.integer({ min: 2, max: 4 }),
          fc.boolean(),
          fc.boolean(),
          async (kind, rows, published, reverse) => {
            reach.beginHistory();
            const h = script(kind, rows, reverse),
              native = await nativeTriggerSession(
                [
                  ...h.schema,
                  ...h.seed,
                  ...h.specs.map(
                    (s) =>
                      `CREATE TRIGGER "${s.name}" ${s.timing} ${s.event} ON ${s.table} BEGIN ${s.commands.join(";")}; END`,
                  ),
                ],
                h.sql,
                published,
              );
            try {
              await pg.exec(
                "DROP TABLE IF EXISTS c,p,guard_table,audit,mirror,target_unique CASCADE",
              );
              for (const sql of [...h.schema, ...h.seed]) await pg.exec(sql);
              for (const [i, s] of h.specs.entries())
                await pg.exec(
                  `CREATE OR REPLACE FUNCTION error_f${String(i)}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${s.commands.join(";")}; RETURN ${s.event === "DELETE" ? "OLD" : "NEW"}; END $$; CREATE TRIGGER "${s.name}" ${s.timing} ${s.event} ON ${s.table} FOR EACH ROW EXECUTE FUNCTION error_f${String(i)}()`,
                );
              const before = new Map<string, unknown>();
              for (const query of queries) before.set(query, (await pg.query(query)).rows);
              expect(await state(() => pg.exec(h.sql)), `authority error ${kind}`).toBe(
                expected[kind],
              );
              expect(await state(() => native.execute({ slots: [] })), `native error ${kind}`).toBe(
                expected[kind],
              );
              for (const query of queries) {
                expect(await native.query(query), `native error undo ${kind}: ${query}`).toEqual(
                  before.get(query),
                );
                expect((await pg.query(query)).rows).toEqual(before.get(query));
              }
              await native.commit();
              for (const query of queries)
                expect(await native.committedQuery(query)).toEqual(before.get(query));
              reach.reach(kind);
              reach.reach(published ? "published source" : "resident source");
              reach.reach(reverse ? "reverse creation" : "forward creation");
              reach.reach("exact statement undo");
            } finally {
              await native.close();
            }
            reach.reach("zero native ownership");
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: cases.map((kind, i): [Kind, number, boolean, boolean] => [
            kind,
            i % 2 ? 3 : 2,
            i % 2 === 0,
            i % 2 === 1,
          ]),
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
