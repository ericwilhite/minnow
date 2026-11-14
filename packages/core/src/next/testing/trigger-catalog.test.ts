/** The shipped CREATE/DROP/schema-dependency contract, persisted through native MCAT/WAL. */
import { column, schema, table as schemaTable } from "../../engine/schema.js";
import { currentInput } from "../lower/ddl.js";
import { inspectCatalog } from "../lower/catalog-schema.js";
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TRIGGER_CATALOG_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { Catalog } from "../write/catalog.js";
import { decodeCatalogStream, encodeCatalogStream } from "../write/catalog-stream.js";
import { encodeCatalogRecord, decodeCatalogRecord } from "../write/catalog-records.js";
const q = (s: string) => `"${s.replaceAll('"', '""')}"`;
const accepts = async (run: () => unknown) => {
  try {
    await run();
    return true;
  } catch {
    return false;
  }
};
for (const seed of seedsFor("next-trigger-catalog", [0xb7c4, 0xb7c5]))
  it(`keeps shipped trigger definitions and stable dependencies seed ${String(seed)}`, async () => {
    const reach = createReach("trigger catalog", TRIGGER_CATALOG_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("insert", "update", "delete"),
        fc.constantFrom("before", "after"),
        fc.constantFrom("n", "Case", "a.b"),
        fc.integer({ min: 1, max: 3 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (event, timing, name, bodies, implicit, checkpoint, repeated) => {
          reach.beginHistory();
          const native = await new StorageCore().open(),
            old = new MinnowDatabase(new MemoryBlockStore()),
            n = q(name);
          const both = async (sql: string) => {
            const expected = await accepts(() => old.execute(sql));
            expect(await accepts(() => native.execute(sql)), sql).toBe(expected);
            return expected;
          };
          try {
            for (const sql of [
              `CREATE TABLE src(id INT PRIMARY KEY,${n} INT,msg TEXT)`,
              "CREATE TABLE audit(msg TEXT,n INT DEFAULT 17,g INT GENERATED ALWAYS AS(n+1) STORED)",
              "CREATE TABLE target(id INT PRIMARY KEY,n INT)",
            ])
              expect(await both(sql)).toBe(true);
            const source = event === "delete" ? "OLD" : "NEW",
              statements = Array.from({ length: bodies }, (_, i) =>
                i === 0
                  ? implicit
                    ? `INSERT INTO audit VALUES(${source}.msg,${source}.${n},DEFAULT)${repeated ? `,(${source}.msg,${source}.${n},DEFAULT)` : ""}`
                    : `INSERT INTO audit(msg,n) VALUES(${source}.msg,${source}.${n})${repeated ? `,(${source}.msg,${source}.${n})` : ""}`
                  : i === 1
                    ? `UPDATE target SET n=n+${source}.${n} WHERE id=${source}.id`
                    : `DELETE FROM target WHERE id=${source}.id`,
              );
            const create = `CREATE TRIGGER tr ${timing.toUpperCase()} ${event.toUpperCase()} ON src BEGIN ${statements.join(";")}; END`;
            expect(await both(create)).toBe(true);
            const def = native.harness.path.catalog.triggers[0];
            if (!def) throw new Error("missing trigger");
            expect(def.event).toBe(event);
            expect(def.timing).toBe(timing);
            expect(
              inspectCatalog(native.harness.path.catalog).tables.find((t) => t.name === "src")
                ?.triggers,
            ).toEqual([{ id: String(def.id), name: def.name, event, timing }]);
            reach.reach("introspection");
            expect(def.statements.map((b) => b.kind)).toEqual([
              "insert",
              ...(bodies > 1 ? ["update"] : []),
              ...(bodies > 2 ? ["delete"] : []),
            ]);
            expect(def.statements[0]?.bindings.map((b) => b.name)).toEqual(
              Array.from({ length: repeated ? 2 : 1 }, () => ["msg", name]).flat(),
            );
            expect(
              def.statements.every((b) =>
                b.bindings.every((x) => x.source === source.toLowerCase()),
              ),
            ).toBe(true);
            const again = encodeCatalogStream(
              decodeCatalogStream(native.harness.path.catalog.encode()),
            );
            expect(Catalog.decode(again).triggers).toEqual([def]);
            const record = encodeCatalogRecord("trigger", def);
            try {
              expect(decodeCatalogRecord("trigger", record.bytes)).toEqual(def);
            } finally {
              record.close();
            }
            expect(await both(create)).toBe(false);
            expect(
              await both(
                "CREATE TRIGGER tr AFTER INSERT ON target BEGIN INSERT INTO audit(msg) VALUES('duplicate'); END",
              ),
            ).toBe(false);
            reach.reach("global name uniqueness");
            const invalid = [
              `CREATE TRIGGER bad ${timing.toUpperCase()} ${event.toUpperCase()} ON src BEGIN INSERT INTO audit(msg) VALUES(${event === "insert" ? "OLD" : event === "delete" ? "NEW" : "NEW"}.missing); END`,
              `CREATE TRIGGER bad AFTER INSERT ON src BEGIN INSERT INTO src(id) VALUES(1); END`,
              `CREATE TRIGGER bad AFTER INSERT ON src BEGIN INSERT INTO target(id,n) VALUES(1,2); END`,
              `CREATE TRIGGER bad AFTER INSERT ON src BEGIN UPDATE audit SET n=1; END`,
              `CREATE TRIGGER bad AFTER INSERT ON src BEGIN INSERT INTO audit(msg,n,g) VALUES('bad',1,9); END`,
              `CREATE TRIGGER bad AFTER INSERT ON src BEGIN UPDATE target SET missing=1; END`,
            ];
            for (const sql of invalid) expect(await both(sql)).toBe(false);
            reach.reach("invalid definition refused");
            for (const sql of [
              `ALTER TABLE src RENAME COLUMN ${n} TO renamed`,
              `ALTER TABLE src DROP COLUMN ${n}`,
              "DROP TABLE audit",
            ])
              expect(() => native.lowerer.lower(sql, native.context())).toThrow(
                expect.objectContaining({ sqlState: "2BP01" }),
              );
            expect(await both(`ALTER TABLE src RENAME COLUMN ${n} TO renamed`)).toBe(false);
            expect(await both(`ALTER TABLE src DROP COLUMN ${n}`)).toBe(false);
            expect(await both("DROP TABLE audit")).toBe(false);
            reach.reach("dependency refused");
            expect(await both("ALTER TABLE audit ADD COLUMN extra TEXT")).toBe(!implicit);
            if (checkpoint) {
              await native.harness.path.recycleLog();
              reach.reach("checkpoint");
            } else reach.reach("WAL");
            await native.reopen();
            expect(native.harness.path.catalog.triggers).toEqual([def]);
            reach.reach("reopen");
            const data = decodeCatalogStream(native.harness.path.catalog.encode());
            const mutations = [
              { ...def, tableId: 9999 },
              { ...def, event: "other" },
              { ...def, statements: [] },
              {
                ...def,
                statements: def.statements.map((b) => ({ ...b, targetTableId: def.tableId })),
              },
              {
                ...def,
                statements: def.statements.map((b) => ({
                  ...b,
                  bindings: b.bindings.map((x) => ({ ...x, columnId: 9999 })),
                })),
              },
            ];
            for (const bad of mutations)
              expect(() => Catalog.fromStored({ ...data, triggers: [bad as typeof def] })).toThrow(
                expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
              );
            expect(() =>
              Catalog.fromStored({ ...data, triggers: [{ ...def, dialectVersion: 2 }] }),
            ).toThrow(expect.objectContaining({ code: "MINNOW_FORMAT_UNSUPPORTED" }));
            reach.reach("corruption refused");
            const optional = decodeCatalogStream(
              data.tables.length ? native.harness.path.catalog.encode() : again,
            );
            expect(optional.triggers).toEqual([def]);
            expect(await both("DROP TRIGGER tr")).toBe(true);
            expect(native.harness.path.catalog.triggers).toEqual([]);
            expect(await both(create)).toBe(true);
            expect(native.harness.path.catalog.triggers[0]?.id).toBeGreaterThan(def.id);
            reach.reach("identity not reused");
            expect(await both("DROP TABLE src")).toBe(true);
            expect(native.harness.path.catalog.triggers).toEqual([]);
            reach.reach("owner drop");
            expect(await both("DROP TRIGGER tr")).toBe(false);
            reach.reach(event);
            reach.reach(timing);
            reach.reach(name === "n" ? "plain binding" : "quoted binding");
            reach.reach(implicit ? "positional body" : "named body");
            reach.reach(bodies > 1 ? "ordered bodies" : "single body");
            reach.reach(repeated ? "repeated binding" : "single binding");
          } finally {
            await native.close();
            await old.close();
          }
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          ["insert", "before", "n", 1, false, false, false],
          ["update", "after", "a.b", 3, true, true, true],
          ["delete", "before", "Case", 2, false, true, true],
        ],
      },
    );
    reach.assertReached();
  }, 120_000);

it("folds unquoted trigger names and keeps quoted names exactly as PostgreSQL", async () => {
  const pg = new PGlite(),
    native = await new StorageCore().open();
  try {
    await native.execute("CREATE TABLE src(n INT)");
    await native.execute("CREATE TABLE audit(n INT)");
    await pg.exec(
      "CREATE TABLE src(n INT);CREATE FUNCTION trigger_names() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
    );
    const check = async (name: string, drop = false) => {
      const ours = drop
        ? `DROP TRIGGER ${name}`
        : `CREATE TRIGGER ${name} AFTER INSERT ON src BEGIN INSERT INTO audit(n) VALUES(1); END`;
      const theirs = drop
        ? `DROP TRIGGER ${name} ON src`
        : `CREATE TRIGGER ${name} AFTER INSERT ON src FOR EACH ROW EXECUTE FUNCTION trigger_names()`;
      expect(await accepts(() => native.execute(ours))).toBe(await accepts(() => pg.exec(theirs)));
      expect(native.harness.path.catalog.triggers.map((t) => t.name).sort()).toEqual(
        (
          await pg.query<{ tgname: string }>(
            "SELECT tgname FROM pg_trigger WHERE tgrelid='src'::regclass ORDER BY tgname",
          )
        ).rows.map((r) => r.tgname),
      );
    };
    await check("MiXeD");
    await check('"mixed"');
    await check('"MiXeD"');
    await check("MIXED", true);
    await check('"MiXeD"', true);
  } finally {
    await native.close();
    await pg.close();
  }
});

it("keeps required trigger INSERT defaults through managed-schema changes", async () => {
  const native = await new StorageCore().open(),
    old = new MinnowDatabase(new MemoryBlockStore());
  try {
    await native.execute("CREATE TABLE source(n INT NOT NULL)");
    await native.execute("CREATE TABLE audit(msg TEXT,n INT NOT NULL DEFAULT 17)");
    await old.migrate(
      schema([
        schemaTable("source", { n: column.integer() }),
        schemaTable("audit", { msg: column.string().nullable(), n: column.integer().default(17) }),
      ]),
    );
    const sql =
      "CREATE TRIGGER uses_default AFTER INSERT ON source BEGIN INSERT INTO audit(msg) VALUES('value'); END";
    await native.execute(sql);
    await old.execute(sql);
    expect(
      await accepts(() =>
        old.migrate(
          schema([
            schemaTable("source", { n: column.integer() }),
            schemaTable("audit", { msg: column.string().nullable(), n: column.integer() }),
          ]),
        ),
      ),
    ).toBe(false);
    const target = native.harness.path.catalog.tableByName("audit");
    if (!target) throw new Error("missing target");
    const { input, columns } = currentInput(target);
    const altered = columns.map((c) => {
      if (c.name !== "n") return c;
      const { defaultDependencies: _d, ...rest } = c;
      void _d;
      return { ...rest, defaultSql: null };
    });
    await expect(
      native.change({
        kind: "alterTable",
        tableId: target.id,
        table: { ...input, columns: altered },
      }),
    ).rejects.toMatchObject({ code: "MINNOW_CATALOG_CHANGE" });
    expect(native.harness.path.catalog.table(target.id)?.columns[1]?.defaultSql).toBe("17");
  } finally {
    await native.close();
    await old.close();
  }
});
