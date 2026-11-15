/** Shipped ColumnDefault keeps the API source kind and JS value type, not just SQL spelling. */
import fc from "fast-check";
import { dateIsoString } from "../../date-value.js";
import { catalogDate, dateOverrides } from "./catalog-date.test-support.js";
import { expect, it } from "vitest";
import { column, schema, table } from "../../engine/schema.js";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_DEFAULT_SOURCE_REACH, createReach } from "../../testing/reach.js";
import { inspectCatalog } from "../lower/catalog-schema.js";
import { migrateCatalog } from "../owner/catalog-migration.js";
import { StorageCore } from "./storage-core.js";

const kinds = [
  "boolean",
  "integer",
  "float",
  "text",
  "enum",
  "timestamp",
  "timestamptz",
  "numeric-number",
  "numeric-string",
] as const;
function definition(
  kind: (typeof kinds)[number],
  n: number,
  expression: boolean,
  override: (typeof dateOverrides)[number],
) {
  const text = kind === "text" && n === 0 ? "" : `汉'${String(n)}`,
    stringSql = `'${text.replaceAll("'", "''")}'`,
    date = catalogDate(Date.UTC(2001, 1, 3, 4, 5, 6, 123) + n, override);
  const value =
    kind === "boolean"
      ? expression
        ? column.boolean().defaultSql(n % 2 === 0 ? "TRUE" : "FALSE")
        : column.boolean().default(n % 2 === 0)
      : kind === "integer"
        ? expression
          ? column.integer().defaultSql(String(n))
          : column.integer().default(n)
        : kind === "float"
          ? expression
            ? column
                .number()
                .defaultSql(n === 0 ? "CAST('-0' AS DOUBLE PRECISION)" : String(n + 0.25))
            : column.number().default(n === 0 ? -0 : n + 0.25)
          : kind === "text"
            ? expression
              ? column.string().defaultSql(stringSql)
              : column.string().default(text)
            : kind === "enum"
              ? expression
                ? column.enum([text, "suffix"]).defaultSql(stringSql)
                : column.enum([text, "suffix"]).default(text)
              : kind === "numeric-number"
                ? expression
                  ? column.numeric().defaultSql(String(n + 0.25))
                  : column.numeric().default(n + 0.25)
                : kind === "numeric-string"
                  ? expression
                    ? column.numeric().defaultSql(`'${String(n)}.250'`)
                    : column.numeric().default(`${String(n)}.250`)
                  : kind === "timestamp"
                    ? expression
                      ? column.timestamp().defaultSql(`CAST('${dateIsoString(date)}' AS TIMESTAMP)`)
                      : column.timestamp().default(date)
                    : expression
                      ? column
                          .datetime()
                          .defaultSql(`CAST('${dateIsoString(date)}' AS TIMESTAMP WITH TIME ZONE)`)
                      : column.datetime().default(date);
  return schema([table("defaults", { id: column.integer().unique(), value })]);
}
for (const seed of seedsFor("next-catalog-default-source", [0xb7ec, 0xb7ed]))
  it(`retains literal/expression source identity through migrate and reopen seed ${String(seed)}`, async () => {
    const reach = createReach("default source", CATALOG_DEFAULT_SOURCE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...kinds),
        fc.oneof(fc.integer({ min: -1000, max: 1000 }), fc.constant(-0)),
        fc.boolean(),
        fc.boolean(),
        fc.constantFrom(...dateOverrides),
        async (kind, n, expression, checkpoint, override) => {
          reach.beginHistory();
          const core = await new StorageCore().open(),
            old = new MinnowDatabase(new MemoryBlockStore()),
            def = definition(kind, n, expression, override);
          const migrate = (next = def) =>
            migrateCatalog(core.harness.path, core.lowerer, core.harness.governor, next);
          try {
            await old.migrate(def);
            await migrate();
            let expected = (await old.introspect()).tables[0]?.columns[1]?.defaultValue;
            const check = () =>
              expect(
                inspectCatalog(core.harness.path.catalog).tables[0]?.columns[1]?.defaultValue,
              ).toEqual(expected);
            check();
            expect((await migrate()).steps).toEqual([]);
            if (checkpoint) await core.harness.path.recycleLog();
            await core.reopen();
            check();
            expect((await migrate()).steps).toEqual([]);
            const alternate = definition(kind, n, !expression, override);
            await old.migrate(alternate);
            await migrate(alternate);
            expected = (await old.introspect()).tables[0]?.columns[1]?.defaultValue;
            check();
            expect((await migrate(alternate)).steps).toEqual([]);
            reach.reach("ALTER source kind");
            const valueColumn = alternate.tables[0]?.columns.value;
            if (!valueColumn) throw new Error("missing source declaration");
            const { defaultSpec, ...without } = valueColumn;
            void defaultSpec;
            const reduced = schema([
              table("defaults", { id: column.integer().unique(), value: without }),
            ]);
            await old.migrate(reduced);
            await migrate(reduced);
            expected = undefined;
            check();
            expect((await migrate(reduced)).steps).toEqual([]);
            reach.reach("default removal clears source");
            await old.migrate(alternate);
            await migrate(alternate);
            expected = (await old.introspect()).tables[0]?.columns[1]?.defaultValue;
            await old.execute("ALTER TABLE defaults ADD COLUMN extra INTEGER");
            await core.execute("ALTER TABLE defaults ADD COLUMN extra INTEGER");
            check();
            await core.reopen();
            check();
            reach.reach(kind);
            if (kind === "float" && n === 0) reach.reach("signed zero");
            if (kind === "integer" && Object.is(n, -0)) reach.reach("integer signed zero");
            if ((kind === "timestamp" || kind === "timestamptz") && override !== "none")
              reach.reach(
                override === "throw" ? "throwing Date methods" : "overridden Date methods",
              );
            if (kind === "text" && n === 0) reach.reach("empty text");
            reach.reach(expression ? "expression spelling" : "literal JS type");
            reach.reach(checkpoint ? "checkpoint" : "WAL");
            reach.reach("idempotent");
            reach.reach("unrelated ALTER preserves source");
          } finally {
            await core.close();
            await old.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
          reach.reach("zero resources");
        },
      ),
      {
        seed,
        numRuns: 36,
        examples: [
          ...kinds.flatMap((kind) => [
            [kind, -7, false, false, "none"] as [
              (typeof kinds)[number],
              number,
              boolean,
              boolean,
              (typeof dateOverrides)[number],
            ],
            [kind, 8, true, true, "none"] as [
              (typeof kinds)[number],
              number,
              boolean,
              boolean,
              (typeof dateOverrides)[number],
            ],
          ]),
          ["float", 0, false, true, "none"],
          ["float", 0, true, false, "none"],
          ["text", 0, false, false, "none"],
          ["text", 0, true, true, "none"],
          ["integer", -0, false, true, "none"],
          ["timestamp", 0, false, false, "epoch"],
          ["timestamptz", 0, false, true, "epoch"],
          ["timestamp", 1, false, true, "throw"],
          ["timestamptz", -1, false, false, "throw"],
        ],
      },
    );
    reach.assertReached();
  }, 120_000);
