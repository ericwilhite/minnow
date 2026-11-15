/** Every supported logical descriptor survives the ordinary WAL/native reopen boundary. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_FREEZE_TYPES_REACH, createReach } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";

for (const seed of seedsFor("next-catalog-freeze-types", [0xb7e6, 0xb7e7]))
  it(`reopens all logical catalog descriptors seed ${String(seed)}`, async () => {
    const reach = createReach("catalog freeze descriptors", CATALOG_FREEZE_TYPES_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 255 }),
        fc.integer({ min: 10, max: 40 }),
        fc.integer({ min: 0, max: 9 }),
        fc.boolean(),
        async (width, precision, scale, checkpoint) => {
          reach.beginHistory();
          const core = await new StorageCore().open();
          try {
            await core.execute("CREATE TYPE m AS ENUM ('z', 'a', '汉')");
            await core.execute(`CREATE TABLE typed(
              id INT PRIMARY KEY, small SMALLINT, big BIGINT, single REAL, double DOUBLE PRECISION,
              n NUMERIC(${String(precision)},${String(scale)}), v VARCHAR(${String(width)}), c CHAR(${String(width)}),
              b BOOLEAN, d DATE, t TIME, ts TIMESTAMP, tz TIMESTAMPTZ,
              j JSON, jb JSONB, u UUID, i INTERVAL, a INTEGER[], e m,
              g INT GENERATED ALWAYS AS (id+1) STORED, CONSTRAINT positive CHECK(id>0)
            )`);
            const catalog = core.harness.path.catalog,
              table = catalog.tableByName("typed");
            if (!table) throw new Error("missing generated type control");
            const types = new Map(table.columns.map((column) => [column.name, column.type]));
            for (const [name, type] of Object.entries({
              id: { kind: "int32", bits: 32 },
              small: { kind: "int32", bits: 16 },
              big: { kind: "int64" },
              single: { kind: "float32" },
              double: { kind: "float64" },
              n: { kind: "decimal", precision, scale },
              v: { kind: "text", maxLength: width },
              c: { kind: "text", maxLength: width, fixed: true },
              b: { kind: "bool" },
              d: { kind: "date" },
              t: { kind: "time" },
              ts: { kind: "timestamp", zoned: false },
              tz: { kind: "timestamp", zoned: true },
              j: { kind: "json", binary: false },
              jb: { kind: "json", binary: true },
              u: { kind: "uuid" },
              i: { kind: "interval" },
              a: { kind: "array", element: { kind: "int32", bits: 32 } },
              e: { kind: "enum", name: "m", labels: ["z", "a", "汉"] },
              g: { kind: "int32", bits: 32 },
            } as const)) {
              expect(types.get(name), name).toEqual(type);
              reach.reach(type.kind);
            }
            expect(table.columns.find((column) => column.name === "n")?.declared.sqlDomain).toEqual(
              { kind: "numeric", precision, scale },
            );
            expect(
              table.columns.find((column) => column.name === "v")?.declared.declaredLength,
            ).toEqual({ length: width, fixed: false });
            expect(
              table.columns.find((column) => column.name === "c")?.declared.declaredLength,
            ).toEqual({ length: width, fixed: true });
            const bytes = catalog.encode();
            if (checkpoint) await core.harness.path.recycleLog();
            await core.reopen();
            expect(core.harness.path.catalog.encode()).toEqual(bytes);
            reach.reach(checkpoint ? "checkpoint" : "WAL");
            await core.execute("CREATE VIEW typed_v AS SELECT id, n, g FROM typed");
            await core.reopen();
            expect(core.harness.path.catalog.tableByName("typed")).toEqual(table);
            expect(
              core.harness.path.catalog.views[0]?.outputs?.map((column) => column.name),
            ).toEqual(["id", "n", "g"]);
            reach.reach("continue DDL");
          } finally {
            await core.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
          reach.reach("zero resources");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [1, 10, 0, false],
          [255, 40, 9, true],
        ],
      },
    );
    reach.assertReached();
  }, 120_000);
