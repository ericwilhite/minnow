/** Closed text sets remain column restrictions when complete-row rules own validation. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { MinnowDatabase, type CreateTableInput } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  MODIFY_CLOSED_SET_REACH,
  CLOSED_SET_REPLAY_REACH,
} from "../../testing/reach.js";
import { LoweredDatabase } from "./lowering-harness.js";
import { compileTableRules } from "../exec/rules.js";
import { ints, texts } from "../write/test-support.js";
import { newDictionaryId, type Vector } from "../vector/vector.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

for (const seed of seedsFor("next-modify-closed-sets", [0xb5ce]))
  it(`keeps closed sets with CHECK/generated rules seed ${String(seed)}`, async () => {
    const reach = createReach("closed-set DML", MODIFY_CLOSED_SET_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 1, max: 6 }),
        async (check, generated, n) => {
          reach.beginHistory();
          const core = await new StorageCore().open(),
            old = new MinnowDatabase(new MemoryBlockStore()),
            pg = new PGlite();
          const input: CreateTableInput = {
            name: "closed_rows",
            uniqueKey: "id",
            columns: [
              { name: "id", type: "number", integer: true },
              { name: "tag", type: "string", nullable: true, enumValues: ["ok", "\u00e9"] },
              { name: "n", type: "number", integer: true },
              ...(generated
                ? [
                    {
                      name: "g",
                      type: "number" as const,
                      integer: true as const,
                      generatedValue: { kind: "stored" as const, sql: "n+1" },
                    },
                  ]
                : []),
            ],
            ...(check ? { checks: [{ name: "positive", sql: "n>0" }] } : {}),
          };
          try {
            await core.createTable(input);
            await old.createTable(input);
            // A PG CHECK is the equivalent accepted-value law; native identity stays column based.
            await pg.exec(
              `CREATE TABLE closed_rows(id BIGINT PRIMARY KEY,tag TEXT CHECK(tag IN ('ok','é')),n BIGINT${generated ? ",g BIGINT GENERATED ALWAYS AS(n+1) STORED" : ""}${check ? ",CONSTRAINT positive CHECK(n>0)" : ""})`,
            );
            const table = core.harness.path.catalog.tableByName("closed_rows");
            if (!table) throw new Error("table");
            const tag = table.columns.find((c) => c.name === "tag");
            if (!tag) throw new Error("tag");
            const all = async (sql: string) => {
              await core.execute(sql);
              await old.execute(sql);
              await pg.exec(sql);
            };
            const values = Array.from(
              { length: n },
              (_, k) => `(${String(k + 1)},'ok',${String(k + 1)})`,
            ).join(",");
            await all(`INSERT INTO closed_rows(id,tag,n) VALUES ${values}`);
            const before = await core.execute("SELECT * FROM closed_rows ORDER BY id");
            const failures = [
              `INSERT INTO closed_rows(id,tag,n) VALUES(99,'bad',1)`,
              "UPDATE closed_rows SET tag='bad' WHERE id=1",
              "INSERT INTO closed_rows(id,tag,n) VALUES(99,'bad',1) ON CONFLICT(id) DO NOTHING",
              "INSERT INTO closed_rows(id,tag,n) VALUES(1,'bad',1) ON CONFLICT(id) DO UPDATE SET tag=EXCLUDED.tag",
              "INSERT INTO closed_rows(id,tag,n) VALUES(1,'ok',1) ON CONFLICT(id) DO UPDATE SET tag='bad'",
            ];
            for (const sql of failures) {
              await expect(core.execute(sql)).rejects.toMatchObject({
                name: "ClosedSetViolationError",
                sqlState: "23514",
                tableId: table.id,
                columnId: tag.id,
              });
              await expect(old.execute(sql)).rejects.toThrow(/must be one of/);
              await expect(pg.exec(sql)).rejects.toMatchObject({ code: "23514" });
            }
            reach.reach("INSERT");
            reach.reach("UPDATE");
            reach.reach("upsert insert");
            reach.reach("upsert proposed");
            reach.reach("upsert assignment");
            // Same closed column, distinguish first invalid physical row after a valid prefix.
            const bad = `INSERT INTO closed_rows(id,tag,n) VALUES ${Array.from({ length: n }, (_, k) => `(${String(k + 100)},'ok',1)`).join(",")},(900,'bad',1),(901,'worse',1)`;
            await expect(core.execute(bad)).rejects.toMatchObject({ columnId: tag.id, row: n });
            await expect(old.execute(bad)).rejects.toThrow(`tag[${String(n)}] must be one of`);
            await expect(pg.exec(bad)).rejects.toMatchObject({ code: "23514" });
            reach.reach("first invalid row");
            const after = await core.execute("SELECT * FROM closed_rows ORDER BY id");
            if (
              before.kind !== "rows" ||
              !("batches" in before) ||
              after.kind !== "rows" ||
              !("batches" in after)
            )
              throw new Error("rows");
            expect(oracleRows(after)).toEqual(oracleRows(before));
            reach.reach("rollback");
            reach.reach("column identity");
            // NULL passes a nullable closed set; successful generated values remain correct.
            await all("UPDATE closed_rows SET tag=NULL WHERE id=1");
            await all(
              "INSERT INTO closed_rows(id,tag,n) VALUES(1,'é',1) ON CONFLICT(id) DO UPDATE SET tag=EXCLUDED.tag,n=EXCLUDED.n",
            );
            const rows = await core.execute("SELECT * FROM closed_rows ORDER BY id"),
              shipped = await old.execute("SELECT * FROM closed_rows ORDER BY id"),
              expected = await pg.query<Record<string, unknown>>(
                "SELECT * FROM closed_rows ORDER BY id",
              );
            if (rows.kind !== "rows" || !("batches" in rows) || shipped.kind !== "rows")
              throw new Error("rows");
            expect(oracleRows(rows)).toEqual(shipped.result.rows);
            expect(oracleRows(rows)).toEqual(
              expected.rows.map((r) =>
                Object.fromEntries(
                  Object.entries(r).map(([k, v]) => [k, typeof v === "bigint" ? Number(v) : v]),
                ),
              ),
            );
            reach.reach("NULL and UTF8 allowed");
            reach.reach(check ? "CHECK" : "no CHECK");
            reach.reach(generated ? "generated" : "no generated");
            expect(core.spill.bytesInUse).toBe(0);
          } finally {
            await core.close();
            await old.close();
            await pg.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [true, false, 1],
          [false, true, 2],
          [true, true, 3],
          [false, false, 1],
        ],
      },
    );
    reach.assertReached();
  }, 120000);

for (const seed of seedsFor("next-closed-set-replay", [0xb5cf]))
  it(`keeps closed-set identity through generated replay seed ${String(seed)}`, async () => {
    const reach = createReach("closed-set replay", CLOSED_SET_REPLAY_REACH),
      pg = new PGlite();
    try {
      await pg.exec(
        "CREATE TABLE replay_closed(id INT PRIMARY KEY,tag TEXT CHECK(tag IN ('ok','é')),n INT,g INT GENERATED ALWAYS AS(100/n) STORED,CHECK(n>0))",
      );
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(1, 2, 8),
          fc.constantFrom("flat", "dictionary", "constant"),
          fc.boolean(),
          async (width, layout, selected) => {
            reach.beginHistory();
            const db = new LoweredDatabase();
            db.execute(
              "CREATE TABLE replay_closed(id INT PRIMARY KEY,tag TEXT,n INT,g INT GENERATED ALWAYS AS(100/n) STORED,CHECK(n>0))",
            );
            const original = db.catalog.view.tableByName("replay_closed");
            if (!original) throw new Error("table");
            const table = {
              ...original,
              columns: original.columns.map((c) =>
                c.name === "tag" ? { ...c, allowedValues: ["ok", "é"] } : c,
              ),
            };
            const active = Array.from({ length: width }, (_, i) => i).filter(
                (i) => !selected || width === 1 || i % 2 === 1,
              ),
              first = active[0];
            if (first === undefined) throw new Error("active row");
            const tags = Array.from({ length: width }, () => "bad"),
              divisors = Array.from({ length: width }, () => 1);
            if (active.length > 1) divisors[active[active.length - 1] ?? 0] = 0;
            let tag: Vector = texts(tags);
            if (layout === "dictionary")
              tag = {
                form: "dictionary",
                type: tag.type,
                length: width,
                codes: new Int32Array(width),
                dictionary: texts(["bad"]),
                dictionaryId: newDictionaryId(),
                validity: null,
              };
            if (layout === "constant")
              tag = { form: "constant", type: tag.type, length: width, value: texts(["bad"]) };
            const batch = {
              length: width,
              columns: [
                ints(Array.from({ length: width }, (_, i) => i + 1)),
                tag,
                ints(divisors),
                ints(Array.from({ length: width }, () => 0)),
              ],
              selection: selected ? { rows: Uint16Array.from(active), count: active.length } : null,
            };
            let held = 0;
            const bound = compileTableRules(
              table,
              db.catalog.view.version,
              db.lowerer.lowerTableRules(table, db.catalog.view),
              {
                capacity: width,
                charge: {
                  allocate: (n) => {
                    held += n;
                    return true;
                  },
                  free: (n) => {
                    held -= n;
                  },
                },
              },
            );
            try {
              const changed = table.columns.map((c) => c.id);
              expect(() => {
                bound.generate(batch, changed);
                bound.validate?.(batch, changed);
              }).toThrow(
                expect.objectContaining({
                  name: "ClosedSetViolationError",
                  sqlState: "23514",
                  columnId: table.columns[1]?.id,
                  row: first,
                }),
              );
              await expect(
                pg.exec(
                  `INSERT INTO replay_closed(id,tag,n) VALUES ${active.map((i) => `(${String(i + 1)},'bad',${String(divisors[i])})`).join(",")}`,
                ),
              ).rejects.toMatchObject({ code: "23514" });
              reach.reach(layout);
              reach.reach(selected ? "selection" : "all rows");
              reach.reach(active.length > 1 ? "before later generated error" : "single active row");
              reach.reach("physical row and column");
            } finally {
              bound.close();
            }
            expect(held).toBe(0);
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [1, "flat", false],
            [2, "dictionary", false],
            [8, "constant", true],
            [8, "flat", true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 60000);
