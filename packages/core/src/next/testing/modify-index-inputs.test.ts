/** Planner-selected index predicates have scan-equivalent DML and statement snapshots. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_INDEX_REACH } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import type { Snapshot } from "../access/snapshot.js";
import type { Rel } from "../ir/ir.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
const operators = (rel: Rel): string[] => [
  rel.op,
  ...("input" in rel ? operators(rel.input) : []),
  ...("left" in rel ? operators(rel.left) : []),
  ...("right" in rel ? operators(rel.right) : []),
];
it.each(seedsFor("next-modify-index-inputs", [0xb5bf, 0xb5c0]))(
  "executes selected native lookups/ranges (seed %i)",
  async (seed) => {
    const core = await new StorageCore(8).open(),
      pg = new PGlite(),
      shipped = await currentEngine("memory");
    await pg.waitReady;
    const original = core.context.bind(core);
    core.context = () => ({
      ...original(),
      statistics: { table: () => ({ rowCount: 1000000, column: () => null }) },
    });
    const reach = createReach("index modification inputs", MODIFY_INDEX_REACH);
    const bag = (rows: readonly unknown[] | undefined) =>
      [...(rows ?? [])].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    const all = async (
      sql: string,
      params: ReadonlyArray<number | string | null> = [],
      route?: "keyLookup" | "indexRange",
    ): Promise<void> => {
      const lowered = core.lowerer.lower(sql, core.context());
      if (route) {
        await core.harness.path.quiesce();
        if (lowered.kind !== "modify") throw new Error("modify");
        expect(operators(lowered.plan.root)).toContain(route);
        reach.reach(route);
        const table = core.harness.path.catalog.tableByName("ixmod");
        if (!table) throw new Error("index table");
        const snapshot: Snapshot = core.harness.path.pin();
        try {
          const source = await snapshot.table(table.id, {
            budget: { allocate: () => false, free: () => undefined },
          });
          for (const segment of source.segments) {
            if (segment.kind === "group") reach.reach("stored segments");
            else if (segment.rowCount > 0 && segment.portableChunk) {
              const chunk = await segment.portableChunk(table.columns[0]?.id ?? -1, 0);
              reach.reach(chunk.kind === "stored" ? "stored segments" : "memory segments");
            }
          }
        } finally {
          snapshot.release();
        }
      }
      const actual = await core.execute(sql, params),
        expected = await pg.query(sql, [...params]),
        old = await shipped.run({ kind: "sql", sql, params });
      if ("returning" in actual) {
        expect(bag(oracleRows(actual.returning))).toEqual(bag(expected.rows));
        expect(bag(oracleRows(actual.returning))).toEqual(bag(old.rows));
      }
      if (actual.kind === "rows" && "batches" in actual) {
        expect(oracleRows(actual)).toEqual(expected.rows);
        expect(oracleRows(actual)).toEqual(old.rows);
      }
      expect(core.spill.bytesInUse).toBe(0);
      if (route) await all("SELECT * FROM ixmod ORDER BY id");
    };
    try {
      await all("CREATE TABLE ixmod(id INTEGER PRIMARY KEY,tag INTEGER,v INTEGER,note TEXT)");
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 8, max: 22 }),
          fc.integer({ min: 1, max: 9 }),
          fc.boolean(),
          async (n, delta, inclusive) => {
            reach.beginHistory();
            await all("DROP TABLE ixmod");
            await all("CREATE TABLE ixmod(id INTEGER PRIMARY KEY,tag INTEGER,v INTEGER,note TEXT)");
            if (!inclusive) await all("CREATE INDEX ixmod_tag ON ixmod(tag)");
            await all(
              `INSERT INTO ixmod VALUES ${Array.from({ length: n }, (_, i) => `(${String(i + 1)},${String(i % 4)},${String(i * 10)},'row-${String(i)}')`).join(",")}`,
            );
            if (inclusive) await all("CREATE INDEX ixmod_tag ON ixmod(tag)");
            await core.harness.path.quiesce();
            await all(
              "UPDATE ixmod SET v=v+$1 WHERE id IN($2,$3,$4) RETURNING id,v,note",
              [delta, 1, 2, 1],
              "keyLookup",
            );
            reach.reach("duplicate keys");
            await all(
              `UPDATE ixmod SET v=v+${String(delta)} WHERE tag >= 1 AND tag ${inclusive ? "<=" : "<"} 3 RETURNING id,v,note`,
              [],
              "indexRange",
            );
            reach.reach(inclusive ? "inclusive bound" : "exclusive bound");
            reach.reach("nonunique range");
            await all(
              "UPDATE ixmod SET v=v+$1 WHERE tag>= $2 AND tag< $3 RETURNING id,v",
              [delta, 1, 3],
              "indexRange",
            );
            reach.reach("parameter bounds");
            await all(
              "UPDATE ixmod SET id=id+100 WHERE id IN(1,2,1) RETURNING id,v",
              [],
              "keyLookup",
            );
            reach.reach("rekey snapshot");
            await all("DELETE FROM ixmod WHERE id IN(3,4,3) RETURNING id,v,note", [], "keyLookup");
            reach.reach("key delete");
            await all(
              "DELETE FROM ixmod WHERE tag>=1 AND tag<3 RETURNING id,v,note",
              [],
              "indexRange",
            );
            reach.reach("range delete");
            await all("UPDATE ixmod SET v=v+1 WHERE id=999 RETURNING id,v", [], "keyLookup");
            await all("UPDATE ixmod SET v=v+1 WHERE id=$1 RETURNING id,v", [null], "keyLookup");
            reach.reach("null key");
            reach.reach("absent target");
            await all("SELECT * FROM ixmod ORDER BY id");
            await all(
              "CREATE TABLE typed_ix(code TEXT PRIMARY KEY,a INTEGER,b INTEGER,v INTEGER,UNIQUE(a,b))",
            );
            await all(
              `INSERT INTO typed_ix VALUES ${Array.from({ length: n }, (_, i) => `('k${String(i + 1)}',1,${String(i + 1)},${String(i * 10)})`).join(",")}`,
            );
            await all(
              "UPDATE typed_ix SET v=v+$1 WHERE code=$2 RETURNING code,v",
              [delta, `k${String(1 + (delta % n))}`],
              "keyLookup",
            );
            reach.reach("text key");
            await all(
              "UPDATE typed_ix SET v=v+$1 WHERE a=1 AND b=$2 RETURNING code,v",
              [delta, 1 + (delta % n)],
              "keyLookup",
            );
            reach.reach("composite key");
            await all("SELECT * FROM typed_ix ORDER BY code");
            await all("DROP TABLE typed_ix");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [12, 2, false],
            [16, 3, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
      await shipped.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  },
  60000,
);
