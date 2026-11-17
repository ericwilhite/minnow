/** Catalog and row checkpoints are independent, even when ALTER and another build share an LSN. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, DDL_INDEX_SCHEMA_REACH } from "../../testing/reach.js";
import { oracleRows } from "./differential-adapter.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import { texts } from "../write/test-support.js";
import { UNCHARGED } from "../write/bytes.js";
import { StorageCore } from "./storage-core.js";

for (const seed of seedsFor("ddl-index-schema", [47397, 47398]))
  it(`advances an ALTER/index cohort and preserves rows seed ${String(seed)}`, async () => {
    const reach = createReach("DDL/index checkpoint cohort", DDL_INDEX_SCHEMA_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("drop", "add", "rename"),
        fc.constantFrom(0, 1, 2),
        fc.integer({ min: 1, max: 32 }),
        async (op, position, width) => {
          reach.beginHistory();
          reach.reach(op);
          reach.reach((["first", "middle", "last"] as const)[position]);
          const c = await new StorageCore(16).open();
          try {
            const names = ["a", "b", "c"];
            const expected: Record<string, string> = Object.fromEntries(
              names.map((name) => [name, name.repeat(width)]),
            );
            const name = names[position];
            if (!name) throw new Error("missing cohort column");
            await c.execute("CREATE TABLE docs(a TEXT,b TEXT,c TEXT)");
            await c.writeBatch("insertBatch", "docs", [expected]);
            await c.execute("CREATE TABLE aux(t TEXT)");
            await c.writeBatch("insertBatch", "aux", [{ t: "old" }]);
            await c.write(async (s) => {
              const sql =
                op === "drop"
                  ? `ALTER TABLE docs DROP COLUMN ${name}`
                  : op === "rename"
                    ? `ALTER TABLE docs RENAME COLUMN ${name} TO renamed`
                    : "ALTER TABLE docs ADD COLUMN extra TEXT DEFAULT 'missing'";
              for (const text of [sql, "CREATE INDEX aux_index ON aux(t)"]) {
                const lowered = c.lowerer.lower(text, { ...c.context(), catalog: s.catalog });
                if (lowered.kind !== "ddl" || !lowered.change)
                  throw new Error("missing cohort DDL");
                await s.changeCatalog(lowered.change);
              }
            });
            if (op === "drop") Reflect.deleteProperty(expected, name);
            else if (op === "rename") {
              expected.renamed = expected[name] ?? "";
              Reflect.deleteProperty(expected, name);
            } else expected.extra = "missing";
            reach.reach("shared DDL LSN");
            const before = c.harness.path.publications;
            await c.writeBatch(
              "insertBatch",
              "aux",
              Array.from({ length: 15 }, () => ({ t: "x" })),
            );
            await c.writeBatch("insertBatch", "aux", [{ t: "new" }]);
            await c.harness.path.quiesce();
            expect(c.harness.path.publications, "DDL/index checkpoint cycle").toBeGreaterThan(
              before,
            );
            reach.reach("publication advances");
            for (let cut = 0; cut < 2; cut++) {
              await c.reopen();
              const docs = await c.execute("SELECT * FROM docs");
              if (!("batches" in docs)) throw new Error("missing cohort rows");
              expect(oracleRows(docs)).toEqual([expected]);
              const aux = c.harness.path.catalog.tableByName("aux");
              const index = aux?.indexes.find((entry) => entry.name === "aux_index");
              if (!aux || !index) throw new Error("missing cohort index");
              const keys = new KeyBatch(UNCHARGED);
              const view = c.harness.path.pin();
              try {
                encodeKeyBatch(
                  "equality",
                  [{ vector: texts(["old"]), direction: "asc", nulls: "first" }],
                  { length: 1, selection: null },
                  keys,
                );
                const out = { probes: new Int32Array(2), locators: new Float64Array(2), count: 0 };
                await view.index(aux.id, index.id).probe(keys, out);
                expect(out.count).toBe(1);
              } finally {
                keys.close();
                view.release();
              }
              await c.harness.path.recycleLog();
            }
            reach.reach("exact reopen");
          } finally {
            await c.close();
          }
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: (["drop", "add", "rename"] as const).flatMap((op) =>
          ([0, 1, 2] as const).map(
            (position) => [op, position, 4] as [typeof op, typeof position, number],
          ),
        ),
      },
    );
    reach.assertReached();
  }, 120000);
