/** A first catalog entry can contain CREATE, rows, and ALTER in one atomic LSN. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, DDL_ATOMIC_SCHEMA_REACH } from "../../testing/reach.js";
import { ftsDef } from "../access/fts-def.js";
import { ftsSemantics } from "../fts-config.js";
import { ftsValue, FTS_WORKSPACE_BYTES } from "../exec/fts-value.js";
import { batch, texts } from "../write/test-support.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

for (const seed of seedsFor("ddl-atomic-schema", [47395, 47396]))
  it(`reopens an atomic CREATE/rows/ALTER after publication seed ${String(seed)}`, async () => {
    const reach = createReach("atomic schema cut", DDL_ATOMIC_SCHEMA_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("drop", "add", "rename", "replace"),
        fc.constantFrom(0, 1, 2),
        fc.boolean(),
        async (op, position, fts) => {
          reach.beginHistory();
          reach.reach(op);
          reach.reach(fts ? "FTS" : "plain");
          const c = await new StorageCore(16).open();
          c.harness.path.installFtsRenderer({
            ...ftsSemantics(),
            workspaceBytes: FTS_WORKSPACE_BYTES,
            render: ftsValue,
          });
          const names = ["a", "b", "c"];
          const expected: Record<string, string> = Object.fromEntries(names.map((n) => [n, n]));
          const name = names[position];
          if (!name) throw new Error("missing atomic column");
          try {
            await c.write(async (s) => {
              const ddl = async (sql: string) => {
                const p = c.lowerer.lower(sql, { ...c.context(), catalog: s.catalog });
                if (p.kind !== "ddl" || !p.change) throw new Error("missing atomic DDL");
                await s.changeCatalog(p.change);
              };
              await ddl("CREATE TABLE docs(a TEXT,b TEXT,c TEXT)");
              const id = s.catalog.tableByName("docs")?.id;
              if (id === undefined) throw new Error("missing table");
              await s.table(id).insert(batch(...names.map((n) => texts([n]))), null);
              if (op === "drop") {
                await ddl(`ALTER TABLE docs DROP COLUMN ${name}`);
                Reflect.deleteProperty(expected, name);
              } else if (op === "rename") {
                await ddl(`ALTER TABLE docs RENAME COLUMN ${name} TO renamed`);
                expected.renamed = name;
                Reflect.deleteProperty(expected, name);
              } else {
                await ddl("ALTER TABLE docs ADD COLUMN extra TEXT DEFAULT 'missing'");
                expected.extra = "missing";
                if (op === "replace")
                  for (const old of names) {
                    await ddl(`ALTER TABLE docs DROP COLUMN ${old}`);
                    Reflect.deleteProperty(expected, old);
                  }
              }
            });
            await c.execute("CREATE TABLE aux(t TEXT)");
            if (fts) {
              const aux = c.harness.path.catalog.tableByName("aux"),
                col = aux?.columns[0];
              if (!aux || !col) throw new Error("missing aux");
              await c.change({ kind: "createFts", fts: ftsDef(1, aux.id, col.id) });
              await c.writeBatch("insertBatch", "aux", [{ t: "alpha" }]);
            } else {
              const before = c.harness.path.publications;
              await c.writeBatch(
                "insertBatch",
                "aux",
                Array.from({ length: 16 }, () => ({ t: "alpha" })),
              );
              await c.writeBatch("insertBatch", "aux", [{ t: "freeze" }]);
              await c.harness.path.quiesce();
              expect(c.harness.path.publications).toBeGreaterThan(before);
            }
            for (let cut = 0; cut < 2; cut++) {
              await c.reopen();
              const result = await c.execute("SELECT * FROM docs");
              if (!("batches" in result)) throw new Error("missing rows");
              expect(oracleRows(result)).toEqual([expected]);
              await c.harness.path.recycleLog();
            }
            reach.reach("atomic reopened");
          } finally {
            await c.close();
          }
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: (["drop", "add", "rename", "replace"] as const).flatMap((op) =>
          [false, true].map((fts) => [op, 1, fts] as [typeof op, 0 | 1 | 2, boolean]),
        ),
      },
    );
    reach.assertReached();
  }, 120000);
