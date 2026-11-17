/** Every publication route carries the checkpoint catalog's replay version, not its old root's. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, DDL_PUBLICATION_SCHEMA_REACH } from "../../testing/reach.js";
import { batch, texts } from "../write/test-support.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

for (const route of ["merge", "stage"] as const)
  for (const seed of seedsFor("ddl-publication-schema", [47401, 47402]))
    it(`keeps replay versions on ${route} after ALTER seed ${String(seed)}`, async () => {
      const reach = createReach("replaced replay roots", DDL_PUBLICATION_SCHEMA_REACH);
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("drop", "add", "rename"),
          fc.constantFrom(0, 1, 2),
          fc.integer({ min: 1, max: 8 }),
          async (op, position, width) => {
            reach.beginHistory();
            reach.reach(op);
            reach.reach((["first", "middle", "last"] as const)[position]);
            const c = new StorageCore(2);
            Object.assign(c.harness, { tuning: { merges: { mode: "manual", fanIn: 2 } } });
            await c.open();
            try {
              const names = ["a", "b", "c"];
              const target = names[position];
              const indexed = names[(position + 1) % names.length];
              if (!target || !indexed) throw new Error("missing publication column");
              const expected: Array<Record<string, string>> = [0, 1].map((n) =>
                Object.fromEntries(
                  names.map((name) => [name, `${name}:${String(n)}:` + "x".repeat(width)]),
                ),
              );
              await c.execute("CREATE TABLE docs(a TEXT,b TEXT,c TEXT)");
              await c.execute(`CREATE INDEX kept ON docs(${indexed})`);
              for (const row of expected) {
                await c.writeBatch("insertBatch", "docs", [row]);
                await c.harness.path.recycleLog();
              }
              if (op === "drop") {
                await c.execute(`ALTER TABLE docs DROP COLUMN ${target}`);
                names.splice(position, 1);
                for (const row of expected) Reflect.deleteProperty(row, target);
              } else if (op === "rename") {
                await c.execute(`ALTER TABLE docs RENAME COLUMN ${target} TO renamed`);
                names[position] = "renamed";
                for (const row of expected) {
                  row.renamed = row[target] ?? "";
                  Reflect.deleteProperty(row, target);
                }
              } else {
                await c.execute("ALTER TABLE docs ADD COLUMN extra TEXT DEFAULT 'missing'");
                names.push("extra");
                for (const row of expected) row.extra = "missing";
              }
              const version = c.harness.path.catalog.tableByName("docs")?.schemaVersion;
              const aligned = () => {
                const p = c.harness.path;
                expect(p.state().root.tables[0]?.schemaVersion, "published replay version").toBe(
                  version,
                );
              };
              if (route === "merge") {
                const before = c.harness.path.maintenance.merges;
                await c.harness.path.runMaintenance();
                expect(c.harness.path.maintenance.merges).toBeGreaterThan(before);
                aligned();
              } else {
                const before = c.harness.path.maintenance.stages;
                await c.write(async (s) => {
                  const id = s.catalog.tableByName("docs")?.id;
                  if (id === undefined) throw new Error("missing staging table");
                  for (let step = 0; step < 8; step++) {
                    const rows = [0, 1].map((k) =>
                      Object.fromEntries(
                        names.map((name) => [name, `${name}:${String(step)}:${String(k)}`]),
                      ),
                    );
                    await s
                      .table(id)
                      .insert(
                        batch(...names.map((name) => texts(rows.map((row) => row[name] ?? "")))),
                        null,
                      );
                    expected.push(...rows);
                  }
                  expect(c.harness.path.maintenance.stages).toBeGreaterThan(before);
                  aligned(); // before commit/fold can conceal a stale staging root
                });
              }
              reach.reach("route observed");
              reach.reach("version aligned");
              for (let cut = 0; cut < 2; cut++) {
                await c.reopen();
                const result = await c.execute(`SELECT ${names.join(",")} FROM docs`);
                if (!("batches" in result)) throw new Error("missing publication rows");
                expect(oracleRows(result)).toEqual(expected);
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
          numRuns: 18,
          examples: (["drop", "add", "rename"] as const).flatMap((op) =>
            ([0, 1, 2] as const).map(
              (position) => [op, position, 4] as [typeof op, typeof position, number],
            ),
          ),
        },
      );
      reach.assertReached();
    }, 120000);
