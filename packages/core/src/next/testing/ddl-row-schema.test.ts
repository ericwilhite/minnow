/** ALTER preserves stable column identity across independently checkpointed catalog and rows.
 * Authority: access/catalog.ts ColumnDef IDs and format/manifest.ts checkpoint contract. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, DDL_ROW_SCHEMA_REACH } from "../../testing/reach.js";
import { ftsDef } from "../access/fts-def.js";
import { ftsSemantics } from "../fts-config.js";
import { ftsValue, FTS_WORKSPACE_BYTES } from "../exec/fts-value.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

for (const op of ["drop", "add", "rename"] as const)
  for (const seed of seedsFor("ddl-row-schema", [47391, 47392]))
    it(`preserves columns across ${op}, publication and replay seed ${String(seed)}`, async () => {
      const positions = op === "add" ? ([0, 1, 2, 3, 4] as const) : ([0, 1, 2, 3] as const);
      const reach = createReach(
        `DDL ${op} schema cuts`,
        op === "add"
          ? [...DDL_ROW_SCHEMA_REACH, "position 4", "NULL missing", "default missing"]
          : DDL_ROW_SCHEMA_REACH,
      );
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            position: fc.constantFrom(...positions),
            width: fc.integer({ min: 1, max: 32 }),
            fts: fc.boolean(),
            stored: fc.boolean(),
            missing: fc.boolean(),
            rootFirst: fc.boolean(),
          }),
          async ({ position, width, fts, stored, missing, rootFirst }) => {
            reach.beginHistory();
            reach.reach(
              (["position 0", "position 1", "position 2", "position 3", "position 4"] as const)[
                position
              ],
            );
            reach.reach(fts ? "FTS publication" : "plain publication");
            reach.reach(stored ? "stored rows" : "resident rows");
            reach.reach(rootFirst ? "empty root before rows" : "no initial empty root");
            const core = new StorageCore(16);
            Object.assign(core.harness, { tuning: { merges: { mode: "manual", fanIn: 2 } } });
            await core.open();
            const install = () =>
              core.harness.path.installFtsRenderer({
                ...ftsSemantics(),
                workspaceBytes: FTS_WORKSPACE_BYTES,
                render: ftsValue,
              });
            install();
            try {
              const names = ["a", "b", "c", "d"];
              const rows: Array<Record<string, string | null>> = [0, 1].map((r) =>
                Object.fromEntries(names.map((n) => [n, `${n}:${String(r)}:` + "x".repeat(width)])),
              );
              await core.execute("CREATE TABLE docs(a TEXT,b TEXT,c TEXT,d TEXT)");
              if (rootFirst) await core.harness.path.recycleLog();
              await core.writeBatch("insertBatch", "docs", rows);
              if (stored) await core.harness.path.recycleLog();
              const name = names[position] ?? "extra";
              if (op === "drop") {
                await core.execute(`ALTER TABLE docs DROP COLUMN ${name}`);
                names.splice(position, 1);
                for (const row of rows) Reflect.deleteProperty(row, name);
              } else if (op === "rename") {
                await core.execute(`ALTER TABLE docs RENAME COLUMN ${name} TO renamed`);
                names[position] = "renamed";
                for (const row of rows) {
                  row.renamed = row[name] ?? "";
                  Reflect.deleteProperty(row, name);
                }
              } else {
                // SQL appends; the native catalog also supports insertion at any position.
                const lowered = core.lowerer.lower(
                  `ALTER TABLE docs ADD COLUMN extra TEXT${missing ? " DEFAULT 'missing'" : ""}`,
                  core.context(),
                );
                if (lowered.kind !== "ddl" || lowered.change?.kind !== "alterTable")
                  throw new Error("missing ALTER");
                const columns = [...lowered.change.table.columns];
                const added = columns.pop();
                if (!added) throw new Error("missing added column");
                columns.splice(position, 0, added);
                await core.change({
                  ...lowered.change,
                  table: { ...lowered.change.table, columns },
                });
                names.splice(position, 0, "extra");
                for (const row of rows) row.extra = missing ? "missing" : null;
                reach.reach(missing ? "default missing" : "NULL missing");
              }
              const check = async () => {
                const result = await core.execute(`SELECT ${names.join(",")} FROM docs`);
                if (!("batches" in result)) throw new Error("missing rows");
                expect(oracleRows(result)).toEqual(rows);
              };
              const reopen = async () => {
                await core.reopen();
                install();
                await check();
              };
              await check();
              // Publication by an unrelated table must not erase docs' historical WAL schema.
              await core.execute("CREATE TABLE aux(t TEXT)");
              if (fts) {
                const aux = core.harness.path.catalog.tableByName("aux");
                const col = aux?.columns[0];
                if (!aux || !col) throw new Error("missing aux");
                const def = ftsDef(1, aux.id, col.id);
                await core.change({ kind: "createFts", fts: def });
                await core.writeBatch("insertBatch", "aux", [{ t: "alpha" }]);
              } else {
                const before = core.harness.path.publications;
                await core.writeBatch(
                  "insertBatch",
                  "aux",
                  Array.from({ length: 16 }, () => ({ t: "alpha" })),
                );
                await core.writeBatch("insertBatch", "aux", [{ t: "force freeze" }]);
                await core.harness.path.quiesce();
                expect(core.harness.path.publications).toBeGreaterThan(before);
              }
              await check();
              await reopen();
              reach.reach("reopen after publication");
              const appended = Object.fromEntries(names.map((n) => [n, `${n}:new`]));
              await core.writeBatch("insertBatch", "docs", [appended]);
              rows.push(appended);
              await check();
              await reopen();
              reach.reach("new schema write");
              const tail = names[names.length - 1];
              if (!tail) throw new Error("missing final column");
              await core.execute(`ALTER TABLE docs RENAME COLUMN ${tail} TO tail`);
              names[names.length - 1] = "tail";
              for (const row of rows) {
                row.tail = row[tail] ?? null;
                Reflect.deleteProperty(row, tail);
              }
              await core.writeBatch("insertBatch", "aux", [{ t: "second schema cut" }]);
              await check();
              await reopen();
              reach.reach("second ALTER");
              await core.execute("CREATE INDEX tail_index ON docs(tail)");
              let merges = 0;
              for (let step = 0; step < 2; step++) {
                const row = Object.fromEntries(
                  names.map((n) => [n, `native:${String(step)}:${n}`]),
                );
                await core.writeBatch("insertBatch", "docs", [row]);
                rows.push(row);
                await check();
                await core.harness.path.recycleLog();
                await core.harness.path.runMaintenance();
                merges += core.harness.path.maintenance.merges;
                await reopen();
              }
              expect(merges).toBeGreaterThan(0);
              reach.reach("checkpoint and maintenance");
            } finally {
              await core.close();
            }
          },
        ),
        {
          seed,
          numRuns: 64,
          examples: [
            [
              {
                position: 1 as const,
                width: 4,
                fts: true,
                stored: false,
                missing: true,
                rootFirst: false,
              },
            ],
            ...positions.flatMap((position) =>
              [false, true].flatMap((fts) =>
                [false, true].flatMap((stored) =>
                  [false, true].map((missing) => [
                    { position, width: 4, fts, stored, missing, rootFirst: !stored },
                  ]),
                ),
              ),
            ),
          ],
        },
      );
      reach.assertReached();
    }, 120000);
