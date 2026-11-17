/** Old staged groups retain physical membership when a later catalog checkpoints ADD. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, DDL_STAGED_ALTER_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ftsDef } from "../access/fts-def.js";
import { ftsSemantics } from "../fts-config.js";
import { ftsValue, FTS_WORKSPACE_BYTES } from "../exec/fts-value.js";
import { allocateFlat, type FlatVector } from "../vector/vector.js";
import { VarlenBuilder } from "../vector/varlen.js";
import { batch, ints, texts } from "../write/test-support.js";
import { need } from "../write/bytes.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

function foldGate() {
  let release!: () => void;
  return { wait: new Promise<void>((resolve) => (release = resolve)), release: () => release() };
}

for (const timing of ["before", "after"] as const)
  for (const seed of seedsFor("ddl-staged-alter", [47403, 47404]))
    it(`reopens ADD ${timing} staging before folding seed ${String(seed)}`, async () => {
      const reach = createReach("staged physical schema", DDL_STAGED_ALTER_REACH);
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            text: fc.boolean(),
            missing: fc.boolean(),
            position: fc.integer({ min: 0, max: 2 }),
            width: fc.integer({ min: 1, max: 16 }),
            fts: fc.boolean(),
          }),
          async ({ text, missing, position, width, fts }) => {
            reach.beginHistory();
            reach.reach(text ? "varlen" : "fixed");
            reach.reach(missing ? "frozen default" : "NULL missing");
            reach.reach(need((["first", "middle", "last"] as const)[position]));
            reach.reach(fts ? "FTS checkpoint" : "plain staging checkpoint");
            let gate = foldGate();
            const core = new StorageCore(2);
            Object.assign(core.harness, {
              tuning: {
                merges: { mode: "off" },
                beforeFlushPublish: () => gate.wait,
              },
            });
            await core.open();
            const install = () =>
              core.harness.path.installFtsRenderer({
                ...ftsSemantics(),
                workspaceBytes: FTS_WORKSPACE_BYTES,
                render: ftsValue,
              });
            install();
            try {
              await core.execute("CREATE TABLE docs(a TEXT,b TEXT)");
              const names = ["a", "b"];
              const frozen = missing ? (text ? "frozen".repeat(width) : 37) : null;
              const written = text ? "written".repeat(width) : 91;
              const add = async () => {
                const literal =
                  frozen === null
                    ? ""
                    : ` DEFAULT ${text ? `'${String(frozen)}'` : String(frozen)}`;
                const lowered = core.lowerer.lower(
                  `ALTER TABLE docs ADD COLUMN extra ${text ? "TEXT" : "INT"}${literal}`,
                  core.context(),
                );
                if (lowered.kind !== "ddl" || lowered.change?.kind !== "alterTable")
                  throw new Error("missing ADD");
                const columns = [...lowered.change.table.columns];
                const added = columns.pop();
                if (!added) throw new Error("missing added column");
                columns.splice(position, 0, added);
                await core.change({
                  ...lowered.change,
                  table: { ...lowered.change.table, columns },
                });
                names.splice(position, 0, "extra");
              };
              if (timing === "before") await add();
              const expected = Array.from({ length: 16 }, (_, n) => ({
                a: `a:${String(n)}`,
                b: `b:${String(n)}`,
                extra: timing === "before" ? written : frozen,
              }));
              await core.write(async (s) => {
                const table = s.catalog.tableByName("docs");
                if (!table) throw new Error("missing docs");
                for (let from = 0; from < expected.length; from += 2) {
                  const rows = expected.slice(from, from + 2);
                  const vectors = names.map((name) =>
                    name === "extra" && !text
                      ? ints(rows.map(() => Number(written)))
                      : texts(rows.map((row) => String(row[name as keyof typeof row]))),
                  );
                  await s.table(table.id).insert(batch(...vectors), null);
                }
              });
              expect(core.harness.path.maintenance.stages).toBeGreaterThan(0);
              reach.reach("staging observed");
              if (timing === "after") await add();
              const alterLsn = core.harness.path.appliedLsn;
              await core.execute("CREATE TABLE aux(t TEXT)");
              if (fts) {
                const aux = core.harness.path.catalog.tableByName("aux");
                if (!aux?.columns[0]) throw new Error("missing aux");
                await core.change({ kind: "createFts", fts: ftsDef(1, aux.id, aux.columns[0].id) });
                await core.writeBatch("insertBatch", "aux", [{ t: "alpha" }]);
              }
              // A second table stages independently of docs' parked fold. Its publication
              // checkpoints the newer catalog, whether or not it also has FTS mutations.
              await core.write(async (s) => {
                const aux = s.catalog.tableByName("aux");
                if (!aux) throw new Error("missing aux");
                for (let k = 0; k < 8; k++)
                  await s.table(aux.id).insert(batch(texts(["alpha", "beta"])), null);
              });
              const root = core.harness.path.state().root;
              expect(root.globalLsn).toBeGreaterThanOrEqual(alterLsn);
              expect(root.tables.find((t) => t.tableId === 1)?.schemaVersion).toBe(2);
              expect(root.tables.find((t) => t.tableId === 1)?.staged.length).toBeGreaterThan(0);
              reach.reach("new catalog with unfolded claim");
              // Close cancels the old flush before its publication; release every test gate.
              const closing = core.harness.path.close();
              gate.release();
              await closing;
              gate = foldGate();
              await core.reopen();
              install();
              expect(core.harness.path.maintenance.publishedReplays).toBeGreaterThan(0);
              const result = await core.execute(`SELECT ${names.join(",")} FROM docs`);
              if (!("batches" in result)) throw new Error("missing rows");
              expect(oracleRows(result)).toEqual(expected);
              reach.reach("exact pre-fold reopen");
              if (timing === "after") {
                const pin = core.harness.path.pin();
                try {
                  const def = pin.catalog.tableByName("docs");
                  const extra = def?.columns.find((c) => c.name === "extra");
                  if (!def || !extra) throw new Error("missing extra");
                  const table = pin.table(def.id, {
                    budget: { allocate: () => true, free: () => undefined },
                  });
                  for (const segment of table.segments) {
                    if (segment.kind !== "memory") continue;
                    const out = {
                      flat: allocateFlat(extra.type, 2),
                      codes: new Int32Array(2),
                      varlen: new VarlenBuilder(2, { allocate: () => true, free: () => undefined }),
                    };
                    const legacy = await segment.chunk(extra.id, 0, out);
                    expect(legacy.vector.form).toBe("constant");
                    if (!segment.portableChunk) throw new Error("missing portable memory reader");
                    const portable = await segment.portableChunk(extra.id, 0);
                    expect(portable.kind).toBe("resident");
                    if (legacy.vector.form !== "constant" || portable.kind !== "resident")
                      throw new Error("missing constant backfill");
                    const scalar: FlatVector = legacy.vector.value;
                    if (missing) expect(scalar).toEqual(extra.missing);
                    else expect(scalar.validity?.[0]).toBe(0);
                  }
                } finally {
                  pin.release();
                }
              }
              reach.reach("native reads");
              gate.release();
              await core.harness.path.quiesce();
              await core.reopen();
              const folded = await core.execute(`SELECT ${names.join(",")} FROM docs`);
              if (!("batches" in folded)) throw new Error("missing folded rows");
              expect(oracleRows(folded)).toEqual(expected);
              reach.reach("exact post-fold reopen");
            } finally {
              gate.release();
              await core.close();
              expect(core.harness.governor.usedBytes).toBe(0);
            }
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [true, false].flatMap((text) =>
            [true, false].flatMap((missing) =>
              [0, 1, 2].map((position) => [{ text, missing, position, width: 2, fts: text }]),
            ),
          ),
        },
      );
      reach.assertReached();
    }, 120000);
