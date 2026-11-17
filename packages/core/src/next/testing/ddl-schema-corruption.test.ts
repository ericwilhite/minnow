/** A mismatched development checkpoint must refuse before reconstructing row state. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, DDL_SCHEMA_CORRUPTION_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { decodeManifestRoot, encodeManifestRoot } from "../format/manifest-codec.js";
import type { ManifestRoot } from "../format/manifest.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { need, UNCHARGED } from "../write/bytes.js";

for (const seed of seedsFor("ddl-schema-corruption", [47393, 47394]))
  it(`refuses a mismatched replay version and permits repaired retry seed ${String(seed)}`, async () => {
    const reach = createReach("DDL checkpoint corruption", DDL_SCHEMA_CORRUPTION_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 8 }), fc.boolean(), async (offset, past) => {
        reach.beginHistory();
        reach.reach(past ? "past schema" : "future schema");
        const core = await new StorageCore(16).open();
        const h = core.harness;
        const publish = async (root: ManifestRoot) => {
          const lease = await h.backend.acquire();
          try {
            const session = await h.backend.open(lease);
            try {
              await session.publish(encodeManifestRoot({ ...root, epoch: lease.epoch }));
            } finally {
              await session.close();
            }
          } finally {
            await lease.release();
          }
        };
        try {
          await core.execute("CREATE TABLE docs(a TEXT,b TEXT)");
          await core.writeBatch("insertBatch", "docs", [{ a: "a", b: "correct" }]);
          await core.execute("ALTER TABLE docs DROP COLUMN a");
          await h.path.recycleLog();
          const root = decodeManifestRoot(need(h.space.root), UNCHARGED).value;
          const table = need(root.tables[0]);
          await core.close();
          const version = past ? 1 : table.schemaVersion + offset;
          await publish({
            ...root,
            sequence: root.sequence + 1,
            tables: [{ ...table, schemaVersion: version }],
          });
          await expect(h.open()).rejects.toMatchObject({
            code: "MINNOW_MANIFEST_CORRUPT",
            message: "Table replay schema disagrees with checkpoint catalog",
          });
          reach.reach("typed refusal");
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("charge returned");
          await publish({ ...root, sequence: root.sequence + 2 });
          await h.open();
          const rows = await core.execute("SELECT b FROM docs");
          if (!("batches" in rows)) throw new Error("missing repaired rows");
          expect(oracleRows(rows)).toEqual([{ b: "correct" }]);
          reach.reach("repair retry");
        } finally {
          await core.close();
        }
      }),
      {
        seed,
        numRuns: 8,
        examples: [
          [1, true],
          [1, false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
