/** Catalog path limits are checked after replay, without replacing structural run checks. */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_RUN_LIMIT_STATES } from "../../testing/reach.js";
import { decodeManifestRoot, encodeManifestRoot } from "../format/manifest-codec.js";
import type { ManifestRoot } from "../format/manifest.js";
import { indexMaximumKeyBytes } from "./index-limits.js";
import { need, UNCHARGED } from "./bytes.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  TEXT,
  texts,
  tableInput,
  rowsOf,
} from "./test-support.js";
const reach = createReach("catalog run capacity", CATALOG_RUN_LIMIT_STATES);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-catalog-run-capacity", [0xb51b]))
  it(`refuses structurally possible capacities beyond the recovered path (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(4096, 16384), fc.boolean(), async (units, corrupt) => {
        reach.beginHistory();
        const h = new Harness("strict", "memory", 8, undefined, { merges: { mode: "off" } });
        await h.open();
        try {
          let id = 0;
          await autocommit(h.path, async (s) => {
            id = need(
              (
                await s.changeCatalog({
                  kind: "createTable",
                  table: tableInput("t", [column("k", TEXT, false)]),
                })
              ).id,
            );
          });
          await autocommit(h.path, (s) =>
            s
              .table(id)
              .insert(
                batch(
                  texts(
                    Array.from(
                      { length: 8 },
                      (_, i) => "\u0800".repeat(units - 1) + String.fromCharCode(0x0810 + i),
                    ),
                  ),
                ),
                null,
              ),
          );
          await autocommit(h.path, (s) => s.table(id).insert(batch(texts(["sentinel"])), null));
          await h.path.quiesce();
          const root = decodeManifestRoot(need(h.space.root), UNCHARGED).value;
          const table = need(h.path.catalog.table(id));
          const primary = need(table.indexes.find((i) => i.id === table.primaryKey));
          const maximum = indexMaximumKeyBytes(primary, table);
          const stored = need(root.tables.find((t) => t.tableId === id));
          const run = need(stored.keyIndex?.runs.find((r) => r.encoding === 2));
          expect(run.entries.offset).toBeGreaterThan(maximum);
          const changed = {
            ...root,
            tables: root.tables.map((t) =>
              t !== stored
                ? t
                : {
                    ...t,
                    keyIndex: {
                      ...need(t.keyIndex),
                      runs: need(t.keyIndex).runs.map((r) =>
                        r !== run
                          ? r
                          : {
                              ...r,
                              keyStorage: {
                                ...need(r.keyStorage),
                                maxKeyBytes: corrupt ? maximum + 1 : need(r.keyStorage).maxKeyBytes,
                              },
                            },
                      ),
                    },
                  },
            ),
          };
          await h.close();
          async function publish(root: ManifestRoot) {
            const lease = await h.backend.acquire();
            try {
              const session = await h.backend.open(lease);
              try {
                // Test-only checksummed native root rewrite; payloads and extent lists stay intact.
                await session.publish(encodeManifestRoot({ ...root, epoch: lease.epoch }));
              } finally {
                await session.close();
              }
            } finally {
              await lease.release();
            }
          }
          await publish({ ...changed, sequence: root.sequence + 1 });
          if (corrupt) {
            await expect(h.open()).rejects.toMatchObject({
              code: "MINNOW_MANIFEST_CORRUPT",
              message: "Run key capacity exceeds its catalog path",
            });
            expect(h.governor.usedBytes).toBe(0);
            reach.reach("catalog capacity refusal");
            await publish({ ...root, sequence: root.sequence + 2 });
            await h.open();
            const view = h.path.pin();
            try {
              expect(await rowsOf(view, id)).toHaveLength(9);
            } finally {
              view.release();
            }
            reach.reach("refused open retries");
          } else {
            await h.open();
            const view = h.path.pin();
            try {
              expect(await rowsOf(view, id)).toHaveLength(9);
            } finally {
              view.release();
            }
            reach.reach("intact capacity continues");
          }
        } finally {
          await h.close();
        }
        expect(h.governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 8 },
    );
  }, 180_000);
