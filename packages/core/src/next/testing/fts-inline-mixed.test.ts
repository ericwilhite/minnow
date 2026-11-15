import { MAX_LSN } from "../access/snapshot.js";
import { decodeManifestRoot, encodeManifestRoot } from "../format/manifest-codec.js";
/** Posting claims keep their own LSN when later reference-free invalidations arrive. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_INLINE_MIXED_REACH } from "../../testing/reach.js";
import { ftsDef } from "../access/fts-def.js";
import { readFtsJournal, ftsPreparedEntries, type FtsJournalEntry } from "../format/fts-journal.js";
import type { BlobRef } from "../storage/extents.js";
import {
  Harness,
  autocommit,
  column,
  tableInput,
  TEXT,
  batch,
  texts,
  locators,
  rules,
  watchGrants,
  rowsOf,
} from "../write/test-support.js";
import { setupFts, ftsBudget, lastFtsCommit } from "./fts-transaction.test-support.js";
for (const seed of seedsFor("next-fts-inline-mixed", [0xb875, 0xb876]))
  it(`keeps mixed FTS claims and subsequent invalidations at their own LSN ${String(seed)}`, async (context) => {
    let retired = false,
      retire: (() => void) | undefined;
    context.onTestFinished(() => {
      retired = true;
      retire?.();
    });
    const checkActive = () => {
      if (retired) throw new Error("Retired mixed FTS history");
    };
    const reach = createReach("FTS mixed inline history", FTS_INLINE_MIXED_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.constantFrom<string | null>(null, "alpha beta", "gamma"), {
          minLength: 1,
          maxLength: 4,
        }),
        async (values) => {
          checkActive();
          reach.beginHistory();
          const h = new Harness();
          let coveredReplay = false,
            coveredAllocations = 0;
          const restores: Array<() => void> = [];
          watchGrants(h.governor, (grant) => {
            if (retired || grant.label !== "write path metadata") return;
            const allocate = grant.allocate.bind(grant);
            grant.allocate = (bytes) => {
              if (coveredReplay && (new Error().stack ?? "").includes("fts-mutations"))
                coveredAllocations++;
              return allocate(bytes);
            };
            restores.push(() => {
              grant.allocate = allocate;
            });
          });
          const restore = () => {
            coveredReplay = false;
            h.onSpace = null;
            for (const undo of restores.splice(0)) undo();
          };
          retire = () => {
            restore();
            void h.close().catch(() => undefined);
          };
          try {
            const path = await setupFts(h);
            let other = 0;
            await autocommit(path, async (s) => {
              other =
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("other", [column("body", TEXT)], { primaryKey: null }),
                  })
                ).id ?? 0;
            });
            const id = path.catalog.table(other)?.columns[0]?.id;
            if (id === undefined) throw new Error("Missing other column");
            const { id: _id, ...fts } = ftsDef(2, other, id);
            void _id;
            await autocommit(path, (s) => s.changeCatalog({ kind: "createFts", fts }));
            // Catalog stays unchanged after this bound. Keep WAL and a valid conservative
            // global checkpoint while the independent FTS/table checkpoints move ahead.
            const base = path.durableLsn;
            const wrap = (space: Harness["space"]) => {
              if (retired) throw new Error("Retired mixed FTS space");
              const publish = space.publish.bind(space),
                budget = ftsBudget();
              space.publish = async (bytes, change) => {
                const root = decodeManifestRoot(bytes, budget);
                try {
                  return await publish(
                    encodeManifestRoot({
                      ...root.value,
                      globalLsn: Math.min(base, root.value.globalLsn),
                    }),
                    change,
                  );
                } finally {
                  budget.free(root.charge);
                }
              };
              restores.push(() => {
                space.publish = publish;
              });
            };
            h.onSpace = wrap;
            wrap(h.space);
            await autocommit(path, (s) => s.table(1).insert(batch(texts(["old"])), null));
            await autocommit(path, (s) => s.table(other).insert(batch(texts(["seed"])), null));
            const tx = await path.begin(),
              statement = tx.statement({ rules });
            await statement.table(1).delete(batch(locators([0], 1)), null);
            await statement.table(other).insert(batch(texts(values)), null);
            await statement.end();
            const mixed = await tx.commit(null);
            await mixed.durable;
            const logged = (await lastFtsCommit(h)).body.effects;
            expect(logged.filter((e) => e.kind === "fts")).toHaveLength(1);
            expect(logged.filter((e) => e.kind === "ftsMutation")).toHaveLength(1);
            reach.reach("same-body append and keyed");
            const later = await path.begin(),
              deletion = later.statement({ rules });
            await deletion.table(other).delete(batch(locators([0], other)), null);
            await deletion.end();
            const keyed = await later.commit(null);
            await keyed.durable;
            expect(keyed.lsn).toBeGreaterThan(mixed.lsn);
            await h.reopen();
            reach.reach("uncheckpointed reopen");
            // Keep the old WAL bytes for the independent-coverage replay proof.
            for (const table of h.path.state().tables.values())
              for (const memtable of table.memtables) {
                expect(memtable.visibleRows(MAX_LSN)).toBe(memtable.rowCount);
                memtable.state = "frozen";
                memtable.measure();
              }
            await h.path.quiesce();
            const b = ftsBudget(),
              checkpoints: Array<{ lsn: number; entries: FtsJournalEntry[] }> = [];
            let ref: BlobRef | null = h.path.state().root.fts.checkpoint;
            while (ref) {
              const held = await readFtsJournal(ref, b, h.space);
              try {
                const page = held.value;
                if (page.kind !== "checkpoint") throw new Error("Missing checkpoint");
                const entries = [];
                for await (const e of ftsPreparedEntries(page.staged, b, h.space))
                  entries.push(structuredClone(e));
                checkpoints.push({ lsn: page.lsn, entries });
                ref = page.previous;
              } finally {
                held.release();
              }
            }
            const append = checkpoints.find((c) => c.lsn === mixed.lsn);
            expect(
              append?.entries
                .filter((e) => e.kind === "insert" && e.tableId === other)
                .reduce((n, e) => n + (e.kind === "insert" ? e.documents.count : 0), 0),
            ).toBe(values.length);
            expect(
              append?.entries.some(
                (e) => e.kind === "keyed" && e.tableId === 1 && e.mutationEpoch === mixed.lsn,
              ),
            ).toBe(true);
            expect(
              checkpoints
                .find((c) => c.lsn === keyed.lsn)
                ?.entries.some(
                  (e) => e.kind === "keyed" && e.tableId === other && e.mutationEpoch === keyed.lsn,
                ),
            ).toBe(true);
            expect(b.held).toBe(0);
            reach.reach("separate actual LSNs");
            if (values.includes(null)) reach.reach("NULL append");
            coveredReplay = true;
            for (let pass = 0; pass < 2; pass++) {
              checkActive();
              await h.reopen();
              checkActive();
              const view = h.path.pin();
              try {
                expect(await rowsOf(view, 1)).toEqual([]);
                expect(await rowsOf(view, other)).toEqual(values.map((v) => [v]));
              } finally {
                view.release();
              }
            }
            coveredReplay = false;
            expect(coveredAllocations, "covered inline WAL must not prepare another mutation").toBe(
              0,
            );
            reach.reach("exact rows");
            reach.reach("checkpoint replay");
            reach.reach("independent coverage");
          } finally {
            restore();
            await h.close();
            retire = undefined;
          }
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("all charges returned");
        },
      ),
      { seed, numRuns: 12, examples: [[[null]]] },
    );
    reach.assertReached();
  }, 60_000);
