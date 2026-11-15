import { Governor } from "../memory/governor.js";
import { MAX_LSN } from "../access/snapshot.js";
import { decodeManifestRoot } from "../format/manifest-codec.js";
/** WAL publication must finish while an unrelated root publication is held (native flush law). */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, FTS_INLINE_PUBLICATION_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { Grant } from "../memory/governor.js";
import {
  Harness,
  autocommit,
  batch,
  texts,
  locators,
  rules,
  rowsOf,
} from "../write/test-support.js";
import { yieldTask } from "../task.js";
import { setupFts, ftsParts, lastFtsCommit } from "./fts-transaction.test-support.js";
for (const seed of seedsFor("next-fts-inline-publication", [0xb873, 0xb874]))
  it(`publishes native keyed effects under a held root and competing grant ${String(seed)}`, async (context) => {
    const reach = createReach("FTS inline publication", FTS_INLINE_PUBLICATION_REACH);
    let retired = false,
      retire: (() => void) | undefined;
    context.onTestFinished(() => {
      retired = true;
      retire?.();
    });
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.constantFrom(32, 128), async (definition, capMiB) => {
        reach.beginHistory();
        for (const kind of ["update", "delete", "upsert"] as const) {
          if (retired) throw new Error("Retired inline FTS history");
          const h = new Harness();
          Object.assign(h, { governor: new Governor(capMiB * 1024 * 1024) });
          let competitor: Grant | undefined;
          let open!: () => void, entered!: () => void;
          const gate = new Promise<void>((resolve) => {
            open = resolve;
          });
          const atPublish = new Promise<void>((resolve) => {
            entered = resolve;
          });
          let pending: Promise<unknown> | undefined, flush: Promise<unknown> | undefined;
          let restore: (() => void) | undefined;
          const release = () => {
            open();
            restore?.();
            restore = undefined;
            if (competitor) {
              competitor.free(competitor.allocated);
              competitor.close();
              competitor = undefined;
            }
          };
          retire = () => {
            release();
            void h.close().catch(() => undefined);
          };
          try {
            const path = await setupFts(h);
            if (!definition)
              await autocommit(path, (s) => s.changeCatalog({ kind: "dropFts", ftsId: 1 }));
            const publish = h.space.publish.bind(h.space);
            let publishes = 0,
              held = false;
            h.space.publish = async (...args) => {
              const root = decodeManifestRoot(args[0], {
                allocate: () => true,
                free: () => undefined,
              }).value;
              if (held) publishes++;
              else if (root.tables.some((table) => table.groupCount > 0)) {
                held = true;
                publishes = 1;
                entered();
                await gate;
              }
              return publish(...args);
            };
            restore = () => {
              h.space.publish = publish;
            };
            await autocommit(path, (s) => s.table(1).insert(batch(texts(["alpha", "beta"])), null));
            const old = path.pin();
            // Same representation-only transition as native pressure relief: only committed
            // rows freeze. flush() alone flushes WAL; quiesce() then publishes these groups.
            for (const memtable of path.state().tables.get(1)?.memtables ?? []) {
              expect(memtable.visibleRows(MAX_LSN)).toBe(memtable.rowCount);
              memtable.state = "frozen";
              memtable.measure();
            }
            try {
              flush = path.quiesce();
              void flush.catch(() => undefined);
              await atPublish;
              const tx = await path.begin(),
                statement = tx.statement({ rules });
              try {
                if (kind === "delete")
                  await statement.table(1).delete(batch(locators([0], 1)), null);
                else if (kind === "update")
                  await statement
                    .table(1, { assigned: [1] })
                    .update(batch(locators([0], 1), texts(["gamma"])), null);
                else {
                  await statement.table(1).insert(batch(texts(["omega"])), null);
                  statement.noteFtsMutation(1, "upsert");
                }
                await statement.end();
                competitor = await h.governor.admit({
                  label: "inline FTS competitor",
                  minimumBytes: 0,
                  spillable: true,
                });
                while (competitor.tryGrow(64 * 1024)) {
                  /* all remaining ordinary credit */
                }
                expect(competitor.allocate(competitor.held)).toBe(true);
                const settled = { value: false };
                const run = tx.commit(null);
                pending = run;
                void run.then(
                  () => {
                    settled.value = true;
                  },
                  () => {
                    settled.value = true;
                  },
                );
                for (let turn = 0; turn < 32 && !settled.value; turn++) await yieldTask();
                expect(settled.value, "keyed WAL must not wait behind the held root").toBe(true);
                const receipt = await run;
                await receipt.durable;
                expect(publishes).toBe(1);
                expect(
                  (await lastFtsCommit(h)).body.effects.filter((e) => e.kind === "ftsMutation"),
                ).toHaveLength(1);
                expect(await rowsOf(old, 1)).toEqual([["alpha"], ["beta"]]);
                reach.reach("older snapshot");
                reach.reach("no second root publish");
                reach.reach("competing grant");
                reach.reach(capMiB === 32 ? "32MiB cap" : "128MiB cap");
                reach.reach(kind);
                reach.reach(definition ? "definition" : "no definition");
                release();
                await flush;
                await path.recycleLog();
                await path.quiesce();
                expect(path.state().root.fts.checkpoint).not.toBeNull();
                const keyed = (await ftsParts(h)).entries.find((e) => e.kind === "keyed");
                expect(keyed?.mutationEpoch).toBe(receipt.lsn);
                reach.reach("actual-LSN checkpoint");
                await path.recycleLog();
                old.release();
                await h.reopen();
                const view = h.path.pin();
                try {
                  const actual = (await rowsOf(view, 1)).map((row) => String(row[0])).sort();
                  expect(actual).toEqual(
                    kind === "delete"
                      ? ["beta"]
                      : kind === "update"
                        ? ["beta", "gamma"]
                        : ["alpha", "beta", "omega"],
                  );
                } finally {
                  view.release();
                }
                expect((await ftsParts(h)).entries.some((e) => e.kind === "keyed")).toBe(true);
                reach.reach("exact reopen");
              } finally {
                release();
                await Promise.allSettled([pending, flush]);
                tx.rollback();
              }
            } finally {
              old.release();
            }
          } finally {
            release();
            await Promise.allSettled([pending, flush]);
            await h.close();
            retire = undefined;
          }
          expect(h.governor.usedBytes).toBe(0);
        }
      }),
      {
        seed,
        numRuns: 8,
        examples: [
          [false, 32],
          [true, 32],
          [false, 128],
          [true, 128],
        ],
      },
    );
    reach.assertReached();
  }, 60_000);
