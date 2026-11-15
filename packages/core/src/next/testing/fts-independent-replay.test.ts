/** Independent FTS coverage law: a conservative global checkpoint may lag a complete
 * FTS checkpoint. Rows still replay once; the covered FTS claim never claims newer staging.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_INDEPENDENT_REPLAY_REACH } from "../../testing/reach.js";
import { Harness, autocommit, batch, texts, rowsOf } from "../write/test-support.js";
import { setupFts, ftsBudget } from "./fts-transaction.test-support.js";
import { decodeManifestRoot, encodeManifestRoot } from "../format/manifest-codec.js";
import { readFtsJournal, ftsPreparedEntries } from "../format/fts-journal.js";
import type { BlobRef } from "../storage/extents.js";
for (const seed of seedsFor("next-fts-independent-replay", [0xb853, 0xb854]))
  it(`replays rows once when FTS coverage leads the global checkpoint ${String(seed)}`, async () => {
    const reach = createReach("FTS independent replay", FTS_INDEPENDENT_REPLAY_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 4 }), async (n) => {
        reach.beginHistory();
        const h = new Harness();
        try {
          const path = await setupFts(h),
            base = path.durableLsn,
            publish = h.space.publish.bind(h.space),
            b = ftsBudget();
          // Publish a VALID conservative global checkpoint. The catalog is unchanged after base;
          // no log recycle occurs, and all row/FTS extents and checksums remain exactly the writer's.
          h.space.publish = async (bytes, change) => {
            const parsed = decodeManifestRoot(bytes, b);
            try {
              return await publish(
                encodeManifestRoot({ ...parsed.value, globalLsn: base }),
                change,
              );
            } finally {
              b.free(parsed.charge);
            }
          };
          const receipts: number[] = [];
          for (let i = 0; i < n; i++)
            receipts.push(
              await autocommit(path, (s) =>
                s.table(1).insert(batch(texts([`alpha ${String(i)}`])), null),
              ),
            );
          expect(path.state().root.fts.checkpoint).not.toBeNull();
          await h.reopen();
          reach.reach("global trails FTS");
          for (let pass = 0; pass < 3; pass++) {
            if (pass) await h.reopen();
            const pin = h.path.pin();
            try {
              expect(await rowsOf(pin, 1)).toEqual(
                Array.from({ length: n }, (_, i) => [`alpha ${String(i)}`]),
              );
            } finally {
              pin.release();
            }
            reach.reach("rows once");
          }
          await h.path.recycleLog();
          let ref: BlobRef | null = h.path.state().root.fts.checkpoint;
          const seen: number[] = [],
            ids: number[] = [];
          while (ref) {
            const held = await readFtsJournal(ref, b, h.space);
            try {
              const p = held.value;
              if (p.kind !== "checkpoint") throw new Error("Missing FTS checkpoint");
              seen.push(p.lsn);
              for await (const e of ftsPreparedEntries(p.staged, b, h.space))
                if (e.kind === "insert") ids.push(e.documents.first);
              ref = p.previous;
            } finally {
              held.release();
            }
          }
          expect(seen).toEqual([...receipts].reverse());
          expect(ids.sort((a, b) => a - b)).toEqual(Array.from({ length: n }, (_, i) => i + 1));
          expect(b.held).toBe(0);
          reach.reach("each claim once");
          reach.reach("actual LSNs");
          reach.reach("repeated reopen");
          reach.reach("recycle");
        } finally {
          await h.close();
        }
      }),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  }, 60_000);
