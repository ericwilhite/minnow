/** R25: replacing unclaimed FTS payload is bounded independently of its page count. */
import { afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_RETIREMENT_PRESSURE_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { Harness, batch, texts, rules, rowsOf } from "../write/test-support.js";
import { setupFts } from "./fts-transaction.test-support.js";
import { writeFtsNode } from "../format/fts-storage.js";
import { writeFtsJournal, type FtsJournalEntry } from "../format/fts-journal.js";
import { encodeManifestRoot } from "../format/manifest-codec.js";
import { ExtentListKeeper } from "../storage/extent-list.js";
import type { BlobRef } from "../storage/extents.js";
import { FTS_PUBLICATION_MINIMUM_BYTES } from "../write/fts-minimum.js";

class PressureHarness extends Harness {
  override readonly governor = new Governor(64 << 20);
}
const reach = createReach("FTS unclaimed retirement", FTS_RETIREMENT_PRESSURE_REACH);
for (const seed of seedsFor("next-fts-retirement-pressure", [0xb8c1, 0xb8c2]))
  it(`streams a large abandoned preparation under competing credit ${String(seed)}`, async (context) => {
    const h = new PressureHarness("strict", "memory"),
      aliases = (seed & 1) !== 0,
      count = 32769 + (seed & 1);
    const stop = new AbortController(),
      lifetime: { close?: () => Promise<void> } = {};
    context.onTestFinished(() => {
      stop.abort(new Error("Retired FTS retirement history"));
      void lifetime.close?.().catch(() => undefined);
    });
    let metadata: Grant | undefined,
      competitor: Grant | undefined,
      pressure = false,
      growths = 0;
    const admit = h.governor.admitGroup.bind(h.governor);
    h.governor.admitGroup = async (options) => {
      if (pressure && options.label === "grow write path metadata") {
        growths++;
        throw new Error("FTS retirement requested fresh credit under its fixed peak");
      }
      const result = await admit(options);
      metadata = result.find((g) => g.label === "write path metadata") ?? metadata;
      return result;
    };
    try {
      reach.beginHistory();
      const first = await setupFts(h);
      lifetime.close = () => h.path.close();
      await first.close();
      const before = first.state().root,
        seedGrant = await h.governor.admit({
          label: "abandoned FTS fixture",
          minimumBytes: 32 << 20,
          spillable: false,
        }),
        writer = h.space.beginWrite("abandoned FTS fixture", { budget: seedGrant }),
        keeper = new ExtentListKeeper(before.extentList, { reader: h.space, budget: seedGrant });
      let head: BlobRef | null = null,
        ordinal = 0;
      const entries: FtsJournalEntry[] = [];
      async function flush() {
        if (!entries.length) return;
        head = await writeFtsJournal(
          { kind: "prepared", transaction: 1, ordinal: ++ordinal, previous: head, entries },
          seedGrant,
          writer,
        );
        entries.length = 0;
      }
      try {
        for (let id = 1; id <= count; id++) {
          stop.signal.throwIfAborted();
          const ref = await writeFtsNode(
              "ftsDocuments",
              {
                kind: "leaf",
                ids: Float64Array.of(id),
                lengths: Uint16Array.of(0),
                renderable: Uint8Array.of(1),
              },
              seedGrant,
              writer,
            ),
            entry: FtsJournalEntry = {
              kind: "insert",
              tableId: 1,
              schemaVersion: 1,
              ftsId: 1,
              columnId: 1,
              generation: 1,
              mutationEpoch: 0,
              documents: { ref, first: id, last: id, count: 1, tokens: 0n },
              terms: null,
            };
          entries.push(entry);
          if (entries.length === 64) await flush();
          if (aliases && id % 17 === 0) {
            entries.push(entry);
            if (entries.length === 64) await flush();
          }
        }
        await flush();
        const prepared = await keeper.prepare(1, { added: writer.extents, released: [], writer }),
          root = {
            ...before,
            sequence: before.sequence + 1,
            fts: { ...before.fts, staged: head },
            extentList: prepared.list,
          };
        await h.space.publish(encodeManifestRoot(root), {
          writers: [writer],
          retired: prepared.retire,
          list: prepared.list,
          budget: seedGrant,
        });
        keeper.commit(1, root.sequence);
      } finally {
        if (writer.state === "open") await writer.abort();
        keeper.close();
        expect(seedGrant.allocated).toBe(0);
        seedGrant.close();
      }
      const path = await h.reopen();
      let inventoryCount: number | undefined,
        inventoryReleased = false,
        publications = 0;
      const prepare = h.space.prepareRetirements.bind(h.space);
      h.space.prepareRetirements = async (...args) => {
        const result = await prepare(...args);
        if (inventoryCount === undefined) {
          inventoryCount = result.count;
          const release = result.release.bind(result);
          result.release = async () => {
            await release();
            inventoryReleased = true;
          };
        }
        return result;
      };
      const publish = h.space.publish.bind(h.space);
      h.space.publish = async (root, change) => {
        if (change.retiredInventory !== undefined) {
          expect(inventoryReleased).toBe(false);
          publications++;
        }
        const result = await publish(root, change);
        if (change.retiredInventory !== undefined) expect(inventoryReleased).toBe(false);
        return result;
      };
      const tx = await path.begin(),
        statement = tx.statement({ rules });
      await statement.table(1).insert(batch(texts(["kept"])), null);
      await statement.end();
      if (!metadata) throw new Error("Missing native metadata account");
      const excess = metadata.held - metadata.allocated - FTS_PUBLICATION_MINIMUM_BYTES;
      if (excess > 0) metadata.release(excess);
      const held = metadata.held;
      competitor = await h.governor.admit({
        label: "all competing ordinary credit",
        minimumBytes: 0,
        spillable: false,
      });
      while (competitor.tryGrow(64 << 10)) {
        /* Drain ordinary/cache credit. */
      }
      while (competitor.tryGrow(1)) {
        /* Drain the final fractional window. */
      }
      expect(competitor.allocate(competitor.held)).toBe(true);
      pressure = true;
      const receipt = await tx.commit(null);
      await receipt.durable;
      stop.signal.throwIfAborted();
      expect(growths).toBe(0);
      expect(metadata.held).toBe(held);
      expect(inventoryCount).toBe(count + ordinal);
      expect(publications).toBe(1);
      expect(inventoryReleased).toBe(true);
      reach.reach("owned through publication");
      reach.reach("released after settlement");
      reach.reach("large preparation");
      reach.reach("fixed native credit");
      reach.reach(aliases ? "shared source refs" : "unique source refs");
      reach.reach("disk dedupe");
      reach.reach("no fresh admission");
      pressure = false;
      competitor.free(competitor.allocated);
      competitor.close();
      competitor = undefined;
      await path.flush();
      await path.recycleLog();
      await h.space.audit();
      const reopened = await h.reopen(),
        pin = reopened.pin();
      try {
        expect(await rowsOf(pin, 1)).toEqual([["kept"]]);
      } finally {
        pin.release();
      }
      await h.space.audit();
      reach.reach("reopen exact rows");
    } finally {
      pressure = false;
      if (competitor) {
        competitor.free(competitor.allocated);
        competitor.close();
      }
      await h.close();
      h.governor.admitGroup = admit;
      expect(h.governor.usedBytes).toBe(0);
    }
    reach.reach("zero charges");
  }, 120_000);

afterAll(() => reach.assertReached());
