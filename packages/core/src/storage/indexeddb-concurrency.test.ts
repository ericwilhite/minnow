/**
 * Several connections (tabs) over one IndexedDB database racing commits against the same
 * expected manifest version: exactly one wins each version, a concurrent reader never sees a
 * torn manifest, and engine-level autocommit writers lose no rows.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "../engine/database.js";
import type { IndexedDbBlockStore } from "./indexeddb.js";
import { WriteConflictError } from "./types.js";
import {
  EVENTS_TABLE,
  NOW,
  activeTransaction,
  openStore,
  segment,
} from "./indexeddb-audit-helpers.js";

async function manifestBlockIds(store: IndexedDbBlockStore, version: number): Promise<string[]> {
  const ids: string[] = [];
  let cursor: string | null = null;
  do {
    const page = await store.listManifestBlockPage({ version, afterBlockId: cursor, limit: 256 });
    ids.push(...page.records.map((record) => record.blockId));
    cursor = page.nextCursor;
  } while (cursor !== null);
  return ids;
}

describe("IndexedDB concurrent commits across connections", () => {
  it("lets exactly one of N racing commits win per version and never publishes a phantom", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const seedStore = await openStore(indexedDB, name);
    await seedStore.addTable(EVENTS_TABLE);
    const tabs = await Promise.all(Array.from({ length: 4 }, () => openStore(indexedDB, name)));
    const reader = await openStore(indexedDB, name);

    // Reader: continuously read the current version and every block it names, and flag any
    // manifest that names a block it cannot serve.
    let stopReading = false;
    const readerIssues: string[] = [];
    const readerLoop = (async () => {
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- set by the writers
      while (!stopReading) {
        const manifest = await reader.getCurrentManifest();
        if (manifest !== undefined) {
          const ids = await manifestBlockIds(reader, manifest.version);
          const present = await reader.hasManifestBlocks(manifest.version, ids);
          if (present.some((flag) => !flag)) {
            readerIssues.push(`version ${String(manifest.version)} lists a block it lacks`);
          }
          for (const id of ids) {
            const bytes = await reader.readManifestBlock(manifest.version, id);
            if (bytes === undefined) {
              readerIssues.push(`version ${String(manifest.version)} block ${id} unreadable`);
            }
          }
        }
        await new Promise((resolve) => setTimeout(resolve, 0));
      }
    })();

    // Each tab runs rounds: begin at the current version, stage, commit; on conflict, abort
    // and retry from the new version.
    const rounds = 6;
    const wins = new Map<number, string>();
    await Promise.all(
      tabs.map(async (tab, tabIndex) => {
        for (let round = 0; round < rounds; round += 1) {
          for (let attempt = 0; ; attempt += 1) {
            const current = (await tab.getCurrentManifestVersion()) ?? null;
            const id = `t${String(tabIndex)}-r${String(round)}-a${String(attempt)}`;
            await tab.createTransaction(activeTransaction(id, current));
            const blockId = `${id}-block`;
            const staged = await tab.stageTransactionArtifacts({
              transactionId: id,
              expectedRevision: 0,
              blocks: [{ id: blockId, bytes: Uint8Array.of(tabIndex, round) }],
              segments: [segment(`${id}-seg`, id, blockId, 0, 1n)],
              updatedAt: NOW,
            });
            try {
              const manifest = await tab.commitTransaction({
                transactionId: id,
                expectedTransactionRevision: staged.revision,
                expectedManifestVersion: current,
                levelZeroSegmentLimits: [{ tableId: "events", limit: 4_096 }],
                committedAt: NOW,
              });
              if (wins.has(manifest.version)) {
                readerIssues.push(`version ${String(manifest.version)} published twice`);
              }
              wins.set(manifest.version, id);
              break;
            } catch (error) {
              if (!(error instanceof WriteConflictError)) throw error;
              await tab.updateTransaction(id, staged.revision, {
                status: "aborted",
                updatedAt: NOW,
              });
            }
          }
        }
      }),
    );
    stopReading = true;
    await readerLoop;

    expect(readerIssues).toEqual([]);
    expect(wins.size).toBe(tabs.length * rounds);
    expect([...wins.keys()].sort((a, b) => a - b)).toEqual(
      Array.from({ length: tabs.length * rounds }, (_, index) => index),
    );
    // The final manifest's membership is exactly the winners' cumulative blocks.
    const final = await reader.getCurrentManifest();
    expect(final?.version).toBe(tabs.length * rounds - 1);
    const finalIds = await manifestBlockIds(reader, final?.version ?? 0);
    expect(finalIds.sort()).toEqual([...wins.values()].map((id) => `${id}-block`).sort());
    expect((await reader.checkIntegrity()).issues).toEqual([]);
    for (const tab of tabs) tab.close();
    reader.close();
    seedStore.close();
  });

  it("loses no rows from engine autocommit writers on three connections", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const stores = await Promise.all(Array.from({ length: 3 }, () => openStore(indexedDB, name)));
    const databases = stores.map(
      (store) => new MinnowDatabase(store, { autoCompact: false, maxCommitRetries: 64 }),
    );
    await databases[0]?.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "who", type: "number" },
      ],
    });
    const perWriter = 15;
    await Promise.all(
      databases.map(async (database, who) => {
        for (let index = 0; index < perWriter; index += 1) {
          await database.insert("items", { id: who * 1_000 + index, who });
        }
      }),
    );
    for (const database of databases) {
      const rows = (await database.query("SELECT COUNT(*) AS n FROM items", { memoize: false }))
        .rows as Array<{ n: number }>;
      expect(rows[0]?.n).toBe(perWriter * databases.length);
    }
    for (const database of databases) await database.close();
  });
});
