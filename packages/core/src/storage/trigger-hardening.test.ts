import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { IndexedDbBlockStore } from "./indexeddb.js";
import { MemoryBlockStore } from "./memory.js";
import { OpfsBlockStore } from "./opfs/index.js";
import { RecordCore } from "./toolkit/record-core.js";
import {
  SNAPSHOT_FRAME_KINDS,
  type BlockStore,
  type SnapshotFrameStreamHeader,
  type TableRecord,
  type TriggerRecord,
} from "./types.js";

const CREATED_AT = "2026-08-25T00:00:00.000Z";

function trigger(id: string, name: string): TriggerRecord {
  return {
    id,
    name,
    event: "insert",
    timing: "after",
    statements: [{ sql: "INSERT INTO sink (value) VALUES (1)", bindings: [] }],
    createdAt: CREATED_AT,
  };
}

function table(name: string, triggers: readonly TriggerRecord[] = []): TableRecord {
  return {
    id: `table-${name}`,
    name,
    columns: [{ id: "value", name: "value", type: "number", nullable: false }],
    managed: false,
    revision: 0,
    ...(triggers.length === 0 ? {} : { triggers: [...triggers] }),
    createdAt: CREATED_AT,
  };
}

function requestResult<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.addEventListener("success", () => resolve(request.result), { once: true });
    request.addEventListener(
      "error",
      () => reject(request.error ?? new Error("IndexedDB request failed")),
      { once: true },
    );
  });
}

function transactionDone(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.addEventListener("complete", () => resolve(), { once: true });
    const rejectTransaction = (): void =>
      reject(transaction.error ?? new Error("IndexedDB transaction failed"));
    transaction.addEventListener("abort", rejectTransaction, { once: true });
    transaction.addEventListener("error", rejectTransaction, { once: true });
  });
}

const implementations: Array<{
  name: string;
  open: () => Promise<BlockStore>;
}> = [
  { name: "memory", open: async () => new MemoryBlockStore() },
  {
    name: "IndexedDB",
    open: async () =>
      IndexedDbBlockStore.open({ indexedDB: new IDBFactory(), name: crypto.randomUUID() }),
  },
  {
    name: "OPFS",
    open: async () => {
      const opfs = new MemoryOpfs();
      return OpfsBlockStore.open({ root: opfs.root, name: crypto.randomUUID() });
    },
  },
];

describe("global trigger identity hardening", () => {
  for (const implementation of implementations) {
    it(`${implementation.name} atomically owns names and IDs and releases both on table removal`, async () => {
      const store = await implementation.open();
      try {
        const first = table("first");
        const second = table("second");
        await store.addTable(first);
        await store.addTable(second);
        const installed = await store.updateTable(first.id, first.revision, {
          triggers: [trigger("trigger-1", "audit")],
        });

        const beforeRejected = await store.getCatalogProbe();
        await expect(
          store.updateTable(second.id, second.revision, {
            triggers: [trigger("trigger-2", "audit")],
          }),
        ).rejects.toThrow("Trigger already exists: audit");
        await expect(
          store.updateTable(second.id, second.revision, {
            triggers: [trigger("trigger-1", "other_audit")],
          }),
        ).rejects.toThrow("Trigger ID already exists: trigger-1");
        await expect(
          store.updateTable(first.id, installed.revision, {
            triggers: [trigger("trigger-replacement", "audit")],
          }),
        ).rejects.toThrow("Trigger already exists: audit");
        await expect(
          store.updateTable(first.id, installed.revision, {
            triggers: [trigger("trigger-1", "renamed_audit")],
          }),
        ).rejects.toThrow("Trigger ID already exists: trigger-1");

        expect(await store.getCatalogProbe()).toEqual(beforeRejected);
        expect(await store.getTable(second.id)).toMatchObject({ revision: 0 });
        expect((await store.getTable(second.id))?.triggers).toBeUndefined();

        await store.removeTable(first.id, installed.revision);
        const reused = await store.updateTable(second.id, second.revision, {
          triggers: [trigger("trigger-1", "audit")],
        });
        expect(reused.triggers).toEqual([trigger("trigger-1", "audit")]);
        if (store instanceof IndexedDbBlockStore) {
          expect((await store.checkIntegrity()).issues).toEqual([]);
        }
      } finally {
        store.close();
      }
    });
  }

  it("OPFS rebuilds trigger ownership from its checkpoint and WAL on reopen", async () => {
    const opfs = new MemoryOpfs();
    const name = crypto.randomUUID();
    let store = await OpfsBlockStore.open({ root: opfs.root, name, checkpointEntries: 1 });
    const first = table("opfs_first", [trigger("opfs-trigger", "opfs_audit")]);
    const second = table("opfs_second");
    await store.addTable(first);
    await store.addTable(second);
    store.close();

    store = await OpfsBlockStore.open({ root: opfs.root, name, checkpointEntries: 1 });
    try {
      await expect(
        store.updateTable(second.id, second.revision, {
          triggers: [trigger("another-trigger", "opfs_audit")],
        }),
      ).rejects.toThrow("Trigger already exists: opfs_audit");
      await expect(
        store.updateTable(second.id, second.revision, {
          triggers: [trigger("opfs-trigger", "another_audit")],
        }),
      ).rejects.toThrow("Trigger ID already exists: opfs-trigger");
    } finally {
      store.close();
    }
  });

  it("IndexedDB integrity reports orphan markers and open refuses a missing marker", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await IndexedDbBlockStore.open({ indexedDB, name });
    const record = table("integrity", [trigger("integrity-trigger", "integrity_audit")]);
    await store.addTable(record);

    const raw = await requestResult(indexedDB.open(name));
    let transaction = raw.transaction("catalog", "readwrite");
    transaction
      .objectStore("catalog")
      .put({ tableId: "missing-table", triggerId: "missing-trigger" }, "trigger/name/orphan_audit");
    await transactionDone(transaction);
    expect((await store.checkIntegrity()).issues).toContainEqual(
      expect.objectContaining({
        code: "broken-trigger-name",
        location: "trigger/name/orphan_audit",
      }),
    );
    store.close();

    transaction = raw.transaction("catalog", "readwrite");
    transaction.objectStore("catalog").delete("trigger/name/orphan_audit");
    transaction.objectStore("catalog").delete("trigger/name/integrity_audit");
    await transactionDone(transaction);
    raw.close();

    await expect(IndexedDbBlockStore.open({ indexedDB, name })).rejects.toThrow(
      "trigger name marker is missing or mismatched",
    );
  });

  it("rejects duplicate trigger names and IDs in checkpoints without changing live state", () => {
    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    const first = table("checkpoint_first", [trigger("checkpoint-trigger", "checkpoint_audit")]);
    const second = table("checkpoint_second");
    core.addTable(first);
    core.addTable(second);
    const before = core.dump();

    const withDuplicateName = {
      ...before,
      tables: before.tables.map((record) =>
        record.id === second.id
          ? { ...record, triggers: [trigger("different-trigger", "checkpoint_audit")] }
          : record,
      ),
    };
    expect(() => core.load(withDuplicateName)).toThrow("Trigger already exists: checkpoint_audit");
    expect(core.dump()).toEqual(before);

    const withDuplicateId = {
      ...before,
      tables: before.tables.map((record) =>
        record.id === second.id
          ? { ...record, triggers: [trigger("checkpoint-trigger", "different_audit")] }
          : record,
      ),
    };
    expect(() => core.load(withDuplicateId)).toThrow(
      "Trigger ID already exists: checkpoint-trigger",
    );
    expect(core.dump()).toEqual(before);
  });

  it("rejects duplicate trigger ownership while atomically promoting a snapshot", () => {
    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    const header: SnapshotFrameStreamHeader = {
      formatVersion: 1,
      databaseVersion: 0,
      createdAt: CREATED_AT,
      kinds: Object.fromEntries(
        SNAPSHOT_FRAME_KINDS.map((kind) => [kind, { frameCount: 0, itemCount: 0, storedBytes: 0 }]),
      ) as SnapshotFrameStreamHeader["kinds"],
    };
    expect(() =>
      core.loadSnapshotFrameItems(
        header,
        [
          {
            kind: "table",
            record: table("snapshot_first", [trigger("snapshot-one", "snapshot_audit")]),
            nextRowId: 1n,
            autoIncrement: [],
          },
          {
            kind: "table",
            record: table("snapshot_second", [trigger("snapshot-two", "snapshot_audit")]),
            nextRowId: 1n,
            autoIncrement: [],
          },
        ],
        [],
      ),
    ).toThrow("Trigger already exists: snapshot_audit");
    expect(core.listTables()).toEqual([]);
  });
});
