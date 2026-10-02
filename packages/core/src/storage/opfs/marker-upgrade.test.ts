/* eslint-disable no-restricted-imports -- Released-writer and failure-injection tests run only in Node. */
/**
 * Layouts 7 and 8 to 9: the store's bytes stay as they are and only the marker moves. Layout 8
 * admits compaction jobs with replayed merge plans, which a layout-7 reader cannot parse;
 * layout 9 admits WAL frames whose payload spans continuation frames and checkpointed index
 * deltas of any size, which a layout-8 reader cannot. For each older layout these tests use its
 * frozen fixture and its last released writer — 0.12.1 for layout 7, 0.13.1 for layout 8 —
 * upgrade through the ordinary open API, keep writing, reopen, and check that the released
 * reader is refused afterwards without a byte changing. Every I/O boundary of the marker
 * publication is cut, by tab death and by power loss, and retried.
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { MinnowDatabase as ReleasedDatabase } from "@minnowdb/core-layout7";
import { OpfsBlockStore as ReleasedOpfsStore } from "@minnowdb/core-layout7/storage/opfs";
import { MinnowDatabase as Layout8Database } from "@minnowdb/core-layout8";
import { OpfsBlockStore as Layout8OpfsStore } from "@minnowdb/core-layout8/storage/opfs";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { StorageCorruptionError, StorageFormatVersionError } from "../types.js";
import { OpfsTree } from "./files.js";
import { OpfsBlockStore } from "./index.js";
import { walStagingTestHooks } from "./leader.js";
import { PowerLossModel } from "./power-loss-model.js";

const NAME = "layout7";
const PREFIX = `minnowdb/${NAME}/`;
const LAYOUT7 = '{"formatVersion":7}';
const LAYOUT8 = '{"formatVersion":8}';
const CURRENT = '{"formatVersion":9}';
const failure = new DOMException("injected upgrade I/O failure", "QuotaExceededError");

interface Fixture {
  files: Record<string, string>;
  expectations: { tables: string[]; blockId: string; blockValues: Array<string | null> };
}

function readFixture(layout: number): Fixture {
  return JSON.parse(
    readFileSync(
      new URL(`../../../format-fixtures/opfs-layout${String(layout)}.json`, import.meta.url),
      "utf8",
    ),
  ) as Fixture;
}

const fixture = readFixture(7);

function marker(shim: MemoryOpfs): string {
  return new TextDecoder().decode(shim.readFileBytes(`${PREFIX}format.json`));
}

function rows(version: number, count = 600) {
  return Array.from({ length: count }, (_, id) => ({
    id,
    label: `label-${String((id + version) % 13)}`,
    amount: id * 10 + version,
  }));
}

/** A deterministic shuffle, so the released fold's plan holds a range per replaced cell. */
function shuffled<T>(values: readonly T[]): T[] {
  const result = [...values];
  let state = 17;
  for (let index = result.length - 1; index > 0; index -= 1) {
    state = (state * 1_103_515_245 + 12_345) % 2 ** 31;
    const other = state % (index + 1);
    const value = result[index];
    const swap = result[other];
    if (value === undefined || swap === undefined) continue;
    result[index] = swap;
    result[other] = value;
  }
  return result;
}

/**
 * Leaves the released writer's merge-v1 fold half written: planned, one output block staged and
 * checkpointed, its transaction open. Returns the job and the rows the table must still read.
 */
async function releasedFoldInFlight(shim: MemoryOpfs) {
  const store = await ReleasedOpfsStore.open({ name: NAME, root: shim.root });
  const database = new ReleasedDatabase(store, { autoCompact: false, autoCollect: false });
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
      { name: "amount", type: "number" },
    ],
  });
  await database.insertBatch("items", rows(0));
  await database.upsertBatch("items", shuffled(rows(1)));
  await database.execute("UPDATE items SET label = 'patched' WHERE id % 5 = 0");
  const progress = await database.compactTableStep("items", { maxBlocks: 1 });
  if (progress.jobId === null || progress.result !== null) {
    throw new Error("Expected the released writer to leave a fold in flight");
  }
  const job = (await database.listCompactionJobs("items")).find(
    (candidate) => candidate.id === progress.jobId,
  );
  expect(job?.rewritePlan.kind).toBe("merge-v1");
  expect(job?.outputBlockIds.length).toBeGreaterThan(0);
  const expected = await database.readTable("items");
  await database.close();
  store._crashForTests();
  return { jobId: progress.jobId, expected };
}

function hydrateLayout7(source = fixture): MemoryOpfs {
  const shim = new MemoryOpfs();
  for (const [path, base64] of Object.entries(source.files)) {
    shim.writeFileBytes(
      path.replace("minnowdb/native-fixture/", PREFIX),
      Uint8Array.from(atob(base64), (character) => character.charCodeAt(0)),
    );
  }
  return shim;
}

async function verifyLayout7Fixture(shim: MemoryOpfs, extra: string[] = [], source = fixture) {
  const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
  try {
    expect(marker(shim)).toBe(CURRENT);
    expect(shim.readFileBytes(`${PREFIX}upgrade-7-8`)).toBeUndefined();
    expect(shim.readFileBytes(`${PREFIX}upgrade-8-9`)).toBeUndefined();
    expect((await store.listTables()).map(({ name }) => name)).toEqual(
      [...source.expectations.tables, ...extra].sort(),
    );
    const block = await store.getBlock(source.expectations.blockId);
    expect(block).toBeDefined();
    expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true, issueCount: 0 });
    return store;
  } catch (error) {
    store._crashForTests();
    throw error;
  }
}

function table(name: string) {
  return {
    id: name,
    name,
    managed: false,
    columns: [{ id: "value", name: "value", type: "string" as const, nullable: true }],
    revision: 0,
    createdAt: "2026-10-01T12:00:00.000Z",
  };
}

describe("OPFS layout 7 to 9", () => {
  it("finishes the released 0.12.1 writer's fold in flight, then writes and reopens", async () => {
    const shim = new MemoryOpfs();
    const { jobId, expected } = await releasedFoldInFlight(shim);
    expect(marker(shim)).toBe(LAYOUT7);

    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(marker(shim)).toBe(CURRENT);
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    expect(await database.readTable("items")).toEqual(expected);
    let progress = await database.resumeCompactionJob(jobId, { maxBlocks: 2 });
    while (progress.result === null) {
      progress = await database.resumeCompactionJob(jobId, { maxBlocks: 2 });
    }
    expect(progress.result.compacted).toBe(true);
    expect(await database.readTable("items")).toEqual(expected);

    // The current writer's next fold of the same table records a replayed plan.
    await database.upsertBatch("items", shuffled(rows(2)));
    const second = await database.compactTable("items", { minimumLevel0Segments: 1 });
    expect(second.compacted).toBe(true);
    const job = second.jobId === undefined ? undefined : await store.getCompactionJob(second.jobId);
    expect(job?.rewritePlan.kind).toBe("merge-v2");
    const latest = await database.readTable("items");
    expect(latest).toHaveLength(600);
    await database.close();
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    const reader = new MinnowDatabase(reopened, { autoCompact: false, autoCollect: false });
    expect(await reader.readTable("items")).toEqual(latest);
    expect(await reopened.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
    await reader.close();
    reopened._crashForTests();
  });

  it("refuses the released 0.12.1 reader after the upgrade without changing a byte", async () => {
    const shim = new MemoryOpfs();
    await releasedFoldInFlight(shim);
    (await OpfsBlockStore.open({ name: NAME, root: shim.root }))._crashForTests();
    const writes: string[] = [];
    shim.setWriteFault((path, phase) => {
      if (phase !== "flush") writes.push(`${phase}: ${path}`);
    });
    shim.setDeleteFault((path) => writes.push(`delete: ${path}`));
    await expect(ReleasedOpfsStore.open({ name: NAME, root: shim.root })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: 9,
      supportedVersion: 7,
      relation: "newer",
    });
    expect(writes).toEqual([]);
    shim.setWriteFault(null);
    shim.setDeleteFault(null);
    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
    store._crashForTests();
  });

  it("upgrades the frozen layout-7 fixture through the ordinary open API", async () => {
    const shim = hydrateLayout7();
    const store = await verifyLayout7Fixture(shim);
    await store.addTable(table("after-upgrade"));
    store._crashForTests();
    (await verifyLayout7Fixture(shim, ["after-upgrade"]))._crashForTests();
  });

  it("serializes concurrent openers through the marker upgrade", async () => {
    const shim = hydrateLayout7();
    const stores = await Promise.all(
      Array.from({ length: 4 }, () => OpfsBlockStore.open({ name: NAME, root: shim.root })),
    );
    try {
      expect(marker(shim)).toBe(CURRENT);
      for (const store of stores) {
        expect((await store.listTables()).map(({ name }) => name)).toEqual(
          fixture.expectations.tables,
        );
      }
      await stores[3]?.addTable(table("concurrent-write"));
      expect((await stores[0]?.listTables())?.map(({ name }) => name)).toContain(
        "concurrent-write",
      );
    } finally {
      for (const store of stores) store._crashForTests();
    }
    (await verifyLayout7Fixture(shim, ["concurrent-write"]))._crashForTests();
  });

  for (const mode of ["tab-death", "power-loss"] as const) {
    it(`recovers every marker publication ${mode} boundary`, async () => {
      const trace: string[] = [];
      const traced = hydrateLayout7();
      traced.setWriteFault((path, phase) => trace.push(`${phase}: ${path}`));
      traced.setDeleteFault((path) => trace.push(`delete: ${path}`));
      (await OpfsBlockStore.open({ name: NAME, root: traced.root }))._crashForTests();
      const boundaries = trace.filter(
        (entry) => entry.endsWith("upgrade-8-9") || entry.endsWith("format.json"),
      );
      // Witness written and flushed, then the marker rewritten (a truncate and a write) and
      // flushed, then the witness removed.
      expect(boundaries).toEqual([
        `create: ${PREFIX}upgrade-8-9`,
        `write: ${PREFIX}upgrade-8-9`,
        `write: ${PREFIX}upgrade-8-9`,
        `flush: ${PREFIX}upgrade-8-9`,
        `write: ${PREFIX}format.json`,
        `write: ${PREFIX}format.json`,
        `flush: ${PREFIX}format.json`,
        `delete: ${PREFIX}upgrade-8-9`,
      ]);
      for (const [cut, boundary] of boundaries.entries()) {
        const shim = hydrateLayout7();
        // Repeated entries are cut at their own occurrence, not the first.
        const occurrence = boundaries.slice(0, cut).filter((entry) => entry === boundary).length;
        let seen = 0;
        let armed = false;
        const fault = (path: string, phase: string) => {
          if (armed && `${phase}: ${path}` === boundary && seen++ === occurrence) {
            armed = false;
            throw failure;
          }
        };
        const model = new PowerLossModel(shim, fault);
        // The frozen files are already durable on the device.
        const tree = new OpfsTree(shim.root);
        for (const path of Object.keys(fixture.files)) {
          const handle = await tree.openHandle(
            path.replace("minnowdb/native-fixture/", PREFIX).split("/"),
            { create: false },
          );
          handle.flush();
          handle.close();
        }
        shim.setDeleteFault((path) => fault(path, "delete"));
        armed = true;
        await expect(
          OpfsBlockStore.open({ name: NAME, root: shim.root }),
          `${mode} at ${boundary}`,
        ).rejects.toThrow();
        if (mode === "power-loss") model.powerLoss();
        shim.setWriteFault(null);
        shim.setDeleteFault(null);
        const resumed = await verifyLayout7Fixture(shim);
        await resumed.addTable(table("after-retry"));
        resumed._crashForTests();
        (await verifyLayout7Fixture(shim, ["after-retry"]))._crashForTests();
      }
    });
  }

  it("finishes a marker torn mid-rewrite when its witness survived", async () => {
    // A 0.13 build tearing the marker on its way to layout 8 left the first witness; this
    // build's own rewrite to layout 9 leaves the second. Either finishes at layout 9.
    for (const [name, bytes] of [
      ["upgrade-7-8", '{"from":7,"to":8}'],
      ["upgrade-8-9", '{"from":8,"to":9}'],
    ] as const) {
      const shim = hydrateLayout7();
      shim.writeFileBytes(`${PREFIX}${name}`, new TextEncoder().encode(bytes));
      shim.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"formatVe'));
      (await verifyLayout7Fixture(shim))._crashForTests();
    }
  });

  it("rewrites a half-written witness beside an intact older marker", async () => {
    for (const name of ["upgrade-7-8", "upgrade-8-9"]) {
      const shim = hydrateLayout7();
      shim.writeFileBytes(`${PREFIX}${name}`, new Uint8Array());
      (await verifyLayout7Fixture(shim))._crashForTests();
    }
  });

  it("removes a witness that outlived a finished rewrite", async () => {
    for (const name of ["upgrade-7-8", "upgrade-8-9"]) {
      const shim = hydrateLayout7();
      (await OpfsBlockStore.open({ name: NAME, root: shim.root }))._crashForTests();
      shim.writeFileBytes(`${PREFIX}${name}`, new TextEncoder().encode("leftover"));
      (await verifyLayout7Fixture(shim))._crashForTests();
    }
  });

  it("refuses a torn marker whose witness is missing or damaged, without changing files", async () => {
    for (const [name, witness] of [
      ["upgrade-7-8", undefined],
      ["upgrade-7-8", new TextEncoder().encode('{"from":7,"to":9}')],
      ["upgrade-8-9", new TextEncoder().encode('{"from":7,"to":9}')],
    ] as const) {
      const shim = hydrateLayout7();
      if (witness !== undefined) shim.writeFileBytes(`${PREFIX}${name}`, witness);
      shim.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"formatVe'));
      const before = shim.readFileBytes(`${PREFIX}checkpoint-b`);
      await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toBeInstanceOf(
        StorageCorruptionError,
      );
      expect(shim.readFileBytes(`${PREFIX}checkpoint-b`)).toEqual(before);
      expect(marker(shim)).toBe('{"formatVe');
    }
  });

  it("refuses a layout newer than 9 without changing files", async () => {
    const shim = hydrateLayout7();
    shim.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"formatVersion":10}'));
    await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toBeInstanceOf(
      StorageFormatVersionError,
    );
    expect(marker(shim)).toBe('{"formatVersion":10}');
  });
});

const layout8 = readFixture(8);

/** A keyed, indexed table written by the released 0.13.1 build — the last layout-8 writer. */
async function releasedLayout8Database(shim: MemoryOpfs) {
  const store = await Layout8OpfsStore.open({ name: NAME, root: shim.root });
  const database = new Layout8Database(store, { autoCompact: false, autoCollect: false });
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
      { name: "amount", type: "number" },
    ],
  });
  await database.execute("CREATE INDEX items_amount ON items (amount)");
  await database.insertBatch("items", rows(0));
  await database.upsertBatch("items", shuffled(rows(1)));
  const expected = await database.readTable("items");
  await database.close();
  store._crashForTests();
  return expected;
}

async function amountLookup(database: MinnowDatabase, amount: number): Promise<unknown[]> {
  return (await database.query("SELECT id FROM items WHERE amount = ?", { params: [amount] })).rows;
}

describe("OPFS layout 8 to 9", () => {
  it("upgrades the released 0.13.1 writer's database, then logs a commit too large for one frame", async () => {
    const shim = new MemoryOpfs();
    const expected = await releasedLayout8Database(shim);
    expect(marker(shim)).toBe(LAYOUT8);

    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(marker(shim)).toBe(CURRENT);
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    expect(await database.readTable("items")).toEqual(expected);
    expect(await amountLookup(database, 10 * 7 + 1)).toEqual([{ id: 7 }]);

    // Layout 9's records: a frame spanning continuation pieces, carrying more distinct index
    // values than one layout-8 delta chunk holds.
    const previous = walStagingTestHooks.pieceBytes;
    walStagingTestHooks.pieceBytes = 64 * 1024;
    try {
      await database.insertBatch(
        "items",
        Array.from({ length: 70_000 }, (_, index) => ({
          id: 1_000 + index,
          label: "wide",
          amount: 1_000_000 + index,
        })),
      );
    } finally {
      walStagingTestHooks.pieceBytes = previous;
    }
    const wal = shim.readFileBytes(`${PREFIX}wal`) ?? new Uint8Array();
    expect(new TextDecoder("latin1").decode(wal).split("MNWC").length - 1).toBeGreaterThan(1);
    await database.close();
    store._crashForTests();

    // The continuation frames replay; then a checkpoint holds the large delta, and loads.
    for (const step of ["replay", "checkpoint"] as const) {
      const reopened = await OpfsBlockStore.open({ name: NAME, root: shim.root });
      const reader = new MinnowDatabase(reopened, { autoCompact: false, autoCollect: false });
      expect((await reader.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: 70_600 }]);
      expect(await amountLookup(reader, 1_000_000 + 69_999)).toEqual([{ id: 70_999 }]);
      expect(await amountLookup(reader, 10 * 7 + 1)).toEqual([{ id: 7 }]);
      expect(await reopened.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
      await reader.close();
      if (step === "replay") reopened.close();
      else reopened._crashForTests();
    }
  });

  it("refuses the released 0.13.1 reader after the upgrade without changing a byte", async () => {
    const shim = new MemoryOpfs();
    await releasedLayout8Database(shim);
    (await OpfsBlockStore.open({ name: NAME, root: shim.root }))._crashForTests();
    const writes: string[] = [];
    shim.setWriteFault((path, phase) => {
      if (phase !== "flush") writes.push(`${phase}: ${path}`);
    });
    shim.setDeleteFault((path) => writes.push(`delete: ${path}`));
    await expect(Layout8OpfsStore.open({ name: NAME, root: shim.root })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: 9,
      supportedVersion: 8,
      relation: "newer",
    });
    expect(writes).toEqual([]);
    shim.setWriteFault(null);
    shim.setDeleteFault(null);
    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
    store._crashForTests();
  });

  it("upgrades the frozen layout-8 fixture through the ordinary open API", async () => {
    const shim = hydrateLayout7(layout8);
    expect(marker(shim)).toBe(LAYOUT8);
    const store = await verifyLayout7Fixture(shim, [], layout8);
    await store.addTable(table("after-upgrade"));
    store._crashForTests();
    (await verifyLayout7Fixture(shim, ["after-upgrade"], layout8))._crashForTests();
  });

  it("serializes concurrent openers through the marker upgrade", async () => {
    const shim = hydrateLayout7(layout8);
    const stores = await Promise.all(
      Array.from({ length: 4 }, () => OpfsBlockStore.open({ name: NAME, root: shim.root })),
    );
    try {
      expect(marker(shim)).toBe(CURRENT);
      for (const store of stores) {
        expect((await store.listTables()).map(({ name }) => name)).toEqual(
          layout8.expectations.tables,
        );
      }
      await stores[2]?.addTable(table("concurrent-write"));
    } finally {
      for (const store of stores) store._crashForTests();
    }
    (await verifyLayout7Fixture(shim, ["concurrent-write"], layout8))._crashForTests();
  });

  for (const mode of ["tab-death", "power-loss"] as const) {
    it(`recovers every marker publication ${mode} boundary`, async () => {
      const trace: string[] = [];
      const traced = hydrateLayout7(layout8);
      traced.setWriteFault((path, phase) => trace.push(`${phase}: ${path}`));
      traced.setDeleteFault((path) => trace.push(`delete: ${path}`));
      (await OpfsBlockStore.open({ name: NAME, root: traced.root }))._crashForTests();
      const boundaries = trace.filter(
        (entry) => entry.endsWith("upgrade-8-9") || entry.endsWith("format.json"),
      );
      expect(boundaries).toHaveLength(8);
      for (const [cut, boundary] of boundaries.entries()) {
        const shim = hydrateLayout7(layout8);
        const occurrence = boundaries.slice(0, cut).filter((entry) => entry === boundary).length;
        let seen = 0;
        let armed = false;
        const fault = (path: string, phase: string) => {
          if (armed && `${phase}: ${path}` === boundary && seen++ === occurrence) {
            armed = false;
            throw failure;
          }
        };
        const model = new PowerLossModel(shim, fault);
        const tree = new OpfsTree(shim.root);
        for (const path of Object.keys(layout8.files)) {
          const handle = await tree.openHandle(
            path.replace("minnowdb/native-fixture/", PREFIX).split("/"),
            { create: false },
          );
          handle.flush();
          handle.close();
        }
        shim.setDeleteFault((path) => fault(path, "delete"));
        armed = true;
        await expect(
          OpfsBlockStore.open({ name: NAME, root: shim.root }),
          `${mode} at ${boundary}`,
        ).rejects.toThrow();
        if (mode === "power-loss") model.powerLoss();
        shim.setWriteFault(null);
        shim.setDeleteFault(null);
        const resumed = await verifyLayout7Fixture(shim, [], layout8);
        await resumed.addTable(table("after-retry"));
        resumed._crashForTests();
        (await verifyLayout7Fixture(shim, ["after-retry"], layout8))._crashForTests();
      }
    });
  }

  it("finishes a torn layout-8 marker whose witness survived, and refuses one without", async () => {
    const shim = hydrateLayout7(layout8);
    shim.writeFileBytes(`${PREFIX}upgrade-8-9`, new TextEncoder().encode('{"from":8,"to":9}'));
    shim.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"formatVersion"'));
    (await verifyLayout7Fixture(shim, [], layout8))._crashForTests();

    const bare = hydrateLayout7(layout8);
    bare.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"formatVersion"'));
    await expect(OpfsBlockStore.open({ name: NAME, root: bare.root })).rejects.toBeInstanceOf(
      StorageCorruptionError,
    );
  });
});
