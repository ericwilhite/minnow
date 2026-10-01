/* eslint-disable no-restricted-imports -- Released-writer and failure-injection tests run only in Node. */
/**
 * Layout 7 to 8: the store's bytes stay as they are and only the marker moves, because layout 8
 * admits compaction jobs with replayed merge plans that a layout-7 reader cannot parse. These
 * tests use the released 0.12.1 package — the last layout-7 writer — to leave a merge-v1 fold in
 * flight, then upgrade through the ordinary open API, finish that fold, keep writing, reopen,
 * and check that the released reader is refused afterwards without a byte changing. Every I/O
 * boundary of the marker publication is cut, by tab death and by power loss, and retried.
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { MinnowDatabase as ReleasedDatabase } from "@minnowdb/core-layout7";
import { OpfsBlockStore as ReleasedOpfsStore } from "@minnowdb/core-layout7/storage/opfs";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { StorageCorruptionError, StorageFormatVersionError } from "../types.js";
import { OpfsTree } from "./files.js";
import { OpfsBlockStore } from "./index.js";
import { PowerLossModel } from "./power-loss-model.js";

const NAME = "layout7";
const PREFIX = `minnowdb/${NAME}/`;
const LAYOUT7 = '{"formatVersion":7}';
const LAYOUT8 = '{"formatVersion":8}';
const failure = new DOMException("injected upgrade I/O failure", "QuotaExceededError");

const fixture = JSON.parse(
  readFileSync(new URL("../../../format-fixtures/opfs-layout7.json", import.meta.url), "utf8"),
) as {
  files: Record<string, string>;
  expectations: { tables: string[]; blockId: string; blockValues: Array<string | null> };
};

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

function hydrateLayout7(): MemoryOpfs {
  const shim = new MemoryOpfs();
  for (const [path, base64] of Object.entries(fixture.files)) {
    shim.writeFileBytes(
      path.replace("minnowdb/native-fixture/", PREFIX),
      Uint8Array.from(atob(base64), (character) => character.charCodeAt(0)),
    );
  }
  return shim;
}

async function verifyLayout7Fixture(shim: MemoryOpfs, extra: string[] = []) {
  const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
  try {
    expect(marker(shim)).toBe(LAYOUT8);
    expect(shim.readFileBytes(`${PREFIX}upgrade-7-8`)).toBeUndefined();
    expect((await store.listTables()).map(({ name }) => name)).toEqual(
      [...fixture.expectations.tables, ...extra].sort(),
    );
    const block = await store.getBlock(fixture.expectations.blockId);
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

describe("OPFS layout 7 to 8", () => {
  it("finishes the released 0.12.1 writer's fold in flight, then writes and reopens", async () => {
    const shim = new MemoryOpfs();
    const { jobId, expected } = await releasedFoldInFlight(shim);
    expect(marker(shim)).toBe(LAYOUT7);

    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(marker(shim)).toBe(LAYOUT8);
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
      actualVersion: 8,
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
      expect(marker(shim)).toBe(LAYOUT8);
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
        (entry) => entry.endsWith("upgrade-7-8") || entry.endsWith("format.json"),
      );
      // Witness written and flushed, then the marker rewritten (a truncate and a write) and
      // flushed, then the witness removed.
      expect(boundaries).toEqual([
        `create: ${PREFIX}upgrade-7-8`,
        `write: ${PREFIX}upgrade-7-8`,
        `write: ${PREFIX}upgrade-7-8`,
        `flush: ${PREFIX}upgrade-7-8`,
        `write: ${PREFIX}format.json`,
        `write: ${PREFIX}format.json`,
        `flush: ${PREFIX}format.json`,
        `delete: ${PREFIX}upgrade-7-8`,
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
    const shim = hydrateLayout7();
    shim.writeFileBytes(`${PREFIX}upgrade-7-8`, new TextEncoder().encode('{"from":7,"to":8}'));
    shim.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"formatVe'));
    (await verifyLayout7Fixture(shim))._crashForTests();
  });

  it("rewrites a half-written witness beside an intact layout-7 marker", async () => {
    const shim = hydrateLayout7();
    shim.writeFileBytes(`${PREFIX}upgrade-7-8`, new Uint8Array());
    (await verifyLayout7Fixture(shim))._crashForTests();
  });

  it("refuses a torn marker whose witness is missing or damaged, without changing files", async () => {
    for (const witness of [undefined, new TextEncoder().encode('{"from":7,"to":9}')]) {
      const shim = hydrateLayout7();
      if (witness !== undefined) shim.writeFileBytes(`${PREFIX}upgrade-7-8`, witness);
      shim.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"formatVe'));
      const before = shim.readFileBytes(`${PREFIX}checkpoint-b`);
      await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toBeInstanceOf(
        StorageCorruptionError,
      );
      expect(shim.readFileBytes(`${PREFIX}checkpoint-b`)).toEqual(before);
      expect(marker(shim)).toBe('{"formatVe');
    }
  });

  it("refuses a layout newer than 8 without changing files", async () => {
    const shim = hydrateLayout7();
    shim.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"formatVersion":9}'));
    await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toBeInstanceOf(
      StorageFormatVersionError,
    );
    expect(marker(shim)).toBe('{"formatVersion":9}');
  });
});
