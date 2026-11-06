/** Byte accounting uses stored ByteRecord/IndexEntry shapes, not just flat numeric values. */
import "fake-indexeddb/auto";
import { IDBFactory } from "fake-indexeddb";
import { expect, it, vi } from "vitest";
import { openIndexedDb } from "../src/next/storage/idb-like.js";
import {
  countDatabase,
  openRealStorage,
  startMeasuredOwner,
  firstStatisticsCount,
  type OwnerPhase,
} from "./next-storage-owner-reopen.js";
import { FakeLockRegistry } from "../src/next/testing/fake-locks.js";
import { Governor } from "../src/next/memory/governor.js";
import { WritePathImpl } from "../src/next/write/engine.js";
import { SimDisk } from "../src/next/storage/sim-disk.js";
import {
  autocommit,
  batch,
  ints,
  column,
  tableInput,
  INT,
} from "../src/next/write/test-support.js";

it("counts cloned record bytes and the numeric fields of persisted index entries separately", async () => {
  const raw = await openIndexedDb(new IDBFactory(), "owner-read-byte-accounting");
  try {
    await raw.transaction(["data", "data-index"], "readwrite", "strict", (tx) => {
      tx.store("data").put(0, { offset: 0, bytes: new ArrayBuffer(32) });
      tx.store("data-index").put(0, { offset: 0, length: 32 });
    });
    const counts = { payloadBytes: 0, numericBytes: 0, reads: 0 };
    const observed = countDatabase(raw, counts);
    await observed.transaction(["data", "data-index"], "readonly", "relaxed", (tx) => {
      tx.store("data-index").last(null, () => undefined);
      tx.store("data-index").getAll({}, () => undefined);
      tx.store("data-index").getAllKeys({}, () => undefined);
      tx.store("data").get(0, () => undefined);
    });
    // Two 16-byte index values, one 8-byte key and the byte record's 8-byte offset.
    expect(counts).toEqual({ payloadBytes: 32, numericBytes: 48, reads: 4 });
  } finally {
    raw.close();
  }
});

import { loadOwnerDataset } from "./next-storage-owner-load.js";
import { OwnerPhaseMeter } from "./next-storage-owner-phase.js";

it("keeps wall time and reads between synchronous wall-only checkpoints", async () => {
  let now = 0;
  const counts = { payloadBytes: 0, numericBytes: 0, reads: 0 };
  const phases: unknown[] = [];
  const meter = new OwnerPhaseMeter(
    counts,
    (value) => {
      phases.push(value);
    },
    () => now,
  );
  now = 4;
  counts.payloadBytes = 8;
  counts.reads = 1;
  const first = meter.finish("backend");
  // A background continuation runs before the caller resumes from await finish().
  now = 11;
  counts.payloadBytes += 32;
  counts.reads++;
  await first;
  const second = await meter.finish("space");
  expect(second).toMatchObject({ wallMs: 7, reads: { payloadBytes: 32, reads: 1 } });
  expect(phases).toHaveLength(2);
});

it("keeps reads arriving during a CPU-sampling wait without charging the wait to phase wall", async () => {
  let now = 0;
  const counts = { payloadBytes: 0, numericBytes: 0, reads: 0 };
  let resume!: () => void;
  const checkpoint = new Promise<void>((resolve) => {
    resume = resolve;
  });
  const meter = new OwnerPhaseMeter(
    counts,
    (phase) => (phase.phase === "backend" ? checkpoint : undefined),
    () => now,
  );
  now = 4;
  counts.payloadBytes = 8;
  const first = meter.finish("backend");
  now = 100;
  counts.payloadBytes += 32;
  resume();
  await first;
  now = 107;
  counts.payloadBytes += 16;
  const second = await meter.finish("space");
  expect(second).toMatchObject({ wallMs: 7, reads: { payloadBytes: 48 } });
});

it("captures ready in the real host callback and counts through the real recovered write path", async () => {
  const admissions = vi.spyOn(Governor.prototype, "admit");
  const locks = new FakeLockRegistry().context();
  const disk = new SimDisk();
  const directory = {
    getFileHandle: async (name: string) => ({
      createSyncAccessHandle: async () => disk.open(name, true),
    }),
  };
  vi.stubGlobal("navigator", {
    locks,
    storage: { getDirectory: async () => ({ getDirectoryHandle: async () => directory }) },
  });
  let owner: Awaited<ReturnType<typeof startMeasuredOwner>> | null = null;
  let restoreDate: () => void = () => undefined;
  let restoreRecovery: () => void = () => undefined;
  try {
    const seed = await openRealStorage("opfs", "unit-ready-endpoint");
    try {
      const startup = admissions.mock.results.find(
        (_, index) => admissions.mock.calls[index]?.[0].label === "extent space startup",
      );
      if (startup?.type !== "return") throw new Error("Startup was not admitted");
      const grant = await startup.value;
      expect(grant.allocated).toBe(0);
      expect(grant.held).toBe(0); // completed startup must not obstruct writer/merge admission
      await autocommit(seed.path, (statement) =>
        statement.changeCatalog({
          kind: "createTable",
          table: tableInput("tiny", [column("id", INT, false)]),
        }),
      );
      const table = seed.path.catalog.tableByName("tiny");
      if (table === undefined) throw new Error("Missing test table");
      await autocommit(seed.path, (statement) =>
        statement.table(table.id).insert(batch(ints([1, 2, 3])), null),
      );
    } finally {
      await seed.close();
    }
    const phases: OwnerPhase[] = [];
    let readyCalls = 0;
    // OwnerEngineFactory's contract puts recovery in open(), after fence acquisition. A long
    // recovery must not consume the 5s fence/handle acquisition budget (host.ts startup step 2).
    const at = Date.now();
    const date = vi.spyOn(Date, "now").mockReturnValue(at);
    restoreDate = () => date.mockRestore();
    const open = WritePathImpl.open.bind(WritePathImpl);
    const recovery = vi.spyOn(WritePathImpl, "open").mockImplementation((options) => {
      date.mockReturnValue(at + 6_000);
      return open(options);
    });
    restoreRecovery = () => recovery.mockRestore();
    owner = await startMeasuredOwner(
      "opfs",
      "unit-ready-endpoint",
      (value) => {
        phases.push(value);
      },
      () => {
        readyCalls++;
        expect(phases.at(-1)?.phase).toBe("host");
      },
    );
    expect(readyCalls).toBe(1);
    expect(owner.executor.prepared).toBe(0);
    expect(owner.rootInfo?.maintenanceAtReady).toBeDefined();
    expect((await firstStatisticsCount(owner, "tiny")).count).toBe("3");
    expect(phases.map((value) => value.phase)).toEqual([
      "backend",
      "space",
      "recovery",
      "host",
      "countSetup",
      "count",
    ]);
  } finally {
    await owner?.close();
    restoreRecovery();
    restoreDate();
    vi.unstubAllGlobals();
    admissions.mockRestore();
    locks.kill();
  }
});

it("validates and resumes an already completed canonical load without duplicating rows", async () => {
  const locks = new FakeLockRegistry().context();
  const disk = new SimDisk();
  const directory = {
    getFileHandle: async (name: string) => ({
      createSyncAccessHandle: async () => disk.open(name, true),
    }),
  };
  vi.stubGlobal("navigator", {
    locks,
    storage: { getDirectory: async () => ({ getDirectoryHandle: async () => directory }) },
  });
  const opened = await openRealStorage("opfs", "resume-unit", undefined, undefined, 512 << 20);
  try {
    await loadOwnerDataset(opened.path, "W5M", () => undefined, 1000);
    const before = opened.path.catalog.tables.map(
      (table) => opened.path.statistics(table.id)?.rowCount,
    );
    await loadOwnerDataset(opened.path, "W5M", () => undefined, 1000, true);
    expect(opened.governor.capBytes).toBe(512 << 20);
    expect(
      opened.path.catalog.tables.map((table) => opened.path.statistics(table.id)?.rowCount),
    ).toEqual(before);
    expect(before).toEqual([1000, 50, 100, 1000]);
  } finally {
    await opened.close();
    vi.unstubAllGlobals();
    locks.kill();
  }
});
