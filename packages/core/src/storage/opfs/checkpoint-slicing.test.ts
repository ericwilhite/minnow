/**
 * Scheduled checkpoints encode and write a slice at a time, handing the event loop a turn
 * between slices, so checkpointing a large database does not hold the thread for its whole
 * state. The slots, their order, and their bytes are the synchronous checkpoint's; these tests
 * pin that a multi-slice checkpoint really spans turns, publishes bytes the synchronous encoder
 * would have written, and survives power loss or a refused write at every one of its file
 * operations — with the refused checkpoint retried by later writes.
 */
import { describe, expect, it, vi } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import type { TableRecord } from "../types.js";
import { decodeSyncCheckpoint, encodeSyncCheckpoint } from "../toolkit/wire.js";
import { PowerLossModel } from "./power-loss-model.js";
import { OpfsBlockStore } from "./index.js";
import { heavyTestTimeout } from "../../engine/storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(60_000) });

/** Wide records, so a few hundred tables make a checkpoint several write slices long. */
const TABLES = 240;
const COLUMNS = 96;

function wideTable(index: number): TableRecord {
  return {
    id: `table-${String(index)}`,
    name: `wide_${String(index)}`,
    columns: Array.from({ length: COLUMNS }, (_, column) => ({
      id: `c${String(column)}`,
      name: `measurement_column_with_a_long_descriptive_name_${String(column)}`,
      type: "number" as const,
      nullable: true,
    })),
    managed: false,
    revision: 0,
    createdAt: "2026-10-01T00:00:00.000Z",
  };
}

async function waitFor(condition: () => boolean, what: string): Promise<void> {
  for (let attempt = 0; attempt < 2_000; attempt += 1) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error(`Timed out waiting for ${what}`);
}

/**
 * A store holding `TABLES` wide tables, one write short of its scheduled checkpoint. The next
 * write is acknowledged first and checkpoints after, as its own queued step.
 */
async function storeBeforeCheckpoint(shim: MemoryOpfs, name: string): Promise<OpfsBlockStore> {
  const store = await OpfsBlockStore.open({
    name,
    root: shim.root,
    checkpointEntries: TABLES + 1,
  });
  await waitFor(
    () => (shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength ?? -1) === 0,
    "the initial checkpoint",
  );
  for (let index = 0; index < TABLES; index += 1) await store.addTable(wideTable(index));
  return store;
}

async function expectTables(shim: MemoryOpfs, name: string, count: number): Promise<void> {
  const reopened = await OpfsBlockStore.open({ name, root: shim.root });
  try {
    expect((await reopened.listTables()).length).toBe(count);
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    await reopened.addTable(wideTable(count));
  } finally {
    reopened._crashForTests();
  }
  const again = await OpfsBlockStore.open({ name, root: shim.root });
  try {
    expect((await again.listTables()).length).toBe(count + 1);
  } finally {
    again.close();
  }
}

describe("sliced OPFS checkpoints", () => {
  it("span several event-loop turns and publish the synchronous encoding", async () => {
    const shim = new MemoryOpfs();
    const name = "sliced";
    const prefix = `minnowdb/${name}/`;
    const store = await storeBeforeCheckpoint(shim, name);
    let turns = 0;
    let ticking = true;
    const tick = (): void => {
      if (!ticking) return;
      turns += 1;
      setImmediate(tick);
    };
    setImmediate(tick);
    const turnsAtSlotWrites: number[] = [];
    shim.setWriteFault((path, phase) => {
      if (path.startsWith(`${prefix}checkpoint-`) && phase === "write") {
        turnsAtSlotWrites.push(turns);
      }
    });
    await store.addTable(wideTable(TABLES));
    await waitFor(() => shim.readFileBytes(`${prefix}wal`)?.byteLength === 0, "the checkpoint");
    ticking = false;
    shim.setWriteFault(null);

    const slot = shim.readFileBytes(`${prefix}checkpoint-a`) ?? new Uint8Array();
    expect(slot.byteLength).toBeGreaterThan(2 * 1024 * 1024);
    expect(shim.readFileBytes(`${prefix}checkpoint-b`)).toEqual(slot);
    // Each slot is truncated, then written a megabyte at a time.
    expect(turnsAtSlotWrites.length).toBeGreaterThanOrEqual(2 * 4);
    // The event loop ran between the slot's pieces, not only after the checkpoint.
    expect(new Set(turnsAtSlotWrites).size).toBeGreaterThanOrEqual(4);
    expect(Buffer.from(encodeSyncCheckpoint(decodeSyncCheckpoint(slot))).equals(slot)).toBe(true);
    store.close();
    await expectTables(shim, name, TABLES + 1);
  });

  it("survives power loss at every file operation of a multi-slice checkpoint", async () => {
    const operations = await countCheckpointOperations();
    expect(operations).toBeGreaterThanOrEqual(10);
    for (let boundary = 1; boundary <= operations; boundary += 1) {
      const shim = new MemoryOpfs();
      const name = `power-loss-${String(boundary)}`;
      let armed = false;
      let seen = 0;
      let stopped = false;
      const model = new PowerLossModel(shim, (path) => {
        if (!armed || stopped || !path.startsWith(`minnowdb/${name}/`)) return;
        if (seen === 0 && !path.includes("/checkpoint-")) return;
        seen += 1;
        if (seen === boundary) {
          stopped = true;
          throw new Error(`power lost before checkpoint operation ${String(boundary)}`);
        }
      });
      const store = await storeBeforeCheckpoint(shim, name);
      armed = true;
      await store.addTable(wideTable(TABLES));
      await waitFor(
        () => stopped || shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength === 0,
        `operation ${String(boundary)}`,
      );
      armed = false;
      store._crashForTests();
      model.powerLoss();
      shim.setWriteFault(null);
      // Strict durability acknowledged every table before its checkpoint began.
      await expectTables(shim, name, TABLES + 1);
    }
  });

  it("retries a checkpoint refused at any file operation on later writes", async () => {
    const operations = await countCheckpointOperations();
    for (let boundary = 1; boundary <= operations; boundary += 1) {
      const shim = new MemoryOpfs();
      const name = `refused-${String(boundary)}`;
      const prefix = `minnowdb/${name}/`;
      const store = await storeBeforeCheckpoint(shim, name);
      let seen = 0;
      let refused = false;
      shim.setWriteFault((path) => {
        if (refused || !path.startsWith(prefix)) return;
        if (seen === 0 && !path.includes("/checkpoint-")) return;
        seen += 1;
        if (seen === boundary) {
          refused = true;
          throw new DOMException("injected checkpoint refusal", "QuotaExceededError");
        }
      });
      await store.addTable(wideTable(TABLES));
      await waitFor(
        () => refused || shim.readFileBytes(`${prefix}wal`)?.byteLength === 0,
        `refusal ${String(boundary)}`,
      );
      shim.setWriteFault(null);
      // A refused checkpoint leaves the WAL covering everything and retries once enough
      // further entries arrive; each retry runs after the write that scheduled it.
      let retried = false;
      for (let extra = 1; extra <= 8 && !retried; extra += 1) {
        await store.addTable(wideTable(TABLES + extra));
        for (let wait = 0; wait < 40 && !retried; wait += 1) {
          retried = shim.readFileBytes(`${prefix}wal`)?.byteLength === 0;
          await new Promise((resolve) => setTimeout(resolve, 5));
        }
      }
      expect(retried, `retry after refusal ${String(boundary)}`).toBe(true);
      const count = (await store.listTables()).length;
      store._crashForTests();
      await expectTables(shim, name, count);
    }
  });
});

/**
 * File operations (writes, truncates, flushes) the scheduled checkpoint performs, from its
 * first slot truncate on.
 */
async function countCheckpointOperations(): Promise<number> {
  const shim = new MemoryOpfs();
  const name = "count";
  const store = await storeBeforeCheckpoint(shim, name);
  let operations = 0;
  shim.setWriteFault((path) => {
    if (!path.startsWith(`minnowdb/${name}/`)) return;
    if (operations === 0 && !path.includes("/checkpoint-")) return;
    operations += 1;
  });
  await store.addTable(wideTable(TABLES));
  await waitFor(() => shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength === 0, "count");
  shim.setWriteFault(null);
  store.close();
  return operations;
}
