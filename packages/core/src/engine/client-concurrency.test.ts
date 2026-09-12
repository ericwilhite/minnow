/** Admission, ordering, and close/reopen hygiene of the worker client. */
import { expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/memory.js";
import { MinnowDatabaseClient } from "./client.js";
import { MinnowDatabase } from "./database.js";
import { attachDatabaseWorker, exposeDatabase } from "./worker-host.js";
import { MAX_DATABASE_RPC_IN_FLIGHT } from "../worker-protocol/index.js";
import { createBoundary, settled } from "./client-audit-harness.js";

it("reports visibility changes again after close() and reopen()", async () => {
  const listeners = new Map<string, () => void>();
  const fakeDocument = {
    visibilityState: "visible",
    addEventListener: (type: string, listener: () => void) => listeners.set(type, listener),
    removeEventListener: (type: string) => listeners.delete(type),
  };
  vi.stubGlobal("document", fakeDocument);
  try {
    const first = createBoundary();
    const second = createBoundary();
    const onVisibility = vi.fn();
    exposeDatabase(new MinnowDatabase(new MemoryBlockStore()), first.workerSide, { onVisibility });
    exposeDatabase(new MinnowDatabase(new MemoryBlockStore()), second.workerSide, {
      onVisibility,
    });
    const client = new MinnowDatabaseClient(first.clientSide);
    await client.ready();
    await settled(10);
    expect(onVisibility).toHaveBeenLastCalledWith(true);
    await client.close();
    expect(listeners.has("visibilitychange")).toBe(false);
    await client.reopen(second.clientSide);
    await settled(10);
    onVisibility.mockClear();
    fakeDocument.visibilityState = "hidden";
    expect(listeners.has("visibilitychange")).toBe(true);
    listeners.get("visibilitychange")?.();
    await settled(10);
    expect(onVisibility).toHaveBeenCalledWith(false);
    await client.close();
    expect(listeners.has("visibilitychange")).toBe(false);
  } finally {
    vi.unstubAllGlobals();
  }
});

it("refuses scope calls beyond the queue cap and keeps the earlier ones ordered", async () => {
  const boundary = createBoundary();
  attachDatabaseWorker(boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide, { store: { kind: "memory" } });
  await client.createTable({
    name: "t",
    columns: [{ name: "id", type: "number" }],
    uniqueKey: "id",
  });
  const { result } = await client.write(async (s) => {
    const queued = Array.from({ length: MAX_DATABASE_RPC_IN_FLIGHT }, (_, i) =>
      s.execute(`INSERT INTO t (id) VALUES (${String(i)})`),
    );
    const overflow = await s.execute("INSERT INTO t (id) VALUES (9999)").catch((e: unknown) => e);
    await Promise.all(queued);
    const { rows } = await s.query("SELECT COUNT(*) AS n FROM t");
    return { overflow: overflow instanceof RangeError, n: rows[0]?.n };
  });
  expect(result).toEqual({ overflow: true, n: MAX_DATABASE_RPC_IN_FLIGHT });
  await client.close();
});

it("answers pending mutations before close() disposes the worker", async () => {
  const boundary = createBoundary();
  attachDatabaseWorker(boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide, { store: { kind: "memory" } });
  await client.createTable({
    name: "t",
    columns: [{ name: "id", type: "number" }],
    uniqueKey: "id",
  });
  const inserts = Array.from({ length: 20 }, (_, i) => client.insert("t", { id: i }));
  const closed = client.close();
  const outcomes = await Promise.allSettled(inserts);
  await closed;
  expect(outcomes.every((o) => o.status === "fulfilled")).toBe(true);
});
