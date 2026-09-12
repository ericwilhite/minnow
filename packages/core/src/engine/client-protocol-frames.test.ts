/** Protocol version mismatch and unreadable frames on both sides of the worker boundary. */
import { expect, it, vi } from "vitest";
import { MinnowDatabaseClient } from "./client.js";
import { classifyError, DatabaseWorkerFailedError } from "./errors.js";
import { attachDatabaseWorker } from "./worker-host.js";
import { protocolVersion } from "../worker-protocol/index.js";
import { createBoundary } from "./client-audit-harness.js";

it("fails the client as a lost connection on an unreadable frame that names no request", async () => {
  const boundary = createBoundary();
  attachDatabaseWorker(boundary.workerSide);
  const onConnectionLost = vi.fn();
  const client = new MinnowDatabaseClient(boundary.clientSide, {
    store: { kind: "memory" },
    onConnectionLost,
  });
  await client.ready();
  // A worker from another release posting an event frame (a live change, a diagnostic).
  boundary.injectToClient({
    version: protocolVersion - 1,
    requestId: null,
    kind: "rpc-event",
    handleId: "x",
    event: "change",
    payload: null,
  });
  const error = await client.listTables().catch((e: unknown) => e);
  expect(error).toBeInstanceOf(DatabaseWorkerFailedError);
  expect(error).toMatchObject({ reason: "messageerror" });
  expect((error as Error).message).toMatch(/cannot read/);
  expect((error as Error).cause).toBeInstanceOf(Error);
  // Every later call fails the same way: the client is dead.
  const again = await client.listTables().catch((e: unknown) => e);
  expect(again).toBe(error);
  expect(onConnectionLost).toHaveBeenCalledTimes(1);
  expect(onConnectionLost).toHaveBeenCalledWith(error);
  const classified = classifyError(error);
  expect(classified.kind).toBe("connection-lost");
  expect(classified.connectionUsable).toBe(false);
});

it("fails only the named call on a mismatched response and keeps the client alive", async () => {
  const boundary = createBoundary();
  attachDatabaseWorker(boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide, { store: { kind: "memory" } });
  await client.ready();
  const pending = client.listTables();
  const requestId = (boundary.sentByClient.at(-1) as { requestId: string }).requestId;
  boundary.injectToClient({
    version: protocolVersion - 1,
    requestId,
    kind: "rpc-result",
    result: [],
  });
  const error = await pending.catch((e: unknown) => e);
  expect((error as Error).message).toMatch(/Unsupported protocol version/);
  await expect(client.listTables()).resolves.toEqual([]);
  await client.close();
});

it("refuses an old-version request in the worker per request id", async () => {
  const boundary = createBoundary();
  attachDatabaseWorker(boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide, { store: { kind: "memory" } });
  await client.ready();
  boundary.injectToWorker({
    version: protocolVersion - 1,
    requestId: "old-1",
    kind: "rpc-call",
    handleId: null,
    method: "listTables",
    args: [],
  });
  await boundary.flush();
  const reply = boundary.sentByWorker.find(
    (f) => (f as { requestId?: string }).requestId === "old-1",
  ) as { kind: string; error?: { message: string } } | undefined;
  expect(reply?.kind).toBe("rpc-failure");
  expect(reply?.error?.message).toMatch(/Unsupported protocol version/);
  await client.close();
});
