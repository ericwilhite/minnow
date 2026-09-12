import { afterEach, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/memory.js";
import { MinnowDatabaseClient, type ClientTransport } from "./client.js";
import { MinnowDatabase } from "./database.js";
import { classifyError, DatabaseWorkerFailedError, DatabaseWorkerTimeoutError } from "./errors.js";
import { ConnectionLostError } from "../storage/types.js";
import { attachDatabaseWorker, type RpcScope } from "./worker-host.js";
import { attachWorkerHost, type WorkerStoreOptions } from "./worker-server.js";

/**
 * An in-process worker boundary whose worker side also has the global error events a real
 * DedicatedWorkerGlobalScope fires, so the host's listeners can be driven from the test.
 */
function createScopedBoundary(): {
  clientSide: ClientTransport;
  workerSide: RpcScope;
  emit(type: "error" | "unhandledrejection" | "messageerror", event: Record<string, unknown>): void;
  /** Fires the client-side Worker events (`error`, `messageerror`). */
  emitTransport(type: "error" | "messageerror", event: Record<string, unknown>): void;
} {
  const clientListeners: Array<(event: MessageEvent<unknown>) => void> = [];
  const clientTransportListeners = new Map<string, Array<(event: unknown) => void>>();
  const workerListeners = new Map<string, Array<(event: unknown) => void>>();
  let chain = Promise.resolve();
  const deliver = (
    listeners: Array<(event: MessageEvent<unknown>) => void>,
    message: unknown,
    transfer?: ArrayBuffer[],
  ): void => {
    const data = structuredClone(message, transfer === undefined ? undefined : { transfer });
    chain = chain.then(() => {
      for (const listener of listeners) listener({ data } as MessageEvent<unknown>);
    });
  };
  return {
    clientSide: {
      postMessage: (message, options) => {
        deliver(
          workerListeners.get("message") as Array<(event: MessageEvent<unknown>) => void>,
          message,
          (options as { transfer?: ArrayBuffer[] } | undefined)?.transfer,
        );
      },
      addEventListener: (type: string, listener: (event: MessageEvent<unknown>) => void) => {
        if (type === "message") {
          clientListeners.push(listener);
          return;
        }
        const list = clientTransportListeners.get(type) ?? [];
        list.push(listener as (event: unknown) => void);
        clientTransportListeners.set(type, list);
      },
      removeEventListener: (type: string, listener: (event: MessageEvent<unknown>) => void) => {
        if (type === "message") {
          const index = clientListeners.indexOf(listener);
          if (index >= 0) clientListeners.splice(index, 1);
          return;
        }
        const list = clientTransportListeners.get(type) ?? [];
        const index = list.indexOf(listener as (event: unknown) => void);
        if (index >= 0) list.splice(index, 1);
      },
    },
    workerSide: {
      postMessage: (message, options) => {
        deliver(
          clientListeners,
          message,
          (options as { transfer?: ArrayBuffer[] } | undefined)?.transfer,
        );
      },
      addEventListener: (type: string, listener: (event: never) => void) => {
        const list = workerListeners.get(type) ?? [];
        list.push(listener as (event: unknown) => void);
        workerListeners.set(type, list);
      },
    },
    emit: (type, event) => {
      for (const listener of workerListeners.get(type) ?? []) listener(event);
    },
    emitTransport: (type, event) => {
      for (const listener of clientTransportListeners.get(type) ?? []) listener(event);
    },
  };
}

function settled(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 20));
}

afterEach(() => {
  vi.restoreAllMocks();
});

it("delivers an unhandled rejection inside the worker to onWorkerError, typed and with context", async () => {
  const boundary = createScopedBoundary();
  attachDatabaseWorker(boundary.workerSide);
  const reports: Array<{ kind: string; context: string; error: Error }> = [];
  const client = new MinnowDatabaseClient(boundary.clientSide, {
    store: { kind: "memory" },
    onWorkerError: (event) => reports.push(event),
  });
  await client.ready();

  const rejection = { reason: new TypeError("nobody caught me"), preventDefault: vi.fn() };
  boundary.emit("unhandledrejection", rejection);
  await settled();
  expect(rejection.preventDefault).toHaveBeenCalled();
  expect(reports).toHaveLength(1);
  expect(reports[0]).toMatchObject({ kind: "unhandled-rejection", context: "worker global scope" });
  expect(reports[0]?.error).toBeInstanceOf(TypeError);
  expect(reports[0]?.error.message).toBe("nobody caught me");

  // An uncaught script error carries where it happened.
  boundary.emit("error", {
    message: "boom",
    filename: "db-worker.js",
    lineno: 12,
    colno: 3,
    error: new RangeError("boom"),
    preventDefault: vi.fn(),
  });
  await settled();
  expect(reports[1]).toMatchObject({ kind: "uncaught", context: "db-worker.js:12:3" });
  expect(reports[1]?.error).toBeInstanceOf(RangeError);

  // The connection is still perfectly usable: nothing about a background failure is fatal.
  await expect(client.listTables()).resolves.toEqual([]);
  await client.close();
});

it("writes worker diagnostics to console.error when nobody listens", async () => {
  const boundary = createScopedBoundary();
  attachDatabaseWorker(boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide, { store: { kind: "memory" } });
  await client.ready();
  const logged = vi.spyOn(console, "error").mockImplementation(() => undefined);
  boundary.emit("unhandledrejection", { reason: new Error("quiet failure") });
  await settled();
  expect(logged).toHaveBeenCalledTimes(1);
  expect(logged.mock.calls[0]?.[0]).toContain("worker unhandled-rejection");
  expect(logged.mock.calls[0]?.[1]).toBeInstanceOf(Error);
  expect((logged.mock.calls[0]?.[1] as Error).message).toBe("quiet failure");
  await client.close();
});

it("keeps the Worker error event's message, location, and cause on the failure", async () => {
  const listeners = new Map<string, (event?: unknown) => void>();
  let messageListener: ((event: MessageEvent<unknown>) => void) | undefined;
  const transport: ClientTransport = {
    postMessage: (message) => {
      const request = message as { kind: string; requestId: string };
      if (request.kind !== "rpc-init") return;
      queueMicrotask(() => {
        messageListener?.({
          data: { version: 7, requestId: request.requestId, kind: "rpc-result", result: {} },
        } as MessageEvent<unknown>);
      });
    },
    addEventListener: (type, listener) => {
      if (type === "message") messageListener = listener;
      else listeners.set(type, listener as (event?: unknown) => void);
    },
  };
  const reports: Array<{ kind: string; error: Error }> = [];
  const client = new MinnowDatabaseClient(transport, {
    store: { kind: "memory" },
    onWorkerError: (event) => reports.push(event),
  });
  await client.ready();
  const pending = client.listTables();
  const thrown = new Error("worker exploded");
  listeners.get("error")?.({
    message: "Uncaught Error: worker exploded",
    filename: "https://app.example/db-worker.js",
    lineno: 44,
    colno: 9,
    error: thrown,
  });
  const failure = await pending.catch((error: unknown) => error);
  expect(failure).toBeInstanceOf(DatabaseWorkerFailedError);
  expect(failure).toMatchObject({ reason: "error", cause: thrown });
  expect((failure as Error).message).toContain("Uncaught Error: worker exploded");
  expect((failure as Error).message).toContain("db-worker.js:44:9");
  expect(reports).toHaveLength(1);
  expect(reports[0]).toMatchObject({ kind: "transport" });
  // The failure is fatal for the channel, as before, and every later call says why.
  await expect(client.listTables()).rejects.toBeInstanceOf(DatabaseWorkerFailedError);
});

it("forwards a store's diagnostics as coordination reports", async () => {
  const boundary = createScopedBoundary();
  let diagnostic: WorkerStoreOptions["onDiagnostic"];
  attachWorkerHost(boundary.workerSide, (_descriptor, options) => {
    diagnostic = options.onDiagnostic;
    return new MemoryBlockStore();
  });
  const reports: Array<{ kind: string; context: string; error: Error }> = [];
  const client = new MinnowDatabaseClient(boundary.clientSide, {
    store: { kind: "opfs", name: "any" },
    onWorkerError: (event) => reports.push(event),
  });
  await client.ready();
  expect(diagnostic).toBeDefined();
  diagnostic?.(new Error("checkpoint slot refused"), "opfs checkpoint");
  await settled();
  expect(reports).toHaveLength(1);
  expect(reports[0]).toMatchObject({ kind: "coordination", context: "opfs checkpoint" });
  expect(reports[0]?.error.message).toBe("checkpoint slot refused");
  await client.close();
});

it("reports a buffered writer's failed background flush when no onError is given", async () => {
  const background: Array<{ error: unknown; context: string }> = [];
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    onBackgroundError: (error, context) => background.push({ error, context }),
  });
  const writer = database.bufferedWriter("missing", { maxAgeMs: 5 });
  await writer.add({ id: 1 });
  await new Promise((resolve) => setTimeout(resolve, 40));
  expect(background).toHaveLength(1);
  expect(background[0]?.context).toBe("buffered writer flush for missing");
  expect(background[0]?.error).toBeInstanceOf(Error);
  await writer.close().catch(() => undefined);
  await database.close();
});

it("keeps a typed init failure as the cause of calls pipelined behind it", async () => {
  const boundary = createScopedBoundary();
  attachDatabaseWorker(boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide, {
    store: { kind: "indexeddb", name: "nowhere" },
  });
  await expect(client.ready()).rejects.toThrow("IndexedDB is unavailable");
  const failure = await client.listTables().catch((error: unknown) => error as Error);
  expect(failure).toBeInstanceOf(Error);
  expect((failure as Error).cause).toBeInstanceOf(Error);
  expect(((failure as Error).cause as Error).message).toContain("IndexedDB is unavailable");
});

function keepaliveTransport(): {
  transport: ClientTransport;
  keepalive: (requestId: string) => void;
  lastRequestId: () => string;
} {
  let messageListener: ((event: MessageEvent<unknown>) => void) | undefined;
  let last = "";
  const deliver = (data: unknown): void => {
    messageListener?.({ data } as MessageEvent<unknown>);
  };
  return {
    transport: {
      postMessage: (message) => {
        const request = message as { kind: string; requestId: string };
        last = request.requestId;
        if (request.kind === "rpc-init") {
          queueMicrotask(() =>
            deliver({ version: 7, requestId: request.requestId, kind: "rpc-result", result: {} }),
          );
        }
      },
      addEventListener: (type, listener) => {
        if (type === "message") messageListener = listener;
      },
    },
    keepalive: (requestId) =>
      deliver({
        version: 7,
        requestId,
        kind: "rpc-event",
        handleId: "$worker",
        event: "keepalive",
        payload: null,
      }),
    lastRequestId: () => last,
  };
}

it("counts silence, not wall time: a call the worker keeps reporting on outlives its deadline", async () => {
  vi.useFakeTimers();
  try {
    const { transport, keepalive, lastRequestId } = keepaliveTransport();
    const client = new MinnowDatabaseClient(transport, {
      store: { kind: "memory" },
      requestTimeoutMs: 100,
    });
    await client.ready();
    const pending = client.listTables();
    const requestId = lastRequestId();
    let settled = false;
    pending.then(
      () => (settled = true),
      () => (settled = true),
    );
    for (let tick = 0; tick < 5; tick += 1) {
      await vi.advanceTimersByTimeAsync(80);
      keepalive(requestId);
    }
    // 400 ms of a 100 ms deadline, and the call is still alive.
    expect(settled).toBe(false);
    // Silence, and it ends the way it always did.
    await vi.advanceTimersByTimeAsync(101);
    await expect(pending).rejects.toBeInstanceOf(DatabaseWorkerTimeoutError);
  } finally {
    vi.useRealTimers();
  }
});

it("still ends a call whose worker reports forever, at ten deadlines", async () => {
  vi.useFakeTimers();
  try {
    const { transport, keepalive, lastRequestId } = keepaliveTransport();
    const client = new MinnowDatabaseClient(transport, {
      store: { kind: "memory" },
      requestTimeoutMs: 100,
    });
    await client.ready();
    const pending = client.listTables();
    const requestId = lastRequestId();
    const failure = pending.catch((error: unknown) => error);
    let ticks = 0;
    for (; ticks < 40; ticks += 1) {
      await vi.advanceTimersByTimeAsync(50);
      keepalive(requestId);
    }
    expect(await failure).toBeInstanceOf(DatabaseWorkerTimeoutError);
    expect(ticks * 50).toBeLessThanOrEqual(100 * 10 * 2);
  } finally {
    vi.useRealTimers();
  }
});

it("reports a lost connection once, and reopen() puts a fresh worker behind the same client", async () => {
  const first = createScopedBoundary();
  attachDatabaseWorker(first.workerSide);
  const lost: Error[] = [];
  const client = new MinnowDatabaseClient(first.clientSide, {
    store: { kind: "memory" },
    onWorkerError: () => undefined,
    onConnectionLost: (error) => lost.push(error),
  });
  await client.ready();
  await client.createTable({ name: "items", columns: [{ name: "id", type: "number" }] });

  // A transport error is fatal for this connection.
  first.emitTransport("error", { message: "worker died" });
  expect(lost).toHaveLength(1);
  expect(lost[0]).toBeInstanceOf(DatabaseWorkerFailedError);
  expect(classifyError(lost[0]).kind).toBe("connection-lost");
  await expect(client.listTables()).rejects.toBeInstanceOf(ConnectionLostError);
  // A second failure does not re-fire the hook.
  first.emitTransport("error", { message: "again" });
  expect(lost).toHaveLength(1);

  // A fresh worker, same store descriptor: the client is usable again.
  const second = createScopedBoundary();
  attachDatabaseWorker(second.workerSide);
  await client.reopen(second.clientSide);
  await expect(client.listTables()).resolves.toEqual([]);
  await client.close();
});

it("reopens from a transport factory without an argument", async () => {
  const boundaries: Array<ReturnType<typeof createScopedBoundary>> = [];
  const factory = (): ClientTransport => {
    const boundary = createScopedBoundary();
    attachDatabaseWorker(boundary.workerSide);
    boundaries.push(boundary);
    return boundary.clientSide;
  };
  const client = new MinnowDatabaseClient(factory, { store: { kind: "memory" } });
  await client.ready();
  expect(boundaries).toHaveLength(1);
  const pending = client.listTables();
  await client.reopen();
  // The call in flight when the client was reopened fails as a lost connection.
  await expect(pending).rejects.toBeInstanceOf(ConnectionLostError);
  expect(boundaries).toHaveLength(2);
  await expect(client.listTables()).resolves.toEqual([]);
  await client.close();
});

it("asks a buffered writer to flush when the page is hidden or unloading", async () => {
  const documentListeners = new Map<string, () => void>();
  const windowListeners = new Map<string, () => void>();
  const fakeDocument = {
    visibilityState: "visible",
    addEventListener: (type: string, listener: () => void) => documentListeners.set(type, listener),
    removeEventListener: (type: string) => documentListeners.delete(type),
  };
  const fakeWindow = {
    addEventListener: (type: string, listener: () => void) => windowListeners.set(type, listener),
    removeEventListener: (type: string) => windowListeners.delete(type),
  };
  const globals = globalThis as { document?: unknown; window?: unknown };
  const previous = { document: globals.document, window: globals.window };
  globals.document = fakeDocument;
  globals.window = fakeWindow;
  try {
    const boundary = createScopedBoundary();
    attachDatabaseWorker(boundary.workerSide);
    const client = new MinnowDatabaseClient(boundary.clientSide, { store: { kind: "memory" } });
    await client.ready();
    await client.createTable({
      name: "items",
      columns: [
        { name: "id", type: "number" },
        { name: "value", type: "string" },
      ],
    });
    const writer = client.bufferedWriter("items", { maxAgeMs: 60_000, maxRows: 1_000 });
    await writer.add({ id: 1, value: "hidden" });
    expect((await client.query('SELECT count(*) AS n FROM "items"')).rows[0]?.n).toBe(0);
    fakeDocument.visibilityState = "hidden";
    documentListeners.get("visibilitychange")?.();
    await settled();
    expect((await client.query('SELECT count(*) AS n FROM "items"')).rows[0]?.n).toBe(1);
    await writer.add({ id: 2, value: "unload" });
    windowListeners.get("pagehide")?.();
    await settled();
    expect((await client.query('SELECT count(*) AS n FROM "items"')).rows[0]?.n).toBe(2);
    await writer.close();
    expect(documentListeners.has("visibilitychange")).toBe(false);
    expect(windowListeners.has("pagehide")).toBe(false);
    await client.close();
  } finally {
    globals.document = previous.document;
    globals.window = previous.window;
  }
});
