import {
  MemoryBlockStore,
  MAX_ACTIVE_LEASES,
  OpfsUncertainOutcomeError,
  SnapshotImportConflictError,
  StorageCorruptionError,
  StorageResourceLimitError,
  TableInUseError,
  type TableRecord,
} from "../storage/index.js";
import * as storageTypes from "../storage/types.js";
import {
  MAX_DATABASE_RPC_IN_FLIGHT,
  parseRpcResponse,
  protocolVersion,
  rpcFailure,
  type RpcResponse,
} from "../worker-protocol/index.js";
import { describe, expect, it, vi } from "vitest";
import { MinnowDatabaseClient, type ClientTransport } from "./client.js";
import { MinnowDatabase } from "./database.js";
import {
  MaintenanceBacklogError,
  SqlCompileError,
  UniqueConstraintError,
  VisibleSegmentCursorStaleError,
} from "./errors.js";
import { QueryMemoryBudgetError } from "./memory.js";
import { compileQuery, compileStatement, type QueryResult } from "./query.js";
import { column, schema, table, typedTable } from "./schema.js";
import {
  attachDatabaseWorker,
  exposeDatabase,
  MAX_WORKER_HANDLES_PER_CONNECTION,
  type ExposeDatabaseOptions,
  type RpcScope,
} from "./worker-host.js";

/**
 * An in-process stand-in for the worker boundary: two endpoints whose messages are
 * structured-cloned and delivered asynchronously in order, exactly like postMessage.
 */
function createBoundary(): {
  clientSide: ClientTransport;
  workerSide: RpcScope;
  clientListenerCount(): number;
} {
  const clientListeners: Array<(event: MessageEvent<unknown>) => void> = [];
  const workerListeners: Array<(event: MessageEvent<unknown>) => void> = [];
  let chain = Promise.resolve();
  const deliver = (
    listeners: Array<(event: MessageEvent<unknown>) => void>,
    message: unknown,
  ): void => {
    const data = structuredClone(message);
    chain = chain.then(() => {
      for (const listener of listeners) listener({ data } as MessageEvent<unknown>);
    });
  };
  return {
    clientSide: {
      postMessage: (message) => {
        deliver(workerListeners, message);
      },
      addEventListener: (type: string, listener: (event: MessageEvent<unknown>) => void) => {
        if (type === "message") clientListeners.push(listener);
      },
      removeEventListener: (type: string, listener: (event: MessageEvent<unknown>) => void) => {
        if (type !== "message") return;
        const index = clientListeners.indexOf(listener);
        if (index >= 0) clientListeners.splice(index, 1);
      },
    },
    workerSide: {
      postMessage: (message) => {
        deliver(clientListeners, message);
      },
      addEventListener: (_type, listener) => {
        workerListeners.push(listener);
      },
    },
    clientListenerCount: () => clientListeners.length,
  };
}

function connect(): MinnowDatabaseClient {
  const { clientSide, workerSide } = createBoundary();
  attachDatabaseWorker(workerSide);
  return new MinnowDatabaseClient(clientSide, { store: { kind: "memory" } });
}

function exposeRaw(
  database: MinnowDatabase,
  options: ExposeDatabaseOptions = {},
): {
  call(handleId: string | null, method: string, args?: unknown[]): Promise<RpcResponse>;
  post(message: unknown): void;
  response(requestId: string): Promise<RpcResponse>;
} {
  const { clientSide, workerSide } = createBoundary();
  exposeDatabase(database, workerSide, options);
  const pending = new Map<string, (response: RpcResponse) => void>();
  clientSide.addEventListener("message", (event) => {
    const parsed = parseRpcResponse(event.data);
    if (parsed?.requestId !== null && parsed?.requestId !== undefined) {
      pending.get(parsed.requestId)?.(parsed);
    }
  });
  let nextRequestId = 0;
  const response = (requestId: string): Promise<RpcResponse> =>
    new Promise((resolve) => pending.set(requestId, resolve));
  return {
    call: (handleId, method, args = []) => {
      const requestId = `exposed-${String((nextRequestId += 1))}`;
      const result = response(requestId);
      clientSide.postMessage({
        version: protocolVersion,
        requestId,
        kind: "rpc-call",
        handleId,
        method,
        args,
      });
      return result;
    },
    post: (message) => clientSide.postMessage(message),
    response,
  };
}

type StorageErrorConstructor = new (...args: never[]) => Error;

function exportedStorageErrorConstructors(): Array<readonly [string, StorageErrorConstructor]> {
  const constructors: Array<readonly [string, StorageErrorConstructor]> = [];
  for (const [name, value] of Object.entries(storageTypes)) {
    if (typeof value === "function" && value.prototype instanceof Error) {
      constructors.push([name, value as StorageErrorConstructor]);
    }
  }
  return constructors.sort(([left], [right]) => left.localeCompare(right));
}

function createStorageErrorFixture(constructor: StorageErrorConstructor): Error {
  // Supplying the longest current scalar signature exercises every constructor's enumerable
  // fields; extra arguments are ignored by shorter JavaScript constructors.
  return new (constructor as unknown as new (...args: unknown[]) => Error)(
    "boundary-fixture",
    17,
    23,
    29,
    "newer",
    false,
  );
}

async function createPeopleTable(client: MinnowDatabaseClient): Promise<void> {
  await client.createTable({
    name: "people",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "joined", type: "datetime" },
    ],
  });
}

class DiagnosticMemoryBlockStore extends MemoryBlockStore {
  async checkIntegrity(): Promise<never> {
    throw new StorageCorruptionError("diagnostic", "records/7", "checksum mismatch");
  }

  override async getStorageStats() {
    return {
      backend: "diagnostic",
      logicalBytes: 7,
      physicalBytes: 11,
      liveBlockCount: 1,
      obsoleteBlockCount: 1,
      liveBlockBytes: 5,
      obsoleteBlockBytes: 2,
      temporaryBytes: 0,
      walBytes: null,
      checkpointBytes: null,
      orphanBytes: 4,
      manifestCount: 1,
      transactionCount: 2,
      segmentCount: 3,
    };
  }

  override async inspectInterruptedImport() {
    return {
      identity: "import-7",
      version: 4,
      createdAt: "2026-08-24T00:00:00.000Z",
      stagedBlockCount: 2,
      stagedBytes: 19,
    };
  }

  override async abortInterruptedImport(identity: string) {
    return { identity, removedBlockCount: 2, removedBytes: 19 };
  }
}

class UncertainOutcomeMemoryBlockStore extends MemoryBlockStore {
  override async addTable(record: TableRecord): Promise<never> {
    void record;
    throw new OpfsUncertainOutcomeError("addTable");
  }
}

class TableInUseMemoryBlockStore extends MemoryBlockStore {
  override async dropTable(input: Parameters<MemoryBlockStore["dropTable"]>[0]): Promise<never> {
    throw new TableInUseError(input.tableId, "transaction", "txn-still-writing");
  }
}

class ClientMaintenanceFaultStore extends MemoryBlockStore {
  failCollection = false;

  override async listGarbageCollectionJobPage(afterId: string | null, limit: number) {
    if (this.failCollection) throw new Error("diagnostic collection failure");
    return super.listGarbageCollectionJobPage(afterId, limit);
  }
}

class PausedWorkerStageStore extends MemoryBlockStore {
  pauseStages = false;
  readonly stageStarted: Promise<void>;
  readonly #resumeStages: Promise<void>;
  #markStageStarted!: () => void;
  #resume!: () => void;

  constructor() {
    super();
    this.stageStarted = new Promise((resolve) => {
      this.#markStageStarted = resolve;
    });
    this.#resumeStages = new Promise((resolve) => {
      this.#resume = resolve;
    });
  }

  resumeStages(): void {
    this.#resume();
  }

  override async stageTransactionArtifacts(
    input: Parameters<MemoryBlockStore["stageTransactionArtifacts"]>[0],
  ) {
    if (this.pauseStages) {
      this.#markStageStarted();
      await this.#resumeStages;
    }
    return super.stageTransactionArtifacts(input);
  }
}

class PausedWorkerWriteTransactionStore extends MemoryBlockStore {
  readonly writeStarted: Promise<void>;
  readonly writeApplied: Promise<void>;
  readonly #resumeWrite: Promise<void>;
  #markWriteStarted!: () => void;
  #markWriteApplied!: () => void;
  #resume!: () => void;

  constructor(readonly pause: "before-apply" | "after-apply") {
    super();
    this.writeStarted = new Promise((resolve) => {
      this.#markWriteStarted = resolve;
    });
    this.writeApplied = new Promise((resolve) => {
      this.#markWriteApplied = resolve;
    });
    this.#resumeWrite = new Promise((resolve) => {
      this.#resume = resolve;
    });
  }

  resumeWrite(): void {
    this.#resume();
  }

  override async writeTransaction(input: Parameters<MemoryBlockStore["writeTransaction"]>[0]) {
    this.#markWriteStarted();
    if (this.pause === "before-apply") await this.#resumeWrite;
    const result = await super.writeTransaction(input);
    this.#markWriteApplied();
    if (this.pause === "after-apply") await this.#resumeWrite;
    return result;
  }
}

class PausedWorkerAbortStore extends MemoryBlockStore {
  readonly abortStarted: Promise<void>;
  readonly #resumeAbort: Promise<void>;
  #markAbortStarted!: () => void;
  #resume!: () => void;

  constructor() {
    super();
    this.abortStarted = new Promise((resolve) => {
      this.#markAbortStarted = resolve;
    });
    this.#resumeAbort = new Promise((resolve) => {
      this.#resume = resolve;
    });
  }

  resumeAbort(): void {
    this.#resume();
  }

  override async updateTransaction(
    id: string,
    expectedRevision: number,
    update: Parameters<MemoryBlockStore["updateTransaction"]>[2],
  ) {
    if (update.status === "aborted") {
      this.#markAbortStarted();
      await this.#resumeAbort;
    }
    return super.updateTransaction(id, expectedRevision, update);
  }
}

class PausedWorkerQueryStore extends MemoryBlockStore {
  readonly queryReadStarted: Promise<void>;
  readonly #resumeQueryRead: Promise<void>;
  #markQueryReadStarted!: () => void;
  #resume!: () => void;
  pauseQueryReads = false;

  constructor() {
    super();
    this.queryReadStarted = new Promise((resolve) => {
      this.#markQueryReadStarted = resolve;
    });
    this.#resumeQueryRead = new Promise((resolve) => {
      this.#resume = resolve;
    });
  }

  resumeQueryRead(): void {
    this.#resume();
  }

  override async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    if (this.pauseQueryReads) {
      this.#markQueryReadStarted();
      await this.#resumeQueryRead;
    }
    return super.getBlocks(ids);
  }
}

class PausedWorkerCreateLeaseStore extends MemoryBlockStore {
  readonly createStarted: Promise<void>;
  readonly createApplied: Promise<void>;
  readonly #resumeCreate: Promise<void>;
  #markCreateStarted!: () => void;
  #markCreateApplied!: () => void;
  #resume!: () => void;

  constructor(readonly pause: "before-apply" | "after-apply") {
    super();
    this.createStarted = new Promise((resolve) => {
      this.#markCreateStarted = resolve;
    });
    this.createApplied = new Promise((resolve) => {
      this.#markCreateApplied = resolve;
    });
    this.#resumeCreate = new Promise((resolve) => {
      this.#resume = resolve;
    });
  }

  resumeCreate(): void {
    this.#resume();
  }

  override async createLease(
    record: Parameters<MemoryBlockStore["createLease"]>[0],
  ): Promise<void> {
    this.#markCreateStarted();
    if (this.pause === "before-apply") await this.#resumeCreate;
    await super.createLease(record);
    this.#markCreateApplied();
    if (this.pause === "after-apply") await this.#resumeCreate;
  }
}

class PausedWorkerRemoveLeaseStore extends MemoryBlockStore {
  pauseRemovals = false;
  removeStartedCount = 0;
  readonly #resumeRemovals: Promise<void>;
  #resume!: () => void;

  constructor() {
    super();
    this.#resumeRemovals = new Promise((resolve) => {
      this.#resume = resolve;
    });
  }

  resumeRemovals(): void {
    this.#resume();
  }

  override async removeLease(
    input: Parameters<MemoryBlockStore["removeLease"]>[0],
  ): Promise<boolean> {
    if (this.pauseRemovals) {
      this.removeStartedCount += 1;
      await this.#resumeRemovals;
    }
    return super.removeLease(input);
  }
}

class LeaseCapacityWorkerStore extends MemoryBlockStore {
  refusedLeaseCreates = MAX_WORKER_HANDLES_PER_CONNECTION;

  override async createLease(
    record: Parameters<MemoryBlockStore["createLease"]>[0],
  ): Promise<void> {
    if (this.refusedLeaseCreates > 0) {
      this.refusedLeaseCreates -= 1;
      throw new StorageResourceLimitError("lease", MAX_ACTIVE_LEASES + 1, MAX_ACTIVE_LEASES);
    }
    return super.createLease(record);
  }
}

async function openTransactionId(store: MemoryBlockStore): Promise<string> {
  const page = await store.listTransactionPage(null, 256);
  const active = page.records.find((record) => record.status === "active");
  if (active === undefined) throw new Error("Expected one open transaction");
  return active.id;
}

async function waitForTransactionStatus(
  store: MemoryBlockStore,
  transactionId: string,
  status: "committed" | "aborted",
): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if ((await store.getTransaction(transactionId))?.status === status) return;
    await Promise.resolve();
  }
  throw new Error(`Transaction ${transactionId} did not become ${status}`);
}

async function createWorkerWriteTable(database: MinnowDatabase): Promise<void> {
  await database.createTable({
    name: "worker_writes",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "value", type: "string" },
    ],
  });
}

async function expectWorkerRpcPending(rpc: Promise<RpcResponse>): Promise<void> {
  let settled = false;
  void rpc.then(() => {
    settled = true;
  });
  await Promise.resolve();
  await Promise.resolve();
  expect(settled).toBe(false);
}

async function waitForLeaseRemovals(
  store: PausedWorkerRemoveLeaseStore,
  count: number,
): Promise<void> {
  for (let attempt = 0; attempt < 1_000; attempt += 1) {
    if (store.removeStartedCount >= count) return;
    await Promise.resolve();
  }
  throw new Error(`Expected ${String(count)} pending lease removals`);
}

async function expectNoActiveWorkerOwners(store: MemoryBlockStore): Promise<void> {
  const transactions = await store.listTransactionPage(null, 256);
  expect(transactions.records.filter((record) => record.status === "active")).toEqual([]);
  expect(await store.listLeases()).toEqual([]);
}

describe("MinnowDatabaseClient", () => {
  it("disposes every open worker handle and rejects malformed frames with their request IDs", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      autoCollect: false,
      autoCompact: false,
    });
    const raw = exposeRaw(database);
    expect(
      await raw.call(null, "createTable", [
        {
          name: "dispose_rows",
          uniqueKey: "id",
          columns: [
            { name: "id", type: "number" },
            { name: "value", type: "string" },
          ],
        },
      ]),
    ).toMatchObject({ kind: "rpc-result" });

    const write = await raw.call(null, "writeOpen");
    if (write.kind !== "rpc-result") throw new Error("writeOpen failed");
    const writeId = (write.result as { handleId: string }).handleId;
    expect(
      (await raw.call(writeId, "execute", ["INSERT INTO dispose_rows VALUES (1, 'one')"])).kind,
    ).toBe("rpc-result");
    expect(
      (await raw.call(writeId, "stage", ["upsertBatch", "dispose_rows", [{ id: 1, value: "ONE" }]]))
        .kind,
    ).toBe("rpc-result");

    const snapshot = await raw.call(null, "snapshotOpen");
    const cursor = await raw.call(null, "queryCursorOpen", [
      "dispose-cursor",
      "SELECT 1 AS value",
      {},
    ]);
    const exported = await raw.call(null, "exportSnapshotOpen");
    const imported = await raw.call(null, "importSnapshotOpen", ["dispose-import"]);
    const writer = await raw.call(null, "bufferedWriter", [
      "dispose-writer",
      "dispose_rows",
      { maxRows: 10 },
    ]);
    const live = await raw.call(null, "liveQueries", ["dispose-live", {}]);
    for (const opened of [snapshot, cursor, exported, imported, writer, live]) {
      expect(opened.kind).toBe("rpc-result");
    }
    expect(
      (await raw.call("dispose-import", "write", [new Uint8Array(1024 * 1024 + 1)])).kind,
    ).toBe("rpc-failure");
    expect((await raw.call("dispose-import", "write", ["not-bytes"])).kind).toBe("rpc-failure");
    expect((await raw.call("dispose-import", "unknown")).kind).toBe("rpc-failure");

    const malformed = raw.response("malformed-exposed");
    raw.post({
      version: protocolVersion + 1,
      requestId: "malformed-exposed",
      kind: "rpc-call",
      handleId: null,
      method: "listTables",
      args: [],
    });
    expect((await malformed).kind).toBe("rpc-failure");
    expect((await raw.call(null, "dispose")).kind).toBe("rpc-result");
  });

  it("fails writeOpen promptly at the durable lease ceiling without leaking reservations", async () => {
    const store = new LeaseCapacityWorkerStore();
    const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
    const raw = exposeRaw(database);
    for (let attempt = 0; attempt < MAX_WORKER_HANDLES_PER_CONNECTION; attempt += 1) {
      const refused = await raw.call(null, "writeOpen");
      expect(refused.kind).toBe("rpc-failure");
      if (refused.kind === "rpc-failure") {
        expect(refused.error.name).toBe("StorageResourceLimitError");
      }
    }
    // Every failed generated ID was released, so a healthy lease can still publish a handle.
    const opened = await raw.call(null, "writeOpen");
    expect(opened.kind).toBe("rpc-result");
    if (opened.kind !== "rpc-result") throw new Error("Expected write handle");
    const handleId = (opened.result as { handleId: string }).handleId;
    expect((await raw.call(handleId, "abort")).kind).toBe("rpc-result");
    expect((await raw.call(null, "dispose")).kind).toBe("rpc-result");
  });

  it("preserves snapshot lease-admission failures without leaking reservations", async () => {
    const store = new LeaseCapacityWorkerStore();
    const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
    const raw = exposeRaw(database);
    for (let attempt = 0; attempt < MAX_WORKER_HANDLES_PER_CONNECTION; attempt += 1) {
      const refused = await raw.call(null, "snapshotOpen");
      expect(refused.kind).toBe("rpc-failure");
      if (refused.kind === "rpc-failure") {
        expect(refused.error.name).toBe("StorageResourceLimitError");
      }
    }
    const opened = await raw.call(null, "snapshotOpen");
    expect(opened.kind).toBe("rpc-result");
    if (opened.kind !== "rpc-result") throw new Error("Expected snapshot handle");
    const handleId = (opened.result as { handleId: string }).handleId;
    expect((await raw.call(handleId, "close")).kind).toBe("rpc-result");
    expect((await raw.call(null, "dispose")).kind).toBe("rpc-result");
    expect(await store.listLeases()).toEqual([]);
  });

  for (const pause of ["before-apply", "after-apply"] as const) {
    it(`joins a write opener paused ${pause} before worker disposal`, async () => {
      const store = new PausedWorkerCreateLeaseStore(pause);
      const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
      const raw = exposeRaw(database);
      const opening = raw.call(null, "writeOpen");
      await store.createStarted;
      if (pause === "after-apply") await store.createApplied;
      const dispose = raw.call(null, "dispose");
      await expectWorkerRpcPending(opening);
      await expectWorkerRpcPending(dispose);

      store.resumeCreate();
      const openResult = await opening;
      expect(openResult.kind).toBe("rpc-failure");
      if (openResult.kind === "rpc-failure") {
        expect(openResult.error.message).toContain("disposed");
      }
      expect((await dispose).kind).toBe("rpc-result");
      await expectNoActiveWorkerOwners(store);
    });

    it(`joins a snapshot opener paused ${pause} before worker disposal`, async () => {
      const store = new PausedWorkerCreateLeaseStore(pause);
      const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
      const raw = exposeRaw(database);
      const opening = raw.call(null, "snapshotOpen");
      await store.createStarted;
      if (pause === "after-apply") await store.createApplied;
      const dispose = raw.call(null, "dispose");
      await expectWorkerRpcPending(opening);
      await expectWorkerRpcPending(dispose);

      store.resumeCreate();
      const openResult = await opening;
      expect(openResult.kind).toBe("rpc-failure");
      if (openResult.kind === "rpc-failure") {
        expect(openResult.error.message).toContain("disposed");
      }
      expect((await dispose).kind).toBe("rpc-result");
      await expectNoActiveWorkerOwners(store);
    });
  }

  for (const terminal of ["close", "dispose"] as const) {
    it(`joins snapshot lease release on ${terminal}`, async () => {
      const store = new PausedWorkerRemoveLeaseStore();
      const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
      if (terminal === "close") await createWorkerWriteTable(database);
      const raw = exposeRaw(database);
      const opened = await raw.call(null, "snapshotOpen");
      if (opened.kind !== "rpc-result") throw new Error("Expected snapshot handle");
      const handleId = (opened.result as { handleId: string }).handleId;
      if (terminal === "close") {
        // Retire the handle's shared lease by moving the database cache to a newer manifest.
        // Closing completes the callback and starts that release; disposal must join it.
        await database.insertBatch("worker_writes", {
          columns: { id: [1], value: ["one"] },
        });
        await database.query("SELECT * FROM worker_writes");
      }
      store.pauseRemovals = true;
      const terminalCall = raw.call(terminal === "close" ? handleId : null, terminal);
      if (terminal === "close") expect((await terminalCall).kind).toBe("rpc-result");
      await waitForLeaseRemovals(store, 1);
      const dispose = terminal === "close" ? raw.call(null, "dispose") : terminalCall;
      await expectWorkerRpcPending(dispose);
      expect((await store.listLeases()).length).toBeGreaterThan(0);

      store.resumeRemovals();
      expect((await dispose).kind).toBe("rpc-result");
      expect(await store.listLeases()).toEqual([]);
    });
  }

  it("counts opening, active, and settling write handles against one connection cap", async () => {
    const store = new PausedWorkerRemoveLeaseStore();
    const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
    const raw = exposeRaw(database);
    const handleIds: string[] = [];
    for (let index = 0; index < MAX_WORKER_HANDLES_PER_CONNECTION; index += 1) {
      const opened = await raw.call(null, "writeOpen");
      if (opened.kind !== "rpc-result") throw new Error("Expected write handle");
      handleIds.push((opened.result as { handleId: string }).handleId);
    }
    store.pauseRemovals = true;
    const aborts = handleIds.slice(0, -1).map((handleId) => raw.call(handleId, "abort"));
    await waitForLeaseRemovals(store, MAX_WORKER_HANDLES_PER_CONNECTION - 1);
    const refused = await raw.call(null, "writeOpen");
    expect(refused.kind).toBe("rpc-failure");
    if (refused.kind === "rpc-failure") {
      expect(refused.error.message).toContain("open handles (active or settling)");
    }

    store.resumeRemovals();
    expect((await Promise.all(aborts)).every((result) => result.kind === "rpc-result")).toBe(true);
    const recovered = await raw.call(null, "writeOpen");
    expect(recovered.kind).toBe("rpc-result");
    if (recovered.kind !== "rpc-result") throw new Error("Expected recovered capacity");
    const recoveredId = (recovered.result as { handleId: string }).handleId;
    expect((await raw.call(recoveredId, "abort")).kind).toBe("rpc-result");
    expect((await raw.call(handleIds.at(-1) ?? "", "abort")).kind).toBe("rpc-result");
    expect((await raw.call(null, "dispose")).kind).toBe("rpc-result");
    await expectNoActiveWorkerOwners(store);
  });

  for (const pause of ["before-apply", "after-apply"] as const) {
    it(`joins a root execute paused ${pause} before worker disposal`, async () => {
      const store = new PausedWorkerWriteTransactionStore(pause);
      const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
      await createWorkerWriteTable(database);
      let storeClosed = false;
      const raw = exposeRaw(database, {
        onDispose: () => {
          storeClosed = true;
        },
      });
      const order: string[] = [];
      const execute = raw
        .call(null, "execute", ["INSERT INTO worker_writes VALUES (1, 'one')"])
        .then((result) => {
          order.push("execute");
          return result;
        });
      await store.writeStarted;
      if (pause === "after-apply") await store.writeApplied;
      const dispose = raw.call(null, "dispose").then((result) => {
        order.push("dispose");
        return result;
      });
      await expectWorkerRpcPending(dispose);
      expect(storeClosed).toBe(false);

      store.resumeWrite();
      expect((await execute).kind).toBe("rpc-result");
      expect((await dispose).kind).toBe("rpc-result");
      expect(order).toEqual(["execute", "dispose"]);
      expect(storeClosed).toBe(true);
      await expectNoActiveWorkerOwners(store);
      const verifier = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
      expect((await verifier.query("SELECT * FROM worker_writes")).rows).toEqual([
        { id: 1, value: "one" },
      ]);
      await verifier.close();
    });
  }

  for (const method of ["query", "readTable"] as const) {
    it(`joins an admitted root ${method} before worker disposal`, async () => {
      const store = new PausedWorkerQueryStore();
      const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
      await createWorkerWriteTable(database);
      await database.insertBatch("worker_writes", {
        columns: { id: [1], value: ["one"] },
      });
      let storeClosed = false;
      const raw = exposeRaw(database, {
        onDispose: () => {
          storeClosed = true;
          store.close();
        },
      });
      store.pauseQueryReads = true;
      const order: string[] = [];
      const operation = raw
        .call(
          null,
          method,
          method === "query" ? ["SELECT * FROM worker_writes"] : ["worker_writes"],
        )
        .then((result) => {
          order.push("operation");
          return result;
        });
      await store.queryReadStarted;
      const dispose = raw.call(null, "dispose").then((result) => {
        order.push("dispose");
        return result;
      });
      await expectWorkerRpcPending(dispose);
      expect(storeClosed).toBe(false);

      store.resumeQueryRead();
      expect((await operation).kind).toBe("rpc-result");
      expect((await dispose).kind).toBe("rpc-result");
      expect(order).toEqual(["operation", "dispose"]);
      expect(storeClosed).toBe(true);
      await expectNoActiveWorkerOwners(store);
    });
  }

  it("signals and joins an admitted cursor next before worker disposal", async () => {
    const store = new PausedWorkerQueryStore();
    const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
    await createWorkerWriteTable(database);
    await database.insertBatch("worker_writes", {
      columns: { id: [1], value: ["one"] },
    });
    const raw = exposeRaw(database);
    const opened = await raw.call(null, "queryCursorOpen", [
      "dispose-cursor-next",
      "SELECT * FROM worker_writes",
      {},
    ]);
    expect(opened.kind).toBe("rpc-result");
    store.pauseQueryReads = true;
    const order: string[] = [];
    const next = raw.call("dispose-cursor-next", "next").then((result) => {
      order.push("next");
      return result;
    });
    await store.queryReadStarted;
    const dispose = raw.call(null, "dispose").then((result) => {
      order.push("dispose");
      return result;
    });
    await expectWorkerRpcPending(dispose);

    store.resumeQueryRead();
    const nextResult = await next;
    expect(nextResult.kind).toBe("rpc-failure");
    if (nextResult.kind === "rpc-failure") {
      expect(nextResult.error.message).toContain("Query cursor is closed");
    }
    expect((await dispose).kind).toBe("rpc-result");
    expect(order).toEqual(["next", "dispose"]);
    await expectNoActiveWorkerOwners(store);
  });

  it("rolls back an idle worker write and rejects every late handle operation", async () => {
    vi.useFakeTimers();
    try {
      const store = new MemoryBlockStore();
      const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
      await createWorkerWriteTable(database);
      const raw = exposeRaw(database, { writeHandleIdleTimeoutMs: 10 });
      const opened = await raw.call(null, "writeOpen");
      if (opened.kind !== "rpc-result") throw new Error("Expected write handle");
      const handleId = (opened.result as { handleId: string }).handleId;
      expect(
        (
          await raw.call(handleId, "stage", [
            "insertBatch",
            "worker_writes",
            { columns: { id: [1], value: ["one"] } },
          ])
        ).kind,
      ).toBe("rpc-result");
      expect(
        (
          await raw.call(handleId, "stage", [
            "insertBatch",
            "worker_writes",
            { columns: { id: [2], value: ["two"] } },
          ])
        ).kind,
      ).toBe("rpc-result");
      const transactionId = await openTransactionId(store);

      await vi.advanceTimersByTimeAsync(9);
      expect(
        (await raw.call(handleId, "query", ["SELECT COUNT(*) AS count FROM worker_writes"])).kind,
      ).toBe("rpc-result");
      // The successful query refreshed the whole deadline. Only a full subsequent interval
      // with no handle traffic expires the scope.
      await vi.advanceTimersByTimeAsync(9);
      expect((await raw.call(handleId, "query", ["SELECT 1 AS alive"])).kind).toBe("rpc-result");
      await vi.advanceTimersByTimeAsync(10);
      await waitForTransactionStatus(store, transactionId, "aborted");

      for (const [method, args] of [
        ["query", ["SELECT 1"]],
        ["stage", ["deleteBatch", "worker_writes", { keys: [1] }]],
        ["commit", []],
        ["abort", []],
      ] as const) {
        const late = await raw.call(handleId, method, [...args]);
        expect(late.kind).toBe("rpc-failure");
        if (late.kind === "rpc-failure") expect(late.error.message).toContain("Unknown handle");
      }
      expect((await database.query("SELECT * FROM worker_writes")).rows).toEqual([]);
      expect((await raw.call(null, "dispose")).kind).toBe("rpc-result");
    } finally {
      vi.useRealTimers();
    }
  });

  it("uses the stock worker database inactivity option for client write callbacks", async () => {
    vi.useFakeTimers();
    try {
      const { clientSide, workerSide } = createBoundary();
      attachDatabaseWorker(workerSide);
      const client = new MinnowDatabaseClient(clientSide, {
        store: { kind: "memory" },
        databaseOptions: {
          transactionIdleTimeoutMs: 10,
          autoCollect: false,
          autoCompact: false,
        },
      });
      await client.createTable({
        name: "worker_writes",
        columns: [{ name: "id", type: "number" }],
      });
      await expect(
        client.write(async (session) => {
          await vi.advanceTimersByTimeAsync(10);
          await session.insertBatch("worker_writes", { columns: { id: [1] } });
        }),
      ).rejects.toThrow("Unknown handle");
      expect((await client.query("SELECT * FROM worker_writes")).rows).toEqual([]);
      await client.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it("never expires an active slow handle call and restarts inactivity afterward", async () => {
    vi.useFakeTimers();
    try {
      const store = new PausedWorkerStageStore();
      const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
      await createWorkerWriteTable(database);
      const raw = exposeRaw(database, { writeHandleIdleTimeoutMs: 10 });
      const opened = await raw.call(null, "writeOpen");
      if (opened.kind !== "rpc-result") throw new Error("Expected write handle");
      const handleId = (opened.result as { handleId: string }).handleId;
      expect(
        (
          await raw.call(handleId, "stage", [
            "insertBatch",
            "worker_writes",
            { columns: { id: [1], value: ["one"] } },
          ])
        ).kind,
      ).toBe("rpc-result");
      store.pauseStages = true;
      const slowStage = raw.call(handleId, "stage", [
        "insertBatch",
        "worker_writes",
        { columns: { id: [2], value: ["two"] } },
      ]);
      await store.stageStarted;
      await vi.advanceTimersByTimeAsync(100);
      for (const [method, args] of [
        ["commit", []],
        [
          "stage",
          ["insertBatch", "worker_writes", { columns: { id: [3], value: ["must-not-land"] } }],
        ],
      ] as const) {
        const overlap = await raw.call(handleId, method, [...args]);
        expect(overlap.kind).toBe("rpc-failure");
        if (overlap.kind === "rpc-failure") {
          expect(overlap.error.message).toContain("already has a call in flight");
        }
      }
      store.resumeStages();
      expect((await slowStage).kind).toBe("rpc-result");
      const transactionId = await openTransactionId(store);
      expect((await store.getTransaction(transactionId))?.status).toBe("active");

      // Completion, not start, is the activity point: almost one whole timeout later commit is
      // still accepted, and its terminal handle cannot be reached by the old timer afterward.
      await vi.advanceTimersByTimeAsync(9);
      expect((await raw.call(handleId, "commit")).kind).toBe("rpc-result");
      await waitForTransactionStatus(store, transactionId, "committed");
      await vi.advanceTimersByTimeAsync(100);
      expect((await database.query("SELECT id FROM worker_writes ORDER BY id")).rows).toEqual([
        { id: 1 },
        { id: 2 },
      ]);
      const lateCommit = await raw.call(handleId, "commit");
      expect(lateCommit.kind).toBe("rpc-failure");
      if (lateCommit.kind === "rpc-failure") {
        expect(lateCommit.error.message).toContain("Unknown handle");
      }
      expect((await raw.call(null, "dispose")).kind).toBe("rpc-result");
    } finally {
      vi.useRealTimers();
    }
  });

  it("clears worker write timers on abort and connection disposal", async () => {
    vi.useFakeTimers();
    try {
      for (const outcome of ["abort", "dispose"] as const) {
        const store = new MemoryBlockStore();
        const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
        await createWorkerWriteTable(database);
        const raw = exposeRaw(database, { writeHandleIdleTimeoutMs: 10 });
        const opened = await raw.call(null, "writeOpen");
        if (opened.kind !== "rpc-result") throw new Error("Expected write handle");
        const handleId = (opened.result as { handleId: string }).handleId;
        await raw.call(handleId, "stage", [
          "insertBatch",
          "worker_writes",
          { columns: { id: [1], value: ["one"] } },
        ]);
        await raw.call(handleId, "stage", [
          "insertBatch",
          "worker_writes",
          { columns: { id: [2], value: ["two"] } },
        ]);
        const transactionId = await openTransactionId(store);
        expect((await raw.call(outcome === "abort" ? handleId : null, outcome)).kind).toBe(
          "rpc-result",
        );
        await waitForTransactionStatus(store, transactionId, "aborted");
        await vi.advanceTimersByTimeAsync(100);
        expect((await store.getTransaction(transactionId))?.status).toBe("aborted");
        if (outcome === "abort") {
          expect((await raw.call(null, "dispose")).kind).toBe("rpc-result");
        }
      }
    } finally {
      vi.useRealTimers();
    }
  });

  for (const pause of ["before-apply", "after-apply"] as const) {
    it(`joins a commit paused ${pause} before worker disposal closes resources`, async () => {
      const store = new PausedWorkerWriteTransactionStore(pause);
      const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
      await createWorkerWriteTable(database);
      let disposed = false;
      const raw = exposeRaw(database, {
        onDispose: () => {
          disposed = true;
        },
      });
      const opened = await raw.call(null, "writeOpen");
      if (opened.kind !== "rpc-result") throw new Error("Expected write handle");
      const handleId = (opened.result as { handleId: string }).handleId;
      expect(
        (
          await raw.call(handleId, "stage", [
            "insertBatch",
            "worker_writes",
            { columns: { id: [1], value: ["one"] } },
          ])
        ).kind,
      ).toBe("rpc-result");

      const commit = raw.call(handleId, "commit");
      await store.writeStarted;
      if (pause === "after-apply") await store.writeApplied;
      const dispose = raw.call(null, "dispose");
      await expectWorkerRpcPending(dispose);
      expect(disposed).toBe(false);
      store.resumeWrite();
      expect((await commit).kind).toBe("rpc-result");
      expect((await dispose).kind).toBe("rpc-result");
      expect(disposed).toBe(true);

      await expectNoActiveWorkerOwners(store);
      const verifier = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
      expect((await verifier.query("SELECT id, value FROM worker_writes")).rows).toEqual([
        { id: 1, value: "one" },
      ]);
      await verifier.close();
    });
  }

  it("joins an already-decided abort before worker disposal closes resources", async () => {
    const store = new PausedWorkerAbortStore();
    const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
    await createWorkerWriteTable(database);
    const raw = exposeRaw(database);
    const opened = await raw.call(null, "writeOpen");
    if (opened.kind !== "rpc-result") throw new Error("Expected write handle");
    const handleId = (opened.result as { handleId: string }).handleId;
    await raw.call(handleId, "stage", [
      "insertBatch",
      "worker_writes",
      { columns: { id: [1], value: ["one"] } },
    ]);
    await raw.call(handleId, "stage", [
      "insertBatch",
      "worker_writes",
      { columns: { id: [2], value: ["two"] } },
    ]);
    const transactionId = await openTransactionId(store);

    const abort = raw.call(handleId, "abort");
    await store.abortStarted;
    const dispose = raw.call(null, "dispose");
    await expectWorkerRpcPending(dispose);
    store.resumeAbort();
    expect((await abort).kind).toBe("rpc-result");
    expect((await dispose).kind).toBe("rpc-result");
    expect((await store.getTransaction(transactionId))?.status).toBe("aborted");
    await expectNoActiveWorkerOwners(store);

    const verifier = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
    expect((await verifier.query("SELECT * FROM worker_writes")).rows).toEqual([]);
    await verifier.close();
  });

  it("joins an idle-expiry rollback before worker disposal closes resources", async () => {
    vi.useFakeTimers();
    try {
      const store = new PausedWorkerAbortStore();
      const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
      await createWorkerWriteTable(database);
      const raw = exposeRaw(database, { writeHandleIdleTimeoutMs: 10 });
      const opened = await raw.call(null, "writeOpen");
      if (opened.kind !== "rpc-result") throw new Error("Expected write handle");
      const handleId = (opened.result as { handleId: string }).handleId;
      await raw.call(handleId, "stage", [
        "insertBatch",
        "worker_writes",
        { columns: { id: [1], value: ["one"] } },
      ]);
      await raw.call(handleId, "stage", [
        "insertBatch",
        "worker_writes",
        { columns: { id: [2], value: ["two"] } },
      ]);
      const transactionId = await openTransactionId(store);

      await vi.advanceTimersByTimeAsync(10);
      await store.abortStarted;
      const dispose = raw.call(null, "dispose");
      await expectWorkerRpcPending(dispose);
      store.resumeAbort();
      expect((await dispose).kind).toBe("rpc-result");
      expect((await store.getTransaction(transactionId))?.status).toBe("aborted");
      await expectNoActiveWorkerOwners(store);
    } finally {
      vi.useRealTimers();
    }
  });

  it("waits for an active stage, then aborts it before worker disposal", async () => {
    const store = new PausedWorkerStageStore();
    const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
    await createWorkerWriteTable(database);
    const raw = exposeRaw(database);
    const opened = await raw.call(null, "writeOpen");
    if (opened.kind !== "rpc-result") throw new Error("Expected write handle");
    const handleId = (opened.result as { handleId: string }).handleId;
    await raw.call(handleId, "stage", [
      "insertBatch",
      "worker_writes",
      { columns: { id: [1], value: ["one"] } },
    ]);
    store.pauseStages = true;
    const stage = raw.call(handleId, "stage", [
      "insertBatch",
      "worker_writes",
      { columns: { id: [2], value: ["two"] } },
    ]);
    await store.stageStarted;
    const transactionId = await openTransactionId(store);
    const dispose = raw.call(null, "dispose");
    await expectWorkerRpcPending(dispose);
    expect((await store.getTransaction(transactionId))?.status).toBe("active");

    store.resumeStages();
    expect((await stage).kind).toBe("rpc-result");
    expect((await dispose).kind).toBe("rpc-result");
    expect((await store.getTransaction(transactionId))?.status).toBe("aborted");
    await expectNoActiveWorkerOwners(store);
    const verifier = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
    expect((await verifier.query("SELECT * FROM worker_writes")).rows).toEqual([]);
    await verifier.close();
  });

  it("waits for an active session query before worker disposal closes resources", async () => {
    const store = new PausedWorkerQueryStore();
    const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
    await createWorkerWriteTable(database);
    await database.insertBatch("worker_writes", {
      columns: { id: [1], value: ["one"] },
    });
    let storeClosed = false;
    const raw = exposeRaw(database, {
      onDispose: () => {
        storeClosed = true;
        store.close();
      },
    });
    const opened = await raw.call(null, "writeOpen");
    if (opened.kind !== "rpc-result") throw new Error("Expected write handle");
    const handleId = (opened.result as { handleId: string }).handleId;
    store.pauseQueryReads = true;
    const query = raw.call(handleId, "query", ["SELECT * FROM worker_writes"]);
    await store.queryReadStarted;
    const dispose = raw.call(null, "dispose");
    await expectWorkerRpcPending(dispose);
    expect(storeClosed).toBe(false);

    store.resumeQueryRead();
    expect((await query).kind).toBe("rpc-result");
    expect((await dispose).kind).toBe("rpc-result");
    expect(storeClosed).toBe(true);
    await expectNoActiveWorkerOwners(store);
  });

  it("releases reserved worker handle names after synchronous open failures", async () => {
    class RefusingDatabase extends MinnowDatabase {
      override queryCursor(
        ...args: Parameters<MinnowDatabase["queryCursor"]>
      ): ReturnType<MinnowDatabase["queryCursor"]> {
        void args;
        throw new Error("cursor open refused");
      }

      override bufferedWriter(
        ...args: Parameters<MinnowDatabase["bufferedWriter"]>
      ): ReturnType<MinnowDatabase["bufferedWriter"]> {
        void args;
        throw new Error("writer open refused");
      }

      override liveQueries(
        ...args: Parameters<MinnowDatabase["liveQueries"]>
      ): ReturnType<MinnowDatabase["liveQueries"]> {
        void args;
        throw new Error("live set open refused");
      }
    }
    const raw = exposeRaw(new RefusingDatabase(new MemoryBlockStore()));
    expect((await raw.call(null, "queryCursorOpen", ["reused", "SELECT 1", {}])).kind).toBe(
      "rpc-failure",
    );
    expect((await raw.call(null, "bufferedWriter", ["reused", "missing", {}])).kind).toBe(
      "rpc-failure",
    );
    expect((await raw.call(null, "liveQueries", ["reused", {}])).kind).toBe("rpc-failure");
    expect((await raw.call(null, "dispose")).kind).toBe("rpc-result");
  });

  it("returns malformed attach-worker frames to a recoverable request ID", async () => {
    const { clientSide, workerSide } = createBoundary();
    attachDatabaseWorker(workerSide);
    const response = new Promise<RpcResponse>((resolve) => {
      clientSide.addEventListener("message", (event) => {
        const parsed = parseRpcResponse(event.data);
        if (parsed?.requestId === "malformed-attached") resolve(parsed);
      });
    });
    clientSide.postMessage({
      version: protocolVersion + 1,
      requestId: "malformed-attached",
      kind: "rpc-init",
      payload: {},
    });
    expect((await response).kind).toBe("rpc-failure");
  });

  it("fails every pending call when the transport reports a fatal channel error", async () => {
    const listeners = new Map<string, () => void>();
    let messageListener: ((event: MessageEvent<unknown>) => void) | undefined;
    const transport: ClientTransport = {
      postMessage: (message) => {
        const request = message as { kind: string; requestId: string; method?: string };
        if (request.kind === "rpc-init") {
          queueMicrotask(() => {
            messageListener?.({
              data: {
                version: protocolVersion,
                requestId: request.requestId,
                kind: "rpc-result",
                result: {},
              },
            } as MessageEvent<unknown>);
          });
        }
      },
      addEventListener: (type, listener) => {
        if (type === "message") messageListener = listener;
        else listeners.set(type, listener as () => void);
      },
    };
    const client = new MinnowDatabaseClient(transport, { store: { kind: "memory" } });
    await client.ready();
    const pending = client.listTables();
    listeners.get("error")?.();
    await expect(pending).rejects.toThrow("database worker failed");
    await expect(client.listTables()).rejects.toThrow("database worker failed");

    const messageErrorListeners = new Map<string, () => void>();
    const broken = new MinnowDatabaseClient(
      {
        postMessage: () => undefined,
        addEventListener: (type, listener) => {
          messageErrorListeners.set(type, listener as () => void);
        },
      },
      { store: { kind: "memory" } },
    );
    messageErrorListeners.get("messageerror")?.();
    await expect(broken.ready()).rejects.toThrow("could not be deserialized");
  });

  it("installs and removes the browser visibility reporter", async () => {
    const documentListeners = new Map<string, () => void>();
    const removed: string[] = [];
    const fakeDocument = {
      visibilityState: "hidden",
      addEventListener: (type: string, listener: () => void) => {
        documentListeners.set(type, listener);
      },
      removeEventListener: (type: string) => {
        removed.push(type);
      },
    };
    vi.stubGlobal("document", fakeDocument);
    try {
      const calls: Array<{ requestId: string; method?: string; args?: unknown[] }> = [];
      let listener: ((event: MessageEvent<unknown>) => void) | undefined;
      const client = new MinnowDatabaseClient(
        {
          postMessage: (message) => {
            const request = message as {
              requestId: string;
              method?: string;
              args?: unknown[];
            };
            calls.push(request);
            queueMicrotask(() => {
              listener?.({
                data: {
                  version: protocolVersion,
                  requestId: request.requestId,
                  kind: "rpc-result",
                  result: {},
                },
              } as MessageEvent<unknown>);
            });
          },
          addEventListener: (type, next) => {
            if (type === "message") listener = next;
          },
        },
        { store: { kind: "memory" } },
      );
      await client.ready();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(calls).toContainEqual(
        expect.objectContaining({ method: "setVisibility", args: [false] }),
      );
      fakeDocument.visibilityState = "visible";
      documentListeners.get("visibilitychange")?.();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(calls).toContainEqual(
        expect.objectContaining({ method: "setVisibility", args: [true] }),
      );
      await client.close();
      expect(removed).toContain("visibilitychange");
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("caps client-side in-flight requests and reuses capacity after a response", async () => {
    let listener: ((event: MessageEvent<unknown>) => void) | undefined;
    const calls: Array<{ requestId: string; method: string }> = [];
    const reply = (requestId: string, result: unknown): void => {
      listener?.({
        data: { version: protocolVersion, requestId, kind: "rpc-result", result },
      } as MessageEvent<unknown>);
    };
    const transport: ClientTransport = {
      postMessage: (message) => {
        const request = message as {
          kind: string;
          requestId: string;
          method?: string;
        };
        if (request.kind === "rpc-init") queueMicrotask(() => reply(request.requestId, {}));
        else if (request.method === "dispose") queueMicrotask(() => reply(request.requestId, {}));
        else calls.push({ requestId: request.requestId, method: request.method ?? "" });
      },
      addEventListener: (type, next) => {
        if (type === "message") listener = next;
      },
    };
    const client = new MinnowDatabaseClient(transport, { store: { kind: "memory" } });
    await client.ready();
    const pending = Array.from({ length: MAX_DATABASE_RPC_IN_FLIGHT }, () => client.listTables());
    expect(calls).toHaveLength(MAX_DATABASE_RPC_IN_FLIGHT);
    await expect(client.listTables()).rejects.toThrow(
      `more than ${String(MAX_DATABASE_RPC_IN_FLIGHT)} in-flight requests`,
    );

    const completed = calls[0];
    if (completed === undefined) throw new Error("Expected a pending client call");
    reply(completed.requestId, []);
    await pending[0];
    const replacement = client.listTables();
    expect(calls).toHaveLength(MAX_DATABASE_RPC_IN_FLIGHT + 1);
    for (const call of calls.slice(1)) reply(call.requestId, []);
    await Promise.all([...pending.slice(1), replacement]);
    await client.close();
  });

  it("caps forged worker-side concurrency before dispatch and recovers after completion", async () => {
    let release!: () => void;
    const barrier = new Promise<void>((resolve) => {
      release = resolve;
    });
    class BarrierDatabase extends MinnowDatabase {
      dispatched = 0;

      override async listTables() {
        this.dispatched += 1;
        await barrier;
        return super.listTables();
      }
    }
    let workerListener: ((event: MessageEvent<unknown>) => void) | undefined;
    const responses: RpcResponse[] = [];
    const scope: RpcScope = {
      postMessage: (message) => {
        const response = parseRpcResponse(message);
        if (response !== null) responses.push(response);
      },
      addEventListener: (_type, next) => {
        workerListener = next;
      },
    };
    const database = new BarrierDatabase(new MemoryBlockStore(), {
      autoCollect: false,
      autoCompact: false,
    });
    exposeDatabase(database, scope);
    const send = (requestId: string): void => {
      workerListener?.({
        data: {
          version: protocolVersion,
          requestId,
          kind: "rpc-call",
          handleId: null,
          method: "listTables",
          args: [],
        },
      } as MessageEvent<unknown>);
    };
    for (let index = 0; index < MAX_DATABASE_RPC_IN_FLIGHT; index += 1) {
      send(`stalled-${String(index)}`);
    }
    send("refused");
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(database.dispatched).toBe(MAX_DATABASE_RPC_IN_FLIGHT);
    expect(responses).toEqual([
      expect.objectContaining({ kind: "rpc-failure", requestId: "refused" }),
    ]);

    release();
    await new Promise((resolve) => setTimeout(resolve, 0));
    send("replacement");
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(database.dispatched).toBe(MAX_DATABASE_RPC_IN_FLIGHT + 1);
    expect(responses).toContainEqual(
      expect.objectContaining({ kind: "rpc-result", requestId: "replacement" }),
    );
    await database.close();
  });

  it("pulls query cursor pages across the worker boundary", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insertBatch("people", {
      columns: {
        id: [1, 2, 3, 4, 5],
        name: ["Ada", "Grace", "Katherine", "Dorothy", "Mary"],
        joined: [new Date(1), new Date(2), new Date(3), new Date(4), new Date(5)],
      },
    });
    const batches = [];
    for await (const batch of client.queryCursor(
      "SELECT id, name, joined FROM people ORDER BY id",
      { batchRows: 2 },
    )) {
      batches.push(batch);
    }
    expect(batches.map(({ rows }) => rows.length)).toEqual([2, 2, 1]);
    expect(batches.flatMap(({ rows }) => rows).map(({ id }) => id)).toEqual([1, 2, 3, 4, 5]);
    expect(batches[0]?.rows[0]?.joined).toEqual(new Date(1));
    expect(() => client.queryCursor("SELECT id FROM people", { onStats: () => undefined })).toThrow(
      /not available across a worker/,
    );
    await client.close();
  });

  it("caps abandoned worker handles and reuses capacity after close", async () => {
    const client = connect();
    await client.ready();
    const cursors: Array<AsyncIterator<QueryResult>> = [];
    for (let index = 0; index < MAX_WORKER_HANDLES_PER_CONNECTION; index += 1) {
      const cursor = client.queryCursor(`SELECT ${String(index)} AS value`)[Symbol.asyncIterator]();
      expect((await cursor.next()).done).toBe(false);
      cursors.push(cursor);
    }
    const refused = client.queryCursor("SELECT 999 AS value")[Symbol.asyncIterator]();
    await expect(refused.next()).rejects.toThrow(
      `cannot hold more than ${String(MAX_WORKER_HANDLES_PER_CONNECTION)} open handles`,
    );

    await cursors[0]?.return?.();
    const replacement = client.queryCursor("SELECT 1000 AS value")[Symbol.asyncIterator]();
    expect((await replacement.next()).done).toBe(false);
    await replacement.return?.();
    await Promise.all(cursors.slice(1).map(async (cursor) => cursor.return?.()));
    await client.close();
  });

  it("detaches its transport listener when closed without terminating the worker", async () => {
    const boundary = createBoundary();
    exposeDatabase(new MinnowDatabase(new MemoryBlockStore()), boundary.workerSide);
    const client = new MinnowDatabaseClient(boundary.clientSide);
    await client.ready();
    expect(boundary.clientListenerCount()).toBe(1);
    await client.close();
    expect(boundary.clientListenerCount()).toBe(0);
    await expect(
      (
        client as unknown as {
          _invoke(handleId: string, method: string, args: unknown[]): Promise<unknown>;
        }
      )._invoke("closed", "stats", []),
    ).rejects.toThrow("Database client is closed");
  });

  it("replays a failed init's reason to calls that arrive after it", async () => {
    const { clientSide, workerSide } = createBoundary();
    attachDatabaseWorker(workerSide);
    // Node has no IndexedDB and none is injected, so the store open fails inside the worker.
    const client = new MinnowDatabaseClient(clientSide, {
      store: { kind: "indexeddb", name: "nowhere" },
    });
    await expect(client.ready()).rejects.toThrow("IndexedDB is unavailable");
    // The failure reset the worker for a retry; a pipelined call landing after that reset must
    // carry the real reason, not a generic "send init first".
    await expect(client.listTables()).rejects.toThrow(
      /Database initialization failed: .*IndexedDB is unavailable/,
    );
  });

  it("initializes, writes, and queries through the boundary", async () => {
    const client = connect();
    await client.ready();
    await createPeopleTable(client);
    // Rows here, columns in the typed-query test below: both forms cross the worker boundary.
    const inserted = await client.insertBatch("people", [
      { id: 1, name: "Ada", joined: new Date("2024-01-02T03:04:05Z") },
      { id: 2, name: "Grace", joined: new Date("2024-06-07T08:09:10Z") },
    ]);
    expect(inserted.rowCount).toBe(2);
    const result = await client.query("SELECT name, joined FROM people ORDER BY name");
    expect(result.rows.map((row) => row.name)).toEqual(["Ada", "Grace"]);
    expect(result.rows[0]?.joined).toBeInstanceOf(Date);
    expect((result.rows[0]?.joined as Date).toISOString()).toBe("2024-01-02T03:04:05.000Z");
    const tables = await client.listTables();
    expect(tables.map(({ name }) => name)).toEqual(["people"]);
    await client.close();
  });

  it("carries finite and explicit-unbounded memory defaults through worker initialization", async () => {
    const boundedBoundary = createBoundary();
    attachDatabaseWorker(boundedBoundary.workerSide);
    const bounded = new MinnowDatabaseClient(boundedBoundary.clientSide, {
      store: { kind: "memory" },
      databaseOptions: {
        executionMemoryBudgetBytes: 1,
        autoCollect: false,
        autoCompact: false,
        autoCollectDebtLimitCommits: 17,
        transactionOwnerLeaseMs: 1234,
      },
    });
    await bounded.ready();
    expect(await bounded.maintenanceStatus()).toMatchObject({
      autoCollectionEnabled: false,
      pendingCommitDebt: 0,
    });
    await expect(bounded.query("SELECT 1 AS n", { memoize: false })).rejects.toBeInstanceOf(
      QueryMemoryBudgetError,
    );
    expect(
      (
        await bounded.query("SELECT 1 AS n", {
          memoize: false,
          executionMemoryBudgetBytes: 1_024,
        })
      ).rows,
    ).toEqual([{ n: 1 }]);
    await bounded.close();

    const unboundedBoundary = createBoundary();
    attachDatabaseWorker(unboundedBoundary.workerSide);
    const unbounded = new MinnowDatabaseClient(unboundedBoundary.clientSide, {
      store: { kind: "memory" },
      databaseOptions: { executionMemoryBudgetBytes: null },
    });
    await unbounded.ready();
    expect((await unbounded.query("SELECT 1 AS n", { memoize: false })).rows).toEqual([{ n: 1 }]);
    await unbounded.close();
  });

  it("binds statement parameters across the boundary", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.execute("INSERT INTO people (id, name, joined) VALUES (?, ?, ?)", [
      1,
      "Ada",
      new Date("2024-01-02T03:04:05Z"),
    ]);
    const result = await client.query("SELECT name FROM people WHERE joined = $1", {
      params: [new Date("2024-01-02T03:04:05Z")],
    });
    expect(result.rows).toEqual([{ name: "Ada" }]);
    const updated = await client.execute("UPDATE people SET name = $2 WHERE id = $1", [
      1,
      "Countess",
    ]);
    expect(updated.kind).toBe("update");
    const requeried = await client.query("SELECT name FROM people WHERE id = ?", {
      params: [1],
    });
    expect(requeried.rows).toEqual([{ name: "Countess" }]);
    await client.close();
  });

  it("round-trips the mutation, diagnostics, maintenance, and observer RPC surface", async () => {
    const client = connect();
    await client.createTable({
      name: "rpc_surface",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "value", type: "string" },
      ],
    });

    await client.insert("rpc_surface", { id: 1, value: "one" });
    await client.upsert("rpc_surface", { id: 1, value: "ONE" });
    await client.upsertBatch("rpc_surface", [
      { id: 2, value: "two" },
      { id: 3, value: "three" },
    ]);
    await client.update("rpc_surface", 1, { value: "first" });
    await client.updateBatch("rpc_surface", {
      keys: [2, 3],
      changes: { value: ["second", "third"] },
    });
    await client.deleteBatch("rpc_surface", { keys: [3] });
    await client.delete("rpc_surface", 2);
    const statementResult = await client.runStatement(
      compileStatement("INSERT INTO rpc_surface (id, value) VALUES (4, 'four')"),
    );
    expect(statementResult).toMatchObject({ kind: "insert", rowCount: 1 });
    expect(await client.explain("SELECT value FROM rpc_surface WHERE id = 4")).toContain("scan");

    const writer = client.bufferedWriter("rpc_surface", { maxRows: 10 });
    await writer.add({ id: 5, value: "discarded" });
    writer.requestFlush();
    await writer.flush();
    await writer.add({ id: 6, value: "discarded" });
    expect(await writer.discard()).toBe(1);
    await writer.close();

    expect(await client.bufferPoolStats()).toMatchObject({
      limitBytes: expect.any(Number) as unknown,
    });
    await expect(client.checkIntegrity()).rejects.toThrow(/cannot check integrity/);
    await expect(client.checkIntegrity({ mode: "full", maxIssues: 3 })).rejects.toThrow(
      /cannot check integrity/,
    );
    expect(await client.storageStats()).toMatchObject({ backend: "memory" });
    expect(await client.inspectInterruptedImport()).toBeNull();
    await expect(client.abortInterruptedImport("missing-import")).rejects.toThrow(/not found/);
    expect(await client.cleanupQuerySpill()).toMatchObject({ ownersReclaimed: 0 });
    expect(await client.cleanupQuerySpill({ maxOwners: 1 })).toMatchObject({ ownersReclaimed: 0 });

    const observerSet = client.liveQueries();
    const invalidations: Array<{ initial: boolean }> = [];
    let completed = 0;
    const observer = await observerSet.observe("SELECT value FROM rpc_surface", {
      onInvalidate: (invalidation) => invalidations.push(invalidation),
      onComplete: () => {
        completed += 1;
      },
    });
    expect(invalidations).toEqual([expect.objectContaining({ initial: true })]);
    observerSet.notifyLocalCommit();
    await client.insert("rpc_surface", { id: 7, value: "seven" });
    await observerSet.refresh();
    expect(invalidations.at(-1)).toMatchObject({ initial: false });
    await observer.close();
    expect(completed).toBe(1);
    await observerSet.close();

    const compacted = await client.compactTable("rpc_surface", {
      minimumLevel0Segments: 2,
      maxLevel0Segments: 16,
      maxBlocksPerStep: 1,
      outputCompression: "raw",
    });
    expect(compacted.tableName).toBe("rpc_surface");
    expect(await client.listCompactionJobs()).not.toEqual([]);
    expect(await client.listCompactionJobs("rpc_surface")).not.toEqual([]);
    const skipped = await client.compactTableStep("rpc_surface", { maxBlocks: 1 });
    expect(skipped.result).not.toBeNull();
    await expect(client.resumeCompactionJob("missing-job")).rejects.toThrow(/not found/);
    await expect(client.resumeCompactionJob("missing-job", { maxBlocks: 1 })).rejects.toThrow(
      /not found/,
    );
    await expect(client.cancelCompactionJob("missing-job")).rejects.toThrow(/not found/);

    const collection = await client.collectGarbage({
      retainRecentVersions: 0,
      maxItemsPerStep: 1,
      maxPlanningItems: 1,
    });
    expect(collection.jobId).toEqual(expect.any(String));
    const collectionStep = await client.collectGarbageStep({
      retainRecentVersions: 0,
      maxItems: 1,
      maxPlanningItems: 1,
    });
    expect(collectionStep.jobId).toEqual(expect.any(String));
    await expect(client.resumeGarbageCollectionJob("missing-job")).rejects.toThrow(/not found/);
    await expect(client.resumeGarbageCollectionJob("missing-job", { maxItems: 1 })).rejects.toThrow(
      /not found/,
    );
    expect(await client.listGarbageCollectionJobs()).not.toEqual([]);
    await client.collectGarbage();
    await client.close({ terminateWorker: false });
  });

  it("issues calls without awaiting ready because the channel is ordered", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insert("people", { id: 1, name: "Ada", joined: new Date() });
    const rows = await client.readTable("people", { columns: ["name"] });
    expect(rows).toEqual([{ name: "Ada" }]);
  });

  it("round-trips bounded visible-segment pages at one captured version", async () => {
    const client = connect();
    await createPeopleTable(client);
    const before = [];
    for (let id = 1; id <= 3; id += 1) {
      before.push(
        await client.insert("people", {
          id,
          name: `person-${String(id)}`,
          joined: new Date(id),
        }),
      );
    }
    const first = await client.listVisibleSegmentPage("people", { limit: 1 });
    expect(first.records).toHaveLength(1);
    expect(first.nextCursor).not.toBeNull();
    const concurrent = await client.insert("people", {
      id: 4,
      name: "person-4",
      joined: new Date(4),
    });

    const captured = [...first.records];
    let cursor = first.nextCursor;
    while (cursor !== null) {
      const page = await client.listVisibleSegmentPage("people", { cursor, limit: 1 });
      expect(page.records.length).toBeLessThanOrEqual(1);
      expect(page.version).toBe(first.version);
      captured.push(...page.records);
      cursor = page.nextCursor;
    }
    expect(captured.map(({ id }) => id).sort()).toEqual(
      before.map(({ segmentId }) => segmentId).sort(),
    );
    expect(captured.map(({ id }) => id)).not.toContain(concurrent.segmentId);
    await expect(client.listVisibleSegmentPage("people", { limit: 65 })).rejects.toThrow(
      /cannot exceed 64/u,
    );

    if (first.nextCursor === null) throw new Error("Expected a visible segment cursor");
    const capturedTableId = first.nextCursor.tableId;
    await client.execute("DROP TABLE people");
    await createPeopleTable(client);
    const stale = await client
      .listVisibleSegmentPage("people", { cursor: first.nextCursor })
      .catch((error: unknown) => error);
    expect(stale).toBeInstanceOf(VisibleSegmentCursorStaleError);
    const typedStale = stale as VisibleSegmentCursorStaleError;
    expect(typedStale).toMatchObject({ tableName: "people", capturedTableId });
    expect(typeof typedStale.currentTableId).toBe("string");
    expect(typedStale.currentTableId).not.toBe(capturedTableId);
    await client.close();
  });

  it("rehydrates typed engine errors with their fields", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insert("people", { id: 1, name: "Ada", joined: new Date() });
    const failure = await client
      .insert("people", { id: 1, name: "Twin", joined: new Date() })
      .catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(UniqueConstraintError);
    const typed = failure as UniqueConstraintError;
    expect(typed.name).toBe("UniqueConstraintError");
    expect(typed.tableName).toBe("people");
    expect(typed.columnName).toBe("id");
    expect(typed.value).toBe(1);
  });

  it("rehydrates every exported storage error with its prototype and fields", async () => {
    let listener: ((event: MessageEvent<unknown>) => void) | undefined;
    let nextError: Error | undefined;
    const respond = (response: RpcResponse): void => {
      queueMicrotask(() =>
        listener?.({ data: structuredClone(response) } as MessageEvent<unknown>),
      );
    };
    const transport: ClientTransport = {
      postMessage: (message) => {
        const request = message as { kind: string; requestId: string; method?: string };
        if (request.kind === "rpc-init" || request.method === "dispose") {
          respond({
            version: protocolVersion,
            requestId: request.requestId,
            kind: "rpc-result",
            result: {},
          });
          return;
        }
        const error = nextError;
        nextError = undefined;
        respond(rpcFailure(request.requestId, error ?? new Error("Missing error fixture")));
      },
      addEventListener: (type, next) => {
        if (type === "message") listener = next;
      },
    };
    const client = new MinnowDatabaseClient(transport, { store: { kind: "memory" } });
    await client.ready();

    const constructors = exportedStorageErrorConstructors();
    expect(constructors.length).toBeGreaterThan(0);
    for (const [exportName, constructor] of constructors) {
      const original = createStorageErrorFixture(constructor);
      nextError = original;
      const rehydrated = await client.listTables().catch((error: unknown) => error);
      expect(rehydrated, exportName).toBeInstanceOf(constructor);
      expect(rehydrated, exportName).toMatchObject({
        name: original.name,
        message: original.message,
        ...Object.fromEntries(Object.entries(original)),
      });
    }
    await client.close();
  });

  it("routes storage diagnostics and preserves hardening error details", async () => {
    const diagnosticsBoundary = createBoundary();
    exposeDatabase(
      new MinnowDatabase(new DiagnosticMemoryBlockStore()),
      diagnosticsBoundary.workerSide,
    );
    const diagnostics = new MinnowDatabaseClient(diagnosticsBoundary.clientSide);
    await diagnostics.ready();
    const corruption = await diagnostics.checkIntegrity().catch((error: unknown) => error);
    expect(corruption).toBeInstanceOf(StorageCorruptionError);
    expect(corruption).toMatchObject({
      backend: "diagnostic",
      location: "records/7",
      message: "diagnostic storage corruption at records/7: checksum mismatch",
    });
    expect(await diagnostics.storageStats()).toMatchObject({
      backend: "diagnostic",
      logicalBytes: 7,
      orphanBytes: 4,
    });
    expect(await diagnostics.inspectInterruptedImport()).toMatchObject({
      identity: "import-7",
      stagedBytes: 19,
    });
    expect(await diagnostics.abortInterruptedImport("import-7")).toEqual({
      identity: "import-7",
      removedBlockCount: 2,
      removedBytes: 19,
    });
    await diagnostics.close();

    const uncertainBoundary = createBoundary();
    exposeDatabase(
      new MinnowDatabase(new UncertainOutcomeMemoryBlockStore()),
      uncertainBoundary.workerSide,
    );
    const uncertainClient = new MinnowDatabaseClient(uncertainBoundary.clientSide);
    await uncertainClient.ready();
    const uncertain = await uncertainClient
      .createTable({ name: "uncertain", columns: [{ name: "value", type: "number" }] })
      .catch((error: unknown) => error);
    expect(uncertain).toBeInstanceOf(OpfsUncertainOutcomeError);
    expect(uncertain).toMatchObject({ method: "addTable" });
    await uncertainClient.close();

    const tableBusyBoundary = createBoundary();
    exposeDatabase(
      new MinnowDatabase(new TableInUseMemoryBlockStore(), { maxCommitRetries: 1 }),
      tableBusyBoundary.workerSide,
    );
    const tableBusyClient = new MinnowDatabaseClient(tableBusyBoundary.clientSide);
    await tableBusyClient.ready();
    await tableBusyClient.createTable({
      name: "busy",
      columns: [{ name: "value", type: "number" }],
    });
    const tableBusy = await tableBusyClient
      .execute("DROP TABLE busy")
      .catch((error: unknown) => error);
    expect(tableBusy).toBeInstanceOf(TableInUseError);
    expect(tableBusy).toMatchObject({
      ownerKind: "transaction",
      ownerId: "txn-still-writing",
    });
    await tableBusyClient.close();

    const maintenanceBoundary = createBoundary();
    const maintenanceStore = new ClientMaintenanceFaultStore();
    exposeDatabase(
      new MinnowDatabase(maintenanceStore, {
        autoCompact: false,
        autoCollectDebtLimitCommits: 1,
      }),
      maintenanceBoundary.workerSide,
    );
    const maintenance = new MinnowDatabaseClient(maintenanceBoundary.clientSide);
    await maintenance.ready();
    await createPeopleTable(maintenance);
    await maintenance.insert("people", { id: 1, name: "Ada", joined: new Date(0) });
    maintenanceStore.failCollection = true;
    const backlog = await maintenance
      .insert("people", { id: 2, name: "Grace", joined: new Date(1) })
      .catch((error: unknown) => error);
    expect(backlog).toBeInstanceOf(MaintenanceBacklogError);
    expect(backlog).toMatchObject({
      pendingCommits: 1,
      causeMessage: "diagnostic collection failure",
    });
    maintenanceStore.failCollection = false;
    await maintenance.close();
  });

  it("carries compile-error positions across the worker boundary", async () => {
    // A devtools editor compiles in the page, but a query typed against the client fails in the
    // worker; the position has to survive serialization or the squiggle lands nowhere.
    const client = connect();
    await createPeopleTable(client);
    const sql = "SELECT * FROM people WHERE name = 'unclosed";
    const failure = await client.query(sql).catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(SqlCompileError);
    const typed = failure as SqlCompileError;
    expect(typed.message).toBe("Unterminated string literal");
    expect(sql.slice(typed.offset, typed.offset + typed.length)).toBe("'unclosed");
  });

  it("migrates a schema DSL definition and reports wire-format steps", async () => {
    const client = connect();
    const people = table("people", {
      id: column.number().unique(),
      name: column.string(),
      nickname: column.string().nullable(),
    });
    const first = await client.migrate(schema([people]));
    expect(first.createdTables).toEqual(["people"]);
    expect(first.steps).toEqual([
      {
        kind: "create-table",
        table: {
          name: "people",
          columns: {
            id: { type: "number", isNullable: false, isUnique: true },
            name: { type: "string", isNullable: false, isUnique: false },
            nickname: { type: "string", isNullable: true, isUnique: false },
          },
        },
      },
    ]);
    const second = await client.migrate(schema([people]));
    expect(second.steps).toEqual([]);
    const handle = typedTable(client, people);
    await handle.insert([{ id: 1, name: "Ada" }]);
    expect(await handle.rows()).toEqual([{ id: 1, name: "Ada", nickname: null }]);
  });

  it("returns generated columns across the worker boundary", async () => {
    const client = connect();
    // Defaults declared through the schema DSL must survive serialization to the worker, and
    // the generated values (including Dates) must survive the structured clone back.
    const notes = table("notes", {
      id: column.number().unique().autoIncrement(),
      created: column.datetime().defaultSql("CURRENT_TIMESTAMP"),
      body: column.string(),
    });
    await client.migrate(schema([notes]));
    const result = await client.insertBatch("notes", [{ body: "hello" }, { body: "there" }]);
    expect(result.generatedColumns?.id).toEqual([1, 2]);
    expect(result.generatedColumns?.created?.[0]).toBeInstanceOf(Date);
    const rows = await client.readTable("notes");
    expect(rows.map((row) => row.id).sort()).toEqual([1, 2]);

    // A batch whose every column is generated still carries its row count across the boundary
    // (the columnar pivot of empty rows would otherwise lose it).
    const stamps = table("stamps", {
      id: column.number().unique().autoIncrement(),
      created: column.datetime().defaultSql("CURRENT_TIMESTAMP"),
    });
    await client.migrate(schema([stamps]));
    const empty = await client.insertBatch("stamps", [{}, {}]);
    expect(empty.rowCount).toBe(2);
    expect(empty.generatedColumns?.id).toEqual([1, 2]);
    await client.close();
  });

  it("preserves both a backfill and a persistent default across schema serialization", async () => {
    const client = connect();
    const before = table("wire_defaults", {
      id: column.number().unique(),
    });
    await client.migrate(schema([before]));
    await client.insertBatch("wire_defaults", [{ id: 1 }]);

    const after = table("wire_defaults", {
      id: column.number().unique(),
      status: column.string().default("future").backfill("existing"),
    });
    const migration = await client.migrate(schema([after]));
    expect(migration.steps).toMatchObject([
      {
        kind: "add-column",
        definition: {
          defaultSpec: { kind: "literal", value: "future" },
          backfillValue: "existing",
        },
      },
    ]);
    expect((await client.introspect()).tables[0]?.columns[1]).toMatchObject({
      defaultValue: { kind: "literal", value: "future" },
      backfill: "existing",
    });
    await expect(client.insertBatch("wire_defaults", [{ id: 2, status: null }])).rejects.toThrow(
      "status[0] cannot be null",
    );
    await client.insertBatch("wire_defaults", [{ id: 2 }]);

    expect(await client.query("SELECT id, status FROM wire_defaults ORDER BY id")).toMatchObject({
      rows: [
        { id: 1, status: "existing" },
        { id: 2, status: "future" },
      ],
    });
    await client.close();
  });

  it("runs compiled typed queries", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insertBatch("people", {
      columns: { id: [1, 2], name: ["Ada", "Grace"], joined: [new Date(), new Date()] },
    });
    const rows = await client.run<{ name: string }>({
      kind: "typed-query",
      plan: compileQuery("SELECT name FROM people ORDER BY name DESC"),
    });
    expect(rows).toEqual([{ name: "Grace" }, { name: "Ada" }]);
  });

  it("rebuilds rows of every value type from the columnar result frame", async () => {
    // Results cross as one array per column; the client's rows must be indistinguishable from
    // the worker's: plain objects keyed in column order, Dates as Date instances, booleans and
    // nulls intact, and a mixed-type column (CASE) carried value by value.
    const client = connect();
    await client.createTable({
      name: "mixed",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "label", type: "string", nullable: true },
        { name: "flag", type: "boolean", nullable: true },
        { name: "at", type: "datetime", nullable: true },
        { name: "score", type: "number", nullable: true },
      ],
    });
    await client.insertBatch("mixed", {
      columns: {
        id: [1, 2, 3],
        label: ["one", null, ""],
        flag: [true, null, false],
        at: [new Date("2024-01-02T03:04:05.678Z"), null, new Date(0)],
        score: [null, 2.5, -0],
      },
    });
    const result = await client.query(
      "SELECT id, label, flag, at, score, CASE WHEN id = 2 THEN label ELSE id END AS either, NULL AS nothing FROM mixed ORDER BY id",
    );
    expect(result.columns).toEqual(["id", "label", "flag", "at", "score", "either", "nothing"]);
    expect(result.rows).toEqual([
      {
        id: 1,
        label: "one",
        flag: true,
        at: new Date("2024-01-02T03:04:05.678Z"),
        score: null,
        either: 1,
        nothing: null,
      },
      { id: 2, label: null, flag: null, at: null, score: 2.5, either: null, nothing: null },
      { id: 3, label: "", flag: false, at: new Date(0), score: -0, either: 3, nothing: null },
    ]);
    expect(result.rows.map((row) => Object.keys(row))).toEqual(
      result.rows.map(() => result.columns),
    );
    expect(result.rows[0]?.at).toBeInstanceOf(Date);
    expect(Object.is(result.rows[2]?.score, -0)).toBe(true);
    expect(Object.getPrototypeOf(result.rows[0])).toBe(Object.prototype);
    const empty = await client.query("SELECT id FROM mixed WHERE id > 10");
    expect(empty).toEqual({ columns: ["id"], rows: [] });
    await client.close();
  });

  it("reads a table back through the columnar frame, every value type intact", async () => {
    const client = connect();
    await client.ready();
    await createPeopleTable(client);
    await client.insertBatch("people", [
      { id: 1, name: "Ada", joined: new Date("2024-01-02T03:04:05.678Z") },
      { id: 2, name: "Grace", joined: new Date("1999-12-31T23:59:59.999Z") },
    ]);
    const rows = await client.readTable("people");
    expect(rows).toEqual([
      { id: 1, name: "Ada", joined: new Date("2024-01-02T03:04:05.678Z") },
      { id: 2, name: "Grace", joined: new Date("1999-12-31T23:59:59.999Z") },
    ]);
    expect(rows[0]?.joined).toBeInstanceOf(Date);
    // A column subset and an explicit version take the same path.
    const version = (
      await client.insertBatch("people", [{ id: 3, name: "Linus", joined: new Date(0) }])
    ).version;
    expect(await client.readTable("people", { columns: ["name"] })).toEqual([
      { name: "Ada" },
      { name: "Grace" },
      { name: "Linus" },
    ]);
    expect((await client.readTable("people", version)).length).toBe(3);
    expect(await client.readTable("people", { version: version - 1, columns: ["id"] })).toEqual([
      { id: 1 },
      { id: 2 },
    ]);
    await client.close();
  });

  it("rebuilds a `__proto__` result column as an own property", async () => {
    const client = connect();
    await client.createTable({
      name: "special_names",
      columns: [{ name: "__proto__", type: "string" }],
    });
    await client.insertBatch("special_names", { columns: { ["__proto__"]: ["kept"] } });
    const result = await client.query('SELECT "__proto__" FROM special_names');
    expect(result.columns).toEqual(["__proto__"]);
    const row = result.rows[0] ?? {};
    expect(Object.hasOwn(row, "__proto__")).toBe(true);
    expect(Reflect.get(row, "__proto__")).toBe("kept");
    expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
    await client.close();
  });

  it("serves a real message port, transferring result buffers instead of copying them", async () => {
    const channel = new MessageChannel();
    const database = new MinnowDatabase(new MemoryBlockStore());
    const transfers: boolean[] = [];
    exposeDatabase(database, {
      addEventListener: (type, listener) => {
        channel.port1.addEventListener(type, listener);
      },
      postMessage: (message, options) => {
        channel.port1.postMessage(message, options);
        // After a transfer the sender's buffers are detached; a copy would leave them intact.
        if (options !== undefined && options.transfer.length > 0) {
          transfers.push(options.transfer.every((buffer) => buffer.byteLength === 0));
        }
      },
    });
    channel.port1.start();
    channel.port2.start();
    const client = new MinnowDatabaseClient(channel.port2);
    await createPeopleTable(client);
    await client.insertBatch("people", {
      columns: {
        id: [1, 2],
        name: ["Ada", "Grace"],
        joined: [new Date("2024-01-02T03:04:05Z"), new Date("2024-06-07T08:09:10Z")],
      },
    });
    const result = await client.query("SELECT id, name, joined FROM people ORDER BY id");
    expect(result.rows).toEqual([
      { id: 1, name: "Ada", joined: new Date("2024-01-02T03:04:05Z") },
      { id: 2, name: "Grace", joined: new Date("2024-06-07T08:09:10Z") },
    ]);
    expect(transfers).toEqual([true]);
    await client.close();
    channel.port1.close();
    channel.port2.close();
  });

  it("runs an atomic write scope across the channel", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insert("people", { id: 1, name: "Ada", joined: new Date() });
    const { result, version } = await client.write(async (tx) => {
      const staged = await tx.insertBatch("people", {
        columns: { id: [2], name: ["Grace"], joined: [new Date()] },
      });
      await tx.updateBatch("people", { keys: [1], changes: { name: ["Countess"] } });
      // Read-your-writes across the channel: the staged rows are visible in-scope only.
      expect((await tx.query("SELECT name FROM people ORDER BY name")).rows).toEqual([
        { name: "Countess" },
        { name: "Grace" },
      ]);
      expect((await client.query("SELECT COUNT(*) AS n FROM people")).rows).toEqual([{ n: 1 }]);
      return staged.rowCount;
    });
    expect(result).toBe(1);
    expect(version).not.toBeNull();
    expect((await client.query("SELECT name FROM people ORDER BY name")).rows).toEqual([
      { name: "Countess" },
      { name: "Grace" },
    ]);
    // A failing scope publishes nothing.
    await expect(
      client.write(async (tx) => {
        await tx.deleteBatch("people", { keys: [2] });
        throw new Error("abort it");
      }),
    ).rejects.toThrow("abort it");
    expect((await client.query("SELECT COUNT(*) AS n FROM people")).rows).toEqual([{ n: 2 }]);
  });

  it("pins a snapshot scope across the channel while writes continue", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insert("people", { id: 1, name: "Ada", joined: new Date() });
    const observed = await client.snapshot(async (session) => {
      const before = await session.query("SELECT COUNT(*) AS people FROM people");
      // A commit lands mid-scope; the session must keep observing the pinned version while
      // fresh queries outside the scope see the new row immediately.
      await client.insert("people", { id: 2, name: "Grace", joined: new Date() });
      const still = await session.query("SELECT COUNT(*) AS people FROM people");
      const fresh = await client.query("SELECT COUNT(*) AS people FROM people");
      return { before: before.rows, still: still.rows, fresh: fresh.rows };
    });
    expect(observed.before).toEqual([{ people: 1 }]);
    expect(observed.still).toEqual([{ people: 1 }]);
    expect(observed.fresh).toEqual([{ people: 2 }]);
    // After the scope, queries are fresh by construction.
    expect((await client.query("SELECT COUNT(*) AS people FROM people")).rows).toEqual([
      { people: 2 },
    ]);
  });

  it("copies a snapshot out in slices and loads one back with progress", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insert("people", { id: 1, name: "Ada", joined: new Date("2024-01-02T03:04:05Z") });
    await client.insert("people", { id: 2, name: "Grace", joined: new Date() });

    const phases: string[] = [];
    const bytes = await client.exportSnapshot({
      onProgress: (progress) => phases.push(progress.phase),
    });
    expect(phases[0]).toBe("reading");
    expect(phases.at(-1)).toBe("done");
    expect(phases.filter((phase) => phase === "done")).toHaveLength(1);
    expect(phases).toContain("transfer");
    expect(bytes.byteLength).toBeGreaterThan(0);

    const restored = connect();
    const loads: number[] = [];
    await restored.importSnapshot(bytes, {
      onProgress: (progress) => {
        if (progress.phase === "done") loads.push(progress.totalBytes);
      },
    });
    expect(loads.length).toBe(1);
    expect((await restored.query("SELECT id, name FROM people ORDER BY id")).rows).toEqual([
      { id: 1, name: "Ada" },
      { id: 2, name: "Grace" },
    ]);
    // Replaying the exact file is a no-op: this is the lost-final-ack recovery path.
    await restored.importSnapshot(bytes);
    // The completed load released its handle rather than leaving a queue in the worker.
    await expect(
      (
        restored as unknown as { _invoke(h: string, m: string, a: unknown[]): Promise<unknown> }
      )._invoke("missing-handle", "finish", []),
    ).rejects.toThrow(/Unknown handle/);
  });

  it("keeps crash-interrupted worker imports resumable, types conflicts, and cleans explicit cancellation", async () => {
    const source = connect();
    await createPeopleTable(source);
    await source.insert("people", { id: 1, name: "Ada", joined: new Date(0) });
    const bytes = await source.exportSnapshot();
    const storedHeaderLength = new DataView(
      bytes.buffer,
      bytes.byteOffset,
      bytes.byteLength,
    ).getUint32(12, true);
    const headerEnd = 28 + storedHeaderLength;

    const interrupted = connect();
    const crash = async function* (): AsyncGenerator<Uint8Array> {
      yield bytes.slice(0, headerEnd);
      throw new Error("transport disappeared");
    };
    await expect(interrupted.importSnapshotStream(crash())).rejects.toThrow(
      "transport disappeared",
    );
    expect(await interrupted.inspectInterruptedImport()).not.toBeNull();

    const foreignSource = connect();
    await createPeopleTable(foreignSource);
    await foreignSource.insert("people", { id: 2, name: "Grace", joined: new Date(1) });
    const foreign = await foreignSource.exportSnapshot();
    const conflict = await interrupted.importSnapshot(foreign).catch((error: unknown) => error);
    expect(conflict).toBeInstanceOf(SnapshotImportConflictError);
    expect(conflict).toMatchObject({ name: "SnapshotImportConflictError" });

    const cancelled = connect();
    const controller = new AbortController();
    const cancel = async function* (): AsyncGenerator<Uint8Array> {
      yield bytes.slice(0, headerEnd);
      controller.abort(new Error("user cancelled"));
      yield bytes.slice(headerEnd);
    };
    await expect(
      cancelled.importSnapshotStream(cancel(), { signal: controller.signal }),
    ).rejects.toThrow("user cancelled");
    expect(await cancelled.inspectInterruptedImport()).toBeNull();
  });

  it("proxies buffered writers, including flush results and stats", async () => {
    const client = connect();
    await createPeopleTable(client);
    const writer = client.bufferedWriter("people", { maxRows: 100 });
    await writer.add({ id: 1, name: "Ada", joined: new Date() });
    await writer.add({ id: 2, name: "Grace", joined: new Date() });
    expect(await writer.stats()).toEqual(
      expect.objectContaining({ pendingRowCount: 2 }) as unknown,
    );
    const flushed = await writer.flush();
    expect(flushed?.rowCount).toBe(2);
    expect(await writer.close()).toBeUndefined();
    expect((await client.readTable("people")).length).toBe(2);
  });

  it("streams live query changes and stops after close", async () => {
    const client = connect();
    await createPeopleTable(client);
    await client.insert("people", { id: 1, name: "Ada", joined: new Date() });
    const live = client.liveQueries();
    const changes: QueryResult[] = [];
    const subscription = await live.subscribe("SELECT name FROM people ORDER BY name", {
      onChange: (result) => changes.push(result),
    });
    expect(subscription.dependencyTableIds.length).toBe(1);
    expect(changes.length).toBe(1);
    expect(changes[0]?.rows).toEqual([{ name: "Ada" }]);
    await client.insert("people", { id: 2, name: "Grace", joined: new Date() });
    await live.refresh();
    expect(changes.length).toBe(2);
    expect(changes[1]?.rows).toEqual([{ name: "Ada" }, { name: "Grace" }]);
    // Change events cross as columnar frames too; the rows arrive rebuilt, Dates included.
    const dated = await live.subscribe("SELECT name, joined FROM people ORDER BY name", {
      onChange: (result) => changes.push(result),
    });
    expect(changes[2]?.columns).toEqual(["name", "joined"]);
    expect(changes[2]?.rows.map((row) => row.name)).toEqual(["Ada", "Grace"]);
    expect(changes[2]?.rows.every((row) => row.joined instanceof Date)).toBe(true);
    await dated.close();
    await subscription.close();
    await client.insert("people", { id: 3, name: "Edsger", joined: new Date() });
    await live.refresh();
    expect(changes.length).toBe(3);
    const stats = await live.stats();
    expect(stats.sweeps).toBeGreaterThanOrEqual(1);
    await live.close();
  });

  it("serves a caller-constructed database through exposeDatabase", async () => {
    const { clientSide, workerSide } = createBoundary();
    let closeCalls = 0;
    class TrackingDatabase extends MinnowDatabase {
      override close(): Promise<void> {
        closeCalls += 1;
        return super.close();
      }
    }
    const database = new TrackingDatabase(new MemoryBlockStore());
    let disposed = false;
    exposeDatabase(database, workerSide, {
      onDispose: () => {
        disposed = true;
      },
    });
    const client = new MinnowDatabaseClient(clientSide);
    await client.ready();
    await createPeopleTable(client);
    await client.insert("people", { id: 1, name: "Ada", joined: new Date() });
    expect((await client.readTable("people")).length).toBe(1);
    await client.close();
    expect(closeCalls).toBe(1);
    expect(disposed).toBe(true);
    await expect(client.listTables()).rejects.toThrow(/closed/);
  });

  it("forwards page-visibility reports to the host's onVisibility seam", async () => {
    const { clientSide, workerSide } = createBoundary();
    const reports: boolean[] = [];
    exposeDatabase(new MinnowDatabase(new MemoryBlockStore()), workerSide, {
      onVisibility: (visible) => {
        reports.push(visible);
      },
    });
    const client = new MinnowDatabaseClient(clientSide);
    await client.ready();
    // Node has no document, so the client sends nothing on its own; drive the frame the way
    // the browser listener does. This is the store's leadership-preference channel — the OPFS
    // store's setForeground rides on it.
    await (
      client as unknown as {
        _invoke(handleId: string | null, method: string, args: unknown[]): Promise<unknown>;
      }
    )._invoke(null, "setVisibility", [true]);
    await (
      client as unknown as {
        _invoke(handleId: string | null, method: string, args: unknown[]): Promise<unknown>;
      }
    )._invoke(null, "setVisibility", [false]);
    expect(reports).toEqual([true, false]);
    await client.close();
  });

  it("rejects wire-supplied stage ops outside the whitelist", async () => {
    // Raw frames stand in for a hostile or buggy client: the real client only ever names the
    // four staged-mutation ops, so anything else (including Object.prototype members) must get
    // a failure reply instead of indexing into the session.
    const { clientSide, workerSide } = createBoundary();
    exposeDatabase(new MinnowDatabase(new MemoryBlockStore()), workerSide);
    const pending = new Map<string, (response: RpcResponse) => void>();
    clientSide.addEventListener("message", (event) => {
      const response = parseRpcResponse(event.data);
      if (response !== null && response.requestId !== null) {
        pending.get(response.requestId)?.(response);
      }
    });
    let nextRequestId = 0;
    const call = (handleId: string | null, method: string, args: unknown[]): Promise<RpcResponse> =>
      new Promise((resolve) => {
        const requestId = `raw-${String((nextRequestId += 1))}`;
        pending.set(requestId, resolve);
        clientSide.postMessage({
          version: protocolVersion,
          requestId,
          kind: "rpc-call",
          handleId,
          method,
          args,
        });
      });
    const opened = await call(null, "writeOpen", []);
    if (opened.kind !== "rpc-result") throw new Error("writeOpen failed");
    const { handleId } = opened.result as { handleId: string };
    for (const op of ["constructor", "toString", "hasOwnProperty", "query"]) {
      const reply = await call(handleId, "stage", [op, "people", { columns: {} }]);
      expect(reply.kind).toBe("rpc-failure");
      if (reply.kind === "rpc-failure") {
        expect(reply.error.message).toBe(`Unsupported write stage operation: ${op}`);
      }
    }
    expect((await call(handleId, "unknown", [])).kind).toBe("rpc-failure");
    // The scope survives the rejected frames and still aborts cleanly.
    expect((await call(handleId, "abort", [])).kind).toBe("rpc-result");

    expect((await call(null, "unknown", [])).kind).toBe("rpc-failure");
    expect((await call(null, "liveQueries", ["", {}])).kind).toBe("rpc-failure");
    expect((await call(null, "bufferedWriter", ["writer", "missing", { maxRows: 1 }])).kind).toBe(
      "rpc-result",
    );
    expect((await call("writer", "add", [{ value: 1 }])).kind).toBe("rpc-failure");
    expect((await call("writer", "unknown", [])).kind).toBe("rpc-failure");

    const snapshot = await call(null, "snapshotOpen", []);
    if (snapshot.kind !== "rpc-result") throw new Error("snapshotOpen failed");
    const snapshotId = (snapshot.result as { handleId: string }).handleId;
    expect((await call(snapshotId, "unknown", [])).kind).toBe("rpc-failure");
    expect((await call(snapshotId, "close", [])).kind).toBe("rpc-result");

    expect((await call(null, "queryCursorOpen", ["cursor", "SELECT 1 AS n", {}])).kind).toBe(
      "rpc-result",
    );
    expect((await call("cursor", "unknown", [])).kind).toBe("rpc-failure");
    expect((await call("cursor", "close", [])).kind).toBe("rpc-result");

    expect((await call(null, "liveQueries", ["live", {}])).kind).toBe("rpc-result");
    expect((await call(null, "liveQueries", ["live", {}])).kind).toBe("rpc-failure");
    expect((await call("live", "unknown", [])).kind).toBe("rpc-failure");
    expect((await call("live", "observe", ["observed", "SELECT 1 AS n"])).kind).toBe("rpc-result");
    expect((await call("observed", "unknown", [])).kind).toBe("rpc-failure");
    expect((await call("observed", "close", [])).kind).toBe("rpc-result");
    expect((await call("live", "close", [])).kind).toBe("rpc-result");

    const exported = await call(null, "exportSnapshotOpen", []);
    if (exported.kind !== "rpc-result") throw new Error("exportSnapshotOpen failed");
    const exportId = (exported.result as { handleId: string }).handleId;
    expect((await call(exportId, "unknown", [])).kind).toBe("rpc-failure");
    expect((await call(exportId, "close", [])).kind).toBe("rpc-result");

    expect((await call(null, "dispose", [])).kind).toBe("rpc-result");
  });

  it("rejects calls after dispose and unknown methods cleanly", async () => {
    const client = connect();
    await client.ready();
    await expect(
      (
        client as unknown as { _invoke(h: string, m: string, a: unknown[]): Promise<unknown> }
      )._invoke("missing-handle", "close", []),
    ).rejects.toThrow(/Unknown handle/);
    await client.close();
    await expect(client.listTables()).rejects.toThrow(/closed/);
  });
});
