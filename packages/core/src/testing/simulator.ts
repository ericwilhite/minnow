import { MinnowDatabase, type GarbageCollectionResult } from "../engine/database.js";
import { MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { FaultInjectingBlockStore, type FaultPoint } from "./index.js";
import { md5Hex } from "./sqllogictest.js";

export interface SimulationPut {
  readonly kind: "put";
  readonly client: number;
  readonly key: number;
  readonly amount: number;
}

export interface SimulationDelete {
  readonly kind: "delete";
  readonly client: number;
  readonly key: number;
}

export type SimulationMutation = SimulationPut | SimulationDelete;

export type SimulationStep =
  | { readonly kind: "concurrent"; readonly operations: readonly SimulationMutation[] }
  | { readonly kind: "checkpoint" }
  | { readonly kind: "maintenance" }
  | {
      readonly kind: "fault";
      readonly point: FaultPoint;
      readonly occurrence: number;
      readonly operation: SimulationMutation;
    };

export interface SimulationPlan {
  readonly version: 1;
  readonly seed: number;
  readonly clients: number;
  readonly keySpace: number;
  readonly steps: readonly SimulationStep[];
}

export interface SimulationTraceEvent {
  readonly sequence: number;
  readonly operation: string;
  readonly outcome: "resolved" | "rejected";
}

export interface SimulationResult {
  readonly seed: number;
  readonly acceptedMutations: number;
  readonly rejectedConflicts: number;
  readonly injectedFaults: number;
  readonly checkpoints: number;
  readonly maintenanceCompactions: number;
  readonly compactionStop: string;
  readonly collectionPasses: number;
  readonly collectionTail: readonly CollectionPassSummary[];
  readonly rows: ReadonlyArray<{
    readonly id: number;
    readonly client: number;
    readonly amount: number;
  }>;
  readonly blockCount: number;
  readonly segmentCount: number;
  readonly transactionCount: number;
  readonly manifestCount: number;
  readonly leaseCount: number;
  readonly storageBytes: number;
  readonly schedulerHighWater: number;
  readonly trace: readonly SimulationTraceEvent[];
  readonly traceDigest: string;
}

export interface SimulationOptions {
  /** Bounded diagnostic tail; old events are evicted as the simulator runs. */
  readonly traceEvents?: number;
}

export interface CollectionPassSummary {
  readonly prunedManifests: number;
  readonly retainedManifests: number;
  readonly reclaimedSegments: number;
  readonly retainedSegments: number;
  readonly reclaimedBlocks: number;
  readonly retainedBlocks: number;
  readonly reclaimedTransactions: number;
  readonly retainedTransactions: number;
}

async function collectStoragePages<Item, Cursor>(
  read: (cursor: Cursor | null) => Promise<{ records: Item[]; nextCursor: Cursor | null }>,
): Promise<Item[]> {
  const records: Item[] = [];
  let cursor: Cursor | null = null;
  for (;;) {
    const page = await read(cursor);
    records.push(...page.records);
    if (page.nextCursor === null) return records;
    cursor = page.nextCursor;
  }
}

interface PendingOperation<T = unknown> {
  readonly label: string;
  readonly run: () => Promise<T>;
  readonly resolve: (value: T) => void;
  readonly reject: (reason: unknown) => void;
}

/** Chooses every storage completion from a seeded queue and keeps only a bounded trace tail. */
export class DeterministicScheduler {
  readonly #random: () => number;
  readonly #traceLimit: number;
  readonly #pending: PendingOperation[] = [];
  readonly #trace: SimulationTraceEvent[] = [];
  #sequence = 0;
  #highWater = 0;

  constructor(seed: number, traceLimit = 256) {
    if (!Number.isSafeInteger(traceLimit) || traceLimit < 0) {
      throw new RangeError("Simulator trace limit must be a non-negative whole number");
    }
    this.#random = mulberry32(seed);
    this.#traceLimit = traceLimit;
  }

  get highWater(): number {
    return this.#highWater;
  }

  get trace(): readonly SimulationTraceEvent[] {
    return this.#trace;
  }

  schedule<T>(label: string, run: () => Promise<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      this.#pending.push({ label, run, resolve, reject } as PendingOperation);
      this.#highWater = Math.max(this.#highWater, this.#pending.length);
    });
  }

  async settle<T>(promises: ReadonlyArray<Promise<T>>): Promise<Array<PromiseSettledResult<T>>> {
    const state = { complete: false };
    const settled = Promise.allSettled(promises).then((results) => {
      state.complete = true;
      return results;
    });
    let emptyTurns = 0;
    while (!state.complete) {
      await Promise.resolve();
      const pending = this.#pending.length;
      if (pending === 0) {
        // Production maintenance deliberately yields through a timer between bounded pages.
        // A microtask-only poll can therefore declare deadlock while the next scheduled store
        // call is merely waiting for its event-loop turn. Let timers advance before counting an
        // empty turn; the chosen storage completion order remains seeded and deterministic.
        await nextEventLoopTurn();
        if (this.#pending.length > 0) {
          emptyTurns = 0;
          continue;
        }
        emptyTurns++;
        if (emptyTurns > 100) {
          throw new Error("Deterministic simulator deadlocked with no scheduled storage operation");
        }
        continue;
      }
      emptyTurns = 0;
      const index = Math.floor(this.#random() * pending);
      const operation = this.#pending.splice(index, 1)[0];
      if (operation === undefined) continue;
      try {
        const value = await operation.run();
        operation.resolve(value);
        this.#record(operation.label, "resolved");
      } catch (error) {
        operation.reject(error);
        this.#record(operation.label, "rejected");
      }
    }
    return settled;
  }

  #record(operation: string, outcome: SimulationTraceEvent["outcome"]): void {
    this.#sequence++;
    if (this.#traceLimit === 0) return;
    this.#trace.push({ sequence: this.#sequence, operation, outcome });
    if (this.#trace.length > this.#traceLimit) this.#trace.shift();
  }
}

function nextEventLoopTurn(): Promise<void> {
  if (typeof MessageChannel === "undefined") {
    return new Promise((resolve) => setTimeout(resolve, 0));
  }
  return new Promise((resolve) => {
    const channel = new MessageChannel();
    channel.port1.onmessage = () => {
      channel.port1.close();
      channel.port2.close();
      resolve();
    };
    channel.port2.postMessage(undefined);
  });
}

/** Wraps every asynchronous BlockStore entry point in the deterministic completion scheduler. */
export function scheduledBlockStore(
  store: BlockStore,
  scheduler: DeterministicScheduler,
): BlockStore {
  return new Proxy(store, {
    get(target, property) {
      const value: unknown = Reflect.get(target, property, target);
      if (typeof value !== "function") return value;
      const method = value as (...arguments_: unknown[]) => unknown;
      if (property === "close")
        return (...arguments_: unknown[]) => method.apply(target, arguments_);
      return (...arguments_: unknown[]) =>
        scheduler.schedule(String(property), async () => method.apply(target, arguments_));
    },
  });
}

export function generateSimulationPlan(
  seed: number,
  options: { readonly rounds?: number; readonly clients?: number; readonly keySpace?: number } = {},
): SimulationPlan {
  if (!Number.isSafeInteger(seed)) {
    throw new RangeError("Simulator seed must be a whole number");
  }
  const rounds = options.rounds ?? 48;
  const clients = options.clients ?? 4;
  const keySpace = options.keySpace ?? 32;
  if (!Number.isSafeInteger(rounds) || rounds < 1 || rounds > 100_000) {
    throw new RangeError("Simulator rounds must be between 1 and 100,000");
  }
  if (!Number.isSafeInteger(clients) || clients < 1 || clients > 32) {
    throw new RangeError("Simulator clients must be between 1 and 32");
  }
  if (!Number.isSafeInteger(keySpace) || keySpace < 1 || keySpace > 10_000) {
    throw new RangeError("Simulator key space must be between 1 and 10,000");
  }
  if (keySpace < clients) {
    throw new RangeError("Simulator key space must be at least the client count");
  }
  const random = mulberry32(seed);
  const steps: SimulationStep[] = [];
  const faultPoints: readonly FaultPoint[] = [
    "beforeBlockWrite",
    "afterBlockWrite",
    "beforeTransactionCommit",
    "afterTransactionCommit",
  ];
  for (let round = 0; round < rounds; round++) {
    const operations: SimulationMutation[] = [];
    const start = Math.floor(random() * keySpace);
    let stride = Math.floor(random() * keySpace) + 1;
    while (greatestCommonDivisor(stride, keySpace) !== 1) stride++;
    for (let client = 0; client < clients; client++) {
      // A round is a genuinely concurrent commit race, but its row mutations commute. That gives
      // the reference model one exact answer instead of guessing which same-key promise linearized
      // last. Contention still happens at the shared manifest/transaction boundary.
      const key = ((start + client * stride) % keySpace) + 1;
      if (random() < 0.18) operations.push({ kind: "delete", client, key });
      else operations.push({ kind: "put", client, key, amount: Math.floor(random() * 1_000_000) });
    }
    steps.push({ kind: "concurrent", operations });
    if (round % 4 === 3) steps.push({ kind: "checkpoint" });
    if (round % 12 === 11) steps.push({ kind: "maintenance" });
    if (round % 20 === 19) {
      const point = faultPoints[Math.floor(random() * faultPoints.length)] ?? "beforeBlockWrite";
      steps.push({
        kind: "fault",
        point,
        occurrence: 1,
        operation: {
          kind: "put",
          client: Math.floor(random() * clients),
          key: Math.floor(random() * keySpace) + 1,
          amount: Math.floor(random() * 1_000_000),
        },
      });
    }
  }
  steps.push({ kind: "checkpoint" }, { kind: "maintenance" }, { kind: "checkpoint" });
  return { version: 1, seed, clients, keySpace, steps };
}

export function parseSimulationPlan(source: string): SimulationPlan {
  const value: unknown = JSON.parse(source);
  if (typeof value !== "object" || value === null)
    throw new TypeError("Simulation plan must be an object");
  const plan = value as Partial<SimulationPlan>;
  if (plan.version !== 1) throw new TypeError("Simulation plan version must be 1");
  if (!Number.isSafeInteger(plan.seed))
    throw new TypeError("Simulation seed must be a whole number");
  if (!Number.isSafeInteger(plan.clients) || (plan.clients ?? 0) < 1 || (plan.clients ?? 0) > 32) {
    throw new TypeError("Simulation client count is invalid");
  }
  if (
    !Number.isSafeInteger(plan.keySpace) ||
    (plan.keySpace ?? 0) < 1 ||
    (plan.keySpace ?? 0) > 10_000
  ) {
    throw new TypeError("Simulation key space is invalid");
  }
  if ((plan.keySpace ?? 0) < (plan.clients ?? 0)) {
    throw new TypeError("Simulation key space must be at least the client count");
  }
  if (!Array.isArray(plan.steps) || plan.steps.length > 200_000) {
    throw new TypeError("Simulation steps must be a bounded array");
  }
  validateSteps(plan.steps, plan.clients ?? 0, plan.keySpace ?? 0);
  return plan as SimulationPlan;
}

/** Runs a replayable state-machine plan through production SQL, transactions, and block storage. */
export async function runSimulation(
  plan: SimulationPlan,
  options: SimulationOptions = {},
): Promise<SimulationResult> {
  parseSimulationPlan(JSON.stringify(plan));
  const base = new MemoryBlockStore();
  const faults = new FaultController();
  const scheduler = new DeterministicScheduler(plan.seed ^ 0x5f3759df, options.traceEvents ?? 256);
  const store = scheduledBlockStore(
    new FaultInjectingBlockStore(base, (point) => faults.inject(point)),
    scheduler,
  );
  let clock = Date.parse("2026-01-01T00:00:00.000Z");
  let id = 0;
  const createDatabase = (): MinnowDatabase =>
    new MinnowDatabase(store, {
      compression: "raw",
      rowsPerBlock: 8,
      maxCommitRetries: 16,
      now: () => new Date(clock),
      createId: () => `sim-${String(id++)}`,
      bufferPoolBytes: 0,
      autoCompact: false,
      autoCollect: false,
    });
  const setup = createDatabase();
  await drive(
    scheduler,
    setup.execute(
      "CREATE TABLE items (id INTEGER PRIMARY KEY, client INTEGER NOT NULL, amount INTEGER NOT NULL)",
    ),
  );
  await drive(scheduler, setup.close());
  const clients = Array.from({ length: plan.clients }, createDatabase);
  const model = new Map<number, { id: number; client: number; amount: number }>();
  let acceptedMutations = 0;
  let rejectedConflicts = 0;
  let injectedFaults = 0;
  let checkpoints = 0;

  for (const step of plan.steps) {
    clock += 1_000;
    if (step.kind === "concurrent") {
      const operations = step.operations.map((operation) =>
        executeMutation(requireClient(clients, operation.client), operation)
          .then(() => {
            applyMutation(model, operation);
            acceptedMutations++;
          })
          .catch((error: unknown) => {
            if (/manifest changed|conflict/iu.test(errorMessage(error))) {
              rejectedConflicts++;
              return;
            }
            throw error;
          }),
      );
      assertSettled(await scheduler.settle(operations));
      continue;
    }
    if (step.kind === "checkpoint") {
      await assertModel(scheduler, clients[0] ?? setup, model);
      checkpoints++;
      continue;
    }
    if (step.kind === "maintenance") {
      const database = clients[0] ?? setup;
      await drive(scheduler, database.compactTable("items"));
      // One pass can remove obsolete manifests/segments while their transaction journals are
      // still provenance roots. The next pass can reclaim those now-unreferenced journals.
      await drive(scheduler, database.collectGarbage());
      await drive(scheduler, database.collectGarbage());
      continue;
    }

    const before = new Map(model);
    const after = new Map(model);
    applyMutation(after, step.operation);
    faults.arm(step.point, step.occurrence);
    const result = await scheduler.settle([
      executeMutation(requireClient(clients, step.operation.client), step.operation),
    ]);
    if (!faults.fired) throw new Error(`Simulation fault was not reached: ${step.point}`);
    injectedFaults++;
    faults.disarm();
    const failedClient = requireClient(clients, step.operation.client);
    await drive(scheduler, failedClient.close());
    const replacement = createDatabase();
    clients[step.operation.client] = replacement;
    const actual = await readRows(scheduler, replacement);
    if (sameRows(actual, before)) {
      if (result[0]?.status === "fulfilled") {
        throw new Error(`Fault ${step.point} returned success but did not persist its mutation`);
      }
      replaceModel(model, before);
    } else if (sameRows(actual, after)) replaceModel(model, after);
    else
      throw new Error(
        `Fault ${step.point} left a state that was neither before nor after the mutation`,
      );
  }

  await assertModel(scheduler, clients[0] ?? setup, model);
  const closeResults = await scheduler.settle(clients.map((client) => client.close()));
  assertSettled(closeResults);

  // Measure eventual storage after the simulated tabs release their deliberately cached reader
  // leases. A fresh maintenance client can then collapse history without an artificial pin from
  // the workload whose final state has already been checked.
  const maintenance = createDatabase();
  const compacted = await compactToFixedPoint(scheduler, maintenance, plan.steps.length);
  const maintenanceCompactions = compacted.passes;
  const compactionStop = compacted.stop;
  const collected = await collectToFixedPoint(scheduler, maintenance, plan.steps.length);
  let collectionPasses = collected.passes;
  const collectionTail = [...collected.tail];
  await assertModel(scheduler, maintenance, model);
  await drive(scheduler, maintenance.close());
  const sweeper = createDatabase();
  const swept = await collectToFixedPoint(scheduler, sweeper, plan.steps.length);
  collectionPasses += swept.passes;
  collectionTail.push(...swept.tail);
  if (collectionTail.length > 8) collectionTail.splice(0, collectionTail.length - 8);
  await drive(scheduler, sweeper.close());

  const rows = sortedRows(model);
  const [segments, transactions, manifests, currentManifest, leases, compactions, storageBytes] =
    await Promise.all([
      collectStoragePages((cursor: string | null) => base.listSegmentPage(cursor, 256)),
      collectStoragePages((cursor: string | null) => base.listTransactionPage(cursor, 256)),
      collectStoragePages((cursor: number | null) => base.listManifestPage(cursor, 256)),
      base.getCurrentManifest(),
      base.listLeases(),
      base.listCompactionJobs(),
      base.getLogicalStorageBytes(),
    ]);
  const blockCount = currentManifest?.liveBlockCount ?? 0;
  const blockLimit = plan.keySpace * 16 + 64;
  const segmentLimit = plan.keySpace + 16;
  const transactionLimit = segmentLimit + 16;
  if (
    blockCount > blockLimit ||
    segments.length > segmentLimit ||
    transactions.length > transactionLimit
  ) {
    const transactionStates = transactions.reduce<Record<string, number>>((counts, transaction) => {
      counts[transaction.status] = (counts[transaction.status] ?? 0) + 1;
      return counts;
    }, {});
    const compactionStates = compactions.reduce<Record<string, number>>((counts, compaction) => {
      counts[compaction.state] = (counts[compaction.state] ?? 0) + 1;
      return counts;
    }, {});
    throw new Error(
      `Storage did not compact to a bound: ${String(blockCount)}/${String(blockLimit)} live blocks, ` +
        `${String(segments.length)}/${String(segmentLimit)} segments, ` +
        `${String(transactions.length)}/${String(transactionLimit)} transactions; ` +
        `${JSON.stringify(transactionStates)} transaction states, ` +
        `${JSON.stringify(compactionStates)} compaction states, ` +
        `${String(currentManifest?.liveBlockCount ?? 0)} current blocks, compaction stopped at ${compactionStop}, ` +
        `${String(manifests.length)} manifests (${String(manifests.filter((manifest) => manifest.prunedAt !== undefined).length)} pruned) remain after ${String(maintenanceCompactions)} compactions ` +
        `and ${String(collectionPasses)} collection passes; collection tail ${JSON.stringify(collectionTail)}`,
    );
  }
  if (manifests.length > 2) {
    throw new Error(
      `Garbage collection left ${String(manifests.length)} manifests and ${String(leases.length)} leases`,
    );
  }
  if (leases.length !== 0) {
    throw new Error(`Simulation clients leaked ${String(leases.length)} leases`);
  }
  const trace = [...scheduler.trace];
  return {
    seed: plan.seed,
    acceptedMutations,
    rejectedConflicts,
    injectedFaults,
    checkpoints,
    maintenanceCompactions,
    compactionStop,
    collectionPasses,
    collectionTail,
    rows,
    blockCount,
    segmentCount: segments.length,
    transactionCount: transactions.length,
    manifestCount: manifests.length,
    leaseCount: leases.length,
    storageBytes,
    schedulerHighWater: scheduler.highWater,
    trace,
    traceDigest: md5Hex(JSON.stringify(trace)),
  };
}

class FaultController {
  #point: FaultPoint | undefined;
  #occurrence = 0;
  #seen = 0;
  fired = false;

  arm(point: FaultPoint, occurrence: number): void {
    this.#point = point;
    this.#occurrence = occurrence;
    this.#seen = 0;
    this.fired = false;
  }

  disarm(): void {
    this.#point = undefined;
  }

  inject(point: FaultPoint): void {
    if (point !== this.#point || this.fired) return;
    this.#seen++;
    if (this.#seen !== this.#occurrence) return;
    this.fired = true;
    throw new Error(`injected ${point} #${String(this.#occurrence)}`);
  }
}

async function drive<T>(scheduler: DeterministicScheduler, promise: Promise<T>): Promise<T> {
  const result = (await scheduler.settle([promise]))[0];
  if (result?.status === "fulfilled") return result.value;
  throw result?.reason ?? new Error("Scheduled operation did not settle");
}

function assertSettled(results: ReadonlyArray<PromiseSettledResult<unknown>>): void {
  const failed = results.find((result) => result.status === "rejected");
  if (failed?.status === "rejected") throw failed.reason;
}

function executeMutation(
  database: MinnowDatabase,
  operation: SimulationMutation,
): Promise<unknown> {
  if (operation.kind === "delete") {
    return database.execute("DELETE FROM items WHERE id = ?", [operation.key]);
  }
  return database.execute(
    "INSERT INTO items (id, client, amount) VALUES (?, ?, ?) " +
      "ON CONFLICT (id) DO UPDATE SET client = excluded.client, amount = excluded.amount",
    [operation.key, operation.client, operation.amount],
  );
}

function applyMutation(
  model: Map<number, { id: number; client: number; amount: number }>,
  operation: SimulationMutation,
): void {
  if (operation.kind === "delete") model.delete(operation.key);
  else
    model.set(operation.key, {
      id: operation.key,
      client: operation.client,
      amount: operation.amount,
    });
}

async function assertModel(
  scheduler: DeterministicScheduler,
  database: MinnowDatabase,
  model: ReadonlyMap<number, { id: number; client: number; amount: number }>,
): Promise<void> {
  const actual = await readRows(scheduler, database);
  if (!sameRows(actual, model)) {
    throw new Error(
      `Model mismatch:\nexpected ${JSON.stringify(sortedRows(model))}\nreceived ${JSON.stringify(actual)}`,
    );
  }
}

async function readRows(
  scheduler: DeterministicScheduler,
  database: MinnowDatabase,
): Promise<Array<{ id: number; client: number; amount: number }>> {
  const result = await drive(
    scheduler,
    database.query("SELECT id, client, amount FROM items ORDER BY id", { memoize: false }),
  );
  return result.rows as Array<{ id: number; client: number; amount: number }>;
}

function sameRows(
  rows: ReadonlyArray<{ id: number; client: number; amount: number }>,
  model: ReadonlyMap<number, { id: number; client: number; amount: number }>,
): boolean {
  return JSON.stringify(rows) === JSON.stringify(sortedRows(model));
}

function sortedRows(
  model: ReadonlyMap<number, { id: number; client: number; amount: number }>,
): Array<{ id: number; client: number; amount: number }> {
  return [...model.values()].sort((left, right) => left.id - right.id);
}

function replaceModel(
  target: Map<number, { id: number; client: number; amount: number }>,
  source: ReadonlyMap<number, { id: number; client: number; amount: number }>,
): void {
  target.clear();
  for (const [key, value] of source) target.set(key, value);
}

function requireClient(clients: readonly MinnowDatabase[], index: number): MinnowDatabase {
  const client = clients[index];
  if (client === undefined)
    throw new RangeError(`Simulation client is out of range: ${String(index)}`);
  return client;
}

function validateSteps(steps: readonly unknown[], clients: number, keySpace: number): void {
  const validPoint = new Set<FaultPoint>([
    "beforeBlockWrite",
    "afterBlockWrite",
    "beforeBlockRead",
    "afterBlockRead",
    "beforeTransactionCommit",
    "afterTransactionCommit",
  ]);
  const validateMutation = (value: unknown): void => {
    if (typeof value !== "object" || value === null) {
      throw new TypeError("Simulation mutation must be an object");
    }
    const operation = value as Record<string, unknown>;
    if (operation.kind !== "put" && operation.kind !== "delete") {
      throw new TypeError("Simulation mutation kind is invalid");
    }
    const client = operation.client;
    if (
      typeof client !== "number" ||
      !Number.isSafeInteger(client) ||
      client < 0 ||
      client >= clients
    ) {
      throw new TypeError("Simulation mutation has an invalid client");
    }
    const key = operation.key;
    if (typeof key !== "number" || !Number.isSafeInteger(key) || key < 1 || key > keySpace) {
      throw new TypeError("Simulation mutation has an invalid key");
    }
    if (
      operation.kind === "put" &&
      (typeof operation.amount !== "number" || !Number.isSafeInteger(operation.amount))
    ) {
      throw new TypeError("Simulation put has an invalid amount");
    }
  };
  for (const value of steps) {
    if (typeof value !== "object" || value === null) {
      throw new TypeError("Simulation step must be an object");
    }
    const step = value as Record<string, unknown>;
    if (step.kind === "concurrent") {
      if (!Array.isArray(step.operations) || step.operations.length > clients * 4) {
        throw new TypeError("Simulation concurrent step is unbounded");
      }
      step.operations.forEach(validateMutation);
    } else if (step.kind === "fault") {
      if (typeof step.point !== "string" || !validPoint.has(step.point as FaultPoint)) {
        throw new TypeError("Simulation fault point is invalid");
      }
      const occurrence = step.occurrence;
      if (typeof occurrence !== "number" || !Number.isSafeInteger(occurrence) || occurrence < 1) {
        throw new TypeError("Simulation fault occurrence is invalid");
      }
      validateMutation(step.operation);
    } else if (step.kind !== "checkpoint" && step.kind !== "maintenance") {
      throw new TypeError("Simulation step kind is invalid");
    }
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function compactToFixedPoint(
  scheduler: DeterministicScheduler,
  database: MinnowDatabase,
  workloadSteps: number,
): Promise<{ passes: number; stop: string }> {
  const maximumPasses = Math.min(10_000, workloadSteps + 128);
  for (let pass = 0; pass < maximumPasses; pass++) {
    const result = await drive(scheduler, database.compactTable("items"));
    if (!result.compacted) return { passes: pass, stop: result.skipReason ?? "unknown" };
  }
  throw new Error(`Compaction did not converge within ${String(maximumPasses)} passes`);
}

async function collectToFixedPoint(
  scheduler: DeterministicScheduler,
  database: MinnowDatabase,
  workloadSteps: number,
): Promise<{ passes: number; tail: CollectionPassSummary[] }> {
  const maximumPasses = Math.min(10_000, workloadSteps + 128);
  let emptyPasses = 0;
  const tail: CollectionPassSummary[] = [];
  for (let pass = 1; pass <= maximumPasses; pass++) {
    const result = await drive(scheduler, database.collectGarbage());
    tail.push(summarizeCollection(result));
    if (tail.length > 8) tail.shift();
    const changed =
      result.prunedManifestCount +
      result.reclaimedSegmentCount +
      result.reclaimedBlockCount +
      result.reclaimedTransactionCount;
    // collectGarbage also prunes finished maintenance records after returning its counters. A
    // zero-artifact pass can therefore remove the final provenance root that lets the next pass
    // reclaim bytes. Two consecutive empty passes prove both layers have settled.
    if (changed === 0) {
      emptyPasses++;
      if (emptyPasses === 2) return { passes: pass, tail };
    } else emptyPasses = 0;
  }
  throw new Error(`Garbage collection did not converge within ${String(maximumPasses)} passes`);
}

function summarizeCollection(result: GarbageCollectionResult): CollectionPassSummary {
  return {
    prunedManifests: result.prunedManifestCount,
    retainedManifests: result.retainedManifestCount,
    reclaimedSegments: result.reclaimedSegmentCount,
    retainedSegments: result.retainedSegmentCount,
    reclaimedBlocks: result.reclaimedBlockCount,
    retainedBlocks: result.retainedBlockCount,
    reclaimedTransactions: result.reclaimedTransactionCount,
    retainedTransactions: result.retainedTransactionCount,
  };
}

// Stream-identical copy of `mulberry32` in ./seeds.ts. This module ships in the tarball and
// seeds.ts does not (it reads MINNOW_SEED and the unpublished regression-seeds.json), so the
// published simulator cannot import the canonical copy.
function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = state;
    mixed = Math.imul(mixed ^ (mixed >>> 15), mixed | 1);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), mixed | 61);
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4_294_967_296;
  };
}

function greatestCommonDivisor(left: number, right: number): number {
  let a = left;
  let b = right;
  while (b !== 0) [a, b] = [b, a % b];
  return a;
}
