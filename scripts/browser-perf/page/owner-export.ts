/** BENCH fixture only: real ProtocolClient (page) -> OwnerServer -> composeEngine -> SQL lowerer
 * -> production executor/result path. Catalog and immutable snapshot descriptors replace B3 root
 * discovery; OPFS pages are real. No stand-in frame handle or credit loop. Setup is not timed.
 */
import type { WritePath } from "../../../packages/core/src/next/access/write-path.js";
import type { Snapshot } from "../../../packages/core/src/next/access/snapshot.js";
import { RequestLedger } from "../../../packages/core/src/next/format/ledger.js";
import { createExecutor } from "../../../packages/core/src/next/exec/create.js";
import { EXECUTOR_COSTS, type Executor } from "../../../packages/core/src/next/exec/executor.js";
import { frameByteLength, type FramePayload } from "../../../packages/core/src/next/exec/result.js";
import type { ReturnPool } from "../../../packages/core/src/next/exec/return-pool.js";
import { chunkReads } from "../../../packages/core/src/next/exec/scan.js";
import { createLowerer } from "../../../packages/core/src/next/lower/create.js";
import { MemoryCatalog } from "../../../packages/core/src/next/lower/memory-catalog.js";
import type { MemoryGovernor } from "../../../packages/core/src/next/memory/governor.js";
import { composeEngine } from "../../../packages/core/src/next/owner/compose.js";
import { payloadBytes } from "../../../packages/core/src/next/owner/frame.js";
import { OwnerServer } from "../../../packages/core/src/next/owner/server.js";
import {
  DEFAULT_RESULT_WINDOW_BYTES,
  type OwnerMessage,
} from "../../../packages/core/src/next/owner/protocol.js";
import type { ExtentReader } from "../../../packages/core/src/next/storage/extents.js";
import type { PhysicalType } from "../../../packages/core/src/next/types.js";
import type { ExportGroup, ExportResult } from "./export-stream.js";
import type { ExecutorExportStats } from "./executor-export.js";
import { outputCapacity } from "../../../packages/core/src/next/exec/return-pool.js";
import { traceReturnPool } from "./return-pool-trace.js";
import { benchSnapshotOf } from "./scan-fixture.js";

/** R3: last frames may be inline in done, including an empty terminal message. */
export function carriedFrames(message: OwnerMessage): readonly FramePayload[] {
  return message.t === "rows" || message.t === "done" ? message.frames : [];
}

function sqlType(type: PhysicalType): string {
  switch (type.kind) {
    case "int32":
      return type.bits === 16 ? "SMALLINT" : "INTEGER";
    case "int64":
      return "BIGINT";
    case "float64":
      return "DOUBLE PRECISION";
    case "float32":
      return "REAL";
    case "bool":
      return "BOOLEAN";
    case "text":
      return "TEXT";
    case "date":
      return "DATE";
    case "timestamp":
      return type.zoned ? "TIMESTAMPTZ" : "TIMESTAMP";
    case "decimal":
      return type.precision === null || type.scale === null
        ? "NUMERIC"
        : `NUMERIC(${String(type.precision)},${String(type.scale)})`;
    default:
      throw new Error(`Owner export fixture does not declare ${type.kind}`);
  }
}
export function prepareOwnerExport(
  space: ExtentReader,
  groups: readonly ExportGroup[],
  options: {
    readonly governor: MemoryGovernor;
    readonly poolTrace?: boolean;
    readonly port: MessagePort;
    readonly memory: () => Record<string, number>;
  },
) {
  const unsupported = (): never => {
    throw new Error("Read-only BENCH fixture operation unavailable");
  };
  const { table, pages } = benchSnapshotOf(space, groups);
  const catalog = new MemoryCatalog(),
    planner = createLowerer();
  const ddl = planner.lower(
    `CREATE TABLE t (${table.columns.map((c) => `${c.name} ${sqlType(c.type)}`).join(",")})`,
    { catalog: catalog.view, costs: EXECUTOR_COSTS, statistics: { table: () => null } },
  );
  if (ddl.kind !== "ddl" || ddl.change?.kind !== "createTable")
    throw new Error("Missing fixture DDL");
  catalog.apply({
    ...ddl.change,
    table: {
      ...ddl.change.table,
      columns: ddl.change.table.columns.map((column, k) => ({
        ...column,
        id: table.columns[k]?.id ?? unsupported(),
      })),
    },
  });
  const definition = catalog.view.tableByName("t");
  if (!definition) throw new Error("Missing fixture catalog");
  let pins = 0;
  const writePath: WritePath = {
    durability: "strict",
    appliedLsn: 0,
    durableLsn: 0,
    catalog: catalog.view,
    ledger: new RequestLedger(),
    sequences: { next: unsupported, set: unsupported },
    failure: null,
    pin: () => {
      let released = false;
      pins++;
      const snapshot: Snapshot = {
        rootSequence: 1,
        bound: 0,
        observedLsn: 0,
        catalog: catalog.view,
        pages,
        get released() {
          return released;
        },
        table: (id) => {
          if (released || id !== definition.id) return unsupported();
          return { ...table, tableId: definition.id, columns: definition.columns };
        },
        index: unsupported,
        release: () => {
          if (!released) {
            released = true;
            pins--;
          }
        },
      };
      return snapshot;
    },
    begin: unsupported,
    durable: () => null,
    flush: async () => undefined,
    stats: unsupported,
    statistics: () => null,
    close: async () => {
      if (pins !== 0) throw new Error("Fixture pin leak");
    },
  };
  let finish: ((result: ExportResult & { executorStats: ExecutorExportStats }) => void) | null =
    null;
  let fail: ((error: unknown) => void) | null = null;
  let start = 0,
    frames = 0,
    wireBytes = 0,
    frameBufferBytes = 0,
    transferredBuffers = 0;
  let maxFrameRows = 0,
    maxFrameBytes = 0,
    maxPostedBytes = 0,
    postedBytes = 0,
    postMs = 0;
  const posted = new Map<number, number>();
  let maxima: Record<string, number> = {},
    samples: Array<Record<string, number>> = [];
  const sample = () => {
    const point = { frames, postedBytes, ...options.memory() };
    for (const [key, value] of Object.entries(point))
      maxima[key] = Math.max(maxima[key] ?? 0, value);
    if (frames % 32 === 0) samples.push(point);
  };
  const real = createExecutor();
  const executor: Executor = {
    prepare: (plan) => {
      const prepared = real.prepare(plan);
      return {
        plan: prepared.plan,
        columns: prepared.columns,
        kind: prepared.kind,
        usesSequences: prepared.usesSequences,
        bind: prepared.bind.bind(prepared),
        execute: (request) => {
          const execution = prepared.execute(request);
          Promise.all([execution.executed, execution.delivered]).then(
            ([summary]) => {
              sample();
              const complete = finish;
              finish = null;
              complete?.({
                rows: summary.rowCount,
                ms: performance.now() - start,
                loadMs: 0,
                openMs: 0,
                loads: 0,
                executorStats: {
                  path: "production-owner-api",
                  outputRecycling: true,
                  frames,
                  wireBytes,
                  frameBufferBytes,
                  transferredBuffers,
                  maxFrameRows,
                  maxFrameBytes,
                  maxPostedBytes,
                  resultWindowBytes: DEFAULT_RESULT_WINDOW_BYTES,
                  readWindowPolicy: "executor-default",
                  // Pull/drain internals are intentionally not monkey-patched; zero means uninstrumented.
                  nextMs: 0,
                  postMs,
                  drainMs: 0,
                  profile: summary.profile,
                  memory: { maxima, samples },
                },
              });
            },
            (error) => {
              const reject = fail;
              finish = null;
              fail = null;
              reject?.(error);
            },
          );
          return execution;
        },
      };
    },
  };
  const engine = composeEngine({
    planner,
    executor,
    writePath,
    governor: options.governor,
    reads: chunkReads,
    spill: { bytesInUse: 0, create: unsupported, sweep: async () => undefined },
  });
  let pool: ReturnPool | null = null;
  const createPool = engine.createReturnPool.bind(engine);
  let trace: ReturnType<typeof traceReturnPool> | null = null;
  let pass = 0;
  let policy: ((bytes: number) => number) | undefined;
  let requestedBuffers = 0,
    hitsAtArm = 0;
  engine.createReturnPool = (cap) => {
    pool = createPool(cap);
    if (policy) pool.outputSize = policy;
    const take = pool.take.bind(pool);
    pool.take = (bytes) => {
      if (bytes > 0) requestedBuffers++;
      return take(bytes);
    };
    if (options.poolTrace) trace = traceReturnPool(pool);
    return pool;
  };
  const port = options.port,
    post = port.postMessage.bind(port);
  port.postMessage = (raw: unknown, transfer?: Transferable[] | StructuredSerializeOptions) => {
    const message = raw as OwnerMessage;
    if (message.t === "error") {
      const reject = fail;
      finish = null;
      fail = null;
      reject?.(new Error(message.error.message));
    }
    const framesInMessage = carriedFrames(message);
    if (framesInMessage.length > 0) {
      for (const frame of framesInMessage) {
        const bytes = frameByteLength(frame);
        frames++;
        wireBytes += payloadBytes(frame);
        frameBufferBytes += bytes;
        maxFrameRows = Math.max(maxFrameRows, frame.rowCount);
        maxFrameBytes = Math.max(maxFrameBytes, bytes);
        posted.set(frame.sequence, bytes);
        postedBytes += bytes;
        maxPostedBytes = Math.max(maxPostedBytes, postedBytes);
      }
      if (Array.isArray(transfer)) transferredBuffers += transfer.length;
    }
    const before = performance.now();
    if (Array.isArray(transfer)) post(raw, transfer);
    else post(raw, transfer);
    postMs += performance.now() - before;
    sample();
  };
  const server = new OwnerServer(engine, { ownerId: "export-bench", epoch: 1, build: "bench" });
  server.accept(port);
  const receive = port.onmessage?.bind(port);
  port.onmessage = (event) => {
    const message = event.data as {
      t: string;
      through?: number;
      releases?: Array<{ through: number }>;
    };
    const credits =
      message.t === "release" && message.through !== undefined
        ? [message.through]
        : (message.releases ?? []).map((item) => item.through);
    for (const through of credits)
      for (const [sequence, bytes] of posted)
        if (sequence <= through) {
          posted.delete(sequence);
          postedBytes -= bytes;
        }
    receive?.(event);
  };
  return {
    arm: (outputClasses?: boolean) => {
      if (finish) throw new Error("Owner export is already armed");
      // Bench A/B only: each policy starts with an empty bounded pool on the same real
      // connection. Expire old capabilities; late returns are dropped, never awaited.
      if (outputClasses !== undefined) {
        policy = outputClasses ? outputCapacity : (bytes) => bytes;
        pool?.evict(Infinity);
        if (pool) pool.outputSize = policy;
      }
      requestedBuffers = 0;
      hitsAtArm = pool?.hits ?? 0;
      trace?.begin(pass);
      pass++;
      frames =
        wireBytes =
        frameBufferBytes =
        transferredBuffers =
        maxFrameRows =
        maxFrameBytes =
        maxPostedBytes =
        postedBytes =
        postMs =
          0;
      posted.clear();
      maxima = {};
      samples = [];
      start = performance.now();
      return new Promise<ExportResult & { executorStats: ExecutorExportStats }>(
        (resolve, reject) => {
          finish = resolve;
          fail = reject;
        },
      );
    },
    returnStats: () => ({
      returnPoolMiB: (pool?.evictableBytes ?? 0) / 1048576,
      returnHits: pool?.hits ?? 0,
      requestedBuffers,
      reusedBuffers: (pool?.hits ?? 0) - hitsAtArm,
      returnAccepted: pool?.accepted ?? 0,
      ...(trace ? { poolTrace: trace.summary() } : {}),
    }),
    close: async () => {
      const stop = server.quiesce("closing");
      await stop.drained;
      stop.finish();
      await engine.close();
      port.close();
    },
  };
}
