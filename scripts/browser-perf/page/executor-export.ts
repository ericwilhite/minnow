/** Gate A export: production Executor -> ResultSink/ResultChannel -> unchanged owner transfer
 * policy -> real MessagePort credits. Only plan/snapshot/catalog descriptors are bench fixtures;
 * SQL lowering, B3 catalog/root reads and client row-object construction are excluded. Preparation
 * happens before profiling. Execution admission, scanning, framing, posting and credit drain count.
 * No scan-window/ownership override: this measures the executor's current production defaults.
 */
import type { Snapshot } from "../../../packages/core/src/next/access/snapshot.js";
import { createExecutor } from "../../../packages/core/src/next/exec/create.js";
import type { Executor, ExecutionProfile } from "../../../packages/core/src/next/exec/executor.js";
import { chunkReads, type ChunkReads } from "../../../packages/core/src/next/exec/scan.js";
import type { ResultFrame } from "../../../packages/core/src/next/exec/result.js";
import type { PhysicalPlan } from "../../../packages/core/src/next/ir/ir.js";
import type { MemoryGovernor } from "../../../packages/core/src/next/memory/governor.js";
import { payloadBytes, transferFor } from "../../../packages/core/src/next/owner/frame.js";
import { DEFAULT_RESULT_WINDOW_BYTES } from "../../../packages/core/src/next/owner/protocol.js";
import type { ExtentReader } from "../../../packages/core/src/next/storage/extents.js";
import type { ExportGroup, ExportResult } from "./export-stream.js";
import { benchTableOf } from "./scan-fixture.js";

export interface ExecutorExportStats {
  readonly path: "production-executor";
  readonly frames: number;
  readonly wireBytes: number;
  readonly frameBufferBytes: number;
  readonly transferredBuffers: number;
  readonly maxFrameRows: number;
  readonly maxFrameBytes: number;
  readonly maxPostedBytes: number;
  readonly resultWindowBytes: number;
  readonly readWindowPolicy: "executor-default";
  readonly nextMs: number;
  readonly postMs: number;
  readonly drainMs: number;
  readonly profile: ExecutionProfile;
  readonly memory: {
    readonly maxima: Record<string, number>;
    readonly samples: Array<Record<string, number>>;
  };
}

/** A minimal fixture Snapshot; every unsupported operation fails instead of silently substituting
 * another execution path. It supplies the same immutable table/group descriptors as page mode.
 */
export function prepareExecutorExport(
  space: ExtentReader,
  groups: readonly ExportGroup[],
  options: {
    readonly governor: MemoryGovernor;
    /** Bench-only frozen executor for paired comparisons, selected before timing. */
    readonly executor?: Executor;
    readonly reads?: ChunkReads;
    readonly port: MessagePort;
    readonly memory?: () => Record<string, number>;
  },
): () => Promise<ExportResult & { executorStats: ExecutorExportStats }> {
  const table = benchTableOf(groups);
  const unsupported = (): never => {
    throw new Error("Export fixture does not provide SQL/catalog/index/spill access");
  };
  const columns = table.columns.map((c) => ({ name: c.name, type: c.type, nullable: c.nullable }));
  let released = false;
  const snapshot: Snapshot = {
    rootSequence: 1,
    bound: 0,
    observedLsn: 0,
    get released() {
      return released;
    },
    catalog: {
      version: 1,
      tables: [],
      sequences: [],
      views: [],
      types: [],
      table: unsupported,
      tableByName: unsupported,
      sequence: unsupported,
      type: unsupported,
      typeByName: unsupported,
    },
    pages: space,
    table: (id) => {
      if (released || id !== table.tableId) throw new Error("Invalid export snapshot table");
      return table;
    },
    index: unsupported,
    release: () => {
      released = true;
    },
  };
  const plan: PhysicalPlan = {
    root: {
      id: 1,
      op: "scan",
      tableId: table.tableId,
      columns: table.columns.map((c) => c.id),
      output: columns,
      filter: null,
      runtimeFilters: [],
      emitLocator: false,
    },
    columns,
    parameters: [],
    placeholderCount: 0,
    clockSlot: null,
    initPlans: [],
    catalogVersion: 1,
    volatile: false,
  };
  const prepared = (options.executor ?? createExecutor()).prepare(plan);
  return async () => {
    const started = performance.now(),
      port = options.port;
    const posted = new Map<number, { readonly frame: ResultFrame; readonly bytes: number }>();
    let highest = -1,
      through = -1,
      rows = 0,
      frames = 0,
      wireBytes = 0,
      frameBufferBytes = 0,
      transferredBuffers = 0,
      maxFrameRows = 0,
      maxFrameBytes = 0,
      postedBytes = 0,
      maxPostedBytes = 0,
      nextMs = 0,
      postMs = 0;
    const execution = prepared.execute({
      snapshot,
      statement: null,
      parameters: [],
      statementTime: 0n,
      reads: options.reads ?? chunkReads,
      governor: options.governor,
      spill: { bytesInUse: 0, create: unsupported, sweep: async () => undefined },
      queryId: 1,
      sequences: null,
      sequenceSession: null,
      returning: "atExecuted",
      resultWindowBytes: DEFAULT_RESULT_WINDOW_BYTES,
    });
    const maxima: Record<string, number> = {},
      samples: Array<Record<string, number>> = [];
    const sample = () => {
      const point = { rows, frames, postedBytes, ...options.memory?.() };
      for (const [key, n] of Object.entries(point)) maxima[key] = Math.max(maxima[key] ?? 0, n);
      if (frames % 32 === 0) samples.push(point);
    };
    let protocolError: unknown;
    const cancel = (reason: unknown) => {
      protocolError = reason;
      execution.cancel(reason);
      for (const item of posted.values()) item.frame.release();
      posted.clear();
      postedBytes = 0;
    };
    port.onmessage = (event: MessageEvent<unknown>) => {
      const message = event.data as { t?: string; target?: number; through?: number };
      if (
        message.t !== "release" ||
        message.target !== 1 ||
        message.through === undefined ||
        !Number.isInteger(message.through) ||
        message.through > highest
      ) {
        cancel(new Error("Invalid executor export result credit"));
        return;
      }
      if (message.through <= through) return;
      through = message.through;
      for (const [sequence, item] of posted)
        if (sequence <= through) {
          item.frame.release();
          postedBytes -= item.bytes;
          posted.delete(sequence);
        }
      sample();
    };
    port.onmessageerror = () => cancel(new Error("Executor export port decode failed"));
    port.start();
    try {
      for (;;) {
        const before = performance.now();
        const frame = await execution.results.next();
        nextMs += performance.now() - before;
        if (frame === null) break;
        posted.set(frame.payload.sequence, { frame, bytes: frame.byteLength });
        postedBytes += frame.byteLength;
        maxPostedBytes = Math.max(maxPostedBytes, postedBytes);
        highest = frame.payload.sequence;
        rows += frame.payload.rowCount;
        frames++;
        maxFrameRows = Math.max(maxFrameRows, frame.payload.rowCount);
        maxFrameBytes = Math.max(maxFrameBytes, frame.byteLength);
        wireBytes += payloadBytes(frame.payload);
        frameBufferBytes += frame.byteLength;
        const transfer = transferFor(frame.transfer);
        transferredBuffers += transfer.length;
        const postStart = performance.now();
        port.postMessage({ t: "rows", seq: 1, columns: null, frames: [frame.payload] }, transfer);
        postMs += performance.now() - postStart;
        if (transfer.some((buffer) => buffer.byteLength !== 0))
          throw new Error("Executor frame was not transferred");
        sample();
      }
      const summary = await execution.executed;
      const beforeDrain = performance.now();
      await execution.delivered;
      const drainMs = performance.now() - beforeDrain;
      if (protocolError) throw protocolError;
      if (rows !== table.liveRowCount || summary.rowCount !== rows || posted.size !== 0)
        throw new Error("Incomplete executor export transport");
      sample();
      return {
        rows,
        ms: performance.now() - started,
        loadMs: 0,
        openMs: 0,
        loads: 0,
        executorStats: {
          path: "production-executor",
          frames,
          wireBytes,
          frameBufferBytes,
          transferredBuffers,
          maxFrameRows,
          maxFrameBytes,
          maxPostedBytes,
          resultWindowBytes: DEFAULT_RESULT_WINDOW_BYTES,
          readWindowPolicy: "executor-default",
          nextMs,
          postMs,
          drainMs,
          profile: summary.profile,
          memory: { maxima, samples },
        },
      };
    } catch (error) {
      cancel(error);
      await Promise.allSettled([execution.executed, execution.delivered]);
      throw error;
    } finally {
      snapshot.release();
      port.close();
    }
  };
}
