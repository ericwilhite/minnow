/**
 * Gate A bench mode: real TableScan and pipeline driver -> BENCH frame assembler -> real
 * ResultChannel -> owner transfer policy -> MessagePort -> client credit. The assembler stands
 * in for the executor's future result sink; this is not SQL lowering or a production Executor.
 * Table/group descriptors are supplied by the fixture (no B3 catalog/group-root loading).
 * Keep this adapter under the bench directory; production modules must not import it.
 */
import type { ColumnDef } from "../../../packages/core/src/next/access/catalog.js";
import type {
  GroupSegment,
  TableSnapshot,
} from "../../../packages/core/src/next/access/snapshot.js";
import type { ByteBudget, ExtentReader } from "../../../packages/core/src/next/storage/extents.js";
import { chunkReads } from "../../../packages/core/src/next/exec/scan.js";
import { TableScan, type ScanStats } from "../../../packages/core/src/next/exec/table-scan.js";
import { BATCH_TARGET_BYTES } from "../../../packages/core/src/next/vector/vector.js";
import { OutputArena } from "../../../packages/core/src/next/exec/kernels/vectors.js";
import {
  drive,
  isPromise,
  type BatchSource,
  type Operator,
} from "../../../packages/core/src/next/exec/pipeline.js";
import { ResultChannel, type ResultFrame } from "../../../packages/core/src/next/exec/result.js";
import { DEFAULT_RESULT_WINDOW_BYTES } from "../../../packages/core/src/next/owner/protocol.js";
import { transferFor } from "../../../packages/core/src/next/owner/frame.js";
import type { ExportGroup, ExportResult } from "./export-stream.js";
import { BenchFrameAssembler } from "./owner-frames.js";

export interface ScanExportStats {
  readonly copies: BenchFrameAssembler["copyStats"];
  /** Charged-byte snapshots, not physical RSS. Independent maxima must not be added. */
  readonly memory?: {
    maxima: Record<string, number>;
    atPeak: Record<string, number>;
    samples: Array<Record<string, number>>;
  };
  readonly frames: number;
  readonly batchTargetBytes: number;
  readonly maxBatchRows: number;
  readonly maxFrameBytes: number;
  readonly maxOutstandingBytes: number;
  readonly flattenedDictionaryColumns: number;
  readonly dictionaryBytes: number;
  readonly wireBytes: number;
  readonly transferredBuffers: number;
  /** Inclusive elapsed next() time; overlaps consumer posting when reads await. */
  readonly nextMs: number;
  readonly assemblyMs: number;
  readonly postMs: number;
  readonly creditWaitMs: number;
  readonly scan: ScanStats;
}

/** BENCH-only snapshot descriptors; stands in for catalog/group-root reads. */
export function benchTableOf(groups: readonly ExportGroup[]): TableSnapshot {
  const first = groups[0];
  if (!first) throw new Error("No scan groups");
  const columns: ColumnDef[] = first.chunks.map((chunk) => ({
    id: chunk.columnId,
    name: `c${String(chunk.columnId)}`,
    type: chunk.type,
    get declared(): never {
      throw new Error("Bench has no SQL catalog descriptors");
    },
    nullable: true,
    defaultSql: null,
    generatedSql: null,
    identitySequence: null,
    missing: null,
  }));
  const segments: GroupSegment[] = groups.map((g, index) => ({
    kind: "group",
    index,
    rowCount: g.rows,
    liveRowCount: g.rows,
    staged: false,
    get group(): never {
      throw new Error("Bench supplies decoded group descriptors, not B3 GroupRefs");
    },
    root: () => ({ rowCount: g.rows, rowIds: { kind: "dense" }, columns: g.chunks }),
    chunk: (id: number) => g.chunks.find((c) => c.columnId === id) ?? null,
    deletions: () => null,
    statistics: () => null,
    pages: () => {
      throw new Error("Unfiltered bench does not supply B3 page summaries");
    },
  }));
  return {
    tableId: 1,
    schemaVersion: 1,
    columns,
    segments,
    liveRowCount: groups.reduce((n, g) => n + g.rows, 0),
  };
}

export async function scanExport(
  space: ExtentReader,
  groups: readonly ExportGroup[],
  options: {
    budget: ByteBudget;
    resultBudget: ByteBudget;
    port: MessagePort;
    targetBytes?: number;
    readWindowBytes?: number;
    transferOutputs?: boolean | "dictionary";
    /** Bench-only charged counters supplied by the worker, sampled at frame boundaries. */
    memory?: () => {
      queryAllocated: number;
      queryHeld: number;
      resultHeld: number;
      cacheCharge: number;
      resultAllocated: number;
      governorHeld: number;
      idlePool: number;
      cacheResident: number;
      cachePinned: number;
    };
  },
): Promise<ExportResult & { scanStats: ScanExportStats }> {
  const table = benchTableOf(groups),
    port = options.port;
  const posted = new Map<number, ResultFrame>();
  let rows = 0,
    frames = 0,
    maxBatchRows = 0,
    maxFrameBytes = 0,
    maxOutstandingBytes = 0,
    wireBytes = 0,
    transferredBuffers = 0,
    nextMs = 0,
    assemblyMs = 0,
    postMs = 0,
    creditWaitMs = 0;
  let highest = -1,
    released = -1;
  const started = performance.now();
  const arena = new OutputArena(options.budget, 2048);
  const scan = new TableScan({
    table,
    pages: space,
    reads: chunkReads,
    ...(options.targetBytes === undefined ? {} : { targetBytes: options.targetBytes }),
    readWindowBytes: options.readWindowBytes ?? 64 * 1024 * 1024,
    transferOutputs: options.transferOutputs ?? false,
    outputBudget: options.resultBudget,
    budget: options.budget,
    columns: table.columns.map((c) => c.id),
    filter: null,
    emitLocator: false,
    context: { arena, params: { slots: [] }, input: [] },
  });
  const assembler = new BenchFrameAssembler(options.resultBudget);
  const channel = new ResultChannel(
    table.columns.map((c) => ({ name: c.name, type: c.type, nullable: c.nullable })),
    { windowBytes: DEFAULT_RESULT_WINDOW_BYTES },
  );
  const maxima: Record<string, number> = {},
    samples: Array<Record<string, number>> = [];
  let atPeak: Record<string, number> = {};
  const sample = () => {
    const external = options.memory?.();
    if (!external) return;
    const decoded = scan.memoryStats,
      copies = assembler.copyStats;
    const activeReadsAndDescriptors = Math.max(
      0,
      external.queryAllocated -
        decoded.reusedDecodeBytes -
        decoded.ownedOutputInQueryBytes -
        decoded.scratchBytes -
        decoded.reserveBytes,
    );
    const cacheResidentLessPins = Math.max(0, external.cacheResident - external.cachePinned);
    const snapshot = {
      rows,
      frames,
      ...external,
      ...decoded,
      activeReadsAndDescriptors,
      cacheResidentLessPins,
      unusedGrantHeadroom:
        external.queryHeld +
        external.resultHeld -
        external.queryAllocated -
        external.resultAllocated,
      cacheOtherCharge: Math.max(0, external.cacheCharge - external.idlePool - 2 * 1024 * 1024),
      frameBytes: channel.outstandingBytes,
      liveCopiedBytes: copies.liveCopiedBytes,
      liveAdoptedBytes: copies.liveAdoptedBytes,
      // Includes the cache's fixed slabs/idle pool, without adding them twice. Frame bytes
      // already occur in queryAllocated (adopted) or resultAllocated (copies). Prepaid reserve
      // is charged before allocation; all of these are accounting, not physical RSS.
      attributedCharge: external.queryAllocated + external.resultAllocated + external.cacheCharge,
    };
    for (const [key, value] of Object.entries(snapshot))
      maxima[key] = Math.max(maxima[key] ?? 0, value);
    if (snapshot.attributedCharge > (atPeak.attributedCharge ?? 0)) atPeak = snapshot;
    if (frames % 32 === 0) samples.push(snapshot);
  };
  let protocolError: unknown;
  const cancel = (error: unknown) => {
    protocolError = error;
    channel.cancel(error);
    scan.abort();
    for (const frame of posted.values()) frame.release();
    posted.clear();
  };
  port.onmessage = (event: MessageEvent<unknown>) => {
    const message = event.data as { t?: string; target?: number; through?: number };
    if (
      message.t !== "release" ||
      message.target !== 1 ||
      !Number.isInteger(message.through) ||
      message.through === undefined ||
      message.through > highest
    ) {
      cancel(new Error("Invalid bench result credit"));
      return;
    }
    if (message.through <= released) return;
    released = message.through;
    for (const [sequence, frame] of posted)
      if (sequence <= released) {
        frame.release();
        posted.delete(sequence);
        sample();
      }
  };
  port.onmessageerror = () => cancel(new Error("Bench result port decode failed"));
  port.start();
  const source: BatchSource = {
    next() {
      const before = performance.now(),
        step = scan.next();
      if (isPromise(step))
        return step.finally(() => {
          sample();
          nextMs += performance.now() - before;
        });
      sample();
      nextMs += performance.now() - before;
      return step;
    },
    abort: () => scan.abort(),
  };
  const sink: Operator = {
    push(batch, transfer) {
      const before = performance.now();
      try {
        maxBatchRows = Math.max(maxBatchRows, batch.length);
        const readiness = channel.offer(assembler.offer(batch, transfer));
        maxOutstandingBytes = Math.max(maxOutstandingBytes, channel.outstandingBytes);
        sample();
        return readiness;
      } finally {
        assemblyMs += performance.now() - before;
      }
    },
    async ready() {
      const before = performance.now();
      try {
        await channel.ready();
      } finally {
        creditWaitMs += performance.now() - before;
      }
    },
    finish: () => channel.end(),
    abort: (error) => channel.fail(error),
  };
  const consume = async () => {
    for (let frame = await channel.next(); frame !== null; frame = await channel.next()) {
      posted.set(frame.payload.sequence, frame);
      highest = frame.payload.sequence;
      rows += frame.payload.rowCount;
      frames++;
      maxFrameBytes = Math.max(maxFrameBytes, frame.byteLength);
      wireBytes += frame.byteLength;
      const transfer = transferFor(frame.transfer);
      transferredBuffers += transfer.length;
      const before = performance.now();
      port.postMessage({ t: "rows", seq: 1, columns: null, frames: [frame.payload] }, transfer);
      postMs += performance.now() - before;
      if (transfer.some((buffer) => buffer.byteLength !== 0))
        throw new Error("Bench frame was not transferred");
    }
    await channel.delivered;
  };
  const producer = Promise.resolve().then(() => drive(source, sink));
  const consumer = consume();
  try {
    await Promise.all([producer, consumer]);
    if (protocolError) throw protocolError;
    if (rows !== table.liveRowCount || posted.size !== 0)
      throw new Error("Incomplete scan transport");
    return {
      rows,
      ms: performance.now() - started,
      loadMs: 0,
      openMs: 0,
      loads: 0,
      scanStats: {
        copies: assembler.copyStats,
        ...(options.memory ? { memory: { maxima, atPeak, samples } } : {}),
        flattenedDictionaryColumns: assembler.flattenedColumns,
        dictionaryBytes: assembler.dictionaryBytes,
        frames,
        batchTargetBytes: options.targetBytes ?? BATCH_TARGET_BYTES,
        maxBatchRows,
        maxFrameBytes,
        maxOutstandingBytes,
        wireBytes,
        transferredBuffers,
        nextMs,
        assemblyMs,
        postMs,
        creditWaitMs,
        scan: { ...scan.stats },
      },
    };
  } catch (error) {
    cancel(error);
    await Promise.allSettled([producer, consumer]);
    throw error;
  } finally {
    scan.close();
    arena.close();
    assembler.close();
    port.close();
  }
}
