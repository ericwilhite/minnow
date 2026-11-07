/** Node CPU components over the same real stored fixture as next-executor-bench.mts. */
import { cpuUsage } from "node:process";
import { writeFile } from "node:fs/promises";
import { Session } from "node:inspector/promises";
import type { StorageCore } from "../packages/core/dist/next/testing/storage-core.js";
import { batchTargetFor } from "../packages/core/dist/next/memory/governor.js";
import { BATCH_ROWS } from "../packages/core/dist/next/vector/vector.js";
import { TableScan } from "../packages/core/dist/next/exec/table-scan.js";
import { ChunkReader } from "../packages/core/dist/next/page/reader.js";
import { reclaimingReader } from "../packages/core/dist/next/exec/reclaim-reader.js";
import { OutputArena } from "../packages/core/dist/next/exec/kernels/vectors.js";
import { LimitOperator } from "../packages/core/dist/next/exec/limit.js";
import { drive, type SourceItem } from "../packages/core/dist/next/exec/pipeline.js";
import { compilePlan } from "../packages/core/dist/next/exec/plan.js";
import { QueryExecution } from "../packages/core/dist/next/exec/run.js";
import { oracleRows } from "../packages/core/dist/next/testing/differential-adapter.js";
import { isDeepStrictEqual } from "node:util";

function distribution(samples: readonly number[]) {
  const sorted = [...samples].sort((a, b) => a - b),
    median = sorted[sorted.length >>> 1],
    min = sorted[0],
    max = sorted.at(-1);
  if (median === undefined || min === undefined || max === undefined)
    throw new Error("missing CPU samples");
  return { samples, median, spread: max - min, min, max };
}

/** Isolate the prepared scan/sort/fetch pipeline, including owned frames and cleanup.
 * SQL, lowering, compilation and JS row decoding are outside the timed samples. The two
 * strategies share the exact pinned storage fixture, governor and operator implementations. */
export async function lateSortComponent(core: StorageCore, rounds: number, kind = "late-sort") {
  if (!Number.isSafeInteger(rounds) || rounds < 5)
    throw new RangeError("at least five interleaved rounds required");
  const prepared = core.prepare(
    kind === "key-order"
      ? "SELECT * FROM events_wide ORDER BY id LIMIT 100"
      : "SELECT * FROM events_wide ORDER BY f09 DESC,id LIMIT 100",
  );
  const programs = [
    compilePlan(prepared.plan, kind === "key-order" ? { keyOrder: false } : { lateSort: false }),
    compilePlan(prepared.plan, kind === "key-order" ? { keyOrder: true } : {}),
  ];
  let queryId = 1000000;
  const run = async (late: boolean, decode = false) => {
    const pin = core.harness.path.pin();
    const program = programs[late ? 1 : 0];
    if (program === undefined) throw new Error("missing component program");
    const execution = new QueryExecution(
      prepared.plan,
      {
        parameters: [],
        snapshot: pin,
        statement: null,
        statementTime: 0n,
        reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
        governor: core.harness.governor,
        spill: core.spill,
        queryId: ++queryId,
        sequences: null,
        sequenceSession: null,
        returning: "atExecuted",
      },
      program,
    );
    let rows = 0;
    const decoded: unknown[] = [];
    try {
      for (;;) {
        const frame = await execution.results.next();
        if (frame === null) break;
        try {
          rows += frame.payload.rowCount;
          if (decode)
            decoded.push(
              ...oracleRows({
                columns: prepared.columns,
                batches: [
                  {
                    length: frame.payload.rowCount,
                    selection: null,
                    columns: frame.payload.columns.map((vector, index) => {
                      if (vector.form !== "flat") throw new Error("encoded result frame");
                      const type = prepared.columns[index]?.type;
                      if (type === undefined) throw new Error("missing result type");
                      return { ...vector, type };
                    }),
                  },
                ],
              }),
            );
        } finally {
          frame.release();
        }
      }
      const summary = await execution.executed;
      await execution.delivered;
      if (rows !== 100 || core.spill.bytesInUse !== 0)
        throw new Error("late-sort component lost rows or leaked spill");
      return { rows, decoded, profile: summary.profile };
    } catch (error) {
      execution.cancel(error);
      await Promise.allSettled([execution.executed, execution.delivered]);
      throw error;
    } finally {
      pin.release();
    }
  };
  const control = await run(false, true),
    candidate = await run(true, true);
  if (!isDeepStrictEqual(control.decoded, candidate.decoded))
    throw new Error("late-sort warmup differs from full-payload sorting");
  const before: number[] = [],
    after: number[] = [],
    order: string[] = [];
  let lastBefore = control.profile,
    lastAfter = candidate.profile;
  for (let round = 0; round < rounds; round++)
    for (const late of round % 2 === 0 ? [false, true] : [true, false]) {
      const start = cpuUsage();
      const result = await run(late);
      const elapsed = cpuUsage(start);
      (late ? after : before).push((elapsed.user + elapsed.system) / 1000);
      if (late) lastAfter = result.profile;
      else lastBefore = result.profile;
      order.push(
        kind === "key-order"
          ? late
            ? "index-cursor"
            : "scan-sort-locators"
          : late
            ? "keys-and-locators"
            : "full-payload",
      );
    }
  const baseline = distribution(before),
    optimized = distribution(after),
    delta = baseline.median - optimized.median;
  return {
    lane: "Node",
    strategy: kind,
    component: "prepared TableScan/sort/fetch/frames pipeline; no SQL/lowering/compile/JS rows",
    rounds,
    warmupsPerVariant: 1,
    order,
    before: { ...baseline, profile: lastBefore },
    after: { ...optimized, profile: lastAfter },
    medianDeltaMs: delta,
    beatsLargestSpread: delta > Math.max(baseline.spread, optimized.spread),
    beatsCombinedSpreads: delta > baseline.spread + optimized.spread,
  };
}

/** Five or more interleaved rounds; each sample averages seven complete scan/operator runs.
 * Averaging retains natural GC costs while reducing individual-allocation noise. Full-batch
 * and bounded scans use the same code, fixture, governor and sink. Neither materializes rows. */
export async function scanLimitComponent(
  core: StorageCore,
  rounds: number,
  profilePath: string | null = null,
) {
  if (!Number.isSafeInteger(rounds) || rounds < 5)
    throw new RangeError("at least five interleaved rounds required");
  const tableDef = core.harness.path.catalog.tableByName("events_wide");
  if (tableDef === undefined) throw new Error("component requires events_wide");
  const run = async (bounded: boolean) => {
    const pin = core.harness.path.pin(),
      target = batchTargetFor(core.harness.governor.capBytes, 4);
    const grant = await core.harness.governor.admit({
      label: "component/scan-limit",
      minimumBytes: target + BATCH_ROWS * 16,
      spillable: true,
    });
    const arena = new OutputArena(grant, BATCH_ROWS);
    let rows = 0,
      scannedRows = 0;
    const limit = new LimitOperator(100, 0, arena, {
      push(batch) {
        rows += batch.selection?.count ?? batch.length;
        return "ready";
      },
      ready: () => Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    });
    let scan: TableScan | null = null;
    try {
      const table = pin.table(tableDef.id, { budget: grant });
      scan = new TableScan({
        table,
        pages: reclaimingReader(pin.pages, grant),
        reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
        budget: grant,
        columns: tableDef.columns.map((c) => c.id),
        filter: null,
        emitLocator: false,
        context: { arena, params: { slots: [] }, input: [] },
        loans: { borrow: (bytes) => grant.borrow(bytes) },
        targetBytes: target,
        ...(bounded ? { rowsNeeded: () => limit.inputNeeded() } : {}),
      });
      const source = scan;
      const observed = (item: SourceItem | null) => {
        if (item !== null) scannedRows += item.batch.selection?.count ?? item.batch.length;
        return item;
      };
      await drive(
        {
          next() {
            if (limit.done) {
              source.abort();
              return null;
            }
            const item = source.next();
            return item instanceof Promise ? item.then(observed) : observed(item);
          },
          abort: () => source.abort(),
        },
        limit,
      );
      if (rows !== 100) throw new Error(`component returned ${String(rows)} rows`);
      return { rows, scannedRows, pagesRead: scan.stats.pagesRead };
    } finally {
      scan?.close();
      arena.close();
      pin.release();
      grant.close();
    }
  };
  for (let k = 0; k < 3; k++) {
    await run(false);
    await run(true);
  }
  const iterations = 7,
    before: number[] = [],
    after: number[] = [],
    order: string[] = [];
  let lastBefore, lastAfter;
  for (let round = 0; round < rounds; round++)
    for (const bounded of round % 2 === 0 ? [false, true] : [true, false]) {
      const start = cpuUsage();
      let last;
      for (let k = 0; k < iterations; k++) last = await run(bounded);
      const elapsed = cpuUsage(start),
        sample = (elapsed.user + elapsed.system) / 1000 / iterations;
      (bounded ? after : before).push(sample);
      if (bounded) lastAfter = last;
      else lastBefore = last;
      order.push(bounded ? "bounded" : "full-batch");
    }
  const baseline = distribution(before),
    candidate = distribution(after);
  if (profilePath !== null) {
    const session = new Session();
    session.connect();
    try {
      await session.post("Profiler.enable");
      await session.post("Profiler.setSamplingInterval", { interval: 100 });
      await session.post("Profiler.start");
      // Profile enough scans to keep profiler startup/stop costs separate from useful samples.
      for (let k = 0; k < 100; k++) await run(false);
      const { profile } = await session.post("Profiler.stop");
      await writeFile(profilePath, JSON.stringify(profile));
    } finally {
      session.disconnect();
    }
  }
  const delta = baseline.median - candidate.median;
  return {
    lane: "Node",
    component:
      "real stored TableScan + LIMIT, pin/admission/cleanup included; no SQL/lowering/output materialization",
    rounds,
    iterationsPerRound: iterations,
    warmupsPerVariant: 3,
    order,
    before: { ...baseline, last: lastBefore },
    after: { ...candidate, last: lastAfter },
    medianDeltaMs: delta,
    beatsLargestSpread: delta > Math.max(baseline.spread, candidate.spread),
    beatsCombinedSpreads: delta > baseline.spread + candidate.spread,
  };
}
