/** Node diagnostics: the stored scan feed, its exact recorded input, and prepared result frames. */
import { cpuUsage } from "node:process";
import type { StorageCore } from "../packages/core/dist/next/testing/storage-core.js";
import type { Rel } from "../packages/core/dist/next/ir/ir.js";
import type { Batch } from "../packages/core/dist/next/vector/vector.js";
import { BATCH_ROWS } from "../packages/core/dist/next/vector/vector.js";
import { OutputArena } from "../packages/core/dist/next/exec/kernels/vectors.js";
import { ProjectOperator } from "../packages/core/dist/next/exec/filter.js";
import { AggregateOperator } from "../packages/core/dist/next/exec/agg/aggregate.js";
import { TableScan } from "../packages/core/dist/next/exec/table-scan.js";
import { chunkReads } from "../packages/core/dist/next/exec/scan.js";
import { reclaimingReader } from "../packages/core/dist/next/exec/reclaim-reader.js";
import { drive, type Operator } from "../packages/core/dist/next/exec/pipeline.js";
import { batchTargetFor } from "../packages/core/dist/next/memory/governor.js";

function aggregateOf(root: Rel): Extract<Rel, { op: "aggregate" }> {
  if (root.op === "aggregate") return root;
  if ("input" in root) return aggregateOf(root.input);
  throw new Error("component requires one unary aggregate pipeline");
}
function distribution(samples: number[]) {
  const sorted = [...samples].sort((a, b) => a - b);
  return {
    samples,
    median: sorted[sorted.length >>> 1] ?? NaN,
    spread: (sorted.at(-1) ?? 0) - (sorted[0] ?? 0),
  };
}

export async function aggregationComponents(
  core: StorageCore,
  rounds: number,
  queries: ReadonlyArray<{ id: string; sql: string }>,
  lightDirectory?: string,
) {
  if (!Number.isSafeInteger(rounds) || rounds < 5) throw new RangeError("five rounds required");
  const records = [];
  for (const query of queries) {
    if (!["agg_3dims_20aggs", "agg_highcard_int", "agg_count_distinct"].includes(query.id))
      continue;
    const prepared = core.prepare(query.sql),
      aggregate = aggregateOf(prepared.plan.root),
      project = aggregate.input;
    if (project.op !== "project" || project.input.op !== "scan")
      throw new Error("component requires scan/project aggregate input");
    const scanRel = project.input,
      scanTypes = scanRel.output.map((c) => c.type),
      inputTypes = project.output.map((c) => c.type),
      captured: Batch[] = [],
      forms = inputTypes.map(() => new Map<string, number>()),
      batchSizes = new Map<number, number>(),
      target = batchTargetFor(core.harness.governor.capBytes, 4);
    let inputRows = 0;
    const scan = async (record = false) => {
      const pin = core.harness.path.pin(),
        grant = await core.harness.governor.admit({
          label: "component/scan-feed",
          minimumBytes: target + BATCH_ROWS * 16,
          spillable: true,
        }),
        arena = new OutputArena(grant, BATCH_ROWS);
      let source: TableScan | null = null,
        rows = 0;
      try {
        const table = pin.table(scanRel.tableId, { budget: grant });
        source = new TableScan({
          table,
          pages: reclaimingReader(pin.pages, grant),
          reads: chunkReads,
          budget: grant,
          columns: scanRel.columns,
          filter: scanRel.filter,
          emitLocator: scanRel.emitLocator,
          context: { arena, params: { slots: [] }, input: scanTypes },
          targetBytes: target,
          loans: { borrow: (n) => grant.borrow(n) },
        });
        await drive(
          source,
          new ProjectOperator(
            project.exprs,
            { arena, params: { slots: [] }, input: scanTypes },
            {
              push(batch) {
                const n = batch.selection?.count ?? batch.length;
                rows += n;
                if (record) {
                  // Copies are fixture setup, outside timing. Preserve dictionary forms and ids;
                  // no borrowed scan vector or dictionary survives the next scan step.
                  captured.push(structuredClone(batch));
                  inputRows += n;
                  batchSizes.set(n, (batchSizes.get(n) ?? 0) + 1);
                  for (const [k, vector] of batch.columns.entries()) {
                    const counts = forms[k];
                    if (!counts) throw new Error("input width");
                    counts.set(vector.form, (counts.get(vector.form) ?? 0) + n);
                  }
                }
                return "ready";
              },
              ready: () => Promise.resolve(),
              finish: () => undefined,
              abort: () => undefined,
            },
          ),
        );
        return { rows, pagesRead: source.stats.pagesRead };
      } finally {
        source?.close();
        arena.close();
        grant.close();
        pin.release();
      }
    };
    const operator = async () => {
      const grant = await core.harness.governor.admit({
          label: "component/recorded-aggregate",
          minimumBytes: 16 << 20,
          spillable: true,
        }),
        arena = new OutputArena(grant, BATCH_ROWS);
      let rows = 0,
        at = 0;
      const sink: Operator = {
          push(batch) {
            rows += batch.selection?.count ?? batch.length;
            return "ready";
          },
          ready: () => Promise.resolve(),
          finish: () => undefined,
          abort: () => undefined,
        },
        aggregateOperator = new AggregateOperator(
          aggregate,
          { arena, params: { slots: [] }, input: inputTypes },
          grant,
          sink,
        );
      try {
        await drive(
          {
            next: () => {
              const batch = captured[at++];
              return batch ? { batch } : null;
            },
            abort: () => undefined,
          },
          aggregateOperator,
        );
        return { rows };
      } finally {
        aggregateOperator.abort(new Error("component cleanup"));
        arena.close();
        grant.close();
      }
    };
    const pipeline = async () => {
      const started = core.start(prepared);
      let rows = 0,
        frames = 0;
      try {
        for (;;) {
          const frame = await started.execution.results.next();
          if (!frame) break;
          rows += frame.payload.rowCount;
          frames++;
          frame.release();
        }
        const summary = await started.execution.executed;
        await started.execution.delivered;
        return { rows, frames, profile: summary.profile };
      } catch (error) {
        started.execution.cancel(error);
        await Promise.allSettled([started.execution.executed, started.execution.delivered]);
        throw error;
      } finally {
        started.release();
      }
    };
    const recorded = await scan(true);
    if (lightDirectory !== undefined) {
      const { lightComparison } = await import("./next-aggregation-light.mts");
      records.push(
        await lightComparison(
          core,
          query.id,
          aggregate,
          inputTypes,
          captured,
          rounds,
          lightDirectory,
          pipeline,
          scan,
        ),
      );
      captured.length = 0;
      continue;
    }
    const expectedOperator = await operator(),
      expectedPipeline = await pipeline();
    const samples = { scan: [] as number[], operator: [] as number[], pipeline: [] as number[] },
      operations = { scan, operator, pipeline },
      order: string[] = [];
    for (let round = 0; round < rounds; round++) {
      const kinds: Array<keyof typeof samples> = ["scan", "operator", "pipeline"];
      for (let k = 0; k < kinds.length; k++) {
        const kind = kinds[(k + round) % kinds.length];
        if (!kind) throw new Error("component order");
        const start = cpuUsage(),
          result = await operations[kind](),
          elapsed = cpuUsage(start);
        samples[kind].push((elapsed.user + elapsed.system) / 1000);
        order.push(kind);
        const expected =
          kind === "scan"
            ? inputRows
            : kind === "operator"
              ? expectedOperator.rows
              : expectedPipeline.rows;
        if (result.rows !== expected) throw new Error("diagnostic lost rows");
        if (core.spill.bytesInUse !== 0) throw new Error("diagnostic leaked spill");
      }
    }
    records.push({
      id: query.id,
      scanColumns: scanRel.output.map((c) => c.name),
      inputRows,
      inputForms: project.output.map((c, k) => ({
        name: c.name,
        type: c.type,
        rowsByForm: Object.fromEntries(forms[k] ?? []),
      })),
      batchSizes: Object.fromEntries(batchSizes),
      recordedScan: recorded,
      aggregateRows: expectedOperator.rows,
      resultFrames: expectedPipeline.frames,
      resultRows: expectedPipeline.rows,
      order,
      cpuMs: {
        scan: distribution(samples.scan),
        operator: distribution(samples.operator),
        pipeline: distribution(samples.pipeline),
      },
      limits:
        "Scan/project/discard; aggregate over cloned scan-input fixture/discard; prepared full pipeline/owned frames/cleanup. Not additive: distinct sinks, cache/JIT/GC interactions, and downstream sort differ. Clone setup excluded. Node StorageCore bypasses owner worker transport.",
    });
    captured.length = 0;
  }
  return { lane: "Node", rounds, warmupsPerVariant: lightDirectory === undefined ? 1 : 3, records };
}
