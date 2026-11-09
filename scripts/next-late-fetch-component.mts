/** Isolate winner payload fetch over the exact winners of the catalogue's wide top-N. */
import { cpuUsage } from "node:process";
import { isDeepStrictEqual } from "node:util";
import type { StorageCore } from "../packages/core/dist/next/testing/storage-core.js";
import { oracleRows } from "../packages/core/dist/next/testing/differential-adapter.js";
import { BATCH_ROWS, allocateFlat } from "../packages/core/dist/next/vector/vector.js";
import { OutputArena, slotOf } from "../packages/core/dist/next/exec/kernels/vectors.js";
import { TableScan } from "../packages/core/dist/next/exec/table-scan.js";
import { LateFetchOperator, ORDINAL_TYPE } from "../packages/core/dist/next/exec/late-fetch.js";
import { lateSortShape } from "../packages/core/dist/next/exec/late-sort.js";
import { SortOperator, sortMinimumBytes } from "../packages/core/dist/next/exec/sort.js";
import { drive } from "../packages/core/dist/next/exec/pipeline.js";
import { chunkReads } from "../packages/core/dist/next/exec/scan.js";
import { batchTargetFor } from "../packages/core/dist/next/memory/governor.js";

export async function lateFetchComponent(core: StorageCore, rounds: number) {
  if (!Number.isSafeInteger(rounds) || rounds < 5) throw new RangeError("five rounds required");
  const prepared = core.prepare("SELECT * FROM events_wide ORDER BY f09 DESC,id LIMIT 100"),
    root = prepared.plan.root;
  if (root.op !== "sort") throw new Error("expected catalogue sort root");
  const shape = lateSortShape(root);
  if (!shape) throw new Error("expected late-sort shape");
  const types = shape.project.output.map((field) => field.type),
    target = batchTargetFor(core.harness.governor.capBytes, 4),
    locators = allocateFlat({ kind: "locator", tableId: shape.scan.tableId }, 100),
    pin = core.harness.path.pin(),
    setup = await core.harness.governor.admit({
      label: "component/select-keys",
      minimumBytes: 16 << 20,
      spillable: true,
    }),
    sortGrant = await core.harness.governor.admit({
      label: "component/select-sort",
      minimumBytes: sortMinimumBytes(shape.narrow.output.map((f) => f.type)),
      spillable: true,
    }),
    arena = new OutputArena(setup, BATCH_ROWS);
  let scan: TableScan | null = null;
  try {
    const input = shape.narrow.output.map((field) => field.type);
    scan = new TableScan({
      table: pin.table(shape.scan.tableId, { budget: setup }),
      pages: pin.pages,
      reads: chunkReads,
      budget: setup,
      columns: shape.narrow.columns,
      filter: null,
      emitLocator: true,
      context: { arena, params: { slots: [] }, input },
      targetBytes: target,
      loans: { borrow: (n) => setup.borrow(n) },
    });
    await drive(
      scan,
      new SortOperator(
        {
          types: input,
          keys: shape.keys,
          limit: 100,
          offset: 0,
          withTies: false,
        },
        { grant: sortGrant, spill: core.spill, queryId: 900001 },
        {
          push(batch) {
            const vector = batch.columns[shape.locator];
            if (!vector || locators.data.layout !== "f64") throw new Error("missing locator");
            for (let k = 0; k < (batch.selection?.count ?? batch.length); k++) {
              const [flat, row] = slotOf(vector, batch.selection?.rows[k] ?? k);
              if (flat.data.layout !== "f64") throw new Error("locator layout");
              locators.data.values[locators.length++] = flat.data.values[row] ?? -1;
            }
            return "ready";
          },
          ready: () => Promise.resolve(),
          finish: () => undefined,
          abort: () => undefined,
        },
      ),
    );
    if (locators.length !== 100) throw new Error("selection lost winners");
  } finally {
    scan?.close();
    arena.close();
    setup.close();
    sortGrant.close();
    pin.release();
  }
  let queryId = 900001;
  const run = async (onePass: boolean, decode = false) => {
    const snapshot = core.harness.path.pin(),
      grant = await core.harness.governor.admit({
        label: "component/fetch",
        minimumBytes: 16 << 20,
        spillable: true,
      }),
      restoreGrant = await core.harness.governor.admit({
        label: "component/restore",
        minimumBytes: sortMinimumBytes([...types, ORDINAL_TYPE]),
        spillable: true,
      }),
      arena = new OutputArena(grant, BATCH_ROWS),
      decoded: unknown[] = [];
    let rows = 0;
    const fetch = new LateFetchOperator(
      {
        onePass,
        snapshot,
        tableId: shape.scan.tableId,
        columns: shape.scan.columns,
        types,
        locator: 0,
        compile: { arena, params: { slots: [] }, input: [locators.type] },
        grant,
        restoreGrant,
        reads: chunkReads,
        spill: core.spill,
        queryId: ++queryId,
        targetBytes: target,
        signal: new AbortController().signal,
        onPipeline: () => undefined,
      },
      {
        push(batch) {
          rows += batch.selection?.count ?? batch.length;
          if (decode) decoded.push(...oracleRows({ columns: prepared.columns, batches: [batch] }));
          return "ready";
        },
        ready: () => Promise.resolve(),
        finish: () => undefined,
        abort: () => undefined,
      },
    );
    try {
      fetch.push({ length: 100, columns: [locators], selection: null });
      await fetch.ready();
      await fetch.finish();
      if (rows !== 100 || core.spill.bytesInUse !== 0)
        throw new Error("fetch lost rows or leaked spill");
      return { rows, pagesRead: fetch.pagesRead, decoded };
    } finally {
      fetch.abort(new Error("component cleanup"));
      await fetch.closed;
      arena.close();
      grant.close();
      restoreGrant.close();
      snapshot.release();
    }
  };
  const beforeWarm = await run(false, true),
    afterWarm = await run(true, true);
  if (!isDeepStrictEqual(beforeWarm.decoded, afterWarm.decoded))
    throw new Error("fetch strategies differ");
  const before: number[] = [],
    after: number[] = [],
    order: string[] = [];
  let pagesRead = 0;
  for (let round = 0; round < rounds; round++)
    for (const once of round % 2 === 0 ? [false, true] : [true, false]) {
      const started = cpuUsage(),
        result = await run(once),
        elapsed = cpuUsage(started);
      (once ? after : before).push((elapsed.user + elapsed.system) / 1000);
      pagesRead = result.pagesRead;
      order.push(once ? "one-pass" : "cached");
    }
  const distribution = (samples: number[]) => {
    const sorted = [...samples].sort((a, b) => a - b);
    return {
      samples,
      median: sorted[sorted.length >>> 1] ?? NaN,
      spread: (sorted.at(-1) ?? 0) - (sorted[0] ?? 0),
    };
  };
  const baseline = distribution(before),
    candidate = distribution(after);
  return {
    lane: "Node",
    component:
      "LateFetchOperator + ordinal restore + pin/admission/cleanup; selection and frames excluded",
    rounds,
    warmupsPerVariant: 1,
    order,
    pagesRead,
    before: baseline,
    after: candidate,
    medianDeltaMs: baseline.median - candidate.median,
    beatsCombinedSpreads: baseline.median - candidate.median > baseline.spread + candidate.spread,
  };
}
