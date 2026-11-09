/**
 * Aggregate output/top-N and grouped DISTINCT components; fixture/state setup is untimed.
 * --only selection --warmups 12 --iterations 5 --rounds 7 alternates both selectors.
 * Each sample is mean CPU per complete selection across all five calls (no trimming).
 * Output/finish and recorded-batch selection are distinct, overlapping regions, not additive.
 */
import { strict as assert } from "node:assert";
import { cpuUsage } from "node:process";
import { Session } from "node:inspector/promises";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve, join } from "node:path";
import { tmpdir, loadavg } from "node:os";
import { aggShapes, mix } from "../packages/core/browser/next-agg-shapes.ts";
import * as lib from "../packages/core/dist/next/exec/kernels/test-support.js";
import { AggregateOperator } from "../packages/core/dist/next/exec/agg/aggregate.js";
import { GroupTopN } from "../packages/core/dist/next/exec/agg/group-top-n.js";
import { DistinctSet } from "../packages/core/dist/next/exec/agg/distinct.js";
import { OutputArena } from "../packages/core/dist/next/exec/kernels/vectors.js";
import { SortOperator } from "../packages/core/dist/next/exec/sort.js";
import { drive, type Operator } from "../packages/core/dist/next/exec/pipeline.js";
import { Governor } from "../packages/core/dist/next/memory/governor.js";
import { MemorySpillService } from "../packages/core/dist/next/spill/memory-spill.js";
import type { Batch } from "../packages/core/dist/next/vector/vector.js";
import { value } from "./next-executor-workload/gen.mjs";
const option = (key: string, fallback: string) => {
  const at = process.argv.indexOf(key);
  return at < 0 ? fallback : (process.argv[at + 1] ?? fallback);
};
const rows = Number(option("--rows", "1000000")),
  rounds = Number(option("--rounds", "7")),
  warmups = Number(option("--warmups", "3")),
  iterations = Number(option("--iterations", "1")),
  directory = resolve(option("--output", join(tmpdir(), "b4-agg-selection"))),
  profile = option("--profile", "0") === "1",
  only = option("--only", "all");
if (
  !Number.isSafeInteger(rows) ||
  rows < 1 ||
  !Number.isInteger(rounds) ||
  rounds < 5 ||
  !Number.isSafeInteger(warmups) ||
  warmups < 1 ||
  !Number.isSafeInteger(iterations) ||
  iterations < 1
)
  throw new RangeError("positive rows and at least five rounds required");
await mkdir(directory, { recursive: true });
const generated = aggShapes(lib, only === "distinct" ? 0 : rows, ["agg_highcard_int"])[0];
if (!generated) throw new Error("missing fixture");
const shape = generated;
const discard: Operator = {
  push: () => "ready",
  ready: () => Promise.resolve(),
  finish: () => undefined,
  abort: () => undefined,
};
const distinctPeaks: Record<string, number> = {};
const outputTypes = [lib.I32, lib.I64, lib.F64];
const rel = {
  id: 1,
  op: "aggregate" as const,
  output: [],
  input: { id: 0, op: "values" as const, output: [], rows: [] },
  groupBy: shape.groupBy,
  groupingSets: null,
  aggregates: shape.aggregates,
};
const captured: Batch[] = [];
{
  const arena = new OutputArena(lib.UNCHARGED, 2048);
  const aggregate = new AggregateOperator(
    rel,
    { arena, input: shape.input, params: { slots: [] } },
    lib.UNCHARGED,
    {
      ...discard,
      push(batch) {
        captured.push(structuredClone(batch));
        return "ready";
      },
    },
  );
  for (const batch of shape.batches) aggregate.push(batch);
  await aggregate.finish();
  arena.close();
}
const groups = captured.reduce((n, b) => n + b.length, 0);
const makeSort = async () => {
  const governor = new Governor(256 << 20),
    grant = await governor.admit({
      label: "component/group-output",
      minimumBytes: 16 << 20,
      spillable: true,
    }),
    spill = new MemorySpillService(),
    result: unknown[][] = [];
  const sorter = new SortOperator(
    {
      types: outputTypes,
      keys: [
        { column: 1, direction: "desc", nulls: "last", collation: null },
        { column: 0, direction: "asc", nulls: "last", collation: null },
      ],
      limit: 100,
      offset: 0,
      withTies: false,
    },
    { grant, spill, queryId: 1 },
    {
      ...discard,
      push(batch) {
        for (let i = 0; i < batch.length; i++)
          result.push(batch.columns.map((c) => lib.valueAt(c, i)));
        return "ready";
      },
    },
  );
  return { governor, grant, spill, sorter, result };
};
async function outputSetup(withSort: boolean, top = false) {
  const sorted = await makeSort(),
    arena = new OutputArena(sorted.grant, 2048);
  let produced = 0;
  const aggregate = new AggregateOperator(
    rel,
    { arena, input: shape.input, params: { slots: [] } },
    sorted.grant,
    withSort
      ? sorted.sorter
      : {
          ...discard,
          push(batch) {
            produced += batch.length;
            return "ready";
          },
        },
    top
      ? {
          outputTopN: {
            count: 100,
            keys: [
              { column: 1, direction: "desc", nulls: "last", collation: null },
              { column: 0, direction: "asc", nulls: "last", collation: null },
            ],
          },
        }
      : {},
  );
  for (const batch of shape.batches) aggregate.push(batch);
  return async () => {
    try {
      await aggregate.finish();
      return withSort
        ? JSON.stringify(sorted.result, (_key, item: unknown) =>
            typeof item === "bigint" ? String(item) : item,
          )
        : produced;
    } finally {
      aggregate.abort(new Error("component cleanup"));
      sorted.sorter.abort(undefined);
      await sorted.sorter.closed;
      arena.close();
      sorted.grant.close();
      assert.equal(sorted.governor.usedBytes, 0, "component leaked");
      assert.equal(sorted.spill.bytesInUse, 0, "component leaked");
    }
  };
}
async function sortSetup() {
  const sorted = await makeSort();
  return async () => {
    let at = 0;
    try {
      await drive(
        {
          next: () => {
            const batch = captured[at++];
            return batch ? { batch } : null;
          },
          abort: () => undefined,
        },
        sorted.sorter,
      );
      return JSON.stringify(sorted.result, (_key, item: unknown) =>
        typeof item === "bigint" ? String(item) : item,
      );
    } finally {
      sorted.sorter.abort(undefined);
      await sorted.sorter.closed;
      sorted.grant.close();
      assert.equal(sorted.governor.usedBytes, 0, "sort component leaked");
      assert.equal(sorted.spill.bytesInUse, 0, "sort component leaked");
    }
  };
}
async function selectionSetup() {
  const sorted = await makeSort();
  const top = new GroupTopN(
    {
      count: 100,
      keys: [
        { column: 1, direction: "desc", nulls: "last", collation: null },
        { column: 0, direction: "asc", nulls: "last", collation: null },
      ],
    },
    sorted.grant,
  );
  return async () => {
    try {
      let first = 0;
      for (const batch of captured) {
        const count = batch.columns[1],
          key = batch.columns[0];
        if (count?.form !== "flat" || key?.form !== "flat") throw new Error("flat rank fixture");
        top.offer([count, key], first, batch.length);
        first += batch.length;
      }
      // This fixture uses full 2048-group batches except its last (fixed-width outputs).
      const selected = Array.from({ length: top.size }, (_, k) => top.group(k));
      const columns = outputTypes.map((type, c) =>
        lib.numbers(
          type,
          selected.map((id) => {
            const batch = captured[Math.floor(id / 2048)];
            if (!batch) throw new Error("fixture group");
            const vector = batch.columns[c];
            if (!vector) throw new Error("fixture column");
            const value = lib.valueAt(vector, id % 2048);
            return value === null ? null : Number(value);
          }),
        ),
      );
      const result = sorted.sorter.push({ length: top.size, selection: null, columns });
      if (result === "blocked") await sorted.sorter.ready();
      await sorted.sorter.finish();
      return JSON.stringify(sorted.result, (_key, item: unknown) =>
        typeof item === "bigint" ? String(item) : item,
      );
    } finally {
      top.close();
      sorted.sorter.abort(undefined);
      await sorted.sorter.closed;
      sorted.grant.close();
      assert.equal(sorted.governor.usedBytes, 0, "selector leaked");
      assert.equal(sorted.spill.bytesInUse, 0, "selector leaked");
    }
  };
}
const distinctInput = Array.from(
  { length: only === "selection" ? 0 : Math.ceil(rows / 2048) },
  (_, b) => {
    const from = b * 2048,
      n = Math.min(2048, rows - from);
    return {
      vector: lib.texts(
        Array.from({ length: n }, (_, k) => String(value({ fam: "h", k: 5 }, from + k, rows))),
      ),
      gids: Int32Array.from({ length: n }, (_, k) => mix(from + k, 11) % 10),
      n,
    };
  },
);
async function distinctSetup(bitmap = false, scalar = false) {
  const governor = new Governor(256 << 20),
    grant = await governor.admit({
      label: "component/grouped-distinct",
      minimumBytes: 16 << 20,
      spillable: true,
    }),
    set = new DistinctSet(lib.TEXT, grant, 2048, bitmap),
    out = { rows: new Uint16Array(2048), count: 0 };
  return async () => {
    let selected = 0;
    try {
      for (const batch of distinctInput)
        selected += set.filter(batch.vector, batch.gids, batch.n, null, scalar, out).count;
      distinctPeaks[(scalar ? "scalar" : "grouped") + (bitmap ? "Bitmap" : "Distinct")] =
        governor.peakBytes;
      return selected;
    } finally {
      set.close();
      grant.close();
      assert.equal(governor.usedBytes, 0, "distinct leaked");
    }
  };
}
const setups = {
    outputOnly: () => outputSetup(false),
    outputTopN: () => outputSetup(true),
    groupTopN: () => outputSetup(true, true),
    sortOnly: sortSetup,
    selectionTopN: selectionSetup,
    groupedDistinct: () => distinctSetup(),
    groupedBitmap: () => distinctSetup(true),
    scalarDistinct: () => distinctSetup(false, true),
    scalarBitmap: () => distinctSetup(true, true),
  },
  kinds = (Object.keys(setups) as Array<keyof typeof setups>).filter(
    (k) =>
      only === "all" ||
      (only === "selection"
        ? ["sortOnly", "selectionTopN"].includes(k)
        : only === "distinct"
          ? ["groupedDistinct", "groupedBitmap", "scalarDistinct", "scalarBitmap"].includes(k)
          : k.includes(only)),
  ),
  samples = Object.fromEntries(kinds.map((k) => [k, [] as number[]])) as Record<
    keyof typeof setups,
    number[]
  >,
  expected = new Map<string, unknown>(),
  order: string[] = [];
if (kinds.length === 0) throw new Error("unknown component selection");
for (const kind of kinds) {
  for (let warm = 0; warm < warmups; warm++) {
    const once = await setups[kind]();
    expected.set(kind, await once());
  }
}
if (
  only === "all" &&
  (expected.get("outputOnly") !== groups ||
    expected.get("outputTopN") !== expected.get("sortOnly") ||
    expected.get("groupTopN") !== expected.get("sortOnly") ||
    expected.get("selectionTopN") !== expected.get("sortOnly"))
)
  throw new Error("fixture mismatch");
for (let round = 0; round < rounds; round++)
  for (let i = 0; i < kinds.length; i++) {
    const kind = kinds[(i + round) % kinds.length];
    if (!kind) throw new Error("order");
    let totalCpu = 0;
    for (let iteration = 0; iteration < iterations; iteration++) {
      const once = await setups[kind](),
        start = cpuUsage(),
        result = await once(),
        spent = cpuUsage(start);
      totalCpu += (spent.user + spent.system) / 1000;
      if (result !== expected.get(kind)) throw new Error(`${kind} changed result`);
    }
    samples[kind].push(totalCpu / iterations);
    order.push(kind);
  }
if (expected.has("selectionTopN") && expected.get("selectionTopN") !== expected.get("sortOnly"))
  throw new Error("selector result mismatch");
if (
  expected.has("groupedBitmap") &&
  expected.get("groupedBitmap") !== expected.get("groupedDistinct")
)
  throw new Error("grouped bitmap result mismatch");
if (expected.has("scalarBitmap") && expected.get("scalarBitmap") !== expected.get("scalarDistinct"))
  throw new Error("scalar bitmap result mismatch");
if (profile)
  for (const kind of [
    "outputTopN",
    "groupTopN",
    "selectionTopN",
    "groupedDistinct",
    "groupedBitmap",
    "scalarBitmap",
  ] as const) {
    if (!kinds.includes(kind)) continue;
    const once = await setups[kind](),
      session = new Session();
    session.connect();
    await session.post("Profiler.enable");
    await session.post("Profiler.setSamplingInterval", { interval: 100 });
    await session.post("Profiler.start");
    await once();
    const { profile: capturedProfile } = await session.post("Profiler.stop");
    session.disconnect();
    await writeFile(join(directory, `${kind}.cpuprofile`), JSON.stringify(capturedProfile));
  }
const summary = Object.fromEntries(
  kinds.map((kind) => {
    const sorted = [...samples[kind]].sort((a, b) => a - b);
    return [
      kind,
      {
        samples: samples[kind],
        median: sorted[sorted.length >>> 1],
        spread: (sorted.at(-1) ?? 0) - (sorted[0] ?? 0),
      },
    ];
  }),
);
const report = {
  lane: "Node",
  rows,
  groups,
  rounds,
  warmups,
  iterationsPerSample: iterations,
  order,
  cpuMs: summary,
  groupedDistinctPairs: expected.get("groupedDistinct"),
  distinctGroups: 10,
  distinctPeaks,
  load: loadavg(),
  limits:
    "Isolated component CPU; acceptance is assessed from paired medians/spreads. State/input fixture construction excluded; finish/output/top-N/cleanup included. Regions overlap and are not additive. Grouped DISTINCT is exact catalogue d01/h05 values, excluding scan and grouping.",
};
await writeFile(join(directory, "baseline.json"), JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify(report));
