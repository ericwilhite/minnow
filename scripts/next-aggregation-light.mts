/** Matched-output three-way diagnostics; plain TS is an optimistic observation, not a proof of
 * a JavaScript lower bound. No engine implementation changes are enabled by this benchmark. */
import { strict as assert } from "node:assert";
import { cpuUsage } from "node:process";
import { loadavg } from "node:os";
import { Session } from "node:inspector/promises";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { Batch } from "../packages/core/dist/next/vector/vector.js";
import type { AggregateCall, Rel } from "../packages/core/dist/next/ir/ir.js";
import type { PhysicalType } from "../packages/core/dist/next/types.js";
import type { StorageCore } from "../packages/core/dist/next/testing/storage-core.js";
import { valueAt } from "../packages/core/dist/next/exec/kernels/test-support.js";
import {
  AggregateOperator,
  type AggregateOptions,
} from "../packages/core/dist/next/exec/agg/aggregate.js";
import { SortOperator } from "../packages/core/dist/next/exec/sort.js";
import { OutputArena } from "../packages/core/dist/next/exec/kernels/vectors.js";
import { drive, type Operator } from "../packages/core/dist/next/exec/pipeline.js";
import {
  highcard,
  wide,
  wideSplit,
  wideNative,
  distinct,
  type LightInput,
  type Bytes,
} from "./lib/aggregation-light-kernels.mts";

function need<T>(values: ArrayLike<T>, i: number): T {
  const value = values[i];
  if (value === undefined) throw new Error("fixture width");
  return value;
}
function argument(call: AggregateCall): number {
  const arg = call.args[0];
  if (arg?.op !== "column") throw new Error("direct column required");
  return arg.index;
}
function distribution(samples: number[]) {
  const sorted = [...samples].sort((a, b) => a - b);
  return {
    samples,
    median: need(sorted, sorted.length >>> 1),
    spread: need(sorted, sorted.length - 1) - need(sorted, 0),
  };
}
export async function lightComparison(
  core: StorageCore,
  id: string,
  rel: Extract<Rel, { op: "aggregate" }>,
  types: readonly PhysicalType[],
  captured: readonly Batch[],
  rounds: number,
  directory: string,
  pipeline?: () => Promise<{ rows: number }>,
  feed?: () => Promise<{ rows: number }>,
  candidate?: AggregateOptions,
) {
  const N = captured.reduce((n, b) => n + (b.selection?.count ?? b.length), 0);
  assert.ok(N <= 1000000 && N > 0);
  // Flatten decoded vectors only: no hashing, canonical IDs, key interning, grouping or statistics
  // are computed here. The operator keeps its original dictionary representation (reported).
  const numeric = types.map(() => new Float64Array(N)),
    valid = types.map(() => new Uint8Array(N)),
    strings: Bytes[] = types.map(() => ({
      bytes: new Uint8Array(0),
      offsets: new Int32Array(N + 1),
    }));
  for (let col = 0; col < types.length; col++) {
    const type = need(types, col),
      isText = type.kind === "text";
    let row = 0,
      used = 0;
    const chunks: Uint8Array[] = [];
    for (const batch of captured)
      for (let k = 0; k < (batch.selection?.count ?? batch.length); k++) {
        const i = batch.selection === null ? k : need(batch.selection.rows, k),
          vector = need(batch.columns, col);
        if (isText) {
          const flat =
              vector.form === "dictionary"
                ? vector.dictionary
                : vector.form === "constant"
                  ? vector.value
                  : vector,
            slot =
              vector.form === "dictionary"
                ? need(vector.codes, i)
                : vector.form === "constant"
                  ? 0
                  : i;
          const mask = vector.form === "constant" ? flat.validity : vector.validity;
          assert.ok(mask === null || ((mask[i >>> 5] ?? 0) & (1 << (i & 31))) !== 0);
          assert.ok(
            flat.validity === null || ((flat.validity[slot >>> 5] ?? 0) & (1 << (slot & 31))) !== 0,
          );
          assert.ok(flat.data.layout === "varlen");
          const bytes = flat.data.bytes.subarray(
            need(flat.data.offsets, slot),
            need(flat.data.offsets, slot + 1),
          );
          chunks.push(bytes);
          used += bytes.length;
          need(strings, col).offsets[row + 1] = used;
        } else {
          const v = valueAt(vector, i);
          need(valid, col)[row] = v === null ? 0 : 1;
          const n = v === null ? 0 : Number(v);
          assert.ok(Number.isFinite(n));
          if (typeof v === "bigint") assert.ok(Number.isSafeInteger(n));
          need(numeric, col)[row] = n;
        }
        row++;
      }
    if (isText) {
      const bytes = new Uint8Array(used);
      let at = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, at);
        at += chunk.length;
      }
      need(strings, col).bytes = bytes;
    }
  }
  const input: LightInput = { rows: N, n: [], b: [], valid: new Uint8Array(0) },
    calls = rel.aggregates;
  let kernel: typeof highcard;
  if (id === "agg_highcard_int") {
    input.n = [need(numeric, need(rel.groupBy, 0)), need(numeric, argument(need(calls, 1)))];
    assert.ok(
      need(input.n, 0).every((v) => Number.isInteger(v) && v >= -2147483648 && v <= 2147483647),
    );
    assert.ok(need(input.n, 1).every((v) => Math.abs(v) <= 1e6));
    kernel = highcard;
  } else if (id === "agg_count_distinct") {
    input.b = calls.map((c) => need(strings, argument(c)));
    kernel = distinct;
  } else {
    assert.equal(calls.length, 21);
    input.b = [
      ...rel.groupBy.map((i) => need(strings, i)),
      need(strings, argument(need(calls, 19))),
    ];
    input.n = calls
      .slice(1)
      .map((c) =>
        need(types, argument(c)).kind === "text" ? new Float64Array(0) : need(numeric, argument(c)),
      );
    input.valid = need(valid, argument(need(calls, 18)));
    input.dictionaries = [...rel.groupBy, argument(need(calls, 19))].map((col) => {
      let start = 0;
      return captured.map((batch) => {
        assert.equal(batch.selection, null);
        const vector = need(batch.columns, col);
        assert.ok(vector.form === "dictionary");
        assert.equal(vector.validity, null);
        assert.equal(vector.dictionary.validity, null);
        const data = vector.dictionary.data;
        assert.ok(data.layout === "varlen");
        const segment = {
          start,
          codes: vector.codes.subarray(0, batch.length),
          entries: { bytes: data.bytes, offsets: data.offsets },
        };
        start += batch.length;
        return segment;
      });
    });
    kernel = wide;
    for (let col = 0; col < 20; col++)
      if (col !== 17 && col !== 18)
        assert.ok(
          need(valid, argument(need(calls, col + 1))).every((v) => v === 1),
          "dense bound precondition",
        );
    for (const col of [0, 1, 2, 3, 4, 16])
      assert.ok(
        need(input.n, col).every((v) => Number.isInteger(v) && v >= -2147483648 && v <= 2147483647),
      );
    for (const col of [14, 15]) assert.ok(need(input.n, col).every((v) => Math.abs(v) <= 1e6));
    for (let col = 5; col < 10; col++)
      assert.ok(need(input.n, col).every((v) => Math.abs(v) <= 1e6));
  }
  // All integer SUMs are exact because <=1e6 int32 values total below2^51.
  // AVG inputs |x|<=1e6,N<=1e6: rounded |Sx|<=2NM, |tmp|<=8NM,
  // tmp²<=2*(8NM)² and even N rounded increments fit far below1e308. We still
  // execute all PG Youngs-Cramer Sxx transitions; no per-batch skip is used.
  const plain = kernel.bind(undefined, input);
  const keys =
    id === "agg_highcard_int"
      ? [
          { column: 1, direction: "desc" as const, nulls: "last" as const, collation: null },
          { column: 0, direction: "asc" as const, nulls: "last" as const, collation: null },
        ]
      : rel.groupBy.map((_, column) => ({
          column,
          direction: "asc" as const,
          nulls: "last" as const,
          collation: null,
        }));
  const operator = async (options: AggregateOptions = {}) => {
    const grant = await core.harness.governor.admit({
        label: "light/operator",
        minimumBytes: 16 << 20,
        spillable: true,
      }),
      arena = new OutputArena(grant, 2048),
      rows: unknown[][] = [];
    const sink: Operator = {
      push(batch) {
        for (let i = 0; i < batch.length; i++)
          rows.push(
            batch.columns.map((c) => {
              const v = valueAt(c, i);
              return typeof v === "bigint" ? Number(v) : v;
            }),
          );
        return "ready";
      },
      ready: () => Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    };
    const sort =
      keys.length === 0
        ? null
        : new SortOperator(
            {
              types: rel.output.map((c) => c.type),
              keys,
              limit: id === "agg_highcard_int" ? 100 : null,
              offset: 0,
              withTies: false,
            },
            { grant, spill: core.spill, queryId: 991 },
            sink,
          );
    const aggregate = new AggregateOperator(
      rel,
      { arena, params: { slots: [] }, input: types },
      grant,
      sort ?? sink,
      { ...options, ...(id === "agg_highcard_int" ? { outputTopN: { count: 100, keys } } : {}) },
    );
    try {
      let at = 0;
      await drive(
        {
          next: () => {
            const batch = captured[at++];
            return batch ? { batch } : null;
          },
          abort: () => undefined,
        },
        aggregate,
      );
      return { rows };
    } finally {
      aggregate.abort(undefined);
      sort?.abort(undefined);
      if (sort) await sort.closed;
      arena.close();
      grant.close();
    }
  };
  const expected = plain();
  assert.deepEqual((await operator()).rows, expected.rows);
  if (pipeline) assert.equal((await pipeline()).rows, expected.rows.length);
  // Preserve every Sxx array in the consumed result so its updates cannot be dead work.
  if (expected.sxx) for (const q of expected.sxx) assert.ok(q.every(Number.isFinite));
  for (let k = 0; k < 3; k++) {
    plain();
    await operator();
    if (pipeline) await pipeline();
  }
  const samples = {
      bound: [] as number[],
      split: [] as number[],
      native: [] as number[],
      feed: [] as number[],
      operator: [] as number[],
      candidate: [] as number[],
      whole: [] as number[],
    },
    order: string[] = [],
    loads: number[][] = [];
  const split = id === "agg_3dims_20aggs" ? wideSplit.bind(undefined, input) : plain;
  assert.deepEqual(split().rows, expected.rows);
  for (let w = 0; w < 3; w++) split();
  const native = id === "agg_3dims_20aggs" ? wideNative.bind(undefined, input) : plain;
  const nativeExpected = native();
  assert.deepEqual(nativeExpected.rows, expected.rows);
  if (expected.sxx) {
    assert.deepEqual(split().sxx, expected.sxx);
    assert.deepEqual(nativeExpected.sxx, expected.sxx);
    if (N >= 8192) for (const q of expected.sxx) assert.ok(q.some((v) => v > 0));
  }
  for (let w = 0; w < 3; w++) native();
  if (feed) for (let w = 0; w < 3; w++) assert.equal((await feed()).rows, N);
  if (candidate)
    for (let w = 0; w < 3; w++) assert.deepEqual((await operator(candidate)).rows, expected.rows);
  const operations = {
    bound: plain,
    split,
    native,
    operator: () => operator(),
    candidate: candidate ? () => operator(candidate) : undefined,
    whole: pipeline,
    feed,
  };
  const kinds = (
    ["bound", "split", "native", "operator", "candidate", "whole", "feed"] as const
  ).filter((kind) => operations[kind] !== undefined);
  for (let round = 0; round < rounds; round++) {
    loads.push(loadavg());
    for (let j = 0; j < kinds.length; j++) {
      const kind = need(kinds, (j + round) % kinds.length),
        operation = operations[kind];
      if (!operation) throw new Error("missing benchmark operation");
      const start = cpuUsage(),
        result = await operation(),
        spent = cpuUsage(start);
      samples[kind].push((spent.user + spent.system) / 1000);
      order.push(kind);
      if (kind === "feed") assert.equal(result.rows, N);
      else if (kind !== "whole") assert.deepEqual(result.rows, expected.rows);
      else assert.equal(result.rows, expected.rows.length);
    }
    await writeFile(
      join(directory, id + ".partial.json"),
      JSON.stringify({
        id,
        inputRows: N,
        completedRounds: round + 1,
        requiredRounds: rounds,
        samples,
        order,
        loads,
      }),
    );
    console.error(
      JSON.stringify({
        id,
        componentRound: round,
        cpuMs: Object.fromEntries(
          Object.entries(samples).map(([name, values]) => [name, values.at(-1)]),
        ),
      }),
    );
  }
  const profiles: Record<string, string> = {};
  const ownScripts = new Map<string, string>();
  for (const kind of kinds) {
    const profiler = new Session();
    profiler.connect();
    await profiler.post("Profiler.enable");
    await profiler.post("Profiler.setSamplingInterval", { interval: 100 });
    await profiler.post("Profiler.start");
    const operation = operations[kind];
    if (!operation) throw new Error("missing profile operation");
    await operation();
    const { profile } = await profiler.post("Profiler.stop");
    profiler.disconnect();
    for (const node of profile.nodes)
      if (node.callFrame.url.includes("aggregation-light"))
        ownScripts.set(node.callFrame.scriptId, node.callFrame.url);
    const path = join(directory, id + "." + kind + ".cpuprofile");
    await writeFile(path, JSON.stringify(profile));
    profiles[kind] = path;
  }
  const sources: Record<string, { url: string; path: string }> = {};
  const debuggerSession = new Session();
  debuggerSession.connect();
  await debuggerSession.post("Debugger.enable");
  try {
    for (const [scriptId, url] of ownScripts) {
      const { scriptSource } = await debuggerSession.post("Debugger.getScriptSource", { scriptId });
      const path = join(directory, id + ".script-" + scriptId + ".js");
      await writeFile(path, scriptSource);
      sources[scriptId] = { url, path };
    }
  } finally {
    await debuggerSession.post("Debugger.disable");
    debuggerSession.disconnect();
  }
  return {
    sources,
    id,
    inputRows: N,
    inputLayout: {
      numeric: "Float64, widening excluded as fixture setup",
      flatTextBytes: input.b.reduce((n, c) => n + c.bytes.length, 0),
      dictionarySegments: input.dictionaries?.map((parts) => parts.length),
    },
    resultRows: expected.rows.length,
    order,
    loads,
    profiles,
    cpuMs: Object.fromEntries(
      Object.entries(samples)
        .filter(([, values]) => values.length > 0)
        .map(([kind, values]) => [kind, distribution(values)]),
    ),
    componentOnly: pipeline === undefined && feed === undefined,
    limits:
      "Optimistic fixed-workload TS observation, not a mathematical lower bound or universal JS impossibility result. Raw bound/split read flat decoded bytes/numbers and hash every text row. Native wide consumes the original decoded dictionary segments, hashes/maps all entries and translates every row within timing, then uses packed five-AVG state. No precomputed canonical IDs/ranks. All use exact collision equality. Split/native alias bound for the other shapes (not independent algorithms). Operator keeps original scan dictionary forms and includes matching sort/top-N, copying and governor/driver/task clock. Whole is prepared pipeline/owned frames; outer harness separately measures SQL through cleanup. Feed is scan/project/discard only, not additive with the other sinks. Setup/flattening (including numeric widening to Float64) excluded; kernel allocation/state/final ordering included. Borrowed immutable inputs need no key copy; no memory governor/spill/cancellation in plain TS. AVG Sxx retained. No production changes.",
  };
}
