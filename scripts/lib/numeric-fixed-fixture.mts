import { strict as assert } from "node:assert";

import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { Rel, AggregateCall, OutputColumn } from "../../packages/core/dist/next/ir/ir.js";
import type { PhysicalType } from "../../packages/core/dist/next/types.js";
import type { Batch, Vector, FlatVector } from "../../packages/core/dist/next/vector/vector.js";
import type { Operator } from "../../packages/core/dist/next/exec/pipeline.js";
import {
  numericFixedLight,
  type NumericFixtureBatch,
  type NumericFixtureRow,
} from "./numeric-fixed-light.mts";

export async function numericFixture(N: number) {
  const generator = (await import("../next-executor-workload/gen.mjs")) as {
    wideSpec(n: number): Array<{ name: string; fam: string; k: number }>;
    value(col: { name: string; fam: string; k: number }, row: number, rows: number): string;
  };
  const spec = generator.wideSpec(200),
    columns = ["d02", "n05", "n06", "n07", "n08"].map((name) => {
      const c = spec.find((c) => c.name === name);
      if (!c) throw new Error("catalogue column");
      return c;
    });
  const TEXT: PhysicalType = { kind: "text" },
    D: PhysicalType = { kind: "decimal", precision: 12, scale: 2 },
    SUM: PhysicalType = { kind: "decimal", precision: null, scale: null };
  const types = [TEXT, D, D, D, D];
  const encoder = new TextEncoder(),
    fixture: NumericFixtureBatch[] = [],
    batches: Batch[] = [];
  for (let start = 0, dictionaryId = 1; start < N; start += 6144, dictionaryId++) {
    const end = Math.min(N, start + 6144),
      codes = new Int32Array(end - start),
      entries: string[] = [],
      index = new Map<string, number>();
    const values = Array.from({ length: 4 }, () => new Float64Array(end - start));
    for (let r = start; r < end; r++) {
      const key = generator.value(required(columns[0]), r, N);
      let code = index.get(key);
      if (code === undefined) {
        code = entries.length;
        index.set(key, code);
        entries.push(key);
      }
      codes[r - start] = code;
      for (let c = 0; c < 4; c++) {
        const coefficient = Number(
          generator.value(required(columns[c + 1]), r, N).replace(".", ""),
        );
        assert.ok(Number.isSafeInteger(coefficient));
        required(values[c])[r - start] = coefficient;
      }
    }
    const chunks = entries.map((key) => encoder.encode(key)),
      offsets = new Int32Array(entries.length + 1);
    for (let c = 0; c < entries.length; c++)
      offsets[c + 1] = required(offsets[c]) + required(chunks[c]).length;
    const bytes = new Uint8Array(required(offsets[entries.length]));
    for (let c = 0; c < entries.length; c++) bytes.set(required(chunks[c]), offsets[c]);
    const keys = { bytes, offsets },
      dictionary: FlatVector = {
        form: "flat",
        type: TEXT,
        length: entries.length,
        validity: null,
        data: { layout: "varlen", ...keys },
      };
    for (let at = 0; at < end - start; at += 2048) {
      const length = Math.min(2048, end - start - at),
        selected = values.map((v) => v.subarray(at, at + length));
      const part = codes.subarray(at, at + length);
      fixture.push({ length, dictionaryId, codes: part, keys, values: selected });
      const vectors: Vector[] = [
        {
          form: "dictionary",
          type: TEXT,
          length,
          validity: null,
          dictionaryId,
          dictionary,
          codes: part,
        },
        ...selected.map((values): FlatVector => ({
          form: "flat",
          type: D,
          length,
          validity: null,
          data: {
            layout: "decimal",
            coefficients: { repr: "safe", values },
            scale: 2,
            scales: null,
            specials: null,
          },
        })),
      ];
      batches.push({ length, columns: vectors, selection: null });
    }
  }
  const calls: AggregateCall[] = (["sum", "sum", "min", "max"] as const).map((fn, c) => ({
    fn,
    type: c < 2 ? SUM : D,
    args: [{ op: "column", index: c + 1, type: D }],
    distinct: false,
    filter: null,
    orderBy: [],
  }));
  const schema: OutputColumn[] = ["d02", "s5", "s6", "m7", "x8"].map((name, c) => ({
    name,
    type: c === 0 ? TEXT : c < 3 ? SUM : D,
    nullable: c !== 0,
  }));
  const rel: Extract<Rel, { op: "aggregate" }> = {
    id: 1,
    op: "aggregate",
    output: schema,
    input: { id: 0, op: "values", output: [], rows: [] },
    groupBy: [0],
    groupingSets: null,
    aggregates: calls,
  };
  async function native(runtime: string) {
    const load = (name: string) =>
      import(pathToFileURL(join(runtime, "packages/core/dist/next", name + ".js")).href);
    const [{ AggregateOperator }, { OutputArena }, { drive }, { Governor }, { valueAt }] =
      (await Promise.all([
        load("exec/agg/aggregate"),
        load("exec/kernels/vectors"),
        load("exec/pipeline"),
        load("memory/governor"),
        load("exec/kernels/test-support"),
      ])) as [
        typeof import("../../packages/core/dist/next/exec/agg/aggregate.js"),
        typeof import("../../packages/core/dist/next/exec/kernels/vectors.js"),
        typeof import("../../packages/core/dist/next/exec/pipeline.js"),
        typeof import("../../packages/core/dist/next/memory/governor.js"),
        typeof import("../../packages/core/dist/next/exec/kernels/test-support.js"),
      ];
    const governor = new Governor(256 << 20);
    return {
      async prepare() {
        const grant = await governor.admit({
          label: "numeric component",
          minimumBytes: 64 << 20,
          spillable: true,
        });
        return {
          async run(): Promise<NumericFixtureRow[]> {
            const rows: NumericFixtureRow[] = [],
              arena = new OutputArena(grant, 2048);
            const sink: Operator = {
              push(batch) {
                for (let row = 0; row < batch.length; row++) {
                  const key = valueAt(required(batch.columns[0]), row);
                  if (typeof key !== "string") throw new Error("key output");
                  const out: bigint[] = [];
                  for (let c = 1; c < 5; c++) {
                    const v = valueAt(required(batch.columns[c]), row);
                    if (!Array.isArray(v) || typeof v[0] !== "bigint" || v[1] !== 2)
                      throw new Error("numeric output");
                    out.push(v[0]);
                  }
                  rows.push([
                    key,
                    required(out[0]),
                    required(out[1]),
                    required(out[2]),
                    required(out[3]),
                  ]);
                }
                return "ready";
              },
              ready: () => Promise.resolve(),
              finish: () => undefined,
              abort: () => undefined,
            };
            const agg = new AggregateOperator(
              rel,
              { arena, input: types, params: { slots: [] } },
              grant,
              sink,
            );
            let at = 0;
            try {
              await drive(
                {
                  next: () => (at < batches.length ? { batch: required(batches[at++]) } : null),
                  abort: () => undefined,
                },
                agg,
              );
              rows.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
              return rows;
            } finally {
              agg.abort(undefined);
              arena.close();
            }
          },
          close() {
            assert.equal(grant.allocated, 0);
            grant.close();
            assert.equal(governor.usedBytes, 0);
          },
        };
      },
    };
  }

  return {
    source: fixture,
    native,
    plain: () => numericFixedLight(fixture),
    expected: numericFixedLight(fixture),
    batches: batches.length,
  };
}
function required<T>(value: T | undefined | null): T {
  if (value == null) throw new Error("Missing numeric fixture value");
  return value;
}
