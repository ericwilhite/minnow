/** Direct fixed-width key CPU benchmark: prebuilt batches, untimed byte-key oracle and warmup.
 * Bundle with esbuild for Node; compare revision bundles in >=5 interleaved rounds.
 * ROWS/RUNS control rows and repetitions. This is an encoder component, not the browser gate.
 */
import type { PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";
import { DirectKeyEncoder, SortKeyEncoder, PREFIX_WORDS, type SortColumn } from "./sort-keys.js";
const n = Number(process.env.ROWS ?? 1_000_000);
const runs = Number(process.env.RUNS ?? 5);
const floatType: PhysicalType = { kind: "float64" };
const intType: PhysicalType = { kind: "int64" };
const types: PhysicalType[] = [floatType, intType];
const keys: SortColumn[] = types.map((_, column) => ({
  column,
  direction: "asc",
  nulls: "last",
  collation: null,
}));
const input: FlatVector[][] = [];
for (let at = 0; at < n; at += 2048) {
  const length = Math.min(2048, n - at);
  const f = Float64Array.from(
    { length },
    (_, i) => ((Math.imul(at + i, 2654435761) >>> 0) % 1000000) / 7,
  );
  const id = Float64Array.from({ length }, (_, i) => at + i);
  input.push([
    { form: "flat", type: floatType, length, validity: null, data: { layout: "f64", values: f } },
    {
      form: "flat",
      type: intType,
      length,
      validity: null,
      data: { layout: "int64", values: { repr: "safe", values: id } },
    },
  ]);
}
let held = 0;
const charge = {
  allocate(bytes: number) {
    held += bytes;
    return true;
  },
  free(bytes: number) {
    held -= bytes;
  },
};
const direct = new DirectKeyEncoder(types, keys, charge);
const oracle = new SortKeyEncoder(keys, charge);
// Validate every encoded word against the frozen byte encoding, outside timing.
for (const columns of input) {
  const first = columns[0];
  if (first === undefined) throw new Error("Missing column");
  const length = first.length;
  direct.encode(columns, null, 0, length);
  if (oracle.encode(columns, null, 0, length).done !== length)
    throw new Error("Oracle refused input");
  for (let w = 0; w < PREFIX_WORDS; w++) {
    const actual = direct.prefixes[w];
    const expected = oracle.prefixes[w];
    if (actual === undefined || expected === undefined) throw new Error("Missing prefix");
    for (let i = 0; i < length; i++)
      if (actual[i] !== expected[i]) throw new Error("Direct words differ from byte keys");
  }
}
oracle.close(charge);
for (let r = 0; r <= runs; r++) {
  (globalThis as { gc?: () => void }).gc?.();
  const start = process.cpuUsage();
  for (const columns of input) {
    const first = columns[0];
    if (first === undefined) throw new Error("Missing column");
    direct.encode(columns, null, 0, first.length);
  }
  const used = process.cpuUsage(start);
  if (r > 0) console.log(JSON.stringify({ rows: n, cpuMs: (used.user + used.system) / 1000 }));
}
direct.close(charge);
if (held !== 0) throw new Error("Encoder charge leak");
