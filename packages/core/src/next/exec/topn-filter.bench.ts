/** Top-N key/filter component, prebuilt batches and fixed threshold. MODE=encode models
 * the parent (encode every row); MODE=filter uses the conservative raw gate first.
 * Warmup, allocation, and full-key result validation are outside CPU timing.
 * Compare >=7 alternating rounds; ROWS/RUNS control size and repetitions.
 */
import type { FlatVector } from "../vector/vector.js";
import { DirectKeyEncoder, FirstKeyThreshold, PREFIX_WORDS, type SortColumn } from "./sort-keys.js";
const n = Number(process.env.ROWS ?? 1000000),
  runs = Number(process.env.RUNS ?? 5);
const first = { kind: "float64" } as const,
  second = { kind: "int64" } as const;
const keys: SortColumn[] = [
  { column: 0, direction: "desc", nulls: "last", collation: null },
  { column: 1, direction: "asc", nulls: "last", collation: null },
];
function columns(start: number, length: number, threshold = false): FlatVector[] {
  return [
    {
      form: "flat",
      type: first,
      length,
      validity: null,
      data: {
        layout: "f64",
        values: Float64Array.from({ length }, (_, i) =>
          threshold ? 0.99 : (Math.imul(start + i, 2654435761) >>> 0) / 4294967296,
        ),
      },
    },
    {
      form: "flat",
      type: second,
      length,
      validity: null,
      data: {
        layout: "int64",
        values: {
          repr: "safe",
          values: Float64Array.from({ length }, (_, i) => (threshold ? 0 : start + i)),
        },
      },
    },
  ];
}
const input: FlatVector[][] = [];
for (let at = 0; at < n; at += 2048) input.push(columns(at, Math.min(2048, n - at)));
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
const encoder = new DirectKeyEncoder([first, second], keys, charge);
const boundary = columns(0, 1, true);
encoder.encode(boundary, null, 0, 1);
const words = Uint32Array.from({ length: PREFIX_WORDS }, (_, i) => encoder.prefixes[i]?.[0] ?? 0);
const key = keys[0];
const value = boundary[0];
if (key === undefined || value === undefined) throw new Error("Missing threshold");
charge.allocate(FirstKeyThreshold.BYTES);
const gate = new FirstKeyThreshold(key);
gate.capture(value, 0);
const kept = new Uint16Array(2048);
function run(filter: boolean): number {
  let sum = 0;
  for (const cols of input) {
    const vector = cols[0];
    if (vector === undefined) throw new Error("Missing key");
    const count = filter ? gate.filter(vector, null, vector.length, kept) : vector.length;
    if (count === 0) continue;
    encoder.encode(cols, filter ? kept : null, 0, count);
    for (let i = 0; i < count; i++) {
      let order = 0;
      for (let w = 0; w < PREFIX_WORDS && order === 0; w++) {
        const x = (encoder.prefixes[w]?.[i] ?? 0) >>> 0,
          y = words[w] ?? 0;
        if (x !== y) order = x < y ? -1 : 1;
      }
      if (order < 0) sum += 1 + (filter ? (kept[i] ?? 0) : i);
    }
  }
  return sum;
}
const expected = run(false);
if (run(true) !== expected) throw new Error("Gate changed survivors");
const filter = process.env.MODE !== "encode";
for (let r = 0; r <= runs; r++) {
  (globalThis as { gc?: () => void }).gc?.();
  const start = process.cpuUsage();
  const got = run(filter);
  const used = process.cpuUsage(start);
  if (got !== expected) throw new Error("Wrong survivors");
  if (r > 0)
    console.log(
      JSON.stringify({
        mode: filter ? "filter" : "encode",
        rows: n,
        cpuMs: (used.user + used.system) / 1000,
      }),
    );
}
encoder.close(charge);
charge.free(FirstKeyThreshold.BYTES);
if (held !== 0) throw new Error("Charge leak");
