/** Store gather CPU, prebuilt input and IDs; allocation/warmup/validation outside timing.
 * Compare revision bundles in >=7 alternating rounds. ROWS/RUNS set work and repetitions.
 */
import type { PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";
import { ColumnStore } from "./column-store.js";
const n = Number(process.env.ROWS ?? 1_000_000);
const runs = Number(process.env.RUNS ?? 5);
const floatType: PhysicalType = { kind: "float64" };
const intType: PhysicalType = { kind: "int32", bits: 32 };
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
const source = new ColumnStore([floatType, intType], charge, { chunkRows: 2048 });
for (let at = 0; at < n; at += 2048) {
  const length = Math.min(2048, n - at);
  const columns: FlatVector[] = [
    {
      form: "flat",
      type: floatType,
      length,
      validity: null,
      data: { layout: "f64", values: Float64Array.from({ length }, (_, i) => (at + i) / 4) },
    },
    {
      form: "flat",
      type: intType,
      length,
      validity: null,
      data: { layout: "i32", values: Int32Array.from({ length }, (_, i) => at + i) },
    },
  ];
  if (source.append(columns, null, 0, length) !== length) throw new Error("Input refused");
}
const out = new ColumnStore([floatType, intType], charge, {
  chunkRows: 2048,
  targetBytes: 64 * 1024,
});
const modes = process.argv.slice(2).length
  ? process.argv.slice(2)
  : ["contiguous", "random", "partitioned"];
for (const mode of modes) {
  if (mode === "partitioned" && n % 50 !== 0)
    throw new Error("Partitioned rows must be divisible by 50");
  const ids = Int32Array.from({ length: n }, (_, i) =>
    mode === "contiguous"
      ? i
      : mode === "partitioned"
        ? (i % (n / 50)) * 50 + Math.floor(i / (n / 50))
        : (Math.imul(i, 2654435761) >>> 0) % n,
  );
  const copy = (validate: boolean) => {
    for (let at = 0; at < n; at += 2048) {
      const count = Math.min(2048, n - at);
      out.reset();
      if (out.appendFrom(source, [0, 1], ids, at, count) !== count)
        throw new Error("Output refused");
      if (validate) {
        const f = out.chunk(0, 0).data;
        const ints = out.chunk(1, 0).data;
        if (f.layout !== "f64" || ints.layout !== "i32") throw new Error("Wrong layout");
        for (let i = 0; i < count; i++)
          if (f.values[i] !== (ids[at + i] ?? 0) / 4 || ints.values[i] !== ids[at + i])
            throw new Error("Wrong gathered row");
      }
    }
  };
  copy(true);
  for (let warm = 0; warm < 5; warm++) copy(false);
  for (let r = 0; r <= runs; r++) {
    (globalThis as { gc?: () => void }).gc?.();
    const start = process.cpuUsage();
    copy(false);
    const used = process.cpuUsage(start);
    if (r > 0)
      console.log(JSON.stringify({ mode, rows: n, cpuMs: (used.user + used.system) / 1000 }));
  }
}
out.close();
source.close();
if (held !== 0) throw new Error("Charge leak");
