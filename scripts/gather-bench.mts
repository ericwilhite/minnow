/**
 * The float64 gather path (page/decode.ts): `ChunkReader.gather` of an ascending selection from
 * plain float64 pages (random doubles, so no page is constant or scaled), at 1%, 10% and 50% of
 * the rows. Reported: ns per gathered row, the median of 15 runs after a warm-up. Used to price
 * copying gathered floats as raw words (bit-exact for NaN payloads) against a Number assignment.
 *
 *   npx vite-node scripts/gather-bench.mts
 */
import { performance } from "node:perf_hooks";
import { MemoryExtentSpace, UNLIMITED } from "../packages/core/src/next/page/memory-extents.ts";
import { ChunkReader } from "../packages/core/src/next/page/reader.ts";
import { targetFor } from "../packages/core/src/next/page/test-support.ts";
import { GroupWriter } from "../packages/core/src/next/page/writer.ts";
import type { FlatVector } from "../packages/core/src/next/vector/vector.ts";

const ROWS = 65_536;
const type = { kind: "float64" } as const;
const values = new Float64Array(ROWS);
const words = new Uint32Array(values.buffer);
let state = 0x9e3779b9;
for (let k = 0; k < words.length; k++) {
  state ^= state << 13;
  state ^= state >>> 17;
  state ^= state << 5;
  words[k] = state >>> 0;
}
// Keep them finite doubles (a NaN or Infinity exponent turned into an ordinary one).
for (let row = 0; row < ROWS; row++)
  if (!Number.isFinite(values[row] ?? 0)) values[row] = row * 1.000000001;

const space = new MemoryExtentSpace();
const writer = new GroupWriter({ columns: [{ columnId: 1, type }], writer: space.beginWrite("g") });
const vector: FlatVector = {
  form: "flat",
  type,
  length: ROWS,
  validity: null,
  data: { layout: "f64", values },
};
await writer.append([vector], ROWS);
const chunk = (await writer.finish()).columns[0];
if (chunk === undefined) throw new Error("no chunk");
const reader = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
await reader.load(reader.directory.pages.map((_, page) => page));

const median = (xs: number[]): number =>
  [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)] ?? 0;
const results: string[] = [];
for (const density of [0.01, 0.1, 0.5]) {
  const step = Math.round(1 / density);
  const rows = Uint32Array.from({ length: Math.floor(ROWS / step) }, (_, k) => k * step);
  const target = targetFor(reader, rows.length, 0);
  const run = (): void => {
    for (let repeat = 0; repeat < 20; repeat++) reader.gather(rows, target, 0);
  };
  for (let warm = 0; warm < 5; warm++) run();
  const times: number[] = [];
  for (let k = 0; k < 15; k++) {
    const started = performance.now();
    run();
    times.push(performance.now() - started);
  }
  results.push(
    `${String(density * 100)}%: ${((median(times) * 1e6) / (20 * rows.length)).toFixed(2)} ns/row`,
  );
}
console.log(`gather float64 | ${results.join(" | ")}`);
reader.close();
