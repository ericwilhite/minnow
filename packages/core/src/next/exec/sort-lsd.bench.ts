/**
 * LSD row-id scatter versus the production MSD tuples. Prebuilt normalized words, allocation,
 * warmup and permutation/order checks are untimed. MODE=lsd/msd/lsd-tuples, SHAPE=gate/u32/u64,
 * ROWS=100000/1000000, RUNS=3. Run >=7 alternating interleaved rounds and record host load.
 * Gate is the numeric Gate B key shape (float64 then int64); u32/u64 isolate one numeric key.
 * This isolates ordering; whole-operator checks must include preparation and output.
 */
import {
  DirectKeyEncoder,
  PREFIX_WORDS,
  RadixScratch,
  TUPLE_WORDS,
  msdSort,
  msdStackWords,
} from "./sort-keys.js";
import { lsdSortIds } from "./sort-lsd.js";
import { Slice, runToEnd } from "./slices.js";
import type { FlatVector } from "../vector/vector.js";

const n = Number(process.env.ROWS ?? 1_000_000);
const runs = Number(process.env.RUNS ?? 3);
const mode = process.env.MODE ?? "msd";
const shape = process.env.SHAPE ?? "gate";
const input = new Uint32Array(n * TUPLE_WORDS);
if (shape === "gate") {
  const encoder = new DirectKeyEncoder(
    [{ kind: "float64" }, { kind: "int64" }],
    [0, 1].map((column) => ({ column, direction: "asc", nulls: "last", collation: null })),
    { allocate: () => true, free: () => undefined },
  );
  for (let at = 0; at < n; at += 2048) {
    const length = Math.min(2048, n - at);
    const f = Float64Array.from({ length }, (_, i) => {
      const id = at + i;
      const hash = (Math.imul(id ^ 9, 2654435761) >>> 0) ^ (id * 40503);
      return (hash % 1_000_000) / 7;
    });
    const ids = Float64Array.from({ length }, (_, i) => at + i);
    const columns: FlatVector[] = [
      {
        form: "flat",
        type: { kind: "float64" },
        length,
        validity: null,
        data: { layout: "f64", values: f },
      },
      {
        form: "flat",
        type: { kind: "int64" },
        length,
        validity: null,
        data: { layout: "int64", values: { repr: "safe", values: ids } },
      },
    ];
    encoder.encode(columns, null, 0, length);
    for (let i = 0; i < length; i++)
      for (let w = 0; w < PREFIX_WORDS; w++)
        input[(at + i) * TUPLE_WORDS + w] = encoder.prefixes[w]?.[i] ?? 0;
  }
} else {
  let state = 42;
  for (let i = 0; i < n; i++)
    for (let w = 0; w < (shape === "u32" ? 1 : 2); w++) {
      state ^= state << 13;
      state ^= state >>> 17;
      state ^= state << 5;
      input[i * TUPLE_WORDS + w] = state >>> 0;
    }
}
for (let i = 0; i < n; i++) input[i * TUPLE_WORDS + PREFIX_WORDS] = i;
const scratch = new Uint32Array(input.length);
const ids = new Int32Array(n);
const temporary = new Int32Array(n);
const radix = new RadixScratch();
const stack = new Int32Array(msdStackWords(n));
const slice = new Slice(Infinity);

/** Stable 16-bit LSD passes over ids; skip only digits proven equal across all input keys. */
function lsd(words: Uint32Array): void {
  const diffs = radix.diffs;
  diffs.fill(0);
  for (let i = 0; i < n; i++) {
    ids[i] = i;
    for (let w = 0; w < PREFIX_WORDS; w++)
      diffs[w] = (diffs[w] ?? 0) | ((words[i * TUPLE_WORDS + w] ?? 0) ^ (words[w] ?? 0));
  }
  let source = ids;
  let target = temporary;
  const counts = radix.counts;
  for (let w = PREFIX_WORDS - 1; w >= 0; w--)
    for (const shift of [0, 16]) {
      if ((((diffs[w] ?? 0) >>> shift) & 65535) === 0) continue;
      counts.fill(0);
      for (let i = 0; i < n; i++) {
        const digit = ((words[(source[i] ?? 0) * TUPLE_WORDS + w] ?? 0) >>> shift) & 65535;
        counts[digit + 1] = (counts[digit + 1] ?? 0) + 1;
      }
      for (let d = 0; d < 65536; d++) counts[d + 1] = (counts[d + 1] ?? 0) + (counts[d] ?? 0);
      for (let i = 0; i < n; i++) {
        const id = source[i] ?? 0;
        const digit = ((words[id * TUPLE_WORDS + w] ?? 0) >>> shift) & 65535;
        target[counts[digit] ?? 0] = id;
        counts[digit] = (counts[digit] ?? 0) + 1;
      }
      [source, target] = [target, source];
    }
  if (source !== ids) ids.set(source);
}

// Contiguous normalized columns model the store's physical layout; transposition is untimed
// here, just as tuple construction is untimed for MSD. Whole-operator A/B includes preparation.
const columns = Array.from({ length: PREFIX_WORDS }, (_, w) =>
  Uint32Array.from({ length: n }, (_, i) => input[i * TUPLE_WORDS + w] ?? 0),
);

for (let r = 0; r <= runs; r++) {
  const tuples = input.slice();
  (globalThis as { gc?: () => void }).gc?.();
  const start = process.cpuUsage();
  if (mode === "lsd-tuples") lsd(tuples);
  else if (mode === "lsd") runToEnd(lsdSortIds(columns, ids, temporary, n, slice, radix));
  else {
    runToEnd(msdSort(tuples, scratch, n, null, slice, radix, stack));
    for (let i = 0; i < n; i++) ids[i] = tuples[i * TUPLE_WORDS + PREFIX_WORDS] ?? 0;
  }
  const used = process.cpuUsage(start);
  const seen = new Uint8Array(n);
  for (let i = 0; i < n; i++) {
    const id = ids[i] ?? -1;
    if (id < 0 || id >= n || seen[id] !== 0) throw new Error("Lost/duplicate id");
    seen[id] = 1;
    if (i === 0) continue;
    const previous = ids[i - 1] ?? -1;
    for (let w = 0; w < PREFIX_WORDS; w++) {
      const a = input[previous * TUPLE_WORDS + w] ?? 0;
      const b = input[id * TUPLE_WORDS + w] ?? 0;
      if (a > b) throw new Error("Unsorted normalized key");
      if (a < b) break;
      if (w === PREFIX_WORDS - 1 && previous > id) throw new Error("Unstable tie");
    }
  }
  if (r > 0)
    console.log(JSON.stringify({ shape, mode, rows: n, cpuMs: (used.user + used.system) / 1000 }));
}
