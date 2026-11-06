/**
 * Isolated prefix radix benchmark. Bundle with esbuild, run with node --expose-gc. Input,
 * scratch allocation, warmup and validation are outside process CPU timing. Compare two
 * revision bundles in at least five interleaved rounds; ROWS and RUNS override the defaults.
 * This measures the sort component, not a SQL query or the browser Gate B lane.
 */
import { PREFIX_WORDS, RadixScratch, TUPLE_WORDS, msdSort, msdStackWords } from "./sort-keys.js";
import { Slice, runToEnd } from "./slices.js";

const n = Number(process.env.ROWS ?? 1_000_000);
const runs = Number(process.env.RUNS ?? 5);
const input = new Uint32Array(n * TUPLE_WORDS);
let state = 42;
for (let i = 0; i < n; i++) {
  state ^= state << 13;
  state ^= state >>> 17;
  state ^= state << 5;
  input[i * TUPLE_WORDS] = state >>> 0;
  input[i * TUPLE_WORDS + 1] = i;
  input[i * TUPLE_WORDS + PREFIX_WORDS] = i;
}
const scratch = new Uint32Array(input.length);
const radix = new RadixScratch();
const stack = new Int32Array(msdStackWords(n));
const slice = new Slice(Infinity);
for (let r = 0; r <= runs; r++) {
  const tuples = input.slice();
  (globalThis as { gc?: () => void }).gc?.();
  const start = process.cpuUsage();
  runToEnd(msdSort(tuples, scratch, n, null, slice, radix, stack));
  const used = process.cpuUsage(start);
  // Verify ordering and that each payload still belongs to its input key.
  let sum = 0;
  const seen = new Uint8Array(n);
  for (let i = 0; i < n; i++) {
    const at = i * TUPLE_WORDS;
    const id = tuples[at + PREFIX_WORDS] ?? 0;
    const key = tuples[at] ?? 0;
    if (id >= n || seen[id] !== 0) throw new Error("Duplicate or invalid row id");
    seen[id] = 1;
    if (key !== input[id * TUPLE_WORDS] || tuples[at + 1] !== id)
      throw new Error("Tuple payload changed");
    if (i > 0 && key < (tuples[at - TUPLE_WORDS] ?? 0)) throw new Error("Unsorted prefix");
    if (i > 0 && key === tuples[at - TUPLE_WORDS] && id <= (tuples[at - TUPLE_WORDS + 1] ?? 0))
      throw new Error("Unsorted secondary prefix");
    for (let w = 2; w < PREFIX_WORDS; w++)
      if (tuples[at + w] !== 0) throw new Error("Prefix changed");
    sum += id;
  }
  if (sum !== (n * (n - 1)) / 2) throw new Error("Lost rows");
  if (r > 0) console.log(JSON.stringify({ rows: n, cpuMs: (used.user + used.system) / 1000 }));
}
