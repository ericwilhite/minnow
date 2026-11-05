/**
 * Benchmarks xxHash32 (packages/core/src/next/hash/xxhash32.ts) against a byte-at-a-time XXH32
 * (what it was before ruling R15.8's word reads) by input length and alignment, and shows where
 * `WORD_READ_BYTES` comes from: word reads pay a `DataView` per call, so they win only from a
 * few hundred bytes.
 *
 *   npx vite-node scripts/xxhash-bench.mts [--repeats 9]
 *
 * Each figure is the median of `repeats` samples (ns per call); the inputs walk a 4 MiB buffer so
 * they are not all in one cache line. The machine is shared: compare ratios, not absolute numbers.
 */
import { performance } from "node:perf_hooks";
import { WORD_READ_BYTES, xxHash32 } from "../packages/core/src/next/hash/xxhash32.ts";

const args = process.argv.slice(2);
const index = args.indexOf("--repeats");
const REPEATS = index >= 0 ? Number(args[index + 1]) : 9;

const P1 = 0x9e3779b1;
const P2 = 0x85ebca77;
const P3 = 0xc2b2ae3d;
const P4 = 0x27d4eb2f;
const P5 = 0x165667b1;
const read32 = (b: Uint8Array, i: number): number =>
  (b[i] ?? 0) | ((b[i + 1] ?? 0) << 8) | ((b[i + 2] ?? 0) << 16) | ((b[i + 3] ?? 0) << 24);
const round = (acc: number, lane: number): number => {
  const sum = (acc + Math.imul(lane, P2)) | 0;
  return Math.imul((sum << 13) | (sum >>> 19), P1);
};

/** The byte-at-a-time XXH32 (the implementation before R15.8). */
function bytewise(bytes: Uint8Array, seed: number, start: number, end: number): number {
  const length = end - start;
  let at = start;
  let hash: number;
  if (length >= 16) {
    let v1 = (seed + P1 + P2) | 0;
    let v2 = (seed + P2) | 0;
    let v3 = seed | 0;
    let v4 = (seed - P1) | 0;
    for (; at <= end - 16; at += 16) {
      v1 = round(v1, read32(bytes, at));
      v2 = round(v2, read32(bytes, at + 4));
      v3 = round(v3, read32(bytes, at + 8));
      v4 = round(v4, read32(bytes, at + 12));
    }
    hash =
      (((v1 << 1) | (v1 >>> 31)) +
        ((v2 << 7) | (v2 >>> 25)) +
        ((v3 << 12) | (v3 >>> 20)) +
        ((v4 << 18) | (v4 >>> 14))) |
      0;
  } else hash = (seed + P5) | 0;
  hash = (hash + length) | 0;
  for (; at + 4 <= end; at += 4) {
    hash = (hash + Math.imul(read32(bytes, at), P3)) | 0;
    hash = Math.imul((hash << 17) | (hash >>> 15), P4);
  }
  for (; at < end; at++) {
    hash = (hash + Math.imul(bytes[at] ?? 0, P5)) | 0;
    hash = Math.imul((hash << 11) | (hash >>> 21), P1);
  }
  hash ^= hash >>> 15;
  hash = Math.imul(hash, P2);
  hash ^= hash >>> 13;
  hash = Math.imul(hash, P3);
  hash ^= hash >>> 16;
  return hash >>> 0;
}

const buffer = new Uint8Array(4 * 1024 * 1024 + 64);
for (let k = 0; k < buffer.length; k++) buffer[k] = (k * 131 + (k >>> 9)) & 0xff;

type Hash = (bytes: Uint8Array, seed: number, start: number, end: number) => number;

function sample(hash: Hash, size: number, offset: number): number {
  const calls = Math.max(20, Math.floor(8e6 / Math.max(size, 32)));
  let sink = 0;
  const begin = performance.now();
  for (let k = 0, at = offset; k < calls; k++) {
    sink ^= hash(buffer, k, at, at + size);
    at += size + 8;
    if (at + size > buffer.length) at = offset;
  }
  const ns = ((performance.now() - begin) * 1e6) / calls;
  if (sink === 0.5) console.log(sink);
  return ns;
}

function median(hash: Hash, size: number, offset: number): number {
  sample(hash, size, offset);
  const samples = Array.from({ length: REPEATS }, () => sample(hash, size, offset));
  samples.sort((a, b) => a - b);
  return samples[Math.floor(samples.length / 2)] ?? 0;
}

for (let size = 0; size < 2 * WORD_READ_BYTES; size += 7)
  if (xxHash32(buffer, size, 3, 3 + size) !== bytewise(buffer, size, 3, 3 + size))
    throw new Error(`xxHash32 disagrees with the byte-wise reference at ${String(size)} bytes`);

console.log(`WORD_READ_BYTES = ${String(WORD_READ_BYTES)}, median of ${String(REPEATS)}`);
console.log("bytes | offset | byte-wise ns | xxHash32 ns | MB/s byte-wise → xxHash32 | ×");
for (const size of [16, 48, 128, 255, 256, 512, 1024, 4096, 32_768, 1_048_576])
  for (const offset of [0, 1]) {
    const before = median(bytewise, size, offset);
    const after = median(xxHash32, size, offset);
    const mbps = (ns: number): string => ((size / ns) * 1e3).toFixed(0);
    console.log(
      [
        size,
        offset,
        before.toFixed(1),
        after.toFixed(1),
        `${mbps(before)} → ${mbps(after)}`,
        (before / after).toFixed(2),
      ].join(" | "),
    );
  }
