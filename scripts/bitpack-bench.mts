/**
 * Codec B's decision (bit-packed frame-of-reference widths, next/README "Pages"): the FOR decode
 * kernel (base + delta into an Int32Array) and the pushdown compare on deltas, over 1M values, as
 * stored today (u8/u16/u32 lanes) and bit-packed LSB-first into u32 words at the same bit width,
 * unpacked in the same loop. Medians of 15 runs after a warm-up.
 *
 *   node scripts/bitpack-bench.mts
 */
const N = 1 << 20;

function median(values: readonly number[]): number {
  return [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)] ?? 0;
}

function time(run: () => unknown, repeats = 15): number {
  run();
  const runs: number[] = [];
  for (let k = 0; k < repeats; k++) {
    const started = performance.now();
    run();
    runs.push(performance.now() - started);
  }
  return median(runs);
}

for (const bits of [7, 10, 17, 20]) {
  const width = bits <= 8 ? 1 : bits <= 16 ? 2 : 4;
  const lane =
    width === 1 ? new Uint8Array(N) : width === 2 ? new Uint16Array(N) : new Uint32Array(N);
  const mask = 2 ** bits - 1;
  for (let i = 0; i < N; i++) lane[i] = (Math.imul(i, 2654435761) >>> 0) & mask;
  const words = new Uint32Array(Math.ceil((N * bits) / 32) + 1);
  for (let i = 0; i < N; i++) {
    const bit = i * bits;
    const word = bit >>> 5;
    const shift = bit & 31;
    const value = lane[i] ?? 0;
    words[word] = (words[word] ?? 0) | (value << shift);
    if (shift + bits > 32) words[word + 1] = (words[word + 1] ?? 0) | (value >>> (32 - shift));
  }
  const out = new Int32Array(N);
  const base = 1000;
  const threshold = mask >>> 1;
  const selected = new Uint16Array(N);
  const unpack = (i: number, bit: number): number => {
    const word = bit >>> 5;
    const shift = bit & 31;
    let value = (words[word] ?? 0) >>> shift;
    if (shift + bits > 32) value |= (words[word + 1] ?? 0) << (32 - shift);
    void i;
    return value & mask;
  };
  const aligned = (): void => {
    for (let i = 0; i < N; i++) out[i] = base + (lane[i] ?? 0);
  };
  const packed = (): void => {
    for (let i = 0, bit = 0; i < N; i++, bit += bits) out[i] = base + unpack(i, bit);
  };
  const alignedSelect = (): number => {
    let count = 0;
    for (let i = 0; i < N; i++) if ((lane[i] ?? 0) >= threshold) selected[count++ & 0xffff] = i;
    return count;
  };
  const packedSelect = (): number => {
    let count = 0;
    for (let i = 0, bit = 0; i < N; i++, bit += bits)
      if (unpack(i, bit) >= threshold) selected[count++ & 0xffff] = i;
    return count;
  };
  for (let k = 0; k < 4; k++) {
    aligned();
    packed();
    alignedSelect();
    packedSelect();
  }
  const [a, p, as, ps] = [time(aligned), time(packed), time(alignedSelect), time(packedSelect)];
  console.log(
    `bits ${String(bits)} (u${String(width * 8)}): decode ${a.toFixed(2)} → ${p.toFixed(2)} ms (+${((p / a - 1) * 100).toFixed(0)}%), ` +
      `select ${as.toFixed(2)} → ${ps.toFixed(2)} ms (+${((ps / as - 1) * 100).toFixed(0)}%), ` +
      `bytes ${String(N * width)} → ${String(words.byteLength)}`,
  );
}
