/** Optimistic NUMERIC(12,2) catalogue lower bound, never an executor path.
 * Inputs are decoded, finite, non-NULL cents. All hashing/collisions, dictionary translation,
 * allocations, transitions and final ordering happen here. The committed generator has 20
 * keys, <=1M rows and |c|<1e8: every sum prefix is <1e14<2^47 and therefore exact in float64.
 * Production cannot assume these fixture bounds. There is no governor, spill or pacing here.
 */
export interface NumericFixtureBatch {
  readonly length: number;
  readonly dictionaryId: number;
  readonly codes: Int32Array;
  readonly keys: { readonly bytes: Uint8Array; readonly offsets: Int32Array };
  readonly values: readonly Float64Array[];
}
export type NumericFixtureRow = [string, bigint, bigint, bigint, bigint];
const decoder = new TextDecoder();
export function numericFixedLight(batches: readonly NumericFixtureBatch[]): NumericFixtureRow[] {
  const slots = new Int32Array(64),
    lengths = new Int32Array(20),
    keys = new Uint8Array(20 * 16);
  const sums0 = new Float64Array(20),
    sums1 = new Float64Array(20);
  const min = new Float64Array(20).fill(Infinity),
    max = new Float64Array(20).fill(-Infinity);
  const mapped = new Int32Array(20);
  let groups = 0,
    previous = -1,
    rows = 0;
  for (const batch of batches) {
    rows += batch.length;
    if (rows > 1000000) throw new RangeError("plain fixture row bound");
    const { bytes, offsets } = batch.keys;
    if (batch.dictionaryId !== previous) {
      previous = batch.dictionaryId;
      if (offsets.length > 21) throw new RangeError("plain dictionary bound");
      for (let code = 0; code + 1 < offsets.length; code++) {
        const from = offsets[code] as number,
          end = offsets[code + 1] as number,
          length = end - from;
        if (length > 16) throw new RangeError("plain key width");
        let hash = 0x811c9dc5;
        for (let p = from; p < end; p++) hash = Math.imul(hash ^ (bytes[p] as number), 16777619);
        hash ^= hash >>> 16;
        let at = hash & 63;
        for (;;) {
          const prior = slots[at] as number;
          if (prior === 0) {
            if (groups === 20) throw new RangeError("plain key domain");
            const g = groups++;
            slots[at] = g + 1;
            lengths[g] = length;
            for (let j = 0; j < length; j++) keys[g * 16 + j] = bytes[from + j] as number;
            mapped[code] = g;
            break;
          }
          const g = prior - 1;
          let equal = lengths[g] === length;
          if (equal)
            for (let j = 0; j < length; j++)
              if (keys[g * 16 + j] !== bytes[from + j]) {
                equal = false;
                break;
              }
          if (equal) {
            mapped[code] = g;
            break;
          }
          at = (at + 1) & 63;
        }
      }
    }
    const a = batch.values[0],
      b = batch.values[1],
      c = batch.values[2],
      d = batch.values[3];
    if (!a || !b || !c || !d) throw new Error("plain input width");
    for (let r = 0; r < batch.length; r++) {
      const g = mapped[batch.codes[r] as number] as number;
      const x = a[r] as number,
        y = b[r] as number,
        lo = c[r] as number,
        hi = d[r] as number;
      if (!(Math.abs(x) < 1e8 && Math.abs(y) < 1e8 && Math.abs(lo) < 1e8 && Math.abs(hi) < 1e8))
        throw new RangeError("plain coefficient certificate");
      sums0[g] = (sums0[g] as number) + x;
      sums1[g] = (sums1[g] as number) + y;
      if (lo <= (min[g] as number)) min[g] = lo;
      if (hi >= (max[g] as number)) max[g] = hi;
    }
  }
  const output: NumericFixtureRow[] = [];
  for (let g = 0; g < groups; g++)
    output.push([
      decoder.decode(keys.subarray(g * 16, g * 16 + (lengths[g] as number))),
      BigInt(sums0[g] as number),
      BigInt(sums1[g] as number),
      BigInt(min[g] as number),
      BigInt(max[g] as number),
    ]);
  output.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
  return output;
}
