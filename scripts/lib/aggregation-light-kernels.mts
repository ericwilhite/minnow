/** Workload-specialized optimistic TS baselines; never executor paths. All input hashing,
 * collision checks, allocations, aggregate transitions and final ordering are timed.
 * Inputs are immutable, already decoded arrays. Int32 sums are exact: N<=1e6 gives
 * N*2^31<2^51. All AVG inputs must be non-NULL; independent Sx/Sxx are retained.
 * Every typed-array index is bounded by the validated fixture/construction. Type assertions
 * erase TS optional-read bookkeeping; these loops do not implement engine fallback semantics.
 * Presizing is optimistic (row-count upper bound, 1000 known wide groups), not a scan hint.
 */
export interface Bytes {
  bytes: Uint8Array;
  offsets: Int32Array;
}
export interface LightInput {
  rows: number;
  n: Float64Array[];
  b: Bytes[];
  valid: Uint8Array;
  dictionaries?: ReadonlyArray<ReadonlyArray<{ start: number; codes: Int32Array; entries: Bytes }>>;
}
export interface LightResult {
  rows: unknown[][];
  sxx?: Float64Array[];
}
function need<T>(values: ArrayLike<T>, i: number): T {
  const v = values[i];
  if (v === undefined) throw new Error("input width");
  return v;
}
const decoder = new TextDecoder();
function text(c: Bytes, row: number): string {
  return decoder.decode(c.bytes.subarray(c.offsets[row] as number, c.offsets[row + 1] as number));
}
export function highcard(input: LightInput): LightResult {
  const key = need(input.n, 0),
    value = need(input.n, 1),
    N = input.rows;
  let size = 1;
  while (size < 2 * N) size *= 2;
  const slots = new Int32Array(size),
    keys = new Int32Array(N),
    counts = new Float64Array(N),
    sums = new Float64Array(N);
  sums.fill(-0);
  let groups = 0;
  for (let i = 0; i < N; i++) {
    const k = key[i] as number;
    // Identity/low-bit hash: the catalogue's uniform int keys occupy a compact range.
    // General collisions still probe and compare the original key.
    let at = k & (size - 1),
      g: number;
    for (;;) {
      const prior = slots[at] as number;
      if (prior === 0) {
        g = groups++;
        slots[at] = g + 1;
        keys[g] = k;
        break;
      }
      g = prior - 1;
      if (keys[g] === k) break;
      at = (at + 1) & (size - 1);
    }
    counts[g] = (counts[g] as number) + 1;
    sums[g] = (sums[g] as number) + (value[i] as number);
  }
  // Heap's root is the worst retained group: smaller count, then larger key.
  const heap = new Int32Array(100);
  let used = 0;
  for (let g = 0; g < groups; g++) {
    if (used < 100) {
      let p = used++;
      while (p > 0) {
        const up = (p - 1) >>> 1,
          u = heap[up] as number;
        if (
          (counts[u] as number) < (counts[g] as number) ||
          (counts[u] === counts[g] && (keys[u] as number) >= (keys[g] as number))
        )
          break;
        heap[p] = u;
        p = up;
      }
      heap[p] = g;
      continue;
    }
    const root = heap[0] as number;
    if (
      (counts[g] as number) < (counts[root] as number) ||
      (counts[g] === counts[root] && (keys[g] as number) >= (keys[root] as number))
    )
      continue;
    let p = 0;
    while (2 * p + 1 < used) {
      let child = 2 * p + 1;
      const a = heap[child] as number,
        b = heap[child + 1] as number;
      if (
        child + 1 < used &&
        ((counts[b] as number) < (counts[a] as number) ||
          (counts[b] === counts[a] && (keys[b] as number) > (keys[a] as number)))
      )
        child++;
      const c = heap[child] as number;
      if (
        (counts[g] as number) < (counts[c] as number) ||
        (counts[g] === counts[c] && (keys[g] as number) >= (keys[c] as number))
      )
        break;
      heap[p] = c;
      p = child;
    }
    heap[p] = g;
  }
  return {
    rows: Array.from(heap.subarray(0, used), (g) => [keys[g], counts[g], sums[g]]).sort(
      (a, b) => Number(b[1]) - Number(a[1]) || Number(a[0]) - Number(b[0]),
    ),
  };
}
export function distinct(input: LightInput): LightResult {
  const totals: number[] = [];
  for (let col = 0; col < 2; col++) {
    const { bytes, offsets } = need(input.b, col),
      view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength),
      N = input.rows;
    let size = 1;
    while (size < 2 * N) size *= 2;
    const slots = new Int32Array(size),
      hashes = new Int32Array(size);
    let count = 0;
    for (let i = 0; i < N; i++) {
      const from = offsets[i] as number,
        end = offsets[i + 1] as number;
      let h = end - from,
        p = from;
      for (; p + 4 <= end; p += 4) h = Math.imul(h ^ view.getInt32(p, true), 0x85ebca77);
      for (; p < end; p++) h = Math.imul(h ^ (bytes[p] as number), 0x9e3779b1);
      h ^= h >>> 16;
      h = Math.imul(h, 0x7feb352d);
      h ^= h >>> 15;
      let at = h & (size - 1),
        found = false;
      for (;;) {
        const old = slots[at] as number;
        if (old === 0) break;
        if (hashes[at] === h) {
          const row = old - 1,
            a = offsets[row] as number,
            b = offsets[row + 1] as number;
          if (b - a === end - from) {
            let j = 0;
            for (; j < end - from; j++) if (bytes[a + j] !== bytes[from + j]) break;
            if (j === end - from) {
              found = true;
              break;
            }
          }
        }
        at = (at + 1) & (size - 1);
      }
      if (!found) {
        slots[at] = i + 1;
        hashes[at] = h;
        count++;
      }
    }
    totals.push(count);
  }
  return { rows: [totals] };
}
export function wide(input: LightInput): LightResult {
  const N = input.rows,
    dim0 = need(input.b, 0),
    dim1 = need(input.b, 1),
    dim2 = need(input.b, 2),
    labels = need(input.b, 3);
  const slots = new Int32Array(2048),
    first = new Int32Array(1000),
    counts = new Float64Array(1000),
    bestText = new Int32Array(1000);
  bestText.fill(-1);
  const v0 = need(input.n, 0);
  const v1 = need(input.n, 1);
  const v2 = need(input.n, 2);
  const v3 = need(input.n, 3);
  const v4 = need(input.n, 4);
  const v5 = need(input.n, 5);
  const v6 = need(input.n, 6);
  const v7 = need(input.n, 7);
  const v8 = need(input.n, 8);
  const v9 = need(input.n, 9);
  const v10 = need(input.n, 10);
  const v11 = need(input.n, 11);
  const v12 = need(input.n, 12);
  const v13 = need(input.n, 13);
  const v14 = need(input.n, 14);
  const v15 = need(input.n, 15);
  const v16 = need(input.n, 16);
  const v19 = need(input.n, 19);
  const a0 = new Float64Array(1000);
  const a1 = new Float64Array(1000);
  const a2 = new Float64Array(1000);
  const a3 = new Float64Array(1000);
  const a4 = new Float64Array(1000);
  const a5 = new Float64Array(1000);
  const a6 = new Float64Array(1000);
  const a7 = new Float64Array(1000);
  const a8 = new Float64Array(1000);
  const a9 = new Float64Array(1000);
  const a10 = new Float64Array(1000);
  const a11 = new Float64Array(1000);
  const a12 = new Float64Array(1000);
  const a13 = new Float64Array(1000);
  const a14 = new Float64Array(1000);
  a14.fill(-0);
  const a15 = new Float64Array(1000);
  a15.fill(-0);
  const a16 = new Float64Array(1000);
  const a17 = new Float64Array(1000);
  const a18 = new Float64Array(1000);
  const q0 = new Float64Array(1000);
  const q1 = new Float64Array(1000);
  const q2 = new Float64Array(1000);
  const q3 = new Float64Array(1000);
  const q4 = new Float64Array(1000);
  let groups = 0;
  for (let i = 0; i < N; i++) {
    let h = 0x811c9dc5;
    for (let p = dim0.offsets[i] as number, end = dim0.offsets[i + 1] as number; p < end; p++)
      h = Math.imul(h ^ (dim0.bytes[p] as number), 0x01000193);
    for (let p = dim1.offsets[i] as number, end = dim1.offsets[i + 1] as number; p < end; p++)
      h = Math.imul(h ^ (dim1.bytes[p] as number), 0x01000193);
    for (let p = dim2.offsets[i] as number, end = dim2.offsets[i + 1] as number; p < end; p++)
      h = Math.imul(h ^ (dim2.bytes[p] as number), 0x01000193);
    let at = h & 2047,
      g: number;
    for (;;) {
      const old = slots[at] as number;
      if (old === 0) {
        g = groups++;
        if (g >= 1000) throw new Error("wide group bound");
        slots[at] = g + 1;
        first[g] = i;
        break;
      }
      g = old - 1;
      const row = first[g] as number;
      let equal = true;
      {
        const a = dim0.offsets[row] as number,
          b = dim0.offsets[row + 1] as number,
          c = dim0.offsets[i] as number,
          d = dim0.offsets[i + 1] as number;
        if (b - a !== d - c) equal = false;
        else
          for (let p = 0; p < b - a; p++)
            if (dim0.bytes[a + p] !== dim0.bytes[c + p]) {
              equal = false;
              break;
            }
      }
      {
        const a = dim1.offsets[row] as number,
          b = dim1.offsets[row + 1] as number,
          c = dim1.offsets[i] as number,
          d = dim1.offsets[i + 1] as number;
        if (b - a !== d - c) equal = false;
        else
          for (let p = 0; p < b - a; p++)
            if (dim1.bytes[a + p] !== dim1.bytes[c + p]) {
              equal = false;
              break;
            }
      }
      {
        const a = dim2.offsets[row] as number,
          b = dim2.offsets[row + 1] as number,
          c = dim2.offsets[i] as number,
          d = dim2.offsets[i + 1] as number;
        if (b - a !== d - c) equal = false;
        else
          for (let p = 0; p < b - a; p++)
            if (dim2.bytes[a + p] !== dim2.bytes[c + p]) {
              equal = false;
              break;
            }
      }
      if (equal) break;
      at = (at + 1) & 2047;
    }
    const before = counts[g] as number,
      n = before + 1;
    counts[g] = n;
    a0[g] = (a0[g] as number) + (v0[i] as number);
    a1[g] = (a1[g] as number) + (v1[i] as number);
    a2[g] = (a2[g] as number) + (v2[i] as number);
    a3[g] = (a3[g] as number) + (v3[i] as number);
    a4[g] = (a4[g] as number) + (v4[i] as number);
    {
      const v = v5[i] as number,
        sum = (a5[g] as number) + v,
        t = v * n - sum;
      q0[g] = before > 0 ? (q0[g] as number) + (t * t) / (n * before) : 0;
      a5[g] = sum;
    }
    {
      const v = v6[i] as number,
        sum = (a6[g] as number) + v,
        t = v * n - sum;
      q1[g] = before > 0 ? (q1[g] as number) + (t * t) / (n * before) : 0;
      a6[g] = sum;
    }
    {
      const v = v7[i] as number,
        sum = (a7[g] as number) + v,
        t = v * n - sum;
      q2[g] = before > 0 ? (q2[g] as number) + (t * t) / (n * before) : 0;
      a7[g] = sum;
    }
    {
      const v = v8[i] as number,
        sum = (a8[g] as number) + v,
        t = v * n - sum;
      q3[g] = before > 0 ? (q3[g] as number) + (t * t) / (n * before) : 0;
      a8[g] = sum;
    }
    {
      const v = v9[i] as number,
        sum = (a9[g] as number) + v,
        t = v * n - sum;
      q4[g] = before > 0 ? (q4[g] as number) + (t * t) / (n * before) : 0;
      a9[g] = sum;
    }
    if (before === 0 || (v10[i] as number) <= (a10[g] as number)) a10[g] = v10[i] as number;
    if (before === 0 || (v11[i] as number) >= (a11[g] as number)) a11[g] = v11[i] as number;
    if (before === 0 || (v12[i] as number) <= (a12[g] as number)) a12[g] = v12[i] as number;
    if (before === 0 || (v13[i] as number) >= (a13[g] as number)) a13[g] = v13[i] as number;
    a14[g] = (a14[g] as number) + (v14[i] as number);
    a15[g] = (a15[g] as number) + (v15[i] as number);
    a16[g] = (a16[g] as number) + (v16[i] as number);
    a17[g] = (a17[g] as number) + (input.valid[i] as number);
    {
      const old = bestText[g] ?? -1;
      let take = old < 0;
      if (!take) {
        let a = labels.offsets[i] as number,
          b = labels.offsets[old] as number;
        const ae = labels.offsets[i + 1] as number,
          be = labels.offsets[old + 1] as number;
        while (a < ae && b < be && labels.bytes[a] === labels.bytes[b]) {
          a++;
          b++;
        }
        take =
          a === ae
            ? b !== be
            : b !== be && (labels.bytes[a] as number) < (labels.bytes[b] as number);
      }
      if (take) bestText[g] = i;
    }
    if (before === 0 || (v19[i] as number) >= (a18[g] as number)) a18[g] = v19[i] as number;
  }
  const rows: unknown[][] = [];
  for (let g = 0; g < groups; g++) {
    const row = first[g] as number;
    rows.push([
      text(dim0, row),
      text(dim1, row),
      text(dim2, row),
      counts[g],
      a0[g],
      a1[g],
      a2[g],
      a3[g],
      a4[g],
      (a5[g] as number) / (counts[g] as number),
      (a6[g] as number) / (counts[g] as number),
      (a7[g] as number) / (counts[g] as number),
      (a8[g] as number) / (counts[g] as number),
      (a9[g] as number) / (counts[g] as number),
      a10[g],
      a11[g],
      a12[g],
      a13[g],
      a14[g],
      a15[g],
      (a16[g] as number) / (counts[g] as number),
      a17[g],
      text(labels, bestText[g] as number),
      a18[g],
    ]);
  }
  rows.sort(
    (a, b) =>
      (String(a[0]) < String(b[0]) ? -1 : String(a[0]) > String(b[0]) ? 1 : 0) ||
      (String(a[1]) < String(b[1]) ? -1 : String(a[1]) > String(b[1]) ? 1 : 0) ||
      (String(a[2]) < String(b[2]) ? -1 : String(a[2]) > String(b[2]) ? 1 : 0),
  );
  return { rows, sxx: [q0, q1, q2, q3, q4] };
}

/** Alternative plain TS structure: one group-id pass, then monomorphic full-column loops.
 * This avoids assuming that fusing 21 states is the speed-of-light layout on a JS JIT. */
function sumFlat(values: Float64Array, gids: Int32Array, N: number): Float64Array {
  const out = new Float64Array(1000);
  out.fill(-0);
  for (let i = 0; i < N; i++) {
    const g = gids[i] as number;
    out[g] = (out[g] as number) + (values[i] as number);
  }
  return out;
}
function avgFlat(
  values: Float64Array,
  gids: Int32Array,
  N: number,
): { out: Float64Array; sxx: Float64Array } {
  const out = new Float64Array(1000),
    counts = new Float64Array(1000),
    sxx = new Float64Array(1000);
  for (let i = 0; i < N; i++) {
    const g = gids[i] as number,
      v = values[i] as number,
      before = counts[g] as number,
      n = before + 1,
      sum = (out[g] as number) + v,
      t = v * n - sum;
    sxx[g] = before > 0 ? (sxx[g] as number) + (t * t) / (n * before) : 0;
    out[g] = sum;
    counts[g] = n;
  }
  for (let g = 0; g < 1000; g++) out[g] = (out[g] as number) / (counts[g] as number);
  return { out, sxx };
}
function extremaFlat(
  values: Float64Array,
  gids: Int32Array,
  N: number,
  max: boolean,
): Float64Array {
  const out = new Float64Array(1000);
  out.fill(max ? -Infinity : Infinity);
  if (max)
    for (let i = 0; i < N; i++) {
      const g = gids[i] as number,
        v = values[i] as number;
      if (v >= (out[g] as number)) out[g] = v;
    }
  else
    for (let i = 0; i < N; i++) {
      const g = gids[i] as number,
        v = values[i] as number;
      if (v <= (out[g] as number)) out[g] = v;
    }
  return out;
}
export function wideSplit(input: LightInput): LightResult {
  const N = input.rows,
    dim0 = need(input.b, 0),
    dim1 = need(input.b, 1),
    dim2 = need(input.b, 2),
    labels = need(input.b, 3),
    slots = new Int32Array(2048),
    first = new Int32Array(1000),
    counts = new Float64Array(1000),
    gids = new Int32Array(N);
  let groups = 0;
  for (let i = 0; i < N; i++) {
    let h = 0x811c9dc5;
    for (let p = dim0.offsets[i] as number, end = dim0.offsets[i + 1] as number; p < end; p++)
      h = Math.imul(h ^ (dim0.bytes[p] as number), 0x01000193);
    for (let p = dim1.offsets[i] as number, end = dim1.offsets[i + 1] as number; p < end; p++)
      h = Math.imul(h ^ (dim1.bytes[p] as number), 0x01000193);
    for (let p = dim2.offsets[i] as number, end = dim2.offsets[i + 1] as number; p < end; p++)
      h = Math.imul(h ^ (dim2.bytes[p] as number), 0x01000193);
    let at = h & 2047,
      g: number;
    for (;;) {
      const old = slots[at] as number;
      if (old === 0) {
        g = groups++;
        if (g >= 1000) throw new Error("wide group bound");
        slots[at] = g + 1;
        first[g] = i;
        break;
      }
      g = old - 1;
      const row = first[g] as number;
      let equal = true;
      {
        const a = dim0.offsets[row] as number,
          b = dim0.offsets[row + 1] as number,
          c = dim0.offsets[i] as number,
          d = dim0.offsets[i + 1] as number;
        if (b - a !== d - c) equal = false;
        else
          for (let p = 0; p < b - a; p++)
            if (dim0.bytes[a + p] !== dim0.bytes[c + p]) {
              equal = false;
              break;
            }
      }
      {
        const a = dim1.offsets[row] as number,
          b = dim1.offsets[row + 1] as number,
          c = dim1.offsets[i] as number,
          d = dim1.offsets[i + 1] as number;
        if (b - a !== d - c) equal = false;
        else
          for (let p = 0; p < b - a; p++)
            if (dim1.bytes[a + p] !== dim1.bytes[c + p]) {
              equal = false;
              break;
            }
      }
      {
        const a = dim2.offsets[row] as number,
          b = dim2.offsets[row + 1] as number,
          c = dim2.offsets[i] as number,
          d = dim2.offsets[i + 1] as number;
        if (b - a !== d - c) equal = false;
        else
          for (let p = 0; p < b - a; p++)
            if (dim2.bytes[a + p] !== dim2.bytes[c + p]) {
              equal = false;
              break;
            }
      }
      if (equal) break;
      at = (at + 1) & 2047;
    }
    gids[i] = g;
    counts[g] = (counts[g] as number) + 1;
  }
  const states: Float64Array[] = [],
    sxx: Float64Array[] = [];
  for (let col = 0; col < 5; col++) states.push(sumFlat(need(input.n, col), gids, N));
  for (let col = 5; col < 10; col++) {
    const result = avgFlat(need(input.n, col), gids, N);
    states.push(result.out);
    sxx.push(result.sxx);
  }
  for (let col = 10; col < 14; col++)
    states.push(extremaFlat(need(input.n, col), gids, N, col % 2 === 1));
  states.push(sumFlat(need(input.n, 14), gids, N), sumFlat(need(input.n, 15), gids, N));
  const means = sumFlat(need(input.n, 16), gids, N);
  for (let g = 0; g < groups; g++) means[g] = (means[g] as number) / (counts[g] as number);
  states.push(means);
  const countValid = new Float64Array(1000);
  for (let i = 0; i < N; i++) {
    const g = gids[i] as number;
    countValid[g] = (countValid[g] as number) + (input.valid[i] as number);
  }
  states.push(countValid);
  const bestText = new Int32Array(1000);
  bestText.fill(-1);
  for (let i = 0; i < N; i++) {
    const g = gids[i] as number,
      old = bestText[g] ?? -1;
    let take = old < 0;
    if (!take) {
      let a = labels.offsets[i] as number,
        b = labels.offsets[old] as number;
      const ae = labels.offsets[i + 1] as number,
        be = labels.offsets[old + 1] as number;
      while (a < ae && b < be && labels.bytes[a] === labels.bytes[b]) {
        a++;
        b++;
      }
      take =
        a === ae ? b !== be : b !== be && (labels.bytes[a] as number) < (labels.bytes[b] as number);
    }
    if (take) bestText[g] = i;
  }
  const timestamp = extremaFlat(need(input.n, 19), gids, N, true),
    rows: unknown[][] = [];
  for (let g = 0; g < groups; g++) {
    const row = first[g] as number;
    rows.push([
      text(dim0, row),
      text(dim1, row),
      text(dim2, row),
      counts[g],
      ...states.map((a) => a[g]),
      text(labels, bestText[g] as number),
      timestamp[g],
    ]);
  }
  rows.sort(
    (a, b) =>
      (String(a[0]) < String(b[0]) ? -1 : String(a[0]) > String(b[0]) ? 1 : 0) ||
      (String(a[1]) < String(b[1]) ? -1 : String(a[1]) > String(b[1]) ? 1 : 0) ||
      (String(a[2]) < String(b[2]) ? -1 : String(a[2]) > String(b[2]) ? 1 : 0),
  );
  return { rows, sxx };
}

/** Dictionary-native input baseline. Mapping every page's entries into global IDs is TIMED.
 * No precomputed equality/rank IDs are supplied by fixture setup. */
function dictionaryCodes(
  segments: ReadonlyArray<{ start: number; codes: Int32Array; entries: Bytes }>,
  N: number,
): { codes: Int32Array; ranks: Int32Array; count: number } {
  const slots = new Int32Array(2048),
    froms = new Int32Array(1000),
    ends = new Int32Array(1000),
    sources: Uint8Array[] = [],
    codes = new Int32Array(N);
  let count = 0;
  for (const segment of segments) {
    const { bytes, offsets } = segment.entries,
      translation = new Int32Array(offsets.length - 1);
    for (let entry = 0; entry < translation.length; entry++) {
      const from = offsets[entry] as number,
        end = offsets[entry + 1] as number;
      let h = 0x811c9dc5;
      for (let p = from; p < end; p++) h = Math.imul(h ^ (bytes[p] as number), 0x01000193);
      let at = h & 2047,
        id: number;
      for (;;) {
        const old = slots[at] as number;
        if (old === 0) {
          id = count++;
          if (id >= 1000) throw new Error("dictionary domain bound");
          slots[at] = id + 1;
          froms[id] = from;
          ends[id] = end;
          sources[id] = bytes;
          break;
        }
        id = old - 1;
        const a = froms[id] as number,
          b = ends[id] as number,
          source = need(sources, id);
        let equal = b - a === end - from;
        if (equal)
          for (let p = 0; p < b - a; p++)
            if (source[a + p] !== bytes[from + p]) {
              equal = false;
              break;
            }
        if (equal) break;
        at = (at + 1) & 2047;
      }
      translation[entry] = id;
    }
    for (let i = 0; i < segment.codes.length; i++)
      codes[segment.start + i] = translation[segment.codes[i] as number] ?? 0;
  }
  const order = Array.from({ length: count }, (_, i) => i);
  order.sort((a, b) => {
    const aa = need(sources, a),
      bb = need(sources, b),
      ae = ends[a] as number,
      be = ends[b] as number;
    let i = froms[a] as number,
      j = froms[b] as number;
    while (i < ae && j < be && aa[i] === bb[j]) {
      i++;
      j++;
    }
    return i === ae ? (j === be ? 0 : -1) : j === be ? 1 : (aa[i] as number) - (bb[j] as number);
  });
  const ranks = new Int32Array(count);
  order.forEach((id, rank) => {
    ranks[id] = rank;
  });
  return { codes, ranks, count };
}
function averagesPacked(
  input: LightInput,
  gids: Int32Array,
): { out: Float64Array[]; sxx: Float64Array[] } {
  const state = new Float64Array(11000),
    v0 = need(input.n, 5),
    v1 = need(input.n, 6),
    v2 = need(input.n, 7),
    v3 = need(input.n, 8),
    v4 = need(input.n, 9),
    N = input.rows;
  for (let i = 0; i < N; i++) {
    const base = (gids[i] as number) * 11,
      before = state[base] as number,
      n = before + 1,
      denominator = n * before;
    state[base] = n;
    {
      const v = v0[i] as number,
        sum = (state[base + 1] as number) + v,
        t = v * n - sum;
      state[base + 6] = before > 0 ? (state[base + 6] as number) + (t * t) / denominator : 0;
      state[base + 1] = sum;
    }
    {
      const v = v1[i] as number,
        sum = (state[base + 2] as number) + v,
        t = v * n - sum;
      state[base + 7] = before > 0 ? (state[base + 7] as number) + (t * t) / denominator : 0;
      state[base + 2] = sum;
    }
    {
      const v = v2[i] as number,
        sum = (state[base + 3] as number) + v,
        t = v * n - sum;
      state[base + 8] = before > 0 ? (state[base + 8] as number) + (t * t) / denominator : 0;
      state[base + 3] = sum;
    }
    {
      const v = v3[i] as number,
        sum = (state[base + 4] as number) + v,
        t = v * n - sum;
      state[base + 9] = before > 0 ? (state[base + 9] as number) + (t * t) / denominator : 0;
      state[base + 4] = sum;
    }
    {
      const v = v4[i] as number,
        sum = (state[base + 5] as number) + v,
        t = v * n - sum;
      state[base + 10] = before > 0 ? (state[base + 10] as number) + (t * t) / denominator : 0;
      state[base + 5] = sum;
    }
  }
  const out: Float64Array[] = [],
    sxx: Float64Array[] = [];
  for (let col = 0; col < 5; col++) {
    const a = new Float64Array(1000),
      q = new Float64Array(1000);
    for (let g = 0; g < 1000; g++) {
      const base = g * 11,
        n = state[base] as number;
      a[g] = n === 0 ? 0 : (state[base + col + 1] as number) / n;
      q[g] = state[base + col + 6] as number;
    }
    out.push(a);
    sxx.push(q);
  }
  return { out, sxx };
}
export function wideNative(input: LightInput): LightResult {
  if (!input.dictionaries) throw new Error("dictionary inputs required");
  const N = input.rows,
    mapped = input.dictionaries.map((segments) => dictionaryCodes(segments, N)),
    d0 = need(mapped, 0),
    d1 = need(mapped, 1),
    d2 = need(mapped, 2),
    labels = need(mapped, 3);
  if (d0.count > 5 || d1.count > 10 || d2.count > 20) throw new Error("catalogue domain bound");
  const slots = new Int32Array(2048),
    first = new Int32Array(1000),
    gids = new Int32Array(N),
    counts = new Float64Array(1000);
  let groups = 0;
  for (let i = 0; i < N; i++) {
    const key =
      (d0.codes[i] as number) + 5 * ((d1.codes[i] as number) + 10 * (d2.codes[i] as number));
    let at = key & 2047,
      g: number;
    for (;;) {
      const old = slots[at] as number;
      if (old === 0) {
        g = groups++;
        slots[at] = g + 1;
        first[g] = i;
        break;
      }
      g = old - 1;
      const row = first[g] as number,
        other =
          (d0.codes[row] as number) +
          5 * ((d1.codes[row] as number) + 10 * (d2.codes[row] as number));
      if (key === other) break;
      at = (at + 1) & 2047;
    }
    gids[i] = g;
    counts[g] = (counts[g] as number) + 1;
  }
  const states: Float64Array[] = [];
  for (let col = 0; col < 5; col++) states.push(sumFlat(need(input.n, col), gids, N));
  const avg = averagesPacked(input, gids);
  states.push(...avg.out);
  for (let col = 10; col < 14; col++)
    states.push(extremaFlat(need(input.n, col), gids, N, col % 2 === 1));
  states.push(sumFlat(need(input.n, 14), gids, N), sumFlat(need(input.n, 15), gids, N));
  const means = sumFlat(need(input.n, 16), gids, N),
    valid = new Float64Array(1000),
    minimum = new Int32Array(1000),
    bestText = new Int32Array(1000);
  minimum.fill(0x7fffffff);
  for (let i = 0; i < N; i++) {
    const g = gids[i] as number,
      rank = labels.ranks[labels.codes[i] as number] ?? 0;
    valid[g] = (valid[g] as number) + (input.valid[i] as number);
    if (rank < (minimum[g] as number)) {
      minimum[g] = rank;
      bestText[g] = i;
    }
  }
  for (let g = 0; g < groups; g++) means[g] = (means[g] as number) / (counts[g] as number);
  states.push(means, valid);
  const timestamp = extremaFlat(need(input.n, 19), gids, N, true),
    rows: unknown[][] = [];
  for (let g = 0; g < groups; g++) {
    const row = first[g] as number;
    rows.push([
      text(need(input.b, 0), row),
      text(need(input.b, 1), row),
      text(need(input.b, 2), row),
      counts[g],
      ...states.map((a) => a[g]),
      text(need(input.b, 3), bestText[g] as number),
      timestamp[g],
    ]);
  }
  rows.sort(
    (a, b) =>
      (String(a[0]) < String(b[0]) ? -1 : String(a[0]) > String(b[0]) ? 1 : 0) ||
      (String(a[1]) < String(b[1]) ? -1 : String(a[1]) > String(b[1]) ? 1 : 0) ||
      (String(a[2]) < String(b[2]) ? -1 : String(a[2]) > String(b[2]) ? 1 : 0),
  );
  return { rows, sxx: avg.sxx };
}

/** Plain-loop experiment on immutable, non-NULL int32 keys and finite float8 values.
 * An exact min/max probe certifies the dense address range before allocating.
 * The catalogue's finite SUM bound (N<=1M, |v|<=1e6) is checked by the caller;
 * all float additions remain in group input order. No NDV/domain is assumed.
 * Wider integer ranges retain the general open-addressing kernel above.
 */
export function highcardDomain(
  input: LightInput,
): LightResult & { domainStrategy: "empty" | "direct" | "hash" } {
  const keys = need(input.n, 0),
    values = need(input.n, 1),
    N = input.rows;
  if (N === 0) return { rows: [], domainStrategy: "empty" };
  let low = Infinity,
    high = -Infinity;
  for (let i = 0; i < N; i++) {
    const key = keys[i] as number;
    if (key < low) low = key;
    if (key > high) high = key;
  }
  const span = high - low + 1;
  if (!Number.isSafeInteger(span) || span < 1) throw new RangeError("invalid int32 domain");
  if (span > 1 << 22) return { ...highcard(input), domainStrategy: "hash" };
  // Count and SUM share a 16-byte record. -0 is SUM's additive identity;
  // it is also equal to 0 for an unseen count.
  const state = new Float64Array(2 * span);
  state.fill(-0);
  for (let i = 0; i < N; i++) {
    const at = ((keys[i] as number) - low) * 2;
    state[at] = (state[at] as number) + 1;
    state[at + 1] = (state[at + 1] as number) + (values[i] as number);
  }
  const heap = new Int32Array(100);
  let used = 0;
  for (let g = 0; g < span; g++) {
    const count = state[g * 2] as number;
    if (count === 0) continue;
    if (used < 100) {
      let p = used++;
      while (p > 0) {
        const up = (p - 1) >>> 1,
          u = heap[up] as number,
          prior = state[u * 2] as number;
        if (prior < count || (prior === count && u >= g)) break;
        heap[p] = u;
        p = up;
      }
      heap[p] = g;
      continue;
    }
    const root = heap[0] as number,
      rootCount = state[root * 2] as number;
    if (count < rootCount || (count === rootCount && g >= root)) continue;
    let p = 0;
    while (2 * p + 1 < used) {
      let child = 2 * p + 1;
      const a = heap[child] as number,
        b = heap[child + 1] as number;
      if (
        child + 1 < used &&
        ((state[b * 2] as number) < (state[a * 2] as number) ||
          (state[b * 2] === state[a * 2] && b > a))
      )
        child++;
      const c = heap[child] as number,
        childCount = state[c * 2] as number;
      if (count < childCount || (count === childCount && g >= c)) break;
      heap[p] = c;
      p = child;
    }
    heap[p] = g;
  }
  return {
    domainStrategy: "direct",
    rows: Array.from(heap.subarray(0, used), (g) => [low + g, state[g * 2], state[g * 2 + 1]]).sort(
      (a, b) => Number(b[1]) - Number(a[1]) || Number(a[0]) - Number(b[0]),
    ),
  };
}
