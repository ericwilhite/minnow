/** Fixed-fixture structural experiment, not an executor implementation. Dictionary mapping,
 * group assignment, allocation, every PG Sx/Sxx transition and output sorting are timed.
 * Finite non-NULL numeric inputs and <=1M rows give the same bounds as wideNative.
 * The production version must certify these preconditions and restore ordinary state otherwise.
 */
import {
  dictionaryCodes,
  type LightInput,
  type LightResult,
} from "./aggregation-light-kernels.mts";

function need<T>(values: ArrayLike<T>, index: number): T {
  const value = values[index];
  if (value === undefined) throw new Error("wide record fixture width");
  return value;
}
const decoder = new TextDecoder();

export function wideRecord(input: LightInput): LightResult {
  if (!input.dictionaries) throw new Error("dictionary inputs required");
  const N = input.rows,
    mapped = input.dictionaries.map((segments) => dictionaryCodes(segments, N)),
    d0 = need(mapped, 0),
    d1 = need(mapped, 1),
    d2 = need(mapped, 2),
    labels = need(mapped, 3);
  if (d0.count > 5 || d1.count > 10 || d2.count > 20) throw new Error("fixture domain bound");
  const slots = new Int32Array(2048),
    first = new Int32Array(1000),
    gids = new Int32Array(N),
    state = new Float64Array(32 * 1000);
  let groups = 0;
  for (let row = 0; row < N; row++) {
    const key =
      (d0.codes[row] as number) + 5 * ((d1.codes[row] as number) + 10 * (d2.codes[row] as number));
    let at = key & 2047;
    for (;;) {
      const old = slots[at] as number;
      if (old === 0) {
        const g = groups++;
        slots[at] = g + 1;
        first[g] = row;
        gids[row] = g;
        break;
      }
      const g = old - 1,
        prior = first[g] as number,
        other =
          (d0.codes[prior] as number) +
          5 * ((d1.codes[prior] as number) + 10 * (d2.codes[prior] as number));
      if (other === key) {
        gids[row] = g;
        break;
      }
      at = (at + 1) & 2047;
    }
  }
  const i0 = need(input.n, 0),
    i1 = need(input.n, 1),
    i2 = need(input.n, 2),
    i3 = need(input.n, 3),
    i4 = need(input.n, 4),
    a0 = need(input.n, 5),
    a1 = need(input.n, 6),
    a2 = need(input.n, 7),
    a3 = need(input.n, 8),
    a4 = need(input.n, 9),
    mnf = need(input.n, 10),
    mxf = need(input.n, 11),
    mni = need(input.n, 12),
    mxi = need(input.n, 13),
    s0 = need(input.n, 14),
    s1 = need(input.n, 15),
    ai = need(input.n, 16),
    ts = need(input.n, 19);
  for (let row = 0; row < N; row++) {
    const at = (gids[row] as number) << 5,
      before = state[at] as number,
      n = before + 1,
      denominator = n * before;
    state[at] = n;
    state[at + 1] = (state[at + 1] as number) + (i0[row] as number);
    state[at + 2] = (state[at + 2] as number) + (i1[row] as number);
    state[at + 3] = (state[at + 3] as number) + (i2[row] as number);
    state[at + 4] = (state[at + 4] as number) + (i3[row] as number);
    state[at + 5] = (state[at + 5] as number) + (i4[row] as number);
    {
      const v = a0[row] as number,
        sum = (state[at + 6] as number) + v,
        t = v * n - sum;
      state[at + 11] = before > 0 ? (state[at + 11] as number) + (t * t) / denominator : 0;
      state[at + 6] = sum;
    }
    {
      const v = a1[row] as number,
        sum = (state[at + 7] as number) + v,
        t = v * n - sum;
      state[at + 12] = before > 0 ? (state[at + 12] as number) + (t * t) / denominator : 0;
      state[at + 7] = sum;
    }
    {
      const v = a2[row] as number,
        sum = (state[at + 8] as number) + v,
        t = v * n - sum;
      state[at + 13] = before > 0 ? (state[at + 13] as number) + (t * t) / denominator : 0;
      state[at + 8] = sum;
    }
    {
      const v = a3[row] as number,
        sum = (state[at + 9] as number) + v,
        t = v * n - sum;
      state[at + 14] = before > 0 ? (state[at + 14] as number) + (t * t) / denominator : 0;
      state[at + 9] = sum;
    }
    {
      const v = a4[row] as number,
        sum = (state[at + 10] as number) + v,
        t = v * n - sum;
      state[at + 15] = before > 0 ? (state[at + 15] as number) + (t * t) / denominator : 0;
      state[at + 10] = sum;
    }
    if (before === 0) {
      state[at + 16] = mnf[row] as number;
      state[at + 17] = mxf[row] as number;
      state[at + 18] = mni[row] as number;
      state[at + 19] = mxi[row] as number;
      state[at + 20] = -0;
      state[at + 21] = -0;
      state[at + 24] = ts[row] as number;
    } else {
      if (!((state[at + 16] as number) < (mnf[row] as number))) state[at + 16] = mnf[row] as number;
      if (!((state[at + 17] as number) > (mxf[row] as number))) state[at + 17] = mxf[row] as number;
      if (!((state[at + 18] as number) < (mni[row] as number))) state[at + 18] = mni[row] as number;
      if (!((state[at + 19] as number) > (mxi[row] as number))) state[at + 19] = mxi[row] as number;
      if (!((state[at + 24] as number) > (ts[row] as number))) state[at + 24] = ts[row] as number;
    }
    state[at + 20] = (state[at + 20] as number) + (s0[row] as number);
    state[at + 21] = (state[at + 21] as number) + (s1[row] as number);
    state[at + 22] = (state[at + 22] as number) + (ai[row] as number);
    state[at + 23] = (state[at + 23] as number) + (input.valid[row] as number);
  }
  const rank = new Int32Array(groups).fill(0x7fffffff),
    bestText = new Int32Array(groups);
  for (let row = 0; row < N; row++) {
    const g = gids[row] as number,
      r = labels.ranks[labels.codes[row] as number] as number;
    if (r < (rank[g] as number)) {
      rank[g] = r;
      bestText[g] = row;
    }
  }
  const text = (column: number, row: number) => {
    const b = need(input.b, column);
    return decoder.decode(b.bytes.subarray(b.offsets[row] as number, b.offsets[row + 1] as number));
  };
  const rows: unknown[][] = [],
    sxx = Array.from({ length: 5 }, () => new Float64Array(1000));
  for (let g = 0; g < groups; g++) {
    const at = g << 5,
      n = state[at] as number,
      firstRow = first[g] as number;
    for (let c = 0; c < 5; c++) need(sxx, c)[g] = state[at + 11 + c] as number;
    rows.push([
      text(0, firstRow),
      text(1, firstRow),
      text(2, firstRow),
      n,
      ...state.slice(at + 1, at + 6),
      ...state.slice(at + 6, at + 11).map((v) => v / n),
      ...state.slice(at + 16, at + 22),
      (state[at + 22] as number) / n,
      state[at + 23],
      text(3, bestText[g] as number),
      state[at + 24],
    ]);
  }
  rows.sort((a, b) => {
    for (let c = 0; c < 3; c++) {
      if (String(a[c]) < String(b[c])) return -1;
      if (String(a[c]) > String(b[c])) return 1;
    }
    return 0;
  });
  return { rows, sxx };
}
