/** Optimistic decoded star loop, never a production route. Every dictionary/domain certificate,
 * translation, index allocation, chain reversal, group transition and final order is timed.
 * The fixture certifies keys d3_[0,50), d4_[0,100), d7_[0,1000), unique dimension keys/labels,
 * tier t[0,4), <=1M finite |f00|<=1e6. No float reassociation; every group sum prefix is finite.
 */
import type { FlatVector, Vector } from "../../packages/core/dist/next/vector/vector.js";
import type { StarInput, StarRow } from "./star-fixture.mts";
import { finishStar } from "./star-native.mts";
const decoder = new TextDecoder();
function slot(vector: Vector, row: number): [FlatVector, number] {
  if (
    vector.form !== "constant" &&
    vector.validity !== null &&
    ((vector.validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0
  )
    throw new Error("Dense plain fixture required");
  if (vector.form === "constant") return [vector.value, 0];
  return vector.form === "dictionary" ? [vector.dictionary, vector.codes[row] ?? 0] : [vector, row];
}
function bytes(flat: FlatVector, row: number): Uint8Array {
  if (flat.validity !== null && ((flat.validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0)
    throw new Error("Non-null plain keys required");
  if (flat.data.layout !== "varlen") throw new Error("Byte fixture required");
  return flat.data.bytes.subarray(flat.data.offsets[row] ?? 0, flat.data.offsets[row + 1] ?? 0);
}
function text(vector: Vector, row: number): string {
  const [flat, index] = slot(vector, row);
  return decoder.decode(bytes(flat, index));
}
function number(vector: Vector, row: number): number {
  const [flat, index] = slot(vector, row);
  if (flat.data.layout !== "bool" && flat.data.layout !== "f64")
    throw new Error("Numeric fixture required");
  return flat.data.values[index] ?? 0;
}
function domain(flat: FlatVector, row: number, index: number, card: number): number {
  const data = flat.data;
  if (data.layout !== "varlen") throw new Error("Byte key fixture");
  const values = data.bytes,
    from = data.offsets[row] ?? 0,
    to = data.offsets[row + 1] ?? 0;
  if (
    values[from] !== 100 ||
    values[from + 1] !== 48 + index ||
    values[from + 2] !== 95 ||
    to - from <= 3
  )
    throw new Error("Fixture key prefix");
  let id = 0;
  for (let p = from + 3; p < to; p++) {
    const digit = (values[p] ?? 0) - 48;
    if (digit < 0 || digit > 9) throw new Error("Fixture key digit");
    id = 10 * id + digit;
  }
  if (id >= card || (to - from > 4 && values[from + 3] === 48))
    throw new Error("Fixture key domain");
  return id;
}
function tier(value: string): number {
  if (value.length !== 2 || value[0] !== "t") throw new Error("Fixture tier");
  const n = value.charCodeAt(1) - 48;
  if (n < 0 || n > 3) throw new Error("Fixture tier domain");
  return n;
}
function at<T>(values: readonly T[], index: number): T {
  const value = values[index];
  if (value === undefined) throw new Error("Missing decoded input");
  return value;
}
export function lightStar(fixture: StarInput): StarRow[] {
  if (fixture.rows > 1000000) throw new Error("Plain row certificate");
  const dimensions = ["dim_d03", "dim_d04", "dim_d07"].map((name, index) => {
    const source = fixture.named.get(name);
    if (!source) throw new Error("Missing dimension");
    const card = [50, 100, 1000][index] ?? 0,
      domainIndex = [3, 4, 7][index] ?? 0;
    const active = new Uint8Array(card),
      tiers = new Uint8Array(card),
      seen = new Uint8Array(card),
      order = new Int32Array(card),
      labels: string[] = new Array(card),
      unique = new Set<string>();
    const key = source.columns.indexOf("k"),
      t = source.columns.indexOf("tier"),
      predicate = source.columns.indexOf(index === 0 ? "active" : index === 1 ? "weight" : "tier"),
      label = source.columns.indexOf("label");
    let rows = 0;
    for (const batch of source.batches) {
      if (batch.selection !== null) throw new Error("Unfiltered fixture required");
      for (let row = 0; row < batch.length; row++) {
        const [flat, position] = slot(at(batch.columns, key), row),
          id = domain(flat, position, domainIndex, card);
        if (seen[id] !== 0) throw new Error("Unique dimension key certificate");
        seen[id] = 1;
        order[rows++] = id;
        tiers[id] = tier(text(at(batch.columns, t), row));
        active[id] =
          index === 0
            ? Number(number(at(batch.columns, predicate), row) === 1)
            : index === 1
              ? Number(number(at(batch.columns, predicate), row) > 1)
              : Number(text(at(batch.columns, predicate), row) === "t1");
        if (index === 2) {
          const value = text(at(batch.columns, label), row);
          if (unique.has(value)) throw new Error("Unique group label certificate");
          unique.add(value);
          labels[id] = value;
        }
      }
    }
    if (rows !== card) throw new Error("Complete dimension domain certificate");
    return { active, tiers, order, labels };
  });
  const a = at(dimensions, 0),
    b = at(dimensions, 1),
    c = at(dimensions, 2),
    source = fixture.named.get("events_wide");
  if (!source) throw new Error("Missing fact");
  const ai = source.columns.indexOf("d03"),
    bi = source.columns.indexOf("d04"),
    ci = source.columns.indexOf("d07"),
    fi = source.columns.indexOf("f00"),
    N = fixture.rows;
  const ca = new Uint8Array(N),
    cb = new Uint8Array(N),
    cc = new Uint16Array(N),
    floats = new Float64Array(N),
    nextA = new Int32Array(N),
    nextB = new Int32Array(N),
    headA = new Int32Array(50),
    headB = new Int32Array(100);
  const translations = [
    new WeakMap<FlatVector, Int32Array>(),
    new WeakMap<FlatVector, Int32Array>(),
    new WeakMap<FlatVector, Int32Array>(),
  ];
  const translate = (vector: Vector, k: number): Int32Array | null => {
    if (vector.form !== "dictionary") return null;
    const map = at(translations, k);
    let result = map.get(vector.dictionary);
    if (result) return result;
    result = Int32Array.from({ length: vector.dictionary.length }, (_, row) =>
      domain(vector.dictionary, row, [3, 4, 7][k] ?? 0, [50, 100, 1000][k] ?? 0),
    );
    map.set(vector.dictionary, result);
    return result;
  };
  let first = 0;
  for (const batch of source.batches) {
    if (batch.selection !== null) throw new Error("Unfiltered fact required");
    const av = at(batch.columns, ai),
      bv = at(batch.columns, bi),
      cv = at(batch.columns, ci),
      fv = at(batch.columns, fi);
    const am = translate(av, 0),
      bm = translate(bv, 1),
      cm = translate(cv, 2);
    const af = av.form === "flat" ? av : av.form === "constant" ? av.value : av.dictionary,
      bf = bv.form === "flat" ? bv : bv.form === "constant" ? bv.value : bv.dictionary,
      cf = cv.form === "flat" ? cv : cv.form === "constant" ? cv.value : cv.dictionary;
    const acodes = av.form === "dictionary" ? av.codes : null,
      bcodes = bv.form === "dictionary" ? bv.codes : null,
      ccodes = cv.form === "dictionary" ? cv.codes : null;
    if (fv.form !== "flat" || fv.data.layout !== "f64" || fv.validity !== null)
      throw new Error("Dense float fixture");
    const values = fv.data.values;
    for (const vector of [av, bv, cv])
      if (vector.form !== "constant" && vector.validity !== null)
        throw new Error("Dense fact keys");
    for (let row = 0; row < batch.length; row++) {
      const r = first + row;
      if (r >= N) throw new Error("Fact row certificate");
      const ac =
          am && acodes
            ? (am[acodes[row] ?? 0] ?? 0)
            : domain(af, av.form === "constant" ? 0 : row, 3, 50),
        bc =
          bm && bcodes
            ? (bm[bcodes[row] ?? 0] ?? 0)
            : domain(bf, bv.form === "constant" ? 0 : row, 4, 100),
        cd =
          cm && ccodes
            ? (cm[ccodes[row] ?? 0] ?? 0)
            : domain(cf, cv.form === "constant" ? 0 : row, 7, 1000),
        value = values[row] ?? 0;
      if (!(Math.abs(value) <= 1e6)) throw new Error("Finite float prefix certificate");
      ca[r] = ac;
      cb[r] = bc;
      cc[r] = cd;
      floats[r] = value;
      if (c.active[cd]) {
        nextA[r] = headA[ac] ?? 0;
        headA[ac] = r + 1;
      }
    }
    first += batch.length;
  }
  if (first !== N) throw new Error("Fact row count");
  // Exactly the two prepend-chain reversals of the native physical tree.
  for (const ac of a.order)
    if (a.active[ac])
      for (let p = headA[ac] ?? 0; p;) {
        const row = p - 1,
          bc = cb[row] ?? 0;
        nextB[row] = headB[bc] ?? 0;
        headB[bc] = row + 1;
        p = nextA[row] ?? 0;
      }
  const counts = new Float64Array(16000),
    sums = new Float64Array(16000).fill(-0);
  for (const bc of b.order)
    if (b.active[bc])
      for (let p = headB[bc] ?? 0; p;) {
        const row = p - 1,
          g = (a.tiers[ca[row] ?? 0] ?? 0) + 4 * (b.tiers[bc] ?? 0) + 16 * (cc[row] ?? 0);
        counts[g] = (counts[g] ?? 0) + 1;
        sums[g] = (sums[g] ?? 0) + (floats[row] ?? 0);
        p = nextB[row] ?? 0;
      }
  const result: StarRow[] = [];
  for (let g = 0; g < counts.length; g++)
    if (counts[g]) {
      const label = c.labels[g >>> 4];
      if (label === undefined) throw new Error("Missing label");
      result.push({
        tier_a: `t${g % 4}`,
        tier_b: `t${(g >>> 2) % 4}`,
        label_c: label,
        n: counts[g] ?? 0,
        s: sums[g] ?? 0,
      });
    }
  return finishStar(result);
}
