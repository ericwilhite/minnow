/** Authorize only tight dedicated buffers named by the source transfer, charged to this sink.
 * Cached pages, pooled slabs, reusable output, foreign grants and wide JS arrays copy instead.
 */
import type { ByteBudget } from "../access/space.js";
import type { Batch, DictionaryVector, FlatVector } from "../vector/vector.js";
import type { Transfer } from "./pipeline.js";
import type { WireVector } from "./result.js";
import { frameTransferList } from "./frame-buffers.js";

const EXACT_CAPACITY = (bytes: number): number => bytes;

function ownedView(
  view: ArrayBufferView,
  bytes: number,
  allowed: ReadonlySet<ArrayBuffer>,
  capacity: (bytes: number) => number,
): boolean {
  return (
    view.buffer instanceof ArrayBuffer &&
    view.byteOffset === 0 &&
    view.byteLength === capacity(bytes) &&
    view.buffer.byteLength === view.byteLength &&
    allowed.has(view.buffer)
  );
}
function ownedValidity(
  v: FlatVector | DictionaryVector,
  n: number,
  allowed: ReadonlySet<ArrayBuffer>,
  capacity: (bytes: number) => number,
): boolean {
  return v.validity === null || ownedView(v.validity, 4 * Math.ceil(n / 32), allowed, capacity);
}
function ownsFlat(
  v: FlatVector,
  n: number,
  allowed: ReadonlySet<ArrayBuffer>,
  capacity: (bytes: number) => number,
): boolean {
  if (!ownedValidity(v, n, allowed, capacity)) return false;
  const d = v.data;
  switch (d.layout) {
    case "bool":
    case "i32":
    case "f32":
    case "f64":
      return ownedView(d.values, n * d.values.BYTES_PER_ELEMENT, allowed, capacity);
    case "int64":
      return ownedView(d.values.values, 8 * n, allowed, capacity);
    case "fixed16":
      return ownedView(d.bytes, 16 * n, allowed, capacity);
    case "interval":
      return (
        ownedView(d.months, 4 * n, allowed, capacity) &&
        ownedView(d.days, 4 * n, allowed, capacity) &&
        ownedView(d.micros.values, 8 * n, allowed, capacity)
      );
    case "varlen":
      return (
        (d.offsets[0] ?? 0) === 0 &&
        ownedView(d.offsets, 4 * (n + 1), allowed, capacity) &&
        ownedView(d.bytes, d.offsets[n] ?? 0, allowed, capacity)
      );
    case "decimal": {
      if (d.scales !== null && !ownedView(d.scales, 2 * n, allowed, capacity)) return false;
      if (d.specials !== null && !ownedView(d.specials, n, allowed, capacity)) return false;
      const c = d.coefficients;
      return c.repr === "int128"
        ? ownedView(c.limbs, 16 * n, allowed, capacity)
        : c.repr !== "wide" && ownedView(c.values, 8 * n, allowed, capacity);
    }
  }
}

export function ownedResultColumns(
  batch: Batch,
  ids: ReadonlyArray<number | null>,
  transfer: Transfer | null,
  budget: ByteBudget,
): Array<WireVector | null> | null {
  if (
    batch.selection !== null ||
    !transfer?.detachableBuffers ||
    transfer.detachableBudget !== budget
  )
    return null;
  const allowed = transfer.detachableBuffers,
    n = batch.length,
    capacity = transfer.detachableCapacity ?? EXACT_CAPACITY;
  const columns = batch.columns.map((v, c): WireVector | null => {
    if (v.form === "flat" && ownsFlat(v, n, allowed, capacity))
      return { form: "flat", length: n, data: v.data, validity: v.validity };
    const id = ids[c];
    if (
      v.form === "dictionary" &&
      id !== null &&
      id !== undefined &&
      ownedView(v.codes, 4 * n, allowed, capacity) &&
      ownedValidity(v, n, allowed, capacity)
    )
      return {
        form: "dictionary",
        length: n,
        codes: v.codes,
        validity: v.validity,
        dictionary: id,
      };
    return null;
  });
  const adopted = new Set<ArrayBuffer>();
  for (let c = 0; c < columns.length; c++) {
    const column = columns[c];
    if (!column) continue;
    const buffers = frameTransferList({ rowCount: n, columns: [column], dictionaries: [] });
    // An authorized source may repeat a buffer in several logical columns. Adopt it
    // at most once; copy other aliases before source readiness/transfer can detach it.
    if (buffers.some((buffer) => adopted.has(buffer))) columns[c] = null;
    else for (const buffer of buffers) adopted.add(buffer);
  }
  return columns.some((v) => v !== null) ? columns : null;
}
