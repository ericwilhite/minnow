/** Authorize only tight dedicated buffers named by the source transfer, charged to this sink.
 * Cached pages, pooled slabs, reusable output, foreign grants and wide JS arrays copy instead.
 */
import type { ByteBudget } from "../access/space.js";
import type { Batch, DictionaryVector, FlatVector } from "../vector/vector.js";
import type { Transfer } from "./pipeline.js";
import type { WireVector } from "./result.js";

function ownedView(
  view: ArrayBufferView,
  bytes: number,
  allowed: ReadonlySet<ArrayBuffer>,
): boolean {
  return (
    view.buffer instanceof ArrayBuffer &&
    view.byteOffset === 0 &&
    view.byteLength === bytes &&
    view.buffer.byteLength === bytes &&
    allowed.has(view.buffer)
  );
}
function ownedValidity(
  v: FlatVector | DictionaryVector,
  n: number,
  allowed: ReadonlySet<ArrayBuffer>,
): boolean {
  return v.validity === null || ownedView(v.validity, 4 * Math.ceil(n / 32), allowed);
}
function ownsFlat(v: FlatVector, n: number, allowed: ReadonlySet<ArrayBuffer>): boolean {
  if (!ownedValidity(v, n, allowed)) return false;
  const d = v.data;
  switch (d.layout) {
    case "bool":
    case "i32":
    case "f32":
    case "f64":
      return ownedView(d.values, n * d.values.BYTES_PER_ELEMENT, allowed);
    case "int64":
      return ownedView(d.values.values, 8 * n, allowed);
    case "fixed16":
      return ownedView(d.bytes, 16 * n, allowed);
    case "interval":
      return (
        ownedView(d.months, 4 * n, allowed) &&
        ownedView(d.days, 4 * n, allowed) &&
        ownedView(d.micros.values, 8 * n, allowed)
      );
    case "varlen":
      return (
        (d.offsets[0] ?? 0) === 0 &&
        ownedView(d.offsets, 4 * (n + 1), allowed) &&
        ownedView(d.bytes, d.offsets[n] ?? 0, allowed)
      );
    case "decimal": {
      if (d.scales !== null && !ownedView(d.scales, 2 * n, allowed)) return false;
      if (d.specials !== null && !ownedView(d.specials, n, allowed)) return false;
      const c = d.coefficients;
      return c.repr === "int128"
        ? ownedView(c.limbs, 16 * n, allowed)
        : c.repr !== "wide" && ownedView(c.values, 8 * n, allowed);
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
    n = batch.length;
  const columns = batch.columns.map((v, c): WireVector | null => {
    if (v.form === "flat" && ownsFlat(v, n, allowed))
      return { form: "flat", length: n, data: v.data, validity: v.validity };
    const id = ids[c];
    if (
      v.form === "dictionary" &&
      id !== null &&
      id !== undefined &&
      ownedView(v.codes, 4 * n, allowed) &&
      ownedValidity(v, n, allowed)
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
  return columns.some((v) => v !== null) ? columns : null;
}
