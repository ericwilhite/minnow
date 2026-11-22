/** Typed OLD/NEW body parameters. Copy unique native columns, not JS row values; retire the
 * image borrow before starting any producer or memory wait. This is not a second admission.
 */
import type { ColumnDef } from "../access/catalog.js";
import type { TriggerBinding } from "../access/trigger-def.js";
import {
  ROW_IMAGE_UNREAD,
  type NativeCommandBoundary,
  type RowBindingLease,
  type RowBindingProjection,
  type RowImageRef,
} from "../access/statement-command.js";
import { ExecutionError } from "./errors.js";
import { pauseIfDue, type TaskWork } from "../task.js";
import { samePhysicalType } from "../types.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { FlatVector, Int64Values, VectorData } from "../vector/vector.js";
import { flatBytes } from "./agg/values.js";
import type { BoundParameters } from "./executor.js";
import { ROW_IMAGE_FRAGMENT_BYTES } from "./row-image-layout.js";
import { sortRowView } from "./sort-row.js";

interface BindingColumn {
  readonly source: "old" | "new";
  readonly position: number;
  readonly type: TriggerBinding["type"];
}
function invalid(what: string): never {
  throw new ExecutionError(
    `Invalid trigger binding ${what}`,
    "MINNOW_TRIGGER_BINDING_STATE",
    "XX000",
  );
}
/** The complete ordinary metadata/view minimum, additive to the native image reader minimum.
 * Reserve the maximum live body's minimum once, not a sum across rows/triggers/occurrences.
 * Exact scalar buffers (including one unique payload copy) use the shared prepaid image step.
 */
export class TriggerBindingProgram {
  readonly projection: RowBindingProjection;
  readonly minimumBytes: number;
  readonly #columns: readonly BindingColumn[];
  readonly #slots: readonly number[];
  readonly #tableId: number;
  readonly #columnCount: number;
  constructor(
    owner: {
      readonly id: number;
      readonly columns: ReadonlyArray<Pick<ColumnDef, "id" | "name" | "type">>;
    },
    bindings: readonly TriggerBinding[],
  ) {
    this.#tableId = owner.id;
    this.#columnCount = owner.columns.length;
    const columns: BindingColumn[] = [],
      slots: number[] = [],
      unique = new Map<string, number>(),
      old: number[] = [],
      next: number[] = [];
    for (const ref of bindings) {
      const position = owner.columns.findIndex((c) => c.id === ref.columnId),
        col = owner.columns[position];
      if (col?.name !== ref.name || !samePhysicalType(col.type, ref.type))
        invalid("column identity/type");
      const key = `${ref.source}:${String(ref.columnId)}`;
      let index = unique.get(key);
      if (index === undefined) {
        index = columns.length;
        unique.set(key, index);
        columns.push({ source: ref.source, position, type: ref.type });
        (ref.source === "old" ? old : next).push(ref.columnId);
      }
      slots.push(index);
    }
    this.#columns = columns;
    this.#slots = slots;
    this.projection = { old, new: next };
    this.minimumBytes = 1024 + 1024 * columns.length + 8 * slots.length;
  }
  async bind(
    images: RowImageRef,
    reader: Pick<NativeCommandBoundary, "readRowBindings">,
    charge: ByteCharge,
    work: TaskWork & { readonly signal: AbortSignal },
  ): Promise<OwnedTriggerBindings> {
    const result = new BindingScope(charge, this.minimumBytes);
    let lease: RowBindingLease | null = null;
    try {
      work.signal.throwIfAborted();
      // A binding-free body does not open or decode even a wide image.
      if (this.#columns.length) {
        lease = await reader.readRowBindings(images, this.projection, work.signal);
        work.signal.throwIfAborted();
        if (lease.tableId !== this.#tableId) invalid("image owner");
        for (const ref of this.#columns) {
          const side = lease[ref.source],
            vector = side?.[ref.position];
          if (
            side?.length !== this.#columnCount ||
            vector === undefined ||
            vector === ROW_IMAGE_UNREAD
          )
            invalid("unread requested column");
          if (vector.length !== 1 || !samePhysicalType(vector.type, ref.type))
            invalid("image shape/type");
          const row = sortRowView(vector, 0),
            data = row.data;
          // sortRowView ignores invalid payloads. Its bounded NULL/view scratch is in minimum.
          const bytes =
            data.layout === "varlen"
              ? (data.offsets[1] ?? 0) - (data.offsets[0] ?? 0) + 8
              : flatBytes(row) - (row.validity?.byteLength ?? 0);
          const copyBytes = bytes + (row.validity?.byteLength ?? 0);
          // The read funded this copy with the image (owner/registry-scalar.ts
          // bindingCopyBytes): charge exactly that, which is never less than the copy.
          const funded = lease.copyBytes?.[ref.source]?.[ref.position];
          if (funded !== undefined && funded < copyBytes) invalid("binding copy above its funding");
          result.take(funded ?? copyBytes);
          result.push({
            ...row,
            validity: row.validity?.slice() ?? null,
            data: await copy(data, work),
          });
        }
      }
      result.bindSlots(this.#slots);
      lease?.release();
      lease = null;
      return result;
    } catch (error) {
      const errors = [error];
      try {
        lease?.release();
      } catch (releaseError) {
        errors.push(releaseError);
      }
      result.close();
      if (errors.length > 1)
        throw new AggregateError(errors, "Trigger binding cleanup failed", { cause: error });
      throw error;
    }
  }
}
/** One body invocation owns these slots until its source has fully retired. Text/decimal/
 * timestamp values never round-trip through Number, Date or caller ParameterValue coercion.
 * close invalidates the parameter scope before returning credit; callers retain no slot after it.
 */
export interface OwnedTriggerBindings extends BoundParameters {
  close(): void;
}
class BindingScope implements OwnedTriggerBindings {
  #slots: FlatVector[] | null;
  #unique: FlatVector[];
  #charged = 0;
  constructor(
    readonly charge: ByteCharge,
    minimumBytes: number,
  ) {
    this.take(minimumBytes);
    this.#slots = [];
    this.#unique = [];
  }
  get slots(): readonly FlatVector[] {
    if (!this.#slots) throw new Error("Trigger binding scope is closed");
    return this.#slots;
  }
  take(bytes: number): void {
    if (!this.charge.allocate(bytes)) throw new Error("Trigger binding workspace was not admitted");
    this.#charged += bytes;
  }
  push(vector: FlatVector): void {
    this.#unique.push(vector);
  }
  bindSlots(indices: readonly number[]): void {
    this.#slots = indices.map((i) => this.#unique[i] ?? invalid("parameter slot"));
  }
  close(): void {
    if (!this.#slots) return;
    this.#slots.length = 0;
    this.#slots = null;
    this.#unique.length = 0;
    this.charge.free(this.#charged);
    this.#charged = 0;
  }
}
const copyInt64 = (v: Int64Values): Int64Values =>
  v.repr === "safe"
    ? { repr: "safe", values: v.values.slice() }
    : { repr: "int64", values: v.values.slice() };
async function copy(data: VectorData, work: TaskWork): Promise<VectorData> {
  const pause = pauseIfDue(work);
  if (pause) await pause;
  switch (data.layout) {
    case "bool":
      return { ...data, values: data.values.slice() };
    case "i32":
      return { ...data, values: data.values.slice() };
    case "f32":
      return { ...data, values: data.values.slice() };
    case "f64":
      return { ...data, values: data.values.slice() };
    case "int64":
      return { ...data, values: copyInt64(data.values) };
    case "fixed16":
      return { ...data, bytes: data.bytes.slice() };
    case "interval":
      return {
        ...data,
        months: data.months.slice(),
        days: data.days.slice(),
        micros: copyInt64(data.micros),
      };
    case "decimal": {
      const c = data.coefficients;
      return {
        ...data,
        scales: data.scales?.slice() ?? null,
        specials: data.specials?.slice() ?? null,
        coefficients:
          c.repr === "int128"
            ? { repr: "int128", limbs: c.limbs.slice() }
            : c.repr === "wide"
              ? { repr: "wide", values: c.values.slice() }
              : copyInt64(c),
      };
    }
    case "varlen": {
      const from = data.offsets[0] ?? 0,
        to = data.offsets[1] ?? from;
      const bytes = new Uint8Array(to - from);
      for (let at = from; at < to; at += ROW_IMAGE_FRAGMENT_BYTES) {
        const pause = pauseIfDue(work);
        if (pause) await pause;
        bytes.set(data.bytes.subarray(at, Math.min(to, at + ROW_IMAGE_FRAGMENT_BYTES)), at - from);
      }
      return { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) };
    }
  }
}
