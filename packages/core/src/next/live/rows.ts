import { dateMilliseconds } from "../../date-value.js";
/** Decoder-neutral result reconciliation. B6 binds this to its external store and lossless
 * result decoder. Explicit resets never invent identity continuity across a missing history.
 */
import type { ByteBudget } from "../storage/extents.js";
import { MemoryAdmissionError, bigintBytes, stringBytes } from "../memory/governor.js";

export type LiveKey = string | number | boolean | bigint | Date;
export type LiveKeyOf<R extends object> = {
  [K in keyof R]-?: R[K] extends LiveKey ? K : never;
}[keyof R];
export interface KeyChange {
  readonly kind?: "update" | "delete";
  readonly fromRowId?: number;
  readonly toRowId?: number;
  readonly table: string;
  readonly from: LiveKey | readonly LiveKey[];
  readonly to: LiveKey | readonly LiveKey[];
}
export type RowChange<R> =
  | { readonly type: "insert"; readonly row: R; readonly index: number }
  | { readonly type: "delete"; readonly key: LiveKey; readonly previous: R; readonly index: number }
  | { readonly type: "update"; readonly row: R; readonly previous: R; readonly index: number }
  | {
      readonly type: "move";
      readonly key: LiveKey;
      readonly row: R;
      readonly from: number;
      readonly to: number;
    }
  | {
      readonly type: "rekey";
      readonly from: LiveKey;
      readonly key: LiveKey;
      readonly row: R;
      readonly previous: R;
      readonly previousIndex: number;
      readonly index: number;
    };
export type RowsPatch<R> =
  | { readonly type: "reset"; readonly rows: readonly R[] }
  | {
      readonly type: "patch";
      readonly retained: Int32Array;
      readonly changedRows: ReadonlyArray<{ readonly index: number; readonly row: R }>;
    };
export interface RowsSnapshot<R> {
  readonly rows: readonly R[];
  readonly changes: ReadonlyArray<RowChange<R>>;
  readonly patch: RowsPatch<R>;
  readonly reset: boolean;
  readonly version: number;
  readonly keyChanges: readonly KeyChange[];
}

function token(value: unknown): string {
  if (typeof value === "number") return Object.is(value, -0) ? "n:-0" : `n:${String(value)}`;
  if (typeof value === "string") return `s:${value}`;
  if (typeof value === "boolean") return value ? "b:1" : "b:0";
  if (typeof value === "bigint") return `i:${value.toString(16)}`;
  if (value instanceof Date) {
    const instant = dateMilliseconds(value);
    if (Number.isFinite(instant)) return `d:${String(instant)}`;
  }
  throw new TypeError("Live keys must be unique non-null scalar values or valid Dates");
}
export function sameLiveValue(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (left instanceof Date || right instanceof Date)
    return (
      left instanceof Date &&
      right instanceof Date &&
      Object.is(dateMilliseconds(left), dateMilliseconds(right))
    );
  if (left === null || right === null || typeof left !== "object" || typeof right !== "object")
    return false;
  if (ArrayBuffer.isView(left) && ArrayBuffer.isView(right)) {
    const a = new Uint8Array(left.buffer, left.byteOffset, left.byteLength);
    const b = new Uint8Array(right.buffer, right.byteOffset, right.byteLength);
    return a.length === b.length && a.every((value, i) => value === b[i]);
  }
  const a = Object.keys(left),
    b = Object.keys(right);
  return (
    a.length === b.length &&
    a.every(
      (key, i) => key === b[i] && sameLiveValue(Reflect.get(left, key), Reflect.get(right, key)),
    )
  );
}
function bytesOf(value: unknown): number {
  if (typeof value === "string") return stringBytes(value);
  if (typeof value === "bigint") return bigintBytes(value);
  if (value === null || typeof value !== "object") return 16;
  if (value instanceof ArrayBuffer) return 128 + value.byteLength;
  // structuredClone retains the whole backing store, even for a one-byte view.
  if (ArrayBuffer.isView(value)) return 128 + value.buffer.byteLength;
  let bytes = 128;
  for (const key of Object.keys(value))
    bytes += stringBytes(key) + bytesOf(Reflect.get(value, key));
  return bytes;
}

/** A charged retained key index and snapshot; no delivery queue. On refusal, the old baseline
 * remains intact. The caller owns any snapshots it retains after replacement or close(). */
export class LiveRows<R extends object> {
  #snapshot: RowsSnapshot<R> | null = null;
  #index = new Map<string, { row: R; index: number }>();
  #bytes = 0;
  #closed = false;
  constructor(
    readonly budget: ByteBudget,
    readonly key: LiveKeyOf<R>,
    readonly options: { readonly table?: string; readonly maxRows?: number } = {},
  ) {
    if (
      options.maxRows !== undefined &&
      (!Number.isSafeInteger(options.maxRows) || options.maxRows < 1)
    )
      throw new RangeError("A live window needs a positive integer limit");
    if (!budget.allocate(512)) throw new MemoryAdmissionError("live row metadata", 512, 0);
    this.#bytes = 512;
  }
  getSnapshot(): RowsSnapshot<R> | null {
    return this.#snapshot;
  }
  apply(
    input: readonly R[],
    version: number,
    delivery: { readonly reset?: boolean; readonly keyChanges?: readonly KeyChange[] } = {},
  ): RowsSnapshot<R> {
    if (this.#closed) throw new Error("Live rows are closed");
    if (this.options.maxRows !== undefined && input.length > this.options.maxRows)
      throw new RangeError("Live query exceeded its window limit");
    const keyChanges = delivery.keyChanges ?? [];
    const bytes =
      2048 +
      6 * bytesOf(input) +
      // Diffs retain deleted/updated previous rows after the old baseline is released.
      6 * bytesOf(this.#snapshot?.rows ?? []) +
      6 * bytesOf(keyChanges) +
      input.length * 1024 +
      this.#index.size * 256;
    if (!this.budget.allocate(bytes))
      throw new MemoryAdmissionError("live row reconciliation", bytes, 0);
    try {
      const reset = this.#snapshot === null || delivery.reset === true;
      const index = new Map<string, { row: R; index: number }>();
      const rows: R[] = [];
      const changes: Array<RowChange<R>> = [];
      const retained = new Int32Array(input.length).fill(-1);
      const changedRows: Array<{ index: number; row: R }> = [];
      const movedKeys = new Map<string, string>();
      if (!reset && this.options.table !== undefined) {
        for (const change of keyChanges)
          if (
            change.table === this.options.table &&
            !Array.isArray(change.from) &&
            !Array.isArray(change.to)
          ) {
            const from = token(change.from),
              to = token(change.to);
            // Preserve the origin across a bounded chain (A->B->C), never guess by values.
            let origin = from;
            for (const [old, current] of movedKeys)
              if (current === from) {
                origin = old;
                movedKeys.delete(old);
                break;
              }
            movedKeys.set(origin, to);
          }
      }
      // Native row-ID links distinguish a continuing UPDATE chain from a deleted key that
      // another row later reused. Value-only notices remain supported for adapter-owned sources.
      if (
        !reset &&
        this.options.table !== undefined &&
        keyChanges.some((change) => change.fromRowId !== undefined)
      ) {
        movedKeys.clear();
        const identities = new Map<number, string | null>();
        const owned = new Set<string>();
        for (const change of keyChanges) {
          if (
            change.table !== this.options.table ||
            change.fromRowId === undefined ||
            change.toRowId === undefined ||
            Array.isArray(change.from) ||
            Array.isArray(change.to)
          )
            continue;
          const from = token(change.from);
          let origin = identities.get(change.fromRowId);
          if (origin === undefined) {
            origin = this.#index.has(from) && !owned.has(from) ? from : null;
            if (origin !== null) owned.add(origin);
          }
          identities.delete(change.fromRowId);
          if (change.kind === "delete") {
            if (origin !== null) movedKeys.delete(origin);
          } else {
            identities.set(change.toRowId, origin);
            if (origin !== null) movedKeys.set(origin, token(change.to));
          }
        }
      }
      const origins = new Map<string, string>();
      for (const [from, to] of movedKeys) origins.set(to, from);
      for (let at = 0; at < input.length; at++) {
        const value = input[at];
        if (value === undefined) throw new TypeError("Sparse live result");
        const id = token(value[this.key]);
        if (index.has(id)) throw new TypeError("Duplicate live result key");
        const origin = origins.get(id);
        const old = reset ? undefined : this.#index.get(origin ?? id);
        const same = old !== undefined && sameLiveValue(old.row, value);
        const row = same ? old.row : Object.freeze(structuredClone(value));
        rows.push(row);
        index.set(id, { row, index: at });
        if (same) retained[at] = old.index;
        else changedRows.push({ index: at, row });
        if (reset) continue;
        if (old === undefined) changes.push({ type: "insert", row, index: at });
        else if (origin !== undefined && origin !== id)
          changes.push({
            type: "rekey",
            from: old.row[this.key] as LiveKey,
            key: row[this.key] as LiveKey,
            row,
            previous: old.row,
            previousIndex: old.index,
            index: at,
          });
        else {
          if (!same) changes.push({ type: "update", row, previous: old.row, index: at });
          if (old.index !== at)
            changes.push({
              type: "move",
              key: row[this.key] as LiveKey,
              row,
              from: old.index,
              to: at,
            });
        }
      }
      const deletions: Array<RowChange<R>> = [];
      if (!reset)
        for (const [id, old] of this.#index) {
          if (!index.has(id) && !index.has(movedKeys.get(id) ?? id))
            deletions.push({
              type: "delete",
              key: old.row[this.key] as LiveKey,
              previous: old.row,
              index: old.index,
            });
        }
      const allChanges = deletions.concat(changes);
      const visibleKeyChanges = keyChanges.filter(
        (change) => change.kind !== "delete" && !sameLiveValue(change.from, change.to),
      );
      if (
        !reset &&
        allChanges.length === 0 &&
        visibleKeyChanges.length === 0 &&
        this.#snapshot !== null
      ) {
        const stable = this.#snapshot;
        this.budget.free(bytes);
        return stable;
      }
      const frozen =
        !reset &&
        this.#snapshot !== null &&
        rows.length === this.#snapshot.rows.length &&
        rows.every((row, i) => row === this.#snapshot?.rows[i])
          ? this.#snapshot.rows
          : Object.freeze(rows);
      const snapshot: RowsSnapshot<R> = Object.freeze({
        rows: frozen,
        changes: Object.freeze(allChanges),
        reset,
        version,
        keyChanges: reset ? Object.freeze([]) : Object.freeze(visibleKeyChanges),
        patch: reset
          ? { type: "reset" as const, rows: frozen }
          : { type: "patch" as const, retained, changedRows: Object.freeze(changedRows) },
      });
      this.#snapshot = snapshot;
      this.#index = index;
      const previousBytes = this.#bytes;
      this.#bytes = bytes;
      this.budget.free(previousBytes);
      return snapshot;
    } catch (error) {
      this.budget.free(bytes);
      throw error;
    }
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#snapshot = null;
    this.#index.clear();
    this.budget.free(this.#bytes);
    this.#bytes = 0;
  }
}
