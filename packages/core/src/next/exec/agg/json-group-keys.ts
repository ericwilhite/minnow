/** Group boundaries decode only keys that need semantic trees; byte arrays use views. */
import { byteArrayGroupKey, equalByteArrayKeys } from "./json-array-key.js";
import type { Grant } from "../../memory/governor.js";
import type { PhysicalType } from "../../types.js";
import type { Vector } from "../../vector/vector.js";
import type { Pool } from "../spill-pool.js";
import {
  compareRows,
  decodeOperands,
  dropOperands,
  emptyOperands,
  operandBytes,
  type SortColumn,
} from "../sort-keys.js";
/** Pair of semantic trees plus encoded read/key/write workspace, relative to four row copies. */
export function jsonGroupWorkspaceMultiplier(
  types: readonly PhysicalType[],
  groupBy: readonly number[],
): number {
  return Math.max(
    1,
    ...groupBy.map((i) => (types[i]?.kind === "json" ? 13 : types[i]?.kind === "array" ? 17 : 1)),
  );
}
export class JsonGroupKeys {
  readonly decodedKeys: readonly SortColumn[];
  readonly #byteArrays: readonly SortColumn[];
  constructor(
    readonly types: readonly PhysicalType[],
    readonly keys: readonly SortColumn[],
    readonly pool: Pool,
    readonly grant: Grant,
    readonly signal: AbortSignal,
    readonly pace: () => Promise<void>,
  ) {
    this.#byteArrays = keys.filter((k) => byteArrayGroupKey(types[k.column]));
    this.decodedKeys = keys.filter((k) => !byteArrayGroupKey(types[k.column]));
  }
  operandBytes(columns: readonly Vector[], row: number): number {
    return operandBytes(this.types, this.decodedKeys, columns, row, row + 1);
  }
  async equal(
    left: readonly Vector[],
    i: number,
    right: readonly Vector[],
    j: number,
  ): Promise<boolean> {
    await this.pace();
    for (const key of this.#byteArrays) {
      const a = left[key.column],
        b = right[key.column];
      if (!a || !b) throw new Error("Missing array grouping vector");
      if (!(await equalByteArrayKeys(a, i, b, j, this.pace))) return false;
    }
    const a = emptyOperands(this.types, this.decodedKeys),
      b = emptyOperands(this.types, this.decodedKeys);
    const bytes = this.operandBytes(left, i) + this.operandBytes(right, j);
    // Preflight both live decoded rows as one step, before building either tree.
    await this.pool.need(bytes, this.grant, this.signal);
    const release = this.pool.pinStep();
    try {
      if (
        decodeOperands(this.types, this.decodedKeys, left, i, i + 1, this.pool, a) !== 0 ||
        decodeOperands(this.types, this.decodedKeys, right, j, j + 1, this.pool, b) !== 0
      )
        throw new Error("JSON grouping operands were not admitted");
      this.signal.throwIfAborted();
      return compareRows(this.types, this.decodedKeys, left, i, right, j, a, b, 0, 0) === 0;
    } finally {
      dropOperands(a, this.pool);
      dropOperands(b, this.pool);
      release();
    }
  }
}
