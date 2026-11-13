/** Group boundaries decode only keys that need semantic trees; byte arrays use views. */
import { byteArrayGroupKey, equalByteArrayKeys } from "./json-array-key.js";
import type { Grant } from "../../memory/governor.js";
import type { PhysicalType } from "../../types.js";
import type { Vector } from "../../vector/vector.js";
import type { Pool } from "../spill-pool.js";
import { SemanticStep, semanticStepBytes } from "../semantic-step.js";
import type { SortColumn } from "../sort-keys.js";
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
    readonly step = new SemanticStep(pool, grant, signal),
  ) {
    this.#byteArrays = keys.filter((k) => byteArrayGroupKey(types[k.column]));
    this.decodedKeys = keys.filter((k) => !byteArrayGroupKey(types[k.column]));
  }
  operandBytes(columns: readonly Vector[], row: number): number {
    return semanticStepBytes(this.types, this.decodedKeys, columns, row, row + 1) / 2;
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
    const bytes = this.operandBytes(left, i) + this.operandBytes(right, j);
    await this.step.prepare(this, bytes);
    try {
      return this.step.compare(this, this.types, this.decodedKeys, left, i, right, j) === 0;
    } finally {
      this.step.release(this);
    }
  }
}
