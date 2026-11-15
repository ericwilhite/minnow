/** Composition-owned semantic adapter; the write layer never imports executor kernels. */
import type { FtsSemantics } from "../fts-config.js";
import type { Vector } from "../vector/vector.js";
export interface FtsWriteRenderer extends FtsSemantics {
  /** Complete bounded renderer + tokenizer scratch, including geometric overlap. */
  readonly workspaceBytes: number;
  render(vector: Vector, row: number): string | undefined;
}
