/** A representation-changing edge borrows output; it cannot forward detachable ownership
 * of the source's old columns. Native consumers copy what they retain before readiness.
 */
import type { Batch } from "../vector/vector.js";
import type { Operator, Transfer } from "./pipeline.js";

export function starColumns(map: (batch: Batch) => Batch, downstream: Operator): Operator {
  let held: Transfer | undefined;
  const release = () => {
    held?.release();
    held = undefined;
  };
  return {
    push(batch, transfer) {
      try {
        const result = downstream.push(map(batch));
        if (result === "ready") transfer?.release();
        else held = transfer;
        return result;
      } catch (error) {
        transfer?.release();
        throw error;
      }
    },
    async ready() {
      try {
        await downstream.ready();
      } finally {
        release();
      }
    },
    finish: () => downstream.finish(),
    abort(reason) {
      try {
        downstream.abort(reason);
      } finally {
        release();
      }
    },
  };
}
