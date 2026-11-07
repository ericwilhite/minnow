/** Explicit relinquishment for a discard/stream export consumer. Credit is independent.
 * release() detaches eligible column buffers immediately: old views become length zero or
 * throw TypeError. No view may be used after release. The receiver must compact stream dictionaries
 * before exposing this handle, so their storage does not alias a released frame. Ordinary Result/cursor views do not use this handle.
 */
import type { FramePayload } from "../exec/result.js";
import { frameTransferList } from "../exec/frame-buffers.js";
export function recyclingFrame(
  payload: FramePayload,
  ids: readonly number[],
  post: (
    buffers: ReadonlyArray<{ id: number; buffer: ArrayBuffer }>,
    transfer: ArrayBuffer[],
  ) => void,
): { readonly payload: FramePayload; release(): void } {
  const buffers = frameTransferList(payload);
  if (ids.length !== buffers.length) throw new RangeError("Return capability count mismatch");

  let released = false;
  return {
    payload,
    release() {
      if (released) return;

      const returns = buffers.map((buffer, i) => ({ id: ids[i] ?? -1, buffer }));
      const transfer = returns.map((item) => item.buffer);
      post(returns, transfer);
      released = transfer.every((buffer) => buffer.byteLength === 0);
      if (transfer.some((buffer) => buffer.byteLength !== 0))
        throw new Error("Released export buffer was not transferred");
    },
  };
}
