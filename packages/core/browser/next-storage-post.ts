import type { ChaosMessage } from "./next-storage-chaos.js";

/** Numbered FIFO campaign posts, with a shared prefix for the kill observation barrier. */
export function chaosPoster(
  port: Pick<MessagePort, "postMessage">,
  counter: Int32Array | null,
): (message: ChaosMessage) => void {
  let claimed = 0;
  return (message) => {
    claimed += 1;
    // Claim before handing off: an interruption after the post cannot hide this message.
    if (counter !== null) Atomics.store(counter, 0, claimed);
    port.postMessage({ ...message, n: claimed });
  };
}
