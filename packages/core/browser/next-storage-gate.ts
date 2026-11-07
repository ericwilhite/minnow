import { chaosPoster } from "./next-storage-post.js";
/** Test-only worker park: terminate at an observed request, after a fully posted prefix.
 * IDB parks at the microtask after transaction creation/body issue, before its end callback.
 * OPFS parks before its synchronous call: this is an edge, never an interrupted native call.
 * No timer length or message-delivery speed can move a parked worker past the chosen request.
 */
import type { ChaosMessage } from "./next-storage-chaos.js";

export const REQUEST_KINDS = ["publish", "log", "spill", "data", "other"] as const;
export type RequestKind = (typeof REQUEST_KINDS)[number];
export function requestKind(target: string): RequestKind {
  if (/roots|root-/u.test(target)) return "publish";
  if (target.includes("log")) return "log";
  if (target.includes("spill")) return "spill";
  if (target.includes("data")) return "data";
  return "other";
}

/** [state: idle/armed/parked, serial sequence (0:any), request kind (0:any), minimum windows]. */
export function armRequestGate(
  gate: Int32Array,
  sequence = 0,
  kind?: RequestKind,
  windows = 0,
): void {
  Atomics.store(gate, 1, sequence);
  Atomics.store(gate, 2, kind === undefined ? 0 : REQUEST_KINDS.indexOf(kind) + 1);
  Atomics.store(gate, 3, windows);
  Atomics.store(gate, 0, 1);
}

export class ChaosRequestGate {
  readonly #active = new Set<number>();
  readonly #requests = new Map<number, string>();
  constructor(
    readonly control: Int32Array,
    readonly asynchronous: boolean,
    readonly park: (control: Int32Array) => void = (control) => {
      // Only worker termination releases this gate. Spurious wakes must not resume storage.
      // WebKit may not interrupt an indefinite futex wait during worker termination.
      // Use100ms futex sleeps to revisit VM termination checks without resuming storage.
      while (Atomics.load(control, 0) === 2) Atomics.wait(control, 0, 2, 100);
    },
  ) {}

  /** Called only AFTER chaosPoster has both claimed and posted this numbered message. */
  posted(message: ChaosMessage): void {
    if (message.chaos === "began") this.#active.add(message.sequence);
    else if (message.chaos === "settled") this.#active.delete(message.sequence);
    else if (message.chaos === "request") {
      if (message.phase === "end") this.#requests.delete(message.id);
      else {
        this.#requests.set(message.id, message.target);
        if (this.asynchronous) queueMicrotask(() => this.#atRequest(message.id));
        else this.#atRequest(message.id);
      }
    }
  }

  /** A selected buffered call stays pending until its scheduled native batch can issue.
   * Batching stays enabled; no platform marker is manufactured. Other calls are unchanged.
   */
  async queuedWrite(): Promise<void> {
    const sequence = Atomics.load(this.control, 1);
    while (
      this.asynchronous &&
      sequence !== 0 &&
      this.#active.has(sequence) &&
      Atomics.load(this.control, 0) === 1
    ) {
      await new Promise<void>((resolve) => {
        setTimeout(resolve, 0);
      });
    }
  }

  #atRequest(id: number): void {
    if (Atomics.load(this.control, 0) !== 1) return;
    const target = this.#requests.get(id);
    if (target === undefined) return; // Never claim a transaction whose end was already posted.
    const sequence = Atomics.load(this.control, 1);
    if (sequence !== 0 && !this.#active.has(sequence)) return;
    if (this.#active.size < Atomics.load(this.control, 3)) return;
    const kind = Atomics.load(this.control, 2);
    if (kind !== 0 && REQUEST_KINDS[kind - 1] !== requestKind(target)) return;
    // No campaign post can be interrupted between its counter claim and its port post:
    // publishing PARKED is after both, and this worker executes no more JS until killed.
    this.#stop();
  }

  #stop(): void {
    Atomics.store(this.control, 0, 2);
    this.park(this.control);
  }
}

/** Keep the publication ordering in one tested function, shared by the real worker and tests. */
export function gatedChaosPoster(
  port: Pick<MessagePort, "postMessage">,
  counter: Int32Array | null,
  gate: ChaosRequestGate | null,
): (message: ChaosMessage) => void {
  const post = chaosPoster(port, counter);
  return (message) => {
    post(message);
    gate?.posted(message);
  };
}
