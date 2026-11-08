/** FIFO workspace ownership with a cooperative flush signal for audit producers. A producer
 * must never hold the workspace while awaiting a caller once another workspace user queues.
 */
export const WORKSPACE_YIELD = Symbol("workspace yield");
export interface MetadataTurn {
  readonly contended: boolean;
  /** Only one outstanding producer wait; completed waits leave no listener on the turn. */
  race<T>(pending: Promise<T>): Promise<T | typeof WORKSPACE_YIELD>;
}
export class MetadataMutex {
  #tail: Promise<void> = Promise.resolve();
  #requestYield: (() => void) | null = null;
  #queued = 0;
  async run<T>(work: (turn: MetadataTurn) => Promise<T>): Promise<T> {
    this.#queued++;
    this.#requestYield?.();
    const previous = this.#tail;
    let done!: () => void;
    this.#tail = new Promise<void>((resolve) => {
      done = resolve;
    });
    await previous;
    this.#queued--;
    let contended = this.#queued > 0,
      interrupt: (() => void) | null = null;
    this.#requestYield = () => {
      contended = true;
      interrupt?.();
    };
    const turn: MetadataTurn = {
      get contended() {
        return contended;
      },
      race: <V>(pending: Promise<V>) =>
        new Promise<V | typeof WORKSPACE_YIELD>((resolve, reject) => {
          if (interrupt !== null) throw new TypeError("Two producer waits on one workspace turn");
          let ended = false;
          const stop = () => {
            if (ended) return;
            ended = true;
            interrupt = null;
            resolve(WORKSPACE_YIELD);
          };
          interrupt = stop;
          pending.then(
            (value) => {
              if (ended) return;
              ended = true;
              interrupt = null;
              resolve(value);
            },
            (error: unknown) => {
              if (ended) return;
              ended = true;
              interrupt = null;
              reject(error instanceof Error ? error : new Error(String(error)));
            },
          );
          if (contended) stop();
        }),
    };
    try {
      return await work(turn);
    } finally {
      this.#requestYield = null;
      done();
    }
  }
}
