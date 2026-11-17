/**
 * A test owns its prototype hooks until its asynchronous work, including cleanup, settles.
 * A runner timeout stops awaiting the test; it does not stop that work. Pass the runner's
 * TestContext.signal to the work and drain this scope before restoring shared hooks.
 */
export class PrototypeRuns {
  readonly #pending = new Set<Promise<unknown>>();

  run<T>(work: () => Promise<T>): Promise<T> {
    const pending = Promise.resolve().then(work);
    this.#pending.add(pending);
    void pending.then(
      () => {
        this.#pending.delete(pending);
      },
      () => {
        this.#pending.delete(pending);
      },
    );
    return pending;
  }

  /** Failed/cancelled runs must finish cleanup before another test installs its hooks. */
  async settled(): Promise<void> {
    while (this.#pending.size > 0) await Promise.allSettled([...this.#pending]);
  }
}
