/** Confirms quiescence without charging its observation window to engine work. The gate
 * still uses total elapsed time; the split is explanatory and never weakens verification. */
export class QuietWindow {
  #previous: string | undefined;
  #quiet = 0;
  #candidate: number | undefined;
  observe(fingerprint: string | undefined, at: number): boolean {
    if (fingerprint === undefined) {
      this.#quiet = 0;
      this.#candidate = undefined;
      return false;
    }
    if (fingerprint === this.#previous) this.#quiet += 1;
    else {
      this.#quiet = 0;
      this.#candidate = at;
    }
    this.#candidate ??= at;
    this.#previous = fingerprint;
    return this.#quiet >= 10;
  }
  timings(started: number, completed: number) {
    if (this.#quiet < 10 || this.#candidate === undefined)
      throw new Error("Quiescence has not been confirmed");
    return {
      observedWorkMs: this.#candidate - started,
      confirmationMs: completed - this.#candidate,
      totalMs: completed - started,
    };
  }
}
