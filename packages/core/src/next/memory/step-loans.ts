/** Progress belongs to one admitted operator; it never queues behind another execution. */
import { GrantClosedError, MemoryAdmissionError, type Loan, type Grant } from "./governor.js";
import { MinimumBudget } from "./working-set.js";

type ByteBudget = Pick<Grant, "allocate" | "free">;

export class StepLoans {
  readonly budget: MinimumBudget;
  #active = false;
  #closed = false;
  constructor(
    owner: ByteBudget,
    readonly capacity: number,
    readonly label: string,
  ) {
    this.budget = new MinimumBudget(owner, capacity);
  }
  async borrow(bytes: number, signal?: AbortSignal): Promise<Loan> {
    signal?.throwIfAborted();
    if (this.#closed) throw new GrantClosedError(this.label);
    if (this.#active)
      throw new RangeError(`${this.label} must repay its step loan before borrowing`);
    if (bytes > this.capacity) throw new MemoryAdmissionError(this.label, bytes, this.capacity);
    this.#active = true;
    let repaid = false;
    return {
      bytes,
      repay: () => {
        if (repaid || this.#closed) return;
        if (this.budget.allocated !== 0)
          throw new RangeError(`${this.label} must free its step loan before repaying`);
        repaid = true;
        this.#active = false;
      },
    };
  }
  close(): void {
    if (this.#closed) return;
    if (this.budget.allocated > 0) this.budget.free(this.budget.allocated);
    this.#closed = true;
    this.#active = false;
  }
}
