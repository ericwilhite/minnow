export interface QueryMemoryUsage {
  readonly budgetBytes: number;
  readonly usedBytes: number;
  readonly peakBytes: number;
}

/** Finite default for every public engine execution path. */
export const DEFAULT_QUERY_MEMORY_BUDGET_BYTES = 64 * 1024 * 1024;

export class QueryMemoryBudgetError extends Error {
  override readonly name = "QueryMemoryBudgetError";

  constructor(
    readonly label: string,
    readonly requestedBytes: number,
    readonly usedBytes: number,
    readonly budgetBytes: number,
  ) {
    super(
      `${label} requested ${String(requestedBytes)} accounted bytes with ${String(usedBytes)} of ${String(budgetBytes)} bytes already reserved`,
    );
  }
}

interface QueryMemoryState {
  readonly budgetBytes: number;
  usedBytes: number;
  peakBytes: number;
}

export class QueryMemoryContext {
  readonly #state: QueryMemoryState;
  readonly #parent: QueryMemoryContext | undefined;
  readonly #children = new Set<QueryMemoryContext>();
  readonly #reservations = new Set<QueryMemoryReservation>();
  #talliedBytes = 0;
  #closed = false;

  constructor(
    budgetBytes: number = DEFAULT_QUERY_MEMORY_BUDGET_BYTES,
    parent?: QueryMemoryContext,
  ) {
    if (!Number.isSafeInteger(budgetBytes) || budgetBytes < 0) {
      throw new RangeError("Query memory budget must be a non-negative safe integer");
    }
    this.#parent = parent;
    if (parent === undefined) this.#state = { budgetBytes, usedBytes: 0, peakBytes: 0 };
    else {
      this.#state = parent.#state;
      parent.#children.add(this);
    }
  }

  get usage(): QueryMemoryUsage {
    return { ...this.#state };
  }

  createChild(): QueryMemoryContext {
    this.#assertOpen();
    return new QueryMemoryContext(this.#state.budgetBytes, this);
  }

  reserve(bytes: number, label: string): QueryMemoryReservation {
    this.#assertOpen();
    validateMemoryBytes(bytes, "Query memory reservation");
    const nextUsed = this.#state.usedBytes + bytes;
    if (!Number.isSafeInteger(nextUsed) || nextUsed > this.#state.budgetBytes) {
      throw new QueryMemoryBudgetError(
        label,
        bytes,
        this.#state.usedBytes,
        this.#state.budgetBytes,
      );
    }
    this.#state.usedBytes = nextUsed;
    this.#state.peakBytes = Math.max(this.#state.peakBytes, nextUsed);
    const reservation = new QueryMemoryReservation(this, bytes);
    this.#reservations.add(reservation);
    return reservation;
  }

  /**
   * Reserves bytes that live until this context closes, without a per-call reservation object.
   * The hot accumulation paths (result rows, spill pages) reserve once per row; tracking each
   * row as a retained `QueryMemoryReservation` in a Set costs O(result rows) live objects on
   * top of the rows themselves. Tallied bytes share the same budget and error semantics as
   * `reserve` and are released together when the context closes.
   */
  tally(bytes: number, label: string): void {
    this.#assertOpen();
    validateMemoryBytes(bytes, "Query memory reservation");
    const nextUsed = this.#state.usedBytes + bytes;
    if (!Number.isSafeInteger(nextUsed) || nextUsed > this.#state.budgetBytes) {
      throw new QueryMemoryBudgetError(
        label,
        bytes,
        this.#state.usedBytes,
        this.#state.budgetBytes,
      );
    }
    this.#state.usedBytes = nextUsed;
    this.#state.peakBytes = Math.max(this.#state.peakBytes, nextUsed);
    this.#talliedBytes += bytes;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const child of [...this.#children]) child.close();
    for (const reservation of [...this.#reservations]) reservation.release();
    this.#state.usedBytes -= this.#talliedBytes;
    this.#talliedBytes = 0;
    if (this.#state.usedBytes < 0) throw new Error("Query memory accounting underflow");
    if (this.#parent !== undefined) this.#parent.#children.delete(this);
  }

  release(reservation: QueryMemoryReservation, bytes: number): void {
    if (!this.#reservations.delete(reservation)) return;
    this.#state.usedBytes -= bytes;
    if (this.#state.usedBytes < 0) throw new Error("Query memory accounting underflow");
  }

  #assertOpen(): void {
    if (this.#closed) throw new Error("Query memory context is closed");
  }
}

export class QueryMemoryReservation {
  readonly #context: QueryMemoryContext;
  readonly bytes: number;
  #released = false;

  constructor(context: QueryMemoryContext, bytes: number) {
    this.#context = context;
    this.bytes = bytes;
  }

  release(): void {
    if (this.#released) return;
    this.#released = true;
    this.#context.release(this, this.bytes);
  }
}

function validateMemoryBytes(bytes: number, label: string): void {
  if (!Number.isSafeInteger(bytes) || bytes < 0) {
    throw new RangeError(`${label} must be a non-negative safe integer`);
  }
}
