/** Snapshot transitions only; B6 owns decoder/facade binding, refresh and subscription lifetime. */
import type { ByteBudget } from "../storage/extents.js";
import { MemoryAdmissionError } from "../memory-values.js";

export type NativeLiveSnapshot<R> =
  | { readonly status: "loading"; readonly rows: readonly R[] }
  | {
      readonly status: "ready";
      readonly rows: readonly R[];
      readonly version: number;
      readonly reset: boolean;
    }
  | {
      readonly status: "error";
      readonly rows: readonly R[];
      readonly version: number | null;
      readonly error: unknown;
    };
const STATE_BYTES = 512,
  LISTENER_BYTES = 256;
const LOADING = Object.freeze({ status: "loading" as const, rows: Object.freeze([]) });

/** Stable external-store snapshots with one retained state, never a snapshot/event queue. */
export class NativeLiveState<R> {
  readonly #listeners = new Set<() => void>();
  #snapshot: NativeLiveSnapshot<R> = LOADING;
  #version: number | null = null;
  #closed = false;
  static create<R>(
    budget: ByteBudget,
    onListenerError?: (error: unknown) => void,
  ): NativeLiveState<R> {
    if (!budget.allocate(STATE_BYTES))
      throw new MemoryAdmissionError("live snapshot", STATE_BYTES, 0);
    return new NativeLiveState<R>(budget, onListenerError);
  }
  private constructor(
    readonly budget: ByteBudget,
    readonly onListenerError?: (error: unknown) => void,
  ) {}
  readonly getSnapshot = (): NativeLiveSnapshot<R> => this.#snapshot;
  readonly subscribe = (listener: () => void): (() => void) => {
    if (this.#closed) throw new Error("Live state is closed");
    if (!this.budget.allocate(LISTENER_BYTES))
      throw new MemoryAdmissionError("live listener", LISTENER_BYTES, 0);
    const cell = () => listener();
    this.#listeners.add(cell);
    return () => {
      if (this.#listeners.delete(cell)) this.budget.free(LISTENER_BYTES);
    };
  };
  loading(): void {
    if (this.#closed || this.#snapshot.status === "loading") return;
    this.#publish(Object.freeze({ status: "loading", rows: this.#snapshot.rows }));
  }
  ready(rows: readonly R[], version: number, reset: boolean, resultChanged = false): void {
    if (this.#closed) return;
    if (
      this.#snapshot.status === "ready" &&
      this.#snapshot.rows === rows &&
      !reset &&
      !resultChanged
    )
      return;
    this.#version = version;
    this.#publish(Object.freeze({ status: "ready", rows, version, reset }));
  }
  /** A database incarnation change retires every row and the prior version. */
  reset(error: unknown): void {
    if (this.#closed) return;
    this.#version = null;
    this.#publish(Object.freeze({ status: "error", rows: LOADING.rows, version: null, error }));
  }
  error(error: unknown): void {
    if (this.#closed) return;
    if (this.#snapshot.status === "error" && Object.is(this.#snapshot.error, error)) return;
    this.#publish(
      Object.freeze({ status: "error", rows: this.#snapshot.rows, version: this.#version, error }),
    );
  }
  #publish(snapshot: NativeLiveSnapshot<R>): void {
    this.#snapshot = snapshot;
    // A callback may unsubscribe/re-register. Re-registering cannot extend this turn past its initial registration count.
    const count = this.#listeners.size;
    let visited = 0;
    for (const listener of this.#listeners) {
      if (visited++ === count) break;
      try {
        listener();
      } catch (error) {
        try {
          this.onListenerError?.(error);
        } catch {
          /* Consumer isolation. */
        }
      }
    }
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.budget.free(STATE_BYTES + LISTENER_BYTES * this.#listeners.size);
    this.#listeners.clear();
    this.#snapshot = LOADING;
  }
}
