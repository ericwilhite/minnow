/** The result sink has no buffers before the plan's atomic whole-minimum admission. */
import { GrantClosedError, type Grant, type Loan } from "../memory/governor.js";

export class DeferredGrant implements Grant {
  readonly spillable = true;
  #target: Grant | null = null;
  #closed = false;
  #reclaim: ((bytes: number) => void | Promise<void>) | null = null;
  #onClose: (() => void) | null = null;
  constructor(readonly label: string) {}
  install(target: Grant, onClose: () => void): void {
    if (this.#closed) {
      target.close();
      onClose();
      return;
    }
    if (this.#target !== null) throw new TypeError("Result budget already installed");
    this.#target = target;
    this.#onClose = onClose;
    if (this.#reclaim) target.onReclaimRequest(this.#reclaim);
  }
  /** A phase may move idle credit only after all frames and dictionary registry bytes return.
   * The caller owns the detached native parts until transfer/reinstallation or cancellation. */
  detach(): Grant {
    const target = this.#target;
    if (this.#closed || target === null) throw new GrantClosedError(this.label);
    if (target.allocated !== 0 || target.loaned !== 0)
      throw new TypeError("Result frames must return before budget handoff");
    this.#target = null;
    this.#onClose = null;
    return target;
  }
  get held(): number {
    return this.#target?.held ?? 0;
  }
  get allocated(): number {
    return this.#target?.allocated ?? 0;
  }
  get loaned(): number {
    return this.#target?.loaned ?? 0;
  }
  allocate(bytes: number): boolean {
    return this.#target?.allocate(bytes) ?? false;
  }
  free(bytes: number): void {
    this.#target?.free(bytes);
  }
  tryGrow(bytes: number): boolean {
    return this.#target?.tryGrow(bytes) ?? false;
  }
  release(bytes: number): void {
    this.#target?.release(bytes);
  }
  borrow(bytes: number, signal?: AbortSignal): Promise<Loan> {
    return this.#target?.borrow(bytes, signal) ?? Promise.reject(new GrantClosedError(this.label));
  }
  onReclaimRequest(callback: (bytes: number) => void | Promise<void>): void {
    this.#reclaim = callback;
    this.#target?.onReclaimRequest(callback);
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#reclaim = null;
    this.#target?.close();
    this.#onClose?.();
    this.#target = null;
    this.#onClose = null;
  }
}
