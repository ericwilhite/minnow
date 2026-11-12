/** Test-owned cancellation/observers must outlive an abandoned async property body. */
import { afterEach, expect, vi } from "vitest";
import type { Execution, PreparedPlan } from "../exec/executor.js";
import { ResultBuffers } from "../exec/result-buffer.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";

// Capture once, before any seed installs observers. Never chain a previous history's spy.
// eslint-disable-next-line @typescript-eslint/unbound-method -- used with its original receiver.
export const allocateResultBytes = ResultBuffers.prototype.bytes;
// eslint-disable-next-line @typescript-eslint/unbound-method -- used with its original receiver.
export const admitGroup = Governor.prototype.admitGroup;
const observers = new Set<() => void>();
const campaigns = new Set<JsonCampaign>();

export class JsonCampaign {
  readonly #controller = new AbortController();
  readonly #histories = new Set<JsonHistory>();
  constructor() {
    campaigns.add(this);
  }
  guard(): void {
    this.#controller.signal.throwIfAborted();
  }
  async history(cap: number): Promise<JsonHistory> {
    this.guard();
    const history = new JsonHistory(cap, () => this.guard());
    this.#histories.add(history); // Own even the asynchronous open before awaiting it.
    await history.ready;
    history.guard();
    return history;
  }
  async close(): Promise<void> {
    this.#controller.abort(new Error("JSON construction test ended"));
    await Promise.all([...this.#histories].map((history) => history.close()));
    this.#histories.clear();
    campaigns.delete(this);
  }
}

export class JsonHistory {
  readonly core = new StorageCore();
  readonly governor: Governor;
  readonly ready = this.core.open();
  readonly #controller = new AbortController();
  readonly #executions = new Set<Execution>();
  readonly #observers = new Set<() => void>();
  readonly #releases = new Set<() => void>();
  #closing: Promise<void> | undefined;
  constructor(
    cap: number,
    readonly campaignGuard: () => void,
  ) {
    this.governor = new Governor(cap);
  }
  guard(): void {
    this.campaignGuard();
    this.#controller.signal.throwIfAborted();
  }
  observe(spy: { mockRestore(): void }): () => void {
    this.guard();
    const close = () => {
      // afterEach retires this membership before a late body-finally can run.
      if (observers.delete(close)) spy.mockRestore();
      this.#observers.delete(close);
    };
    observers.add(close);
    this.#observers.add(close);
    return close;
  }
  wrap(prepared: PreparedPlan): PreparedPlan {
    return {
      ...prepared,
      execute: (request) => {
        this.guard();
        const execution = prepared.execute({
          ...request,
          governor: this.governor,
          signal: this.#controller.signal,
        });
        this.#executions.add(execution);
        return execution;
      },
    };
  }
  ownRelease(release: () => void): () => void {
    const close = () => {
      if (this.#releases.delete(close)) release();
    };
    this.#releases.add(close);
    return close;
  }
  close(): Promise<void> {
    if (this.#closing !== undefined) return this.#closing;
    for (const close of this.#observers) close();
    const reason = new Error("JSON construction history ended");
    this.#controller.abort(reason);
    for (const execution of this.#executions) execution.cancel(reason);
    this.#closing = (async () => {
      await Promise.allSettled([
        this.ready,
        ...[...this.#executions].flatMap((execution) => [execution.executed, execution.delivered]),
      ]);
      for (const release of this.#releases) release();
      await this.core.close();
    })();
    return this.#closing;
  }
}

export async function retireJsonHistories(): Promise<void> {
  // Restore synchronously, before awaiting any execution cleanup. Old closers are now inert.
  for (const close of observers) close();
  vi.restoreAllMocks();
  await Promise.all([...campaigns].map((campaign) => campaign.close()));
  // eslint-disable-next-line @typescript-eslint/unbound-method -- identity assertion.
  expect(ResultBuffers.prototype.bytes).toBe(allocateResultBytes);
  // eslint-disable-next-line @typescript-eslint/unbound-method -- identity assertion.
  expect(Governor.prototype.admitGroup).toBe(admitGroup);
}
afterEach(retireJsonHistories);
