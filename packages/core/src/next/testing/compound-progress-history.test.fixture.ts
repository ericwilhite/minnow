/** Pressure-test cancellation/observers outlive an abandoned generated history. */
import { afterEach, expect, vi } from "vitest";
import type { Execution, PreparedPlan } from "../exec/executor.js";
import { QueryRuntime } from "../exec/run.js";
import * as keys from "../exec/join-keys.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";

// Capture once, before any seed installs observers. Never chain a previous history's spy.
// eslint-disable-next-line @typescript-eslint/unbound-method -- used with its original receiver.
export const reserveWorkingSet = QueryRuntime.prototype.reserve;
export const extractKeys = keys.extractKeysJob;
// eslint-disable-next-line @typescript-eslint/unbound-method -- used with its original receiver.
export const admitGroup = Governor.prototype.admitGroup;
const observers = new Set<() => void>();
const campaigns = new Set<PressureCampaign>();

export class PressureCampaign {
  readonly #controller = new AbortController();
  readonly #histories = new Set<PressureHistory>();
  constructor() {
    campaigns.add(this);
  }
  guard(): void {
    this.#controller.signal.throwIfAborted();
  }
  async history(cap: number): Promise<PressureHistory> {
    this.guard();
    const history = new PressureHistory(cap, () => this.guard());
    this.#histories.add(history); // Own even the asynchronous open before awaiting it.
    await history.ready;
    history.guard();
    return history;
  }
  async close(): Promise<void> {
    this.#controller.abort(new Error("Compound progress test ended"));
    await Promise.all([...this.#histories].map((history) => history.close()));
    this.#histories.clear();
    campaigns.delete(this);
  }
}

export class PressureHistory {
  readonly core = new StorageCore();
  readonly governor: Governor;
  readonly ready = this.core.open();
  readonly #controller = new AbortController();
  readonly #executions = new Set<Execution>();
  readonly #observers = new Set<() => void>();
  readonly #releases = new Set<() => Promise<void>>();
  #closing: Promise<void> | undefined;
  constructor(
    cap: number,
    readonly campaignGuard: () => void,
  ) {
    this.governor = new Governor(cap);
  }
  get signal(): AbortSignal {
    return this.#controller.signal;
  }
  guard(): void {
    this.campaignGuard();
    this.#controller.signal.throwIfAborted();
  }
  observe(factory: () => { mockRestore(): void }): () => void {
    this.guard();
    const spy = factory();
    const close = () => {
      // afterEach retires this membership before a late body-finally can run.
      if (observers.delete(close)) spy.mockRestore();
      this.#observers.delete(close);
    };
    observers.add(close);
    this.#observers.add(close);
    return close;
  }
  wrap(prepared: PreparedPlan, governor = this.governor): PreparedPlan {
    return {
      ...prepared,
      execute: (request) => {
        this.guard();
        const execution = prepared.execute({
          ...request,
          governor,
          signal: this.#controller.signal,
        });
        this.#executions.add(execution);
        return execution;
      },
    };
  }
  ownCleanup(release: () => void | Promise<void>): () => Promise<void> {
    let closing: Promise<void> | undefined;
    const close = () => {
      if (closing !== undefined) return closing;
      this.#releases.delete(close);
      closing = Promise.resolve().then(release);
      return closing;
    };
    this.#releases.add(close);
    return close;
  }
  close(): Promise<void> {
    if (this.#closing !== undefined) return this.#closing;
    for (const close of this.#observers) close();
    const reason = new Error("Compound progress history ended");
    this.#controller.abort(reason);
    for (const execution of this.#executions) execution.cancel(reason);
    this.#closing = (async () => {
      await Promise.allSettled([
        this.ready,
        ...[...this.#executions].flatMap((execution) => [execution.executed, execution.delivered]),
      ]);
      for (const release of this.#releases) await release();
      await this.core.close();
    })();
    return this.#closing;
  }
}

export async function retirePressureHistories(): Promise<void> {
  // Restore synchronously, before awaiting any execution cleanup. Old closers are now inert.
  for (const close of observers) close();
  vi.restoreAllMocks();
  await Promise.all([...campaigns].map((campaign) => campaign.close()));
  // eslint-disable-next-line @typescript-eslint/unbound-method -- identity assertion.
  expect(QueryRuntime.prototype.reserve).toBe(reserveWorkingSet);
  expect(keys.extractKeysJob).toBe(extractKeys);
  // eslint-disable-next-line @typescript-eslint/unbound-method -- identity assertion.
  expect(Governor.prototype.admitGroup).toBe(admitGroup);
}
afterEach(retirePressureHistories);
