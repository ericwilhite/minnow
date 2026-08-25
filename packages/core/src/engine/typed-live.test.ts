import { describe, expect, it } from "vitest";
import type { LiveQueryInput, LiveQueryObserveOptions } from "./live.js";
import {
  LiveQuery,
  LiveQueryManager,
  type LiveQueryBackend,
  type LiveQuerySubscriptionLike,
} from "./typed-live.js";

async function until(assertion: () => void): Promise<void> {
  let failure: unknown;
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      assertion();
      return;
    } catch (error) {
      failure = error;
      await Promise.resolve();
    }
  }
  throw failure;
}

class TestBackend implements LiveQueryBackend {
  options: LiveQueryObserveOptions | undefined;
  opens = 0;
  closes = 0;

  async observe(
    _query: LiveQueryInput,
    options: LiveQueryObserveOptions,
  ): Promise<LiveQuerySubscriptionLike> {
    this.opens += 1;
    this.options = options;
    options.onInvalidate({ manifestVersion: 1, catalogEpoch: 1, initial: true });
    let closed = false;
    return {
      dependencyTableIds: ["table"],
      close: () => {
        if (closed) return;
        closed = true;
        this.closes += 1;
        options.onComplete?.();
      },
    };
  }

  invalidate(manifestVersion: number): void {
    this.options?.onInvalidate({
      manifestVersion,
      catalogEpoch: manifestVersion,
      initial: false,
    });
  }

  refresh(): Promise<void> {
    return Promise.resolve();
  }

  close(): void {
    this.options?.onComplete?.();
  }
}

class RefreshingBackend extends TestBackend {
  override refresh(): Promise<void> {
    this.invalidate(2);
    return Promise.resolve();
  }
}

class FlakyObserveBackend extends TestBackend {
  attempts = 0;

  override observe(
    query: LiveQueryInput,
    options: LiveQueryObserveOptions,
  ): Promise<LiveQuerySubscriptionLike> {
    this.attempts += 1;
    if (this.attempts === 1) return Promise.reject(new Error("observer setup failed"));
    return super.observe(query, options);
  }
}

describe("typed live query", () => {
  it("is a stable external store and suppresses exactly equal snapshots", async () => {
    const backend = new TestBackend();
    let rows = [{ id: 1, label: "one" }];
    let executions = 0;
    const live = new LiveQuery(backend, {
      query: "SELECT id, label FROM rows",
      execute: () => {
        executions += 1;
        return Promise.resolve(rows);
      },
    });
    let notifications = 0;
    const unsubscribe = live.subscribe(() => {
      notifications += 1;
    });
    await until(() => expect(live.getSnapshot().status).toBe("ready"));
    const initial = live.getSnapshot();
    expect(live.getSnapshot()).toBe(initial);
    expect(initial.rows).toEqual(rows);

    // A newer version with equal rows advances metadata but keeps the row-array identity.
    backend.invalidate(2);
    await until(() => expect(live.getSnapshot()).toMatchObject({ status: "ready", version: 2 }));
    expect(live.getSnapshot().rows).toBe(initial.rows);
    const afterVersion = live.getSnapshot();
    backend.invalidate(2);
    await until(() => expect(executions).toBe(3));
    expect(live.getSnapshot()).toBe(afterVersion);

    rows = [{ id: 1, label: "changed" }];
    backend.invalidate(3);
    await until(() => expect(live.getSnapshot().rows).toEqual(rows));
    expect(notifications).toBe(3);
    unsubscribe();
    await until(() => expect(backend.closes).toBe(1));
  });

  it("coalesces invalidations that arrive during a slow execution", async () => {
    const backend = new TestBackend();
    let release!: () => void;
    const first = new Promise<void>((resolve) => {
      release = resolve;
    });
    let executions = 0;
    const live = new LiveQuery(backend, {
      query: "SELECT id FROM rows",
      execute: async () => {
        executions += 1;
        if (executions === 1) await first;
        return [{ id: executions }];
      },
    });
    let notifications = 0;
    const unsubscribe = live.subscribe(() => {
      notifications += 1;
    });
    await until(() => expect(executions).toBe(1));
    backend.invalidate(2);
    backend.invalidate(3);
    backend.invalidate(4);
    release();
    await until(() => expect(live.getSnapshot()).toMatchObject({ status: "ready", version: 4 }));
    expect(executions).toBe(2);
    expect(notifications).toBe(1);
    unsubscribe();
  });

  it("retains good rows on error and refresh retries without another invalidation", async () => {
    const backend = new TestBackend();
    let fail = false;
    const live = new LiveQuery(backend, {
      query: "SELECT id FROM rows",
      execute: () => {
        if (fail) return Promise.reject(new Error("temporary"));
        return Promise.resolve([{ id: 1 }]);
      },
    });
    const unsubscribe = live.subscribe(() => undefined);
    await until(() => expect(live.getSnapshot().status).toBe("ready"));
    const goodRows = live.getSnapshot().rows;
    fail = true;
    backend.invalidate(2);
    await until(() => expect(live.getSnapshot().status).toBe("error"));
    expect(live.getSnapshot().rows).toBe(goodRows);
    fail = false;
    await live.refresh();
    expect(live.getSnapshot()).toMatchObject({ status: "ready", rows: [{ id: 1 }] });
    unsubscribe();
  });

  it("does not execute twice when backend refresh already invalidates observers", async () => {
    const backend = new RefreshingBackend();
    let executions = 0;
    const live = new LiveQuery(backend, {
      query: "SELECT id FROM rows",
      execute: () => Promise.resolve([{ id: ++executions }]),
    });
    const unsubscribe = live.subscribe(() => undefined);
    await until(() => expect(executions).toBe(1));
    await live.refresh();
    expect(executions).toBe(2);
    unsubscribe();
  });

  it("retries an adapter error through the shared manager without a new commit", async () => {
    const backend = new TestBackend();
    const manager = new LiveQueryManager({ liveQueries: () => backend });
    let failing = true;
    let executions = 0;
    const live = manager.watch({
      query: "SELECT id FROM rows",
      execute: () => {
        executions += 1;
        return failing ? Promise.reject(new Error("adapter failed")) : Promise.resolve([{ id: 1 }]);
      },
    });
    const unsubscribe = live.subscribe(() => undefined);
    await until(() => expect(live.getSnapshot().status).toBe("error"));
    failing = false;
    await manager.refresh();
    expect(live.getSnapshot()).toMatchObject({ status: "ready", rows: [{ id: 1 }] });
    expect(executions).toBe(2);
    unsubscribe();
    await manager.close();
  });

  it("reopens observation after transient setup failure", async () => {
    const backend = new FlakyObserveBackend();
    let executions = 0;
    const live = new LiveQuery(backend, {
      query: "SELECT id FROM rows",
      execute: () => Promise.resolve([{ id: ++executions }]),
    });
    const unsubscribe = live.subscribe(() => undefined);
    await until(() => expect(live.getSnapshot().status).toBe("error"));
    await live.refresh();
    expect(live.getSnapshot()).toMatchObject({ status: "ready", version: 1 });
    expect(backend.attempts).toBe(2);

    backend.invalidate(3);
    await until(() => expect(live.getSnapshot()).toMatchObject({ status: "ready", version: 3 }));
    expect(executions).toBe(2);
    unsubscribe();
  });

  it("wakes a parked async iterator when closed", async () => {
    const backend = new TestBackend();
    const live = new LiveQuery(backend, {
      query: "SELECT id FROM rows",
      execute: () => Promise.resolve([{ id: 1 }]),
    });
    const iterator = live[Symbol.asyncIterator]();
    const first = await iterator.next();
    if (first.done) throw new Error("Iterator ended before its first snapshot");
    expect(first.value.status).toBe("ready");
    const parked = iterator.next();
    live.close();
    await expect(parked).resolves.toEqual({ done: true, value: undefined });
  });

  it("keeps observation alive for duplicate callback subscriptions until both clean up", async () => {
    const backend = new TestBackend();
    const live = new LiveQuery(backend, {
      query: "SELECT id FROM rows",
      execute: () => Promise.resolve([{ id: 1 }]),
    });
    const listener = (): void => undefined;
    const first = live.subscribe(listener);
    const second = live.subscribe(listener);
    await until(() => expect(live.getSnapshot().status).toBe("ready"));
    expect(backend.opens).toBe(1);
    first();
    expect(backend.closes).toBe(0);
    second();
    await until(() => expect(backend.closes).toBe(1));
  });
});
