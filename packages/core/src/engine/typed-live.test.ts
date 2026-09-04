import { describe, expect, it } from "vitest";
import type { LiveQueryInput, LiveQueryObserveOptions, LiveQuerySubscribeOptions } from "./live.js";
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
  subscribeOptions: LiveQuerySubscribeOptions | undefined;
  opens = 0;
  subscribed = 0;
  closes = 0;

  async subscribe(
    _query: LiveQueryInput,
    options: LiveQuerySubscribeOptions,
  ): Promise<LiveQuerySubscriptionLike> {
    this.subscribed += 1;
    this.subscribeOptions = options;
    options.onChange(
      { columns: ["id"], columnDomains: [null], rows: [{ id: 1 }] },
      { manifestVersion: 1, catalogEpoch: 1, initial: true },
    );
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

  deliver(rows: Array<{ id: number }>, manifestVersion: number): void {
    this.subscribeOptions?.onChange(
      { columns: ["id"], columnDomains: [null], rows },
      { manifestVersion, catalogEpoch: manifestVersion, initial: false },
    );
  }

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

  it("asks the backend to compare before invalidating", async () => {
    const backend = new TestBackend();
    const query = new LiveQuery(backend, { query: "SELECT 1", execute: async () => [1] });
    const unsubscribe = query.subscribe(() => undefined);
    await until(() => expect(backend.options?.suppressUnchanged).toBe(true));
    unsubscribe();
    query.close();
  });

  it("keeps the object of every row that did not change", async () => {
    const backend = new TestBackend();
    let rows = [
      { id: 1, label: "a" },
      { id: 2, label: "b" },
      { id: 3, label: "c" },
    ];
    const query = new LiveQuery(backend, {
      query: "SELECT id, label FROM t",
      // A fresh object per row per execution, as any adapter produces.
      execute: async () => rows.map((row) => ({ ...row })),
    });
    const emits: Array<ReadonlyArray<{ id: number; label: string }>> = [];
    const unsubscribe = query.subscribe(() => emits.push(query.getSnapshot().rows));
    await until(() => expect(query.getSnapshot().status).toBe("ready"));
    const first = query.getSnapshot().rows;

    // One row changes: the other two keep their identity, the array is new.
    rows = [
      { id: 1, label: "a" },
      { id: 2, label: "B" },
      { id: 3, label: "c" },
    ];
    backend.invalidate(2);
    await until(() => expect(query.getSnapshot().rows[1]?.label).toBe("B"));
    const second = query.getSnapshot().rows;
    expect(second).not.toBe(first);
    expect(second[0]).toBe(first[0]);
    expect(second[1]).not.toBe(first[1]);
    expect(second[2]).toBe(first[2]);
    expect(Object.isFrozen(second)).toBe(true);

    // Nothing changes: the array itself survives, and only the version moves.
    backend.invalidate(3);
    await until(() => expect(query.getSnapshot()).toMatchObject({ version: 3 }));
    expect(query.getSnapshot().rows).toBe(second);

    // A shorter result is a change even when every surviving row is the same.
    rows = rows.slice(0, 2);
    backend.invalidate(4);
    await until(() => expect(query.getSnapshot().rows).toHaveLength(2));
    const third = query.getSnapshot().rows;
    expect(third[0]).toBe(first[0]);
    expect(third[1]).toBe(second[1]);
    expect(emits.length).toBeGreaterThanOrEqual(3);
    unsubscribe();
    query.close();
  });

  it("subscribes for results and decodes them when the source can", async () => {
    const backend = new TestBackend();
    let executes = 0;
    let decodes = 0;
    const query = new LiveQuery(backend, {
      query: "SELECT id FROM t",
      execute: async () => {
        executes += 1;
        return [{ id: -1 }];
      },
      decode: (result) => {
        decodes += 1;
        return result.rows.map((row) => ({ id: row.id as number }));
      },
    });
    const unsubscribe = query.subscribe(() => undefined);
    await until(() => expect(query.getSnapshot().status).toBe("ready"));
    expect(backend.subscribed).toBe(1);
    expect(backend.opens).toBe(0);
    expect(query.getSnapshot()).toMatchObject({ rows: [{ id: 1 }], version: 1 });

    backend.deliver([{ id: 1 }, { id: 2 }], 2);
    await until(() => expect(query.getSnapshot().rows).toHaveLength(2));
    expect(query.getSnapshot()).toMatchObject({ version: 2 });
    expect(executes).toBe(0);
    expect(decodes).toBe(2);
    unsubscribe();
    query.close();
  });

  it("keeps a moved row's object when the engine says it kept the row", async () => {
    const backend = new TestBackend();
    const query = new LiveQuery(backend, {
      query: "SELECT id FROM t",
      execute: async () => [],
      decode: (result) => result.rows.map((row) => ({ id: row.id as number })),
    });
    const unsubscribe = query.subscribe(() => undefined);
    await until(() => expect(query.getSnapshot().status).toBe("ready"));
    const first = query.getSnapshot().rows[0];
    // Row 1 is now second, and the delivery says it was first; positional comparison alone
    // would have made it a new object.
    backend.subscribeOptions?.onChange(
      { columns: ["id"], columnDomains: [null], rows: [{ id: 0 }, { id: 1 }] },
      { manifestVersion: 2, catalogEpoch: 2, initial: false, retained: Int32Array.from([-1, 0]) },
    );
    await until(() => expect(query.getSnapshot().rows).toHaveLength(2));
    expect(query.getSnapshot().rows[1]).toBe(first);
    unsubscribe();
    query.close();
  });

  it("ignores a row map whose base delivery it did not apply", async () => {
    const backend = new TestBackend();
    let fail = false;
    const query = new LiveQuery(backend, {
      query: "SELECT id FROM t",
      execute: async () => [],
      decode: (result) => {
        if (fail) throw new Error("bad decode");
        return result.rows.map((row) => ({ id: row.id as number, tag: "fresh" }));
      },
    });
    const unsubscribe = query.subscribe(() => undefined);
    await until(() => expect(query.getSnapshot().status).toBe("ready"));
    const first = query.getSnapshot().rows[0];
    // Delivery two fails to decode; delivery three says its second row is delivery two's
    // first row. The page never held delivery two, so that map must not resurrect `first`.
    fail = true;
    backend.subscribeOptions?.onChange(
      { columns: ["id"], columnDomains: [null], rows: [{ id: 5 }] },
      { manifestVersion: 2, catalogEpoch: 2, initial: false },
    );
    await until(() => expect(query.getSnapshot().status).toBe("error"));
    fail = false;
    backend.subscribeOptions?.onChange(
      { columns: ["id"], columnDomains: [null], rows: [{ id: 6 }, { id: 5 }] },
      { manifestVersion: 3, catalogEpoch: 3, initial: false, retained: Int32Array.from([-1, 0]) },
    );
    await until(() => expect(query.getSnapshot().rows).toHaveLength(2));
    expect(query.getSnapshot().rows[1]).not.toBe(first);
    expect(query.getSnapshot().rows[1]).toEqual({ id: 5, tag: "fresh" });
    unsubscribe();
    query.close();
  });

  it("retries a failed decode on refresh from the last delivered result", async () => {
    const backend = new TestBackend();
    let fail = true;
    const query = new LiveQuery(backend, {
      query: "SELECT id FROM t",
      execute: async () => [],
      decode: (result) => {
        if (fail) throw new Error("bad decode");
        return result.rows.map((row) => ({ id: row.id as number }));
      },
    });
    const unsubscribe = query.subscribe(() => undefined);
    await until(() => expect(query.getSnapshot().status).toBe("error"));
    fail = false;
    await query.refresh();
    expect(query.getSnapshot()).toMatchObject({ status: "ready", rows: [{ id: 1 }] });
    unsubscribe();
    query.close();
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
