import { describe, expect, expectTypeOf, it } from "vitest";
import type { LiveQueryInput, LiveQueryObserveOptions } from "./live.js";
import { KeyedLiveQuery, type LiveKeyOf } from "./keyed-live.js";
import { LiveQuery, type LiveQueryBackend } from "./typed-live.js";

async function until(assertion: () => void): Promise<void> {
  let failure: unknown;
  for (let attempt = 0; attempt < 100; attempt += 1) {
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

class KeyedBackend implements LiveQueryBackend {
  options: LiveQueryObserveOptions | undefined;
  version = 1;

  async observe(_query: LiveQueryInput, options: LiveQueryObserveOptions) {
    this.options = options;
    options.onInvalidate({
      manifestVersion: this.version,
      catalogEpoch: this.version,
      initial: true,
    });
    return { dependencyTableIds: ["table"], close: () => undefined };
  }

  invalidate(): void {
    this.version += 1;
    this.options?.onInvalidate({
      manifestVersion: this.version,
      catalogEpoch: this.version,
      initial: false,
    });
  }

  refresh(): Promise<void> {
    return Promise.resolve();
  }

  close(): void {
    this.options = undefined;
  }
}

interface Row {
  id: number;
  label: string;
  nested: { active: boolean };
}

interface NullableKeyRow {
  id: number | null;
  optional?: string;
  label: string;
}

describe("keyed live queries", () => {
  it("emits exact insert, update, delete, and move records", async () => {
    const backend = new KeyedBackend();
    let rows: Row[] = [
      { id: 1, label: "one", nested: { active: true } },
      { id: 2, label: "two", nested: { active: false } },
    ];
    const source = new LiveQuery(backend, {
      query: "SELECT id, label FROM rows ORDER BY id",
      execute: () => Promise.resolve(rows),
    });
    const keyed = new KeyedLiveQuery(source, { key: "id" });
    expectTypeOf<LiveKeyOf<Row>>().toEqualTypeOf<"id" | "label">();
    expectTypeOf<LiveKeyOf<NullableKeyRow>>().toEqualTypeOf<"label">();
    expectTypeOf<typeof keyed.$inferKey>().toEqualTypeOf<number>();
    const unsubscribe = keyed.subscribe(() => undefined);
    await until(() => expect(keyed.getSnapshot().status).toBe("ready"));
    expect(keyed.getSnapshot()).toMatchObject({
      initial: true,
      changes: [
        { type: "insert", row: rows[0], index: 0 },
        { type: "insert", row: rows[1], index: 1 },
      ],
    });

    rows = [
      { id: 3, label: "three", nested: { active: true } },
      { id: 1, label: "ONE", nested: { active: false } },
    ];
    backend.invalidate();
    await until(() => expect(keyed.getSnapshot()).toMatchObject({ version: 2 }));
    const snapshot = keyed.getSnapshot();
    if (snapshot.status !== "ready") throw new Error("Expected ready keyed snapshot");
    expect(snapshot.changes).toEqual([
      {
        type: "delete",
        key: 2,
        previous: { id: 2, label: "two", nested: { active: false } },
        index: 1,
      },
      { type: "insert", row: rows[0], index: 0 },
      {
        type: "update",
        row: rows[1],
        previous: { id: 1, label: "one", nested: { active: true } },
        index: 1,
      },
      { type: "move", key: 1, row: rows[1], from: 0, to: 1 },
    ]);
    unsubscribe();
    keyed.close();
  });

  it("rejects duplicate or null keys and retains the last valid rows", async () => {
    const backend = new KeyedBackend();
    let rows: Array<{ id: number | null; label: string }> = [{ id: 1, label: "one" }];
    const source = new LiveQuery(backend, {
      query: "SELECT id, label FROM rows",
      execute: () => Promise.resolve(rows),
    });
    // The cast simulates untrusted SQL/runtime data violating a statically non-null key shape.
    const keyed = new KeyedLiveQuery(source as LiveQuery<{ id: number; label: string }>, {
      key: "id",
    });
    const unsubscribe = keyed.subscribe(() => undefined);
    await until(() => expect(keyed.getSnapshot().status).toBe("ready"));
    const goodRows = keyed.getSnapshot().rows;

    rows = [
      { id: 1, label: "one" },
      { id: 1, label: "duplicate" },
    ];
    backend.invalidate();
    await until(() => expect(keyed.getSnapshot().status).toBe("error"));
    expect(keyed.getSnapshot().rows).toBe(goodRows);
    expect((keyed.getSnapshot() as { error: Error }).error.message).toMatch(/not unique/);

    rows = [{ id: null, label: "null" }];
    backend.invalidate();
    await until(() =>
      expect((keyed.getSnapshot() as { error?: Error }).error?.message).toMatch(/non-null/),
    );
    unsubscribe();
    keyed.close();
  });

  it("bounds retained windows and reports a query that exceeds its contract", async () => {
    const backend = new KeyedBackend();
    const rows = [{ id: 1 }, { id: 2 }];
    const source = new LiveQuery(backend, {
      query: "SELECT id FROM rows",
      execute: () => Promise.resolve(rows),
    });
    const windowed = new KeyedLiveQuery(source, { key: "id", maxRows: 1 });
    const unsubscribe = windowed.subscribe(() => undefined);
    await until(() => expect(windowed.getSnapshot().status).toBe("error"));
    expect((windowed.getSnapshot() as { error: Error }).error.message).toMatch(/maximum is 1/);
    unsubscribe();
    windowed.close();
  });

  it("keeps the first successful keyed snapshot initial after a validation error", async () => {
    const backend = new KeyedBackend();
    let rows = [{ id: new Date(Number.NaN) }];
    const source = new LiveQuery(backend, {
      query: "SELECT id FROM rows",
      execute: () => Promise.resolve(rows),
    });
    const keyed = new KeyedLiveQuery(source, { key: "id" });
    const unsubscribe = keyed.subscribe(() => undefined);
    await until(() => expect(keyed.getSnapshot().status).toBe("error"));
    expect((keyed.getSnapshot() as { error: Error }).error.message).toMatch(/valid Date/);

    rows = [{ id: new Date(0) }];
    backend.invalidate();
    await until(() => expect(keyed.getSnapshot().status).toBe("ready"));
    expect(keyed.getSnapshot()).toMatchObject({ initial: true });
    unsubscribe();
    keyed.close();
  });

  it("returns one update for one changed row in a large keyed result", async () => {
    const backend = new KeyedBackend();
    let rows = Array.from({ length: 20_000 }, (_, id) => ({ id, value: id }));
    const source = new LiveQuery(backend, {
      query: "SELECT id, value FROM rows",
      execute: () => Promise.resolve(rows),
    });
    const keyed = new KeyedLiveQuery(source, { key: "id" });
    const unsubscribe = keyed.subscribe(() => undefined);
    await until(() => expect(keyed.getSnapshot().status).toBe("ready"));
    rows = rows.map((row) => (row.id === 10_000 ? { ...row, value: -1 } : row));
    backend.invalidate();
    await until(() => expect(keyed.getSnapshot()).toMatchObject({ version: 2 }));
    const snapshot = keyed.getSnapshot();
    if (snapshot.status !== "ready") throw new Error("Expected ready keyed snapshot");
    expect(snapshot.changes).toEqual([
      {
        type: "update",
        row: { id: 10_000, value: -1 },
        previous: { id: 10_000, value: 10_000 },
        index: 10_000,
      },
    ]);
    unsubscribe();
    keyed.close();
  });
});
