import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { LiveQuerySet, type LiveQueryHost, type LiveQuerySubscribeOptions } from "./live.js";
import { LiveQuery, type LiveQueryBackend } from "./typed-live.js";
import { bindPlanParameters, compileQuery, type QueryResult } from "./query.js";
import { planMemoKey } from "./query-cache.js";

function rows(ids: number[]): QueryResult {
  return { columns: ["id"], columnDomains: [null], rows: ids.map((id) => ({ id })) };
}

function deliveringBackend() {
  let listener: LiveQuerySubscribeOptions | undefined;
  let opens = 0;
  let closes = 0;
  const backend: LiveQueryBackend = {
    subscribe: async (_query, options) => {
      listener = options;
      opens += 1;
      options.onChange(rows([1, 2]), { manifestVersion: 1, catalogEpoch: 1, initial: true });
      return {
        dependencyTableIds: ["t"],
        close: () => {
          closes += 1;
          options.onComplete?.();
        },
      };
    },
    observe: async () => {
      throw new Error("Expected direct delivery");
    },
    refresh: async () => undefined,
    close: () => undefined,
  };
  return {
    backend,
    counts: () => ({ opens, closes }),
    deliver: (ids: number[], retained: number[]) =>
      listener?.onChange(rows(ids), {
        manifestVersion: 2,
        catalogEpoch: 2,
        initial: false,
        retained: Int32Array.from(retained),
      }),
  };
}

async function settle(): Promise<void> {
  for (let turn = 0; turn < 40; turn += 1) await Promise.resolve();
}

describe("audited public typed query boundaries", () => {
  it("shares transaction visibility, savepoints and domain conversion with SQL", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    const plan = { kind: "typed-query" as const, plan: compileQuery("SELECT x FROM t WHERE id=1") };
    try {
      await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x NUMERIC(10,2))");
      await database.execute("INSERT INTO t VALUES (1,10)");
      expect(await database.run(plan)).toEqual([{ x: "10.00" }]);
      await database.execute("BEGIN");
      await database.execute("UPDATE t SET x=20 WHERE id=1");
      await database.execute("SAVEPOINT a");
      await database.execute("UPDATE t SET x=30 WHERE id=1");
      expect(await database.run(plan)).toEqual([{ x: "30.00" }]);
      await database.execute("ROLLBACK TO a");
      expect(await database.run(plan)).toEqual([{ x: "20.00" }]);
      await database.execute("ROLLBACK");
      expect(await database.run(plan)).toEqual([{ x: "10.00" }]);
      await database.execute("BEGIN");
      await database.execute("UPDATE t SET x=40 WHERE id=1");
      await database.execute("COMMIT");
      expect(await database.run(plan)).toEqual([{ x: "40.00" }]);
      for (const sql of ["SELECT DATE '2026-01-01' AS v", "SELECT JSON_ARRAY(1,2) AS v"]) {
        const expected = (await database.query(sql)).rows;
        const typed = { kind: "typed-query" as const, plan: compileQuery(sql) };
        expect(await database.run(typed)).toEqual(expected);
        expect(await database.run(typed)).toEqual(expected);
      }
    } finally {
      await database.close();
    }
  });

  it("keeps Date and text memo identities separate and never memoizes sequences", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    try {
      const template = compileQuery("SELECT ? AS v");
      const instant = new Date("2026-01-01T00:00:00.000Z");
      const date = bindPlanParameters(template, [instant]);
      const text = bindPlanParameters(template, [instant.toISOString()]);
      expect(planMemoKey(date)).not.toBe(planMemoKey(text));
      expect(await database.run({ kind: "typed-query", plan: date })).toEqual([{ v: instant }]);
      expect(await database.run({ kind: "typed-query", plan: text })).toEqual([
        { v: instant.toISOString() },
      ]);
      await database.execute("CREATE SEQUENCE s");
      const sequence = {
        kind: "typed-query" as const,
        plan: compileQuery("SELECT NEXTVAL('s') AS v"),
      };
      expect(await database.run(sequence)).toEqual([{ v: 1 }]);
      expect(await database.run(sequence)).toEqual([{ v: 2 }]);
      expect((await database.query("SELECT NEXTVAL('s') AS v")).rows).toEqual([{ v: 3 }]);
    } finally {
      await database.close();
    }
  });
});

describe("audited live delivery boundaries", () => {
  it.each(["reorder", "cross-row"])("checks retained rows after a %s decoder", async (kind) => {
    const fixture = deliveringBackend();
    const query = new LiveQuery(fixture.backend, {
      query: "SELECT id FROM t",
      execute: async () => [],
      decode: (result) =>
        kind === "reorder"
          ? [...result.rows].reverse()
          : result.rows.map((row) => ({ ...row, last: result.rows.at(-1)?.id ?? null })),
    });
    const unsubscribe = query.subscribe(() => undefined);
    await settle();
    fixture.deliver([1, 3], [0, -1]);
    await settle();
    expect(query.getSnapshot().rows).toEqual(
      kind === "reorder"
        ? [{ id: 3 }, { id: 1 }]
        : [
            { id: 1, last: 3 },
            { id: 3, last: 3 },
          ],
    );
    unsubscribe();
    query.close();
  });

  it("loads concurrent cold refreshes once and releases the temporary subscription", async () => {
    const fixture = deliveringBackend();
    const query = new LiveQuery(fixture.backend, {
      query: "SELECT id FROM t",
      execute: async () => [],
      decode: (result) => result.rows,
    });
    await Promise.all([query.refresh(), query.refresh()]);
    await settle();
    expect(query.getSnapshot()).toMatchObject({ status: "ready", rows: [{ id: 1 }, { id: 2 }] });
    expect(fixture.counts()).toEqual({ opens: 1, closes: 1 });
    query.close();
  });

  it.each([false, true])(
    "delivers raw invalidations independently of suppression (reverse=%s)",
    async (reverse) => {
      let version = 1;
      const host: LiveQueryHost = {
        currentProbe: async () => ({
          manifestVersion: version,
          catalogEpoch: version,
          schemaEpoch: 0,
        }),
        dependencyTableIds: async () => new Set(["t"]),
        manifestPage: async () => ({
          records: [
            {
              version: 2,
              previousVersion: 1,
              createdAt: "",
              liveBlockCount: 0,
              liveBlockBytes: 0,
              changedTableIds: ["t"],
            },
          ],
          nextCursor: null,
        }),
        execute: async () => rows([1]),
      };
      const set = new LiveQuerySet(host);
      let raw = 0;
      let suppressed = 0;
      const subscriptions = [
        () =>
          set.observe("q", {
            suppressUnchanged: true,
            onInvalidate: () => {
              suppressed += 1;
            },
          }),
        () =>
          set.observe("q", {
            onInvalidate: () => {
              raw += 1;
            },
          }),
      ];
      if (reverse) subscriptions.reverse();
      for (const subscribe of subscriptions) await subscribe();
      version = 2;
      await set.refresh();
      expect({ raw, suppressed }).toEqual({ raw: 2, suppressed: 1 });
      await set.refresh();
      expect(raw).toBe(2);
      set.close();
    },
  );

  it("survives throwing error and completion callbacks", async () => {
    let version = 1;
    let fail = false;
    let delivered = 0;
    let completed = 0;
    const host: LiveQueryHost = {
      currentProbe: async () => ({
        manifestVersion: version,
        catalogEpoch: version,
        schemaEpoch: 0,
      }),
      dependencyTableIds: async () => new Set(["t"]),
      manifestPage: async (after) => ({
        records: Array.from({ length: version - (after ?? 0) }, (_, index) => ({
          version: (after ?? 0) + index + 1,
          previousVersion: (after ?? 0) + index,
          createdAt: "",
          liveBlockCount: 0,
          liveBlockBytes: 0,
          changedTableIds: ["t"],
        })),
        nextCursor: null,
      }),
      execute: async () => {
        if (fail) throw new Error("host");
        return rows([version]);
      },
    };
    const set = new LiveQuerySet(host);
    await set.subscribe("q", {
      onChange: () => {
        delivered += 1;
      },
      onError: () => {
        throw new Error("consumer");
      },
      onComplete: () => {
        throw new Error("completion");
      },
    });
    await set.subscribe("q", {
      onChange: () => undefined,
      onComplete: () => {
        completed += 1;
      },
    });
    fail = true;
    version = 2;
    await set.refresh();
    fail = false;
    version = 3;
    await set.refresh();
    expect(delivered).toBe(2);
    set.close();
    expect(completed).toBe(1);
  });
});
