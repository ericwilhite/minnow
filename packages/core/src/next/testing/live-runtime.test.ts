import { LiveRuntime } from "../live/runtime.js";
import { afterAll, describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createReach, LIVE_RUNTIME_REACH } from "../../testing/reach.js";
import type { ClientLiveHandle } from "../owner/client.js";
import { decodeColumn } from "../write/columns.js";
import { autocommit, batch, texts } from "../write/test-support.js";
import { setup, until, publicationTurn } from "./live-runtime.test-support.js";

const reach = createReach("next-live-runtime", LIVE_RUNTIME_REACH);
afterAll(() => reach.assertReached());

describe("native live-query runtime and MessageChannel transport", () => {
  it("releases failed delivery credit and permits the same result on refresh", async () => {
    const active = await setup();
    const runtime = active.engine.live;
    if (runtime === undefined) throw new Error("Live runtime missing");
    let deliveries = 0;
    let errors = 0;
    const handle = runtime.subscribe(
      "SELECT 1",
      [],
      () => {
        deliveries++;
        throw new Error("delivery failed");
      },
      () => {
        errors++;
        throw new Error("error callback failed too");
      },
      () => undefined,
    );
    try {
      let retained: number | null = null;
      for (let step = 1; step <= 8; step++) {
        await until(() => runtime.stats.running === 0 && runtime.stats.dirty === 0);
        expect(deliveries).toBe(step);
        expect(errors).toBe(step);
        expect(runtime.stats.waiting).toBe(0);
        retained ??= runtime.retainedBytes;
        expect(runtime.retainedBytes).toBe(retained);
        if (step < 8) handle.refresh();
      }
    } finally {
      handle.close();
      await active.close();
    }
    expect(active.h.governor.usedBytes).toBe(0);
  });
  for (const seed of seedsFor("next-live-runtime", [8801, 8802, 8803])) {
    it(`scratch-query oracle for generated interleavings seed=${String(seed)}`, async () => {
      reach.beginHistory();
      const active = await setup();
      const { client } = active;
      const errors: Error[] = [];
      interface Observed {
        sql: string;
        rows: unknown[][] | null;
        handle: ClientLiveHandle;
        deliveries: number;
      }
      const subscriptions: Observed[] = [];
      function subscribe(sql: string): Observed {
        const observed = { sql, rows: null, deliveries: 0 } as Observed;
        observed.handle = client.subscribeLive(sql, [], {
          onResult(result) {
            observed.rows = result.rows();
            observed.deliveries++;
          },
          onError(error) {
            errors.push(error);
          },
        });
        subscriptions.push(observed);
        return observed;
      }
      async function settled() {
        await until(() => {
          const stats = active.engine.live?.stats;
          return (
            stats?.subscriptions === subscriptions.length &&
            stats.dirty === 0 &&
            stats.running === 0 &&
            stats.waiting === 0
          );
        });
      }
      async function oracle() {
        for (const sub of subscriptions) {
          const expected = (await client.query(sub.sql)).rows();
          // Equal rows can predate a reconnect/refresh reset. Drain its ACK before
          // attributing any later delivery to the next generated transaction.
          await settled();
          await until(
            () => JSON.stringify(sub.rows) === JSON.stringify(expected) || errors.length > 0,
          );
          expect(errors).toEqual([]);
          expect(sub.rows).toEqual(expected);
        }
      }
      try {
        await client.query("CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER)");
        await client.query("CREATE TABLE b (id INTEGER PRIMARY KEY, v INTEGER)");
        subscribe("SELECT id, v FROM a ORDER BY v, id LIMIT 4 OFFSET 1");
        subscribe("SELECT id, v FROM b ORDER BY id");
        await oracle();
        reach.reach("subscribe");
        reach.reach("window");
        const random = mulberry32(seed);
        for (let step = 0; step < 36; step++) {
          const table = random() < 0.5 ? "a" : "b";
          const id = Math.floor(random() * 7),
            value = Math.floor(random() * 100);
          await client.query("BEGIN");
          await client.query(
            `INSERT INTO ${table} VALUES (${String(id)}, ${String(value)}) ON CONFLICT (id) DO UPDATE SET v = excluded.v`,
          );
          const before = subscriptions.map((s) => s.deliveries);
          if (step % 7 === 0) {
            await client.query("ROLLBACK");
            reach.reach("rollback");
            expect(subscriptions.map((s) => s.deliveries)).toEqual(before);
          } else {
            await client.query("COMMIT");
            reach.reach("commit");
          }
          await oracle();
          if (step % 9 === 2) {
            await active.reconnect();
            await oracle();
            reach.reach("reconnect");
          }
          if (step % 9 === 4) {
            const sub = subscribe("SELECT count(*) AS n FROM a");
            await oracle();
            sub.handle.close();
            subscriptions.splice(subscriptions.indexOf(sub), 1);
            reach.reach("unsubscribe");
          }
          if (step === 18) {
            await active.changeOwner();
            await oracle();
            reach.reach("owner change");
          }
          if (step % 8 === 3) {
            for (const sub of subscriptions) sub.handle.refresh();
            await oracle();
            reach.reach("refresh");
          }
        }
        await until(() => active.engine.live?.subscriptionCount === subscriptions.length);
        const counts = subscriptions.map((s) => s.deliveries);
        await client.query("CREATE TABLE unrelated (id INTEGER)");
        await oracle();
        // Drain the catalog sweep before testing a known unrelated data effect.
        await settled();
        const deliveries = subscriptions.map((s) => s.deliveries);
        await client.query("INSERT INTO unrelated VALUES (1)");
        await settled();
        expect(subscriptions.map((s) => s.deliveries)).toEqual(deliveries);
        expect(counts.every((n) => n > 0)).toBe(true);
        reach.reach("unaffected table");
      } finally {
        for (const sub of subscriptions) sub.handle.close();
        await active.close();
      }
      expect(active.h.governor.usedBytes).toBe(0);
      reach.reach("zero charges");
    }, 60000);
  }
  it.each(seedsFor("next-live-cleanup-fence", [8812, 8813]))(
    "drains result cleanup before owner close seed=%i",
    async (seed) => {
      reach.beginHistory();
      const active = await setup(64 * 1024 * 1024);
      const grant = await active.h.governor.admit({
        label: "test delayed result cleanup",
        minimumBytes: 65536,
        spillable: false,
      });
      let release: (() => void) | undefined;
      const barrier = new Promise<void>((resolve) => {
        release = resolve;
      });
      let entered = false;
      const open = active.engine.openSession.bind(active.engine);
      active.engine.openSession = (...args) => {
        const session = open(...args);
        if (args[0] === "live") {
          const run = session.run.bind(session);
          session.run = (...runArgs) => {
            const result = run(...runArgs);
            return {
              ...result,
              cleaned: Promise.resolve(result.cleaned ?? result.done).then(async () => {
                entered = true;
                await barrier;
                grant.close();
              }),
            };
          };
        }
        return session;
      };
      const sub = active.client.subscribeLive("SELECT 1", [], {
        onResult: () => undefined,
        onError(error) {
          throw error;
        },
      });
      try {
        await until(() => entered);
        // A separate round trip drains producer/result microtasks without releasing cleanup.
        await active.client.query("SELECT 9");
        if (seed % 2 === 0) sub.close();
        else sub.refresh();
        let closed = false;
        const close = active.engine.live?.close().then(() => {
          closed = true;
        });
        await Promise.resolve();
        expect(closed, "owner close must fence pending result cleanup").toBe(false);
        release?.();
        await close;
        expect(grant.held).toBe(0);
        reach.reach("result cleanup fence");
      } finally {
        release?.();
        sub.close();
        await active.close();
        grant.close();
      }
      expect(active.h.governor.usedBytes).toBe(0);
    },
  );
  for (const seed of seedsFor("next-live-rejected-delivery", [8826, 8827])) {
    for (const kind of ["rekey", "unchanged-result"] as const) {
      it(`keeps the last accepted watermark after ${kind} decode rejection seed=${String(seed)}`, async () => {
        reach.beginHistory();
        const active = await setup(64 * 1024 * 1024);
        let handle: ClientLiveHandle | undefined;
        let observed = -1;
        let rejectNext = false;
        const errors: Error[] = [];
        const notices: Array<{ reset: boolean; from: number[]; to: number[] }> = [];
        const decode = (bytes: Uint8Array | undefined) => {
          if (bytes === undefined) throw new Error("Key image missing");
          const data = decodeColumn(bytes, { kind: "int32", bits: 32 }).data;
          if (data.layout !== "i32") throw new Error("Wrong key image");
          return data.values[0] ?? 0;
        };
        try {
          await active.client.query(
            kind === "rekey"
              ? "CREATE TABLE rejected (id INTEGER PRIMARY KEY)"
              : "CREATE TABLE rejected (v INTEGER)",
          );
          await active.client.query("INSERT INTO rejected VALUES (1)");
          const sql =
            kind === "rekey"
              ? "SELECT id FROM rejected"
              : "SELECT count(*) AS n FROM rejected WHERE v > 0";
          handle = active.client.subscribeLive(sql, [], {
            onResult(result, _version, _generation, delivery) {
              if (rejectNext) {
                rejectNext = false;
                const error = new Error("decode fixture rejection");
                if (seed % 2 === 0) return Promise.reject(error);
                throw error;
              }
              observed = Number(result.rows()[0]?.[0]);
              notices.push({
                reset: delivery.reset,
                from: delivery.keyChanges.filter((e) => e.keyChanged).map((e) => decode(e.from[0])),
                to: delivery.keyChanges.filter((e) => e.keyChanged).map((e) => decode(e.to[0])),
              });
            },
            onError(error) {
              errors.push(error);
            },
          });
          const idle = () => {
            const stats = active.engine.live?.stats;
            return stats?.running === 0 && stats.dirty === 0 && stats.waiting === 0;
          };
          await until(() => observed === 1 && idle());
          rejectNext = true;
          await active.client.query(
            kind === "rekey" ? "UPDATE rejected SET id = 2" : "INSERT INTO rejected VALUES (2)",
          );
          await until(() => errors.length === 1 && idle());
          expect(observed).toBe(1);
          expect(errors.map((e) => e.message)).toEqual(["decode fixture rejection"]);
          reach.reach(seed % 2 === 0 ? "async delivery rejection" : "sync delivery rejection");
          const next = 3 + Math.floor(mulberry32(seed)() * 5);
          await active.client.query(
            kind === "rekey"
              ? `UPDATE rejected SET id = ${String(next)}`
              : "INSERT INTO rejected VALUES (-1)",
          );
          await publicationTurn();
          await until(idle);
          const expected = Number((await active.client.query(sql)).rows()[0]?.[0]);
          expect(
            observed,
            "the final committed value must not be suppressed after a rejected decode",
          ).toBe(expected);
          if (kind === "rekey") {
            expect(notices.at(-1)).toEqual({ reset: false, from: [1, 2], to: [2, next] });
            reach.reach("rejected delivery rekey continuity");
          } else reach.reach("rejected delivery final value");
        } finally {
          handle?.close();
          await active.close();
        }
        expect(active.h.governor.usedBytes).toBe(0);
      });
    }
  }
  it("coalesces slow consumers and cannot lose the final committed update", async () => {
    const active = await setup();
    let unblock: (() => void) | undefined;
    const blocked = new Promise<void>((resolve) => {
      unblock = resolve;
    });
    let last = -1,
      deliveries = 0;
    try {
      await active.client.query("CREATE TABLE a (v INTEGER)");
      const sub = active.client.subscribeLive("SELECT count(*) AS n FROM a", [], {
        onResult(result) {
          last = Number(result.rows()[0]?.[0]);
          deliveries++;
          if (deliveries === 1) return blocked;
        },
        onError(error) {
          throw error;
        },
      });
      await until(() => deliveries === 1);
      const retained = active.engine.live?.retainedBytes;
      for (let i = 0; i < 60; i++) await active.client.query("INSERT INTO a VALUES (1)");
      expect(deliveries).toBe(1);
      expect(active.engine.live?.retainedBytes).toBe(retained);
      unblock?.();
      await until(() => last === 60);
      expect(deliveries).toBe(2);
      sub.close();
    } finally {
      unblock?.();
      await active.close();
    }
    expect(active.h.governor.usedBytes).toBe(0);
  });
  it.each(seedsFor("next-live-rekey-history", [8806, 8807]))(
    "accepted rekey history gaps and resets seed=%i",
    async (seed) => {
      reach.beginHistory();
      const width = 4 + Math.floor(mulberry32(seed)() * 4);
      const active = await setup(64 * 1024 * 1024);
      let release: (() => void) | undefined;
      let block: Promise<void> | null = new Promise<void>((resolve) => {
        release = resolve;
      });
      const updates: Array<{ reset: boolean; from: number[]; to: number[] }> = [];
      const failures: Error[] = [];
      const decode = (bytes: Uint8Array | undefined) => {
        if (bytes === undefined) throw new Error("Key image missing");
        const value = decodeColumn(bytes, { kind: "int32", bits: 32 }).data;
        if (value.layout !== "i32") throw new Error("Wrong key layout");
        return value.values[0] ?? 0;
      };
      try {
        await active.client.query("CREATE TABLE keyed (id INTEGER PRIMARY KEY)");
        await active.client.query("INSERT INTO keyed VALUES (1)");
        const sub = active.client.subscribeLive("SELECT id FROM keyed ORDER BY id", [], {
          onResult(_result, _version, _generation, delivery) {
            updates.push({
              reset: delivery.reset,
              from: delivery.keyChanges.filter((e) => e.keyChanged).map((e) => decode(e.from[0])),
              to: delivery.keyChanges.filter((e) => e.keyChanged).map((e) => decode(e.to[0])),
            });
            const pending = block;
            block = null;
            return pending ?? undefined;
          },
          onError(error) {
            failures.push(error);
          },
        });
        await until(() => updates.length === 1 || failures.length > 0);
        expect(failures).toEqual([]);
        for (let id = 2; id <= width + 1; id++)
          await active.client.query(`UPDATE keyed SET id = ${String(id)}`);
        release?.();
        await until(() => updates.length === 2 || failures.length > 0);
        expect(failures).toEqual([]);
        expect(updates[1]).toEqual({
          reset: false,
          from: Array.from({ length: width }, (_, i) => i + 1),
          to: Array.from({ length: width }, (_, i) => i + 2),
        });
        reach.reach("rekey covered");
        // Block another delivery, then outrun both the bounded byte and record history.
        block = new Promise<void>((resolve) => {
          release = resolve;
        });
        await active.client.query(`UPDATE keyed SET id = ${String(width + 2)}`);
        await until(() => updates.length === 3);
        for (let id = width + 3; id <= 80; id++)
          await active.client.query(`UPDATE keyed SET id = ${String(id)}`);
        release?.();
        await until(() => updates.length === 4 || failures.length > 0);
        expect(failures).toEqual([]);
        expect(updates[3]).toEqual({ reset: true, from: [], to: [] });
        reach.reach("rekey reset");
        await active.changeOwner();
        await until(() => updates.length === 5);
        expect(updates[4]).toEqual({ reset: true, from: [], to: [] });
        reach.reach("rekey owner reset");
        // A separate delete/insert has no accepted UPDATE provenance.
        await active.client.query("BEGIN");
        await active.client.query("DELETE FROM keyed");
        await active.client.query("INSERT INTO keyed VALUES (81)");
        await active.client.query("COMMIT");
        await until(() => updates.length === 6);
        expect(updates[5]).toEqual({ reset: false, from: [], to: [] });
        reach.reach("row identity reuse");
        // Keep the old table's rekey pending while its SQL name is reused by a new table.
        block = new Promise<void>((resolve) => {
          release = resolve;
        });
        await active.client.query("UPDATE keyed SET id = 82");
        await until(() => updates.length === 7);
        await active.client.query("UPDATE keyed SET id = 83");
        await active.client.query("DROP TABLE keyed");
        await active.client.query("CREATE TABLE keyed (id INTEGER PRIMARY KEY)");
        await active.client.query("INSERT INTO keyed VALUES (83)");
        release?.();
        await until(() => updates.length === 8 || failures.length > 0);
        expect(failures).toEqual([]);
        expect(updates[7]).toEqual({ reset: true, from: [], to: [] });
        reach.reach("catalog identity reset");
        sub.close();
      } finally {
        release?.();
        await active.close();
      }
      expect(active.h.governor.usedBytes).toBe(0);
    },
    60000,
  );

  const pressureDefaults = [8814, 8815, 8816, 8817, 8818, 8819, 8820, 8821, 8822, 8823, 8824, 8825];
  it.each(seedsFor("next-live-pressure", pressureDefaults))(
    "refuses oversized retained state under a small cap and drains cancellation seed=%i",
    async (seed) => {
      reach.beginHistory();
      const active = await setup(32 * 1024 * 1024);
      let results = 0,
        failed = 0;
      const failures: Error[] = [];
      try {
        await active.client.query("CREATE TABLE pressure (v TEXT)");
        const sub = active.client.subscribeLive("SELECT v FROM pressure", [], {
          onResult() {
            results++;
          },
          onError(error) {
            failures.push(error);
            failed++;
          },
        });
        await until(() => results === 1);
        const payload = "x".repeat(1024 * 1024);
        const random = mulberry32(seed);
        const table = active.h.path.catalog.tables.find((table) => table.name === "pressure");
        if (table === undefined) throw new Error("Pressure table missing");
        // Isolate live retention from the SQL mutation-input spool's separate progress recipe.
        for (let i = 0; i < 6; i++) {
          await autocommit(active.h.path, (statement) =>
            statement.table(table.id).insert(batch(texts([payload])), null),
          );
          if (random() < 0.5) await new Promise<void>((resolve) => queueMicrotask(resolve));
        }
        await until(() => failed > 0);
        reach.reach("small cap");
        for (const failure of failures)
          expect(failure).toMatchObject({ sqlState: "53200", code: "MINNOW_MEMORY_ADMISSION" });
        expect(active.h.governor.peakBytes).toBeLessThanOrEqual(active.h.governor.capBytes);
        expect(active.engine.live?.retainedBytes).toBeLessThan(8 * 1024 * 1024);
        sub.close();
        await until(() => active.engine.live?.subscriptionCount === 0);
        expect(results).toBeGreaterThanOrEqual(1);
      } finally {
        await active.close();
      }
      expect(active.h.governor.usedBytes).toBe(0);
    },
    60000,
  );
  it("preserves a commit arriving during re-execution, including the final update", async () => {
    const active = await setup();
    let release: (() => void) | undefined;
    const barrier = new Promise<void>((resolve) => {
      release = resolve;
    });
    let started = false,
      gated = false,
      last = -1;
    const errors: Error[] = [];
    const open = active.engine.openSession.bind(active.engine);
    active.engine.openSession = (...args) => {
      const session = open(...args);
      if (args[0] === "live") {
        const run = session.run.bind(session);
        session.run = (...input) => {
          const result = run(...input),
            stream = result.results;
          return {
            ...result,
            results: {
              columns: stream.columns,
              poll: () => stream.poll(),
              cancel: (reason) => stream.cancel(reason),
              next: async () => {
                if (!gated) {
                  gated = true;
                  started = true;
                  await barrier;
                }
                return stream.next();
              },
            },
          };
        };
      }
      return session;
    };
    try {
      await active.client.query("CREATE TABLE race (v INTEGER)");
      const sub = active.client.subscribeLive("SELECT count(*) AS n FROM race", [], {
        onResult(result) {
          last = Number(result.rows()[0]?.[0]);
        },
        onError(error) {
          errors.push(error);
        },
      });
      await until(() => started);
      await active.client.query("INSERT INTO race VALUES (1)");
      release?.();
      await until(
        () =>
          last >= 0 &&
          active.engine.live?.stats.running === 0 &&
          active.engine.live.stats.dirty === 0 &&
          active.engine.live.stats.waiting === 0,
      );
      expect(errors).toEqual([]);
      expect(last, "a dirty watermark set during the old snapshot must survive completion").toBe(1);
      sub.close();
    } finally {
      release?.();
      await active.close();
    }
    expect(active.h.governor.usedBytes).toBe(0);
  });
  it("re-executes an FTS window only over committed source-table changes", async () => {
    const active = await setup();
    const sql = "SELECT id FROM docs WHERE MATCH(body) AGAINST 'alpha' ORDER BY id LIMIT 2";
    let rows: unknown[][] | null = null;
    const errors: Error[] = [];
    try {
      await active.client.query("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)");
      const sub = active.client.subscribeLive(sql, [], {
        onResult(result) {
          rows = result.rows();
        },
        onError(error) {
          errors.push(error);
        },
      });
      await until(() => rows !== null || errors.length > 0);
      expect(errors).toEqual([]);
      for (const statement of [
        "INSERT INTO docs VALUES (1,'alpha')",
        "INSERT INTO docs VALUES (2,'beta')",
        "UPDATE docs SET body = 'alpha' WHERE id = 2",
        "DELETE FROM docs WHERE id = 1",
      ]) {
        await active.client.query(statement);
        const oracle = (await active.client.query(sql)).rows();
        await until(() => JSON.stringify(rows) === JSON.stringify(oracle) || errors.length > 0);
        expect(errors).toEqual([]);
        expect(rows).toEqual(oracle);
      }
      sub.close();
    } finally {
      await active.close();
    }
    expect(active.h.governor.usedBytes).toBe(0);
  });
  it("coalesces reconnects while an old consumer is still decoding", async () => {
    const active = await setup();
    let release: (() => void) | undefined;
    const barrier = new Promise<void>((resolve) => {
      release = resolve;
    });
    const values: number[] = [],
      errors: Error[] = [];
    try {
      await active.client.query("CREATE TABLE reconnecting (v INTEGER)");
      const sub = active.client.subscribeLive("SELECT count(*) AS n FROM reconnecting", [], {
        onResult(result) {
          values.push(Number(result.rows()[0]?.[0]));
          if (values.length === 1) return barrier;
        },
        onError(error) {
          errors.push(error);
        },
      });
      await until(() => values.length === 1);
      for (let i = 0; i < 4; i++) {
        await active.client.query("INSERT INTO reconnecting VALUES (1)");
        await active.reconnect();
        // Fresh initial replies are received, but only one decoder callback may run.
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      expect(values).toEqual([0]);
      release?.();
      await until(() => values.at(-1) === 4 || errors.length > 0);
      expect(errors).toEqual([]);
      expect(values).toEqual([0, 4]);
      sub.close();
    } finally {
      release?.();
      await active.close();
    }
    expect(active.h.governor.usedBytes).toBe(0);
  });
  it.each(seedsFor("next-live-late-registration", [8808, 8809]))(
    "registers during a transaction without exposing uncommitted data seed=%i",
    async (seed) => {
      reach.beginHistory();
      const active = await setup();
      const old = 1 + Math.floor(mulberry32(seed)() * 4),
        next = old + 10;
      const updates: Array<{ rows: unknown[][]; reset: boolean; notices: number }> = [],
        errors: Error[] = [];
      try {
        await active.client.query("CREATE TABLE late (id INTEGER PRIMARY KEY)");
        await active.client.query(`INSERT INTO late VALUES (${String(old)})`);
        await active.client.query("BEGIN");
        await active.client.query(`UPDATE late SET id = ${String(next)}`);
        const sub = active.client.subscribeLive("SELECT id FROM late", [], {
          onResult(result, _version, _generation, delivery) {
            updates.push({
              rows: result.rows(),
              reset: delivery.reset,
              notices: delivery.keyChanges.length,
            });
          },
          onError(error) {
            errors.push(error);
          },
        });
        await until(() => updates.length > 0 || errors.length > 0);
        expect(errors).toEqual([]);
        expect(updates[0]?.rows).toEqual([[old]]);
        await active.client.query("COMMIT");
        await until(() => updates.length > 1 || errors.length > 0);
        expect(errors).toEqual([]);
        expect(updates[1]).toEqual({ rows: [[next]], reset: true, notices: 0 });
        reach.reach("late registration");
        sub.close();
      } finally {
        await active.close();
      }
      expect(active.h.governor.usedBytes).toBe(0);
    },
  );
  it.each(seedsFor("next-live-missed-notification", [8810, 8811]))(
    "catches a dropped middle notification before a later unrelated commit seed=%i",
    async (seed) => {
      reach.beginHistory();
      const active = await setup();
      const runtime = new LiveRuntime(active.engine, active.h.governor, active.h.path.changes, {
        pollIntervalMs: 60000,
      });
      Object.defineProperty(active.engine, "live", { value: runtime });
      let count = -1;
      const errors: Error[] = [];
      const notify = active.h.path.changes.notify.bind(active.h.path.changes);
      try {
        await active.client.query("CREATE TABLE missed (v INTEGER)");
        await active.client.query("CREATE TABLE other (v INTEGER)");
        const sub = active.client.subscribeLive("SELECT count(*) AS n FROM missed", [], {
          onResult(result) {
            count = Number(result.rows()[0]?.[0]);
          },
          onError(error) {
            errors.push(error);
          },
        });
        await until(() => count === 0 && runtime.stats.waiting === 0);
        let drop = true;
        active.h.path.changes.notify = () => {
          if (drop) drop = false;
          else notify();
        };
        const added = 1 + Math.floor(mulberry32(seed)() * 3);
        await active.client.query(
          `INSERT INTO missed VALUES ${Array.from({ length: added }, () => "(1)").join(",")}`,
        );
        await active.client.query("INSERT INTO other VALUES (1)");
        await publicationTurn();
        await until(
          () =>
            runtime.stats.running === 0 && runtime.stats.dirty === 0 && runtime.stats.waiting === 0,
        );
        expect(errors).toEqual([]);
        expect(
          count,
          "publication lineage must detect a missed event even when the final probe matches",
        ).toBe(added);
        reach.reach("missed middle notification");
        sub.close();
      } finally {
        active.h.path.changes.notify = notify;
        await runtime.close();
        await active.close();
      }
      expect(active.h.governor.usedBytes).toBe(0);
    },
  );
});
