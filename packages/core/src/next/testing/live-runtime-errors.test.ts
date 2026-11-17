import { afterAll, describe, expect, it, vi } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createReach, LIVE_ERROR_REACH } from "../../testing/reach.js";
import type { LiveHandle } from "../live/runtime.js";
import type { composeEngine } from "../owner/compose.js";
import type { ClientLiveHandle } from "../owner/client.js";
import type { Governor, EvictableCache } from "../memory/governor.js";
import { decodeColumn } from "../write/columns.js";
import { setup, until } from "./live-runtime.test-support.js";

const errorReach = createReach("next-live-errors", LIVE_ERROR_REACH);
afterAll(() => errorReach.assertReached());

function controlLiveAdmission(refused: () => boolean, capture?: (cache: EvictableCache) => void) {
  return (engine: ReturnType<typeof composeEngine>, governor: Governor) => {
    const register = governor.registerCache.bind(governor);
    const spy = vi.spyOn(governor, "registerCache").mockImplementationOnce((options) => {
      capture?.(options);
      const cache = register(options);
      return {
        ...cache,
        takeFrom: (source: unknown, bytes: number) => !refused() && cache.takeFrom(source, bytes),
      };
    });
    try {
      expect(engine.live).toBeDefined();
      expect(spy).toHaveBeenCalledTimes(1);
    } finally {
      spy.mockRestore();
    }
  };
}

describe("native live-query runtime and MessageChannel transport", () => {
  for (const seed of seedsFor("next-live-errors", [8828, 8829])) {
    for (const [modeIndex, mode] of (["query", "admission"] as const).entries()) {
      for (const [actionIndex, action] of (
        ["none", "refresh", "reconnect", "owner-change"] as const
      ).entries()) {
        it(`bounds live errors and recovers ${mode}/${action} seed=${String(seed)}`, async () => {
          errorReach.beginHistory();
          const random = mulberry32(seed + 17 * modeIndex + 31 * actionIndex);
          const rekey = mode === "query";
          const reject = (seed + modeIndex) % 2 !== 0;
          let refuse = false;
          let cache: EvictableCache | undefined;
          const active = await setup(
            128 * 1024 * 1024,
            controlLiveAdmission(
              () => refuse,
              (registered) => {
                cache = registered;
              },
            ),
          );
          let handle: ClientLiveHandle | undefined;
          let release: (() => void) | undefined;
          const barrier = new Promise<void>((resolve) => {
            release = resolve;
          });
          const errors: Error[] = [];
          const values: unknown[][][] = [];
          const notices: Array<{ reset: boolean; from: number[]; to: number[] }> = [];
          let status = "loading";
          const sql = rekey
            ? "SELECT id, 10 / divisor AS n FROM live_errors ORDER BY id"
            : "SELECT count(*) AS n FROM live_errors WHERE divisor > 0";
          const model = new Map<number, number>();
          const stats = () => {
            const runtime = active.engine.live;
            if (runtime === undefined) throw new Error("Live runtime missing");
            return runtime.stats;
          };
          const idle = () => {
            const current = stats();
            return current.running === 0 && current.dirty === 0 && current.waiting === 0;
          };
          const decode = (bytes: Uint8Array | undefined) => {
            if (bytes === undefined) throw new Error("Missing key image");
            const column = decodeColumn(bytes, { kind: "int32", bits: 32 }).data;
            if (column.layout !== "i32") throw new Error("Wrong key image");
            return column.values[0] ?? 0;
          };
          async function oracle() {
            const divisors = [...model.values()];
            const fails = rekey && divisors.includes(0);
            if (fails) {
              await expect(active.client.query(sql)).rejects.toMatchObject({ sqlState: "22012" });
              return null;
            }
            const expected = rekey
              ? [...model].sort(([a], [b]) => a - b).map(([id, d]) => [id, Math.trunc(10 / d)])
              : [[divisors.filter((d) => d > 0).length]];
            const rows = (await active.client.query(sql)).rows();
            expect(rows.map((row) => row.map(Number))).toEqual(expected);
            return rows;
          }
          try {
            await active.client.query(
              "CREATE TABLE live_errors (id INTEGER PRIMARY KEY, divisor INTEGER)",
            );
            await active.client.query("CREATE TABLE ignored_errors (v INTEGER)");
            const count = rekey ? 1 : 1 + Math.floor(random() * 3);
            for (let id = 1; id <= count; id++) {
              const divisor = 1;
              await active.client.query(
                `INSERT INTO live_errors VALUES (${String(id)}, ${String(divisor)})`,
              );
              model.set(id, divisor);
            }
            handle = active.client.subscribeLive(sql, [], {
              onResult(result, _version, _generation, delivery) {
                status = "ready";
                values.push(result.rows());
                notices.push({
                  reset: delivery.reset,
                  from: delivery.keyChanges
                    .filter((e) => e.keyChanged)
                    .map((e) => decode(e.from[0])),
                  to: delivery.keyChanges.filter((e) => e.keyChanged).map((e) => decode(e.to[0])),
                });
              },
              onError(error) {
                status = "error";
                errors.push(error);
                if (errors.length !== 1) return;
                const pending = barrier.then(() => {
                  if (reject) throw new Error("error consumer fixture rejection");
                });
                // The parent client ignores a returned promise. RED still fails by an assertion.
                void pending.catch(() => undefined);
                return pending;
              },
            });
            await until(() => values.length === 1 && idle());
            expect(values[0]).toEqual(await oracle());
            refuse = mode === "admission";
            const bad = mode === "query" ? 0 : rekey ? 1 : -1;
            let currentId = rekey ? count + 10 : count + 1;
            if (rekey) {
              await active.client.query(
                `UPDATE live_errors SET id=${String(currentId)}, divisor=${String(bad)} WHERE id=${String(count)}`,
              );
              model.delete(count);
            } else
              await active.client.query(
                `INSERT INTO live_errors VALUES (${String(currentId)}, ${String(bad)})`,
              );
            model.set(currentId, bad);
            await oracle();
            await until(() => errors.length === 1 && stats().running === 0);
            expect(errors[0]).toMatchObject({ sqlState: mode === "query" ? "22012" : "53200" });
            expect(stats().waiting, "a consumed error promise must hold one owner credit").toBe(1);
            errorReach.reach("one unacknowledged error");
            if (action === "refresh") handle.refresh();
            else if (action === "reconnect") await active.reconnect();
            else if (action === "owner-change") await active.changeOwner();
            if (action !== "none") {
              // Drain cancellation and registration before measuring the replacement slot.
              await active.client.query("SELECT 9");
              await until(
                () => stats().subscriptions === 1 && stats().waiting === 1 && stats().running === 0,
              );
            }
            if (action === "none") {
              // Error copy credit must not pin the last acknowledged owner baseline.
              expect(cache).toBeDefined();
              const evictable = cache?.evictableBytes ?? 0;
              expect(
                evictable,
                "an unacknowledged error leaves the good baseline evictable",
              ).toBeGreaterThan(0);
              expect(cache?.evict(1)).toBe(evictable);
              errorReach.reach("error baseline evictable");
            }
            const heldBytes = active.engine.live?.retainedBytes;
            const steps = 4 + Math.floor(random() * 8);
            for (let step = 0; step < steps; step++) {
              if (rekey)
                await active.client.query(
                  `UPDATE live_errors SET divisor=${String(bad)} WHERE id=${String(currentId)}`,
                );
              else {
                currentId++;
                await active.client.query(
                  `INSERT INTO live_errors VALUES (${String(currentId)}, ${String(bad)})`,
                );
                model.set(currentId, bad);
              }
              await oracle();
              if (random() < 0.5) {
                await active.client.query(`INSERT INTO ignored_errors VALUES (${String(step)})`);
                await oracle();
              }
              expect(errors).toHaveLength(1);
              expect(stats().waiting).toBe(1);
              expect(
                active.engine.live?.retainedBytes,
                "error bursts retain one notification",
              ).toBe(heldBytes);
            }
            errorReach.reach("error burst coalesced");
            errorReach.reach("error memory bounded");
            refuse = false;
            const finalId = rekey ? count + 20 : currentId + 1;
            if (rekey) {
              await active.client.query(
                `UPDATE live_errors SET id=${String(finalId)}, divisor=1 WHERE id=${String(currentId)}`,
              );
              model.delete(currentId);
            } else
              await active.client.query(`INSERT INTO live_errors VALUES (${String(finalId)}, -1)`);
            model.set(finalId, rekey ? 1 : -1);
            const expected = await oracle();
            release?.();
            await until(idle);
            expect(status, "a successful identical value must clear the error snapshot").toBe(
              "ready",
            );
            expect(values).toHaveLength(2);
            expect(values.at(-1)).toEqual(expected);
            expect(errors).toHaveLength(action === "none" ? 1 : 2);
            if (action !== "none") {
              expect(notices.at(-1)).toEqual({ reset: true, from: [], to: [] });
              errorReach.reach(`error ${action} reset`);
            } else if (rekey) {
              expect(notices.at(-1)).toEqual({
                reset: false,
                from: [count, currentId],
                to: [currentId, finalId],
              });
              errorReach.reach("error ACK preserves accepted rekeys");
            } else {
              expect(values.at(-1)).toEqual(values[0]);
              errorReach.reach("same-row ready recovery");
            }
            errorReach.reach(mode === "query" ? "query error" : "admission error");
            if (reject) errorReach.reach("error callback rejection isolated");
          } finally {
            refuse = false;
            release?.();
            handle?.close();
            await active.close();
          }
          expect(active.h.governor.usedBytes).toBe(0);
          errorReach.reach("zero charges");
        }, 60000);
      }
    }
    it(`clears the error on unchanged result recovery seed=${String(seed)}`, async () => {
      errorReach.beginHistory();
      let refuse = false;
      const active = await setup(
        128 * 1024 * 1024,
        controlLiveAdmission(() => refuse),
      );
      let handle: ClientLiveHandle | undefined;
      let status = "loading";
      const values: unknown[][][] = [];
      const errors: Error[] = [];
      const sql = "SELECT count(*) AS n FROM same_error WHERE divisor > 0";
      const idle = () => {
        const stats = active.engine.live?.stats;
        return stats?.running === 0 && stats.dirty === 0 && stats.waiting === 0;
      };
      try {
        await active.client.query(
          "CREATE TABLE same_error (id INTEGER PRIMARY KEY, divisor INTEGER)",
        );
        await active.client.query("INSERT INTO same_error VALUES (1, 1)");
        handle = active.client.subscribeLive(sql, [], {
          onResult(result) {
            status = "ready";
            values.push(result.rows());
          },
          onError(error) {
            status = "error";
            errors.push(error);
          },
        });
        await until(() => values.length === 1 && idle());
        refuse = true;
        await active.client.query("INSERT INTO same_error VALUES (2, -1)");
        expect((await active.client.query(sql)).rows()).toEqual(values[0]);
        await until(() => errors.length === 1 && idle());
        refuse = false;
        await active.client.query("INSERT INTO same_error VALUES (3, -1)");
        const expected = (await active.client.query(sql)).rows();
        await until(idle);
        expect(status, "ready must follow error even when the wire rows are identical").toBe(
          "ready",
        );
        expect(values).toHaveLength(2);
        expect(values.at(-1)).toEqual(expected);
        expect(values[0]).toEqual(expected);
      } finally {
        handle?.close();
        await active.close();
      }
      expect(active.h.governor.usedBytes).toBe(0);
    }, 60000);
  }

  it.each(seedsFor("next-live-errors", [8828, 8829]))(
    "fences close during diagnostic admission seed=%i",
    async (seed) => {
      errorReach.beginHistory();
      let armed = false;
      let planned = false;
      let handle: LiveHandle | undefined;
      let closing: Promise<void> | undefined;
      let atClose: number | null = null;
      const active = await setup(64 * 1024 * 1024, (engine, governor) => {
        const register = governor.registerCache.bind(governor);
        const spy = vi.spyOn(governor, "registerCache").mockImplementationOnce((options) => {
          const cache = register(options);
          return {
            ...cache,
            charge(bytes) {
              const accepted = cache.charge(bytes);
              if (armed && accepted) {
                armed = false;
                const runtime = engine.live;
                if (runtime === undefined) throw new Error("Live runtime missing");
                if (seed % 2) handle?.close();
                closing = runtime.close().then(() => {
                  atClose = runtime.retainedBytes;
                });
              }
              return accepted;
            },
          };
        });
        try {
          expect(engine.live).toBeDefined();
        } finally {
          spy.mockRestore();
        }
        // The empty committed database is already durable: exercise the no-await launch prefix.
        engine.durable = () => null;
        const open = engine.openSession.bind(engine);
        engine.openSession = (...args) => {
          const session = open(...args);
          if (args[0] === "live")
            session.plan = () => {
              planned = true;
              throw new Error("diagnostic admission fixture");
            };
          return session;
        };
      });
      try {
        const runtime = active.engine.live;
        if (runtime === undefined) throw new Error("Live runtime missing");
        handle = runtime.subscribe(
          "SELECT 1",
          [],
          () => undefined,
          () => undefined,
          () => undefined,
        );
        armed = true;
        await until(
          () =>
            atClose !== null || (planned && closing === undefined && runtime.stats.running === 0),
        );
        // Detect the parent's uncharged error path by assertion, without waiting for a timeout.
        expect(closing, "error preparation must acquire governed copy credit").toBeDefined();
        expect(
          atClose,
          "close must fence the synchronous launch prefix and its prepaid scratch",
        ).toBe(0);
        await closing;
        errorReach.reach("diagnostic admission retirement");
      } finally {
        armed = false;
        handle?.close();
        await closing;
        await active.close();
      }
      expect(active.h.governor.usedBytes).toBe(0);
    },
  );
});
