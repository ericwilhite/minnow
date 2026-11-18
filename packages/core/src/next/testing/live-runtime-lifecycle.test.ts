import { afterAll, describe, expect, it, vi } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createReach, LIVE_LIFECYCLE_REACH } from "../../testing/reach.js";
import type { LiveHandle } from "../live/runtime.js";
import { MemoryAdmissionError, type Governor } from "../memory/governor.js";
import type { composeEngine } from "../owner/compose.js";
import type { ClientLiveHandle } from "../owner/client.js";
import { setup, until } from "./live-runtime.test-support.js";

const reach = createReach("next-live-lifecycle", LIVE_LIFECYCLE_REACH);
afterAll(() => reach.assertReached());
function hookLiveCharge(
  engine: ReturnType<typeof composeEngine>,
  governor: Governor,
  hooks: {
    before?(bytes: number): boolean;
    after?(bytes: number, accepted: boolean): void;
  },
): void {
  const register = governor.registerCache.bind(governor);
  const spy = vi.spyOn(governor, "registerCache").mockImplementationOnce((options) => {
    const cache = register(options);
    return {
      ...cache,
      charge(bytes) {
        if (hooks.before?.(bytes) === false) return false;
        const accepted = cache.charge(bytes);
        hooks.after?.(bytes, accepted);
        return accepted;
      },
    };
  });
  try {
    expect(engine.live).toBeDefined();
  } finally {
    spy.mockRestore();
  }
}

describe("native live registration and dependency lifetimes", () => {
  for (const seed of seedsFor("next-live-lifecycle", [8830, 8831])) {
    for (const kind of ["narrow-view", "shared-buffer"] as const) {
      it(`preflights captured parameter storage ${kind} seed=${String(seed)}`, async () => {
        reach.beginHistory();
        const active = await setup(64 * 1024 * 1024);
        const runtime = active.engine.live;
        if (runtime === undefined) throw new Error("Live runtime missing");
        const baseline = runtime.retainedBytes;
        const random = mulberry32(seed);
        const width = 1 + Math.floor(random() * 8);
        const source =
          kind === "narrow-view"
            ? new Uint8Array(
                new ArrayBuffer((16 + Math.floor(random() * 2)) * 1024 * 1024),
                17,
                width,
              )
            : new Uint8Array(new SharedArrayBuffer(4096), 17, width);
        let handle: LiveHandle | undefined;
        let clone = vi.spyOn(globalThis, "structuredClone");
        try {
          let failure: unknown;
          try {
            handle = runtime.subscribe(
              "SELECT $1",
              [source],
              () => undefined,
              () => undefined,
              () => undefined,
            );
          } catch (error) {
            failure = error;
          }
          if (kind === "narrow-view") {
            expect(
              failure,
              "the complete backing store must fit before a parameter clone",
            ).toMatchObject({
              code: "MINNOW_MEMORY_ADMISSION",
              sqlState: "53200",
            });
            reach.reach("full parameter backing preflight");
          } else {
            expect(failure, "the owner protocol forbids shared parameter storage").toBeInstanceOf(
              TypeError,
            );
            reach.reach("shared parameters refused");
          }
          expect(
            clone,
            "a refused parameter capture must not allocate a probe copy",
          ).not.toHaveBeenCalled();
          expect(runtime.retainedBytes).toBe(baseline);
          expect(runtime.subscriptionCount).toBe(0);
          clone.mockRestore();
          if (kind === "narrow-view") {
            const small = new Uint8Array(new ArrayBuffer(64 * 1024), 23, width);
            small.fill(7);
            const originalClone = structuredClone;
            const credits: number[] = [];
            let captured: Uint8Array | undefined;
            clone = vi.spyOn(globalThis, "structuredClone").mockImplementation((value, options) => {
              credits.push(runtime.retainedBytes - baseline);
              const copy = originalClone(value, options);
              captured = (copy as readonly Uint8Array[])[0];
              return copy;
            });
            handle = runtime.subscribe(
              "SELECT $1",
              [small],
              () => undefined,
              () => undefined,
              () => undefined,
            );
            expect(credits).toHaveLength(1);
            expect(
              credits[0],
              "parameter capacity is admitted before cloning",
            ).toBeGreaterThanOrEqual(small.buffer.byteLength);
            expect(captured?.buffer).not.toBe(small.buffer);
            small.fill(9);
            expect(captured).toEqual(new Uint8Array(width).fill(7));
            handle.close();
            handle = undefined;
            expect(runtime.retainedBytes).toBe(baseline);
            reach.reach("parameter copy funded before allocation");
            reach.reach("parameter bytes captured privately");
          }
        } finally {
          clone.mockRestore();
          handle?.close();
          await active.close();
        }
        expect(active.h.governor.usedBytes).toBe(0);
        reach.reach("zero charges");
      });
    }

    for (const phase of ["admission", "parameter-copy"] as const) {
      it(`refuses registration retired during ${phase} seed=${String(seed)}`, async () => {
        reach.beginHistory();
        let armed = false;
        let closing: Promise<void> | undefined;
        let atClose: number | null = null;
        const owner = seed % 2 !== 0;
        const active = await setup(64 * 1024 * 1024, (engine, governor) => {
          hookLiveCharge(engine, governor, {
            after(_bytes, accepted) {
              if (phase !== "admission" || !armed || !accepted) return;
              armed = false;
              const runtime = engine.live;
              closing = (owner ? engine.close() : runtime?.close())?.then(() => {
                atClose = runtime?.retainedBytes ?? null;
              });
            },
          });
        });
        const runtime = active.engine.live;
        if (runtime === undefined) throw new Error("Live runtime missing");
        let handle: LiveHandle | undefined;
        let failure: unknown;
        const originalClone = structuredClone;
        const clone =
          phase === "parameter-copy"
            ? vi.spyOn(globalThis, "structuredClone").mockImplementationOnce((value, options) => {
                const copied = originalClone(value, options);
                closing = (owner ? active.engine.close() : runtime.close()).then(() => {
                  atClose = runtime.retainedBytes;
                });
                return copied;
              })
            : null;
        try {
          armed = true;
          try {
            handle = runtime.subscribe(
              "SELECT 1",
              [],
              () => undefined,
              () => undefined,
              () => undefined,
            );
          } catch (error) {
            failure = error;
          } finally {
            clone?.mockRestore();
          }
          await closing;
          expect(failure, "retirement must refuse the in-progress registration").toBeInstanceOf(
            Error,
          );
          expect(
            runtime.subscriptionCount,
            "closed owner must not acquire a late subscription",
          ).toBe(0);
          expect(atClose, "registration credit must unwind before close resolves").toBe(0);
          reach.reach(
            phase === "admission"
              ? "registration admission retirement"
              : "parameter-copy retirement",
          );
          reach.reach(owner ? "owner retirement" : "runtime retirement");
        } finally {
          armed = false;
          clone?.mockRestore();
          handle?.close();
          await closing;
          await active.close();
        }
        expect(active.h.governor.usedBytes).toBe(0);
        reach.reach("zero charges");
      });
    }

    it(`does not retain a retired dependency registration seed=${String(seed)}`, async () => {
      reach.beginHistory();
      const sql = "SELECT v FROM lifecycle_index";
      let armed = false;
      let fired = false;
      let handle: LiveHandle | undefined;
      let closing: Promise<void> | undefined;
      const active = await setup(64 * 1024 * 1024, (engine, governor) => {
        hookLiveCharge(engine, governor, {
          after(_bytes, accepted) {
            if (!armed || !accepted) return;
            armed = false;
            fired = true;
            if (seed % 2 === 0) handle?.close();
            else closing = engine.live?.close();
          },
        });
        const readSet = engine.readSet.bind(engine);
        engine.readSet = (plan) => {
          const tables = readSet(plan);
          armed = true;
          return tables;
        };
      });
      const registrations: Array<{ collection: Set<unknown>; value: unknown }> = [];
      // eslint-disable-next-line @typescript-eslint/unbound-method -- native add is invoked with its intercepted Set below.
      const originalAdd = Set.prototype.add;
      const add = vi.spyOn(Set.prototype, "add").mockImplementation(function (
        this: Set<unknown>,
        value: unknown,
      ) {
        if (
          typeof value === "object" &&
          value !== null &&
          Object.getOwnPropertyDescriptor(value, "sql")?.value === sql &&
          typeof Object.getOwnPropertyDescriptor(value, "metadata")?.value === "number"
        ) {
          registrations.push({ collection: this, value });
        }
        return originalAdd.call(this, value);
      });
      try {
        await active.client.query("CREATE TABLE lifecycle_index (v INTEGER)");
        await active.client.query("INSERT INTO lifecycle_index VALUES (1)");
        const runtime = active.engine.live;
        if (runtime === undefined) throw new Error("Live runtime missing");
        handle = runtime.subscribe(
          sql,
          [],
          () => undefined,
          () => undefined,
          () => undefined,
        );
        await until(() => fired);
        await runtime.close();
        await closing;
        expect(registrations.length).toBeGreaterThan(0);
        expect(
          registrations.filter(({ collection, value }) => collection.has(value)),
          "dependency buckets must not retain disposed subscriptions",
        ).toHaveLength(0);
        expect(runtime.retainedBytes).toBe(0);
        reach.reach("dependency admission retirement");
        reach.reach("no retired dependency links");
      } finally {
        armed = false;
        add.mockRestore();
        handle?.close();
        await closing;
        await active.close();
      }
      expect(active.h.governor.usedBytes).toBe(0);
      reach.reach("zero charges");
    });

    for (const action of ["close", "refresh"] as const) {
      for (const transition of ["refresh", "reconnect", "owner-change"] as const) {
        it(`keeps one registration after loading ${action}/${transition} seed=${String(seed)}`, async () => {
          reach.beginHistory();
          const active = await setup(128 * 1024 * 1024);
          let armed = false;
          let handle: ClientLiveHandle | undefined;
          let deliveries = 0;
          let rows: ReadonlyArray<readonly unknown[]> = [];
          const errors: Error[] = [];
          const sql = "SELECT v FROM loading_reentry ORDER BY v";
          try {
            await active.client.query("CREATE TABLE loading_reentry (v INTEGER)");
            await active.client.query(
              `INSERT INTO loading_reentry VALUES (${String(1 + Math.floor(mulberry32(seed)() * 8))})`,
            );
            handle = active.client.subscribeLive(sql, [], {
              onLoading() {
                if (!armed) return;
                armed = false;
                if (action === "close") handle?.close();
                else handle?.refresh();
              },
              onResult(result) {
                deliveries++;
                rows = result.rows();
              },
              onError(error) {
                errors.push(error);
              },
            });
            await until(() => deliveries === 1 && active.engine.live?.stats.waiting === 0);
            armed = true;
            if (transition === "refresh") handle.refresh();
            else if (transition === "reconnect") await active.reconnect();
            else await active.changeOwner();
            await active.client.query("SELECT 9");
            await until(() => active.engine.live?.stats.running === 0);
            expect(
              active.engine.live?.subscriptionCount,
              "a superseded loading callback must not register an orphan read",
            ).toBe(action === "close" ? 0 : 1);
            expect(active.client.liveCount).toBe(action === "close" ? 0 : 1);
            expect(errors).toEqual([]);
            reach.reach(
              action === "close"
                ? "loading closes subscription"
                : "loading refresh supersedes registration",
            );
            reach.reach(`loading ${transition} transition`);
            reach.reach("one current client registration");
            if (action === "refresh") {
              await until(() => deliveries >= 2 && active.engine.live?.stats.waiting === 0);
              expect(rows).toEqual((await active.client.query(sql)).rows());
              await active.client.query("INSERT INTO loading_reentry VALUES (99)");
              const expected = (await active.client.query(sql)).rows();
              await until(
                () =>
                  active.engine.live?.stats.dirty === 0 &&
                  active.engine.live.stats.running === 0 &&
                  active.engine.live.stats.waiting === 0,
              );
              expect(rows).toEqual(expected);
              reach.reach("scratch oracle after commits");
            }
          } finally {
            armed = false;
            handle?.close();
            await active.close();
          }
          expect(active.h.governor.usedBytes).toBe(0);
          reach.reach("zero charges");
        });
      }
    }

    it(`retires registration when its peer steps down during admission seed=${String(seed)}`, async () => {
      reach.beginHistory();
      let armed = false;
      let stop: { readonly drained: Promise<void>; finish(): void } | undefined;
      let quiesce: () => void = () => {
        throw new Error("Peer retirement fixture is not armed");
      };
      const active = await setup(64 * 1024 * 1024, (engine, governor) => {
        hookLiveCharge(engine, governor, {
          after(_bytes, accepted) {
            if (!armed || !accepted) return;
            armed = false;
            quiesce();
          },
        });
      });
      quiesce = () => {
        stop = active.server.quiesce(seed % 2 === 0 ? "closing" : "superseded");
      };
      const baseline = active.engine.live?.retainedBytes;
      let handle: ClientLiveHandle | undefined;
      try {
        armed = true;
        handle = active.client.subscribeLive("SELECT 1", [], {
          onResult: () => undefined,
          onError: () => undefined,
        });
        await until(() => stop !== undefined);
        await stop?.drained;
        expect(
          active.engine.live?.subscriptionCount,
          "a closed peer must not acquire a late live handle",
        ).toBe(0);
        expect(active.engine.live?.retainedBytes).toBe(baseline);
        reach.reach("peer retirement during registration");
      } finally {
        armed = false;
        handle?.close();
        await stop?.drained;
        stop?.finish();
        await active.close();
      }
      expect(active.h.governor.usedBytes).toBe(0);
      reach.reach("zero charges");
    });

    for (const failure of ["plan", "dependency-admission"] as const) {
      for (const trigger of ["new-table", "unrelated-table"] as const) {
        it(`recovers unknown dependencies after ${failure}/${trigger} seed=${String(seed)}`, async () => {
          reach.beginHistory();
          let refusePlan = false;
          let refuseDependencies = false;
          let denyNextCharge = false;
          let plans = 0;
          const active = await setup(128 * 1024 * 1024, (engine, governor) => {
            hookLiveCharge(engine, governor, {
              before() {
                if (!denyNextCharge) return true;
                denyNextCharge = false;
                return false;
              },
            });
            const readSet = engine.readSet.bind(engine);
            engine.readSet = (plan) => {
              const tables = readSet(plan);
              if (refuseDependencies) {
                refuseDependencies = false;
                denyNextCharge = true;
              }
              return tables;
            };
            const open = engine.openSession.bind(engine);
            engine.openSession = (...args) => {
              const session = open(...args);
              if (args[0] === "live") {
                const plan = session.plan.bind(session);
                session.plan = (sql) => {
                  plans++;
                  if (refusePlan) {
                    refusePlan = false;
                    throw new MemoryAdmissionError("live planning fixture", 64, 0);
                  }
                  return plan(sql);
                };
              }
              return session;
            };
          });
          const random = mulberry32(seed);
          const first = 1 + Math.floor(random() * 8);
          const second = 32 + Math.floor(random() * 8);
          const sql = "SELECT v FROM lifecycle_view ORDER BY v";
          let rows: ReadonlyArray<readonly unknown[]> = [];
          let status: "loading" | "ready" | "error" = "loading";
          const errors: Error[] = [];
          let handle: ClientLiveHandle | undefined;
          const idle = () => {
            const stats = active.engine.live?.stats;
            return stats?.running === 0 && stats.waiting === 0 && stats.dirty === 0;
          };
          try {
            await active.client.query("CREATE TABLE lifecycle_old (v INTEGER)");
            await active.client.query("CREATE TABLE lifecycle_new (v INTEGER)");
            await active.client.query("CREATE TABLE lifecycle_ignored (v INTEGER)");
            await active.client.query(`INSERT INTO lifecycle_old VALUES (${String(first)})`);
            await active.client.query(`INSERT INTO lifecycle_new VALUES (${String(second)})`);
            await active.client.query("CREATE VIEW lifecycle_view AS SELECT v FROM lifecycle_old");
            handle = active.client.subscribeLive(sql, [], {
              onResult(result) {
                rows = result.rows();
                status = "ready";
              },
              onError(error) {
                errors.push(error);
                status = "error";
              },
            });
            await until(() => status === "ready" && idle());
            expect(rows).toEqual((await active.client.query(sql)).rows());
            refusePlan = failure === "plan";
            refuseDependencies = failure === "dependency-admission";
            await active.client.query(
              "CREATE OR REPLACE VIEW lifecycle_view AS SELECT v FROM lifecycle_new",
            );
            const changed = (await active.client.query(sql)).rows();
            await until(() => errors.length === 1 && idle());
            expect(errors[0]).toMatchObject({ code: "MINNOW_MEMORY_ADMISSION", sqlState: "53200" });
            expect(rows).toEqual([[first]]);
            expect(changed).toEqual([[second]]);
            const steps = 1 + Math.floor(random() * 4);
            for (let step = 0; step < steps; step++) {
              const table =
                step === 0 && trigger === "unrelated-table" ? "lifecycle_ignored" : "lifecycle_new";
              await active.client.query(`INSERT INTO ${table} VALUES (${String(64 + step)})`);
              const expected = (await active.client.query(sql)).rows();
              await until(idle);
              expect(
                status,
                "an incomplete dependency proof must retry after the next commit",
              ).toBe("ready");
              expect(rows).toEqual(expected);
            }
            reach.reach(
              failure === "plan" ? "planning proof unknown" : "dependency proof admission refused",
            );
            reach.reach(
              trigger === "new-table"
                ? "new dependency invalidation"
                : "unknown proof widens globally",
            );
            const before = plans;
            for (const table of ["lifecycle_old", "lifecycle_ignored"]) {
              await active.client.query(`INSERT INTO ${table} VALUES (99)`);
              await active.client.query("SELECT 9");
              await until(idle);
              expect(plans, "a recovered dependency proof skips unaffected tables").toBe(before);
              expect(rows).toEqual((await active.client.query(sql)).rows());
            }
            reach.reach("recovered proof excludes unaffected tables");
            reach.reach("scratch oracle after commits");
          } finally {
            handle?.close();
            await active.close();
          }
          expect(active.h.governor.usedBytes).toBe(0);
          reach.reach("zero charges");
        });
      }
    }
  }
});
