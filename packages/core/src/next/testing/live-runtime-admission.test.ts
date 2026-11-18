import { afterAll, expect, it, vi } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, LIVE_ADMISSION_CLOSE_REACH } from "../../testing/reach.js";
import type { LiveHandle } from "../live/runtime.js";
import { setup, until } from "./live-runtime.test-support.js";
const reach = createReach("next-live-admission-close", LIVE_ADMISSION_CLOSE_REACH);
afterAll(() => reach.assertReached());
const phases = [
  "cohort durability",
  "begin",
  "next frame",
  "next end",
  "frame durability",
  "adoption",
  "done",
  "cleaned",
  "dependencies",
  "history",
  "copy",
  "deliver",
] as const;
for (const seed of seedsFor("next-live-admission-close", [8866, 8867]))
  for (const phase of phases)
    for (const disposal of ["handle", "runtime"] as const) {
      it(`fences ${disposal} disposal at ${phase} seed=${String(seed)}`, async () => {
        reach.beginHistory();
        let handle: LiveHandle | undefined,
          armed = false,
          closed = false,
          fired = false,
          postClose = 0,
          calls = 0;
        const restorers: Array<() => void> = [];
        let stop: () => void = () => undefined;
        const trip = (at: string) => {
          if (armed && !fired && phase === at) {
            fired = true;
            closed = true;
            stop();
          }
        };
        const active = await setup(64 * 1024 * 1024, (engine, governor) => {
          const register = governor.registerCache.bind(governor);
          const cacheSpy = vi.spyOn(governor, "registerCache").mockImplementationOnce((options) => {
            const cache = register(options);
            return {
              ...cache,
              charge(bytes) {
                const accepted = cache.charge(bytes);
                if (armed) {
                  calls++;
                  trip(calls === 1 ? "dependencies" : calls === 2 ? "history" : "copy");
                }
                return accepted;
              },
            };
          });
          void engine.live;
          cacheSpy.mockRestore();
          const durable = engine.durable.bind(engine);
          const durableSpy = vi.spyOn(engine, "durable").mockImplementation((lsn) => {
            const wait = durable(lsn);
            if (!armed) return wait;
            const point = calls === 0 ? "cohort durability" : "frame durability";
            return Promise.resolve(wait).then(() => {
              trip(point);
            });
          });
          restorers.push(() => durableSpy.mockRestore());
          const open = engine.openSession.bind(engine);
          const sessionSpy = vi.spyOn(engine, "openSession").mockImplementation((...args) => {
            const session = open(...args);
            if (args[0] !== "live") return session;
            const begin = session.begin.bind(session),
              run = session.run.bind(session);
            vi.spyOn(session, "begin").mockImplementation(async (...input) => {
              await begin(...input);
              trip("begin");
            });
            vi.spyOn(session, "run").mockImplementation((...input) => {
              const execution = run(...input),
                next = execution.results.next.bind(execution.results);
              return {
                ...execution,
                results: {
                  ...execution.results,
                  cancel: (reason) => execution.results.cancel(reason),
                  poll: () => execution.results.poll(),
                  next: async () => {
                    const frame = await next();
                    trip(frame === null ? "next end" : "next frame");
                    if (frame?.adopt === undefined) return frame;
                    const adopt = frame.adopt.bind(frame);
                    return {
                      ...frame,
                      adopt(take: Parameters<typeof adopt>[0]) {
                        // governor.ts law8 and ResultFrame.adopt require atomic ownership change.
                        // Cancel after the complete capability, never inside cache.takeFrom.
                        const bytes = adopt(take);
                        if (bytes !== null) trip("adoption");
                        return bytes;
                      },
                    };
                  },
                },
                done: execution.done.then((value) => {
                  trip("done");
                  return value;
                }),
                cleaned: execution.cleaned?.then(() => {
                  trip("cleaned");
                }),
              };
            });
            return session;
          });
          restorers.push(() => sessionSpy.mockRestore());
        });
        try {
          await active.client.query("CREATE TABLE r(v INTEGER)");
          await active.client.query(
            `INSERT INTO r VALUES(${String(1 + Math.floor(mulberry32(seed)() * 100))})`,
          );
          const runtime = active.engine.live;
          if (!runtime) throw new Error("Missing live runtime");
          stop = () => {
            if (disposal === "handle") handle?.close();
            else void runtime.close();
          };
          const delivery = () => {
            if (closed) postClose++;
            trip("deliver");
          };
          handle = runtime.subscribe(
            "SELECT v FROM r",
            [],
            delivery,
            () => {
              if (closed) postClose++;
            },
            () => undefined,
          );
          armed = true;
          await until(() => fired);
          await runtime.close();
          expect(postClose).toBe(0);
          expect(runtime.retainedBytes).toBe(0);
          expect(runtime.subscriptionCount).toBe(0);
          reach.reach(phase);
          reach.reach(disposal);
        } finally {
          handle?.close();
          for (const restore of restorers) restore();
          await active.close();
        }
        expect(active.h.governor.usedBytes).toBe(0);
        reach.reach("zero charges");
      });
    }
