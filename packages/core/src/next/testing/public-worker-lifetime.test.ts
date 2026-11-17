/** Worker startup may be abandoned before any native mutation authority is acquired. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUBLIC_WORKER_LIFETIME_REACH } from "../../testing/reach.js";
import { installNativeOwnerWorker, type NativeWorkerScope } from "../owner/native-worker.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { FakeLockRegistry } from "./fake-locks.js";
for (const seed of seedsFor("next-public-worker-lifetime", [0xb744, 0xb745]))
  it(`closes pending startup and malformed initialization without authority (${String(seed)})`, async () => {
    const reach = createReach("native worker lifetime", PUBLIC_WORKER_LIFETIME_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("before-init", "during-resolve", "malformed"),
        async (phase) => {
          reach.beginHistory();
          const backend = new MemoryBackend(),
            acquire = vi.spyOn(backend, "acquire"),
            registry = new FakeLockRegistry();
          const channel = new MessageChannel(),
            scope: NativeWorkerScope = { onmessage: null, postMessage: () => undefined };
          let entered: () => void = () => undefined,
            release: () => void = () => undefined;
          const entering = new Promise<void>((done) => {
            entered = done;
          });
          const blocked = new Promise<void>((done) => {
            release = done;
          });
          const service = installNativeOwnerWorker(
            scope,
            async () => {
              entered();
              await blocked;
              return backend;
            },
            { memoryLimitBytes: 32 << 20 },
            { locks: registry.context() },
          );
          let settled = false;
          const outcome = service.ready.then(
            () => "ready",
            (error: unknown) => {
              settled = true;
              return error;
            },
          );
          try {
            if (phase !== "before-init")
              scope.onmessage?.({
                data: {
                  t: "minnow:host",
                  database: "startup",
                  build: "test",
                  clientPort: channel.port2,
                  brokerPort: null,
                  settings: phase === "malformed" ? null : {},
                },
              });
            if (phase === "during-resolve") await entering;
            const closing = service.close();
            await Promise.resolve();
            expect(settled).toBe(true);
            release();
            await closing;
            if (phase === "malformed") expect(await outcome).toBeInstanceOf(TypeError);
            else expect(await outcome).toMatchObject({ code: "MINNOW_CLIENT_CLOSED" });
            expect(acquire).not.toHaveBeenCalled();
            expect(registry.query()).toEqual({ held: [], pending: [] });
            expect(scope.onmessage).toBeNull();
            reach.reach(phase);
          } finally {
            release();
            await service.close();
            channel.port1.close();
            channel.port2.close();
            acquire.mockRestore();
          }
        },
      ),
      { seed, numRuns: 12, examples: [["before-init"], ["during-resolve"], ["malformed"]] },
    );
    reach.assertReached();
  }, 60000);
