/** Production funding consumer on B5b's real native boundary; SQL effects are not replay callbacks. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_ROOT_CREDITS_REACH } from "../../testing/reach.js";
import { NativeRootCredits, NATIVE_ROOT_CREDIT_BYTES } from "../owner/native-root-credits.js";
import { NativeWriteStepChangedError } from "../access/native-write-step.js";
import { Governor, type Grant } from "../memory/governor.js";
import { StatementImpl } from "../write/transaction.js";
import { NO_RULES } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";

for (const seed of seedsFor("next-native-root-credits", [46972, 46973]))
  it(`funds compound root steps without retaining an input or stale witness (${String(seed)})`, async () => {
    const reach = createReach("native root credits", NATIVE_ROOT_CREDITS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(32, 64),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 1, max: 16384 }),
        async (cap, progress, invalidate, cancel, width) => {
          reach.beginHistory();
          const core = new StorageCore(64),
            governor = new Governor(cap << 20),
            stop = new AbortController();
          Object.defineProperty(core.harness, "governor", { value: governor });
          await core.open();
          let credits: NativeRootCredits | null = null,
            foreign: Grant | null = null;
          let transaction: Awaited<ReturnType<typeof core.harness.path.begin>> | null = null;
          let statement: StatementImpl | null = null;
          let injection: AbortController | null = null,
            injected = false;
          const requests: number[] = [],
            modes: boolean[] = [];
          const admit = governor.admitGroup.bind(governor);
          const spy = vi.spyOn(governor, "admitGroup").mockImplementation(async (options) => {
            const result = await admit(options);
            if (options.label === "native root step" && invalidate && !injected) {
              injected = true;
              injection?.abort(new NativeWriteStepChangedError());
            }
            return result;
          });
          try {
            await core.execute("CREATE TABLE t(id INT PRIMARY KEY,v TEXT)");
            transaction = await core.harness.path.begin();
            const opened = transaction.statement({ signal: stop.signal, rules: () => NO_RULES });
            if (!(opened instanceof StatementImpl))
              throw new TypeError("Expected native statement");
            statement = opened;
            const native = opened;
            credits = await NativeRootCredits.open({
              governor,
              signal: stop.signal,
              includesProgress: progress,
              statement: {
                async prepareNativeStep(bytes, signal, options) {
                  requests.push(bytes);
                  modes.push(options?.includesProgress === true);
                  const boundary = await native.prepareNativeStep(bytes, signal, options);
                  const local = new AbortController();
                  injection = local;
                  return {
                    ...boundary,
                    protectedWriteBytes: boundary.protectedWriteBytes,
                    ordinaryCapacity: boundary.ordinaryCapacity,
                    capacityBytes: boundary.capacityBytes,
                    invalidated: AbortSignal.any([boundary.invalidated, local.signal]),
                    check() {
                      boundary.check();
                      local.signal.throwIfAborted();
                    },
                    release: () => boundary.release(),
                  };
                },
              },
            });
            expect(credits.metadata.allocated).toBe(NATIVE_ROOT_CREDIT_BYTES);
            reach.reach("control precharged");
            await credits.reserve(
              { metadata: NATIVE_ROOT_CREDIT_BYTES + 16384, rules: 8192, parameters: 4096 },
              stop.signal,
            );
            expect(credits.metadata.allocate(8192)).toBe(true);
            expect(credits.rules.allocate(4096)).toBe(true);
            expect(credits.parameters.allocate(4096)).toBe(true);
            await credits.protectRules(8192 + width, stop.signal);
            await credits.prepareMetadata(width, stop.signal);
            expect(credits.rules.held).toBeGreaterThanOrEqual(8192 + width);
            expect(credits.metadata.allocate(width)).toBe(true);
            credits.metadata.free(width);
            reach.reach("rule workspace protected from image funding");
            credits.releaseIdleStep();
            await credits.prepareSource(width, stop.signal);
            expect(requests.at(-1)).toBe(NATIVE_ROOT_CREDIT_BYTES + 16384 + 8192 + 4096 + width);
            expect(modes.every((mode) => mode === progress)).toBe(true);
            reach.reach(progress ? "complete private mode" : "ordinary reserve mode");
            reach.reach("root floor included once");
            if (invalidate) {
              expect(injected).toBe(true);
              expect(
                requests.filter((bytes) => bytes === NATIVE_ROOT_CREDIT_BYTES + 16384 + 8192 + 4096)
                  .length,
              ).toBeGreaterThan(1);
              reach.reach("invalidated funding retried");
            }
            expect(credits.source.allocate(width)).toBe(true);
            const beforeBorrow = requests.length;
            try {
              await expect(credits.prepareMetadata(2 * width + 32768, stop.signal)).rejects.toThrow(
                "source",
              );
            } finally {
              credits.source.free(width);
            }
            expect(requests.length).toBe(beforeBorrow);
            reach.reach("source cannot span funding");
            // Never enter a native wait under a real governor progress loan.
            const loan = await credits.source.borrow(1, stop.signal);
            try {
              await expect(credits.prepareMetadata(2 * width + 32768, stop.signal)).rejects.toThrow(
                "loan",
              );
            } finally {
              loan.repay();
            }
            reach.reach("loan cannot span funding");
            credits.releaseIdleStep();
            const held = await governor.admitGroup({
              label: "foreign result",
              includesProgress: true,
              grants: [
                {
                  label: "foreign result",
                  minimumBytes: governor.capBytes - governor.usedBytes,
                  spillable: false,
                },
              ],
            });
            foreign = held[0] ?? null;
            if (foreign === null) throw new Error("Missing foreign holder");
            expect(foreign.allocate(foreign.held)).toBe(true);
            const abort = new AbortController();
            // Publish a deterministic gate only after fresh admission was requested.
            let queued: () => void = () => undefined;
            const waiting = new Promise<void>((resolve) => {
              queued = resolve;
            });
            spy.mockImplementation((options) => {
              const result = admit(options);
              if (options.label === "native root step") queued();
              return result;
            });
            const pending = credits.prepareSource(width + 1, abort.signal);
            const outcome = pending.then(
              () => null,
              (error: unknown) => error,
            );
            await waiting;
            if (cancel) {
              const reason = new Error("cancel compound admission");
              abort.abort(reason);
              expect(await outcome).toBe(reason);
              reach.reach("temporary pressure cancels");
            } else {
              foreign.close();
              foreign = null;
              expect(await outcome).toBeNull();
              expect(credits.source.allocate(width + 1)).toBe(true);
              credits.source.free(width + 1);
              reach.reach("temporary pressure waits");
            }
            foreign?.close();
            foreign = null;
            credits.releaseIdleStep();
            // Early close detects the caller's live bindings; it cannot uncharge them.
            await expect(credits.close()).rejects.toThrow("live allocations");
            expect(credits.rules.allocated).toBe(4096);
            credits.metadata.free(8192);
            credits.rules.free(4096);
            credits.parameters.free(4096);
            await credits.close();
            await credits.close();
            credits = null;
            reach.reach("premature close preserves ownership");
            reach.reach("zero root ownership");
          } finally {
            foreign?.close();
            spy.mockRestore();
            if (credits !== null) {
              for (const grant of [credits.source, credits.rules, credits.parameters])
                if (grant.allocated) grant.free(grant.allocated);
              if (credits.metadata.allocated > NATIVE_ROOT_CREDIT_BYTES)
                credits.metadata.free(credits.metadata.allocated - NATIVE_ROOT_CREDIT_BYTES);
              await credits.close();
            }
            statement?.rollback();
            transaction?.rollback();
            await core.close();
            expect(governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [32, false, false, false, 1024],
          [64, true, true, true, 16384],
          [32, true, true, false, 1],
          [64, false, false, true, 8192],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
