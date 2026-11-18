/** Native owner contract: fitting root steps wait for temporary holders, never a second writer.
 * External result retention is a real governor grant; this is not SQL trigger enablement. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_WRITE_STEP_REACH } from "../../testing/reach.js";
import { MetadataBudget } from "../write/budget.js";
import { Governor, type Grant } from "../memory/governor.js";
import type { NativeWriteStepBoundary } from "../access/native-write-step.js";
import type { StatementImpl } from "../write/transaction.js";
import { FIXED_CACHE_BYTES } from "../storage/page-cache.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import { texts, rules, watchGrants } from "../write/test-support.js";
import { yieldTask } from "../task.js";
import { StorageCore } from "./storage-core.js";
const MiB = 1024 * 1024;

for (const seed of seedsFor("next-native-write-owner-step", [0xb70f, 0xb710]))
  it(`funds native owner boundaries without false size errors seed ${String(seed)}`, async () => {
    const reach = createReach("native write-owner step", NATIVE_WRITE_STEP_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(32, 64),
        fc.boolean(),
        fc.constantFrom("release", "cancel", "close", "grow", "oversize", "misuse", "metadata"),
        fc.integer({ min: 1, max: 16 }),
        async (cap, progress, phase, units) => {
          reach.beginHistory();
          const core = new StorageCore(64),
            governor = new Governor(cap * MiB);
          Object.defineProperty(core.harness, "governor", { value: governor });
          Object.defineProperty(core.harness, "tuning", { value: { merges: { mode: "manual" } } });
          const tracked: Grant[] = [];
          watchGrants(governor, (g) => tracked.push(g));
          const native: { metadata: MetadataBudget | null; buffer: Uint8Array | null } = {
            metadata: null,
            buffer: null,
          };
          const allocate = Reflect.get(MetadataBudget.prototype, "allocate"),
            metadataSpy = vi
              .spyOn(MetadataBudget.prototype, "allocate")
              .mockImplementation(function (this: MetadataBudget, bytes) {
                if (this.grant.label === "write path metadata") native.metadata = this;
                return allocate.call(this, bytes);
              });
          await core.open();
          let holder: Grant | null = null,
            retained: Grant | null = null,
            keysGrant: Grant | null = null,
            keys: KeyBatch | null = null,
            observer: ReturnType<typeof core.harness.path.pin> | null = null,
            statement: StatementImpl | null = null,
            tx: Awaited<ReturnType<typeof core.harness.path.begin>> | null = null,
            grants: readonly Grant[] = [],
            boundary: NativeWriteStepBoundary | null = null,
            done: Promise<unknown> | null = null,
            populated = false;
          const stop = new AbortController(),
            reason = new Error("native funding cancelled");
          const protectedNative = () =>
            FIXED_CACHE_BYTES +
            core.harness.space.stats.retainedBytes +
            tracked.reduce(
              (sum, g) =>
                sum +
                ([
                  "write path",
                  "write path metadata",
                  "write commit buffers",
                  "write merge credit",
                ].includes(g.label)
                  ? g.held
                  : 0),
              0,
            );
          try {
            await core.execute("CREATE TABLE t(id INT PRIMARY KEY,v TEXT)");
            await core.execute("CREATE INDEX probe ON t(v)");
            await core.execute(
              `INSERT INTO t VALUES${Array.from({ length: 192 }, (_, i) => `(${String(i)},'v${String(i)}')`).join(",")}`,
            );
            populated = true;
            await core.harness.path.quiesce();
            if (phase === "grow") {
              await core.harness.path.recycleLog();
              await core.harness.reopen();
              observer = core.harness.path.pin();
              keysGrant = await governor.admit({
                label: "foreign probe",
                minimumBytes: 65536,
                spillable: false,
              });
              keys = new KeyBatch(keysGrant);
              encodeKeyBatch(
                "equality",
                [{ vector: texts(["v0"]), direction: "asc", nulls: "first" }],
                { length: 1, selection: null },
                keys,
              );
              expect(keysGrant.allocate(16)).toBe(true);
            }
            tx = await core.harness.path.begin();
            statement = tx.statement({ rules });
            expect(typeof statement.prepareNativeStep, "native write funding capability").toBe(
              "function",
            );
            const fundingStatement = statement;
            const pin = statement.readOwn();
            expect(pin.released).toBe(false);
            const retainedBytes = 128 * 1024;
            retained = await governor.admit({
              label: "retained root IR",
              minimumBytes: retainedBytes,
              spillable: false,
            });
            expect(retained.allocate(retainedBytes)).toBe(true);
            const ir = new Uint8Array(retainedBytes);
            ir[0] = 37;
            const options = { includesProgress: progress };
            boundary = await statement.prepareNativeStep(retainedBytes, stop.signal, options);
            expect(boundary.protectedWriteBytes).toBe(protectedNative());
            reach.reach("native footprint equals protected holdings");
            const originalFloor = boundary.protectedWriteBytes,
              capacity = boundary.capacityBytes;
            expect(boundary.ordinaryCapacity).toBe(governor.capBytes - governor.reserveBytes);
            expect(capacity).toBe(
              progress ? governor.capBytes : governor.capBytes - governor.reserveBytes,
            );
            if (phase === "misuse") {
              expect(() => statement?.end()).toThrow("Retire native funding");
              expect(() => statement?.rollback()).toThrow("Retire native funding");
              reach.reach("statement requires witness retirement");
            }
            boundary.release();
            boundary.release();
            boundary = null;
            if (phase === "oversize") {
              await expect(
                statement.prepareNativeStep(capacity, stop.signal, options),
              ).rejects.toMatchObject({ code: "MINNOW_MEMORY_ADMISSION", sqlState: "53200" });
              reach.reach("true compound size refusal");
            } else {
              const workspace = progress
                ? capacity - originalFloor - retainedBytes - units * 4096
                : governor.reserveBytes + units * 65536;
              expect(workspace).toBeGreaterThan(governor.reserveBytes);
              const rootBytes = retainedBytes + workspace;
              let settled = false,
                attempts = 0;
              const funding = async () => {
                for (;;) {
                  boundary = await fundingStatement.prepareNativeStep(
                    rootBytes,
                    stop.signal,
                    options,
                  );
                  attempts++;
                  const current = boundary;
                  try {
                    current.check();
                    grants = await governor.admitGroup({
                      label: "root image and rules",
                      includesProgress: progress,
                      grants: [
                        {
                          label: "image",
                          minimumBytes: Math.floor(workspace / 2),
                          spillable: false,
                        },
                        {
                          label: "rule evaluator",
                          minimumBytes: workspace - Math.floor(workspace / 2),
                          spillable: false,
                        },
                      ],
                      signal: AbortSignal.any([stop.signal, current.invalidated]),
                    });
                    current.check();
                    return current.protectedWriteBytes;
                  } catch (error) {
                    for (const g of grants) g.close();
                    grants = [];
                    if (
                      !stop.signal.aborted &&
                      error instanceof Error &&
                      "code" in error &&
                      error.code === "MINNOW_NATIVE_WRITE_STEP_CHANGED"
                    )
                      continue;
                    throw error;
                  } finally {
                    current.release();
                    if (boundary === current) boundary = null;
                  }
                }
              };
              const result = funding()
                .then(
                  (value) => ({ value, error: null }),
                  (error: unknown) => ({ value: null, error }),
                )
                .then((value) => {
                  settled = true;
                  return value;
                });
              done = result;
              // Preparation yields before any fresh root admission, so this retained
              // execution competes with both stabilization and the following admission.
              const spare = governor.capBytes - governor.reserveBytes - governor.usedBytes;
              holder = governor.tryAdmit({
                label: "retained foreign result",
                minimumBytes: spare,
                spillable: false,
              });
              expect(holder).not.toBeNull();
              await yieldTask();
              await yieldTask();
              expect(settled).toBe(false);
              expect(ir[0]).toBe(37);
              reach.reach("temporary pressure waits");
              if (phase === "grow") {
                const def = core.harness.path.catalog.tables[0];
                if (!def || !observer || !keys || !keysGrant) throw Error("native probe setup");
                const out = { probes: new Int32Array(1), locators: new Float64Array(1), count: 0 };
                await observer
                  .index(def.id, def.indexes.find((i) => i.role === "secondary")?.id ?? 0)
                  .probe(keys, out);
                expect(out.count).toBe(1);
                keys.close();
                keys = null;
                keysGrant.free(16);
                keysGrant.close();
                keysGrant = null;
                observer.release();
                observer = null;
                await yieldTask();
                await yieldTask();
                expect(attempts).toBeGreaterThan(1);
                expect(settled).toBe(false);
                reach.reach("native metadata invalidates funding");
              }
              if (phase === "metadata") {
                const budget = native.metadata;
                if (!budget) throw Error("native metadata budget");
                const delta = units * 1024;
                expect(budget.allocate(delta)).toBe(true);
                native.buffer = new Uint8Array(delta);
                native.buffer[0] = 41;
                if (progress) {
                  for (let turn = 0; turn < 32 && attempts < 2; turn++) await yieldTask();
                  expect(
                    attempts,
                    "complete private metadata funding progresses before holder release",
                  ).toBeGreaterThan(1);
                }
                expect(settled).toBe(false);
                reach.reach(
                  progress
                    ? "private native metadata progresses"
                    : "ordinary native metadata waits",
                );
              }
              if (phase === "cancel") stop.abort(reason);
              else if (phase === "close") await core.close();
              else {
                holder?.close();
                holder = null;
              }
              const actual = await result;
              if (phase === "cancel") {
                expect(actual.error).toBe(reason);
                reach.reach("cancellable wait");
              } else if (phase === "close") {
                expect(actual.error).toMatchObject({ code: "MINNOW_WRITE_PATH_CLOSED" });
                reach.reach("owner close cancels funding");
              } else {
                expect(actual.error).toBeNull();
                // A resident reader may increase this floor: publication still needs the
                // maximum missing-reader forecast of the other index. Count actual
                // protected accounts, not an assumed monotone warm-cache forecast.
                expect(actual.value).toBe(protectedNative());
                const [image, rule] = grants;
                if (!image || !rule) throw Error("root grants");
                const ruleHeld = rule.held;
                expect(image.allocate(image.held)).toBe(true);
                const pixels = new Uint8Array(image.allocated);
                pixels[0] = 71;
                expect(rule.held).toBe(ruleHeld);
                expect(rule.allocate(rule.held)).toBe(true);
                const evaluated = new Uint8Array(rule.allocated);
                evaluated[0] = 83;
                expect(pixels[0]).toBe(71);
                expect(evaluated[0]).toBe(83);
                image.free(pixels.byteLength);
                rule.free(evaluated.byteLength);
                reach.reach("image cannot consume rule workspace");
              }
            }
            reach.reach(progress ? "whole-cap private progress" : "ordinary capacity");
            reach.reach(cap === 32 ? "32 MiB" : "64 MiB");
          } finally {
            stop.abort(reason);
            await done;
            boundary?.release();
            for (const g of grants) g.close();
            holder?.close();
            keys?.close();
            keysGrant?.close();
            observer?.release();
            retained?.close();
            if (native.buffer) {
              native.metadata?.free(native.buffer.byteLength);
              native.buffer = null;
            }

            await statement?.closeNativeCommands();
            tx?.rollback();
            if (populated && phase !== "close") {
              const check = core.harness.path.pin();
              try {
                const table = check.catalog.tables[0];
                expect(table && check.internalTable(table.id).liveRowCount).toBe(192);
              } finally {
                check.release();
              }
            }
            await core.close();
            metadataSpy.mockRestore();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("all ownership retired");
        },
      ),
      {
        seed,
        numRuns: 22,
        examples: [
          [32, false, "release", 1],
          [64, true, "release", 2],
          [32, false, "cancel", 3],
          [64, true, "close", 4],
          [32, true, "grow", 8],
          [64, false, "grow", 9],
          [32, false, "oversize", 1],
          [64, true, "misuse", 2],
          [64, true, "metadata", 2],
          [32, false, "metadata", 2],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
