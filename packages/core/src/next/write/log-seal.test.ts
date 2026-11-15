/** A seal runs after every append wait, in the synchronous section assigning its LSN. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, LOG_SEAL_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { MAX_FRAME_BODY_BYTES, scanLog } from "../format/log-frame.js";
import { LogAppender } from "./log.js";
import { CommitBuffers, COMMIT_MINIMUM_BYTES, LOG_QUEUE_BYTES } from "./commit-buffers.js";

for (const seed of seedsFor("next-log-seal", [0xb87f, 0xb880]))
  it(`seals fitting, queued, recycled and multipart bodies seed ${String(seed)}`, async (context) => {
    const reach = createReach("serialized append seal", LOG_SEAL_REACH);
    let stopped = false;
    let release: () => void = () => undefined;
    context.onTestFinished(() => {
      stopped = true;
      release();
    });
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(1, 2, 31, 32, 127), async (tail) => {
        for (const mode of ["fitting", "queue", "recycle", "stream"] as const) {
          if (stopped) throw new Error("Retired log seal history");
          reach.beginHistory();
          const governor = new Governor(16 * 1024 * 1024);
          const grant = await governor.admit({
            label: "seal buffers",
            minimumBytes: COMMIT_MINIMUM_BYTES,
            spillable: false,
          });
          let atResume: number | undefined;
          let stale = false,
            calls = 0,
            allocation = false;
          const buffers = new CommitBuffers(grant, (_bytes, owned) => {
              if (owned && allocation) stale = true;
            }),
            backend = new MemoryBackend(),
            lease = await backend.acquire(),
            session = await backend.open(lease);
          const log = new LogAppender(
            session,
            {
              salt: 1,
              chain: 1,
              end: 0,
              lastLsn: 0,
              generation: 0,
              nextSlot: 0,
              acked: { end: 0, chain: 1, lsn: 0 },
              rootSequence: 1,
            },
            true,
            () => undefined,
            buffers,
          );
          const seal = (bytes: Uint8Array) => {
            if (stopped) throw new Error("Retired seal");
            calls++;
            bytes[bytes.length - 1] = stale ? 1 : 0;
            queueMicrotask(() => {
              atResume = log.lastLsn;
            });
          };
          try {
            let expected = 1;
            if (mode === "stream") {
              const result = log.appendStream(
                MAX_FRAME_BODY_BYTES + tail,
                async (out) => {
                  out.fill(42);
                  await Promise.resolve();
                  stale = true;
                },
                seal,
              );
              expected = 2;
              expect(await result).toBe(expected);
              reach.reach("multipart");
              if (tail === 1) reach.reach("one byte final body");
            } else {
              if (mode === "recycle") {
                log.beginRecycle();
                stale = true;
              }
              if (mode === "queue") {
                let entered: () => void = () => undefined;
                const atWrite = new Promise<void>((r) => {
                    entered = r;
                  }),
                  gate = new Promise<void>((r) => {
                    release = r;
                  });
                const append = session.appendLog.bind(session);
                session.appendLog = async (bytes) => {
                  entered();
                  await gate;
                  return append(bytes);
                };
                expect(log.append(new Uint8Array(LOG_QUEUE_BYTES - 512))).toBe(1);
                const result = log.append(new Uint8Array(512), seal);
                expect(result).toBeInstanceOf(Promise);
                await atWrite;
                expect(calls).toBe(0);
                stale = true;
                release();
                expected = 2;
                expect(await result).toBe(expected);
                reach.reach("queue drain");
              } else {
                allocation = mode === "fitting";
                expect(log.append(new Uint8Array(tail), seal)).toBe(expected);
                if (mode === "recycle") {
                  log.abandonRecycle();
                  reach.reach("deferred recycle");
                } else {
                  reach.reach("synchronous fitting");
                  reach.reach("allocation retirement");
                }
              }
            }
            await Promise.resolve();
            expect(calls, "one publication seal per transaction").toBe(1);
            expect(atResume, "no await between seal and LSN assignment").toBe(expected);
            reach.reach("atomic LSN seal");
            await log.flush();
            const scan = scanLog(
              await session.readLog(0, session.logLength()),
              await session.readLogHeaders(),
              { salt: 1, firstLsn: 1, rootSequence: 1 },
            );
            expect(
              scan.frames.at(-1)?.body.at(-1),
              "sealed bytes must precede frame checksum",
            ).toBe(stale ? 1 : 0);
            expect(scan.frames.at(-1)?.lsn).toBe(expected);
            reach.reach("checked bytes");
          } finally {
            release();
            await log.close();
            buffers.close();
            await session.close();
            await lease.release();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("charges drained");
        }
      }),
      { seed, numRuns: 12, examples: [[1], [32]] },
    );
    reach.assertReached();
  });
