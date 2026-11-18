/** Recovery's frame/key workspace is protected while reading, and retired before native ready. */
import fc from "fast-check";
import { expect, it, vi, onTestFinished } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_RECOVERY_CREDIT_REACH } from "../../testing/reach.js";
import { RECOVERY_SCAN_BYTES } from "../format/log-frame.js";
import { Governor } from "../memory/governor.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { WriteCredits } from "../write/credits.js";
import { yieldTask } from "../task.js";
import { MinnowDatabase } from "../owner/database.js";
import { openNativeOwner } from "../owner/native-boot.js";
import { ProtocolClient } from "../owner/client.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";

for (const seed of seedsFor("next-native-recovery-credit", [47223, 47224]))
  it(`reopens without holding completed scan credit ahead of a query (seed ${String(seed)})`, async () => {
    const reach = createReach("native recovery credit retirement", NATIVE_RECOVERY_CREDIT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(32, 64),
        fc.bigInt({ min: -(1n << 60n), max: 1n << 60n }),
        fc.constantFrom("", "水", "🙂", "plain"),
        async (cap, value, text) => {
          reach.beginHistory();
          reach.reach(cap === 32 ? "32 MiB" : "64 MiB");
          const backend = new MemoryBackend(),
            stop = new AbortController();
          let credits: WriteCredits | undefined,
            reopening = false,
            readChecked = false,
            observe = false,
            denied = 0;
          let db: Awaited<ReturnType<typeof MinnowDatabase.open>> | undefined,
            owner: Awaited<ReturnType<typeof openNativeOwner>> | undefined,
            client: MinnowDatabaseClient | undefined,
            channel: MessageChannel | undefined;
          const reads = new Set<Promise<Uint8Array>>();
          let closing: Promise<void> | undefined;
          const opened = WriteCredits.open.bind(WriteCredits),
            originalOpen = backend.open.bind(backend),
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Called with its original governor below.
            admit = Governor.prototype.admitGroup;
          const capture = vi.spyOn(WriteCredits, "open").mockImplementation(async (...args) => {
            credits = await opened(...args);
            return credits;
          });
          const sessions = vi.spyOn(backend, "open").mockImplementation(async (...args) => {
            const session = await originalOpen(...args);
            if (!reopening) return session;
            const read = session.readLog.bind(session);
            session.readLog = (offset, length) => {
              if (readChecked || !credits) return read(offset, length);
              readChecked = true;
              const current = credits;
              const job = (async () => {
                const working = current.minimum.rows - current.rows.allocated;
                expect(working).toBeGreaterThan(0);
                const free =
                  current.governor.capBytes -
                  current.governor.reserveBytes -
                  current.governor.usedBytes;
                const holder = await current.governor.admit({
                  label: "recovery in-flight reader",
                  minimumBytes: free,
                  spillable: true,
                });
                const buffers: { competitor?: Uint8Array; workspace?: Uint8Array } = {};
                let owned = false;
                try {
                  expect(holder.allocate(free)).toBe(true);
                  buffers.competitor = new Uint8Array(free).fill(29);
                  reach.reach("recovery competitor owns real bytes");
                  owned = current.rows.allocate(working);
                  expect(
                    owned,
                    "recovery's key/row workspace must coexist with its still-borrowed frame",
                  ).toBe(true);
                  buffers.workspace = new Uint8Array(working).fill(31);
                  await yieldTask();
                  expect(buffers.workspace.at(-1)).toBe(31);
                  expect(buffers.competitor.at(-1)).toBe(29);
                  reach.reach("recovery read and working buffers coexist");
                  return await read(offset, length);
                } finally {
                  delete buffers.workspace;
                  if (owned) current.rows.free(working);
                  delete buffers.competitor;
                  holder.free(holder.allocated);
                  holder.close();
                }
              })();
              reads.add(job);
              void job.then(
                () => reads.delete(job),
                () => reads.delete(job),
              );
              return job;
            };
            return session;
          });
          const growth = vi.spyOn(Governor.prototype, "admitGroup").mockImplementation(function (
            this: Governor,
            options,
          ) {
            const pending = admit.call(this, options);
            if (
              !observe ||
              !options.label.startsWith("query#") ||
              !options.label.endsWith("/fixed-buffers")
            )
              return pending;
            return (async () => {
              const settlement = { complete: false };
              void pending.then(
                () => {
                  settlement.complete = true;
                },
                () => {
                  settlement.complete = true;
                },
              );
              await yieldTask();
              if (!settlement.complete) {
                denied++;
                const row = credits?.rows;
                if (row) {
                  const unused = Math.max(0, row.held - row.allocated - row.loaned);
                  if (unused > 0) row.release(Math.min(RECOVERY_SCAN_BYTES, unused));
                }
              }
              return await pending;
            })();
          });
          const close = () =>
            (closing ??= (async () => {
              observe = false;
              stop.abort(new Error("native recovery history ended"));
              await Promise.allSettled(reads);
              await client?.close();
              await owner?.close();
              await db?.close();
              channel?.port1.close();
              channel?.port2.close();
              capture.mockRestore();
              sessions.mockRestore();
              growth.mockRestore();
            })());
          onTestFinished(close);
          try {
            const options = { backend, memoryLimitBytes: cap << 20, signal: stop.signal };
            db = await MinnowDatabase.open(options);
            await db.execute("CREATE TABLE t(id INT PRIMARY KEY,n BIGINT,v TEXT)");
            await db.execute("INSERT INTO t VALUES(1,$1,$2)", [value, text]);
            await db.transaction(async (tx) => {
              await tx.execute("INSERT INTO t VALUES(2,$1,$2)", [value + 1n, text]);
            });
            await db.close();
            db = undefined;
            reopening = true;
            owner = await openNativeOwner(options);
            expect(readChecked).toBe(true);
            channel = new MessageChannel();
            const raw = new ProtocolClient();
            owner.accept(channel.port2);
            await raw.connect(channel.port1);
            client = new MinnowDatabaseClient(raw, { ready: Promise.resolve() });
            observe = true;
            expect((await client.query("SELECT n,v FROM t ORDER BY id")).tuples()).toEqual([
              [value, text],
              [value + 1n, text],
            ]);
            observe = false;
            expect(
              denied,
              "an admitted query must not wait for a completed recovery scan's forecast",
            ).toBe(0);
            reach.reach("query admitted without completed scan credit");
            if (!credits) throw new Error("No native write credits");
            expect(
              credits.rows.held,
              "ready must retire only the scan portion of its initial row minimum",
            ).toBeLessThanOrEqual(
              Math.max(
                credits.minimum.rows - RECOVERY_SCAN_BYTES,
                credits.rows.allocated + credits.rows.loaned,
              ),
            );
            reach.reach("scan forecast retired before ready");
            reach.reach("exact reopened rows");
            await client.execute("INSERT INTO t VALUES(3,$1,$2)", [value + 2n, text]);
            expect((await client.query("SELECT count(*) FROM t")).values(0)).toEqual([3]);
            reach.reach("continued native writes");
          } finally {
            await close();
          }
          expect(owner.governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [32, 9007199254740993n, "水"],
          [64, -9007199254740993n, ""],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  }, 60000);
