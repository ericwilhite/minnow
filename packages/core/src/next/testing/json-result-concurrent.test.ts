/** One owner's result allocations keep independent charges and obey its shared admission queue. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_RESULT_CONCURRENT_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { yieldTask } from "../exec/pipeline.js";
import { StorageCore } from "./storage-core.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
function gate() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
it.each(seedsFor("next-json-result-concurrent", [0xb6de, 0xb6df]))(
  "isolates concurrent and queued result phases (seed %i)",
  async (seed) => {
    const reach = createReach<string>(
      "JSON concurrent result phases",
      JSON_RESULT_CONCURRENT_REACH,
    );
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.boolean(), fc.boolean(), async (queued, cancel, binary) => {
        reach.beginHistory();
        const a = await new StorageCore().open(),
          b = await new StorageCore().open(),
          governor = new Governor(16 << 20),
          admit = governor.admitGroup.bind(governor),
          waiting = gate();
        const sql = `SELECT ${binary ? "jsonb_build_object" : "json_build_object"}('v',$1::text)::text j`,
          av = "\u0001".repeat(queued ? 1000000 : 300000),
          bv = "\u0001".repeat(queued ? 200000 : 250000);
        const expectedA = (await pg.query<{ j: string }>(sql, [av])).rows[0]?.j,
          expectedB = (await pg.query<{ j: string }>(sql, [bv])).rows[0]?.j;
        const pa = a.prepare(sql),
          pb = b.prepare(sql),
          ra = a.start({ ...pa, execute: (r) => pa.execute({ ...r, governor }) }, [av]);
        let rb: ReturnType<StorageCore["start"]> | undefined,
          nextB: ReturnType<typeof ra.execution.results.next> | undefined,
          markB = false,
          admitted = false,
          heldA = true;
        const groups = vi.spyOn(governor, "admitGroup").mockImplementation((options) => {
          const total = options.grants.reduce((n, g) => n + g.minimumBytes, 0),
            willWait =
              markB &&
              heldA &&
              total > governor.capBytes - governor.usedBytes + (options.idleCredit?.held ?? 0);
          const promise = admit(options);
          if (willWait) {
            void promise.then(
              () => {
                admitted = true;
              },
              () => {
                admitted = true;
              },
            );
            waiting.resolve();
          }
          return promise;
        });
        let frameA: Awaited<ReturnType<typeof ra.execution.results.next>> = null;
        try {
          frameA = await ra.execution.results.next();
          if (!frameA) throw new Error("Expected first result");
          const dataA = frameA.payload.columns[0];
          if (dataA?.form !== "flat" || dataA.data.layout !== "varlen")
            throw new Error("Expected JSON text");
          expect(new TextDecoder().decode(dataA.data.bytes)).toBe(expectedA);
          reach.reach("exact PostgreSQL");
          markB = true;
          rb = b.start({ ...pb, execute: (r) => pb.execute({ ...r, governor }) }, [bv]);
          nextB = rb.execution.results.next();
          void nextB.catch(() => undefined);
          reach.reach("two queries one governor");
          if (queued) {
            await Promise.race([
              waiting.promise,
              rb.execution.executed.then(() => {
                throw new Error("Follower did not reach its admission wait");
              }),
            ]);
            await yieldTask();
            await yieldTask();
            expect(admitted).toBe(false);
            reach.reach("queued native admission");
            if (cancel) {
              rb.execution.cancel();
              await expect(nextB).rejects.toMatchObject({ sqlState: "57014" });
              await Promise.allSettled([rb.execution.executed, rb.execution.delivered]);
              reach.reach("cancel queued follower");
            }
            expect(new TextDecoder().decode(dataA.data.bytes)).toBe(expectedA);
            reach.reach("held opposite credit");
            heldA = false;
            frameA.release();
            await ra.execution.executed;
            await ra.execution.delivered;
          }
          if (!cancel || !queued) {
            const frameB = await nextB;
            if (!frameB) throw new Error("Expected follower result");
            const dataB = frameB.payload.columns[0];
            if (dataB?.form !== "flat" || dataB.data.layout !== "varlen")
              throw new Error("Expected JSON text");
            expect(new TextDecoder().decode(dataB.data.bytes)).toBe(expectedB);
            expect(dataB.data.bytes.buffer).not.toBe(dataA.data.bytes.buffer);
            if (cancel) {
              rb.execution.cancel();
              await Promise.allSettled([rb.execution.executed, rb.execution.delivered]);
              reach.reach("cancel concurrent delivery");
            } else {
              frameB.release();
              await rb.execution.executed;
              await rb.execution.delivered;
              reach.reach(queued ? "resume queued follower" : "concurrent delivery");
            }
            if (!queued) {
              expect(new TextDecoder().decode(dataA.data.bytes)).toBe(expectedA);
              heldA = false;
              frameA.release();
              await ra.execution.executed;
              await ra.execution.delivered;
              reach.reach("held opposite credit");
            }
          }
          expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
        } finally {
          heldA = false;
          frameA?.release();
          ra.execution.cancel();
          rb?.execution.cancel();
          await Promise.allSettled([
            ra.execution.executed,
            ra.execution.delivered,
            ...(rb ? [rb.execution.executed, rb.execution.delivered] : []),
            ...(nextB ? [nextB] : []),
          ]);
          ra.release();
          rb?.release();
          groups.mockRestore();
          expect(a.spill.bytesInUse).toBe(0);
          expect(b.spill.bytesInUse).toBe(0);
          await a.close();
          await b.close();
        }
        expect(governor.usedBytes).toBe(0);
        reach.reach("zero resources");
      }),
      {
        seed,
        numRuns: 6,
        examples: [
          [true, false, false],
          [true, true, true],
          [false, false, true],
          [false, true, false],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
