/** A paused phase owns no private credit. Native FIFO service adopts pooled idle
 * credit and splits the whole minimum before callbacks or promise continuations. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { Governor, type Grant } from "./governor.js";
const M = 1 << 20;
async function deadline<T>(work: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("FIFO phase progress stalled")), 1000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
it.each(seedsFor("governor-pooled-idle", [0xb4c3, 0xb4c4]))(
  "adopts idle phase credit for earlier fresh heads and concurrent phases (seed %i)",
  async (seed) => {
    const reach = createReach("pooled idle", [
      "fresh head",
      "several phases",
      "8 MiB",
      "transfer wakes",
      "cancel",
      "validation",
      "zero",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 4 }),
        fc.integer({ min: 1, max: 4 }),
        fc.array(fc.integer({ min: 4, max: 8 }), { minLength: 4, maxLength: 4 }),
        async (count, quarter, minima) => {
          reach.beginHistory();
          for (const fresh of [false, true]) {
            const governor = new Governor(8 * M),
              controller = new AbortController();
            const credit = (quarter * M) / 4,
              totalCredit = credit * count;
            const old = await governor.admitGroup({
              label: "old graph",
              includesProgress: true,
              grants: [
                ...Array.from({ length: count }, (_, i) => ({
                  label: `producer ${String(i)}`,
                  minimumBytes:
                    Math.floor((8 * M - totalCredit) / count) +
                    (i === 0 ? (8 * M - totalCredit) % count : 0),
                  spillable: false,
                })),
                ...Array.from({ length: count }, (_, i) => ({
                  label: `result ${String(i)}`,
                  minimumBytes: credit,
                  spillable: false,
                })),
              ],
            });
            const [pool] = await governor.admitGroup({
              label: "pool",
              grants: [{ label: "shared idle", minimumBytes: 0, spillable: false }],
            });
            if (!pool) throw new Error("pool missing");
            const acquired: Grant[] = [];
            let freshServed = false;
            const head = fresh
              ? governor
                  .admit({
                    label: "earlier fresh head",
                    minimumBytes: 7 * M,
                    spillable: false,
                    signal: controller.signal,
                  })
                  .then((g) => {
                    acquired.push(g);
                    freshServed = true;
                    g.close();
                    reach.reach("fresh head");
                  })
              : Promise.resolve();
            let workers: Array<Promise<void>> = [];
            try {
              await governor.admitGroup({
                label: "register pooled credit",
                idleCredit: pool,
                grants: [],
              });
              for (const producer of old.slice(0, count)) producer.close();
              for (const grant of old.slice(count)) {
                expect(governor.transferCredit(grant, pool, grant.held)).toBe(true);
                grant.close();
              }
              expect(old.every((grant) => grant.held === 0)).toBe(true);
              if (fresh) {
                // No I/O or reclaim callbacks exist here: FIFO drain and native adoption
                // are synchronous, followed only by the admission promise continuations.
                // Detect an invisible pool by this assertion, not the deadline backstop.
                for (let turn = 0; turn < 4; turn++) await Promise.resolve();
                expect(freshServed).toBe(true);
              }
              workers = Array.from({ length: count }, (_, i) =>
                governor
                  .admitGroup({
                    label: `restore ${String(i)}`,
                    includesProgress: true,
                    idleCredit: pool,
                    signal: controller.signal,
                    grants: [
                      {
                        label: "state",
                        minimumBytes: ((minima[i] ?? 8) - 2) * M,
                        spillable: false,
                      },
                      { label: "I/O + downstream", minimumBytes: 2 * M, spillable: false },
                    ],
                  })
                  .then((parts) => {
                    acquired.push(...parts);
                    expect(parts.reduce((n, g) => n + g.held, 0)).toBe((minima[i] ?? 8) * M);
                    for (const g of parts) g.close();
                  }),
              );
              await deadline(Promise.all([head, ...workers]));
              expect(governor.peakBytes).toBeLessThanOrEqual(8 * M);
              reach.reach("several phases");
              reach.reach("8 MiB");
              reach.reach("transfer wakes");
            } finally {
              controller.abort(new Error("cleanup"));
              await Promise.allSettled([head, ...workers]);
              for (const g of [...old, ...acquired]) g.close();
              pool.close();
            }
            expect(governor.usedBytes).toBe(0);
            reach.reach("zero");
          }
          const gov = new Governor(8 * M),
            [pool] = await gov.admitGroup({
              label: "pool",
              grants: [{ label: "p", minimumBytes: 2 * M, spillable: false }],
            });
          if (!pool) throw new Error("pool missing");
          const cap = await gov.admitGroup({
            label: "holder",
            includesProgress: true,
            grants: [{ label: "h", minimumBytes: 6 * M, spillable: false }],
          });
          const cancel = new AbortController();
          const waiting = gov.admitGroup({
            label: "cancel",
            includesProgress: true,
            idleCredit: pool,
            signal: cancel.signal,
            grants: [{ label: "r", minimumBytes: 8 * M, spillable: false }],
          });
          cancel.abort(new Error("cancel phase"));
          await expect(waiting).rejects.toThrow("cancel phase");
          reach.reach("cancel");
          await expect(
            gov.admitGroup({
              label: "invalid",
              idleCredit: pool,
              grants: [{ label: "r", minimumBytes: -1, spillable: false }],
            }),
          ).rejects.toThrow();
          expect(gov.usedBytes).toBe(8 * M);
          reach.reach("validation");
          for (const g of cap) g.close();
          pool.close();
          expect(gov.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 8, examples: [[2, 4, [8, 8, 8, 8]]] },
    );
    reach.assertReached();
  },
  30000,
);

it("serves ordinary and loan heads even when idle credit occupies the whole cap", async () => {
  const gov = new Governor(8 * M),
    [pool] = await gov.admitGroup({
      label: "whole idle",
      includesProgress: true,
      grants: [{ label: "p", minimumBytes: 8 * M, spillable: false }],
    });
  if (!pool) throw new Error("pool missing");
  await gov.admitGroup({ label: "register", idleCredit: pool, grants: [] });
  const fresh = await deadline(
    gov.admit({ label: "fresh", minimumBytes: 6 * M, spillable: false }),
  );
  expect(fresh.held).toBe(6 * M);
  expect(pool.held).toBe(2 * M);
  expect(gov.usedBytes).toBe(8 * M);
  const loan = await deadline(fresh.borrow(M));
  expect(fresh.loaned).toBe(M);
  expect(gov.usedBytes).toBe(8 * M);
  loan.repay();
  fresh.close();
  pool.close();
  expect(gov.usedBytes).toBe(0);
});

it("rechecks pooled credit after an eviction callback closes its source", async () => {
  const gov = new Governor(8 * M),
    parts = await gov.admitGroup({
      label: "occupied",
      includesProgress: true,
      grants: [
        { label: "pool", minimumBytes: 4 * M, spillable: false },
        { label: "pinned", minimumBytes: 4 * M, spillable: false },
      ],
    });
  const [pool, pinned] = parts;
  if (!pool || !pinned) throw new Error("parts missing");
  await gov.admitGroup({ label: "register", idleCredit: pool, grants: [] });
  gov.registerCache({
    get evictableBytes() {
      return pool.held;
    },
    evict() {
      const bytes = pool.held;
      pool.close();
      return bytes;
    },
  });
  let served = false;
  const next = gov
    .admitGroup({
      label: "fresh full",
      includesProgress: true,
      grants: [{ label: "next", minimumBytes: 8 * M, spillable: false }],
    })
    .then((grants) => {
      served = true;
      for (const g of grants) g.close();
    });
  await Promise.resolve();
  expect(served).toBe(false);
  expect(gov.usedBytes).toBe(4 * M);
  expect(gov.peakBytes).toBe(8 * M);
  pinned.close();
  await deadline(next);
  expect(gov.usedBytes).toBe(0);
});
