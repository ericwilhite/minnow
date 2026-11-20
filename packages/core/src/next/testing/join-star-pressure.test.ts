/** The optional lookup path uses the already-complete resident admission.
 * Sparse Cartesian domains retain native state; pending below-cap requests
 * cancel with no private query holding. No budget/allocator is replaced.
 */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, B9_STAR_PRESSURE_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { cancelPendingAdmission } from "./cancel-admission.js";
import { QueryCancelledError } from "../exec/errors.js";
import { createExecutor } from "../exec/create.js";
import { StarLookups } from "../exec/join-star-lookups.js";
import { watchGrants } from "../write/test-support.js";
import { TaskClock } from "../exec/pipeline.js";
const sql =
  "SELECT a.tier AS tier_a,b.tier AS tier_b,c.label AS label_c,COUNT(*) AS n,SUM(f.v) AS s FROM sf f JOIN sa a ON a.k=f.a JOIN sb b ON b.k=f.b JOIN sc c ON c.k=f.c WHERE a.active AND b.weight>1 AND c.tier='t1' GROUP BY a.tier,b.tier,c.label ORDER BY s DESC,tier_a,tier_b,label_c LIMIT 50";
async function fixture(core: StorageCore, cardinality: number) {
  await core.execute(
    "CREATE TABLE sf(id INTEGER PRIMARY KEY,a TEXT,b TEXT,c TEXT,v DOUBLE PRECISION)",
  );
  for (const name of ["sa", "sb", "sc"]) {
    await core.execute(
      `CREATE TABLE ${name}(k TEXT PRIMARY KEY,tier TEXT,label TEXT,weight DOUBLE PRECISION,active BOOLEAN)`,
    );
    await core.writeBatch(
      "insertBatch",
      name,
      Array.from({ length: cardinality }, (_, id) => ({
        k: `k${String(id)}`,
        tier: "t1",
        label: id % 2 === 0 ? null : "g1",
        weight: 2,
        active: true,
      })),
    );
  }
  await core.writeBatch(
    "insertBatch",
    "sf",
    Array.from({ length: 4097 }, (_, id) => ({
      id,
      a: id % 11 === 0 ? null : `k${String(id % cardinality)}`,
      b: `k${String(Math.floor(id / 3) % cardinality)}`,
      c: `k${String(Math.floor(id / 5) % cardinality)}`,
      v: id % 13 === 0 ? null : ((id % 17) - 8) / 4,
    })),
  );
  await core.harness.path.recycleLog();
  await core.harness.path.quiesce();
}
async function read(core: StorageCore, signal?: AbortSignal) {
  const result = await core.execute(sql, [], signal);
  if (!("batches" in result)) throw new Error("No star rows");
  return oracleRows(result);
}
const bits = (rows: ReturnType<typeof oracleRows>) =>
  rows.map((row) =>
    Object.entries(row).map(([key, v]) => [
      key,
      typeof v === "number" ? Buffer.from(new Float64Array([v]).buffer).toString("hex") : v,
    ]),
  );
it("32 MiB retains the native cancellable minimum with no query holding", async () => {
  const core = new StorageCore(8192),
    governor = new Governor(32 << 20);
  Object.defineProperty(core.harness, "governor", { value: governor });
  await core.open();
  try {
    await fixture(core, 8);
    // The wait is constructed, not assumed: smaller query minimums now fit a free 32 MiB
    // governor, so a competitor leaves 1 MiB, below the star query's complete minimum.
    const competitor = governor.tryAdmit({
      label: "competing holder",
      minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes - (1 << 20),
      spillable: false,
    });
    if (competitor === null) throw new Error("competitor admission");
    const stop = new AbortController(),
      admission = cancelPendingAdmission(governor, stop);
    try {
      await expect(read(core, stop.signal)).rejects.toBeInstanceOf(QueryCancelledError);
      expect(admission.observed.waiting).toBe(true);
      expect(admission.observed.requested).toBeLessThanOrEqual(governor.capBytes);
      expect(admission.observed.heldAboveBaseline).toBeLessThanOrEqual(0);
    } finally {
      admission.restore();
      competitor.close();
    }
  } finally {
    await core.close();
    expect(governor.usedBytes).toBe(0);
  }
}, 60000);
it.each(seedsFor("b9-star-pressure", [0xb9a25, 0xb9a26]))(
  "native64MiB admission, sparse fallback and cancellable waits (seed %i)",
  async (seed) => {
    const reach = createReach("B9 star pressure", B9_STAR_PRESSURE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.boolean(), async (sparse, pinned) => {
        reach.beginHistory();
        const core = new StorageCore(8192),
          governor = new Governor(64 << 20);
        Object.defineProperty(core.harness, "governor", { value: governor });
        await core.open();
        try {
          await fixture(core, sparse ? 1000 : 8);
          const control = createExecutor({ starLookups: false }),
            prepare = vi
              .spyOn(core.executor, "prepare")
              .mockImplementation((plan) => control.prepare(plan));
          let before: Awaited<ReturnType<typeof read>>;
          try {
            before = await read(core);
          } finally {
            prepare.mockRestore();
          }
          const replay = vi.spyOn(StarLookups.prototype, "probe");
          let after: Awaited<ReturnType<typeof read>>;
          try {
            after = await read(core);
            expect(replay).toHaveBeenCalledTimes(sparse ? 0 : 1);
          } finally {
            replay.mockRestore();
          }
          expect(bits(after)).toEqual(bits(before));
          reach.reach(sparse ? "sparse buckets keep native state" : "dense resident sink");
          reach.reach("exact default bits");
          reach.reach("64 MiB cap");
          if (pinned) {
            const stop = new AbortController(),
              competitor = await governor.admit({
                label: "star pinned competitor",
                minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
                spillable: false,
              }),
              admission = cancelPendingAdmission(governor, stop);
            try {
              await expect(read(core, stop.signal)).rejects.toBeInstanceOf(QueryCancelledError);
              expect(admission.observed.waiting).toBe(true);
              expect(admission.observed.heldAboveBaseline).toBeLessThanOrEqual(0);
              expect(admission.observed.requested).toBeLessThanOrEqual(governor.capBytes);
              reach.reach("cancelled native wait");
              reach.reach("no hold while waiting");
            } finally {
              admission.restore();
              competitor.close();
            }
          } else reach.reach("uncontended completion");
          expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
        } finally {
          await core.close();
          expect(governor.usedBytes).toBe(0);
          reach.reach("all credit returned");
        }
      }),
      {
        seed,
        numRuns: 4,
        examples: [
          [false, false],
          [true, false],
          [false, true],
          [true, true],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  },
  180000,
);

it("64 MiB cancellation retains a pending B job's owner grant through settlement", async () => {
  const core = new StorageCore(8192),
    governor = new Governor(64 << 20);
  Object.defineProperty(core.harness, "governor", { value: governor });
  await core.open();
  try {
    await fixture(core, 1);
    const stop = new AbortController(),
      reason = new Error("cancel B prefix"),
      grants: Grant[] = [];
    watchGrants(governor, (grant) => {
      grants.push(grant);
    });
    const tryAdmit = governor.tryAdmit.bind(governor);
    const synchronous = vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
      const grant = tryAdmit(options);
      if (grant) grants.push(grant);
      return grant;
    });
    let witnessed = false,
      pendingSettled = false,
      allocatedAfterAbort = 0,
      sidecarBytes = 0,
      closeBeforeSettled = false,
      fence: Promise<void> | undefined;
    // eslint-disable-next-line @typescript-eslint/unbound-method -- original is explicitly called with the selected lookup
    const original = StarLookups.prototype.probe;
    const probe = vi.spyOn(StarLookups.prototype, "probe").mockImplementation(function (
      this: StarLookups,
      downstream,
    ) {
      const port = original.call(this, downstream);
      return {
        ...port,
        push: (batch) => {
          const due = vi.spyOn(TaskClock.prototype, "due").mockReturnValue(true);
          let result: ReturnType<typeof port.push>;
          try {
            result = port.push(batch);
          } finally {
            due.mockRestore();
          }
          if (!witnessed && result === "blocked") {
            witnessed = true;
            const owner = grants.find((g) =>
              g.label.endsWith(`/join#${String(this.bound.owner.id)}/minimum`),
            );
            if (!owner)
              throw new Error(
                `No actual star owner grant; labels=${grants.map((g) => g.label).join(",")}`,
              );
            const close = owner.close.bind(owner);
            owner.close = () => {
              if (!pendingSettled) closeBeforeSettled = true;
              close();
            };
            const pending = this.ready();
            void pending.then(
              () => {
                pendingSettled = true;
              },
              () => {
                pendingSettled = true;
              },
            );
            sidecarBytes = 8 * this.bound.facts;
            // The driver first receives blocked and installs the ready() borrow fence.
            // Cancellation then runs before the real task yield resumes this job.
            queueMicrotask(() => {
              stop.abort(reason);
              allocatedAfterAbort = owner.allocated;
              const closed = this.closed;
              if (closed instanceof Promise) fence = closed;
            });
          }
          return result;
        },
      };
    });
    try {
      await expect(read(core, stop.signal)).rejects.toBeInstanceOf(QueryCancelledError);
      expect(fence, "pending star close fence").toBeInstanceOf(Promise);
      await fence;
      expect(witnessed).toBe(true);
      expect(allocatedAfterAbort).toBeGreaterThanOrEqual(sidecarBytes);
      expect(pendingSettled).toBe(true);
      expect(closeBeforeSettled, "owner grant retired before pending B work").toBe(false);
    } finally {
      probe.mockRestore();
      synchronous.mockRestore();
    }
  } finally {
    await core.close();
    expect(governor.usedBytes).toBe(0);
    expect(core.spill.bytesInUse).toBe(0);
  }
}, 60000);
