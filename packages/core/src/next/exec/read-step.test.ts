/** Whole minima remove hold-and-wait, including when every execution keeps pinned input. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { ReadDeniedError, type SpaceReader } from "../access/space.js";
import { Governor } from "../memory/governor.js";
import { reclaimingReader } from "./reclaim-reader.js";
import { ReservedGrant } from "./admission.js";
import { StepLoans } from "../memory/step-loans.js";
import { ReadStep } from "./read-step.js";
import { WorkingSet } from "../memory/working-set.js";
import { yieldTask } from "../task.js";

it.each(seedsFor("next-bounded-read-steps", [0xc411, 0xc412]))(
  "two or more admitted executions pin input and request more without shared headroom (seed %i)",
  async (seed) => {
    const reach = createReach("bounded read step", [
      "two",
      "more",
      "pinned",
      "no-headroom",
      "read-more",
      "downstream",
      "all-released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 4 }),
        fc.integer({ min: 1, max: 4 }),
        fc.integer({ min: 2, max: 6 }),
        async (count, units, retained) => {
          reach.beginHistory();
          const bytes = units * 65536,
            minimum = bytes * (retained + 2),
            gov = new Governor(count * minimum),
            executions: WorkingSet[] = [];
          const pages: SpaceReader = {
            peek: () => null,
            async read(_refs, options) {
              if (!options.budget.allocate(bytes)) throw new ReadDeniedError(bytes);
              const buffer = new Uint8Array(bytes);
              buffer[0] = 42;
              let freed = false;
              return {
                bytes: [buffer],
                release() {
                  if (!freed) {
                    freed = true;
                    options.budget.free(bytes);
                  }
                },
              };
            },
          };
          try {
            for (let k = 0; k < count; k++)
              executions.push(await WorkingSet.admit(gov, [bytes, minimum - bytes]));
            expect(gov.usedBytes).toBe(gov.capBytes);
            reach.reach("no-headroom");
            await Promise.all(
              executions.map(async (execution) => {
                const [io, state] = execution.parts;
                if (!io || !state) throw new Error("missing fixed part");
                const step = new ReadStep(io, new AbortController().signal),
                  reader = step.reader(pages),
                  leases = [];
                try {
                  for (let k = 0; k < retained; k++)
                    leases.push(await reader.read([], { budget: state }));
                  expect(state.allocated).toBe(retained * bytes);
                  expect(state.allocated).toBeLessThanOrEqual(gov.capBytes / 2);
                  reach.reach("pinned");
                  leases.push(await reader.read([], { budget: state }));
                  reach.reach("read-more");
                  // The input is consumed before asking for downstream scratch. Its lease may not
                  // steal that scratch's separately reserved part of the whole minimum.
                  for (const lease of leases) {
                    expect(lease.bytes[0]?.[0]).toBe(42);
                    lease.release();
                  }
                  step.beforeDownstream();
                  expect(io.allocated, "progress credit must be empty at the boundary").toBe(0);
                  expect(
                    state.allocate((retained + 1) * bytes),
                    "downstream must finish at its minimum",
                  ).toBe(true);
                  state.free((retained + 1) * bytes);
                  reach.reach("downstream");
                } finally {
                  for (const lease of leases) lease.release();
                  await step.closed;
                }
              }),
            );
            reach.reach(count === 2 ? "two" : "more");
          } finally {
            for (const execution of executions) execution.close();
            expect(gov.usedBytes).toBe(0);
            expect(gov.peakBytes).toBeLessThanOrEqual(gov.capBytes);
            reach.reach("all-released");
          }
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [2, 4, 6],
          [4, 2, 3],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-read-step-cancellation", [0xc413, 0xc414]))(
  "cancels queued reads immediately and drains the private lane before reuse (seed %i)",
  async (seed) => {
    const reach = createReach("read step cancellation", [
      "active",
      "queued",
      "cancelled",
      "drained",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 5 }), async (count) => {
        reach.beginHistory();
        const gov = new Governor(1 << 20),
          execution = await WorkingSet.admit(gov, [65536, 65536]),
          [io, state] = execution.parts;
        if (!io || !state) throw new Error("missing minimum");
        const controller = new AbortController(),
          step = new ReadStep(io, controller.signal);
        let release!: () => void,
          entered!: () => void,
          calls = 0;
        const gate = new Promise<void>((resolve) => {
          release = resolve;
        });
        const started = new Promise<void>((resolve) => {
          entered = resolve;
        });
        const pages: SpaceReader = {
          peek: () => null,
          async read(_refs, options) {
            calls++;
            expect(options.budget.allocate(65536)).toBe(true);
            entered();
            await gate;
            let closed = false;
            return {
              bytes: [new Uint8Array(65536)],
              release() {
                if (!closed) {
                  closed = true;
                  options.budget.free(65536);
                }
              },
            };
          },
        };
        const pending = Array.from({ length: count }, () =>
          step.reader(pages).read([], { budget: state }),
        );
        void Promise.allSettled(pending);
        try {
          await started;
          expect(calls).toBe(1);
          reach.reach("active");
          reach.reach("queued");
          const reason = new Error("cancel bounded read");
          controller.abort(reason);
          for (const read of pending) await expect(read).rejects.toBe(reason);
          expect(io.allocated).toBe(65536);
          expect(step.busy).toBe(true);
          reach.reach("cancelled");
          release();
          await step.closed;
          expect(calls).toBe(1);
          expect(io.allocated).toBe(0);
          expect(state.allocated).toBe(0);
          reach.reach("drained");
        } finally {
          release();
          await step.closed;
          execution.close();
        }
        expect(gov.usedBytes).toBe(0);
      }),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  },
);

it("queues whole minima FIFO, holds nothing while waiting, and cancels admission", async () => {
  const gov = new Governor(1024 * 1024),
    first = await WorkingSet.admit(gov, [600000, 400000]);
  const cancel = new AbortController();
  const order: number[] = [];
  const second = WorkingSet.admit(gov, [300000, 700000]).then((value) => {
    order.push(2);
    return value;
  });
  const cancelled = WorkingSet.admit(gov, [100000, 900000], cancel.signal);
  const third = WorkingSet.admit(gov, [200000, 800000]).then((value) => {
    order.push(3);
    return value;
  });
  const reason = new Error("cancel admission");
  cancel.abort(reason);
  await expect(cancelled).rejects.toBe(reason);
  expect(gov.usedBytes).toBe(1000000);
  expect(order).toEqual([]);
  first.close();
  const next = await second;
  expect(order).toEqual([2]);
  expect(gov.usedBytes).toBe(1000000);
  next.close();
  const last = await third;
  expect(order).toEqual([2, 3]);
  last.close();
  expect(gov.usedBytes).toBe(0);
  await expect(WorkingSet.admit(gov, [gov.capBytes, 1])).rejects.toMatchObject({
    code: "MINNOW_MEMORY_ADMISSION",
    capacity: gov.capBytes,
  });
});

const MiB = 1024 * 1024;
it.each(seedsFor("next-memory-pinned-pressure", [0xc403, 0xc404]))(
  "waits for temporary pinned holders or cancellation, without a final read refusal (seed %i)",
  async (seed) => {
    const reach = createReach("pinned read pressure", [
      "multiple holders",
      "waited",
      "released",
      "cancelled",
      "no leak",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 4 }),
        fc.integer({ min: 1, max: 1024 }),
        fc.boolean(),
        async (count, bytes, cancel) => {
          reach.beginHistory();
          const gov = new Governor(8 * MiB),
            grants = [];
          let remaining = gov.capBytes - gov.reserveBytes;
          for (let k = 0; k < count; k++) {
            const size = Math.floor(remaining / (count - k)),
              grant = await gov.admit({
                label: `pinned${String(k)}`,
                minimumBytes: size,
                spillable: false,
              });
            remaining -= size;
            expect(grant.allocate(size)).toBe(true);
            grants.push(grant);
          }
          const reader = await gov.admit({ label: "reader", minimumBytes: 0, spillable: true }),
            controller = new AbortController();
          const loanHolder = grants[0];
          if (!loanHolder) throw new Error("missing pinned holder");
          const loan = await loanHolder.borrow(gov.reserveBytes);
          expect(loanHolder.allocate(loan.bytes)).toBe(true);
          let repaid = false;
          let settled = false;
          const pages = reclaimingReader(
            {
              peek: () => null,
              async read(_refs, options) {
                if (!options.budget.allocate(bytes)) throw new ReadDeniedError(bytes);
                let freed = false;
                return {
                  bytes: [new Uint8Array(bytes)],
                  release() {
                    if (!freed) {
                      freed = true;
                      options.budget.free(bytes);
                    }
                  },
                };
              },
            },
            reader,
            controller.signal,
          );
          const pending = pages.read([], { budget: reader, signal: controller.signal }).then(
            (value) => {
              settled = true;
              return value;
            },
            (error: unknown) => {
              settled = true;
              throw error;
            },
          );
          void pending.catch(() => undefined);
          try {
            for (let k = 0; k < 16; k++) await Promise.resolve();
            expect(settled, "full-cap pinned pressure must wait").toBe(false);
            reach.reach("waited");
            if (count > 1) reach.reach("multiple holders");
            if (cancel) {
              const reason = new Error("cancel pinned wait");
              controller.abort(reason);
              await expect(pending).rejects.toBe(reason);
              reach.reach("cancelled");
            } else {
              loanHolder.free(loan.bytes);
              loan.repay();
              repaid = true;
              for (const grant of grants) grant.close();
              const lease = await pending;
              expect(lease.bytes[0]?.length).toBe(bytes);
              lease.release();
              reach.reach("released");
            }
          } finally {
            controller.abort(new Error("test cleanup"));
            if (!repaid) {
              loanHolder.free(loan.bytes);
              loan.repay();
            }
            await Promise.allSettled([pending]);
            reader.close();
            for (const grant of grants) grant.close();
            expect(gov.usedBytes).toBe(0);
            reach.reach("no leak");
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [2, 1, false],
          [4, 1024, true],
        ],
      },
    );
    reach.assertReached();
  },
);

it.each(seedsFor("next-retained-read-minimum", [0xc415, 0xc416]))(
  "keeps a source minimum charged until retained leases return after close (seed %i)",
  async (seed) => {
    const reach = createReach("retained read minimum", [
      "one",
      "many",
      "closed-pinned",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 4 }),
        fc.integer({ min: 1, max: 4 }),
        async (count, units) => {
          reach.beginHistory();
          const bytes = units * 65536,
            gov = new Governor(4 << 20),
            extra = await gov.admit({ label: "source growth", minimumBytes: 0, spillable: true }),
            fixedParts = await gov.admitGroup({
              label: "whole source",
              includesProgress: true,
              grants: [{ label: "whole source", minimumBytes: bytes * count, spillable: false }],
            });
          const fixed = fixedParts[0];
          if (!fixed) throw new Error("missing source minimum");
          const source = new ReservedGrant(
            fixed,
            extra,
            bytes * count,
            new StepLoans(fixed, 0, "source"),
            true,
          );
          expect(source.allocate(bytes * count)).toBe(true);
          source.close();
          expect(source.allocated).toBe(bytes * count);
          expect(source.held).toBe(bytes * count);
          expect(gov.usedBytes).toBe(bytes * count);
          expect(source.allocate(1)).toBe(false);
          reach.reach("closed-pinned");
          for (let k = 0; k < count; k++) {
            source.free(bytes);
            if (k < count - 1) expect(source.held).toBe(bytes * count);
          }
          expect(source.held).toBe(0);
          expect(source.allocated).toBe(0);
          fixed.close();
          expect(gov.usedBytes).toBe(0);
          reach.reach(count === 1 ? "one" : "many");
          reach.reach("released");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [1, 1],
          [4, 4],
        ],
      },
    );
    reach.assertReached();
  },
);

it.each(seedsFor("next-read-step-partial-room", [0xc437, 0xc438]))(
  "waits with no partial holding when one part fits but the whole minimum does not (seed %i)",
  async (seed) => {
    const reach = createReach("read minimum partial room", [
      "partial-room",
      "whole-wait",
      "cancelled",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 512 * 1024, max: 768 * 1024 }),
        fc.integer({ min: 1, max: 128 * 1024 }),
        async (pinned, first) => {
          reach.beginHistory();
          const governor = new Governor(1 << 20);
          const holding = await WorkingSet.admit(governor, [pinned]);
          const holder = holding.parts[0];
          if (!holder) throw new Error("missing pinned minimum");
          expect(holder.allocate(pinned)).toBe(true);
          expect(first).toBeLessThan(governor.capBytes - pinned);
          reach.reach("partial-room");
          const cancel = new AbortController();
          const pending = WorkingSet.admit(governor, [first, 768 * 1024 - first], cancel.signal);
          void pending.catch(() => undefined);
          const reason = new Error("cancel whole minimum");
          let completed = false;
          void pending.then(
            () => {
              completed = true;
            },
            () => undefined,
          );
          try {
            await yieldTask();
            expect(completed).toBe(false);
            expect(governor.usedBytes, "a waiter retained a partial minimum").toBe(pinned);
            reach.reach("whole-wait");
            cancel.abort(reason);
            await expect(pending).rejects.toBe(reason);
            expect(governor.usedBytes).toBe(pinned);
            reach.reach("cancelled");
          } finally {
            cancel.abort(reason);
            const settled = await Promise.allSettled([pending]);
            for (const result of settled) if (result.status === "fulfilled") result.value.close();
            holding.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("released");
        },
      ),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  },
);
