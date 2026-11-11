/** IndexCursor consumes bounded prefixes; pending writes outlive abort until drained. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { OutputArena } from "./kernels/vectors.js";
import { IndexOrderSource } from "./key-order.js";
import { createExecutor } from "./create.js";
import { UnsupportedPlanError } from "../errors.js";
import type { PhysicalPlan } from "../ir/ir.js";

it.each(seedsFor("next-key-order-prefix", [0xe103, 0xe104]))(
  "bounds cursor reads and closes once (seed %i)",
  async (seed) => {
    const reach = createReach("index prefix", [
      "zero",
      "offset",
      "offset-past-end",
      "multiple-batches",
      "closed",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 6000 }),
        fc.integer({ min: 0, max: 6000 }),
        async (limit, offset) => {
          reach.beginHistory();
          const governor = new Governor(8 << 20),
            grant = await governor.admit({ label: "test", minimumBytes: 1 << 20, spillable: true });
          const arena = new OutputArena(grant, 2048);
          let at = 0,
            read = 0,
            skips = 0,
            closes = 0,
            batches = 0;
          const source = new IndexOrderSource(
            {
              skip(count) {
                skips++;
                const n = Math.min(count, 5000 - at);
                at += n;
                return n;
              },
              next(out) {
                const count = Math.min(out.length, 5000 - at);
                for (let i = 0; i < count; i++) out[i] = at++;
                read += count;
                return count;
              },
              close() {
                closes++;
              },
            },
            arena.flat({ kind: "locator", tableId: 1 }).vector,
            new AbortController().signal,
            limit,
            offset,
          );
          try {
            const output: number[] = [];
            for (;;) {
              const item = await source.next();
              if (!item) break;
              batches++;
              const v = item.batch.columns[0];
              if (v?.form !== "flat" || v.data.layout !== "f64") throw new Error("locator output");
              output.push(...v.data.values.subarray(0, item.batch.length));
            }
            expect(output).toEqual(
              Array.from({ length: 5000 }, (_, i) => i).slice(offset, offset + limit),
            );
            expect(read, "only surviving locators are materialized").toBe(output.length);
            expect(skips).toBe(limit > 0 && offset > 0 ? 1 : 0);
            source.abort();
            expect(closes).toBe(1);
            if (limit === 0) reach.reach("zero");
            if (offset > 0 && limit > 0) reach.reach("offset");
            if (offset >= 5000 && limit > 0) reach.reach("offset-past-end");
            if (batches > 1) reach.reach("multiple-batches");
            reach.reach("closed");
          } finally {
            source.abort();
            await source.closed;
            arena.close();
            grant.close();
            expect(governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 13,
        examples: [
          [0, 3],
          [4097, 0],
          [100, 3],
          [1, 6000],
        ],
      },
    );
    reach.assertReached();
  },
);

it.each(["next", "skip"] as const)(
  "drains a pending cursor %s before closing output storage",
  async (method) => {
    const governor = new Governor(8 << 20),
      grant = await governor.admit({ label: "drain", minimumBytes: 1 << 20, spillable: true }),
      arena = new OutputArena(grant, 2048);
    let entered: (() => void) | undefined,
      release: (() => void) | undefined,
      closes = 0;
    const arrived = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const source = new IndexOrderSource(
      {
        skip(count) {
          entered?.();
          return new Promise<number>((resolve) => {
            release = () => resolve(count);
          });
        },
        next(out) {
          entered?.();
          return new Promise<number>((resolve) => {
            release = () => {
              out[0] = 42;
              resolve(1);
            };
          });
        },
        close() {
          closes++;
        },
      },
      arena.flat({ kind: "locator", tableId: 1 }).vector,
      new AbortController().signal,
      1,
      method === "skip" ? 17 : 0,
    );
    const pending = source.next();
    try {
      await arrived;
      source.abort();
      let drained = false;
      const closed = source.closed.then(() => {
        drained = true;
      });
      await Promise.resolve();
      expect(drained, "cursor writes must drain before arena disposal").toBe(false);
      expect(closes).toBe(1);
      release?.();
      expect(await pending).toBeNull();
      await closed;
      expect(drained).toBe(true);
    } finally {
      release?.();
      await Promise.allSettled([pending, source.closed]);
      source.abort();
      arena.close();
      grant.close();
      expect(governor.usedBytes).toBe(0);
    }
  },
);

it.each(seedsFor("next-key-order-held-floor", [0xe105, 0xe106]))(
  "retains restore admission under a competing holder (seed %i)",
  async (seed) => {
    const { scopedGrant } = await import("./late-fetch.js");
    const reach = createReach("restore floor", [
      "fixed-retained",
      "active-reclaim",
      "loan-retained",
      "competing-holder",
      "next-batch",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 16 }),
        fc.integer({ min: 64, max: 4096 }),
        fc.integer({ min: 0, max: 4 }),
        fc.boolean(),
        fc.boolean(),
        async (units, bytes, growth, reclaim, borrow) => {
          reach.beginHistory();
          const governor = new Governor(8 << 20),
            minimum = units * 65536;
          const restore = await governor.admit({
              label: "restore",
              minimumBytes: minimum,
              spillable: true,
            }),
            other = await governor.admit({ label: "other", minimumBytes: 0, spillable: true });
          const stage = scopedGrant(restore, minimum);
          try {
            expect(stage.allocate(bytes)).toBe(true);
            expect(stage.tryGrow(growth * 65536)).toBe(true);
            stage.free(bytes);
            const loan = borrow ? await stage.borrow(bytes) : null;
            if (reclaim) {
              // Sort.reclaim releases unused holdings before its final close. The next
              // cursor batch still needs the admission floor even when this sort spilled.
              stage.release(stage.held - stage.allocated - stage.loaned);
              expect(restore.held - restore.loaned).toBe(minimum);
              reach.reach("active-reclaim");
            }
            if (loan !== null) {
              loan.repay();
              expect(restore.held).toBe(minimum + (reclaim ? 0 : growth * 65536));
              reach.reach("loan-retained");
            }
            stage.close();
            expect(restore.held).toBe(minimum);
            expect(stage.allocate(bytes)).toBe(false);
            reach.reach("fixed-retained");
            expect(
              other.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
            ).toBe(true);
            reach.reach("competing-holder");
            const next = scopedGrant(restore, minimum);
            expect(
              next.allocate(bytes),
              "the next batch keeps its pre-admitted fixed buffers",
            ).toBe(true);
            next.free(bytes);
            next.close();
            reach.reach("next-batch");
          } finally {
            other.close();
            restore.close();
            expect(governor.usedBytes).toBe(0);
            reach.reach("released");
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [16, 4096, 4, true, true],
          [1, 64, 0, false, false],
        ],
      },
    );
    reach.assertReached();
  },
);

it.each(seedsFor("next-key-order-refusal", [0xe107, 0xe108]))(
  "refuses unimplemented range fields even below LIMIT zero (seed %i)",
  (seed) => {
    const reach = createReach("index range refusal", ["lower", "upper", "locator"]);
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 7 }), fc.boolean(), (mask, inclusive) => {
        reach.beginHistory();
        const type = { kind: "int32", bits: 32 } as const;
        const output = [{ name: "v", type, nullable: false }];
        const bound = { keys: [{ op: "param", slot: 0, type } as const], inclusive };
        const plan: PhysicalPlan = {
          root: {
            id: 2,
            op: "limit",
            output,
            limit: { kind: "const", value: 0 },
            offset: null,
            input: {
              id: 1,
              op: "indexRange",
              output,
              tableId: 1,
              indexId: 1,
              columns: [1],
              lower: mask & 1 ? bound : null,
              upper: mask & 2 ? bound : null,
              emitLocator: (mask & 4) !== 0,
              descending: false,
            },
          },
          columns: output,
          parameters: [type],
          placeholderCount: 1,
          clockSlot: null,
          initPlans: [],
          catalogVersion: 1,
          volatile: false,
        };
        expect(() => createExecutor().prepare(plan)).toThrow(UnsupportedPlanError);
        if (mask & 1) reach.reach("lower");
        if (mask & 2) reach.reach("upper");
        if (mask & 4) reach.reach("locator");
      }),
      { seed, numRuns: 12, examples: [[7, true]] },
    );
    reach.assertReached();
  },
);
