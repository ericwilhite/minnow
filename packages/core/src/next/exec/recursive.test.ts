/** Borrow/close fences of the recursive work table, independent of the SQL adapter. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, RECURSIVE_LIFECYCLE_REACH } from "../../testing/reach.js";
import { BATCH_ROWS } from "../vector/vector.js";
import type { Rel } from "../ir/ir.js";
import type { SpillService } from "../spill/contract.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { Governor } from "../memory/governor.js";
import { OutputArena } from "./kernels/vectors.js";
import { lit, numbers, col, BOOL } from "./kernels/test-support.js";
import { INT, ArraySource, batchesOf } from "./join-fixtures.js";
import { drive, type Operator } from "./pipeline.js";
import { RecursiveOperator, validateRecursive } from "./recursive.js";
const output = [{ name: "n", type: INT, nullable: false }];
const scan: Rel = { op: "workTableScan", id: 3, output, workTable: 1 };
const rel: Extract<Rel, { op: "recursive" }> = {
  op: "recursive",
  id: 1,
  output,
  workTable: 1,
  all: true,
  base: { op: "values", id: 2, output, rows: [[lit(numbers(INT, [0]))]] },
  step: {
    op: "filter",
    id: 4,
    output,
    input: scan,
    predicate: {
      op: "compare",
      cmp: "<",
      left: col(0, INT),
      right: lit(numbers(INT, [0])),
      collation: null,
      type: BOOL,
    },
  },
};
const phases = [
  "append",
  "read",
  "consumer",
  "append failure",
  "read failure",
  "consumer failure",
] as const;
it.each(seedsFor("next-recursive-lifecycle", [0xb672, 0xb673]))(
  "waits for borrows and returns all bytes on cancellation/fault (seed %i)",
  async (seed) => {
    const reach = createReach<string>("Recursive lifecycle", RECURSIVE_LIFECYCLE_REACH);
    const examples: Array<[number, number]> = phases.map((_, i) => [i, 127]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: phases.length - 1 }),
        fc.integer({ min: 17, max: 1024 }),
        async (phaseIndex, batchRows) => {
          reach.beginHistory();
          const phase = phases[phaseIndex];
          if (!phase) throw new Error("Missing phase");
          const governor = new Governor(8 << 20),
            grant = await governor.admit({
              label: "recursive fence",
              minimumBytes: 4 << 20,
              spillable: true,
            });
          const arena = new OutputArena(grant, BATCH_ROWS),
            memory = new MemorySpillService(),
            reason = new Error(phase);
          let entered!: () => void, resume!: () => void;
          const begun = new Promise<void>((resolve) => {
              entered = resolve;
            }),
            blocked = new Promise<void>((resolve) => {
              resume = resolve;
            });
          let gated = false,
            released = 0,
            consumerBlocked = false;
          const gate = async (at: string) => {
            if (gated || !phase.startsWith(at)) return;
            gated = true;
            entered();
            if (phase.endsWith("failure")) throw reason;
            await blocked;
          };
          const spill: SpillService = {
            get bytesInUse() {
              return memory.bytesInUse;
            },
            sweep: () => memory.sweep(),
            create: async (queryId) => {
              const file = await memory.create(queryId);
              return {
                queryId,
                get length() {
                  return file.length;
                },
                close: file.close.bind(file),
                readInto: file.readInto.bind(file),
                append: async (page) => {
                  await gate("append");
                  return file.append(page);
                },
                read: async (ref) => {
                  await gate("read");
                  return file.read(ref);
                },
              };
            },
          };
          const sink: Operator = {
            push: () => {
              if (!gated && phase.startsWith("consumer")) {
                consumerBlocked = true;
                return "blocked";
              }
              return "ready";
            },
            ready: async () => {
              if (consumerBlocked) {
                consumerBlocked = false;
                await gate("consumer");
              }
            },
            finish: () => undefined,
            abort: () => undefined,
          };
          const op = new RecursiveOperator(
            rel,
            { arena, input: [INT], params: { slots: [] } },
            { grant, spill, queryId: 1 },
            sink,
          );
          const source = new ArraySource(
            batchesOf(
              [INT],
              Array.from({ length: 4001 }, (_, i) => [i]),
              { batchRows },
            ),
          );
          // Use a transfer on the first source batch: no byte may be returned while append or consumer holds it.
          const next = source.next.bind(source);
          let first = true;
          source.next = () => {
            const item = next();
            if (item === null) return null;
            if (first) {
              first = false;
              return {
                ...item,
                transfer: {
                  release: () => {
                    released++;
                  },
                },
              };
            }
            return item;
          };
          const running = Promise.resolve(drive(source, op.base));
          const outcome = running.then(
            () => null,
            (error: unknown) => error,
          );
          try {
            if (!phase.endsWith("failure")) {
              await Promise.race([
                begun,
                running.then(() => {
                  throw new Error("Fence not reached");
                }),
              ]);
              op.abort(reason);
              let closed = false;
              const closing = op.closed.then(() => {
                closed = true;
              });
              await new Promise<void>((resolve) => setImmediate(resolve));
              expect(closed).toBe(false);
              if (phase === "consumer") expect(released).toBe(0);
              resume();
              expect(await outcome).toBe(reason);
              await closing;
              reach.reach("borrow fence");
            } else {
              expect(await outcome).toBe(reason);
              op.abort(reason);
              await op.closed;
              reach.reach("fault cleanup");
            }
            expect(gated).toBe(true);
            expect(released).toBe(1);
            reach.reach(phase);
          } finally {
            resume();
            op.abort(reason);
            await outcome;
            await op.closed;
            arena.close();
            grant.close();
          }
          expect(memory.bytesInUse).toBe(0);
          expect(governor.usedBytes).toBe(0);
          expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
          reach.reach("returned all bytes");
        },
      ),
      { seed, numRuns: 6 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);
it("keeps unsupported recursive shapes typed and explicit", () => {
  expect(() => validateRecursive({ ...rel, all: false })).toThrow(
    expect.objectContaining({ construct: "recursive", sqlState: "0A000" }),
  );
  expect(() =>
    validateRecursive({ ...rel, output: [{ name: "s", type: { kind: "text" }, nullable: true }] }),
  ).toThrow(expect.objectContaining({ construct: "recursive", sqlState: "0A000" }));
  expect(() => validateRecursive({ ...rel, step: { ...scan, workTable: 99 } })).toThrow(
    expect.objectContaining({ construct: "recursive", sqlState: "0A000" }),
  );
});
