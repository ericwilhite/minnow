/** Borrowed input and spill I/O outlive cancellation only until their exact fences settle. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, EXCEPT_LIFECYCLE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { SpillService } from "../spill/contract.js";
import { ExceptOperator, exceptMinimumBytes } from "./except.js";
import { TEXT, ArraySource, batchesOf, type Row } from "./join-fixtures.js";
import { drive, type BatchSource } from "./pipeline.js";
it.each(seedsFor("next-except-lifecycle", [0xb635, 0xb636]))(
  "drains cancelled borrows and I/O at every phase (seed %i)",
  async (seed) => {
    const reach = createReach<string>("EXCEPT cancellation", EXCEPT_LIFECYCLE_REACH);
    const examples: Array<["append" | "read" | "output", number]> = [
      ["append", 1],
      ["append", 4],
      ["read", 1],
      ["read", 5],
      ["output", 1],
      ["output", 5],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("append" as const, "read" as const, "output" as const),
        fc.integer({ min: 1, max: 6 }),
        async (phase, target) => {
          reach.beginHistory();
          const types = [TEXT],
            minimum = exceptMinimumBytes(types),
            cap = Math.max(minimum + (1 << 20), Math.ceil((minimum * 8) / 7));
          const governor = new Governor(cap),
            memory = new MemorySpillService();
          let enter!: () => void, resume!: () => void, reject!: (reason: unknown) => void;
          const entered = new Promise<void>((resolve) => {
            enter = resolve;
          });
          const gate = new Promise<void>((resolve, no) => {
            resume = resolve;
            reject = no;
          });
          const calls = { append: 0, read: 0, output: 0 };
          const spill: SpillService = {
            get bytesInUse() {
              return memory.bytesInUse;
            },
            sweep: () => memory.sweep(),
            create: async (id) => {
              const file = await memory.create(id);
              return {
                queryId: file.queryId,
                get length() {
                  return file.length;
                },
                close: () => file.close(),
                readInto: (ref, out) => file.readInto(ref, out),
                append: async (page) => {
                  calls.append++;
                  if (phase === "append" && calls.append === target) {
                    enter();
                    await gate;
                  }
                  return file.append(page);
                },
                read: async (ref) => {
                  calls.read++;
                  if (phase === "read" && calls.read === target) {
                    enter();
                    await gate;
                  }
                  return file.read(ref);
                },
              };
            },
          };
          const sink = {
            push: () => {
              calls.output++;
              if (phase === "output" && calls.output === target) {
                enter();
                return "blocked" as const;
              }
              return "ready" as const;
            },
            ready: () => gate,
            finish: () => undefined,
            abort: (reason: unknown) => {
              if (phase === "output") reject(reason);
            },
          };
          const grant = await governor.admit({
            label: "EXCEPT cancellation",
            minimumBytes: minimum,
            spillable: true,
          });
          const operator = new ExceptOperator(types, true, { grant, spill, queryId: 1 }, sink);
          const rows: Row[] = Array.from({ length: 3200 }, (_, i) => [
            `${String(i)}:${"x".repeat(512)}`,
          ]);
          const original = new ArraySource(batchesOf(types, rows, { batchRows: 127 }));
          let borrowed = 0,
            released = 0;
          const source: BatchSource = {
            next: async () => {
              const next = original.next();
              if (next === null) return null;
              borrowed++;
              return {
                ...next,
                transfer: {
                  release: () => {
                    released++;
                  },
                },
              };
            },
            abort: () => original.abort(),
          };
          let running = Promise.resolve(drive(source, operator.left));
          if (phase !== "append") {
            await running;
            running = Promise.resolve(drive(new ArraySource([]), operator.right));
          }
          const reason = new Error(`cancel ${phase}`);
          try {
            await entered;
            const atCancel = { ...calls };
            operator.abort(reason);
            if (phase !== "output") {
              let closed = false;
              void operator.closed.then(() => {
                closed = true;
              });
              await Promise.resolve();
              expect(closed).toBe(false);
              if (phase === "append") expect(released).toBe(borrowed - 1);
              resume();
            }
            await expect(running).rejects.toBe(reason);
            await operator.closed;
            expect(calls).toEqual(atCancel);
            expect(released).toBe(borrowed);
          } finally {
            resume();
            operator.abort(reason);
            await running.catch(() => undefined);
            await operator.closed;
            grant.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(memory.bytesInUse).toBe(0);
          expect(governor.peakBytes).toBeLessThanOrEqual(cap);
          reach.reach(phase);
          reach.reach("borrow returned");
          reach.reach("cleanup");
          reach.reach("no work after abort");
          if (phase !== "output") reach.reach("I/O fence");
        },
      ),
      { seed, numRuns: 6 + examples.length, examples },
    );
    reach.assertReached();
  },
  120_000,
);
