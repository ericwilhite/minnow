/** Allocation denial retries deterministic kernels; successful adaptive outputs retain borrows. */
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  PROJECTION_SPLIT_REACH,
  PROJECTION_ARRAY_STEP_REACH,
} from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { Batch } from "../vector/vector.js";
import * as compiler from "./kernels/compile.js";
import { encodeArrayValue } from "../vector/array-value.js";
import type { FlatVector } from "../vector/vector.js";
import { OutputArena, flatOf, slotIndex, isValidSlot } from "./kernels/vectors.js";
import { BOOL, TEXT, col, texts, numbers, valueAt } from "./kernels/test-support.js";
import { projectionStep } from "./projection-step.js";
import { ProjectOperator } from "./filter.js";
import { DivisionByZeroError } from "./errors.js";
import type { Operator } from "./pipeline.js";

const compile = compiler.compileExpr;
afterEach(() => vi.restoreAllMocks());
it.each(seedsFor("projection-split", [0xb7b3, 0xb7b4]))(
  "splits after denial and retains borrows seed%i",
  async (seed) => {
    const reach = createReach("projection split", PROJECTION_SPLIT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1024, max: 80000 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (width, sparse, cancel, error, funded) => {
          reach.beginHistory();
          const governor = new Governor(16 << 20);
          const grant = await governor.admit({
            label: "projection",
            minimumBytes: 0,
            spillable: true,
          });
          const competitor = await governor.admit({
            label: "pinned competitor",
            minimumBytes: 0,
            spillable: true,
          });
          const arena = new OutputArena(grant, 16);
          const abort = new AbortController();
          const counts = new Uint8Array(8);
          const values = Array.from({ length: 8 }, (_, i) => `${String(i)}${"x".repeat(width)}`);
          const batch: Batch = {
            length: 8,
            columns: [numbers(BOOL, [1, 0, 1, 0, 1, 0, 1, 0]), texts(values)],
            selection: sparse ? { rows: Uint16Array.of(1, 3, 4, 7), count: 4 } : null,
          };
          const expr: ScalarExpr = {
            op: "case",
            type: TEXT,
            branches: [{ when: col(0, BOOL), then: col(1, TEXT) }],
            otherwise: col(1, TEXT),
          };
          const mock = vi.spyOn(compiler, "compileExpr").mockImplementation((ir, context) => {
            const node = compile(ir, context);
            return {
              ...node,
              type: node.type,
              evaluate: (b, rows) => {
                const result = node.evaluate(b, rows);
                const n = rows?.count ?? b.length;
                for (let k = 0; k < n; k++) {
                  const row = rows?.rows[k] ?? k;
                  counts[row] = (counts[row] ?? 0) + 1;
                  if (error && !cancel && row === (sparse ? 3 : 1)) throw new DivisionByZeroError();
                }
                return result;
              },
            };
          });
          let released = 0,
            pushes = 0;
          let ordinary = false,
            holdNext = false,
            releaseExpected = 0;
          let releaseNext: () => void = () => undefined;
          const actual: unknown[] = [];
          let fail: (reason: unknown) => void = () => undefined;
          let operator: ProjectOperator | undefined;
          const sink: Operator = {
            push: (b) => {
              pushes++;
              const first = b.selection?.rows[0] ?? 0;
              const vector = b.columns[0];
              if (!vector) throw new Error("Missing borrowed output");
              const before = valueAt(vector, first);
              const active = b.selection?.count ?? b.length;
              for (let k = 0; k < active; k++)
                actual.push(valueAt(vector, b.selection?.rows[k] ?? k));
              if (active > 1) reach.reach("multi-row prefix");
              expect(released).toBe(releaseExpected);
              const wait = new Promise<void>((resolve, reject) => {
                fail = reject;
                if (holdNext) {
                  releaseNext = resolve;
                  return;
                }
                queueMicrotask(() => {
                  try {
                    expect(valueAt(vector, first)).toEqual(before);
                    if (cancel) abort.abort(new Error("cancel wait"));
                    else resolve();
                  } catch (e) {
                    reject(e instanceof Error ? e : new Error("Borrow check failed", { cause: e }));
                  }
                });
              });
              ready = wait;
              return "blocked";
            },
            ready: () => ready,
            finish: () => undefined,
            abort: (reason) => fail(reason),
          };
          let ready = Promise.resolve();
          const onAbort = () => operator?.abort(abort.signal.reason);
          abort.signal.addEventListener("abort", onAbort, { once: true });
          try {
            operator = new ProjectOperator(
              [expr],
              { arena, params: { slots: [] }, input: [BOOL, TEXT] },
              sink,
              {
                available: () =>
                  ordinary
                    ? grant.held -
                      grant.allocated +
                      governor.capBytes -
                      governor.reserveBytes -
                      governor.usedBytes
                    : funded
                      ? grant.held - grant.allocated
                      : 0,
                signal: abort.signal,
              },
            );
            mock.mockRestore();
            grant.release(grant.held - grant.allocated);
            const fixed = grant.allocated;
            const stepCredit = funded ? 4 * (width + 1) : 0;
            if (stepCredit) expect(grant.tryGrow(stepCredit)).toBe(true);
            expect(
              competitor.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
            ).toBe(true);
            expect(operator.push(batch, { release: () => released++ })).toBe("blocked");
            if (cancel || error)
              await expect(operator.ready()).rejects.toThrow(
                cancel ? "cancel wait" : "division by zero",
              );
            else {
              await operator.ready();
              const rows = batch.selection ? [...batch.selection.rows] : [0, 1, 2, 3, 4, 5, 6, 7];
              expect(actual).toEqual(rows.map((r) => values[r]));
              expect(pushes).toBeLessThanOrEqual(rows.length);
              if (!funded) expect(pushes).toBe(rows.length);
              reach.reach("exact values");
            }
            if (!error) {
              expect([...counts].every((n) => n <= 1)).toBe(true);
              reach.reach("one evaluation");
            }
            expect(released).toBe(1);
            expect(grant.allocated - fixed).toBeLessThanOrEqual(stepCredit);
            if (!funded) {
              expect(grant.allocated).toBe(fixed);
              reach.reach("no payload allocation");
            }
            reach.reach("held borrow");
            reach.reach(sparse ? "sparse" : "dense");
            if (cancel) reach.reach("cancel wait");
            else if (error) reach.reach("error once");
            else {
              // A following funded batch must wait on ITS borrow, not an already settled
              // readiness promise left over from the adaptive prefix loop.
              ordinary = true;
              holdNext = true;
              releaseExpected = 1;
              competitor.close();
              counts.fill(0);
              expect(operator.push(batch, { release: () => released++ })).toBe("blocked");
              let settled = false;
              const next = operator.ready().then(() => {
                settled = true;
              });
              await Promise.resolve();
              await Promise.resolve();
              expect(settled).toBe(false);
              expect(released).toBe(1);
              releaseNext();
              await next;
              expect(released).toBe(2);
              const active = batch.selection ? [...batch.selection.rows] : [0, 1, 2, 3, 4, 5, 6, 7];
              expect(active.every((row) => counts[row] === 1)).toBe(true);
              reach.reach("adaptive to ordinary");
            }
          } finally {
            mock.mockRestore();
            abort.signal.removeEventListener("abort", onAbort);
            operator?.abort(new Error("test cleanup"));
            await operator?.closed;
            arena.close();
            grant.close();
            competitor.close();
            expect(governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        endOnFailure: true,
        examples: [
          [80000, false, false, false, false],
          [80000, true, false, false, false],
          [80000, false, true, false, false],
          [80000, true, false, true, false],
          [80000, false, false, false, true],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);

/** Selectors copy opaque array encodings; they never parse or normalize their elements. */
it.each(seedsFor("projection-array-step", [47117, 47118]))(
  "borrows serialized array selectors under competing credit seed%i",
  async (seed) => {
    const reach = createReach("projection array step", PROJECTION_ARRAY_STEP_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 80000 }),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: -8, max: 8 }),
        async (width, sparse, coalesce, lower) => {
          reach.beginHistory();
          const type = { kind: "array", element: TEXT } as const;
          const encoded = (tag: string) =>
            encodeArrayValue(TEXT, {
              dimensions: [2],
              lowerBounds: [lower],
              elements: [new TextEncoder().encode(tag + "x".repeat(width)), null],
            });
          const left = [encoded("a"), null, encoded("c"), null];
          const right = [encoded("w"), encoded("x"), encoded("y"), encoded("z")];
          const vector = (values: ReadonlyArray<Uint8Array | null>): FlatVector => {
            const offsets = new Int32Array(5);
            let size = 0;
            for (let i = 0; i < values.length; i++) {
              offsets[i] = size;
              size += values[i]?.length ?? 0;
            }
            offsets[4] = size;
            const bytes = new Uint8Array(size),
              validity = new Uint32Array(1);
            for (let i = 0; i < values.length; i++) {
              const value = values[i];
              if (value) {
                bytes.set(value, offsets[i]);
                validity[0] = (validity[0] ?? 0) | (1 << i);
              }
            }
            return {
              form: "flat",
              type,
              length: 4,
              validity,
              data: { layout: "varlen", bytes, offsets },
            };
          };
          const batch: Batch = {
            length: 4,
            columns: [numbers(BOOL, [1, 0, 1, 0]), vector(left), vector(right)],
            selection: sparse ? { rows: Uint16Array.of(1, 3), count: 2 } : null,
          };
          const expr: ScalarExpr = coalesce
            ? { op: "call", fn: "coalesce", type, args: [col(1, type), col(2, type)] }
            : {
                op: "case",
                type,
                branches: [{ when: col(0, BOOL), then: col(1, type) }],
                otherwise: col(2, type),
              };
          // Array casts may change encoded element widths. A same-kind cast is
          // not a byte-preserving selector unless its full physical type agrees.
          const cast: ScalarExpr = {
            op: "cast",
            type: { kind: "array", element: { kind: "bytes" } },
            input: col(1, type),
          };
          expect(
            projectionStep(
              [{ type: cast.type, op: "call", fn: "coalesce", args: [cast, col(2, cast.type)] }],
              { slots: [] },
            ),
          ).toBeNull();
          expect(
            projectionStep(
              [
                {
                  op: "case",
                  type,
                  branches: [
                    {
                      when: {
                        op: "compare",
                        cmp: "=",
                        collation: null,
                        type: BOOL,
                        left: col(1, type),
                        right: col(2, type),
                      },
                      then: col(1, type),
                    },
                  ],
                  otherwise: col(2, type),
                },
              ],
              { slots: [] },
            ),
          ).toBeNull();
          const governor = new Governor(16 << 20);
          const grant = await governor.admit({
            label: "array selector",
            minimumBytes: 0,
            spillable: true,
          });
          const competitor = await governor.admit({
            label: "retained competitor",
            minimumBytes: 0,
            spillable: true,
          });
          const arena = new OutputArena(grant, 16);
          let operator: ProjectOperator | undefined,
            released = 0;
          const actual: Array<Uint8Array | null> = [];
          try {
            operator = new ProjectOperator(
              [expr],
              { arena, params: { slots: [] }, input: [BOOL, type, type] },
              {
                push: (output) => {
                  const v = output.columns[0];
                  if (!v) throw new Error("Missing selected array");
                  expect(output.selection?.count ?? output.length).toBe(1);
                  const row = output.selection?.rows[0] ?? 0;
                  const flat = flatOf(v),
                    slot = slotIndex(v, row);
                  if (flat.data.layout !== "varlen") throw new Error("Missing array encoding");
                  actual.push(
                    isValidSlot(v, row)
                      ? flat.data.bytes.slice(flat.data.offsets[slot], flat.data.offsets[slot + 1])
                      : null,
                  );
                  return "ready";
                },
                ready: () => Promise.resolve(),
                finish: () => undefined,
                abort: () => undefined,
              },
              { available: () => 0, signal: new AbortController().signal },
            );
            grant.release(grant.held - grant.allocated);
            const fixed = grant.allocated;
            expect(
              competitor.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
            ).toBe(true);
            expect(operator.push(batch, { release: () => released++ })).toBe("blocked");
            await operator.ready();
            const rows = sparse ? [1, 3] : [0, 1, 2, 3];
            const expected = rows.map((row) => left[row] ?? right[row]);
            expect(actual.length).toBe(expected.length);
            expect(
              actual.every((value, i) => {
                const wanted = expected[i];
                return (
                  value !== null &&
                  value.length === wanted?.length &&
                  value.every((byte, k) => byte === wanted[k])
                );
              }),
            ).toBe(true);
            expect(released).toBe(1);
            expect(grant.allocated).toBe(fixed);
            reach.reach(coalesce ? "COALESCE" : "CASE");
            reach.reach(sparse ? "sparse" : "dense");
            reach.reach("exact encoding");
            reach.reach("no payload allocation");
            if (lower !== 1) reach.reach("nondefault bounds");
            reach.reach("element NULL");
          } finally {
            operator?.abort(new Error("array step cleanup"));
            await operator?.closed;
            arena.close();
            grant.close();
            competitor.close();
            expect(governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        endOnFailure: true,
        examples: [
          [80000, false, false, -1],
          [80000, true, true, 2],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
