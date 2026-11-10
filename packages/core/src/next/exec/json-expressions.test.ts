/** Borrowed JSON frames prepay their input-sized workspace and never inspect inactive slots. */
import { expect, it, vi } from "vitest";
import fc from "fast-check";
import type { PhysicalType } from "../types.js";
import type { Batch, Vector } from "../vector/vector.js";
import type { ScalarExpr } from "../ir/ir.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_FRAME_REACH, JSON_PROJECTION_REACH } from "../../testing/reach.js";
import { JsonExpressions } from "./json-expressions.js";
import type { Operator } from "./pipeline.js";
import { OutputArena } from "./kernels/vectors.js";
import {
  TEXT,
  call,
  col,
  lit,
  texts,
  batchOf,
  dictionary,
  constant,
  valueAt,
} from "./kernels/test-support.js";
const JSON_TYPE: PhysicalType = { kind: "json", binary: false };
const get = call("json_get_text", TEXT, col(0, JSON_TYPE), lit(texts(["n"])));
function frame(
  expr: ScalarExpr | readonly ScalarExpr[],
  width: number,
  sink: Operator,
  capacity = 32,
) {
  let held = 0,
    peak = 0,
    allocations = 0;
  const arena = new OutputArena(
    {
      allocate: (bytes) => {
        held += bytes;
        peak = Math.max(peak, held);
        allocations++;
        return true;
      },
      free: (bytes) => {
        held -= bytes;
        expect(held).toBeGreaterThanOrEqual(0);
      },
    },
    capacity,
  );
  const operator = new JsonExpressions(
    Array.isArray(expr) ? expr : [expr as ScalarExpr],
    { arena, input: [JSON_TYPE], params: { slots: [] } },
    { bytes: width },
    sink,
  );
  return { operator, arena, held: () => held, peak: () => peak, allocations: () => allocations };
}
function sinkFor(rows: unknown[]): Operator {
  return {
    push: (batch) => {
      const row = batch.selection?.rows[0] ?? 0;
      const value = batch.columns[0];
      if (value === undefined) throw new Error("Missing JSON output");
      rows.push(valueAt(value, row));
      return "ready";
    },
    ready: () => Promise.resolve(),
    finish: () => undefined,
    abort: () => undefined,
  };
}
it.each(seedsFor("next-json-frame", [0xb642, 0xb643]))(
  "keeps selected, nullable and encoded input order with fixed prepaid bytes (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON expression frames", JSON_FRAME_REACH);
    const examples: Array<[number, number, boolean]> = [
      [0, 1, false],
      [1, 3, true],
      [2, 5, false],
      [0, 20, true],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 2 }),
        fc.integer({ min: 1, max: 24 }),
        fc.boolean(),
        async (form, count, reverse) => {
          reach.beginHistory();
          const doc = '{"n":"' + "é\\n".repeat(count) + '"}';
          const expected = "é\n".repeat(count),
            width = new TextEncoder().encode(doc).length + 1;
          const selected = reverse ? [3, 1, 0] : [0, 1, 3];
          let vector: Vector;
          if (form === 0) vector = texts([doc, null, "garbage", doc], JSON_TYPE);
          else if (form === 1)
            vector = dictionary(texts([doc, "invalid"], JSON_TYPE), [0, null, 1, 0]);
          else vector = constant(texts([doc], JSON_TYPE), 4);
          const rows: unknown[] = [];
          const test = frame(get, width, sinkFor(rows));
          const empty = frame(get, 0, sinkFor([]));
          expect(test.held() - empty.held()).toBe(width); // Workspace exists before a source can push.
          empty.operator.abort(undefined);
          await empty.operator.closed;
          empty.arena.close();
          expect(empty.held()).toBe(0);
          const allocations = test.allocations(),
            fixed = test.held();
          let released = 0;
          const batch: Batch = {
            ...batchOf([vector]),
            selection: { rows: Uint16Array.from(selected), count: selected.length },
          };
          expect(test.operator.push(batch, { release: () => released++ })).toBe("blocked");
          await test.operator.ready();
          expect(rows).toEqual(selected.map((row) => (form !== 2 && row === 1 ? null : expected)));
          expect(released).toBe(1);
          expect(test.allocations()).toBe(allocations);
          expect(test.peak()).toBe(fixed);
          await test.operator.finish();
          await test.operator.closed;
          test.arena.close();
          expect(test.held()).toBe(0);
          reach.reach(["flat", "dictionary", "constant"][form] ?? "flat");
          reach.reach(reverse ? "reverse selection" : "forward selection");
          if (form !== 2) reach.reach("NULL and inactive garbage");
          reach.reach("prepaid workspace");
          reach.reach("released borrow");
        },
      ),
      { seed, numRuns: 40 + examples.length, examples },
    );
    reach.assertReached();
  },
);
it("holds bytes and source borrow until downstream readiness", async () => {
  let release!: () => void, seen: Batch | undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let firstPush!: () => void;
  const pushed = new Promise<void>((resolve) => {
    firstPush = resolve;
  });
  const sink: Operator = {
    push: (batch) => {
      seen = batch;
      firstPush();
      return "blocked";
    },
    ready: () => gate,
    finish: () => undefined,
    abort: () => release(),
  };
  const test = frame(get, 64, sink);
  let released = 0;
  test.operator.push(batchOf([texts(['{"n":"kept"}'], JSON_TYPE)]), { release: () => released++ });
  // A real task yield before the first push is permitted; readiness is still held by the gate.
  await pushed;
  expect(seen).toBeDefined();
  expect(released).toBe(0);
  const value = seen?.columns[0];
  if (value === undefined) throw new Error("Missing borrowed JSON result");
  expect(valueAt(value, 0)).toBe("kept");
  release();
  await test.operator.ready();
  expect(released).toBe(1);
  await test.operator.finish();
  test.arena.close();
  expect(test.held()).toBe(0);
});
it("cancels a deep scan at the shared task clock and returns all prepaid memory", async () => {
  const depth = 12000,
    doc = '{"n":' + "[".repeat(depth) + "0" + "]".repeat(depth) + "}";
  const test = frame(get, doc.length + 1, sinkFor([]));
  const due = vi.spyOn(test.operator.slice, "due").mockReturnValue(true);
  let released = 0;
  const reason = new Error("cancel JSON scan");
  test.operator.push(batchOf([texts([doc], JSON_TYPE)]), { release: () => released++ });
  const pending = test.operator.ready();
  setTimeout(() => test.operator.abort(reason), 0);
  await expect(pending).rejects.toBe(reason);
  await test.operator.closed;
  expect(due).toHaveBeenCalled();
  expect(released).toBe(1);
  due.mockRestore();
  test.arena.close();
  expect(test.held()).toBe(0);
});

it.each(seedsFor("next-json-projection", [0xb64c, 0xb64d]))(
  "resumes wide projections with linear JSON evaluation (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON projection resumption", JSON_PROJECTION_REACH);
    const examples: Array<[number, number]> = [
      [1, 1],
      [16, 20],
      [64, 100],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 64 }),
        fc.integer({ min: 1, max: 200 }),
        async (columns, length) => {
          reach.beginHistory();
          const value = "é".repeat(length),
            document = JSON.stringify({ n: value }),
            rows: unknown[] = [];
          const sink: Operator = {
            push: (batch) => {
              const row = batch.selection?.rows[0] ?? 0;
              rows.push(...batch.columns.map((column) => valueAt(column, row)));
              return "ready";
            },
            ready: () => Promise.resolve(),
            finish: () => undefined,
            abort: () => undefined,
          };
          const test = frame(
            Array.from({ length: columns }, () => get),
            new TextEncoder().encode(document).length + 1,
            sink,
          );
          const evaluations = vi.spyOn(test.operator, "evaluate");
          test.operator.push(batchOf([texts([document], JSON_TYPE)]));
          await test.operator.ready();
          expect(rows).toEqual(Array.from({ length: columns }, () => value));
          expect(evaluations.mock.calls.length).toBeLessThanOrEqual(2 * columns);
          await test.operator.finish();
          test.arena.close();
          expect(test.held()).toBe(0);
          evaluations.mockRestore();
          reach.reach(columns === 1 ? "one column" : "many columns");
          if (columns === 64) reach.reach("64 columns");
          reach.reach("linear evaluation");
          reach.reach("returned charges");
        },
      ),
      { seed, numRuns: 30 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);
