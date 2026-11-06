/**
 * Review B4-p2b-1: each finding's probe as a committed regression test (PostgreSQL via PGlite is
 * the law for values; the pipeline contract in exec/pipeline.ts and the governor's charge-first
 * rule for memory and pacing).
 */
import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "vitest";
import type { AggregateCall, OutputColumn } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { Batch, FlatVector } from "../../vector/vector.js";
import {
  I32,
  I64,
  TEXT,
  UNCHARGED,
  bigints,
  col,
  decimal,
  decimals,
  numbers,
  texts,
  valueAt,
} from "../kernels/test-support.js";
import { IDENTITY, OutputArena } from "../kernels/vectors.js";
import type { Operator, PushResult } from "../pipeline.js";
import { AggregateOperator, type AggregateOptions } from "./aggregate.js";
import { Charged, GroupTable } from "./groups.js";
import { accumulatorFor } from "./states.js";
import { GroupValues, flatBytes } from "./values.js";

/** A charge that counts what is held, optionally refusing. */
function counting(refuse: (bytes: number) => boolean = () => false): ByteCharge & {
  held: number;
  peak: number;
} {
  const charge = {
    held: 0,
    peak: 0,
    allocate(bytes: number) {
      if (refuse(bytes)) return false;
      charge.held += bytes;
      charge.peak = Math.max(charge.peak, charge.held);
      return true;
    },
    free(bytes: number) {
      charge.held -= bytes;
    },
  };
  return charge;
}

function aggregate(
  input: readonly PhysicalType[],
  groupBy: number[],
  aggregates: AggregateCall[],
  charge: ByteCharge,
  sink: Operator,
  options?: AggregateOptions,
): AggregateOperator {
  const output: OutputColumn[] = [
    ...groupBy.map((k) => ({ name: "k", type: input[k] ?? I32, nullable: true })),
    ...aggregates.map((a) => ({ name: a.fn, type: a.type, nullable: true })),
  ];
  return new AggregateOperator(
    {
      id: 1,
      op: "aggregate",
      output,
      input: { id: 0, op: "values", output: [], rows: [] },
      groupBy,
      groupingSets: null,
      aggregates,
    },
    { arena: new OutputArena(UNCHARGED, 2048), input, params: { slots: [] } },
    charge,
    sink,
    options,
  );
}

const call = (
  fn: AggregateCall["fn"],
  type: PhysicalType,
  input: PhysicalType | null,
): AggregateCall => ({
  fn,
  args: input === null ? [] : [col(1, input)],
  distinct: false,
  filter: null,
  orderBy: [],
  type,
});

describe("review B4-p2b-1", () => {
  it("1. NUMERIC SUM crosses ±2^127 exactly (int128 batch, then a safe one), as PostgreSQL", async () => {
    const pg = new PGlite();
    try {
      const D = decimal(null, null);
      const max = 2n ** 127n - 1n;
      for (const [first, second] of [
        [max, 1n],
        [-max - 1n, -1n],
        [max, max],
      ] as const) {
        for (const average of [false, true]) {
          const a = accumulatorFor(average ? "avg" : "sum", D, D, UNCHARGED);
          a.grow(1);
          a.update(decimals(D, [[first, 0]], "int128", 0), new Int32Array(1), IDENTITY, 1, true);
          a.update(
            decimals(
              D,
              [[second, 0]],
              second < 2n ** 53n && second > -(2n ** 53n) ? "safe" : "int128",
              0,
            ),
            new Int32Array(1),
            IDENTITY,
            1,
            true,
          );
          const theirs = (
            await pg.query<{ v: string }>(
              `select ${average ? "avg" : "sum"}(x)::text v from (values (${first.toString()}::numeric), (${second.toString()}::numeric)) t(x)`,
            )
          ).rows[0]?.v;
          const ours = valueAt(a.emit(0, 1), 0) as [bigint, number];
          const [c, s] = ours;
          const text =
            s === 0
              ? c.toString()
              : `${c < 0n ? "-" : ""}${(c < 0n ? -c : c)
                  .toString()
                  .padStart(s + 1, "0")
                  .replace(new RegExp(`(\\d{${String(s)}})$`), ".$1")}`;
          expect(
            text,
            `${average ? "avg" : "sum"}(${first.toString()}, ${second.toString()})`,
          ).toBe(theirs);
          a.close();
        }
      }
    } finally {
      await pg.close();
    }
  }, 60_000);

  it("2. a growing text MIN/MAX value survives compaction, and dead bytes stay counted", () => {
    const values = new GroupValues(TEXT, UNCHARGED);
    values.append(texts(["a".repeat(1500)]), 0);
    values.set(0, texts(["b".repeat(1600)]), 0);
    expect(valueAt(values.emit(0, 1), 0)).toBe("b".repeat(1600));
    // A model: many groups, each replaced by ever longer values; the store must match and its
    // charge stay within a small multiple of the live bytes.
    const store = new GroupValues(TEXT, UNCHARGED);
    const model: string[] = [];
    for (let g = 0; g < 50; g++) {
      store.append(texts([`g${String(g)}`]), 0);
      model.push(`g${String(g)}`);
    }
    for (let round = 1; round <= 40; round++)
      for (let g = 0; g < 50; g += 1 + (round % 3)) {
        const value = `${String(g)}:${"x".repeat(round * 37 + g)}`;
        store.set(g, texts([value]), 0);
        model[g] = value;
      }
    const out = store.emit(0, 50);
    expect(Array.from({ length: 50 }, (_, g) => valueAt(out, g))).toEqual(model);
    const live = model.reduce((n, v) => n + v.length, 0);
    expect(store.chargedBytes).toBeLessThan(4 * live + 64 * 1024);
    store.close();
  });

  it("3. output batches are charged before they are built, byte-bounded, and released", async () => {
    const charge = counting();
    let heldDuringPush = 0;
    let pushed = 0;
    let largest = 0;
    let release: (() => void) | null = null;
    const sink: Operator = {
      push(batch: Batch): PushResult {
        const bytes = batch.columns.reduce((n, c) => n + flatBytes(c as FlatVector), 0);
        largest = Math.max(largest, bytes);
        heldDuringPush = Math.max(heldDuringPush, charge.held);
        expect(charge.held).toBeGreaterThanOrEqual(bytes);
        pushed += 1;
        return pushed === 2 ? "blocked" : "ready";
      },
      ready: () =>
        new Promise<void>((resolve) => {
          release = resolve;
        }),
      finish: () => undefined,
      abort: () => undefined,
    };
    // A frozen clock: the slice must not end (and yield) before the blocked second batch.
    const op = aggregate([TEXT], [0], [], charge, sink, {
      targetBytes: 256 * 1024,
      now: () => 0,
    });
    for (let start = 0; start < 100; start += 20)
      op.push({
        length: 20,
        selection: null,
        columns: [
          texts(Array.from({ length: 20 }, (_, i) => `${String(start + i)}${"x".repeat(20_000)}`)),
        ],
      });
    const before = charge.held;
    const done = op.finish();
    expect(done).toBeInstanceOf(Promise);
    // Blocked on the second batch: its charge is still held until downstream is ready.
    expect(charge.held).toBeGreaterThan(before);
    (release as unknown as () => void)();
    await done;
    expect(pushed).toBeGreaterThan(5);
    expect(largest).toBeLessThanOrEqual(256 * 1024 + 64 * 1024);
    expect(charge.held).toBe(0); // state and output both returned
  });

  it("3. a refused output charge shrinks the batch, and refuses only a single group", () => {
    let limit = Infinity;
    const charge = counting((bytes) => bytes > limit);
    const sizes: number[] = [];
    const sink: Operator = {
      push(batch: Batch): PushResult {
        sizes.push(batch.length);
        return "ready";
      },
      ready: () => Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    };
    // A frozen task clock: only a real Promise could make the finish async (ruling R23.5).
    const op = aggregate([I32], [0], [], charge, sink, { now: () => 0 });
    op.push({
      length: 1000,
      selection: null,
      columns: [
        numbers(
          I32,
          Array.from({ length: 1000 }, (_, i) => i),
        ),
      ],
    });
    limit = 1000; // bytes per output batch from now on
    expect(op.finish()).toBeUndefined(); // synchronous: a ready sink and a short output
    expect(sizes.every((n) => n * 4 <= 1000)).toBe(true);
    expect(sizes.reduce((a, b) => a + b, 0)).toBe(1000);
  });

  it("4. a long finish yields with a task and stops when aborted", async () => {
    let batches = 0;
    let tick = 0;
    const sink: Operator = {
      push: () => {
        batches += 1;
        return "ready";
      },
      ready: () => Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    };
    // A clock that advances 1 ms per reading: every 4 readings end a slice.
    const op = aggregate([I32], [0], [], UNCHARGED, sink, {
      now: () => tick++,
      yieldIntervalMs: 4,
    });
    for (let start = 0; start < 200_000; start += 2000)
      op.push({
        length: 2000,
        selection: null,
        columns: [
          numbers(
            I32,
            Array.from({ length: 2000 }, (_, i) => i + start),
          ),
        ],
      });
    let timerRan = false;
    setImmediate(() => {
      timerRan = true;
    });
    const result = op.finish();
    expect(result).toBeInstanceOf(Promise);
    const before = batches;
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(timerRan).toBe(true);
    expect(batches).toBeGreaterThan(before);
    const reason = new Error("cancelled");
    op.abort(reason);
    const atAbort = batches;
    // A finish resumed after an abort rejects with its reason (R23), emitting nothing more.
    await expect(result).rejects.toBe(reason);
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
    expect(batches).toBe(atAbort);
    expect(batches).toBeLessThan(100);
  });

  it("5. typed arrays are charged before they are constructed (a refusal builds nothing)", () => {
    for (const Type of [Uint8Array, Int32Array] as const) {
      const Real = globalThis[Type.name as "Uint8Array" | "Int32Array"];
      let constructed = 0;
      (globalThis as Record<string, unknown>)[Type.name] = new Proxy(Real, {
        construct(target, args: [number]) {
          if (args[0] >= 100_000) constructed += 1;
          return Reflect.construct(target, args) as object;
        },
      });
      try {
        const memory = new Charged(counting(() => true));
        expect(() =>
          Type === Uint8Array
            ? memory.uint8(new Real(0) as Uint8Array, 1_000_000)
            : memory.int32(new Real(0) as Int32Array, 1_000_000),
        ).toThrow(RangeError);
        expect(constructed).toBe(0);
      } finally {
        (globalThis as Record<string, unknown>)[Type.name] = Real;
      }
    }
  });

  it("6. wide int8 keys are charged like safe ones (no Map), and representations share ids", () => {
    const run = (wide: boolean) => {
      const charge = counting();
      const table = new GroupTable([I64], charge);
      const ids = new Int32Array(2048);
      table.assign(
        [
          wide
            ? bigints(
                I64,
                Array.from({ length: 2000 }, (_, i) => 2n ** 60n + BigInt(i)),
              )
            : numbers(
                I64,
                Array.from({ length: 2000 }, (_, i) => i),
              ),
        ],
        2000,
        null,
        ids,
      );
      const held = charge.held;
      table.close();
      expect(charge.held).toBe(0);
      return held;
    };
    expect(run(true)).toBe(run(false));
    const table = new GroupTable([I64], UNCHARGED);
    const ids = new Int32Array(2048);
    table.assign([numbers(I64, [0, 9007199254740991, -9007199254740991])], 3, null, ids);
    table.assign(
      [bigints(I64, [0n, 9007199254740991n, -9007199254740991n, 9007199254740992n, -(2n ** 63n)])],
      5,
      null,
      ids,
    );
    expect([...ids.slice(0, 5)]).toEqual([0, 1, 2, 3, 4]);
    expect(valueAt(table.emitKey(0, 3, 2), 1)).toBe(-(2n ** 63n));
    table.close();
  });

  it("7. a constructor that throws leaves nothing charged", () => {
    const charge = counting();
    const stringAgg: AggregateCall = { ...call("string_agg", TEXT, TEXT), args: [col(0, TEXT)] };
    expect(() =>
      aggregate([TEXT], [0], [stringAgg], charge, {
        push: () => "ready",
        ready: () => Promise.resolve(),
        finish: () => undefined,
        abort: () => undefined,
      }),
    ).toThrow();
    expect(charge.held).toBe(0);
    // A refusal midway (the group table or a DISTINCT set cannot be built) unwinds too.
    for (let budget = 0; budget < 200_000; budget += 9_000) {
      const tight = counting((bytes) => tight.held + bytes > budget);
      const distinct: AggregateCall = { ...call("count", I64, TEXT), distinct: true };
      try {
        aggregate([I32, TEXT], [0], [distinct, call("sum", I64, I32)], tight, {
          push: () => "ready",
          ready: () => Promise.resolve(),
          finish: () => undefined,
          abort: () => undefined,
        }).abort(new Error("done"));
      } catch {
        // refused: nothing may stay charged
      }
      expect(tight.held).toBe(0);
    }
  });
});
