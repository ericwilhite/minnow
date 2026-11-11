/** PostgreSQL order is the authority; the physical path must not change values or borrows. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { COLUMNAR_SORT_REACH, createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import type { Operator } from "./pipeline.js";
import { drive, TaskClock } from "./pipeline.js";
import {
  ArraySource,
  CollectSink,
  INT,
  DBL,
  BIG,
  TEXT,
  batchesOf,
  type Row,
} from "./join-fixtures.js";
import { SortOperator, sortMinimumBytes, type SortSpec } from "./sort.js";

const reach = createReach("next-sort-columnar", COLUMNAR_SORT_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});

async function setup(types: readonly PhysicalType[], cap = 64 << 20) {
  const governor = new Governor(cap),
    spill = new MemorySpillService();
  const grant = await governor.admit({
    label: "columnar-sort",
    minimumBytes: sortMinimumBytes(types),
    spillable: true,
  });
  return { governor, spill, grant, context: { grant, spill, queryId: 1, columnar: true } };
}
const asc = (column: number) => ({
  column,
  direction: "asc" as const,
  nulls: "last" as const,
  collation: null,
});
const full = (types: readonly PhysicalType[]): SortSpec => ({
  types,
  keys: [asc(1), asc(0)],
  limit: null,
  offset: 0,
  withTies: false,
});

for (const seed of seedsFor("next-sort-columnar", [0x12c0, 0x6a31])) {
  it(`mixed nullable float/int64 keys and payload vs PGlite (${String(seed)})`, async () => {
    const random = mulberry32(seed),
      shift = Math.floor(random() * 17);
    const types = [INT, DBL, BIG, TEXT];
    for (const n of [31, 70003]) {
      const data: Row[] = Array.from({ length: n }, (_, id) => {
        const p = (id + shift) % 17;
        const value =
          p === 0
            ? null
            : p === 1
              ? NaN
              : p === 2
                ? Infinity
                : p === 3
                  ? -Infinity
                  : p === 4
                    ? -0
                    : p === 5
                      ? 0
                      : (((id * 7919) % 1009) - 500) / 7;
        const integer =
          p === 6
            ? null
            : p === 7
              ? -9223372036854775808n
              : p === 8
                ? 9223372036854775807n
                : p === 9
                  ? -4611686018427387905n
                  : 9007199254740992n + BigInt(id % 23);
        return [
          id,
          value,
          integer,
          id % 13 === 0 ? null : (["", "é", "😀", "payload"][id % 4] ?? ""),
        ];
      });
      const expr = `CASE (i+${String(shift)})%17 WHEN 0 THEN NULL WHEN 1 THEN 'NaN'::float8 WHEN 2 THEN 'Infinity'::float8 WHEN 3 THEN '-Infinity'::float8 WHEN 4 THEN '-0'::float8 WHEN 5 THEN 0::float8 ELSE (((i::bigint*7919)%1009)-500)::float8/7 END`;
      const big = `CASE (i+${String(shift)})%17 WHEN 6 THEN NULL WHEN 7 THEN '-9223372036854775808'::bigint WHEN 8 THEN '9223372036854775807'::bigint WHEN 9 THEN '-4611686018427387905'::bigint ELSE 9007199254740992::bigint+i%23 END`;
      for (const direction of ["asc", "desc"] as const)
        for (const nulls of ["first", "last"] as const) {
          const expected = (
            await pg.query<{ id: number }>(
              `SELECT i AS id FROM generate_series(0,${String(n - 1)}) g(i) ORDER BY ${expr} ${direction} NULLS ${nulls}, ${big} DESC NULLS LAST, i DESC`,
            )
          ).rows.map((row) => row.id);
          const { governor, spill, context } = await setup(types),
            sink = new CollectSink();
          const op = new SortOperator(
            {
              types,
              keys: [
                { column: 1, direction, nulls, collation: null },
                { ...asc(2), direction: "desc" },
                { ...asc(0), direction: "desc" },
              ],
              limit: null,
              offset: 0,
              withTies: false,
            },
            context,
            sink,
          );
          await drive(
            new ArraySource(
              batchesOf(types, data, { dictionary: true, int64: "int64", batchRows: 997 }),
            ),
            op,
          );
          await op.closed;
          expect(sink.rows.map((row) => row[0])).toEqual(expected);
          for (const row of sink.rows) {
            const original = data[Number(row[0])];
            expect(row[3]).toBe(original?.[3]);
            expect(row[2] === null ? null : String(row[2])).toBe(
              original?.[2] === null ? null : String(original?.[2]),
            );
            expect(row[1]).toBe(original?.[1]); // Includes NaN and the payload sign of zero.
          }
          expect(op.profile.columnarRows).toBe(n);
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.beginHistory();
          for (const state of [
            n < 65536 ? "small stable merge" : "large LSD",
            "float peers",
            "wide int64",
            "null keys",
            "nullable dictionary payload",
            direction,
            nulls,
          ] as const)
            reach.reach(state);
        }
    }
  }, 120000);

  it(`safe and wide int64 representations have the same PG order (${String(seed)})`, async () => {
    const random = mulberry32(seed),
      shift = Math.floor(random() * 14),
      types = [INT, BIG];
    const integers = [
      -9007199254740991,
      -4294967297,
      -4294967296,
      -4294967295,
      -2147483649,
      -1,
      0,
      1,
      2147483648,
      4294967295,
      4294967296,
      4294967297,
      9007199254740991,
      null,
    ];
    const expr = `CASE (i+${String(shift)})%14 ${integers.map((value, k) => `WHEN ${String(k)} THEN ${value === null ? "NULL::bigint" : `'${String(value)}'::bigint`}`).join(" ")} END`;
    for (const n of [31, 65537]) {
      const data: Row[] = Array.from({ length: n }, (_, id) => [
        id,
        integers[(id + shift) % 14] ?? null,
      ]);
      const expected = (
        await pg.query<{ id: number }>(
          `SELECT i AS id FROM generate_series(0,${String(n - 1)}) g(i) ORDER BY ${expr}, i`,
        )
      ).rows.map((row) => row.id);
      for (const representation of ["safe", "int64"] as const) {
        const { governor, spill, context } = await setup(types),
          sink = new CollectSink();
        const op = new SortOperator(full(types), context, sink);
        await drive(
          new ArraySource(batchesOf(types, data, { int64: representation, batchRows: 997 })),
          op,
        );
        await op.closed;
        expect(sink.rows.map((row) => row[0])).toEqual(expected);
        expect(sink.rows.map((row) => row[1])).toEqual(
          expected.map((id) => {
            const value = data[id]?.[1];
            return value == null ? null : BigInt(value);
          }),
        );
        expect(op.profile.columnarRows).toBe(n);
        expect(governor.usedBytes).toBe(0);
        expect(spill.bytesInUse).toBe(0);
        reach.beginHistory();
        reach.reach(representation === "safe" ? "safe int64" : "wide int64");
      }
    }
  }, 120000);

  it(`stable ties and selected input on both paths (${String(seed)})`, async () => {
    const random = mulberry32(seed),
      shift = Math.floor(random() * 11),
      types = [INT, INT];
    for (const n of [31, 65537]) {
      const data: Row[] = Array.from({ length: n }, (_, id) => [id, (id + shift) % 11]);
      const expected = (
        await pg.query<{ id: number }>(
          `SELECT i AS id FROM generate_series(0,${String(n - 1)}) g(i) ORDER BY (i+${String(shift)})%11 DESC, i`,
        )
      ).rows.map((row) => row.id);
      const { governor, spill, context } = await setup(types),
        sink = new CollectSink();
      const op = new SortOperator(
        { ...full(types), keys: [{ ...asc(1), direction: "desc" }] },
        context,
        sink,
      );
      await drive(new ArraySource(batchesOf(types, data, { batchRows: 997, skip: 3 })), op);
      await op.closed;
      // The operator's stable-tie law preserves arrival order. The PG oracle adds the
      // original row id explicitly, instead of relying on PG's unspecified order of peers.
      expect(sink.rows.map((row) => row[0])).toEqual(expected);
      expect(op.profile.columnarRows).toBe(n);
      expect(governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
      reach.beginHistory();
      reach.reach("stable ties");
      reach.reach("selected input");
    }
  }, 60000);

  it(`payload-only resident budget, no normalized tail columns (${String(seed)})`, async () => {
    const random = mulberry32(seed),
      types = [INT, DBL, TEXT],
      n = 70003;
    const data: Row[] = Array.from({ length: n }, (_, id) => [
      id,
      Math.floor(random() * 1000) / 7,
      "p",
    ]);
    const { governor, spill, context } = await setup(types, 8 << 20),
      sink = new CollectSink();
    const op = new SortOperator(full(types), context, sink);
    await drive(new ArraySource(batchesOf(types, data, { batchRows: 2048 })), op);
    await op.closed;
    expect(sink.rows.map((row) => row[0])).toEqual(
      data
        .slice()
        .sort((a, b) => Number(a[1]) - Number(b[1]) || Number(a[0]) - Number(b[0]))
        .map((row) => row[0]),
    );
    expect(op.profile.columnarRows).toBe(n);
    expect(op.profile.spilledBytes).toBe(0);
    expect(governor.peakBytes).toBeLessThanOrEqual(8 << 20);
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  }, 60000);
}

for (const seed of seedsFor("next-sort-columnar", [0x12c0, 0x6a31]))
  it(`budget refusal really spills through the fallback (${String(seed)})`, async () => {
    const random = mulberry32(seed),
      types = [INT, DBL, TEXT],
      n = 70003;
    const data: Row[] = Array.from({ length: n }, (_, id) => [
      id,
      Math.floor(random() * 1000) / 7,
      "p",
    ]);
    const { governor, spill, context } = await setup(types, 4 << 20),
      sink = new CollectSink();
    const op = new SortOperator(full(types), context, sink);
    await drive(new ArraySource(batchesOf(types, data)), op);
    await op.closed;
    expect(sink.rows.map((row) => row[0])).toEqual(
      data
        .slice()
        .sort((a, b) => Number(a[1]) - Number(b[1]) || Number(a[0]) - Number(b[0]))
        .map((row) => row[0]),
    );
    expect(op.profile.columnarRows).toBe(0);
    expect(op.profile.spilledBytes).toBeGreaterThan(0);
    expect(governor.peakBytes).toBeLessThanOrEqual(4 << 20);
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
    reach.beginHistory();
    reach.reach("external spill");
  }, 60000);

it("reclamation replays every row before using the general sorter", async () => {
  const types = [INT, DBL],
    data: Row[] = Array.from({ length: 5003 }, (_, i) => [i, i % 37]);
  const { governor, spill, context } = await setup(types),
    sink = new CollectSink(),
    op = new SortOperator(full(types), context, sink);
  const input = batchesOf(types, data, { batchRows: 997 });
  for (const batch of input.slice(0, 3)) if (op.push(batch) === "blocked") await op.ready();
  await op.reclaim();
  for (const batch of input.slice(3)) if (op.push(batch) === "blocked") await op.ready();
  await op.finish();
  await op.closed;
  expect(sink.rows.map((row) => row[0])).toEqual(
    data
      .slice()
      .sort((a, b) => Number(a[1]) - Number(b[1]) || Number(a[0]) - Number(b[0]))
      .map((row) => row[0]),
  );
  expect(op.profile.columnarRows).toBe(0);
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
  reach.beginHistory();
  reach.reach("fallback replay");
});

it("abort holds output, payload and grant until outstanding downstream readiness settles", async () => {
  const types = [INT, DBL],
    { governor, context } = await setup(types);
  let release!: () => void, entered!: () => void;
  const gate = new Promise<void>((resolve) => {
      release = resolve;
    }),
    blocked = new Promise<void>((resolve) => {
      entered = resolve;
    });
  const sink: Operator = {
    push: () => {
      entered();
      return "blocked";
    },
    ready: () => gate,
    finish: () => undefined,
    abort: () => undefined,
  };
  const op = new SortOperator(full(types), context, sink);
  for (const batch of batchesOf(types, [
    [2, 3],
    [1, 1],
  ]))
    if (op.push(batch) === "blocked") await op.ready();
  const finishing = op.finish();
  await blocked;
  const reason = new Error("cancel held gather");
  const allocated = context.grant.allocated;
  let closed = false;
  void op.closed.then(() => {
    closed = true;
  });
  op.abort(reason);
  await Promise.resolve();
  const held = { closed, allocated: context.grant.allocated, used: governor.usedBytes };
  release();
  await expect(finishing).rejects.toBe(reason);
  await op.closed;
  expect(held.closed).toBe(false);
  expect(held.allocated).toBe(allocated);
  expect(held.used).toBeGreaterThan(0);
  expect(governor.usedBytes).toBe(0);
  reach.beginHistory();
  reach.reach("abort fence");
});

it("the external fallback also keeps borrowed output charged after abort", async () => {
  const types = [INT, DBL],
    { governor, context } = await setup(types);
  let release!: () => void, entered!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const blocked = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const sink: Operator = {
    push: () => {
      entered();
      return "blocked";
    },
    ready: () => gate,
    finish: () => undefined,
    abort: () => undefined,
  };
  const op = new SortOperator(full(types), context, sink);
  for (const batch of batchesOf(types, [
    [2, 3],
    [1, 1],
  ]))
    if (op.push(batch) === "blocked") await op.ready();
  await op.reclaim();
  const finishing = op.finish();
  await blocked;
  const reason = new Error("cancel fallback output");
  const allocated = context.grant.allocated;
  let closed = false;
  void op.closed.then(() => {
    closed = true;
  });
  op.abort(reason);
  await Promise.resolve();
  const held = { closed, allocated: context.grant.allocated, used: governor.usedBytes };
  release();
  await expect(finishing).rejects.toBe(reason);
  await op.closed;
  expect(held.closed).toBe(false);
  expect(held.allocated).toBe(allocated);
  expect(held.used).toBeGreaterThan(0);
  expect(governor.usedBytes).toBe(0);
});

it("checks the existing task deadline before arrival and aborts on resume", async () => {
  const types = [INT, DBL],
    { governor, context } = await setup(types);
  let now = 0;
  const clock = new TaskClock(() => now);
  clock.begin();
  now = 4;
  const op = new SortOperator(full(types), { ...context, clock }, new CollectSink());
  const batch = batchesOf(types, [[1, 2]])[0];
  if (!batch) throw new Error("input missing");
  expect(op.push(batch)).toBe("blocked");
  const reason = new Error("cancel arrival slice");
  op.abort(reason);
  await expect(op.ready()).rejects.toBe(reason);
  await op.closed;
  expect(governor.usedBytes).toBe(0);
});

it("gathers each supported numeric payload layout without changing NULL validity", async () => {
  const bool: PhysicalType = { kind: "bool" },
    f32: PhysicalType = { kind: "float32" };
  const types = [INT, DBL, bool, f32],
    data: Row[] = [
      [3, 1, null, -0],
      [1, 0, true, 1.5],
      [2, -1, false, null],
    ];
  const { governor, context } = await setup(types),
    sink = new CollectSink(),
    op = new SortOperator(full(types), context, sink);
  const input = batchesOf([INT, DBL, bool, DBL], data).map((batch) => ({
    ...batch,
    columns: batch.columns.map((vector, c) => {
      if (c !== 3 || vector.form !== "flat" || vector.data.layout !== "f64") return vector;
      return {
        ...vector,
        type: f32,
        data: { layout: "f32" as const, values: Float32Array.from(vector.data.values) },
      };
    }),
  }));
  await drive(new ArraySource(input), op);
  await op.closed;
  expect(sink.rows).toEqual([data[2], data[1], data[0]]);
  expect(sink.rows[2]?.[3]).toBe(-0);
  expect(governor.usedBytes).toBe(0);
});

for (const phase of ["arrival", "sort scratch"] as const)
  it(`refusal during ${phase} replays the full input without duplicates`, async () => {
    const types = [INT, DBL],
      data: Row[] = Array.from({ length: 2991 + 2048 }, (_, i) => [i, i % 37]);
    const { governor, spill, grant, context } = await setup(types),
      sink = new CollectSink();
    let refuse = false,
      refused = 0;
    const controlled = new Proxy(grant, {
      get(target, property) {
        if (property === "allocate")
          return (bytes: number) => {
            if (refuse) {
              refuse = false;
              refused++;
              return false;
            }
            return target.allocate(bytes);
          };
        const value: unknown = Reflect.get(target, property);
        return typeof value === "function" ? (value.bind(target) as unknown) : value;
      },
    });
    const op = new SortOperator(full(types), { ...context, grant: controlled }, sink);
    const first = batchesOf(types, data.slice(0, 2991), { batchRows: 997 });
    for (const batch of first) if (op.push(batch) === "blocked") await op.ready();
    // Arrival crosses a partially filled store chunk; a refusal must replay the complete
    // borrowed batch, not just the prefix already appended before allocation failed.
    if (phase === "arrival") refuse = true;
    for (const batch of batchesOf(types, data.slice(2991)))
      if (op.push(batch) === "blocked") await op.ready();
    if (phase === "sort scratch") refuse = true;
    await op.finish();
    await op.closed;
    expect(refused).toBe(1);
    expect(op.profile.columnarRows).toBe(0);
    expect(sink.rows.map((row) => row[0])).toEqual(
      data
        .slice()
        .sort((a, b) => Number(a[1]) - Number(b[1]) || Number(a[0]) - Number(b[0]))
        .map((row) => row[0]),
    );
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
    reach.beginHistory();
    reach.reach(`${phase} refusal`);
  });

it("combined wide payloads use the general row construction contract", async () => {
  const types = [INT, DBL, TEXT, TEXT],
    data: Row[] = [
      [2, 3, "a".repeat(180000), "b".repeat(180000)],
      [1, 1, "c".repeat(180000), "d".repeat(180000)],
    ];
  const { governor, spill, context } = await setup(types),
    sink = new CollectSink(),
    op = new SortOperator(full(types), context, sink);
  await drive(new ArraySource(batchesOf(types, data, { batchRows: 1 })), op);
  await op.closed;
  expect(sink.rows).toEqual([data[1], data[0]]);
  expect(op.profile.columnarRows).toBe(0);
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
});

it("resolved output readiness preserves the existing task deadline", async () => {
  const types = [INT, DBL],
    { governor, context } = await setup(types);
  let now = 0,
    taskRan = false;
  const clock = new TaskClock(() => now);
  clock.begin();
  const sink: Operator = {
    push() {
      now = 4;
      return "blocked";
    },
    ready: () => Promise.resolve(),
    finish: () => undefined,
    abort: () => undefined,
  };
  const op = new SortOperator(full(types), { ...context, clock }, sink);
  for (const batch of batchesOf(types, [
    [2, 3],
    [1, 1],
  ]))
    if (op.push(batch) === "blocked") await op.ready();
  const turn = new Promise<void>((resolve) =>
    setImmediate(() => {
      taskRan = true;
      resolve();
    }),
  );
  await op.finish();
  await op.closed;
  const observed = taskRan;
  await turn;
  expect(observed).toBe(true);
  expect(governor.usedBytes).toBe(0);
  reach.beginHistory();
  reach.reach("resolved readiness preserves deadline");
});

it("governor-requested fallback reclaim keeps its buffer charged through cancellation", async () => {
  const { ColumnarSortOperator } = await import("./sort-columnar.js");
  const types = [INT, DBL],
    { governor, grant, context } = await setup(types);
  const capture: { request?: (bytes: number) => unknown } = {};
  const controlled = new Proxy(grant, {
    get(target, property) {
      if (property === "onReclaimRequest")
        return (callback: (bytes: number) => unknown) => {
          capture.request = callback;
          target.onReclaimRequest(callback);
        };
      const value: unknown = Reflect.get(target, property);
      return typeof value === "function" ? (value.bind(target) as unknown) : value;
    },
  });
  let release!: () => void, entered!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const blocked = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const reason = new Error("cancel fallback reclaim");
  const op = new ColumnarSortOperator(
    full(types),
    { ...context, grant: controlled },
    new CollectSink(),
    sortMinimumBytes(types),
    (ctx) => {
      expect(ctx.grant.allocate(1024)).toBe(true);
      let page: Uint8Array | null = new Uint8Array(1024),
        freed = false;
      const reclaim = async () => {
        // The real sorter can have no spill files yet: closed alone is not a work fence.
        const borrowed = page;
        entered();
        try {
          await gate;
          ctx.signal?.throwIfAborted();
          expect(borrowed?.length).toBe(1024);
        } finally {
          page = null;
        }
      };
      ctx.grant.onReclaimRequest(reclaim);
      return {
        checkpoint: () => Promise.reject(new Error("unused checkpoint fake")),
        profile: {
          mode: "keys",
          rowsIn: 0,
          rowsOut: 0,
          rejected: 0,
          compactions: 0,
          runs: 0,
          mergePasses: 0,
          spilledBytes: 0,
          peakHeldBytes: 0,
          loans: 0,
          operandBytes: 0,
          thresholdBytes: 0,
        },
        closed: Promise.resolve(),
        reclaim,
        push: () => "ready",
        ready: () => Promise.resolve(),
        finish: () => undefined,
        discard: () => undefined,
        abort() {
          if (!freed) {
            freed = true;
            ctx.grant.free(1024);
            ctx.grant.close();
          }
        },
      };
    },
  );
  await op.reclaim();
  const pending = Promise.resolve(capture.request?.(1024));
  await blocked;
  const allocated = grant.allocated;
  let closed = false;
  void op.closed.then(() => {
    closed = true;
  });
  op.abort(reason);
  await new Promise<void>((resolve) => setImmediate(resolve));
  const held = { closed, allocated: grant.allocated };
  release();
  await expect(pending).rejects.toBe(reason);
  await op.closed;
  expect(held.closed).toBe(false);
  expect(held.allocated).toBe(allocated);
  expect(governor.usedBytes).toBe(0);
  reach.beginHistory();
  reach.reach("fallback reclaim fence");
});
