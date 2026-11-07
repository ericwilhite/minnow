import fc from "fast-check";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { expect, it } from "vitest";
import { Governor, ValueTooLargeError } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import type { Batch, FlatVector } from "../vector/vector.js";
import { int64At } from "../vector/int64.js";
import { isValid } from "../vector/validity.js";
import {
  I32,
  I64,
  TEXT,
  decimal,
  decimals,
  numbers,
  texts,
  valueAt,
} from "./kernels/test-support.js";
import { ResultChannel } from "./result.js";
import { ResultSink, type ResultSinkOptions } from "./result-sink.js";

async function setup(
  types: readonly PhysicalType[],
  windowBytes = 64,
  options: ResultSinkOptions = {},
) {
  const governor = new Governor(8 * 1024 * 1024);
  const grant = await governor.admit({ label: "result", minimumBytes: 1024, spillable: true });
  const channel = new ResultChannel(
    types.map((type, i) => ({ name: String(i), type, nullable: true })),
    { windowBytes },
  );
  return {
    governor,
    grant,
    channel,
    sink: new ResultSink(types, grant, channel, 17, { now: () => 0, ...options }),
  };
}

it("compacts selections into owned transferable frames, retaining input until copied", async () => {
  const h = await setup([I32, TEXT]);
  const values = Array.from({ length: 40 }, (_, i) => i);
  const vector = numbers(I32, values);
  const selected = Uint16Array.from(values.filter((i) => i % 2 === 0).reverse());
  const batch: Batch = {
    length: values.length,
    columns: [vector, texts(values.map(String))],
    selection: { rows: selected, count: selected.length },
  };
  let released = 0;
  const producer = (async () => {
    if (
      h.sink.push(batch, {
        release: () => {
          released++;
          if (vector.data.layout === "i32") vector.data.values.fill(-1);
        },
      }) === "blocked"
    )
      await h.sink.ready();
    await h.sink.finish();
  })();
  const actual: unknown[][] = [];
  for (;;) {
    const frame = await h.channel.next();
    if (frame === null) break;
    expect(frame.requiresLsn).toBe(17);
    const received = structuredClone(frame.payload, { transfer: [...frame.transfer] });
    for (let row = 0; row < received.rowCount; row++)
      actual.push(
        received.columns.map((column, c) => {
          if (column.form !== "flat") throw new Error("the sink emits flat frames");
          const type = h.channel.columns[c]?.type;
          if (type === undefined) throw new Error("missing result type");
          return valueAt({ ...column, type }, row);
        }),
      );
    frame.release();
  }
  await producer;
  await h.channel.delivered;
  expect(actual).toEqual([...selected].map((i) => [i, String(i)]));
  expect(released).toBe(1);
  expect(h.sink.rowCount).toBe(selected.length);
  expect(h.grant.allocated).toBe(0);
  h.grant.close();
  expect(h.governor.usedBytes).toBe(0);
});

it("discards handed-out frames and returns the pending input on cancellation", async () => {
  const h = await setup([I32]);
  let released = 0;
  expect(
    h.sink.push(
      {
        length: 100,
        columns: [
          numbers(
            I32,
            Array.from({ length: 100 }, (_, i) => i),
          ),
        ],
        selection: null,
      },
      {
        release: () => {
          released++;
        },
      },
    ),
  ).toBe("blocked");
  const frame = h.channel.poll();
  expect(frame).toBeTruthy();
  const ready = h.sink.ready();
  const reason = new Error("stopped");
  h.sink.discard(reason);
  h.channel.cancel(reason);
  await expect(ready).rejects.toThrow("stopped");
  expect(released).toBe(1);
  expect(h.grant.allocated).toBe(0);
  frame?.release();
  expect(h.grant.allocated).toBe(0);
  h.grant.close();
  expect(h.governor.usedBytes).toBe(0);
});

it("preserves int64 words, wide coefficients, scales and NULLs through transfers", async () => {
  const type = decimal(null, null);
  const h = await setup([I64, type]);
  const beyondSafe = (1n << 53n) + 1n;
  const wide = (1n << 5000n) + 7n;
  const integers: FlatVector = {
    form: "flat",
    type: I64,
    length: 3,
    validity: Uint32Array.of(3),
    data: {
      layout: "int64",
      values: { repr: "int64", values: BigInt64Array.of(beyondSafe, -beyondSafe, 0n) },
    },
  };
  const producer = (async () => {
    if (
      h.sink.push({
        length: 3,
        columns: [integers, decimals(type, [[wide, 2], null, [-wide, 4]], "wide")],
        selection: null,
      }) === "blocked"
    )
      await h.sink.ready();
    await h.sink.finish();
  })();
  const actual: unknown[][] = [];
  for (;;) {
    const frame = await h.channel.next();
    if (frame === null) break;
    const received = structuredClone(frame.payload, { transfer: [...frame.transfer] });
    const [integer, numeric] = received.columns;
    if (
      integer?.form !== "flat" ||
      integer.data.layout !== "int64" ||
      numeric?.form !== "flat" ||
      numeric.data.layout !== "decimal" ||
      numeric.data.coefficients.repr !== "wide"
    )
      throw new Error("wrong result storage tier");
    for (let row = 0; row < received.rowCount; row++)
      actual.push([
        isValid(integer.validity, row) ? int64At(integer.data.values, row) : null,
        isValid(numeric.validity, row)
          ? [
              numeric.data.coefficients.values[row],
              numeric.data.scale ?? numeric.data.scales?.[row],
            ]
          : null,
      ]);
    frame.release();
  }
  await producer;
  await h.channel.delivered;
  expect(actual).toEqual([
    [beyondSafe, [wide, 2]],
    [-beyondSafe, null],
    [null, [-wide, 4]],
  ]);
  expect(h.grant.allocated).toBe(0);
  h.grant.close();
  expect(h.governor.usedBytes).toBe(0);
});

it("coalesces zero-column rows into one frame with bounded metadata", async () => {
  const h = await setup([]);
  for (let i = 0; i < 100; i++)
    expect(h.sink.push({ length: 10, columns: [], selection: null })).toBe("ready");
  expect(h.channel.poll()).toBeUndefined();
  await h.sink.finish();
  const frame = h.channel.poll();
  expect(frame?.payload.rowCount).toBe(1000);
  expect(frame?.payload.columns).toEqual([]);
  expect(h.channel.poll()).toBeNull();
  frame?.release();
  await h.channel.delivered;
  expect(h.grant.allocated).toBe(0);
  h.grant.close();
});

async function pressured(types: readonly PhysicalType[] = [TEXT]) {
  const governor = new Governor(4 * 1024 * 1024);
  const hog = await governor.admit({
    label: "other",
    minimumBytes: 3 * 1024 * 1024 - 1024,
    spillable: false,
  });
  const grant = await governor.admit({ label: "result", minimumBytes: 1024, spillable: true });
  const channel = new ResultChannel(
    types.map((type, i) => ({ name: String(i), type, nullable: false })),
    { windowBytes: 64 },
  );
  const sink = new ResultSink(types, grant, channel, 0, { now: () => 0 });
  return { governor, hog, grant, channel, sink };
}

it("uses one progress loan per oversized frame and returns it before the next", async () => {
  const h = await pressured();
  const original = h.grant.borrow.bind(h.grant);
  let loans = 0;
  h.grant.borrow = (bytes, signal) => {
    loans++;
    return original(bytes, signal);
  };
  const expected = ["a".repeat(200_000), "b".repeat(200_000)];
  const producer = (async () => {
    expect(h.sink.push({ length: 2, columns: [texts(expected)], selection: null })).toBe("blocked");
    await h.sink.ready();
    await h.sink.finish();
  })();
  const actual: unknown[] = [];
  for (;;) {
    const frame = await h.channel.next();
    if (frame === null) break;
    expect(h.grant.loaned).toBeGreaterThan(0);
    const column = frame.payload.columns[0];
    if (column?.form !== "flat") throw new Error("missing frame column");
    actual.push(valueAt({ ...column, type: TEXT }, 0));
    frame.release();
    expect(h.grant.loaned).toBe(0);
  }
  await producer;
  await h.channel.delivered;
  expect(actual).toEqual(expected);
  expect(loans).toBe(2);
  expect(h.grant.allocated).toBe(0);
  expect(h.governor.peakBytes).toBeLessThanOrEqual(h.governor.capBytes);
  h.grant.close();
  h.hog.close();
  expect(h.governor.usedBytes).toBe(0);
});

it("refuses a single value beyond the reserve and unwinds its borrowed input", async () => {
  const h = await pressured();
  let released = 0;
  expect(
    h.sink.push(
      { length: 1, columns: [texts(["x".repeat(2 * h.governor.reserveBytes)])], selection: null },
      {
        release: () => {
          released++;
        },
      },
    ),
  ).toBe("blocked");
  await expect(h.sink.ready()).rejects.toBeInstanceOf(ValueTooLargeError);
  expect(released).toBe(1);
  expect(h.grant.allocated).toBe(0);
  h.channel.cancel();
  h.grant.close();
  h.hog.close();
  expect(h.governor.usedBytes).toBe(0);
});

it("cancels a queued result loan without touching another grant's loan", async () => {
  const h = await pressured();
  const blocker = await h.governor.admit({
    label: "reserve holder",
    minimumBytes: 0,
    spillable: true,
  });
  const loan = await blocker.borrow(h.governor.reserveBytes);
  let released = 0;
  expect(
    h.sink.push(
      { length: 1, columns: [texts(["x".repeat(200_000)])], selection: null },
      {
        release: () => {
          released++;
        },
      },
    ),
  ).toBe("blocked");
  const pending = h.sink.ready();
  const reason = new Error("queued cancellation");
  h.sink.discard(reason);
  h.channel.cancel(reason);
  await expect(pending).rejects.toThrow("queued cancellation");
  expect(released).toBe(1);
  expect(h.grant.allocated).toBe(0);
  expect(blocker.loaned).toBe(h.governor.reserveBytes);
  loan.repay();
  blocker.close();
  h.grant.close();
  h.hog.close();
  expect(h.governor.usedBytes).toBe(0);
});

it("unwinds a served loan when cancellation precedes its promise continuation", async () => {
  const h = await pressured();
  expect(h.sink.push({ length: 1, columns: [texts(["x".repeat(200_000)])], selection: null })).toBe(
    "blocked",
  );
  expect(h.grant.loaned).toBeGreaterThan(0);
  const reason = new Error("served cancellation");
  h.sink.discard(reason);
  h.channel.cancel(reason);
  h.grant.close();
  await expect(h.sink.ready()).rejects.toThrow("served cancellation");
  expect(h.grant.loaned).toBe(0);
  expect(h.grant.allocated).toBe(0);
  h.hog.close();
  expect(h.governor.usedBytes).toBe(0);
});

for (const seed of seedsFor("next-result-frame-rows", [0xf101, 0xf102]))
  it(`caps selected and zero-column frames without losing rows (${String(seed)})`, async () => {
    const reach = createReach(`result frame cap ${String(seed)}`, [
      "dense",
      "reverse",
      "zeroColumns",
      "nonPowerOfTwo",
    ] as const);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 13 }),
        fc.constantFrom("dense", "reverse", "zeroColumns"),
        async (cap, mode) => {
          reach.beginHistory();
          const h = await setup(mode === "zeroColumns" ? [] : [I32], 4096, { maxRows: cap });
          const values = Array.from({ length: 31 }, (_, i) => i);
          const ids = mode === "reverse" ? [...values].reverse() : values;
          const producer = (async () => {
            if (
              h.sink.push({
                length: 31,
                columns: mode === "zeroColumns" ? [] : [numbers(I32, values)],
                selection:
                  mode === "reverse" ? { rows: Uint16Array.from(ids), count: ids.length } : null,
              }) === "blocked"
            )
              await h.sink.ready();
            await h.sink.finish();
          })();
          const actual: unknown[] = [];
          let rows = 0;
          for (;;) {
            const frame = await h.channel.next();
            if (frame === null) break;
            expect(frame.payload.rowCount).toBeGreaterThan(0);
            expect(frame.payload.rowCount).toBeLessThanOrEqual(cap);
            rows += frame.payload.rowCount;
            const col = frame.payload.columns[0];
            if (col !== undefined) {
              if (col.form !== "flat") throw new Error("expected flat result");
              for (let row = 0; row < frame.payload.rowCount; row++)
                actual.push(valueAt({ ...col, type: I32 }, row));
            }
            frame.release();
          }
          await producer;
          await h.channel.delivered;
          expect(rows).toBe(31);
          if (mode !== "zeroColumns") expect(actual).toEqual(ids);
          expect(h.grant.allocated).toBe(0);
          h.grant.close();
          expect(h.governor.usedBytes).toBe(0);
          reach.reach(mode);
          if ((cap & (cap - 1)) !== 0) reach.reach("nonPowerOfTwo");
        },
      ),
      { seed, numRuns: 60 },
    );
    reach.assertReached();
  });

it("yields a task while splitting a batch into many tiny frames", async () => {
  let time = 0;
  const h = await setup([I32], 4096, { maxRows: 1, now: () => time });
  time = 5;
  let timer = false;
  const pendingTimer = new Promise<void>((resolve) =>
    setImmediate(() => {
      timer = true;
      resolve();
    }),
  );
  expect(
    h.sink.push({
      length: 31,
      columns: [
        numbers(
          I32,
          Array.from({ length: 31 }, (_, i) => i),
        ),
      ],
      selection: null,
    }),
  ).toBe("blocked");
  const first = h.channel.poll();
  expect(first?.payload.rowCount).toBe(1);
  first?.release();
  expect(h.channel.poll()).toBeUndefined();
  const producer = h.sink.ready().then(() => h.sink.finish());
  for (;;) {
    const frame = await h.channel.next();
    if (frame === null) break;
    frame.release();
  }
  await producer;
  expect(timer).toBe(true);
  await pendingTimer;
  await h.channel.delivered;
  h.grant.close();
  expect(h.governor.usedBytes).toBe(0);
});

for (const seed of seedsFor("next-result-large-row", [0xf201, 0xf202]))
  it(`loans actual oversized row bytes, preserving room for metadata (${String(seed)})`, async () => {
    const reach = createReach(`large result ${String(seed)}`, [
      "singleColumn",
      "windowColumns",
      "loanRepaid",
    ] as const);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 550000, max: 900000 }),
        fc.boolean(),
        async (length, windowColumns) => {
          reach.beginHistory();
          const types = windowColumns ? [I32, TEXT, I64] : [TEXT];
          const h = await pressured(types);
          const text = texts(["x".repeat(length)]);
          const columns = windowColumns
            ? [
                numbers(I32, [17]),
                text,
                {
                  form: "flat" as const,
                  type: I64,
                  length: 1,
                  validity: null,
                  data: {
                    layout: "int64" as const,
                    values: { repr: "int64" as const, values: BigInt64Array.of(1n) },
                  },
                },
              ]
            : [text];
          const producer = (async () => {
            if (h.sink.push({ length: 1, columns, selection: null }) === "blocked")
              await h.sink.ready();
            await h.sink.finish();
          })().catch((error: unknown) => {
            h.channel.fail(error);
            throw error;
          });
          producer.catch(() => undefined);
          try {
            const frame = await h.channel.next();
            expect(frame?.payload.rowCount).toBe(1);
            expect(h.grant.loaned).toBeGreaterThan(0);
            const column = frame?.payload.columns[windowColumns ? 1 : 0];
            if (column?.form !== "flat") throw new Error("missing text frame");
            expect(valueAt({ ...column, type: TEXT }, 0)).toBe("x".repeat(length));
            frame?.release();
            expect(h.grant.loaned).toBe(0);
            expect(await h.channel.next()).toBeNull();
            await producer;
            await h.channel.delivered;
            expect(h.grant.allocated).toBe(0);
            expect(h.governor.peakBytes).toBeLessThanOrEqual(h.governor.capBytes);
            reach.reach(windowColumns ? "windowColumns" : "singleColumn");
            reach.reach("loanRepaid");
          } finally {
            h.sink.discard(new Error("probe finished"));
            h.channel.cancel();
            await Promise.allSettled([producer, h.channel.delivered]);
            h.grant.close();
            h.hog.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-result-wide-frame", [0xf301, 0xf302]))
  it(`bounds whole frames across variable-width columns (${String(seed)})`, async () => {
    const reach = createReach(`wide result ${String(seed)}`, [
      "dense",
      "reverse",
      "multipleFrames",
    ] as const);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 6 }),
        fc.integer({ min: 80, max: 120 }),
        fc.integer({ min: 17, max: 67 }),
        fc.boolean(),
        async (width, length, count, reverse) => {
          reach.beginHistory();
          const h = await setup(
            Array.from({ length: width }, () => TEXT),
            2048,
          );
          const values = Array.from({ length: width }, (_, c) =>
            Array.from({ length: count }, (_, r) =>
              `${String(c)}:${String(r)}`.padEnd(length, "x"),
            ),
          );
          const ids = Array.from({ length: count }, (_, i) => i);
          if (reverse) ids.reverse();
          const producer = (async () => {
            if (
              h.sink.push({
                length: count,
                columns: values.map((column) => texts(column)),
                selection: reverse ? { rows: Uint16Array.from(ids), count } : null,
              }) === "blocked"
            )
              await h.sink.ready();
            await h.sink.finish();
          })().catch((error: unknown) => {
            h.channel.fail(error);
            throw error;
          });
          producer.catch(() => undefined);
          const actual: unknown[][] = [];
          let frames = 0;
          try {
            for (;;) {
              const frame = await h.channel.next();
              if (frame === null) break;
              try {
                // Even the widest individual row is < 2 KiB, so no oversized-row exception applies.
                expect(frame.byteLength).toBeLessThanOrEqual(2048);
                frames++;
                for (let row = 0; row < frame.payload.rowCount; row++)
                  actual.push(
                    frame.payload.columns.map((column) => {
                      if (column.form !== "flat") throw new Error("expected compact frame");
                      return valueAt({ ...column, type: TEXT }, row);
                    }),
                  );
              } finally {
                frame.release();
              }
            }
            await producer;
            await h.channel.delivered;
            expect(actual).toEqual(ids.map((row) => values.map((column) => column[row])));
            expect(frames).toBeGreaterThan(1);
            expect(h.grant.allocated).toBe(0);
            reach.reach(reverse ? "reverse" : "dense");
            reach.reach("multipleFrames");
          } finally {
            h.sink.discard(new Error("test finished"));
            h.channel.cancel();
            await Promise.allSettled([producer, h.channel.delivered]);
            h.grant.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 40 },
    );
    reach.assertReached();
  });
