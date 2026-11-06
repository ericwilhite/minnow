/** Governor rule 4: reserve exact arithmetic before allocating it; drop references before refund. */
import { afterAll, expect, it, vi } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import * as frames from "./window-frames.js";
import { Charge } from "./spill-pool.js";
import type { PhysicalType } from "../types.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ArraySource, CollectSink, batchesOf, BIG, INT, type Row } from "./join-fixtures.js";
import { drive } from "./pipeline.js";
import { WindowOperator, windowMinimumBytes, type WindowSpec } from "./window.js";

const n = 10000;
const reach = createReach("next-window-bigint-accounting", [
  "resident:positive",
  "resident:negative",
  "streamed:positive",
  "streamed:negative",
  "resident:timestamp min",
  "resident:timestamp max",
  "resident:zoned timestamp",
  "resident:unzoned timestamp",
]);
afterAll(() => reach.assertReached());
async function run(
  value: bigint,
  deny = false,
  count = n,
  fn: "sum" | "min" | "max" = "sum",
  argType: PhysicalType = BIG,
) {
  const spec: WindowSpec = {
    types: [INT, argType],
    partitionBy: [],
    orderBy: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
    functions: [
      {
        fn,
        arg: 1,
        type: fn === "sum" ? { kind: "decimal", precision: 38, scale: 0 } : argType,
        frame: null,
      },
    ],
  };
  const governor = new Governor(64 << 20);
  const grant = await governor.admit({
    label: "big",
    minimumBytes: windowMinimumBytes(spec.types),
    spillable: true,
  });
  const spill = new MemorySpillService();
  const sink = new CollectSink();
  const op = new WindowOperator(spec, { grant, spill, queryId: 1 }, sink);
  const allocations: number[] = [];
  // Forwarding spy restores the original receiver with .call(this, ...).
  // eslint-disable-next-line @typescript-eslint/unbound-method
  const allocate = Charge.prototype.allocate;
  let denied = false;
  const allocation = vi.spyOn(Charge.prototype, "allocate").mockImplementation(function (
    this: Charge,
    bytes,
  ) {
    allocations.push(bytes);
    if (deny && !denied && bytes >= (2 * count + 1) * 48 && bytes < 2 * count * 48 + 1024) {
      denied = true;
      return false;
    }
    return allocate.call(this, bytes);
  });
  const atKernel: number[] = [];
  const owners: Array<{ big: Array<bigint[] | null> }> = [];
  // eslint-disable-next-line @typescript-eslint/unbound-method
  const free = Charge.prototype.free;
  const refund = vi.spyOn(Charge.prototype, "free").mockImplementation(function (
    this: Charge,
    bytes,
  ) {
    if (bytes === (2 * count + 1) * 48)
      for (const owner of owners) expect(owner.big.every((x) => x === null)).toBe(true);
    return free.call(this, bytes);
  });
  const kernel = frames.aggregate;
  const aggregate = vi.spyOn(frames, "aggregate").mockImplementation(function* (...args) {
    atKernel.push(grant.allocated);
    if (args[9] !== null && args[9] !== undefined)
      owners.push(args[9] as frames.AggregateScratch & { big: Array<bigint[] | null> });
    yield* kernel(...args);
  });
  try {
    const data: Row[] = Array.from({ length: count }, (_, i) => [i, value]);
    await drive(
      new ArraySource(batchesOf(spec.types, data, { batchRows: 97, int64: "int64" })),
      op,
    );
    await op.closed;
    expect(String(sink.rows[count - 1]?.[2])).toBe(
      String(fn === "sum" ? value * BigInt(count) : value),
    );
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
    for (const owner of owners) expect(owner.big.every((x) => x === null)).toBe(true);
    return { allocations, profile: op.profile, denied, atKernel };
  } finally {
    allocation.mockRestore();
    aggregate.mockRestore();
    refund.mockRestore();
  }
}
it("reserves bigint prefixes/results when individually safe inputs have an unsafe sum", async () => {
  const small = await run(1n);
  const large = await run(1n << 52n);
  expect(
    large.allocations.reduce((a, b) => a + b, 0) - small.allocations.reduce((a, b) => a + b, 0),
  ).toBeGreaterThanOrEqual((2 * n + 1) * 48);
}, 120000);
it("streams after exact-arithmetic reservation refusal and still returns an exact sum", async () => {
  const result = await run(1n << 52n, true);
  expect(result.denied).toBe(true);
  expect(result.profile.streamedPartitions).toBe(1);
}, 120000);

for (const seed of seedsFor("next-window-bigint-accounting", [0xb17, 0x87]))
  it(`safe-input promotion accounting seed ${String(seed)}`, async () => {
    const random = mulberry32(seed);
    for (const deny of [false, true])
      for (const sign of [-1n, 1n]) {
        const count = 97 + Math.floor(random() * 200);
        const value = sign * (1n << BigInt(50 + Math.floor(random() * 3)));
        const small = await run(1n, false, count);
        const large = await run(value, deny, count);
        expect(
          large.allocations.reduce((a, b) => a + b, 0) -
            small.allocations.reduce((a, b) => a + b, 0),
        ).toBeGreaterThanOrEqual((2 * count + 1) * 48);
        expect(large.denied).toBe(deny);
        reach.beginHistory();
        reach.reach(`${deny ? "streamed" : "resident"}:${sign < 0n ? "negative" : "positive"}`);
      }
    for (const zoned of [false, true])
      for (const fn of ["min", "max"] as const) {
        const count = 97 + Math.floor(random() * 200);
        const type: PhysicalType = { kind: "timestamp", zoned };
        const small = await run(1n, false, count, fn, type);
        const large = await run((1n << 60n) + 1n, false, count, fn, type);
        // Net allocation at kernel entry excludes already-refunded representation growth.
        expect((large.atKernel[0] ?? 0) - (small.atKernel[0] ?? 0)).toBeGreaterThanOrEqual(
          2 * count * 48,
        );
        reach.beginHistory();
        reach.reach(`resident:timestamp ${fn}`);
        reach.reach(`resident:${zoned ? "zoned" : "unzoned"} timestamp`);
      }
  }, 120000);
