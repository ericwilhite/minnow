/** Governor rule 4: native backing capacities are paid before construction, including overlap. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_DELTA_ACCOUNTING_STATES } from "../../testing/reach.js";
import { taskClock, beginTask } from "../exec/pipeline.js";
import { Governor } from "../memory/governor.js";
import { MetadataBudget } from "./budget.js";
import { need, type Charge } from "./bytes.js";
import { KeyDelta } from "./key-hash.js";

const reach = createReach("native delta allocation ownership", WRITE_DELTA_ACCOUNTING_STATES);
function ledger() {
  let bytes = 0,
    peak = 0,
    limit = Infinity;
  const charge: Charge = {
    allocate(n) {
      if (bytes + n > limit) return false;
      bytes += n;
      peak = Math.max(peak, bytes);
      return true;
    },
    free(n) {
      bytes -= n;
      expect(bytes, "no double release").toBeGreaterThanOrEqual(0);
    },
  };
  return {
    charge,
    get bytes() {
      return bytes;
    },
    get peak() {
      return peak;
    },
    set limit(n: number) {
      limit = n;
    },
  };
}
/** Observe actual native constructors, not KeyDelta's counters or its charge callbacks. */
async function nativeAllocations(
  work: () => unknown,
  observe: (kind: string, bytes: number) => void,
): Promise<void> {
  const fromDelta = () => new Error().stack?.includes("/write/key-hash.") === true;
  for (const [name, ctor] of [
    ["Int32Array", Int32Array],
    ["Float64Array", Float64Array],
    ["Uint8Array", Uint8Array],
  ] as const) {
    vi.stubGlobal(
      name,
      new Proxy(ctor, {
        construct(target, args) {
          if (fromDelta()) {
            const input = args[0] as unknown;
            const length =
              typeof input === "number"
                ? input
                : ArrayBuffer.isView(input) && "length" in input
                  ? Number(input.length)
                  : 0;
            observe(name, length * target.BYTES_PER_ELEMENT);
          }
          return Reflect.construct(target, args, target) as object;
        },
      }),
    );
  }
  // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the original receiver via call below.
  const original = Uint8Array.prototype.slice;
  vi.spyOn(Uint8Array.prototype, "slice").mockImplementation(function (
    this: Uint8Array,
    start = 0,
    end = this.length,
  ) {
    if (fromDelta()) observe("Uint8Array", Math.max(0, end - start));
    return original.call(this, start, end);
  });
  try {
    await work();
  } finally {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  }
}

it("pays the initial arrays before field constructors run", async () => {
  reach.beginHistory();
  const paid = ledger();
  let actual = 0,
    delta: KeyDelta | undefined;
  await nativeAllocations(
    () => {
      delta = new KeyDelta(paid.charge);
      expect(actual).toBeGreaterThan(0);
      expect(paid.bytes).toBe(actual);
      reach.reach("constructor precharge");
    },
    (_kind, bytes) => {
      actual += bytes;
      expect(paid.bytes, "precharge at initial native constructor").toBeGreaterThanOrEqual(actual);
    },
  );
  delta?.releaseCharge();
  delta?.releaseCharge();
  expect(paid.bytes).toBe(0);
});
it.each([1, 6, 11, 12])("refunds a constructor failure at initial array %i", async (at) => {
  reach.beginHistory();
  const paid = ledger(),
    stopped = new Error("initial native constructor failed");
  let seen = 0;
  await nativeAllocations(
    () => {
      expect(() => new KeyDelta(paid.charge)).toThrow(stopped);
    },
    () => {
      if (++seen === at) throw stopped;
    },
  );
  expect(paid.bytes, "failed initial construction refunds its reservation").toBe(0);
  reach.reach("initial constructor failure");
});
for (const seed of seedsFor("next-write-delta-accounting", [0xb3c1]))
  it(`pays actual traversal capacities and resize overlap (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          count: fc.constantFrom(64, 128, 1024, 16384, 65536),
          unique: fc.boolean(),
          reverse: fc.boolean(),
          failVisitor: fc.boolean(),
        }),
        async (s) => {
          reach.beginHistory();
          const paid = ledger(),
            delta = new KeyDelta(paid.charge),
            key = new Uint8Array(8);
          for (let k = 0; k < s.count; k++) {
            if (s.unique) new DataView(key.buffer).setUint32(0, s.reverse ? s.count - k : k, false);
            delta.add(key, 0, key.length, k + 1, 1, false);
          }
          const base = paid.bytes,
            retained = delta.bytes,
            stopped = new Error("visitor stopped");
          let order = 0,
            scratch = 0,
            constructors = 0,
            independentPeak = 0,
            visited = 0;
          try {
            await nativeAllocations(
              async () => {
                const walk = async () => {
                  await delta.forEachSorted(Infinity, () => {
                    if (++visited === s.count >>> 1 && s.failVisitor) throw stopped;
                  });
                };
                if (s.failVisitor) {
                  await expect(walk()).rejects.toBe(stopped);
                  reach.reach("visitor failure");
                } else {
                  await walk();
                  expect(visited).toBe(s.count);
                }
              },
              (kind, bytes) => {
                if (kind !== "Int32Array") return;
                const live = constructors++ === 0 ? bytes : order + scratch + bytes;
                if (constructors === 1) {
                  order = bytes;
                  reach.reach("order buffer");
                } else {
                  if (scratch > 0) reach.reach("chain growth overlap");
                  scratch = bytes;
                }
                independentPeak = Math.max(independentPeak, live);
                expect(
                  paid.bytes - base,
                  "precharge at traversal native constructor",
                ).toBeGreaterThanOrEqual(live);
              },
            );
            expect(constructors).toBeGreaterThan(0);
            expect(paid.peak).toBeGreaterThanOrEqual(base + independentPeak);
            expect(paid.bytes).toBe(base);
            expect(delta.bytes).toBe(retained);
            if (s.count === 65536) reach.reach("full memtable");
          } finally {
            delta.releaseCharge();
            delta.releaseCharge();
          }
          expect(paid.bytes).toBe(0);
        },
      ),
      { seed, numRuns: 40 },
    );
  }, 120_000);

for (const seed of seedsFor("next-write-delta-range-accounting", [0xb3c2]))
  it(`owns cached range copies and refunds failed replacements (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 128 }), async (width) => {
        reach.beginHistory();
        const paid = ledger(),
          delta = new KeyDelta(paid.charge);
        const first = new Uint8Array(width).fill(2),
          last = new Uint8Array(width).fill(8);
        delta.add(first, 0, first.length, 1, 1, false);
        delta.add(last, 0, last.length, 2, 1, false);
        let base = paid.bytes,
          copied = 0,
          watching = false;
        try {
          await nativeAllocations(
            () => {
              const range = () => {
                watching = true;
                try {
                  return delta.keyRange();
                } finally {
                  watching = false;
                }
              };
              const expectRange = (min: Uint8Array, max: Uint8Array) => {
                const value = need(range());
                expect(value.min).toEqual(min);
                expect(value.max).toEqual(max);
                const count = copied;
                expect(range()).toBe(value);
                expect(copied).toBe(count);
                return value.min.byteLength + value.max.byteLength;
              };
              const oldBytes = expectRange(first, last);
              expect(paid.bytes).toBe(base + oldBytes);
              reach.reach("range cache");
              const lower = new Uint8Array(width + 3).fill(1);
              delta.add(lower, 0, lower.length, 3, 1, false);
              base = paid.bytes;
              copied = 0;
              const nextBytes = expectRange(lower, last);
              expect(paid.bytes).toBe(base + nextBytes - oldBytes);
              reach.reach("range replacement overlap");
              const upper = new Uint8Array(width + 5).fill(9);
              delta.add(upper, 0, upper.length, 4, 1, false);
              base = paid.bytes;
              copied = 0;
              paid.limit = base;
              expect(range()).toBeNull();
              expect(copied).toBe(0);
              expect(paid.bytes).toBe(base - nextBytes);
              expect(range()).toBeNull();
              expect(paid.bytes).toBe(base - nextBytes);
              paid.limit = Infinity;
              base = paid.bytes;
              copied = 0;
              const funded = expectRange(lower, upper);
              expect(paid.bytes).toBe(base + funded);
              reach.reach("denial before allocation");
            },
            (kind, bytes) => {
              if (!watching || kind !== "Uint8Array") return;
              copied += bytes;
              expect(
                paid.bytes - base,
                "precharge at range native constructor",
              ).toBeGreaterThanOrEqual(copied);
            },
          );
        } finally {
          delta.releaseCharge();
        }
        expect(paid.bytes).toBe(0);
      }),
      { seed, numRuns: 40 },
    );
  });

for (const seed of seedsFor("next-write-delta-growth-accounting", [0xb3c3]))
  it(`retains old arrays through growth and refunds constructor failures (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({ keys: fc.boolean(), at: fc.integer({ min: 1, max: 5 }) }),
        async (s) => {
          reach.beginHistory();
          const paid = ledger(),
            delta = new KeyDelta(paid.charge),
            same = Uint8Array.of(1);
          for (let k = 0; k < 16; k++) {
            const key = s.keys ? Uint8Array.of(k + 1) : same;
            delta.add(key, 0, 1, k + 1, 1, false);
          }
          const key = s.keys ? Uint8Array.of(17) : same,
            base = paid.bytes,
            stopped = new Error("native constructor failed");
          let actual = 0,
            seen = 0;
          try {
            await nativeAllocations(
              () => {
                expect(() => delta.add(key, 0, 1, 17, 1, false)).toThrow(stopped);
              },
              (_kind, bytes) => {
                actual += bytes;
                expect(
                  paid.bytes,
                  "old and new retained arrays overlap at native constructor",
                ).toBeGreaterThanOrEqual(base + actual);
                if (++seen === s.at) throw stopped;
              },
            );
            expect(paid.bytes).toBe(base);
            expect(delta.entryCount).toBe(16);
            delta.add(key, 0, 1, 17, 1, false);
            const rows: number[] = [];
            await delta.forEachSorted(Infinity, (_a, _s, _e, rowId) => rows.push(rowId));
            expect(rows).toEqual(Array.from({ length: 17 }, (_, k) => k + 1));
            reach.reach("retained growth overlap");
            reach.reach("constructor failure refund");
            if (!s.keys && s.at === 5) reach.reach("last entry array failure");
            if (s.keys && s.at === 5) reach.reach("last key array failure");
          } finally {
            delta.releaseCharge();
          }
          expect(paid.bytes).toBe(0);
        },
      ),
      { seed, numRuns: 40 },
    );
  });

it.each([false, true])(
  "keeps retained capacities charged through native growth (keys %s)",
  async (keys) => {
    const paid = ledger(),
      delta = new KeyDelta(paid.charge),
      same = Uint8Array.of(1);
    for (let k = 0; k < 16; k++) {
      const key = keys ? Uint8Array.of(k + 1) : same;
      delta.add(key, 0, 1, k + 1, 1, false);
    }
    const base = paid.bytes,
      key = keys ? Uint8Array.of(17) : same;
    let seen = 0,
      made = 0;
    try {
      await nativeAllocations(
        () => delta.add(key, 0, 1, 17, 1, false),
        (_kind, bytes) => {
          // The first replacement group is five key arrays or five entry arrays. A later group
          // may already have retired this group's old capacity, so it has its own witness above.
          if (seen++ >= 5) return;
          made += bytes;
          expect(
            paid.bytes,
            "old and new retained arrays overlap at native constructor",
          ).toBeGreaterThanOrEqual(base + made);
        },
      );
      expect(delta.entryCount).toBe(17);
    } finally {
      delta.releaseCharge();
    }
    expect(paid.bytes).toBe(0);
  },
);

it("retains source arrays until a suspended traversal settles", async () => {
  reach.beginHistory();
  const paid = ledger(),
    delta = new KeyDelta(paid.charge),
    key = Uint8Array.of(1);
  for (let k = 0; k < 4096; k++) delta.add(key, 0, 1, k + 1, 1, false);
  let now = 0,
    visited = 0;
  vi.spyOn(taskClock, "now").mockImplementation(() => ++now);
  beginTask();
  now += 6;
  let pending: ReturnType<KeyDelta["forEachSorted"]> = undefined;
  try {
    const base = paid.bytes;
    pending = delta.forEachSorted(Infinity, () => {
      visited++;
    });
    expect(pending).toBeInstanceOf(Promise);
    delta.releaseCharge();
    delta.releaseCharge();
    expect(paid.bytes, "source arrays stay charged until traversal settles").toBeGreaterThanOrEqual(
      base,
    );
    await pending;
    expect(visited).toBe(4096);
    expect(paid.bytes).toBe(0);
    delta.releaseCharge();
    expect(paid.bytes).toBe(0);
    reach.reach("release while suspended");
  } finally {
    if (pending instanceof Promise) await pending.catch(() => undefined);
    vi.restoreAllMocks();
    beginTask();
    delta.releaseCharge();
  }
});

it("waits through workspace pressure using the publication metadata budget", async () => {
  reach.beginHistory();
  const governor = new Governor(32 * 1024 * 1024),
    grant = await governor.admit({ label: "delta", minimumBytes: 0, spillable: true }),
    metadata = await governor.admit({ label: "metadata", minimumBytes: 0, spillable: true }),
    reader = await governor.admit({ label: "reader", minimumBytes: 0, spillable: true }),
    budget = new MetadataBudget(metadata, () => 0, governor.capBytes),
    delta = new KeyDelta(grant),
    key = Uint8Array.of(1);
  for (let k = 0; k < 16384; k++) delta.add(key, 0, 1, k + 1, 1, false);
  const held = governor.capBytes - governor.reserveBytes - governor.usedBytes - 1;
  expect(reader.allocate(held)).toBe(true);
  let realReader: Uint8Array | null = new Uint8Array(held).fill(7),
    released = false,
    visited = 0,
    waits = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const grow = metadata.tryGrow.bind(metadata);
  metadata.tryGrow = (bytes) => {
    const ok = grow(bytes);
    if (!ok && timer === undefined) {
      waits++;
      timer = setTimeout(() => {
        expect(realReader?.[0]).toBe(7);
        realReader = null;
        reader.close();
        released = true;
      }, 5);
    }
    return ok;
  };
  try {
    await budget.retry(
      () =>
        delta.forEachSorted(
          Infinity,
          () => {
            visited++;
          },
          null,
          null,
          budget,
        ),
      () => undefined,
    );
    expect(visited).toBe(16384);
    expect(released).toBe(true);
    expect(waits).toBeGreaterThan(0);
    expect(metadata.allocated).toBe(0);
    expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
    reach.reach("publication workspace pressure");
  } finally {
    clearTimeout(timer);
    reader.close();
    delta.releaseCharge();
    grant.close();
    metadata.close();
  }
  expect(governor.usedBytes).toBe(0);
});
it("reaches independent delta accounting states", () => reach.assertReached());
