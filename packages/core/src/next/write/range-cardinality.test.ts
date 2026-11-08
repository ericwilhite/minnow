/** Fixed-run-count cardinality law: only the shared cache may grow with directory cardinality. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_RANGE_CARDINALITY_STATES } from "../../testing/reach.js";
import type { KeyRange } from "../access/key-index.js";
import type { RunRef } from "../format/manifest.js";
import { beginTask } from "../exec/pipeline.js";
import { ByteReader, need } from "./bytes.js";
import { Harness } from "./test-support.js";
import { RunBuilder, runRef } from "./runs.js";
import { StreamingRangeCursor } from "./range-cursor.js";
import { DirectoryNavigation } from "./range-navigation.js";
import type { ViewIndex } from "./view.js";

const reach = createReach("cardinality-independent cursor state", WRITE_RANGE_CARDINALITY_STATES);
const rows = [1024, 16384, 131072] as const;
const measurements: Array<Record<string, unknown>> = [];
function key(n: number, width: number): Uint8Array {
  const out = new Uint8Array(width).fill(0x42);
  new DataView(out.buffer).setUint32(0, n, false);
  return out;
}
function reader(h: Harness, ref: RunRef): ViewIndex {
  return {
    def: { id: 1, unique: true },
    table: {
      snapshot: {
        check: () => undefined,
        watchCursor: () => () => undefined,
        bound: 1,
        shared: { space: h.space, governor: h.governor },
      },
      tableView: { memtables: [], runs: new Map([[1, [ref]]]) },
      locator: (row: number) => row,
    },
  } as unknown as ViewIndex;
}
/** Eviction, not a test-only cache reset: exercise the governor's ordinary pressure path. */
function evict(h: Harness): void {
  const bytes = h.space.stats.cache.residentBytes + h.space.stats.cache.poolBytes;
  const grant = need(
    h.governor.tryAdmit({
      label: "evict navigation metadata",
      minimumBytes: h.governor.capBytes - h.governor.reserveBytes - h.governor.usedBytes + bytes,
      spillable: false,
    }),
  );
  grant.close();
  expect(h.space.stats.cache.residentBytes).toBe(0);
}
async function measure(
  h: Harness,
  ref: RunRef,
  range: KeyRange,
  direction: "asc" | "desc",
  capacity: number,
) {
  let privateNative = 0,
    cacheNative = 0,
    cacheCharge = 0,
    directoryBytesRead = 0,
    credit = 0,
    varuints = 0,
    minimum = 0,
    privateCharge = 0;
  const directories = new Map<string, number>(),
    out = new Float64Array(capacity);
  const firstCaller = () => new Error().stack?.split("\n")[3] ?? "";
  const Uint8 = Uint8Array,
    Uint32 = Uint32Array,
    Float64 = Float64Array;
  const observe = (capacity: number, bytes: number, caller: string) => {
    if (caller.includes("range-navigation.ts") && capacity > 0) {
      expect(credit, "cache index charged before its native allocation").toBeGreaterThanOrEqual(
        bytes,
      );
      credit = 0;
      cacheNative += bytes;
    } else if (/range-(cursor|directory|input)\.ts/.test(caller)) privateNative += bytes;
  };
  const install = (name: string, Type: typeof Uint8 | typeof Uint32 | typeof Float64) =>
    vi.stubGlobal(
      name,
      new Proxy(Type, {
        construct(target, args: unknown[], newTarget) {
          if (typeof args[0] === "number")
            observe(args[0], args[0] * target.BYTES_PER_ELEMENT, firstCaller());
          return Reflect.construct(target, args, newTarget) as object;
        },
      }),
    );
  // eslint-disable-next-line @typescript-eslint/unbound-method -- called with the observed reader below.
  const varuint = ByteReader.prototype.varuint;
  const count = vi.spyOn(ByteReader.prototype, "varuint").mockImplementation(function (
    this: ByteReader,
  ) {
    varuints++;
    return varuint.call(this);
  });
  const originalRead = h.space.read.bind(h.space),
    originalAttach = h.space.attach?.bind(h.space) ?? (() => false),
    admit = h.governor.tryAdmit.bind(h.governor);
  const reads = vi.spyOn(h.space, "read").mockImplementation((refs, options) => {
    for (const r of refs)
      if (
        r.offset === ref.entries.offset ||
        r.offset === ref.fences.offset ||
        r.offset === ref.bloom.offset
      ) {
        directoryBytesRead += r.length;
        directories.set(
          `${String(r.offset)}:${String(r.checksum)}`,
          (directories.get(`${String(r.offset)}:${String(r.checksum)}`) ?? 0) + 1,
        );
      }
    return originalRead(refs, options);
  });
  const attach = vi.spyOn(h.space, "attach").mockImplementation((r, value, bytes, of) => {
    const before = h.governor.usedBytes,
      accepted = originalAttach(r, value, bytes, of);
    if (accepted && value instanceof DirectoryNavigation) {
      expect(h.governor.usedBytes - before).toBe(bytes);
      credit = bytes;
      cacheCharge += bytes;
    }
    return accepted;
  });
  const admission = vi.spyOn(h.governor, "tryAdmit").mockImplementation((options) => {
    const grant = admit(options);
    if (options.label === "write index range") {
      minimum = options.minimumBytes;
      if (grant !== null) {
        const allocate = grant.allocate.bind(grant);
        grant.allocate = (bytes) => {
          if (firstCaller().includes("range-cursor.ts")) privateCharge += bytes;
          return allocate(bytes);
        };
      }
    }
    return grant;
  });
  const cursor = new StreamingRangeCursor(reader(h, ref), range, direction);
  try {
    install("Uint8Array", Uint8);
    install("Uint32Array", Uint32);
    install("Float64Array", Float64);
    beginTask();
    const produced = await cursor.next(out);
    return {
      produced,
      values: Array.from(out.subarray(0, produced)),
      minimum,
      privateCharge,
      privateNative,
      cacheNative,
      cacheCharge,
      directoryBytesRead,
      varuints,
      directoryReads: [...directories.values()],
      cacheResident: h.space.stats.cache.residentBytes,
    };
  } finally {
    cursor.close();
    vi.unstubAllGlobals();
    count.mockRestore();
    reads.mockRestore();
    attach.mockRestore();
    admission.mockRestore();
  }
}

for (const seed of seedsFor("next-write-range-cardinality", [0xb3cf]))
  it(`keeps cursor admission/native state flat and warm work logarithmic (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          width: fc.constantFrom(64, 128),
          fraction: fc.constantFrom(1, 2, 3),
          capacity: fc.constantFrom(1, 7),
        }),
        async (s) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory");
          await h.open();
          const minima = new Map<string, number>(),
            native = new Map<string, number>(),
            charge = new Map<string, number>();
          try {
            for (const n of rows) {
              reach.reach(n === 1024 ? "1024 keys" : n === 16384 ? "16384 keys" : "131072 keys");
              const writer = h.space.beginWrite("fixed one-run cardinality"),
                builder = new RunBuilder(),
                bytes = key(0, s.width),
                view = new DataView(bytes.buffer);
              try {
                for (let k = 0; k < n; k++) {
                  view.setUint32(0, k, false);
                  builder.add(bytes, k + 1, false);
                }
                const encoded = need(builder.finish()),
                  ref = runRef(encoded, (await writer.write(encoded.parts)).blobs),
                  from = Math.floor((n * s.fraction) / 4);
                const ranges: Array<["all" | "bounded", KeyRange]> = [
                  ["all", { lower: null, upper: null }],
                  [
                    "bounded",
                    {
                      lower: { kind: "atLeast", key: key(from, s.width) },
                      upper: { kind: "through", key: key(from + 31, s.width) },
                    },
                  ],
                ];
                for (const [name, range] of ranges)
                  for (const direction of ["asc", "desc"] as const) {
                    evict(h);
                    expect(h.space.peek(ref.entries)).toBeNull();
                    expect(h.space.parsed?.(ref.entries)).toBeNull();
                    reach.reach("governor eviction");
                    const mode = `${name} ${direction}` as const,
                      cold = await measure(h, ref, range, direction, s.capacity),
                      warm = await measure(h, ref, range, direction, s.capacity);
                    const first =
                      name === "bounded"
                        ? direction === "asc"
                          ? from + 1
                          : from + 32
                        : direction === "asc"
                          ? 1
                          : n;
                    const expected = Array.from(
                      { length: s.capacity },
                      (_, k) => first + (direction === "asc" ? k : -k),
                    );
                    expect(cold.values).toEqual(expected);
                    expect(warm.values).toEqual(expected);
                    expect(cold.directoryReads.every((reads) => reads === 1)).toBe(true);
                    expect(warm.directoryReads).toEqual([]);
                    expect(warm.cacheNative).toBe(0);
                    expect(cold.privateNative).toBe(8192);
                    expect(cold.privateCharge).toBeGreaterThanOrEqual(cold.privateNative);
                    expect(cold.privateNative).toBe(warm.privateNative);
                    if (!minima.has(mode)) {
                      minima.set(mode, cold.minimum);
                      native.set(mode, cold.privateNative);
                      charge.set(mode, cold.privateCharge);
                    }
                    expect(cold.minimum).toBe(minima.get(mode));
                    expect(warm.minimum).toBe(minima.get(mode));
                    expect(cold.privateNative).toBe(native.get(mode));
                    expect(cold.privateCharge).toBe(charge.get(mode));
                    const blocks = Math.ceil(n / Math.floor(4096 / (s.width + 14)));
                    expect(warm.varuints).toBeLessThanOrEqual(
                      name === "all" ? 8 : Math.ceil(Math.log2(blocks + 1)) + 8,
                    );
                    expect(cold.varuints).toBeLessThanOrEqual(
                      blocks * 4 + Math.ceil(Math.log2(blocks + 1)) + 16,
                    );
                    if (direction === "desc" || name === "bounded") {
                      expect(cold.cacheNative).toBeGreaterThan(0);
                      reach.reach("shared precharged index");
                    }
                    reach.reach(`cold ${mode}`);
                    reach.reach(`warm ${mode}`);
                    measurements.push({ keys: n, runs: 1, keyBytes: s.width, mode, cold, warm });
                  }
              } finally {
                await writer.abort();
              }
            }
          } finally {
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 6 },
    );
    if (process.env.MINNOW_RANGE_SCALING_REPORT === "1")
      console.info("RANGE_CARDINALITY", JSON.stringify(measurements));
  }, 120000);
it("reaches all fixed-run cardinality states", () => reach.assertReached());
