/** Packing and dictionary ranks must preserve the complete normalized lexicographic order. */
import { PGlite } from "@electric-sql/pglite";
import { beforeAll, afterAll, expect, it } from "vitest";
import { createReach, SORT_COMPRESSION_REACH } from "../../testing/reach.js";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
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
import { drive } from "./pipeline.js";
import { SortOperator, sortMinimumBytes } from "./sort.js";
const reach = createReach("next-sort-compress", SORT_COMPRESSION_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});
for (const seed of seedsFor("next-sort-compress", [0x1827, 0x91b3, 0x1826]))
  for (const ordered of [true, false])
    for (const desc of [false, true]) {
      it(`packs exact composite keys vs PG (${String(seed)}, ordered=${String(ordered)}, desc=${String(desc)})`, async () => {
        const n = seed % 2 === 0 ? 65536 : 65537,
          random = mulberry32(seed),
          types = [INT, DBL, BIG, TEXT, INT];
        // Construct two valid floats whose entire normalized tuple hashes collide, including
        // the validity header and DESC transform. Equality must still inspect all key words.
        const base = Math.imul(2166136261 ^ 1, 16777619),
          h1 = desc ? ~0xbff00000 : 0xbff00000,
          h2 = desc ? ~0xbff10000 : 0xbff10000,
          l1 = desc ? 0xffffffff : 0;
        const normalizedLow =
          (Math.imul(base ^ h1, 16777619) ^ Math.imul(base ^ h2, 16777619) ^ l1) >>> 0;
        const buffer = new ArrayBuffer(8),
          view = new DataView(buffer);
        view.setUint32(0, 0x3ff10000, false);
        view.setUint32(4, desc ? ~normalizedLow : normalizedLow, false);
        const collision = view.getFloat64(0, false);
        const ids = Array.from({ length: n }, (_, i) => i);
        if (!ordered)
          for (let i = n - 1; i > 0; i--) {
            const at = Math.floor(random() * (i + 1));
            [ids[i], ids[at]] = [ids[at] ?? 0, ids[i] ?? 0];
          }
        const f = (i: number) => {
          switch (i % 13) {
            case 0:
              return null;
            case 1:
              return NaN;
            case 2:
              return Infinity;
            case 3:
              return -Infinity;
            case 4:
              return -0;
            case 5:
              return 0;
            case 6:
              return 5e-324;
            case 7:
              return -5e-324;
            case 8:
              return 1;
            case 9:
              return collision;
            default:
              return (((i * 7919) % 29) - 14) / 7;
          }
        };
        const b = (i: number) => {
          switch (i % 9) {
            case 0:
              return null;
            case 1:
              return -9223372036854775808n;
            case 2:
              return 9223372036854775807n;
            case 3:
              return -4611686018427387905n;
            case 4:
              return -9007199254740993n;
            case 5:
              return 9007199254740993n;
            default:
              return 9007199254740992n + BigInt(i % 17);
          }
        };
        const rows: Row[] = ids.map((i) => [
          i,
          f(i >>> 1),
          b(i >>> 1),
          i % 17 === 0 ? null : `v${String(i % 7)}`,
          ((i >>> 1) * 13) % 1021,
        ]);
        const float = `CASE i%13 WHEN 0 THEN NULL WHEN 1 THEN 'NaN'::float8 WHEN 2 THEN 'Infinity'::float8 WHEN 3 THEN '-Infinity'::float8 WHEN 4 THEN '-0'::float8 WHEN 5 THEN 0::float8 WHEN 6 THEN '5e-324'::float8 WHEN 7 THEN '-5e-324'::float8 WHEN 8 THEN 1::float8 WHEN 9 THEN '${String(collision)}'::float8 ELSE (((i::bigint*7919)%29)-14)::float8/7 END`;
        const big =
          "CASE i%9 WHEN 0 THEN NULL WHEN 1 THEN '-9223372036854775808'::bigint WHEN 2 THEN '9223372036854775807'::bigint WHEN 3 THEN '-4611686018427387905'::bigint WHEN 4 THEN '-9007199254740993'::bigint WHEN 5 THEN '9007199254740993'::bigint ELSE 9007199254740992::bigint+i%17 END";
        const expected = (
          await pg.query<{ id: number }>(
            `SELECT i AS id FROM (SELECT i,i/2 AS k FROM generate_series(0,${String(n - 1)}) g(i)) q ORDER BY ${float.replaceAll(/\bi\b/g, "k")} ${desc ? "DESC NULLS LAST" : "ASC NULLS FIRST"},${big.replaceAll(/\bi\b/g, "k")} DESC NULLS LAST,(k*13)%1021,i`,
          )
        ).rows.map((r) => r.id);
        const governor = new Governor(64 << 20),
          spill = new MemorySpillService(),
          grant = await governor.admit({
            label: "compressed-sort",
            minimumBytes: sortMinimumBytes(types),
            spillable: true,
          }),
          sink = new CollectSink();
        const op = new SortOperator(
          {
            types,
            keys: [
              {
                column: 1,
                direction: desc ? "desc" : "asc",
                nulls: desc ? "last" : "first",
                collation: null,
              },
              { column: 2, direction: "desc", nulls: "last", collation: null },
              { column: 4, direction: "asc", nulls: "last", collation: null },
              { column: 0, direction: "asc", nulls: "last", collation: null },
            ],
            limit: null,
            offset: 0,
            withTies: false,
          },
          { grant, spill, queryId: 1, columnar: true },
          sink,
        );
        await drive(
          new ArraySource(
            batchesOf(types, rows, { dictionary: true, int64: "int64", batchRows: 997 }),
          ),
          op,
        );
        await op.closed;
        const actual = sink.rows.map((r) => Number(r[0]));
        expect(actual.length).toBe(expected.length);
        const mismatch = actual.findIndex((id, at) => id !== expected[at]);
        expect(
          mismatch < 0
            ? null
            : { at: mismatch, actual: actual[mismatch], expected: expected[mismatch] },
        ).toBeNull();
        for (const row of sink.rows) {
          const id = Number(row[0]);
          expect(String(row[2])).toBe(String(b(id >>> 1)));
          expect(row[1]).toBe(f(id >>> 1));
        }
        expect(op.profile).toMatchObject({
          packedWords: ordered ? 1 : 2,
          orderedSuffixKeys: ordered ? 1 : 0,
          rankedKeys: 2,
          keyBits: ordered ? 21 : 21 + Math.ceil(Math.log2(n)),
          radixPasses: ordered ? 2 : 3,
          rankRefusals: ordered || n === 65536 ? 0 : 1,
        });
        expect(governor.usedBytes).toBe(0);
        expect(spill.bytesInUse).toBe(0);
        reach.beginHistory();
        for (const state of [
          ordered ? "ordered suffix proof" : "shuffled suffix retained",
          "composite packing",
          "tuple ranks",
          "float peers",
          "wide int64",
          "NULL keys",
          "hash collision",
          "explicit tied IDs",
          n === 65536 ? "LSD threshold" : "above threshold",
          ordered ? "one word" : "word boundary",
          desc ? "desc" : "asc",
        ] as const)
          reach.reach(state);
      }, 120000);
    }

it("checks the shared deadline in compression and aborts before reading again on resume", async () => {
  const { SortKeyCompression } = await import("./sort-compress.js");
  const { Slice, runSliced } = await import("./slices.js");
  const { TaskClock } = await import("./pipeline.js");
  const controller = new AbortController(),
    reason = new Error("stop compression");
  let reads = 0,
    live = 0;
  const data = Uint32Array.from({ length: 32768 }, (_, i) => 32768 - i);
  const values = new Proxy(data, {
    get(target, key) {
      if (typeof key === "string" && /^\d+$/.test(key)) {
        reads++;
        if (reads === 4000) controller.abort(reason);
      }
      return Reflect.get(target, key, target) as unknown;
    },
  });
  const compression = new SortKeyCompression({
    allocate(bytes) {
      live += bytes;
      return true;
    },
    free(bytes) {
      live -= bytes;
    },
  });
  const clock = new TaskClock(() => reads * 0.001);
  clock.begin();
  const slice = new Slice(4, clock),
    job = compression.pack([values], [1], data.length, slice);
  const work = runSliced(job, slice, () => controller.signal.throwIfAborted()),
    atYield = reads;
  expect(atYield).toBeGreaterThanOrEqual(4000);
  expect(atYield).toBeLessThan(6144);
  await expect(Promise.resolve(work).finally(() => job.return(null))).rejects.toBe(reason);
  expect(reads).toBe(atYield);
  compression.close();
  expect(live).toBe(0);
});

it("declines an admitted compression step without changing its original key columns", async () => {
  const { SortKeyCompression } = await import("./sort-compress.js");
  const { Slice, runSliced } = await import("./slices.js");
  const { TaskClock } = await import("./pipeline.js");
  const values = Uint32Array.from({ length: 32768 }, (_, i) => (i * 7919 * 65537) >>> 0),
    original = values.slice();
  let live = 0,
    refused = 0;
  const compression = new SortKeyCompression({
    allocate(bytes) {
      if (bytes > 8192) {
        refused++;
        return false;
      }
      live += bytes;
      return true;
    },
    free(bytes) {
      live -= bytes;
    },
  });
  const slice = new Slice(4, new TaskClock(() => 0));
  expect(
    await runSliced(compression.pack([values], [1], values.length, slice), slice, () => undefined),
  ).toBeNull();
  expect(refused).toBeGreaterThan(0);
  expect(values).toEqual(original);
  compression.close();
  expect(live).toBe(0);
});

for (const seed of seedsFor("next-sort-compress-probes", [0x13, 0x17]))
  it(`bounds exact-tuple probing under full hash collisions (${String(seed)})`, async () => {
    const { SortKeyCompression } = await import("./sort-compress.js");
    const { Slice, runSliced } = await import("./slices.js");
    const { TaskClock } = await import("./pipeline.js");
    const n = 129 + (seed % 3),
      high = new Uint32Array(n),
      low = new Uint32Array(n),
      target = Math.imul(2166136261 ^ 0xbff00000, 16777619);
    let live = 0;
    for (let i = 0; i < n; i++) {
      high[i] = 0xbff00000 + n - i;
      low[i] = (target ^ Math.imul(2166136261 ^ (high[i] ?? 0), 16777619)) >>> 0;
    }
    const compression = new SortKeyCompression({
        allocate(bytes) {
          live += bytes;
          return true;
        },
        free(bytes) {
          live -= bytes;
        },
      }),
      slice = new Slice(4, new TaskClock(() => 0));
    expect(
      await runSliced(compression.pack([high, low], [2], n, slice), slice, () => undefined),
    ).toBeNull();
    expect(compression.stats.rankRefusals).toBe(1);
    compression.close();
    expect(live).toBe(0);
    reach.beginHistory();
    reach.reach("probe bound");
  });
