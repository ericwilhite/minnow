/** PostgreSQL owns strict join bags/NULL equality. The native resident LIFO
 * contract plus B9's stable-demand rule owns transition visitation order.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  B9_STAR_BUCKET_REACH,
  B9_STAR_STAGED_PREFIX_REACH,
} from "../../testing/reach.js";
import { StarBuckets } from "./join-star-buckets.js";
import { INT, DBL, runJoin, type Row } from "./join-fixtures.js";
import type { HashJoinSpec } from "./join.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
function spec(
  probeTypes: HashJoinSpec["probeTypes"],
  buildTypes: HashJoinSpec["buildTypes"],
  probe: number,
  build: number,
): HashJoinSpec {
  return {
    kind: "inner",
    probeTypes,
    buildTypes,
    probeKeys: [probe],
    buildKeys: [build],
    nullSafe: [false],
    residual: null,
    publishes: [],
  };
}
it.each(seedsFor("b9-star-bucket-order", [0xb9a21, 0xb9a22]))(
  "bucket row IDs preserve all three native joins and PostgreSQL bags (seed %i)",
  async (seed) => {
    const reach = createReach("B9 star bucket order", B9_STAR_BUCKET_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.shuffledSubarray([0, 1, 2, 3, 4, 5], { minLength: 2, maxLength: 6 }),
        fc.shuffledSubarray([0, 1, 2, 3, 4, 5], { minLength: 2, maxLength: 6 }),
        fc.shuffledSubarray([0, 1, 2, 3, 4, 5], { minLength: 2, maxLength: 6 }),
        fc.array(
          fc.tuple(
            ...[0, 1, 2].map(() => fc.option(fc.integer({ min: 0, max: 8 }), { nil: null })),
          ),
          { minLength: 32, maxLength: 128 },
        ),
        async (ak, bk, ck, keys) => {
          reach.beginHistory();
          const a = ak.map((k) => [k, k % 3 === 0 ? null : k % 2] as Row),
            b = bk.map((k) => [k, k % 3 === 0 ? null : k % 2] as Row),
            c = ck.map((k) => [k, k % 3 === 0 ? null : k % 2] as Row),
            facts = keys.map((key, id) => [...key, id] as Row),
            first = await runJoin(spec([INT, INT, INT, DBL], [INT, INT], 2, 0), c, facts, {
              batchRows: 7,
              probeBatchRows: 5,
              cap: 8 << 20,
            }),
            middle = await runJoin(
              spec([INT, INT], [INT, INT, INT, DBL, INT, INT], 0, 0),
              first.rows,
              a,
              { batchRows: 7, probeBatchRows: 3, cap: 8 << 20 },
            ),
            last = await runJoin(
              spec([INT, INT], [INT, INT, INT, INT, INT, DBL, INT, INT], 0, 3),
              middle.rows,
              b,
              { batchRows: 7, probeBatchRows: 4, cap: 8 << 20 },
            ),
            native = last.rows.map((row) => ({ id: row[7], ag: row[3], bg: row[1], cg: row[9] }));
          for (const result of [first, middle, last]) {
            expect(result.join.profile.spilledBytes).toBe(0);
            expect(result.governor.usedBytes).toBe(0);
          }
          let used = 0;
          const bytes = StarBuckets.bytes(facts.length, a.length, b.length);
          if (bytes === null) throw new Error("Unfunded small test");
          const charge = {
              allocate(n: number) {
                if (used + n > bytes) return false;
                used += n;
                return true;
              },
              free(n: number) {
                used -= n;
              },
            },
            buckets = new StarBuckets(facts.length, a.length, b.length, charge),
            fused: Array<{ id: number; ag: unknown; bg: unknown; cg: unknown }> = [];
          try {
            expect(used).toBe(bytes);
            const amap = new Map(ak.map((k, i) => [k, i])),
              bmap = new Map(bk.map((k, i) => [k, i])),
              cmap = new Map(c.map((r) => [r[0], r[1]]));
            for (const row of facts) {
              const aa = row[0] === null ? undefined : amap.get(Number(row[0])),
                bb = row[1] === null ? undefined : bmap.get(Number(row[1]));
              if (aa === undefined || bb === undefined || row[2] === null || !cmap.has(row[2]))
                continue;
              const cc = cmap.get(row[2]);
              buckets.append(aa, bb, cc === null ? -1 : Number(cc), Number(row[3]));
            }
            buckets.begin(a.length, b.length);
            const records = new Int32Array(5),
              as = new Int32Array(5),
              bs = new Int32Array(5);
            for (;;) {
              const step = buckets.read(records, as, bs, 3);
              for (let i = 0; i < step.length; i++) {
                const row = records[i] ?? -1;
                fused.push({
                  id: buckets.values[row] ?? -1,
                  ag: a[as[i] ?? -1]?.[1],
                  bg: b[bs[i] ?? -1]?.[1],
                  cg: (buckets.groupCodes[row] ?? -1) < 0 ? null : buckets.groupCodes[row],
                });
              }
              if (step.length === 0 && !step.done) reach.reach("bounded empty bucket steps");
              if (step.done) break;
            }
            expect(fused).toEqual(native);
          } finally {
            buckets.close();
            expect(used).toBe(0);
          }
          await pg.exec(
            "DROP TABLE IF EXISTS sf,sa,sb,sc;CREATE TABLE sf(a int,b int,c int,id int);CREATE TABLE sa(k int PRIMARY KEY,g int);CREATE TABLE sb(k int PRIMARY KEY,g int);CREATE TABLE sc(k int PRIMARY KEY,g int)",
          );
          await pg.query(
            "INSERT INTO sf SELECT * FROM json_to_recordset($1::json) x(a int,b int,c int,id int)",
            [JSON.stringify(facts.map((r) => ({ a: r[0], b: r[1], c: r[2], id: r[3] })))],
          );
          for (const [table, rows] of [
            ["sa", a],
            ["sb", b],
            ["sc", c],
          ] as const)
            await pg.query(
              `INSERT INTO ${table} SELECT * FROM json_to_recordset($1::json) x(k int,g int)`,
              [JSON.stringify(rows.map((r) => ({ k: r[0], g: r[1] })))],
            );
          const expected = await pg.query(
            "SELECT f.id,a.g AS ag,b.g AS bg,c.g AS cg FROM sf f JOIN sa a ON a.k=f.a JOIN sb b ON b.k=f.b JOIN sc c ON c.k=f.c ORDER BY f.id",
          );
          expect(fused.sort((a, b) => a.id - b.id)).toEqual(expected.rows);
          if (native.length >= 8) reach.reach("native transition order");
          if (first.rows.length > 2048) reach.reach("multiple native chunks");
          if (native.length > 0) reach.reach("PostgreSQL bags");
          if (native.some((r) => r.ag === null || r.bg === null || r.cg === null))
            reach.reach("nullable groups");
          if (native.length < facts.length) reach.reach("strict key misses");
          reach.reach("all credit returned");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [
            [2, 0, 1],
            [1, 0, 2],
            [0, 1, 2],
            Array.from({ length: 96 }, (_, i) => [
              i % 3,
              Math.floor(i / 3) % 3,
              Math.floor(i / 9) % 3,
            ]),
          ],
          [
            [0, 1, 2, 3, 4, 5],
            [5, 3, 1],
            [0, 2],
            [[0, 5, 0], ...Array.from({ length: 32 }, (_, i) => [i % 2 === 0 ? null : 8, 8, 8])],
          ],
          [
            [2, 0, 1],
            [1, 0, 2],
            [0, 1, 2],
            Array.from({ length: 4097 }, (_, i) => [
              i % 3,
              Math.floor(i / 3) % 3,
              Math.floor(i / 9) % 3,
            ]),
          ],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  },
  90000,
);

it("NULL flags survive links and finite/special payload bits survive one copy", () => {
  const source = new ArrayBuffer(8),
    view = new DataView(source);
  view.setUint32(0, 0x1234, true);
  view.setUint32(4, 0x7ff80000, true);
  const nan = view.getFloat64(0, true),
    payloads = [null, -0, nan, Infinity, -Infinity] as const;
  let allocated = 0;
  const memory = {
    allocate(n: number) {
      allocated += n;
      return true;
    },
    free(n: number) {
      allocated -= n;
    },
  };
  const buckets = new StarBuckets(payloads.length, 2, 2, memory);
  try {
    for (const payload of payloads) buckets.append(0, 0, -1, payload);
    buckets.begin(1, 1);
    const refs = new Int32Array(2),
      a = new Int32Array(2),
      b = new Int32Array(2),
      seen: number[] = [];
    for (;;) {
      const step = buckets.read(refs, a, b, 1);
      for (let i = 0; i < step.length; i++) seen.push(refs[i] ?? -1);
      if (step.done) break;
    }
    expect(seen).toEqual([0, 1, 2, 3, 4]);
    for (const [row, payload] of payloads.entries()) {
      expect(buckets.isNull(row)).toBe(payload === null);
      expect(buckets.groupCodes[row]).toBe(-1);
      if (payload !== null) {
        const expected = new Float64Array([payload]),
          actual = buckets.values.subarray(row, row + 1);
        expect(new Uint8Array(actual.buffer, actual.byteOffset, 8)).toEqual(
          new Uint8Array(expected.buffer),
        );
      }
    }
  } finally {
    buckets.close();
    expect(allocated).toBe(0);
  }
});

it("rejects unaffordable sparse domains and refunds a refused construction", () => {
  expect(StarBuckets.bytes(1, 1 << 20, 2)).toBeNull();
  expect(StarBuckets.bytes(1, 1024, 1024)).toBeNull();
  let allocated = 0,
    calls = 0;
  expect(
    () =>
      new StarBuckets(32, 2, 3, {
        allocate(n) {
          if (++calls === 4) return false;
          allocated += n;
          return true;
        },
        free(n) {
          allocated -= n;
        },
      }),
  ).toThrow(/refused/);
  expect(allocated).toBe(0);
});

it("the native star's finite float transition order is also PostgreSQL's explicit ordered SUM", async () => {
  const max = Number.MAX_VALUE,
    first = await runJoin(
      spec([INT, INT, INT, DBL], [INT], 2, 0),
      [[0]],
      [
        [0, 0, 0, max],
        [1, 0, 0, max],
        [1, 0, 0, -max],
      ],
      { cap: 8 << 20 },
    ),
    middle = await runJoin(spec([INT], [INT, INT, INT, DBL, INT], 0, 0), first.rows, [[0], [1]], {
      cap: 8 << 20,
    }),
    last = await runJoin(spec([INT], [INT, INT, INT, INT, DBL, INT], 0, 2), middle.rows, [[0]], {
      cap: 8 << 20,
    }),
    native = last.rows.map((r) => Number(r[5]));
  expect(native).toEqual([max, -max, max]);
  let held = 0;
  const buckets = new StarBuckets(3, 2, 1, {
      allocate(n) {
        held += n;
        return true;
      },
      free(n) {
        held -= n;
      },
    }),
    refs = new Int32Array(3),
    a = new Int32Array(3),
    b = new Int32Array(3),
    ordered: number[] = [];
  try {
    buckets.append(0, 0, 0, max);
    buckets.append(1, 0, 0, max);
    buckets.append(1, 0, 0, -max);
    buckets.begin(2, 1);
    for (;;) {
      const step = buckets.read(refs, a, b);
      for (let i = 0; i < step.length; i++) ordered.push(buckets.values[refs[i] ?? -1] ?? 0);
      if (step.done) break;
    }
    expect(ordered).toEqual(native);
    const sum = await pg.query<{ s: number }>(
      "SELECT SUM(v ORDER BY ord) AS s FROM unnest($1::float8[]) WITH ORDINALITY t(v,ord)",
      ["{" + ordered.join(",") + "}"],
    );
    expect(sum.rows[0]?.s).toBe(max);
    await expect(
      pg.query("SELECT SUM(v ORDER BY ord) FROM unnest($1::float8[]) WITH ORDINALITY t(v,ord)", [
        "{" + [max, max, -max].join(",") + "}",
      ]),
    ).rejects.toMatchObject({ code: "22003" });
  } finally {
    buckets.close();
    expect(held).toBe(0);
  }
});

it.each(seedsFor("b9-star-staged-prefix", [0xb9f93, 0xb9f94]))(
  "staged IDs preserve skipped A rows, duplicate/nonmonotonic B prefixes and NULLs (seed %i)",
  (seed) => {
    const reach = createReach("B9 staged prefixes", B9_STAR_STAGED_PREFIX_REACH);
    fc.assert(
      fc.property(
        fc.array(
          fc.tuple(
            fc.integer({ min: -1, max: 2 }),
            fc.integer({ min: 0, max: 4 }),
            fc.option(fc.integer({ min: -9, max: 9 }), { nil: null }),
          ),
          { minLength: 8, maxLength: 256 },
        ),
        fc.array(fc.integer({ min: 0, max: 4 }), { minLength: 2, maxLength: 16 }),
        (facts, probes) => {
          reach.beginHistory();
          let charged = 0;
          const buckets = new StarBuckets(facts.length, 3, 5, {
            allocate(n) {
              charged += n;
              return true;
            },
            free(n) {
              charged -= n;
            },
          });
          try {
            expect(charged).toBe(StarBuckets.bytes(facts.length, 3, 5));
            for (const [row, [, , value]] of facts.entries())
              expect(buckets.stage(row % 3 === 0 ? -1 : row % 3, value)).toBe(row);
            for (const [row, [a, b]] of facts.entries()) if (a >= 0) buckets.link(row, a, b);
            const refs = new Int32Array(3),
              as = new Int32Array(3),
              bs = new Int32Array(3),
              actual: Array<{
                row: number;
                a: number;
                b: number;
                value: number | null;
                group: number;
              }> = [];
            for (const b of probes) {
              buckets.beginB(3, b);
              for (;;) {
                const step = buckets.read(refs, as, bs, 2);
                for (let i = 0; i < step.length; i++) {
                  const row = refs[i] ?? -1;
                  actual.push({
                    row,
                    a: as[i] ?? -1,
                    b: bs[i] ?? -1,
                    value: buckets.isNull(row) ? null : (buckets.values[row] ?? 0),
                    group: buckets.groupCodes[row] ?? -2,
                  });
                }
                if (step.done) break;
              }
            }
            const expected = probes.flatMap((b) =>
              [2, 1, 0].flatMap((a) =>
                facts.flatMap(([fa, fb, value], row) =>
                  fa === a && fb === b
                    ? [{ row, a, b, value, group: row % 3 === 0 ? -1 : row % 3 }]
                    : [],
                ),
              ),
            );
            expect(actual).toEqual(expected);
            if (facts.some(([a]) => a === -1)) reach.reach("skipped staged A");
            if (facts.some(([, , value]) => value === null)) reach.reach("staged NULL payload");
            if (new Set(probes).size < probes.length) reach.reach("duplicate B visits");
            if (probes.some((b, i) => i > 0 && b < (probes[i - 1] ?? 0)))
              reach.reach("nonmonotonic query IDs");
            reach.reach("bounded prefix steps");
            reach.reach("native B A fact order");
          } finally {
            buckets.close();
            expect(charged).toBe(0);
            reach.reach("all credit returned");
          }
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          [
            [
              [0, 4, null],
              [2, 4, 1],
              [-1, 4, 2],
              [2, 0, 3],
              [1, 4, 4],
              [2, 4, 5],
              [0, 0, 6],
              [0, 3, 7],
            ],
            [4, 0, 4, 3],
          ],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  },
);
