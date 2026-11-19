/** int8 mutations preserve the exact signed word (vector/vector.ts value rules, PostgreSQL int8). */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, BIGINT_MODIFY_REACH, BIGINT_REPOINT_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import type { FlatVector } from "../vector/vector.js";
import { int64At } from "../vector/int64.js";
import type { ParameterValue } from "../exec/executor.js";

const MIN = -(1n << 63n),
  MAX = (1n << 63n) - 1n,
  EDGE = 1n << 53n;
const anchors = [
  MIN,
  MAX,
  -EDGE - 1n,
  -EDGE,
  -EDGE + 1n,
  EDGE - 1n,
  EDGE,
  EDGE + 1n,
  EDGE + 2n,
  -10000000000000000n,
  10000000000000000n,
  0n,
];
function word(value: unknown): string {
  if (typeof value !== "number" && typeof value !== "bigint" && typeof value !== "string")
    throw new TypeError("Expected an exact integer scalar");
  return String(value);
}
function exact(rows: ReadonlyArray<Record<string, unknown>>) {
  return rows.map((row) => ({
    k: word(row.k),
    n: row.n === null ? null : word(row.n),
    tag: row.tag,
  }));
}
const int8 = fc.bigInt({ min: MIN, max: MAX });
function unused(value: bigint, occupied: ReadonlySet<bigint>): bigint {
  while (occupied.has(value)) value = value === MAX ? MIN : value + 1n;
  return value;
}
const shape = fc.record({
  wideKey: fc.boolean(),
  indexed: fc.boolean(),
  deleteFirst: fc.boolean(),
  returning: fc.boolean(),
  parameters: fc.boolean(),
  rows: fc.integer({ min: 20, max: 40 }),
  words: fc.tuple(int8, int8, int8, int8),
});
for (const placement of ["resident", "published", "reopened"] as const)
  for (const seed of seedsFor("next-modify-bigint", [0xb5e9, 0xb5ea]))
    it(`keeps exact int8 keys and values through native modifications (${placement}, ${String(seed)})`, async () => {
      const pg = new PGlite();
      const reach = createReach(
        "exact bigint modify",
        BIGINT_MODIFY_REACH.filter(
          (state) => !["resident", "published", "reopened"].includes(state) || state === placement,
        ),
      );
      await pg.waitReady;
      try {
        await fc.assert(
          fc.asyncProperty(shape, async (s) => {
            reach.beginHistory();
            const core = new StorageCore(placement === "resident" ? 256 : 8);
            Object.assign(core.harness.tuning, {
              merges: { mode: "manual", fanIn: 2, sliceEntries: 3 },
            });
            await core.open();
            const wide = [...anchors],
              seen = new Set(wide);
            for (const word of s.words) {
              const v = unused(word, seen);
              wide.push(v);
              seen.add(v);
            }
            for (let v = 1n; wide.length < s.rows; v++) {
              const next = unused(v, seen);
              wide.push(next);
              seen.add(next);
            }
            const keys: Array<number | bigint> = wide.map((value, tag) =>
              s.wideKey ? value : tag,
            );
            const values: Array<bigint | null> = [...wide];
            if (!s.wideKey) [values[0], values[9]] = [values[9] ?? null, values[0] ?? null];
            values[values.length - 1] = null;
            const type = s.wideKey ? "BIGINT" : "INT";
            const ddl = `CREATE TABLE wide_i8(k ${type} PRIMARY KEY,n BIGINT,tag INT)`;
            const check = async () => {
              const sql = "SELECT k::TEXT AS k,n::TEXT AS n,tag FROM wide_i8 ORDER BY wide_i8.k";
              const result = await core.execute("SELECT k,n,tag FROM wide_i8 ORDER BY k");
              if (!("batches" in result)) throw new Error("Missing native rows");
              expect(exact(oracleRows(result))).toEqual((await pg.query(sql)).rows);
            };
            const mutate = async (sql: string, params: ParameterValue[]) => {
              if (!s.parameters) {
                sql = sql.replace(/\$(\d+)/gu, (_, index: string) => {
                  const value = params[Number(index) - 1];
                  return value === null ? "NULL" : `(${word(value)})`;
                });
                params = [];
              }
              const result = await core.execute(
                sql.replace(
                  "RETURNING k::TEXT AS k,n::TEXT AS n,tag",
                  s.returning ? "RETURNING k,n,tag" : "",
                ),
                params,
              );
              const expected = await pg.query(sql, params);
              if (s.returning) {
                if (!("returning" in result)) throw new Error("Missing native RETURNING");
                expect(exact(oracleRows(result.returning))).toEqual(expected.rows);
              } else expect("returning" in result).toBe(false);
              expect("rowCount" in result ? result.rowCount : undefined).toBe(expected.rows.length);
            };
            try {
              await pg.exec("DROP TABLE IF EXISTS wide_i8");
              await pg.exec(ddl);
              await core.execute(ddl);
              if (s.indexed) {
                const sql = "CREATE UNIQUE INDEX wide_n ON wide_i8(n)";
                await core.execute(sql);
                await pg.exec(sql);
              }
              for (let from = 0; from < wide.length; from += 8) {
                const params: ParameterValue[] = [];
                const tuples = keys.slice(from, from + 8).map((key, offset) => {
                  const tag = from + offset;
                  params.push(key, values[tag] ?? null, tag);
                  const p = params.length;
                  return `($${String(p - 2)}::${type},$${String(p - 1)}::BIGINT,$${String(p)}::INT)`;
                });
                const sql = `INSERT INTO wide_i8 VALUES ${tuples.join(",")} RETURNING k::TEXT AS k,n::TEXT AS n,tag`;
                await mutate(sql, params);
              }
              reach.reach("insert");
              reach.reach(wide.length > 32 ? "multiple validity words" : "one validity word");
              reach.reach(s.wideKey ? "bigint primary key" : "integer primary key");
              reach.reach(s.indexed ? "unique secondary" : "unindexed non-key");
              if (placement !== "resident") {
                await core.harness.path.recycleLog();
                await core.harness.path.quiesce();
              }
              if (placement === "reopened") await core.reopen();
              reach.reach(placement);
              reach.reach(s.parameters ? "parameters" : "exact SQL literals");
              await check();
              if (placement !== "resident") {
                const pin = core.harness.path.pin();
                try {
                  const table = pin.catalog.tableByName("wide_i8");
                  if (!table) throw new Error("Missing persisted table");
                  expect(
                    pin
                      .table(table.id, {
                        budget: { allocate: () => true, free: () => undefined },
                      })
                      .segments.some((segment) => segment.kind === "group"),
                  ).toBe(true);
                } finally {
                  pin.release();
                }
              }
              reach.reach(s.returning ? "RETURNING" : "without RETURNING");
              const projection = " RETURNING k::TEXT AS k,n::TEXT AS n,tag";
              const update = async () => {
                const next = unused(s.words[0], new Set(values.filter((value) => value !== null)));
                await mutate(
                  `UPDATE wide_i8 SET n=$1::BIGINT WHERE ${s.indexed ? `k=$2::${type}` : "tag=$2::INT"}${projection}`,
                  [next, s.indexed ? (keys[0] ?? 0) : 0],
                );
                values[0] = next;
                reach.reach("update");
              };
              const remove = async () => {
                await mutate(
                  `DELETE FROM wide_i8 WHERE ${s.indexed ? `k=$1::${type}` : "tag=$1::INT"}${projection}`,
                  [s.indexed ? (keys[1] ?? 1) : 1],
                );
                reach.reach("delete");
              };
              if (s.deleteFirst) {
                reach.reach("delete before update");
                await remove();
                await update();
              } else {
                reach.reach("update before delete");
                await update();
                await remove();
              }
              await check();
              const nextKey = s.wideKey
                ? unused(s.words[1], new Set(keys.map((key) => BigInt(key))))
                : 10000;
              await mutate(`UPDATE wide_i8 SET k=$1::${type} WHERE k=$2::${type}${projection}`, [
                nextKey,
                keys[2] ?? 2,
              ]);
              reach.reach("rekey");
              await mutate(`DELETE FROM wide_i8 WHERE n=$1::BIGINT${projection}`, [
                values[3] ?? 0n,
              ]);
              reach.reach("non-key predicate");
              await check();
              await core.reopen();
              await check();
              reach.reach("WAL replay");
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
              await core.reopen();
              await check();
              reach.reach("published reopen");
              for (const state of [
                "int8 minimum",
                "int8 maximum",
                "unsafe positive",
                "unsafe negative",
                "adjacent wide words",
                "generated full range",
                "nullable value",
              ] as const)
                reach.reach(state);
            } finally {
              await core.close();
            }
            expect(core.harness.governor.usedBytes).toBe(0);
            reach.reach("zero charge");
          }),
          {
            seed,
            numRuns: 16,
            examples: [
              [
                {
                  wideKey: false,
                  indexed: false,
                  deleteFirst: true,
                  returning: false,
                  parameters: false,
                  rows: 33,
                  words: [MIN, MAX, EDGE + 1n, -EDGE - 1n],
                },
              ],
              [
                {
                  wideKey: true,
                  indexed: true,
                  deleteFirst: false,
                  returning: true,
                  parameters: true,
                  rows: 33,
                  words: [EDGE + 1n, -EDGE - 1n, MIN, MAX],
                },
              ],
            ],
          },
        );
        reach.assertReached();
      } finally {
        await pg.close();
      }
    }, 120000);

for (const seed of seedsFor("next-bigint-repoint", [0xb5eb, 0xb5ec]))
  it(`preserves the caller's full-width lane after memory publication (${String(seed)})`, async () => {
    const reach = createReach("bigint repointed reader", BIGINT_REPOINT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.tuple(int8, int8), fc.boolean(), async (words, nullable) => {
        reach.beginHistory();
        const core = new StorageCore(8);
        await core.open();
        let used = 0;
        const budget = {
          allocate(bytes: number) {
            used += bytes;
            return true;
          },
          free(bytes: number) {
            used -= bytes;
            expect(used).toBeGreaterThanOrEqual(0);
          },
        };
        try {
          await core.execute("CREATE TABLE repoint(k INT, wide BIGINT, small BIGINT)");
          const values: Array<bigint | null> = [MIN, MAX, EDGE + 1n, -EDGE - 1n, ...words];
          if (nullable) values[5] = null;
          const params: ParameterValue[] = [];
          const tuples = values.map((v, k) => {
            params.push(k, v, nullable && k === 5 ? null : k);
            const n = params.length;
            return `($${String(n - 2)}::INT,$${String(n - 1)}::BIGINT,$${String(n)}::BIGINT)`;
          });
          await core.execute(`INSERT INTO repoint VALUES ${tuples.join(",")}`, params);
          const path = core.harness.path;
          const table = path.catalog.tableByName("repoint");
          if (!table) throw new Error("Missing table");
          const pin = path.pin();
          try {
            const segment = pin.table(table.id, { budget }).segments[0];
            if (segment?.kind !== "memory" || !segment.portableChunk)
              throw new Error("Expected portable memory segment");
            await core.execute(
              "INSERT INTO repoint VALUES (6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(11,11,11),(12,12,12),(13,13,13),(14,14,14),(15,15,15)",
            );
            await path.quiesce();
            expect((await segment.portableChunk(table.columns[1]?.id ?? 0, 0)).kind).toBe("stored");
            for (const [index, expected] of [
              [1, values],
              [2, values.map((_, k) => (nullable && k === 5 ? null : BigInt(k)))],
            ] as const) {
              const type = table.columns[index]?.type;
              if (!type) throw new Error("Missing column");
              const bytes = values.length * 12 + 4;
              budget.allocate(bytes);
              try {
                const flat: FlatVector = {
                  form: "flat",
                  type,
                  length: 0,
                  validity: new Uint32Array(1),
                  data: {
                    layout: "int64",
                    values: { repr: "int64", values: new BigInt64Array(values.length) },
                  },
                };
                const result = await segment.chunk(table.columns[index]?.id ?? 0, 0, {
                  flat,
                  codes: new Int32Array(values.length),
                  varlen: null,
                });
                expect(result.vector).toBe(flat);
                if (flat.data.layout !== "int64") throw new Error("Expected int64 output");
                const data = flat.data;
                expect(data.values.repr).toBe("int64");
                expect(
                  Array.from({ length: flat.length }, (_, k) =>
                    flat.validity !== null &&
                    ((flat.validity[k >>> 5] ?? 0) & (1 << (k & 31))) === 0
                      ? null
                      : int64At(data.values, k),
                  ),
                ).toEqual(expected);
                reach.reach(index === 1 ? "wide page" : "safe page into wide output");
              } finally {
                budget.free(bytes);
              }
            }
            reach.reach(nullable ? "nullable" : "all valid");
            reach.reach("published memory handle");
          } finally {
            pin.release();
          }
          expect(used).toBe(0);
        } finally {
          await core.close();
        }
        expect(core.harness.governor.usedBytes).toBe(0);
        reach.reach("zero charge");
      }),
      {
        seed,
        numRuns: 8,
        examples: [
          [[MIN, MAX], true],
          [[EDGE + 1n, -EDGE - 1n], false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
