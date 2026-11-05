/**
 * Every hash-join kind against PostgreSQL itself (PGlite). The law is PostgreSQL's behaviour:
 * JOIN / LEFT / RIGHT / FULL with extra ON conditions, EXISTS and NOT EXISTS, `IS NOT DISTINCT
 * FROM` keys, and `[NOT] IN` with its three-valued result over single and composite keys,
 * correlated and not. Generated tables have duplicate keys and NULLs everywhere.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import {
  INT,
  TEXT,
  canonical,
  residualOf,
  runJoin,
  type Row,
  type Value,
} from "./join-fixtures.js";
import type { HashJoinKind, HashJoinSpec } from "./join.js";

const SEEDS = seedsFor("next-hash-join-pglite", [0x9e, 0x1f2, 0x33]);
let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});

/** (k1 int, k2 text, v int): small domains so keys repeat, NULLs in keys and payload. */
function table(random: () => number, rows: number): Row[] {
  return Array.from({ length: rows }, (_, i): Row => [
    random() < 0.15 ? null : Math.floor(random() * 6),
    random() < 0.15 ? null : `s${String(Math.floor(random() * 4))}`,
    random() < 0.1 ? null : i * 11 - 40,
  ]);
}

async function load(name: string, rows: readonly Row[]): Promise<void> {
  await pg.exec(
    `DROP TABLE IF EXISTS ${name}; CREATE TABLE ${name} (k1 int, k2 text COLLATE "C", v int);`,
  );
  for (const row of rows) await pg.query(`INSERT INTO ${name} VALUES ($1, $2, $3)`, row as Value[]);
}

/** PostgreSQL rows as fixture values (ints as numbers, booleans and NULL as is). */
async function postgres(sql: string): Promise<Row[]> {
  const result = await pg.query<Record<string, Value>>(sql, [], { rowMode: "array" } as never);
  return (result.rows as unknown as Value[][]).map((row) => row.map((v) => v ?? null));
}

const RESIDUAL_SQL = "(CASE WHEN p.v % 7 = 3 THEN NULL ELSE p.v < b.v + 150 END)";
/** The same condition in JS: `%` truncates like PostgreSQL's; NULL propagates. */
const residual = (p: Row, b: Row): boolean | null => {
  const x = p[2] as number | null;
  const y = b[2] as number | null;
  if (x !== null && x % 7 === 3) return null;
  if (x === null || y === null) return null;
  return x < y + 150;
};

interface Case {
  readonly kind: HashJoinKind;
  readonly keys: readonly number[];
  readonly nullSafe: readonly boolean[];
  readonly correlated?: number;
  readonly withResidual: boolean;
}

function sqlFor(c: Case): string {
  const cmp = (i: number) => {
    const column = c.keys[i] === 0 ? "k1" : "k2";
    return c.nullSafe[i] === true
      ? `p.${column} IS NOT DISTINCT FROM b.${column}`
      : `p.${column} = b.${column}`;
  };
  const on = [...c.keys.map((_, i) => cmp(i)), ...(c.withResidual ? [RESIDUAL_SQL] : [])].join(
    " AND ",
  );
  switch (c.kind) {
    case "inner":
      return `SELECT p.*, b.* FROM p JOIN b ON ${on}`;
    case "left":
      return `SELECT p.*, b.* FROM p LEFT JOIN b ON ${on}`;
    case "right":
      return `SELECT p.*, b.* FROM p RIGHT JOIN b ON ${on}`;
    case "full":
      // PostgreSQL refuses FULL JOIN without one hashable `=`; then use its definition:
      // the LEFT JOIN plus every build row no probe row matches, null-extended.
      if (c.nullSafe.every((safe) => safe))
        return `SELECT p.*, b.* FROM p LEFT JOIN b ON ${on} UNION ALL SELECT NULL::int, NULL::text, NULL::int, b.* FROM b WHERE NOT EXISTS (SELECT 1 FROM p WHERE ${on})`;
      return `SELECT p.*, b.* FROM p FULL JOIN b ON ${on}`;
    case "semi":
      return `SELECT p.* FROM p WHERE EXISTS (SELECT 1 FROM b WHERE ${on})`;
    case "anti":
      return `SELECT p.* FROM p WHERE NOT EXISTS (SELECT 1 FROM b WHERE ${on})`;
    case "mark": {
      const correlated = c.correlated ?? 0;
      const columns = c.keys.map((k) => (k === 0 ? "k1" : "k2"));
      const correlation = columns.slice(0, correlated).map((col) => `b.${col} = p.${col}`);
      const tested = columns.slice(correlated);
      const from = `FROM b${correlation.length > 0 ? ` WHERE ${correlation.join(" AND ")}` : ""}`;
      if (!c.withResidual) {
        // PostgreSQL's own [NOT] IN, row-wise for composite keys.
        const left =
          tested.length === 1
            ? `p.${tested[0] ?? ""}`
            : `(${tested.map((t) => `p.${t}`).join(", ")})`;
        return `SELECT p.*, ${left} IN (SELECT ${tested.map((t) => `b.${t}`).join(", ")} ${from}) FROM p`;
      }
      // Ruling R20.1: the three-valued OR of the whole condition, PostgreSQL's `= ANY`.
      const cond = [...tested.map((t) => `p.${t} = b.${t}`), RESIDUAL_SQL].join(" AND ");
      return `SELECT p.*, true = ANY (SELECT (${cond}) ${from}) FROM p`;
    }
    default:
      throw new Error(c.kind);
  }
}

const KEYS: ReadonlyArray<readonly number[]> = [[0], [1], [0, 1]];

describe("hash join vs PostgreSQL", () => {
  for (const seed of SEEDS)
    it(`every kind, generated tables (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed);
      const types = [INT, TEXT, INT];
      for (let round = 0; round < 4; round++) {
        const build = table(random, 5 + Math.floor(random() * 25));
        const probe = table(random, 5 + Math.floor(random() * 25));
        await load("b", build);
        await load("p", probe);
        for (const kind of ["inner", "left", "right", "full", "semi", "anti", "mark"] as const)
          for (const keys of KEYS) {
            const markCorrelations =
              kind === "mark" ? (keys.length === 2 ? [0, 1] : [0]) : [undefined];
            for (const correlated of markCorrelations)
              for (const withResidual of [false, true]) {
                const nullSafe = keys.map(() => kind !== "mark" && random() < 0.35);
                const c: Case = {
                  kind,
                  keys,
                  nullSafe,
                  withResidual,
                  ...(correlated === undefined ? {} : { correlated }),
                };
                const spec: HashJoinSpec = {
                  kind,
                  probeTypes: types,
                  buildTypes: types,
                  probeKeys: keys,
                  buildKeys: keys,
                  nullSafe,
                  residual: withResidual ? residualOf(3, residual) : null,
                  publishes: [],
                  ...(correlated === undefined ? {} : { correlated }),
                };
                const tight = random() < 0.3;
                const ours = await runJoin(spec, build, probe, {
                  batchRows: 1 + Math.floor(random() * 9),
                  blockEvery: random() < 0.5 ? 2 : 0,
                  dictionary: random() < 0.5,
                  ...(tight ? { cap: "tight" as const, slack: 0 } : {}),
                });
                const sql = sqlFor(c);
                expect(canonical(ours.rows), sql).toEqual(canonical(await postgres(sql)));
              }
          }
      }
    }, 120_000);
});
