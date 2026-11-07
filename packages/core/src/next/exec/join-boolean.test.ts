/** PostgreSQL boolean equality/IN remains exact when the hash table chooses direct addressing. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { createReach, JOIN_BOOLEAN_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import type { PhysicalType } from "../types.js";
import { INT, canonical, runJoin, type Row, type Value } from "./join-fixtures.js";
import type { HashJoinKind, HashJoinSpec } from "./join.js";

const BOOL: PhysicalType = { kind: "bool" };
const kinds: HashJoinKind[] = ["inner", "left", "right", "full", "semi", "anti", "mark"];
const domains: ReadonlyArray<ReadonlyArray<boolean | null>> = [
  [true],
  [false],
  [true, false],
  [true, false, null],
  [null],
  [],
];
const reach = createReach("boolean direct joins", JOIN_BOOLEAN_REACH, { minimum: 2 });

for (const seed of seedsFor("next-join-boolean", [0xb001, 0xb002]))
  it(`boolean direct tables against PostgreSQL, seed ${String(seed)}`, async () => {
    const pg = new PGlite();
    await pg.waitReady;
    const random = mulberry32(seed);
    try {
      for (const domain of domains) {
        const build: Row[] =
          domain.length === 0
            ? []
            : Array.from({ length: 9 }, (_, i) => [
                domain[i < domain.length ? i : Math.floor(random() * domain.length)] ?? null,
                i,
              ]);
        const probe: Row[] = [
          [true, 10],
          [false, 11],
          [null, 12],
          [true, 13],
        ];
        await pg.exec(
          "DROP TABLE IF EXISTS p,b; CREATE TABLE p(k boolean,id integer); CREATE TABLE b(k boolean,id integer)",
        );
        for (const [table, rows] of [
          ["p", probe],
          ["b", build],
        ] as const)
          for (const row of rows) await pg.query(`INSERT INTO ${table} VALUES ($1,$2)`, [...row]);
        for (const kind of kinds) {
          const sql =
            kind === "mark"
              ? "SELECT p.*,p.k IN(SELECT b.k FROM b) AS found FROM p"
              : kind === "semi" || kind === "anti"
                ? `SELECT p.* FROM p WHERE ${kind === "anti" ? "NOT " : ""}EXISTS(SELECT 1 FROM b WHERE p.k=b.k)`
                : `SELECT p.*,b.* FROM p ${kind === "inner" ? "INNER" : kind.toUpperCase()} JOIN b ON p.k=b.k`;
          const expected = (
            await pg.query<Record<string, Value>>(sql, [], { rowMode: "array" })
          ).rows as unknown as Row[];
          for (const direct of [true, false]) {
            reach.beginHistory();
            const spec: HashJoinSpec = {
              kind,
              probeTypes: [BOOL, INT],
              buildTypes: [BOOL, INT],
              probeKeys: [0],
              buildKeys: [0],
              nullSafe: [false],
              residual: null,
              publishes: [],
            };
            const result = await runJoin(spec, build, probe, { direct, batchRows: 3, skip: 3 });
            expect(
              canonical(result.rows),
              `${kind} ${String(domain)} direct=${String(direct)}`,
            ).toEqual(canonical(expected));
            if (direct && build.length > 0) {
              expect(result.join.profile.table).toBe("direct");
              reach.reach("direct");
            }
            if (!direct) reach.reach("hashed");
            reach.reach(kind);
            if (domain.length === 1 && domain[0] === true) reach.reach("TRUE only");
            if (domain.length === 1 && domain[0] === false) reach.reach("FALSE only");
            if (domain.length === 2) reach.reach("both");
            if (domain.includes(null)) reach.reach("NULL");
            if (domain.length === 0) reach.reach("empty");
            await result.join.closed;
            expect(result.governor.usedBytes).toBe(0);
            expect(result.spill.bytesInUse).toBe(0);
            reach.reach("released");
          }
        }
      }
    } finally {
      await pg.close();
    }
  }, 120_000);

it("reaches every declared boolean table and join kind", () => reach.assertReached());
