/** INSERT ... SELECT, UPDATE and DELETE with computed expressions (text concatenation, casts,
 * arithmetic, COALESCE, NULLIF, CASE) over every kind of source, against PostgreSQL (PGlite), the
 * authority for rows, NULL propagation and affected-row counts. Text concatenation evaluates in
 * producer phases (exec/plan.ts), so these statements cross a phase handoff with a modification
 * spool alive. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, DML_COMPUTED_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

type Reach = ReturnType<typeof createReach<(typeof DML_COMPUTED_REACH)[number]>>;
/** A source's FROM clause and its key (unique per row), text and integer expressions. */
interface Source {
  readonly name: (typeof DML_COMPUTED_REACH)[number];
  readonly from: string;
  readonly k: string;
  readonly s: string;
  readonly a: string;
}
const SRC_ROWS = 40;
const sources = (series: number, limit: number): readonly Source[] => [
  {
    name: "series source",
    from: `generate_series(1, ${String(series)}) g`,
    k: "g",
    s: "nullif('s' || (g % 4)::text, 's0')",
    a: "nullif(g % 6, 0)",
  },
  { name: "table source", from: "src", k: "src.id", s: "src.s", a: "src.a" },
  {
    name: "values source",
    from: "(VALUES (1, 'a', 2), (2, NULL::text, NULL::int), (3, 's1', 7), (4, 'z', 0)) v(k, s, a)",
    k: "v.k",
    s: "v.s",
    a: "v.a",
  },
  {
    name: "join source",
    from: "src JOIN dim ON dim.k = src.a",
    k: "src.id",
    s: "dim.label",
    a: "src.a",
  },
  {
    name: "aggregate source",
    from: "(SELECT a, count(*)::int AS c, min(s) AS ms FROM src GROUP BY a) q",
    k: "coalesce(q.a, -1) + 50",
    s: "q.ms",
    a: "q.c",
  },
  {
    name: "limited subquery source",
    from: `(SELECT id, a, s FROM src ORDER BY id LIMIT ${String(limit)}) q`,
    k: "q.id",
    s: "q.s",
    a: "q.a",
  },
];
const texts: ReadonlyArray<{
  readonly reach: ReadonlyArray<(typeof DML_COMPUTED_REACH)[number]>;
  readonly sql: (x: Source) => string;
}> = [
  { reach: ["concatenation", "cast"], sql: (x) => `'p' || (${x.k})::text` },
  {
    reach: ["concatenation", "null text propagated"],
    sql: (x) => `${x.s} || '-' || (${x.k})::text`,
  },
  {
    reach: ["concatenation", "coalesce"],
    sql: (x) => `coalesce(${x.s}, 'none') || (${x.a})::text`,
  },
  {
    reach: ["case", "concatenation"],
    sql: (x) => `CASE WHEN (${x.k}) % 2 = 0 THEN 'even' ELSE coalesce(${x.s}, '') || 'odd' END`,
  },
  { reach: ["cast"], sql: (x) => `(${x.k})::text` },
  { reach: ["nullif"], sql: (x) => `nullif(${x.s}, 's1')` },
  { reach: ["concatenation", "arithmetic", "cast"], sql: (x) => `((${x.a}) * 2)::text || ${x.s}` },
  { reach: [], sql: (x) => x.s },
];
const ints: ReadonlyArray<{
  readonly reach: ReadonlyArray<(typeof DML_COMPUTED_REACH)[number]>;
  readonly sql: (x: Source) => string;
}> = [
  { reach: ["arithmetic"], sql: (x) => `(${x.k}) * 3 + 1` },
  { reach: ["arithmetic"], sql: (x) => `(${x.a}) - (${x.k})` },
  { reach: ["arithmetic", "coalesce"], sql: (x) => `coalesce(${x.a}, 0) % 7` },
  { reach: ["nullif", "arithmetic"], sql: (x) => `nullif((${x.k}) % 4, 1)` },
  {
    reach: ["case", "arithmetic"],
    sql: (x) => `CASE WHEN ${x.a} IS NULL THEN -1 ELSE (${x.a}) * (${x.a}) END`,
  },
];
const filters: ReadonlyArray<{
  readonly reach: ReadonlyArray<(typeof DML_COMPUTED_REACH)[number]>;
  readonly sql: (x: Source) => string | null;
}> = [
  { reach: [], sql: () => null },
  { reach: ["arithmetic"], sql: (x) => `(${x.k}) % 3 <> 0` },
  { reach: ["concatenation predicate"], sql: (x) => `${x.s} || 'z' <> 's1z'` },
];

const step = fc.oneof(
  fc.record({
    kind: fc.constant("insert" as const),
    source: fc.nat(5),
    text: fc.nat(texts.length - 1),
    int: fc.nat(ints.length - 1),
    filter: fc.nat(filters.length - 1),
  }),
  fc.record({
    kind: fc.constant("update" as const),
    suffix: fc.constantFrom("u", "", "-x"),
    delta: fc.integer({ min: -3, max: 3 }),
    where: fc.constantFrom("id % 2 = 0", "(v || 'q') <> 'evenq'", "n > 5", "true"),
  }),
  fc.record({
    kind: fc.constant("delete" as const),
    where: fc.constantFrom("v || '!' = 'even!'", "n % 5 = 0", "v IS NULL", "(v || '-') = 'p3-'"),
  }),
);

type Step = typeof step extends fc.Arbitrary<infer T> ? T : never;

async function history(
  steps: readonly Step[],
  series: number,
  limit: number,
  reach: Reach,
): Promise<void> {
  const core = await new StorageCore(32).open(),
    pg = new PGlite();
  await pg.waitReady;
  const both = async (sql: string): Promise<void> => {
    const expected = await pg.query(sql);
    const actual = await core.execute(sql);
    if ("rowCount" in actual && expected.affectedRows !== undefined)
      expect(actual.rowCount, sql).toBe(expected.affectedRows);
    const now = await core.execute("SELECT id, v, n FROM dst ORDER BY id");
    if (!("batches" in now)) throw new Error("dst rows");
    expect(oracleRows(now), sql).toEqual(
      (await pg.query("SELECT id, v, n FROM dst ORDER BY id")).rows,
    );
    expect(core.spill.bytesInUse, sql).toBe(0);
  };
  try {
    for (const ddl of [
      "CREATE TABLE src(id INT PRIMARY KEY, a INT, s TEXT)",
      "CREATE TABLE dim(k INT PRIMARY KEY, label TEXT)",
      "CREATE TABLE dst(id INT PRIMARY KEY, v TEXT, n INT)",
      `INSERT INTO src VALUES ${Array.from({ length: SRC_ROWS }, (_, i) => `(${String(i + 1)}, ${i % 7 === 0 ? "NULL" : String(i % 9)}, ${i % 5 === 0 ? "NULL" : `'s${String(i % 4)}'`})`).join(", ")}`,
      `INSERT INTO dim VALUES ${Array.from({ length: 9 }, (_, k) => `(${String(k)}, ${k === 4 ? "NULL" : `'d${String(k)}'`})`).join(", ")}`,
    ]) {
      await pg.query(ddl);
      await core.execute(ddl);
    }
    const from = sources(series, limit);
    for (const [index, item] of steps.entries()) {
      if (item.kind === "insert") {
        const source = from[item.source],
          text = texts[item.text],
          int = ints[item.int],
          filter = filters[item.filter];
        if (!source || !text || !int || !filter) throw new Error("generator");
        const where = filter.sql(source);
        // Keys stay unique across a history: each insert owns its own block of ids.
        const base = 10000 * (index + 1);
        await both(
          `INSERT INTO dst(id, v, n) SELECT ${String(base)} + ${source.k}, ${text.sql(source)}, ${int.sql(source)} FROM ${source.from}${where === null ? "" : ` WHERE ${where}`}`,
        );
        for (const name of [source.name, ...text.reach, ...int.reach, ...filter.reach])
          reach.reach(name);
        if (source.name === "series source" && series >= 4096) reach.reach("large insert");
      } else if (item.kind === "update") {
        await both(
          `UPDATE dst SET v = v || '${item.suffix}', n = n + ${String(item.delta)} WHERE ${item.where}`,
        );
        reach.reach("update concatenation");
      } else {
        await both(`DELETE FROM dst WHERE ${item.where}`);
        if (item.where.includes("||")) reach.reach("delete concatenation predicate");
      }
    }
  } finally {
    await core.close();
    await pg.close();
  }
}

for (const seed of seedsFor("next-dml-computed", [0xd5e1, 0xd5e2]))
  it(`computed INSERT ... SELECT, UPDATE and DELETE match PostgreSQL (seed ${String(seed)})`, async () => {
    const reach = createReach("computed DML", DML_COMPUTED_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(step, { minLength: 1, maxLength: 6 }),
        fc.constantFrom<number>(5, 50, 300),
        fc.integer({ min: 0, max: 45 }),
        async (steps, series, limit) => {
          reach.beginHistory();
          await history(steps, series, limit, reach);
        },
      ),
      {
        seed,
        numRuns: 10,
        // Every source kind, text and integer expression once, and the reported 5,000-row shape.
        examples: [
          [
            texts.map((_, text): Step => ({
              kind: "insert",
              source: text % 6,
              text,
              int: text % ints.length,
              filter: text % filters.length,
            })),
            50,
            7,
          ],
          [[{ kind: "insert", source: 0, text: 0, int: 0, filter: 0 }], 5000, 0],
          [
            [
              { kind: "insert", source: 1, text: 1, int: 4, filter: 2 },
              { kind: "update", suffix: "u", delta: 1, where: "(v || 'q') <> 'evenq'" },
              { kind: "delete", where: "v || '!' = 'even!'" },
            ],
            5,
            3,
          ],
        ],
      },
    );
    reach.assertReached();
  }, 240000);
