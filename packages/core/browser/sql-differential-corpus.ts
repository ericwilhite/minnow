export type SqlParameter = boolean | number | string | null;

export interface DifferentialQuery {
  readonly label: string;
  readonly sql: string;
  readonly params?: readonly SqlParameter[];
  readonly ordered?: boolean;
  readonly oracles?: ReadonlyArray<"sqlite" | "pglite">;
  readonly jsonColumns?: readonly number[];
  readonly numericColumns?: readonly number[];
  readonly datetimeColumns?: readonly number[];
  readonly affectedRows?: number;
  /** Decimal comparison precision for floating aggregates whose accumulation differs by engine. */
  readonly numericDigits?: number;
}

export interface FixtureRow {
  readonly id: number;
  readonly region: string | null;
  readonly amount: number;
  readonly active: boolean;
  readonly note: string | null;
  readonly payload: string;
  readonly joined: string | null;
}

const REGIONS = ["west", "east", "north", "south", null] as const;
const NOTES = [
  "plain ASCII",
  "café",
  "emoji 🐟",
  "e\u0301 combining",
  "line one\nline two",
  "tab\there",
  "apostrophe ' and quote \"",
  "control \u007f marker",
  "東京",
  null,
] as const;

/** The same tiny PRNG as the Node generative suites, kept local to the browser-only harness. */
function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
  };
}

function pick<T>(random: () => number, values: readonly T[]): T {
  const value = values[Math.floor(random() * values.length)];
  if (value === undefined) throw new Error("empty SQL differential choice");
  return value;
}

export function fixtureRows(seed: number): FixtureRow[] {
  const random = mulberry32(seed ^ 0x5eed);
  return Array.from({ length: 32 }, (_, index) => {
    const id = index + 1;
    const region = pick(random, REGIONS);
    const amount = (Math.floor(random() * 160) - 40) / 4;
    return {
      id,
      region,
      amount,
      active: random() < 0.5,
      note: NOTES[index % NOTES.length] ?? null,
      payload: JSON.stringify({
        kind: region ?? "none",
        nested: { id, active: id % 2 === 0 },
        tags: [id % 3, `tag-${String(id % 5)}`],
      }),
      joined:
        id % 7 === 0
          ? null
          : new Date(Date.UTC(2025 + (id % 2), id % 12, 1 + (id % 27), id % 24)).toISOString(),
    };
  });
}

export const fixedQueries: readonly DifferentialQuery[] = [
  {
    label: "empty result preserves its columns",
    sql: "SELECT id, region FROM items WHERE id < 0 ORDER BY id",
    ordered: true,
  },
  {
    label: "NULL disjunction follows three-valued logic",
    sql: "SELECT id FROM items WHERE region = 'west' OR region = NULL ORDER BY id",
    ordered: true,
  },
  {
    label: "NOT IN with a NULL member returns no true predicate",
    sql: "SELECT id FROM items WHERE region NOT IN ('west', NULL) ORDER BY id",
    ordered: true,
  },
  {
    label: "NULL ordering is explicit",
    sql: "SELECT id, region FROM items ORDER BY region NULLS FIRST, id",
    ordered: true,
  },
  {
    label: "Unicode and control text survives the worker and Wasm boundaries",
    sql: "SELECT id, note FROM items WHERE id <= 10 ORDER BY id",
    ordered: true,
  },
  {
    label: "quoted question marks stay text beside a real placeholder",
    sql: "SELECT '?' AS literal, ? AS bound FROM items WHERE id = 1",
    params: ["value?"],
  },
  {
    label: "integer division and signed remainders",
    sql: "SELECT id, id / 3 AS bucket, -id % 3 AS remainder FROM items ORDER BY id",
    ordered: true,
  },
  {
    label: "JSON scalar extraction follows PostgreSQL",
    sql: "SELECT id, CAST(payload AS JSON) ->> 'kind' AS kind FROM items ORDER BY id",
    ordered: true,
    oracles: ["pglite"],
  },
  {
    label: "JSON documents compare by value across client representations",
    sql: "SELECT id, CAST(payload AS JSON) -> 'nested' AS nested FROM items ORDER BY id",
    ordered: true,
    oracles: ["pglite"],
    jsonColumns: [1],
  },
  {
    label: "timestamp literals and returned instants",
    sql: "SELECT id, joined FROM items WHERE joined >= TIMESTAMP '2026-01-01 00:00:00' ORDER BY id",
    ordered: true,
    oracles: ["pglite"],
    datetimeColumns: [1],
  },
];

type Template = (random: () => number, round: number) => DifferentialQuery;

const templates: readonly Template[] = [
  (random) => ({
    label: "numeric predicate",
    sql: "SELECT id, amount FROM items WHERE amount >= ? ORDER BY id",
    params: [(Math.floor(random() * 120) - 30) / 4],
    ordered: true,
  }),
  (random) => ({
    label: "nullable predicate",
    sql: "SELECT id, region FROM items WHERE region = ? OR region IS NULL ORDER BY id",
    params: [pick(random, REGIONS)],
    ordered: true,
  }),
  (random) => ({
    label: "IN and boolean predicate",
    sql: "SELECT id, region FROM items WHERE region IN (?, ?) AND active = ? ORDER BY id",
    params: [pick(random, REGIONS), pick(random, REGIONS), random() < 0.5],
    ordered: true,
  }),
  (random) => ({
    label: "parameterized row window",
    sql: "SELECT id, amount FROM items ORDER BY amount, id LIMIT ? OFFSET ?",
    params: [1 + Math.floor(random() * 12), Math.floor(random() * 8)],
    ordered: true,
  }),
  (random) => ({
    label: "grouped aggregates",
    sql: "SELECT region, COUNT(*) AS n, SUM(amount) AS total, AVG(amount) AS mean, MIN(amount) AS low, MAX(amount) AS high FROM items WHERE amount >= ? GROUP BY region",
    params: [(Math.floor(random() * 80) - 30) / 4],
    numericDigits: 9,
  }),
  (random) => ({
    label: "join with an extra ON predicate",
    sql: "SELECT i.id, d.label, d.weight FROM items i LEFT JOIN dims d ON d.region = i.region AND d.weight >= ? ORDER BY i.id",
    params: [1 + Math.floor(random() * 4)],
    ordered: true,
  }),
  (random) => ({
    label: "CTE filter",
    sql: "WITH filtered AS (SELECT id, region, amount FROM items WHERE amount > ?) SELECT id, region FROM filtered WHERE region IS NOT NULL ORDER BY id",
    params: [(Math.floor(random() * 80) - 30) / 4],
    ordered: true,
  }),
  (random) => ({
    label: "derived aggregate",
    sql: "SELECT grouped.region, grouped.total FROM (SELECT region, SUM(amount) AS total FROM items GROUP BY region) grouped WHERE grouped.total >= ?",
    params: [Math.floor(random() * 30) - 15],
    numericDigits: 9,
  }),
  () => ({
    label: "DISTINCT over nullable columns",
    sql: "SELECT DISTINCT region, active FROM items",
  }),
  () => ({
    label: "window rank with deterministic ties",
    sql: "SELECT id, region, RANK() OVER (PARTITION BY region ORDER BY amount, id) AS position FROM items ORDER BY id",
    ordered: true,
  }),
  (random) => ({
    label: "set operation",
    sql: "SELECT region FROM items WHERE amount > ? UNION SELECT region FROM dims",
    params: [Math.floor(random() * 20) - 5],
  }),
  (random) => ({
    label: "CASE and NULL branch",
    sql: "SELECT id, CASE WHEN amount > ? THEN 'high' WHEN region IS NULL THEN 'none' ELSE 'low' END AS band FROM items ORDER BY id",
    params: [Math.floor(random() * 20) - 5],
    ordered: true,
  }),
  (random, round) => ({
    label: "Unicode parameter round trip",
    sql: "SELECT id, note FROM items WHERE note = ? OR id = ? ORDER BY id",
    params: [NOTES[round % NOTES.length] ?? null, 1 + Math.floor(random() * 32)],
    ordered: true,
  }),
  () => ({
    label: "correlated scalar aggregate",
    sql: "SELECT i.id, i.amount FROM items i WHERE i.amount > (SELECT AVG(j.amount) FROM items j WHERE j.region = i.region) ORDER BY i.id",
    ordered: true,
    numericDigits: 9,
  }),
];

export function generatedQueries(seed: number, rounds = 42): DifferentialQuery[] {
  const random = mulberry32(seed);
  return Array.from({ length: rounds }, (_, round) =>
    templates[round % templates.length]?.(random, round),
  ).filter((query) => query !== undefined);
}

export const mutationQueries: readonly DifferentialQuery[] = [
  {
    label: "INSERT RETURNING Unicode and JSON text",
    sql: "INSERT INTO items VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id, region, amount, active, note, payload",
    params: [
      100,
      null,
      12.5,
      true,
      "inserted 🐟\nrow",
      '{"kind":"new","n":1}',
      "2026-08-01T12:30:00.000Z",
    ],
    affectedRows: 1,
  },
  {
    label: "UPDATE RETURNING expressions and NULL replacement",
    sql: "UPDATE items SET amount = amount + ?, note = COALESCE(note, '') || ? WHERE id IN (?, ?) RETURNING id, amount, note, active",
    params: [1.25, " Δ", 3, 10],
    affectedRows: 2,
  },
  {
    label: "upsert RETURNING the post-image",
    sql: "INSERT INTO items (id, region, amount, active, note, payload, joined) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET amount = excluded.amount, note = excluded.note RETURNING id, region, amount, note",
    params: [3, "ignored", -9.75, false, "upserted", '{"kind":"upsert"}', null],
    affectedRows: 1,
  },
  {
    label: "DELETE RETURNING the pre-image",
    sql: "DELETE FROM items WHERE id IN (?, ?) RETURNING id, note, amount",
    params: [4, 100],
    affectedRows: 2,
  },
];
