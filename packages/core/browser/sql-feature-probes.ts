export interface ExpectedFeatureResult {
  readonly columns: readonly string[];
  readonly rows: ReadonlyArray<readonly unknown[]>;
  readonly ordered?: boolean;
}

export type FeatureProbeParameter = boolean | number | string | null;

export type ExpectedFeatureValue =
  | boolean
  | number
  | string
  | null
  | { readonly matcher: "timestamp" }
  | { readonly matcher: "uuid" }
  | {
      readonly matcher: "finite-number";
      readonly minimumInclusive?: number;
      readonly minimumExclusive?: number;
      readonly maximumExclusive?: number;
    };

export interface ExpectedFeaturePatternResult {
  readonly columns: readonly string[];
  readonly rows: ReadonlyArray<readonly ExpectedFeatureValue[]>;
}

export type FeatureBehaviorStep =
  | {
      readonly kind: "query";
      readonly sql: string;
      readonly params?: readonly FeatureProbeParameter[];
      readonly expected: ExpectedFeatureResult;
      /** Run a statement such as SHOW through execute() and inspect its row result. */
      readonly throughExecute?: boolean;
      /** Also compare this stable result with PGlite when the matrix entry is compatible. */
      readonly compareWithPglite?: boolean;
    }
  | {
      readonly kind: "query-pattern";
      readonly sql: string;
      readonly params?: readonly FeatureProbeParameter[];
      readonly expected: ExpectedFeaturePatternResult;
    }
  | {
      readonly kind: "mutation";
      readonly sql: string;
      readonly params?: readonly FeatureProbeParameter[];
      readonly affectedRows: number;
      readonly expected?: ExpectedFeatureResult;
    }
  | {
      readonly kind: "execute";
      readonly sql: string;
      readonly params?: readonly FeatureProbeParameter[];
    }
  | {
      readonly kind: "error";
      readonly sql: string;
      readonly params?: readonly FeatureProbeParameter[];
      readonly errorName: string;
      readonly includes: string;
    };

export interface FeatureBehaviorProbe {
  readonly featureId: string;
  readonly steps: readonly FeatureBehaviorStep[];
  /** This probe queries the mutation's real target, replacing the generic `keyed` comparison. */
  readonly replacesCompatibleMutationState?: boolean;
}

const emptyMutationResult: ExpectedFeatureResult = { columns: [], rows: [] };
const keyedBaseRows: ReadonlyArray<readonly unknown[]> = [
  ["x", 1, null],
  ["y", -1, null],
];

/**
 * Fixed semantic postconditions for matrix examples whose syntax acceptance is not enough.
 *
 * These answers are authored from the fixture and each feature's documented behavior. They are
 * deliberately data rather than results captured from Minnow, so an engine regression cannot
 * update its own oracle.
 */
export const featureBehaviorProbes: readonly FeatureBehaviorProbe[] = [
  {
    featureId: "aggregate.any-value",
    steps: [
      {
        kind: "query",
        sql: "SELECT ANY_VALUE(amount) AS sample FROM rows",
        expected: { columns: ["sample"], rows: [[3]] },
      },
    ],
  },
  {
    featureId: "mutation.insert-default-values",
    replacesCompatibleMutationState: true,
    steps: [
      {
        kind: "query",
        sql: "SELECT name, score FROM defaulted_insert",
        expected: { columns: ["name", "score"], rows: [["generated", 7]] },
        compareWithPglite: true,
      },
    ],
  },
  {
    featureId: "mutation.insert-runtime-values",
    replacesCompatibleMutationState: true,
    steps: [
      {
        kind: "query-pattern",
        sql: "SELECT id, noted_at, sample, token FROM runtime_values",
        expected: {
          columns: ["id", "noted_at", "sample", "token"],
          rows: [
            [
              1,
              { matcher: "timestamp" },
              { matcher: "finite-number", minimumInclusive: 0, maximumExclusive: 1 },
              { matcher: "uuid" },
            ],
          ],
        },
      },
      {
        kind: "query",
        sql: "SELECT id, noted_at IS NOT NULL AS has_time, sample >= 0 AND sample < 1 AS sample_in_range, token IS NOT NULL AS has_token FROM runtime_values",
        expected: {
          columns: ["id", "has_time", "sample_in_range", "has_token"],
          rows: [[1, true, true, true]],
        },
        compareWithPglite: true,
      },
    ],
  },
  {
    featureId: "mutation.truncate",
    steps: [
      {
        kind: "query",
        sql: "SELECT name, score, bonus FROM keyed ORDER BY name NULLS LAST",
        expected: { columns: ["name", "score", "bonus"], rows: [], ordered: true },
      },
    ],
  },
  {
    featureId: "mutation.upsert-replace",
    steps: [
      {
        kind: "query",
        sql: "SELECT name, score, bonus FROM keyed ORDER BY name NULLS LAST",
        expected: {
          columns: ["name", "score", "bonus"],
          rows: [
            ["x", 50, 9],
            ["y", -1, null],
          ],
          ordered: true,
        },
      },
    ],
  },
  {
    featureId: "mutation.upsert-expression",
    steps: [
      {
        kind: "query",
        sql: "SELECT name, score, bonus FROM keyed ORDER BY name NULLS LAST",
        expected: {
          columns: ["name", "score", "bonus"],
          rows: [
            ["x", 3, null],
            ["y", -1, null],
          ],
          ordered: true,
        },
      },
    ],
  },
  ...["trigger.create-after", "trigger.create-before"].map((featureId): FeatureBehaviorProbe => ({
    featureId,
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO keyed (name, score) VALUES ('z', 3)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT region, amount FROM rows ORDER BY region, amount",
        expected: {
          columns: ["region", "amount"],
          rows: [
            ["east", 2],
            ["west", 1],
            ["z", 3],
          ],
          ordered: true,
        },
      },
    ],
  })),
  {
    featureId: "trigger.body-update-delete",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO keyed (name, score) VALUES ('east', 4)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT region, total FROM stats ORDER BY region",
        expected: {
          columns: ["region", "total"],
          rows: [
            ["east", 4],
            ["west", 0],
          ],
          ordered: true,
        },
      },
    ],
  },
  {
    featureId: "trigger.drop",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO keyed (name, score) VALUES ('z', 3)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT region, amount FROM rows ORDER BY region, amount",
        expected: {
          columns: ["region", "amount"],
          rows: [
            ["east", 2],
            ["west", 1],
          ],
          ordered: true,
        },
      },
    ],
  },
  {
    featureId: "ddl.create-table-default",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO defaulted (id) VALUES (1)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT id, tier FROM defaulted",
        expected: { columns: ["id", "tier"], rows: [[1, "basic"]] },
      },
    ],
  },
  {
    featureId: "ddl.generated-column",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO generated_value (base) VALUES (3)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT base, doubled FROM generated_value",
        expected: { columns: ["base", "doubled"], rows: [[3, 6]] },
      },
    ],
  },
  {
    featureId: "ddl.unique-secondary-index",
    steps: [
      {
        kind: "mutation",
        sql: "UPDATE keyed SET bonus = 7 WHERE name = 'x'",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "error",
        sql: "INSERT INTO keyed (name, score, bonus) VALUES ('z', 2, 7)",
        errorName: "UniqueConstraintError",
        includes: "Duplicate value for keyed.bonus: 7",
      },
      {
        kind: "query",
        sql: "SELECT name, score, bonus FROM keyed ORDER BY name NULLS LAST",
        expected: {
          columns: ["name", "score", "bonus"],
          rows: [
            ["x", 1, 7],
            ["y", -1, null],
          ],
          ordered: true,
        },
      },
    ],
  },
  {
    featureId: "ddl.check-constraint",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO checked (a) VALUES (1), (99)",
        affectedRows: 2,
        expected: emptyMutationResult,
      },
      {
        kind: "error",
        sql: "INSERT INTO checked (a) VALUES (0)",
        errorName: "TypeError",
        includes: "CHECK checked_a_check failed for row 0 of checked",
      },
      {
        kind: "query",
        sql: "SELECT a FROM checked ORDER BY a",
        expected: { columns: ["a"], rows: [[1], [99]], ordered: true },
      },
    ],
  },
  {
    featureId: "ddl.foreign-key",
    steps: [
      {
        kind: "error",
        sql: "INSERT INTO children (id, parent) VALUES (1, 9)",
        errorName: "TypeError",
        includes: "FOREIGN KEY children_parent_fkey has no parents row with id 9",
      },
      {
        kind: "mutation",
        sql: "INSERT INTO parents (id, label) VALUES (9, 'parent')",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "mutation",
        sql: "INSERT INTO children (id, parent) VALUES (1, 9)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "mutation",
        sql: "DELETE FROM parents WHERE id = 9",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT id, parent FROM children",
        expected: { columns: ["id", "parent"], rows: [] },
      },
    ],
  },
  {
    featureId: "ddl.create-view",
    steps: [
      {
        kind: "query",
        sql: "SELECT region, amount FROM west",
        expected: { columns: ["region", "amount"], rows: [["west", 1]] },
      },
    ],
  },
  {
    featureId: "ddl.sequence",
    steps: [
      {
        kind: "query",
        sql: "SELECT NEXTVAL('order_ids') AS id",
        expected: { columns: ["id"], rows: [[1]] },
      },
      {
        kind: "query",
        sql: "SELECT NEXTVAL('order_ids') AS id",
        expected: { columns: ["id"], rows: [[2]] },
      },
    ],
  },
  ...(
    [
      ["ddl.serial", "ticketed"],
      ["ddl.identity", "identified"],
    ] as const
  ).map(([featureId, table]): FeatureBehaviorProbe => ({
    featureId,
    steps: [
      {
        kind: "mutation",
        sql: `INSERT INTO ${table} (label) VALUES ('a'), ('b')`,
        affectedRows: 2,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: `SELECT id, label FROM ${table} ORDER BY id`,
        expected: {
          columns: ["id", "label"],
          rows: [
            [1, "a"],
            [2, "b"],
          ],
          ordered: true,
        },
      },
    ],
  })),
  {
    featureId: "ddl.alter-table-add-column-default",
    steps: [
      {
        kind: "query",
        sql: "SELECT id, tier FROM filled ORDER BY id",
        expected: {
          columns: ["id", "tier"],
          rows: [
            [1, "basic"],
            [2, "basic"],
          ],
          ordered: true,
        },
      },
    ],
  },
  {
    featureId: "transaction.begin",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO keyed (name, score) VALUES ('z', 3)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT name, score, bonus FROM keyed ORDER BY name NULLS LAST",
        expected: {
          columns: ["name", "score", "bonus"],
          rows: [...keyedBaseRows, ["z", 3, null]],
          ordered: true,
        },
      },
      { kind: "execute", sql: "ROLLBACK" },
      {
        kind: "query",
        sql: "SELECT name, score, bonus FROM keyed ORDER BY name NULLS LAST",
        expected: { columns: ["name", "score", "bonus"], rows: keyedBaseRows, ordered: true },
      },
    ],
  },
  ...(
    [
      ["transaction.end", "END", true],
      ["transaction.abort", "ABORT", false],
      ["transaction.commit", "COMMIT", true],
      ["transaction.rollback", "ROLLBACK", false],
    ] as const
  ).map(([featureId, closeSql, commits]): FeatureBehaviorProbe => ({
    featureId,
    steps: [
      { kind: "execute", sql: "BEGIN" },
      {
        kind: "mutation",
        sql: "INSERT INTO keyed (name, score) VALUES ('z', 3)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      { kind: "execute", sql: closeSql },
      {
        kind: "query",
        sql: "SELECT name, score, bonus FROM keyed ORDER BY name NULLS LAST",
        expected: {
          columns: ["name", "score", "bonus"],
          rows: commits ? [...keyedBaseRows, ["z", 3, null]] : keyedBaseRows,
          ordered: true,
        },
      },
    ],
  })),
  {
    featureId: "transaction.savepoint",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO keyed (name, score) VALUES ('z', 3)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      { kind: "execute", sql: "ROLLBACK TO line_item" },
      { kind: "execute", sql: "RELEASE line_item" },
      { kind: "execute", sql: "COMMIT" },
      {
        kind: "query",
        sql: "SELECT name, score, bonus FROM keyed ORDER BY name NULLS LAST",
        expected: { columns: ["name", "score", "bonus"], rows: keyedBaseRows, ordered: true },
      },
    ],
  },
  {
    featureId: "transaction.session-settings",
    steps: [
      {
        kind: "query",
        sql: "SHOW search_path",
        throughExecute: true,
        expected: { columns: ["search_path"], rows: [["public"]] },
      },
    ],
  },
  {
    featureId: "transaction.show-setting",
    steps: [
      {
        kind: "query",
        sql: "SHOW server_version",
        throughExecute: true,
        expected: { columns: ["server_version"], rows: [["16.0"]] },
      },
    ],
  },
  {
    featureId: "expression.arithmetic",
    steps: [
      {
        kind: "query",
        sql: "SELECT amount * 2 + 1 AS scaled FROM rows",
        expected: { columns: ["scaled"], rows: [[21], [13], [7], [17]] },
      },
    ],
  },
  {
    featureId: "expression.round",
    steps: [
      {
        kind: "query",
        sql: "SELECT ROUND(amount / 3, 2) AS thirds FROM rows",
        expected: { columns: ["thirds"], rows: [[3.33], [2], [1], [2.67]] },
      },
    ],
  },
  {
    featureId: "parameter.positional",
    steps: [
      {
        kind: "query",
        sql: "SELECT region, amount FROM rows WHERE amount >= ? AND active = ? ORDER BY amount",
        params: [6, true],
        expected: {
          columns: ["region", "amount"],
          rows: [
            [null, 8],
            ["west", 10],
          ],
          ordered: true,
        },
      },
    ],
  },
  ...[
    ["predicate.match", "SELECT region FROM rows WHERE MATCH(region) AGAINST 'west'"],
    ["predicate.match-star", "SELECT region FROM rows WHERE MATCH(*) AGAINST 'wes*'"],
  ].map(([featureId, sql]): FeatureBehaviorProbe => ({
    featureId: featureId ?? "",
    steps: [
      {
        kind: "query",
        sql: sql ?? "",
        expected: { columns: ["region"], rows: [["west"], ["west"]] },
      },
    ],
  })),
  {
    featureId: "predicate.match-parameter",
    steps: [
      {
        kind: "query",
        sql: "SELECT region FROM rows WHERE MATCH(region) AGAINST $1 ORDER BY BM25(region) AGAINST $1 DESC",
        params: ["west"],
        expected: { columns: ["region"], rows: [["west"], ["west"]], ordered: true },
      },
    ],
  },
  {
    featureId: "function.bm25",
    steps: [
      {
        kind: "query-pattern",
        sql: "SELECT region, BM25(region) AGAINST 'west' AS score FROM rows WHERE MATCH(region) AGAINST 'west' ORDER BY score DESC",
        expected: {
          columns: ["region", "score"],
          rows: [
            ["west", { matcher: "finite-number", minimumExclusive: 0 }],
            ["west", { matcher: "finite-number", minimumExclusive: 0 }],
          ],
        },
      },
    ],
  },
  {
    featureId: "subquery.correlated-json-aggregate",
    steps: [
      {
        kind: "query",
        sql: "SELECT r.region, (SELECT JSON_ARRAYAGG(JSON_OBJECT('amount' VALUE q.amount) ORDER BY q.amount) FROM rows q WHERE q.region = r.region) AS amounts FROM rows r",
        expected: {
          columns: ["region", "amounts"],
          rows: [
            ["west", '[{"amount":6},{"amount":10}]'],
            ["west", '[{"amount":6},{"amount":10}]'],
            ["east", '[{"amount":3}]'],
            [null, null],
          ],
        },
      },
    ],
  },
  {
    featureId: "expression.modulo",
    steps: [
      {
        kind: "query",
        sql: "SELECT amount % 3 AS remainder FROM rows",
        expected: { columns: ["remainder"], rows: [[1], [0], [0], [2]] },
      },
    ],
  },
  {
    featureId: "function.numeric-core",
    steps: [
      {
        kind: "query",
        sql: "SELECT NULLIF(amount, 3) AS n, GREATEST(amount, 5) AS g, LEAST(amount, 5) AS l, FLOOR(amount) AS f, CEILING(amount) AS c, MOD(amount, 4) AS m, POWER(2, 3) AS p, SQRT(16) AS s FROM rows",
        expected: {
          columns: ["n", "g", "l", "f", "c", "m", "p", "s"],
          rows: [
            [10, 10, 5, 10, 10, 2, 8, 4],
            [6, 6, 5, 6, 6, 2, 8, 4],
            [null, 5, 3, 3, 3, 3, 8, 4],
            [8, 8, 5, 8, 8, 0, 8, 4],
          ],
        },
      },
    ],
  },
  {
    featureId: "function.string-extended",
    steps: [
      {
        kind: "query",
        sql: "SELECT REPLACE(region, 'we', 'be') AS r, LTRIM(' x') AS lt, RTRIM('x ') AS rt, INSTR(region, 'st') AS i FROM rows WHERE region IS NOT NULL",
        expected: {
          columns: ["r", "lt", "rt", "i"],
          rows: [
            ["best", "x", "x", 3],
            ["best", "x", "x", 3],
            ["east", "x", "x", 3],
          ],
        },
      },
    ],
  },
  {
    featureId: "function.trim-multi-character",
    steps: [
      {
        kind: "query",
        sql: "SELECT TRIM(BOTH 'we' FROM region) AS trimmed FROM rows WHERE region IS NOT NULL",
        expected: { columns: ["trimmed"], rows: [["st"], ["st"], ["east"]] },
      },
    ],
  },
  {
    featureId: "literal.scientific",
    steps: [
      {
        kind: "query-pattern",
        sql: "SELECT 2.5e-1 AS quarter",
        expected: { columns: ["quarter"], rows: [[0.25]] },
      },
    ],
  },
  {
    featureId: "json.query",
    steps: [
      {
        kind: "query",
        sql: `SELECT JSON_QUERY('{"a": [1, 2]}', '$.a') AS a`,
        expected: { columns: ["a"], rows: [["[1,2]"]] },
      },
    ],
  },
  {
    featureId: "json.object",
    steps: [
      {
        kind: "query",
        sql: "SELECT JSON_OBJECT('a' VALUE 1, 'detail' VALUE JSON_OBJECT('name' VALUE 'Acme')) AS document",
        expected: { columns: ["document"], rows: [['{"a":1,"detail":{"name":"Acme"}}']] },
      },
    ],
  },
  {
    featureId: "json.array",
    steps: [
      {
        kind: "query",
        sql: "SELECT JSON_ARRAY(1, NULL, 2) AS document",
        expected: { columns: ["document"], rows: [["[1,null,2]"]] },
      },
    ],
  },
  {
    featureId: "json.arrow",
    steps: [
      {
        kind: "query",
        sql: `SELECT CAST('{"a": {"b": [5, 6]}}' AS JSON) -> 'a' -> 'b' -> 1 AS element`,
        expected: { columns: ["element"], rows: [["6"]] },
      },
    ],
  },
  {
    featureId: "json.arrow-untyped",
    steps: [
      {
        kind: "query",
        sql: `SELECT '{"a": {"b": ["x", "y"]}}' -> 'a' -> 'b' ->> 1 AS second_item`,
        expected: { columns: ["second_item"], rows: [["y"]] },
      },
    ],
  },
  {
    featureId: "type.exact-numeric",
    steps: [
      {
        kind: "query-pattern",
        sql: "SELECT CAST(1.25 AS DECIMAL(12, 2)) AS amount",
        expected: { columns: ["amount"], rows: [["1.25"]] },
      },
    ],
  },
  {
    featureId: "type.json-jsonb",
    steps: [
      {
        kind: "query",
        sql: `SELECT CAST('{"a":1}' AS JSONB) AS document`,
        expected: { columns: ["document"], rows: [['{"a":1}']] },
      },
    ],
  },
  {
    featureId: "type.interval",
    steps: [
      {
        kind: "query",
        sql: "SELECT joined + INTERVAL '1 day' AS next_day FROM rows",
        expected: {
          columns: ["next_day"],
          rows: [
            [new Date("2026-01-03T00:00:00.000Z")],
            [new Date("2025-12-31T00:00:00.000Z")],
            [new Date("2026-02-02T00:00:00.000Z")],
            [null],
          ],
        },
      },
    ],
  },
  {
    featureId: "aggregate.json",
    steps: [
      {
        kind: "query",
        sql: "SELECT JSON_ARRAYAGG(JSON_OBJECT('region' VALUE region) ORDER BY amount) AS regions FROM rows",
        expected: {
          columns: ["regions"],
          rows: [['[{"region":"east"},{"region":"west"},{"region":null},{"region":"west"}]']],
        },
      },
    ],
  },
  {
    featureId: "aggregate.array-agg",
    steps: [
      {
        kind: "query",
        sql: "SELECT region, array_agg(amount ORDER BY amount) AS amounts FROM rows GROUP BY region ORDER BY region",
        expected: {
          columns: ["region", "amounts"],
          rows: [
            ["east", "[3]"],
            ["west", "[6,10]"],
            [null, "[8]"],
          ],
          ordered: true,
        },
      },
    ],
  },
  {
    featureId: "type.array",
    steps: [
      {
        kind: "query-pattern",
        sql: "SELECT ARRAY[1, 2] AS pair",
        expected: { columns: ["pair"], rows: [["[1,2]"]] },
      },
    ],
  },
  {
    featureId: "type.date",
    steps: [
      {
        kind: "query-pattern",
        sql: "SELECT CAST('2026-08-26' AS DATE) AS day",
        expected: { columns: ["day"], rows: [["2026-08-26"]] },
      },
    ],
  },
  {
    featureId: "function.to-date-timestamp",
    steps: [
      {
        kind: "query",
        sql: "SELECT TO_DATE('02/01/2026', 'DD/MM/YYYY') AS day, TO_TIMESTAMP('2026-01-02 03:04 PM', 'YYYY-MM-DD HH12:MI AM') AS at, TO_TIMESTAMP(1767322800) AS epoch",
        expected: {
          columns: ["day", "at", "epoch"],
          rows: [
            [
              "2026-01-02",
              new Date("2026-01-02T15:04:00.000Z"),
              new Date("2026-01-02T03:00:00.000Z"),
            ],
          ],
        },
      },
    ],
  },
  {
    featureId: "function.make-date",
    steps: [
      {
        kind: "query",
        sql: "SELECT MAKE_DATE(2026, 1, 2) AS day, MAKE_TIMESTAMP(2026, 1, 2, 3, 4, 5.5) AS at",
        expected: {
          columns: ["day", "at"],
          rows: [["2026-01-02", new Date("2026-01-02T03:04:05.500Z")]],
        },
      },
    ],
  },
  {
    featureId: "function.age",
    steps: [
      {
        kind: "query",
        sql: "SELECT AGE(TIMESTAMP '2026-03-15 12:00:00', joined) AS since FROM rows WHERE joined IS NOT NULL",
        expected: {
          columns: ["since"],
          rows: [
            ["2 mons 13 days 43200000000 usecs"],
            ["2 mons 16 days 43200000000 usecs"],
            ["1 mons 14 days 43200000000 usecs"],
          ],
        },
      },
      {
        kind: "query",
        sql: "SELECT AGE(joined) = AGE(CURRENT_DATE, joined) AS uses_statement_date FROM rows ORDER BY joined NULLS LAST",
        expected: {
          columns: ["uses_statement_date"],
          rows: [[true], [true], [true], [null]],
          ordered: true,
        },
      },
    ],
  },
  {
    featureId: "ddl.create-table",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO made VALUES (1, 'made', TIMESTAMP '2026-01-02 03:04:05')",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT id, label, at FROM made",
        expected: {
          columns: ["id", "label", "at"],
          rows: [[1, "made", new Date("2026-01-02T03:04:05.000Z")]],
        },
      },
    ],
  },
  {
    featureId: "ddl.type-spellings",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO spelled VALUES (1, 'abc', 'note', TIMESTAMP '2026-01-02 03:04:05', TIMESTAMP '2026-01-03 04:05:06', 8, 2, 1.5, 2.5, TRUE)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT id, name, note, at, seen, big, small, f, r, flag FROM spelled",
        expected: {
          columns: ["id", "name", "note", "at", "seen", "big", "small", "f", "r", "flag"],
          rows: [
            [
              1,
              "abc",
              "note",
              new Date("2026-01-02T03:04:05.000Z"),
              new Date("2026-01-03T04:05:06.000Z"),
              8,
              2,
              1.5,
              2.5,
              true,
            ],
          ],
        },
      },
    ],
  },
  {
    featureId: "ddl.temporary-table",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO scratch VALUES (1, 'temporary')",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT id, note FROM scratch",
        expected: { columns: ["id", "note"], rows: [[1, "temporary"]] },
      },
    ],
  },
  {
    featureId: "ddl.named-column-constraint",
    steps: [
      {
        kind: "error",
        sql: "INSERT INTO guarded VALUES (1, 0)",
        errorName: "TypeError",
        includes: "CHECK guarded_n_positive failed for row 0 of guarded",
      },
      {
        kind: "mutation",
        sql: "INSERT INTO guarded VALUES (1, 2)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT id, n FROM guarded",
        expected: { columns: ["id", "n"], rows: [[1, 2]] },
      },
    ],
  },
  {
    featureId: "ddl.create-table-if-not-exists",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO made VALUES (7)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT a FROM made",
        expected: { columns: ["a"], rows: [[7]] },
      },
    ],
  },
  {
    featureId: "ddl.create-table-key-clause",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO keyed_clause VALUES (1, 'first')",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "error",
        sql: "INSERT INTO keyed_clause VALUES (1, 'duplicate')",
        errorName: "UniqueConstraintError",
        includes: "Duplicate value for keyed_clause.a: 1",
      },
      {
        kind: "query",
        sql: "SELECT a, b FROM keyed_clause",
        expected: { columns: ["a", "b"], rows: [[1, "first"]] },
      },
    ],
  },
  {
    featureId: "ddl.alter-table-add-column",
    steps: [
      {
        kind: "query",
        sql: "SELECT region, amount, note FROM rows ORDER BY region",
        expected: {
          columns: ["region", "amount", "note"],
          rows: [
            ["east", 2, null],
            ["west", 1, null],
          ],
          ordered: true,
        },
      },
    ],
  },
  {
    featureId: "ddl.create-table-as-select",
    steps: [
      {
        kind: "query",
        sql: "SELECT region FROM copied ORDER BY region",
        expected: { columns: ["region"], rows: [["east"], ["west"]], ordered: true },
      },
    ],
  },
  {
    featureId: "ddl.enum",
    steps: [
      { kind: "execute", sql: "CREATE TABLE enum_values (id INTEGER PRIMARY KEY, feeling mood)" },
      {
        kind: "mutation",
        sql: "INSERT INTO enum_values VALUES (1, 'sad')",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "error",
        sql: "INSERT INTO enum_values VALUES (2, 'unknown')",
        errorName: "TypeError",
        includes: "unknown is not a value of enum mood",
      },
      {
        kind: "query",
        sql: "SELECT id, feeling FROM enum_values",
        expected: { columns: ["id", "feeling"], rows: [[1, "sad"]] },
      },
    ],
  },
  {
    featureId: "ddl.secondary-index",
    steps: [
      {
        kind: "error",
        sql: "CREATE INDEX keyed_score_idx ON keyed(score)",
        errorName: "TypeError",
        includes: "Index already exists: keyed_score_idx",
      },
      {
        kind: "query",
        sql: "SELECT name FROM keyed WHERE score = 1",
        expected: { columns: ["name"], rows: [["x"]] },
      },
    ],
  },
  {
    featureId: "ddl.drop-secondary-index",
    steps: [
      { kind: "execute", sql: "CREATE INDEX keyed_score_idx ON keyed(score)" },
      {
        kind: "error",
        sql: "CREATE INDEX keyed_score_idx ON keyed(score)",
        errorName: "TypeError",
        includes: "Index already exists: keyed_score_idx",
      },
    ],
  },
  {
    featureId: "ddl.composite-secondary-index",
    steps: [
      {
        kind: "error",
        sql: "CREATE INDEX keyed_score_bonus_idx ON keyed(score, bonus)",
        errorName: "TypeError",
        includes: "Index already exists: keyed_score_bonus_idx",
      },
      {
        kind: "query",
        sql: "SELECT name FROM keyed WHERE score = 1 AND bonus IS NULL",
        expected: { columns: ["name"], rows: [["x"]] },
      },
    ],
  },
  {
    featureId: "ddl.multiple-unique-constraints",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO multi_unique VALUES (1, 'one@example.test')",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "error",
        sql: "INSERT INTO multi_unique VALUES (2, 'one@example.test')",
        errorName: "UniqueConstraintError",
        includes: "Duplicate value for multi_unique.email: one@example.test",
      },
      {
        kind: "query",
        sql: "SELECT id, email FROM multi_unique",
        expected: { columns: ["id", "email"], rows: [[1, "one@example.test"]] },
      },
    ],
  },
  {
    featureId: "ddl.composite-primary-key",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO composite_key VALUES (1, 2)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "error",
        sql: "INSERT INTO composite_key VALUES (1, 2)",
        errorName: "UniqueConstraintError",
        includes: "Duplicate value for composite_key.(shop, receipt)",
      },
      {
        kind: "query",
        sql: "SELECT shop, receipt FROM composite_key",
        expected: { columns: ["shop", "receipt"], rows: [[1, 2]] },
      },
    ],
  },
  {
    featureId: "ddl.composite-foreign-key",
    steps: [
      {
        kind: "error",
        sql: "INSERT INTO composite_child VALUES (1, 2)",
        errorName: "TypeError",
        includes:
          "FOREIGN KEY composite_child_shop_receipt_fkey has no composite_key row with (shop, receipt) (1, 2)",
      },
      {
        kind: "mutation",
        sql: "INSERT INTO composite_key VALUES (1, 2)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "mutation",
        sql: "INSERT INTO composite_child VALUES (1, 2)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT shop, receipt FROM composite_child",
        expected: { columns: ["shop", "receipt"], rows: [[1, 2]] },
      },
    ],
  },
  {
    featureId: "ddl.alter-table-drop-column",
    steps: [
      {
        kind: "mutation",
        sql: "INSERT INTO keyed (name, score) VALUES ('z', 3)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT * FROM keyed ORDER BY name",
        expected: {
          columns: ["name", "score"],
          rows: [
            ["x", 1],
            ["y", -1],
            ["z", 3],
          ],
          ordered: true,
        },
      },
    ],
  },
  {
    featureId: "ddl.drop-table",
    steps: [
      { kind: "execute", sql: "CREATE TABLE doomed (a INTEGER)" },
      {
        kind: "mutation",
        sql: "INSERT INTO doomed VALUES (7)",
        affectedRows: 1,
        expected: emptyMutationResult,
      },
      {
        kind: "query",
        sql: "SELECT a FROM doomed",
        expected: { columns: ["a"], rows: [[7]] },
      },
    ],
  },
  {
    featureId: "ddl.drop-view",
    steps: [
      {
        kind: "execute",
        sql: "CREATE VIEW doomed_view AS SELECT region FROM rows WHERE region = 'east'",
      },
      {
        kind: "query",
        sql: "SELECT region FROM doomed_view",
        expected: { columns: ["region"], rows: [["east"]] },
      },
    ],
  },
];
