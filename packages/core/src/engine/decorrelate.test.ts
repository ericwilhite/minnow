import { describe, expect, it } from "vitest";
import type { DatabaseRow } from "./database.js";
import { compileQuery, executeQuery, executeRowQuery } from "./query.js";

const tables = new Map<string, DatabaseRow[]>([
  [
    "rows",
    [
      { region: "west", amount: 10 },
      { region: "west", amount: 6 },
      { region: "east", amount: 3 },
      { region: null, amount: 8 },
    ],
  ],
  [
    "dims",
    [
      { region: "west", label: "West Coast", rank: 1 },
      { region: "north", label: "North", rank: 3 },
    ],
  ],
  [
    "probes",
    [
      { g: "empty", v: null },
      { g: "clean", v: 1 },
      { g: "clean", v: 2 },
      { g: "clean", v: null },
      { g: "poisoned", v: 1 },
      { g: "poisoned", v: 3 },
    ],
  ],
  [
    "members",
    [
      { g: "clean", v: 2 },
      { g: "clean", v: 3 },
      { g: "poisoned", v: null },
      { g: "poisoned", v: 4 },
    ],
  ],
  [
    "scope_parents",
    [
      { id: 1, tenant: "a", all_access: true },
      { id: 2, tenant: "a", all_access: false },
      { id: 3, tenant: "a", all_access: false },
      { id: 4, tenant: "b", all_access: false },
      { id: 5, tenant: "b", all_access: null },
    ],
  ],
  [
    "scope_children",
    [
      { parent_id: 2, tenant: "a", scope: "read" },
      { parent_id: 2, tenant: "a", scope: "read" },
      { parent_id: 3, tenant: "a", scope: "write" },
      { parent_id: 4, tenant: "b", scope: "read" },
    ],
  ],
  [
    "aa",
    [
      { aaKey: 0, bbKey: 1 },
      { aaKey: 2, bbKey: 2 },
      { aaKey: 3, bbKey: 3 },
      { aaKey: 4, bbKey: null },
    ],
  ],
  [
    "bb",
    [
      { bbKey: 1, ok: true },
      { bbKey: 2, ok: false },
      { bbKey: 3, ok: true },
    ],
  ],
  [
    "cc",
    [
      { ccKey: 10, bbKey: 1 },
      { ccKey: 20, bbKey: 2 },
    ],
  ],
  [
    "owners",
    [
      { ownerKey: 1, name: "Alice" },
      { ownerKey: 2, name: "Bob" },
      { ownerKey: 3, name: "Cara" },
    ],
  ],
  [
    "pets",
    [
      { petKey: 10, ownerKey: 1, petName: "Rex" },
      { petKey: 11, ownerKey: 1, petName: "Milo" },
      { petKey: 12, ownerKey: 2, petName: "Zed" },
    ],
  ],
  ["locations", [{ locationKey: 10, regionKey: 100 }]],
  ["yards", [{ yardKey: 1, locationKey: 10 }]],
]);

function run(sql: string): DatabaseRow[] {
  const plan = compileQuery(sql);
  const vectorized = executeQuery(plan, tables);
  const byRow = executeRowQuery(plan, tables);
  expect(vectorized.rows).toEqual(byRow.rows);
  return vectorized.rows;
}

describe("correlated subquery decorrelation", () => {
  it("answers a correlated scalar aggregate comparison", () => {
    expect(
      run(
        "SELECT r.region, r.amount FROM rows r WHERE r.amount > (SELECT AVG(q.amount) FROM rows q WHERE q.region = r.region)",
      ),
    ).toEqual([{ region: "west", amount: 10 }]);
  });

  it("treats a NULL correlation key as matching nothing", () => {
    // The NULL-region row joins no group: AVG is NULL, the comparison is UNKNOWN, the row drops.
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE r.amount >= (SELECT MIN(q.amount) FROM rows q WHERE q.region = r.region)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }, { amount: 3 }]);
  });

  it("answers correlated EXISTS and NOT EXISTS", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE EXISTS (SELECT d.region FROM dims d WHERE d.region = r.region)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }]);
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE NOT EXISTS (SELECT d.region FROM dims d WHERE d.region = r.region)",
      ),
    ).toEqual([{ amount: 3 }, { amount: 8 }]);
  });

  it("does not multiply outer rows when the subquery matches many inner rows", () => {
    // Both west rows match the two west entries of the self-join; each outer row appears once.
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE EXISTS (SELECT q.amount FROM rows q WHERE q.region = r.region)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }, { amount: 3 }]);
  });

  it("answers correlated COUNT with zero for unmatched groups", () => {
    expect(
      run(
        "SELECT r.region, r.amount FROM rows r WHERE (SELECT COUNT(*) FROM dims d WHERE d.region = r.region) = 0",
      ),
    ).toEqual([
      { region: "east", amount: 3 },
      { region: null, amount: 8 },
    ]);
  });

  it("answers correlated IN", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE r.region IN (SELECT d.region FROM dims d WHERE d.region = r.region)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }]);
  });

  it("answers correlated IN over non-equality predicates", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE r.region IN " +
          "(SELECT q.region FROM rows q WHERE q.amount < r.amount)",
      ),
    ).toEqual([{ amount: 10 }]);
  });

  it("decorrelates equality LATERAL sources into a hash join", () => {
    const plan = compileQuery(
      "SELECT r.amount, x.label FROM rows r, " +
        "LATERAL (SELECT d.label FROM dims d WHERE d.region = r.region) x",
    );
    expect(plan.joins.at(-1)?.kind).toBe("inner");
    expect(plan.joins.at(-1)?.on).toBeUndefined();
    expect(executeQuery(plan, tables).rows).toEqual([
      { amount: 10, label: "West Coast" },
      { amount: 6, label: "West Coast" },
    ]);
    expect(executeRowQuery(plan, tables).rows).toEqual([
      { amount: 10, label: "West Coast" },
      { amount: 6, label: "West Coast" },
    ]);
  });

  it("decorrelates range-correlated LATERAL sources into a general join", () => {
    expect(
      run(
        "SELECT r.amount AS outer_amount, x.amount AS inner_amount FROM rows r, " +
          "LATERAL (SELECT q.amount FROM rows q WHERE q.amount < r.amount) x " +
          "ORDER BY outer_amount, inner_amount",
      ),
    ).toEqual([
      { outer_amount: 6, inner_amount: 3 },
      { outer_amount: 8, inner_amount: 3 },
      { outer_amount: 8, inner_amount: 6 },
      { outer_amount: 10, inner_amount: 3 },
      { outer_amount: 10, inner_amount: 6 },
      { outer_amount: 10, inner_amount: 8 },
    ]);
  });

  it("preserves LEFT LATERAL rows and hides decorrelation keys from wildcards", () => {
    expect(
      run(
        "SELECT r.amount, x.* FROM rows r LEFT JOIN LATERAL " +
          "(SELECT d.label FROM dims d WHERE d.region = r.region) x ON TRUE ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 3, label: null },
      { amount: 6, label: "West Coast" },
      { amount: 8, label: null },
      { amount: 10, label: "West Coast" },
    ]);
  });

  it("groups, aggregates, and limits inside an equality-correlated LATERAL query", () => {
    // Global aggregates answer once per outer row, including the rows with no match: COUNT is
    // 0 and the rest NULL, for inner and left joins alike, exactly as PostgreSQL returns.
    for (const sql of [
      "SELECT r.amount, x.n, x.best FROM rows r JOIN LATERAL (SELECT COUNT(*) AS n, MAX(d.rank) AS best FROM dims d WHERE d.region = r.region) x ON TRUE ORDER BY r.amount",
      "SELECT r.amount, x.n, x.best FROM rows r, LATERAL (SELECT COUNT(*) AS n, MAX(d.rank) AS best FROM dims d WHERE d.region = r.region) x ORDER BY r.amount",
      "SELECT r.amount, x.n, x.best FROM rows r LEFT JOIN LATERAL (SELECT COUNT(*) AS n, MAX(d.rank) AS best FROM dims d WHERE d.region = r.region) x ON TRUE ORDER BY r.amount",
    ]) {
      expect(run(sql), sql).toEqual([
        { amount: 3, n: 0, best: null },
        { amount: 6, n: 1, best: 1 },
        { amount: 8, n: 0, best: null },
        { amount: 10, n: 1, best: 1 },
      ]);
    }
    // The aggregate column may sit inside an outer expression and a WHERE clause.
    expect(
      run(
        "SELECT r.amount, x.n + 100 AS shifted FROM rows r JOIN LATERAL (SELECT COUNT(*) AS n FROM dims d WHERE d.region = r.region) x ON TRUE WHERE x.n = 0 ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 3, shifted: 100 },
      { amount: 8, shifted: 100 },
    ]);
    // GROUP BY and HAVING inside the lateral query group per outer row.
    expect(
      run(
        "SELECT r.region, x.label, x.n FROM rows r JOIN LATERAL (SELECT d.label, COUNT(*) AS n FROM dims d WHERE d.region = r.region GROUP BY d.label HAVING COUNT(*) >= 1) x ON TRUE ORDER BY r.amount",
      ),
    ).toEqual([
      { region: "west", label: "West Coast", n: 1 },
      { region: "west", label: "West Coast", n: 1 },
    ]);
    // ORDER BY ... LIMIT ranks within each outer row; a left join keeps rows with no match,
    // OFFSET skips the first ranked row, and an unselected order term is still honored.
    expect(
      run(
        "SELECT r.amount, x.v FROM rows r JOIN LATERAL (SELECT q.amount AS v FROM rows q WHERE q.region = r.region ORDER BY q.amount DESC LIMIT 1) x ON TRUE ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 3, v: 3 },
      { amount: 6, v: 10 },
      { amount: 10, v: 10 },
    ]);
    expect(
      run(
        "SELECT r.amount, x.v FROM rows r LEFT JOIN LATERAL (SELECT q.amount AS v FROM rows q WHERE q.region = r.region ORDER BY q.amount LIMIT 1 OFFSET 1) x ON TRUE ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 3, v: null },
      { amount: 6, v: 10 },
      { amount: 8, v: null },
      { amount: 10, v: 10 },
    ]);
    expect(
      run(
        "SELECT r.amount, x.label FROM rows r LEFT JOIN LATERAL (SELECT d.label FROM dims d WHERE d.region = r.region ORDER BY d.rank DESC LIMIT 1) x ON TRUE ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 3, label: null },
      { amount: 6, label: "West Coast" },
      { amount: 8, label: null },
      { amount: 10, label: "West Coast" },
    ]);
    // A range correlation cannot be grouped or partitioned on.
    expect(() =>
      compileQuery(
        "SELECT r.amount, x.n FROM rows r JOIN LATERAL (SELECT COUNT(*) AS n FROM rows q WHERE q.amount < r.amount) x ON TRUE",
      ),
    ).toThrow("need equality correlations");
    expect(() =>
      compileQuery(
        "SELECT r.amount, x.v FROM rows r JOIN LATERAL (SELECT q.amount AS v FROM rows q WHERE q.amount < r.amount ORDER BY q.amount LIMIT 1) x ON TRUE",
      ),
    ).toThrow("need equality correlations");
  });

  it("supports multi-key correlation", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE EXISTS (SELECT q.amount FROM rows q WHERE q.region = r.region AND q.amount = r.amount)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }, { amount: 3 }]);
  });

  it("answers non-equality correlated EXISTS without multiplying outer rows", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE EXISTS (SELECT q.amount FROM rows q WHERE q.amount < r.amount)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }, { amount: 8 }]);
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE NOT EXISTS (SELECT q.amount FROM rows q WHERE r.amount > q.amount)",
      ),
    ).toEqual([{ amount: 3 }]);
  });

  it("combines equality and range correlation in a semi-join", () => {
    const plan = compileQuery(
      "SELECT r.amount FROM rows r WHERE EXISTS " +
        "(SELECT q.amount FROM rows q WHERE q.region = r.region AND q.amount < r.amount)",
    );
    expect(plan.joins.at(-1)?.kind).toBe("semi");
    expect(executeQuery(plan, tables).rows).toEqual([{ amount: 10 }]);
    expect(executeRowQuery(plan, tables).rows).toEqual([{ amount: 10 }]);
  });

  it("uses an anti-join for non-equality NOT EXISTS", () => {
    const plan = compileQuery(
      "SELECT r.amount FROM rows r WHERE NOT EXISTS " +
        "(SELECT q.amount FROM rows q WHERE q.amount < r.amount)",
    );
    expect(plan.joins.at(-1)?.kind).toBe("anti");
    expect(executeQuery(plan, tables).rows).toEqual([{ amount: 3 }]);
    expect(executeRowQuery(plan, tables).rows).toEqual([{ amount: 3 }]);
  });

  it("answers correlated NOT IN with empty-set and NULL semantics", () => {
    const plan = compileQuery(
      "SELECT p.g, p.v FROM probes p WHERE p.v NOT IN " +
        "(SELECT m.v FROM members m WHERE m.g = p.g)",
    );
    expect(plan.joins.slice(-2).map(({ kind, on }) => [kind, on])).toEqual([
      ["anti", undefined],
      ["left", undefined],
    ]);
    expect(executeQuery(plan, tables).rows).toEqual([
      // NULL NOT IN an empty correlated set is true.
      { g: "empty", v: null },
      // 1 has no equal member and the set contains no NULL.
      { g: "clean", v: 1 },
    ]);
    expect(executeRowQuery(plan, tables).rows).toEqual([
      { g: "empty", v: null },
      { g: "clean", v: 1 },
    ]);
  });

  it("answers range-correlated NOT IN with empty-set and NULL semantics", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE 'west' NOT IN " +
          "(SELECT q.region FROM rows q WHERE q.amount < r.amount)",
      ),
    ).toEqual([{ amount: 6 }, { amount: 3 }]);
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE 'north' NOT IN " +
          "(SELECT q.region FROM rows q WHERE q.amount < r.amount)",
      ),
    ).toEqual([{ amount: 6 }, { amount: 3 }, { amount: 8 }]);
  });

  it("answers correlated scalar aggregates in the select list", () => {
    expect(
      run(
        "SELECT r.amount, (SELECT AVG(q.amount) FROM rows q WHERE q.region = r.region) AS a FROM rows r WHERE r.region = 'west'",
      ),
    ).toEqual([
      { amount: 10, a: 8 },
      { amount: 6, a: 8 },
    ]);
  });

  it("carries additional outer columns into a correlated scalar projection", () => {
    expect(
      run(
        "SELECT l.locationKey, " +
          "(SELECT MAX(y.yardKey + l.regionKey) FROM yards y " +
          "WHERE y.locationKey = l.locationKey) AS v " +
          "FROM locations l",
      ),
    ).toEqual([{ locationKey: 10, v: 101 }]);
  });

  it("supports outer values throughout the general correlated scalar body", () => {
    expect(
      run(
        "SELECT r.amount, " +
          "(SELECT MAX(q.amount + r.amount) FROM rows q WHERE q.region = r.region) AS v " +
          "FROM rows r WHERE r.region = 'west' ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 6, v: 16 },
      { amount: 10, v: 20 },
    ]);
    expect(
      run(
        "SELECT r.amount, " +
          "(SELECT MAX(q.amount + r.amount + d.rank) FROM rows q " +
          "WHERE q.region = r.region) AS v " +
          "FROM rows r JOIN dims d ON d.region = r.region " +
          "WHERE r.region = 'west' ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 6, v: 17 },
      { amount: 10, v: 21 },
    ]);
    expect(
      run(
        "SELECT r.amount, " +
          "(SELECT MAX(q.amount) FROM rows q " +
          "JOIN dims d ON d.region = q.region AND d.rank < r.amount " +
          "WHERE q.region = r.region) AS v " +
          "FROM rows r WHERE r.region = 'west' ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 6, v: 10 },
      { amount: 10, v: 10 },
    ]);
    expect(
      run(
        "SELECT r.amount, " +
          "(SELECT MAX(q.amount + r.amount) FROM dims d " +
          "JOIN rows q ON q.region = d.region WHERE q.region = r.region) AS v " +
          "FROM rows r WHERE r.region = 'west' ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 6, v: 16 },
      { amount: 10, v: 20 },
    ]);
    expect(
      run(
        "SELECT r.amount, " +
          "(SELECT MAX(q.amount) + r.amount FROM rows q WHERE q.region = 'east') AS v " +
          "FROM rows r WHERE r.region = 'west' ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 6, v: 9 },
      { amount: 10, v: 13 },
    ]);
    expect(
      run(
        "SELECT r.amount, " +
          "(SELECT COUNT(*) FROM rows q WHERE q.amount < r.amount + 1) AS c " +
          "FROM rows r WHERE r.region = 'west' ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 6, c: 2 },
      { amount: 10, c: 4 },
    ]);
    expect(
      run(
        "SELECT r.amount, " +
          "(SELECT COALESCE(MAX(q.amount), r.amount) FROM rows q " +
          "WHERE q.region = 'missing') AS fallback " +
          "FROM rows r WHERE r.region = 'west' ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 6, fallback: 6 },
      { amount: 10, fallback: 10 },
    ]);
    expect(
      run(
        "SELECT r.amount, " +
          "(SELECT COALESCE(MAX(q.amount + r.amount), r.amount) FROM rows q " +
          "WHERE q.region = r.region) AS fallback " +
          "FROM rows r WHERE r.region IS NULL",
      ),
    ).toEqual([{ amount: 8, fallback: 8 }]);
    expect(
      run(
        "SELECT r.amount, " +
          "(SELECT q.amount + r.amount FROM rows q WHERE q.region = 'east') AS v " +
          "FROM rows r WHERE r.region = 'west' ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 6, v: 9 },
      { amount: 10, v: 13 },
    ]);
  });

  it("answers grouped correlated scalars determined by grouping keys", () => {
    expect(
      run(
        "SELECT r.region AS g, COUNT(*) AS c, " +
          "(SELECT AVG(q.amount) FROM rows q WHERE q.region = r.region) AS regional " +
          "FROM rows r GROUP BY r.region ORDER BY g NULLS LAST",
      ),
    ).toEqual([
      { g: "east", c: 1, regional: 3 },
      { g: "west", c: 2, regional: 8 },
      { g: null, c: 1, regional: null },
    ]);
  });

  it("builds sibling correlated JSON array and object projections", () => {
    expect(
      run(
        "SELECT o.ownerKey, " +
          "COALESCE((SELECT JSON_ARRAYAGG(JSON_OBJECT('pet_id' VALUE a.pet_id, 'profile' VALUE a.profile)) " +
          "FROM (SELECT p.petKey AS pet_id, JSON_OBJECT('name' VALUE p.petName) AS profile " +
          "FROM pets p WHERE p.ownerKey = o.ownerKey ORDER BY p.petName LIMIT 2) AS a), JSON_ARRAY()) AS pets, " +
          "(SELECT JSON_OBJECT('pet_id' VALUE x.pet_id, 'name' VALUE x.name) " +
          "FROM (SELECT p.petKey AS pet_id, p.petName AS name FROM pets p " +
          "WHERE p.ownerKey = o.ownerKey ORDER BY p.petName LIMIT 1) AS x) AS first_pet " +
          "FROM owners o ORDER BY o.ownerKey",
      ),
    ).toEqual([
      {
        ownerKey: 1,
        pets: '[{"pet_id":11,"profile":{"name":"Milo"}},{"pet_id":10,"profile":{"name":"Rex"}}]',
        first_pet: '{"pet_id":11,"name":"Milo"}',
      },
      {
        ownerKey: 2,
        pets: '[{"pet_id":12,"profile":{"name":"Zed"}}]',
        first_pet: '{"pet_id":12,"name":"Zed"}',
      },
      { ownerKey: 3, pets: "[]", first_pet: null },
    ]);
  });

  it("enforces correlated scalar row cardinality after per-probe ordering and limits", () => {
    expect(
      run(
        "SELECT o.ownerKey, (SELECT p.petName FROM pets p WHERE p.ownerKey = o.ownerKey " +
          "ORDER BY p.petName LIMIT 1) AS first_pet FROM owners o ORDER BY o.ownerKey",
      ),
    ).toEqual([
      { ownerKey: 1, first_pet: "Milo" },
      { ownerKey: 2, first_pet: "Zed" },
      { ownerKey: 3, first_pet: null },
    ]);

    const plan = compileQuery(
      "SELECT o.ownerKey, (SELECT p.petName FROM pets p WHERE p.ownerKey = o.ownerKey) AS pet " +
        "FROM owners o",
    );
    expect(() => executeQuery(plan, tables)).toThrow("A scalar subquery returned 2 rows");
    expect(() => executeRowQuery(plan, tables)).toThrow("A scalar subquery returned 2 rows");
  });

  it("answers scalar aggregates over non-equality correlations", () => {
    expect(
      run(
        "SELECT r.amount, (SELECT COUNT(*) FROM rows q WHERE q.amount < r.amount) AS c " +
          "FROM rows r ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 3, c: 0 },
      { amount: 6, c: 1 },
      { amount: 8, c: 2 },
      { amount: 10, c: 3 },
    ]);
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE r.amount > " +
          "(SELECT AVG(q.amount) FROM rows q WHERE q.amount < r.amount)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }, { amount: 8 }]);
  });

  it("combines equality and range correlation for scalar aggregates", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE r.amount > " +
          "(SELECT AVG(q.amount) FROM rows q " +
          "WHERE q.region = r.region AND q.amount < r.amount)",
      ),
    ).toEqual([{ amount: 10 }]);
  });

  it("builds probe tuples from multiple outer sources", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r JOIN dims d ON d.region = r.region WHERE r.amount > " +
          "(SELECT COUNT(*) FROM rows q " +
          "WHERE q.amount < r.amount AND q.region <> d.region)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }]);
  });

  it("orders by a correlated scalar via the hidden select-item desugar", () => {
    // ORDER BY expressions hoist into hidden select items before decorrelation, so a
    // correlated ordering key rides the same rewrite as a visible select item.
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE r.region IS NOT NULL ORDER BY (SELECT AVG(q.amount) FROM rows q WHERE q.region = r.region), r.amount",
      ),
    ).toEqual([{ amount: 3 }, { amount: 6 }, { amount: 10 }]);
  });

  it("rejects correlated scalar subqueries outside supported positions", () => {
    expect(() =>
      compileQuery(
        "SELECT r.region AS g, COUNT(*) AS c FROM rows r GROUP BY r.region HAVING COUNT(*) > (SELECT AVG(q.amount) FROM rows q WHERE q.region = r.region)",
      ),
    ).toThrow("top-level WHERE");
  });

  it("answers correlated EXISTS nested inside boolean expressions", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE r.amount > 100 OR " +
          "EXISTS (SELECT d.region FROM dims d WHERE d.region = r.region)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }]);
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE r.amount = 3 OR " +
          "NOT EXISTS (SELECT d.region FROM dims d WHERE d.region = r.region)",
      ),
    ).toEqual([{ amount: 3 }, { amount: 8 }]);
  });

  it("keeps generated aliases unique across sibling and nested correlated EXISTS", () => {
    const sql =
      'SELECT "aaKey" FROM "aa" WHERE "aa"."aaKey" = 0 ' +
      'OR EXISTS (SELECT 1 FROM "bb" WHERE "bb"."bbKey" = "aa"."bbKey" AND "bb"."ok" = true) ' +
      'OR EXISTS (SELECT 1 FROM "cc" WHERE "cc"."bbKey" = "aa"."bbKey" ' +
      'AND EXISTS (SELECT 1 FROM "bb" WHERE "bb"."bbKey" = "cc"."bbKey" AND "bb"."ok" = true)) ' +
      'ORDER BY "aaKey"';
    const plan = compileQuery(sql);
    const aliases: string[] = [];
    const collect = (block: typeof plan): void => {
      for (const source of [block.base, ...block.joins]) {
        if (source.alias.startsWith("corr_")) aliases.push(source.alias);
        if (source.derived !== undefined) collect(source.derived);
      }
    };
    collect(plan);
    expect(new Set(aliases).size).toBe(aliases.length);
    expect(run(sql)).toEqual([{ aaKey: 0 }, { aaKey: 3 }]);
  });

  it("decorrelates through deeper scopes without colliding with user corr aliases", () => {
    expect(
      run(
        'SELECT "corr_1"."aaKey" FROM "aa" AS "corr_1" WHERE EXISTS (' +
          'SELECT 1 FROM "cc" AS "corr_2" WHERE "corr_2"."bbKey" = "corr_1"."bbKey" ' +
          'AND EXISTS (SELECT 1 FROM "bb" AS "corr_3" ' +
          'WHERE "corr_3"."bbKey" = "corr_2"."bbKey" ' +
          'AND EXISTS (SELECT 1 FROM "bb" AS "corr_4" ' +
          'WHERE "corr_4"."bbKey" = "corr_3"."bbKey" AND "corr_4"."ok" = true))) ' +
          'ORDER BY "corr_1"."aaKey"',
      ),
    ).toEqual([{ aaKey: 0 }]);
  });

  it("recurses through correlated EXISTS well beyond the former one-level shape", () => {
    const depth = 12;
    const nested = (level: number, parent: string): string => {
      const alias = `b${String(level)}`;
      const tail =
        level === depth ? ` AND ${alias}.ok = true` : ` AND EXISTS (${nested(level + 1, alias)})`;
      return `SELECT 1 FROM bb ${alias} WHERE ${alias}.bbKey = ${parent}.bbKey${tail}`;
    };
    expect(
      run(`SELECT a.aaKey FROM aa a WHERE EXISTS (${nested(1, "a")}) ORDER BY a.aaKey`),
    ).toEqual([{ aaKey: 0 }, { aaKey: 3 }]);
  });

  it("carries outer correlation through nested scalar subqueries", () => {
    expect(
      run(
        'SELECT "aa"."aaKey" FROM "aa" WHERE EXISTS (' +
          'SELECT 1 FROM "cc" WHERE "cc"."bbKey" = (SELECT "aa"."bbKey")) ' +
          'ORDER BY "aa"."aaKey"',
      ),
    ).toEqual([{ aaKey: 0 }, { aaKey: 2 }]);
    expect(
      run(
        'SELECT "aa"."aaKey" FROM "aa" WHERE EXISTS (' +
          'SELECT 1 FROM "cc" WHERE "cc"."bbKey" = (' +
          'SELECT MAX("bb"."bbKey") FROM "bb" ' +
          'WHERE "bb"."bbKey" = "aa"."bbKey" AND "bb"."ok" = true)) ' +
          'ORDER BY "aa"."aaKey"',
      ),
    ).toEqual([{ aaKey: 0 }]);
  });

  it("keeps NULL outer probes when correlation is nested in an expression", () => {
    expect(
      run(
        'SELECT "aa"."aaKey" FROM "aa" WHERE EXISTS (' +
          'SELECT 1 FROM "cc" WHERE "aa"."bbKey" IS NULL) ' +
          'ORDER BY "aa"."aaKey"',
      ),
    ).toEqual([{ aaKey: 4 }]);
  });

  it("decorrelates nested NOT IN below a sibling boolean branch", () => {
    expect(
      run(
        'SELECT "aa"."aaKey" FROM "aa" WHERE "aa"."aaKey" = 3 OR EXISTS (' +
          'SELECT 1 FROM "cc" WHERE "cc"."bbKey" = "aa"."bbKey" ' +
          'AND "cc"."bbKey" NOT IN (' +
          'SELECT "bb"."bbKey" FROM "bb" ' +
          'WHERE "bb"."bbKey" = "cc"."bbKey" AND "bb"."ok" = false)) ' +
          'ORDER BY "aa"."aaKey"',
      ),
    ).toEqual([{ aaKey: 0 }, { aaKey: 3 }]);
  });

  it("preserves correlated IN and NOT IN below OR with empty-set and NULL semantics", () => {
    expect(
      run(
        "SELECT p.g, p.v FROM probes p WHERE p.g = 'empty' OR p.v IN " +
          "(SELECT m.v FROM members m WHERE m.g = p.g) ORDER BY p.g, p.v NULLS LAST",
      ),
    ).toEqual([
      { g: "clean", v: 2 },
      { g: "empty", v: null },
    ]);
    expect(
      run(
        "SELECT p.g, p.v FROM probes p WHERE p.g = 'empty' OR p.v NOT IN " +
          "(SELECT m.v FROM members m WHERE m.g = p.g) ORDER BY p.g, p.v NULLS LAST",
      ),
    ).toEqual([
      { g: "clean", v: 1 },
      { g: "empty", v: null },
    ]);
  });

  it("returns three-valued correlated ANY and ALL results from nested expression positions", () => {
    expect(
      run(
        "SELECT p.g, p.v, " +
          "p.v < ANY (SELECT m.v FROM members m WHERE m.g = p.g) AS any_match, " +
          "p.v < ALL (SELECT m.v FROM members m WHERE m.g = p.g) AS all_match " +
          "FROM probes p ORDER BY p.g, p.v NULLS LAST",
      ),
    ).toEqual([
      { g: "clean", v: 1, any_match: true, all_match: true },
      { g: "clean", v: 2, any_match: true, all_match: false },
      { g: "clean", v: null, any_match: null, all_match: null },
      { g: "empty", v: null, any_match: false, all_match: true },
      { g: "poisoned", v: 1, any_match: true, all_match: null },
      { g: "poisoned", v: 3, any_match: true, all_match: null },
    ]);
    expect(
      run(
        "SELECT r.amount, 'z' > ALL " +
          "(SELECT q.region FROM rows q WHERE q.amount < r.amount) AS passes " +
          "FROM rows r ORDER BY r.amount",
      ),
    ).toEqual([
      { amount: 3, passes: true },
      { amount: 6, passes: true },
      { amount: 8, passes: true },
      { amount: 10, passes: null },
    ]);
  });

  it("decorrelates scalar aggregates and EXISTS in select and OR expressions", () => {
    expect(
      run(
        'SELECT "aa"."aaKey" FROM "aa" WHERE "aa"."aaKey" = 3 OR "aa"."bbKey" = (' +
          'SELECT MAX("bb"."bbKey") FROM "bb" ' +
          'WHERE "bb"."bbKey" = "aa"."bbKey" AND "bb"."ok" = true) ' +
          'ORDER BY "aa"."aaKey"',
      ),
    ).toEqual([{ aaKey: 0 }, { aaKey: 3 }]);
    expect(
      run(
        'SELECT "aa"."aaKey", EXISTS (' +
          'SELECT 1 FROM "bb" WHERE "bb"."bbKey" = "aa"."bbKey" AND "bb"."ok" = true' +
          ') AS "allowed" FROM "aa" ORDER BY "aa"."aaKey"',
      ),
    ).toEqual([
      { aaKey: 0, allowed: true },
      { aaKey: 2, allowed: false },
      { aaKey: 3, allowed: true },
      { aaKey: 4, allowed: false },
    ]);
  });

  it("optimizes correlations inside an otherwise uncorrelated subquery", () => {
    expect(
      run(
        'SELECT "aa"."aaKey" FROM "aa" WHERE "aa"."bbKey" IN (' +
          'SELECT "cc"."bbKey" FROM "cc" WHERE EXISTS (' +
          'SELECT 1 FROM "bb" WHERE "bb"."bbKey" = "cc"."bbKey" AND "bb"."ok" = true)) ' +
          'ORDER BY "aa"."aaKey"',
      ),
    ).toEqual([{ aaKey: 0 }]);
  });

  it("supports the multi-key all-access OR EXISTS auth-scope shape", () => {
    const plan = compileQuery(
      "SELECT p.id FROM scope_parents p WHERE p.all_access = true OR EXISTS (" +
        "SELECT c.parent_id FROM scope_children c " +
        "WHERE c.parent_id = p.id AND c.tenant = p.tenant AND c.scope = 'read') ORDER BY p.id",
    );
    expect(plan.joins.at(-1)?.kind).toBe("left");
    expect(executeQuery(plan, tables).rows).toEqual([{ id: 1 }, { id: 2 }, { id: 4 }]);
    expect(executeRowQuery(plan, tables).rows).toEqual([{ id: 1 }, { id: 2 }, { id: 4 }]);
  });

  it("preserves correlated EXISTS below NOT and CASE", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE NOT (EXISTS " +
          "(SELECT d.region FROM dims d WHERE d.region = r.region))",
      ),
    ).toEqual([{ amount: 3 }, { amount: 8 }]);
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE CASE WHEN EXISTS " +
          "(SELECT d.region FROM dims d WHERE d.region = r.region) THEN true ELSE r.amount = 3 END",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }, { amount: 3 }]);
  });

  it("answers non-equality correlated EXISTS nested below OR", () => {
    expect(
      run(
        "SELECT r.amount FROM rows r WHERE r.amount = 3 OR " +
          "EXISTS (SELECT q.amount FROM rows q WHERE q.amount < r.amount)",
      ),
    ).toEqual([{ amount: 10 }, { amount: 6 }, { amount: 3 }, { amount: 8 }]);
  });

  it("hides decorrelation columns from SELECT *", () => {
    expect(
      run(
        "SELECT * FROM rows r WHERE EXISTS (SELECT d.region FROM dims d WHERE d.region = r.region)",
      ),
    ).toEqual([
      { region: "west", amount: 10 },
      { region: "west", amount: 6 },
    ]);
    expect(
      run(
        "SELECT DISTINCT * FROM rows r WHERE " +
          "EXISTS (SELECT d.region FROM dims d WHERE d.region = r.region)",
      ),
    ).toEqual([
      { region: "west", amount: 10 },
      { region: "west", amount: 6 },
    ]);
  });

  it("accepts a single-row correlated scalar subquery without an aggregate", () => {
    expect(
      run(
        "SELECT r.region FROM rows r WHERE r.amount = " +
          "(SELECT q.amount FROM rows q WHERE q.region = r.region ORDER BY q.amount DESC LIMIT 1)",
      ),
    ).toEqual([{ region: "west" }, { region: "east" }]);
  });

  it("leaves uncorrelated subqueries on the existing resolution path", () => {
    expect(
      run("SELECT r.amount FROM rows r WHERE r.amount > (SELECT AVG(q.amount) FROM rows q)"),
    ).toEqual([{ amount: 10 }, { amount: 8 }]);
  });
});
