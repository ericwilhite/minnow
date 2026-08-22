import { describe, expect, it } from "vitest";
import {
  type CompiledQuery,
  type Expression,
  type JoinPlan,
  type TableSource,
} from "@minnowdb/core/plan";
import { renderMutationSql, renderPlanSql } from "./plan-sql.js";

function plan(overrides: Partial<CompiledQuery> = {}): CompiledQuery {
  return {
    sql: "test",
    base: { table: "items", alias: "i" },
    joins: [],
    select: [{ expression: { kind: "column", reference: "i.id" }, alias: "id" }],
    predicates: [],
    groupBy: [],
    having: [],
    orderBy: [],
    ...overrides,
  };
}

describe("plan to parameterized SQL", () => {
  it("renders every client expression family and keeps literals in parameters", () => {
    const subquery = plan({
      select: [{ expression: { kind: "literal", value: 1 }, alias: "one" }],
      limit: 1,
    });
    const expressions: Expression[] = [
      { kind: "wildcard", table: "i" },
      {
        kind: "not",
        operand: {
          kind: "condition",
          operator: "LIKE",
          left: { kind: "column", reference: "i.name" },
          right: { kind: "literal", value: "a%" },
          escape: "!",
        },
      },
      { kind: "exists", block: subquery, negated: false },
      { kind: "exists", block: subquery, negated: true },
      {
        kind: "case",
        branches: [
          {
            when: { kind: "literal", value: true },
            then: { kind: "literal", value: "yes" },
          },
        ],
        otherwise: { kind: "literal", value: "no" },
      },
      { kind: "fts", op: "match", columns: "*", query: "a'b" },
      {
        kind: "fts",
        op: "bm25",
        columns: [{ kind: "column", reference: "i.name" }],
        query: "term",
      },
      {
        kind: "window",
        name: "LAG",
        argument: { kind: "column", reference: "i.score" },
        offset: 2,
        fallback: -1,
        partitionBy: [{ kind: "column", reference: "i.group" }],
        orderBy: [
          {
            expression: { kind: "column", reference: "i.score" },
            direction: "desc",
            nulls: "last",
          },
        ],
        frame: {
          unit: "rows",
          start: { kind: "preceding", offset: 2 },
          end: { kind: "following", offset: 1 },
          exclude: "current-row",
        },
      },
      {
        kind: "window",
        name: "ROW_NUMBER",
        partitionBy: [],
        orderBy: [],
        frame: {
          unit: "range",
          start: { kind: "unbounded-preceding" },
          end: { kind: "current-row" },
        },
      },
      {
        kind: "window",
        name: "COUNT",
        partitionBy: [],
        orderBy: [],
        frame: {
          unit: "groups",
          start: { kind: "current-row" },
          end: { kind: "unbounded-following" },
          exclude: "no-others",
        },
      },
    ];
    const rendered = renderPlanSql(
      plan({
        select: expressions.map((expression, index) => ({
          expression,
          alias: `value ${String(index)}`,
        })),
      }),
    );
    expect(rendered.sql).toContain('"i".* AS "value 0"');
    expect(rendered.sql).toContain("NOT EXISTS");
    expect(rendered.sql).toContain("ELSE");
    expect(rendered.sql).toContain("ESCAPE '!'");
    expect(rendered.sql).toMatch(/MATCH\(\*\) AGAINST \$\d+/);
    expect(rendered.sql).toMatch(/BM25\("i"\."name"\) AGAINST \$\d+/);
    expect(rendered.sql).toContain("NULLS LAST");
    expect(rendered.sql).toContain("EXCLUDE CURRENT ROW");
    expect(rendered.sql).toContain("UNBOUNDED PRECEDING");
    expect(rendered.sql).toContain("UNBOUNDED FOLLOWING");
    expect(rendered.params).toContain(-1);
    expect(rendered.params).toContain("a'b");
    expect(rendered.params).toContain("term");
  });

  it("renders joins, aliases, grouping, ordering, and row limits", () => {
    const rendered = renderPlanSql(
      plan({
        base: {
          table: "items",
          alias: "i",
          columnAliases: ["identifier", "label"],
        },
        joins: [
          {
            table: "groups",
            alias: "g",
            kind: "left",
            left: { kind: "literal", value: null },
            right: { kind: "literal", value: null },
            on: {
              kind: "condition",
              operator: "=",
              left: { kind: "column", reference: "g.id" },
              right: { kind: "column", reference: "i.group_id" },
            },
          },
        ],
        groupBy: [{ kind: "column", reference: "i.id" }],
        having: [
          {
            left: { kind: "call", name: "COUNT", arguments: [{ kind: "wildcard" }] },
            operator: ">",
            right: { kind: "literal", value: 1 },
          },
        ],
        orderBy: [
          {
            expression: { kind: "column", reference: "i.id" },
            direction: "asc",
            nulls: "first",
          },
        ],
        limit: 5,
        offset: 2,
        limitWithTies: true,
      }),
    );
    expect(rendered.sql).toContain('AS "i"("identifier", "label")');
    expect(rendered.sql).toContain("LEFT JOIN");
    expect(rendered.sql).toContain("HAVING");
    expect(rendered.sql).toContain("NULLS FIRST");
    expect(rendered.sql).toContain("OFFSET $2 ROWS FETCH FIRST $3 ROWS WITH TIES");
    expect(rendered.params).toEqual([1, 2, 5]);
  });

  it("refuses to render internal existence joins as ordinary joins", () => {
    const semiJoin: JoinPlan = {
      table: "matches",
      alias: "m",
      kind: "semi",
      left: { kind: "column", reference: "i.id" },
      right: { kind: "column", reference: "m.id" },
    };
    expect(() => renderPlanSql(plan({ joins: [semiJoin] }))).toThrow(
      "Cannot render the optimizer's internal semi-join as equivalent SQL",
    );
  });

  it("renders lowered set and window sources", () => {
    const member = plan();
    const unionSource: TableSource = {
      table: "set",
      alias: "set",
      union: { blocks: [member, member], ops: [] },
    };
    const windowSource: TableSource = {
      table: "window",
      alias: "window",
      windowed: {
        block: plan({
          select: [{ expression: { kind: "column", reference: "i.score" }, alias: "score" }],
        }),
        windows: [
          {
            alias: "previous",
            name: "LAG",
            partitionAliases: [],
            orderAliases: [{ alias: "score", direction: "asc" }],
            argumentAlias: "score",
            offset: 1,
            fallback: 0,
          },
        ],
      },
    };
    expect(renderPlanSql(plan({ base: unionSource })).sql).toContain("UNION");
    const windowed = renderPlanSql(plan({ base: windowSource }));
    expect(windowed.sql).toContain('LAG("score", 1, $1) OVER');
    expect(windowed.params).toEqual([0]);
    expect(() =>
      renderPlanSql(plan({ base: { table: "empty", alias: "e", union: { blocks: [], ops: [] } } })),
    ).toThrow("first query");
    expect(() =>
      renderPlanSql(
        plan({
          base: {
            table: "recursive",
            alias: "r",
            recursive: { reference: "r", base: member, step: member, all: true },
          },
        }),
      ),
    ).toThrow("Recursive plan sources");
  });

  it("rejects an unbound parameter node", () => {
    expect(() =>
      renderPlanSql(
        plan({ select: [{ expression: { kind: "parameter", index: 0 }, alias: "value" }] }),
      ),
    ).toThrow("bind parameters");
  });

  it("renders empty membership lists as constant truth values", () => {
    const membership = (operator: "IN" | "NOT IN"): CompiledQuery =>
      plan({
        predicates: [
          {
            left: { kind: "column", reference: "i.id" },
            operator,
            right: { kind: "list", items: [] },
          },
        ],
      });
    expect(renderPlanSql(membership("IN")).sql).toContain("WHERE (1 = 0)");
    expect(renderPlanSql(membership("NOT IN")).sql).toContain("WHERE (1 = 1)");
  });

  it("renders update and delete statements with returning", () => {
    const update = renderMutationSql(
      {
        kind: "update",
        table: "items",
        assignments: [{ column: "score", expression: { kind: "literal", value: 3 } }],
        predicates: [],
      },
      "*",
    );
    expect(update).toEqual({ sql: 'UPDATE "items" SET "score" = $1 RETURNING *', params: [3] });

    const deletion = renderMutationSql(
      {
        kind: "delete",
        table: "items",
        predicates: [
          {
            left: { kind: "column", reference: "id" },
            operator: "=",
            right: { kind: "literal", value: 7 },
          },
        ],
      },
      ["id", "name"],
    );
    expect(deletion.sql).toBe('DELETE FROM "items" WHERE ("id" = $1) RETURNING "id", "name"');
    expect(deletion.params).toEqual([7]);
  });
});
