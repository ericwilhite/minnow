import {
  type CompiledQuery,
  type CompiledStatement,
  type Expression,
  type JoinPlan,
  type Predicate,
  type QueryValue,
  type TableSource,
} from "@minnowdb/core/plan";
import { type RenderedSql } from "./sql-tag.js";

/**
 * Renders a typed client plan as parameterized SQL. The SQL text stays stable when values change,
 * so the engine can reuse its cached plan.
 */
class PlanSqlWriter {
  readonly params: QueryValue[] = [];

  parameter(value: QueryValue): string {
    this.params.push(value);
    return `$${String(this.params.length)}`;
  }

  identifier(name: string): string {
    return `"${name.replaceAll('"', '""')}"`;
  }

  reference(reference: string): string {
    return reference
      .split(".")
      .map((part) => (part === "*" ? "*" : this.identifier(part)))
      .join(".");
  }

  stringLiteral(value: string): string {
    return `'${value.replaceAll("'", "''")}'`;
  }

  expression(expression: Expression): string {
    switch (expression.kind) {
      case "literal":
        return this.parameter(expression.value);
      case "parameter":
        throw new TypeError("Builder plans must bind parameters before rendering SQL");
      case "column":
        return this.reference(expression.reference);
      case "wildcard":
        return expression.table === undefined ? "*" : `${this.identifier(expression.table)}.*`;
      case "binary":
        return `(${this.expression(expression.left)} ${expression.operator} ${this.expression(expression.right)})`;
      case "call": {
        const args = expression.arguments.map((argument) => this.expression(argument)).join(", ");
        return `${expression.name}(${expression.distinct === true ? "DISTINCT " : ""}${args})`;
      }
      case "list":
        return `(${expression.items.map((item) => this.expression(item)).join(", ")})`;
      case "subquery":
        return `(${this.query(expression.block)})`;
      case "condition":
        return this.condition(expression);
      case "logical":
        return `(${this.expression(expression.left)} ${expression.operator.toUpperCase()} ${this.expression(expression.right)})`;
      case "not":
        return `(NOT ${this.expression(expression.operand)})`;
      case "exists":
        return `${expression.negated ? "NOT " : ""}EXISTS (${this.query(expression.block)})`;
      case "case": {
        const branches = expression.branches
          .map(({ when, then }) => `WHEN ${this.expression(when)} THEN ${this.expression(then)}`)
          .join(" ");
        const otherwise =
          expression.otherwise === undefined
            ? ""
            : ` ELSE ${this.expression(expression.otherwise)}`;
        return `(CASE ${branches}${otherwise} END)`;
      }
      case "window":
        return this.windowExpression(expression);
      case "fts": {
        const columns =
          expression.columns === "*"
            ? "*"
            : expression.columns.map((column) => this.expression(column)).join(", ");
        return `${expression.op === "match" ? "MATCH" : "BM25"}(${columns}) AGAINST ${this.parameter(expression.query)}`;
      }
    }
  }

  condition(expression: Extract<Expression, { kind: "condition" }>): string {
    if (
      (expression.operator === "IN" || expression.operator === "NOT IN") &&
      expression.right.kind === "list" &&
      expression.right.items.length === 0
    ) {
      return expression.operator === "IN" ? "(1 = 0)" : "(1 = 1)";
    }
    const left = this.expression(expression.left);
    if (
      expression.operator === "IS NULL" ||
      expression.operator === "IS NOT NULL" ||
      expression.operator === "IS TRUE"
    ) {
      return `(${left} ${expression.operator})`;
    }
    const escape =
      expression.escape === undefined ? "" : ` ESCAPE ${this.stringLiteral(expression.escape)}`;
    return `(${left} ${expression.operator} ${this.expression(expression.right)}${escape})`;
  }

  predicate(predicate: Predicate): string {
    return this.condition({ kind: "condition", ...predicate });
  }

  windowExpression(expression: Extract<Expression, { kind: "window" }>): string {
    const ranking = new Set(["ROW_NUMBER", "RANK", "DENSE_RANK", "PERCENT_RANK", "CUME_DIST"]);
    const args: string[] = [];
    if (expression.argument !== undefined) args.push(this.expression(expression.argument));
    if (expression.offset !== undefined) args.push(String(expression.offset));
    if (expression.fallback !== undefined) args.push(this.parameter(expression.fallback));
    const argumentText = ranking.has(expression.name) ? "" : args.join(", ") || "*";
    const over = this.overClause(
      expression.partitionBy.map((item) => this.expression(item)),
      expression.orderBy.map(({ expression: item, direction, nulls }) => ({
        value: this.expression(item),
        direction,
        nulls,
      })),
      expression.frame,
    );
    return `${expression.name}(${argumentText}) OVER (${over})`;
  }

  overClause(
    partitionBy: string[],
    orderBy: Array<{
      value: string;
      direction: "asc" | "desc";
      nulls: "first" | "last" | undefined;
    }>,
    frame:
      | {
          unit: "rows" | "range" | "groups";
          start: { kind: string; offset?: number };
          end: { kind: string; offset?: number };
          exclude?: string;
        }
      | undefined,
  ): string {
    const parts: string[] = [];
    if (partitionBy.length > 0) parts.push(`PARTITION BY ${partitionBy.join(", ")}`);
    if (orderBy.length > 0) {
      parts.push(
        `ORDER BY ${orderBy
          .map(
            ({ value, direction, nulls }) =>
              `${value} ${direction.toUpperCase()}${nulls === undefined ? "" : ` NULLS ${nulls.toUpperCase()}`}`,
          )
          .join(", ")}`,
      );
    }
    if (frame !== undefined) {
      const bound = ({ kind, offset }: { kind: string; offset?: number }): string => {
        switch (kind) {
          case "unbounded-preceding":
            return "UNBOUNDED PRECEDING";
          case "preceding":
            return `${String(offset ?? 0)} PRECEDING`;
          case "current-row":
            return "CURRENT ROW";
          case "following":
            return `${String(offset ?? 0)} FOLLOWING`;
          default:
            return "UNBOUNDED FOLLOWING";
        }
      };
      const exclusion =
        frame.exclude === undefined || frame.exclude === "no-others"
          ? ""
          : ` EXCLUDE ${frame.exclude.replaceAll("-", " ").toUpperCase()}`;
      parts.push(
        `${frame.unit.toUpperCase()} BETWEEN ${bound(frame.start)} AND ${bound(frame.end)}${exclusion}`,
      );
    }
    return parts.join(" ");
  }

  source(source: TableSource): string {
    let body: string;
    if (source.derived !== undefined) {
      body = `(${this.query(source.derived)})`;
    } else if (source.union !== undefined) {
      const [first, ...rest] = source.union.blocks;
      if (first === undefined) throw new TypeError("A set operation needs a first query");
      // Each compiled member owns its own ORDER BY, LIMIT, and OFFSET. Parentheses keep those
      // tails attached to that member when the SQL parser builds the set operation again.
      body = `((${this.query(first)})${rest
        .map(
          (block, index) =>
            ` ${(source.union?.ops[index] ?? "union").toUpperCase()} (${this.query(block)})`,
        )
        .join("")})`;
    } else if (source.windowed !== undefined) {
      body = `(${this.windowedSource(source.windowed)})`;
    } else if (source.recursive !== undefined) {
      throw new TypeError("Recursive plan sources are not emitted by the typed client yet");
    } else {
      body = this.identifier(source.table);
    }
    const columns =
      source.columnAliases === undefined
        ? ""
        : `(${source.columnAliases.map((name) => this.identifier(name)).join(", ")})`;
    return `${body} AS ${this.identifier(source.alias)}${columns}`;
  }

  windowedSource(windowed: NonNullable<TableSource["windowed"]>): string {
    const inputColumns = windowed.block.select.map(({ alias }) => this.identifier(alias));
    const windows = windowed.windows.map((window) => {
      const ranking = new Set(["ROW_NUMBER", "RANK", "DENSE_RANK", "PERCENT_RANK", "CUME_DIST"]);
      const args: string[] = [];
      if (window.argumentAlias !== undefined) args.push(this.identifier(window.argumentAlias));
      if (window.offset !== undefined) args.push(String(window.offset));
      if (window.fallback !== undefined) args.push(this.parameter(window.fallback));
      const argumentText = ranking.has(window.name) ? "" : args.join(", ") || "*";
      const over = this.overClause(
        window.partitionAliases.map((alias) => this.identifier(alias)),
        window.orderAliases.map(({ alias, direction, nulls }) => ({
          value: this.identifier(alias),
          direction,
          nulls,
        })),
        window.frame,
      );
      return `${window.name}(${argumentText}) OVER (${over}) AS ${this.identifier(window.alias)}`;
    });
    return `SELECT ${[...inputColumns, ...windows].join(", ")} FROM (${this.query(windowed.block)}) AS ${this.identifier("window input")}`;
  }

  join(join: JoinPlan): string {
    if (join.kind === "semi" || join.kind === "anti") {
      throw new TypeError(
        `Cannot render the optimizer's internal ${join.kind}-join as equivalent SQL`,
      );
    }
    const condition =
      join.on === undefined
        ? `(${this.expression(join.left)} = ${this.expression(join.right)})`
        : this.expression(join.on);
    return `${join.kind === "left" ? "LEFT JOIN" : "JOIN"} ${this.source(join)} ON ${condition}`;
  }

  query(plan: CompiledQuery): string {
    const select = plan.select
      .map(({ expression, alias }) => {
        const rendered = this.expression(expression);
        return expression.kind === "wildcard" && alias === "*"
          ? rendered
          : `${rendered} AS ${this.identifier(alias)}`;
      })
      .join(", ");
    const parts = [`SELECT ${select}`, `FROM ${this.source(plan.base)}`];
    parts.push(...plan.joins.map((join) => this.join(join)));
    if (plan.predicates.length > 0) {
      parts.push(
        `WHERE ${plan.predicates.map((predicate) => this.predicate(predicate)).join(" AND ")}`,
      );
    }
    if (plan.groupBy.length > 0) {
      parts.push(`GROUP BY ${plan.groupBy.map((item) => this.expression(item)).join(", ")}`);
    }
    if (plan.having.length > 0) {
      parts.push(
        `HAVING ${plan.having.map((predicate) => this.predicate(predicate)).join(" AND ")}`,
      );
    }
    if (plan.orderBy.length > 0) {
      parts.push(
        `ORDER BY ${plan.orderBy
          .map(
            ({ expression, direction, nulls }) =>
              `${this.expression(expression)} ${direction.toUpperCase()}${nulls === undefined ? "" : ` NULLS ${nulls.toUpperCase()}`}`,
          )
          .join(", ")}`,
      );
    }
    if (plan.limitWithTies === true && plan.limit !== undefined) {
      if (plan.offset !== undefined) {
        parts.push(`OFFSET ${this.parameter(plan.offset)} ROWS`);
      }
      parts.push(`FETCH FIRST ${this.parameter(plan.limit)} ROWS WITH TIES`);
    } else {
      if (plan.limit !== undefined) parts.push(`LIMIT ${this.parameter(plan.limit)}`);
      if (plan.offset !== undefined) parts.push(`OFFSET ${this.parameter(plan.offset)}`);
    }
    return parts.join(" ");
  }

  mutation(
    statement: Extract<CompiledStatement, { kind: "update" | "delete" }>,
    returning: readonly string[] | "*" | undefined,
  ): string {
    const parts: string[] = [];
    if (statement.kind === "update") {
      parts.push(
        `UPDATE ${this.identifier(statement.table)} SET ${statement.assignments
          .map(
            ({ column, expression }) =>
              `${this.identifier(column)} = ${this.expression(expression)}`,
          )
          .join(", ")}`,
      );
    } else {
      parts.push(`DELETE FROM ${this.identifier(statement.table)}`);
    }
    if (statement.predicates.length > 0) {
      parts.push(
        `WHERE ${statement.predicates.map((predicate) => this.predicate(predicate)).join(" AND ")}`,
      );
    }
    if (returning !== undefined) {
      parts.push(
        `RETURNING ${
          returning === "*" ? "*" : returning.map((column) => this.identifier(column)).join(", ")
        }`,
      );
    }
    return parts.join(" ");
  }
}

export function renderPlanSql(plan: CompiledQuery): RenderedSql {
  const writer = new PlanSqlWriter();
  return { sql: writer.query(plan), params: writer.params };
}

export function renderMutationSql(
  statement: Extract<CompiledStatement, { kind: "update" | "delete" }>,
  returning?: readonly string[] | "*",
): RenderedSql {
  const writer = new PlanSqlWriter();
  return { sql: writer.mutation(statement, returning), params: writer.params };
}
