import type { SqlDomain } from "../storage/types.js";
import type {
  CompiledQuery,
  Expression,
  QueryResult,
  QueryRow,
  QueryValue,
} from "../plan/model.js";
import {
  childExpressions,
  hasAggregate,
  mapChildExpressions,
  executeRowQueryInternal,
  inferResultColumnDomains,
  type SqlColumnSchema,
  externalizeQueryResult,
} from "./query.js";
import { encodeQueryIdentity } from "./query-identity.js";
import {
  exactNumericBinary,
  exactNumericValue,
  externalSqlDomainValue,
  isExactNumeric,
} from "./sql-domains.js";
import { encodeSqlEqualityValue } from "./sql-semantics.js";

interface Aggregate {
  name: "COUNT" | "SUM" | "AVG";
  alias: string;
  argument: Expression;
}
interface Contribution {
  group: string;
  values: QueryValue[];
}
interface Group {
  keys: QueryValue[];
  members: number;
  counts: number[];
  sums: string[];
  absolute: number[];
}

/** Single-table COUNT/SUM/AVG contributions; SQL still evaluates filters and arguments. */
export class LiveAggregate {
  readonly inputPlan: CompiledQuery;
  readonly #outputPlan: CompiledQuery;
  readonly #aggregates: Aggregate[];
  readonly #groupAliases: string[];
  readonly #rows: Map<string, Contribution>;
  readonly #groups: Map<string, Group>;
  readonly #domains: ReadonlyArray<SqlDomain | null>;
  readonly keyAlias: string;
  readonly #schema: SqlColumnSchema[];

  private constructor(
    inputPlan: CompiledQuery,
    outputPlan: CompiledQuery,
    aggregates: Aggregate[],
    groupAliases: string[],
    keyAlias: string,
    rows = new Map<string, Contribution>(),
    groups = new Map<string, Group>(),
    domains: ReadonlyArray<SqlDomain | null> = [],
    schema: SqlColumnSchema[] = [],
  ) {
    this.inputPlan = inputPlan;
    this.#outputPlan = outputPlan;
    this.#aggregates = aggregates;
    this.#groupAliases = groupAliases;
    this.keyAlias = keyAlias;
    this.#rows = rows;
    this.#groups = groups;
    this.#domains = domains;
    this.#schema = schema;
  }

  static plan(plan: CompiledQuery, qualifiedKey: string): LiveAggregate | undefined {
    const aggregates: Aggregate[] = [];
    const groupAliases = plan.groupBy.map((_, index) => `__minnow_live_group_${String(index)}`);
    const groupKeys = new Map(
      plan.groupBy.map((expression, index) => [
        encodeQueryIdentity(expression),
        groupAliases[index] ?? "",
      ]),
    );
    const rowLocal = (expression: Expression): boolean =>
      !hasAggregate(expression) &&
      !["subquery", "exists", "window", "parameter", "wildcard"].includes(expression.kind) &&
      childExpressions(expression).every(rowLocal);
    if (!plan.groupBy.every(rowLocal)) return undefined;
    const rewrite = (expression: Expression): Expression => {
      const grouped = groupKeys.get(encodeQueryIdentity(expression));
      if (grouped !== undefined) return { kind: "column", reference: grouped };
      if (expression.kind === "call" && hasAggregate(expression)) {
        if (!["COUNT", "SUM", "AVG"].includes(expression.name) || expression.arguments.length !== 1)
          throw new TypeError("Not an additive live aggregate");
        const argument = expression.arguments[0];
        if (
          argument === undefined ||
          !(rowLocal(argument) || (expression.name === "COUNT" && argument.kind === "wildcard"))
        )
          throw new TypeError("Not a row-local aggregate argument");
        const alias = `__minnow_live_value_${String(aggregates.length)}`;
        aggregates.push({
          name: expression.name as Aggregate["name"],
          alias,
          argument: argument.kind === "wildcard" ? { kind: "literal", value: 1 } : argument,
        });
        return { kind: "column", reference: alias };
      }
      if (
        expression.kind === "column" ||
        expression.kind === "window" ||
        expression.kind === "subquery" ||
        expression.kind === "exists"
      )
        throw new TypeError("Not a grouped live expression");
      return mapChildExpressions(expression, rewrite);
    };
    try {
      const select = plan.select.map((item) => ({ ...item, expression: rewrite(item.expression) }));
      const having = plan.having.map((predicate) => ({
        ...predicate,
        left: rewrite(predicate.left),
        right: rewrite(predicate.right),
      }));
      const outputAliases = new Set(select.map((item) => item.alias));
      const orderBy = plan.orderBy.map((term) => ({
        ...term,
        expression:
          term.expression.kind === "column" && outputAliases.has(term.expression.reference)
            ? term.expression
            : rewrite(term.expression),
      }));
      if (aggregates.length === 0) return undefined;
      const keyAlias = "__minnow_live_aggregate_key";
      const inputPlan: CompiledQuery = {
        ...plan,
        select: [
          { alias: keyAlias, expression: { kind: "column", reference: qualifiedKey } },
          ...plan.groupBy.map((expression, index) => ({
            alias: groupAliases[index] ?? "",
            expression,
          })),
          ...aggregates.map(({ alias, argument }) => ({ alias, expression: argument })),
        ],
        groupBy: [],
        having: [],
        orderBy: [],
      };
      delete inputPlan.limit;
      delete inputPlan.offset;
      const outputPlan: CompiledQuery = {
        ...plan,
        base: { table: "__minnow_live_groups", alias: "__minnow_live_groups" },
        joins: [],
        select,
        predicates: having,
        groupBy: [],
        having: [],
        orderBy,
      };
      return new LiveAggregate(inputPlan, outputPlan, aggregates, groupAliases, keyAlias);
    } catch {
      return undefined;
    }
  }

  patch(
    result: QueryResult,
    changed: ReadonlySet<string>,
    token: (value: QueryValue) => string,
  ): LiveAggregate {
    const rows = new Map(this.#rows);
    const groups = new Map(this.#groups);
    const touched = new Set<string>();
    const groupFor = (key: string, keys: QueryValue[] = []): Group => {
      let group = groups.get(key);
      if (group === undefined) {
        group = {
          keys,
          members: 0,
          counts: this.#aggregates.map(() => 0),
          sums: this.#aggregates.map(() => exactNumericValue(0) ?? ""),
          absolute: this.#aggregates.map(() => 0),
        };
        groups.set(key, group);
        touched.add(key);
      } else if (!touched.has(key)) {
        group = {
          ...group,
          counts: [...group.counts],
          sums: [...group.sums],
          absolute: [...group.absolute],
        };
        groups.set(key, group);
        touched.add(key);
      }
      return group;
    };
    const apply = (group: Group, values: QueryValue[], sign: 1 | -1): void => {
      group.members += sign;
      for (const [index, aggregate] of this.#aggregates.entries()) {
        const value = values[index] ?? null;
        if (value === null) continue;
        group.counts[index] = (group.counts[index] ?? 0) + sign;
        if (aggregate.name !== "COUNT") {
          if (typeof value !== "number" && !isExactNumeric(value))
            throw new TypeError("Live SUM requires numeric values");
          if (typeof value === "number") {
            if (!Number.isSafeInteger(value))
              throw new TypeError("Floating-point aggregates require full execution");
            group.absolute[index] = (group.absolute[index] ?? 0) + sign * Math.abs(value);
            if (!Number.isSafeInteger(group.absolute[index]))
              throw new TypeError("Aggregate sum may round; requires full execution");
          }
          // Exact contributions support reversible removal; plain numeric inputs are maintained
          // only while every summation order is provably exact in the full float executor.
          const sum = exactNumericBinary(
            sign === 1 ? "+" : "-",
            group.sums[index] ?? exactNumericValue(0),
            exactNumericValue(value),
          );
          if (sum === null || sum === undefined)
            throw new TypeError("Invalid numeric aggregate contribution");
          group.sums[index] = sum;
        }
      }
    };
    for (const key of changed) {
      const old = rows.get(key);
      if (old === undefined) continue;
      apply(groupFor(old.group), old.values, -1);
      rows.delete(key);
    }
    for (const row of result.rows) {
      const key = token(row[this.keyAlias] ?? null);
      const keys = this.#groupAliases.map((alias) => row[alias] ?? null);
      const group = JSON.stringify(keys.map(encodeSqlEqualityValue));
      const values = this.#aggregates.map(({ alias }) => row[alias] ?? null);
      apply(groupFor(group, keys), values, 1);
      rows.set(key, { group, values });
    }
    if (this.#groupAliases.length === 0) groupFor("[]");
    else for (const key of touched) if (groups.get(key)?.members === 0) groups.delete(key);
    const domains = this.#aggregates.map(
      ({ alias }, index) =>
        result.columnDomains[result.columns.indexOf(alias)] ?? this.#domains[index] ?? null,
    );
    return new LiveAggregate(
      this.inputPlan,
      this.#outputPlan,
      this.#aggregates,
      this.#groupAliases,
      this.keyAlias,
      rows,
      groups,
      domains,
      result.columns.map((name, index) => {
        const domain = result.columnDomains[index];
        const value = result.rows.find((row) => row[name] !== null)?.[name];
        const aggregate = this.#aggregates.find((item) => item.alias === name);
        return {
          name,
          type:
            aggregate?.name === "COUNT"
              ? "number"
              : domain !== null && domain !== undefined
                ? "string"
                : typeof value === "number"
                  ? "number"
                  : typeof value === "boolean"
                    ? "boolean"
                    : value instanceof Date
                      ? "datetime"
                      : "string",
          ...(domain === null || domain === undefined || aggregate?.name === "COUNT"
            ? {}
            : { sqlDomain: domain }),
        };
      }),
    );
  }

  result(): QueryResult {
    const rows: QueryRow[] = [];
    for (const group of this.#groups.values()) {
      const row: QueryRow = {};
      for (const [index, alias] of this.#groupAliases.entries())
        row[alias] = group.keys[index] ?? null;
      for (const [index, aggregate] of this.#aggregates.entries()) {
        const count = group.counts[index] ?? 0;
        const domain = this.#domains[index];
        let value: QueryValue =
          aggregate.name === "COUNT" ? count : count === 0 ? null : (group.sums[index] ?? null);
        if (value !== null && aggregate.name !== "COUNT") {
          if (domain?.kind !== "numeric") {
            value = Number(externalSqlDomainValue(value));
            if (aggregate.name === "AVG") value /= count;
          } else if (aggregate.name === "AVG")
            value = exactNumericBinary("/", value, count, domain.scale) ?? null;
        }
        row[aggregate.alias] = value;
      }
      rows.push(row);
    }
    const result = executeRowQueryInternal(
      this.#outputPlan,
      new Map([["__minnow_live_groups", rows]]),
    );
    result.columnDomains = inferResultColumnDomains(
      this.#outputPlan,
      new Map([["__minnow_live_groups", this.#schema]]),
    );
    return externalizeQueryResult(result);
  }

  get retainedBytes(): number {
    let bytes =
      256 +
      encodeQueryIdentity(this.inputPlan).length * 2 +
      encodeQueryIdentity(this.#outputPlan).length * 2;
    for (const [key, row] of this.#rows)
      bytes +=
        64 +
        key.length * 2 +
        row.group.length * 2 +
        row.values.reduce<number>(
          (sum, value) => sum + (typeof value === "string" ? value.length * 2 + 16 : 16),
          0,
        );
    for (const [key, group] of this.#groups)
      bytes +=
        96 + key.length * 2 + group.sums.reduce((sum, value) => sum + value.length * 2 + 24, 0);
    return bytes;
  }
}
