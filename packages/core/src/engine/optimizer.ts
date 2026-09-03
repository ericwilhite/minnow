import { dateIsoString, dateMilliseconds } from "../date-value.js";
import { crossJoinPlan } from "../plan/model.js";
import {
  blockHasRowWindow,
  blockHasSubqueries,
  childExpressions,
  DUAL_TABLE,
  expressionAliases,
  forEachBlockExpression,
  forEachNestedBlock,
  hasAggregate,
  isAggregateCall,
  isScalarFunctionName,
  mapBlockExpressions,
  mapChildExpressions,
  parseQuantified,
  scalarFunctionNames,
  scalarFunctionValue,
  splitCondition,
  statementDatetimeNames,
  volatileScalarFunctionNames,
  type BinaryOperator,
  type ComparisonOperator,
  type CompiledQuery,
  type Expression,
  type JoinPlan,
  type Predicate,
  type PredicateOperator,
  type QueryValue,
  type TableSource,
  transparentProjectionSource,
  dateTruncValue,
} from "./query.js";
import { concatenatedSqlValue, isSqlDomainValue } from "./sql-domains.js";
import { simpleScalarFunctions } from "./sql-functions.js";

/**
 * Deterministic plan-to-plan rewrites over the shared compiled representation. Every rule
 * preserves result semantics exactly; rules that cannot prove safety leave the plan unchanged.
 * The optimizer runs before subquery resolution, so subquery blocks optimize recursively and
 * predicates containing subqueries stay self-contained when they move.
 */
export function optimizePlan(plan: CompiledQuery, options: OptimizeOptions = {}): CompiledQuery {
  const optimized = structuredClone(plan);
  const aliases = new Set<string>();
  collectPlanAliases(optimized, aliases);
  optimizeBlock(optimized, correlationAliasFactory(aliases, options.resolvedNames === true));
  optimized.optimized = true;
  return optimized;
}

export interface OptimizeOptions {
  /**
   * Every unqualified column name in every subquery has been resolved against its scopes, so
   * one that remains is local to its block. Compilation has no catalog and cannot know whether
   * `WHERE region = home_region` inside a subquery reads the subquery's table or the enclosing
   * query's, so it holds back the rewrites that would move such a name into a derived block —
   * an uncorrelated `IN (SELECT …)` becoming a join, an unqualified outer predicate entering a
   * correlation probe. The database applies them after `qualifyCorrelatedReferences`.
   */
  readonly resolvedNames?: boolean;
}

/** The correlation alias generator, carrying whether subquery names are resolved. */
type NextAlias = (() => string) & { readonly resolvedNames: boolean };

/**
 * The bind-time half of the calendar rewrite. Compilation runs before parameters exist, so
 * `DATE_TRUNC('day', col) = ?` reaches execution as a per-row call over the whole scan; once
 * the placeholder is a Date literal the same range rewrite applies. Walks every block the
 * optimizer would have — nested sources and subquery blocks included.
 */
export function rewriteBoundCalendarEqualities(plan: CompiledQuery): void {
  rewriteCalendarEqualities(plan);
  forEachNestedBlock(plan, rewriteBoundCalendarEqualities);
  const visit = (expression: Expression): void => {
    if (expression.kind === "subquery" || expression.kind === "exists") {
      rewriteBoundCalendarEqualities(expression.block);
      return;
    }
    for (const child of childExpressions(expression)) visit(child);
  };
  forEachBlockExpression(plan, visit);
}

/**
 * Resolves an unqualified column in a nested block against the nearest enclosing query scope
 * when that name does not exist in the nested block itself. Compilation has no catalog, so this
 * SQL name-resolution step runs later for database-backed plans and returns the original plan
 * unchanged when it finds nothing to qualify.
 */
export function qualifyCorrelatedReferences(
  plan: CompiledQuery,
  tableColumns: ReadonlyMap<string, readonly string[]>,
): CompiledQuery {
  if (!blockHasSubqueries(plan)) return plan;
  const qualified = structuredClone(plan);
  const changedReferences: string[] = [];
  type Scope = ReadonlyMap<string, readonly string[]>;
  const outputColumns = (source: TableSource): readonly string[] => {
    if (source.columnAliases !== undefined) return source.columnAliases;
    if (source.derived !== undefined) return source.derived.select.map(({ alias }) => alias);
    if (source.union !== undefined) {
      return source.union.blocks[0]?.select.map(({ alias }) => alias) ?? [];
    }
    if (source.windowed !== undefined) {
      return [
        ...source.windowed.block.select.map(({ alias }) => alias),
        ...source.windowed.windows.map(({ alias }) => alias),
      ];
    }
    if (source.recursive !== undefined) {
      return source.recursive.base.select.map(({ alias }) => alias);
    }
    return tableColumns.get(source.table) ?? [];
  };
  const blockScope = (block: CompiledQuery): Scope => {
    const byColumn = new Map<string, string[]>();
    for (const source of [block.base, ...block.joins]) {
      for (const column of outputColumns(source)) {
        const aliases = byColumn.get(column) ?? [];
        aliases.push(source.alias);
        byColumn.set(column, aliases);
      }
    }
    return byColumn;
  };
  const resolveBlock = (block: CompiledQuery, outerScopes: readonly Scope[]): void => {
    const local = blockScope(block);
    const rewrite = (expression: Expression): Expression => {
      if (expression.kind === "column" && !expression.reference.includes(".")) {
        if ((local.get(expression.reference)?.length ?? 0) > 0) return expression;
        for (const outer of outerScopes) {
          const aliases = outer.get(expression.reference) ?? [];
          if (aliases.length === 0) continue;
          if (aliases.length > 1) {
            throw new TypeError(`Ambiguous outer column: ${expression.reference}`);
          }
          changedReferences.push(expression.reference);
          return { kind: "column", reference: `${aliases[0] ?? ""}.${expression.reference}` };
        }
        return expression;
      }
      if (expression.kind === "subquery" || expression.kind === "exists") {
        resolveBlock(expression.block, [local, ...outerScopes]);
        return expression;
      }
      if (expression.kind === "window") {
        expression.partitionBy = expression.partitionBy.map(rewrite);
        for (const order of expression.orderBy) order.expression = rewrite(order.expression);
        if (expression.argument !== undefined) expression.argument = rewrite(expression.argument);
        return expression;
      }
      return mapChildExpressions(expression, rewrite);
    };
    mapBlockExpressions(block, rewrite);
    for (const source of [block.base, ...block.joins]) {
      const sourceOuter = source.lateral === true ? [local, ...outerScopes] : outerScopes;
      if (source.derived !== undefined) resolveBlock(source.derived, sourceOuter);
      source.union?.blocks.forEach((member) => resolveBlock(member, sourceOuter));
      if (source.windowed !== undefined) resolveBlock(source.windowed.block, sourceOuter);
      if (source.recursive !== undefined) {
        resolveBlock(source.recursive.base, sourceOuter);
        resolveBlock(source.recursive.step, sourceOuter);
      }
    }
  };
  resolveBlock(qualified, []);
  return changedReferences.length > 0 ? qualified : plan;
}

/** Every source alias in a plan tree, including blocks nested inside expressions. */
function collectPlanAliases(block: CompiledQuery, aliases: Set<string>): void {
  aliases.add(block.base.alias);
  for (const join of block.joins) aliases.add(join.alias);
  const visitExpression = (expression: Expression): void => {
    if (expression.kind === "subquery" || expression.kind === "exists") {
      collectPlanAliases(expression.block, aliases);
      return;
    }
    for (const child of childExpressions(expression)) visitExpression(child);
  };
  forEachBlockExpression(block, visitExpression);
  forEachNestedBlock(block, (nested) => collectPlanAliases(nested, aliases));
}

/**
 * Cost-based build-side selection using prepared or catalog-derived row counts. A single inner
 * equi-join probes from the base into an index over the joined table, so a joined input more than
 * twice the base size swaps sides and builds over the smaller input. Asymmetric and observable
 * source-order shapes stay unchanged.
 */
export function chooseJoinOrder(
  plan: CompiledQuery,
  inputs: ReadonlyMap<string, { readonly rowCount: number }>,
): CompiledQuery {
  const join = plan.joins[0];
  if (
    plan.joins.length !== 1 ||
    join?.kind !== "inner" ||
    join.on !== undefined ||
    plan.select[0]?.expression.kind === "wildcard"
  ) {
    return plan;
  }
  const baseRows = inputs.get(plan.base.table)?.rowCount ?? 0;
  const joinRows = inputs.get(join.table)?.rowCount ?? 0;
  if (joinRows <= baseRows * 2) return plan;
  const { kind, left, right, ...joinSource } = join;
  void kind;
  return {
    ...plan,
    base: joinSource,
    joins: [{ ...plan.base, kind: "inner", left, right }],
  };
}

function optimizeBlock(block: CompiledQuery, nextCorrelationAlias: NextAlias): void {
  decorrelateLateralSources(block);
  decorrelateBlock(block, nextCorrelationAlias);
  for (const source of [block.base, ...block.joins]) {
    if (source.derived !== undefined) optimizeBlock(source.derived, nextCorrelationAlias);
    source.union?.blocks.forEach((member) => optimizeBlock(member, nextCorrelationAlias));
    if (source.windowed !== undefined) {
      optimizeBlock(source.windowed.block, nextCorrelationAlias);
    }
    if (source.recursive !== undefined) {
      optimizeBlock(source.recursive.base, nextCorrelationAlias);
      optimizeBlock(source.recursive.step, nextCorrelationAlias);
    }
  }
  const optimizeExpressionBlock = (expression: Expression): void => {
    if (expression.kind === "subquery" || expression.kind === "exists") {
      optimizeBlock(expression.block, nextCorrelationAlias);
      return;
    }
    for (const child of childExpressions(expression)) optimizeExpressionBlock(child);
  };
  forEachBlockExpression(block, optimizeExpressionBlock);
  foldBlockConstants(block);
  extractJoinKeys(block);
  normalizeBooleanPredicates(block);
  coalesceOrEqualityLists(block);
  rewriteCalendarEqualities(block);
  propagateJoinKeyConstants(block);
  pushPredicatesIntoDerived(block);
  pruneDerivedProjections(block);
  combineDerivedLimit(block);
}

/**
 * Converts correlated LATERAL derived tables into ordinary set-at-a-time joins. The inner
 * block's correlation predicates become join keys; its grouping, per-row ordering, and LIMIT
 * are kept by grouping on the keys and ranking within them, the same way a correlated scalar
 * subquery is rewritten:
 *
 * - `GROUP BY` gains the equality keys, so each outer row still sees only its own group.
 * - A global aggregate (`SELECT COUNT(*) …` with no GROUP BY) always yields one row per outer
 *   row, so the join becomes a left join and the outer block reads each aggregate through
 *   `CASE WHEN <key> IS NULL THEN <empty value> ELSE <column> END`: COUNT reads 0, others NULL.
 * - `ORDER BY … LIMIT` becomes a row number partitioned by the keys, filtered to the window.
 *
 * Range correlations (`o.amount > c.threshold`) cannot be grouped or partitioned on, so those
 * shapes keep the plain set-at-a-time join and are refused with grouping or LIMIT.
 */
function decorrelateLateralSources(block: CompiledQuery): void {
  if (block.base.lateral === true) {
    throw new TypeError("LATERAL needs a source to its left");
  }
  const available = new Set([block.base.alias]);
  let lateralSequence = 0;
  for (const join of block.joins) {
    if (join.lateral !== true) {
      available.add(join.alias);
      continue;
    }
    delete join.lateral;
    const derived = join.derived;
    if (derived === undefined) throw new TypeError("LATERAL requires a derived query");
    if (!blockReferencesOutside(derived)) {
      available.add(join.alias);
      continue;
    }
    // An ORDER BY over an unselected expression parses as a projection over the ordered block;
    // the correlation, grouping, and ranking live in that inner block, and the projection is
    // widened afterwards so the join can reach the key columns it adds.
    const wrapper = transparentProjectionSource(derived);
    const inner = wrapper?.inner ?? derived;
    const keys = extractCorrelation(inner, available, "LATERAL", true);
    const keyAliases = keys.map((_, index) => `\u0000lateral_key_${String(index + 1)}`);
    const grouped = inner.groupBy.length > 0;
    const globalAggregate =
      !grouped &&
      inner.select.some((item) => containsAggregateCall(item.expression)) &&
      inner.having.length === 0;
    const ranked = blockHasRowWindow(inner);
    const cross =
      (join.on?.kind === "condition" &&
        join.on.operator === "=" &&
        join.on.left.kind === "literal" &&
        join.on.left.value === 1 &&
        join.on.right.kind === "literal" &&
        join.on.right.value === 1) ||
      (join.on?.kind === "literal" && join.on.value === true);
    if (grouped || globalAggregate || ranked || inner.having.length > 0) {
      if (!keys.every((key) => key.operator === "=")) {
        throw new TypeError(
          "Correlated LATERAL queries with grouping, HAVING, ORDER BY, or LIMIT need equality correlations",
        );
      }
      if (inner.having.length > 0 && !grouped) {
        throw new TypeError("HAVING in a correlated LATERAL query needs GROUP BY");
      }
    }
    if (grouped || globalAggregate) {
      for (const key of keys) inner.groupBy.push(structuredClone(key.inner));
    }
    keys.forEach((key, index) => {
      inner.select.push({ expression: key.inner, alias: keyAliases[index] ?? "" });
    });
    if (ranked) {
      lateralSequence += 1;
      const rankedRows = rankLateralRows(inner, keyAliases, lateralSequence);
      if (wrapper === undefined) join.derived = rankedRows;
      else derived.base.derived = rankedRows;
    } else {
      inner.orderBy = [];
    }
    if (wrapper !== undefined) {
      for (const keyAlias of keyAliases) {
        derived.select.push({
          expression: { kind: "column", reference: `${derived.base.alias}.${keyAlias}` },
          alias: keyAlias,
        });
      }
    }
    if (globalAggregate) {
      if (!cross) {
        throw new TypeError(
          "A correlated LATERAL aggregate without GROUP BY needs ON TRUE or a comma join",
        );
      }
      join.kind = "left";
      const keyReference: Expression = {
        kind: "column",
        reference: `${join.alias}.${keyAliases[0] ?? ""}`,
      };
      const replacements = new Map<string, Expression>();
      const visible = new Set(derived.select.map((item) => item.alias));
      for (const item of inner.select) {
        if (item.alias.startsWith("\u0000") || !visible.has(item.alias)) continue;
        const empty = emptyScalarExpression(structuredClone(item.expression));
        if (empty.kind === "literal" && empty.value === null) continue;
        replacements.set(`${join.alias}.${item.alias}`, {
          kind: "case",
          branches: [
            {
              when: {
                kind: "condition",
                operator: "IS NULL",
                left: structuredClone(keyReference),
                right: { kind: "literal", value: null },
              },
              then: empty,
            },
          ],
          otherwise: { kind: "column", reference: `${join.alias}.${item.alias}` },
        });
      }
      if (replacements.size > 0) {
        replaceOuterReferences(block, join.alias, replacements);
      }
    }
    const correlations = keys.map<Expression>((key, index) => ({
      kind: "condition",
      operator: reverseComparison(key.operator),
      left: key.outer,
      right: { kind: "column", reference: `${join.alias}.${keyAliases[index] ?? ""}` },
    }));
    const original = cross
      ? undefined
      : (join.on ?? {
          kind: "condition" as const,
          operator: "=" as const,
          left: join.left,
          right: join.right,
        });
    const conditions = [...correlations, ...(original === undefined ? [] : [original])];
    const first = conditions[0];
    if (conditions.length === 1 && first?.kind === "condition" && first.operator === "=") {
      join.left = first.left;
      join.right = first.right;
      delete join.on;
    } else {
      join.left = { kind: "literal", value: null };
      join.right = { kind: "literal", value: null };
      const seed = conditions[0];
      if (seed === undefined) throw new Error("LATERAL correlation condition is missing");
      join.on = conditions.slice(1).reduce<Expression>(
        (combined, condition) => ({
          kind: "logical",
          operator: "and",
          left: combined,
          right: condition,
        }),
        seed,
      );
    }
    available.add(join.alias);
  }
}

/**
 * Wraps a LATERAL inner block whose ORDER BY/LIMIT apply per outer row: the block's order
 * expressions project as hidden columns, a row number (RANK for WITH TIES) partitioned by the
 * correlation keys ranks them, and a filtering block keeps the requested window. The result
 * exposes the inner block's own aliases, so the outer query's references still resolve.
 */
function rankLateralRows(
  inner: CompiledQuery,
  keyAliases: readonly string[],
  sequence: number,
): CompiledQuery {
  // An order term that names one of the block's own output aliases (the parser's hidden
  // "(order n)" projections included) ranks on that column; any other term projects as a
  // hidden column of its own.
  const selectAliases = new Set(inner.select.map((item) => item.alias));
  const orderAliases = inner.orderBy.map((order, index) => {
    if (
      order.expression.kind === "column" &&
      !order.expression.reference.includes(".") &&
      selectAliases.has(order.expression.reference)
    ) {
      return order.expression.reference;
    }
    const alias = `\u0000lateral_order_${String(sequence)}_${String(index + 1)}`;
    inner.select.push({ expression: structuredClone(order.expression), alias });
    return alias;
  });
  const windows = [
    {
      alias: `\u0000lateral_row_${String(sequence)}`,
      name: inner.limitWithTies === true ? ("RANK" as const) : ("ROW_NUMBER" as const),
      partitionAliases: [...keyAliases],
      orderAliases: inner.orderBy.map((order, index) => ({
        alias: orderAliases[index] ?? "",
        direction: order.direction,
        ...(order.nulls === undefined ? {} : { nulls: order.nulls }),
      })),
    },
  ];
  const { limit, limitParameter, offset, offsetParameter, limitWithTies, ...rows } = inner;
  void limitWithTies;
  rows.orderBy = [];
  const rankedAlias = `\u0000lateral_ranked_${String(sequence)}`;
  const rowNumber: Expression = {
    kind: "column",
    reference: `${rankedAlias}.${windows[0]?.alias ?? ""}`,
  };
  const offsetExpression: Expression =
    offsetParameter === undefined
      ? { kind: "literal", value: offset ?? 0 }
      : { kind: "parameter", index: offsetParameter };
  const predicates: Predicate[] = [];
  if ((offset ?? 0) > 0 || offsetParameter !== undefined) {
    predicates.push({ left: rowNumber, operator: ">", right: offsetExpression });
  }
  if (limit !== undefined || limitParameter !== undefined) {
    const limitExpression: Expression =
      limitParameter === undefined
        ? { kind: "literal", value: limit ?? 0 }
        : { kind: "parameter", index: limitParameter };
    predicates.push({
      left: structuredClone(rowNumber),
      operator: "<=",
      right:
        offset === undefined && offsetParameter === undefined
          ? limitExpression
          : {
              kind: "binary",
              operator: "+",
              left: structuredClone(offsetExpression),
              right: limitExpression,
            },
    });
  }
  return {
    sql: "(lateral ranked rows)",
    base: { table: rankedAlias, alias: rankedAlias, windowed: { block: rows, windows } },
    joins: [],
    select: rows.select.map((item) => ({
      expression: { kind: "column" as const, reference: `${rankedAlias}.${item.alias}` },
      alias: item.alias,
    })),
    predicates,
    groupBy: [],
    having: [],
    orderBy: [],
    ...(limitParameter === undefined ? {} : { limitValidationParameters: [limitParameter] }),
    ...(offsetParameter === undefined ? {} : { offsetValidationParameters: [offsetParameter] }),
  };
}

/**
 * Replaces the outer block's references to one join alias's columns — everywhere except the
 * join's own key expressions, which must keep addressing the raw column.
 */
function replaceOuterReferences(
  block: CompiledQuery,
  alias: string,
  replacements: ReadonlyMap<string, Expression>,
): void {
  const rewrite = (expression: Expression): Expression => {
    if (expression.kind === "column") {
      const replacement = replacements.get(expression.reference);
      return replacement === undefined ? expression : structuredClone(replacement);
    }
    if (expression.kind === "subquery" || expression.kind === "exists") {
      replaceOuterReferences(expression.block, alias, replacements);
      return expression;
    }
    if (expression.kind === "window") {
      expression.partitionBy = expression.partitionBy.map(rewrite);
      for (const order of expression.orderBy) order.expression = rewrite(order.expression);
      if (expression.argument !== undefined) expression.argument = rewrite(expression.argument);
      return expression;
    }
    return mapChildExpressions(expression, rewrite);
  };
  for (const item of block.select) item.expression = rewrite(item.expression);
  for (const predicate of [...block.predicates, ...block.having]) {
    predicate.left = rewrite(predicate.left);
    predicate.right = rewrite(predicate.right);
  }
  block.groupBy = block.groupBy.map(rewrite);
  for (const order of block.orderBy) order.expression = rewrite(order.expression);
  for (const join of block.joins) {
    if (join.alias === alias) continue;
    if (join.on !== undefined) join.on = rewrite(join.on);
  }
}

// --- Join key extraction ---------------------------------------------------------------------

/**
 * Pulls a hash key out of a conjunctive ON clause.
 *
 * A join carries either an equality — which builds a hash index and probes it — or a general
 * condition, which is a nested loop over every pair of rows. Only a bare `a.x = b.x` was ever
 * recognized as the first kind, so `ON b.order_id = a.order_id AND b.product_id > a.product_id`
 * became a cross product with a filter, as did the plain multi-key `ON a.x = b.x AND a.y = b.y`.
 * The equality is right there in both.
 *
 * For an inner join the ON clause and WHERE are the same thing, so one equality becomes the key
 * and every remaining conjunct becomes an ordinary predicate. A left join is left alone: its ON
 * decides which rows are null-extended, and a predicate applied afterwards would delete them.
 */
function extractJoinKeys(block: CompiledQuery): void {
  const available = new Set([block.base.alias]);
  for (const join of block.joins) {
    const condition = join.on;
    if (condition === undefined || join.kind !== "inner") {
      available.add(join.alias);
      continue;
    }
    const conjuncts = splitCondition(condition);
    const keyIndex = conjuncts.findIndex(
      (conjunct) =>
        conjunct.operator === "=" &&
        joinsAcross(conjunct.left, conjunct.right, join.alias, available),
    );
    const key = conjuncts[keyIndex];
    if (key === undefined) {
      available.add(join.alias);
      continue;
    }
    join.left = key.left;
    join.right = key.right;
    delete join.on;
    block.predicates.push(...conjuncts.filter((_, index) => index !== keyIndex));
    available.add(join.alias);
  }
}

/**
 * `DATE_TRUNC('unit', col) = ts` and `EXTRACT(YEAR FROM col) = n` are equalities on a derived
 * value that the scan cannot use; the same truth as a range on the column itself — `col >=
 * start AND col < start + 1 unit` — reads by value range, prunes blocks by their statistics,
 * and runs on the raw datetime kernel. A timestamp that is not aligned to its unit can never
 * equal a truncation, so that predicate becomes a constant false.
 */
function rewriteCalendarEqualities(block: CompiledQuery): void {
  const rewritten: Predicate[] = [];
  for (const predicate of block.predicates) {
    if (predicate.operator !== "=") {
      rewritten.push(predicate);
      continue;
    }
    let call: Extract<Expression, { kind: "call" }> | undefined;
    let literal: Extract<Expression, { kind: "literal" }> | undefined;
    if (predicate.left.kind === "call" && predicate.right.kind === "literal") {
      call = predicate.left;
      literal = predicate.right;
    } else if (predicate.left.kind === "literal" && predicate.right.kind === "call") {
      call = predicate.right;
      literal = predicate.left;
    }
    if (call === undefined || literal === undefined) {
      rewritten.push(predicate);
      continue;
    }
    const range = calendarRange(call, literal);
    if (range === undefined) {
      rewritten.push(predicate);
      continue;
    }
    if (range === "never") {
      rewritten.push({
        left: { kind: "literal", value: 1 },
        operator: "=",
        right: { kind: "literal", value: 0 },
      });
      continue;
    }
    rewritten.push(
      {
        left: structuredClone(range.column),
        operator: ">=",
        right: { kind: "literal", value: range.start },
      },
      {
        left: structuredClone(range.column),
        operator: "<",
        right: { kind: "literal", value: range.end },
      },
    );
  }
  block.predicates = rewritten;
}

function calendarRange(
  call: Extract<Expression, { kind: "call" }>,
  literal: Extract<Expression, { kind: "literal" }>,
): { column: Expression; start: Date; end: Date } | "never" | undefined {
  if (call.name === "DATE_TRUNC") {
    const [unit, column] = call.arguments;
    if (
      unit?.kind !== "literal" ||
      typeof unit.value !== "string" ||
      column?.kind !== "column" ||
      !(literal.value instanceof Date)
    ) {
      return undefined;
    }
    const normalized = unit.value.toLowerCase();
    if (!calendarUnitSteps.has(normalized)) return undefined;
    const start = dateTruncValue(normalized, literal.value);
    if (start === null || Number.isNaN(start.getTime())) return undefined;
    if (start.getTime() !== literal.value.getTime()) return "never";
    return { column, start, end: addCalendarUnit(start, normalized) };
  }
  if (call.name === "EXTRACT") {
    const [field, column] = call.arguments;
    if (
      field?.kind !== "literal" ||
      typeof field.value !== "string" ||
      field.value.toLowerCase() !== "year" ||
      column?.kind !== "column" ||
      typeof literal.value !== "number"
    ) {
      return undefined;
    }
    const year = literal.value;
    if (!Number.isInteger(year) || year < 1 || year > 9999) return "never";
    return {
      column,
      start: new Date(Date.UTC(year, 0, 1)),
      end: new Date(Date.UTC(year + 1, 0, 1)),
    };
  }
  return undefined;
}

const calendarUnitSteps: ReadonlySet<string> = new Set([
  "year",
  "quarter",
  "month",
  "week",
  "day",
  "hour",
  "minute",
  "second",
]);

/** The instant one calendar unit after an aligned UTC instant. */
function addCalendarUnit(start: Date, unit: string): Date {
  const year = start.getUTCFullYear();
  const month = start.getUTCMonth();
  const day = start.getUTCDate();
  switch (unit) {
    case "year":
      return new Date(Date.UTC(year + 1, 0, 1));
    case "quarter":
      return new Date(Date.UTC(year, month + 3, 1));
    case "month":
      return new Date(Date.UTC(year, month + 1, 1));
    case "week":
      return new Date(start.getTime() + 7 * 86_400_000);
    case "day":
      return new Date(Date.UTC(year, month, day + 1));
    case "hour":
      return new Date(start.getTime() + 3_600_000);
    case "minute":
      return new Date(start.getTime() + 60_000);
    default:
      return new Date(start.getTime() + 1000);
  }
}

/**
 * An inner equi-join makes its two key columns equal on every output row, so a constant
 * equality, range, or IN list on one key holds for the other: `c.id = 4 AND o.customer = c.id`
 * implies `o.customer = 4`. The implied predicate is added (never substituted), which lets the
 * table that only carried the join key filter its own scan — through a secondary index when it
 * has one — instead of joining first and filtering after. A placeholder counts as a constant:
 * it is one value for the whole execution once bound. Only inner joins qualify: an outer join
 * keeps rows whose key is NULL, for which the implication fails.
 */
function propagateJoinKeyConstants(block: CompiledQuery): void {
  const earlier = new Set([block.base.alias]);
  const pairs: Array<{ inner: string; outer: string; toInner: boolean; toOuter: boolean }> = [];
  for (const join of block.joins) {
    const ownAlias = join.alias;
    // Only joins that drop an unmatched outer row qualify: a mirrored predicate is applied to
    // the join's output, where a LEFT or anti join's null-extended row would fail it.
    if (
      join.on === undefined &&
      join.left.kind === "column" &&
      join.right.kind === "column" &&
      (join.kind === "inner" || join.kind === "semi")
    ) {
      const leftOwn = join.left.reference.startsWith(`${ownAlias}.`);
      const rightOwn = join.right.reference.startsWith(`${ownAlias}.`);
      if (leftOwn !== rightOwn) {
        const inner = leftOwn ? join.left.reference : join.right.reference;
        const outer = leftOwn ? join.right.reference : join.left.reference;
        const outerAlias = outer.slice(0, outer.indexOf("."));
        // Both directions hold: the key values are equal on every surviving row, and a row
        // without a match on either side is dropped anyway.
        if (earlier.has(outerAlias)) pairs.push({ inner, outer, toInner: true, toOuter: true });
      }
    }
    earlier.add(ownAlias);
  }
  if (pairs.length === 0) return;
  const signature = (predicate: Predicate): string => JSON.stringify(predicate);
  const present = new Set(block.predicates.map(signature));
  const implied: Predicate[] = [];
  const constantSide = (predicate: Predicate): "left" | "right" | undefined => {
    if (predicate.operator === "IN") {
      return predicate.left.kind === "column" &&
        predicate.right.kind === "list" &&
        predicate.right.items.every((item) => isBoundConstant(item))
        ? "right"
        : undefined;
    }
    if (!rangeOrEqualityOperators.has(predicate.operator)) return undefined;
    if (predicate.left.kind === "column" && isBoundConstant(predicate.right)) return "right";
    if (predicate.right.kind === "column" && isBoundConstant(predicate.left)) return "left";
    return undefined;
  };
  for (const predicate of block.predicates) {
    const side = constantSide(predicate);
    if (side === undefined) continue;
    const column = side === "right" ? predicate.left : predicate.right;
    if (column.kind !== "column") continue;
    for (const pair of pairs) {
      const target =
        column.reference === pair.outer && pair.toInner
          ? pair.inner
          : column.reference === pair.inner && pair.toOuter
            ? pair.outer
            : undefined;
      if (target === undefined) continue;
      const replacement: Expression = { kind: "column", reference: target };
      const mirrored: Predicate =
        side === "right"
          ? { ...predicate, left: replacement, right: structuredClone(predicate.right) }
          : { ...predicate, left: structuredClone(predicate.left), right: replacement };
      const key = signature(mirrored);
      if (present.has(key)) continue;
      present.add(key);
      implied.push(mirrored);
    }
  }
  block.predicates.push(...implied);
}

const rangeOrEqualityOperators: ReadonlySet<string> = new Set(["=", "<", "<=", ">", ">="]);

/** A literal, or a placeholder that binding turns into one before execution. */
function isBoundConstant(expression: Expression): boolean {
  return expression.kind === "literal" || expression.kind === "parameter";
}

/**
 * Whether an equality's two sides split cleanly along this join: one side reads only the table
 * being joined, the other only tables already in hand. Anything else — a side spanning both, a
 * side naming a table that has not been joined yet, a comparison of two columns of the same
 * table — is a filter rather than a key, and stays in the residual.
 */
function joinsAcross(
  left: Expression,
  right: Expression,
  alias: string,
  available: ReadonlySet<string>,
): boolean {
  const sides = [expressionAliases(left), expressionAliases(right)];
  const [joined, prior] = sides[0]?.has(alias) === true ? sides : [sides[1], sides[0]];
  if (joined === undefined || prior === undefined) return false;
  if (joined.size !== 1 || !joined.has(alias)) return false;
  return prior.size > 0 && !prior.has(alias) && [...prior].every((name) => available.has(name));
}

// --- Boolean predicate normalization --------------------------------------------------------------
//
// The scan compiles an unboxed kernel per predicate, but only for a predicate that *is* a
// comparison -- a NOT, or a conjunction nested inside one predicate, has no kernel and sends
// every row through the generic expression evaluator. Both shapes have kernel-compilable
// equivalents. NOT distributes into its operand (De Morgan, and each comparison into its
// complement), and a conjunction splits into independent predicates. Every rewrite below holds
// in three-valued logic: negating a comparison keeps NULL comparisons unknown, exactly as
// negating the original did.

/** The comparison that is true exactly when `operator` is false, and unknown where it is. */
const complementaryOperators = new Map<PredicateOperator, PredicateOperator>([
  ["=", "!="],
  ["!=", "="],
  ["<>", "="],
  [">", "<="],
  [">=", "<"],
  ["<", ">="],
  ["<=", ">"],
  ["IN", "NOT IN"],
  ["NOT IN", "IN"],
  ["LIKE", "NOT LIKE"],
  ["NOT LIKE", "LIKE"],
  ["ILIKE", "NOT ILIKE"],
  ["NOT ILIKE", "ILIKE"],
  ["IS NULL", "IS NOT NULL"],
  ["IS NOT NULL", "IS NULL"],
  ["IS DISTINCT FROM", "IS NOT DISTINCT FROM"],
  ["IS NOT DISTINCT FROM", "IS DISTINCT FROM"],
]);

/** Rewrites `NOT x` into an equivalent form without the NOT, or undefined when x has none. */
function negated(expression: Expression): Expression | undefined {
  if (expression.kind === "not") return simplifyNegations(expression.operand);
  if (expression.kind === "logical") {
    // De Morgan. Both branches must lose their NOT too, or the rewrite trades one for two.
    const left = negated(expression.left);
    const right = negated(expression.right);
    if (left === undefined || right === undefined) return undefined;
    return { kind: "logical", operator: expression.operator === "and" ? "or" : "and", left, right };
  }
  if (expression.kind !== "condition") return undefined;
  const complement = complementaryOperators.get(expression.operator);
  if (complement === undefined) return undefined;
  return {
    ...expression,
    operator: complement,
    left: simplifyNegations(expression.left),
    right: simplifyNegations(expression.right),
  };
}

function simplifyNegations(expression: Expression): Expression {
  if (expression.kind === "not") {
    return (
      negated(expression.operand) ?? { kind: "not", operand: simplifyNegations(expression.operand) }
    );
  }
  if (expression.kind === "logical" || expression.kind === "condition") {
    return {
      ...expression,
      left: simplifyNegations(expression.left),
      right: simplifyNegations(expression.right),
    };
  }
  if (expression.kind === "list") {
    return { kind: "list", items: expression.items.map(simplifyNegations) };
  }
  if (expression.kind === "case") {
    return {
      ...expression,
      branches: expression.branches.map((branch) => ({
        when: simplifyNegations(branch.when),
        then: simplifyNegations(branch.then),
      })),
      ...(expression.otherwise === undefined
        ? {}
        : { otherwise: simplifyNegations(expression.otherwise) }),
    };
  }
  return expression;
}

/**
 * Expands one predicate into the conjunction it represents. A bare boolean expression arrives
 * wrapped in IS TRUE, which keeps a row exactly when the expression is true -- the same test the
 * WHERE filter applies -- so a comparison inside the wrapper becomes the predicate itself, and an
 * AND inside it becomes separate predicates that each compile their own kernel.
 */
function expandPredicate(predicate: Predicate, output: Predicate[]): void {
  if (predicate.operator !== "IS TRUE") {
    output.push(predicate);
    return;
  }
  const inner = predicate.left;
  if (inner.kind === "logical" && inner.operator === "and") {
    expandPredicate({ left: inner.left, operator: "IS TRUE", right: predicate.right }, output);
    expandPredicate({ left: inner.right, operator: "IS TRUE", right: predicate.right }, output);
    return;
  }
  if (inner.kind === "condition") {
    output.push({
      left: inner.left,
      operator: inner.operator,
      right: inner.right,
      ...(inner.escape === undefined ? {} : { escape: inner.escape }),
    });
    return;
  }
  output.push(predicate);
}

function normalizeBooleanPredicates(block: CompiledQuery): void {
  const expanded: Predicate[] = [];
  for (const predicate of block.predicates) {
    expandPredicate(
      {
        ...predicate,
        left: simplifyNegations(predicate.left),
        right: simplifyNegations(predicate.right),
      },
      expanded,
    );
  }
  block.predicates = expanded;
}

// --- OR-of-equalities to IN ---------------------------------------------------------------------
//
// `col = a OR col = b OR col = c` is exactly how SQL defines `col IN (a, b, c)` -- same result,
// same three-valued NULL behaviour -- but the two shapes execute nothing alike. An OR is one
// predicate the scan cannot compile a kernel for, so every row walks the generic expression
// evaluator; an IN list compiles to an unboxed membership kernel and, because zone maps
// understand IN, prunes whole blocks the list cannot reach. Normalizing the disjunction into
// the list form is the difference between reading the table and reading one block of it.

/** The column reference an `=` leaf tests, with the value it is compared against. */
function orEqualityLeaf(
  expression: Expression,
): { reference: string; value: Expression } | undefined {
  if (expression.kind !== "condition" || expression.operator !== "=") return undefined;
  // Only literals and placeholders become list members: they are side-effect free and stay
  // valid when the list binds later. Anything else keeps its own evaluation order.
  const isValue = (candidate: Expression): boolean =>
    candidate.kind === "literal" || candidate.kind === "parameter";
  const { left, right } = expression;
  if (left.kind === "column" && isValue(right)) return { reference: left.reference, value: right };
  if (right.kind === "column" && isValue(left)) return { reference: right.reference, value: left };
  return undefined;
}

/**
 * Flattens an OR tree into `=` leaves that all test the same column, or undefined when any
 * branch is something else -- a mixed disjunction keeps its original shape rather than being
 * split into a partial rewrite that would still leave an OR behind.
 */
function sameColumnOrEqualities(
  expression: Expression,
): { reference: string; values: Expression[] } | undefined {
  const values: Expression[] = [];
  let reference: string | undefined;
  const visit = (node: Expression): boolean => {
    if (node.kind === "logical" && node.operator === "or") {
      return visit(node.left) && visit(node.right);
    }
    const leaf = orEqualityLeaf(node);
    if (leaf === undefined) return false;
    reference ??= leaf.reference;
    if (leaf.reference !== reference) return false;
    values.push(leaf.value);
    return true;
  };
  if (!visit(expression)) return undefined;
  if (reference === undefined || values.length < 2) return undefined;
  return { reference, values };
}

function coalesceOrEqualityLists(block: CompiledQuery): void {
  block.predicates = block.predicates.map((predicate) => {
    // A bare OR reaches the predicate list wrapped in IS TRUE, which collapses unknown to
    // false exactly as the WHERE filter does -- so dropping the wrapper changes nothing.
    if (predicate.operator !== "IS TRUE" || predicate.left.kind !== "logical") return predicate;
    const coalesced = sameColumnOrEqualities(predicate.left);
    if (coalesced === undefined) return predicate;
    return {
      left: { kind: "column", reference: coalesced.reference },
      operator: "IN",
      right: { kind: "list", items: coalesced.values },
    } satisfies Predicate;
  });
}

// --- Correlated subquery decorrelation ----------------------------------------------------------
//
// A subquery that references the enclosing block's aliases rewrites into derived-table joins:
// EXISTS becomes a semi-join (or a hidden boolean flag when nested in an expression), IN carries
// the compared value, and scalar aggregates group by either equality keys or distinct outer probe
// values. Decorrelation runs at compile time, so both executors see plain joins and never execute
// a subquery once per outer row.

interface CorrelationKey {
  inner: Expression;
  outer: Expression;
  operator: ComparisonOperator;
}

const comparisonOperators = new Set(["=", "!=", "<>", ">", ">=", "<", "<="]);
const correlationKeyAlias = (index: number): string => `\0correlation_key_${String(index)}`;
const correlationProbeAlias = (index: number): string => `\0correlation_probe_${String(index)}`;
const correlationInputAlias = (index: number): string => `\0correlation_input_${String(index)}`;
const correlationOrderAlias = (index: number): string => `\0correlation_order_${String(index)}`;
const correlationValueAlias = "\0correlation_value";
const correlationMarkerAlias = "\0correlation_match";
const correlationRowNumberAlias = "\0correlation_row_number";
const correlationCountAlias = "\0correlation_count";
const correlationNonNullCountAlias = "\0correlation_non_null_count";
const correlationDecisionCountAlias = "\0correlation_decision_count";
const correlationTruthCountAlias = "\0correlation_truth_count";

function decorrelateBlock(block: CompiledQuery, nextAlias: NextAlias): void {
  const scope = new Set([block.base.alias, ...block.joins.map((join) => join.alias)]);
  const rewritten: Predicate[] = [];
  for (const predicate of block.predicates) {
    const replacement = decorrelatePredicate(block, predicate, scope, nextAlias);
    if (replacement === "consumed") continue;
    rewritten.push(replacement ?? predicate);
  }
  block.predicates = rewritten.map((predicate) => ({
    ...predicate,
    left: rewriteCorrelatedExpression(block, predicate.left, scope, nextAlias),
    right: rewriteCorrelatedExpression(block, predicate.right, scope, nextAlias),
  }));
  const grouped =
    block.groupBy.length > 0 || block.select.some((item) => containsAggregateCall(item.expression));
  for (const item of block.select) {
    if (!expressionHasCorrelatedSubquery(item.expression)) continue;
    if (grouped) {
      const groupedExpressions = new Set(block.groupBy.map((group) => JSON.stringify(group)));
      const outerReferences = correlatedOuterReferences(item.expression);
      if (
        containsAggregateCall(item.expression) ||
        outerReferences.some(
          (reference) =>
            !groupedExpressions.has(
              JSON.stringify({ kind: "column", reference } satisfies Expression),
            ),
        )
      ) {
        throw new TypeError(
          "A grouped correlated select-list subquery must reference only GROUP BY columns and cannot be nested inside an aggregate",
        );
      }
    }
    item.expression = rewriteCorrelatedExpression(block, item.expression, scope, nextAlias);
    // The decorrelated value is functionally determined by the outer grouping keys. Carrying it
    // as one additional key makes that dependency explicit to both grouped executors.
    if (grouped) block.groupBy.push(structuredClone(item.expression));
  }
  assertNoCorrelation(block);
}

function containsAggregateCall(expression: Expression): boolean {
  return hasAggregate(expression);
}

function expressionHasCorrelatedSubquery(expression: Expression): boolean {
  if (expression.kind === "subquery" || expression.kind === "exists") {
    return blockReferencesOutside(expression.block);
  }
  return childExpressions(expression).some(expressionHasCorrelatedSubquery);
}

/** Qualified outer references used by correlated scalar subqueries inside one expression. */
function correlatedOuterReferences(expression: Expression): string[] {
  if (
    (expression.kind === "subquery" || expression.kind === "exists") &&
    blockReferencesOutside(expression.block)
  ) {
    const references: string[] = [];
    collectOutsideReferences(expression.block, new Set(), references);
    return references;
  }
  return childExpressions(expression).flatMap(correlatedOuterReferences);
}

/** Replaces every supported correlated subquery leaf inside an arbitrary expression. */
function rewriteCorrelatedExpression(
  block: CompiledQuery,
  expression: Expression,
  scope: ReadonlySet<string>,
  nextAlias: NextAlias,
): Expression {
  if (expression.kind === "exists" && blockReferencesOutside(expression.block)) {
    return decorrelateExistsExpression(block, expression, scope, nextAlias);
  }
  if (expression.kind === "condition") {
    expression.left = rewriteCorrelatedExpression(block, expression.left, scope, nextAlias);
    if (expression.right.kind === "subquery" && blockReferencesOutside(expression.right.block)) {
      if (expression.operator === "IN" || expression.operator === "NOT IN") {
        const value = decorrelateQuantifiedExpression(
          block,
          expression.left,
          expression.right.block,
          { comparison: "=", quantifier: "any" },
          scope,
          nextAlias,
          "IN",
        );
        return expression.operator === "NOT IN" ? { kind: "not", operand: value } : value;
      }
      const quantified = parseQuantified(expression.operator);
      if (quantified !== undefined) {
        return decorrelateQuantifiedExpression(
          block,
          expression.left,
          expression.right.block,
          quantified,
          scope,
          nextAlias,
          "ANY/ALL",
        );
      }
    }
    expression.right = rewriteCorrelatedExpression(block, expression.right, scope, nextAlias);
    return expression;
  }
  if (expression.kind === "subquery" && blockReferencesOutside(expression.block)) {
    return decorrelateScalarSubquery(block, expression, scope, nextAlias);
  }
  if (expression.kind === "binary" || expression.kind === "logical") {
    expression.left = rewriteCorrelatedExpression(block, expression.left, scope, nextAlias);
    expression.right = rewriteCorrelatedExpression(block, expression.right, scope, nextAlias);
    return expression;
  }
  if (expression.kind === "call") {
    expression.arguments = expression.arguments.map((argument) =>
      rewriteCorrelatedExpression(block, argument, scope, nextAlias),
    );
    return expression;
  }
  if (expression.kind === "not") {
    expression.operand = rewriteCorrelatedExpression(block, expression.operand, scope, nextAlias);
    return expression;
  }
  if (expression.kind === "case") {
    for (const branch of expression.branches) {
      branch.when = rewriteCorrelatedExpression(block, branch.when, scope, nextAlias);
      branch.then = rewriteCorrelatedExpression(block, branch.then, scope, nextAlias);
    }
    if (expression.otherwise !== undefined) {
      expression.otherwise = rewriteCorrelatedExpression(
        block,
        expression.otherwise,
        scope,
        nextAlias,
      );
    }
    return expression;
  }
  return expression;
}

function correlationAliasFactory(scope: Set<string>, resolvedNames: boolean): NextAlias {
  let sequence = 0;
  return Object.assign(
    () => {
      for (;;) {
        sequence += 1;
        const alias = `corr_${String(sequence)}`;
        if (!scope.has(alias)) {
          scope.add(alias);
          return alias;
        }
      }
    },
    { resolvedNames },
  );
}

function decorrelatePredicate(
  block: CompiledQuery,
  predicate: Predicate,
  scope: ReadonlySet<string>,
  nextAlias: NextAlias,
): Predicate | "consumed" | undefined {
  // EXISTS / NOT EXISTS as a bare WHERE conjunct.
  if (predicate.operator === "IS TRUE") {
    let node = predicate.left;
    let negated = false;
    while (node.kind === "not") {
      negated = !negated;
      node = node.operand;
    }
    if (node.kind !== "exists") return undefined;
    if (node.negated) negated = !negated;
    const inner = node.block;
    if (!blockReferencesOutside(inner)) return undefined;
    rejectGroupedInner(inner, "EXISTS");
    if (!canExtractCorrelation(inner, scope, "EXISTS", true)) {
      const replacement = decorrelateExistsWithProbes(
        block,
        { ...node, negated },
        scope,
        nextAlias,
      );
      return {
        left: replacement,
        operator: "IS TRUE",
        right: { kind: "literal", value: true },
      };
    }
    const keys = extractCorrelation(inner, scope, "EXISTS", true);
    const alias = nextAlias();
    const derived: CompiledQuery = {
      sql: "(correlated exists)",
      base: inner.base,
      joins: inner.joins,
      select: keys.map((key, index) => ({
        expression: key.inner,
        alias: correlationKeyAlias(index),
      })),
      predicates: inner.predicates,
      // Grouping by the keys deduplicates, so the join multiplies no outer row. The parser's
      // injected LIMIT 1 and any ORDER BY are dropped: existence ignores both.
      groupBy: keys.map((key) => key.inner),
      having: [],
      orderBy: [],
    };
    const general = keys.some((key) => key.operator !== "=");
    pushCorrelationJoin(
      block,
      alias,
      derived,
      keys,
      general ? (negated ? "anti" : "semi") : negated ? "left" : "inner",
    );
    if (general) return "consumed";
    if (!negated) return "consumed";
    return {
      left: { kind: "column", reference: `${alias}.${correlationKeyAlias(0)}` },
      operator: "IS NULL",
      right: { kind: "literal", value: null },
    };
  }
  // outer IN / NOT IN (correlated subquery)
  if (
    (predicate.operator === "IN" || predicate.operator === "NOT IN") &&
    predicate.right.kind === "subquery"
  ) {
    const inner = predicate.right.block;
    if (!blockReferencesOutside(inner)) {
      // Only once names are resolved: an unqualified outer reference inside the subquery would
      // otherwise be sealed into a derived block where resolution never reaches it.
      return predicate.operator === "IN" && nextAlias.resolvedNames
        ? rewriteUncorrelatedIn(block, predicate, inner, nextAlias)
        : undefined;
    }
    rejectGroupedInner(inner, "IN");
    const item = inner.select[0];
    if (inner.select.length !== 1 || item === undefined || item.expression.kind === "wildcard") {
      throw new TypeError("An IN subquery must select exactly one column");
    }
    const keys = extractCorrelation(inner, scope, "IN", true);
    const general = keys.some((key) => key.operator !== "=");
    if (predicate.operator === "NOT IN" && general) {
      return decorrelateRangeNotIn(block, predicate.left, inner, item.expression, keys, nextAlias);
    }
    if (predicate.operator === "NOT IN") {
      return decorrelateNotIn(block, predicate.left, inner, item.expression, keys, nextAlias);
    }
    const alias = nextAlias();
    const derived: CompiledQuery = {
      sql: "(correlated in)",
      base: inner.base,
      joins: inner.joins,
      select: [
        ...keys.map((key, index) => ({
          expression: key.inner,
          alias: correlationKeyAlias(index),
        })),
        { expression: item.expression, alias: correlationValueAlias },
      ],
      predicates: inner.predicates,
      groupBy: [...keys.map((key) => key.inner), item.expression],
      having: [],
      orderBy: [],
    };
    if (general) {
      const join = generalCorrelationJoin(
        alias,
        derived,
        [
          ...keys.map((_, index): Expression => ({
            kind: "column",
            reference: `${alias}.${correlationKeyAlias(index)}`,
          })),
          { kind: "column", reference: `${alias}.${correlationValueAlias}` },
        ],
        [...keys.map((key) => key.outer), predicate.left],
        [...keys.map((key) => key.operator), "="],
      );
      join.kind = "semi";
      block.joins.push(join);
      return "consumed";
    }
    pushCorrelationJoin(block, alias, derived, keys, "inner");
    return {
      left: predicate.left,
      operator: "=",
      right: { kind: "column", reference: `${alias}.${correlationValueAlias}` },
    };
  }
  // outer comparison ANY / ALL (correlated subquery). At top-level WHERE, ANY is existence of a
  // true comparison; ALL is absence of a false or unknown comparison. Empty sets therefore keep
  // their standard false/true answers without materializing a per-row value list.
  const quantified = parseQuantified(predicate.operator);
  if (quantified !== undefined && predicate.right.kind === "subquery") {
    const inner = predicate.right.block;
    if (!blockReferencesOutside(inner)) return undefined;
    rejectGroupedInner(inner, "ANY/ALL");
    const item = inner.select[0];
    if (inner.select.length !== 1 || item === undefined || item.expression.kind === "wildcard") {
      throw new TypeError("An ANY/ALL subquery must select exactly one column");
    }
    const keys = extractCorrelation(inner, scope, "ANY/ALL", true);
    const alias = nextAlias();
    const derived: CompiledQuery = {
      sql: "(correlated quantified comparison)",
      base: inner.base,
      joins: inner.joins,
      select: [
        ...keys.map((key, index) => ({
          expression: key.inner,
          alias: correlationKeyAlias(index),
        })),
        { expression: item.expression, alias: correlationValueAlias },
      ],
      predicates: inner.predicates,
      groupBy: [...keys.map((key) => key.inner), item.expression],
      having: [],
      orderBy: [],
    };
    const join = generalCorrelationJoin(
      alias,
      derived,
      keys.map((_, index): Expression => ({
        kind: "column",
        reference: `${alias}.${correlationKeyAlias(index)}`,
      })),
      keys.map((key) => key.outer),
      keys.map((key) => key.operator),
    );
    const value: Expression = {
      kind: "column",
      reference: `${alias}.${correlationValueAlias}`,
    };
    const comparison: Expression = {
      kind: "condition",
      operator: quantified.comparison,
      left: structuredClone(predicate.left),
      right: value,
    };
    let quantifiedCondition: Expression = comparison;
    if (quantified.quantifier === "all") {
      quantifiedCondition = {
        kind: "logical",
        operator: "or",
        left: {
          kind: "logical",
          operator: "or",
          left: {
            kind: "condition",
            operator: "IS NULL",
            left: structuredClone(predicate.left),
            right: { kind: "literal", value: null },
          },
          right: {
            kind: "condition",
            operator: "IS NULL",
            left: value,
            right: { kind: "literal", value: null },
          },
        },
        right: { kind: "not", operand: comparison },
      };
    }
    join.on =
      join.on === undefined
        ? quantifiedCondition
        : { kind: "logical", operator: "and", left: join.on, right: quantifiedCondition };
    join.kind = quantified.quantifier === "all" ? "anti" : "semi";
    block.joins.push(join);
    return "consumed";
  }
  // comparison against a correlated scalar aggregate
  if (!comparisonOperators.has(predicate.operator)) return undefined;
  const scalarSide =
    predicate.left.kind === "subquery" && blockReferencesOutside(predicate.left.block)
      ? "left"
      : predicate.right.kind === "subquery" && blockReferencesOutside(predicate.right.block)
        ? "right"
        : undefined;
  if (scalarSide === undefined) return undefined;
  const subquery = scalarSide === "left" ? predicate.left : predicate.right;
  if (subquery.kind !== "subquery") return undefined;
  const value = decorrelateScalarSubquery(block, subquery, scope, nextAlias);
  return scalarSide === "left"
    ? { left: value, operator: predicate.operator, right: predicate.right }
    : { left: predicate.left, operator: predicate.operator, right: value };
}

/**
 * Rewrites `probe NOT IN (correlated values)` without losing SQL's empty-set and NULL rules.
 *
 * One hash anti-join removes exact matches using a prefix-free composite key containing every
 * correlation key plus the compared value. A grouped left join then carries two counts for the
 * correlated set: total rows and non-null values. An absent group means the set is empty (true,
 * even for a NULL probe); otherwise the probe must be non-null and the two counts equal. The two
 * derived scans are bounded set-at-a-time work—no per-outer-row subquery or nested-loop join.
 */
function decorrelateNotIn(
  block: CompiledQuery,
  probe: Expression,
  inner: CompiledQuery,
  item: Expression,
  keys: CorrelationKey[],
  nextAlias: NextAlias,
): Predicate {
  const valuesAlias = nextAlias();
  const values: CompiledQuery = {
    sql: "(correlated not in values)",
    base: structuredClone(inner.base),
    joins: structuredClone(inner.joins),
    select: [
      ...keys.map((key, index) => ({
        expression: structuredClone(key.inner),
        alias: correlationKeyAlias(index),
      })),
      { expression: structuredClone(item), alias: correlationValueAlias },
    ],
    predicates: structuredClone(inner.predicates),
    groupBy: [...keys.map((key) => structuredClone(key.inner)), structuredClone(item)],
    having: [],
    orderBy: [],
  };
  const tuple = (arguments_: Expression[]): Expression => ({
    kind: "call",
    name: "MINNOW_TUPLE_KEY",
    arguments: arguments_,
  });
  block.joins.push({
    table: valuesAlias,
    alias: valuesAlias,
    derived: values,
    kind: "anti",
    left: tuple([...keys.map((key) => structuredClone(key.outer)), structuredClone(probe)]),
    right: tuple([
      ...keys.map((_, index) => ({
        kind: "column" as const,
        reference: `${valuesAlias}.${correlationKeyAlias(index)}`,
      })),
      { kind: "column", reference: `${valuesAlias}.${correlationValueAlias}` },
    ]),
  });

  const flagsAlias = nextAlias();
  const flags: CompiledQuery = {
    sql: "(correlated not in flags)",
    base: structuredClone(inner.base),
    joins: structuredClone(inner.joins),
    select: [
      ...keys.map((key, index) => ({
        expression: structuredClone(key.inner),
        alias: correlationKeyAlias(index),
      })),
      {
        expression: { kind: "call", name: "COUNT", arguments: [{ kind: "wildcard" }] },
        alias: correlationCountAlias,
      },
      {
        expression: { kind: "call", name: "COUNT", arguments: [structuredClone(item)] },
        alias: correlationNonNullCountAlias,
      },
    ],
    predicates: structuredClone(inner.predicates),
    groupBy: keys.map((key) => structuredClone(key.inner)),
    having: [],
    orderBy: [],
  };
  pushCorrelationJoin(block, flagsAlias, flags, keys, "left");

  const total: Expression = {
    kind: "column",
    reference: `${flagsAlias}.${correlationCountAlias}`,
  };
  const nonNull: Expression = {
    kind: "column",
    reference: `${flagsAlias}.${correlationNonNullCountAlias}`,
  };
  const safe: Expression = {
    kind: "logical",
    operator: "or",
    left: {
      kind: "condition",
      operator: "IS NULL",
      left: total,
      right: { kind: "literal", value: null },
    },
    right: {
      kind: "logical",
      operator: "and",
      left: {
        kind: "condition",
        operator: "IS NOT NULL",
        left: structuredClone(probe),
        right: { kind: "literal", value: null },
      },
      right: { kind: "condition", operator: "=", left: total, right: nonNull },
    },
  };
  return {
    left: safe,
    operator: "IS TRUE",
    right: { kind: "literal", value: true },
  };
}

/**
 * Range-correlated NOT IN uses a general anti-join for exact matches, then joins back one flag row
 * per distinct outer probe tuple. The counts preserve empty-set and NULL poisoning semantics.
 */
function decorrelateRangeNotIn(
  block: CompiledQuery,
  probe: Expression,
  inner: CompiledQuery,
  item: Expression,
  keys: CorrelationKey[],
  nextAlias: NextAlias,
): Predicate {
  const valuesAlias = nextAlias();
  const values: CompiledQuery = {
    sql: "(range-correlated not in values)",
    base: structuredClone(inner.base),
    joins: structuredClone(inner.joins),
    select: [
      ...keys.map((key, index) => ({
        expression: structuredClone(key.inner),
        alias: correlationKeyAlias(index),
      })),
      { expression: structuredClone(item), alias: correlationValueAlias },
    ],
    predicates: structuredClone(inner.predicates),
    groupBy: [...keys.map((key) => structuredClone(key.inner)), structuredClone(item)],
    having: [],
    orderBy: [],
  };
  const exactMatch = generalCorrelationJoin(
    valuesAlias,
    values,
    [
      ...keys.map((_, index): Expression => ({
        kind: "column",
        reference: `${valuesAlias}.${correlationKeyAlias(index)}`,
      })),
      { kind: "column", reference: `${valuesAlias}.${correlationValueAlias}` },
    ],
    [...keys.map((key) => structuredClone(key.outer)), structuredClone(probe)],
    [...keys.map((key) => key.operator), "="],
  );
  exactMatch.kind = "anti";
  block.joins.push(exactMatch);

  const probes = outerProbeBlock(block, keys, nextAlias.resolvedNames);
  const probesAlias = nextAlias();
  const innerRowsAlias = nextAlias();
  const innerRows: CompiledQuery = {
    sql: "(range-correlated not in rows)",
    base: inner.base,
    joins: inner.joins,
    select: [
      ...keys.map((key, index) => ({
        expression: key.inner,
        alias: correlationKeyAlias(index),
      })),
      { expression: item, alias: correlationValueAlias },
    ],
    predicates: inner.predicates,
    groupBy: [],
    having: [],
    orderBy: [],
  };
  const probeReferences = keys.map((_, index): Expression => ({
    kind: "column",
    reference: `${probesAlias}.${correlationProbeAlias(index)}`,
  }));
  const flags: CompiledQuery = {
    sql: "(range-correlated not in flags)",
    base: { table: probesAlias, alias: probesAlias, derived: probes },
    joins: [
      generalCorrelationJoin(
        innerRowsAlias,
        innerRows,
        keys.map((_, index): Expression => ({
          kind: "column",
          reference: `${innerRowsAlias}.${correlationKeyAlias(index)}`,
        })),
        probeReferences,
        keys.map(({ operator }) => operator),
      ),
    ],
    select: [
      ...probeReferences.map((expression, index) => ({
        expression,
        alias: correlationKeyAlias(index),
      })),
      {
        expression: { kind: "call", name: "COUNT", arguments: [{ kind: "wildcard" }] },
        alias: correlationCountAlias,
      },
      {
        expression: {
          kind: "call",
          name: "COUNT",
          arguments: [{ kind: "column", reference: `${innerRowsAlias}.${correlationValueAlias}` }],
        },
        alias: correlationNonNullCountAlias,
      },
    ],
    predicates: [],
    groupBy: probeReferences,
    having: [],
    orderBy: [],
  };
  const flagsAlias = nextAlias();
  pushCorrelationJoin(
    block,
    flagsAlias,
    flags,
    keys.map((key) => ({ ...key, operator: "=" })),
    "left",
  );

  const total: Expression = {
    kind: "column",
    reference: `${flagsAlias}.${correlationCountAlias}`,
  };
  const nonNull: Expression = {
    kind: "column",
    reference: `${flagsAlias}.${correlationNonNullCountAlias}`,
  };
  return {
    left: {
      kind: "logical",
      operator: "or",
      left: {
        kind: "condition",
        operator: "IS NULL",
        left: total,
        right: { kind: "literal", value: null },
      },
      right: {
        kind: "logical",
        operator: "and",
        left: {
          kind: "condition",
          operator: "IS NOT NULL",
          left: structuredClone(probe),
          right: { kind: "literal", value: null },
        },
        right: { kind: "condition", operator: "=", left: total, right: nonNull },
      },
    },
    operator: "IS TRUE",
    right: { kind: "literal", value: true },
  };
}

/**
 * Produces the three-valued result of a correlated IN/ANY/ALL expression at any expression
 * position. Distinct outer probe tuples join the correlated set once, and grouped counts retain
 * the difference between true, false, unknown, and an empty set. Top-level WHERE predicates keep
 * their cheaper semi/anti-join rewrites; this path exists for OR, NOT, CASE, and select items.
 */
function decorrelateQuantifiedExpression(
  block: CompiledQuery,
  probe: Expression,
  inner: CompiledQuery,
  quantified: { comparison: ComparisonOperator; quantifier: "any" | "all" },
  scope: ReadonlySet<string>,
  nextAlias: NextAlias,
  label: "IN" | "ANY/ALL",
): Expression {
  rejectGroupedInner(inner, label);
  const item = inner.select[0];
  if (inner.select.length !== 1 || item === undefined || item.expression.kind === "wildcard") {
    throw new TypeError(
      label === "IN"
        ? "An IN subquery must select exactly one column"
        : "An ANY/ALL subquery must select exactly one column",
    );
  }
  const keys = extractCorrelation(inner, scope, label, true);
  const outerExpressions = [
    ...keys.map((key) => structuredClone(key.outer)),
    structuredClone(probe),
  ];
  const probes = outerProbeBlockForExpressions(block, outerExpressions, nextAlias.resolvedNames);
  const probesAlias = nextAlias();
  const probeReferences = outerExpressions.map<Expression>((_, index) => ({
    kind: "column",
    reference: `${probesAlias}.${correlationProbeAlias(index)}`,
  }));

  const innerRowsAlias = nextAlias();
  const innerRows: CompiledQuery = {
    sql: `(nested correlated ${label.toLowerCase()} rows)`,
    base: inner.base,
    joins: inner.joins,
    select: [
      ...keys.map((key, index) => ({
        expression: key.inner,
        alias: correlationKeyAlias(index),
      })),
      { expression: item.expression, alias: correlationValueAlias },
      { expression: { kind: "literal", value: 1 }, alias: correlationMarkerAlias },
    ],
    predicates: inner.predicates,
    groupBy: [],
    having: [],
    orderBy: [],
  };
  const setJoin = generalCorrelationJoin(
    innerRowsAlias,
    innerRows,
    keys.map((_, index): Expression => ({
      kind: "column",
      reference: `${innerRowsAlias}.${correlationKeyAlias(index)}`,
    })),
    probeReferences.slice(0, keys.length),
    keys.map(({ operator }) => operator),
  );
  setJoin.kind = "left";

  const comparison: Expression = {
    kind: "condition",
    operator: quantified.comparison,
    left: probeReferences[keys.length] ?? { kind: "literal", value: null },
    right: { kind: "column", reference: `${innerRowsAlias}.${correlationValueAlias}` },
  };
  const decisiveComparison: Expression =
    quantified.quantifier === "any"
      ? structuredClone(comparison)
      : { kind: "not", operand: structuredClone(comparison) };
  const truthMarker: Expression = {
    kind: "case",
    branches: [{ when: decisiveComparison, then: { kind: "literal", value: 1 } }],
    otherwise: { kind: "literal", value: null },
  };
  const flags: CompiledQuery = {
    sql: `(nested correlated ${label.toLowerCase()} flags)`,
    base: { table: probesAlias, alias: probesAlias, derived: probes },
    joins: [setJoin],
    select: [
      ...probeReferences.map((expression, index) => ({
        expression,
        alias: correlationKeyAlias(index),
      })),
      {
        expression: {
          kind: "call",
          name: "COUNT",
          arguments: [{ kind: "column", reference: `${innerRowsAlias}.${correlationMarkerAlias}` }],
        },
        alias: correlationCountAlias,
      },
      {
        expression: { kind: "call", name: "COUNT", arguments: [comparison] },
        alias: correlationDecisionCountAlias,
      },
      {
        expression: { kind: "call", name: "COUNT", arguments: [truthMarker] },
        alias: correlationTruthCountAlias,
      },
    ],
    predicates: [],
    groupBy: probeReferences,
    having: [],
    orderBy: [],
  };
  const flagsAlias = nextAlias();
  pushNullSafeProbeJoin(block, flagsAlias, flags, outerExpressions);

  const total: Expression = {
    kind: "column",
    reference: `${flagsAlias}.${correlationCountAlias}`,
  };
  const decisions: Expression = {
    kind: "column",
    reference: `${flagsAlias}.${correlationDecisionCountAlias}`,
  };
  const truths: Expression = {
    kind: "column",
    reference: `${flagsAlias}.${correlationTruthCountAlias}`,
  };
  const defaultValue = quantified.quantifier === "all";
  return {
    kind: "case",
    branches: [
      {
        when: {
          kind: "condition",
          operator: ">",
          left: truths,
          right: { kind: "literal", value: 0 },
        },
        then: { kind: "literal", value: !defaultValue },
      },
      {
        when: {
          kind: "condition",
          operator: "=",
          left: total,
          right: { kind: "literal", value: 0 },
        },
        then: { kind: "literal", value: defaultValue },
      },
      {
        when: { kind: "condition", operator: "<", left: decisions, right: total },
        then: { kind: "literal", value: null },
      },
    ],
    otherwise: { kind: "literal", value: defaultValue },
  };
}

/**
 * Rewrites one correlated scalar-aggregate subquery into a left join against the grouped
 * aggregate, returning the expression that replaces the subquery node (COUNT wraps in COALESCE
 * so an unmatched group reads 0, matching the empty-subquery semantics).
 */
function decorrelateScalarSubquery(
  block: CompiledQuery,
  subquery: Extract<Expression, { kind: "subquery" }>,
  scope: ReadonlySet<string>,
  nextAlias: NextAlias,
): Expression {
  const inner = subquery.block;
  const passthrough = scalarPassthrough(inner);
  if (passthrough !== undefined) return passthrough;
  const item = inner.select[0];
  if (
    inner.select.length !== 1 ||
    item === undefined ||
    !isAggregateCall(item.expression) ||
    inner.groupBy.length > 0 ||
    inner.having.length > 0 ||
    inner.orderBy.length > 0 ||
    blockHasRowWindow(inner) ||
    !canExtractCorrelation(inner, scope, "scalar", true)
  ) {
    return decorrelateGeneralScalar(block, inner, scope, nextAlias);
  }
  const previewKeys = extractCorrelation(structuredClone(inner), scope, "scalar", true);
  if (
    previewKeys.some((key) => key.operator !== "=") &&
    (item.expression.arguments.length !== 1 || item.expression.aggregateOrderBy !== undefined)
  ) {
    return decorrelateGeneralScalar(block, inner, scope, nextAlias);
  }
  const keys = extractCorrelation(inner, scope, "scalar", true);
  if (keys.some((key) => key.operator !== "=")) {
    return decorrelateNonEqualityScalar(block, inner, item.expression, keys, nextAlias);
  }
  const alias = nextAlias();
  const derived: CompiledQuery = {
    sql: "(correlated scalar)",
    base: inner.base,
    joins: inner.joins,
    select: [
      ...keys.map((key, index) => ({
        expression: key.inner,
        alias: correlationKeyAlias(index),
      })),
      { expression: item.expression, alias: correlationValueAlias },
    ],
    predicates: inner.predicates,
    groupBy: keys.map((key) => key.inner),
    having: [],
    orderBy: [],
  };
  pushCorrelationJoin(block, alias, derived, keys, "left");
  let value: Expression = {
    kind: "column",
    reference: `${alias}.${correlationValueAlias}`,
  };
  if (item.expression.name === "COUNT") {
    // COUNT over an empty group is 0, but the unmatched left-join side is NULL.
    value = { kind: "call", name: "COALESCE", arguments: [value, { kind: "literal", value: 0 }] };
  }
  return value;
}

/** A SELECT without a row source is already the scalar expression from its enclosing scope. */
function scalarPassthrough(inner: CompiledQuery): Expression | undefined {
  const item = inner.select[0];
  if (
    inner.base.table !== DUAL_TABLE ||
    inner.base.derived !== undefined ||
    inner.joins.length > 0 ||
    inner.select.length !== 1 ||
    item === undefined ||
    inner.predicates.length > 0 ||
    inner.groupBy.length > 0 ||
    inner.having.length > 0 ||
    inner.orderBy.length > 0 ||
    inner.offset !== undefined ||
    inner.offsetParameter !== undefined ||
    inner.limitParameter !== undefined ||
    (inner.limit !== undefined && inner.limit < 1)
  ) {
    return undefined;
  }
  return structuredClone(item.expression);
}

interface ScalarShape {
  rows: CompiledQuery;
  expression: Expression;
}

/**
 * Pulls a scalar expression through projection-only derived wrappers. The parser uses these
 * wrappers to hide ORDER BY support columns, and Kysely's JSON helpers use one to turn an
 * explicitly selected row into JSON. Row-affecting clauses remain on the deepest block, where
 * the probe rewrite can apply them once per outer key.
 */
function scalarShape(inner: CompiledQuery): ScalarShape {
  const item = inner.select[0];
  if (inner.select.length !== 1 || item === undefined || item.expression.kind === "wildcard") {
    throw new TypeError("A scalar subquery must select exactly one column");
  }
  let rows = inner;
  let expression: Expression = structuredClone(item.expression);
  for (;;) {
    const derived = rows.base.derived;
    if (
      derived === undefined ||
      rows.joins.length > 0 ||
      rows.predicates.length > 0 ||
      rows.groupBy.length > 0 ||
      rows.having.length > 0 ||
      rows.orderBy.length > 0 ||
      blockHasRowWindow(rows) ||
      rows.distinctWildcard === true
    ) {
      break;
    }
    expression = inlineDerivedProjection(expression, rows.base.alias, derived);
    rows = derived;
  }
  return { rows, expression };
}

/** Replaces references to a derived row with the expressions that produced its named columns. */
function inlineDerivedProjection(
  expression: Expression,
  alias: string,
  derived: CompiledQuery,
): Expression {
  const rewrite = (node: Expression): Expression => {
    if (node.kind === "column") {
      const parts = node.reference.split(".");
      if (parts.length === 2 && parts[0] !== alias) return node;
      const name = parts.length === 2 ? (parts[1] ?? "") : (parts[0] ?? "");
      const matches = derived.select.filter((item) => item.alias === name);
      if (matches.length === 1) return structuredClone(matches[0]?.expression ?? node);
      return node;
    }
    return mapChildExpressions(node, rewrite);
  };
  return rewrite(expression);
}

/** ORDER BY may name a hidden or visible select alias; recover its row-level expression. */
function scalarOrderExpression(expression: Expression, rows: CompiledQuery): Expression {
  if (expression.kind !== "column" || expression.reference.includes(".")) {
    return structuredClone(expression);
  }
  const matches = rows.select.filter((item) => item.alias === expression.reference);
  return matches.length === 1
    ? structuredClone(matches[0]?.expression ?? expression)
    : structuredClone(expression);
}

/** The value a scalar aggregate expression produces for an empty correlated input. */
function emptyScalarExpression(
  expression: Expression,
  preservedColumns: ReadonlySet<string> = new Set(),
): Expression {
  if (isAggregateCall(expression)) {
    return { kind: "literal", value: expression.name === "COUNT" ? 0 : null };
  }
  if (expression.kind === "column") {
    return preservedColumns.has(expression.reference)
      ? structuredClone(expression)
      : { kind: "literal", value: null };
  }
  if (expression.kind === "wildcard") {
    return { kind: "literal", value: null };
  }
  return mapChildExpressions(expression, (child) => emptyScalarExpression(child, preservedColumns));
}

/**
 * General correlated scalar rewrite. Distinct outer probe tuples join the inner rows once,
 * ORDER BY/LIMIT rank within each probe, and one grouped result joins back by NULL-safe equality.
 * Aggregate expressions retain their empty-input rules; ordinary scalar rows use the internal
 * single-value aggregate, which raises the same cardinality error as an uncorrelated scalar.
 */
function decorrelateGeneralScalar(
  block: CompiledQuery,
  inner: CompiledQuery,
  scope: ReadonlySet<string>,
  nextAlias: NextAlias,
): Expression {
  const shape = scalarShape(inner);
  const rows = shape.rows;
  if (rows.groupBy.length > 0 || rows.having.length > 0) {
    throw new TypeError("A correlated scalar subquery cannot use GROUP BY or HAVING");
  }
  if (rows.limitWithTies === true && rows.orderBy.length === 0) {
    throw new TypeError("A correlated scalar WITH TIES query requires ORDER BY");
  }
  const expressionOutside: string[] = [];
  collectExpressionOutsideReferences(
    shape.expression,
    new Set([rows.base.alias, ...rows.joins.map((join) => join.alias)]),
    expressionOutside,
  );
  const probeLifted =
    expressionOutside.length > 0 || !canExtractCorrelation(rows, scope, "scalar", true);
  const references = probeLifted
    ? probeOuterReferences(rows, expressionOutside, scope, "scalar")
    : [];
  const keys = probeLifted
    ? takeCorrelationPredicates(rows, scope, true)
    : extractCorrelation(rows, scope, "scalar", true);
  const outerExpressions = probeLifted
    ? references.map<Expression>((reference) => ({ kind: "column", reference }))
    : keys.map((key) => structuredClone(key.outer));
  const originalExpression = structuredClone(shape.expression);
  const probes = outerProbeBlockForExpressions(block, outerExpressions, nextAlias.resolvedNames);
  const probesAlias = nextAlias();
  const probeReferences = outerExpressions.map<Expression>((_, index) => ({
    kind: "column",
    reference: `${probesAlias}.${correlationProbeAlias(index)}`,
  }));
  if (probeLifted) {
    const replacements = new Map(
      references.map((reference, index) => [reference, probeReferences[index] ?? null]),
    );
    replacePlanReferences(rows, replacements);
    shape.expression = replaceExpressionReferences(shape.expression, replacements);
    const source = { table: probesAlias, alias: probesAlias, derived: probes };
    const missingProbe: Expression = { kind: "literal", value: null };
    const keyProbes = keys.map((key) => {
      const reference = key.outer.kind === "column" ? key.outer.reference : "";
      return probeReferences[references.indexOf(reference)] ?? missingProbe;
    });
    const keysReadBase = keys.every((key) => {
      const parts = key.inner.kind === "column" ? key.inner.reference.split(".") : [];
      return parts.length === 1 || parts[0] === rows.base.alias;
    });
    if (keys.length > 0 && keysReadBase) {
      rows.joins.unshift(
        generalCorrelationJoin(
          probesAlias,
          probes,
          keys.map((key) => key.inner),
          keyProbes,
          keys.map((key) => key.operator),
        ),
      );
    } else {
      // A probe introduced before the original joins is visible to every JOIN ON expression.
      // Keys that read one of those later joins remain WHERE predicates over the complete row.
      rows.joins.unshift(crossJoinPlan(source));
      rows.predicates.push(
        ...keys.map((key, index) => ({
          left: key.inner,
          operator: key.operator,
          right: keyProbes[index] ?? missingProbe,
        })),
      );
    }
  }

  const finalRowsAlias = nextAlias();
  const projected: Array<{ expression: Expression; alias: string }> = [];
  const project = (expression: Expression, alias: string): Expression => {
    projected.push({ expression: structuredClone(expression), alias });
    return { kind: "column", reference: `${finalRowsAlias}.${alias}` };
  };
  const orderInputs = rows.orderBy.map((order, index) => ({
    reference: project(scalarOrderExpression(order.expression, rows), correlationOrderAlias(index)),
    direction: order.direction,
    ...(order.nulls === undefined ? {} : { nulls: order.nulls }),
  }));

  let inputSequence = 0;
  const aggregateResult = (expression: Expression): Expression => {
    if (isAggregateCall(expression)) {
      const arguments_ = expression.arguments.map((argument) =>
        argument.kind === "wildcard"
          ? structuredClone(argument)
          : project(argument, correlationInputAlias(inputSequence++)),
      );
      const ownOrder = expression.aggregateOrderBy?.map((order) => ({
        expression: project(order.expression, correlationInputAlias(inputSequence++)),
        direction: order.direction,
        ...(order.nulls === undefined ? {} : { nulls: order.nulls }),
      }));
      const inheritedOrder =
        expression.name === "JSON_ARRAYAGG" || expression.name === "STRING_AGG"
          ? orderInputs.map(({ reference, direction, nulls }) => ({
              expression: structuredClone(reference),
              direction,
              ...(nulls === undefined ? {} : { nulls }),
            }))
          : undefined;
      const { aggregateOrderBy: ignoredOrder, ...base } = expression;
      void ignoredOrder;
      const aggregateOrder =
        ownOrder !== undefined && ownOrder.length > 0
          ? ownOrder
          : inheritedOrder !== undefined && inheritedOrder.length > 0
            ? inheritedOrder
            : undefined;
      const rewritten: Expression = {
        ...base,
        arguments: arguments_,
        ...(aggregateOrder === undefined ? {} : { aggregateOrderBy: aggregateOrder }),
      };
      return rewritten;
    }
    if (expression.kind === "column" || expression.kind === "wildcard") {
      if (probeLifted && expression.kind === "column") {
        const index = probeReferences.findIndex(
          (reference) =>
            reference.kind === "column" && reference.reference === expression.reference,
        );
        if (index >= 0) {
          return {
            kind: "column",
            reference: `${finalRowsAlias}.${correlationKeyAlias(index)}`,
          };
        }
      }
      throw new TypeError("A correlated scalar aggregate cannot select an ungrouped inner column");
    }
    if (expression.kind === "subquery" || expression.kind === "exists") {
      throw new TypeError("A correlated scalar aggregate cannot contain another subquery");
    }
    return mapChildExpressions(expression, aggregateResult);
  };

  const grouped = hasAggregate(shape.expression);
  const valueExpression = grouped
    ? aggregateResult(shape.expression)
    : {
        kind: "call" as const,
        name: "MINNOW_SINGLE_VALUE" as const,
        arguments: [project(shape.expression, correlationValueAlias)],
      };

  const innerRowsAlias = nextAlias();
  const rawRows: CompiledQuery = {
    sql: "(correlated scalar input rows)",
    base: rows.base,
    joins: rows.joins,
    select: [
      ...(probeLifted ? probeReferences : keys.map((key) => key.inner)).map(
        (expression, index) => ({
          expression,
          alias: correlationKeyAlias(index),
        }),
      ),
      ...projected,
    ],
    // Every inner row must equal a surviving probe on the keys, so what the probe block filters
    // on its key columns filters the inner scan too: the range on the outer key becomes a
    // range on the inner key, and the scan prunes blocks by it instead of reading the table.
    predicates: [
      ...rows.predicates,
      ...(probeLifted ? [] : mirrorPredicatesThroughKeys(probes.predicates, keys)),
    ],
    groupBy: [],
    having: [],
    orderBy: [],
  };
  const matched: CompiledQuery = probeLifted
    ? rawRows
    : {
        sql: "(correlated scalar matched probes)",
        base: { table: probesAlias, alias: probesAlias, derived: probes },
        joins: [
          generalCorrelationJoin(
            innerRowsAlias,
            rawRows,
            keys.map((_, index): Expression => ({
              kind: "column",
              reference: `${innerRowsAlias}.${correlationKeyAlias(index)}`,
            })),
            probeReferences,
            keys.map(({ operator }) => operator),
          ),
        ],
        select: [
          ...probeReferences.map((expression, index) => ({
            expression,
            alias: correlationKeyAlias(index),
          })),
          ...projected.map(({ alias }) => ({
            expression: { kind: "column" as const, reference: `${innerRowsAlias}.${alias}` },
            alias,
          })),
        ],
        predicates: [],
        groupBy: [],
        having: [],
        orderBy: [],
      };

  let finalRows = matched;
  if (blockHasRowWindow(rows)) {
    const rankedAlias = nextAlias();
    const rankedTable = nextAlias();
    const ranked = {
      table: rankedTable,
      alias: rankedAlias,
      windowed: {
        block: matched,
        windows: [
          {
            alias: correlationRowNumberAlias,
            name: rows.limitWithTies === true ? ("RANK" as const) : ("ROW_NUMBER" as const),
            partitionAliases: outerExpressions.map((_, index) => correlationKeyAlias(index)),
            orderAliases: rows.orderBy.map((order, index) => ({
              alias: correlationOrderAlias(index),
              direction: order.direction,
              ...(order.nulls === undefined ? {} : { nulls: order.nulls }),
            })),
          },
        ],
      },
    };
    const rowNumber: Expression = {
      kind: "column",
      reference: `${rankedAlias}.${correlationRowNumberAlias}`,
    };
    const offset: Expression =
      rows.offsetParameter === undefined
        ? { kind: "literal", value: rows.offset ?? 0 }
        : { kind: "parameter", index: rows.offsetParameter };
    const predicates: Predicate[] = [];
    if ((rows.offset ?? 0) > 0 || rows.offsetParameter !== undefined) {
      predicates.push({
        left: rowNumber,
        operator: ">",
        right: offset,
      });
    }
    if (rows.limit !== undefined || rows.limitParameter !== undefined) {
      const limit: Expression =
        rows.limitParameter === undefined
          ? { kind: "literal", value: rows.limit ?? 0 }
          : { kind: "parameter", index: rows.limitParameter };
      predicates.push({
        left: structuredClone(rowNumber),
        operator: "<=",
        right:
          rows.offset === undefined && rows.offsetParameter === undefined
            ? limit
            : { kind: "binary", operator: "+", left: structuredClone(offset), right: limit },
      });
    }
    finalRows = {
      sql: "(correlated scalar limited rows)",
      base: ranked,
      joins: [],
      select: [
        ...outerExpressions.map((_, index) => ({
          expression: {
            kind: "column" as const,
            reference: `${rankedAlias}.${correlationKeyAlias(index)}`,
          },
          alias: correlationKeyAlias(index),
        })),
        ...projected.map(({ alias }) => ({
          expression: { kind: "column" as const, reference: `${rankedAlias}.${alias}` },
          alias,
        })),
      ],
      predicates,
      groupBy: [],
      having: [],
      orderBy: [],
      ...(rows.limitParameter === undefined
        ? {}
        : { limitValidationParameters: [rows.limitParameter] }),
      ...(rows.offsetParameter === undefined
        ? {}
        : { offsetValidationParameters: [rows.offsetParameter] }),
    };
  }

  const finalProbeReferences = outerExpressions.map((_, index): Expression => ({
    kind: "column",
    reference: `${finalRowsAlias}.${correlationKeyAlias(index)}`,
  }));
  const aggregate: CompiledQuery = {
    sql: "(correlated scalar result)",
    base: { table: finalRowsAlias, alias: finalRowsAlias, derived: finalRows },
    joins: [],
    select: [
      ...finalProbeReferences.map((expression, index) => ({
        expression,
        alias: correlationKeyAlias(index),
      })),
      { expression: valueExpression, alias: correlationValueAlias },
      { expression: { kind: "literal", value: 1 }, alias: correlationMarkerAlias },
    ],
    predicates: [],
    groupBy: finalProbeReferences,
    having: [],
    orderBy: [],
  };
  const resultAlias = nextAlias();
  pushNullSafeProbeJoin(block, resultAlias, aggregate, outerExpressions);
  const marker: Expression = {
    kind: "column",
    reference: `${resultAlias}.${correlationMarkerAlias}`,
  };
  return {
    kind: "case",
    branches: [
      {
        when: {
          kind: "condition",
          operator: "IS NULL",
          left: marker,
          right: { kind: "literal", value: null },
        },
        then: grouped
          ? emptyScalarExpression(originalExpression, new Set(references))
          : { kind: "literal", value: null },
      },
    ],
    otherwise: {
      kind: "column",
      reference: `${resultAlias}.${correlationValueAlias}`,
    },
  };
}

/**
 * Aggregates a range-correlated scalar subquery once per distinct outer probe tuple. The probe
 * table prevents a general join from multiplying outer rows and lets the final join use equality
 * keys even when the original correlation uses `<`, `>=`, or another comparison.
 */
function decorrelateNonEqualityScalar(
  block: CompiledQuery,
  inner: CompiledQuery,
  aggregate: Extract<Expression, { kind: "call" }>,
  keys: CorrelationKey[],
  nextAlias: NextAlias,
): Expression {
  if (aggregate.aggregateOrderBy !== undefined) {
    throw new TypeError("A range-correlated scalar aggregate cannot use aggregate ORDER BY");
  }
  const argument = aggregate.arguments[0];
  if (aggregate.arguments.length !== 1 || argument === undefined) {
    throw new TypeError("A correlated scalar aggregate must have exactly one argument");
  }

  const probes = outerProbeBlock(block, keys, nextAlias.resolvedNames);
  const probesAlias = nextAlias();
  const innerRowsAlias = nextAlias();
  const innerRows: CompiledQuery = {
    sql: "(range-correlated scalar rows)",
    base: inner.base,
    joins: inner.joins,
    select: [
      ...keys.map((key, index) => ({
        expression: key.inner,
        alias: correlationKeyAlias(index),
      })),
      {
        expression: argument.kind === "wildcard" ? { kind: "literal", value: 1 } : argument,
        alias: correlationValueAlias,
      },
    ],
    predicates: inner.predicates,
    groupBy: [],
    having: [],
    orderBy: [],
  };
  const joinedAggregate: Extract<Expression, { kind: "call" }> = {
    ...aggregate,
    arguments: [{ kind: "column", reference: `${innerRowsAlias}.${correlationValueAlias}` }],
  };
  const probeReferences = keys.map((_, index): Expression => ({
    kind: "column",
    reference: `${probesAlias}.${correlationProbeAlias(index)}`,
  }));
  const aggregateBlock: CompiledQuery = {
    sql: "(range-correlated scalar aggregate)",
    base: { table: probesAlias, alias: probesAlias, derived: probes },
    joins: [
      generalCorrelationJoin(
        innerRowsAlias,
        innerRows,
        keys.map((_, index): Expression => ({
          kind: "column",
          reference: `${innerRowsAlias}.${correlationKeyAlias(index)}`,
        })),
        probeReferences,
        keys.map(({ operator }) => operator),
      ),
    ],
    select: [
      ...probeReferences.map((expression, index) => ({
        expression,
        alias: correlationKeyAlias(index),
      })),
      { expression: joinedAggregate, alias: correlationValueAlias },
    ],
    predicates: [],
    groupBy: probeReferences,
    having: [],
    orderBy: [],
  };
  const aggregateAlias = nextAlias();
  pushCorrelationJoin(
    block,
    aggregateAlias,
    aggregateBlock,
    keys.map((key) => ({ ...key, operator: "=" })),
    "left",
  );

  let value: Expression = {
    kind: "column",
    reference: `${aggregateAlias}.${correlationValueAlias}`,
  };
  if (aggregate.name === "COUNT") {
    value = { kind: "call", name: "COALESCE", arguments: [value, { kind: "literal", value: 0 }] };
  }
  return value;
}

/** Builds the hidden boolean flag used when correlated EXISTS is nested below OR/NOT/CASE. */
function decorrelateExistsExpression(
  block: CompiledQuery,
  exists: Extract<Expression, { kind: "exists" }>,
  scope: ReadonlySet<string>,
  nextAlias: NextAlias,
): Expression {
  const inner = exists.block;
  rejectGroupedInner(inner, "EXISTS");
  if (!canExtractCorrelation(inner, scope, "EXISTS", true)) {
    return decorrelateExistsWithProbes(block, exists, scope, nextAlias);
  }
  const keys = extractCorrelation(inner, scope, "EXISTS", true);
  if (keys.every(({ operator }) => operator === "=")) {
    const flagsAlias = nextAlias();
    const flags: CompiledQuery = {
      sql: "(nested correlated exists flags)",
      base: inner.base,
      joins: inner.joins,
      select: [
        ...keys.map((key, index) => ({
          expression: key.inner,
          alias: correlationKeyAlias(index),
        })),
        { expression: { kind: "literal", value: 1 }, alias: correlationMarkerAlias },
      ],
      predicates: inner.predicates,
      // EXISTS only needs one flag per correlation tuple. This is the direct equality-key path
      // used by auth scopes such as `all_access OR EXISTS (...)` and cannot multiply outer rows.
      groupBy: keys.map((key) => key.inner),
      having: [],
      orderBy: [],
    };
    pushCorrelationJoin(block, flagsAlias, flags, keys, "left");
    return {
      kind: "condition",
      operator: exists.negated ? "IS NULL" : "IS NOT NULL",
      left: { kind: "column", reference: `${flagsAlias}.${correlationMarkerAlias}` },
      right: { kind: "literal", value: null },
    };
  }
  const probes = outerProbeBlock(block, keys, nextAlias.resolvedNames);
  const probesAlias = nextAlias();
  const innerRowsAlias = nextAlias();
  const innerRows: CompiledQuery = {
    sql: "(nested correlated exists rows)",
    base: inner.base,
    joins: inner.joins,
    select: keys.map((key, index) => ({
      expression: key.inner,
      alias: correlationKeyAlias(index),
    })),
    predicates: inner.predicates,
    groupBy: keys.map((key) => key.inner),
    having: [],
    orderBy: [],
  };
  const probeReferences = keys.map((_, index): Expression => ({
    kind: "column",
    reference: `${probesAlias}.${correlationProbeAlias(index)}`,
  }));
  const flags: CompiledQuery = {
    sql: "(nested correlated exists flags)",
    base: { table: probesAlias, alias: probesAlias, derived: probes },
    joins: [
      generalCorrelationJoin(
        innerRowsAlias,
        innerRows,
        keys.map((_, index): Expression => ({
          kind: "column",
          reference: `${innerRowsAlias}.${correlationKeyAlias(index)}`,
        })),
        probeReferences,
        keys.map(({ operator }) => operator),
      ),
    ],
    select: [
      ...probeReferences.map((expression, index) => ({
        expression,
        alias: correlationKeyAlias(index),
      })),
      { expression: { kind: "literal", value: 1 }, alias: correlationMarkerAlias },
    ],
    predicates: [],
    groupBy: probeReferences,
    having: [],
    orderBy: [],
  };
  const flagsAlias = nextAlias();
  pushCorrelationJoin(
    block,
    flagsAlias,
    flags,
    keys.map((key) => ({ ...key, operator: "=" })),
    "left",
  );
  return {
    kind: "condition",
    operator: exists.negated ? "IS NULL" : "IS NOT NULL",
    left: { kind: "column", reference: `${flagsAlias}.${correlationMarkerAlias}` },
    right: { kind: "literal", value: null },
  };
}

/**
 * Decorrelation fallback for an EXISTS whose outer values occur below another subquery or in a
 * larger expression instead of as direct inner-column/outer-column predicates. Distinct outer
 * probe tuples become an ordinary derived source of the EXISTS body, and every outer reference
 * is rebound to that source. The whole nested tree can then use the normal recursive rewrites.
 */
function decorrelateExistsWithProbes(
  block: CompiledQuery,
  exists: Extract<Expression, { kind: "exists" }>,
  scope: ReadonlySet<string>,
  nextAlias: NextAlias,
): Expression {
  const inner = exists.block;
  const references = probeOuterReferences(inner, [], scope, "EXISTS");

  const outerExpressions = references.map<Expression>((reference) => ({
    kind: "column",
    reference,
  }));
  const probes = outerProbeBlockForExpressions(block, outerExpressions, nextAlias.resolvedNames);
  const probesAlias = nextAlias();
  const probeReferences = references.map<Expression>((_, index) => ({
    kind: "column",
    reference: `${probesAlias}.${correlationProbeAlias(index)}`,
  }));
  replacePlanReferences(
    inner,
    new Map(references.map((reference, index) => [reference, probeReferences[index] ?? null])),
  );
  inner.joins.push(crossJoinPlan({ table: probesAlias, alias: probesAlias, derived: probes }));

  const flagsAlias = nextAlias();
  const flags: CompiledQuery = {
    sql: "(probe-lifted correlated exists flags)",
    base: inner.base,
    joins: inner.joins,
    select: [
      ...probeReferences.map((expression, index) => ({
        expression,
        alias: correlationKeyAlias(index),
      })),
      { expression: { kind: "literal", value: 1 }, alias: correlationMarkerAlias },
    ],
    predicates: inner.predicates,
    groupBy: probeReferences,
    having: [],
    orderBy: [],
  };
  const flagKeys = outerExpressions.map<Expression>((_, index) => ({
    kind: "column",
    reference: `${flagsAlias}.${correlationKeyAlias(index)}`,
  }));
  const probeConditions = outerExpressions.map<Expression>((outer, index) => ({
    kind: "condition",
    operator: "IS NOT DISTINCT FROM",
    left: outer,
    right: flagKeys[index] ?? { kind: "literal", value: null },
  }));
  let probeJoin = probeConditions[0];
  if (probeJoin === undefined) throw new Error("Correlation probe join is missing its key");
  for (const condition of probeConditions.slice(1)) {
    probeJoin = {
      kind: "logical",
      operator: "and",
      left: probeJoin,
      right: condition,
    };
  }
  block.joins.push({
    table: flagsAlias,
    alias: flagsAlias,
    derived: flags,
    kind: "left",
    left: { kind: "literal", value: null },
    right: { kind: "literal", value: null },
    on: probeJoin,
  });
  return {
    kind: "condition",
    operator: exists.negated ? "IS NULL" : "IS NOT NULL",
    left: { kind: "column", reference: `${flagsAlias}.${correlationMarkerAlias}` },
    right: { kind: "literal", value: null },
  };
}

/** Rebinds qualified references everywhere below one plan without touching the probe definition. */
function replaceExpressionReferences(
  expression: Expression,
  replacements: ReadonlyMap<string, Expression | null>,
): Expression {
  const rewrite = (expression: Expression): Expression => {
    if (expression.kind === "column") {
      const replacement = replacements.get(expression.reference);
      return replacement == null ? expression : structuredClone(replacement);
    }
    if (expression.kind === "subquery" || expression.kind === "exists") {
      replacePlanReferences(expression.block, replacements);
      return expression;
    }
    if (expression.kind === "window") {
      expression.partitionBy = expression.partitionBy.map(rewrite);
      for (const order of expression.orderBy) order.expression = rewrite(order.expression);
      if (expression.argument !== undefined) expression.argument = rewrite(expression.argument);
      return expression;
    }
    return mapChildExpressions(expression, rewrite);
  };
  return rewrite(expression);
}

/** Rebinds qualified references everywhere below one plan without touching the probe definition. */
function replacePlanReferences(
  block: CompiledQuery,
  replacements: ReadonlyMap<string, Expression | null>,
): void {
  mapBlockExpressions(block, (expression) => replaceExpressionReferences(expression, replacements));
  forEachNestedBlock(block, (nested) => replacePlanReferences(nested, replacements));
}

/** Distinct outer key tuples, preserving the FROM joins needed by every referenced source. */
function outerProbeBlock(
  block: CompiledQuery,
  keys: CorrelationKey[],
  resolvedNames: boolean,
): CompiledQuery {
  return outerProbeBlockForExpressions(
    block,
    keys.map((key) => key.outer),
    resolvedNames,
  );
}

function outerProbeBlockForExpressions(
  block: CompiledQuery,
  outerExpressions: readonly Expression[],
  resolvedNames: boolean,
): CompiledQuery {
  const sources: TableSource[] = [block.base, ...block.joins];
  const sourceIndexes = outerExpressions.flatMap((expression) => {
    const aliases = [...expressionAliases(expression)];
    // An unqualified expression can resolve against any visible source. Retaining the whole
    // prefix is conservative and preserves SQL name resolution without a catalog dependency.
    if (aliases.length === 0) return [sources.length - 1];
    return aliases.map((alias) => {
      const index = sources.findIndex((source) => source.alias === alias);
      if (index < 0) throw new TypeError(`Cannot resolve correlated outer source: ${alias}`);
      return index;
    });
  });
  const lastSource = Math.max(...sourceIndexes);
  const expressions = outerExpressions.map((expression) => structuredClone(expression));
  const retained = new Set(sources.slice(0, lastSource + 1).map((source) => source.alias));
  // An unqualified name resolves the same way only if the probe keeps every source — and only
  // once names are resolved, since before that it might belong to an enclosing query.
  const retainsAll = resolvedNames && lastSource === sources.length - 1;
  return {
    sql: "(correlation probes)",
    base: structuredClone(block.base),
    joins: structuredClone(block.joins.slice(0, lastSource)),
    select: expressions.map((expression, index) => ({
      expression,
      alias: correlationProbeAlias(index),
    })),
    predicates: probePredicates(block, retained, retainsAll),
    groupBy: expressions,
    having: [],
    orderBy: [],
  };
}

/**
 * The outer WHERE conjuncts a probe block applies to itself. The probe replicates the outer
 * FROM prefix its keys need, so a conjunct that reads only that prefix decides the same rows
 * there as in the outer block: a row it rejects never needs a probe (the join back is LEFT on
 * the probe key, so a missing probe is unobservable), and without it every outer key tuple in
 * the table would be probed — 200,000 groups and a 200,000-row join for a query that keeps 100
 * outer rows. What must stay out: any reference beyond the prefix (unqualified names too,
 * unless the whole FROM is retained), subqueries and EXISTS (they may be the next correlation
 * to rewrite), aggregates and windows, full-text documents (defined against their own scan),
 * and calls that must run once per statement rather than once more inside a derived block —
 * volatile functions, sequence reads, and the statement clock.
 */
function probePredicates(
  block: CompiledQuery,
  retained: ReadonlySet<string>,
  retainsAll: boolean,
): Predicate[] {
  return block.predicates
    .filter(
      (predicate) =>
        probeSafeExpression(predicate.left, retained, retainsAll) &&
        probeSafeExpression(predicate.right, retained, retainsAll),
    )
    .map((predicate) => structuredClone(predicate));
}

/**
 * Calls that must run once per statement and never again inside a derived block. Decided per
 * call rather than as a module-level set: this module and query.ts import each other, and
 * query.ts's tables are not initialized yet when this module's top level runs.
 */
function isStatementScopedCall(name: string): boolean {
  return (
    volatileScalarFunctionNames.has(name) ||
    statementDatetimeNames.has(name) ||
    name === "NEXTVAL" ||
    name === "CURRVAL"
  );
}

function probeSafeExpression(
  expression: Expression,
  retained: ReadonlySet<string>,
  retainsAll: boolean,
): boolean {
  switch (expression.kind) {
    case "literal":
    case "parameter":
      return true;
    case "column": {
      const dot = expression.reference.indexOf(".");
      return dot < 0 ? retainsAll : retained.has(expression.reference.slice(0, dot));
    }
    case "wildcard":
    case "subquery":
    case "exists":
    case "window":
    case "fts":
      return false;
    case "call":
      if (!scalarFunctionNames.has(expression.name) || isStatementScopedCall(expression.name)) {
        return false;
      }
      return expression.arguments.every((argument) =>
        probeSafeExpression(argument, retained, retainsAll),
      );
    default:
      return childExpressions(expression).every((child) =>
        probeSafeExpression(child, retained, retainsAll),
      );
  }
}

/**
 * Restates predicates over outer key columns in terms of the matching inner key expressions.
 * Only equality keys whose outer side is a bare column qualify, and a predicate that reads any
 * other column is dropped rather than half-translated. Rows the inner side loses this way are
 * exactly the ones no surviving probe could have matched.
 */
function mirrorPredicatesThroughKeys(
  predicates: readonly Predicate[],
  keys: readonly CorrelationKey[],
): Predicate[] {
  const substitutions = new Map<string, Expression>();
  for (const key of keys) {
    if (key.operator === "=" && key.outer.kind === "column") {
      substitutions.set(key.outer.reference, key.inner);
    }
  }
  if (substitutions.size === 0) return [];
  const mirrored: Predicate[] = [];
  for (const predicate of predicates) {
    const left = substituteColumns(predicate.left, substitutions);
    if (left === undefined) continue;
    const right = substituteColumns(predicate.right, substitutions);
    if (right === undefined) continue;
    mirrored.push({ ...predicate, left, right });
  }
  return mirrored;
}

function substituteColumns(
  expression: Expression,
  substitutions: ReadonlyMap<string, Expression>,
): Expression | undefined {
  if (expression.kind === "column") {
    const replacement = substitutions.get(expression.reference);
    return replacement === undefined ? undefined : structuredClone(replacement);
  }
  if (expression.kind === "literal" || expression.kind === "parameter") {
    return structuredClone(expression);
  }
  if (
    expression.kind === "wildcard" ||
    expression.kind === "subquery" ||
    expression.kind === "exists" ||
    expression.kind === "window" ||
    expression.kind === "fts"
  ) {
    return undefined;
  }
  const progress = { complete: true };
  const mapped = mapChildExpressions(structuredClone(expression), (child) => {
    const substituted = substituteColumns(child, substitutions);
    if (substituted === undefined) {
      progress.complete = false;
      return child;
    }
    return substituted;
  });
  return progress.complete ? mapped : undefined;
}

/** Joins one row per outer probe tuple back with NULL-safe equality. */
function pushNullSafeProbeJoin(
  block: CompiledQuery,
  alias: string,
  derived: CompiledQuery,
  outerExpressions: readonly Expression[],
): void {
  const conditions = outerExpressions.map<Expression>((outer, index) => ({
    kind: "condition",
    operator: "IS NOT DISTINCT FROM",
    left: structuredClone(outer),
    right: { kind: "column", reference: `${alias}.${correlationKeyAlias(index)}` },
  }));
  let on = conditions[0];
  if (on === undefined) throw new Error("Correlation probe join is missing its key");
  for (const condition of conditions.slice(1)) {
    on = { kind: "logical", operator: "and", left: on, right: condition };
  }
  block.joins.push({
    table: alias,
    alias,
    derived,
    kind: "left",
    left: { kind: "literal", value: null },
    right: { kind: "literal", value: null },
    on,
  });
}

/** A general ON join carrying one comparison for each corresponding expression pair. */
function generalCorrelationJoin(
  alias: string,
  derived: CompiledQuery,
  inner: Expression[],
  outer: Expression[],
  operators: ComparisonOperator[],
): JoinPlan {
  let on: Expression | undefined;
  inner.forEach((left, index) => {
    const right = outer[index];
    const operator = operators[index];
    if (right === undefined || operator === undefined) return;
    const comparison: Expression = { kind: "condition", operator, left, right };
    on =
      on === undefined
        ? comparison
        : { kind: "logical", operator: "and", left: on, right: comparison };
  });
  return {
    table: alias,
    alias,
    derived,
    kind: "inner",
    left: { kind: "literal", value: null },
    right: { kind: "literal", value: null },
    ...(on === undefined ? {} : { on }),
  };
}

function pushCorrelationJoin(
  block: CompiledQuery,
  alias: string,
  derived: CompiledQuery,
  keys: CorrelationKey[],
  kind: JoinPlan["kind"],
): void {
  const source = { table: alias, alias, derived };
  const keyReference = (index: number): Expression => ({
    kind: "column",
    reference: `${alias}.${correlationKeyAlias(index)}`,
  });
  const first = keys[0];
  if (keys.length === 1 && first?.operator === "=") {
    block.joins.push({ ...source, kind, left: first.outer, right: keyReference(0) });
    return;
  }
  let on: Expression | undefined;
  keys.forEach((key, index) => {
    const comparison: Expression = {
      kind: "condition",
      operator: key.operator,
      left: keyReference(index),
      right: key.outer,
    };
    on =
      on === undefined
        ? comparison
        : { kind: "logical", operator: "and", left: on, right: comparison };
  });
  block.joins.push({
    ...source,
    kind,
    left: { kind: "literal", value: null },
    right: { kind: "literal", value: null },
    ...(on === undefined ? {} : { on }),
  });
}

function blockReferencesOutside(block: CompiledQuery): boolean {
  const refs: string[] = [];
  collectOutsideReferences(block, new Set(), refs);
  return refs.length > 0;
}

/** Collects qualified references in one expression whose alias no available scope defines. */
function collectExpressionOutsideReferences(
  expression: Expression,
  scope: ReadonlySet<string>,
  refs: string[],
): void {
  const visit = (expression: Expression): void => {
    if (expression.kind === "column") {
      const parts = expression.reference.split(".");
      const alias = parts[0];
      if (parts.length === 2 && alias !== undefined && !scope.has(alias)) {
        refs.push(expression.reference);
      }
      return;
    }
    if (expression.kind === "subquery" || expression.kind === "exists") {
      collectOutsideReferences(expression.block, scope, refs);
      return;
    }
    for (const child of childExpressions(expression)) visit(child);
  };
  visit(expression);
}

/** Collects qualified column references whose alias no enclosing scope inside `block` defines. */
function collectOutsideReferences(
  block: CompiledQuery,
  outer: ReadonlySet<string>,
  refs: string[],
): void {
  const scope = new Set([...outer, block.base.alias, ...block.joins.map((join) => join.alias)]);
  forEachBlockExpression(block, (expression) =>
    collectExpressionOutsideReferences(expression, scope, refs),
  );
  forEachNestedBlock(block, (nested) => {
    collectOutsideReferences(nested, scope, refs);
  });
}

/** Returns every available qualified outer reference used by a correlated block. */
function probeOuterReferences(
  block: CompiledQuery,
  additional: readonly string[],
  scope: ReadonlySet<string>,
  label: string,
): string[] {
  const outside: string[] = [];
  collectOutsideReferences(block, new Set(), outside);
  const references = [...new Set([...outside, ...additional])];
  if (references.length === 0) {
    throw new TypeError(`Correlated ${label} subquery has no usable outer reference`);
  }
  for (const reference of references) {
    const parts = reference.split(".");
    const alias = parts.length === 2 ? parts[0] : undefined;
    if (alias === undefined || !scope.has(alias)) {
      throw new TypeError(
        `Correlated ${label} references an unavailable outer source: ${reference}`,
      );
    }
  }
  return references;
}

function extractCorrelation(
  inner: CompiledQuery,
  outerScope: ReadonlySet<string>,
  label: string,
  comparisons = false,
): CorrelationKey[] {
  const keys = takeCorrelationPredicates(inner, outerScope, comparisons);
  const leftover: string[] = [];
  collectOutsideReferences(inner, new Set(), leftover);
  if (leftover.length > 0) {
    const supported = comparisons ? "comparison" : "equality";
    throw new TypeError(
      `Correlated ${label} subqueries support only ${supported} conditions between one inner and one outer qualified column (unsupported reference: ${leftover[0] ?? ""})`,
    );
  }
  if (keys.length === 0) {
    throw new TypeError(`Correlated ${label} subquery has no usable correlation condition`);
  }
  return keys;
}

/** Removes the simple inner/outer comparisons that can become probe-join conditions. */
function takeCorrelationPredicates(
  inner: CompiledQuery,
  outerScope: ReadonlySet<string>,
  comparisons: boolean,
): CorrelationKey[] {
  const innerScope = new Set([inner.base.alias, ...inner.joins.map((join) => join.alias)]);
  const keys: CorrelationKey[] = [];
  const kept: Predicate[] = [];
  for (const predicate of inner.predicates) {
    const key = correlationComparison(predicate, innerScope, outerScope, comparisons);
    if (key === undefined) kept.push(predicate);
    else keys.push(key);
  }
  inner.predicates = kept;
  return keys;
}

/** Whether the fast correlation-key path can consume every outside reference in this block. */
function canExtractCorrelation(
  inner: CompiledQuery,
  outerScope: ReadonlySet<string>,
  label: string,
  comparisons: boolean,
): boolean {
  try {
    extractCorrelation(structuredClone(inner), outerScope, label, comparisons);
    return true;
  } catch (error) {
    if (error instanceof TypeError) return false;
    throw error;
  }
}

function correlationComparison(
  predicate: Predicate,
  innerScope: ReadonlySet<string>,
  outerScope: ReadonlySet<string>,
  comparisons: boolean,
): CorrelationKey | undefined {
  if (
    predicate.operator !== "=" &&
    (!comparisons || !comparisonOperators.has(predicate.operator))
  ) {
    return undefined;
  }
  const operator = predicate.operator as ComparisonOperator;
  const { left, right } = predicate;
  if (left.kind !== "column" || right.kind !== "column") return undefined;
  const sideOf = (reference: string): "inner" | "outer" | undefined => {
    const parts = reference.split(".");
    // An unqualified reference resolves inside the subquery, as it always has.
    if (parts.length !== 2) return "inner";
    const alias = parts[0] ?? "";
    if (innerScope.has(alias)) return "inner";
    if (outerScope.has(alias)) return "outer";
    return undefined;
  };
  const leftSide = sideOf(left.reference);
  const rightSide = sideOf(right.reference);
  if (leftSide === "inner" && rightSide === "outer") return { inner: left, outer: right, operator };
  if (leftSide === "outer" && rightSide === "inner") {
    return { inner: right, outer: left, operator: reverseComparison(operator) };
  }
  return undefined;
}

function reverseComparison(operator: ComparisonOperator): ComparisonOperator {
  switch (operator) {
    case ">":
      return "<";
    case ">=":
      return "<=";
    case "<":
      return ">";
    case "<=":
      return ">=";
    default:
      return operator;
  }
}

/**
 * `col IN (SELECT value …)` with no correlation is an inner join to the subquery's distinct
 * values: the row survives exactly when a non-null equal value exists, which is what the
 * membership test answers as a WHERE conjunct (a NULL probe and an empty subquery both reject
 * the row either way). The subquery block is kept whole as a derived source — its own grouping,
 * ordering, and limit stay in force — and only its one output is grouped for the join. A
 * constant on the probe column then reaches the subquery's own scan through join-key mirroring
 * and pushdown, instead of the subquery materializing every value before the first row is
 * tested.
 */
function rewriteUncorrelatedIn(
  block: CompiledQuery,
  predicate: Predicate,
  inner: CompiledQuery,
  nextAlias: NextAlias,
): "consumed" | undefined {
  const item = inner.select[0];
  if (
    predicate.left.kind !== "column" ||
    inner.select.length !== 1 ||
    item === undefined ||
    item.expression.kind === "wildcard" ||
    item.alias.includes(".")
  ) {
    return undefined;
  }
  const valuesAlias = nextAlias();
  const alias = nextAlias();
  const value: Expression = { kind: "column", reference: `${valuesAlias}.${item.alias}` };
  const derived: CompiledQuery = {
    sql: "(in subquery values)",
    base: { table: valuesAlias, alias: valuesAlias, derived: inner },
    joins: [],
    select: [{ expression: value, alias: correlationValueAlias }],
    predicates: [],
    groupBy: [structuredClone(value)],
    having: [],
    orderBy: [],
  };
  block.joins.push({
    table: alias,
    alias,
    derived,
    kind: "inner",
    left: structuredClone(predicate.left),
    right: { kind: "column", reference: `${alias}.${correlationValueAlias}` },
  });
  return "consumed";
}

function rejectGroupedInner(inner: CompiledQuery, label: string): void {
  if (inner.groupBy.length > 0 || inner.having.length > 0) {
    throw new TypeError(`Correlated ${label} subqueries cannot use GROUP BY or HAVING`);
  }
}

function assertNoCorrelation(block: CompiledQuery): void {
  const visit = (expression: Expression): void => {
    if (expression.kind === "subquery" || expression.kind === "exists") {
      const refs: string[] = [];
      collectOutsideReferences(expression.block, new Set(), refs);
      if (refs.length > 0) {
        throw new TypeError(
          `Correlated subqueries are supported only as top-level WHERE conditions (reference: ${refs[0] ?? ""})`,
        );
      }
      return;
    }
    for (const child of childExpressions(expression)) visit(child);
  };
  forEachBlockExpression(block, visit);
}

// --- Constant folding ---------------------------------------------------------------------------

function foldBlockConstants(block: CompiledQuery): void {
  for (const item of block.select) item.expression = foldExpression(item.expression);
  block.groupBy = block.groupBy.map(foldExpression);
  for (const predicate of [...block.predicates, ...block.having]) {
    predicate.left = foldExpression(predicate.left);
    predicate.right = foldExpression(predicate.right);
  }
  for (const order of block.orderBy) order.expression = foldExpression(order.expression);
}

function foldExpression(expression: Expression): Expression {
  if (expression.kind === "binary") {
    const left = foldExpression(expression.left);
    const right = foldExpression(expression.right);
    if (left.kind === "literal" && right.kind === "literal") {
      const folded = foldBinary(expression.operator, left.value, right.value);
      if (folded !== undefined) {
        const domain =
          left.sqlDomain?.kind === "numeric" || right.sqlDomain?.kind === "numeric"
            ? ({ kind: "numeric" } as const)
            : undefined;
        return {
          kind: "literal",
          value: folded,
          ...(isSqlDomainValue(folded) ? { internalSqlValue: true as const } : {}),
          ...(domain === undefined ? {} : { sqlDomain: domain }),
        };
      }
    }
    return { ...expression, left, right };
  }
  if (expression.kind === "call") {
    const foldedArguments = expression.arguments.map(foldExpression);
    const literalValues = foldedArguments.flatMap((argument) =>
      argument.kind === "literal" ? [argument.value] : [],
    );
    if (
      expression.name !== "COALESCE" &&
      !volatileScalarFunctionNames.has(expression.name) &&
      isScalarFunctionName(expression.name) &&
      literalValues.length === foldedArguments.length
    ) {
      try {
        const folded = scalarFunctionValue(expression.name, literalValues);
        if (
          folded === null ||
          typeof folded === "string" ||
          typeof folded === "boolean" ||
          folded instanceof Date ||
          (typeof folded === "number" && Number.isFinite(folded))
        ) {
          const target = foldedArguments[1];
          const targetWord =
            expression.name === "CAST" &&
            target?.kind === "literal" &&
            typeof target.value === "string"
              ? target.value
              : undefined;
          const sqlDomain =
            targetWord?.startsWith("numeric:") === true
              ? (() => {
                  const [, precisionWord = "", scaleWord = ""] = targetWord.split(":");
                  return {
                    kind: "numeric" as const,
                    ...(precisionWord === "" ? {} : { precision: Number(precisionWord) }),
                    ...(scaleWord === "" ? {} : { scale: Number(scaleWord) }),
                  };
                })()
              : targetWord === "json" ||
                  targetWord === "jsonb" ||
                  targetWord === "uuid" ||
                  targetWord === "date" ||
                  targetWord === "time" ||
                  targetWord === "interval"
                ? ({ kind: targetWord } as const)
                : expression.name === "JSON_QUERY" ||
                    expression.name === "JSON_OBJECT" ||
                    expression.name === "JSON_ARRAY" ||
                    expression.name === "MINNOW_JSON_GET"
                  ? ({ kind: "json" } as const)
                  : simpleScalarFunctions.get(expression.name)?.returns === "date"
                    ? ({ kind: "date" } as const)
                    : simpleScalarFunctions.get(expression.name)?.returns === "interval"
                      ? ({ kind: "interval" } as const)
                      : undefined;
          return {
            kind: "literal",
            value: folded,
            ...(isSqlDomainValue(folded) ? { internalSqlValue: true as const } : {}),
            ...(sqlDomain === undefined ? {} : { sqlDomain }),
          };
        }
      } catch {
        // A function that rejects its constant arguments folds nowhere; execution reports it.
      }
    }
    return { ...expression, arguments: foldedArguments };
  }
  if (expression.kind === "list") {
    return { ...expression, items: expression.items.map(foldExpression) };
  }
  if (expression.kind === "window") {
    return {
      ...expression,
      partitionBy: expression.partitionBy.map(foldExpression),
      orderBy: expression.orderBy.map((order) => ({
        ...order,
        expression: foldExpression(order.expression),
      })),
      ...(expression.argument === undefined
        ? {}
        : { argument: foldExpression(expression.argument) }),
    };
  }
  if (expression.kind === "condition" || expression.kind === "logical") {
    return {
      ...expression,
      left: foldExpression(expression.left),
      right: foldExpression(expression.right),
    };
  }
  if (expression.kind === "not") {
    return { ...expression, operand: foldExpression(expression.operand) };
  }
  if (expression.kind === "case") {
    const otherwise =
      expression.otherwise === undefined ? undefined : foldExpression(expression.otherwise);
    return {
      ...expression,
      branches: expression.branches.map((branch) => ({
        when: foldExpression(branch.when),
        then: foldExpression(branch.then),
      })),
      ...(otherwise === undefined ? {} : { otherwise }),
    };
  }
  return expression;
}

/** Folds only when the result stays a finite SQL value, preserving runtime semantics otherwise. */
function foldBinary(
  operator: BinaryOperator,
  leftValue: QueryValue,
  rightValue: QueryValue,
): QueryValue | undefined {
  if (leftValue === null || rightValue === null) return null;
  if (operator === "||") {
    if (typeof leftValue === "string" && typeof rightValue === "string") {
      // Same helper the executors use, so folding cannot concatenate internal domain
      // encodings. A refused shape stays unfolded and raises the same error at execution.
      try {
        return concatenatedSqlValue(leftValue, rightValue);
      } catch {
        return undefined;
      }
    }
    return undefined;
  }
  if (typeof leftValue === "boolean" || typeof rightValue === "boolean") return undefined;
  if (typeof leftValue === "string" || typeof rightValue === "string") return undefined;
  const left = leftValue instanceof Date ? dateMilliseconds(leftValue) : leftValue;
  const right = rightValue instanceof Date ? dateMilliseconds(rightValue) : rightValue;
  if ((operator === "/" || operator === "%") && right === 0) return null;
  const result =
    operator === "+"
      ? left + right
      : operator === "-"
        ? left - right
        : operator === "*"
          ? left * right
          : operator === "%"
            ? left % right
            : left / right;
  return Number.isFinite(result) ? result : undefined;
}

// --- Predicate pushdown into derived sources ----------------------------------------------------

function pushPredicatesIntoDerived(block: CompiledQuery): void {
  // A left join's null-extended rows must still meet the outer WHERE, so only the base source
  // and inner-join sources accept pushed predicates.
  const sources = [block.base, ...block.joins.filter((join) => join.kind === "inner")];
  const singleSource = block.joins.length === 0;
  const remaining: Predicate[] = [];
  for (const predicate of block.predicates) {
    const target = pushdownTarget(predicate, sources, singleSource);
    if (target === undefined) {
      remaining.push(predicate);
      continue;
    }
    target.derived.predicates.push(target.rewritten);
    // The derived block was optimized before this predicate arrived; let it travel on to a
    // nested derived source when one exposes the column it names.
    pushPredicatesIntoDerived(target.derived);
  }
  block.predicates = remaining;
}

function pushdownTarget(
  predicate: Predicate,
  sources: readonly TableSource[],
  singleSource: boolean,
): { derived: CompiledQuery; rewritten: Predicate } | undefined {
  for (const source of sources) {
    const derived = source.derived;
    // A filter pushed below LIMIT or OFFSET — literal or placeholder — would change which rows
    // the window keeps, so a paged block only ever receives the predicate above it.
    if (derived === undefined || blockHasRowWindow(derived)) continue;
    const left = rewriteForInner(predicate.left, source, derived, singleSource);
    if (left === undefined) continue;
    const right = rewriteForInner(predicate.right, source, derived, singleSource);
    if (right === undefined) continue;
    return {
      derived,
      rewritten: { left, operator: predicate.operator, right },
    };
  }
  return undefined;
}

/**
 * Rewrites an outer predicate side into the derived block's own scope, or returns undefined when
 * the side cannot be proven to reference only this source's pushable outputs. A column maps to
 * the inner select expression it names; for a grouped inner block only group-key outputs are
 * pushable, because filtering other outputs before aggregation changes group contents.
 */
function rewriteForInner(
  expression: Expression,
  source: TableSource,
  derived: CompiledQuery,
  singleSource: boolean,
): Expression | undefined {
  if (expression.kind === "literal") return expression;
  if (expression.kind === "subquery") return expression;
  if (expression.kind === "wildcard" || expression.kind === "window") return undefined;
  if (expression.kind === "column") {
    const parts = expression.reference.split(".");
    if (parts.length === 2 && parts[0] !== source.alias) return undefined;
    if (parts.length === 1 && !singleSource) return undefined;
    const name = parts.length === 2 ? parts[1] : parts[0];
    const item = derived.select.find(({ alias }) => alias === name);
    if (item === undefined || item.expression.kind === "wildcard") return undefined;
    if (containsAggregateOrWindow(item.expression)) return undefined;
    if (derived.groupBy.length > 0) {
      const signature = JSON.stringify(item.expression);
      if (!derived.groupBy.some((group) => JSON.stringify(group) === signature)) {
        return undefined;
      }
    }
    return structuredClone(item.expression);
  }
  if (expression.kind === "binary") {
    const left = rewriteForInner(expression.left, source, derived, singleSource);
    if (left === undefined) return undefined;
    const right = rewriteForInner(expression.right, source, derived, singleSource);
    if (right === undefined) return undefined;
    return { ...expression, left, right };
  }
  if (expression.kind === "call") {
    if (containsAggregateOrWindow(expression)) return undefined;
    const rewrittenArguments: Expression[] = [];
    for (const argument of expression.arguments) {
      const rewritten = rewriteForInner(argument, source, derived, singleSource);
      if (rewritten === undefined) return undefined;
      rewrittenArguments.push(rewritten);
    }
    return { ...expression, arguments: rewrittenArguments };
  }
  if (expression.kind === "list") {
    const rewrittenItems: Expression[] = [];
    for (const item of expression.items) {
      const rewritten = rewriteForInner(item, source, derived, singleSource);
      if (rewritten === undefined) return undefined;
      rewrittenItems.push(rewritten);
    }
    return { ...expression, items: rewrittenItems };
  }
  if (expression.kind === "condition" || expression.kind === "logical") {
    const left = rewriteForInner(expression.left, source, derived, singleSource);
    if (left === undefined) return undefined;
    const right = rewriteForInner(expression.right, source, derived, singleSource);
    if (right === undefined) return undefined;
    return { ...expression, left, right };
  }
  if (expression.kind === "not") {
    const operand = rewriteForInner(expression.operand, source, derived, singleSource);
    return operand === undefined ? undefined : { ...expression, operand };
  }
  if (expression.kind === "case") {
    const branches: Array<{ when: Expression; then: Expression }> = [];
    for (const branch of expression.branches) {
      const when = rewriteForInner(branch.when, source, derived, singleSource);
      const then = rewriteForInner(branch.then, source, derived, singleSource);
      if (when === undefined || then === undefined) return undefined;
      branches.push({ when, then });
    }
    let otherwise: Expression | undefined;
    if (expression.otherwise !== undefined) {
      otherwise = rewriteForInner(expression.otherwise, source, derived, singleSource);
      if (otherwise === undefined) return undefined;
    }
    return { ...expression, branches, ...(otherwise === undefined ? {} : { otherwise }) };
  }
  // A full-text document is defined against its own scan source; never push it inward.
  if (expression.kind === "fts") return undefined;
  // An EXISTS block (the only remaining kind) is self-contained, so it moves unchanged.
  return expression;
}

function containsAggregateOrWindow(expression: Expression): boolean {
  if (expression.kind === "window") return true;
  if (expression.kind === "call") {
    if (!scalarFunctionNames.has(expression.name)) return true;
    return expression.arguments.some(containsAggregateOrWindow);
  }
  if (expression.kind === "binary") {
    return (
      containsAggregateOrWindow(expression.left) || containsAggregateOrWindow(expression.right)
    );
  }
  if (expression.kind === "list") return expression.items.some(containsAggregateOrWindow);
  if (expression.kind === "condition" || expression.kind === "logical") {
    return (
      containsAggregateOrWindow(expression.left) || containsAggregateOrWindow(expression.right)
    );
  }
  if (expression.kind === "not") return containsAggregateOrWindow(expression.operand);
  if (expression.kind === "case") {
    return (
      expression.branches.some(
        (branch) =>
          containsAggregateOrWindow(branch.when) || containsAggregateOrWindow(branch.then),
      ) ||
      (expression.otherwise !== undefined && containsAggregateOrWindow(expression.otherwise))
    );
  }
  return false;
}

// --- Projection pruning into derived sources ----------------------------------------------------

function pruneDerivedProjections(block: CompiledQuery): void {
  const sources = [block.base, ...block.joins];
  const singleSource = sources.length === 1;
  if (block.select.some((item) => item.expression.kind === "wildcard")) return;
  for (const source of sources) {
    const derived = source.derived;
    if (derived === undefined) continue;
    // Grouped or aggregate-bearing blocks define semantics through their select list (DISTINCT
    // desugars into grouping), so only plain projection/filter blocks prune.
    if (derived.groupBy.length > 0) continue;
    if (derived.select.some((item) => containsAggregateOrWindow(item.expression))) continue;
    if (derived.select.some((item) => item.expression.kind === "wildcard")) continue;
    const referenced = referencedOutputAliases(block, source, singleSource);
    if (referenced === undefined) continue;
    // The block's own ORDER BY resolves against its select aliases (including the hidden
    // "(order N)" items the ORDER-BY-expression desugar adds), so those outputs are
    // load-bearing even when the outer block never reads them.
    const orderReferences = new Set<string>();
    for (const order of derived.orderBy) {
      if (order.expression.kind === "column") orderReferences.add(order.expression.reference);
    }
    // An ORDER BY column can name either a select alias or the projected source column
    // itself (`SELECT b AS y ... ORDER BY b`), so both spellings pin the item.
    const kept = derived.select.filter(
      (item) =>
        referenced.has(item.alias) ||
        orderReferences.has(item.alias) ||
        (item.expression.kind === "column" && orderReferences.has(item.expression.reference)),
    );
    if (kept.length === derived.select.length) continue;
    derived.select = kept.length > 0 ? kept : derived.select.slice(0, 1);
  }
}

/** Every output alias of one source the outer block references, or undefined when unprovable. */
function referencedOutputAliases(
  block: CompiledQuery,
  source: TableSource,
  singleSource: boolean,
): Set<string> | undefined {
  const referenced = new Set<string>();
  const state = { provable: true };
  const visit = (expression: Expression): void => {
    if (!state.provable) return;
    if (expression.kind === "column") {
      const parts = expression.reference.split(".");
      if (parts.length === 2) {
        if (parts[0] === source.alias) referenced.add(parts[1] ?? "");
        return;
      }
      if (singleSource) referenced.add(parts[0] ?? "");
      else state.provable = false;
      return;
    }
    if (expression.kind === "binary" || expression.kind === "condition") {
      visit(expression.left);
      visit(expression.right);
    } else if (expression.kind === "logical") {
      visit(expression.left);
      visit(expression.right);
    } else if (expression.kind === "not") visit(expression.operand);
    else if (expression.kind === "case") {
      for (const branch of expression.branches) {
        visit(branch.when);
        visit(branch.then);
      }
      if (expression.otherwise !== undefined) visit(expression.otherwise);
    } else if (expression.kind === "call") {
      expression.arguments.forEach(visit);
      expression.aggregateOrderBy?.forEach((order) => visit(order.expression));
    } else if (expression.kind === "list") expression.items.forEach(visit);
    else if (expression.kind === "fts") {
      // MATCH(*) references every source column, so pruning is unprovable.
      if (expression.columns === "*") state.provable = false;
      else expression.columns.forEach(visit);
    } else if (expression.kind === "window") {
      expression.partitionBy.forEach(visit);
      expression.orderBy.forEach((order) => {
        visit(order.expression);
      });
      if (expression.argument !== undefined) visit(expression.argument);
    }
  };
  for (const item of block.select) visit(item.expression);
  block.groupBy.forEach(visit);
  for (const predicate of [...block.predicates, ...block.having]) {
    visit(predicate.left);
    visit(predicate.right);
  }
  for (const join of block.joins) {
    visit(join.left);
    visit(join.right);
    if (join.on !== undefined) visit(join.on);
  }
  for (const order of block.orderBy) {
    // An ORDER BY reference may name an output alias of the outer block rather than a source
    // column; treating it as a source reference is conservative (it only keeps more columns),
    // except that unqualified multi-source references stay unprovable.
    visit(order.expression);
  }
  return state.provable ? referenced : undefined;
}

// --- LIMIT combining ----------------------------------------------------------------------------

function combineDerivedLimit(block: CompiledQuery): void {
  const derived = block.base.derived;
  if (
    derived === undefined ||
    block.joins.length > 0 ||
    block.predicates.length > 0 ||
    block.groupBy.length > 0 ||
    block.having.length > 0 ||
    block.orderBy.length > 0 ||
    block.limit === undefined ||
    // An OFFSET at either level changes which rows survive, so the limits cannot merge; a
    // placeholder limit or offset is unknown until binding, so those blocks keep their own.
    block.offset !== undefined ||
    block.offsetParameter !== undefined ||
    derived.offset !== undefined ||
    derived.offsetParameter !== undefined ||
    derived.limitParameter !== undefined
  ) {
    return;
  }
  derived.limit = Math.min(derived.limit ?? Number.MAX_SAFE_INTEGER, block.limit);
}

// --- Plan rendering -----------------------------------------------------------------------------

/** Renders a stable, human-readable plan for snapshot tests and explain(). */
export function renderPlan(plan: CompiledQuery): string {
  const lines: string[] = [];
  renderBlock(plan, 0, lines);
  return lines.join("\n");
}

function renderBlock(block: CompiledQuery, depth: number, lines: string[]): void {
  const pad = "  ".repeat(depth);
  lines.push(
    `${pad}select ${block.select
      .map((item) => `${item.alias}: ${renderExpression(item.expression)}`)
      .join(", ")}`,
  );
  renderSource(block.base, "from", depth, lines);
  for (const join of block.joins) {
    renderSource(
      join,
      `${join.kind} join on ${renderExpression(join.left)} = ${renderExpression(join.right)}`,
      depth,
      lines,
    );
  }
  for (const predicate of block.predicates) {
    lines.push(`${pad}  where ${renderPredicate(predicate)}`);
  }
  if (block.groupBy.length > 0) {
    lines.push(`${pad}  group by ${block.groupBy.map(renderExpression).join(", ")}`);
  }
  for (const predicate of block.having) {
    lines.push(`${pad}  having ${renderPredicate(predicate)}`);
  }
  if (block.orderBy.length > 0) {
    lines.push(
      `${pad}  order by ${block.orderBy
        .map((order) => `${renderExpression(order.expression)} ${order.direction}`)
        .join(", ")}`,
    );
  }
  if (block.limit !== undefined) lines.push(`${pad}  limit ${String(block.limit)}`);
  if (block.offset !== undefined) lines.push(`${pad}  offset ${String(block.offset)}`);
}

function renderSource(source: TableSource, label: string, depth: number, lines: string[]): void {
  const pad = "  ".repeat(depth);
  if (source.union !== undefined) {
    lines.push(`${pad}  ${label} union [${source.alias}]`);
    source.union.blocks.forEach((member, index) => {
      if (index > 0) {
        lines.push(`${pad}    ${source.union?.ops[index - 1] ?? "union"}`);
      }
      renderBlock(member, depth + 2, lines);
    });
    return;
  }
  if (source.windowed !== undefined) {
    lines.push(
      `${pad}  ${label} window [${source.windowed.windows
        .map((window) => `${window.alias}: ${window.name}`)
        .join(", ")}]`,
    );
    renderBlock(source.windowed.block, depth + 2, lines);
    return;
  }
  if (source.recursive !== undefined) {
    lines.push(
      `${pad}  ${label} recursive [${source.alias}] ${source.recursive.all ? "union all" : "union"}`,
    );
    renderBlock(source.recursive.base, depth + 2, lines);
    lines.push(`${pad}    step`);
    renderBlock(source.recursive.step, depth + 2, lines);
    return;
  }
  if (source.derived !== undefined) {
    lines.push(`${pad}  ${label} derived [${source.alias}]`);
    renderBlock(source.derived, depth + 2, lines);
    return;
  }
  lines.push(
    `${pad}  ${label} table ${source.table}${source.alias === source.table ? "" : ` as ${source.alias}`}`,
  );
}

function renderPredicate(predicate: Predicate): string {
  return `${renderExpression(predicate.left)} ${predicate.operator} ${renderExpression(predicate.right)}`;
}

function renderExpression(expression: Expression): string {
  if (expression.kind === "literal") {
    const value = expression.value;
    if (value === null) return "null";
    if (value instanceof Date) return `date ${dateIsoString(value)}`;
    if (typeof value === "string") return `'${value}'`;
    return String(value);
  }
  if (expression.kind === "parameter") return `$${String(expression.index + 1)}`;
  if (expression.kind === "column") return expression.reference;
  if (expression.kind === "wildcard") return "*";
  if (expression.kind === "binary") {
    return `(${renderExpression(expression.left)} ${expression.operator} ${renderExpression(expression.right)})`;
  }
  if (expression.kind === "call") {
    return `${expression.name}(${expression.arguments.map(renderExpression).join(", ")})`;
  }
  if (expression.kind === "list") {
    return `(${expression.items.map(renderExpression).join(", ")})`;
  }
  if (expression.kind === "subquery") return "(subquery)";
  if (expression.kind === "condition") {
    return `(${renderExpression(expression.left)} ${expression.operator} ${renderExpression(expression.right)})`;
  }
  if (expression.kind === "logical") {
    return `(${renderExpression(expression.left)} ${expression.operator} ${renderExpression(expression.right)})`;
  }
  if (expression.kind === "not") return `(not ${renderExpression(expression.operand)})`;
  if (expression.kind === "exists") return "exists (subquery)";
  if (expression.kind === "case") {
    return `case [${String(expression.branches.length)} branches]`;
  }
  if (expression.kind === "fts") {
    const columns =
      expression.columns === "*" ? "*" : expression.columns.map(renderExpression).join(", ");
    const name = expression.op === "match" ? "match" : "bm25";
    return `${name}(${columns}) against '${expression.query}'`;
  }
  return `${expression.name}(...) over (...)`;
}
