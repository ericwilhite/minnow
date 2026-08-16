import {
  childExpressions,
  forEachBlockExpression,
  forEachNestedBlock,
  isScalarFunctionName,
  scalarFunctionNames,
  scalarFunctionValue,
  type BinaryOperator,
  type CompiledQuery,
  type Expression,
  type Predicate,
  type PredicateOperator,
  type QueryValue,
  type TableSource,
} from "./query.js";

/**
 * Deterministic plan-to-plan rewrites over the shared compiled representation. Every rule
 * preserves result semantics exactly; rules that cannot prove safety leave the plan unchanged.
 * The optimizer runs before subquery resolution, so subquery blocks optimize recursively and
 * predicates containing subqueries stay self-contained when they move.
 */
export function optimizePlan(plan: CompiledQuery): CompiledQuery {
  const optimized = structuredClone(plan);
  optimizeBlock(optimized);
  return optimized;
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

function optimizeBlock(block: CompiledQuery): void {
  decorrelateBlock(block);
  for (const source of [block.base, ...block.joins]) {
    if (source.derived !== undefined) optimizeBlock(source.derived);
    source.union?.blocks.forEach(optimizeBlock);
    if (source.windowed !== undefined) optimizeBlock(source.windowed.block);
    if (source.recursive !== undefined) {
      optimizeBlock(source.recursive.base);
      optimizeBlock(source.recursive.step);
    }
  }
  foldBlockConstants(block);
  normalizeBooleanPredicates(block);
  coalesceOrEqualityLists(block);
  pushPredicatesIntoDerived(block);
  pruneDerivedProjections(block);
  combineDerivedLimit(block);
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
// A subquery that references the enclosing block's aliases rewrites into a derived-table join:
// EXISTS becomes an inner join against the subquery's distinct correlation keys, NOT EXISTS a
// left join plus IS NULL, IN an inner join carrying the compared value, and a scalar aggregate
// a left join against the grouped aggregate (COUNT wraps in COALESCE for the empty group).
// Only equality correlation on alias-qualified references is supported, and only in top-level
// WHERE conjuncts; every other correlated shape fails with an explicit error. Decorrelation runs
// at compile time, so both executors see plain joins and never a correlated node.

interface CorrelationKey {
  inner: Expression;
  outer: Expression;
}

const comparisonOperators = new Set(["=", "!=", "<>", ">", ">=", "<", "<="]);
const aggregateCallNames = new Set(["COUNT", "SUM", "AVG", "MIN", "MAX"]);

function decorrelateBlock(block: CompiledQuery): void {
  const scope = new Set([block.base.alias, ...block.joins.map((join) => join.alias)]);
  const nextAlias = correlationAliasFactory(scope);
  const rewritten: Predicate[] = [];
  for (const predicate of block.predicates) {
    const replacement = decorrelatePredicate(block, predicate, scope, nextAlias);
    if (replacement === "consumed") continue;
    rewritten.push(replacement ?? predicate);
  }
  block.predicates = rewritten;
  const grouped =
    block.groupBy.length > 0 || block.select.some((item) => containsAggregateCall(item.expression));
  for (const item of block.select) {
    if (!expressionHasCorrelatedSubquery(item.expression)) continue;
    if (grouped) {
      throw new TypeError(
        "Correlated select-list subqueries are not supported with grouping or aggregates",
      );
    }
    item.expression = rewriteCorrelatedScalars(block, item.expression, scope, nextAlias);
  }
  assertNoCorrelation(block);
}

function containsAggregateCall(expression: Expression): boolean {
  if (expression.kind === "call" && aggregateCallNames.has(expression.name)) return true;
  return childExpressions(expression).some(containsAggregateCall);
}

function expressionHasCorrelatedSubquery(expression: Expression): boolean {
  if (expression.kind === "subquery") return blockReferencesOutside(expression.block);
  if (expression.kind === "exists") return false;
  return childExpressions(expression).some(expressionHasCorrelatedSubquery);
}

/** Replaces correlated scalar subqueries inside one select expression with decorrelated joins. */
function rewriteCorrelatedScalars(
  block: CompiledQuery,
  expression: Expression,
  scope: ReadonlySet<string>,
  nextAlias: () => string,
): Expression {
  if (expression.kind === "subquery" && blockReferencesOutside(expression.block)) {
    return decorrelateScalarSubquery(block, expression, scope, nextAlias);
  }
  if (
    expression.kind === "binary" ||
    expression.kind === "condition" ||
    expression.kind === "logical"
  ) {
    expression.left = rewriteCorrelatedScalars(block, expression.left, scope, nextAlias);
    expression.right = rewriteCorrelatedScalars(block, expression.right, scope, nextAlias);
    return expression;
  }
  if (expression.kind === "call") {
    expression.arguments = expression.arguments.map((argument) =>
      rewriteCorrelatedScalars(block, argument, scope, nextAlias),
    );
    return expression;
  }
  if (expression.kind === "not") {
    expression.operand = rewriteCorrelatedScalars(block, expression.operand, scope, nextAlias);
    return expression;
  }
  if (expression.kind === "case") {
    for (const branch of expression.branches) {
      branch.when = rewriteCorrelatedScalars(block, branch.when, scope, nextAlias);
      branch.then = rewriteCorrelatedScalars(block, branch.then, scope, nextAlias);
    }
    if (expression.otherwise !== undefined) {
      expression.otherwise = rewriteCorrelatedScalars(
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

function correlationAliasFactory(scope: ReadonlySet<string>): () => string {
  let sequence = 0;
  return () => {
    for (;;) {
      sequence += 1;
      const alias = `corr_${String(sequence)}`;
      if (!scope.has(alias)) return alias;
    }
  };
}

function decorrelatePredicate(
  block: CompiledQuery,
  predicate: Predicate,
  scope: ReadonlySet<string>,
  nextAlias: () => string,
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
    guardWildcard(block);
    const keys = extractCorrelation(inner, scope, "EXISTS");
    const alias = nextAlias();
    const derived: CompiledQuery = {
      sql: "(correlated exists)",
      base: inner.base,
      joins: inner.joins,
      select: keys.map((key, index) => ({ expression: key.inner, alias: `k${String(index)}` })),
      predicates: inner.predicates,
      // Grouping by the keys deduplicates, so the join multiplies no outer row. The parser's
      // injected LIMIT 1 and any ORDER BY are dropped: existence ignores both.
      groupBy: keys.map((key) => key.inner),
      having: [],
      orderBy: [],
    };
    pushCorrelationJoin(block, alias, derived, keys, negated ? "left" : "inner");
    if (!negated) return "consumed";
    return {
      left: { kind: "column", reference: `${alias}.k0` },
      operator: "IS NULL",
      right: { kind: "literal", value: null },
    };
  }
  // outer IN (correlated subquery)
  if (
    (predicate.operator === "IN" || predicate.operator === "NOT IN") &&
    predicate.right.kind === "subquery"
  ) {
    const inner = predicate.right.block;
    if (!blockReferencesOutside(inner)) return undefined;
    if (predicate.operator === "NOT IN") {
      throw new TypeError("Correlated NOT IN subqueries are not supported; use NOT EXISTS");
    }
    rejectGroupedInner(inner, "IN");
    const item = inner.select[0];
    if (inner.select.length !== 1 || item === undefined || item.expression.kind === "wildcard") {
      throw new TypeError("An IN subquery must select exactly one column");
    }
    guardWildcard(block);
    const keys = extractCorrelation(inner, scope, "IN");
    const alias = nextAlias();
    const derived: CompiledQuery = {
      sql: "(correlated in)",
      base: inner.base,
      joins: inner.joins,
      select: [
        ...keys.map((key, index) => ({ expression: key.inner, alias: `k${String(index)}` })),
        { expression: item.expression, alias: "v" },
      ],
      predicates: inner.predicates,
      groupBy: [...keys.map((key) => key.inner), item.expression],
      having: [],
      orderBy: [],
    };
    pushCorrelationJoin(block, alias, derived, keys, "inner");
    return {
      left: predicate.left,
      operator: "=",
      right: { kind: "column", reference: `${alias}.v` },
    };
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
 * Rewrites one correlated scalar-aggregate subquery into a left join against the grouped
 * aggregate, returning the expression that replaces the subquery node (COUNT wraps in COALESCE
 * so an unmatched group reads 0, matching the empty-subquery semantics).
 */
function decorrelateScalarSubquery(
  block: CompiledQuery,
  subquery: Extract<Expression, { kind: "subquery" }>,
  scope: ReadonlySet<string>,
  nextAlias: () => string,
): Expression {
  const inner = subquery.block;
  const item = inner.select[0];
  const isAggregate =
    item?.expression.kind === "call" && aggregateCallNames.has(item.expression.name);
  if (inner.select.length !== 1 || item === undefined || !isAggregate) {
    throw new TypeError(
      "A correlated scalar subquery must select exactly one aggregate expression",
    );
  }
  if (
    inner.groupBy.length > 0 ||
    inner.having.length > 0 ||
    inner.orderBy.length > 0 ||
    inner.limit !== undefined
  ) {
    throw new TypeError(
      "A correlated scalar subquery cannot use GROUP BY, HAVING, ORDER BY, or LIMIT",
    );
  }
  guardWildcard(block);
  const keys = extractCorrelation(inner, scope, "scalar");
  const alias = nextAlias();
  const derived: CompiledQuery = {
    sql: "(correlated scalar)",
    base: inner.base,
    joins: inner.joins,
    select: [
      ...keys.map((key, index) => ({ expression: key.inner, alias: `k${String(index)}` })),
      { expression: item.expression, alias: "v" },
    ],
    predicates: inner.predicates,
    groupBy: keys.map((key) => key.inner),
    having: [],
    orderBy: [],
  };
  pushCorrelationJoin(block, alias, derived, keys, "left");
  let value: Expression = { kind: "column", reference: `${alias}.v` };
  if (item.expression.kind === "call" && item.expression.name === "COUNT") {
    // COUNT over an empty group is 0, but the unmatched left-join side is NULL.
    value = { kind: "call", name: "COALESCE", arguments: [value, { kind: "literal", value: 0 }] };
  }
  return value;
}

function pushCorrelationJoin(
  block: CompiledQuery,
  alias: string,
  derived: CompiledQuery,
  keys: CorrelationKey[],
  kind: "inner" | "left",
): void {
  const source = { table: alias, alias, derived };
  const keyReference = (index: number): Expression => ({
    kind: "column",
    reference: `${alias}.k${String(index)}`,
  });
  const first = keys[0];
  if (keys.length === 1 && first !== undefined) {
    block.joins.push({ ...source, kind, left: first.outer, right: keyReference(0) });
    return;
  }
  let on: Expression | undefined;
  keys.forEach((key, index) => {
    const equality: Expression = {
      kind: "condition",
      operator: "=",
      left: key.outer,
      right: keyReference(index),
    };
    on =
      on === undefined ? equality : { kind: "logical", operator: "and", left: on, right: equality };
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

/** Collects qualified column references whose alias no enclosing scope inside `block` defines. */
function collectOutsideReferences(
  block: CompiledQuery,
  outer: ReadonlySet<string>,
  refs: string[],
): void {
  const scope = new Set([...outer, block.base.alias, ...block.joins.map((join) => join.alias)]);
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
  forEachBlockExpression(block, visit);
  forEachNestedBlock(block, (nested) => {
    collectOutsideReferences(nested, scope, refs);
  });
}

function extractCorrelation(
  inner: CompiledQuery,
  outerScope: ReadonlySet<string>,
  label: string,
): CorrelationKey[] {
  const innerScope = new Set([inner.base.alias, ...inner.joins.map((join) => join.alias)]);
  const keys: CorrelationKey[] = [];
  const kept: Predicate[] = [];
  for (const predicate of inner.predicates) {
    const key = correlationEquality(predicate, innerScope, outerScope);
    if (key === undefined) kept.push(predicate);
    else keys.push(key);
  }
  inner.predicates = kept;
  const leftover: string[] = [];
  collectOutsideReferences(inner, new Set(), leftover);
  if (leftover.length > 0) {
    throw new TypeError(
      `Correlated ${label} subqueries support only equality conditions between one inner and one outer qualified column (unsupported reference: ${leftover[0] ?? ""})`,
    );
  }
  if (keys.length === 0) {
    throw new TypeError(`Correlated ${label} subquery has no usable correlation condition`);
  }
  return keys;
}

function correlationEquality(
  predicate: Predicate,
  innerScope: ReadonlySet<string>,
  outerScope: ReadonlySet<string>,
): CorrelationKey | undefined {
  if (predicate.operator !== "=") return undefined;
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
  if (leftSide === "inner" && rightSide === "outer") return { inner: left, outer: right };
  if (leftSide === "outer" && rightSide === "inner") return { inner: right, outer: left };
  return undefined;
}

function rejectGroupedInner(inner: CompiledQuery, label: string): void {
  if (inner.groupBy.length > 0 || inner.having.length > 0) {
    throw new TypeError(`Correlated ${label} subqueries cannot use GROUP BY or HAVING`);
  }
}

function guardWildcard(block: CompiledQuery): void {
  if (block.select.some((item) => item.expression.kind === "wildcard")) {
    throw new TypeError("Correlated subqueries cannot be combined with SELECT *");
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
      if (folded !== undefined) return { kind: "literal", value: folded };
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
          return { kind: "literal", value: folded };
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
      return leftValue + rightValue;
    }
    return undefined;
  }
  if (typeof leftValue === "boolean" || typeof rightValue === "boolean") return undefined;
  if (typeof leftValue === "string" || typeof rightValue === "string") return undefined;
  const left = leftValue instanceof Date ? leftValue.getTime() : leftValue;
  const right = rightValue instanceof Date ? rightValue.getTime() : rightValue;
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
    if (derived === undefined || derived.limit !== undefined) continue;
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
    } else if (expression.kind === "call") expression.arguments.forEach(visit);
    else if (expression.kind === "list") expression.items.forEach(visit);
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
    // An OFFSET at either level changes which rows survive, so the limits cannot merge.
    block.offset !== undefined ||
    derived.offset !== undefined
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
    if (value instanceof Date) return `date ${value.toISOString()}`;
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
