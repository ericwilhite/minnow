import {
  type CompiledQuery,
  type Expression,
  type Predicate,
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

function optimizeBlock(block: CompiledQuery): void {
  for (const source of [block.base, ...block.joins]) {
    if (source.derived !== undefined) optimizeBlock(source.derived);
    source.union?.blocks.forEach(optimizeBlock);
    if (source.windowed !== undefined) optimizeBlock(source.windowed.block);
  }
  foldBlockConstants(block);
  pushPredicatesIntoDerived(block);
  pruneDerivedProjections(block);
  combineDerivedLimit(block);
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
    if (
      expression.name === "ROUND" &&
      foldedArguments.every((argument) => argument.kind === "literal")
    ) {
      const value = foldedArguments[0]?.kind === "literal" ? foldedArguments[0].value : null;
      const digitsValue =
        foldedArguments[1]?.kind === "literal" ? foldedArguments[1].value : undefined;
      if (
        (value === null || typeof value === "number") &&
        (digitsValue === undefined || typeof digitsValue === "number")
      ) {
        if (value === null) return { kind: "literal", value: null };
        const factor = 10 ** (digitsValue ?? 0);
        const rounded = Math.round(value * factor) / factor;
        if (Number.isFinite(rounded)) return { kind: "literal", value: rounded };
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
  operator: "+" | "-" | "*" | "/",
  leftValue: QueryValue,
  rightValue: QueryValue,
): QueryValue | undefined {
  if (leftValue === null || rightValue === null) return null;
  if (typeof leftValue === "boolean" || typeof rightValue === "boolean") return undefined;
  if (typeof leftValue === "string" || typeof rightValue === "string") return undefined;
  const left = leftValue instanceof Date ? leftValue.getTime() : leftValue;
  const right = rightValue instanceof Date ? rightValue.getTime() : rightValue;
  const result =
    operator === "+"
      ? left + right
      : operator === "-"
        ? left - right
        : operator === "*"
          ? left * right
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
  // An EXISTS block is self-contained (no correlation), so it moves unchanged.
  if (expression.kind === "exists") return expression;
  return undefined;
}

function containsAggregateOrWindow(expression: Expression): boolean {
  if (expression.kind === "window") return true;
  if (expression.kind === "call") {
    if (expression.name !== "ROUND") return true;
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
    const kept = derived.select.filter((item) => referenced.has(item.alias));
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
    else if (expression.kind === "window") {
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
  return `${expression.name}(...) over (...)`;
}
