/**
 * Prepared read plans: the catalog-rewritten form of a compiled statement, built once with its
 * placeholders still unbound and re-bound per execution by copying only the paths that lead to
 * a placeholder. An execution that would otherwise clone the plan and re-run every catalog pass
 * (case folding, view expansion, domain coercion, integer typing, key-dependent GROUP BY,
 * wildcard binding, subquery optimization) pays one small path copy instead.
 *
 * The template is exact only where the passes cannot see a placeholder's value, so everything a
 * value could change is either replayed here at bind time or declines the execution back to the
 * ordinary path, which stays the authority on semantics and errors:
 *
 * - Domain and typed-column coercion (`normalizePlanDomainLiterals`) annotates each placeholder
 *   it would have coerced. A DOMAIN position is normalized here exactly as the pass normalizes a
 *   literal; a string opposite a non-text typed column, or a non-text value opposite a TEXT
 *   column, declines (the pass's coercion and type error depend on the value).
 * - Integer division is typed by its operands, a bound literal's included, so a template with a
 *   placeholder and any division is never built.
 * - The bind-time calendar rewrite reads the bound value, so a template with a placeholder and a
 *   DATE_TRUNC or EXTRACT call is never built.
 * - Key forwards rewrite key constants by value; the caller declines while any forward is live.
 * - `x = ANY($n)` binds its array here, into the same IN list the ordinary binder builds, each
 *   member taking the placeholder's coercion; the list length is the array's, per execution.
 * - Full-text query placeholders, sequence calls, and time travel are left to the ordinary path.
 */
import type {
  CompiledQuery,
  Expression,
  PredicateOperator,
  QueryParameter,
  QueryValue,
} from "../plan/model.js";
import type { SimpleDataType, SqlDomain } from "../storage/types.js";
import {
  bindBlockLimits,
  bindQuantifiedArray,
  containsParameter,
  parseQuantified,
} from "./query.js";
import { normalizeSqlDomainValue } from "./sql-domains.js";

/**
 * What the domain pass would have done to a literal standing where this placeholder stands.
 * Set on the placeholder node by `normalizePlanDomainLiterals` when it runs in template mode;
 * plain data, so plan clones carry it with the node.
 */
export interface ParameterPosition {
  /** Opposite a column with this logical domain: the literal is normalized into it. */
  domain?: SqlDomain;
  /** Opposite a non-text column of this type: an untyped string literal would be read as it. */
  typed?: SimpleDataType;
  /** Opposite a plain TEXT expression: a non-text literal is a comparison type error. */
  text?: true;
}

/** The placeholder node with the template-mode annotation the binder reads. */
export type AnnotatedParameter = Extract<Expression, { kind: "parameter" }> & {
  bindPosition?: ParameterPosition;
};

/** Test-only counters: in-repo suites assert the cache served or declined, and can turn it off. */
export const preparedPlanTestHooks = {
  disabled: false,
  /** Builds a template on a statement's first execution instead of its second. */
  eager: false,
  /** Templates built (each runs the catalog passes once). */
  built: 0,
  /** Executions bound from a template without re-running a catalog pass. */
  served: 0,
  /** Executions a template declined to the ordinary path. */
  declined: 0,
  /** Prepared executions that failed and were re-run by the ordinary path. */
  retried: 0,
  /** Executor analyses answered from a template's memo instead of a walk of the plan. */
  analysisHits: 0,
  /** Runs of the catalog-rewrite passes, prepared or not: a served execution adds none. */
  catalogRewrites: 0,
  /** Scans of a statement's text for liftable literals: a repeated text adds none. */
  literalScans: 0,
};

type PathKey = string | number;

interface BindTrie {
  readonly children: Map<PathKey, BindTrie>;
  /** A placeholder node to replace with its bound literal. */
  parameter?: AnnotatedParameter;
  /** A block whose LIMIT/OFFSET placeholders bind into its own fields. */
  limits?: true;
  /** A comparison `x op ANY/ALL($n)` whose array placeholder binds into a list. */
  quantified?: AnnotatedParameter;
}

const LIMIT_FIELDS = [
  "limitParameter",
  "offsetParameter",
  "limitValidationParameters",
  "offsetValidationParameters",
] as const;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || value instanceof Date) return false;
  const prototype = Object.getPrototypeOf(value) as unknown;
  return prototype === Object.prototype || prototype === null;
}

/** Why a template cannot be built, or undefined when it can. */
interface TemplateFacts {
  parameters: number;
  divisions: boolean;
  calendarCalls: boolean;
  quantified: boolean;
  unsupported: boolean;
}

/**
 * Collects every binding site in the template into a trie of property paths. Plans are plain
 * data — objects, arrays, primitives, Dates — so a generic walk reaches every placeholder,
 * wherever a pass moved or copied it.
 */
function collectSites(value: unknown, trie: BindTrie, facts: TemplateFacts): boolean {
  if (Array.isArray(value)) {
    let found = false;
    for (let index = 0; index < value.length; index += 1) {
      const child: BindTrie = { children: new Map() };
      if (collectSites(value[index], child, facts)) {
        trie.children.set(index, child);
        found = true;
      }
    }
    return found;
  }
  if (typeof value !== "object" || value === null || value instanceof Date) return false;
  if (!isPlainObject(value)) {
    // A plan holds only plain data; anything else is outside what the binder can copy.
    facts.unsupported = true;
    return false;
  }
  const node = value;
  if (node.kind === "parameter" && typeof node.index === "number") {
    trie.parameter = node as unknown as AnnotatedParameter;
    facts.parameters += 1;
    return true;
  }
  if (node.kind === "fts" && node.queryParameter !== undefined) facts.unsupported = true;
  if (node.kind === "binary" && node.operator === "/") facts.divisions = true;
  if (node.kind === "call" && (node.name === "DATE_TRUNC" || node.name === "EXTRACT")) {
    facts.calendarCalls = true;
  }
  let found = false;
  if (LIMIT_FIELDS.some((field) => node[field] !== undefined)) {
    trie.limits = true;
    facts.parameters += 1;
    found = true;
  }
  let skip: string | undefined;
  if (
    typeof node.operator === "string" &&
    parseQuantified(node.operator as PredicateOperator) !== undefined &&
    typeof node.right === "object" &&
    node.right !== null
  ) {
    const right = node.right as Expression;
    if (right.kind === "parameter") {
      trie.quantified = right;
      facts.parameters += 1;
      facts.quantified = true;
      found = true;
      skip = "right";
    } else if (containsParameter(right)) {
      // `ANY($n::type[])` casts each member as it binds; the ordinary binder does that.
      facts.unsupported = true;
    }
  }
  for (const key of Object.keys(node)) {
    if (key === skip) continue;
    const child: BindTrie = { children: new Map() };
    if (collectSites(node[key], child, facts)) {
      trie.children.set(key, child);
      found = true;
    }
  }
  return found;
}

/** Thrown inside a bind to hand the execution back to the ordinary path. */
class Decline extends Error {}

function boundLiteral(node: AnnotatedParameter, value: QueryParameter): Expression {
  // An array binds only under ANY or ALL; the ordinary binder reports any other use.
  if (Array.isArray(value)) throw new Decline();
  const position = node.bindPosition;
  if (position !== undefined) {
    if (position.text === true && value !== null && typeof value !== "string") throw new Decline();
    if (position.typed !== undefined && typeof value === "string") throw new Decline();
    if (position.domain !== undefined) {
      return {
        kind: "literal",
        value: normalizeSqlDomainValue(position.domain, value),
        internalSqlValue: true,
        sqlDomain: position.domain,
      };
    }
  }
  return { kind: "literal", value: value as QueryValue };
}

/** The template each bound plan came from, for the executor's per-template analyses. */
const templates = new WeakMap<CompiledQuery, PreparedPlanTemplate>();

/** The template a plan was bound from, if it is a prepared plan. */
export function preparedTemplateOf(plan: CompiledQuery): PreparedPlanTemplate | undefined {
  return templates.get(plan);
}

/**
 * One statement's prepared template. Immutable once built: executions share it and receive
 * fresh nodes only along the copied paths, as every other pass already treats a compiled plan.
 */
export class PreparedPlanTemplate {
  readonly plan: CompiledQuery;
  /** Compares a key column to a constant or placeholder, so live key forwards decline it. */
  readonly keySensitive: boolean;
  /**
   * Whether a placeholder reaches the select list: only then can the output's inferred types
   * and domains depend on a bound value rather than on the template and the catalog alone.
   */
  readonly selectHasParameters: boolean;
  readonly #trie: BindTrie;
  readonly #parameterized: boolean;
  readonly #analyses = new Map<string, { key: readonly unknown[]; value: unknown }>();

  private constructor(plan: CompiledQuery, trie: BindTrie, parameterized: boolean, keys: boolean) {
    this.plan = plan;
    this.#trie = trie;
    this.#parameterized = parameterized;
    this.keySensitive = keys;
    this.selectHasParameters = trie.children.has("select");
    templates.set(plan, this);
  }

  /**
   * An analysis of every plan this template binds that depends only on the template's shape and
   * the given inputs (catalog records, by identity): computed once per input set, not per run.
   * The caller vouches that the analysis reads no bound value.
   */
  analysis<T>(name: string, key: readonly unknown[], compute: () => T): T {
    const cached = this.#analyses.get(name);
    if (
      cached?.key.length === key.length &&
      cached.key.every((item, index) => item === key[index])
    ) {
      preparedPlanTestHooks.analysisHits += 1;
      return cached.value as T;
    }
    const value = compute();
    this.#analyses.set(name, { key: [...key], value });
    return value;
  }

  /**
   * The template for a rewritten, still-unbound plan, or undefined when a value-dependent pass
   * could have acted on one of its placeholders.
   */
  static create(plan: CompiledQuery, keySensitive: boolean): PreparedPlanTemplate | undefined {
    const trie: BindTrie = { children: new Map() };
    const facts: TemplateFacts = {
      parameters: 0,
      divisions: false,
      calendarCalls: false,
      quantified: false,
      unsupported: false,
    };
    collectSites(plan, trie, facts);
    if (facts.unsupported) return undefined;
    if (facts.parameters > 0 && (facts.divisions || facts.calendarCalls)) return undefined;
    // An ANY list is a key site once bound, though no key-site walk reads it unbound.
    return new PreparedPlanTemplate(
      plan,
      trie,
      facts.parameters > 0,
      keySensitive || facts.quantified,
    );
  }

  /**
   * The plan bound to these (already validated) parameters, or undefined when a value needs the
   * ordinary path. Only nodes on a path to a binding site are new; the rest is the template's.
   */
  bind(params: readonly QueryParameter[]): CompiledQuery | undefined {
    if (!this.#parameterized) return this.plan;
    try {
      const bound = this.#copy(this.plan, this.#trie, params) as CompiledQuery;
      delete bound.parameterCount;
      templates.set(bound, this);
      return bound;
    } catch {
      // A declined value, or a coercion or LIMIT the ordinary path would also attempt and
      // reject: either way that path decides, and reports any error itself.
      return undefined;
    }
  }

  #copy(value: unknown, trie: BindTrie, params: readonly QueryParameter[]): unknown {
    const parameter = trie.parameter;
    if (parameter !== undefined) return boundLiteral(parameter, params[parameter.index] ?? null);
    if (Array.isArray(value)) {
      const copy = value.slice();
      for (const [key, child] of trie.children) {
        copy[key as number] = this.#copy(value[key as number], child, params);
      }
      return copy;
    }
    const node = value as Record<string, unknown>;
    const copy: Record<string, unknown> = { ...node };
    for (const [key, child] of trie.children) {
      copy[key as string] = this.#copy(node[key as string], child, params);
    }
    if (trie.limits === true) bindBlockLimits(copy as unknown as CompiledQuery, params);
    const quantified = trie.quantified;
    if (quantified !== undefined) {
      const bound = bindQuantifiedArray(
        copy.operator as PredicateOperator,
        params[quantified.index] ?? null,
        quantified.index,
      );
      copy.operator = bound.operator;
      copy.right =
        bound.members === null
          ? ({ kind: "literal", value: null } satisfies Expression)
          : ({
              kind: "list",
              items: bound.members.map((member: QueryValue) => boundLiteral(quantified, member)),
            } satisfies Expression);
    }
    return copy;
  }
}
