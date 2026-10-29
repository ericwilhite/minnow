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
  outputRowComparator,
} from "./query.js";
import { encodeQueryIdentity } from "./query-identity.js";
import {
  exactNumericBinary,
  exactNumericValue,
  externalSqlDomainValue,
  isExactNumeric,
} from "./sql-domains.js";
import { encodeSqlEqualityValue } from "./sql-semantics.js";
import { maybeYieldToEventLoop } from "../work-slicer.js";

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
  /** The key the group is stored under. */
  key: string;
  keys: QueryValue[];
  /** Creation order: the row executor's stable sort keeps equal groups in this order. */
  seq: number;
  /**
   * The group's output row after HAVING and SELECT, null when HAVING drops it, absent until
   * evaluated, with the result domains it was evaluated under. A group object never changes once
   * published (a patch copies the groups it touches), so for given domains the row is a pure
   * function of the object and safe to memoize on it.
   */
  output?: { readonly domains: string; readonly row: QueryRow | null };
  members: number;
  counts: number[];
  /**
   * Running sums. A plain number while every contribution has been a safe integer: `absolute`
   * proves no partial sum can round, so integer addition is exact and reversible on its own. An
   * exact decimal string once any exact numeric value has contributed.
   */
  sums: Array<number | string>;
  absolute: number[];
}

interface Contributions {
  readonly rows: Map<string, Contribution>;
  revision: number;
}

/** A group's output row at its place in the ORDER BY. */
interface OutputEntry {
  readonly seq: number;
  readonly row: QueryRow;
}

/**
 * Every output row in ORDER BY order, ties in creation order: the order a full evaluation sorts
 * them into. A patch moves only the groups it touched, so a commit to one group of a hundred
 * thousand costs one evaluation and two binary searches, not a rebuild and a sort.
 */
interface OrderedOutput {
  readonly entries: readonly OutputEntry[];
  /** The domains the rows were evaluated under; a change re-evaluates every group. */
  readonly domains: string;
  readonly bytes: number;
}

/**
 * The groups, as an immutable table a patch can extend without copying: a shared base map and
 * the changes since it. A table never changes once built; a patch edits a successor. The changes
 * fold back into a new base once there are about √(2·size) of them, which balances copying the
 * changes on every patch against copying the base once per fold.
 */
class GroupTable {
  readonly #base: ReadonlyMap<string, Group>;
  /** Changes since the base: a replaced or added group, or null for a removed one. */
  readonly #changes: ReadonlyMap<string, Group | null>;
  readonly size: number;

  constructor(
    base: ReadonlyMap<string, Group> = new Map(),
    changes: ReadonlyMap<string, Group | null> = new Map(),
    size = base.size,
  ) {
    this.#base = base;
    this.#changes = changes;
    this.size = size;
  }

  get(key: string): Group | undefined {
    const changed = this.#changes.get(key);
    if (changed !== undefined) return changed ?? undefined;
    return this.#base.get(key);
  }

  /** Every group in creation order, which is the order a map of them would iterate in. */
  values(): Group[] {
    if (this.#changes.size === 0) return [...this.#base.values()];
    const groups: Group[] = [];
    for (const [key, group] of this.#base) if (!this.#changes.has(key)) groups.push(group);
    for (const group of this.#changes.values()) if (group !== null) groups.push(group);
    return groups.sort((left, right) => left.seq - right.seq);
  }

  edit(): GroupEdit {
    return new GroupEdit(this.#base, new Map(this.#changes), this.size);
  }
}

class GroupEdit {
  readonly #base: ReadonlyMap<string, Group>;
  readonly #changes: Map<string, Group | null>;
  #size: number;

  constructor(base: ReadonlyMap<string, Group>, changes: Map<string, Group | null>, size: number) {
    this.#base = base;
    this.#changes = changes;
    this.#size = size;
  }

  get(key: string): Group | undefined {
    const changed = this.#changes.get(key);
    if (changed !== undefined) return changed ?? undefined;
    return this.#base.get(key);
  }

  set(key: string, group: Group): void {
    if (this.get(key) === undefined) this.#size += 1;
    this.#changes.set(key, group);
  }

  delete(key: string): void {
    if (this.get(key) === undefined) return;
    this.#size -= 1;
    if (this.#base.has(key)) this.#changes.set(key, null);
    else this.#changes.delete(key);
  }

  finish(): GroupTable {
    if (this.#changes.size * this.#changes.size <= 2 * this.#size + 64)
      return new GroupTable(this.#base, this.#changes, this.#size);
    const table = new GroupTable(this.#base, this.#changes, this.#size);
    return new GroupTable(new Map(table.values().map((group) => [group.key, group])));
  }
}

/** Counts which way ordered aggregate output was produced, for tests that must reach both. */
export const liveAggregateTestHooks = {
  rebuilds: 0,
  moves: 0,
  /** Groups whose output row HAVING and SELECT evaluated. */
  evaluatedGroups: 0,
  /** Groups whose output row was put in order: every group on a rebuild, the touched on a move. */
  placedGroups: 0,
};

/** Hidden column that carries a group's place through output evaluation. */
const GROUP_REF = "__minnow_live_group_ref";

/** Past this share of touched groups, sorting everything again is cheaper than moving each. */
const REBUILD_SHARE = 8;

function outputRowBytes(row: QueryRow): number {
  let bytes = 64;
  for (const value of Object.values(row))
    bytes += typeof value === "string" ? value.length * 2 + 16 : 16;
  return bytes;
}

function contributionBytes(key: string, row: Contribution): number {
  return (
    64 +
    key.length * 2 +
    row.group.length * 2 +
    row.values.reduce<number>(
      (sum, value) => sum + (typeof value === "string" ? value.length * 2 + 16 : 16),
      0,
    )
  );
}

function groupBytes(key: string, group: Group): number {
  return (
    96 +
    key.length * 2 +
    group.sums.reduce<number>(
      (sum, value) => sum + (typeof value === "string" ? value.length * 2 + 24 : 16),
      0,
    )
  );
}

/** Single-table COUNT/SUM/AVG contributions; SQL still evaluates filters and arguments. */
export class LiveAggregate {
  readonly inputPlan: CompiledQuery;
  readonly #outputPlan: CompiledQuery;
  readonly #aggregates: Aggregate[];
  readonly #groupAliases: string[];
  readonly #contributions: Contributions;
  #revision: number;
  #pending: Map<string, Contribution | undefined> | undefined;
  readonly #bytes: number;
  readonly #groups: GroupTable;
  readonly #domains: ReadonlyArray<SqlDomain | null>;
  readonly keyAlias: string;
  readonly #schema: SqlColumnSchema[];
  #nextSeq: number;
  /** The groups this instance was patched from, and which of them the patch touched. */
  readonly #previous: { groups: GroupTable; touched: ReadonlySet<string> } | undefined;
  /** The previous instance's ordered output, which this one moves its touched groups in. */
  readonly #baseOutput: OrderedOutput | undefined;
  #output: OrderedOutput | undefined;

  private constructor(
    inputPlan: CompiledQuery,
    outputPlan: CompiledQuery,
    aggregates: Aggregate[],
    groupAliases: string[],
    keyAlias: string,
    contributions: Contributions = { rows: new Map(), revision: 0 },
    groups = new GroupTable(),
    domains: ReadonlyArray<SqlDomain | null> = [],
    schema: SqlColumnSchema[] = [],
    bytes = 256 +
      encodeQueryIdentity(inputPlan).length * 2 +
      encodeQueryIdentity(outputPlan).length * 2,
    pending?: Map<string, Contribution | undefined>,
    nextSeq = 0,
    previous?: { groups: GroupTable; touched: ReadonlySet<string> },
    baseOutput?: OrderedOutput,
  ) {
    this.inputPlan = inputPlan;
    this.#outputPlan = outputPlan;
    this.#aggregates = aggregates;
    this.#groupAliases = groupAliases;
    this.keyAlias = keyAlias;
    this.#contributions = contributions;
    this.#revision = contributions.revision;
    this.#pending = pending;
    this.#bytes = bytes;
    this.#groups = groups;
    this.#domains = domains;
    this.#schema = schema;
    this.#nextSeq = nextSeq;
    this.#previous = previous;
    this.#baseOutput = baseOutput;
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
        if (
          !["COUNT", "SUM", "AVG"].includes(expression.name) ||
          expression.arguments.length !== 1 ||
          expression.distinct === true
        )
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

  /**
   * Applies one commit's changed rows. Long inputs hand the event loop a turn between slices:
   * a refresh of a hundred thousand rows patches without holding other queries for its length.
   * Nothing shared changes until `accept`, so a query running between slices sees the previous
   * result.
   */
  async patch(
    result: QueryResult,
    changed: ReadonlySet<string>,
    token: (value: QueryValue) => string,
  ): Promise<LiveAggregate> {
    if (this.#pending !== undefined || this.#revision !== this.#contributions.revision)
      throw new Error("Unaccepted or stale aggregate");
    // Sequence numbers only grow, so a candidate that is never accepted wastes some but never
    // reuses one.
    const rows = this.#contributions.rows;
    const pending = new Map<string, Contribution | undefined>();
    let bytes = this.#bytes;
    const groups = this.#groups.edit();
    const touched = new Set<string>();
    const groupFor = (key: string, keys: QueryValue[] = []): Group => {
      let group = groups.get(key);
      if (group !== undefined && !touched.has(key)) bytes -= groupBytes(key, group);
      if (group === undefined) {
        group = {
          key,
          keys,
          seq: this.#nextSeq++,
          members: 0,
          counts: this.#aggregates.map(() => 0),
          sums: this.#aggregates.map(() => 0),
          absolute: this.#aggregates.map(() => 0),
        };
        groups.set(key, group);
        touched.add(key);
      } else if (!touched.has(key)) {
        const { output: _stale, ...copied } = group;
        void _stale;
        group = {
          ...copied,
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
          const current = group.sums[index] ?? 0;
          if (typeof value === "number" && typeof current === "number") {
            group.sums[index] = current + sign * value;
            continue;
          }
          const sum = exactNumericBinary(
            sign === 1 ? "+" : "-",
            typeof current === "number" ? exactNumericValue(current) : current,
            typeof value === "number" ? exactNumericValue(value) : value,
          );
          if (sum === null || sum === undefined)
            throw new TypeError("Invalid numeric aggregate contribution");
          group.sums[index] = sum;
        }
      }
    };
    let step = 0;
    for (const key of changed) {
      if ((++step & 1023) === 0) await maybeYieldToEventLoop();
      const old = rows.get(key);
      if (old === undefined) continue;
      apply(groupFor(old.group), old.values, -1);
      pending.set(key, undefined);
      bytes -= contributionBytes(key, old);
    }
    for (const row of result.rows) {
      if ((++step & 1023) === 0) await maybeYieldToEventLoop();
      const key = token(row[this.keyAlias] ?? null);
      if (pending.get(key) !== undefined || (rows.has(key) && !changed.has(key)))
        throw new TypeError("Duplicate live input key");
      const keys = this.#groupAliases.map((alias) => row[alias] ?? null);
      const group = JSON.stringify(keys.map(encodeSqlEqualityValue));
      const values = this.#aggregates.map(({ alias }) => row[alias] ?? null);
      apply(groupFor(group, keys), values, 1);
      const contribution = { group, values };
      pending.set(key, contribution);
      bytes += contributionBytes(key, contribution);
    }
    if (this.#groupAliases.length === 0) groupFor("[]");
    else for (const key of touched) if (groups.get(key)?.members === 0) groups.delete(key);
    for (const key of touched) {
      const group = groups.get(key);
      if (group !== undefined) bytes += groupBytes(key, group);
    }
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
      this.#contributions,
      groups.finish(),
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
                      : value === undefined || value === null
                        ? (this.#schema[index]?.type ?? "string")
                        : "string",
          ...(domain === null || domain === undefined || aggregate?.name === "COUNT"
            ? {}
            : { sqlDomain: domain }),
        };
      }),
      bytes,
      pending,
      this.#nextSeq,
      { groups: this.#groups, touched },
      this.#output,
    );
  }

  /** Publish only after result evaluation and live-set admission succeed. Old revisions cannot
   * be patched or accepted again, and rejected candidates never edit the shared contribution map. */
  accept(): void {
    const pending = this.#pending;
    if (pending === undefined) return;
    if (this.#revision !== this.#contributions.revision) throw new Error("Stale aggregate");
    for (const [key, contribution] of pending) {
      if (contribution === undefined) this.#contributions.rows.delete(key);
      else this.#contributions.rows.set(key, contribution);
    }
    this.#revision = ++this.#contributions.revision;
    this.#pending = undefined;
  }

  /** One group's row as the output plan reads it: its keys and finished aggregate values. */
  #groupRow(group: Group): QueryRow {
    const row: QueryRow = {};
    for (const [index, alias] of this.#groupAliases.entries())
      row[alias] = group.keys[index] ?? null;
    for (const [index, aggregate] of this.#aggregates.entries()) {
      const count = group.counts[index] ?? 0;
      const domain = this.#domains[index];
      const sum = group.sums[index] ?? null;
      let value: QueryValue =
        aggregate.name === "COUNT"
          ? count
          : count === 0
            ? null
            : typeof sum === "number" && domain?.kind === "numeric"
              ? (exactNumericValue(sum) ?? null)
              : sum;
      if (value !== null && aggregate.name !== "COUNT") {
        if (domain?.kind !== "numeric") {
          value = Number(externalSqlDomainValue(value));
          if (aggregate.name === "AVG") value /= count;
        } else if (aggregate.name === "AVG")
          value = exactNumericBinary("/", value, count, domain.scale) ?? null;
      }
      row[aggregate.alias] = value;
    }
    return row;
  }

  result(): QueryResult {
    const ordered = this.#orderedPlan();
    if (ordered === undefined) {
      const rows = [...this.#groups.values()].map((group) => this.#groupRow(group));
      return this.#publish(
        executeRowQueryInternal(this.#outputPlan, new Map([["__minnow_live_groups", rows]])),
      );
    }
    const output = this.#orderedOutput(ordered);
    this.#output = output;
    const start = this.#outputPlan.offset ?? 0;
    const limit = this.#outputPlan.limit;
    const visible = output.entries.slice(start, limit === undefined ? undefined : start + limit);
    return this.#publish({
      columns: this.#outputPlan.select.map((item) => item.alias),
      columnDomains: [],
      rows: visible.map((entry) => entry.row),
    });
  }

  #publish(result: QueryResult): QueryResult {
    result.columnDomains = inferResultColumnDomains(
      this.#outputPlan,
      new Map([["__minnow_live_groups", this.#schema]]),
    );
    return externalizeQueryResult(result);
  }

  /**
   * The output plan without its ordering and window, which evaluates any set of groups, and the
   * comparison that orders them. Undefined for the shapes the row executor orders some other
   * way — a wildcard select or `WITH TIES` — which evaluate every group as before.
   */
  #orderedPlan():
    { evaluate: CompiledQuery; compare: (left: QueryRow, right: QueryRow) => number } | undefined {
    const plan = this.#outputPlan;
    // Shapes whose output is not one row per group in a fixed order evaluate every group, as
    // does anything a later execution could answer differently: a statement datetime, or a
    // window bound to a parameter.
    if (
      plan.limitWithTies === true ||
      plan.usesStatementDatetime === true ||
      plan.usesVolatileFunctions === true ||
      plan.limitParameter !== undefined ||
      plan.offsetParameter !== undefined ||
      plan.pendingSelectShape !== undefined ||
      plan.pendingOutputAliases !== undefined ||
      plan.pendingSetOrder !== undefined ||
      plan.distinctWildcard === true ||
      plan.select.some((item) => item.expression.kind === "wildcard")
    )
      return undefined;
    let compare: (left: QueryRow, right: QueryRow) => number;
    try {
      compare = outputRowComparator(plan);
    } catch {
      return undefined;
    }
    const { limit: _limit, offset: _offset, ...unwindowed } = plan;
    void _limit;
    void _offset;
    return {
      evaluate: {
        ...unwindowed,
        select: [
          ...plan.select,
          { alias: GROUP_REF, expression: { kind: "column", reference: GROUP_REF } },
        ],
        orderBy: [],
      },
      compare,
    };
  }

  /**
   * Evaluates HAVING and SELECT for each group, null where HAVING drops it. Rows memoized under
   * the same domains are reused; the rest are evaluated together and memoized once the whole
   * evaluation succeeded, so a failure leaves no group with a row it never got.
   */
  #evaluate(
    groups: readonly Group[],
    plan: CompiledQuery,
    domains: string,
  ): Array<QueryRow | null> {
    const outputs = groups.map((group) =>
      group.output?.domains === domains ? group.output.row : undefined,
    );
    const pending = groups.flatMap((group, index) =>
      outputs[index] === undefined ? [{ group, index }] : [],
    );
    if (pending.length === 0) return outputs as Array<QueryRow | null>;
    liveAggregateTestHooks.evaluatedGroups += pending.length;
    const rows = pending.map(({ group }, position) => ({
      ...this.#groupRow(group),
      [GROUP_REF]: position,
    }));
    const result = executeRowQueryInternal(plan, new Map([["__minnow_live_groups", rows]]));
    const evaluated = new Map<number, QueryRow>();
    for (const row of result.rows) {
      const { [GROUP_REF]: position, ...output } = row;
      evaluated.set(position as number, output);
    }
    for (const [position, { group, index }] of pending.entries()) {
      const row = evaluated.get(position) ?? null;
      outputs[index] = row;
      group.output = { domains, row };
    }
    return outputs as Array<QueryRow | null>;
  }

  #orderedOutput(ordered: {
    evaluate: CompiledQuery;
    compare: (left: QueryRow, right: QueryRow) => number;
  }): OrderedOutput {
    const domains = JSON.stringify(this.#domains);
    const order = (left: OutputEntry, right: OutputEntry): number =>
      ordered.compare(left.row, right.row) || left.seq - right.seq;
    const base = this.#baseOutput;
    const previous = this.#previous;
    // Moving needs each touched group's previous row as `base` holds it. A candidate evaluated
    // under other domains, then rejected, may have re-memoized shared groups; rebuild then.
    const movable =
      base !== undefined &&
      previous !== undefined &&
      base.domains === domains &&
      previous.touched.size * REBUILD_SHARE <= this.#groups.size &&
      [...previous.touched].every((key) => {
        const old = previous.groups.get(key);
        return old === undefined || old.output?.domains === domains;
      });
    if (!movable) {
      liveAggregateTestHooks.rebuilds += 1;
      const groups = this.#groups.values();
      liveAggregateTestHooks.placedGroups += groups.length;
      const outputs = this.#evaluate(groups, ordered.evaluate, domains);
      const entries: OutputEntry[] = [];
      let bytes = 0;
      for (const [index, group] of groups.entries()) {
        const row = outputs[index];
        if (row === null || row === undefined) continue;
        entries.push({ seq: group.seq, row });
        bytes += 48 + outputRowBytes(row);
      }
      entries.sort(order);
      return { entries, domains, bytes };
    }
    liveAggregateTestHooks.moves += 1;
    const touched = [...previous.touched];
    liveAggregateTestHooks.placedGroups += touched.length;
    const current = touched.flatMap((key) => {
      const group = this.#groups.get(key);
      return group === undefined ? [] : [group];
    });
    const outputs = this.#evaluate(current, ordered.evaluate, domains);
    const entries = base.entries.slice();
    let bytes = base.bytes;
    // Lower bound: the first index whose entry does not order before `entry`.
    const place = (entry: OutputEntry): number => {
      let low = 0;
      let high = entries.length;
      while (low < high) {
        const middle = (low + high) >>> 1;
        const probe = entries[middle];
        if (probe !== undefined && order(probe, entry) < 0) low = middle + 1;
        else high = middle;
      }
      return low;
    };
    // The previous instance built `base` from its own groups, so each touched group it held sits
    // at exactly the place this search finds for its memoized row.
    for (const key of touched) {
      const row = previous.groups.get(key)?.output?.row;
      const old = previous.groups.get(key);
      if (old === undefined || row === null || row === undefined) continue;
      const index = place({ seq: old.seq, row });
      if (entries[index]?.seq !== old.seq) throw new Error("Live aggregate output lost a group");
      entries.splice(index, 1);
      bytes -= 48 + outputRowBytes(row);
    }
    for (const [index, group] of current.entries()) {
      const row = outputs[index];
      if (row === null || row === undefined) continue;
      const entry = { seq: group.seq, row };
      entries.splice(place(entry), 0, entry);
      bytes += 48 + outputRowBytes(row);
    }
    return { entries, domains, bytes };
  }

  get retainedBytes(): number {
    return this.#bytes + (this.#output?.bytes ?? this.#baseOutput?.bytes ?? 0);
  }
}
