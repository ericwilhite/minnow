/**
 * Static upper bounds for native trigger preparation, computed from catalog metadata before any
 * lowering or compilation runs (owner/native-command.ts admits the whole bound first).
 *
 * A trigger body is one INSERT ... VALUES, UPDATE or DELETE over a base table (lower/triggers.ts):
 * no view, FROM list or INSERT ... SELECT reaches it, and a subquery can name base tables only.
 * Lowering therefore creates at most a constant number of IR nodes per source token, plus the
 * columns, defaults, generated expressions and indexes of each table it names, plus `*`
 * expansions over those tables, plus one stored-default copy per explicit DEFAULT cell (the only
 * place one token re-lowers stored text). String content is retained at most a constant number of bytes
 * per source character. Table rules lower the stored DEFAULT, generated and CHECK text of one
 * table the same way. Each bound adds those terms with per-term constants; the generated
 * calibration in testing/native-preparation-bound.test.ts checks every term with margin, and
 * native-command.ts fails loudly if an actual preparation ever exceeds its bound.
 */
import type { CatalogView, TableDef } from "../access/catalog.js";
import type { TriggerBody } from "../access/trigger-def.js";
import type { PhysicalPlan } from "../ir/ir.js";
import { OBJECT_OVERHEAD_BYTES } from "../memory/governor.js";
import { retainedBytes } from "./rule-cache.js";

/** Retained IR per source token (node, its type and inserted coercions). */
export const PREPARATION_TOKEN_BYTES = 1024;
/** Retained string/byte content per source character. */
export const PREPARATION_TEXT_BYTES = 4;
/** Retained per column of a named table (types, names, default/generated slots). */
export const PREPARATION_COLUMN_BYTES = 2048;
/** Retained per table a statement names, before its columns. */
export const PREPARATION_TABLE_BYTES = 8192;
/** Retained per index column or foreign key of a named table. */
export const PREPARATION_KEY_BYTES = 512;
/** Retained per lowered body before its tokens (plan root, runner and binding program). */
export const PREPARATION_BODY_BYTES = 16384;
/** Retained per OLD/NEW binding (binding program column and slot). */
export const PREPARATION_BINDING_BYTES = 512;
/** Retained per table rule set before its expressions (compiled program and defaults). */
export const PREPARATION_RULE_BYTES = 16384;

export interface SqlScan {
  readonly tokens: number;
  readonly stars: number;
  readonly characters: number;
}

/**
 * Counts SQL tokens without building them. Identifiers (unquoted ones folded to lower case) are
 * reported to `name`; strings, quoted identifiers, numbers, comments and operators are skipped
 * in place. Unknown characters count as one token each, which only raises the bound.
 */
export function scanSql(sql: string, name?: (identifier: string) => void): SqlScan {
  let tokens = 0,
    stars = 0,
    i = 0;
  const n = sql.length;
  const word = (c: number) =>
    (c >= 97 && c <= 122) || (c >= 65 && c <= 90) || c === 95 || c > 127 || c === 36;
  const digit = (c: number) => c >= 48 && c <= 57;
  while (i < n) {
    const c = sql.charCodeAt(i);
    if (c === 32 || c === 9 || c === 10 || c === 13 || c === 12) {
      i++;
      continue;
    }
    if (c === 45 && sql.charCodeAt(i + 1) === 45) {
      while (i < n && sql.charCodeAt(i) !== 10) i++;
      continue;
    }
    if (c === 47 && sql.charCodeAt(i + 1) === 42) {
      const end = sql.indexOf("*/", i + 2);
      i = end < 0 ? n : end + 2;
      continue;
    }
    tokens++;
    if (c === 39 || c === 34) {
      // 'string' or "identifier"; a doubled quote continues it.
      const start = i + 1;
      i++;
      for (;;) {
        const end = sql.indexOf(String.fromCharCode(c), i);
        if (end < 0) {
          i = n;
          break;
        }
        if (sql.charCodeAt(end + 1) === c) i = end + 2;
        else {
          i = end + 1;
          if (c === 34 && name) name(sql.slice(start, end).replaceAll('""', '"'));
          break;
        }
      }
      continue;
    }
    if (word(c)) {
      const start = i;
      while (i < n && (word(sql.charCodeAt(i)) || digit(sql.charCodeAt(i)))) i++;
      if (name) name(sql.slice(start, i).toLowerCase());
      continue;
    }
    if (digit(c) || (c === 46 && digit(sql.charCodeAt(i + 1)))) {
      while (
        i < n &&
        (digit(sql.charCodeAt(i)) || word(sql.charCodeAt(i)) || sql.charCodeAt(i) === 46)
      )
        i++;
      continue;
    }
    if (c === 42) stars++;
    i++;
  }
  return { tokens, stars, characters: n };
}

/**
 * Where the shared parser (engine/query.ts) copies one source subtree into several AST nodes,
 * each copy is lowered separately, so retained IR multiplies. The structural scan below mirrors
 * every such site reachable from a trigger body, a stored default, generated expression or CHECK:
 * - simple `CASE x WHEN …`: x is copied once per WHEN;
 * - `BETWEEN`: its left operand twice; `BETWEEN SYMMETRIC`: four times, and both bounds twice;
 * - row comparisons `(a, b) op (c, d)`: each row at most twice; row `IN`: the left row once per
 *   list item;
 * - VARIANCE/STDDEV forms: the argument at most eight times;
 * - in a subquery, GROUP BY / ORDER BY / HAVING / DISTINCT names and ordinals may each become a
 *   copy of a select-list expression, and USING/NATURAL joins replace every unqualified name
 *   with a merge over the joined tables.
 * Copies nest, so a group's effective size includes its own copies before a parent copies it.
 * GROUPING SETS / ROLLUP / CUBE (unbounded copies) are reported, and refused before lowering.
 * A left operand is everything since the last token that cannot occur inside an additive
 * expression at that level; that over-approximates the parser's additive operand.
 */
const SEPARATORS = new Set([
  "and",
  "or",
  "not",
  "is",
  "in",
  "like",
  "ilike",
  "similar",
  "between",
  "when",
  "then",
  "else",
  "where",
  "set",
  "values",
  "select",
  "from",
  "on",
  "having",
  "group",
  "order",
  "by",
  "limit",
  "offset",
  "join",
  "returning",
  "using",
  "as",
  "distinct",
  "union",
  "except",
  "intersect",
  "escape",
  "into",
  "update",
  "delete",
  "insert",
  "case",
  "end",
]);
const STATISTICAL = new Set([
  "variance",
  "var_pop",
  "var_samp",
  "stddev",
  "stddev_pop",
  "stddev_samp",
]);
const CLAUSE_REFS = new Set(["group", "order", "having", "distinct"]);
const CLAUSE_OTHER = new Set(["from", "where", "limit", "offset"]);
interface Frame {
  readonly kind: "root" | "paren" | "case";
  bytes: number;
  sinceSeparator: number;
  commas: number;
  words: number;
  // simple CASE
  operandFrom: number;
  operand: number;
  whens: number;
  inOperand: boolean;
  // BETWEEN
  between: number;
  symmetric: number[];
  // row predicates and statistical calls
  lastGroup: { readonly bytes: number; readonly commas: number } | null;
  rowCompare: boolean;
  rowIn: number;
  statistical: boolean;
  // subquery clauses
  select: boolean;
  clause: "list" | "refs" | "other";
  selectList: number;
  refs: number;
  joins: number;
  merged: boolean;
  names: number;
}
function frame(kind: Frame["kind"]): Frame {
  return {
    kind,
    bytes: 0,
    sinceSeparator: 0,
    commas: 0,
    words: 0,
    operandFrom: -1,
    operand: 0,
    whens: 0,
    inOperand: false,
    between: -1,
    symmetric: [],
    lastGroup: null,
    rowCompare: false,
    rowIn: 0,
    statistical: false,
    select: false,
    clause: "other",
    selectList: 0,
    refs: 0,
    joins: 0,
    merged: false,
    names: 0,
  };
}
export interface SqlBytes {
  readonly bytes: number;
  /** GROUPING SETS / ROLLUP / CUBE: copies grow with the set count; refused before lowering. */
  readonly groupingSets: boolean;
}
/**
 * The effective retained-IR bound of one SQL text: per-token and per-character costs, plus
 * `extra(word)` for identifiers (named tables, DEFAULT cells) and `star` for each `*`, all
 * multiplied by the parser's copy sites above. Builds no tokens: one frame per open group.
 */
export function sqlBytes(
  sql: string,
  extra: (word: string) => number = () => 0,
  star = 0,
): SqlBytes {
  const stack: Frame[] = [frame("root")];
  let groupingSets = false,
    previous = "";
  const top = () => stack[stack.length - 1] ?? frame("root");
  const close = (): void => {
    const f = stack.pop();
    if (!f) return;
    let g = f.bytes;
    for (const from of f.symmetric) g += f.bytes - from;
    if (f.kind === "case" && f.whens > 1) g += f.operand * (f.whens - 1);
    if (f.select) {
      g += f.refs * f.selectList;
      if (f.merged) g += f.names * (f.joins + 1) * 2 * PREPARATION_TOKEN_BYTES;
    }
    const parent = top();
    if (parent.statistical) g *= 8;
    if (parent.rowCompare && f.commas > 0) g *= 2;
    if (parent.rowIn > 0) g += parent.rowIn * (f.commas + 1);
    parent.statistical = false;
    parent.rowCompare = false;
    parent.rowIn = 0;
    parent.bytes += g;
    parent.sinceSeparator += g;
    if (parent.select && parent.clause === "list") parent.selectList += g;
    parent.lastGroup = { bytes: g, commas: f.commas };
  };
  const token = (word: string | null, punct: string | null, bytes: number): void => {
    let f = top();
    if (word === "case") {
      // A CASE that opens an enclosing simple CASE's operand starts that operand first.
      if (f.kind === "case" && f.words === 1 && f.operandFrom < 0) {
        f.inOperand = true;
        f.operandFrom = f.bytes;
        f.words++;
      }
      stack.push(frame("case"));
      f = top();
    }
    if (f.between >= 0) {
      if (word === "symmetric") {
        f.bytes += 2 * f.between;
        f.symmetric.push(f.bytes);
      }
      f.between = -1;
    }
    if (f.kind === "case" && f.words === 1 && f.operandFrom < 0 && word !== "when") {
      f.inOperand = true;
      f.operandFrom = f.bytes;
    }
    if (word !== null) {
      f.words++;
      if (f.kind === "paren" && f.words === 1 && word === "select") {
        f.select = true;
        f.clause = "list";
      } else if (f.select) {
        if (CLAUSE_REFS.has(word)) f.clause = "refs";
        else if (CLAUSE_OTHER.has(word)) f.clause = "other";
        if (word === "join") f.joins++;
        if (word === "using" || word === "natural") f.merged = true;
        if (!SEPARATORS.has(word)) {
          f.names++;
          if (f.clause === "refs") f.refs++;
        }
      }
      if (word === "when" && f.kind === "case") {
        if (f.inOperand) {
          f.operand = f.bytes - f.operandFrom;
          f.inOperand = false;
        }
        f.whens++;
      }
      if (word === "between") {
        f.between = f.sinceSeparator;
        f.bytes += f.sinceSeparator;
      }
      if (word === "in" && (f.lastGroup?.commas ?? 0) > 0) f.rowIn = f.lastGroup?.bytes ?? 0;
      if (word === "rollup" || word === "cube" || (word === "sets" && previous === "grouping"))
        groupingSets = true;
    } else if (punct !== null && punct !== "(") {
      if (f.select && f.clause === "refs" && /^[0-9]/.test(punct)) f.refs++;
      if (
        (punct === "=" || punct === "<" || punct === ">" || punct === "!") &&
        (f.lastGroup?.commas ?? 0) > 0
      ) {
        f.bytes += f.lastGroup?.bytes ?? 0;
        f.rowCompare = true;
      }
    }
    const statistical = word !== null && STATISTICAL.has(word);
    f.bytes += bytes;
    f.sinceSeparator += bytes;
    if (f.select && f.clause === "list") f.selectList += bytes;
    if (punct === ",") f.commas++;
    if ((word !== null && SEPARATORS.has(word)) || (punct !== null && /^[,=<>!]$/.test(punct)))
      f.sinceSeparator = 0;
    // An opening parenthesis keeps the mark for the group it opens.
    const opening = punct === "(";
    if (!opening && !(punct !== null && /^[=<>!]$/.test(punct))) f.rowCompare = false;
    if (word !== "in" && !opening) f.rowIn = 0;
    if (!opening) f.statistical = statistical;
    f.lastGroup = null;
    previous = word ?? "";
    if (word === "end" && f.kind === "case") close();
  };
  let i = 0;
  const n = sql.length;
  const isWord = (c: number) =>
    (c >= 97 && c <= 122) || (c >= 65 && c <= 90) || c === 95 || c > 127 || c === 36;
  const isDigit = (c: number) => c >= 48 && c <= 57;
  while (i < n) {
    const c = sql.charCodeAt(i);
    if (c === 32 || c === 9 || c === 10 || c === 13 || c === 12) {
      i++;
      continue;
    }
    if (c === 45 && sql.charCodeAt(i + 1) === 45) {
      while (i < n && sql.charCodeAt(i) !== 10) i++;
      continue;
    }
    if (c === 47 && sql.charCodeAt(i + 1) === 42) {
      const end = sql.indexOf("*/", i + 2);
      i = end < 0 ? n : end + 2;
      continue;
    }
    const start = i;
    if (c === 39 || c === 34) {
      const quote = String.fromCharCode(c);
      i++;
      for (;;) {
        const end = sql.indexOf(quote, i);
        if (end < 0) {
          i = n;
          break;
        }
        if (sql.charCodeAt(end + 1) === c) i = end + 2;
        else {
          i = end + 1;
          break;
        }
      }
      const text = sql.slice(start + 1, Math.max(start + 1, i - 1));
      const cost = PREPARATION_TOKEN_BYTES + PREPARATION_TEXT_BYTES * (i - start);
      if (c === 34)
        token(text.replaceAll('""', '"'), null, cost + extra(text.replaceAll('""', '"')));
      else token(null, "'", cost);
      continue;
    }
    if (isWord(c)) {
      while (i < n && (isWord(sql.charCodeAt(i)) || isDigit(sql.charCodeAt(i)))) i++;
      const word = sql.slice(start, i).toLowerCase();
      token(
        word,
        null,
        PREPARATION_TOKEN_BYTES + PREPARATION_TEXT_BYTES * (i - start) + extra(word),
      );
      continue;
    }
    if (isDigit(c) || (c === 46 && isDigit(sql.charCodeAt(i + 1)))) {
      while (
        i < n &&
        (isDigit(sql.charCodeAt(i)) || isWord(sql.charCodeAt(i)) || sql.charCodeAt(i) === 46)
      )
        i++;
      token(
        null,
        sql.slice(start, Math.min(i, start + 1)),
        PREPARATION_TOKEN_BYTES + PREPARATION_TEXT_BYTES * (i - start),
      );
      continue;
    }
    i++;
    const punct = String.fromCharCode(c);
    const cost = PREPARATION_TOKEN_BYTES + PREPARATION_TEXT_BYTES + (c === 42 ? star : 0);
    if (punct === "(") {
      token(null, "(", cost);
      stack.push(frame("paren"));
    } else if (punct === ")") {
      // The closing parenthesis belongs to its group, so the parent still sees that group last.
      if (top().kind === "case") close();
      top().bytes += cost;
      if (stack.length > 1) close();
    } else token(null, punct, cost);
  }
  while (stack.length > 1) close();
  return { bytes: checked(top().bytes), groupingSets };
}

/** One named table's retained share: its columns, stored expressions and keys. */
export function tablePreparationBound(table: TableDef): number {
  let bytes = PREPARATION_TABLE_BYTES + PREPARATION_TEXT_BYTES * table.name.length;
  for (const column of table.columns) {
    bytes += PREPARATION_COLUMN_BYTES + PREPARATION_TEXT_BYTES * column.name.length;
    if (column.defaultSql !== null) bytes += sqlBytes(column.defaultSql).bytes;
    if (column.generatedSql !== null) bytes += sqlBytes(column.generatedSql).bytes;
  }
  for (const index of table.indexes) bytes += PREPARATION_KEY_BYTES * (1 + index.columnIds.length);
  bytes += PREPARATION_KEY_BYTES * table.foreignKeys.length;
  return checked(bytes);
}

/**
 * One explicit `DEFAULT` cell: lowering parses and lowers the column's stored default again for
 * every such cell (lower/modify.ts), so each cell retains its own copy, including its own UTF-8
 * buffer for a text literal. The largest column default bounds any cell of the table.
 */
export function defaultCellPreparationBound(table: TableDef): number {
  let bytes = PREPARATION_COLUMN_BYTES;
  for (const column of table.columns)
    if (column.defaultSql !== null)
      bytes = Math.max(bytes, PREPARATION_COLUMN_BYTES + sqlBytes(column.defaultSql).bytes);
  return checked(bytes);
}

/** A lowered body plan, its runner and binding program. */
export function bodyPreparationBound(body: TriggerBody, catalog: CatalogView): number {
  return bodyPreparation(body, catalog).bytes;
}
/** The body bound and whether the body uses GROUPING SETS / ROLLUP / CUBE. */
export function bodyPreparation(body: TriggerBody, catalog: CatalogView): SqlBytes {
  // First pass: the columns `*` may expand, over every table the text names and the target.
  let columns = 0;
  const named = new Set<number>();
  const target = catalog.table(body.targetTableId);
  if (target) {
    named.add(target.id);
    columns += target.columns.length;
  }
  scanSql(body.sql, (identifier) => {
    const table = catalog.tableByName(identifier);
    if (!table || named.has(table.id)) return;
    named.add(table.id);
    columns += table.columns.length;
  });
  const cell = target ? defaultCellPreparationBound(target) : 0;
  // Every table occurrence may be its own scan, and every DEFAULT keyword one more lowered
  // copy of a stored default; both multiply with the copies that contain them.
  const scan = sqlBytes(
    body.sql,
    (word) => {
      if (word === "default") return cell;
      const table = catalog.tableByName(word);
      return table ? tablePreparationBound(table) : 0;
    },
    columns * PREPARATION_COLUMN_BYTES,
  );
  return {
    bytes: checked(
      PREPARATION_BODY_BYTES +
        scan.bytes +
        (target ? tablePreparationBound(target) : 0) +
        PREPARATION_BINDING_BYTES * body.bindings.length,
    ),
    groupingSets: scan.groupingSets,
  };
}

/** A table's lowered rule expressions, compiled program and default entries. */
export function rulePreparationBound(table: TableDef): number {
  let bytes = PREPARATION_RULE_BYTES + tablePreparationBound(table);
  for (const check of table.checks)
    bytes +=
      PREPARATION_TABLE_BYTES / 4 +
      PREPARATION_TEXT_BYTES * check.name.length +
      2 * sqlBytes(check.conditionSql).bytes;
  return checked(bytes);
}

/** What a prepared body retains: the runner's plan graph (shared with the lowered plan) and
 * its binding program's column/slot entries. */
export function bodyRetainedBytes(plan: PhysicalPlan, bindings: number): number {
  return retainedBytes(plan) + OBJECT_OVERHEAD_BYTES * (4 + 2 * bindings);
}
/** What a prepared table retains: lowered rule expressions, compiled program, default entries. */
export function ruleRetainedBytes(prepared: {
  readonly expr: unknown;
  readonly program: unknown;
  readonly defaults: unknown;
}): number {
  return retainedBytes(prepared);
}

/** The preparation containers: preparation object, arrays, one entry per table/trigger/body. */
export function containerPreparationBound(
  tables: number,
  triggers: number,
  bodies: number,
): number {
  return checked(
    OBJECT_OVERHEAD_BYTES * (8 + 2 * tables + 2 * triggers) +
      8 * (tables + triggers + bodies) +
      CLOCK_BOUND_BYTES,
  );
}
/** A zoned timestamp clock parameter: a small fixed object with one bigint. */
export const CLOCK_BOUND_BYTES = 1024;

/**
 * A superset of the command reach (owner/command-reach.ts) and its preparation bound, from the
 * catalog alone: the root table, every trigger on a reached table whose budget allows bodies
 * (any event), each body target one budget lower, and every foreign-key child at the same budget.
 * Events, assigned columns and moved keys only narrow the real reach, so ignoring them keeps a
 * superset. Returns the bound for the closure's rule sets, bodies and containers, and the
 * closure itself so the caller can check the real reach stays inside it.
 */
export function closurePreparationBound(
  catalog: CatalogView,
  rootTableId: number,
): {
  readonly bytes: number;
  readonly tables: ReadonlySet<number>;
  readonly triggers: ReadonlySet<number>;
  readonly groupingSets: boolean;
} {
  const budgets = new Map<number, number>(),
    queue: number[] = [];
  const push = (id: number, budget: number) => {
    if (budget < -1) return;
    if ((budgets.get(id) ?? -2) >= budget) return;
    budgets.set(id, budget);
    queue.push(id);
  };
  push(rootTableId, 1);
  const triggers = new Set<number>();
  while (queue.length) {
    const id = queue.pop() ?? -1,
      budget = budgets.get(id) ?? -2;
    for (const trigger of catalog.triggers) {
      if (trigger.tableId !== id || budget < 0) continue;
      triggers.add(trigger.id);
      for (const body of trigger.statements) push(body.targetTableId, budget - 1);
    }
    for (const child of catalog.tables)
      for (const fk of child.foreignKeys) if (fk.parentTableId === id) push(child.id, budget);
  }
  let bytes = 0,
    bodies = 0,
    groupingSets = false;
  for (const id of budgets.keys()) {
    const table = catalog.table(id);
    if (table) bytes += rulePreparationBound(table);
  }
  for (const trigger of catalog.triggers) {
    if (!triggers.has(trigger.id)) continue;
    for (const body of trigger.statements) {
      bodies++;
      const prepared = bodyPreparation(body, catalog);
      bytes += prepared.bytes;
      if (prepared.groupingSets) groupingSets = true;
    }
  }
  bytes += containerPreparationBound(budgets.size, triggers.size, bodies);
  return { bytes: checked(bytes), tables: new Set(budgets.keys()), triggers, groupingSets };
}

function checked(bytes: number): number {
  if (!Number.isSafeInteger(bytes)) throw new RangeError("Native preparation bound is too large");
  return bytes;
}
