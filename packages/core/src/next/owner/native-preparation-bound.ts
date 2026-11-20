/**
 * Static upper bounds for native trigger preparation, computed from catalog metadata before any
 * lowering or compilation runs (owner/native-command.ts admits the whole bound first).
 *
 * A trigger body is one INSERT ... VALUES, UPDATE or DELETE over a base table (lower/triggers.ts):
 * no view, FROM list or INSERT ... SELECT reaches it, and a subquery can name base tables only.
 * Lowering therefore creates at most a constant number of IR nodes per source token, plus the
 * columns, defaults, generated expressions and indexes of each table it names, plus `*`
 * expansions over those tables. String content is retained at most a constant number of bytes
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

function textBytes(scan: SqlScan): number {
  return PREPARATION_TOKEN_BYTES * scan.tokens + PREPARATION_TEXT_BYTES * scan.characters;
}

/** One named table's retained share: its columns, stored expressions and keys. */
export function tablePreparationBound(table: TableDef): number {
  let bytes = PREPARATION_TABLE_BYTES + PREPARATION_TEXT_BYTES * table.name.length;
  for (const column of table.columns) {
    bytes += PREPARATION_COLUMN_BYTES + PREPARATION_TEXT_BYTES * column.name.length;
    if (column.defaultSql !== null) bytes += textBytes(scanSql(column.defaultSql));
    if (column.generatedSql !== null) bytes += textBytes(scanSql(column.generatedSql));
  }
  for (const index of table.indexes) bytes += PREPARATION_KEY_BYTES * (1 + index.columnIds.length);
  bytes += PREPARATION_KEY_BYTES * table.foreignKeys.length;
  return checked(bytes);
}

/** A lowered body plan, its runner and binding program. */
export function bodyPreparationBound(body: TriggerBody, catalog: CatalogView): number {
  let tables = 0,
    columns = 0;
  const named = new Set<number>();
  const scan = scanSql(body.sql, (identifier) => {
    const table = catalog.tableByName(identifier);
    if (!table) return;
    // Every occurrence may become its own scan; `*` may expand any named table.
    tables += tablePreparationBound(table);
    if (!named.has(table.id)) {
      named.add(table.id);
      columns += table.columns.length;
    }
  });
  const target = catalog.table(body.targetTableId);
  if (target && !named.has(target.id)) {
    tables += tablePreparationBound(target);
    columns += target.columns.length;
  }
  return checked(
    PREPARATION_BODY_BYTES +
      textBytes(scan) +
      tables +
      scan.stars * columns * PREPARATION_COLUMN_BYTES +
      PREPARATION_BINDING_BYTES * body.bindings.length,
  );
}

/** A table's lowered rule expressions, compiled program and default entries. */
export function rulePreparationBound(table: TableDef): number {
  let bytes = PREPARATION_RULE_BYTES + tablePreparationBound(table);
  for (const check of table.checks)
    bytes +=
      PREPARATION_TABLE_BYTES / 4 +
      PREPARATION_TEXT_BYTES * check.name.length +
      2 * textBytes(scanSql(check.conditionSql));
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

function checked(bytes: number): number {
  if (!Number.isSafeInteger(bytes)) throw new RangeError("Native preparation bound is too large");
  return bytes;
}
