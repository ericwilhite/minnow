/**
 * A table's row rules in IR form: what lowering produces from the catalog's SQL text
 * (`lower/` `Lowerer.lowerTableRules`) and the executor compiles (`exec/rules.ts`
 * `compileTableRules(table, catalogVersion, expressions)`, ruling R14.3). It lives in ir/ because
 * lower/ and exec/ may not import each other.
 *
 * ## Shape
 * - Every expression is positional over the table's columns in catalog order
 *   (access/catalog.ts `TableDef.columns`): `{ op: "column", index: i }` is column i, the layout of
 *   every batch the rules see.
 * - No parameter slots. A rule reads its row and nothing else, except `nextval` in a default,
 *   which is a `call` with `fn: "nextval"` and the sequence id as an int64 literal argument.
 * - Types are exact: a check is `bool`; a generated column or default has exactly its column's
 *   physical type (lowering inserts the cast PostgreSQL's assignment cast would).
 * - Lists are complete and ordered: one entry per `TableDef.checks` entry (catalog order), per
 *   column with `generatedSql`, and per column with `defaultSql` or `identitySequence`.
 */
import type { ScalarExpr } from "./ir.js";

export interface TableRuleExpressions {
  /** One per `TableDef.checks` entry, by `checkId`: a bool expression over the table's columns. */
  readonly checks: ReadonlyArray<{ readonly checkId: number; readonly condition: ScalarExpr }>;
  /** One per column with `generatedSql`, by column id: an expression of exactly its type. */
  readonly generated: ReadonlyArray<{ readonly columnId: number; readonly expr: ScalarExpr }>;
  /**
   * One per column with a default (`defaultSql`, or SERIAL / IDENTITY through its
   * `identitySequence`), by column id: an expression of exactly its type that reads no column.
   * Lowering fills these same expressions into every insert plan it builds (INSERT, MERGE's
   * INSERT, ON CONFLICT, batch writes), so the executor never applies a default on its own; the
   * list is here so one cached object holds a table's rules per catalog version and
   * `compileTableRules` can validate them. A column without an entry defaults to NULL. The
   * `missing` value of rows stored before a column existed is the write path's, not a default.
   */
  readonly defaults: ReadonlyArray<{ readonly columnId: number; readonly expr: ScalarExpr }>;
}
