/**
 * The planner port: what the composition root (compose.ts) needs from lowering. It is the shape
 * of the lowering track's `Lowerer` (B6, lower/lowerer.ts on rebuild/lower: `lower`,
 * `lowerWrite`, `LoweredStatement`), cut to the fields the owner reads, so B6's implementation
 * satisfies it structurally when it lands. Until then tests use the stand-in planner
 * (testing/owner-standins.ts).
 *
 * Planning is synchronous and runs the whole front end (parse, catalog rewrites, lowering) over
 * one catalog version. The composition caches plans by (text, catalog version).
 */
import type { CatalogChange, CatalogView } from "../access/catalog.js";
import type { PhysicalPlan } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import type { WireColumnMeta } from "./protocol.js";

export interface PlannerContext {
  readonly catalog: CatalogView;
}

interface LoweredBase {
  readonly text: string;
  /** The plan is valid only at this catalog version. */
  readonly catalogVersion: number;
  /** PostgreSQL's command tag word(s): `SELECT`, `INSERT`, `CREATE TABLE`, `BEGIN`, … */
  readonly command: string;
  /** The statement may open, end or change a transaction. */
  readonly controlsTransaction: boolean;
  /** Types of the `$n` placeholders, `$1` first. */
  readonly parameters: readonly PhysicalType[];
}

export interface LoweredQuery extends LoweredBase {
  readonly kind: "query";
  readonly plan: PhysicalPlan;
  readonly columns: readonly WireColumnMeta[];
  /** Every table id the plan reads. */
  readonly reads: readonly number[];
  /** Only `keyLookup` with constant or parameter keys: the owner tries the synchronous path. */
  readonly pointRead: boolean;
}

export interface LoweredModify extends LoweredBase {
  readonly kind: "modify";
  readonly plan: PhysicalPlan;
  /** RETURNING columns; empty without RETURNING. */
  readonly columns: readonly WireColumnMeta[];
  readonly reads: readonly number[];
  readonly writes: readonly number[];
}

export interface LoweredDdl extends LoweredBase {
  readonly kind: "ddl";
  /** Null when the statement is a no-op at this catalog version (IF [NOT] EXISTS). */
  readonly change: CatalogChange | null;
  /** SQL run after `change` in the same statement and transaction (CREATE TABLE AS), or null. */
  readonly followUp: string | null;
}

export interface LoweredTransactionControl extends LoweredBase {
  readonly kind: "transactionControl";
  readonly action: "begin" | "commit" | "rollback" | "savepoint" | "rollbackTo" | "release";
  readonly savepoint: string | null;
  readonly readOnly: boolean;
}

export interface LoweredUtility extends LoweredBase {
  readonly kind: "utility";
  /** SHOW's one-row answer, or null for SET/RESET (accepted and ignored). */
  readonly show: { readonly column: WireColumnMeta; readonly value: string } | null;
}

export type LoweredStatement =
  LoweredQuery | LoweredModify | LoweredDdl | LoweredTransactionControl | LoweredUtility;

/** A batch write (protocol.ts `WriteRequest`) as the planner sees it. */
export interface WriteShape {
  readonly table: string;
  readonly op: "insert" | "upsert" | "update" | "delete";
  readonly names: readonly string[];
}

export interface StatementPlanner {
  /** Lowers one SQL statement. Throws SQL errors (syntax, unknown table, …). */
  lower(text: string, context: PlannerContext): LoweredStatement;
  /** A batch write as a `modify` plan whose input is the client's rows. */
  lowerWrite(shape: WriteShape, context: PlannerContext): LoweredModify;
}
