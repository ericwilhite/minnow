/** Portable FTS definitions. Physical roots, build leases and coverage never belong here.
 * Authority: B7-fts-1.0-design.md and the agreed MCAT21/config26 field contract.
 */
import { StoreCorruptionError } from "../storage/contract.js";
import { ftsSemantics, requireFtsSemantics, type FtsSemantics } from "../fts-config.js";

export type { FtsConfig } from "../fts-config.js";
/** MCAT family21 fields1..7, in this order. IDs are stable and never reused. */
export interface FtsDef extends FtsSemantics {
  readonly id: number;
  readonly tableId: number;
  readonly columnId: number;
}

const fields = new Set([
  "id",
  "tableId",
  "columnId",
  "tokenizerVersion",
  "rendererVersion",
  "scorerVersion",
  "config",
]);
function record(value: unknown, label: string): asserts value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new StoreCorruptionError(`Invalid ${label}`);
}
function positive(value: unknown, label: string): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1)
    throw new StoreCorruptionError(`Invalid FTS ${label}`);
}

/** Shape/range corruption is distinct from a valid, unsupported semantic version.
 * Unknown optional display fields live in MCAT's opaque extension storage, not these objects.
 */
export function validateFtsDef(value: unknown): asserts value is FtsDef {
  record(value, "FTS definition");
  for (const key of Object.keys(value))
    if (!fields.has(key)) throw new StoreCorruptionError(`Unexpected FTS definition field ${key}`);
  positive(value.id, "id");
  positive(value.tableId, "table id");
  positive(value.columnId, "column id");
  positive(value.tokenizerVersion, "tokenizer version");
  positive(value.rendererVersion, "renderer version");
  positive(value.scorerVersion, "scorer version");
  record(value.config, "FTS configuration");
  if (Object.keys(value.config).some((key) => key !== "version"))
    throw new StoreCorruptionError("Unexpected FTS configuration field");
  positive(value.config.version, "configuration version");
  requireFtsSemantics(value as unknown as FtsDef);
}

/** Definitions are logical; building/ready/invalid and every BlobRef stay in native state. */
export function ftsDef(id: number, tableId: number, columnId: number): FtsDef {
  const value = {
    id,
    tableId,
    columnId,
    ...ftsSemantics(),
  };
  validateFtsDef(value);
  return value;
}
