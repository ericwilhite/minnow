import { dateMilliseconds } from "../date-value.js";
import type { TableColumnRecord, TableRecord } from "../storage/types.js";
import type { BatchValue } from "./batch.js";
import { getUniqueKeyColumn, keyToken, secondaryTupleComponent } from "./index-terms.js";
import { KEY_FORWARDS_TABLE_ID } from "./key-change.js";

export { KEY_FORWARDS_TABLE_ID, type KeyChange } from "./key-change.js";

/**
 * Rekeying: a row's primary key may change, and the engine keeps a forward from every retired
 * key to the row's current one. Forwards live in one internal table, so they commit with the
 * rekey that wrote them, reach every tab the way any commit does, and survive a reload.
 *
 * A forward is keyed by the retired row's storage key — the scalar key itself, or a composite
 * key's hidden locator — so batch keys, SQL key equalities, and foreign-key values all resolve
 * through the same token. Each forward also carries both public key tuples, which is what a
 * key-change notice reports.
 */

/** The internal table's name. The leading NUL keeps it out of reach of user SQL and listings. */
export const KEY_FORWARDS_TABLE = "\u0000minnow_key_forwards";

/** How long a retired key keeps forwarding when the database is opened without an override. */
export const DEFAULT_KEY_FORWARD_RETENTION_MS = 10 * 24 * 60 * 60 * 1000;

/**
 * The internal table's columns. `key` is `{tableId}\0{retired storage token}`; `from_key` and
 * `to_key` hold the retired and current storage keys, and the `_values` columns the public tuples.
 */
export const KEY_FORWARD_COLUMNS = {
  key: "key",
  table: "table_id",
  from: "from_key",
  to: "to_key",
  fromValues: "from_values",
  toValues: "to_values",
  retiredAt: "retired_at",
} as const;

/** The internal table's catalog record: a constant apart from its creation time. */
export function keyForwardsTableRecord(createdAt: string): TableRecord {
  const column = (name: string, type: TableColumnRecord["type"]): TableColumnRecord => ({
    id: `${KEY_FORWARDS_TABLE_ID}/${name}`,
    name,
    type,
    nullable: false,
  });
  const keyColumn = column(KEY_FORWARD_COLUMNS.key, "string");
  return {
    id: KEY_FORWARDS_TABLE_ID,
    name: KEY_FORWARDS_TABLE,
    columns: [
      keyColumn,
      column(KEY_FORWARD_COLUMNS.table, "string"),
      column(KEY_FORWARD_COLUMNS.from, "string"),
      column(KEY_FORWARD_COLUMNS.to, "string"),
      column(KEY_FORWARD_COLUMNS.fromValues, "string"),
      column(KEY_FORWARD_COLUMNS.toValues, "string"),
      column(KEY_FORWARD_COLUMNS.retiredAt, "number"),
    ],
    uniqueKeyColumnId: keyColumn.id,
    primaryKeyColumnIds: [keyColumn.id],
    managed: false,
    keyForwards: true,
    revision: 0,
    createdAt,
  };
}

/** One retired key and where its row lives now. */
export interface KeyForward {
  readonly tableId: string;
  /** The retired storage key. */
  readonly from: Exclude<BatchValue, null>;
  /** The row's current storage key. */
  readonly to: Exclude<BatchValue, null>;
  /** The retired public key, one value per primary-key column. */
  readonly fromValues: readonly BatchValue[];
  /** The current public key, one value per primary-key column. */
  readonly toValues: readonly BatchValue[];
  /** When the key was retired, in epoch milliseconds. */
  readonly retiredAt: number;
}

/**
 * How a table addresses its rows: the storage key column, and the public primary-key columns
 * that determine it. A table without a public key (an append-only table addressed by a hidden
 * row id) cannot be rekeyed and has no address.
 */
export interface TableAddress {
  readonly keyColumn: TableColumnRecord;
  readonly columns: readonly TableColumnRecord[];
}

export function tableAddress(table: TableRecord): TableAddress | undefined {
  const keyColumn = getUniqueKeyColumn(table);
  if (keyColumn === undefined) return undefined;
  if (keyColumn.hidden !== true) return { keyColumn, columns: [keyColumn] };
  const columns = (table.primaryKeyColumnIds ?? []).flatMap((columnId) => {
    const column = table.columns.find(
      (candidate) => candidate.id === columnId && candidate.hidden !== true,
    );
    return column === undefined ? [] : [column];
  });
  return columns.length === 0 ? undefined : { keyColumn, columns };
}

/** The storage key a public key tuple addresses: the value itself, or the composite locator. */
export function addressStorageKey(
  address: TableAddress,
  values: readonly BatchValue[],
): Exclude<BatchValue, null> {
  if (values.length !== address.columns.length || values.some((value) => value === null)) {
    throw new TypeError("A key needs one non-null value per primary-key column");
  }
  const only = values[0];
  if (address.keyColumn.hidden !== true && only !== undefined && only !== null) return only;
  return address.columns
    .map((column, index) => secondaryTupleComponent(column.type, values[index] ?? null))
    .join("");
}

/** A storage key's token, read off the value's own type. */
export function storageKeyToken(value: Exclude<BatchValue, null>): string {
  if (value instanceof Date) return keyToken("datetime", value);
  switch (typeof value) {
    case "string":
      return keyToken("string", value);
    case "number":
      return keyToken("number", value);
    default:
      return keyToken("boolean", value);
  }
}

/** Encodes a key tuple to text that decodes to equal values, dates included. */
export function encodeKeyValues(values: readonly BatchValue[]): string {
  return JSON.stringify(
    values.map((value) => (value instanceof Date ? { $d: dateMilliseconds(value) } : value)),
  );
}

export function decodeKeyValues(text: string): BatchValue[] {
  const parsed: unknown = JSON.parse(text);
  if (!Array.isArray(parsed)) throw new TypeError("Key forward values are invalid");
  return parsed.map((value: unknown) => {
    if (value === null || typeof value === "string" || typeof value === "boolean") return value;
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (
      typeof value === "object" &&
      Object.keys(value).length === 1 &&
      typeof (value as { $d?: unknown }).$d === "number"
    ) {
      return new Date((value as { $d: number }).$d);
    }
    throw new TypeError("Key forward values are invalid");
  });
}

export function forwardRowKey(tableId: string, fromToken: string): string {
  return `${tableId}\u0000${fromToken}`;
}

/** Reads a forward back from the internal table's row. */
export function forwardFromRow(row: Readonly<Record<string, unknown>>): KeyForward {
  const key = row[KEY_FORWARD_COLUMNS.key];
  const tableId = row[KEY_FORWARD_COLUMNS.table];
  const from = row[KEY_FORWARD_COLUMNS.from];
  const to = row[KEY_FORWARD_COLUMNS.to];
  const fromValues = row[KEY_FORWARD_COLUMNS.fromValues];
  const toValues = row[KEY_FORWARD_COLUMNS.toValues];
  const retiredAt = row[KEY_FORWARD_COLUMNS.retiredAt];
  if (
    typeof key !== "string" ||
    typeof tableId !== "string" ||
    !key.startsWith(`${tableId}\u0000`) ||
    typeof from !== "string" ||
    typeof to !== "string" ||
    typeof fromValues !== "string" ||
    typeof toValues !== "string" ||
    typeof retiredAt !== "number"
  ) {
    throw new TypeError("A key forward row is invalid");
  }
  const [toKey] = decodeKeyValues(to);
  const [fromKey] = decodeKeyValues(from);
  if (toKey === null || toKey === undefined || fromKey === null || fromKey === undefined) {
    throw new TypeError("A key forward row is invalid");
  }
  return {
    tableId,
    from: fromKey,
    to: toKey,
    fromValues: decodeKeyValues(fromValues),
    toValues: decodeKeyValues(toValues),
    retiredAt,
  };
}

/** Lookup over forwards: retired token to its forward, and current token to its sources. */
export interface KeyForwardLookup {
  /** True when there are no forwards at all, so nothing can be followed. */
  isEmpty(): boolean;
  /** True when any unexpired forward exists for the table. */
  hasTable(tableId: string): boolean;
  get(tableId: string, token: string): KeyForward | undefined;
  /** Every unexpired forward whose target is this token. */
  sourcesOf(tableId: string, toToken: string): KeyForward[];
}

/** A set of forwards indexed both ways. */
export class KeyForwardSet {
  readonly #byTable = new Map<string, Map<string, KeyForward>>();
  readonly #byTarget = new Map<string, Map<string, Set<string>>>();

  get size(): number {
    let size = 0;
    for (const forwards of this.#byTable.values()) size += forwards.size;
    return size;
  }

  add(forward: KeyForward): void {
    const fromToken = storageKeyToken(forward.from);
    this.delete(forward.tableId, fromToken);
    let forwards = this.#byTable.get(forward.tableId);
    if (forwards === undefined) {
      forwards = new Map();
      this.#byTable.set(forward.tableId, forwards);
    }
    forwards.set(fromToken, forward);
    let targets = this.#byTarget.get(forward.tableId);
    if (targets === undefined) {
      targets = new Map();
      this.#byTarget.set(forward.tableId, targets);
    }
    const toToken = storageKeyToken(forward.to);
    let sources = targets.get(toToken);
    if (sources === undefined) {
      sources = new Set();
      targets.set(toToken, sources);
    }
    sources.add(fromToken);
  }

  delete(tableId: string, fromToken: string): void {
    const forwards = this.#byTable.get(tableId);
    const existing = forwards?.get(fromToken);
    if (forwards === undefined || existing === undefined) return;
    forwards.delete(fromToken);
    if (forwards.size === 0) this.#byTable.delete(tableId);
    const targets = this.#byTarget.get(tableId);
    const toToken = storageKeyToken(existing.to);
    const sources = targets?.get(toToken);
    sources?.delete(fromToken);
    if (sources?.size === 0) targets?.delete(toToken);
    if (targets?.size === 0) this.#byTarget.delete(tableId);
  }

  hasTable(tableId: string): boolean {
    return this.#byTable.has(tableId);
  }

  isEmpty(): boolean {
    return this.#byTable.size === 0;
  }

  get(tableId: string, token: string): KeyForward | undefined {
    return this.#byTable.get(tableId)?.get(token);
  }

  sourcesOf(tableId: string, toToken: string): KeyForward[] {
    const forwards = this.#byTable.get(tableId);
    const sources = this.#byTarget.get(tableId)?.get(toToken);
    if (forwards === undefined || sources === undefined) return [];
    return [...sources].flatMap((token) => {
      const forward = forwards.get(token);
      return forward === undefined ? [] : [forward];
    });
  }

  *entries(): IterableIterator<KeyForward> {
    for (const forwards of this.#byTable.values()) yield* forwards.values();
  }
}

/** Committed forwards as of one generation of the internal table, minus expired ones. */
export class CommittedKeyForwards implements KeyForwardLookup {
  constructor(
    readonly generation: string,
    readonly forwards: KeyForwardSet,
    readonly expiresBefore: () => number,
  ) {}

  #live(forward: KeyForward | undefined): KeyForward | undefined {
    return forward !== undefined && forward.retiredAt >= this.expiresBefore() ? forward : undefined;
  }

  hasTable(tableId: string): boolean {
    return this.forwards.hasTable(tableId);
  }

  isEmpty(): boolean {
    return this.forwards.isEmpty();
  }

  get(tableId: string, token: string): KeyForward | undefined {
    return this.#live(this.forwards.get(tableId, token));
  }

  sourcesOf(tableId: string, toToken: string): KeyForward[] {
    const cutoff = this.expiresBefore();
    return this.forwards
      .sourcesOf(tableId, toToken)
      .filter((forward) => forward.retiredAt >= cutoff);
  }

  /** Forwards past retention, which maintenance deletes. */
  expired(): KeyForward[] {
    const cutoff = this.expiresBefore();
    return [...this.forwards.entries()].filter((forward) => forward.retiredAt < cutoff);
  }
}

/**
 * A write scope's view: forwards it staged over the committed ones. A staged forward replaces a
 * committed one with the same retired key, which is how a chain collapses inside a scope.
 */
export class ScopedKeyForwards implements KeyForwardLookup {
  readonly staged = new KeyForwardSet();
  /** The rows each rekey moved, in order, for the commit's key-change notice. */
  readonly changes: KeyForward[] = [];

  constructor(readonly committed: () => KeyForwardLookup | undefined) {}

  hasTable(tableId: string): boolean {
    return this.staged.hasTable(tableId) || this.committed()?.hasTable(tableId) === true;
  }

  isEmpty(): boolean {
    return this.staged.isEmpty() && this.committed()?.isEmpty() !== false;
  }

  get(tableId: string, token: string): KeyForward | undefined {
    return this.staged.get(tableId, token) ?? this.committed()?.get(tableId, token);
  }

  sourcesOf(tableId: string, toToken: string): KeyForward[] {
    const staged = this.staged.sourcesOf(tableId, toToken);
    const seen = new Set(staged.map((forward) => storageKeyToken(forward.from)));
    const committed = (this.committed()?.sourcesOf(tableId, toToken) ?? []).filter(
      (forward) =>
        !seen.has(storageKeyToken(forward.from)) &&
        this.staged.get(tableId, storageKeyToken(forward.from)) === undefined,
    );
    return [...staged, ...committed];
  }
}

/** Follows a retired storage key to the current one; returns the input when nothing forwards. */
export function forwardStorageKey(
  lookup: KeyForwardLookup | undefined,
  tableId: string,
  value: Exclude<BatchValue, null>,
): KeyForward | undefined {
  if (!lookup?.hasTable(tableId)) return undefined;
  return lookup.get(tableId, storageKeyToken(value));
}
