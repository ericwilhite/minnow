import { dateMilliseconds } from "../date-value.js";
import { parseJson } from "../block-format/json-parse.js";
import type { TableColumnRecord, TableRecord } from "../storage/types.js";
import type { BatchValue } from "./batch.js";
import { getUniqueKeyColumn, keyToken, secondaryTupleComponent } from "./index-terms.js";
import { maybeYieldToEventLoop } from "../work-slicer.js";
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
  expiresAt: "expires_at",
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
      column(KEY_FORWARD_COLUMNS.expiresAt, "number"),
    ],
    uniqueKeyColumnId: keyColumn.id,
    // Like any table created with a key: writes check keys through the store's key index
    // instead of reading the table.
    uniqueKeyLookupReady: true,
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
  /**
   * When the forward stops, in epoch milliseconds: the retirement time plus the retention of
   * the database that retired the key. Fixed when written, so every connection agrees on it.
   */
  readonly expiresAt: number;
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
  const parsed: unknown = parseJson(text);
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
  const expiresAt = row[KEY_FORWARD_COLUMNS.expiresAt];
  if (
    typeof key !== "string" ||
    typeof tableId !== "string" ||
    !key.startsWith(`${tableId}\u0000`) ||
    typeof from !== "string" ||
    typeof to !== "string" ||
    typeof fromValues !== "string" ||
    typeof toValues !== "string" ||
    typeof expiresAt !== "number"
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
    expiresAt,
  };
}

/**
 * Lookup over forwards. Forwards are immutable: a rekey writes one forward from the key it
 * retired to the key it gave the row, and never rewrites an older one. A row rekeyed twice,
 * A to B and then B to C, keeps both, and A reaches the row by following A → B → C.
 */
export interface KeyForwardLookup {
  /** True when there are no forwards at all, so nothing can be followed. */
  isEmpty(): boolean;
  /** True when any unexpired forward exists for the table. */
  hasTable(tableId: string): boolean;
  /** The one unexpired forward stored for a retired key: a single step of a chain. */
  hop(tableId: string, token: string): KeyForward | undefined;
  /**
   * Where a retired key leads: its chain of unexpired forwards followed to the row's current
   * key, as one forward from the key asked about to that current key. Undefined when the key
   * was never retired or its first step has expired.
   */
  get(tableId: string, token: string): KeyForward | undefined;
}

/** Longest chain followed; a row rekeyed this often in one retention window is unheard of. */
const MAX_FORWARD_HOPS = 64;

/**
 * Follows a retired key's chain of single steps. A chain ends at a key with no unexpired forward
 * of its own: the row's current key, or the last key it held before a later step expired. A
 * step back to a key already visited — possible only once some forward has expired and its key
 * was used again — ends the chain there too.
 */
export function followForwards(
  hop: (tableId: string, token: string) => KeyForward | undefined,
  tableId: string,
  token: string,
): KeyForward | undefined {
  const first = hop(tableId, token);
  if (first === undefined) return undefined;
  let last = first;
  let expiresAt = first.expiresAt;
  let visited: Set<string> | undefined;
  for (let step = 1; step < MAX_FORWARD_HOPS; step += 1) {
    const next = storageKeyToken(last.to);
    const forward = hop(tableId, next);
    if (forward === undefined) break;
    visited ??= new Set([token]);
    if (visited.has(next)) break;
    visited.add(next);
    last = forward;
    expiresAt = Math.min(expiresAt, forward.expiresAt);
  }
  if (last === first) return first;
  return { ...first, to: last.to, toValues: last.toValues, expiresAt };
}

/** A set of forwards, indexed by table and retired key. */
export class KeyForwardSet {
  readonly #byTable = new Map<string, Map<string, KeyForward>>();

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
  }

  delete(tableId: string, fromToken: string): void {
    const forwards = this.#byTable.get(tableId);
    const existing = forwards?.get(fromToken);
    if (forwards === undefined || existing === undefined) return;
    forwards.delete(fromToken);
    if (forwards.size === 0) this.#byTable.delete(tableId);
  }

  hasTable(tableId: string): boolean {
    return this.#byTable.has(tableId);
  }

  tableIds(): IterableIterator<string> {
    return this.#byTable.keys();
  }

  isEmpty(): boolean {
    return this.#byTable.size === 0;
  }

  get(tableId: string, token: string): KeyForward | undefined {
    return this.#byTable.get(tableId)?.get(token);
  }

  *entries(): IterableIterator<KeyForward> {
    for (const forwards of this.#byTable.values()) yield* forwards.values();
  }

  /** An independent copy, for a savepoint to restore. */
  clone(): KeyForwardSet {
    const copy = new KeyForwardSet();
    for (const forward of this.entries()) copy.add(forward);
    return copy;
  }
}

/** Counts for tests: how often a connection read the forwards table whole. */
export const keyForwardTestHooks = { wholeReads: 0 };

/** One forward a commit window wrote (`forward`) or deleted (`forward: null`), by its row. */
export interface KeyForwardChange {
  readonly tableId: string;
  /** The retired storage key's token. */
  readonly token: string;
  readonly forward: KeyForward | null;
}

/** The forwards one commit window changed, layered over the generation before it. */
interface KeyForwardLayer {
  readonly written: KeyForwardSet;
  /** Row keys the window deleted and did not write again. */
  readonly deleted: ReadonlySet<string>;
  /** Expiries the window added and removed, ascending, so expired forwards count cheaply. */
  readonly addedExpiries: Float64Array;
  readonly removedExpiries: Float64Array;
}

/** A generation folds its layers into a new base past this many layers… */
const KEY_FORWARD_MAX_LAYERS = 16;
/** …or once they hold this many forwards, or an eighth of the base if that is more. */
const KEY_FORWARD_MIN_LAYERED = 1024;
/** How many generations back a key-change diff may walk before it compares in full. */
const KEY_FORWARD_HISTORY = 32;

function countAtOrBelow(sorted: Float64Array, limit: number): number {
  let low = 0;
  let high = sorted.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((sorted[middle] ?? 0) <= limit) low = middle + 1;
    else high = middle;
  }
  return low;
}

function sortedExpiries(values: readonly number[]): Float64Array {
  return Float64Array.from(values).sort();
}

/**
 * A fold's expiries: the base's, with every layer's additions merged in and removals taken
 * out, in one linear pass over arrays that are already sorted rather than a sort of them all.
 * Undefined if the removals do not match, which a consistent history never produces.
 */
function foldedExpiries(
  base: Float64Array,
  layers: readonly KeyForwardLayer[],
): Float64Array | undefined {
  const added = sortedExpiries(layers.flatMap((layer) => [...layer.addedExpiries]));
  const removed = sortedExpiries(layers.flatMap((layer) => [...layer.removedExpiries]));
  const folded = new Float64Array(base.length + added.length - removed.length);
  let fromBase = 0;
  let fromAdded = 0;
  let nextRemoved = 0;
  let length = 0;
  while (fromBase < base.length || fromAdded < added.length) {
    const baseValue = base[fromBase] ?? Number.POSITIVE_INFINITY;
    const addedValue = added[fromAdded] ?? Number.POSITIVE_INFINITY;
    let value: number;
    if (baseValue <= addedValue) {
      value = baseValue;
      fromBase += 1;
    } else {
      value = addedValue;
      fromAdded += 1;
    }
    if (nextRemoved < removed.length && removed[nextRemoved] === value) {
      nextRemoved += 1;
      continue;
    }
    if (length === folded.length) return undefined;
    folded[length] = value;
    length += 1;
  }
  return nextRemoved === removed.length && length === folded.length ? folded : undefined;
}

/**
 * Committed forwards as of one generation of the internal table, minus expired ones.
 *
 * A generation is a base, built by reading the whole table, plus a layer per commit window
 * since, built from just the rows that window changed. A rekey therefore costs the reader the
 * forwards it wrote, not every forward held. Layers fold into a new base, a slice at a time,
 * once they are many or large. Every generation is immutable once built: a query still reading
 * an older manifest version keeps the forwards that version had.
 */
export class CommittedKeyForwards implements KeyForwardLookup {
  readonly #base: KeyForwardSet;
  /** Every base forward's expiry, ascending. */
  readonly #baseExpiries: Float64Array;
  /** Oldest first. */
  readonly #layers: readonly KeyForwardLayer[];
  /** The generation this one was layered over, with the forwards written since; for diffs. */
  #previous: CommittedKeyForwards | undefined;
  readonly #written: readonly KeyForward[];

  private constructor(
    readonly generation: string,
    readonly now: () => number,
    base: KeyForwardSet,
    baseExpiries: Float64Array,
    layers: readonly KeyForwardLayer[],
    previous: CommittedKeyForwards | undefined,
    written: readonly KeyForward[],
  ) {
    this.#base = base;
    this.#baseExpiries = baseExpiries;
    this.#layers = layers;
    this.#previous = previous;
    this.#written = written;
  }

  /** A generation read whole from the table, offering the event loop turns as it indexes. */
  static async load(
    generation: string,
    forwards: Iterable<KeyForward>,
    now: () => number,
  ): Promise<CommittedKeyForwards> {
    const expiries: number[] = [];
    const base = await CommittedKeyForwards.#index(forwards, expiries);
    return new CommittedKeyForwards(
      generation,
      now,
      base,
      sortedExpiries(expiries),
      [],
      undefined,
      [],
    );
  }

  /** Indexes forwards a slice at a time, collecting their expiries as it goes. */
  static async #index(forwards: Iterable<KeyForward>, expiries: number[]): Promise<KeyForwardSet> {
    const base = new KeyForwardSet();
    let count = 0;
    for (const forward of forwards) {
      base.add(forward);
      expiries.push(forward.expiresAt);
      count += 1;
      if (count % 256 === 0) await maybeYieldToEventLoop();
    }
    return base;
  }

  /**
   * The next generation: this one with one commit window's changes layered on. Folds the
   * layers into a new base, yielding between slices, once they outgrow their bounds.
   */
  async next(
    generation: string,
    changes: readonly KeyForwardChange[],
  ): Promise<CommittedKeyForwards> {
    const written = new KeyForwardSet();
    const deleted = new Set<string>();
    const added: number[] = [];
    const removed: number[] = [];
    const writtenList: KeyForward[] = [];
    for (const { tableId, token, forward } of changes) {
      const before = this.#raw(tableId, token);
      if (before !== undefined) removed.push(before.expiresAt);
      const rowKey = forwardRowKey(tableId, token);
      if (forward === null) {
        written.delete(tableId, token);
        if (before !== undefined) deleted.add(rowKey);
        continue;
      }
      deleted.delete(rowKey);
      written.add(forward);
      added.push(forward.expiresAt);
      writtenList.push(forward);
    }
    const layers = [
      ...this.#layers,
      {
        written,
        deleted,
        addedExpiries: sortedExpiries(added),
        removedExpiries: sortedExpiries(removed),
      },
    ];
    let layered = 0;
    for (const layer of layers) layered += layer.written.size + layer.deleted.size;
    let next: CommittedKeyForwards;
    if (
      layers.length <= KEY_FORWARD_MAX_LAYERS &&
      layered <= Math.max(KEY_FORWARD_MIN_LAYERED, this.#baseExpiries.length / 8)
    ) {
      next = new CommittedKeyForwards(
        generation,
        this.now,
        this.#base,
        this.#baseExpiries,
        layers,
        this,
        writtenList,
      );
    } else {
      const layeredView = new CommittedKeyForwards(
        generation,
        this.now,
        this.#base,
        this.#baseExpiries,
        layers,
        undefined,
        [],
      );
      const expiries: number[] = [];
      const base = await CommittedKeyForwards.#index(layeredView.entries(), expiries);
      next = new CommittedKeyForwards(
        generation,
        this.now,
        base,
        foldedExpiries(this.#baseExpiries, layers) ?? sortedExpiries(expiries),
        [],
        this,
        writtenList,
      );
    }
    // Keep a bounded history, so old generations do not stay reachable forever.
    let cursor: CommittedKeyForwards | undefined = next;
    for (let depth = 0; depth < KEY_FORWARD_HISTORY && cursor !== undefined; depth += 1) {
      cursor = cursor.#previous;
    }
    if (cursor !== undefined) cursor.#previous = undefined;
    return next;
  }

  /**
   * The forwards written between `prior` and this generation that are still current, or
   * undefined when `prior` is too far back to walk to and the caller must compare in full.
   */
  writtenSince(prior: CommittedKeyForwards): KeyForward[] | undefined {
    const windows: Array<readonly KeyForward[]> = [];
    if (this !== prior) {
      windows.push(this.#written);
      let cursor = this.#previous;
      while (cursor !== prior) {
        if (cursor === undefined) return undefined;
        windows.push(cursor.#written);
        cursor = cursor.#previous;
      }
    }
    const seen = new Set<KeyForward>();
    const current: KeyForward[] = [];
    for (const window of windows.reverse()) {
      for (const forward of window) {
        if (seen.has(forward)) continue;
        seen.add(forward);
        if (this.#raw(forward.tableId, storageKeyToken(forward.from)) === forward) {
          current.push(forward);
        }
      }
    }
    return current;
  }

  /** The stored forward for a retired key, expired or not. */
  #raw(tableId: string, token: string): KeyForward | undefined {
    for (let index = this.#layers.length - 1; index >= 0; index -= 1) {
      const layer = this.#layers[index];
      if (layer === undefined) continue;
      const written = layer.written.get(tableId, token);
      if (written !== undefined) return written;
      if (layer.deleted.has(forwardRowKey(tableId, token))) return undefined;
    }
    return this.#base.get(tableId, token);
  }

  /** The stored forward for a retired key, expired or not; what a diff compares. */
  stored(tableId: string, token: string): KeyForward | undefined {
    return this.#raw(tableId, token);
  }

  /** A batch reads one immutable generation and evaluates retention at one instant. */
  atTime(now: number): KeyForwardLookup {
    const hop = (tableId: string, token: string) => {
      const forward = this.#raw(tableId, token);
      return forward !== undefined && forward.expiresAt > now ? forward : undefined;
    };
    return {
      isEmpty: () => this.isEmpty(),
      hasTable: (tableId) => this.hasTable(tableId),
      hop,
      get: (tableId, token) => followForwards(hop, tableId, token),
    };
  }

  /** How many stored forwards have expired by now; it only grows until the table next changes. */
  expiredCount(): number {
    const now = this.now();
    let count = countAtOrBelow(this.#baseExpiries, now);
    for (const layer of this.#layers) {
      count += countAtOrBelow(layer.addedExpiries, now);
      count -= countAtOrBelow(layer.removedExpiries, now);
    }
    return count;
  }

  #live(forward: KeyForward | undefined): KeyForward | undefined {
    return forward !== undefined && forward.expiresAt > this.now() ? forward : undefined;
  }

  /** True when the table may have forwards; a deletion since the last fold can leave it true. */
  hasTable(tableId: string): boolean {
    return (
      this.#base.hasTable(tableId) || this.#layers.some((layer) => layer.written.hasTable(tableId))
    );
  }

  isEmpty(): boolean {
    return this.#base.isEmpty() && this.#layers.every((layer) => layer.written.isEmpty());
  }

  /** The tables that may have forwards; like `hasTable`, a superset after deletions. */
  tableIds(): Set<string> {
    const ids = new Set(this.#base.tableIds());
    for (const layer of this.#layers) for (const id of layer.written.tableIds()) ids.add(id);
    return ids;
  }

  hop(tableId: string, token: string): KeyForward | undefined {
    return this.#live(this.#raw(tableId, token));
  }

  get(tableId: string, token: string): KeyForward | undefined {
    return followForwards((table, key) => this.hop(table, key), tableId, token);
  }

  /**
   * Up to `limit` stored forwards past their expiry, for maintenance to delete. Reads the
   * forwards this generation already holds, a slice at a time, instead of scanning the table.
   */
  async expired(limit: number): Promise<KeyForward[]> {
    const now = this.now();
    const found: KeyForward[] = [];
    let visited = 0;
    for (const forward of this.entries()) {
      if (forward.expiresAt <= now) {
        found.push(forward);
        if (found.length >= limit) break;
      }
      visited += 1;
      if (visited % 1024 === 0) await maybeYieldToEventLoop();
    }
    return found;
  }

  /** Every stored forward, expired or not, newest layer first. */
  *entries(): IterableIterator<KeyForward> {
    const seen = new Set<string>();
    for (let index = this.#layers.length - 1; index >= 0; index -= 1) {
      const layer = this.#layers[index];
      if (layer === undefined) continue;
      for (const forward of layer.written.entries()) {
        const rowKey = forwardRowKey(forward.tableId, storageKeyToken(forward.from));
        if (seen.has(rowKey)) continue;
        seen.add(rowKey);
        yield forward;
      }
      for (const rowKey of layer.deleted) seen.add(rowKey);
    }
    for (const forward of this.#base.entries()) {
      if (
        seen.size > 0 &&
        seen.has(forwardRowKey(forward.tableId, storageKeyToken(forward.from)))
      ) {
        continue;
      }
      yield forward;
    }
  }
}

/**
 * A write scope's view: forwards it staged over the committed ones. A chain may run through
 * both — a committed A → B and a B → C this scope staged.
 */
export class ScopedKeyForwards implements KeyForwardLookup {
  staged = new KeyForwardSet();

  constructor(readonly committed: () => KeyForwardLookup | undefined) {}

  hasTable(tableId: string): boolean {
    return this.staged.hasTable(tableId) || this.committed()?.hasTable(tableId) === true;
  }

  isEmpty(): boolean {
    return this.staged.isEmpty() && this.committed()?.isEmpty() !== false;
  }

  hop(tableId: string, token: string): KeyForward | undefined {
    return this.staged.get(tableId, token) ?? this.committed()?.hop(tableId, token);
  }

  get(tableId: string, token: string): KeyForward | undefined {
    return followForwards((table, key) => this.hop(table, key), tableId, token);
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
