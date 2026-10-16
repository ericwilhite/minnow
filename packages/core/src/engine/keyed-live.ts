import type { KeyChange } from "./key-change.js";
import { dateMilliseconds } from "../date-value.js";
import { sameLiveValue } from "./live-equal.js";
import type { LiveQuery, LiveSnapshot } from "./typed-live.js";

export type LiveResultKey = string | number | boolean | Date;

export type LiveKeyOf<TRow extends object> = {
  [TKey in keyof TRow]-?: TRow[TKey] extends LiveResultKey ? TKey : never;
}[keyof TRow];

export type LiveResultChange<TRow extends object, TKey extends keyof TRow> =
  | { readonly type: "insert"; readonly row: TRow; readonly index: number }
  | {
      readonly type: "update";
      readonly row: TRow;
      readonly previous: TRow;
      readonly index: number;
    }
  | {
      readonly type: "delete";
      readonly key: TRow[TKey];
      readonly previous: TRow;
      readonly index: number;
    }
  | {
      readonly type: "move";
      readonly key: TRow[TKey];
      readonly row: TRow;
      readonly from: number;
      readonly to: number;
    }
  /**
   * The same row under a new key: its primary key was changed (rekeyed). Reported in place of
   * the delete and insert it would otherwise look like, so a renderer keeps the row's state.
   */
  | {
      readonly type: "rekey";
      readonly from: TRow[TKey];
      readonly key: TRow[TKey];
      readonly row: TRow;
      readonly previous: TRow;
      readonly previousIndex: number;
      readonly index: number;
    };

export type LiveChangesSnapshot<TRow extends object, TKey extends keyof TRow> =
  | { readonly status: "loading"; readonly rows: readonly TRow[] }
  | {
      readonly status: "ready";
      readonly rows: readonly TRow[];
      readonly changes: ReadonlyArray<LiveResultChange<TRow, TKey>>;
      readonly initial: boolean;
      readonly version: number | null;
      /** As on `LiveSnapshot`: rows the commits behind this snapshot rekeyed. */
      readonly keyChanges?: readonly KeyChange[];
    }
  | {
      readonly status: "error";
      readonly rows: readonly TRow[];
      readonly error: unknown;
      readonly version: number | null;
    };

export interface KeyedLiveQueryOptions<TRow extends object, TKey extends LiveKeyOf<TRow>> {
  readonly key: TKey;
  /** A window refuses a result larger than this instead of retaining an accidentally unbounded query. */
  readonly maxRows?: number;
  /**
   * The table whose primary key `key` is. Naming it turns a row that table rekeyed into one
   * `rekey` change; without it a rekey shows as the delete and insert it looks like, since a key
   * value alone cannot say which table's rekey it was.
   */
  readonly table?: string;
}

/**
 * The map key for a row key. Strings, finite numbers, and booleans are their own map keys — a
 * Map tells 1 from "1" from true — so the common case builds no token at all. The values a Map
 * would merge or refuse are spelled out: -0 and 0 stay distinct keys as they always were, and a
 * Date keys by its instant under a tag no string column value can collide with, since string
 * keys are never tagged.
 */
type KeyToken = string | number | boolean | symbol;

const dateTokens = new Map<number, symbol>();

function keyToken(value: unknown, name: PropertyKey): KeyToken {
  if (typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (Number.isNaN(value)) return NaN;
    if (Object.is(value, -0)) return Symbol.for("minnow.live.key.-0");
    return value;
  }
  if (value instanceof Date) {
    const time = dateMilliseconds(value);
    if (!Number.isFinite(time)) {
      throw new TypeError(`Live query key ${String(name)} must be a valid Date`);
    }
    let token = dateTokens.get(time);
    if (token === undefined) {
      token = Symbol(`minnow.live.key.date:${String(time)}`);
      dateTokens.set(time, token);
      // Dates as keys are rare; keep the interning table from growing without bound.
      if (dateTokens.size > 65_536) dateTokens.clear();
    }
    return token;
  }
  throw new TypeError(
    `Live query key ${String(name)} must be a non-null string, number, boolean, or Date`,
  );
}

interface IndexedRow<TRow extends object> {
  readonly row: TRow;
  readonly index: number;
}

type RowIndex<TRow extends object> = Map<KeyToken, IndexedRow<TRow>>;

function indexRows<TRow extends object>(rows: readonly TRow[], key: keyof TRow): RowIndex<TRow> {
  const indexed: RowIndex<TRow> = new Map();
  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    if (row === undefined) continue;
    const token = keyToken(row[key], key);
    if (indexed.has(token)) {
      throw new TypeError(
        `Live query key ${String(key)} is not unique: duplicate at row ${String(index)}`,
      );
    }
    indexed.set(token, { row, index });
  }
  return indexed;
}

interface Diff<TRow extends object, TKey extends keyof TRow> {
  readonly changes: Array<LiveResultChange<TRow, TKey>>;
  /** The new rows, with every unchanged row keeping its previous object. */
  readonly rows: readonly TRow[];
  readonly index: RowIndex<TRow>;
}

/**
 * Keyed diff of the previous index against the new rows. The previous index is retained
 * across snapshots, so each snapshot indexes its own rows once; a row equal to its predecessor
 * keeps the predecessor's object, so identity-keyed renderers leave it alone.
 */
function diffRows<TRow extends object, TKey extends keyof TRow>(
  previous: RowIndex<TRow>,
  previousRows: readonly TRow[],
  rows: readonly TRow[],
  key: TKey,
  keyChanges: readonly KeyChange[] = [],
  table?: string,
): Diff<TRow, TKey> {
  const current = indexRows(rows, key);
  const changes: Array<LiveResultChange<TRow, TKey>> = [];
  for (const [token, old] of previous) {
    if (current.has(token)) continue;
    changes.push({ type: "delete", key: old.row[key], previous: old.row, index: old.index });
  }
  let reused = 0;
  const reconciled = new Array<TRow>(rows.length);
  for (const [token, next] of current) {
    const old = previous.get(token);
    if (old === undefined) {
      reconciled[next.index] = next.row;
      changes.push({ type: "insert", row: next.row, index: next.index });
      continue;
    }
    let kept = next.row;
    if (old.row === next.row || sameLiveValue(old.row, next.row)) {
      kept = old.row;
      if (old.row !== next.row) {
        current.set(token, { row: old.row, index: next.index });
        reused += 1;
      }
    } else {
      changes.push({ type: "update", row: next.row, previous: old.row, index: next.index });
    }
    reconciled[next.index] = kept;
    if (old.index !== next.index) {
      changes.push({
        type: "move",
        key: next.row[key],
        row: kept,
        from: old.index,
        to: next.index,
      });
    }
  }
  if (changes.length === 0 && rows.length === previousRows.length) {
    // Same keys, same order, same values: the view is what it was, array included.
    return { changes, rows: previousRows, index: current };
  }
  return {
    changes:
      table === undefined || keyChanges.length === 0
        ? changes
        : pairRekeys(
            changes,
            keyChanges.filter((change) => change.table === table),
            key,
          ),
    rows: reused === 0 ? rows : Object.freeze(reconciled),
    index: current,
  };
}

/**
 * Replaces each delete and insert that a key change, or a chain of them, connects with one
 * `rekey` entry. Only a scalar key can be matched: the result's key column is compared with the
 * change's values.
 */
function pairRekeys<TRow extends object, TKey extends keyof TRow>(
  changes: Array<LiveResultChange<TRow, TKey>>,
  keyChanges: readonly KeyChange[],
  key: TKey,
): Array<LiveResultChange<TRow, TKey>> {
  const tokenOf = (value: unknown): KeyToken | undefined => {
    try {
      return keyToken(value, key);
    } catch {
      return undefined;
    }
  };
  const targets = new Map<KeyToken, KeyToken>();
  for (const change of keyChanges) {
    if (Array.isArray(change.from) || Array.isArray(change.to)) continue;
    const from = tokenOf(change.from);
    const to = tokenOf(change.to);
    if (from !== undefined && to !== undefined) targets.set(from, to);
  }
  const inserts = new Map<KeyToken, Extract<LiveResultChange<TRow, TKey>, { type: "insert" }>>();
  for (const change of changes) {
    if (change.type === "insert") {
      const token = tokenOf(change.row[key]);
      if (token !== undefined) inserts.set(token, change);
    }
  }
  const paired = new Set<LiveResultChange<TRow, TKey>>();
  const rekeys: Array<LiveResultChange<TRow, TKey>> = [];
  for (const change of changes) {
    if (change.type !== "delete") continue;
    const from = tokenOf(change.key);
    // Several rekeys of one row can land in one delivery, each reported as its own step:
    // follow them to the key the row now has.
    let to = from === undefined ? undefined : targets.get(from);
    const seen = new Set<KeyToken | undefined>([from]);
    while (to !== undefined && !inserts.has(to) && !seen.has(to)) {
      seen.add(to);
      to = targets.get(to);
    }
    const insert = to === undefined ? undefined : inserts.get(to);
    if (insert === undefined || paired.has(insert)) continue;
    paired.add(change);
    paired.add(insert);
    rekeys.push({
      type: "rekey",
      from: change.key,
      key: insert.row[key],
      row: insert.row,
      previous: change.previous,
      previousIndex: change.index,
      index: insert.index,
    });
  }
  return rekeys.length === 0
    ? changes
    : [...changes.filter((change) => !paired.has(change)), ...rekeys];
}

/** Keyed exact diffs over a typed live query, with optional bounded-window enforcement. */
export class KeyedLiveQuery<TRow extends object, TKey extends LiveKeyOf<TRow>> {
  declare readonly $inferRow: TRow;
  declare readonly $inferKey: TRow[TKey];

  readonly #source: LiveQuery<TRow>;
  readonly #key: TKey;
  readonly #maxRows: number | undefined;
  readonly #table: string | undefined;
  readonly #listeners = new Set<() => void>();
  #sourceUnsubscribe: (() => void) | undefined;
  #rows: readonly TRow[] = [];
  /** The source array the current rows were derived from; the same array means no change. */
  #sourceRows: readonly TRow[] | undefined;
  #index: RowIndex<TRow> | undefined;
  #snapshot: LiveChangesSnapshot<TRow, TKey> = { status: "loading", rows: [] };
  #hasReadySnapshot = false;
  #closed = false;

  constructor(source: LiveQuery<TRow>, options: KeyedLiveQueryOptions<TRow, TKey>) {
    if (
      options.maxRows !== undefined &&
      (!Number.isSafeInteger(options.maxRows) || options.maxRows <= 0)
    ) {
      throw new RangeError("Live query window maxRows must be a positive whole number");
    }
    this.#source = source;
    this.#key = options.key;
    this.#maxRows = options.maxRows;
    this.#table = options.table;
  }

  getSnapshot = (): LiveChangesSnapshot<TRow, TKey> => this.#snapshot;

  subscribe = (listener: () => void): (() => void) => {
    if (this.#closed) throw new Error("Keyed live query is closed");
    const registered = (): void => listener();
    this.#listeners.add(registered);
    if (this.#listeners.size === 1) {
      this.#sourceUnsubscribe = this.#source.subscribe(() =>
        this.#accept(this.#source.getSnapshot()),
      );
      this.#accept(this.#source.getSnapshot());
    }
    let subscribed = true;
    return () => {
      if (!subscribed) return;
      subscribed = false;
      this.#listeners.delete(registered);
      if (this.#listeners.size === 0) {
        this.#sourceUnsubscribe?.();
        this.#sourceUnsubscribe = undefined;
      }
    };
  };

  async refresh(): Promise<void> {
    if (this.#closed) throw new Error("Keyed live query is closed");
    await this.#source.refresh();
    this.#accept(this.#source.getSnapshot());
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#sourceUnsubscribe?.();
    this.#sourceUnsubscribe = undefined;
    this.#source.close();
    this.#emit();
    this.#listeners.clear();
  }

  #accept(snapshot: LiveSnapshot<TRow>): void {
    if (this.#closed) return;
    if (snapshot.status === "loading") {
      if (this.#snapshot.status === "loading") return;
      this.#snapshot = { status: "loading", rows: this.#rows };
      this.#emit();
      return;
    }
    if (snapshot.status === "error") {
      this.#snapshot = {
        status: "error",
        rows: this.#rows,
        error: snapshot.error,
        version: snapshot.version,
      };
      this.#emit();
      return;
    }
    // The source keeps its row array when nothing changed; then neither did the keyed view,
    // beyond the version it now stands at.
    if (
      this.#snapshot.status === "ready" &&
      snapshot.rows === this.#sourceRows &&
      this.#snapshot.version === snapshot.version
    ) {
      return;
    }
    try {
      if (this.#maxRows !== undefined && snapshot.rows.length > this.#maxRows) {
        throw new RangeError(
          `Live query window returned ${String(snapshot.rows.length)} rows; maximum is ${String(this.#maxRows)}`,
        );
      }
      const initial = !this.#hasReadySnapshot;
      let changes: Array<LiveResultChange<TRow, TKey>>;
      let rows: readonly TRow[];
      if (initial || this.#index === undefined) {
        // The first index doubles as the uniqueness check.
        this.#index = indexRows(snapshot.rows, this.#key);
        rows = snapshot.rows;
        changes = snapshot.rows.map((row, index) => ({ type: "insert", row, index }));
      } else if (snapshot.rows === this.#sourceRows) {
        rows = this.#rows;
        changes = [];
      } else {
        const diff = diffRows(
          this.#index,
          this.#rows,
          snapshot.rows,
          this.#key,
          snapshot.keyChanges,
          this.#table,
        );
        this.#index = diff.index;
        rows = diff.rows;
        changes = diff.changes;
      }
      this.#rows = rows;
      this.#sourceRows = snapshot.rows;
      this.#hasReadySnapshot = true;
      this.#snapshot = {
        status: "ready",
        rows,
        changes: Object.freeze(changes),
        initial,
        version: snapshot.version,
        ...(snapshot.keyChanges === undefined ? {} : { keyChanges: snapshot.keyChanges }),
      };
      this.#emit();
    } catch (error) {
      this.#snapshot = {
        status: "error",
        rows: this.#rows,
        error,
        version: snapshot.version,
      };
      this.#emit();
    }
  }

  #emit(): void {
    for (const listener of [...this.#listeners]) {
      try {
        listener();
      } catch {
        // Isolate external-store listeners.
      }
    }
  }
}
