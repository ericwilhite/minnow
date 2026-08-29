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
    };

export type LiveChangesSnapshot<TRow extends object, TKey extends keyof TRow> =
  | { readonly status: "loading"; readonly rows: readonly TRow[] }
  | {
      readonly status: "ready";
      readonly rows: readonly TRow[];
      readonly changes: ReadonlyArray<LiveResultChange<TRow, TKey>>;
      readonly initial: boolean;
      readonly version: number | null;
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
}

function keyToken(value: unknown, name: PropertyKey): string {
  if (typeof value === "string") return `s:${String(value.length)}:${value}`;
  if (typeof value === "boolean") return value ? "b:1" : "b:0";
  if (typeof value === "number") {
    if (Number.isNaN(value)) return "n:NaN";
    if (Object.is(value, -0)) return "n:-0";
    return `n:${String(value)}`;
  }
  if (value instanceof Date) {
    const time = dateMilliseconds(value);
    if (!Number.isFinite(time)) {
      throw new TypeError(`Live query key ${String(name)} must be a valid Date`);
    }
    return `d:${String(time)}`;
  }
  throw new TypeError(
    `Live query key ${String(name)} must be a non-null string, number, boolean, or Date`,
  );
}

interface IndexedRow<TRow extends object> {
  readonly row: TRow;
  readonly index: number;
}

function indexRows<TRow extends object>(
  rows: readonly TRow[],
  key: keyof TRow,
): Map<string, IndexedRow<TRow>> {
  const indexed = new Map<string, IndexedRow<TRow>>();
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

function diffRows<TRow extends object, TKey extends keyof TRow>(
  previousRows: readonly TRow[],
  rows: readonly TRow[],
  key: TKey,
): Array<LiveResultChange<TRow, TKey>> {
  const previous = indexRows(previousRows, key);
  const current = indexRows(rows, key);
  const changes: Array<LiveResultChange<TRow, TKey>> = [];
  for (const [token, old] of previous) {
    if (current.has(token)) continue;
    changes.push({ type: "delete", key: old.row[key], previous: old.row, index: old.index });
  }
  for (const [token, next] of current) {
    const old = previous.get(token);
    if (old === undefined) {
      changes.push({ type: "insert", row: next.row, index: next.index });
      continue;
    }
    if (!sameLiveValue(old.row, next.row)) {
      changes.push({ type: "update", row: next.row, previous: old.row, index: next.index });
    }
    if (old.index !== next.index) {
      changes.push({
        type: "move",
        key: next.row[key],
        row: next.row,
        from: old.index,
        to: next.index,
      });
    }
  }
  return changes;
}

/** Keyed exact diffs over a typed live query, with optional bounded-window enforcement. */
export class KeyedLiveQuery<TRow extends object, TKey extends LiveKeyOf<TRow>> {
  declare readonly $inferRow: TRow;
  declare readonly $inferKey: TRow[TKey];

  readonly #source: LiveQuery<TRow>;
  readonly #key: TKey;
  readonly #maxRows: number | undefined;
  readonly #listeners = new Set<() => void>();
  #sourceUnsubscribe: (() => void) | undefined;
  #rows: readonly TRow[] = [];
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
    try {
      if (this.#maxRows !== undefined && snapshot.rows.length > this.#maxRows) {
        throw new RangeError(
          `Live query window returned ${String(snapshot.rows.length)} rows; maximum is ${String(this.#maxRows)}`,
        );
      }
      const initial = !this.#hasReadySnapshot;
      let changes: Array<LiveResultChange<TRow, TKey>>;
      if (initial) {
        // Initial snapshots still validate uniqueness even though no previous index is needed.
        indexRows(snapshot.rows, this.#key);
        changes = snapshot.rows.map((row, index) => ({ type: "insert", row, index }));
      } else {
        // diffRows builds each old/new key index exactly once and validates duplicates as it goes.
        changes = diffRows(this.#rows, snapshot.rows, this.#key);
      }
      this.#rows = snapshot.rows;
      this.#hasReadySnapshot = true;
      this.#snapshot = {
        status: "ready",
        rows: snapshot.rows,
        changes: Object.freeze(changes),
        initial,
        version: snapshot.version,
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
