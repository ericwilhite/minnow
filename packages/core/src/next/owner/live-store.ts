/** Private decoded external store over the native live protocol. No polling or SQL replay. */
import type { Param, Result, Row } from "./api.js";
import {
  ProtocolClient,
  type ClientLiveOptions,
  type ClientLiveHandle,
  type ClientResult,
} from "./client.js";
import { decodeLiveColumn } from "./live-column.js";
import { scalarValue } from "./result-values.js";
import type { KeyChange, LiveKey } from "../live/rows.js";
import { decodeResult } from "./result.js";
import { captureLiveParams } from "./live-params.js";
import { payloadBytes } from "./frame.js";
import { NativeLiveState, type NativeLiveSnapshot } from "../live/state.js";
import {
  LiveRows,
  sameLiveValue,
  liveValueBytes,
  type LiveKeyOf,
  type RowsSnapshot,
} from "../live/rows.js";
import { MemoryAdmissionError } from "../memory-values.js";
import { prepareLiveError } from "../live/errors.js";
import { LIVE_ERROR_SCRATCH_BYTES } from "../live/constants.js";
import { DatabaseReplacedError, fromWireError, ownerErrorRegistry } from "./errors.js";
const CLIENT_LIVE_CAP = 64 * 1048576;
const CLOSED_SNAPSHOT = Object.freeze({ status: "loading" as const, rows: Object.freeze([]) });
const accounts = new WeakMap<ProtocolClient, { used: number }>();

export interface NativeLiveOptions<R extends object> {
  readonly key?: LiveKeyOf<R>;
  readonly table?: string;
  readonly maxRows?: number;
  readonly memoryBytes?: number;
  /** Transforms observe cancellation and return data properties containing SQL values, Dates,
   * non-shared binary values, arrays or plain records. Adapter-private allocations are caller-owned. */
  readonly decode?: (
    result: Result<R>,
    signal: AbortSignal,
  ) => readonly R[] | Promise<readonly R[]>;
}
export type NativeStoreSnapshot<R extends object> = NativeLiveSnapshot<R> &
  Partial<Pick<RowsSnapshot<R>, "changes" | "patch" | "keyChanges">> & {
    readonly result?: Result;
  };
export interface NativeLiveDriver {
  liveQuery<R extends object>(
    sql: string,
    params?: readonly Param[],
    options?: NativeLiveOptions<R>,
  ): NativeLiveStore<R>;
}
export type NativeKeyedLiveStore<R extends object, K extends LiveKeyOf<R>> = Omit<
  NativeLiveStore<R>,
  "getSnapshot"
> & {
  readonly $inferKey: R[K];
  readonly getSnapshot: () =>
    | Exclude<NativeLiveSnapshot<R>, { readonly status: "ready" }>
    | (Extract<NativeLiveSnapshot<R>, { readonly status: "ready" }> &
        RowsSnapshot<R> & { readonly result?: Result });
};

export class NativeLiveStore<R extends object = Row> {
  declare readonly $inferRow: R;
  static clientUsedBytes(client: ProtocolClient): number {
    return accounts.get(client)?.used ?? 0;
  }
  #used = 0;
  readonly #budget = {
    allocate: (bytes: number) => this.#allocate(bytes),
    free: (bytes: number) => this.#free(bytes),
  };
  readonly #cap: number;
  readonly #account: { used: number };
  readonly #state: NativeLiveState<R>;
  #keyed: LiveRows<R> | null;
  #details: RowsSnapshot<R> | null = null;
  #result: Result | null = null;
  #cell: NativeLiveSnapshot<R> | null = null;
  #snapshot: NativeStoreSnapshot<R> | null = null;
  #handle: ClientLiveHandle | null = null;
  #listeners = 0;
  #token = 0;
  #abort: AbortController | null = new AbortController();
  #closed = false;
  #replacement: DatabaseReplacedError | null = null;
  #starting = false;
  #working = false;
  #resuming = false;
  #closing: Promise<void> | null = null;
  #idle: ReturnType<typeof setTimeout> | null = null;
  #work: Promise<void> = Promise.resolve();
  #retained = 0;
  #source = 0;
  #diagnostic = 0;
  #sql: string;
  #options: NativeLiveOptions<R>;
  #params: readonly Param[];
  #wait: { promise: Promise<void>; resolve(): void } | null = null;
  readonly #iterators = new Set<() => void>();
  constructor(
    readonly client: ProtocolClient,
    sql: string,
    params: readonly Param[] = [],
    options: NativeLiveOptions<R> = {},
  ) {
    const { key, table, maxRows, memoryBytes, decode } = options;
    this.#options = {
      ...(key === undefined ? {} : { key }),
      ...(table === undefined ? {} : { table }),
      ...(maxRows === undefined ? {} : { maxRows }),
      ...(memoryBytes === undefined ? {} : { memoryBytes }),
      ...(decode === undefined ? {} : { decode }),
    };
    this.#sql = sql;
    this.#params = [];
    this.#cap = memoryBytes ?? CLIENT_LIVE_CAP;
    if (!Number.isSafeInteger(this.#cap) || this.#cap < 1024 || this.#cap > CLIENT_LIVE_CAP)
      throw new RangeError("Live store memory must be between 1024 bytes and 64 MiB");
    if (maxRows !== undefined && (!Number.isSafeInteger(maxRows) || maxRows < 1))
      throw new RangeError("A live window needs a positive integer limit");
    this.#account = accounts.get(client) ?? { used: 0 };
    accounts.set(client, this.#account);
    this.#state = NativeLiveState.create<R>(this.#budget);
    try {
      this.#source =
        LIVE_ERROR_SCRATCH_BYTES +
        2048 +
        6 * liveValueBytes([sql, typeof key === "symbol" ? key.description : key, table]);
      if (!this.#allocate(this.#source)) {
        const refusedBytes = this.#source;
        this.#source = 0;
        throw new MemoryAdmissionError("live source", refusedBytes, 0);
      }
      this.#params = captureLiveParams(params, (bytes) => {
        const overlap = 6 * bytes;
        if (!this.#allocate(overlap)) throw new MemoryAdmissionError("live source", overlap, 0);
        this.#source += overlap;
      });
      this.#keyed =
        key === undefined
          ? null
          : new LiveRows(this.#budget, key, {
              ...(table === undefined ? {} : { table }),
              ...(maxRows === undefined ? {} : { maxRows }),
            });
    } catch (error) {
      this.#free(this.#source);
      this.#state.close();
      throw error;
    }
  }
  get usedBytes(): number {
    return this.#used;
  }
  /** Call at user-code boundaries: callbacks and cloning may reenter close/refresh. */
  #current(token = this.#token, signal?: AbortSignal): boolean {
    return !this.#closed && this.#replacement === null && token === this.#token && !signal?.aborted;
  }
  #allocate(bytes: number): boolean {
    if (this.#closed || !Number.isSafeInteger(bytes) || bytes < 0) return false;
    if (bytes > this.#cap - this.#used || bytes > CLIENT_LIVE_CAP - this.#account.used)
      return false;
    this.#used += bytes;
    this.#account.used += bytes;
    return true;
  }
  #free(bytes: number): void {
    this.#used -= bytes;
    this.#account.used -= bytes;
    if (this.#account.used === 0 && accounts.get(this.client) === this.#account)
      accounts.delete(this.client);
  }
  readonly getSnapshot = (): NativeStoreSnapshot<R> => {
    if (this.#closed) return CLOSED_SNAPSHOT;
    const cell = this.#state.getSnapshot();
    if (this.#cell !== cell) {
      this.#cell = cell;
      this.#snapshot =
        cell.status === "ready" && this.#details !== null
          ? Object.freeze({
              ...cell,
              changes: this.#details.changes,
              patch: this.#details.patch,
              keyChanges: this.#details.keyChanges,
            })
          : cell;
      if (cell.status === "ready" && this.#result !== null)
        this.#snapshot = Object.freeze({ ...this.#snapshot, result: this.#result });
    }
    return this.#snapshot ?? cell;
  };
  readonly subscribe = (listener: () => void): (() => void) => {
    if (this.#closed) throw new Error("Live store is closed");
    const stop = this.#state.subscribe(() => {
      if (!this.#closed) listener();
    });
    if (this.#idle !== null) clearTimeout(this.#idle);
    this.#idle = null;
    this.#listeners++;
    try {
      this.#start();
    } catch (error) {
      stop();
      this.#listeners--;
      throw error;
    }
    let active = true;
    return () => {
      if (!active) return;
      active = false;
      stop();
      if (--this.#listeners === 0) this.#stop();
    };
  };
  #stop(): void {
    if (this.#idle !== null) clearTimeout(this.#idle);
    this.#idle = null;
    this.#token++;
    this.#abort?.abort();
    this.#handle?.close();
    this.#handle = null;
    this.#wait?.resolve();
    this.#wait = null;
  }
  #loading(): void {
    if (this.#closed || this.#replacement !== null) return;
    this.#token++;
    this.#abort?.abort();
    this.#abort = new AbortController();
    this.#state.loading();
  }
  #settle(): void {
    this.#wait?.resolve();
    this.#wait = null;
    if (!this.#closed && this.#listeners === 0 && this.#idle === null)
      this.#idle = setTimeout(() => {
        this.#idle = null;
        if (this.#listeners === 0) this.#stop();
      }, 30_000);
  }
  #reset(): void {
    if (this.#closed || this.#replacement !== null) return;
    // This fixed diagnostic fits the prepaid control/error scratch. Detach retained ownership
    // before aborting user code, which may synchronously reenter close or refresh.
    const error = new DatabaseReplacedError();
    this.#replacement = error;
    const keyed = this.#keyed,
      retained = this.#retained,
      diagnostic = this.#diagnostic;
    this.#keyed = null;
    this.#retained = 0;
    this.#diagnostic = 0;
    this.#details = null;
    this.#result = null;
    this.#cell = null;
    this.#snapshot = null;
    this.#stop();
    keyed?.close();
    this.#free(retained + diagnostic);
    this.#state.reset(error);
  }
  #error(error: unknown): void {
    if (this.#closed) return;
    if (this.#replacement !== null) {
      this.#state.error(this.#replacement);
      this.#settle();
      return;
    }
    const token = this.#token;
    let charged = 0;
    const wire = prepareLiveError(error, (bytes) => {
      if (!this.#allocate(bytes)) return false;
      charged += bytes;
      return true;
    });
    const owned = fromWireError(wire, ownerErrorRegistry);
    this.#free(this.#diagnostic);
    this.#diagnostic = charged;
    this.#state.error(owned);
    if (this.#current(token)) this.#settle();
  }
  #start(): void {
    if (this.#handle !== null || this.#starting || this.#closed) return;
    if (this.#replacement !== null) {
      this.#state.error(this.#replacement);
      this.#settle();
      return;
    }
    // An unsubscribe/re-subscribe cannot launch a second decoder while the retired one drains.
    if (this.#working) {
      if (!this.#resuming) {
        this.#resuming = true;
        void this.#work.then(() => {
          this.#resuming = false;
          if (!this.#closed && (this.#listeners > 0 || this.#wait !== null)) {
            try {
              this.#start();
            } catch (error) {
              this.#error(error);
            }
          }
        });
      }
      return;
    }
    this.#starting = true;
    const token = this.#token;
    try {
      const handle = this.client.subscribeLive(this.#sql, this.#params, {
        onLoading: () => this.#loading(),
        onReset: () => this.#reset(),
        onError: (error) => this.#error(error),
        onResult: (result, _version, _generation, delivery) => {
          const controller = this.#abort;
          if (this.#closed || controller === null) return;
          const token = this.#token,
            signal = controller.signal;
          this.#working = true;
          const work = Promise.resolve()
            .then(() => this.#accept(result, token, signal, delivery))
            .finally(() => {
              this.#working = false;
            });
          this.#work = work.catch(() => undefined);
          return work;
        },
      });
      if (!this.#current(token + 1)) handle.close();
      else this.#handle = handle;
    } finally {
      this.#starting = false;
    }
  }
  async #accept(
    input: ClientResult,
    token: number,
    signal: AbortSignal,
    delivery: Parameters<ClientLiveOptions["onResult"]>[3],
  ): Promise<void> {
    if (!this.#current(token, signal)) return;
    // Reserve decoding's vectors, strings, row objects and owned copies BEFORE materializing.
    let bytes =
      4096 +
      6 * liveValueBytes(input.columns) +
      delivery.keyChanges.reduce((sum, effect) => sum + 16 * effect.bytes, 0) +
      input.frames.reduce(
        (sum, frame) =>
          sum + 32 * payloadBytes(frame) + frame.rowCount * (input.columns.length + 1) * 1024,
        0,
      );
    if (!this.#allocate(bytes)) throw new MemoryAdmissionError("live client decode", bytes, 0);
    let retained = false;
    try {
      const result = decodeResult<R>(input);
      // Plugins may mutate their rows and binary views. The admitted copy is separate
      // from the canonical Result and rebuilds its own dictionaries from complete frames.
      const adapted =
        this.#options.decode === undefined
          ? result
          : decodeResult<R>({
              ...structuredClone({ columns: input.columns, frames: input.frames }),
              summary: input.summary,
            });
      if (!this.#current(token, signal)) return;
      const rows = await (this.#options.decode?.(adapted, signal) ?? result.rows());
      if (!this.#current(token, signal)) return;
      if (this.#options.maxRows !== undefined && rows.length > this.#options.maxRows)
        throw new RangeError("Live query exceeded its window limit");
      const required =
        4096 +
        6 * liveValueBytes(rows) +
        (this.#keyed === null ? 6 * liveValueBytes(this.#state.getSnapshot().rows) : 0);
      if (required > bytes) {
        if (!this.#allocate(required - bytes))
          throw new MemoryAdmissionError("live transformed rows", required, 0);
        bytes = required;
      }
      if (!this.#current(token, signal)) return;
      const version = input.summary.lsn ?? 0;
      let reset = delivery.reset;
      const changes: KeyChange[] = [];
      for (const effect of delivery.keyChanges) {
        if (effect.table !== this.#options.table) continue;
        if (effect.columns.length !== 1) {
          reset = true;
          continue;
        }
        const column = effect.columns[0];
        if (column === undefined) throw new TypeError("Missing native rekey metadata");
        const decode = (values: readonly Uint8Array[]): LiveKey => {
          const bytes = values[0];
          if (bytes === undefined) throw new TypeError("Missing native rekey column");
          const value = scalarValue(
            { ...column, sqlType: "", nullable: false, render: null },
            decodeLiveColumn(bytes, column.type).data,
            0,
          );
          if (value === null || typeof value === "object")
            throw new TypeError("Unsupported native rekey scalar");
          return value;
        };
        changes.push({
          table: effect.table,
          kind: effect.kind,
          fromRowId: effect.fromRowId,
          toRowId: effect.toRowId,
          from: decode(effect.from),
          to: decode(effect.to),
        });
      }
      const install = () => {
        this.#free(this.#diagnostic);
        this.#diagnostic = 0;
        this.#free(this.#retained);
        this.#retained = bytes;
        // Canonical labels/values remain raw even when the adapter changes R.
        this.#result = result as unknown as Result;
        this.#cell = null;
        this.#snapshot = null;
      };
      if (this.#keyed !== null) {
        let previous = this.#keyed.getSnapshot();
        const reconciled = this.#keyed.apply(rows, version, { reset, keyChanges: changes });
        const unchanged = reconciled === previous;
        previous = null;
        if (unchanged) {
          // LiveRows reuses its snapshot on equal rows. A new canonical publication must
          // describe an identity transition, rather than re-emitting that old snapshot's diff.
          const indices = new Int32Array(reconciled.rows.length);
          for (let i = 0; i < indices.length; i++) indices[i] = i;
          this.#details = Object.freeze({
            ...reconciled,
            version,
            reset: false,
            changes: Object.freeze([]),
            keyChanges: Object.freeze([]),
            patch: Object.freeze({
              type: "patch" as const,
              retained: indices,
              changedRows: Object.freeze([]),
            }),
          });
        } else this.#details = reconciled;
        if (!this.#current(token, signal)) return;
        install();
        retained = true;
        this.#state.ready(this.#details.rows, version, this.#details.reset, true);
      } else {
        const previous = this.#state.getSnapshot().rows;
        const owned = Object.freeze(
          rows.map((row, i) => {
            const old = previous[i];
            return old !== undefined && sameLiveValue(old, row) ? old : structuredClone(row);
          }),
        );
        if (!this.#current(token, signal)) return;
        install();
        retained = true;
        this.#state.ready(owned, version, delivery.reset);
      }
      if (this.#current(token)) this.#settle();
    } finally {
      if (!retained) this.#free(bytes);
    }
  }
  readonly refresh = (): Promise<void> => {
    if (this.#closed) return Promise.reject(new Error("Live store is closed"));
    if (this.#wait !== null) return this.#wait.promise;
    let resolve!: () => void;
    const promise = new Promise<void>((done) => {
      resolve = done;
    });
    this.#wait = { promise, resolve };
    try {
      if (this.#handle === null) this.#start();
      else if (this.#state.getSnapshot().status !== "loading") this.#handle.refresh();
    } catch (error) {
      this.#error(error);
    }
    return promise;
  };
  [Symbol.asyncIterator](): AsyncIterator<NativeStoreSnapshot<R>, undefined> {
    if (this.#closed) throw new Error("Live store is closed");
    if (!this.#allocate(1024)) throw new MemoryAdmissionError("live iterator", 1024, 0);
    let stopped = false,
      dirty = true,
      gap = false,
      baseline = false;
    const read = (): NativeStoreSnapshot<R> => {
      const snapshot = this.getSnapshot();
      const reset = !baseline || gap;
      baseline = snapshot.status === "ready";
      gap = false;
      if (snapshot.status !== "ready" || !reset || snapshot.reset) return snapshot;
      // Patches/notices belong to the preceding published ready snapshot. A new or
      // coalesced iterator has no such baseline; publish the full rows without stale notices.
      return Object.freeze({
        ...snapshot,
        reset: true,
        ...(snapshot.patch === undefined
          ? {}
          : {
              changes: Object.freeze([]),
              keyChanges: Object.freeze([]),
              patch: Object.freeze({ type: "reset" as const, rows: snapshot.rows }),
            }),
      });
    };
    const done = { done: true as const, value: undefined };
    let pending: {
      resolve(value: IteratorResult<NativeStoreSnapshot<R>, undefined>): void;
    } | null = null;
    let unsubscribe: () => void;
    try {
      unsubscribe = this.subscribe(() => {
        gap ||= dirty;
        dirty = true;
        if (pending !== null) {
          const waiter = pending;
          pending = null;
          dirty = false;
          waiter.resolve({ done: false, value: read() });
        }
      });
    } catch (error) {
      this.#free(1024);
      throw error;
    }
    const finish = () => {
      if (stopped) return;
      stopped = true;
      unsubscribe();
      this.#iterators.delete(finish);
      this.#free(1024);
      pending?.resolve(done);
      pending = null;
    };
    this.#iterators.add(finish);
    if (!this.#current()) finish();
    return {
      next: () => {
        if (stopped || this.#closed) return Promise.resolve(done);
        if (pending !== null)
          return Promise.reject(new TypeError("Only one live iterator next() may be pending"));
        if (dirty) {
          dirty = false;
          return Promise.resolve({ done: false, value: read() });
        }
        return new Promise((resolve) => {
          pending = { resolve };
        });
      },
      return: () => {
        finish();
        return Promise.resolve(done);
      },
      throw: (error?: unknown) => {
        finish();
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- AsyncIterator.throw forwards the caller's reason unchanged.
        return Promise.reject(error);
      },
    };
  }
  close(): Promise<void> {
    if (this.#closing !== null) return this.#closing;
    // Install the shared fence before cancellation can reenter through client activity.
    this.#closing = this.#work.then(() => {
      this.#keyed?.close();
      this.#keyed = null;
      this.#abort = null;
      this.#free(this.#retained);
      this.#free(this.#source);
      this.#source = 0;
      this.#free(this.#diagnostic);
      this.#diagnostic = 0;
      this.#sql = "";
      this.#params = [];
      this.#options = {};
      this.#replacement = null;
      this.#retained = 0;
      this.#details = null;
      this.#result = null;
      this.#state.close();
      this.#cell = null;
      this.#snapshot = null;
    });
    this.#closed = true;
    this.#details = null;
    this.#result = null;
    this.#cell = null;
    this.#snapshot = null;
    this.#stop();
    for (const finish of this.#iterators) finish();
    return this.#closing;
  }
}
