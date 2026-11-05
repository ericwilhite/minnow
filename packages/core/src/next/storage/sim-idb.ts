/**
 * A simulated IndexedDB (idb-like.ts) whose durability comes from the simulated disk (sim-disk.ts),
 * so the storage kit's physical crash sweep applies to the IndexedDB backend as it does to files.
 *
 * ## Model
 * - **One write-ahead log per database** (`idb-wal` on the disk), as the platforms keep one per
 *   origin database. A transaction's commit appends one record holding all its changes; a `strict`
 *   commit then flushes the file. A `relaxed` commit is only written: it becomes durable with the
 *   next flush, and is lost, kept or torn by a crash before that like any unflushed write.
 * - **Records are checksum-chained** (each carries the checksum of the one before it). Opening
 *   replays the longest valid chain from the start and appends after it, so a crash leaves exactly
 *   a prefix of committed transactions, never a later one without an earlier one: the platforms'
 *   guarantee, and the one the backend relies on.
 * - **Transactions run one at a time**, in creation order (a valid IndexedDB schedule). Requests
 *   are answered asynchronously, in order; a transaction commits when its callbacks leave nothing
 *   pending. Changes apply in memory at once (with an undo list for abort) and reach the log at
 *   commit.
 * - **Processes.** Every backend over one disk shares one database, as tabs share an origin's
 *   IndexedDB. After `disk.powerLoss()` the next user reloads it from the log (a new process).
 *
 * Not modelled: parallel readonly transactions, quota, version changes, blocked opens.
 */
import { xxHash32 } from "../hash/xxhash32.js";
import type { SyncFile } from "./contract.js";
import type {
  IdbDatabaseLike,
  IdbKey,
  IdbRange,
  IdbStoreLike,
  IdbTransactionLike,
} from "./idb-like.js";
import type { SimDisk } from "./sim-disk.js";

const WAL = "idb-wal";
const MAGIC = 0x4c41574d; // "MWAL"
const HEADER = 16;

type Value = unknown;
type Op =
  | { readonly kind: "put"; readonly store: string; readonly key: IdbKey; readonly value: Value }
  | { readonly kind: "delete"; readonly store: string; readonly key: IdbKey }
  | { readonly kind: "clear"; readonly store: string };

/** One object store: values by key, keys kept sorted (numbers before strings, as IndexedDB). */
class Store {
  readonly values = new Map<IdbKey, Value>();
  keys: IdbKey[] = [];

  static compare(a: IdbKey, b: IdbKey): number {
    if (typeof a === "number" && typeof b === "number") return a - b;
    if (typeof a === "number") return -1;
    if (typeof b === "number") return 1;
    return a < b ? -1 : a > b ? 1 : 0;
  }

  #index(key: IdbKey): number {
    let low = 0;
    let high = this.keys.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if (Store.compare(this.keys[middle] ?? 0, key) < 0) low = middle + 1;
      else high = middle;
    }
    return low;
  }

  put(key: IdbKey, value: Value): void {
    if (!this.values.has(key)) this.keys.splice(this.#index(key), 0, key);
    this.values.set(key, value);
  }

  delete(key: IdbKey): void {
    if (!this.values.delete(key)) return;
    this.keys.splice(this.#index(key), 1);
  }

  /** Numeric keys inside `range`, ascending. */
  inRange(range: IdbRange | null): number[] {
    const out: number[] = [];
    for (const key of this.keys) {
      if (typeof key !== "number") break;
      const { lower, upper, lowerOpen = false, upperOpen = false } = range ?? {};
      if (lower !== undefined && (key < lower || (lowerOpen && key === lower))) continue;
      if (upper !== undefined && (key > upper || (upperOpen && key === upper))) break;
      out.push(key);
    }
    return out;
  }
}

/** Values in the log: numbers, strings, undefined, ArrayBuffers and plain objects of those. */
function encodeOps(ops: readonly Op[]): Uint8Array {
  const buffers: ArrayBuffer[] = [];
  const json = JSON.stringify(ops, (_key, value: unknown) => {
    if (value instanceof ArrayBuffer) {
      buffers.push(value);
      return { $buffer: buffers.length - 1 };
    }
    return value;
  });
  const text = new TextEncoder().encode(json);
  let size = 4 + text.length;
  for (const buffer of buffers) size += 4 + buffer.byteLength;
  const out = new Uint8Array(size);
  const view = new DataView(out.buffer);
  view.setUint32(0, text.length, true);
  out.set(text, 4);
  let at = 4 + text.length;
  for (const buffer of buffers) {
    view.setUint32(at, buffer.byteLength, true);
    out.set(new Uint8Array(buffer), at + 4);
    at += 4 + buffer.byteLength;
  }
  return out;
}

function decodeOps(body: Uint8Array): Op[] {
  const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
  const textLength = view.getUint32(0, true);
  const text = new TextDecoder().decode(body.subarray(4, 4 + textLength));
  const buffers: ArrayBuffer[] = [];
  let at = 4 + textLength;
  while (at < body.length) {
    const length = view.getUint32(at, true);
    buffers.push(body.slice(at + 4, at + 4 + length).buffer);
    at += 4 + length;
  }
  return JSON.parse(text, (_key, value: unknown) =>
    typeof value === "object" && value !== null && "$buffer" in value
      ? buffers[(value as { $buffer: number }).$buffer]
      : value,
  ) as Op[];
}

class SimDatabase {
  readonly stores = new Map<string, Store>();
  readonly #file: SyncFile;
  #end = 0;
  #sequence = 0;
  #chain = 0;
  #queue: Promise<unknown> = Promise.resolve();

  constructor(disk: SimDisk) {
    this.#file = disk.open(WAL, false);
    this.#replay();
  }

  store(name: string): Store {
    let store = this.stores.get(name);
    if (store === undefined) {
      store = new Store();
      this.stores.set(name, store);
    }
    return store;
  }

  #apply(op: Op): void {
    const store = this.store(op.store);
    if (op.kind === "put") store.put(op.key, op.value);
    else if (op.kind === "delete") store.delete(op.key);
    else {
      store.values.clear();
      store.keys = [];
    }
  }

  /** Applies the longest valid chain of records; appends continue after it. */
  #replay(): void {
    const size = this.#file.getSize();
    let at = 0;
    while (at + HEADER + 4 <= size) {
      const header = new Uint8Array(HEADER);
      this.#file.read(header, { at });
      const view = new DataView(header.buffer);
      const length = view.getUint32(12, true);
      if (
        view.getUint32(0, true) !== MAGIC ||
        view.getUint32(4, true) !== this.#sequence + 1 ||
        view.getUint32(8, true) !== this.#chain ||
        at + HEADER + length + 4 > size
      )
        break;
      const record = new Uint8Array(HEADER + length + 4);
      this.#file.read(record, { at });
      const checksum = new DataView(record.buffer).getUint32(HEADER + length, true);
      if (xxHash32(record, 0, 0, HEADER + length) !== checksum) break;
      for (const op of decodeOps(record.subarray(HEADER, HEADER + length))) this.#apply(op);
      this.#sequence += 1;
      this.#chain = checksum;
      at += record.length;
    }
    this.#end = at;
  }

  /** Appends one committed transaction; `flush` for strict durability. */
  #commit(ops: readonly Op[], flush: boolean): void {
    if (ops.length > 0) {
      const body = encodeOps(ops);
      const record = new Uint8Array(HEADER + body.length + 4);
      const view = new DataView(record.buffer);
      view.setUint32(0, MAGIC, true);
      view.setUint32(4, this.#sequence + 1, true);
      view.setUint32(8, this.#chain, true);
      view.setUint32(12, body.length, true);
      record.set(body, HEADER);
      const checksum = xxHash32(record, 0, 0, HEADER + body.length);
      view.setUint32(HEADER + body.length, checksum, true);
      this.#file.write(record, { at: this.#end });
      this.#end += record.length;
      this.#sequence += 1;
      this.#chain = checksum;
    }
    if (flush) this.#file.flush();
  }

  transaction(
    names: readonly string[],
    mode: "readonly" | "readwrite",
    durability: "strict" | "relaxed",
    body: (tx: IdbTransactionLike) => void,
  ): Promise<void> {
    const run = this.#queue.then(() => this.#run(names, mode, durability, body));
    this.#queue = run.catch(() => undefined);
    return run;
  }

  async #run(
    names: readonly string[],
    mode: "readonly" | "readwrite",
    durability: "strict" | "relaxed",
    body: (tx: IdbTransactionLike) => void,
  ): Promise<void> {
    const scope = new Set(names);
    const pending: Array<() => void> = [];
    const ops: Op[] = [];
    const undo: Array<() => void> = [];
    // Set from callbacks: a holder, so the loop below sees each change.
    const state: { failure: Error | null } = { failure: null };
    const fail = (error: unknown) => {
      state.failure ??= error instanceof Error ? error : new Error(String(error));
    };
    const call = (work: () => void) => {
      try {
        work();
      } catch (error) {
        fail(error);
      }
    };
    const write = (op: Op) => {
      if (mode !== "readwrite") throw new Error("A readonly transaction cannot write");
      const store = this.store(op.store);
      if (op.kind === "clear") {
        const saved = new Map(store.values);
        const keys = store.keys.slice();
        undo.push(() => {
          store.values.clear();
          for (const [key, value] of saved) store.values.set(key, value);
          store.keys = keys;
        });
      } else {
        const had = store.values.has(op.key);
        const before = store.values.get(op.key);
        undo.push(() => {
          if (had) store.put(op.key, before);
          else store.delete(op.key);
        });
      }
      this.#apply(op);
      ops.push(op);
    };
    const storeOf = (name: string): IdbStoreLike => {
      if (!scope.has(name)) throw new Error(`Store ${name} is not in the transaction's scope`);
      const store = this.store(name);
      return {
        get: (key, then) => pending.push(() => then(store.values.get(key))),
        getAll: (range, then) =>
          pending.push(() => then(store.inRange(range).map((key) => store.values.get(key)))),
        last: (range, then) =>
          pending.push(() => {
            const keys = store.inRange(range);
            const key = keys.at(-1);
            then(key === undefined ? undefined : store.values.get(key));
          }),
        put: (key, value) => write({ kind: "put", store: name, key, value }),
        delete: (key) => write({ kind: "delete", store: name, key }),
        deleteRange: (range) => {
          for (const key of store.inRange(range)) write({ kind: "delete", store: name, key });
        },
        clear: () => write({ kind: "clear", store: name }),
      };
    };
    call(() => {
      body({ store: storeOf, abort: fail });
    });
    while (state.failure === null && pending.length > 0) {
      await Promise.resolve();
      const next = pending.shift();
      if (next !== undefined) call(next);
    }
    if (state.failure !== null) {
      for (const restore of undo.reverse()) restore();
      throw state.failure;
    }
    // A crash while committing (SimulatedCrash from the disk) rejects the transaction.
    this.#commit(ops, mode === "readwrite" && durability === "strict");
  }
}

const databases = new WeakMap<SimDisk, { crashes: number; database: SimDatabase }>();

/** The simulated IndexedDB of `disk` (one per disk, reloaded after each power loss). */
export function simIndexedDb(disk: SimDisk): IdbDatabaseLike {
  let entry = databases.get(disk);
  if (entry?.crashes !== disk.crashes) {
    entry = { crashes: disk.crashes, database: new SimDatabase(disk) };
    databases.set(disk, entry);
  }
  const database = entry.database;
  return {
    transaction: (names, mode, durability, body) =>
      database.transaction(names, mode, durability, body),
    close: () => undefined,
  };
}
