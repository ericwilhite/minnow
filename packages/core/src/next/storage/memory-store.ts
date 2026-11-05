/**
 * The in-memory durable store: the reference implementation of contract.ts and the substrate of
 * the fault-injecting store. Every completed call is immediately "durable" (the state outlives
 * sessions, never the process).
 */
import {
  LOG_HEADER_SLOT_BYTES,
  StaleEpochError,
  StoreClosedError,
  StoreCorruptionError,
  type DurableStore,
  type PackId,
  type PackRange,
  type PackWriter,
  type SealedPack,
  type StoreSession,
} from "./contract.js";

/** The durable state. Exposed (not exported from any entry) so fault-store.ts can perturb it. */
export interface MemoryStoreState {
  epoch: number;
  root: Uint8Array | null;
  packs: Map<PackId, Uint8Array>;
  /** Packs whose writer has been created and not yet sealed or aborted, with their bytes so far. */
  writing: Map<PackId, Uint8Array[]>;
  log: Uint8Array;
  logLength: number;
  headers: [Uint8Array | null, Uint8Array | null];
}

export function emptyMemoryState(): MemoryStoreState {
  return {
    epoch: 0,
    root: null,
    packs: new Map(),
    writing: new Map(),
    log: new Uint8Array(4096),
    logLength: 0,
    headers: [null, null],
  };
}

/** Writes `bytes` at `offset` of the state's log, growing it (gaps read as zero). */
export function writeLogBytes(state: MemoryStoreState, offset: number, bytes: Uint8Array): void {
  const end = offset + bytes.length;
  if (end > state.log.length) {
    let size = state.log.length * 2;
    while (size < end) size *= 2;
    const grown = new Uint8Array(size);
    grown.set(state.log.subarray(0, state.logLength));
    state.log = grown;
  }
  if (offset > state.logLength) state.log.fill(0, state.logLength, offset);
  state.log.set(bytes, offset);
  state.logLength = Math.max(state.logLength, end);
}

function concat(chunks: readonly Uint8Array[]): Uint8Array {
  let length = 0;
  for (const chunk of chunks) length += chunk.length;
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  return bytes;
}

export class MemoryStore implements DurableStore {
  readonly state: MemoryStoreState;

  constructor(state: MemoryStoreState = emptyMemoryState()) {
    this.state = state;
  }

  async open(): Promise<StoreSession> {
    this.state.epoch += 1;
    return new MemorySession(this.state, this.state.epoch);
  }
}

class MemorySession implements StoreSession {
  readonly #state: MemoryStoreState;
  readonly epoch: number;
  readonly #sealed = new Set<PackId>();
  #closed = false;

  constructor(state: MemoryStoreState, epoch: number) {
    this.#state = state;
    this.epoch = epoch;
  }

  #live(): MemoryStoreState {
    if (this.#closed) throw new StoreClosedError();
    return this.#state;
  }

  #mutable(): MemoryStoreState {
    const state = this.#live();
    if (state.epoch !== this.epoch) throw new StaleEpochError(this.epoch, state.epoch);
    return state;
  }

  async readRoot(): Promise<Uint8Array | null> {
    const root = this.#live().root;
    return root === null ? null : root.slice();
  }

  async publish(root: Uint8Array, packs: readonly SealedPack[]): Promise<void> {
    const state = this.#mutable();
    for (const pack of packs)
      if (!this.#sealed.has(pack.id) || state.packs.get(pack.id)?.length !== pack.length)
        throw new TypeError(`Pack ${String(pack.id)} was not sealed in this session`);
    state.root = root.slice();
  }

  createPack(id: PackId): PackWriter {
    const state = this.#mutable();
    if (!Number.isSafeInteger(id) || id < 1)
      throw new RangeError("Pack id must be a positive integer");
    if (state.packs.has(id) || state.writing.has(id))
      throw new TypeError(`Pack ${String(id)} already exists`);
    const chunks: Uint8Array[] = [];
    state.writing.set(id, chunks);
    let length = 0;
    let open = true;
    const guard = (): MemoryStoreState => {
      const current = this.#mutable();
      if (!open) throw new TypeError(`Pack ${String(id)} is no longer writable`);
      return current;
    };
    return {
      id,
      get length() {
        return length;
      },
      append: async (bytes) => {
        guard();
        chunks.push(bytes.slice());
        const offset = length;
        length += bytes.length;
        return offset;
      },
      seal: async () => {
        const current = guard();
        open = false;
        current.writing.delete(id);
        current.packs.set(id, concat(chunks));
        this.#sealed.add(id);
        return { id, length };
      },
      abort: async () => {
        const current = this.#mutable();
        open = false;
        current.writing.delete(id);
      },
    };
  }

  async read(ranges: readonly PackRange[]): Promise<Uint8Array[]> {
    const state = this.#live();
    return ranges.map(({ pack, offset, length }) => {
      const bytes = state.packs.get(pack);
      if (bytes === undefined) throw new StoreCorruptionError(`Pack ${String(pack)} is missing`);
      if (offset < 0 || length < 0 || offset + length > bytes.length)
        throw new StoreCorruptionError(`Read outside pack ${String(pack)}`);
      return bytes.subarray(offset, offset + length);
    });
  }

  async listPacks(): Promise<PackId[]> {
    const state = this.#live();
    return [...new Set([...state.packs.keys(), ...state.writing.keys()])].sort((a, b) => a - b);
  }

  async reclaim(ids: readonly PackId[]): Promise<void> {
    const state = this.#mutable();
    for (const id of ids) {
      state.packs.delete(id);
      state.writing.delete(id);
    }
  }

  logLength(): number {
    return this.#live().logLength;
  }

  async readLog(offset: number, length: number): Promise<Uint8Array> {
    const state = this.#live();
    if (offset < 0 || length < 0 || offset + length > state.logLength)
      throw new StoreCorruptionError("Read outside the log");
    return state.log.slice(offset, offset + length);
  }

  async readLogHeaders(): Promise<readonly [Uint8Array | null, Uint8Array | null]> {
    const [first, second] = this.#live().headers;
    return [first?.slice() ?? null, second?.slice() ?? null];
  }

  appendLog(bytes: Uint8Array): number {
    const state = this.#mutable();
    const offset = state.logLength;
    writeLogBytes(state, offset, bytes);
    return offset;
  }

  writeLogHeader(slot: 0 | 1, bytes: Uint8Array): void {
    const state = this.#mutable();
    if (bytes.length > LOG_HEADER_SLOT_BYTES) throw new RangeError("Log header slot overflow");
    state.headers[slot] = bytes.slice();
  }

  async flushLog(): Promise<void> {
    this.#mutable();
  }

  async truncateLog(length: number): Promise<void> {
    const state = this.#mutable();
    if (length < 0 || length > state.logLength) throw new RangeError("Truncate outside the log");
    state.logLength = length;
  }

  async resetLog(): Promise<void> {
    const state = this.#mutable();
    state.logLength = 0;
    state.headers = [null, null];
  }

  close(): void {
    this.#closed = true;
  }
}
