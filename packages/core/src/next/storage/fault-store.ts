/**
 * A fault-injecting durable store for tests: a MemoryStore that can crash at any mutating step
 * and then exhibits, chosen by a seeded generator, any post-crash state the contract allows
 * (contract.ts guarantees 1–6) and no other:
 *
 * - unflushed log appends and header writes are each kept, lost (previous bytes), or torn
 *   (a per-512-byte-sector mix of new and previous bytes, cut mid-sector);
 * - a crash during `flushLog` leaves its writes undetermined, exactly as before it;
 * - unpublished packs vanish, survive whole, or survive truncated (orphans);
 * - a crash during `publish` leaves the old root or the new one; with the new root, its packs are
 *   whole;
 * - reclaims since the last flush or publish may be undone;
 * - a crash during `truncateLog` or `resetLog` leaves the old log or the new one.
 *
 * Usage: count a workload's steps with a plan that never crashes, then replay it once per step
 * with `crashAtStep`, call `recover()`, reopen, and check the engine's recovery invariants.
 */
import {
  type DurableStore,
  type PackId,
  type PackRange,
  type PackWriter,
  type SealedPack,
  type StoreSession,
} from "./contract.js";
import { MemoryStore, type MemoryStoreState } from "./memory-store.js";

export class SimulatedCrash extends Error {
  constructor() {
    super("Simulated crash");
    this.name = "SimulatedCrash";
  }
}

export interface FaultPlan {
  /** Crash at this 0-based mutating step (across sessions). Absent: never crash on its own. */
  readonly crashAtStep?: number;
  /** Seeds every post-crash choice. */
  readonly seed: number;
}

const SECTOR = 512;

/** mulberry32: a small deterministic generator, enough to pick crash outcomes. */
function generator(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
  };
}

interface PendingWrite {
  readonly offset: number;
  readonly bytes: Uint8Array;
  /** What the region physically held before (stale bytes or zeros past the end). */
  readonly before: Uint8Array;
}

interface PendingHeader {
  readonly slot: 0 | 1;
  readonly bytes: Uint8Array;
  readonly before: Uint8Array | null;
}

type Inflight =
  | { readonly kind: "publish"; readonly root: Uint8Array; readonly packs: readonly SealedPack[] }
  | { readonly kind: "truncate"; readonly length: number }
  | { readonly kind: "reset" };

/** Mixes `next` over `before` by sector with one torn sector: what a torn write can leave. */
function tear(next: Uint8Array, before: Uint8Array, random: () => number): Uint8Array {
  const result =
    before.length >= next.length ? before.slice(0, next.length) : new Uint8Array(next.length);
  if (before.length < next.length) result.set(before);
  const sectors = Math.ceil(next.length / SECTOR);
  const tornSector = Math.floor(random() * sectors);
  for (let sector = 0; sector < sectors; sector++) {
    const start = sector * SECTOR;
    const end = Math.min(next.length, start + SECTOR);
    if (sector === tornSector) {
      const cut = start + Math.floor(random() * (end - start));
      result.set(next.subarray(start, cut), start);
    } else if (random() < 0.5) result.set(next.subarray(start, end), start);
  }
  return result;
}

export class FaultInjectingStore implements DurableStore {
  readonly memory: MemoryStore;
  readonly #plan: FaultPlan;
  readonly #random: () => number;
  #steps = 0;
  #crashed = false;
  #durableLogLength: number;
  #pendingWrites: PendingWrite[] = [];
  #pendingHeaders: PendingHeader[] = [];
  /** Packs created and not yet covered by a completed publish. */
  #pendingPacks = new Set<PackId>();
  #pendingReclaims = new Map<PackId, Uint8Array>();
  #inflight: Inflight | null = null;

  constructor(plan: FaultPlan, state?: MemoryStoreState) {
    this.memory = new MemoryStore(state);
    this.#plan = plan;
    this.#random = generator(plan.seed);
    this.#durableLogLength = this.memory.state.logLength;
  }

  /** Mutating steps taken so far. */
  get steps(): number {
    return this.#steps;
  }

  get crashed(): boolean {
    return this.#crashed;
  }

  async open(): Promise<StoreSession> {
    if (this.#crashed) throw new TypeError("Call recover() after a crash before reopening");
    return new FaultSession(this, await this.memory.open());
  }

  /** Crashes now (a tab kill at an arbitrary moment). Every live session call then throws. */
  crash(): void {
    this.#crashed = true;
  }

  /** Resolves the crash into one allowed durable state. Idempotent when not crashed. */
  recover(): void {
    if (!this.#crashed) return;
    const state = this.memory.state;
    const random = this.#random;
    const inflight = this.#inflight;
    if (inflight?.kind === "truncate" && random() < 0.5) state.logLength = inflight.length;
    if (inflight?.kind === "reset" && random() < 0.5) {
      state.logLength = 0;
      state.headers = [null, null];
    }
    if (inflight?.kind === "publish" && random() < 0.5) {
      state.root = inflight.root.slice();
      for (const pack of inflight.packs) this.#pendingPacks.delete(pack.id);
    }
    // Log appends: undo in reverse so each `before` is restored over what followed it.
    let length = this.#durableLogLength;
    const outcomes = this.#pendingWrites.map(() => random());
    for (const [index, write] of [...this.#pendingWrites.entries()].reverse()) {
      const outcome = outcomes[index] ?? 0;
      if (outcome < 1 / 3) state.log.set(write.before, write.offset);
      else if (outcome < 2 / 3)
        state.log.set(tear(write.bytes, write.before, random), write.offset);
    }
    for (const [index, write] of this.#pendingWrites.entries())
      if ((outcomes[index] ?? 0) >= 1 / 3 || random() < 0.25)
        length = Math.max(length, write.offset + write.bytes.length);
    if (this.#pendingWrites.length > 0) state.logLength = length;
    for (const header of [...this.#pendingHeaders].reverse()) {
      const outcome = random();
      if (outcome < 1 / 3) state.headers[header.slot] = header.before;
      else if (outcome < 2 / 3)
        state.headers[header.slot] = tear(header.bytes, header.before ?? new Uint8Array(0), random);
    }
    for (const id of this.#pendingPacks) {
      const outcome = random();
      const chunks = state.writing.get(id);
      const bytes = state.packs.get(id) ?? (chunks === undefined ? undefined : concat(chunks));
      state.writing.delete(id);
      if (bytes === undefined || outcome < 1 / 3) state.packs.delete(id);
      else if (outcome < 2 / 3)
        state.packs.set(id, bytes.slice(0, Math.floor(random() * bytes.length)));
      else state.packs.set(id, bytes);
    }
    for (const [id, bytes] of this.#pendingReclaims) if (random() < 0.5) state.packs.set(id, bytes);
    state.writing.clear();
    this.#settle();
    this.#durableLogLength = state.logLength;
    this.#crashed = false;
  }

  #settle(): void {
    this.#pendingWrites = [];
    this.#pendingHeaders = [];
    this.#pendingPacks.clear();
    this.#pendingReclaims.clear();
    this.#inflight = null;
  }

  /** Counts one mutating step; crashes instead when the plan says so. */
  step(inflight?: Inflight): void {
    if (this.#crashed) throw new SimulatedCrash();
    const step = this.#steps++;
    if (step === this.#plan.crashAtStep) {
      this.#inflight = inflight ?? null;
      this.#crashed = true;
      throw new SimulatedCrash();
    }
  }

  alive(): void {
    if (this.#crashed) throw new SimulatedCrash();
  }

  recordAppend(offset: number, bytes: Uint8Array): void {
    const state = this.memory.state;
    const physical =
      state.log.length > offset
        ? state.log.slice(offset, offset + bytes.length)
        : new Uint8Array(0);
    const before = new Uint8Array(bytes.length);
    before.set(physical);
    this.#pendingWrites.push({ offset, bytes: bytes.slice(), before });
  }

  recordHeader(slot: 0 | 1, bytes: Uint8Array): void {
    this.#pendingHeaders.push({
      slot,
      bytes: bytes.slice(),
      before: this.memory.state.headers[slot],
    });
  }

  recordPack(id: PackId): void {
    this.#pendingPacks.add(id);
  }

  recordReclaim(id: PackId): void {
    const bytes = this.memory.state.packs.get(id);
    if (bytes !== undefined) this.#pendingReclaims.set(id, bytes);
  }

  flushed(): void {
    this.#pendingWrites = [];
    this.#pendingHeaders = [];
    this.#pendingReclaims.clear();
    this.#durableLogLength = this.memory.state.logLength;
  }

  published(packs: readonly SealedPack[]): void {
    for (const pack of packs) this.#pendingPacks.delete(pack.id);
    this.#pendingReclaims.clear();
  }

  logRewritten(): void {
    this.#pendingWrites = [];
    this.#pendingHeaders = [];
    this.#durableLogLength = this.memory.state.logLength;
  }
}

function concat(chunks: readonly Uint8Array[]): Uint8Array {
  const bytes = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  return bytes;
}

class FaultSession implements StoreSession {
  readonly #store: FaultInjectingStore;
  readonly #inner: StoreSession;

  constructor(store: FaultInjectingStore, inner: StoreSession) {
    this.#store = store;
    this.#inner = inner;
  }

  get epoch(): number {
    return this.#inner.epoch;
  }

  async readRoot(): Promise<Uint8Array | null> {
    this.#store.alive();
    return this.#inner.readRoot();
  }

  async publish(root: Uint8Array, packs: readonly SealedPack[]): Promise<void> {
    this.#store.step({ kind: "publish", root: root.slice(), packs: [...packs] });
    await this.#inner.publish(root, packs);
    this.#store.published(packs);
  }

  createPack(id: PackId): PackWriter {
    this.#store.step();
    const writer = this.#inner.createPack(id);
    this.#store.recordPack(id);
    const store = this.#store;
    return {
      id,
      get length() {
        return writer.length;
      },
      async append(bytes) {
        store.step();
        return writer.append(bytes);
      },
      async seal() {
        store.step();
        return writer.seal();
      },
      async abort() {
        store.step();
        return writer.abort();
      },
    };
  }

  async read(ranges: readonly PackRange[]): Promise<Uint8Array[]> {
    this.#store.alive();
    return this.#inner.read(ranges);
  }

  async listPacks(): Promise<PackId[]> {
    this.#store.alive();
    return this.#inner.listPacks();
  }

  async reclaim(ids: readonly PackId[]): Promise<void> {
    this.#store.step();
    for (const id of ids) this.#store.recordReclaim(id);
    await this.#inner.reclaim(ids);
  }

  logLength(): number {
    this.#store.alive();
    return this.#inner.logLength();
  }

  async readLog(offset: number, length: number): Promise<Uint8Array> {
    this.#store.alive();
    return this.#inner.readLog(offset, length);
  }

  async readLogHeaders(): Promise<readonly [Uint8Array | null, Uint8Array | null]> {
    this.#store.alive();
    return this.#inner.readLogHeaders();
  }

  appendLog(bytes: Uint8Array): number {
    this.#store.alive();
    const offset = this.#inner.logLength();
    this.#store.recordAppend(offset, bytes);
    // The bytes are written before the step check so a crash at this step can tear them.
    const written = this.#inner.appendLog(bytes);
    this.#store.step();
    return written;
  }

  writeLogHeader(slot: 0 | 1, bytes: Uint8Array): void {
    this.#store.alive();
    this.#store.recordHeader(slot, bytes);
    this.#inner.writeLogHeader(slot, bytes);
    this.#store.step();
  }

  async flushLog(): Promise<void> {
    this.#store.step();
    await this.#inner.flushLog();
    this.#store.flushed();
  }

  async truncateLog(length: number): Promise<void> {
    this.#store.step({ kind: "truncate", length });
    await this.#inner.truncateLog(length);
    this.#store.logRewritten();
  }

  async resetLog(): Promise<void> {
    this.#store.step({ kind: "reset" });
    await this.#inner.resetLog();
    this.#store.logRewritten();
  }

  close(): void {
    this.#inner.close();
  }
}
