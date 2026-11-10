/**
 * The extent space's one cache of encoded, verified blobs (extents.ts "Reads and the cache").
 *
 * - **Entries** are exact `(address, length, checksum)` copies in their own ArrayBuffers, indexed
 *   by address and by extent offset (so freeing an extent drops its blobs in one step).
 * - **Charging.** An unpinned entry is charged to the governor's cache handle (its bytes plus
 *   `ENTRY_OVERHEAD`). A pinned entry (held by at least one lease) is charged to each lease's
 *   budget instead and is not evictable: the cache uncharges it on the first pin and charges it
 *   again when the last lease releases it, or drops it if the governor has no room. A reader's
 *   parsed descriptor (`attachParsed`) is charged to the cache handle from the moment it is
 *   attached until its entry is gone, whether the entry is pinned or not: pinning moves only the
 *   blob's bytes and overhead. The two fixed slabs (read and write, 1 MiB each) are charged once
 *   at `attach`.
 * - **Segmented LRU.** A new entry enters probation; a second use promotes it to protected, which
 *   is held to `PROTECTED_SHARE` of resident bytes (its least recently used entries are demoted to
 *   probation). Eviction takes probation's least recently used first. A miss that cannot be
 *   charged evicts probation (never protected) to make room, so a one-pass scan recycles
 *   probation and leaves hot directories, roots and fences alone. `oldestUnusedAt` is when the
 *   next victim was last used, so the governor can evict across caches oldest first.
 * - **Misses** of one call are planned into slabs (slabs.ts), read one slab at a time into the
 *   shared 1 MiB read slab (serialized: the slab is one buffer), verified and copied out.
 * - **Reentrancy.** The governor may call `evict` from inside any charge or release this class
 *   makes (`memory/governor.ts` rule 7). Every list or map change is complete before a call out,
 *   and pinned entries are never in a list, so a nested eviction sees a consistent cache.
 */
import { pauseIfDue, type TaskWork } from "../task.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { VERIFY_CACHED_BYTES, verifyCached } from "./verify-cached.js";
import { StoreClosedError, StoreCorruptionError, type StoreSession } from "./contract.js";
import {
  MAX_BLOB_BYTES,
  MAX_READ_BYTES,
  ReadDeniedError,
  type BlobRef,
  type ByteBudget,
  type CacheMemory,
  type EvictableCacheLike,
  type PeekOptions,
  type ReadLease,
  type ReadOptions,
} from "./extents.js";
import { MAX_SLAB, MAX_SLAB_GAP, planSlabs } from "./slabs.js";

/** Bytes charged per cached entry beyond its data (governor `OBJECT_OVERHEAD_BYTES`). */
export const ENTRY_OVERHEAD = 64;
/** Protected entries may hold at most this share of the resident bytes. */
const PROTECTED_SHARE = 0.8;
/** The read and write slabs the space charges at attach. */
export const FIXED_CACHE_BYTES = 2 * MAX_SLAB;

type Segment = "probation" | "protected" | "pinned";

interface Entry {
  readonly address: number;
  readonly extent: number;
  readonly length: number;
  readonly checksum: number;
  readonly bytes: Uint8Array;
  pins: number;
  /**
   * Pins held by one-pass reads (`ReadOptions.once`). While every pin is one, the entry stays
   * listed where it was, its recency untouched (review B2-p2h #1): `segment` is then its list,
   * not "pinned", and eviction skips it.
   */
  oncePins: number;
  segment: Segment;
  /** Dropped while pinned (its extent was freed): never admitted again. */
  dropped: boolean;
  /** Used again after its first read: admitted to protected when it unpins. */
  hot: boolean;
  /** When it was last used: listed after a release, or peeked (`performance.now()`). */
  usedAt: number;
  /** A reader's parsed descriptor of `bytes` (views into them), and its charged size. */
  parsed: object | null;
  parsedBytes: number;
  prev: Entry | null;
  next: Entry | null;
}

/** A doubly linked LRU list; `head` is the most recently used. */
class List {
  head: Entry | null = null;
  tail: Entry | null = null;
  bytes = 0;
  count = 0;

  pushFront(entry: Entry): void {
    entry.prev = null;
    entry.next = this.head;
    if (this.head !== null) this.head.prev = entry;
    this.head = entry;
    this.tail ??= entry;
    this.bytes += entry.length + entry.parsedBytes;
    this.count += 1;
  }

  remove(entry: Entry): void {
    if (entry.prev !== null) entry.prev.next = entry.next;
    else this.head = entry.next;
    if (entry.next !== null) entry.next.prev = entry.prev;
    else this.tail = entry.prev;
    entry.prev = null;
    entry.next = null;
    this.bytes -= entry.length + entry.parsedBytes;
    this.count -= 1;
  }
}

export interface CacheStats {
  readonly residentBytes: number;
  /** Bytes in the protected segment (entries used more than once), descriptors included. */
  readonly protectedBytes: number;
  /** Idle pooled one-pass buffers (`ReadOptions.reuseBuffers`), charged to the cache. */
  readonly poolBytes: number;
  readonly pinnedBytes: number;
  readonly hits: number;
  readonly misses: number;
  readonly platformReads: number;
  readonly bytesRead: number;
}

/** The slab buffers a one-pass lease owns: its charge adjustment, its blobs' bytes, its views. */
interface OwnedSlabs {
  /** Signed adjustment from blob charges to backing bytes: negative for overlapping refs. */
  chargeDelta: number;
  bytes: number;
  readonly views: Array<{ readonly bytes: Uint8Array; readonly checksum: number }>;
  /** The pooled buffer the lease's views live in (`ReadOptions.reuseBuffers`), or null. */
  pooled: ArrayBuffer | null;
}

const align8 = (bytes: number): number => (bytes + 7) & ~7;

/** Pooled one-pass buffers come in powers of two from this size... */
export const POOL_MIN_BUFFER = 64 * 1024;
/** ...to this (a larger read gets buffers of its own). */
export const POOL_MAX_BUFFER = 16 * 1024 * 1024;
/** At most this many idle pooled bytes are kept (charged to the cache); the rest are dropped. */
export const POOL_IDLE_BYTES = 64 * 1024 * 1024;
/** What a released pooled buffer is filled with in test builds, so a stale view reads garbage. */
const POISON = 0xa5;

/** The pooled buffer size for `bytes`, or null when it is past `POOL_MAX_BUFFER`. */
function poolClass(bytes: number): number | null {
  let size = POOL_MIN_BUFFER;
  while (size < bytes) size *= 2;
  return size <= POOL_MAX_BUFFER ? size : null;
}

/** What an unpinned entry is charged: its bytes, its overhead and any attached descriptor. */
const charge = (entry: Entry) => entry.length + ENTRY_OVERHEAD + entry.parsedBytes;
/**
 * The part of that charge that moves to the leases while an entry is pinned. An attached
 * descriptor's bytes stay charged to the cache from `attachParsed` until the entry is gone,
 * pinned or not (review B2-p2e #1): readers' leases pay for blob bytes only.
 */
const base = (entry: Entry) => entry.length + ENTRY_OVERHEAD;

const EMPTY = new Uint8Array(0);
/** xxHash32 (seed 0) of no bytes: the only valid checksum of a zero-length blob. */
const EMPTY_CHECKSUM = xxHash32(EMPTY);

/** A blob's full identity: its extent, its place in it, its length and its checksum. */
const identity = (ref: BlobRef): string =>
  `${String(ref.extent.offset)}/${String(ref.extent.length)}/${String(ref.offset)}/${String(ref.length)}/${String(ref.checksum)}`;

/** Refuses a ref whose stored coordinates cannot be right, before any I/O. */
function checkRef(ref: BlobRef): void {
  const { extent, offset, length, checksum } = ref;
  if (length > MAX_BLOB_BYTES)
    throw new RangeError(`A blob of ${String(length)} bytes exceeds MAX_BLOB_BYTES`);
  const integral = [extent.offset, extent.length, offset, length].every(
    (value) => Number.isSafeInteger(value) && value >= 0,
  );
  if (
    !integral ||
    !(Number.isInteger(checksum) && checksum >= 0 && checksum <= 0xffffffff) ||
    offset + length > extent.length
  )
    throw new StoreCorruptionError(
      `Blob ${String(offset)}+${String(length)} does not lie inside its extent at ${String(extent.offset)} (${String(extent.length)} bytes)`,
    );
  if (length === 0 && checksum !== EMPTY_CHECKSUM)
    throw new StoreCorruptionError(
      `A zero-length blob at ${String(extent.offset + offset)} has a checksum that is not of no bytes`,
    );
}

export class PageCache implements EvictableCacheLike {
  readonly #session: StoreSession;
  readonly #byAddress = new Map<number, Entry>();
  readonly #byExtent = new Map<number, Set<Entry>>();
  readonly #probation = new List();
  readonly #protected = new List();
  #slab = new Uint8Array(0);
  #memory: CacheMemory | null = null;
  #slabBusy: Promise<void> = Promise.resolve();
  #pinnedBytes = 0;
  /** Idle pooled one-pass buffers by size, and their bytes (charged to the cache). */
  readonly #pool = new Map<number, ArrayBuffer[]>();
  #poolIdle = 0;
  readonly #poolLimit: number;
  /** Entries pinned only by one-pass reads, still listed where they were (review B2-p2h #1). */
  readonly #listedPinned = new Set<Entry>();
  #hits = 0;
  #misses = 0;
  #platformReads = 0;
  #bytesRead = 0;
  #closed = false;

  /** `poolIdleBytes`: the idle pooled bytes kept (default `POOL_IDLE_BYTES`). */
  constructor(session: StoreSession, options: { readonly poolIdleBytes?: number } = {}) {
    this.#session = session;
    this.#poolLimit = options.poolIdleBytes ?? POOL_IDLE_BYTES;
  }

  /** Connects the governor's handle and charges the fixed slabs (`RangeError` if it cannot). */
  attach(memory: CacheMemory, deferReadSlab = false): void {
    if (this.#memory !== null || this.#closed)
      throw new TypeError("Cache already attached or closed");
    if (!memory.charge(FIXED_CACHE_BYTES)) {
      memory.unregister?.();
      throw new RangeError("The memory cap cannot hold the extent space's 2 MiB of fixed slabs");
    }
    try {
      if (!deferReadSlab) this.#slab = new Uint8Array(MAX_SLAB);
    } catch (error) {
      memory.release(FIXED_CACHE_BYTES);
      memory.unregister?.();
      throw error;
    }
    this.#memory = memory;
  }

  /** Root loading temporarily uses the paid slab allowance for one native slot response. */
  activateReadSlab(): void {
    if (this.#memory === null || this.#closed || this.#slab.length !== 0)
      throw new TypeError("Read slab cannot activate");
    this.#slab = new Uint8Array(MAX_SLAB);
  }

  /** Borrow the already charged read slab for raw list cursors. Acquire before taking the
   * ownership-transition barrier, and release before freeing list pages. The callback performs
   * only prepaid work/native I/O: no cache.read reentry, governor admission, or caller waits.
   */
  async withReadSlab<T>(work: (slab: Uint8Array) => Promise<T>): Promise<T> {
    const previous = this.#slabBusy;
    let done!: () => void;
    this.#slabBusy = new Promise<void>((resolve) => {
      done = resolve;
    });
    await previous;
    try {
      this.#live();
      return await work(this.#slab);
    } finally {
      done();
    }
  }
  /** Close drains borrowed/shared-slab operations before returning the fixed slab charge. */
  async settled(): Promise<void> {
    for (;;) {
      const pending = this.#slabBusy;
      await pending;
      if (pending === this.#slabBusy) return;
    }
  }
  /** Exact raw read into a borrowed slab view; raw list decoding verifies its blob checksum. */
  async readRaw(offset: number, target: Uint8Array): Promise<void> {
    this.#live();
    if (
      target.buffer !== this.#slab.buffer ||
      target.byteOffset < this.#slab.byteOffset ||
      target.byteOffset + target.length > this.#slab.byteOffset + this.#slab.length
    )
      throw new TypeError("Raw list reads require a borrowed read-slab view");
    await this.#session.readDataInto(offset, target, { serialRecords: true });
    this.#platformReads++;
    this.#bytesRead += target.length;
    this.#live();
  }

  get stats(): CacheStats {
    let listedPinned = 0;
    let protectedPinned = 0;
    for (const entry of this.#listedPinned) {
      listedPinned += entry.length;
      if (entry.segment === "protected") protectedPinned += entry.length + entry.parsedBytes;
    }
    return {
      residentBytes: this.#probation.bytes + this.#protected.bytes + this.#pinnedBytes,
      poolBytes: this.#poolIdle,
      protectedBytes: this.#protected.bytes - protectedPinned,
      pinnedBytes: this.#pinnedBytes + listedPinned,
      hits: this.#hits,
      misses: this.#misses,
      platformReads: this.#platformReads,
      bytesRead: this.#bytesRead,
    };
  }

  // ── Governor side ──────────────────────────────────────────────────────────────────────────

  get evictableBytes(): number {
    let pinned = 0;
    for (const entry of this.#listedPinned) pinned += charge(entry);
    return (
      this.#poolIdle +
      this.#probation.bytes +
      this.#protected.bytes +
      ENTRY_OVERHEAD * (this.#probation.count + this.#protected.count) -
      pinned
    );
  }

  /** When the entry `evict` takes next was last used (the governor's cross-cache order). */
  get oldestUnusedAt(): number | undefined {
    // Idle pooled buffers are the cheapest bytes to give back: they go first.
    if (this.#poolIdle > 0) return 0;
    return this.#victim()?.usedAt;
  }

  evict(bytes: number): number {
    let freed = this.#trimPool(bytes);
    while (freed < bytes) {
      const victim = this.#victim();
      if (victim === null) break;
      freed += this.#evictEntry(victim);
    }
    return freed;
  }

  /** The next entry to evict: least recently used of probation, then protected; never pinned. */
  #victim(only: "probation" | null = null, except: Entry | null = null): Entry | null {
    for (const list of only === null ? [this.#probation, this.#protected] : [this.#probation])
      for (let entry = list.tail; entry !== null; entry = entry.prev)
        if (entry.pins === 0 && entry !== except) return entry;
    return null;
  }

  // ── Reads ──────────────────────────────────────────────────────────────────────────────────

  /** The descriptor attached to `ref`'s entry, or null (extents.ts `ExtentReader.parsed`). */
  parsed(ref: BlobRef): object | null {
    return this.#find(ref)?.parsed ?? null;
  }

  /**
   * Attaches a parsed descriptor to `ref`'s resident entry (extents.ts `ExtentReader.attach`),
   * charging `bytes` to the cache now, pinned or not; the charge stays until the entry is
   * evicted, dropped or forgotten, with which the descriptor goes.
   */
  attachParsed(ref: BlobRef, parsed: object, bytes: number, of: Uint8Array): boolean {
    const entry = this.#find(ref);
    // Not resident, already carrying one, or a descriptor of other bytes (a one-pass read's
    // private copy of this blob, review B2-p2g #3): no.
    if (entry?.parsed !== null || entry.bytes !== of || this.#closed) return false;
    if (!(Number.isSafeInteger(bytes) && bytes >= 0))
      throw new RangeError("A descriptor's size must be a byte count");
    // Charged before it is attached, pinned or not: refused, nothing is attached.
    if (this.#memory?.charge(bytes) !== true) return false;
    // Listed: its list's bytes include the descriptor from now on.
    if (entry.segment === "probation") this.#probation.bytes += bytes;
    else if (entry.segment === "protected") this.#protected.bytes += bytes;
    entry.parsed = parsed;
    entry.parsedBytes = bytes;
    return true;
  }

  peek(ref: BlobRef, options: PeekOptions = {}): Uint8Array | null {
    const entry = this.#find(ref);
    if (entry === null) return null;
    this.#hits += 1;
    // A one-pass scan's hit is not a use: no promotion, no move, no recency (review B2-p2g #2).
    if (options.once === true) return entry.bytes;
    entry.hot = true;
    entry.usedAt = performance.now();
    this.#touch(entry);
    return entry.bytes;
  }

  /**
   * Requests are deduplicated by full identity (extent, offset, length, checksum), never by
   * address alone (review B1-p2a finding 2): two different refs at one address are each resolved
   * and verified. A zero-length blob needs no I/O and no charge, but its checksum must still be
   * that of no bytes.
   */
  async read(refs: readonly BlobRef[], options: ReadOptions): Promise<ReadLease> {
    this.#live();
    const { budget, signal } = options;
    const once = options.once === true || options.retain === false;
    signal?.throwIfAborted();
    const distinct = new Map<string, BlobRef>();
    let total = 0;
    for (const ref of refs) {
      checkRef(ref);
      const key = identity(ref);
      if (distinct.has(key)) continue;
      distinct.set(key, ref);
      total += ref.length;
    }
    if (total > MAX_READ_BYTES)
      throw new RangeError(`A read of ${String(total)} bytes exceeds MAX_READ_BYTES`);
    // Charge the reader first: the governor may evict cache entries to make room.
    if (!budget.allocate(total)) throw new ReadDeniedError(total);
    const held: Entry[] = [];
    const resolved = new Map<string, Uint8Array>();
    const owned: OwnedSlabs = { chargeDelta: 0, bytes: 0, views: [], pooled: null };
    try {
      const misses: Array<{ readonly key: string; readonly ref: BlobRef }> = [];
      let uncharge = 0;
      for (const [key, ref] of distinct) {
        if (ref.length === 0) {
          resolved.set(key, EMPTY);
          continue;
        }
        const entry = this.#find(ref);
        if (entry === null) {
          misses.push({ key, ref });
          continue;
        }
        this.#hits += 1;
        if (!once) entry.hot = true; // a one-pass scan is not a reuse
        if (entry.pins === 0) {
          uncharge += base(entry); // its descriptor stays charged here
          // A one-pass pin leaves the entry where it is, recency and segment untouched.
          if (once) this.#listedPinned.add(entry);
          else this.#unlistPinned(entry);
        } else if (!once && entry.segment !== "pinned") this.#unlistPinned(entry);
        entry.pins += 1;
        if (once) entry.oncePins += 1;
        held.push(entry);
        resolved.set(key, entry.bytes);
      }
      // Hits are pinned before the cache's charge goes back: a nested eviction cannot take them.
      if (uncharge > 0) this.#memory?.release(uncharge);
      if (misses.length > 0) {
        if (options.retain === false)
          await this.#readMisses(misses, held, resolved, signal, owned, options.task);
        else if (options.once === true)
          await this.#readPrivate(
            misses,
            resolved,
            signal,
            budget,
            owned,
            options.reuseBuffers === true,
          );
        else await this.#readMisses(misses, held, resolved, signal);
      }
    } catch (error) {
      this.#unpinAll(held, once);
      this.#pinnedBytes -= owned.bytes;
      budget.free(total + owned.chargeDelta);
      this.#givePooled(owned);
      throw error;
    }
    const bytes = refs.map((ref) => {
      const found = resolved.get(identity(ref));
      if (found === undefined) throw new Error("A requested blob was not resolved");
      return found;
    });
    let released = false;
    return {
      bytes,
      release: () => {
        if (released) return;
        released = true;
        if (VERIFY_CACHED_BYTES) {
          for (const entry of held) verifyCached(entry.bytes, entry.checksum, "lease release");
          for (const { bytes, checksum } of owned.views)
            verifyCached(bytes, checksum, "lease release");
        }
        this.#unpinAll(held, once);
        this.#pinnedBytes -= owned.bytes;
        budget.free(total + owned.chargeDelta);
        this.#givePooled(owned);
      },
    };
  }

  async #readMisses(
    misses: ReadonlyArray<{ readonly key: string; readonly ref: BlobRef }>,
    held: Entry[],
    resolved: Map<string, Uint8Array>,
    signal: AbortSignal | undefined,
    owned?: OwnedSlabs,
    task?: TaskWork,
  ): Promise<void> {
    const ranges = misses.map(({ ref }) => ({
      offset: ref.extent.offset + ref.offset,
      length: ref.length,
    }));
    const slabs = planSlabs(ranges);
    await this.withReadSlab(async (buffer) => {
      for (const slab of slabs) {
        // Keep exact export continuations cooperative even on synchronous OPFS handles.
        if (owned) {
          const pause = pauseIfDue({ ...task, ...(signal ? { signal } : {}) });
          if (pause) await pause;
        }
        signal?.throwIfAborted();
        this.#live();
        const target = buffer.subarray(0, slab.length);
        await this.#session.readDataInto(slab.offset, target);
        this.#platformReads += 1;
        this.#bytesRead += slab.length;
        for (const member of slab.members) {
          const miss = misses[member];
          if (miss === undefined) continue;
          const { key, ref } = miss;
          const address = ref.extent.offset + ref.offset;
          const start = address - slab.offset;
          const bytes = target.slice(start, start + ref.length);
          if (xxHash32(bytes) !== ref.checksum)
            throw new StoreCorruptionError(
              `The blob at ${String(address)} (extent ${String(ref.extent.offset)}, ${String(ref.length)} bytes) failed its checksum`,
            );
          this.#misses += 1;
          if (owned !== undefined) {
            owned.bytes += bytes.length;
            this.#pinnedBytes += bytes.length;
            if (VERIFY_CACHED_BYTES) owned.views.push({ bytes, checksum: ref.checksum });
            resolved.set(key, bytes);
          } else {
            const entry = this.#insertPinned(ref, address, bytes);
            held.push(entry);
            resolved.set(key, entry.bytes);
          }
        }
      }
    });
  }

  /**
   * A one-pass read's misses (review B2-p2h, increment 9): never admitted, never findable, and
   * never copied. Each slab is read into a buffer of its own, which this lease alone holds, and
   * every blob is a verified view of it (the writer starts pages at 8-byte offsets, so the views
   * are aligned for typed arrays). The lease is charged for the blobs (by `read`) and for the gap
   * bytes between them inside the slabs (here, before any read), and gives both back on
   * release, when the buffers become garbage. No shared slab, no `Entry`, no copy.
   */
  async #readPrivate(
    misses: ReadonlyArray<{ readonly key: string; readonly ref: BlobRef }>,
    resolved: Map<string, Uint8Array>,
    signal: AbortSignal | undefined,
    budget: ByteBudget,
    owned: OwnedSlabs,
    reuse: boolean,
  ): Promise<void> {
    // Each slab is this lease's own buffer, so it may be as large as a read (not the shared slab).
    const slabs = planSlabs(
      misses.map(({ ref }) => ({ offset: ref.extent.offset + ref.offset, length: ref.length })),
      MAX_READ_BYTES + 2 * MAX_SLAB_GAP,
    );
    let gaps = 0;
    let members = 0;
    for (const slab of slabs) {
      let inside = 0;
      for (const member of slab.members) inside += misses[member]?.ref.length ?? 0;
      gaps += Math.max(0, slab.length - inside);
      members += inside;
    }
    // With `reuse`, every slab goes into one pooled buffer at 8-byte offsets, and the lease is
    // charged for the whole buffer (its rest instead of the gaps).
    let packed = 0;
    for (const slab of slabs) packed += align8(slab.length);
    const size = reuse ? poolClass(packed) : null;
    const extra = size === null ? gaps : size - members;
    if (extra > 0 && !budget.allocate(extra)) throw new ReadDeniedError(extra);
    // read() charged distinct reference lengths, which can exceed their buffer's union.
    // Apply both signs, so release/error cleanup frees exactly the net allocation.
    if (extra < 0) budget.free(-extra);
    owned.chargeDelta = extra;
    owned.bytes = members;
    this.#pinnedBytes += members;
    if (size !== null) owned.pooled = this.#takePooled(size);
    let cursor = 0;
    for (const slab of slabs) {
      signal?.throwIfAborted();
      this.#live();
      let buffer: Uint8Array;
      if (owned.pooled === null) buffer = new Uint8Array(slab.length);
      else {
        buffer = new Uint8Array(owned.pooled, cursor, slab.length);
        cursor += align8(slab.length);
      }
      await this.#session.readDataInto(slab.offset, buffer);
      this.#platformReads += 1;
      this.#bytesRead += slab.length;
      for (const member of slab.members) {
        const miss = misses[member];
        if (miss === undefined) continue;
        const { key, ref } = miss;
        const address = ref.extent.offset + ref.offset;
        const start = address - slab.offset;
        const bytes = buffer.subarray(start, start + ref.length);
        if (xxHash32(bytes) !== ref.checksum)
          throw new StoreCorruptionError(
            `The blob at ${String(address)} (extent ${String(ref.extent.offset)}, ${String(ref.length)} bytes) failed its checksum`,
          );
        this.#misses += 1;
        resolved.set(key, bytes);
        if (VERIFY_CACHED_BYTES) owned.views.push({ bytes, checksum: ref.checksum });
      }
    }
  }

  /**
   * A buffer of `size` bytes for one lease: an idle pooled one (its charge moves from the cache
   * to the lease, which has just paid for it), else a new one.
   */
  #takePooled(size: number): ArrayBuffer {
    const idle = this.#pool.get(size)?.pop();
    if (idle === undefined) return new ArrayBuffer(size);
    this.#poolIdle -= size;
    this.#memory?.release(size);
    return idle;
  }

  /**
   * A released lease's pooled buffer back to the pool, charged to the cache while it waits; it is
   * dropped instead when the pool is full, the cache is closed, or the governor refuses.
   */
  #givePooled(owned: OwnedSlabs): void {
    const buffer = owned.pooled;
    if (buffer === null) return;
    owned.pooled = null;
    if (VERIFY_CACHED_BYTES) new Uint8Array(buffer).fill(POISON);
    const size = buffer.byteLength;
    if (this.#closed || this.#poolIdle + size > this.#poolLimit) return;
    if (this.#memory?.charge(size) !== true) return;
    let list = this.#pool.get(size);
    if (list === undefined) {
      list = [];
      this.#pool.set(size, list);
    }
    list.push(buffer);
    this.#poolIdle += size;
  }

  /** Drops idle pooled buffers until `bytes` are freed (or none is left); returns the bytes. */
  #trimPool(bytes: number): number {
    let freed = 0;
    for (const [size, list] of this.#pool) {
      while (freed < bytes && list.pop() !== undefined) {
        this.#poolIdle -= size;
        this.#memory?.release(size);
        freed += size;
      }
      if (freed >= bytes) break;
    }
    return freed;
  }

  /** A verified miss enters the cache pinned (another read may have inserted it meanwhile). */
  #insertPinned(ref: BlobRef, address: number, bytes: Uint8Array): Entry {
    const existing = this.#find(ref);
    if (existing !== null) {
      if (existing.pins === 0) {
        this.#unlistPinned(existing);
        // Its blob bytes move to the lease; an attached descriptor stays charged here (B2-p2e).
        this.#memory?.release(base(existing));
      } else if (existing.segment !== "pinned") this.#unlistPinned(existing);
      existing.pins += 1;
      return existing;
    }
    // Another blob at this address (a different length or checksum) keeps its place while a
    // lease pins it: this one is then private to its lease, never cached. Unpinned, it yields.
    const other = this.#byAddress.get(address);
    if (other !== undefined && other.pins > 0) return this.#privateEntry(ref, address, bytes);
    if (other !== undefined) this.#drop(other);
    const entry: Entry = {
      address,
      extent: ref.extent.offset,
      length: ref.length,
      checksum: ref.checksum,
      bytes,
      pins: 1,
      oncePins: 0,
      segment: "pinned",
      dropped: false,
      hot: false,
      usedAt: 0,
      parsed: null,
      parsedBytes: 0,
      prev: null,
      next: null,
    };
    this.#byAddress.set(address, entry);
    let set = this.#byExtent.get(entry.extent);
    if (set === undefined) {
      set = new Set();
      this.#byExtent.set(entry.extent, set);
    }
    set.add(entry);
    this.#pinnedBytes += entry.length;
    return entry;
  }

  #privateEntry(ref: BlobRef, address: number, bytes: Uint8Array): Entry {
    this.#pinnedBytes += ref.length;
    return {
      address,
      extent: ref.extent.offset,
      length: ref.length,
      checksum: ref.checksum,
      bytes,
      pins: 1,
      oncePins: 0,
      segment: "pinned",
      dropped: true,
      hot: false,
      usedAt: 0,
      parsed: null,
      parsedBytes: 0,
      prev: null,
      next: null,
    };
  }

  /** Lets go of one pin per entry; `once`: the pins were a one-pass read's. */
  #unpinAll(held: readonly Entry[], once: boolean): void {
    for (const entry of held) {
      entry.pins -= 1;
      if (once) entry.oncePins -= 1;
      const listed = entry.segment !== "pinned";
      if (entry.pins > 0) {
        // The last ordinary pin went while one-pass pins remain: placed now, as this release
        // would place it, and paid for by those pins' leases until the last one goes.
        if (!once && !listed && entry.pins === entry.oncePins && !entry.dropped && !this.#closed) {
          this.#pinnedBytes -= entry.length;
          this.#place(entry);
          this.#listedPinned.add(entry);
        }
        continue;
      }
      if (listed) {
        // Only one-pass pins since it was last placed: charged again where it is.
        this.#listedPinned.delete(entry);
        if (!this.#chargeBase(entry)) {
          this.#unlist(entry);
          entry.segment = "probation";
          this.#forget(entry);
          this.#releaseParsed(entry);
        }
        continue;
      }
      this.#pinnedBytes -= entry.length;
      if (entry.dropped || this.#closed) {
        entry.segment = "probation";
        this.#releaseParsed(entry);
        continue;
      }
      this.#admit(entry);
    }
  }

  /** A pinned entry leaves its list (an ordinary use pins it, or it is dropped while pinned). */
  #unlistPinned(entry: Entry): void {
    this.#unlist(entry);
    this.#listedPinned.delete(entry);
    entry.segment = "pinned";
    this.#pinnedBytes += entry.length;
  }

  /** Gives back an entry's descriptor charge (the entry is leaving the cache). */
  #releaseParsed(entry: Entry): void {
    if (entry.parsedBytes > 0) this.#memory?.release(entry.parsedBytes);
    entry.parsed = null;
    entry.parsedBytes = 0;
  }

  /**
   * Charges an unpinned entry to the cache (its descriptor is charged already), evicting
   * probation for it, or drops it with its descriptor.
   */
  #admit(entry: Entry): void {
    if (!this.#chargeBase(entry)) {
      entry.segment = "probation";
      this.#forget(entry);
      this.#releaseParsed(entry);
      return;
    }
    this.#place(entry);
  }

  /** Charges an entry's base to the cache, recycling unpinned probation to make room. */
  #chargeBase(entry: Entry): boolean {
    const memory = this.#memory;
    const cost = base(entry);
    let admitted = memory?.charge(cost) ?? false;
    if (!admitted && memory !== null && this.#probationCharge(entry) >= cost) {
      // Recycle probation (a scan's own pages) to make room; never protected, never pinned.
      let freed = 0;
      for (let victim = this.#victim("probation", entry); freed < cost && victim !== null;) {
        freed += this.#evictEntry(victim);
        victim = this.#victim("probation", entry);
      }
      admitted = freed >= cost && memory.charge(cost);
    }
    return admitted;
  }

  /** Lists an entry as a release does: probation front, then protected if it was reused. */
  #place(entry: Entry): void {
    entry.segment = "probation";
    entry.usedAt = performance.now();
    this.#probation.pushFront(entry);
    if (entry.hot) this.#touch(entry);
  }

  /** What evicting probation could free, apart from pinned entries and `except`. */
  #probationCharge(except: Entry): number {
    let pinned = 0;
    for (const entry of this.#listedPinned)
      if (entry.segment === "probation" || entry === except) pinned += charge(entry);
    if (except.segment === "probation" && !this.#listedPinned.has(except)) pinned += charge(except);
    return this.#probation.bytes + ENTRY_OVERHEAD * this.#probation.count - pinned;
  }

  // ── Invalidation ───────────────────────────────────────────────────────────────────────────

  /** Drops every blob of the extent at `offset` (called before its space can be reused). */
  dropExtent(offset: number): void {
    const set = this.#byExtent.get(offset);
    if (set === undefined) return;
    for (const entry of [...set]) this.#drop(entry);
  }

  /** Drops everything (the space is closing) and returns the fixed charge. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#slab = new Uint8Array(0);
    for (const entry of [...this.#byAddress.values()]) this.#drop(entry);
    this.#trimPool(Number.POSITIVE_INFINITY);
    this.#memory?.release(FIXED_CACHE_BYTES);
    this.#memory?.unregister?.();
  }

  #drop(entry: Entry): void {
    if (entry.pins > 0) {
      if (entry.segment !== "pinned") this.#unlistPinned(entry);
      entry.dropped = true;
      this.#forget(entry);
      return;
    }
    this.#evictEntry(entry);
  }

  /** Removes an unpinned, listed entry and returns its charge to the governor. */
  #evictEntry(entry: Entry): number {
    if (VERIFY_CACHED_BYTES) verifyCached(entry.bytes, entry.checksum, "eviction");
    this.#unlist(entry);
    this.#forget(entry);
    const cost = charge(entry);
    this.#memory?.release(cost);
    return cost;
  }

  #forget(entry: Entry): void {
    if (this.#byAddress.get(entry.address) === entry) this.#byAddress.delete(entry.address);
    const set = this.#byExtent.get(entry.extent);
    if (set !== undefined) {
      set.delete(entry);
      if (set.size === 0) this.#byExtent.delete(entry.extent);
    }
  }

  // ── Lists ──────────────────────────────────────────────────────────────────────────────────

  #find(ref: BlobRef): Entry | null {
    const entry = this.#byAddress.get(ref.extent.offset + ref.offset);
    return entry !== undefined &&
      !entry.dropped &&
      entry.length === ref.length &&
      entry.checksum === ref.checksum
      ? entry
      : null;
  }

  #unlist(entry: Entry): void {
    if (entry.segment === "probation") this.#probation.remove(entry);
    else if (entry.segment === "protected") this.#protected.remove(entry);
  }

  /** A use: probation → protected, protected → front. Pinned entries are not listed. */
  #touch(entry: Entry): void {
    if (entry.segment === "pinned") return;
    this.#unlist(entry);
    entry.segment = "protected";
    this.#protected.pushFront(entry);
    const limit = PROTECTED_SHARE * (this.#protected.bytes + this.#probation.bytes);
    while (this.#protected.bytes > limit && this.#protected.tail !== entry) {
      const demoted = this.#protected.tail;
      if (demoted === null) break;
      this.#protected.remove(demoted);
      demoted.segment = "probation";
      this.#probation.pushFront(demoted);
    }
  }

  #live(): void {
    if (this.#closed) throw new StoreClosedError();
    if (this.#memory === null) throw new TypeError("Cache has not been attached");
  }
}
