/**
 * The extent space's one cache of encoded, verified blobs (extents.ts "Reads and the cache").
 *
 * - **Entries** are exact `(address, length, checksum)` copies in their own ArrayBuffers, indexed
 *   by address and by extent offset (so freeing an extent drops its blobs in one step).
 * - **Charging.** An unpinned entry is charged to the governor's cache handle (its bytes plus
 *   `ENTRY_OVERHEAD`). A pinned entry (held by at least one lease) is charged to each lease's
 *   budget instead and is not evictable: the cache uncharges it on the first pin and charges it
 *   again when the last lease releases it, or drops it if the governor has no room. The two fixed
 *   slabs (read and write, 1 MiB each) are charged once at `attach`.
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
import { xxHash32 } from "../hash/xxhash32.js";
import { StoreClosedError, StoreCorruptionError, type StoreSession } from "./contract.js";
import {
  MAX_BLOB_BYTES,
  MAX_READ_BYTES,
  ReadDeniedError,
  type BlobRef,
  type CacheMemory,
  type EvictableCacheLike,
  type ReadLease,
  type ReadOptions,
} from "./extents.js";
import { MAX_SLAB, planSlabs } from "./slabs.js";

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
  segment: Segment;
  /** Dropped while pinned (its extent was freed): never admitted again. */
  dropped: boolean;
  /** Used again after its first read: admitted to protected when it unpins. */
  hot: boolean;
  /** When it was last used: listed after a release, or peeked (`performance.now()`). */
  usedAt: number;
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
    this.bytes += entry.length;
    this.count += 1;
  }

  remove(entry: Entry): void {
    if (entry.prev !== null) entry.prev.next = entry.next;
    else this.head = entry.next;
    if (entry.next !== null) entry.next.prev = entry.prev;
    else this.tail = entry.prev;
    entry.prev = null;
    entry.next = null;
    this.bytes -= entry.length;
    this.count -= 1;
  }
}

export interface CacheStats {
  readonly residentBytes: number;
  readonly pinnedBytes: number;
  readonly hits: number;
  readonly misses: number;
  readonly platformReads: number;
  readonly bytesRead: number;
}

const charge = (entry: Entry) => entry.length + ENTRY_OVERHEAD;

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
  readonly #slab = new Uint8Array(MAX_SLAB);
  #memory: CacheMemory | null = null;
  #slabBusy: Promise<void> = Promise.resolve();
  #pinnedBytes = 0;
  #hits = 0;
  #misses = 0;
  #platformReads = 0;
  #bytesRead = 0;
  #closed = false;

  constructor(session: StoreSession) {
    this.#session = session;
  }

  /** Connects the governor's handle and charges the fixed slabs (`RangeError` if it cannot). */
  attach(memory: CacheMemory): void {
    if (!memory.charge(FIXED_CACHE_BYTES))
      throw new RangeError("The memory cap cannot hold the extent space's 2 MiB of fixed slabs");
    this.#memory = memory;
  }

  get stats(): CacheStats {
    return {
      residentBytes: this.#probation.bytes + this.#protected.bytes + this.#pinnedBytes,
      pinnedBytes: this.#pinnedBytes,
      hits: this.#hits,
      misses: this.#misses,
      platformReads: this.#platformReads,
      bytesRead: this.#bytesRead,
    };
  }

  // ── Governor side ──────────────────────────────────────────────────────────────────────────

  get evictableBytes(): number {
    return (
      this.#probation.bytes +
      this.#protected.bytes +
      ENTRY_OVERHEAD * (this.#probation.count + this.#protected.count)
    );
  }

  /** When the entry `evict` takes next was last used (the governor's cross-cache order). */
  get oldestUnusedAt(): number | undefined {
    return (this.#probation.tail ?? this.#protected.tail)?.usedAt;
  }

  evict(bytes: number): number {
    let freed = 0;
    while (freed < bytes) {
      const victim = this.#probation.tail ?? this.#protected.tail;
      if (victim === null) break;
      freed += this.#evictEntry(victim);
    }
    return freed;
  }

  // ── Reads ──────────────────────────────────────────────────────────────────────────────────

  peek(ref: BlobRef): Uint8Array | null {
    const entry = this.#find(ref);
    if (entry === null) return null;
    this.#hits += 1;
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
        entry.hot = true;
        if (entry.pins === 0) {
          uncharge += charge(entry);
          this.#unlist(entry);
          entry.segment = "pinned";
          this.#pinnedBytes += entry.length;
        }
        entry.pins += 1;
        held.push(entry);
        resolved.set(key, entry.bytes);
      }
      // Hits are pinned before the cache's charge goes back: a nested eviction cannot take them.
      if (uncharge > 0) this.#memory?.release(uncharge);
      if (misses.length > 0) await this.#readMisses(misses, held, resolved, signal);
    } catch (error) {
      this.#unpinAll(held);
      budget.free(total);
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
        this.#unpinAll(held);
        budget.free(total);
      },
    };
  }

  async #readMisses(
    misses: ReadonlyArray<{ readonly key: string; readonly ref: BlobRef }>,
    held: Entry[],
    resolved: Map<string, Uint8Array>,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    const ranges = misses.map(({ ref }) => ({
      offset: ref.extent.offset + ref.offset,
      length: ref.length,
    }));
    const slabs = planSlabs(ranges);
    const previous = this.#slabBusy;
    let done!: () => void;
    this.#slabBusy = new Promise<void>((resolve) => {
      done = resolve;
    });
    await previous;
    try {
      for (const slab of slabs) {
        signal?.throwIfAborted();
        this.#live();
        const target = this.#slab.subarray(0, slab.length);
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
          const entry = this.#insertPinned(ref, address, bytes);
          held.push(entry);
          resolved.set(key, entry.bytes);
        }
      }
    } finally {
      done();
    }
  }

  /** A verified miss enters the cache pinned (another read may have inserted it meanwhile). */
  #insertPinned(ref: BlobRef, address: number, bytes: Uint8Array): Entry {
    const existing = this.#find(ref);
    if (existing !== null) {
      if (existing.pins === 0) {
        this.#unlist(existing);
        existing.segment = "pinned";
        this.#pinnedBytes += existing.length;
        this.#memory?.release(charge(existing));
      }
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
      segment: "pinned",
      dropped: false,
      hot: false,
      usedAt: 0,
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
      segment: "pinned",
      dropped: true,
      hot: false,
      usedAt: 0,
      prev: null,
      next: null,
    };
  }

  #unpinAll(held: readonly Entry[]): void {
    for (const entry of held) {
      entry.pins -= 1;
      if (entry.pins > 0) continue;
      this.#pinnedBytes -= entry.length;
      if (entry.dropped || this.#closed) {
        entry.segment = "probation";
        continue;
      }
      this.#admit(entry);
    }
  }

  /** Charges an unpinned entry to the cache, evicting probation for it, or drops it. */
  #admit(entry: Entry): void {
    const memory = this.#memory;
    const cost = charge(entry);
    let admitted = memory?.charge(cost) ?? false;
    if (!admitted && memory !== null && this.#probationCharge() >= cost) {
      // Recycle probation (a scan's own pages) to make room; never protected.
      let freed = 0;
      while (freed < cost && this.#probation.tail !== null)
        freed += this.#evictEntry(this.#probation.tail);
      admitted = freed >= cost && memory.charge(cost);
    }
    if (!admitted) {
      entry.segment = "probation";
      this.#forget(entry);
      return;
    }
    entry.segment = "probation";
    entry.usedAt = performance.now();
    this.#probation.pushFront(entry);
    if (entry.hot) this.#touch(entry);
  }

  #probationCharge(): number {
    return this.#probation.bytes + ENTRY_OVERHEAD * this.#probation.count;
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
    for (const entry of [...this.#byAddress.values()]) this.#drop(entry);
    this.#memory?.release(FIXED_CACHE_BYTES);
  }

  #drop(entry: Entry): void {
    if (entry.pins > 0) {
      entry.dropped = true;
      this.#forget(entry);
      return;
    }
    this.#evictEntry(entry);
  }

  /** Removes an unpinned, listed entry and returns its charge to the governor. */
  #evictEntry(entry: Entry): number {
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
  }
}
