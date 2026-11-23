/**
 * A statement-owned memo of opened published readers, for point reads that run one row at a time
 * (exec/point-fetch.ts: the ON CONFLICT fetch of each conflicting row).
 *
 * A memory segment's read (write/memory-read.ts `MemoryReads`) opens the published group root and
 * one chunk reader per column it reads, and closes them when the read ends. A caller that reads
 * single rows of the same published chunk again and again passes one `ReaderMemo` in its
 * `MemoryReadContext.readers`: the read then keeps what it opened in the memo (one entry, for the
 * segment and published root it last read) and the next read of the same root reuses it.
 *
 * - **One entry.** Installing an entry closes the previous one.
 * - **Owned by the caller's statement.** Everything an entry holds is charged to `budget` before it
 *   is allocated, and given back when the entry closes: on `invalidate`, on `close` (statement end,
 *   abort, error), and when its snapshot is released.
 * - **Never stale.** The read that installed an entry checks, before every reuse, that it names the
 *   same segment and the same published root (by full blob identity); otherwise it closes the entry
 *   and opens a new one. A publication, re-staging, merge or compaction that changes the root a
 *   memory segment reads therefore never reaches readers opened on the old root.
 * - **Optional.** A memo that cannot be funded is declined: the read closes the entry and the caller
 *   retries without the memo (`MemoDeclinedError`).
 * - **One read at a time.** Point reads that share a memo must not overlap.
 */
import type { ByteBudget } from "./space.js";

/** What a read keeps in a memo. Owned by the memo once installed. */
export interface ReaderMemoEntry {
  /** The reads (segment) that installed it: only they can reuse it. */
  readonly owner: object;
  /** Gives back everything the entry holds. Idempotent. */
  close(): void;
}

/** The memo could not be funded: the caller retries the read without it. */
export class MemoDeclinedError extends Error {
  readonly code = "MINNOW_READER_MEMO_DECLINED";
  constructor(readonly bytes: number) {
    super(`A reader memo needs ${String(bytes)} bytes its budget refused`);
    this.name = "MemoDeclinedError";
  }
}

export class ReaderMemo {
  #entry: ReaderMemoEntry | null = null;
  #closed = false;
  /** Entries installed (each a group root and its readers opened). */
  opens = 0;
  /** Reads that reused the installed entry. */
  reuses = 0;
  /** Entries closed because their root, segment or snapshot changed, or on decline. */
  invalidations = 0;
  #held = 0;
  /** Charged for every byte an entry holds, before it is allocated (the owner's budget, counted). */
  readonly budget: ByteBudget;
  constructor(
    owner: ByteBudget,
    /** The statement's signal; it must cover the signal of every read that uses the memo. */
    readonly signal: AbortSignal,
  ) {
    this.budget = {
      allocate: (bytes) => {
        if (!owner.allocate(bytes)) return false;
        this.#held += bytes;
        return true;
      },
      free: (bytes) => {
        this.#held -= bytes;
        owner.free(bytes);
      },
    };
  }

  /** Bytes the memo's entries hold now: zero once it is closed and its reads have drained. */
  get held(): number {
    return this.#held;
  }

  get closed(): boolean {
    return this.#closed;
  }

  /** The installed entry when `owner` installed it, else null. */
  entry(owner: object): ReaderMemoEntry | null {
    const entry = this.#entry;
    return entry !== null && entry.owner === owner ? entry : null;
  }

  /** Makes `entry` the memo's entry, closing the previous one. A closed memo closes it at once. */
  install(entry: ReaderMemoEntry): void {
    if (this.#closed) {
      entry.close();
      throw new TypeError("A closed reader memo cannot take an entry");
    }
    this.invalidate();
    this.#entry = entry;
    this.opens += 1;
  }

  /** Closes the installed entry, if any. The memo stays usable. */
  invalidate(): void {
    const entry = this.#entry;
    if (entry === null) return;
    this.#entry = null;
    this.invalidations += 1;
    entry.close();
  }

  /** Closes the installed entry; later reads do not use the memo. Idempotent. */
  close(): void {
    this.#closed = true;
    const entry = this.#entry;
    this.#entry = null;
    entry?.close();
  }
}
