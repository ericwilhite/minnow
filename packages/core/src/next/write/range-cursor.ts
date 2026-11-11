/** Lazy, charged (key,rowId) merge under one captured snapshot bound. */
import {
  IndexCorruptionError,
  type IndexCursor,
  type KeyRange,
  type MaybePromise,
} from "../access/key-index.js";
import { taskClock, yieldTask } from "../exec/pipeline.js";
import type { RunRef } from "../format/manifest.js";
import type { Grant } from "../memory/governor.js";
import { MinimumBudget } from "../memory/working-set.js";
import type { PlannedRange } from "./range-plan.js";
import type { ByteBudget } from "../access/space.js";
import { MAX_BLOB_BYTES } from "../access/space.js";
import { compareBytes } from "../vector/varlen.js";
import { rangeSide } from "./range-bound.js";
import { need } from "./bytes.js";
import type { KeyDelta } from "./key-hash.js";
import { DeltaRangeInput, RunRangeInput, rangeRunBytes, type RangeInput } from "./range-input.js";
import type { ViewIndex } from "./view.js";

const EMPTY = new Uint8Array(0);
const CLOSED = new Error("Index cursor closed");
export class StreamingRangeCursor implements IndexCursor {
  readonly #abort = new AbortController();
  readonly #unwatch: () => void;
  readonly #direction: number;
  #closed = false;
  #busy = false;
  #stopped(): boolean {
    return this.#closed;
  }
  #grant: ByteBudget | null = null;
  #ownedGrant: Grant | null = null;
  #account: MinimumBudget | null = null;
  #unlink: (() => void) | null = null;
  #inputs: RangeInput[] = [];
  #heap: RangeInput[] = [];
  #size = 0;
  #pair = EMPTY;
  #previous = EMPTY;
  #previousLength = -1;
  #previousRow = -1;
  constructor(
    readonly index: ViewIndex,
    readonly range: KeyRange,
    direction: "asc" | "desc",
    readonly admitted?: { plan: PlannedRange; budget: Grant; signal: AbortSignal },
  ) {
    this.#direction = direction === "asc" ? 1 : -1;
    this.#unwatch = index.table.snapshot.watchCursor(this);
    if (admitted !== undefined) {
      const cancel = () => {
        this.#abort.abort(admitted.signal.reason);
        this.close();
      };
      admitted.signal.addEventListener("abort", cancel, { once: true });
      this.#unlink = () => admitted.signal.removeEventListener("abort", cancel);
      if (admitted.signal.aborted) cancel();
    }
  }

  /** Enumeration is synchronous: capture paid inputs before the first metadata read can publish. */
  #layers(
    visit: (delta: KeyDelta | null, run: RunRef | null, limit: number, pure: boolean) => void,
  ): void {
    const id = this.index.def.id,
      table = this.index.table;
    const primary = this.index.def.role === "primary";
    const stored = (run: RunRef, limit: number, pure = false) => {
      if (rangeSide(this.range, run.maxKey) < 0 || rangeSide(this.range, run.minKey) > 0) return;
      visit(null, run, limit, pure);
    };
    for (const memtable of table.tableView.memtables) {
      const published = memtable.published;
      if (published !== null && memtable.deltasDropped) {
        const run = published.runs.get(id);
        if (run !== undefined) stored(run, Infinity);
      } else {
        const delta = memtable.deltas.get(id);
        if (delta !== undefined && delta.entryCount > 0) visit(delta, null, Infinity, false);
      }
      for (const run of memtable.staged?.runs.get(id) ?? [])
        stored(run, memtable.visibleRowIdEnd(table.snapshot.bound), primary);
    }
    // Every stored live row has a primary-key put. Equality leaves no capacity for dead puts,
    // tombstones or duplicate entries. Newer deletions still live in competing inputs, so the
    // disjointness proof below remains mandatory. Secondary indexes can omit nulls: no witness.
    const runs = table.tableView.runs.get(id) ?? [];
    const pure =
      primary &&
      runs.reduce((n, run) => n + run.entryCount, 0) === table.tableView.groups.liveRowCount;
    for (const run of runs) stored(run, Infinity, pure);
  }
  #sizes(): { resident: number; minimum: number; keyBytes: number } {
    if (this.admitted !== undefined) return this.admitted.plan.sizes;
    let resident = 1024,
      io = 0,
      keyBytes = 2048;
    this.#layers((delta, run) => {
      resident += 512; // source, two reference-array slots and heap head
      if (delta !== null) keyBytes = Math.max(keyBytes, delta.maxKeyBytes);
      if (run !== null) {
        resident += rangeRunBytes(run, this.#direction);
        // Fixed credit for one verified directory blob and one one-pass block. Parsed
        // navigation belongs to the governed cache, not this non-spillable admission.
        io = MAX_BLOB_BYTES;
      }
    });
    resident += keyBytes * 2;
    return {
      resident,
      minimum: resident + (io === 0 ? 0 : io * 2 + 128 * 1024),
      keyBytes,
    };
  }
  *#open(): Generator<Promise<unknown>, void, unknown> {
    const shared = this.index.table.snapshot.shared;
    for (;;) {
      const sizes = this.#sizes(),
        options = { label: "write index range", minimumBytes: sizes.minimum, spillable: false };
      let grant: ByteBudget;
      if (this.admitted !== undefined) {
        this.#account = new MinimumBudget(this.admitted.budget, sizes.minimum);
        grant = this.#account;
      } else {
        let owned = shared.governor.tryAdmit(options);
        owned ??= (yield shared.governor.admit({
          ...options,
          signal: this.#abort.signal,
        })) as Grant;
        this.#ownedGrant = owned;
        grant = owned;
      }
      this.#grant = grant;
      if (this.#stopped()) return;
      // Staging may have published during admission: recompute before allocating anything.
      const current = this.#sizes();
      if (this.#ownedGrant !== null && current.minimum > this.#ownedGrant.held) {
        this.#ownedGrant.close();
        this.#ownedGrant = null;
        this.#grant = null;
        continue;
      }
      if (!grant.allocate(current.resident))
        throw new TypeError("Admitted cursor scratch did not fit");
      this.#pair = new Uint8Array(current.keyBytes);
      this.#previous = new Uint8Array(current.keyBytes);
      this.#layers((delta, run, limit, pure) => {
        this.#inputs.push(
          delta !== null
            ? new DeltaRangeInput(
                delta,
                this.index.table.snapshot.bound,
                this.range,
                this.#direction,
                this.#abort.signal,
              )
            : new RunRangeInput(
                shared.space,
                grant,
                need(run),
                this.range,
                this.#direction,
                limit,
                this.#abort.signal,
                pure,
              ),
        );
      });
      this.#heap = new Array<RangeInput>(this.#inputs.length);
      for (const input of this.#inputs) {
        if (this.#stopped()) return;
        if (taskClock.due()) yield yieldTask();
        if (this.#stopped()) return;
        if (input instanceof RunRangeInput) {
          this.#push(input);
          continue;
        }
        let ready = input.advance();
        if (ready instanceof Promise) ready = (yield ready) as boolean;
        if (this.#stopped()) return;
        if (ready) this.#push(input);
      }
      return;
    }
  }
  #less(a: RangeInput, b: RangeInput): boolean {
    return (
      (compareBytes(a.bytes(), a.keyStart(), a.keyEnd(), b.bytes(), b.keyStart(), b.keyEnd()) ||
        a.rowId - b.rowId) *
        this.#direction <
      0
    );
  }

  #push(input: RangeInput): void {
    let k = this.#size++;
    while (k > 0) {
      const parent = (k - 1) >>> 1,
        above = need(this.#heap[parent]);
      if (!this.#less(input, above)) break;
      this.#heap[k] = above;
      k = parent;
    }
    this.#heap[k] = input;
  }
  #pop(): RangeInput {
    const head = need(this.#heap[0]),
      last = need(this.#heap[--this.#size]);
    let k = 0;
    while (k * 2 + 1 < this.#size) {
      let child = k * 2 + 1;
      if (
        child + 1 < this.#size &&
        this.#less(need(this.#heap[child + 1]), need(this.#heap[child]))
      )
        child++;
      if (!this.#less(need(this.#heap[child]), last)) break;
      this.#heap[k] = need(this.#heap[child]);
      k = child;
    }
    if (this.#size > 0) this.#heap[k] = last;
    return head;
  }
  next(out: Float64Array): MaybePromise<number> {
    return this.#consume(out.length, out);
  }
  skip(count: number): MaybePromise<number> {
    if (!Number.isSafeInteger(count) || count < 0)
      throw new RangeError("Index cursor skip count must be a nonnegative safe integer");
    return this.#consume(count, null);
  }
  #consume(limit: number, out: Float64Array | null): MaybePromise<number> {
    this.index.table.snapshot.check();
    this.admitted?.signal.throwIfAborted();
    if (this.#closed || limit === 0) return 0;
    if (this.#busy) throw new TypeError("Only one outstanding index cursor call is allowed");
    this.#busy = true;
    const work = this.#fill(limit, out);
    const resume = (value?: unknown): MaybePromise<number> => {
      try {
        const result = work.next(value);
        if (!result.done) return result.value.then(resume, fail);
        this.admitted?.signal.throwIfAborted();
        this.#busy = false;
        if (this.#stopped()) this.#dispose();
        return result.value;
      } catch (error) {
        return fail(error);
      }
    };
    const fail = (error: unknown): number => {
      this.#busy = false;
      // End the suspended frame as well: a rejected call must not retain the caller's out.
      try {
        work.return(0);
      } finally {
        this.close();
      }
      if (this.#abort.signal.aborted && error === CLOSED) return 0;
      throw error;
    };
    return resume();
  }
  /** A whole run can be drained only before every other head, with both bounds in range.
   * Pending runs expose conservative fence keys; delta heads already enforce the pinned bound.
   * Recheck each block after any yield: no borrowed key or mutable delta range is retained.
   */
  #disjoint(head: RunRangeInput): boolean {
    if (
      rangeSide(this.range, head.ref.minKey) !== 0 ||
      rangeSide(this.range, head.ref.maxKey) !== 0
    )
      return false;
    const end = this.#direction > 0 ? head.ref.maxKey : head.ref.minKey;
    for (let k = 1; k < this.#size; k++) {
      const other = need(this.#heap[k]);
      if (
        compareBytes(end, 0, end.length, other.bytes(), other.keyStart(), other.keyEnd()) *
          this.#direction >=
        0
      )
        return false;
    }
    return true;
  }
  /** Skip-only work is outside next's hot visibility loop. -1 means ordinary resolution. */
  *#skipAhead(limit: number): Generator<Promise<unknown>, number, unknown> {
    const head = need(this.#heap[0]);
    if (!(head instanceof RunRangeInput) || !head.pure || !this.#disjoint(head)) return -1;
    if (head.pending) {
      if (head.limit !== Infinity) return -1;
      if (head.ref.entryCount <= limit) {
        this.#pop();
        this.#previousLength = -1;
        head.close();
        return head.ref.entryCount;
      }
      // Non-null int32 equality keys are five bytes (keys/key-encoding.ts). Each entry has
      // a u16 offset, one key-length byte, five key bytes, six row-id bytes and one flag.
      // A verified directory's block length is therefore 2 + 15*n, including the u16 count.
      const columns = this.index.def.columnIds;
      const column =
        columns.length === 1
          ? this.index.table.def.columns.find((c) => c.id === columns[0])
          : undefined;
      if (column?.type.kind !== "int32" || column.nullable) return -1;
      this.#pop();
      const opened = head.open();
      if (opened instanceof Promise) yield opened;
      if (this.#stopped()) return 0;
      let skipped = head.skipPages(limit, 15);
      if (skipped instanceof Promise) skipped = (yield skipped) as number;
      if (this.#stopped()) return skipped;
      this.#previousLength = -1;
      let ready = head.advance();
      if (ready instanceof Promise) ready = (yield ready) as boolean;
      if (this.#stopped()) return skipped;
      head.pending = false;
      if (ready) this.#push(head);
      return skipped;
    }
    const skipped = head.skipBlock(limit);
    if (skipped > 0) {
      const key = head.bytes(),
        start = head.keyStart(),
        length = head.keyEnd() - start;
      if (this.index.def.unique)
        for (let i = 0; i < length; i++) this.#previous[i] = key[start + i] ?? 0;
      this.#previousLength = length;
      this.#previousRow = head.rowId;
    }
    this.#pop();
    let ready = head.advance();
    if (ready instanceof Promise) ready = (yield ready) as boolean;
    if (!this.#stopped() && ready) this.#push(head);
    return skipped;
  }
  *#fill(limit: number, out: Float64Array | null): Generator<Promise<unknown>, number, unknown> {
    if (this.#grant === null) yield* this.#open();
    let count = 0;
    while (!this.#stopped() && this.#size > 0 && count < limit) {
      this.index.table.snapshot.check();
      if (taskClock.due()) yield yieldTask();
      if (this.#stopped()) break;
      if (out === null) {
        const skipped = yield* this.#skipAhead(limit - count);
        if (skipped >= 0) {
          count += skipped;
          continue;
        }
      }
      const head = need(this.#heap[0]);
      if (head instanceof RunRangeInput && head.pending) {
        this.#pop();
        const opened = head.open();
        if (opened instanceof Promise) yield opened;
        if (this.#stopped()) return count;
        let ready = head.advance();
        if (ready instanceof Promise) ready = (yield ready) as boolean;
        if (this.#stopped()) return count;
        head.pending = false;
        if (ready) this.#push(head);
        continue;
      }
      const first = need(this.#heap[0]),
        key = first.bytes(),
        start = first.keyStart(),
        length = first.keyEnd() - start,
        row = first.rowId;
      for (let i = 0; i < length; i++) this.#pair[i] = key[start + i] ?? 0;
      let put = false,
        dead = false;
      for (;;) {
        const input = this.#pop();
        dead ||= input.tombstone;
        put ||= !input.tombstone;
        let ready = input.advance();
        if (ready instanceof Promise) ready = (yield ready) as boolean;
        if (this.#stopped()) return count;
        if (ready) this.#push(input);
        const next = this.#size > 0 ? need(this.#heap[0]) : null;
        if (next?.rowId !== row) break;
        if (compareBytes(next.bytes(), next.keyStart(), next.keyEnd(), this.#pair, 0, length) !== 0)
          break;
        if (taskClock.due()) yield yieldTask();
        if (this.#stopped()) return count;
      }
      if (!put || dead) continue;
      if (
        this.index.def.unique &&
        this.#previousLength === length &&
        compareBytes(this.#pair, 0, length, this.#previous, 0, length) === 0 &&
        this.#previousRow !== row
      )
        throw new IndexCorruptionError("A unique index has two live rows for one key");
      if (this.index.def.unique)
        for (let i = 0; i < length; i++) this.#previous[i] = this.#pair[i] ?? 0;
      this.#previousLength = length;
      this.#previousRow = row;
      if (out !== null) out[count] = this.index.table.locator(row);
      count++;
    }
    if (!this.#stopped() && this.#size === 0) this.close();
    return count;
  }
  close(): void {
    if (!this.#closed) {
      this.#closed = true;
      this.#abort.abort(CLOSED);
    }
    if (!this.#busy) this.#dispose();
  }
  #dispose(): void {
    for (const input of this.#inputs) input.close();
    this.#inputs = [];
    this.#heap = [];
    this.#size = 0;
    this.#pair = EMPTY;
    this.#previous = EMPTY;
    if (this.#account !== null) {
      this.#account.free(this.#account.allocated);
      this.#account = null;
    }
    this.#ownedGrant?.close();
    this.#ownedGrant = null;
    this.#unlink?.();
    this.#unlink = null;
    this.#grant = null;
    this.#unwatch();
  }
}
