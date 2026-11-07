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
  #grant: Grant | null = null;
  #inputs: RangeInput[] = [];
  #heap: RangeInput[] = [];
  #size = 0;
  #pair = EMPTY;
  #previous = EMPTY;
  #metadata = EMPTY;
  #previousLength = -1;
  #previousRow = -1;
  constructor(
    readonly index: ViewIndex,
    readonly range: KeyRange,
    direction: "asc" | "desc",
  ) {
    this.#direction = direction === "asc" ? 1 : -1;
    this.#unwatch = index.table.snapshot.watchCursor(this);
  }

  /** Enumeration is synchronous: capture paid inputs before the first metadata read can publish. */
  #layers(visit: (delta: KeyDelta | null, run: RunRef | null, limit: number) => void): void {
    const id = this.index.def.id,
      table = this.index.table;
    const stored = (run: RunRef, limit: number) => {
      if (rangeSide(this.range, run.maxKey) < 0 || rangeSide(this.range, run.minKey) > 0) return;
      visit(null, run, limit);
    };
    for (const memtable of table.tableView.memtables) {
      const published = memtable.published;
      if (published !== null && memtable.deltasDropped) {
        const run = published.runs.get(id);
        if (run !== undefined) stored(run, Infinity);
      } else {
        const delta = memtable.deltas.get(id);
        if (delta !== undefined && delta.entryCount > 0) visit(delta, null, Infinity);
      }
      for (const run of memtable.staged?.runs.get(id) ?? [])
        stored(run, memtable.visibleRowIdEnd(table.snapshot.bound));
    }
    for (const run of table.tableView.runs.get(id) ?? []) stored(run, Infinity);
  }
  #sizes(): { resident: number; minimum: number; keyBytes: number; metadataBytes: number } {
    let resident = 1024,
      io = 0,
      metadataBytes = 0,
      keyBytes = 2048;
    this.#layers((delta, run) => {
      resident += 512; // source, two reference-array slots and heap head
      if (delta !== null) keyBytes = Math.max(keyBytes, delta.maxKeyBytes);
      if (run !== null) {
        resident += rangeRunBytes(run, this.#direction);
        const bounded = (this.#direction > 0 ? this.range.lower : this.range.upper) !== null;
        metadataBytes = Math.max(
          metadataBytes,
          bounded ? Math.max(run.fences.length, run.entries.length) : 0,
          this.#direction < 0 ? run.bloom.length : 0,
        );
        io = Math.max(
          io,
          run.entries.length,
          bounded ? run.fences.length : 0,
          this.#direction < 0 ? run.bloom.length : 0,
          4096,
        );
      }
    });
    resident += keyBytes * 2 + metadataBytes;
    return {
      resident,
      minimum: resident + (io === 0 ? 0 : io * 2 + 128 * 1024),
      keyBytes,
      metadataBytes,
    };
  }
  *#open(): Generator<Promise<unknown>, void, unknown> {
    const shared = this.index.table.snapshot.shared;
    for (;;) {
      const sizes = this.#sizes(),
        options = { label: "write index range", minimumBytes: sizes.minimum, spillable: false };
      let grant = shared.governor.tryAdmit(options);
      grant ??= (yield shared.governor.admit({ ...options, signal: this.#abort.signal })) as Grant;
      this.#grant = grant;
      if (this.#stopped()) return;
      // Staging may have published during admission: recompute before allocating anything.
      const current = this.#sizes();
      if (current.minimum > grant.held) {
        grant.close();
        this.#grant = null;
        continue;
      }
      if (!grant.allocate(current.resident))
        throw new TypeError("Admitted cursor scratch did not fit");
      this.#pair = new Uint8Array(current.keyBytes);
      this.#previous = new Uint8Array(current.keyBytes);
      this.#metadata = new Uint8Array(current.metadataBytes);
      this.#layers((delta, run, limit) => {
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
                this.#metadata,
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
    this.index.table.snapshot.check();
    if (this.#closed || out.length === 0) return 0;
    if (this.#busy) throw new TypeError("Only one outstanding index cursor call is allowed");
    this.#busy = true;
    const work = this.#fill(out);
    const resume = (value?: unknown): MaybePromise<number> => {
      try {
        const result = work.next(value);
        if (!result.done) return result.value.then(resume, fail);
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
  *#fill(out: Float64Array): Generator<Promise<unknown>, number, unknown> {
    if (this.#grant === null) yield* this.#open();
    let count = 0;
    while (!this.#stopped() && this.#size > 0 && count < out.length) {
      this.index.table.snapshot.check();
      if (taskClock.due()) yield yieldTask();
      if (this.#stopped()) break;
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
      out[count++] = this.index.table.locator(row);
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
    this.#metadata = EMPTY;
    this.#grant?.close();
    this.#grant = null;
    this.#unwatch();
  }
}
