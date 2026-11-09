/**
 * Navigation indexes over the EXISTING verified directory bytes. Arrays belong to the extent
 * cache, not a cursor: attach reserves their full cache charge BEFORE allocating them. A cache
 * hit is borrowed synchronously; cold fills pin one encoded blob while parsing task slices.
 * Under cache pressure, a scalar scan uses the same fixed I/O credit without a private index.
 */
import type { MaybePromise } from "../access/key-index.js";
import type { ByteBudget, SpaceReader } from "../access/space.js";
import type { BlobRef } from "../format/manifest.js";
import { blobKey } from "../format/blob-cache.js";
import { abortable, taskClock, yieldTask } from "../exec/pipeline.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { ByteReader, need } from "./bytes.js";

export type DirectoryKind = "entries" | "fences";
const EMPTY = new Uint32Array(0);
/** Cache-owned descriptor; no cursor stores it or an array/view derived from it. */
export class DirectoryNavigation {
  positions = EMPTY;
  next = 0;
  offset: number;
  ready = false;
  corrupt = false;
  constructor(
    readonly kind: DirectoryKind,
    readonly source: Uint8Array,
    readonly count: number,
    readonly header: number,
  ) {
    this.offset = header;
  }
  get charge(): number {
    return 256 + (this.count + 1) * 4;
  }
}
interface Filling {
  readonly signal: AbortSignal;
  readonly done: Promise<void>;
}
const filling = new WeakMap<SpaceReader, Map<string, Filling>>();

export function runNavigation<T>(work: Generator<Promise<unknown>, T, unknown>): MaybePromise<T> {
  const advance = (value?: unknown): MaybePromise<T> => {
    const next = work.next(value);
    return next.done ? next.value : next.value.then(advance, fail);
  };
  const fail = (error: unknown): MaybePromise<T> => {
    const next = work.throw(error);
    return next.done ? next.value : next.value.then(advance, fail);
  };
  return advance();
}
function header(bytes: Uint8Array, kind: DirectoryKind): { count: number; start: number } {
  const r = new ByteReader(bytes),
    count = r.varuint();
  if (count > Math.floor((bytes.length - r.offset) / (kind === "entries" ? 6 : 1)))
    throw new StoreCorruptionError("Run directory count exceeds its bytes");
  return { count, start: r.offset };
}
/** Skip ONE checked record; shared by the cache fill and its scalar pressure fallback. */
export function skipDirectoryRecord(r: ByteReader, kind: DirectoryKind): void {
  if (kind === "entries") {
    r.varuint();
    r.varuint();
    r.u32();
  } else {
    const bytes = r.varuint();
    if (bytes > 2048 || r.offset + bytes > r.bytes.length)
      throw new StoreCorruptionError("Truncated or oversized run fence");
    r.offset += bytes;
  }
}
function cached(
  space: SpaceReader,
  ref: BlobRef,
  kind: DirectoryKind,
  bytes: Uint8Array,
): DirectoryNavigation | null {
  const index = space.parsed?.(ref);
  return index instanceof DirectoryNavigation && index.kind === kind && index.source === bytes
    ? index
    : null;
}
function* fill(
  index: DirectoryNavigation,
  signal: AbortSignal,
): Generator<Promise<unknown>, void, unknown> {
  if (index.corrupt) throw new StoreCorruptionError("Run directory is malformed");
  // The descriptor was attached (and its full array capacity charged) before this allocation.
  if (index.positions === EMPTY) index.positions = new Uint32Array(index.count + 1);
  const r = new ByteReader(index.source, index.offset);
  try {
    while (index.next < index.count) {
      if ((index.next & 127) === 0) {
        signal.throwIfAborted();
        if (taskClock.due()) {
          yield yieldTask();
          signal.throwIfAborted();
        }
      }
      index.positions[index.next] = r.offset;
      skipDirectoryRecord(r, index.kind);
      index.offset = r.offset;
      index.next++;
    }
    index.positions[index.count] = r.offset;
    r.end();
    index.ready = true;
  } catch (error) {
    // Cancellation leaves a valid, resumable prefix. Structural corruption is permanent.
    if (error instanceof StoreCorruptionError) index.corrupt = true;
    throw error;
  }
}

/**
 * `use` never suspends with a ready index. With null positions it is a cold/pressure scalar
 * scan and may yield: its one encoded lease stays charged until the generator unwinds.
 * No index is copied, pinned by a cursor, or charged to cursor admission.
 */
export function withDirectory<T>(
  space: SpaceReader,
  ref: BlobRef,
  kind: DirectoryKind,
  budget: ByteBudget,
  signal: AbortSignal,
  use: (
    bytes: Uint8Array,
    positions: Uint32Array | null,
    count: number,
    start: number,
  ) => Generator<Promise<unknown>, T, unknown>,
): MaybePromise<T> {
  signal.throwIfAborted();
  const bytes = space.peek(ref);
  const index = bytes === null ? null : cached(space, ref, kind, bytes);
  if (bytes !== null && index?.ready === true)
    return runNavigation(use(bytes, index.positions, index.count, index.header));
  const key = `${kind}:${blobKey(ref)}`;
  let active = filling.get(space);
  if (active === undefined) {
    active = new Map();
    filling.set(space, active);
  }
  const pending = active.get(key);
  if (pending !== undefined)
    return abortable(pending.done, signal)
      .catch((error: unknown) => {
        signal.throwIfAborted();
        if (!pending.signal.aborted) throw error;
      })
      .then(() => withDirectory(space, ref, kind, budget, signal, use));
  const pendingByKey = active;
  let resolveFill!: () => void, rejectFill!: (error: unknown) => void;
  const done = new Promise<void>((resolve, reject) => {
    resolveFill = resolve;
    rejectFill = reject;
  });
  let announced = false;
  const finish = (error?: unknown): void => {
    if (announced) return;
    announced = true;
    pendingByKey.delete(key);
    if (error === undefined) resolveFill();
    else rejectFill(error);
  };
  pendingByKey.set(key, { signal, done });
  void done.catch(() => undefined);
  const work = function* (): Generator<Promise<unknown>, T, unknown> {
    // A lease also protects its cache descriptor from eviction while a cold parse yields.
    let lease: import("../access/space.js").ReadLease | null = null;
    try {
      lease = (yield space.read([ref], {
        budget,
        signal,
      })) as import("../access/space.js").ReadLease;
      signal.throwIfAborted();
      const raw = need(lease.bytes[0]);
      let navigation = cached(space, ref, kind, raw);
      if (navigation === null) {
        const { count, start } = header(raw, kind);
        const shell = new DirectoryNavigation(kind, raw, count, start);
        if (space.attach?.(ref, shell, shell.charge, raw) === true) navigation = shell;
      }
      if (navigation !== null) {
        yield* fill(navigation, signal);
        signal.throwIfAborted();
        finish();
        return yield* use(raw, navigation.positions, navigation.count, navigation.header);
      }
      // Optional cache admission can refuse under pressure. No uncharged/private index is made.
      const { count, start } = header(raw, kind);
      finish();
      return yield* use(raw, null, count, start);
    } catch (error) {
      finish(error);
      throw error;
    } finally {
      lease?.release();
    }
  };
  return runNavigation(work());
}

/** ASC's forward byte offset already IS a bounded navigator, so it needs no parsed array. */
export function withRawDirectory<T>(
  space: SpaceReader,
  ref: BlobRef,
  budget: ByteBudget,
  signal: AbortSignal,
  use: (bytes: Uint8Array) => T,
): MaybePromise<T> {
  signal.throwIfAborted();
  const bytes = space.peek(ref);
  if (bytes !== null) return use(bytes);
  return space.read([ref], { budget, signal }).then((lease) => {
    try {
      signal.throwIfAborted();
      return use(need(lease.bytes[0]));
    } finally {
      lease.release();
    }
  });
}
