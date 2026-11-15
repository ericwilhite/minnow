/** Root-owned immutable OLD/NEW images. Directory records and fragmented scalar payloads
 * outlive queue packets; only one projected, charged image lease may be live at a time. */
import type { CatalogView, TableDef } from "../access/catalog.js";
import type { MaybePromise } from "../access/key-index.js";
import {
  ROW_IMAGE_UNREAD,
  type RowBindingLease,
  type RowBindingProjection,
  type RowImageLease,
  type RowImageRef,
} from "../access/statement-command.js";
import { SpillCorruptionError } from "../spill/contract.js";
import { samePhysicalType } from "../types.js";
import type { ByteCharge } from "../vector/varlen.js";
import { dataBytes, type Batch, type Vector } from "../vector/vector.js";
import type { RegistryDirectory } from "./registry-directory.js";
import type { RegistryFragments } from "./registry-fragments.js";
import {
  loadRegistryScalar,
  registryScalarBytes,
  registryScalarStoreBytes,
  registryScalarWorkspaceBytes,
  storeRegistryScalar,
} from "./registry-scalar.js";

export const REGISTRY_IMAGE_BYTES = 2048;
const IMAGE = 1,
  SCALAR = 2;
export interface ImageSource {
  readonly batch: Batch;
  readonly row: number;
}
export interface RegistryImageScope {
  readonly rootId: number;
  readonly catalog: CatalogView;
  readonly directory: RegistryDirectory;
  readonly fragments: RegistryFragments;
  readonly charge: ByteCharge;
  readonly signal: AbortSignal;
  /** Root progress envelope, never independent governor admission. Invoked with no lease.
   * append sources are ordinary root scratch: its producer prepays capture scratch in the
   * same protected step, never holding a producer progress loan across this callback. */
  prepare(bytes: number, signal: AbortSignal): MaybePromise<void>;
  checkpoint(): MaybePromise<void>;
}
const viewOf = (bytes: Uint8Array) =>
  new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
function state(): never {
  throw new TypeError("Invalid or retired native row image");
}
function corrupt(): never {
  throw new SpillCorruptionError("Invalid native row image record");
}
function nextId(value: number): number {
  if (!Number.isSafeInteger(value + 1)) throw new RangeError("Native image ID space exhausted");
  return value + 1;
}

export class RegistryImages {
  readonly #stop = new AbortController();
  #signal: AbortSignal | null = null;
  #header: Uint8Array | null = null;
  #scalar: Uint8Array | null = null;
  #nextImage = 0;
  #nextScalar = 0;
  #pending: Promise<unknown> | null = null;
  #lease: Promise<void> | null = null;
  #closed = false;
  #closing: Promise<void> | null = null;
  #failure: { reason: unknown } | null = null;
  #scope: RegistryImageScope | null;
  private get scope(): RegistryImageScope {
    return this.#scope ?? state();
  }
  constructor(scope: RegistryImageScope) {
    this.#scope = scope;
    scope.signal.throwIfAborted();
    if (!Number.isSafeInteger(scope.rootId) || scope.rootId < 1) state();
    if (!scope.charge.allocate(REGISTRY_IMAGE_BYTES))
      throw new RangeError("Image metadata must be admitted");
    try {
      this.#signal = AbortSignal.any([scope.signal, this.#stop.signal]);
      this.#header = new Uint8Array(128);
      this.#scalar = new Uint8Array(128);
    } catch (error) {
      this.#header = this.#scalar = null;
      this.#signal = null;
      scope.charge.free(REGISTRY_IMAGE_BYTES);
      throw error;
    }
  }
  #check(signal?: AbortSignal): void {
    this.#signal?.throwIfAborted();
    signal?.throwIfAborted();
    if (this.#closed) state();
    if (this.#failure) throw this.#failure.reason;
  }
  #work<T>(run: () => Promise<T>): Promise<T> {
    this.#check();
    if (this.#pending || this.#lease) state();
    const work = run().catch((reason: unknown) => {
      this.#failure = { reason };
      throw reason;
    });
    const pending = work.finally(() => {
      if (this.#pending === pending) this.#pending = null;
    });
    this.#pending = pending;
    return pending;
  }
  async #get(kind: number, id: number, into: Uint8Array): Promise<void> {
    if (!Number.isSafeInteger(id) || id < 1 || !(await this.scope.directory.get(kind, id, into)))
      corrupt();
    this.#check();
  }
  async append(
    tableId: number,
    old: ImageSource | null,
    next: ImageSource | null,
  ): Promise<RowImageRef> {
    return this.#work(async () => {
      const table = this.scope.catalog.table(tableId),
        header = this.#header,
        scalar = this.#scalar;
      if (!table || !header || !scalar || (!old && !next)) state();
      let captureBytes = 0;
      for (const source of [old, next])
        if (source) {
          if (
            !Number.isInteger(source.row) ||
            source.row < 0 ||
            source.row >= source.batch.length ||
            source.batch.columns.length !== table.columns.length
          )
            state();
          for (const [position, column] of table.columns.entries()) {
            const vector = source.batch.columns[position];
            if (
              !vector ||
              vector.length < source.batch.length ||
              !samePhysicalType(vector.type, column.type)
            )
              state();
            captureBytes = Math.max(captureBytes, registryScalarStoreBytes(vector, source.row));
          }
        }
      if (captureBytes > 0) {
        await this.scope.prepare(captureBytes, this.#signal ?? state());
        this.#check();
      }
      const heads = [0, 0];
      for (const [side, source] of [old, next].entries()) {
        if (!source) continue;
        if (
          !Number.isInteger(source.row) ||
          source.row < 0 ||
          source.row >= source.batch.length ||
          source.batch.columns.length !== table.columns.length
        )
          state();
        for (let position = table.columns.length - 1; position >= 0; position--) {
          const column = table.columns[position],
            vector = source.batch.columns[position];
          if (
            !column ||
            !vector ||
            vector.length < source.batch.length ||
            !samePhysicalType(vector.type, column.type)
          )
            state();
          await this.scope.checkpoint();
          this.#check();
          const id = (this.#nextScalar = nextId(this.#nextScalar)),
            view = viewOf(scalar);
          scalar.fill(0);
          view.setFloat64(0, heads[side] ?? 0, true);
          view.setFloat64(8, column.id, true);
          await storeRegistryScalar(
            scalar,
            vector,
            source.row,
            this.scope.fragments,
            this.scope.charge,
          );
          this.#check();
          await this.scope.directory.put(SCALAR, id, scalar);
          this.#check();
          heads[side] = id;
        }
      }
      const id = (this.#nextImage = nextId(this.#nextImage)),
        view = viewOf(header);
      header.fill(0);
      view.setFloat64(0, heads[0] ?? 0, true);
      view.setFloat64(8, heads[1] ?? 0, true);
      view.setFloat64(16, table.id, true);
      view.setFloat64(24, table.schemaVersion, true);
      view.setUint8(32, old ? 1 : 0);
      view.setUint8(33, next ? 1 : 0);
      view.setUint32(36, table.columns.length, true);
      await this.scope.directory.put(IMAGE, id, header);
      this.#check();
      return { spoolId: this.scope.rootId, row: id };
    });
  }
  #validateProjection(table: TableDef, projection: RowBindingProjection): void {
    for (const side of [projection.old, projection.new])
      for (const id of side) if (!table.columns.some((column) => column.id === id)) state();
  }
  async #walk(
    table: TableDef,
    head: number,
    selected: readonly number[] | null,
    visit: (position: number, scalar: Uint8Array) => Promise<void>,
    signal: AbortSignal,
  ): Promise<void> {
    const scalar = this.#scalar;
    if (!scalar) state();
    for (const [position, column] of table.columns.entries()) {
      this.#check(signal);
      await this.scope.checkpoint();
      this.#check(signal);
      await this.#get(SCALAR, head, scalar);
      this.#check(signal);
      const view = viewOf(scalar),
        next = view.getFloat64(0, true);
      if (view.getFloat64(8, true) !== column.id) corrupt();
      if (selected === null || selected.includes(column.id)) await visit(position, scalar);
      head = next;
    }
    if (head !== 0) corrupt();
  }
  readRowBindings(
    ref: RowImageRef,
    projection: RowBindingProjection,
    signal: AbortSignal,
  ): Promise<RowBindingLease> {
    return this.#read(ref, projection, signal);
  }
  async readRowImages(ref: RowImageRef, signal: AbortSignal): Promise<RowImageLease> {
    const lease = await this.#read(ref, null, signal);
    try {
      const batch = (columns: RowBindingLease["old"]): Batch | null => {
        if (!columns) return null;
        // A full read never has unread slots; keep the exact same clearing array as the lease.
        if (columns.some((column) => column === ROW_IMAGE_UNREAD)) state();
        return { length: 1, selection: null, columns: columns as readonly Vector[] };
      };
      const old = batch(lease.old),
        next = batch(lease.new);
      return {
        tableId: lease.tableId,
        get old() {
          void lease.old;
          return old;
        },
        get new() {
          void lease.new;
          return next;
        },
        release: () => lease.release(),
      };
    } catch (error) {
      lease.release();
      throw error;
    }
  }
  #read(
    ref: RowImageRef,
    projection: RowBindingProjection | null,
    signal: AbortSignal,
  ): Promise<RowBindingLease> {
    signal = AbortSignal.any([signal, this.scope.signal, this.#stop.signal]);
    this.#check(signal);
    if (
      ref.spoolId !== this.scope.rootId ||
      !Number.isSafeInteger(ref.row) ||
      ref.row < 1 ||
      ref.row > this.#nextImage
    )
      state();
    return this.#work(async () => {
      const header = this.#header;
      if (!header) state();
      await this.#get(IMAGE, ref.row, header);
      this.#check(signal);
      const view = viewOf(header),
        table = this.scope.catalog.table(view.getFloat64(16, true));
      if (
        table?.schemaVersion !== view.getFloat64(24, true) ||
        table.columns.length !== view.getUint32(36, true) ||
        view.getUint8(32) > 1 ||
        view.getUint8(33) > 1
      )
        corrupt();
      if (projection) this.#validateProjection(table, projection);
      const oldHead = view.getFloat64(0, true),
        nextHead = view.getFloat64(8, true),
        hasOld = view.getUint8(32) === 1,
        hasNext = view.getUint8(33) === 1;
      if (!hasOld && !hasNext) corrupt();
      let scalarBytes = 0,
        imageExtra = 0,
        workspace = 0;
      const measure = async (position: number, scalar: Uint8Array) => {
        const column = table.columns[position];
        if (!column) corrupt();
        const retained = registryScalarBytes(scalar, column.type);
        scalarBytes += retained;
        imageExtra += Math.max(0, retained - dataBytes(column.type, 1) - 4);
        workspace = Math.max(workspace, registryScalarWorkspaceBytes(scalar, column.type));
        if (!Number.isSafeInteger(scalarBytes)) corrupt();
      };
      if (hasOld) await this.#walk(table, oldHead, projection?.old ?? null, measure, signal);
      else if (oldHead !== 0) corrupt();
      if (hasNext) await this.#walk(table, nextHead, projection?.new ?? null, measure, signal);
      else if (nextHead !== 0) corrupt();
      const bytes = 2 * table.columns.length * 1024 + scalarBytes;
      // No image lease or native input progress is held here. The host owns the shared
      // progress step, including one unique binding copy; it may wait cancellably.
      await this.scope.prepare(imageExtra + scalarBytes + workspace, signal);
      this.#check(signal);
      if (!this.scope.charge.allocate(bytes)) throw new RangeError("Image lease must be admitted");
      let old: Array<Vector | typeof ROW_IMAGE_UNREAD> | null = null,
        next: Array<Vector | typeof ROW_IMAGE_UNREAD> | null = null;
      let transferred = false;
      try {
        old = hasOld ? [] : null;
        next = hasNext ? [] : null;
        for (let remaining = table.columns.length; remaining > 0; remaining--) {
          old?.push(ROW_IMAGE_UNREAD);
          next?.push(ROW_IMAGE_UNREAD);
        }
        const load =
          (target: Array<Vector | typeof ROW_IMAGE_UNREAD>) =>
          async (position: number, scalar: Uint8Array) => {
            const column = table.columns[position];
            if (!column) corrupt();
            target[position] = await loadRegistryScalar(
              scalar,
              column.type,
              this.scope.fragments,
              this.scope.charge,
              async () => {
                await this.scope.checkpoint();
                this.#check(signal);
              },
              signal,
            );
            this.#check(signal);
          };
        if (old) await this.#walk(table, oldHead, projection?.old ?? null, load(old), signal);
        if (next) await this.#walk(table, nextHead, projection?.new ?? null, load(next), signal);
        this.#check(signal);
        const charge = this.scope.charge;
        let released = false,
          retire!: () => void;
        this.#lease = new Promise<void>((resolve) => {
          retire = resolve;
        });
        const check = () => {
          if (released) state();
        };
        transferred = true;
        return {
          tableId: table.id,
          get old() {
            check();
            return old;
          },
          get new() {
            check();
            return next;
          },
          release: () => {
            if (released) return;
            released = true;
            if (old) old.length = 0;
            if (next) next.length = 0;
            charge.free(bytes);
            this.#lease = null;
            retire();
          },
        };
      } finally {
        if (!transferred) {
          if (old) old.length = 0;
          if (next) next.length = 0;
          this.scope.charge.free(bytes);
        }
      }
    });
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#closed = true;
      this.#stop.abort(new TypeError("Native row images are closed"));
      await this.#pending?.catch(() => undefined);
      await this.#lease;
      this.#header = this.#scalar = null;
      this.#signal = null;
      this.#pending = null;
      this.#failure = null;
      this.scope.charge.free(REGISTRY_IMAGE_BYTES);
      this.#scope = null;
    })());
  }
}
