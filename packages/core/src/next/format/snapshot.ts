/** Portable snapshot1 outer grammar. Payload views borrow their frame until the next read step.
 * CRC32/ISO-HDLC is integrity, not authentication. No native address or storage call appears here.
 */
import { DURABLE_CODECS, DURABLE_FORMATS, requireHeader, requireTag } from "../durable-format.js";
import type { ByteBudget } from "../storage/extents.js";
import { pauseIfDue, type TaskWork } from "../task.js";

export const SNAPSHOT_HEADER_BYTES = 64;
export const SNAPSHOT_FRAME_HEADER_BYTES = 40;
export const SNAPSHOT_PAYLOAD_BYTES = 1 << 20;
export const SNAPSHOT_CONTROL_BYTES = 1024;
export const SnapshotKind = DURABLE_CODECS.snapshot;
export type SnapshotKind = (typeof SnapshotKind)[keyof typeof SnapshotKind];
export class SnapshotFormatError extends Error {
  readonly code = "MINNOW_SNAPSHOT_FORMAT";
  constructor(message: string) {
    super(message);
    this.name = "SnapshotFormatError";
  }
}
export class SnapshotBudgetError extends Error {
  readonly code = "MINNOW_SNAPSHOT_BUDGET";
  constructor(readonly bytes: number) {
    super(`Snapshot needs ${String(bytes)} charged bytes`);
    this.name = "SnapshotBudgetError";
  }
}
export class LegacySnapshotError extends Error {
  readonly code = "MINNOW_LEGACY_DATA";
  constructor() {
    super(
      "This snapshot was written by Minnow 0.x; Minnow 1.0 does not load it. The archive and target are unchanged. Use Minnow 0.x to read it or start fresh.",
    );
    this.name = "LegacySnapshotError";
  }
}
function check(value: boolean, message: string): asserts value {
  if (!value) throw new SnapshotFormatError(message);
}
function whole(value: number, what: string): void {
  check(Number.isSafeInteger(value) && value >= 0, `Invalid snapshot ${what}`);
}
const CRC_TABLE = Uint32Array.from({ length: 256 }, (_, n) => {
  let value = n;
  for (let bit = 0; bit < 8; bit++)
    value = (value >>> 1) ^ ((value & 1) === 0 ? 0 : DURABLE_CODECS.checksum.reflectedPolynomial);
  return value >>> 0;
});
/** Continue a finalized CRC; crc32(empty)=0 and crc32("123456789")=0xcbf43926. */
export function snapshotCrc32(bytes: Uint8Array, previous = 0): number {
  let value = previous ^ 0xffffffff;
  for (const byte of bytes) value = (value >>> 8) ^ (CRC_TABLE[(value ^ byte) & 255] ?? 0);
  return (value ^ 0xffffffff) >>> 0;
}
const viewOf = (bytes: Uint8Array) =>
  new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
function put53(view: DataView, at: number, value: number): void {
  whole(value, "u53");
  view.setUint32(at, value >>> 0, true);
  view.setUint32(at + 4, Math.floor(value / 2 ** 32), true);
}
export function snapshotU53(bytes: Uint8Array, at: number): number {
  check(at >= 0 && at + 8 <= bytes.length, "Truncated snapshot integer");
  const view = viewOf(bytes),
    value = view.getUint32(at, true) + view.getUint32(at + 4, true) * 2 ** 32;
  whole(value, "u53");
  return value;
}
export interface SnapshotHeader {
  readonly id: Uint8Array;
  readonly sourceLsn: number;
}
export interface SnapshotFrame {
  readonly kind: SnapshotKind;
  readonly ordinal: number;
  readonly objectId: number;
  readonly partIndex: number;
  readonly payload: Uint8Array;
}
export interface SnapshotFrameHeader {
  readonly kind: SnapshotKind;
  readonly ordinal: number;
  readonly objectId: number;
  readonly partIndex: number;
  readonly payloadBytes: number;
}
function distinct(a: Uint8Array, b: Uint8Array): void {
  check(a.buffer !== b.buffer, "Snapshot input aliases its output");
}
/** Caller has charged and allocated the exact destination. All argument checks precede mutation. */
export function encodeSnapshotHeader(header: SnapshotHeader, into: Uint8Array): void {
  check(into.length === 64 && header.id.length === 16, "Invalid snapshot header size");
  whole(header.sourceLsn, "LSN");
  distinct(header.id, into);
  into.fill(0);
  const view = viewOf(into);
  view.setUint32(0, DURABLE_FORMATS.snapshot.magic, true);
  view.setUint16(4, 1, true);
  view.setUint32(8, 64, true);
  view.setUint32(12, 1, true);
  into.set(header.id, 16);
  put53(view, 32, header.sourceLsn);
  view.setUint32(60, snapshotCrc32(into.subarray(0, 60)), true);
}
/** UUID borrows the source header. Stream readers retain its charged64-byte buffer. */
export function decodeSnapshotHeader(bytes: Uint8Array): SnapshotHeader {
  if (DURABLE_CODECS.legacySnapshotPrefix.every((byte, i) => bytes[i] === byte))
    throw new LegacySnapshotError();
  check(bytes.length === 64, "Truncated or oversized snapshot header");
  requireHeader("snapshot", bytes, SnapshotFormatError);
  check(
    viewOf(bytes).getUint32(60, true) === snapshotCrc32(bytes.subarray(0, 60)),
    "Snapshot header CRC mismatch",
  );
  return { id: bytes.subarray(16, 32), sourceLsn: snapshotU53(bytes, 32) };
}
export function snapshotFrameBytes(payloadBytes: number): number {
  whole(payloadBytes, "payload length");
  check(payloadBytes <= SNAPSHOT_PAYLOAD_BYTES, "Snapshot payload exceeds1MiB");
  return Math.ceil((40 + payloadBytes + 4) / 8) * 8;
}
function prepareSnapshotFrame(frame: SnapshotFrame, into: Uint8Array): void {
  requireTag(frame.kind, Object.values(SnapshotKind), "snapshot frame kind", SnapshotFormatError);
  whole(frame.ordinal, "ordinal");
  whole(frame.objectId, "object id");
  whole(frame.partIndex, "part index");
  check(frame.objectId > 0 && frame.partIndex <= 0xffffffff, "Invalid snapshot object/part index");
  check(
    into.length === snapshotFrameBytes(frame.payload.length),
    "Wrong snapshot frame destination",
  );
  distinct(frame.payload, into);
  into.fill(0);
  const view = viewOf(into);
  view.setUint32(0, DURABLE_FORMATS.snapshotFrame.magic, true);
  into[4] = frame.kind;
  put53(view, 8, frame.ordinal);
  put53(view, 16, frame.objectId);
  view.setUint32(24, frame.partIndex, true);
  view.setUint32(28, frame.payload.length, true);
  view.setUint32(36, snapshotCrc32(into.subarray(0, 36)), true);
  into.set(frame.payload, 40);
}
export function encodeSnapshotFrame(frame: SnapshotFrame, into: Uint8Array): void {
  prepareSnapshotFrame(frame, into);
  viewOf(into).setUint32(
    40 + frame.payload.length,
    snapshotCrc32(into.subarray(0, 40 + frame.payload.length)),
    true,
  );
}
/** Checks bounded length BEFORE a stream allocates the body. */
export function decodeSnapshotFrameHeader(bytes: Uint8Array): SnapshotFrameHeader {
  check(bytes.length === 40, "Truncated frame header");
  requireHeader("snapshotFrame", bytes, SnapshotFormatError);
  const view = viewOf(bytes),
    payloadBytes = view.getUint32(28, true);
  snapshotFrameBytes(payloadBytes);
  check(
    view.getUint32(36, true) === snapshotCrc32(bytes.subarray(0, 36)),
    "Snapshot frame header CRC mismatch",
  );
  const objectId = snapshotU53(bytes, 16);
  check(objectId > 0, "Snapshot object id zero");
  return {
    kind: bytes[4] as SnapshotKind,
    ordinal: snapshotU53(bytes, 8),
    objectId,
    partIndex: view.getUint32(24, true),
    payloadBytes,
  };
}
export function decodeSnapshotFrame(bytes: Uint8Array): SnapshotFrame {
  check(bytes.length >= 40, "Truncated frame");
  const header = decodeSnapshotFrameHeader(bytes.subarray(0, 40));
  check(bytes.length === snapshotFrameBytes(header.payloadBytes), "Truncated or oversized frame");
  const end = 40 + header.payloadBytes,
    view = viewOf(bytes);
  check(
    view.getUint32(end, true) === snapshotCrc32(bytes.subarray(0, end)),
    "Snapshot frame CRC mismatch",
  );
  for (let at = end + 4; at < bytes.length; at++)
    check(bytes[at] === 0, "Nonzero snapshot frame padding");
  return { ...header, payload: bytes.subarray(40, end) };
}

export interface SnapshotFooter {
  readonly frames: number;
  readonly objects: number;
  readonly tables: number;
  readonly rows: number;
  readonly columnBytes: number;
  readonly catalogCrc: number;
  readonly streamCrc: number;
}
export function encodeSnapshotFooter(
  header: SnapshotHeader,
  totals: SnapshotFooter,
  into: Uint8Array,
): void {
  check(into.length === 80 && header.id.length === 16, "Invalid snapshot footer size");
  distinct(header.id, into);
  for (const n of [
    header.sourceLsn,
    totals.frames,
    totals.objects,
    totals.tables,
    totals.rows,
    totals.columnBytes,
  ])
    whole(n, "footer count");
  for (const n of [totals.catalogCrc, totals.streamCrc])
    check(Number.isInteger(n) && n >= 0 && n <= 0xffffffff, "Invalid footer CRC");
  into.fill(0);
  into.set(header.id);
  const view = viewOf(into);
  put53(view, 16, header.sourceLsn);
  view.setUint32(24, totals.catalogCrc, true);
  put53(view, 32, totals.frames);
  put53(view, 40, totals.objects);
  put53(view, 48, totals.tables);
  put53(view, 56, totals.rows);
  put53(view, 64, totals.columnBytes);
  view.setUint32(72, totals.streamCrc, true);
}
export function decodeSnapshotFooter(header: SnapshotHeader, bytes: Uint8Array): SnapshotFooter {
  check(bytes.length === 80, "Invalid snapshot footer length");
  check(
    header.id.every((byte, i) => bytes[i] === byte) && snapshotU53(bytes, 16) === header.sourceLsn,
    "Snapshot footer identity mismatch",
  );
  requireHeader("snapshotFooter", bytes, SnapshotFormatError);
  const view = viewOf(bytes);
  return {
    catalogCrc: view.getUint32(24, true),
    streamCrc: view.getUint32(72, true),
    frames: snapshotU53(bytes, 32),
    objects: snapshotU53(bytes, 40),
    tables: snapshotU53(bytes, 48),
    rows: snapshotU53(bytes, 56),
    columnBytes: snapshotU53(bytes, 64),
  };
}
/** Canonical begin payloads. COLUMN_BEGIN is48 bytes; TABLE_BEGIN is24; TABLE_END is8. */
export function snapshotNumbers(values: readonly number[], into: Uint8Array): void {
  check(into.length === values.length * 8, "Wrong snapshot numeric record length");
  for (const value of values) whole(value, "record integer");
  values.forEach((value, i) => put53(viewOf(into), i * 8, value));
}

/** Bounded framing state: consecutive object IDs avoid an unbounded seen-ID set. */
class Order {
  ordinal = 0;
  objects = 0;
  tables = 0;
  rows = 0;
  columnBytes = 0;
  catalogCrc = 0;
  done = false;
  #catalog = false;
  #object = 0;
  #part = 0;
  #kind: "catalog" | "column" | null = null;
  #bytes = 0;
  #catalogBytes = 0;
  #table = 0;
  #tableId = 0;
  #schema = 0;
  #columns = 0;
  #seen = 0;
  #previousColumn = 0;
  #blockRows = 0;
  #tableRows = 0;
  #expectedBytes = 0;
  constructor(readonly header: SnapshotHeader) {}
  consume(frame: SnapshotFrame, streamCrc: number, catalogCrc: number): void {
    check(
      !this.done && frame.ordinal === this.ordinal,
      "Snapshot ordinal missing, duplicated or reordered",
    );
    const { kind, objectId, partIndex, payload } = frame;
    if (kind === SnapshotKind.footer) {
      check(
        this.#catalog &&
          this.#kind === null &&
          this.#table === 0 &&
          partIndex === 0 &&
          objectId === this.objects + 1,
        "Premature snapshot footer",
      );
      const got = decodeSnapshotFooter(this.header, payload);
      for (const key of [
        "frames",
        "objects",
        "tables",
        "rows",
        "columnBytes",
        "catalogCrc",
        "streamCrc",
      ] as const) {
        const expected =
          key === "frames" ? this.ordinal : key === "streamCrc" ? streamCrc : this[key];
        check(got[key] === expected, `Snapshot footer ${key} mismatch`);
      }
      this.done = true;
      return;
    }
    if (kind === SnapshotKind.catalogBegin || kind === SnapshotKind.columnBegin) {
      check(
        this.#kind === null && partIndex === 0 && objectId === this.objects + 1,
        "Duplicate/overlapping snapshot object",
      );
      if (kind === SnapshotKind.catalogBegin) {
        check(
          !this.#catalog && this.ordinal === 0 && payload.length === 0,
          "Invalid catalog begin",
        );
        this.#kind = "catalog";
      } else {
        check(
          this.#catalog && this.#table !== 0 && payload.length === 48 && this.#columns > 0,
          "Column outside table",
        );
        const id = snapshotU53(payload, 0),
          column = snapshotU53(payload, 8),
          schema = snapshotU53(payload, 16);
        const first = snapshotU53(payload, 24),
          rows = snapshotU53(payload, 32),
          bytes = snapshotU53(payload, 40);
        check(
          id === this.#tableId &&
            schema === this.#schema &&
            column > this.#previousColumn &&
            first === this.#tableRows &&
            rows > 0 &&
            rows <= 65536,
          "Invalid column block identity/range",
        );
        if (this.#seen === 0) this.#blockRows = rows;
        check(rows === this.#blockRows, "Column blocks disagree on rows");
        this.#previousColumn = column;
        this.#expectedBytes = bytes;
        this.#kind = "column";
      }
      this.objects++;
      this.#object = objectId;
      this.#part = 1;
      this.#bytes = 0;
    } else if (kind === SnapshotKind.catalogPart || kind === SnapshotKind.columnPart) {
      check(
        this.#kind === (kind === SnapshotKind.catalogPart ? "catalog" : "column") &&
          objectId === this.#object &&
          partIndex === this.#part++ &&
          payload.length > 0,
        "Snapshot object part missing/reordered",
      );
      this.#bytes += payload.length;
      whole(this.#bytes, "object bytes");
      if (kind === SnapshotKind.catalogPart) {
        this.catalogCrc = catalogCrc;
        this.#catalogBytes += payload.length;
      } else {
        this.columnBytes += payload.length;
        whole(this.columnBytes, "column bytes");
      }
    } else if (kind === SnapshotKind.catalogEnd || kind === SnapshotKind.columnEnd) {
      check(
        this.#kind === (kind === SnapshotKind.catalogEnd ? "catalog" : "column") &&
          objectId === this.#object &&
          partIndex === this.#part &&
          payload.length === 16,
        "Incomplete snapshot object",
      );
      check(snapshotU53(payload, 0) === this.#bytes, "Snapshot object byte count mismatch");
      if (kind === SnapshotKind.catalogEnd) {
        const view = viewOf(payload);
        check(
          view.getUint32(8, true) === this.catalogCrc &&
            view.getUint32(12, true) === 0 &&
            this.#catalogBytes > 0,
          "Catalog CRC/count mismatch",
        );
        this.#catalog = true;
      } else {
        check(
          this.#bytes === this.#expectedBytes && snapshotU53(payload, 8) === this.#blockRows,
          "Column byte/row count mismatch",
        );
        if (++this.#seen === this.#columns) {
          this.#tableRows += this.#blockRows;
          this.#seen = 0;
          this.#previousColumn = 0;
        }
      }
      this.#kind = null;
    } else if (kind === SnapshotKind.tableBegin) {
      check(
        this.#catalog &&
          this.#kind === null &&
          this.#table === 0 &&
          partIndex === 0 &&
          objectId === this.objects + 1 &&
          payload.length === 24,
        "Overlapping snapshot table",
      );
      this.#tableId = snapshotU53(payload, 0);
      check(this.#tableId > 0, "Table id zero");
      this.#schema = snapshotU53(payload, 8);
      this.#columns = snapshotU53(payload, 16);
      this.#table = objectId;
      this.objects++;
      this.#tableRows = 0;
      this.#seen = 0;
      this.#previousColumn = 0;
    } else {
      check(
        this.#table !== 0 &&
          this.#kind === null &&
          this.#seen === 0 &&
          objectId === this.#table &&
          partIndex === 1 &&
          payload.length === 8,
        "Invalid table end",
      );
      const rows = snapshotU53(payload, 0);
      check(this.#columns === 0 || rows === this.#tableRows, "Table row count mismatch");
      this.rows += rows;
      whole(this.rows, "total rows");
      this.tables++;
      this.#table = 0;
    }
    this.ordinal++;
  }
  totals(streamCrc: number): SnapshotFooter {
    return {
      frames: this.ordinal,
      objects: this.objects,
      tables: this.tables,
      rows: this.rows,
      columnBytes: this.columnBytes,
      catalogCrc: this.catalogCrc,
      streamCrc,
    };
  }
}

/** write consumes/borrows bytes until its promise settles; it must not retain or mutate them. */
export interface SnapshotSink {
  write(bytes: Uint8Array, signal?: AbortSignal): void | Promise<void>;
}
export interface SnapshotWork extends TaskWork {
  readonly signal?: AbortSignal;
}
async function pace(work: SnapshotWork): Promise<void> {
  work.signal?.throwIfAborted();
  const pending = pauseIfDue(work);
  if (pending !== undefined) await pending;
  work.signal?.throwIfAborted();
}
/** Bound checksum CPU as well as I/O continuations on the shared worker clock. */
async function streamCrc(bytes: Uint8Array, previous: number, work: SnapshotWork): Promise<number> {
  let crc = previous;
  for (let from = 0; from < bytes.length; from += 65536) {
    await pace(work);
    crc = snapshotCrc32(bytes.subarray(from, from + 65536), crc);
  }
  return crc;
}
function charge(budget: ByteBudget, bytes: number): void {
  if (!budget.allocate(bytes)) throw new SnapshotBudgetError(bytes);
}
export class SnapshotStreamWriter {
  readonly #order: Order;
  #crc = 0;
  #closed = false;
  #pending = false;
  #control = true;
  private constructor(
    readonly header: SnapshotHeader,
    readonly sink: SnapshotSink,
    readonly budget: ByteBudget,
    readonly work: SnapshotWork,
  ) {
    this.#order = new Order(header);
  }
  static async open(
    header: SnapshotHeader,
    sink: SnapshotSink,
    budget: ByteBudget,
    work: SnapshotWork = {},
  ): Promise<SnapshotStreamWriter> {
    check(header.id.length === 16, "Invalid snapshot identity size");
    whole(header.sourceLsn, "LSN");
    charge(budget, SNAPSHOT_CONTROL_BYTES);
    let writer: SnapshotStreamWriter | undefined;
    try {
      writer = new SnapshotStreamWriter(
        { id: header.id.slice(), sourceLsn: header.sourceLsn },
        sink,
        budget,
        work,
      );
      charge(budget, 64);
      try {
        const bytes = new Uint8Array(64);
        encodeSnapshotHeader(writer.header, bytes);
        writer.#crc = snapshotCrc32(bytes);
        await writer.#emit(bytes);
      } finally {
        budget.free(64);
      }
      return writer;
    } catch (error) {
      if (writer === undefined) budget.free(SNAPSHOT_CONTROL_BYTES);
      else writer.close();
      throw error;
    }
  }
  async #emit(bytes: Uint8Array): Promise<void> {
    await pace(this.work);
    check(!this.#closed, "Snapshot writer closed");
    await this.sink.write(bytes, this.work.signal);
    this.work.signal?.throwIfAborted();
  }
  async frame(
    kind: SnapshotKind,
    objectId: number,
    partIndex: number,
    payload: Uint8Array,
  ): Promise<void> {
    check(!this.#closed && !this.#pending, "Snapshot writer closed or reentered");
    this.#pending = true;
    let size = 0;
    let funded = false;
    try {
      size = snapshotFrameBytes(payload.length);
      charge(this.budget, size);
      funded = true;
      const bytes = new Uint8Array(size);
      prepareSnapshotFrame(
        { kind, ordinal: this.#order.ordinal, objectId, partIndex, payload },
        bytes,
      );
      const end = 40 + payload.length;
      viewOf(bytes).setUint32(end, await streamCrc(bytes.subarray(0, end), 0, this.work), true);
      const own = bytes.subarray(40, end);
      const catalogCrc =
        kind === SnapshotKind.catalogPart
          ? await streamCrc(own, this.#order.catalogCrc, this.work)
          : this.#order.catalogCrc;
      this.#order.consume(
        { kind, ordinal: this.#order.ordinal, objectId, partIndex, payload: own },
        this.#crc,
        catalogCrc,
      );
      const next =
        kind === SnapshotKind.footer ? this.#crc : await streamCrc(bytes, this.#crc, this.work);
      await this.#emit(bytes);
      this.#crc = next;
    } catch (error) {
      this.close();
      throw error;
    } finally {
      if (funded) this.budget.free(size);
      this.#pending = false;
      this.#releaseControl();
    }
  }
  async finish(): Promise<void> {
    let funded = false;
    try {
      charge(this.budget, 80);
      funded = true;
      const payload = new Uint8Array(80);
      encodeSnapshotFooter(this.header, this.#order.totals(this.#crc), payload);
      await this.frame(SnapshotKind.footer, this.#order.objects + 1, 0, payload);
    } finally {
      if (funded) this.budget.free(80);
      this.close();
    }
  }
  close(): void {
    this.#closed = true;
    this.#releaseControl();
  }
  #releaseControl(): void {
    if (this.#closed && !this.#pending && this.#control) {
      this.#control = false;
      this.budget.free(SNAPSHOT_CONTROL_BYTES);
    }
  }
}

/** Incoming chunks borrow producer memory; charged whole while retained. Frames borrow until advance. */
export class SnapshotStreamReader {
  #chunk: Uint8Array | null = null;
  #at = 0;
  #closed = false;
  #crc = 0;
  #headerBytes: Uint8Array | null = null;
  #order: Order | null = null;
  #started = false;
  readonly #owned = new Set<Uint8Array>();
  #free(bytes: Uint8Array): void {
    if (this.#owned.delete(bytes)) this.budget.free(bytes.length);
  }
  private constructor(
    readonly input: AsyncIterator<Uint8Array>,
    readonly budget: ByteBudget,
    readonly work: SnapshotWork,
  ) {}
  get header(): SnapshotHeader {
    check(this.#order !== null, "Snapshot reader not initialized");
    return this.#order.header;
  }
  static async open(
    input: AsyncIterable<Uint8Array>,
    budget: ByteBudget,
    work: SnapshotWork = {},
  ): Promise<SnapshotStreamReader> {
    charge(budget, SNAPSHOT_CONTROL_BYTES);
    let reader: SnapshotStreamReader | undefined;
    try {
      reader = new SnapshotStreamReader(input[Symbol.asyncIterator](), budget, work);
      const prefix = await reader.#exact(8);
      try {
        if (DURABLE_CODECS.legacySnapshotPrefix.every((byte, i) => prefix[i] === byte))
          throw new LegacySnapshotError();
        reader.#headerBytes = await reader.#exact(64, prefix);
      } finally {
        reader.#free(prefix);
      }
      reader.#order = new Order(decodeSnapshotHeader(reader.#headerBytes));
      reader.#crc = snapshotCrc32(reader.#headerBytes);
      return reader;
    } catch (error) {
      if (reader === undefined) budget.free(SNAPSHOT_CONTROL_BYTES);
      else reader.close();
      throw error;
    }
  }
  async #more(): Promise<boolean> {
    if (this.#chunk !== null && this.#at < this.#chunk.length) return true;
    if (this.#chunk !== null) this.budget.free(this.#chunk.length);
    this.#chunk = null;
    this.#at = 0;
    for (;;) {
      await pace(this.work);
      check(!this.#closed, "Snapshot reader closed");
      const next = this.input.next();
      let result: IteratorResult<Uint8Array>;
      const signal = this.work.signal;
      if (signal === undefined) result = await next;
      else {
        let cancel!: () => void;
        try {
          result = await Promise.race([
            next,
            new Promise<never>((_, reject) => {
              cancel = () =>
                reject(
                  signal.reason instanceof Error
                    ? signal.reason
                    : new Error("Snapshot cancelled", { cause: signal.reason }),
                );
              signal.addEventListener("abort", cancel, { once: true });
              if (signal.aborted) cancel();
            }),
          ]);
        } finally {
          signal.removeEventListener("abort", cancel);
        }
      }
      check(!this.#closed, "Snapshot reader closed");
      if (result.done === true) return false;
      check(result.value instanceof Uint8Array, "Snapshot source yielded non-bytes");
      if (result.value.length === 0) continue;
      charge(this.budget, result.value.length);
      this.#chunk = result.value;
      return true;
    }
  }
  async #exact(length: number, prefix?: Uint8Array): Promise<Uint8Array> {
    charge(this.budget, length);
    let output: Uint8Array | undefined;
    try {
      output = new Uint8Array(length);
      this.#owned.add(output);
      let at = 0;
      if (prefix !== undefined) {
        output.set(prefix);
        at = prefix.length;
      }
      while (at < length) {
        check(await this.#more(), "Truncated snapshot stream");
        const bytes = this.#chunk;
        check(bytes !== null, "Missing input chunk");
        const take = Math.min(bytes.length - this.#at, length - at);
        output.set(bytes.subarray(this.#at, this.#at + take), at);
        at += take;
        this.#at += take;
      }
      return output;
    } catch (error) {
      if (output === undefined) this.budget.free(length);
      else this.#free(output);
      throw error;
    }
  }
  async *frames(): AsyncGenerator<SnapshotFrame> {
    check(!this.#started && !this.#closed, "Snapshot reader closed or reentered");
    this.#started = true;
    try {
      const order = this.#order;
      check(order !== null, "Snapshot reader not initialized");
      while (!order.done) {
        await pace(this.work);
        check(!this.#closed, "Snapshot reader closed");
        const prefix = await this.#exact(40);
        let bytes: Uint8Array | null = null;
        try {
          const header = decodeSnapshotFrameHeader(prefix),
            size = snapshotFrameBytes(header.payloadBytes);
          bytes = await this.#exact(size, prefix);
          const end = 40 + header.payloadBytes;
          check(
            viewOf(bytes).getUint32(end, true) ===
              (await streamCrc(bytes.subarray(0, end), 0, this.work)),
            "Snapshot frame CRC mismatch",
          );
          for (let at = end + 4; at < bytes.length; at++)
            check(bytes[at] === 0, "Nonzero snapshot frame padding");
          const frame = { ...header, payload: bytes.subarray(40, end) };
          const catalogCrc =
            frame.kind === SnapshotKind.catalogPart
              ? await streamCrc(frame.payload, order.catalogCrc, this.work)
              : order.catalogCrc;
          order.consume(frame, this.#crc, catalogCrc);
          if (frame.kind !== SnapshotKind.footer)
            this.#crc = await streamCrc(bytes, this.#crc, this.work);
          check(!this.#closed, "Snapshot reader closed");
          yield frame;
        } finally {
          this.#free(prefix);
          if (bytes !== null) this.#free(bytes);
        }
      }
      check(!(await this.#more()), "Trailing bytes after snapshot footer");
    } finally {
      this.close();
    }
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#chunk !== null) {
      this.budget.free(this.#chunk.length);
      this.#chunk = null;
    }
    for (const bytes of this.#owned) this.budget.free(bytes.length);
    this.#owned.clear();
    this.#headerBytes = null;
    this.#order = null;
    this.budget.free(SNAPSHOT_CONTROL_BYTES);
    // Custom iterators may throw before returning a promise. Cleanup must not replace the
    // primary typed legacy/format/cancel refusal; our buffers have already been released.
    try {
      void Promise.resolve(this.input.return?.()).catch(() => undefined);
    } catch {
      // Upstream cleanup is best effort, for both synchronous and asynchronous failures.
    }
  }
}
