/** Native row checkpoints with bounded (64 KiB) payload fragments.
 * Large values are never materialized by the spill codec; the next complete admission
 * separately funds the one restored row. No input vectors/grants survive seal(). */
import type { PhysicalType } from "../types.js";
import { samePhysicalType, layoutOf } from "../types.js";
import type { Vector, FlatVector } from "../vector/vector.js";
import { isValidSlot, slotOf } from "./kernels/vectors.js";
import { bigintBytes } from "../memory/governor.js";
import { SpillCorruptionError } from "../spill/contract.js";
import { ColumnStore } from "./column-store.js";
import { RecordCheckpoint } from "./record-checkpoint.js";
import { decodePage } from "./spill-pages.js";
import type { SpillIo } from "./spill-pool.js";
import type { SpillService } from "../spill/contract.js";
import { nullParameter } from "./parameters.js";
const I32: PhysicalType = { kind: "int32", bits: 32 };
const BYTES: PhysicalType = { kind: "bytes" };
const FRAGMENT = 65536;
const EMPTY = new Uint8Array(0);
const WRITER_BYTES = 2048;
const align = (n: number) => Math.ceil(n / 8) * 8;
interface Writer {
  readonly io: SpillIo;
  readonly headers: Int32Array;
  readonly vectors: readonly FlatVector[];
  readonly data: { layout: "varlen"; bytes: Uint8Array; offsets: Int32Array };
}
export class JsonRecords {
  readonly #records: RecordCheckpoint;
  #writer: Writer | null;
  #payload = 0;
  #rows = 0;
  #extra = 0;
  #closing: Promise<void> | null = null;
  constructor(
    parent: SpillIo,
    service: SpillService,
    readonly types: readonly PhysicalType[],
  ) {
    if (!parent.pool.allocate(WRITER_BYTES)) throw new Error("JSON record metadata not admitted");
    try {
      const headers = new Int32Array(3),
        data = { layout: "varlen" as const, bytes: EMPTY, offsets: new Int32Array(2) };
      const vectors: FlatVector[] = Array.from({ length: 3 }, (_, i) => ({
        form: "flat",
        type: I32,
        length: 1,
        validity: null,
        data: { layout: "i32", values: headers.subarray(i, i + 1) },
      }));
      vectors.push({ form: "flat", type: BYTES, length: 1, validity: null, data });
      this.#records = new RecordCheckpoint(parent, service, [I32, I32, I32, BYTES]);
      this.#writer = { io: parent, headers, vectors, data };
    } catch (error) {
      parent.pool.free(WRITER_BYTES);
      throw error;
    }
  }
  get rowCount(): number {
    return this.#rows;
  }
  get retainedBytes(): number {
    return this.#payload + this.#extra + 2048 + 2048 * this.types.length;
  }
  get files() {
    return this.#records.files;
  }
  async #record(
    column: number,
    offset: number,
    length: number,
    bytes: Uint8Array = EMPTY,
  ): Promise<void> {
    const writer = this.#writer;
    if (!writer) throw new Error("JSON records detached");
    writer.headers[0] = column;
    writer.headers[1] = offset;
    writer.headers[2] = length;
    writer.data.bytes = bytes;
    writer.data.offsets[1] = bytes.length;
    try {
      await this.#records.write(writer.vectors, 0);
    } finally {
      writer.data.bytes = EMPTY;
      writer.data.offsets[1] = 0;
    }
  }
  async write(columns: readonly Vector[], row: number): Promise<void> {
    if (columns.length !== this.types.length) throw new Error("JSON row schema mismatch");
    const writer = this.#writer;
    if (!writer) throw new Error("JSON records detached");
    let payload = 0,
      extra = 0;
    for (const value of columns) {
      if (!isValidSlot(value, row)) continue;
      const [flat, slot] = slotOf(value, row),
        d = flat.data;
      payload = align(payload);
      if (d.layout === "varlen") payload += (d.offsets[slot + 1] ?? 0) - (d.offsets[slot] ?? 0);
      else {
        const wide =
          d.layout === "decimal" && d.coefficients.repr === "wide"
            ? bigintBytes(d.coefficients.values[slot] ?? 0n)
            : 0;
        payload += 512 + wide;
        extra += wide ? wide + 8 : 0;
      }
    }
    if (payload > 0x7fffffff || extra > 0x7fffffff)
      throw new Error("JSON checkpoint row exceeds its native offset range");
    this.#payload = Math.max(this.#payload, payload);
    this.#extra = Math.max(this.#extra, extra);
    await this.#record(-1, payload, extra);
    for (let c = 0; c < columns.length; c++) {
      const value = columns[c];
      if (!value) throw new Error("Missing JSON field");
      if (!isValidSlot(value, row)) {
        await this.#record(c, -1, 0);
        continue;
      }
      const [flat, slot] = slotOf(value, row),
        d = flat.data;
      if (d.layout === "varlen") {
        const start = d.offsets[slot] ?? 0,
          length = (d.offsets[slot + 1] ?? start) - start;
        for (let at = 0; at < Math.max(1, length); at += FRAGMENT)
          await this.#record(
            c,
            at,
            length,
            d.bytes.subarray(start + at, start + Math.min(length, at + FRAGMENT)),
          );
      } else {
        const store = new ColumnStore([value.type], writer.io.pool, {
          chunkRows: 1,
          targetBytes: FRAGMENT,
          varlenBytes: 0,
        });
        try {
          if (store.append([value], null, row, 1) !== 1)
            throw new Error("JSON fixed cell not admitted");
          const encoded = writer.io.pages.encode(
            { queryId: writer.io.context.queryId, run: 0, partition: 0, sequence: 0 },
            store.vectors(),
            1,
          );
          if (!encoded || encoded.length > FRAGMENT)
            throw new Error("JSON fixed cell exceeds fragment proof");
          await this.#record(c, -2, encoded.length, encoded);
        } finally {
          store.close();
        }
      }
    }
    await this.#record(-2, 0, 0);
    this.#rows++;
  }
  #detach(): void {
    const writer = this.#writer;
    this.#writer = null;
    if (writer) writer.io.pool.free(WRITER_BYTES);
  }
  async seal(): Promise<void> {
    await this.#records.seal();
    this.#detach();
  }
  async copyTo(target: JsonRecords, io: SpillIo): Promise<void> {
    if (
      this.types.length !== target.types.length ||
      this.types.some((type, i) => !target.types[i] || !samePhysicalType(type, target.types[i]))
    )
      throw new SpillCorruptionError("JSON checkpoint schema changed");
    await this.#records.read(io, async (columns, rows) => {
      for (let row = 0; row < rows; row++) await target.#records.write(columns, row);
    });
    target.#rows += this.#rows;
    target.#payload = Math.max(target.#payload, this.#payload);
    target.#extra = Math.max(target.#extra, this.#extra);
  }
  async read(
    io: SpillIo,
    prepaid: number,
    visit: (columns: readonly FlatVector[], rows: number) => Promise<void>,
  ): Promise<void> {
    if (prepaid < this.retainedBytes) throw new Error("JSON restored row was not admitted");
    const buffer = new Uint8Array(this.#payload),
      columns: FlatVector[] = [];
    let active = false,
      cursor = 0,
      nextColumn = 0,
      current = -1,
      at = 0,
      total = 0,
      start = 0,
      wide = 0,
      wideLimit = 0,
      payloadLimit = 0;
    const charge = {
      allocate: (n: number) => {
        if (wide + n > wideLimit) return false;
        wide += n;
        return true;
      },
      free: (n: number) => {
        if (n > wide) throw new SpillCorruptionError("JSON wide refund");
        wide -= n;
      },
    };
    function bad(): never {
      throw new SpillCorruptionError("Invalid JSON checkpoint record");
    }
    await this.#records.read(io, async (records, rows) => {
      for (let row = 0; row < rows; row++) {
        io.checkAborted();
        const c = records[0],
          o = records[1],
          n = records[2],
          v = records[3];
        if (
          !c ||
          !o ||
          !n ||
          !v ||
          c.data.layout !== "i32" ||
          o.data.layout !== "i32" ||
          n.data.layout !== "i32" ||
          v.data.layout !== "varlen" ||
          [c, o, n, v].some((x) => !isValidSlot(x, row))
        )
          bad();
        const col = c.data.values[row] ?? 0,
          offset = o.data.values[row] ?? 0,
          length = n.data.values[row] ?? 0;
        const bytes = v.data.bytes.subarray(v.data.offsets[row] ?? 0, v.data.offsets[row + 1] ?? 0);
        if (col === -1) {
          if (
            active ||
            length < 0 ||
            length > this.#extra ||
            offset < 0 ||
            offset > this.#payload ||
            bytes.length
          )
            bad();
          active = true;
          cursor = 0;
          nextColumn = 0;
          current = -1;
          wide = 0;
          wideLimit = length;
          payloadLimit = offset;
          columns.length = 0;
          continue;
        }
        if (!active) bad();
        if (col === -2) {
          if (
            nextColumn !== this.types.length ||
            current !== -1 ||
            bytes.length ||
            offset ||
            length
          )
            bad();
          await visit(columns, 1);
          columns.length = 0;
          active = false;
          continue;
        }
        if (col < 0 || col >= this.types.length || length < 0 || bytes.length > FRAGMENT) bad();
        const type = this.types[col];
        if (!type) bad();
        if (offset === -1) {
          if (col !== nextColumn || current !== -1 || length || bytes.length) bad();
          columns.push(nullParameter(type));
          nextColumn++;
          continue;
        }
        if (offset === -2) {
          if (layoutOf(type) === "varlen") bad();
          if (col !== nextColumn || current !== -1 || length !== bytes.length) bad();
          cursor = align(cursor);
          if (cursor + length > payloadLimit) bad();
          buffer.set(bytes, cursor);
          const decoded = decodePage(buffer.subarray(cursor, cursor + length), [type], charge);
          if ("needed" in decoded || decoded.rows !== 1 || !decoded.columns[0]) bad();
          columns.push(decoded.columns[0]);
          cursor += length;
          nextColumn++;
          continue;
        }
        if (layoutOf(type) !== "varlen") bad();
        if (current === -1) {
          if (col !== nextColumn || offset !== 0) bad();
          start = align(cursor);
          if (start + length > payloadLimit) bad();
          current = col;
          at = 0;
          total = length;
          cursor = start + length;
          columns.push({
            form: "flat",
            type,
            length: 1,
            validity: null,
            data: {
              layout: "varlen",
              bytes: buffer.subarray(start, cursor),
              offsets: Int32Array.of(0, length),
            },
          });
        }
        if (col !== current || offset !== at || length !== total || at + bytes.length > total)
          bad();
        buffer.set(bytes, start + at);
        at += bytes.length;
        if (at === total) {
          current = -1;
          nextColumn++;
        }
      }
    });
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- the awaited record visitor mutates this state
    if (active) bad();
  }
  close(): Promise<void> {
    return (this.#closing ??= this.#records.close().finally(() => this.#detach()));
  }
}
