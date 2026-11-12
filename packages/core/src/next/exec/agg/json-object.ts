/** Ordered object fragments. JSONB's last-key-wins pass uses the shared external sort. */
import type { AggregateCall } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import type { Grant } from "../../memory/governor.js";
import { BATCH_ROWS, type Batch, type Vector, type FlatVector } from "../../vector/vector.js";
import { int64At } from "../../vector/int64.js";
import { isValidSlot, slotOf } from "../kernels/vectors.js";
import { jsonValue, quotedJson } from "../kernels/json-value.js";
import { JsonOutput } from "../kernels/json-output.js";
import { runSliced } from "../slices.js";
import { ExecutionError, UnsupportedPlanError } from "../errors.js";
import type { StreamedJsonField } from "../json-records.js";
import { SortOperator, sortMinimumBytes } from "../sort.js";
import { keysEqual, type SortColumn } from "../sort-keys.js";
import { ColumnStore } from "../column-store.js";
import type { Operator } from "../pipeline.js";
import { JsonBlobs, type JsonBlob } from "./json-blobs.js";
import { JsonStream } from "./json-stream.js";
import {
  jsonArgument,
  jsonFilter,
  streamJsonValue,
  type JsonAggregateHost,
  type JsonTransition,
} from "./json-values.js";
const INT8: PhysicalType = { kind: "int64" },
  INT4: PhysicalType = { kind: "int32", bits: 32 },
  TEXT: PhysicalType = { kind: "text" };
const KEY_TYPES: readonly PhysicalType[] = [
  TEXT,
  INT4,
  INT8,
  INT8,
  INT4,
  INT8,
  INT4,
  INT4,
  INT8,
  INT4,
];
const KEY_ORDER: readonly SortColumn[] = [
  { column: 1, direction: "asc", nulls: "last", collation: null },
  { column: 0, direction: "asc", nulls: "last", collation: "C" },
  { column: 2, direction: "asc", nulls: "last", collation: null },
];
const EQUAL: SortColumn = { column: 0, direction: "asc", nulls: "last", collation: "C" };
const desiredTypes = (c: AggregateCall): readonly PhysicalType[] => [
  ...c.args.map((a) => a.type),
  ...c.orderBy.map((k) => k.expr.type),
  INT8,
];
export function jsonObjectMinimum(call: AggregateCall): number {
  if (call.distinct)
    throw new UnsupportedPlanError("DISTINCT object aggregate", { construct: "aggregate" });
  return (
    (call.orderBy.length ? sortMinimumBytes(desiredTypes(call)) : 0) +
    (call.fn === "jsonb_object_agg" || call.json?.unique ? sortMinimumBytes(KEY_TYPES) : 0)
  );
}
export class JsonObjectTransition implements JsonTransition {
  readonly #desiredGrant: Grant | null;
  readonly #keyGrant: Grant | null;
  readonly #rows: Uint16Array;
  readonly #meta: FlatVector[];
  readonly #keyData: { layout: "varlen"; bytes: Uint8Array; offsets: Int32Array };
  readonly #previous: ColumnStore;
  #sort: SortOperator | null = null;
  #keySort: SortOperator | null = null;
  #log: JsonBlobs;
  #out: JsonStream;
  #seen = 0;
  #emitted = 0;
  #ordered = 0;
  #nullAt = Infinity;
  #duplicateAt = Infinity;
  #chosen: JsonBlob | null = null;
  #pending: Promise<void> | null = null;
  #keyPending: Promise<void> | null = null;
  #result: StreamedJsonField | null = null;
  #closed = false;
  #keyBytes = 0;
  constructor(
    readonly call: AggregateCall,
    readonly host: JsonAggregateHost,
    slot: number,
    keySlot: number,
  ) {
    jsonObjectMinimum(call);
    if (!host.pool.allocate(2 * BATCH_ROWS + 4096))
      throw new Error("JSON object metadata not admitted");
    this.#rows = new Uint16Array(BATCH_ROWS);
    this.#keyData = { layout: "varlen", bytes: new Uint8Array(0), offsets: new Int32Array(2) };
    this.#meta = KEY_TYPES.map((type, i) =>
      i === 0
        ? { form: "flat", type, length: 1, validity: null, data: this.#keyData }
        : {
            form: "flat",
            type,
            length: 1,
            validity: null,
            data:
              type.kind === "int64"
                ? { layout: "int64", values: { repr: "int64", values: new BigInt64Array(1) } }
                : { layout: "i32", values: new Int32Array(1) },
          },
    );
    this.#previous = new ColumnStore([TEXT], host.pool, { chunkRows: 1, targetBytes: Infinity });
    this.#desiredGrant = call.orderBy.length
      ? host.forkSort(sortMinimumBytes(desiredTypes(call)), slot)
      : null;
    this.#keyGrant =
      call.fn === "jsonb_object_agg" || call.json?.unique
        ? host.forkSort(sortMinimumBytes(KEY_TYPES), keySlot)
        : null;
    this.#log = new JsonBlobs(host.io, host.shared.spill);
    this.#out = this.#newOutput();
    this.#makeSorts();
  }
  #newOutput(): JsonStream {
    const out = new JsonStream(
      this.#log,
      this.host.slice,
      () => this.host.check(),
      (n) => this.host.checkValueLength(n),
    );
    out.put(123);
    if (this.call.fn === "json_object_agg") out.put(32);
    return out;
  }
  #makeSorts(): void {
    if (this.#desiredGrant) {
      const types = desiredTypes(this.call),
        sink: Operator = {
          push: (b) => {
            this.#pending = this.#consume(b);
            return "blocked";
          },
          ready: () => this.#pending ?? Promise.resolve(),
          finish: () => undefined,
          abort: () => undefined,
        };
      this.#sort = new SortOperator(
        {
          types,
          keys: [
            ...this.call.orderBy.map((k, i) => ({
              column: this.call.args.length + i,
              direction: k.direction,
              nulls: k.nulls,
              collation: k.collation,
            })),
            { column: types.length - 1, direction: "asc", nulls: "last", collation: null },
          ],
          limit: null,
          offset: 0,
          withTies: false,
        },
        this.host.sortContext(this.#desiredGrant),
        sink,
      );
    }
    if (this.#keyGrant) {
      const sink: Operator = {
        push: (b) => {
          this.#keyPending = this.#keys(b);
          return "blocked";
        },
        ready: () => this.#keyPending ?? Promise.resolve(),
        finish: () => undefined,
        abort: () => undefined,
      };
      this.#keySort = new SortOperator(
        { types: KEY_TYPES, keys: KEY_ORDER, limit: null, offset: 0, withTies: false },
        this.host.sortContext(this.#keyGrant),
        sink,
      );
    }
  }
  async add(
    batch: Batch,
    ids: Uint16Array,
    from: number,
    count: number,
    ordinal: Vector,
  ): Promise<void> {
    let n = 0;
    for (let i = from; i < from + count; i++) {
      const row = ids[i] ?? 0;
      if (jsonFilter(this.call, batch, row)) this.#rows[n++] = row;
    }
    if (!n) return;
    const columns = [
      ...this.call.args.map((_, i) => {
        const v = batch.columns[jsonArgument(this.call, i)];
        if (!v) throw new Error("Missing object argument");
        return v;
      }),
      ...this.call.orderBy.map((k) => {
        if (k.expr.op !== "column") throw new Error("Unprojected object order");
        const v = batch.columns[k.expr.index];
        if (!v) throw new Error("Missing object order");
        return v;
      }),
      ordinal,
    ];
    const input = { length: batch.length, columns, selection: { rows: this.#rows, count: n } };
    if (this.#sort) {
      if (this.#sort.push(input) === "blocked") await this.#sort.ready();
    } else await this.#consume(input);
  }
  async #consume(batch: Batch): Promise<void> {
    const key = batch.columns[0],
      value = batch.columns[1];
    if (!key || !value) throw new Error("Missing object pair");
    const n = batch.selection?.count ?? batch.length;
    for (let i = 0; i < n; i++) {
      await this.host.pace();
      const row = batch.selection?.rows[i] ?? i,
        seq = this.#ordered++;
      this.#seen++;
      // Stop converting at the first NULL key, but finish earlier unique-key validation.
      if (this.#nullAt !== Infinity) continue;
      if (!isValidSlot(key, row)) {
        this.#nullAt = seq;
        continue;
      }
      const raw = await this.#rawKey(key, row);
      try {
        const skip = this.call.json?.skipNulls === true && !isValidSlot(value, row);
        let blob: JsonBlob | null = null;
        if (this.call.fn === "jsonb_object_agg" && !skip) {
          const out = new JsonStream(this.#log, this.host.slice, () => this.host.check());
          try {
            await streamJsonValue(this.host, out, value, row, true);
            blob = await out.finish();
          } finally {
            out.close();
          }
        }
        if (this.call.fn === "json_object_agg" && !skip) {
          if (this.#emitted++) {
            this.#out.put(44);
            this.#out.put(32);
          }
          await this.#out.run(quotedJson(raw, this.#out));
          this.#out.put(32);
          this.#out.put(58);
          this.#out.put(32);
          await streamJsonValue(this.host, this.#out, value, row, false);
        }
        if (this.#keySort) {
          this.#keyData.bytes = raw;
          this.#keyData.offsets[1] = raw.length;
          const values = [
            raw.length,
            seq,
            blob?.head?.offset ?? 0,
            blob?.head?.length ?? 0,
            blob?.head?.checksum ?? 0,
            blob?.pages ?? 0,
            blob?.maximum ?? 0,
            blob?.bytes ?? 0,
            skip ? 1 : 0,
          ];
          for (let j = 1; j < this.#meta.length; j++) {
            const d = this.#meta[j]?.data,
              v = values[j - 1] ?? 0;
            if (d?.layout === "i32") d.values[0] = v;
            else if (d?.layout === "int64" && d.values.repr === "int64")
              d.values.values[0] = BigInt(v);
          }
          if (this.#keySort.push({ length: 1, columns: this.#meta, selection: null }) === "blocked")
            await this.#keySort.ready();
        }
      } finally {
        this.#keyData.bytes = new Uint8Array(0);
        this.#keyData.offsets[1] = 0;
        this.host.pool.free(this.#keyBytes);
        this.#keyBytes = 0;
      }
    }
  }
  async #rawKey(value: Vector, row: number): Promise<Uint8Array> {
    this.#keyBytes = 0;
    if (value.type.kind === "text" || value.type.kind === "enum") {
      const [flat, slot] = slotOf(value, row);
      if (flat.data.layout !== "varlen") throw new Error("Invalid object text key");
      return flat.data.bytes.subarray(
        flat.data.offsets[slot] ?? 0,
        flat.data.offsets[slot + 1] ?? 0,
      );
    }

    const count = new JsonOutput(null, this.host.slice);
    await runSliced(
      jsonValue(value, row, count, this.host.memory, this.host.slice, true, true),
      this.host.slice,
      () => this.host.check(),
    );
    await runSliced(this.host.memory.take(count.position), this.host.slice, () =>
      this.host.check(),
    );
    this.#keyBytes = count.position;
    const bytes = new Uint8Array(count.position);
    try {
      await runSliced(
        jsonValue(
          value,
          row,
          new JsonOutput(bytes, this.host.slice),
          this.host.memory,
          this.host.slice,
          true,
          true,
        ),
        this.host.slice,
        () => this.host.check(),
      );
      return bytes;
    } catch (error) {
      this.host.pool.free(bytes.length);
      throw error;
    }
  }
  #number(batch: Batch, column: number, row: number): number {
    const v = batch.columns[column];
    if (!v) throw new Error("Missing key descriptor");
    const [flat, slot] = slotOf(v, row),
      d = flat.data;
    return d.layout === "int64"
      ? Number(int64At(d.values, slot))
      : d.layout === "i32"
        ? (d.values[slot] ?? 0)
        : NaN;
  }
  async #keys(batch: Batch): Promise<void> {
    const key = batch.columns[0];
    if (!key) throw new Error("Missing sorted object key");
    for (let row = 0; row < batch.length; row++) {
      await this.host.pace();
      const seq = this.#number(batch, 2, row);
      const same =
        this.#previous.rows > 0 &&
        keysEqual([TEXT], [EQUAL], this.#previous.vectors(), 0, [key], row);
      if (same) this.#duplicateAt = Math.min(this.#duplicateAt, seq);
      else {
        await this.#emitChosen();
        await this.host.capture(this.#previous, [key], row);
        this.#chosen = null;
      }
      if (this.#number(batch, 9, row) === 0 && this.call.fn === "jsonb_object_agg") {
        const length = this.#number(batch, 4, row);
        this.#chosen = {
          head: length
            ? { offset: this.#number(batch, 3, row), length, checksum: this.#number(batch, 5, row) }
            : null,
          pages: this.#number(batch, 6, row),
          maximum: this.#number(batch, 7, row),
          bytes: this.#number(batch, 8, row),
        };
      }
    }
  }
  async #emitChosen(): Promise<void> {
    if (!this.#chosen) return;
    if (this.#emitted++) {
      this.#out.put(44);
      this.#out.put(32);
    }
    const key = this.#previous.vectors()[0];
    if (!key) throw new Error("Missing retained object key");
    await this.#out.run(jsonValue(key, 0, this.#out, this.host.memory, this.#out.slice));
    this.#out.put(58);
    this.#out.put(32);
    await this.#log.read(this.#chosen, (bytes) => this.#out.run(this.#out.copy(bytes)));
    this.#chosen = null;
  }
  async result(): Promise<StreamedJsonField> {
    if (this.#result) return this.#result;
    if (this.#sort) {
      await this.#sort.finish();
      await this.#sort.closed;
      if (this.#desiredGrant) this.host.resetSort(this.#desiredGrant);
    }
    if (this.#keySort) {
      await this.#keySort.finish();
      await this.#keySort.closed;
      if (this.#keyGrant) this.host.resetSort(this.#keyGrant);
      await this.#emitChosen();
    }
    if (this.call.json?.unique && this.#duplicateAt < this.#nullAt)
      throw new ExecutionError(
        "duplicate JSON object key value",
        "MINNOW_JSON_DUPLICATE_KEY",
        "22030",
      );
    if (this.#nullAt !== Infinity)
      throw new ExecutionError(
        this.call.fn === "jsonb_object_agg"
          ? "field name must not be null"
          : "null value not allowed for object key",
        "MINNOW_JSON_KEY_NULL",
        this.call.fn === "jsonb_object_agg" ? "22023" : "22004",
      );
    if (this.call.fn === "json_object_agg") this.#out.put(32);
    this.#out.put(125);
    const blob = await this.#out.finish(),
      log = this.#log,
      seen = this.#seen;
    return (this.#result = {
      stream: true,
      type: this.call.type,
      length: seen ? blob.bytes : null,
      read: (visit) => (seen ? log.read(blob, visit) : Promise.resolve()),
    });
  }
  async restart(): Promise<void> {
    this.#out.close();
    this.host.spilled(
      this.#log.files.written +
        (this.#sort?.profile.spilledBytes ?? 0) +
        (this.#keySort?.profile.spilledBytes ?? 0),
    );
    await this.#log.close();
    this.#previous.reset(true);
    this.#log = new JsonBlobs(this.host.io, this.host.shared.spill);
    this.#out = this.#newOutput();
    this.#sort = null;
    this.#keySort = null;
    this.#seen = 0;
    this.#emitted = 0;
    this.#ordered = 0;
    this.#nullAt = Infinity;
    this.#duplicateAt = Infinity;
    this.#chosen = null;
    this.#pending = null;
    this.#keyPending = null;
    this.#result = null;
    this.#makeSorts();
  }
  abort(reason: unknown): void {
    this.#sort?.abort(reason);
    this.#keySort?.abort(reason);
  }
  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    const results = await Promise.allSettled([
      this.#pending,
      this.#keyPending,
      this.#sort?.closed,
      this.#keySort?.closed,
    ]);
    this.host.spilled(
      this.#log.files.written +
        (this.#sort?.profile.spilledBytes ?? 0) +
        (this.#keySort?.profile.spilledBytes ?? 0),
    );
    this.#out.close();
    await this.#log.close();
    this.#previous.close();
    this.host.pool.free(2 * BATCH_ROWS + 4096);
    const failed = results.find((r): r is PromiseRejectedResult => r.status === "rejected");
    if (failed) throw failed.reason;
  }
}
