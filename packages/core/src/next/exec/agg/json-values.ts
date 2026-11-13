/** JSON transitions consume typed values; only bounded fragments are resident. */
import type { AggregateCall } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import type { Grant } from "../../memory/governor.js";
import type { Batch, Vector } from "../../vector/vector.js";
import { BATCH_ROWS } from "../../vector/vector.js";
import type { JsonPhaseContext } from "../json-phase.js";
import type { StreamedJsonField } from "../json-records.js";
import type { Pool, SpillIo } from "../spill-pool.js";
import type { JsonMemory } from "../kernels/json.js";
import { jsonValue } from "../kernels/json-value.js";
import { jsonFloatText } from "../kernels/json-float.js";
import { jsonbNumber } from "../kernels/jsonb-number.js";
import { canonicalJsonb } from "../kernels/jsonb-canonical.js";
import { isValidSlot, slotOf } from "../kernels/vectors.js";
import { ColumnStore } from "../column-store.js";
import { SortOperator, sortMinimumBytes, type SortContext } from "../sort.js";
import { keysEqual, type SortColumn } from "../sort-keys.js";
import type { Slice } from "../slices.js";
import type { Operator } from "../pipeline.js";
import { JsonBlobs } from "./json-blobs.js";
import { JsonStream } from "./json-stream.js";

const INT8: PhysicalType = { kind: "int64" };
const utf8 = new TextEncoder();
export interface JsonAggregateHost {
  readonly pool: Pool;
  readonly io: SpillIo;
  readonly shared: JsonPhaseContext;
  readonly slice: Slice;
  readonly memory: JsonMemory;
  check(): void;
  pace(): Promise<void>;
  capture(store: ColumnStore, vectors: readonly Vector[], row: number): Promise<void>;
  forkSort(minimum: number, slot: number): Grant;
  sortContext(grant: Grant): SortContext;
  resetSort(grant: Grant): void;
  checkValueLength(length: number): void;
  spilled(bytes: number): void;
}
export interface JsonTransition {
  add(batch: Batch, ids: Uint16Array, from: number, count: number, ordinal: Vector): Promise<void>;
  result(): Promise<StreamedJsonField | Vector>;
  restart(): Promise<void>;
  abort(reason: unknown): void;
  close(): Promise<void>;
}
export function jsonArgument(call: AggregateCall, index: number): number {
  const arg = call.args[index];
  if (arg?.op !== "column") throw new Error("JSON arguments must be projected");
  return arg.index;
}
export function jsonFilter(call: AggregateCall, batch: Batch, row: number): boolean {
  if (!call.filter) return true;
  if (call.filter.op !== "column") throw new Error("JSON FILTER must be projected");
  const v = batch.columns[call.filter.index];
  if (!v || !isValidSlot(v, row)) return false;
  const [flat, slot] = slotOf(v, row);
  if (flat.data.layout !== "bool") throw new Error("Invalid JSON FILTER");
  return flat.data.values[slot] !== 0;
}
/** Numeric JSONB uses numeric_in(float*out), not the precision-losing float::numeric cast. */
export async function streamJsonValue(
  host: JsonAggregateHost,
  out: JsonStream,
  value: Vector,
  row: number,
  binary: boolean,
): Promise<void> {
  if (isValidSlot(value, row) && value.type.kind === "json" && (binary || value.type.binary)) {
    const [flat, slot] = slotOf(value, row);
    if (flat.data.layout !== "varlen") throw new Error("Invalid JSON bytes");
    await out.run(
      canonicalJsonb(
        flat.data.bytes.subarray(flat.data.offsets[slot] ?? 0, flat.data.offsets[slot + 1] ?? 0),
        out,
        host.memory,
        out.slice,
      ),
    );
  } else if (
    binary &&
    isValidSlot(value, row) &&
    (value.type.kind === "float32" || value.type.kind === "float64")
  ) {
    const [flat, slot] = slotOf(value, row);
    if (flat.data.layout !== "f32" && flat.data.layout !== "f64")
      throw new Error("Invalid JSON float");
    const n = flat.data.values[slot] ?? 0;
    if (Number.isFinite(n)) {
      const bytes = utf8.encode(jsonFloatText(n, value.type.kind === "float32"));
      await out.run(jsonbNumber(bytes, 0, bytes.length, out, out.slice));
    } else await out.run(jsonValue(value, row, out, host.memory, out.slice));
  } else await out.run(jsonValue(value, row, out, host.memory, out.slice));
}
export function jsonArraySortTypes(call: AggregateCall): readonly PhysicalType[] {
  return [call.args[0]?.type ?? { kind: "bool" }, ...call.orderBy.map((k) => k.expr.type), INT8];
}
export class JsonArrayTransition implements JsonTransition {
  readonly #rows: Uint16Array;
  readonly #previous: ColumnStore;
  readonly #grant: Grant | null;
  readonly #sortTypes: readonly PhysicalType[];
  readonly #keys: readonly SortColumn[];
  #sort: SortOperator | null = null;
  #log: JsonBlobs;
  #out: JsonStream;
  #seen = 0;
  #emitted = 0;
  #result: StreamedJsonField | null = null;
  #pending: Promise<void> | null = null;
  #closed = false;
  constructor(
    readonly call: AggregateCall,
    readonly host: JsonAggregateHost,
    slot: number,
  ) {
    if (!host.pool.allocate(2 * BATCH_ROWS + 4096))
      throw new Error("JSON array metadata not admitted");
    this.#rows = new Uint16Array(BATCH_ROWS);
    this.#previous = new ColumnStore([call.args[0]?.type ?? { kind: "bool" }], host.pool, {
      chunkRows: 1,
      targetBytes: Infinity,
    });
    this.#sortTypes = jsonArraySortTypes(call);
    this.#keys = call.distinct
      ? [
          {
            column: 0,
            direction: call.orderBy[0]?.direction ?? "asc",
            nulls: call.orderBy[0]?.nulls ?? "last",
            collation: call.orderBy[0]?.collation ?? null,
          },
        ]
      : call.orderBy.map((k, i) => ({
          column: i + 1,
          direction: k.direction,
          nulls: k.nulls,
          collation: k.collation,
        }));
    this.#keys = [
      ...this.#keys,
      { column: this.#sortTypes.length - 1, direction: "asc", nulls: "last", collation: null },
    ];
    this.#grant =
      call.distinct || call.orderBy.length
        ? host.forkSort(sortMinimumBytes(this.#sortTypes), slot)
        : null;
    this.#log = new JsonBlobs(host.io, host.shared.spill);
    this.#out = new JsonStream(
      this.#log,
      host.slice,
      () => host.check(),
      (n) => host.checkValueLength(n),
    );
    this.#out.put(91);
    this.#makeSort();
  }
  #makeSort(): void {
    if (!this.#grant) return;
    const sink: Operator = {
      push: (batch) => {
        this.#pending = this.#consume(batch);
        return "blocked";
      },
      ready: () => this.#pending ?? Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    };
    this.#sort = new SortOperator(
      { types: this.#sortTypes, keys: this.#keys, limit: null, offset: 0, withTies: false },
      this.host.sortContext(this.#grant),
      sink,
    );
  }
  async add(
    batch: Batch,
    ids: Uint16Array,
    from: number,
    count: number,
    ordinal: Vector,
  ): Promise<void> {
    const value = batch.columns[jsonArgument(this.call, 0)];
    if (!value) throw new Error("Missing JSON value");
    let selected = 0;
    for (let at = from; at < from + count; at++) {
      const row = ids[at] ?? 0;
      if (!jsonFilter(this.call, batch, row)) continue;
      this.#seen++;
      if (this.call.json?.skipNulls && !isValidSlot(value, row)) continue;
      this.#rows[selected++] = row;
    }
    if (!selected) return;
    if (this.#sort) {
      const columns = [
        value,
        ...this.call.orderBy.map((k) => {
          if (k.expr.op !== "column") throw new Error("JSON ORDER BY not projected");
          const v = batch.columns[k.expr.index];
          if (!v) throw new Error("Missing JSON order key");
          return v;
        }),
        ordinal,
      ];
      if (
        this.#sort.push({
          length: batch.length,
          columns,
          selection: { rows: this.#rows, count: selected },
        }) === "blocked"
      )
        await this.#sort.ready();
    } else for (let at = 0; at < selected; at++) await this.#append(value, this.#rows[at] ?? 0);
  }
  async #consume(batch: Batch): Promise<void> {
    const value = batch.columns[0];
    if (!value) throw new Error("Missing sorted JSON value");
    const key: SortColumn = { column: 0, direction: "asc", nulls: "last", collation: null };
    for (let row = 0; row < batch.length; row++) {
      await this.host.pace();
      if (this.call.distinct) {
        if (
          this.#previous.rows &&
          keysEqual([value.type], [key], this.#previous.vectors(), 0, [value], row)
        )
          continue;
        await this.host.capture(this.#previous, [value], row);
      }
      await this.#append(value, row);
    }
  }
  async #append(value: Vector, row: number): Promise<void> {
    this.host.check();
    if (this.#emitted++) {
      this.#out.put(44);
      this.#out.put(32);
      if (this.call.json?.record && this.call.fn === "json_agg" && isValidSlot(value, row)) {
        this.#out.put(10);
        this.#out.put(32);
      }
    }
    await streamJsonValue(this.host, this.#out, value, row, this.call.fn === "jsonb_agg");
  }
  async result(): Promise<StreamedJsonField> {
    if (this.#result) return this.#result;
    if (this.#sort) {
      await this.#sort.finish();
      await this.#sort.closed;
      if (this.#grant) this.host.resetSort(this.#grant);
    }
    this.#out.put(93);
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
    this.host.spilled(this.#log.files.written + (this.#sort?.profile.spilledBytes ?? 0));
    await this.#log.close();
    this.#previous.reset(true);
    this.#log = new JsonBlobs(this.host.io, this.host.shared.spill);
    this.#out = new JsonStream(
      this.#log,
      this.host.slice,
      () => this.host.check(),
      (n) => this.host.checkValueLength(n),
    );
    this.#out.put(91);
    this.#seen = 0;
    this.#emitted = 0;
    this.#result = null;
    this.#pending = null;
    this.#makeSort();
  }
  abort(reason: unknown): void {
    this.#sort?.abort(reason);
  }
  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    const results = await Promise.allSettled([this.#pending, this.#sort?.closed]);
    this.host.spilled(this.#log.files.written + (this.#sort?.profile.spilledBytes ?? 0));
    this.#out.close();
    await this.#log.close();
    this.#previous.close();
    this.host.pool.free(2 * BATCH_ROWS + 4096);
    const failed = results.find((r): r is PromiseRejectedResult => r.status === "rejected");
    if (failed) throw failed.reason;
  }
}
