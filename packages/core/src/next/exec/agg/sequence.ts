import type { JsonGroupKeys } from "./json-group-keys.js";
/** PostgreSQL string_agg / array_agg, serialized into bounded charged fragments.
 * Growing output is never a resident accumulator: the common aggregate phase
 * measures its final row and transfers it through result-owned delivery. */
import type { AggregateCall } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import { arrayElementTag, MAX_ARRAY_DIMENSIONS } from "../../vector/array-value.js";
import { BATCH_ROWS, type Batch, type Vector } from "../../vector/vector.js";
import {
  ArrayElementBytes,
  arrayElementScratch,
  arrayFunctionBounds,
  arrayHeader,
  nativeArray,
  valueBytes,
} from "../array-data.js";
import { ColumnStore } from "../column-store.js";
import { ExecutionError } from "../errors.js";
import type { Grant } from "../../memory/governor.js";
import type { StreamedJsonField } from "../json-records.js";
import type { Operator } from "../pipeline.js";
import type { SortColumn } from "../sort-keys.js";
import { SortOperator, sortMinimumBytes } from "../sort.js";
import { isValidSlot } from "../kernels/vectors.js";
import { JsonBlobs } from "./json-blobs.js";
import { JsonStream } from "./json-stream.js";
import {
  jsonArgument,
  jsonFilter,
  type JsonAggregateHost,
  type JsonTransition,
} from "./json-values.js";
export const isSequenceAggregate = (call: AggregateCall) =>
  call.fn === "string_agg" || call.fn === "array_agg";
export const sequenceSortTypes = (call: AggregateCall): readonly PhysicalType[] => [
  ...call.args.map((a) => a.type),
  ...call.orderBy.map((k) => k.expr.type),
  { kind: "int64" },
];
export class SequenceTransition implements JsonTransition {
  readonly #rows: Uint16Array;
  readonly #previous: ColumnStore;
  readonly #grant: Grant | null;
  readonly #types: readonly PhysicalType[];
  readonly #keys: readonly SortColumn[];
  readonly #distinct: readonly SortColumn[];
  readonly #equal: JsonGroupKeys | null;
  readonly #element: PhysicalType | null;
  #log: JsonBlobs;
  #bits: JsonBlobs;
  #out: JsonStream;
  #bitmap: JsonStream;
  #sort: SortOperator | null = null;
  #pending: Promise<void> | null = null;
  #result: StreamedJsonField | null = null;
  #seen = 0;
  #count = 0;
  #nulls = false;
  #bit = 0;
  #bitCount = 0;
  #shape: readonly number[] = [];
  #lower: readonly number[] = [];
  #closed = false;
  constructor(
    readonly call: AggregateCall,
    readonly host: JsonAggregateHost,
    slot: number,
  ) {
    if (!host.pool.allocate(2 * BATCH_ROWS + 8192))
      throw new Error("Sequence metadata not admitted");
    this.#rows = new Uint16Array(BATCH_ROWS);
    this.#types = sequenceSortTypes(call);
    const n = call.args.length;
    this.#distinct = call.args.map((_, column) => ({
      column,
      direction: "asc",
      nulls: "last",
      collation: null,
    }));
    this.#equal = call.distinct
      ? host.groupKeys(
          call.args.map((a) => a.type),
          this.#distinct,
        )
      : null;
    const ordered = call.orderBy.map((k, i): SortColumn => ({
      column: call.distinct
        ? call.args.findIndex(
            (a) => a.op === "column" && k.expr.op === "column" && a.index === k.expr.index,
          )
        : n + i,
      direction: k.direction,
      nulls: k.nulls,
      collation: k.collation,
    }));
    if (ordered.some((k) => k.column < 0))
      throw new ExecutionError(
        "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list",
        "MINNOW_AGG_ORDER",
        "42P10",
      );
    this.#keys = [
      ...ordered,
      ...(call.distinct
        ? this.#distinct.filter((k) => !ordered.some((o) => o.column === k.column))
        : []),
      { column: this.#types.length - 1, direction: "asc", nulls: "last", collation: null },
    ];
    this.#previous = new ColumnStore(
      call.args.map((a) => a.type),
      host.pool,
      { chunkRows: 1, targetBytes: Infinity },
    );
    this.#grant =
      call.distinct || call.orderBy.length
        ? host.forkSort(sortMinimumBytes(this.#types), slot)
        : null;
    this.#element = call.type.kind === "array" ? call.type.element : null;
    if (this.#element) arrayElementTag(this.#element);
    this.#log = new JsonBlobs(host.io, host.shared.spill);
    this.#bits = new JsonBlobs(host.io, host.shared.spill);
    this.#out = this.#stream(this.#log);
    this.#bitmap = this.#stream(this.#bits, false);
    this.#makeSort();
  }
  #stream(log: JsonBlobs, check = true): JsonStream {
    return new JsonStream(
      log,
      this.host.slice,
      () => this.host.check(),
      check ? (n) => this.host.checkValueLength(n, 1) : undefined,
    );
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
      { types: this.#types, keys: this.#keys, limit: null, offset: 0, withTies: false },
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
    const args = this.call.args.map((_, i) => {
      const v = batch.columns[jsonArgument(this.call, i)];
      if (!v) throw new Error("Missing sequence argument");
      return v;
    });
    const first = args[0];
    if (!first) throw new Error("Missing sequence value");
    let n = 0;
    for (let k = from; k < from + count; k++) {
      const row = ids[k] ?? 0;
      if (
        !jsonFilter(this.call, batch, row) ||
        (this.call.fn === "string_agg" && !isValidSlot(first, row))
      )
        continue;
      this.#rows[n++] = row;
    }
    if (!n) return;
    if (this.#sort) {
      const columns = [
        ...args,
        ...this.call.orderBy.map((k) => {
          if (k.expr.op !== "column") throw new Error("Unprojected sequence order");
          const v = batch.columns[k.expr.index];
          if (!v) throw new Error("Missing sequence order");
          return v;
        }),
        ordinal,
      ];
      if (
        this.#sort.push({
          length: batch.length,
          columns,
          selection: { rows: this.#rows, count: n },
        }) === "blocked"
      )
        await this.#sort.ready();
    } else
      for (let k = 0; k < n; k++) {
        await this.host.pace();
        await this.#append(args, this.#rows[k] ?? 0);
      }
  }
  async #consume(batch: Batch): Promise<void> {
    const args = batch.columns.slice(0, this.call.args.length),
      n = batch.selection?.count ?? batch.length;
    for (let k = 0; k < n; k++) {
      await this.host.pace();
      const row = batch.selection?.rows[k] ?? k;
      if (this.call.distinct) {
        if (
          this.#previous.rows &&
          (await this.#equal?.equal(this.#previous.vectors(), 0, args, row))
        )
          continue;
        await this.host.capture(this.#previous, args, row);
      }
      await this.#append(args, row);
    }
  }
  async #nullBit(nullValue: boolean): Promise<void> {
    if (nullValue) {
      this.#nulls = true;
      this.#bit |= 1 << this.#bitCount;
    }
    this.#count++;
    this.#bitCount++;
    if (this.#bitCount === 8) {
      this.#bitmap.put(this.#bit);
      this.#bit = 0;
      this.#bitCount = 0;
      if (this.#bitmap.pending) await this.#bitmap.flush();
    }
    if (this.#count > 0x7fffffff)
      throw new ExecutionError("array size exceeds maximum", "MINNOW_ARRAY_SIZE", "54000");
  }
  async #append(args: readonly Vector[], row: number): Promise<void> {
    const value = args[0];
    if (!value) throw new Error("Sequence value missing");
    this.host.check();
    if (this.call.fn === "string_agg") {
      if (!isValidSlot(value, row)) return;
      const delimiter = args[1];
      if (this.#seen && delimiter && isValidSlot(delimiter, row))
        await this.#out.run(this.#out.copy(valueBytes(delimiter, row)));
      await this.#out.run(this.#out.copy(valueBytes(value, row)));
      this.#seen++;
      return;
    }
    if (!this.#element) throw new Error("Missing array element type");
    this.#seen++;
    if (value.type.kind === "array") {
      if (!isValidSlot(value, row))
        throw new ExecutionError("cannot accumulate null arrays", "MINNOW_ARRAY_NULL", "22004");
      const array = nativeArray(this.#element, valueBytes(value, row));
      if (!array.count)
        throw new ExecutionError("cannot accumulate empty arrays", "MINNOW_ARRAY_EMPTY", "2202E");
      if (array.dimensions.length >= MAX_ARRAY_DIMENSIONS)
        throw new ExecutionError(
          "number of array dimensions exceeds the maximum allowed",
          "MINNOW_ARRAY_SIZE",
          "54000",
        );
      if (this.#seen === 1) {
        this.#shape = [...array.dimensions];
        this.#lower = [...array.lowerBounds];
      } else if (
        this.#shape.length !== array.dimensions.length ||
        this.#shape.some(
          (n, i) => n !== array.dimensions[i] || this.#lower[i] !== array.lowerBounds[i],
        )
      )
        throw new ExecutionError(
          "cannot accumulate arrays of different dimensionality",
          "MINNOW_ARRAY_DIMENSIONS",
          "2202E",
        );
      for (let i = 0; i < array.count; i++) {
        await this.#nullBit(
          array.nulls !== null && ((array.nulls[i >>> 3] ?? 0) & (1 << (i & 7))) !== 0,
        );
        if ((i & 1023) === 0) await this.host.pace();
      }
      await this.#out.run(this.#out.copy(array.payload));
    } else {
      const valid = isValidSlot(value, row);
      await this.#nullBit(!valid);
      if (valid) {
        // The scalar codec owns only one element's temporary representation.
        // Large text/byte fields remain borrowed views and stream in fragments.
        await this.#out.run(this.host.memory.take(arrayElementScratch(value, row)));
        const bytes = new ArrayElementBytes(value, row, this.host.pool, true);
        try {
          for (const part of bytes.parts) await this.#out.run(this.#out.copy(part));
        } finally {
          bytes.close();
        }
      }
    }
    this.host.checkValueLength(
      this.#out.position +
        (this.#nulls ? Math.ceil(this.#count / 8) : 0) +
        4 +
        8 * (this.#shape.length + 1),
      1,
    );
  }
  async result(): Promise<StreamedJsonField> {
    if (this.#result) return this.#result;
    if (this.#sort) {
      await this.#sort.finish();
      await this.#sort.closed;
      if (this.#grant) this.host.resetSort(this.#grant);
    }
    if (this.#bitCount) this.#bitmap.put(this.#bit);
    const payload = await this.#out.finish(),
      bitmap = await this.#bitmap.finish(),
      log = this.#log,
      bits = this.#bits;
    if (this.#element && this.#seen)
      arrayFunctionBounds([this.#seen, ...this.#shape], [1, ...this.#lower]);
    const header =
      this.#element && this.#seen
        ? arrayHeader(this.#element, [this.#seen, ...this.#shape], [1, ...this.#lower], this.#nulls)
        : null;
    const length = this.#seen
      ? payload.bytes + (header?.length ?? 0) + (this.#nulls ? bitmap.bytes : 0)
      : null;
    if (length !== null) this.host.checkValueLength(length, 1);
    const nulls = this.#nulls;
    return (this.#result = {
      stream: true,
      type: this.call.type,
      length,
      read: async (visit) => {
        if (length === null) return;
        if (header) await visit(header);
        if (nulls) await bits.read(bitmap, visit);
        await log.read(payload, visit);
      },
    });
  }
  async restart(): Promise<void> {
    await this.#retire();
    this.#previous.reset(true);
    this.#log = new JsonBlobs(this.host.io, this.host.shared.spill);
    this.#bits = new JsonBlobs(this.host.io, this.host.shared.spill);
    this.#out = this.#stream(this.#log);
    this.#bitmap = this.#stream(this.#bits, false);
    this.#seen = 0;
    this.#count = 0;
    this.#nulls = false;
    this.#bit = 0;
    this.#bitCount = 0;
    this.#shape = [];
    this.#lower = [];
    this.#result = null;
    this.#pending = null;
    this.#makeSort();
  }
  abort(reason: unknown): void {
    this.#sort?.abort(reason);
  }
  async #retire(): Promise<void> {
    this.#out.close();
    this.#bitmap.close();
    this.host.spilled(
      this.#log.files.written + this.#bits.files.written + (this.#sort?.profile.spilledBytes ?? 0),
    );
    await this.#log.close();
    await this.#bits.close();
  }
  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    const settled = await Promise.allSettled([this.#pending, this.#sort?.closed]);
    await this.#retire();
    this.#previous.close();
    this.host.pool.free(2 * BATCH_ROWS + 8192);
    const failed = settled.find((s): s is PromiseRejectedResult => s.status === "rejected");
    if (failed) throw failed.reason;
  }
}
