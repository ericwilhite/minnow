/** Resumable JSON kernels with prepaid workspace and borrowed variable-result lifetimes. */
import type { PhysicalType } from "../../types.js";
import type { Batch, FlatVector, Selection, Vector } from "../../vector/vector.js";
import { ExecutionError, UnsupportedPlanError } from "../errors.js";
import type { Slice, SlicedJob } from "../slices.js";
import { jsonValue, JSON_VALUE_KINDS } from "./json-value.js";
import { JsonScan } from "./json-scan.js";
import { JsonOutput } from "./json-output.js";
import { canonicalJsonb } from "./jsonb-canonical.js";
import {
  countIn,
  rowsIn,
  isValidSlot,
  slotOf,
  type Compiled,
  type OutputArena,
} from "./vectors.js";

const CONSTRUCTORS = new Set([
  "json_record",
  "json_record_value",
  "json_object",
  "json_build_object",
  "jsonb_build_object",
  "json_build_array",
  "jsonb_build_array",
  "json_array",
  "to_json",
  "to_jsonb",
]);

export interface JsonMemory {
  take(bytes: number): SlicedJob<void>;
  free(bytes: number): void;
}
export interface JsonHost {
  readonly slice: Slice;
  memory(node: JsonNode): JsonMemory;
  inputsConsumed?(node: JsonNode): void;
  evaluate(node: JsonNode, batch: Batch, rows: Selection | null): Vector;
}
const hosts = new WeakMap<OutputArena, JsonHost>();
export function installJsonHost(arena: OutputArena, host: JsonHost): void {
  hosts.set(arena, host);
}

export class JsonNode implements Compiled {
  readonly #output: FlatVector;
  readonly #data: { layout: "varlen"; bytes: Uint8Array; offsets: Int32Array };
  readonly #validity: Uint32Array;
  readonly #active: Uint8Array;
  readonly #memory: JsonMemory;
  readonly #raw: Array<Uint8Array | undefined>;
  #rawBytes = 0;
  #bytes = 0;
  #writing = false;
  constructor(
    readonly type: PhysicalType,
    readonly fn: string,
    readonly args: readonly Compiled[],
    readonly host: JsonHost,
    arena: OutputArena,
  ) {
    // Charge kernel wrappers, frame metadata and bounded cursor temporaries before allocation.
    const rawCapacity =
      type.kind === "json" && type.binary && CONSTRUCTORS.has(fn) ? arena.capacity : 0;
    arena.take(1024 + args.length * 32 + arena.capacity + 8 * rawCapacity + 4096);
    this.#raw = new Array<Uint8Array | undefined>(rawCapacity);
    this.#active = new Uint8Array(arena.capacity);
    const out = arena.flat(type);
    if (out.vector.data.layout !== "varlen") throw new TypeError("Expected variable JSON output");
    this.#data = { ...out.vector.data };
    this.#output = { ...out.vector, data: this.#data };
    this.#validity = out.validity;
    this.#memory = host.memory(this);
  }
  discard(job: SlicedJob<Vector>): void {
    job.return(this.#output);
  }
  evaluate(batch: Batch, rows: Selection | null): Vector {
    return this.host.evaluate(this, batch, rows);
  }
  reset(): void {
    if (this.#bytes + this.#rawBytes > 0) this.#memory.free(this.#bytes + this.#rawBytes);
    this.#bytes = 0;
    this.#rawBytes = 0;
    this.#raw.fill(undefined);
    this.#data.bytes = new Uint8Array(0);
  }
  *#validated(bytes: Uint8Array): SlicedJob<JsonScan> {
    const scan = new JsonScan(bytes, this.host.slice, this.fn !== "validate_json");
    if (this.#writing) return scan;
    const depth = yield* scan.depth();
    yield* this.#memory.take(depth);
    try {
      yield* scan.validate(new Uint8Array(depth));
    } finally {
      this.#memory.free(depth);
    }
    return scan;
  }
  *#value(value: Vector, row: number, writer: JsonOutput, key = false): SlicedJob<void> {
    // JSONB converts each argument before advancing to the next pair. Deferring all
    // conversion to the completed raw object would let a later bad key hide this value's error.
    if (
      !key &&
      this.type.kind === "json" &&
      this.type.binary &&
      value.type.kind === "json" &&
      !value.type.binary &&
      isValidSlot(value, row)
    )
      yield* canonicalJsonb(valueBytes(value, row), writer, this.#memory, this.host.slice);
    else yield* jsonValue(value, row, writer, this.#memory, this.host.slice, key);
  }
  *#construct(vectors: readonly Vector[], row: number, writer: JsonOutput): SlicedJob<boolean> {
    const first = vectors[0];
    if (this.fn === "json_record_value") {
      const spacing = vectors[1];
      if (!first || !spacing || !isValidSlot(first, row) || !isValidSlot(spacing, row))
        return false;
      const [flag, slot] = slotOf(spacing, row);
      if (flag.data.layout !== "bool") throw new Error("Invalid record spacing");
      const bytes = valueBytes(first, row);
      if (!flag.data.values[slot]) {
        yield* writer.copy(bytes);
        return true;
      }
      let depth = 0,
        string = false,
        escaped = false,
        start = 0;
      for (let i = 0; i < bytes.length; i++) {
        const c = bytes[i] ?? 0;
        if (string) {
          if (escaped) escaped = false;
          else if (c === 92) escaped = true;
          else if (c === 34) string = false;
        } else if (c === 34) string = true;
        else if (c === 123 || c === 91) depth++;
        else if (c === 125 || c === 93) depth--;
        else if (c === 44 && depth === 1) {
          yield* writer.copy(bytes.subarray(start, i + 1));
          writer.put(10);
          writer.put(32);
          start = i + 1;
        }
        if ((i & 1023) === 0 && this.host.slice.due()) yield;
      }
      yield* writer.copy(bytes.subarray(start));
      return true;
    }
    const record = this.fn === "json_record";
    let pretty = false;
    if (record) {
      const p = vectors[1];
      if (!first || !p || !isValidSlot(first, row) || !isValidSlot(p, row)) return false;
      const [presence, ps] = slotOf(first, row),
        [spacing, ss] = slotOf(p, row);
      if (presence.data.layout !== "bool" || spacing.data.layout !== "bool")
        throw new TypeError("Invalid record flags");
      if (!presence.data.values[ps]) return false;
      pretty = spacing.data.values[ss] !== 0;
    }
    if (this.fn === "to_json" || this.fn === "to_jsonb") {
      if (first === undefined || !isValidSlot(first, row)) return false;
      yield* this.#value(first, row, writer);
      return true;
    }
    const object =
      this.fn === "json_build_object" ||
      this.fn === "jsonb_build_object" ||
      this.fn === "json_object" ||
      record;
    if (object && vectors.length % 2 !== 0)
      throw new ExecutionError(
        "argument list must have even number of elements",
        "MINNOW_JSON_PAIRS",
        "22023",
      );
    writer.put(object ? 123 : 91);
    let emitted = false;
    for (let i = record ? 2 : 0; i < vectors.length; i += object ? 2 : 1) {
      const value = vectors[i];
      if (value === undefined) throw new TypeError("Missing JSON argument");
      if (this.fn === "json_array" && !isValidSlot(value, row)) continue;
      if (emitted) {
        writer.put(44);
        if (record) {
          if (pretty) {
            writer.put(10);
            writer.put(32);
          }
        } else writer.put(32);
      }
      emitted = true;
      if (object && this.type.kind === "json" && this.type.binary && !isValidSlot(value, row))
        throw new ExecutionError(
          `argument ${String(i + 1)}: key must not be null`,
          "MINNOW_JSON_KEY_NULL",
          "22023",
        );
      yield* this.#value(value, row, writer, object);
      if (object) {
        if (!record) writer.put(32);
        writer.put(58);
        if (!record) writer.put(32);
        const member = vectors[i + 1];
        if (member === undefined) throw new TypeError("Missing JSON member");
        yield* this.#value(member, row, writer);
      }
    }
    writer.put(object ? 125 : 93);
    return true;
  }
  *#row(vectors: readonly Vector[], row: number, writer: JsonOutput): SlicedJob<boolean> {
    const first = vectors[0];
    if (CONSTRUCTORS.has(this.fn)) {
      if (this.type.kind !== "json" || !this.type.binary)
        return yield* this.#construct(vectors, row, writer);
      if (!this.#writing) {
        const counter = new JsonOutput(null, this.host.slice);
        const valid = yield* this.#construct(vectors, row, counter);
        if (!valid) return false;
        yield* this.#memory.take(counter.position);
        this.#rawBytes += counter.position;
        const bytes = new Uint8Array(counter.position);
        this.#raw[row] = bytes;
        yield* this.#construct(vectors, row, new JsonOutput(bytes, this.host.slice));
      }
      const bytes = this.#raw[row];
      if (bytes === undefined) return false;
      yield* canonicalJsonb(bytes, writer, this.#memory, this.host.slice);
      if (this.#writing) {
        this.#memory.free(bytes.length);
        this.#rawBytes -= bytes.length;
        this.#raw[row] = undefined;
      }
      return true;
    }
    if (this.fn === "validate_jsonb") {
      if (first === undefined || !isValidSlot(first, row)) return false;
      yield* canonicalJsonb(valueBytes(first, row), writer, this.#memory, this.host.slice);
      return true;
    }
    if (this.fn === "validate_json") {
      if (first === undefined || !isValidSlot(first, row)) return false;
      yield* this.#validated(valueBytes(first, row));
      yield* writer.copy(valueBytes(first, row));
      return true;
    }
    if (this.fn === "json_get" || this.fn === "json_get_text") {
      const key = vectors[1];
      if (
        first === undefined ||
        key === undefined ||
        !isValidSlot(first, row) ||
        !isValidSlot(key, row)
      )
        return false;
      const bytes = valueBytes(first, row);
      const scan = yield* this.#validated(bytes);
      let selector: Uint8Array | number;
      if (key.type.kind === "text") selector = valueBytes(key, row);
      else {
        const [flat, slot] = slotOf(key, row);
        if (flat.data.layout !== "i32") throw new TypeError("JSON index must be int4");
        selector = flat.data.values[slot] ?? 0;
      }
      let span = yield* scan.field(selector);
      if (
        span === null &&
        first.type.kind === "json" &&
        first.type.binary &&
        typeof selector === "number" &&
        (selector === 0 || selector === -1)
      ) {
        scan.at = 0;
        yield* scan.space();
        if (bytes[scan.at] !== 123 && bytes[scan.at] !== 91) {
          const start = scan.at;
          yield* scan.valueEnd();
          span = { start, end: scan.at };
        }
      }
      if (span === null) return false;
      if (this.fn === "json_get_text") {
        if (bytes[span.start] === 110 && span.end - span.start === 4) return false;
        if (bytes[span.start] === 34) {
          yield* scan.stringBytes(span.start, span.end, writer);
          return true;
        }
      }
      if (first.type.kind === "json" && first.type.binary)
        yield* canonicalJsonb(
          bytes.subarray(span.start, span.end),
          writer,
          this.#memory,
          this.host.slice,
        );
      else yield* writer.copy(bytes.subarray(span.start, span.end));
      return true;
    }
    throw new TypeError("Unknown JSON kernel " + this.fn);
  }
  *run(batch: Batch, rows: Selection | null): SlicedJob<Vector> {
    this.reset();
    this.#writing = false;
    // Child JSON kernels suspend before this node owns any variable bytes.
    const vectors = this.args.map((arg) => arg.evaluate(batch, rows));
    this.#active.fill(0);
    const indexes = rowsIn(rows),
      count = countIn(rows, batch.length);
    for (let k = 0; k < count; k++) this.#active[indexes[k] ?? 0] = 1;
    this.#validity.fill(0);
    const counter = new JsonOutput(null, this.host.slice);
    for (let row = 0; row < batch.length; row++)
      if (this.#active[row] !== 0) {
        if (yield* this.#row(vectors, row, counter))
          this.#validity[row >>> 5] = (this.#validity[row >>> 5] ?? 0) | (1 << (row & 31));
      }
    // Binary constructors have captured their arguments once into a raw row. Their
    // children can now retire before final canonical output, avoiding a third full copy.
    if (this.type.kind === "json" && this.type.binary && this.#rawBytes > 0)
      this.host.inputsConsumed?.(this);
    if (counter.position > 0x7fff_ffff)
      throw new ExecutionError("JSON result is too large", "MINNOW_JSON_SIZE", "54000");
    yield* this.#memory.take(counter.position);
    this.#bytes = counter.position;
    this.#writing = true;
    const bytes = new Uint8Array(this.#bytes),
      writer = new JsonOutput(bytes, this.host.slice);
    const output = this.#data;
    output.bytes = bytes;
    output.offsets[0] = 0;
    for (let row = 0; row < batch.length; row++) {
      if (this.#active[row] !== 0) yield* this.#row(vectors, row, writer);
      output.offsets[row + 1] = writer.position;
    }
    this.#output.length = batch.length;
    this.#output.validity = this.#validity;
    return this.#output;
  }
}

export function compileJson(
  type: PhysicalType,
  fn: string,
  args: readonly Compiled[],
  arena: OutputArena,
): Compiled {
  if (CONSTRUCTORS.has(fn) && args.some((arg) => !JSON_VALUE_KINDS.has(arg.type.kind)))
    throw new UnsupportedPlanError("JSON construction value type", { construct: "function" });
  const host = hosts.get(arena);
  if (host === undefined)
    throw new UnsupportedPlanError("JSON expression outside a resumable projection/filter", {
      construct: "function",
    });
  return new JsonNode(type, fn, args, host, arena);
}

function valueBytes(vector: Vector, row: number): Uint8Array {
  const [flat, slot] = slotOf(vector, row);
  if (flat.data.layout !== "varlen") throw new TypeError("Expected JSON/text bytes");
  return flat.data.bytes.subarray(flat.data.offsets[slot] ?? 0, flat.data.offsets[slot + 1] ?? 0);
}
