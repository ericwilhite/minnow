/** Resumable JSON kernels with prepaid workspace and borrowed variable-result lifetimes. */
import type { PhysicalType } from "../../types.js";
import type { Batch, FlatVector, Selection, Vector } from "../../vector/vector.js";
import { ExecutionError, UnsupportedPlanError } from "../errors.js";
import type { Slice, SlicedJob } from "../slices.js";
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

export interface JsonMemory {
  take(bytes: number): SlicedJob<void>;
  free(bytes: number): void;
}
export interface JsonHost {
  readonly slice: Slice;
  memory(node: JsonNode): JsonMemory;
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
    arena.take(1024 + args.length * 32 + arena.capacity + 4096);
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
    if (this.#bytes > 0) this.#memory.free(this.#bytes);
    this.#bytes = 0;
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
  *#row(vectors: readonly Vector[], row: number, writer: JsonOutput): SlicedJob<boolean> {
    const first = vectors[0];
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
