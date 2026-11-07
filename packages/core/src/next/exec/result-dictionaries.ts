/** Per-stream UTF-8 dictionary identities. Only scalar ids survive input readiness. */
import { OBJECT_OVERHEAD_BYTES, type Grant } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector, Vector } from "../vector/vector.js";
import { STREAM_DICTIONARY_BYTES, type DictionaryDelta, type WireVector } from "./result.js";

const CODE_TYPE: PhysicalType = { kind: "int32", bits: 32 };
const ENTRY_BYTES = 4 * OBJECT_OVERHEAD_BYTES;
interface Entry {
  readonly source: FlatVector;
  readonly id: number;
  readonly bytes: number;
}
export interface DictionaryPlan {
  readonly columns: readonly Vector[];
  readonly types: readonly PhysicalType[];
  readonly ids: ReadonlyArray<number | null>;
  readonly fresh: readonly Entry[];
}
export interface DictionaryFrame {
  dictionaries: DictionaryDelta[];
  extraBytes: number;
}

export class ResultDictionaries {
  #ids = new WeakMap<FlatVector, number>();
  #next = 0;
  #sent = 0;
  #registry = 0;
  constructor(readonly grant: Grant) {}

  plan(types: readonly PhysicalType[], columns: readonly Vector[]): DictionaryPlan {
    const fresh = new Map<FlatVector, Entry>();
    let bytes = 0;
    const ids = columns.map((vector) => {
      if (
        vector.form !== "dictionary" ||
        vector.type.kind !== "text" ||
        vector.dictionary.data.layout !== "varlen"
      )
        return null;
      const source = vector.dictionary;
      const old = this.#ids.get(source) ?? fresh.get(source)?.id;
      if (old !== undefined) return old;
      const data = source.data;
      if (data.layout !== "varlen") return null;
      const size =
        (data.offsets[source.length] ?? 0) - (data.offsets[0] ?? 0) + 4 * (source.length + 1);
      if (this.#sent + this.#registry + bytes + size + ENTRY_BYTES > STREAM_DICTIONARY_BYTES)
        return null;
      // Stream identities outlive individual frames. Reserve them without a progress loan;
      // otherwise the first frame could not repay its loan while this registry remains live.
      if (!this.grant.allocate(ENTRY_BYTES)) return null;
      this.#registry += ENTRY_BYTES;
      const entry = { source, id: this.#next + fresh.size, bytes: size };
      fresh.set(source, entry);
      bytes += size;
      return entry.id;
    });
    return {
      ids,
      fresh: [...fresh.values()],
      types: types.map((type, c) => (ids[c] === null ? type : CODE_TYPE)),
      columns: columns.map((vector, c) => {
        if (ids[c] === null || vector.form !== "dictionary") return vector;
        return {
          form: "flat",
          type: CODE_TYPE,
          length: vector.length,
          validity: vector.validity,
          data: { layout: "i32", values: vector.codes },
        };
      }),
    };
  }

  needed(plan: DictionaryPlan): number {
    return plan.fresh.reduce((n, e) => n + (this.#ids.has(e.source) ? 0 : e.bytes), 0);
  }

  stage(plan: DictionaryPlan, frame: DictionaryFrame): boolean {
    const needed = this.needed(plan);
    if (!this.grant.allocate(needed)) return false;
    // Set charges before allocation, so the caller's failure cleanup covers a thrown copy.
    frame.extraBytes = needed;
    for (const entry of plan.fresh) {
      if (this.#ids.has(entry.source)) continue;
      const { source, id } = entry,
        data = source.data;
      if (data.layout !== "varlen") throw new Error("Text dictionary changed layout");
      const base = data.offsets[0] ?? 0;
      const offsets = Int32Array.from(data.offsets.subarray(0, source.length + 1), (n) => n - base);
      frame.dictionaries.push({
        id,
        from: 0,
        values: {
          form: "flat",
          length: source.length,
          validity: null,
          data: {
            layout: "varlen",
            offsets,
            bytes: data.bytes.slice(base, data.offsets[source.length] ?? base),
          },
        },
      });
    }
    return true;
  }

  publish(plan: DictionaryPlan, frame: DictionaryFrame): void {
    if (frame.dictionaries.length === 0) return;
    for (const entry of plan.fresh) {
      if (this.#ids.has(entry.source)) continue;
      this.#ids.set(entry.source, entry.id);
      this.#sent += entry.bytes;
      this.#next++;
    }
  }

  wire(plan: DictionaryPlan, column: WireVector, c: number): WireVector {
    const id = plan.ids[c];
    if (id === null || id === undefined) return column;
    if (column.form !== "flat" || column.data.layout !== "i32")
      throw new Error("Result dictionary codes changed layout");
    return {
      form: "dictionary",
      length: column.length,
      validity: column.validity,
      codes: column.data.values,
      dictionary: id,
    };
  }

  abandon(plan: DictionaryPlan): void {
    for (const entry of plan.fresh) {
      if (this.#ids.has(entry.source)) continue;
      this.grant.free(ENTRY_BYTES);
      this.#registry -= ENTRY_BYTES;
    }
  }

  close(): void {
    this.grant.free(this.#registry);
    this.#registry = 0;
    this.#ids = new WeakMap();
  }
}
