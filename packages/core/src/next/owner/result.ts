/** Canonical client Result over transferred frames. Nothing decodes until requested. */
import type { ColumnView, Result, Row, Value } from "./api.js";
import { resultColumn } from "./api.js";
import { StreamDictionaries, type FramePayload, type WireVector, type WireFlat } from "./frame.js";
import { ProtocolError, type WireColumnMeta, type StatementSummary } from "./protocol.js";
import { scalarValue, required } from "./result-values.js";
import { columnView, materialize, valid, type Slot } from "./result-columns.js";
export interface ResultInput {
  readonly columns: readonly WireColumnMeta[];
  readonly frames: readonly FramePayload[];
  readonly summary: StatementSummary;
  /** Already-applied stream dictionary state (cursor chunks may start after sequence zero). */
  readonly dictionaries?: StreamDictionaries;
}
export function decodeResult<TRow = Row>(input: ResultInput): Result<TRow> {
  return new NativeResult<TRow>(input);
}
const NULL_SLOT: Slot = {
  flat: {
    form: "flat",
    length: 1,
    validity: Uint32Array.of(0),
    data: { layout: "bool", values: new Uint8Array(0) },
  },
  row: 0,
};
class NativeResult<TRow> implements Result<TRow> {
  readonly columns;
  readonly command;
  readonly affectedRows;
  readonly replayed;
  readonly rowCount;
  readonly #frames;
  readonly #meta;
  readonly #dictionaries;
  readonly #ends: number[] = [];
  readonly #names = new Map<string, number>();
  readonly #views = new Map<number, ColumnView>();
  readonly #chunks = new Map<number, readonly ColumnView[]>();
  readonly #values = new Map<number, Value[]>();
  readonly #rows = new Map<number, TRow>();
  readonly #dictionaryViews = new Map<number, WireFlat>();
  #allRows: TRow[] | null = null;
  #tuples: Value[][] | null = null;
  constructor(input: ResultInput) {
    if (input.columns.some((column) => column.type.kind === "locator"))
      throw new ProtocolError("A row locator is internal and never a result column");
    this.#frames = input.frames;
    this.#meta = input.columns;
    this.columns = input.columns.map(resultColumn);
    this.command = input.summary.command;
    this.replayed = input.summary.replayed;
    this.affectedRows = /^(?:INSERT|UPDATE|DELETE|MERGE)$/u.test(this.command)
      ? input.summary.rowCount
      : null;
    this.#dictionaries = input.dictionaries ?? new StreamDictionaries();
    let count = 0;
    for (const frame of this.#frames) {
      if (input.dictionaries === undefined) this.#dictionaries.apply(frame);
      if (
        frame.columns.length !== this.columns.length ||
        frame.columns.some((c) => c.length !== frame.rowCount)
      )
        throw new ProtocolError("Result frame disagrees with its column metadata");
      count += frame.rowCount;
      this.#ends.push(count);
    }
    this.rowCount = count;
    input.columns.forEach((column, i) => this.#names.set(column.name, i));
  }
  #index(column: string | number): number {
    const index = typeof column === "string" ? this.#names.get(column) : column;
    if (
      index === undefined ||
      !Number.isInteger(index) ||
      index < 0 ||
      index >= this.columns.length
    )
      throw new RangeError(`Unknown result column ${String(column)}`);
    return index;
  }
  #slot(column: WireVector, row: number): Slot {
    if (column.form === "constant") return { flat: column.value, row: 0 };
    if (column.form === "flat") return { flat: column, row };
    if (
      column.validity !== null &&
      ((required(column.validity[row >>> 5]) >>> (row & 31)) & 1) === 0
    )
      return NULL_SLOT;
    const { entries, index } = this.#dictionaries.locate(
      column.dictionary,
      required(column.codes[row]),
    );
    return { flat: entries, row: index };
  }
  #locate(row: number): {
    frame: FramePayload;
    row: number;
  } {
    if (!Number.isInteger(row) || row < 0 || row >= this.rowCount)
      throw new RangeError(`Unknown result row ${String(row)}`);
    let low = 0,
      high = this.#ends.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if (required(this.#ends[middle]) <= row) low = middle + 1;
      else high = middle;
    }
    return {
      frame: required(this.#frames[low]),
      row: row - (low === 0 ? 0 : required(this.#ends[low - 1])),
    };
  }
  columnChunks(column: string | number): readonly ColumnView[] {
    const index = this.#index(column),
      existing = this.#chunks.get(index);
    if (existing) return existing;
    const type = required(this.#meta[index]).type;
    const chunks = this.#frames.map((frame) => {
      const vector = required(frame.columns[index]);
      if (
        vector.form === "dictionary" &&
        ["text", "json", "enum", "bytes", "array"].includes(type.kind)
      ) {
        let dictionary = this.#dictionaryViews.get(vector.dictionary);
        if (!dictionary) {
          dictionary = materialize(type, this.#dictionaries.size(vector.dictionary), (code) => {
            const entry = this.#dictionaries.locate(vector.dictionary, code);
            return { flat: entry.entries, row: entry.index };
          });
          this.#dictionaryViews.set(vector.dictionary, dictionary);
        }
        if (dictionary.data.layout !== "varlen")
          throw new ProtocolError("Expected a variable-width dictionary");
        return {
          kind: type.kind as "text" | "json" | "enum" | "bytes" | "array",
          encoding: "dictionary" as const,
          codes: vector.codes,
          dictionary: dictionary.data,
          validity:
            vector.validity === null
              ? null
              : new Uint8Array(
                  vector.validity.buffer,
                  vector.validity.byteOffset,
                  vector.validity.byteLength,
                ),
        };
      }
      const flat =
        vector.form === "flat"
          ? vector
          : materialize(type, frame.rowCount, (row) => this.#slot(vector, row));
      return columnView(type, flat);
    });
    this.#chunks.set(index, chunks);
    return chunks;
  }
  column(column: string | number): ColumnView {
    const index = this.#index(column),
      existing = this.#views.get(index);
    if (existing) return existing;
    const view =
      this.#frames.length === 1
        ? required(this.columnChunks(index)[0])
        : columnView(
            required(this.#meta[index]).type,
            materialize(required(this.#meta[index]).type, this.rowCount, (row) => {
              const at = this.#locate(row);
              return this.#slot(required(at.frame.columns[index]), at.row);
            }),
          );
    this.#views.set(index, view);
    return view;
  }
  values(column: string | number): Value[] {
    const index = this.#index(column),
      existing = this.#values.get(index);
    if (existing) return existing;
    const meta = required(this.#meta[index]),
      values: Value[] = [];
    for (const frame of this.#frames)
      for (let row = 0; row < frame.rowCount; row++) {
        const slot = this.#slot(required(frame.columns[index]), row);
        values.push(
          valid(slot.flat, slot.row) ? scalarValue(meta, slot.flat.data, slot.row) : null,
        );
      }
    this.#values.set(index, values);
    return values;
  }
  row(index: number): TRow {
    const at = this.#locate(index);
    if (this.#rows.has(index)) return required(this.#rows.get(index));
    const row = {} as Record<string, Value>;
    this.columns.forEach((column, k) => {
      const cached = this.#values.get(k),
        slot = this.#slot(required(at.frame.columns[k]), at.row);
      const value = cached
        ? cached[index]
        : valid(slot.flat, slot.row)
          ? scalarValue(required(this.#meta[k]), slot.flat.data, slot.row)
          : null;
      Object.defineProperty(row, column.name, {
        value,
        enumerable: true,
        writable: true,
        configurable: true,
      });
    });
    this.#rows.set(index, row as TRow);
    return row as TRow;
  }
  rows(): TRow[] {
    return (this.#allRows ??= Array.from({ length: this.rowCount }, (_, i) => this.row(i)));
  }
  tuples(): Value[][] {
    return (this.#tuples ??= Array.from({ length: this.rowCount }, (_, i) =>
      this.columns.map((_, k) => this.values(k)[i] ?? null),
    ));
  }
  *[Symbol.iterator](): Iterator<TRow> {
    for (let row = 0; row < this.rowCount; row++) yield this.row(row);
  }
}
