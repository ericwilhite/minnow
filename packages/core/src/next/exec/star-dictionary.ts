/** Equality-class IDs owned by one admitted, non-replaying star pipeline.
 * The caller funds every codebook resize/translation and its complete output dictionary
 * before construction. Source predicates run before encode; no borrowed text is retained.
 * The unchanged native joins keep their original traversal and transition order.
 */
import type { ByteCharge } from "../vector/varlen.js";
import {
  BATCH_ROWS,
  newDictionaryId,
  type Batch,
  type FlatVector,
  type Vector,
} from "../vector/vector.js";
import type { StarDictionaryShape } from "./star-shape.js";
import { Charged, GroupTable } from "./agg/groups.js";
import { IDENTITY, isValidSlot } from "./kernels/vectors.js";

interface EncodedColumn {
  readonly book: number;
  readonly column: number;
  readonly ids: Int32Array;
  readonly validity: Uint32Array;
}
export class StarDictionaryBooks {
  readonly #memory: Charged;
  readonly #books: GroupTable[] = [];
  readonly #sources = new Map<number, readonly EncodedColumn[]>();
  readonly #dictionaries: Array<FlatVector | undefined> = [];
  readonly #dictionaryIds: number[] = [];
  #closed = false;
  constructor(
    readonly shape: StarDictionaryShape,
    charge: ByteCharge,
  ) {
    this.#memory = new Charged(charge);
    try {
      for (const book of shape.books) {
        if (book.sources.length === 0) throw new Error("Star book has no source");
        this.#books.push(new GroupTable([{ kind: "text" }], charge));
      }
      for (const scan of shape.scans) {
        const columns: EncodedColumn[] = [];
        for (const [column, book] of (shape.columns.get(scan.id) ?? []).entries()) {
          if (book < 0) continue;
          columns.push({
            book,
            column,
            ids: this.#memory.make(Int32Array, BATCH_ROWS),
            validity: this.#memory.make(Uint32Array, BATCH_ROWS / 32),
          });
        }
        this.#sources.set(scan.id, columns);
      }
    } catch (error) {
      this.close();
      throw error;
    }
  }
  encode(scan: number, batch: Batch): Batch {
    if (this.#closed) throw new Error("Star dictionaries closed");
    if (batch.length > BATCH_ROWS) throw new RangeError("Star input exceeds its bounded step");
    const fields = this.#sources.get(scan);
    if (!fields) throw new Error("Missing star source recipe");
    const columns = [...batch.columns];
    for (const field of fields) {
      const vector = columns[field.column],
        table = this.#books[field.book];
      if (!vector || !table) throw new Error("Missing star source book");
      if (this.#dictionaries[field.book]) throw new Error("Star input resumed after delivery");
      // GroupTable.assign fences its borrowed DataView in its own finally; only canonical
      // bytes and numeric dictionary translations survive the source's ready boundary.
      table.assign([vector], batch.length, batch.selection, field.ids);
      const active = batch.selection?.rows ?? IDENTITY,
        count = batch.selection?.count ?? batch.length;
      let nullable = false;
      if (vector.form === "constant" ? !isValidSlot(vector, 0) : vector.validity !== null) {
        field.validity.fill(0);
        for (let k = 0; k < count; k++) {
          const row = active[k] ?? 0;
          if (isValidSlot(vector, row))
            field.validity[row >>> 5] = (field.validity[row >>> 5] ?? 0) | (1 << (row & 31));
          else nullable = true;
        }
      }
      columns[field.column] = {
        form: "flat",
        type: { kind: "int32", bits: 32 },
        length: batch.length,
        validity: nullable ? field.validity : null,
        data: { layout: "i32", values: field.ids },
      };
    }
    return { ...batch, columns };
  }
  /** Only after every input finished. Output buffers remain owned through downstream ready. */
  decode(batch: Batch): Batch {
    if (this.#closed) throw new Error("Star dictionaries closed");
    if (batch.length > BATCH_ROWS) throw new RangeError("Star output exceeds its bounded step");
    const columns: Vector[] = [...batch.columns];
    for (const [column, book] of this.shape.groupBooks.entries()) {
      const table = this.#books[book],
        vector = columns[column];
      if (!table || !vector) throw new Error("Missing star delivery book");
      // AggregateOperator emits group keys as owned flat vectors. Borrow their exact ID
      // buffer/validity through downstream readiness, just as its ordinary output does.
      if (vector.form !== "flat" || vector.data.layout !== "i32")
        throw new Error("Star group is not a flat class ID");
      let dictionary = this.#dictionaries[book];
      if (!dictionary) {
        this.#memory.take(table.keyBytes(0, 0, table.groupCount));
        dictionary = table.emitKey(0, 0, table.groupCount);
        this.#dictionaries[book] = dictionary;
        this.#dictionaryIds[book] = newDictionaryId();
      }
      columns[column] = {
        form: "dictionary",
        type: { kind: "text" },
        length: batch.length,
        validity: vector.validity,
        codes: vector.data.values,
        dictionary,
        dictionaryId: this.#dictionaryIds[book] ?? 0,
      };
    }
    return { ...batch, columns };
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const book of this.#books) book.close();
    this.#books.length = 0;
    this.#sources.clear();
    this.#dictionaries.length = 0;
    this.#memory.close();
  }
}
