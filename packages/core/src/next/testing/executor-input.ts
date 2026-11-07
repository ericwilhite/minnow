/** Bounded columnar input for the SQL differential and Node baseline, outside execution timing. */
import type { TableDef } from "../access/catalog.js";
import { bindParameter } from "../exec/parameters.js";
import { ColumnStore } from "../exec/column-store.js";
import type { Grant } from "../memory/governor.js";
import { PG_EPOCH_UNIX_MICROS, daysFromCivil } from "../temporal.js";
import {
  BATCH_ROWS,
  allocateFlat,
  dataBytes,
  type FlatVector,
  type Batch,
} from "../vector/vector.js";
import { VarlenBuilder } from "../vector/varlen.js";

export type InputValue = boolean | number | string | Date | null;
export function inputBatch(
  table: TableDef,
  source: Readonly<Record<string, readonly InputValue[]>>,
  grant: Grant,
): { batch: Batch; close(): void } {
  const n = source[table.columns[0]?.name ?? ""]?.length ?? 0;
  if (n > BATCH_ROWS) throw new RangeError("the input helper takes one vector batch");
  let held = 0;
  const closes: Array<() => void> = [];
  const take = (bytes: number) => {
    if (!grant.allocate(bytes)) throw new RangeError("input batch allocation refused");
    held += bytes;
  };
  const close = () => {
    for (const cleanup of closes.splice(0)) cleanup();
    if (held > 0) grant.free(held);
    held = 0;
  };
  try {
    const columns: FlatVector[] = table.columns.map((column, slot) => {
      const values = source[column.name];
      if (values?.length !== n) throw new TypeError("inconsistent column input");
      const type = column.type;
      const words = (n + 31) >>> 5;
      take(4 * words);
      const validity = new Uint32Array(words);
      let nulls = false;
      for (let k = 0; k < n; k++) {
        if (values[k] === null) nulls = true;
        else validity[k >>> 5] = (validity[k >>> 5] ?? 0) | (1 << (k & 31));
      }
      if (type.kind === "text") {
        const builder = new VarlenBuilder(n, grant, Number.POSITIVE_INFINITY);
        closes.push(() => builder.close());
        for (const value of values) {
          if (builder.appendString(value === null ? "" : String(value)) !== "ok")
            throw new RangeError("input string allocation refused");
        }
        return {
          form: "flat",
          type,
          length: n,
          validity: nulls ? validity : null,
          data: builder.data(),
        };
      }
      if (type.kind === "decimal") {
        // The general binder retains exact coefficients, typmod rounding and variable scales.
        const store = new ColumnStore([type], grant, { chunkRows: BATCH_ROWS });
        closes.push(() => store.close());
        for (const value of values) {
          const before = grant.allocated;
          const vector = bindParameter(type, value, slot, grant);
          const bindingBytes = grant.allocated - before;
          try {
            if (store.append([vector], Uint16Array.of(0), 0, 1) !== 1)
              throw new RangeError("input decimal allocation refused");
          } finally {
            grant.free(bindingBytes);
          }
        }
        const vector = store.vectors()[0];
        if (vector === undefined) throw new TypeError("empty decimal input");
        return vector;
      }
      take(dataBytes(type, n));
      const vector = allocateFlat(type, n);
      vector.length = n;
      vector.validity = nulls ? validity : null;
      const data = vector.data;
      for (let k = 0; k < n; k++) {
        const value = values[k];
        if (value === null || value === undefined) continue;
        if (data.layout === "bool") data.values[k] = value ? 1 : 0;
        else if (data.layout === "i32" && type.kind === "date") {
          const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(String(value));
          if (match === null) throw new TypeError("canonical date input expected");
          data.values[k] = daysFromCivil(Number(match[1]), Number(match[2]), Number(match[3]));
        } else if (data.layout === "i32" || data.layout === "f64" || data.layout === "f32")
          data.values[k] = Number(value);
        else if (data.layout === "int64" && data.values.repr === "safe") {
          const integer =
            type.kind === "timestamp"
              ? BigInt(
                  value instanceof Date
                    ? value.getTime()
                    : typeof value === "number"
                      ? value
                      : Date.parse(String(value)),
                ) *
                  1000n -
                PG_EPOCH_UNIX_MICROS
              : BigInt(value as number | string);
          if (
            integer < BigInt(Number.MIN_SAFE_INTEGER) ||
            integer > BigInt(Number.MAX_SAFE_INTEGER)
          )
            throw new RangeError("input helper needs an exact int64 vector for this value");
          data.values.values[k] = Number(integer);
        } else throw new TypeError(`input helper does not implement ${type.kind}`);
      }
      return vector;
    });
    return { batch: { length: n, columns, selection: null }, close };
  } catch (error) {
    close();
    throw error;
  }
}
