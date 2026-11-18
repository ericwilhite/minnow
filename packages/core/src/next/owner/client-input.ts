/** Input shape conversion only. SQL types are bound and validated by the owner. */
import type { WriteInput, ColumnarInput, Param } from "./api.js";
import type { WireInputColumn } from "./frame.js";
export function writeColumns(input: WriteInput): {
  readonly names: string[];
  readonly columns: WireInputColumn[];
  readonly rowCount: number;
} {
  if (Array.isArray(input)) {
    const rows = input as ReadonlyArray<Readonly<Record<string, Param>>>;
    const names = Object.keys(rows[0] ?? {});
    for (const row of rows) {
      const own = Object.keys(row);
      if (own.length !== names.length || names.some((name) => !Object.hasOwn(row, name)))
        throw new TypeError(
          "Batch rows must name the same columns; omit a column in every row to use its default",
        );
    }
    return {
      names,
      rowCount: rows.length,
      columns: names.map((name) => ({
        kind: "values",
        values: rows.map((row) => {
          const value = row[name];
          if (value === undefined)
            throw new TypeError("An input cell cannot be undefined; use null or omit its column");
          return value;
        }),
      })),
    };
  }
  const columns = (input as ColumnarInput).columns;
  const names = Object.keys(columns);
  let rowCount: number | null = null;
  const encoded: WireInputColumn[] = [];
  for (const name of names) {
    const column = columns[name];
    if (column === undefined) throw new TypeError("Missing input column");
    if (Array.isArray(column)) {
      rowCount ??= column.length;
      if (rowCount !== column.length) throw new TypeError("Input columns have different lengths");
      encoded.push({ kind: "values", values: column });
      continue;
    }
    const typed = column as Exclude<typeof column, readonly Param[]>;
    const values = "validity" in typed ? typed.values : typed;
    if (!(
      values instanceof Float64Array ||
      values instanceof Int32Array ||
      values instanceof BigInt64Array
    ))
      throw new TypeError("Unsupported column input");
    rowCount ??= values.length;
    if (rowCount !== values.length) throw new TypeError("Input columns have different lengths");
    const mask = "validity" in typed ? typed.validity : null;
    let validity: Uint32Array | null = null;
    if (mask !== null) {
      if (mask.length < Math.ceil(values.length / 8))
        throw new TypeError("Input validity is too short");
      validity = new Uint32Array(Math.ceil(values.length / 32));
      for (let i = 0; i < values.length; i++)
        if (((mask[i >>> 3] ?? 0) & (1 << (i & 7))) !== 0)
          validity[i >>> 5] = (validity[i >>> 5] ?? 0) | (1 << (i & 31));
    }
    if (values instanceof Float64Array) encoded.push({ kind: "f64", values, validity });
    else if (values instanceof Int32Array) encoded.push({ kind: "i32", values, validity });
    else encoded.push({ kind: "i64", values, validity });
  }
  return { names, columns: encoded, rowCount: rowCount ?? 0 };
}
