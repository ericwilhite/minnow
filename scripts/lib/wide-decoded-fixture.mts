/** Same catalogue values as the operator harness, with independent first-seen page dictionaries. */
import { aggShapes, type ShapeLib } from "../../packages/core/browser/next-agg-shapes.ts";
import type { Rel } from "../../packages/core/src/next/ir/ir.js";
import type { FlatVector } from "../../packages/core/src/next/vector/vector.js";
const decoder = new TextDecoder();
function entryText(vector: FlatVector, code: number): string {
  if (vector.data.layout !== "varlen") throw new Error("text dictionary expected");
  const from = vector.data.offsets[code],
    to = vector.data.offsets[code + 1];
  if (from === undefined || to === undefined) throw new Error("bad entry offsets");
  return decoder.decode(vector.data.bytes.subarray(from, to));
}
export function wideDecodedFixture(lib: ShapeLib, rows: number) {
  const { dictionary, texts } = lib;
  const shape = aggShapes(lib, rows, ["agg_3dims_20aggs"])[0];
  if (!shape) throw new Error("missing wide shape");
  // Stored pages have separate first-seen dictionaries. Reusing one global dictionary would
  // hide MIN/MAX's cross-page rank work; this setup recreates those local identities/orders.
  const batches = shape.batches.map((batch) => ({ ...batch, columns: [...batch.columns] }));
  for (const batch of batches)
    for (let column = 0; column < batch.columns.length; column++) {
      const vector = batch.columns[column];
      if (vector?.form !== "dictionary") continue;
      const translation = new Int32Array(vector.dictionary.length).fill(-1);
      const entries: string[] = [],
        codes: number[] = [];
      for (let row = 0; row < batch.length; row++) {
        const code = vector.codes[row];
        if (code === undefined) throw new Error("missing dictionary code");
        let local = translation[code];
        if (local === undefined) throw new Error("bad dictionary code");
        if (local < 0) {
          local = entries.length;
          const value = entryText(vector.dictionary, code);
          if (typeof value !== "string") throw new Error("text dictionary expected");
          entries.push(value);
          translation[code] = local;
        }
        codes.push(local);
      }
      batch.columns[column] = dictionary(texts(entries), codes);
    }
  shape.batches.length = 0;
  const input = shape.input.map((type, index) => ({
    name: `c${String(index)}`,
    type,
    nullable: index === 20,
  }));
  const rel: Extract<Rel, { op: "aggregate" }> = {
    id: 1,
    op: "aggregate",
    input: { id: 0, op: "values", output: input, rows: [] },
    groupBy: shape.groupBy,
    groupingSets: null,
    aggregates: shape.aggregates,
    output: [
      ...shape.groupBy.map((index) => {
        const column = input[index];
        if (!column) throw new Error("missing group column");
        return column;
      }),
      ...shape.aggregates.map((call, index) => ({
        name: `a${String(index)}`,
        type: call.type,
        nullable: call.fn !== "count" && call.fn !== "count_star",
      })),
    ],
  };

  return { rows, batches, input: shape.input, rel };
}
