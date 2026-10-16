import { describe, expect, it } from "vitest";
import { mulberry32 } from "../testing/seeds.js";
import { toColumnarBatch, type BatchRow } from "./batch.js";

/** The pivot as first written: a map lookup per cell. The fast path must agree with it exactly. */
function referencePivot(rows: readonly BatchRow[]): unknown {
  const columnsByName = new Map<string, unknown[]>();
  const omittedByName = new Map<string, boolean[]>();
  for (const [index, row] of rows.entries()) {
    for (const name of Object.keys(row)) {
      const value: unknown = row[name];
      if (value === undefined) continue;
      let values = columnsByName.get(name);
      if (values === undefined) {
        values = new Array<unknown>(rows.length).fill(null);
        columnsByName.set(name, values);
        omittedByName.set(name, new Array<boolean>(rows.length).fill(true));
      }
      values[index] = value;
      const omitted = omittedByName.get(name);
      if (omitted !== undefined) omitted[index] = false;
    }
  }
  const omitted = Object.fromEntries(
    [...omittedByName].filter(([, values]) => values.some(Boolean)),
  );
  return {
    columns: Object.fromEntries(columnsByName),
    ...(Object.keys(omitted).length === 0 ? {} : { omitted }),
    rowCount: rows.length,
  };
}

function row(entries: Array<[string, unknown]>): BatchRow {
  const value: Record<string, unknown> = {};
  for (const [name, item] of entries) {
    Object.defineProperty(value, name, {
      value: item,
      enumerable: true,
      configurable: true,
      writable: true,
    });
  }
  return value as BatchRow;
}

describe("row batch pivot", () => {
  it("matches the reference pivot on uniform, sparse, reordered, and undefined-valued rows", () => {
    const random = mulberry32(17);
    const names = ["id", "name", "note", "__proto__", "at"];
    for (let trial = 0; trial < 200; trial += 1) {
      const count = 1 + Math.floor(random() * 40);
      const uniform = random() < 0.5;
      const rows = Array.from({ length: count }, (_, index) => {
        const entries: Array<[string, unknown]> = [];
        for (const name of names) {
          const roll = random();
          if (!uniform && roll < 0.15) continue; // Omitted key.
          const value =
            !uniform && roll < 0.25
              ? undefined
              : roll < 0.35
                ? null
                : name === "at"
                  ? new Date(index)
                  : `${name}-${String(index)}`;
          entries.push([name, value]);
        }
        // Occasionally the same keys in another order.
        if (!uniform && random() < 0.2) entries.reverse();
        return row(entries);
      });
      const pivoted = toColumnarBatch(rows);
      expect(pivoted).toEqual(referencePivot(rows));
      // Column order is first-seen order, as the reference discovers it.
      expect(Object.keys(pivoted.columns)).toEqual(
        Object.keys((referencePivot(rows) as { columns: object }).columns),
      );
    }
  });

  it("keeps `__proto__` as an own column, never a prototype", () => {
    const pivoted = toColumnarBatch([
      row([
        ["__proto__", "a"],
        ["id", 1],
      ]),
      row([
        ["__proto__", "b"],
        ["id", 2],
      ]),
    ]);
    expect(Object.getPrototypeOf(pivoted.columns)).toBe(Object.prototype);
    expect(Object.hasOwn(pivoted.columns, "__proto__")).toBe(true);
    expect(Reflect.get(pivoted.columns, "__proto__")).toEqual(["a", "b"]);
  });

  it("marks a column first seen late as omitted for the rows before it", () => {
    expect(toColumnarBatch([{ id: 1 }, { id: 2, note: "x" }, { id: 3 }])).toEqual({
      columns: { id: [1, 2, 3], note: [null, "x", null] },
      omitted: { note: [true, false, true] },
      rowCount: 3,
    });
  });
});
