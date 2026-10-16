import { describe, expect, it } from "vitest";
import { nextSort, sortRows } from "./sort.js";

const rows = [
  { id: 1, name: "b", at: new Date(3) },
  { id: 2, name: null, at: new Date(1) },
  { id: 3, name: "a", at: null },
  { id: 4, name: "b", at: new Date(2) },
  { id: 5, name: "B", at: new Date(2) },
];
const ids = (sorted: ReadonlyArray<{ id?: unknown }>) => sorted.map((row) => row.id);

describe("sortRows", () => {
  it("sorts by code point with NULLs last in both directions, and keeps ties in query order", () => {
    expect(ids(sortRows(rows, { column: "name", direction: "asc" }))).toEqual([5, 3, 1, 4, 2]);
    expect(ids(sortRows(rows, { column: "name", direction: "desc" }))).toEqual([1, 4, 3, 5, 2]);
  });

  it("orders dates by instant rather than by object identity", () => {
    expect(ids(sortRows(rows, { column: "at", direction: "asc" }))).toEqual([2, 4, 5, 1, 3]);
  });

  it("orders numbers numerically, not as text", () => {
    const numbers = [{ n: 10 }, { n: 9 }, { n: -1 }, { n: 100 }];
    expect(sortRows(numbers, { column: "n", direction: "asc" }).map((row) => row.n)).toEqual([
      -1, 9, 10, 100,
    ]);
  });

  it("leaves the input in its original order", () => {
    sortRows(rows, { column: "name", direction: "asc" });
    expect(ids(rows)).toEqual([1, 2, 3, 4, 5]);
  });
});

describe("nextSort", () => {
  it("cycles ascending, descending, then off, and restarts on another column", () => {
    const asc = nextSort(undefined, "a");
    expect(asc).toEqual({ column: "a", direction: "asc" });
    const desc = nextSort(asc, "a");
    expect(desc).toEqual({ column: "a", direction: "desc" });
    expect(nextSort(desc, "a")).toBeUndefined();
    expect(nextSort(desc, "b")).toEqual({ column: "b", direction: "asc" });
  });
});
