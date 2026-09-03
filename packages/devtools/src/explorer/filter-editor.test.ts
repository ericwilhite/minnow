// @vitest-environment happy-dom
import { describe, expect, it, vi } from "vitest";
import type { ColumnInfo, TableInfo } from "./catalog.js";
import { createFilterBar, parseFilterValues } from "./filter-editor.js";

const amount: ColumnInfo = { name: "amount", type: "number", nullable: false, isUniqueKey: false };
const kind: ColumnInfo = {
  name: "kind",
  type: "string",
  nullable: false,
  isUniqueKey: false,
  enumValues: ["paid", "refunded"],
};
const table: TableInfo = { name: "events", columns: [amount, kind] };

describe("parseFilterValues", () => {
  it("refuses text that is not the column's type instead of matching NULL", () => {
    // The reported case: `abc` in a number filter used to become `amount = NULL`, an empty grid,
    // and no word about why.
    expect(parseFilterValues(amount, "=", "abc", "")).toEqual({
      ok: false,
      message: "Not a number: abc",
    });
    expect(parseFilterValues(amount, "=", "12.5", "")).toEqual({ ok: true, values: [12.5] });
  });

  it("asks for a value rather than compiling a blank one", () => {
    expect(parseFilterValues(amount, "=", "  ", "")).toEqual({
      ok: false,
      message: "Enter a value.",
    });
    expect(parseFilterValues(amount, "between", "1", "")).toEqual({
      ok: false,
      message: "Both values are needed.",
    });
    expect(parseFilterValues(amount, "is null", "", "")).toEqual({ ok: true, values: [] });
  });

  it("parses each part of an IN list and skips empty ones", () => {
    expect(parseFilterValues(amount, "in", "1, 2,,3", "")).toEqual({ ok: true, values: [1, 2, 3] });
    expect(parseFilterValues(amount, "in", "1, x", "").ok).toBe(false);
    expect(parseFilterValues(amount, "in", " , ", "")).toEqual({
      ok: false,
      message: "Enter at least one value.",
    });
  });

  it("holds an enum column to its values", () => {
    expect(parseFilterValues(kind, "=", "pending", "")).toEqual({
      ok: false,
      message: "Not one of paid, refunded: pending",
    });
    expect(parseFilterValues(kind, "=", "paid", "")).toEqual({ ok: true, values: ["paid"] });
  });
});

describe("createFilterBar", () => {
  function bar() {
    const onChange = vi.fn();
    const filters = createFilterBar(onChange);
    filters.setTable(table);
    const root = filters.node;
    const value = root.querySelector<HTMLInputElement>("input.value");
    const add = [...root.querySelectorAll("button")].find((b) => b.textContent === "Add");
    const error = root.querySelector<HTMLElement>(".filter-error");
    if (value === null || add === undefined || error === null) throw new Error("missing controls");
    return { filters, onChange, value, add, error };
  }

  it("shows the parse error in place and adds nothing", () => {
    const { filters, onChange, value, add, error } = bar();
    value.value = "abc";
    add.click();
    expect(error.hidden).toBe(false);
    expect(error.textContent).toBe("Not a number: abc");
    expect(filters.filters()).toEqual([]);
    expect(onChange).not.toHaveBeenCalled();
    // Typing again clears the message; the next attempt succeeds.
    value.value = "42";
    value.dispatchEvent(new Event("input"));
    expect(error.hidden).toBe(true);
    add.click();
    expect(filters.filters()).toEqual([
      { column: "amount", type: "number", operator: "=", values: [42] },
    ]);
    expect(onChange).toHaveBeenCalledTimes(1);
  });

  it("adds a filter handed in from outside, such as a cell's value", () => {
    const { filters, onChange } = bar();
    filters.add({ column: "kind", type: "string", operator: "=", values: ["paid"] });
    expect(filters.filters()).toHaveLength(1);
    expect(onChange).toHaveBeenCalledTimes(1);
    expect(filters.node.querySelector(".chip")?.textContent).toContain("kind = paid");
  });
});
