// @vitest-environment happy-dom
import { describe, expect, it, vi } from "vitest";
import type { TableInfo } from "./catalog.js";
import { createInsertForm, fieldFor } from "./insert-form.js";

const table: TableInfo = {
  name: "orders",
  uniqueKey: "order_id",
  columns: [
    {
      name: "order_id",
      type: "number",
      nullable: false,
      isUniqueKey: true,
      hasDefault: true,
    },
    {
      name: "status",
      type: "string",
      nullable: false,
      isUniqueKey: false,
      enumValues: ["new", "paid"],
    },
    { name: "rush", type: "boolean", nullable: true, isUniqueKey: false },
    {
      name: "total",
      type: "number",
      nullable: false,
      isUniqueKey: false,
      typeLabel: "NUMERIC(10,2)",
    },
    {
      name: "total_cents",
      type: "number",
      nullable: false,
      isUniqueKey: false,
      generated: "total * 100",
    },
  ],
};

function column(index: number) {
  const found = table.columns[index];
  if (found === undefined) throw new Error(`no column ${String(index)}`);
  return found;
}

describe("fieldFor", () => {
  it("offers a menu for enum and boolean columns and a box for the rest", () => {
    expect(fieldFor(column(1)).tagName).toBe("SELECT");
    expect([...fieldFor(column(1)).querySelectorAll("option")].map((o) => o.value)).toEqual([
      "",
      "new",
      "paid",
    ]);
    expect(fieldFor(column(2)).tagName).toBe("SELECT");
    expect(fieldFor(column(3)).tagName).toBe("INPUT");
  });

  it("labels the blank choice by what it means for the column", () => {
    const auto = fieldFor({ ...column(1), hasDefault: true });
    expect(auto.querySelector("option")?.textContent).toBe("(set automatically)");
    expect(fieldFor(column(2)).querySelector("option")?.textContent).toBe("(NULL)");
    expect(fieldFor(column(1)).querySelector("option")?.textContent).toBe("(choose)");
  });
});

describe("createInsertForm", () => {
  it("never asks for a generated column and leaves an auto column out when blank", () => {
    const onSubmit = vi.fn();
    const form = createInsertForm({ onSubmit });
    form.open(table);
    const labels = [...form.node.querySelectorAll(".insert-name")].map((n) => n.textContent);
    expect(labels).toEqual(["order_id", "status", "rush", "total", "total_cents"]);
    expect(form.node.querySelector(".insert-generated")?.textContent).toBe("total * 100");
    expect(form.node.querySelectorAll("input, select")).toHaveLength(4);
    expect([...form.node.querySelectorAll(".insert-type")].map((n) => n.textContent)).toEqual([
      "number · key · auto",
      "string",
      "boolean",
      "NUMERIC(10,2)",
      "generated",
    ]);

    const status = form.node.querySelector<HTMLSelectElement>('select[aria-label="status"]');
    const total = form.node.querySelector<HTMLInputElement>('input[aria-label="total"]');
    if (status === null || total === null) throw new Error("missing fields");
    status.value = "paid";
    total.value = "12.5";
    [...form.node.querySelectorAll("button")]
      .find((b) => b.textContent === "Review insert")
      ?.click();
    expect(onSubmit).toHaveBeenCalledWith({ status: "paid", rush: null, total: 12.5 });
  });

  it("refuses a value the column cannot take, naming the column", () => {
    const onSubmit = vi.fn();
    const form = createInsertForm({ onSubmit });
    form.open(table);
    const status = form.node.querySelector<HTMLSelectElement>('select[aria-label="status"]');
    const total = form.node.querySelector<HTMLInputElement>('input[aria-label="total"]');
    if (status === null || total === null) throw new Error("missing fields");
    status.value = "paid";
    total.value = "twelve";
    [...form.node.querySelectorAll("button")]
      .find((b) => b.textContent === "Review insert")
      ?.click();
    expect(onSubmit).not.toHaveBeenCalled();
    expect(form.node.querySelector(".insert-error")?.textContent).toBe(
      "total: Not a number: twelve",
    );
  });

  it("starts from the values handed to it, for a duplicate", () => {
    const form = createInsertForm({ onSubmit: vi.fn() });
    form.open(table, { status: "new", total: 3 });
    expect(form.node.querySelector<HTMLSelectElement>('select[aria-label="status"]')?.value).toBe(
      "new",
    );
    expect(form.node.querySelector<HTMLInputElement>('input[aria-label="total"]')?.value).toBe("3");
  });
});
