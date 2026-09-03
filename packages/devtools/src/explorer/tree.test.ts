// @vitest-environment happy-dom
import { describe, expect, it, vi } from "vitest";
import type { TableInfo } from "./catalog.js";
import { createSchemaRail, describeColumnType, describeForeignKey } from "./tree.js";

const orders: TableInfo = {
  name: "orders",
  uniqueKey: "order_id",
  columns: [
    { name: "order_id", type: "number", nullable: false, isUniqueKey: true, typeLabel: "INTEGER" },
    { name: "customer_id", type: "number", nullable: false, isUniqueKey: false },
    {
      name: "status",
      type: "string",
      nullable: true,
      isUniqueKey: false,
      enumValues: ["new", "paid"],
    },
    {
      name: "cents",
      type: "number",
      nullable: false,
      isUniqueKey: false,
      generated: "total * 100",
    },
  ],
  indexes: [
    {
      name: "orders_by_status",
      columns: [{ name: "status", direction: "asc" }],
      unique: false,
      state: "ready",
    },
  ],
  foreignKeys: [
    {
      name: "orders_customer",
      columns: ["customer_id"],
      parentTable: "customers",
      parentColumns: ["customer_id"],
      onDelete: "cascade",
      enforced: true,
    },
  ],
  checks: [{ name: "positive_total", sql: "total > 0" }],
  triggers: [{ name: "audit", event: "insert", timing: "after" }],
};

function column(index: number) {
  const found = orders.columns[index];
  if (found === undefined) throw new Error(`no column ${String(index)}`);
  return found;
}

const ordersCustomer = orders.foreignKeys?.[0];
if (ordersCustomer === undefined) throw new Error("no foreign key");

const paid: TableInfo = {
  name: "paid_orders",
  columns: [{ name: "order_id", type: "number", nullable: true, isUniqueKey: false }],
  view: { sql: "SELECT order_id FROM orders WHERE status = 'paid'" },
};

function rail() {
  const deps = {
    storageKey: "t",
    onRefresh: vi.fn(),
    onPickTable: vi.fn(),
    onPickColumn: vi.fn(),
    onPickIndex: vi.fn(),
    onPickForeignKey: vi.fn(),
    onPickViewSql: vi.fn(),
  };
  const view = createSchemaRail(deps);
  view.setCatalog([orders, paid]);
  return { view, deps };
}

const texts = (root: HTMLElement, selector: string): string[] =>
  [...root.querySelectorAll(selector)].map((node) => node.textContent);

describe("describeColumnType", () => {
  it("prefers the declared SQL type and marks nullable with a question mark", () => {
    expect(describeColumnType(column(0))).toBe("INTEGER");
    expect(describeColumnType(column(2))).toBe("string?");
  });
});

describe("describeForeignKey", () => {
  it("reads as an arrow, with the delete rule when it is not restrict", () => {
    expect(describeForeignKey(ordersCustomer)).toBe(
      "customer_id → customers.customer_id · on delete cascade",
    );
    expect(describeForeignKey({ ...ordersCustomer, onDelete: "restrict", enforced: false })).toBe(
      "customer_id → customers.customer_id · not enforced",
    );
  });
});

describe("createSchemaRail", () => {
  it("lists views in their own group, badged, and never as keyless tables", () => {
    const { view } = rail();
    expect(texts(view.node, ".rail-group")).toEqual(["Tables · 1", "Views · 1"]);
    const badges = texts(view.node, ".tnode-badge");
    expect(badges).toEqual(["view"]);
  });

  it("expands a table to its columns and every constraint", () => {
    const { view, deps } = rail();
    view.setSelected("orders");
    expect(texts(view.node, ".index-group")).toEqual([
      "Indexes · 1",
      "Foreign keys · 1",
      "Checks · 1",
      "Triggers · 1",
    ]);
    expect(texts(view.node, ".col-badge")).toEqual(["enum", "gen"]);
    expect(texts(view.node, ".col-type")).toEqual(["INTEGER", "number", "string?", "number"]);
    const key = [...view.node.querySelectorAll<HTMLButtonElement>(".index")].find((node) =>
      node.textContent.includes("orders_customer"),
    );
    key?.click();
    expect(deps.onPickForeignKey).toHaveBeenCalledWith(orders, ordersCustomer);
    expect(view.node.querySelector(".tnode.on .tnode-name")?.textContent).toBe("orders");
  });

  it("offers a view's SQL", () => {
    const { view, deps } = rail();
    view.setSelected("paid_orders");
    expect(texts(view.node, ".index-group")).toEqual(["Definition"]);
    view.node.querySelector<HTMLButtonElement>(".index")?.click();
    expect(deps.onPickViewSql).toHaveBeenCalledWith(paid);
  });

  it("filters by name and can be focused from the keyboard", () => {
    const { view } = rail();
    const search = view.node.querySelector<HTMLInputElement>(".rail-search");
    if (search === null) throw new Error("no search");
    search.value = "paid";
    search.dispatchEvent(new Event("input"));
    expect(texts(view.node, ".tnode-name")).toEqual(["paid_orders"]);
    view.focusSearch();
    expect(view.node.classList.contains("collapsed")).toBe(false);
  });
});
