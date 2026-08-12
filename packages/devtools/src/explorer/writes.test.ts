import { describe, expect, it } from "vitest";
import type { TableInfo } from "./catalog.js";
import { confirmCellEdit, confirmDelete, editingBlockedReason, rowKey } from "./writes.js";

const keyed: TableInfo = {
  name: "people",
  uniqueKey: "id",
  columns: [
    { name: "id", type: "number", nullable: false, isUniqueKey: true },
    { name: "name", type: "string", nullable: false, isUniqueKey: false },
    { name: "city", type: "string", nullable: true, isUniqueKey: false },
  ],
};

const keyless: TableInfo = {
  name: "events",
  columns: [{ name: "kind", type: "string", nullable: false, isUniqueKey: false }],
};

describe("editingBlockedReason", () => {
  it("allows editing a keyed table when writes are on and the target can write", () => {
    expect(editingBlockedReason(keyed, true, true)).toBeUndefined();
  });

  it("names the permission first, since it overrides everything else", () => {
    expect(editingBlockedReason(keyed, false, true)).toContain("turned off");
    expect(editingBlockedReason(keyless, false, true)).toContain("turned off");
  });

  it("names a target that simply cannot write", () => {
    expect(editingBlockedReason(keyed, true, false)).toContain("no write API");
  });

  it("explains the keyless case and that inserts still work", () => {
    const reason = editingBlockedReason(keyless, true, true) ?? "";
    expect(reason).toContain("no unique key");
    expect(reason).toContain("Inserts still work");
  });
});

describe("rowKey", () => {
  it("reads the key out of a row", () => {
    expect(rowKey(keyed, { id: 7, name: "Ada", city: null })).toBe(7);
  });

  it("has no key to read on a keyless table", () => {
    expect(rowKey(keyless, { kind: "created" })).toBeUndefined();
  });
});

const cityColumn: TableInfo["columns"][number] = {
  name: "city",
  type: "string",
  nullable: true,
  isUniqueKey: false,
};

describe("confirmCellEdit", () => {
  it("spells out the key and the before/after value", () => {
    const request = confirmCellEdit({
      table: keyed,
      column: cityColumn,
      key: 7,
      from: null,
      to: "London",
    });
    expect(request.title).toBe("Update 1 row in people");
    expect(request.facts).toEqual([
      ["table", "people"],
      ["key", "id = 7"],
      ["city", "NULL → 'London'"],
      ["call", "updateBatch('people', { keys, changes })"],
    ]);
    expect(request.destructive).toBeUndefined();
  });
});

describe("confirmDelete", () => {
  it("identifies the row by more than its key, and reads as destructive", () => {
    const request = confirmDelete(keyed, 7, { id: 7, name: "Ada", city: "London" });
    expect(request.title).toBe("Delete 1 row from people");
    expect(request.destructive).toBe(true);
    expect(request.confirmLabel).toBe("Delete row");
    expect(request.facts).toContainEqual(["row", "'Ada' · 'London'"]);
    expect(request.warning).toContain("not touched");
  });
});
