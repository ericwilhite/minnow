import { describe, expect, it } from "vitest";
import type { Rel } from "./ir.js";

const input: Rel = { op: "values", id: 1, output: [], rows: [] };

describe("IR modify node (ruling R14)", () => {
  it("carries the UPDATE's assigned columns and the row-affected-twice policy", () => {
    const update: Extract<Rel, { op: "modify" }> = {
      op: "modify",
      id: 2,
      output: [],
      kind: "update",
      tableId: 7,
      input,
      assigned: [3, 1],
      onRepeat: "first",
      onConflict: null,
      returning: null,
    };
    expect([update.assigned, update.onRepeat]).toEqual([[3, 1], "first"]);
    // @ts-expect-error -- both fields are required: lowering must decide them.
    const incomplete: Rel = {
      op: "modify",
      id: 3,
      output: [],
      kind: "delete",
      tableId: 7,
      input,
      onConflict: null,
      returning: null,
    };
    expect(incomplete.op).toBe("modify");
  });
});
