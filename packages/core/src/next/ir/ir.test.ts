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

describe("IR additions (ruling R17.5)", () => {
  it("expresses ON CONFLICT arbiters, MERGE, client rows and index ranges", () => {
    const int4 = { kind: "int32", bits: 32 } as const;
    const rows: Rel = { op: "input", id: 1, output: [{ name: "id", type: int4, nullable: false }] };
    const upsert: Extract<Rel, { op: "modify" }> = {
      op: "modify",
      id: 2,
      output: [],
      kind: "upsert",
      tableId: 7,
      input: rows,
      assigned: [],
      onRepeat: "error",
      onConflict: { arbiters: [], indexId: -1, action: "nothing", assignments: [], where: null },
      returning: null,
    };
    const range: Rel = {
      op: "indexRange",
      id: 3,
      output: [],
      tableId: 7,
      indexId: 1,
      lower: { keys: [{ op: "param", slot: 0, type: int4 }], inclusive: true },
      upper: null,
      descending: false,
      columns: [1],
      emitLocator: true,
    };
    const merge: Extract<Rel, { op: "modify" }> = {
      ...upsert,
      kind: "merge",
      onConflict: null,
      input: range,
      merge: {
        sourceColumns: 1,
        actions: [
          { when: "matched", condition: null, action: { kind: "delete" } },
          { when: "notMatched", condition: null, action: { kind: "nothing" } },
        ],
      },
    };
    expect([upsert.onConflict?.arbiters, merge.merge?.actions.length]).toEqual([[], 2]);
  });
});
