import { describe, expect, it } from "vitest";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  tableInput,
  TEXT,
  texts,
} from "./test-support.js";

describe("write path smoke", () => {
  it("creates, inserts, reads back, probes, and survives a reopen", async () => {
    const harness = new Harness();
    const path = await harness.open();
    let tableId = 0;
    await autocommit(path, async (statement) => {
      const { id } = await statement.changeCatalog({
        kind: "createTable",
        table: tableInput("items", [column("id", INT, false), column("name", TEXT)]),
      });
      tableId = id ?? 0;
    });
    await autocommit(path, (statement) =>
      statement.table(tableId).insert(batch(ints([1, 2, 3]), texts(["a", "b", null])), null),
    );
    const view = path.pin();
    expect(await rowsOf(view, tableId)).toEqual([
      [1, "a"],
      [2, "b"],
      [3, null],
    ]);
    const keys = new KeyBatch({ allocate: () => true, free: () => undefined });
    encodeKeyBatch(
      "equality",
      [{ vector: ints([2, 9]), direction: "asc", nulls: "first" }],
      { length: 2, selection: null },
      keys,
    );
    const def = path.catalog.table(tableId);
    const out = { probes: new Int32Array(4), locators: new Float64Array(4), count: 0 };
    const result = await view.index(tableId, def?.primaryKey ?? 0).probe(keys, out);
    expect(result.done).toBe(true);
    expect(out.count).toBe(1);
    expect(out.probes[0]).toBe(0);
    view.release();
    const reopened = await harness.reopen();
    const after = reopened.pin();
    expect(await rowsOf(after, tableId)).toEqual([
      [1, "a"],
      [2, "b"],
      [3, null],
    ]);
    after.release();
    await harness.close();
  });
});
