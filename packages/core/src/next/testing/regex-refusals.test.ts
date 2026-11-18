/** The matrix's unsupported regex array/SRF family stays refused at 1.0 cutover. */
import { afterAll, beforeAll, expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { SqlCompileError } from "../../engine/errors.js";
import { MemoryBlockStore } from "../../storage/memory.js";
import { StorageCore } from "./storage-core.js";
let shipped: MinnowDatabase;
let native: StorageCore;
beforeAll(async () => {
  shipped = new MinnowDatabase(new MemoryBlockStore());
  native = await new StorageCore().open();
});
afterAll(async () => {
  await native.close();
  await shipped.close();
});
it.each([
  "regexp_match('aa', '((a+)+)')",
  "regexp_matches('aa', '(a)+')",
  "regexp_matches('aa', '(a)+', 'g')",
  "regexp_split_to_array('abc', '(x)?')",
  "regexp_split_to_table('abc', '(x)?')",
])("keeps a typed refusal for %s", async (call) => {
  const sql = `SELECT ${call} AS v`;
  await expect(shipped.query(sql)).rejects.toBeInstanceOf(SqlCompileError);
  await expect(native.execute(sql)).rejects.toMatchObject({
    name: "PlanningError",
    sqlState: "42883",
  });
});
