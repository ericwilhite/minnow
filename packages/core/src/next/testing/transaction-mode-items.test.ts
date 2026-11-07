/** Ordered transaction instructions are lossless even though `modes` also keeps final values. */
import { expect, it } from "vitest";
import { createReach, TRANSACTION_MODE_ITEM_STATES } from "../../testing/reach.js";
import { LoweredDatabase } from "./lowering-harness.js";

const ITEMS = [
  ["READ ONLY", { kind: "access", value: "readOnly" }],
  ["READ WRITE", { kind: "access", value: "readWrite" }],
  ["ISOLATION LEVEL READ COMMITTED", { kind: "isolation", value: "read committed" }],
  ["ISOLATION LEVEL REPEATABLE READ", { kind: "isolation", value: "repeatable read" }],
  ["ISOLATION LEVEL READ UNCOMMITTED", { kind: "isolation", value: "read uncommitted" }],
  ["DEFERRABLE", { kind: "deferrable", value: true }],
  ["NOT DEFERRABLE", { kind: "deferrable", value: false }],
] as const;

it("retains every transaction mode item in written order, including duplicates", () => {
  const next = new LoweredDatabase();
  const reach = createReach<string>("transaction mode items", TRANSACTION_MODE_ITEM_STATES);
  const lists: Array<Array<(typeof ITEMS)[number]>> = [[]];
  for (const a of ITEMS) {
    lists.push([a]);
    for (const b of ITEMS) {
      lists.push([a, b]);
      for (const c of ITEMS) lists.push([a, b, c]);
    }
  }
  let cases = 0;
  for (const command of ["BEGIN", "START TRANSACTION", "SET TRANSACTION"])
    for (const separator of [", ", " "])
      for (const list of lists) {
        if (command === "SET TRANSACTION" && list.length === 0) continue;
        reach.beginHistory();
        const sql = `${command} ${list.map(([text]) => text).join(separator)}`;
        const lowered = next.lower(sql);
        if (lowered.kind !== "transactionControl") throw new Error(sql);
        const expected = list.map(([, item]) => item);
        expect(lowered, sql).toHaveProperty("modeItems", expected);
        const last = (kind: string) =>
          [...expected].reverse().find((item) => item.kind === kind)?.value ?? null;
        const access = last("access");
        expect(lowered.modes, sql).toEqual({
          readOnly: access === null ? null : access === "readOnly",
          isolation: last("isolation"),
          deferrable: last("deferrable"),
        });
        reach.reach(
          command === "BEGIN" ? "begin" : command === "START TRANSACTION" ? "start" : "set",
        );
        if (list.length === 0) reach.reach("empty");
        for (const item of expected) reach.reach(item.kind);
        if (new Set(list.map(([text]) => text)).size < list.length) reach.reach("duplicates");
        if (new Set(expected.map((item) => item.kind)).size > 1) reach.reach("mixed kinds");
        cases++;
      }
  expect(cases).toBe(2398);
  for (const sql of ["COMMIT", "ROLLBACK", "SAVEPOINT s", "RELEASE s", "ROLLBACK TO s"])
    expect(next.lower(sql), sql).toHaveProperty("modeItems", []);
  reach.assertReached();
});
