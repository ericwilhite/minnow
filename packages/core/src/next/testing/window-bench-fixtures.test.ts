/** A component benchmark must reproduce the SQL plan and frozen data it claims to measure. */
import { expect, it } from "vitest";
import fixture from "./executor-catalogue.json" with { type: "json" };
import { StorageCore } from "./storage-core.js";
import type { Rel } from "../ir/ir.js";
import { valueAt } from "../exec/join-fixtures.js";
import { windowShape } from "../exec/window-bench-fixtures.js";

function windowOf(rel: Rel): Extract<Rel, { op: "window" }> {
  if (rel.op === "window") return rel;
  if ("input" in rel) return windowOf(rel.input);
  throw new Error("No window in the frozen plan");
}
it("reproduces the frozen SQL window input layout, types, calls and values", async () => {
  const core = await new StorageCore(32).open();
  const names = {
    win_topk_row_number: "topk",
    win_rank: "rank",
    win_running_sum: "runningSum",
    win_lag_lead: "lagLead",
    win_moving_avg: "movingAvg",
  };
  const fact = fixture.datasets.wide.find((table) => table.name === "events_wide");
  if (fact === undefined) throw new Error("Missing wide fixture");
  try {
    for (const table of fixture.datasets.wide) await core.execute(table.ddl);
    for (const [id, name] of Object.entries(names)) {
      const query = fixture.queries.find((query) => query.id === id);
      if (query === undefined) throw new Error("Missing window SQL");
      const rel = windowOf(core.prepare(query.sql).plan.root);
      const shape = windowShape(name, fact.rows.length, true, "sql");
      const input = rel.input;
      if (input.op !== "project" || input.input.op !== "scan")
        throw new Error("Unexpected window input");
      expect(shape.types, id).toEqual(input.output.map((column) => column.type));
      expect(shape.spec.partitionBy, id).toEqual(rel.partitionBy);
      expect(shape.spec.orderBy, id).toEqual(
        rel.orderBy.map((key) => {
          if (key.expr.op !== "column") throw new Error("Unexpected computed key");
          return {
            column: key.expr.index,
            direction: key.direction,
            nulls: key.nulls,
            collation: key.collation,
          };
        }),
      );
      expect(
        shape.spec.functions.map((call) => ({ fn: call.fn, arg: call.arg, type: call.type })),
        id,
      ).toEqual(
        rel.functions.map((call) => {
          const arg = call.args[0];
          if (arg !== undefined && arg.op !== "column")
            throw new Error("Unexpected computed argument");
          return { fn: call.fn, arg: arg?.index ?? null, type: call.type };
        }),
      );
      const batch = shape.input[0];
      if (batch === undefined || shape.input.length !== 1)
        throw new Error("Expected one frozen batch");
      for (const [c, expr] of input.exprs.entries()) {
        if (expr.op !== "column") throw new Error("Unexpected computed projection");
        const column = input.input.output[expr.index],
          vector = batch.columns[c];
        if (column === undefined || vector === undefined)
          throw new Error("Missing projected column");
        for (const [row, original] of fact.rows.entries()) {
          let expected: unknown = original[column.name as keyof typeof original];
          if (column.type.kind === "int64" && typeof expected === "number") {
            expect(Number.isSafeInteger(expected)).toBe(true);
            expected = BigInt(expected);
          }
          expect(valueAt(vector, row), `${id}/${String(c)}/${String(row)}`).toEqual(expected);
        }
      }
    }
  } finally {
    await core.close();
  }
});
