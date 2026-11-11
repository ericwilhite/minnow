/** Previously refused retaining parents now consume the native checkpoint seam. */
import { expect, it, beforeAll, afterAll, vi } from "vitest";
import { PGlite } from "@electric-sql/pglite";
import type { Rel } from "../ir/ir.js";
import { HashJoin } from "../exec/join.js";
import { WindowOperator } from "../exec/window.js";
import { I32 } from "../exec/kernels/test-support.js";
import { StorageCore } from "./storage-core.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(["inner", "left", "right", "full", "semi", "anti", "mark", "window"] as const)(
  "executes expanding JSON below retaining %s",
  async (parent) => {
    const core = await new StorageCore().open();
    const joins = vi.spyOn(HashJoin.prototype, "checkpoint");
    const windows = vi.spyOn(WindowOperator.prototype, "checkpoint");
    try {
      const lowered = core.lowerer.lower(
        `SELECT '{"n":1e10000}'::jsonb AS j${parent === "mark" ? ",1::integer k" : ""}`,
        core.context(),
      );
      if (lowered.kind !== "query") throw new Error("Expected query");
      const input = lowered.plan.root;
      const keyColumn = input.output[1],
        keyExpr = input.op === "project" ? input.exprs[1] : undefined;
      if (parent === "mark" && (!keyColumn || !keyExpr)) throw new Error("Missing mark key");
      const marker = core.lowerer.lower("SELECT TRUE AS m", core.context());
      if (marker.kind !== "query") throw new Error("Expected marker descriptor");
      const root: Rel =
        parent === "window"
          ? {
              op: "window",
              id: input.id + 100,
              input,
              output: input.output,
              partitionBy: [],
              orderBy: [],
              functions: [],
            }
          : {
              op: "join",
              id: input.id + 100,
              output:
                parent === "mark" ? [...input.output, ...marker.plan.root.output] : input.output,
              left: input,
              right:
                parent === "mark" && keyColumn && keyExpr
                  ? { op: "values", id: input.id + 101, output: [keyColumn], rows: [[keyExpr]] }
                  : { op: "values", id: input.id + 101, output: [], rows: [[]] },
              kind: parent,
              algorithm: parent === "mark" ? "hash" : "nestedLoop",
              leftKeys: parent === "mark" ? [{ op: "column", index: 1, type: I32 }] : [],
              rightKeys: parent === "mark" ? [{ op: "column", index: 0, type: I32 }] : [],
              nullSafe: parent === "mark" ? [false] : [],
              residual: null,
              publishes: [],
            };
      const prepared = core.executor.prepare({
        ...lowered.plan,
        root,
        columns:
          parent === "mark"
            ? [...lowered.plan.columns, ...marker.plan.columns]
            : lowered.plan.columns,
      });
      const expected = (
        await pg.query<{ j: string; k?: number; m?: boolean }>(
          `SELECT '{"n":1e10000}'::jsonb::text AS j${parent === "mark" ? ",1 k,true m" : ""}${parent === "anti" ? " WHERE FALSE" : ""}`,
        )
      ).rows;
      const { execution, release } = core.start(prepared);
      const rows: Array<{ j: string; k?: number; m?: boolean }> = [];
      try {
        for (;;) {
          const frame = await execution.results.next();
          if (!frame) break;
          try {
            const value = frame.payload.columns[0];
            if (value?.form !== "flat" || value.data.layout !== "varlen")
              throw new Error("Expected canonical JSONB bytes");
            expect(frame.payload.columns).toHaveLength(parent === "mark" ? 3 : 1);
            for (let row = 0; row < frame.payload.rowCount; row++) {
              const j = new TextDecoder().decode(
                value.data.bytes.subarray(
                  value.data.offsets[row] ?? 0,
                  value.data.offsets[row + 1] ?? 0,
                ),
              );
              if (parent === "mark") {
                const m = frame.payload.columns[2];
                if (m?.form !== "flat" || m.data.layout !== "bool")
                  throw new Error("Expected boolean mark");
                const k = frame.payload.columns[1];
                if (k?.form !== "flat" || k.data.layout !== "i32")
                  throw new Error("Expected mark input key");
                const key = k.data.values[row];
                if (key === undefined) throw new Error("Missing mark input key value");
                rows.push({ j, k: key, m: m.data.values[row] !== 0 });
              } else rows.push({ j });
            }
          } finally {
            frame.release();
          }
        }
        await execution.executed;
        await execution.delivered;
        expect(rows).toEqual(expected);
        expect(joins.mock.calls.length + windows.mock.calls.length).toBeGreaterThan(0);
      } finally {
        execution.cancel();
        await Promise.allSettled([execution.executed, execution.delivered]);
        release();
      }
      expect(core.spill.bytesInUse).toBe(0);
    } finally {
      try {
        await core.close();
      } finally {
        joins.mockRestore();
        windows.mockRestore();
      }
    }
  },
);
