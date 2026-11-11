/** The lead-assigned retaining-parent seam is an explicit capability limit until B7 lands it. */
import { expect, it } from "vitest";
import { UnsupportedPlanError } from "../exec/errors.js";
import type { Rel } from "../ir/ir.js";
import { StorageCore } from "./storage-core.js";
it.each(["join", "window"] as const)(
  "refuses expanding JSON below %s before execution",
  async (parent) => {
    const core = await new StorageCore().open();
    try {
      const lowered = core.lowerer.lower(`SELECT '{"n":1e1000}'::jsonb AS j`, core.context());
      if (lowered.kind !== "query") throw new Error("Expected query");
      const input = lowered.plan.root;
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
              output: input.output,
              left: input,
              right: { op: "values", id: input.id + 101, output: [], rows: [[]] },
              kind: "inner",
              algorithm: "nestedLoop",
              leftKeys: [],
              rightKeys: [],
              nullSafe: [],
              residual: null,
              publishes: [],
            };
      let refusal: unknown;
      try {
        core.executor.prepare({ ...lowered.plan, root });
      } catch (error) {
        refusal = error;
      }
      if (!(refusal instanceof UnsupportedPlanError))
        throw new Error("Expected typed retaining-parent refusal", { cause: refusal });
      expect(refusal.sqlState).toBe("0A000");
      expect(refusal.message).toContain("checkpoint seam pending");
      expect(core.spill.bytesInUse).toBe(0);
    } finally {
      await core.close();
    }
  },
);
