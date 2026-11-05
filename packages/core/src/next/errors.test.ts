import { describe, expect, it } from "vitest";
import { UnsupportedPlanError } from "./errors.js";
import { UnsupportedPlanError as ExecUnsupportedPlanError } from "./exec/errors.js";
import { unsupported } from "./lower/errors.js";

describe("UnsupportedPlanError (ruling R17.6)", () => {
  it("is one class for lowering and the executor", () => {
    expect(ExecUnsupportedPlanError).toBe(UnsupportedPlanError);
    const executor = new UnsupportedPlanError("float32 SUM");
    expect([executor.stage, executor.construct, executor.sqlState, executor.code]).toEqual([
      "execute",
      null,
      "0A000",
      "MINNOW_UNSUPPORTED_PLAN",
    ]);
    expect(executor.message).toBe("The executor does not support float32 SUM");
    const lowering = unsupported("merge", "MERGE INTO t …");
    expect(lowering).toBeInstanceOf(UnsupportedPlanError);
    expect([lowering.stage, lowering.construct]).toEqual(["lower", "merge"]);
    expect(lowering.message).toBe("Not supported yet (merge): MERGE INTO t …");
  });
});
