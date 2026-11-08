/** The owner uses this factory; protocol fixtures may still inject an explicit executor. */
import type { PhysicalPlan } from "../ir/ir.js";
import type { Executor, PreparedPlan, ParameterValue, ExecuteRequest } from "./executor.js";
import type { Grant } from "../memory/governor.js";
import { bindParameters, checkParameterType } from "./parameters.js";
import { compilePlan, type CompileOptions } from "./plan.js";
import { QueryExecution } from "./run.js";

export function createExecutor(options: CompileOptions = {}): Executor {
  return {
    prepare(source: PhysicalPlan): PreparedPlan {
      // The caller may reuse/mutate its lowering result; the compiled recipe owns its snapshot.
      const plan = structuredClone(source);
      for (const type of plan.parameters) checkParameterType(type);
      const program = compilePlan(plan, options);
      return Object.freeze({
        plan,
        columns: plan.columns,
        kind: "query" as const,
        usesSequences: false,
        bind: (values: readonly ParameterValue[], grant: Grant) =>
          bindParameters(plan, values, grant),
        execute: (request: ExecuteRequest) => new QueryExecution(plan, request, program),
      });
    },
  };
}
