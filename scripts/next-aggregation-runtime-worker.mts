/** Full SQL -> delivered frames; shared clock, fixed warmups, immutable native image. */
import createSQLHarness, { type Config } from "./next-sort-runtime-worker.mts";
import { queryLifetimes } from "./lib/query-root-lifetimes.mts";
import type { CPUWorkerContext, CPUWorkerHarness, Verification } from "./lib/independent-cpu.mts";
export default async function createAggregation(
  context: CPUWorkerContext,
): Promise<CPUWorkerHarness> {
  const base = await createSQLHarness(context);
  let checked: Verification | undefined;
  return {
    ...base,
    identity: { native: base.identity, warmups: 10, lifetimes: 10 },
    async verify(id) {
      const value = await base.verify(id);
      if (value.rows === undefined) throw new Error("Missing aggregate output volume");
      if (checked === undefined) {
        for (let i = 0; i < 10; i++) {
          if ((await base.run(id, "frames")).rows !== value.rows)
            throw new Error("Warmup volume drift");
        }
      } else if (value.rows !== checked.rows || value.fingerprint !== checked.fingerprint)
        throw new Error("Aggregation output drift");
      checked = value;
      return { ...value, rows: value.rows * 10 };
    },
    async run(id, boundary) {
      if (checked?.rows === undefined) throw new Error("Unverified aggregation");
      return await queryLifetimes(10, checked.rows, () => base.run(id, boundary));
    },
  };
}
export type AggregationConfig = Config;
