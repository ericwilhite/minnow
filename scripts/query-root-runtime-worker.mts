/** B2 adapter on B7's independent process clock. No nested worker or second timing loop. */
import createSQLHarness, { type Config } from "./next-sort-runtime-worker.mts";
import type { CPUWorkerContext, CPUWorkerHarness, Verification } from "./lib/independent-cpu.mts";
import { queryLifetimes } from "./lib/query-root-lifetimes.mts";
export interface RootRunConfig extends Config {
  repeats: number;
  warmups: number;
}
export default async function createRootHarness(
  context: CPUWorkerContext,
): Promise<CPUWorkerHarness> {
  const config = context.data as RootRunConfig;
  const base = await createSQLHarness(context);
  let checked: Verification | undefined;
  try {
    return {
      ...base,
      identity: { native: base.identity, repeats: config.repeats, warmups: config.warmups },
      async verify(id) {
        const value = await base.verify(id);
        if (value.rows !== (config.rows ?? 1000000))
          throw new Error("Native fixture row count mismatch");
        if (checked === undefined) {
          for (let warm = 0; warm < config.warmups; warm++) {
            const next = await base.run(id, "frames");
            if (next.rows !== value.rows) throw new Error("Warmup row count drift");
          }
        } else if (value.rows !== checked.rows || value.fingerprint !== checked.fingerprint)
          throw new Error("Full output drift");
        checked = value;
        return { ...value, rows: value.rows * config.repeats };
      },
      async run(id, boundary) {
        if (checked?.rows === undefined) throw new Error("Unverified query");
        return await queryLifetimes(config.repeats, checked.rows, () => base.run(id, boundary));
      },
    };
  } catch (error) {
    await base.close();
    throw error;
  }
}
