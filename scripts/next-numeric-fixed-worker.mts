/** Already-decoded NUMERIC component on the shared fresh-process CPU clock. */
import { createHash } from "node:crypto";
import { strict as assert } from "node:assert";
import { numericFixture } from "./lib/numeric-fixed-fixture.mts";
import { queryLifetimes } from "./lib/query-root-lifetimes.mts";
import type { CPUWorkerContext, CPUWorkerHarness } from "./lib/independent-cpu.mts";
export interface NumericConfig {
  runtimeRoot: string;
  rows: number;
  plain?: boolean;
}
export default async function createNumeric(context: CPUWorkerContext): Promise<CPUWorkerHarness> {
  const config = context.data as NumericConfig;
  const fixture = await numericFixture(config.rows);
  const state = config.plain ? null : await (await fixture.native(config.runtimeRoot)).prepare();
  const once = () => (state ? state.run() : Promise.resolve(fixture.plain()));
  const fingerprint = createHash("sha256")
    .update(
      JSON.stringify(fixture.expected, (_, v: unknown) =>
        typeof v === "bigint" ? v.toString() : v,
      ),
    )
    .digest("hex");
  let warmed = false;
  return {
    revision: context.revision,
    identity: {
      runtimeRoot: config.runtimeRoot,
      rows: config.rows,
      plain: !!config.plain,
      batches: fixture.batches,
      warmups: 10,
      lifetimes: 10,
    },
    workloads: ["agg_numeric_grouped"],
    boundaries: ["native-component"],
    async verify() {
      assert.deepEqual(await once(), fixture.expected);
      if (!warmed) {
        for (let i = 0; i < 10; i++) await once();
        warmed = true;
      }
      return { fingerprint, rows: fixture.expected.length * 10 };
    },
    async run() {
      return await queryLifetimes(10, fixture.expected.length, async () => ({
        rows: (await once()).length,
      }));
    },
    async close() {
      state?.close();
    },
  };
}
