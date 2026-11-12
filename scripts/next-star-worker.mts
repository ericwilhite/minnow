/** Decoded star producer on the sole shared fresh-process all-thread CPU clock. */
import { strict as assert } from "node:assert";
import { createHash } from "node:crypto";
import type { CPUWorkerContext, CPUWorkerHarness } from "./lib/independent-cpu.mts";
import { queryLifetimes } from "./lib/query-root-lifetimes.mts";
import { starFixture } from "./lib/star-fixture.mts";
import { nativeStar } from "./lib/star-native.mts";
import { lightStar } from "./lib/star-light.mts";
export default async function createStar(context: CPUWorkerContext): Promise<CPUWorkerHarness> {
  const config = context.data as {
    runtimeRoot: string;
    rows: number;
    plain?: boolean;
    encoded?: boolean;
  };
  const fixture = await starFixture(config.runtimeRoot, config.rows);
  const state = config.plain
    ? null
    : await (
        await nativeStar(config.runtimeRoot, fixture, config.encoded, context.signal)
      ).prepare();
  const once = () => (state ? state.run() : Promise.resolve(lightStar(fixture)));
  const fingerprint = createHash("sha256").update(JSON.stringify(fixture.expected)).digest("hex");
  let warmed = false;
  return {
    revision: context.revision,
    workloads: ["join_star_3dims"],
    boundaries: ["native-component"],
    identity: {
      ...config,
      warmups: 10,
      lifetimes: 10,
      sourceRows: Object.fromEntries(
        [...fixture.named].map(([name, source]) => [
          name,
          source.batches.reduce((n, batch) => n + (batch.selection?.count ?? batch.length), 0),
        ]),
      ),
      fixture: fixture.shape,
    },
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
