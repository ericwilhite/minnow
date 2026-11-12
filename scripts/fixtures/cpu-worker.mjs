/** Small real subprocess fixture; lifecycle assertions never depend on host speed. */
import { appendFile } from "node:fs/promises";

/** @param {import('../lib/independent-cpu.mts').CPUWorkerContext} context */
export default async function createHarness(context) {
  const flags =
    /** @type {{trace: string, wrongRevision?: boolean, mismatch?: boolean, drift?: boolean, wait?: boolean, waitSetup?: boolean}} */ (
      context.data
    );
  const trace = async (kind) =>
    appendFile(
      flags.trace,
      JSON.stringify({
        kind,
        pid: process.pid,
        arm: context.arm,
        cohort: context.cohort,
        boundary: context.boundary,
      }) + "\n",
    );
  await trace("start");
  if (flags.waitSetup) {
    process.send?.({ kind: "setup-start" });
    try {
      await new Promise((_, reject) => {
        if (context.signal.aborted) reject(context.signal.reason);
        else
          context.signal.addEventListener("abort", () => reject(context.signal.reason), {
            once: true,
          });
      });
    } catch (error) {
      await trace("close");
      throw error;
    }
  }
  let ran = false;
  return {
    revision: flags.wrongRevision ? "wrong" : context.revision,
    identity: { adapter: "synthetic CPU lifecycle" },
    workloads: [context.workload],
    boundaries: [context.boundary],
    async verify(workload) {
      await trace("verify");
      return {
        fingerprint: flags.mismatch || (flags.drift && ran) ? "different" : workload,
        rows: 7,
      };
    },
    async run() {
      ran = true;
      if (flags.wait)
        await new Promise((_, reject) => {
          if (context.signal.aborted) reject(context.signal.reason);
          else
            context.signal.addEventListener("abort", () => reject(context.signal.reason), {
              once: true,
            });
        });
      let sum = 0;
      for (let i = 0; i < 10000; i++) sum += i;
      return {
        rows: 7,
        detail: { pid: process.pid, cohort: context.cohort, boundary: context.boundary, sum },
      };
    },
    async close() {
      await trace("close");
    },
  };
}
