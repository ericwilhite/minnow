/**
 * Profile only measured executor regions of a prebundled *.bench.ts harness. Example:
 * node --cpu-prof --cpu-prof-dir=$TMPDIR --expose-gc scripts/profile-exec.mjs \
 *   /absolute/window-bench.mjs /absolute/profile-directory topk rank
 * The CLI profile is a separate whole-process diagnostic. region-*.cpuprofile excludes
 * input construction, warmup, explicit GC, and result validation. RUNS=1 is sufficient.
 */
import { Session } from "node:inspector/promises";
import { writeFile, mkdir } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";

const [bundle, directory, ...shapes] = process.argv.slice(2);
if (!bundle || !directory) throw new Error("Usage: profile-exec.mjs <bundle> <directory> [shapes]");
await mkdir(directory, { recursive: true });
const session = new Session();
session.connect();
await session.post("Profiler.enable");
let regions = 0;
globalThis.minnowBenchProfile = async (label, run) => {
  await session.post("Profiler.start");
  let profile;
  try {
    return await run();
  } finally {
    ({ profile } = await session.post("Profiler.stop"));
    await writeFile(
      resolve(directory, `region-${String(++regions)}-${label}.cpuprofile`),
      JSON.stringify(profile),
    );
  }
};
process.argv = [process.argv[0], bundle, ...shapes];
try {
  await import(pathToFileURL(resolve(bundle)).href);
  if (regions === 0) throw new Error("The harness did not expose a measured profiling region");
} finally {
  delete globalThis.minnowBenchProfile;
  session.disconnect();
}
