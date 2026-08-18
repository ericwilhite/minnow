/**
 * `next build`, with NODE_ENV pinned to production.
 *
 * A build is a production build by definition, but `next build` takes NODE_ENV from the
 * environment if something already set it — and then compiles successfully while prerendering
 * with React's development client build, which fails deep in the export with
 * `Cannot read properties of null (reading 'useContext')`. The message names a page rather than
 * the environment, so it reads as a broken page.
 *
 * Something does set it: vite-node runs with NODE_ENV=development and passes it to every process
 * it spawns, and this repository's scripts are vite-node scripts. The release job hit this
 * through the pre-push hook — a script pushed a tag, the hook ran the gate, and the gate's site
 * build failed for a reason that had nothing to do with the tree.
 *
 * The Next CLI is resolved rather than run from PATH, so this works wherever node does.
 */
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const cli = require.resolve("next/dist/bin/next");

const result = spawnSync(process.execPath, [cli, "build", ...process.argv.slice(2)], {
  stdio: "inherit",
  env: { ...process.env, NODE_ENV: "production" },
});

process.exit(result.status ?? 1);
