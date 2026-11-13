import { defineConfig } from "@playwright/test";
import {
  browserProjects,
  localUrl,
  partReporters,
  partTestMatch,
  runnerDefaults,
} from "./playwright.shared.mjs";

const port = 4180;
const testDir = "./packages/core/browser-tests";
// The rebuild owner's specs need a cross-origin-isolated server (fine timers, SharedArrayBuffer)
// and run under playwright.next-owner.config.ts; here their isolation check would fail.
const testIgnore = /next-owner.*\.spec\.ts$/u;
// CI's parts (`MINNOW_BROWSER_PART`) split the specs by measured duration; unset, all of them.
const testMatch = partTestMatch(testDir, testIgnore);

export default defineConfig({
  ...runnerDefaults,
  testDir,
  testIgnore,
  ...(testMatch === undefined ? {} : { testMatch }),
  reporter: [...runnerDefaults.reporter, ...partReporters()],
  webServer: {
    command: `node apps/site/scripts/vendor-engines.mjs && npx vite --config vite.library.config.ts --host 127.0.0.1 --port ${String(port)} --strictPort`,
    url: `${localUrl(port)}/packages/core/browser/`,
    reuseExistingServer: false,
  },
  use: {
    baseURL: localUrl(port),
    // These runners expose database drivers on otherwise static pages. Keep the action timeline,
    // errors, sources, and attachments without continuously encoding blank-page screenshots or
    // serializing DOM snapshots around thousands of storage calls.
    trace: {
      mode: "retain-on-failure",
      screenshots: false,
      snapshots: false,
      sources: true,
      attachments: true,
    },
  },
  // All three, including WebKit: this runner is the only place real IndexedDB is exercised, and
  // Safari's is the implementation most likely to differ. It was excluded until the harness
  // stopped treating a transient `blocked` on deleteDatabase as a failure -- see run.ts.
  projects: browserProjects,
});
