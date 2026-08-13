import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./apps/site/tests",
  workers: 1,
  webServer: {
    // Build, then serve dist with a plain foreground server. `astro dev` refuses to start
    // when another dev daemon is running, and `astro preview` daemonizes itself when stdout
    // is not a TTY — both break Playwright's webServer supervision.
    command: "npm run build --workspace @minnowdb/site && node apps/site/tests/serve-dist.mjs 4185",
    port: 4185,
    reuseExistingServer: false,
    timeout: 180_000,
  },
  use: { baseURL: "http://127.0.0.1:4185" },
  projects: [
    { name: "chromium", use: { browserName: "chromium" } },
    { name: "firefox", use: { browserName: "firefox" } },
    { name: "webkit", use: { browserName: "webkit" } },
  ],
});
