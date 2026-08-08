import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./apps/bench/tests",
  workers: 1,
  webServer: {
    command:
      "npm run dev --workspace @browserdatabase/bench -- --host 127.0.0.1 --port 4175 --strictPort",
    port: 4175,
    reuseExistingServer: false,
  },
  use: { baseURL: "http://127.0.0.1:4175" },
  projects: [
    { name: "chromium", use: { browserName: "chromium" } },
    { name: "firefox", use: { browserName: "firefox" } },
    { name: "webkit", use: { browserName: "webkit" } },
  ],
});
