import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./packages/core/browser-tests",
  webServer: {
    command: "npx vite --host 127.0.0.1 --port 4180",
    port: 4180,
    reuseExistingServer: true,
  },
  use: { baseURL: "http://127.0.0.1:4180" },
  projects: [
    { name: "chromium", use: { browserName: "chromium" } },
    { name: "firefox", use: { browserName: "firefox" } },
  ],
});
