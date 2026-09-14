import { defineConfig } from "vite";

export default defineConfig({
  // The browser conformance runner loads the same vendored SQLite Wasm and PGlite distributions
  // as the benchmark site. Serving this directory preserves each package's relative asset URLs.
  publicDir: "apps/site/public",
  optimizeDeps: {
    // Scan every page this runner serves without pulling in unrelated demos whose packages have
    // not been built by the library-only test command.
    entries: ["packages/core/browser/**/*.html"],
  },
  server: {
    // Repository-wide checks may edit generated files while a browser campaign is in flight.
    // A conformance page must keep running the code it loaded instead of being replaced by HMR.
    hmr: false,
    watch: null,
  },
});
