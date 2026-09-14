import { defineConfig } from "vite";

export default defineConfig({
  // The browser conformance runner loads the same vendored SQLite Wasm and PGlite distributions
  // as the benchmark site. Serving this directory preserves each package's relative asset URLs.
  publicDir: "apps/site/public",
  server: {
    // Repository-wide checks may edit generated files while a browser campaign is in flight.
    // A conformance page must keep running the code it loaded instead of being replaced by HMR.
    hmr: false,
    watch: null,
  },
});
