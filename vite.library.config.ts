import { defineConfig } from "vite";

export default defineConfig({
  // Test pages only: the next/ engine re-verifies cached bytes at every lease release and
  // eviction, so a writer into them fails in the browser lanes too
  // (packages/core/src/next/storage/verify-cached.ts). Off only for the opt-in OPFS export bench
  // (browser-tests/next-export-bench.spec.ts), which times reads the guard would re-hash.
  define:
    process.env.MINNOW_NEXT_EXPORT_BENCH === "1"
      ? {}
      : { "globalThis.__MINNOW_VERIFY_CACHED_BYTES__": "true" },
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
