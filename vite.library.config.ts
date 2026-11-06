// eslint-disable-next-line no-restricted-imports -- Node-only benchmark compiler; never shipped.
import { execFileSync } from "node:child_process";
import { defineConfig, type Plugin } from "vite";

/**
 * Cross-origin isolation for the pages that ask for it (`?isolated=1`), and for every request that
 * browser context makes after (a cookie: a dedicated worker's script needs the headers too): the
 * storage kill campaign's observation barrier reads the worker's message counter in shared memory,
 * which needs `SharedArrayBuffer`. Each Playwright test runs in its own profile, so no other test's
 * pages are isolated; a page without the parameter, in a profile that never asked, gets exactly
 * what it got before (none here; the owner lane's `vite.next-owner.config.ts` isolates every page
 * itself). WebKit restarts a SharedWorker when the tab that created it closes on isolated pages
 * only, so isolation must never spread to a lane that did not ask (next-storage.spec.ts checks).
 */
const isolatedPages: Plugin = {
  name: "minnow-isolated-pages",
  configureServer(server) {
    server.middlewares.use((request, response, next) => {
      const asked = new URL(request.url ?? "/", "http://host").searchParams.get("isolated") === "1";
      if (asked || /(?:^|;\s*)minnow-isolated=1(?:;|$)/u.test(request.headers.cookie ?? "")) {
        response.setHeader("Cross-Origin-Opener-Policy", "same-origin");
        response.setHeader("Cross-Origin-Embedder-Policy", "require-corp");
        if (asked) response.setHeader("Set-Cookie", "minnow-isolated=1; Path=/; SameSite=Strict");
      }
      next();
    });
  },
};

const exportBaselineRevision = process.env.MINNOW_EXPORT_BASELINE_COMMIT ?? "18548e34";
const exportBaseline =
  process.env.MINNOW_EXPORT_COMPARE_STREAM === "1"
    ? execFileSync(
        "git",
        ["show", `${exportBaselineRevision}:scripts/browser-perf/page/export-stream.ts`],
        { encoding: "utf8" },
      )
    : null;

export default defineConfig({
  plugins: [
    isolatedPages,
    ...(process.env.MINNOW_EXPORT_VARIANCE === "1"
      ? [
          {
            name: "export-checksum-timer",
            enforce: "pre" as const,
            transform(source: string, id: string) {
              if (!id.endsWith("/next/hash/xxhash32.ts")) return;
              return (
                source.replace("export function xxHash32(", "function measuredHash(") +
                `
export function xxHash32(bytes: Uint8Array, seed = 0, start = 0, end = bytes.length): number {
 const state = (globalThis as typeof globalThis & {__minnowExportPhases?: {active: boolean; checksumMs: number; checksumCalls: number}}).__minnowExportPhases;
 if (!state?.active) return measuredHash(bytes, seed, start, end);
 const before = performance.now();
 try { return measuredHash(bytes, seed, start, end); }
 finally { state.checksumMs += performance.now() - before; state.checksumCalls++; }
}`
              );
            },
          },
        ]
      : []),
    ...(exportBaseline === null
      ? []
      : [
          {
            name: "export-baseline",
            enforce: "pre",
            load(id) {
              const [file, query] = id.split("?");
              if (
                file?.endsWith("/scripts/browser-perf/page/export-stream.ts") &&
                new URLSearchParams(query).has("exportBaseline")
              )
                return (
                  exportBaseline +
                  `\nexport const EXPORT_BASELINE_REVISION = ${JSON.stringify(exportBaselineRevision)};\n`
                );
            },
          },
        ]),
  ],
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
