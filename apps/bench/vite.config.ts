import { defineConfig } from "vite";

export default defineConfig({
  server: {
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
      // WebKit fails a reloaded page's module-worker load "due to access control checks" when
      // the worker script revalidates from cache under COEP; bypassing the cache avoids it.
      "Cache-Control": "no-store",
    },
  },
  preview: {
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
      "Cache-Control": "no-store",
    },
  },
  optimizeDeps: {
    exclude: ["@sqlite.org/sqlite-wasm", "@electric-sql/pglite"],
  },
  worker: {
    format: "es",
  },
});
