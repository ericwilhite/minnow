import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";

const page = (path: string): string => fileURLToPath(new URL(path, import.meta.url));

export default defineConfig({
  server: {
    // Default dev port, overridable by the environment (session previews) or --port (Playwright).
    port: Number(process.env.PORT ?? 4173),
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
    // Pre-bundle at server start: a dep first discovered mid-session triggers a full page
    // reload, which kills any in-flight capture worker.
    include: ["apache-arrow"],
    exclude: ["@sqlite.org/sqlite-wasm", "@electric-sql/pglite"],
  },
  worker: {
    format: "es",
  },
  build: {
    rollupOptions: {
      input: {
        index: page("index.html"),
        datasets: page("datasets.html"),
        query: page("query.html"),
        suites: page("suites.html"),
        sql: page("sql.html"),
      },
    },
  },
});
