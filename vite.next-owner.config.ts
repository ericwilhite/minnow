import { defineConfig, mergeConfig } from "vite";
import library from "./vite.library.config.ts";

/**
 * The library runner's server, cross-origin isolated: the owner latency lane needs fine timers
 * (5 µs Chromium, 20 µs Firefox and WebKit, storage probe), which browsers grant only to isolated
 * pages.
 */
export default mergeConfig(
  library,
  defineConfig({
    server: {
      headers: {
        "Cross-Origin-Opener-Policy": "same-origin",
        "Cross-Origin-Embedder-Policy": "require-corp",
      },
    },
  }),
);
