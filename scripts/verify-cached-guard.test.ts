/**
 * The cached-bytes guard's switch (packages/core/src/next/storage/verify-cached.ts): on in every Node suite (vitest.config.ts),
 * and in the browser lanes through vite.library.config.ts's define, which the module must match
 * when bundled without any `process` (as a page or worker sees it).
 */
import { build } from "esbuild";
import { runInNewContext } from "node:vm";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { VERIFY_CACHED_BYTES } from "../packages/core/src/next/storage/verify-cached.ts";
import libraryConfig from "../vite.library.config.ts";

const source = fileURLToPath(
  new URL("../packages/core/src/next/storage/verify-cached.ts", import.meta.url),
);

/** The module bundled for a browser (no `process`), evaluated in a fresh context. */
async function inBrowser(options: {
  define?: Record<string, string>;
  global?: boolean;
}): Promise<boolean> {
  const bundled = await build({
    entryPoints: [source],
    bundle: true,
    write: false,
    format: "iife",
    globalName: "guard",
    platform: "browser",
    define: options.define ?? {},
  });
  const code = bundled.outputFiles[0]?.text ?? "";
  const context: Record<string, unknown> = {};
  if (options.global === true) context.__MINNOW_VERIFY_CACHED_BYTES__ = true;
  runInNewContext(
    `globalThis.__MINNOW_VERIFY_CACHED_BYTES__ = ${String(options.global === true)}; ${code}`,
    context,
  );
  return (context.guard as { VERIFY_CACHED_BYTES: boolean }).VERIFY_CACHED_BYTES;
}

describe("the cached-bytes guard's switch", () => {
  it("is on in the Node suites", () => {
    expect(VERIFY_CACHED_BYTES).toBe(true);
  });

  it("is on in a browser bundle built with the browser lanes' define, and off without it", async () => {
    const define = (libraryConfig as { define?: Record<string, string> }).define ?? {};
    expect(define["globalThis.__MINNOW_VERIFY_CACHED_BYTES__"]).toBe("true");
    expect(await inBrowser({ define })).toBe(true);
    expect(await inBrowser({})).toBe(false);
  });

  it("is on in a browser when a harness sets the global before the engine loads", async () => {
    expect(await inBrowser({ global: true })).toBe(true);
  });
});
