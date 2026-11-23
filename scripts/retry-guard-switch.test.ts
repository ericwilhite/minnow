/**
 * The retry guard's switch (packages/core/src/next/memory/retry-guard.ts): on in every Node suite
 * (vitest.config.ts) and in the browser lanes through vite.library.config.ts's define, which the
 * module must match when bundled without any `process` (as a page or worker sees it); off
 * otherwise.
 */
import { build } from "esbuild";
import { runInNewContext } from "node:vm";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  RETRY_GUARD_LIMIT,
  RetrySpinError,
  checkRetries,
  retryGuard,
} from "../packages/core/src/next/memory/retry-guard.ts";
import libraryConfig from "../vite.library.config.ts";

const source = fileURLToPath(
  new URL("../packages/core/src/next/memory/retry-guard.ts", import.meta.url),
);

/** The module bundled for a browser (no `process`), evaluated in a fresh context. */
async function inBrowser(define: Record<string, string>): Promise<number | null> {
  const bundled = await build({
    entryPoints: [source],
    bundle: true,
    write: false,
    format: "iife",
    globalName: "guard",
    platform: "browser",
    define,
  });
  const context: Record<string, unknown> = {};
  runInNewContext(bundled.outputFiles[0]?.text ?? "", context);
  return (context.guard as { RETRY_GUARD_LIMIT: number | null }).RETRY_GUARD_LIMIT;
}

describe("the retry guard's switch", () => {
  it("is on in the Node suites, at 64", () => {
    expect(RETRY_GUARD_LIMIT).toBe(64);
  });

  it("is on in a browser bundle with the library config's define, off without it", async () => {
    const define = (libraryConfig as { define?: Record<string, string> }).define ?? {};
    expect(define["globalThis.__MINNOW_RETRY_GUARD__"]).toBe("64");
    expect(await inBrowser(define)).toBe(64);
    expect(await inBrowser({})).toBeNull();
  });
});

describe("a guarded loop", () => {
  it("throws on the round past the limit, and only then", () => {
    const guard = retryGuard("a loop", 3);
    guard.retry();
    guard.retry();
    guard.retry();
    expect(() => {
      guard.retry();
    }).toThrow(RetrySpinError);
    expect(() => {
      checkRetries("recursion", 3, 3);
    }).not.toThrow();
    expect(() => {
      checkRetries("recursion", 4, 3);
    }).toThrow(/recursion: 4 retries without progress/);
  });

  it("counts only measured rounds without progress", () => {
    const guard = retryGuard("measured", 3);
    // Rising progress never counts, however many rounds.
    expect(() => {
      for (let progress = 0; progress < 1_000; progress++) guard.round(progress);
    }, "rising progress never counts").not.toThrow();
    // Then three rounds at or below the best so far count, and the fourth throws.
    guard.round(999);
    guard.round(10);
    guard.round(999);
    expect(() => {
      guard.round(500);
    }).toThrow(RetrySpinError);
    // A new best starts the count again.
    const again = retryGuard("again", 2);
    again.round(1);
    again.round(1);
    again.round(1);
    again.round(2);
    again.round(2);
    again.round(2);
    expect(() => {
      again.round(2);
    }).toThrow(/again: 3 retries without progress/);
  });

  it("never throws when off", () => {
    const guard = retryGuard("off", null);
    for (let i = 0; i < 1_000; i++) guard.retry();
    for (let i = 0; i < 1_000; i++) guard.round(0);
    expect(() => {
      checkRetries("off", 1_000, null);
    }).not.toThrow();
  });
});
