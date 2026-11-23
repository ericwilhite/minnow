/**
 * The retry guard's switch (packages/core/src/next/memory/retry-guard.ts): on in every Node suite
 * (vitest.config.ts) and in the browser lanes through vite.library.config.ts's define, which the
 * module must match when bundled without any `process` (as a page or worker sees it); off
 * otherwise.
 */
import { build } from "esbuild";
import { runInNewContext } from "node:vm";
import { fileURLToPath } from "node:url";
import { describe, expect, it, vi } from "vitest";
import {
  RETRY_GUARD_LIMIT,
  RetrySpinError,
  checkRetries,
  macrotaskEpoch,
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

const BUDGET = fileURLToPath(new URL("../packages/core/src/next/write/budget.ts", import.meta.url));

/**
 * A metadata pressure retry run by the shipped code, bundled for a browser (no `process`) with
 * the given defines, in a fresh context whose `setTimeout` counts its calls. Returns how many
 * timers the retry scheduled (Astra's re-check of #60: the guard's epoch timer was armed in a
 * shipped build).
 */
async function timersOfPressureRetry(define: Record<string, string>): Promise<number> {
  const bundled = await build({
    entryPoints: [BUDGET],
    bundle: true,
    write: false,
    format: "iife",
    globalName: "budget",
    platform: "browser",
    define,
  });
  let timers = 0;
  const context: Record<string, unknown> = {
    performance,
    TextEncoder,
    TextDecoder,
    queueMicrotask,
    console,
    setTimeout: (callback: () => void) => {
      timers++;
      void Promise.resolve().then(callback);
      return timers;
    },
  };
  runInNewContext(bundled.outputFiles[0]?.text ?? "", context);
  const { MetadataBudget } = context.budget as {
    MetadataBudget: new (
      grant: unknown,
      reclaim: () => number,
      cap: number,
    ) => {
      allocate(bytes: number): boolean;
      free(bytes: number): void;
      retry<T>(work: () => T, check: () => void, grow: (wanted: number) => Promise<void>): T;
    };
  };
  // A grant of one byte, funded exactly when asked: a few pressure retries.
  const grant = {
    label: "metadata",
    held: 1,
    allocated: 0,
    loaned: 0,
    allocate(bytes: number) {
      if (this.allocated + bytes > this.held) return false;
      this.allocated += bytes;
      return true;
    },
    free(bytes: number) {
      this.allocated -= bytes;
    },
    tryGrow: () => false,
  };
  const metadata = new MetadataBudget(grant, () => 0, 1 << 20);
  let attempts = 0;
  const work = () => {
    attempts++;
    let taken = 0;
    try {
      while (taken < 4) {
        metadata.allocate(1);
        taken++;
      }
    } catch (error) {
      metadata.free(taken);
      throw error;
    }
    metadata.free(taken);
    return attempts;
  };
  const grow = (wanted: number) => {
    if (wanted > grant.held) grant.held = wanted;
    return Promise.resolve();
  };
  expect(await metadata.retry(work, () => undefined, grow), "the retry ran").toBe(4);
  return timers;
}

describe("the guard costs a shipped build nothing", () => {
  it("schedules no timer during a pressure retry when the guard is off", async () => {
    expect(await timersOfPressureRetry({}), "no guard configured").toBe(0);
    // The check sees timers: with the library lanes' define, the same retry arms the epoch's.
    const define = (libraryConfig as { define?: Record<string, string> }).define ?? {};
    expect(await timersOfPressureRetry(define), "the guard on").toBeGreaterThan(0);
  });

  it("never lets the epoch's timer keep a process alive", async () => {
    // Let any timer already armed fire, so the next call arms a fresh one.
    await new Promise((resolve) => setTimeout(resolve, 0));
    const armed = vi.spyOn(globalThis, "setTimeout");
    try {
      macrotaskEpoch();
      const handle = armed.mock.results[0]?.value as { hasRef?: () => boolean } | undefined;
      expect(handle?.hasRef?.(), "the epoch timer is unreferenced").toBe(false);
    } finally {
      armed.mockRestore();
    }
  });
});

describe("a guarded loop", () => {
  it("throws on the round past the limit, and only then", () => {
    const guard = retryGuard("a loop", 3);
    // The first round starts the loop's epoch; the next three are retries within it.
    guard.retry();
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

  it("counts only rounds that did not yield to a macrotask", async () => {
    // Microtask rounds: the macrotask epoch stands still, so they count and the guard trips.
    const spinning = retryGuard("microtasks", 64);
    const spin = async () => {
      for (let round = 0; round < 100; round++) {
        await Promise.resolve();
        spinning.retry();
      }
    };
    await expect(spin(), "a microtask spin trips").rejects.toThrow(RetrySpinError);
    // Timer rounds: each lets a macrotask run, so none counts, however many.
    const waiting = retryGuard("timers", 64);
    const wait = async () => {
      for (let round = 0; round < 100; round++) {
        await new Promise((resolve) => setTimeout(resolve, 0));
        waiting.retry();
        waiting.round(0);
      }
      return "finished";
    };
    expect(await wait(), "rounds that yield to a timer never count").toBe("finished");
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
