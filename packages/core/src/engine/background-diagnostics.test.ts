import { expect, it, vi } from "vitest";
import { BackgroundDiagnostics } from "./background-diagnostics.js";

it("retains bounded ordered evidence independently of observers and returns defensive snapshots", () => {
  const seen: unknown[] = [];
  const diagnostics = new BackgroundDiagnostics(
    () => 42,
    (error) => seen.push(error),
  );
  for (let i = 0; i < 40; i += 1)
    diagnostics.report(new Error(`${String(i)}: ${"x".repeat(2000)}`), `job ${String(i)}`);
  const snapshot = diagnostics.snapshot();
  expect(snapshot.backgroundFailureCount).toBe(40);
  expect(snapshot.backgroundErrors).toHaveLength(32);
  expect(snapshot.backgroundErrors[0]?.sequence).toBe(9);
  expect(snapshot.backgroundErrors[31]?.sequence).toBe(40);
  expect(snapshot.backgroundErrors.every((entry) => entry.message.length === 1024)).toBe(true);
  snapshot.backgroundErrors.splice(0);
  expect(diagnostics.snapshot().backgroundErrors).toHaveLength(32);
  const entry = diagnostics.snapshot().backgroundErrors[0];
  entry?.at.setTime(0);
  expect(diagnostics.snapshot().backgroundErrors[0]?.at.getTime()).toBe(42);
  expect(seen).toHaveLength(40);
});

it("keeps the original failure when its diagnostic observer throws", () => {
  const logged = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const observerError = new Error("observer");
  const failure = new Error("disk");
  try {
    const diagnostics = new BackgroundDiagnostics(
      () => 42,
      () => {
        throw observerError;
      },
    );
    diagnostics.report(failure, "collection");
    expect(diagnostics.snapshot().backgroundErrors[0]?.message).toBe("disk");
    expect(logged).toHaveBeenCalledWith(
      "[minnowdb] onBackgroundError callback failed:",
      observerError,
    );
    expect(logged).toHaveBeenCalledWith("[minnowdb] background failure (collection):", failure);
  } finally {
    logged.mockRestore();
  }
});

it("reports opaque thrown values even when their text conversion throws", () => {
  const failure = {
    toString() {
      throw new Error("hostile conversion");
    },
  };
  const observer = vi.fn();
  const diagnostics = new BackgroundDiagnostics(() => 42, observer);
  expect(() => diagnostics.report(failure, "compaction")).not.toThrow();
  expect(observer).toHaveBeenCalledWith(failure, "compaction");
  expect(diagnostics.snapshot().backgroundErrors[0]?.message).toBe("[unprintable error]");
});
