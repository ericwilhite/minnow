import { describe, expect, it, vi } from "vitest";
import { ensureOriginPersistence, OriginPersistenceRequiredError } from "./persistence.js";

describe("ensureOriginPersistence", () => {
  it("validates policies and reports an unavailable host without prompting", async () => {
    await expect(ensureOriginPersistence("invalid" as never, undefined)).rejects.toThrow(
      "Unknown origin persistence policy",
    );
    await expect(ensureOriginPersistence("best-effort")).resolves.toEqual({
      policy: "best-effort",
      persisted: false,
      requested: false,
      reason: "unsupported",
    });
    await expect(ensureOriginPersistence("request", {})).resolves.toMatchObject({
      persisted: false,
      requested: false,
      reason: "unsupported",
    });
  });

  it("distinguishes optional status, request, and capability failures", async () => {
    const statusFailure = new Error("status blocked");
    await expect(
      ensureOriginPersistence("request", {
        persisted: async () => {
          throw statusFailure;
        },
      }),
    ).resolves.toMatchObject({ requested: false, reason: "unavailable" });
    await expect(
      ensureOriginPersistence("required", {
        persisted: async () => {
          throw statusFailure;
        },
      }),
    ).rejects.toMatchObject({ cause: statusFailure });

    await expect(
      ensureOriginPersistence("request", { persisted: async () => false }),
    ).resolves.toMatchObject({ requested: false, reason: "unsupported" });
    await expect(
      ensureOriginPersistence("required", { persisted: async () => false }),
    ).rejects.toThrow("cannot request");

    await expect(
      ensureOriginPersistence("request", {
        persisted: async () => false,
        persist: async () => {
          throw new Error("prompt blocked");
        },
      }),
    ).resolves.toMatchObject({ requested: true, reason: "unavailable" });
  });

  it("does not prompt under best-effort policy", async () => {
    const persist = vi.fn(async () => true);
    await expect(
      ensureOriginPersistence("best-effort", {
        persisted: async () => false,
        persist,
      }),
    ).resolves.toEqual({
      policy: "best-effort",
      persisted: false,
      requested: false,
      reason: "denied",
    });
    expect(persist).not.toHaveBeenCalled();
  });

  it("does not prompt when protection already exists", async () => {
    const persist = vi.fn(async () => true);
    await expect(
      ensureOriginPersistence("required", {
        persisted: async () => true,
        persist,
      }),
    ).resolves.toEqual({
      policy: "required",
      persisted: true,
      requested: false,
      reason: "already-persisted",
    });
    expect(persist).not.toHaveBeenCalled();
  });

  it("reports a granted request", async () => {
    await expect(
      ensureOriginPersistence("request", {
        persisted: async () => false,
        persist: async () => true,
      }),
    ).resolves.toEqual({
      policy: "request",
      persisted: true,
      requested: true,
      reason: "granted",
    });
  });

  it("reports a denied optional request without claiming protection", async () => {
    await expect(
      ensureOriginPersistence("request", {
        persisted: async () => false,
        persist: async () => false,
      }),
    ).resolves.toEqual({
      policy: "request",
      persisted: false,
      requested: true,
      reason: "denied",
    });
  });

  it("fails closed when required protection is unsupported or denied", async () => {
    await expect(ensureOriginPersistence("required", undefined)).rejects.toBeInstanceOf(
      OriginPersistenceRequiredError,
    );
    await expect(
      ensureOriginPersistence("required", {
        persisted: async () => false,
        persist: async () => false,
      }),
    ).rejects.toThrow("denied");
  });

  it("keeps API failures observable to required callers", async () => {
    const cause = new DOMException("blocked", "SecurityError");
    await expect(
      ensureOriginPersistence("required", {
        persisted: async () => false,
        persist: async () => {
          throw cause;
        },
      }),
    ).rejects.toMatchObject({ cause });
  });
});
