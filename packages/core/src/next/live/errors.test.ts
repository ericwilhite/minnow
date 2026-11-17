import { expect, it, vi } from "vitest";
import { MAX_SERIALIZED_CAUSE_DEPTH } from "../../worker-protocol/index.js";
import { Governor } from "../memory/governor.js";
import { LIVE_ERROR_MAX_BYTES, LIVE_ERROR_SCRATCH_BYTES } from "./constants.js";
import { prepareLiveError } from "./errors.js";

it("admits diagnostic copies before cloning and preserves owned typed properties", async () => {
  const governor = new Governor(8 * 1024 * 1024);
  const grant = await governor.admit({
    label: "live diagnostic",
    minimumBytes: 256 * 1024,
    spillable: false,
  });
  expect(grant.allocate(LIVE_ERROR_SCRATCH_BYTES)).toBe(true);
  const originalClone = structuredClone;
  let admitted = false;
  const cloneAdmissions: boolean[] = [];
  const clone = vi.spyOn(globalThis, "structuredClone").mockImplementation((value, options) => {
    cloneAdmissions.push(admitted);
    return originalClone(value, options);
  });
  const bytes = new Uint8Array([1, 2, 3]);
  const error = Object.assign(new Error("fixture"), {
    code: "fixture",
    sqlState: "22012",
    details: { bytes, wide: 1n << 63n, labels: new Map([["x", new Set([1, 2])]]) },
  });
  let stackReads = 0;
  Object.defineProperty(error, "stack", {
    get() {
      stackReads++;
      return "must not materialize";
    },
  });
  try {
    const wire = prepareLiveError(error, (wanted) => {
      admitted = grant.allocate(wanted);
      return admitted;
    });
    expect(wire).toMatchObject({
      name: "Error",
      message: "fixture",
      props: {
        code: "fixture",
        sqlState: "22012",
        details: { bytes: new Uint8Array([1, 2, 3]), wide: 1n << 63n },
      },
    });
    bytes[0] = 9;
    expect((wire.props?.details as { bytes: Uint8Array }).bytes[0]).toBe(1);
    expect(wire.stack).toBeUndefined();
    expect(stackReads).toBe(0);
    expect(clone).toHaveBeenCalledTimes(1);
    expect(cloneAdmissions, "diagnostic cloning requires prepaid credit").toEqual([true]);
    expect((wire.props?.details as { labels: Map<string, Set<number>> }).labels).toEqual(
      new Map([["x", new Set([1, 2])]]),
    );
  } finally {
    clone.mockRestore();
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
});

it("uses the prepaid fallback when admission refuses, without a probe clone", () => {
  const clone = vi.spyOn(globalThis, "structuredClone");
  try {
    const wire = prepareLiveError(
      Object.assign(new Error("fixture"), { details: { x: 1 } }),
      () => false,
    );
    expect(wire).toMatchObject({
      name: "MemoryAdmissionError",
      props: { code: "MINNOW_MEMORY_ADMISSION", sqlState: "53200" },
    });
    expect(clone).not.toHaveBeenCalled();
  } finally {
    clone.mockRestore();
  }
});

it("bounds oversized messages, graph breadth and a narrow view's complete backing buffer", () => {
  const clone = vi.spyOn(globalThis, "structuredClone");
  const admit = vi.fn(() => true);
  const broad = Object.fromEntries(Array.from({ length: 100 }, (_, i) => [String(i), i]));
  const backing = new Uint8Array(2 * LIVE_ERROR_MAX_BYTES);
  try {
    for (const error of [
      new Error("x".repeat(LIVE_ERROR_MAX_BYTES)),
      Object.assign(new Error("broad"), { broad }),
      Object.assign(new Error("view"), { view: backing.subarray(9, 10) }),
    ]) {
      expect(prepareLiveError(error, admit)).toMatchObject({ props: { sqlState: "53200" } });
    }
    expect(admit).not.toHaveBeenCalled();
    expect(clone).not.toHaveBeenCalled();
  } finally {
    clone.mockRestore();
  }
});

it("does not invoke diagnostic getters or copy a value grown during admission", () => {
  let accessed = false;
  const accessor = new Error("accessor");
  Object.defineProperty(accessor, "details", {
    enumerable: true,
    get() {
      accessed = true;
      return "unexpected";
    },
  });
  const mutable = Object.assign(new Error("mutable"), { details: { text: "small" } });
  const clone = vi.spyOn(globalThis, "structuredClone");
  try {
    expect(prepareLiveError(accessor, () => true)).toMatchObject({ props: { sqlState: "53200" } });
    expect(accessed).toBe(false);
    expect(
      prepareLiveError(mutable, () => {
        mutable.details.text = "x".repeat(LIVE_ERROR_MAX_BYTES);
        return true;
      }),
    ).toMatchObject({ props: { sqlState: "53200" } });
    expect(clone).not.toHaveBeenCalled();
  } finally {
    clone.mockRestore();
  }
});

it("keeps the existing bounded cause depth and platform exception identity", () => {
  const error = new Error("cycle");
  error.cause = error;
  let wire = prepareLiveError(error, () => true);
  let depth = 0;
  while (wire.cause !== undefined) {
    wire = wire.cause;
    depth++;
  }
  expect(depth).toBe(MAX_SERIALIZED_CAUSE_DEPTH);
  expect(prepareLiveError(new DOMException("quota", "QuotaExceededError"), () => true)).toEqual({
    name: "QuotaExceededError",
    message: "quota",
    domException: true,
  });
});
