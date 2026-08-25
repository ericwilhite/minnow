import { describe, expect, it } from "vitest";
import { StorageCorruptionError, StorageFormatVersionError } from "../types.js";
import * as storageTypes from "../types.js";
import {
  estimateRpcValueBytes,
  fingerprintStoreRequest,
  MAX_OPFS_RPC_IDENTIFIER_CHARACTERS,
  MAX_OPFS_RPC_MESSAGE_BYTES,
  parseStoreRpcMessage,
  rehydrateStoreError,
  serializeStoreError,
} from "./rpc.js";

type StorageErrorConstructor = new (...args: never[]) => Error;

function exportedStorageErrorConstructors(): Array<readonly [string, StorageErrorConstructor]> {
  const constructors: Array<readonly [string, StorageErrorConstructor]> = [];
  for (const [name, value] of Object.entries(storageTypes)) {
    if (typeof value === "function" && value.prototype instanceof Error) {
      constructors.push([name, value as StorageErrorConstructor]);
    }
  }
  return constructors.sort(([left], [right]) => left.localeCompare(right));
}

function createStorageErrorFixture(constructor: StorageErrorConstructor): Error {
  return new (constructor as unknown as new (...args: unknown[]) => Error)(
    "boundary-fixture",
    17,
    23,
    29,
    "newer",
    false,
  );
}

describe("OPFS RPC errors", () => {
  it("preserves typed corruption identity and location across a follower hop", () => {
    const original = new StorageCorruptionError("opfs", "checkpoint-a", "checksum mismatch");
    const rehydrated = rehydrateStoreError(serializeStoreError(original));
    expect(rehydrated).toBeInstanceOf(StorageCorruptionError);
    expect(rehydrated).toMatchObject({
      name: "StorageCorruptionError",
      backend: "opfs",
      location: "checkpoint-a",
    });
  });

  it("preserves typed format-version guidance across a follower hop", () => {
    const original = new StorageFormatVersionError("opfs", "format.json", 6, 5, "newer");
    const rehydrated = rehydrateStoreError(serializeStoreError(original));
    expect(rehydrated).toBeInstanceOf(StorageFormatVersionError);
    expect(rehydrated).toMatchObject({
      name: "StorageFormatVersionError",
      backend: "opfs",
      location: "format.json",
      actualVersion: 6,
      supportedVersion: 5,
      relation: "newer",
    });
  });

  it("round-trips every exported storage error prototype and field", () => {
    const constructors = exportedStorageErrorConstructors();
    expect(constructors.length).toBeGreaterThan(0);
    for (const [exportName, constructor] of constructors) {
      const original = createStorageErrorFixture(constructor);
      const rehydrated = rehydrateStoreError(serializeStoreError(original));
      expect(rehydrated, exportName).toBeInstanceOf(constructor);
      expect(rehydrated, exportName).toMatchObject({
        name: original.name,
        message: original.message,
        ...Object.fromEntries(Object.entries(original)),
      });
    }
  });
});

describe("OPFS RPC trust boundary", () => {
  const methods = new Set(["getBlock", "stageTransactionArtifacts"]);

  it("totally ignores malformed, unknown, oversized, cyclic, and extra-field frames", () => {
    const cyclic: unknown[] = [];
    cyclic.push(cyclic);
    for (const value of [
      null,
      {},
      { kind: "unknown" },
      { kind: "ping", extra: true },
      { kind: "op", requestId: "r", from: "f", method: "unknown", args: [] },
      {
        kind: "op",
        requestId: "r".repeat(MAX_OPFS_RPC_IDENTIFIER_CHARACTERS + 1),
        from: "f",
        method: "getBlock",
        args: [],
      },
      { kind: "op", requestId: "r", from: "f", method: "getBlock", args: cyclic },
      { kind: "result", requestId: "r", ok: false, error: { name: "Error" } },
    ]) {
      expect(parseStoreRpcMessage(value, methods)).toBeUndefined();
    }
    expect(
      parseStoreRpcMessage(
        { kind: "op", requestId: "r", from: "f", method: "getBlock", args: ["id"] },
        methods,
      ),
    ).toBeDefined();
  });

  it("bounds retained values and fingerprints byte-heavy requests without retaining them", async () => {
    const bytes = new Uint8Array(1024 * 1024);
    bytes[bytes.length - 1] = 1;
    const first = await fingerprintStoreRequest("stageTransactionArtifacts", [{ bytes }]);
    const same = await fingerprintStoreRequest("stageTransactionArtifacts", [{ bytes }]);
    const changedBytes = bytes.slice();
    changedBytes[changedBytes.length - 1] = 2;
    const changed = await fingerprintStoreRequest("stageTransactionArtifacts", [
      { bytes: changedBytes },
    ]);
    expect(first.signature).toBe(same.signature);
    expect(changed.signature).not.toBe(first.signature);
    expect(first.retainedBytes).toBeGreaterThan(bytes.byteLength);
    expect(() => estimateRpcValueBytes(new Uint8Array(MAX_OPFS_RPC_MESSAGE_BYTES + 1))).toThrow(
      RangeError,
    );
  });

  it("accepts every exact protocol shape and rejects each invalid discriminator field", () => {
    const valid = [
      { kind: "op", requestId: "request", from: "follower", method: "getBlock", args: ["id"] },
      { kind: "result", requestId: "request", ok: true, value: { answer: 1 } },
      {
        kind: "result",
        requestId: "request",
        ok: false,
        error: { name: "Error", message: "failed" },
      },
      {
        kind: "result",
        requestId: "request",
        ok: false,
        error: { name: "QuotaExceededError", message: "full", domException: true },
      },
      {
        kind: "result",
        requestId: "request",
        ok: false,
        error: { name: "Error", message: "failed", props: { code: 1 } },
      },
      {
        kind: "result",
        requestId: "request",
        ok: false,
        error: { name: "Error", message: "failed", domException: true, props: {} },
      },
      { kind: "busy", requestId: "request" },
      { kind: "leader", leaderId: "leader" },
      { kind: "released", leaderId: "leader" },
      { kind: "ping" },
      { kind: "bid", bidderId: "bidder", foreground: true },
      { kind: "yield", to: "bidder" },
    ];
    for (const frame of valid) expect(parseStoreRpcMessage(frame, methods)).toEqual(frame);

    const invalid = [
      [],
      { kind: "op", requestId: "", from: "f", method: "getBlock", args: [] },
      { kind: "op", requestId: "r", from: "", method: "getBlock", args: [] },
      { kind: "op", requestId: "r", from: "f", method: "", args: [] },
      { kind: "op", requestId: "r", from: "f", method: "getBlock", args: {} },
      { kind: "result", requestId: "", ok: true, value: null },
      { kind: "result", requestId: "r", ok: "yes", value: null },
      { kind: "result", requestId: "r", ok: true, value: null, error: null },
      { kind: "result", requestId: "r", ok: false, error: null },
      { kind: "result", requestId: "r", ok: false, error: [] },
      { kind: "result", requestId: "r", ok: false, error: { name: "", message: "x" } },
      { kind: "result", requestId: "r", ok: false, error: { name: "Error", message: 1 } },
      {
        kind: "result",
        requestId: "r",
        ok: false,
        error: { name: "Error", message: "x", domException: false },
      },
      {
        kind: "result",
        requestId: "r",
        ok: false,
        error: { name: "Error", message: "x", props: [] },
      },
      { kind: "busy", requestId: "" },
      { kind: "leader", leaderId: "", extra: true },
      { kind: "released", leaderId: 1 },
      { kind: "bid", bidderId: "bidder", foreground: "yes" },
      { kind: "yield", to: "" },
    ];
    for (const frame of invalid) expect(parseStoreRpcMessage(frame, methods)).toBeUndefined();
  });

  it("estimates every cloneable scalar and container while refusing hostile structures", () => {
    expect(estimateRpcValueBytes(null)).toBe(8);
    expect(estimateRpcValueBytes(undefined)).toBe(8);
    expect(estimateRpcValueBytes(false)).toBe(8);
    expect(estimateRpcValueBytes(1)).toBe(16);
    expect(estimateRpcValueBytes(1n)).toBe(16);
    expect(estimateRpcValueBytes("abc")).toBe(22);
    expect(estimateRpcValueBytes(new ArrayBuffer(4))).toBe(36);
    expect(estimateRpcValueBytes(new Uint16Array(3))).toBe(54);
    expect(estimateRpcValueBytes(new Date(0))).toBe(32);
    expect(estimateRpcValueBytes([1, true])).toBe(56);
    expect(estimateRpcValueBytes(Object.assign(Object.create(null) as object, { key: "v" }))).toBe(
      56,
    );

    expect(() => estimateRpcValueBytes(Symbol("no-clone"))).toThrow(/not cloneable/);
    expect(() => estimateRpcValueBytes(new Map())).toThrow(/unsupported prototype/);
    const cycle: Record<string, unknown> = {};
    cycle.self = cycle;
    expect(() => estimateRpcValueBytes(cycle)).toThrow(/cycles/);
    let nested: unknown = null;
    for (let index = 0; index < 66; index += 1) nested = [nested];
    expect(() => estimateRpcValueBytes(nested)).toThrow(/structural limit/);
    expect(() => estimateRpcValueBytes("x".repeat(MAX_OPFS_RPC_MESSAGE_BYTES / 2 + 1))).toThrow(
      /byte limit/,
    );
    if (typeof SharedArrayBuffer !== "undefined") {
      expect(() => estimateRpcValueBytes(new SharedArrayBuffer(8))).toThrow(/SharedArrayBuffer/);
      expect(() => estimateRpcValueBytes(new Uint8Array(new SharedArrayBuffer(8)))).toThrow(
        /SharedArrayBuffer/,
      );
    }
  });

  it("fingerprints all canonical leaf forms, object order, view slices, cycles, and depth", async () => {
    const buffer = Uint8Array.of(0, 1, 2, 3).buffer;
    const ordered = await fingerprintStoreRequest("method", [
      undefined,
      1n,
      -0,
      Number.NaN,
      buffer,
      new Uint8Array(buffer),
      new Uint8Array(buffer, 1, 2),
      new Date(0),
      { z: 1, a: [true, null] },
    ]);
    const reordered = await fingerprintStoreRequest("method", [
      undefined,
      1n,
      -0,
      Number.NaN,
      buffer,
      new Uint8Array(buffer),
      new Uint8Array(buffer, 1, 2),
      new Date(0),
      { a: [true, null], z: 1 },
    ]);
    expect(ordered).toEqual(reordered);

    const cycle: unknown[] = [];
    cycle.push(cycle);
    await expect(fingerprintStoreRequest("cycle", cycle)).rejects.toThrow(/cycles/);
    let nested: unknown = null;
    for (let index = 0; index < 66; index += 1) nested = [nested];
    await expect(fingerprintStoreRequest("deep", [nested])).rejects.toThrow(
      /structural limit|depth/,
    );
  });

  it("serializes platform, scalar, cloneable, and non-cloneable errors without poisoning RPC", () => {
    const dom = serializeStoreError(new DOMException("disk full", "QuotaExceededError"));
    expect(dom).toEqual({
      name: "QuotaExceededError",
      message: "disk full",
      domException: true,
    });
    expect(rehydrateStoreError(dom)).toBeInstanceOf(DOMException);
    expect(serializeStoreError(17)).toEqual({ name: "Error", message: "17" });

    const original = new Error("mixed") as Error & { code: number; callback: () => void };
    original.code = 42;
    original.callback = () => undefined;
    expect(serializeStoreError(original)).toEqual({
      name: "Error",
      message: "mixed",
      props: { code: 42 },
    });
    const unknown = rehydrateStoreError({
      name: "FutureError",
      message: "future",
      props: { x: 1 },
    });
    expect(unknown).toBeInstanceOf(Error);
    expect(unknown).toMatchObject({ name: "FutureError", message: "future", x: 1 });
  });
});
