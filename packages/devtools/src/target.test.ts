import type { MinnowDatabase } from "@minnowdb/core";
import type { MinnowDatabaseClient } from "@minnowdb/core/client";
import { describe, expect, it } from "vitest";
import {
  isEditableTarget,
  isSnapshotTarget,
  resolveTarget,
  runsOffMainThread,
  type DevtoolsTarget,
  type SnapshotTarget,
} from "./target.js";

function stubTarget(): DevtoolsTarget {
  return {
    listTables: async () => [],
    query: async () => ({ columns: [], rows: [] }),
    explain: async () => "",
    execute: async () => ({ kind: "rows", result: { columns: [], rows: [] } }),
  };
}

describe("resolveTarget", () => {
  it("accepts anything with the four methods", () => {
    const target = stubTarget();
    expect(resolveTarget(target)).toBe(target);
  });

  it("unwraps a facade to the driver behind it", () => {
    const driver = stubTarget();
    expect(resolveTarget({ driver })).toBe(driver);
  });

  it("names what is missing rather than failing later inside the panel", () => {
    expect(() => resolveTarget({ query: () => undefined })).toThrow(/listTables/);
    expect(() => resolveTarget({ query: () => undefined })).toThrow(/explain/);
  });

  it("rejects non-objects", () => {
    expect(() => resolveTarget(undefined)).toThrow(TypeError);
    expect(() => resolveTarget("db")).toThrow(TypeError);
  });

  it("reports the outer object's gaps when the driver is no better", () => {
    expect(() => resolveTarget({ driver: {} })).toThrow(/listTables/);
  });
});

describe("isEditableTarget", () => {
  it("requires every keyed write method, since editing needs all three", () => {
    expect(isEditableTarget(stubTarget())).toBe(false);
    const writes = {
      insert: async () => undefined,
      updateBatch: async () => undefined,
      deleteBatch: async () => undefined,
    };
    expect(isEditableTarget(Object.assign(stubTarget(), writes))).toBe(true);
    expect(
      isEditableTarget(
        Object.assign(stubTarget(), { insert: writes.insert, updateBatch: writes.updateBatch }),
      ),
    ).toBe(false);
  });
});

describe("isSnapshotTarget", () => {
  it("requires both halves, since a download with no way back is half a feature", () => {
    expect(isSnapshotTarget(stubTarget())).toBe(false);
    const snapshots = {
      exportSnapshot: async () => new Uint8Array(0),
      importSnapshot: async () => undefined,
    };
    expect(isSnapshotTarget(Object.assign(stubTarget(), snapshots))).toBe(true);
    expect(
      isSnapshotTarget(Object.assign(stubTarget(), { exportSnapshot: snapshots.exportSnapshot })),
    ).toBe(false);
  });

  it("is satisfied by the real database and the worker client", () => {
    // Type-level only: the capability is declared structurally so both classes qualify as they
    // are. If either signature drifts, this stops compiling.
    const fromDatabase: (database: MinnowDatabase) => SnapshotTarget = (database) => database;
    const fromClient: (client: MinnowDatabaseClient) => SnapshotTarget = (client) => client;
    expect([fromDatabase, fromClient].every((narrow) => typeof narrow === "function")).toBe(true);
  });
});

describe("runsOffMainThread", () => {
  it("recognises the worker client by its ready handshake", () => {
    expect(runsOffMainThread(stubTarget())).toBe(false);
    const client: DevtoolsTarget = Object.assign(stubTarget(), {
      ready: async () => undefined,
    });
    expect(runsOffMainThread(client)).toBe(true);
  });
});
