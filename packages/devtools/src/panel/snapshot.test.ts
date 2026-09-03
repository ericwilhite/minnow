import type { SnapshotSummary } from "@minnowdb/core/storage/snapshots";
import { describe, expect, it } from "vitest";
import { confirmRestore, describeExport, describeLoad, snapshotFileName } from "./snapshot.js";
import { formatBytes } from "../format.js";

const summary: SnapshotSummary = {
  formatVersion: 1,
  version: 42,
  createdAt: "2026-08-16T10:11:12.000Z",
  tableCount: 5,
  blockCount: 31,
  payloadBytes: 3_100_000,
  byteLength: 3_145_728,
};

describe("formatBytes", () => {
  it("keeps a size readable at every scale", () => {
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(999)).toBe("999 B");
    expect(formatBytes(1024)).toBe("1.0 KB");
    expect(formatBytes(1024 * 1024)).toBe("1.0 MB");
    expect(formatBytes(12 * 1024 * 1024)).toBe("12 MB");
    expect(formatBytes(3 * 1024 * 1024 * 1024)).toBe("3.0 GB");
    // Nothing above the largest unit; a terabyte snapshot still reads as gigabytes.
    expect(formatBytes(4096 * 1024 * 1024 * 1024)).toBe("4096 GB");
  });
});

describe("snapshotFileName", () => {
  it("names the file after the version inside it and the day it was taken", () => {
    expect(snapshotFileName(summary, new Date("2026-08-17T23:30:00Z"))).toBe(
      "minnow-v42-2026-08-17.minnow",
    );
  });
});

describe("describeExport", () => {
  it("says what is happening at each phase", () => {
    expect(describeExport({ phase: "reading", transferredBytes: 0, totalBytes: 0 })).toMatch(
      /reading/,
    );
    expect(describeExport({ phase: "transfer", transferredBytes: 512, totalBytes: 2048 })).toBe(
      "copying 25%",
    );
    expect(describeExport({ phase: "done", transferredBytes: 2048, totalBytes: 2048 })).toBe(
      "read 2.0 KB",
    );
  });

  it("does not divide by a total it does not have yet", () => {
    expect(describeExport({ phase: "transfer", transferredBytes: 0, totalBytes: 0 })).toBe(
      "copying 0%",
    );
  });
});

describe("describeLoad", () => {
  it("reports blocks by fraction and the catalog by name", () => {
    expect(describeLoad({ phase: "blocks", writtenBytes: 3, totalBytes: 4 })).toBe("writing 75%");
    expect(describeLoad({ phase: "catalog", writtenBytes: 4, totalBytes: 4 })).toMatch(/catalog/);
    expect(describeLoad({ phase: "done", writtenBytes: 4096, totalBytes: 4096 })).toBe(
      "wrote 4.0 KB",
    );
  });
});

describe("confirmRestore", () => {
  it("spells out what the picked file holds before any of it is loaded", () => {
    const request = confirmRestore("backup.minnow", summary);
    expect(Object.fromEntries(request.facts)).toEqual({
      file: "backup.minnow",
      size: "3.0 MB",
      version: "42",
      tables: "5",
      taken: "2026-08-16T10:11:12.000Z",
      call: "importSnapshot(bytes)",
    });
    // The empty-store rule is the one thing that decides whether the load can work at all.
    expect(request.warning).toMatch(/has to be empty/);
    expect(request.confirmLabel).toBe("Restore database");
  });
});
