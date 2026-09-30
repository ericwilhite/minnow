import { expect, it } from "vitest";
import { serializeError } from "../worker-protocol/index.js";
import {
  PostingBuildConflictError,
  CompactionJobConflictError,
  TransactionRecordConflictError,
  TableRecordConflictError,
} from "../storage/types.js";
import { isMaintenanceContention } from "./maintenance-diagnostics.js";
function event(error: Error, context = "posting delta fold for t/c") {
  return { source: "worker", kind: "maintenance", context, error: serializeError(error) };
}
it.each([
  [
    new PostingBuildConflictError("build", "owner", "another live build exists"),
    "secondary index build for t/i",
  ],
  [
    new PostingBuildConflictError("build", "owner", "ownership changed or expired"),
    "posting delta fold for t/c",
  ],
  [new CompactionJobConflictError("job", 1, 2), "automatic compaction for t"],
  [new TransactionRecordConflictError("tx", 1, null), "automatic compaction for t"],
  [new TableRecordConflictError("table", 1, 3), "full-text index build for t/c"],
] as const)("classifies only typed maintenance contention: %s", (error, context) => {
  expect(isMaintenanceContention(event(error, context))).toBe(true);
});
it("rejects I/O, corruption, changed chunk replay, malformed props, and unrelated routes", () => {
  const contention = event(
    new PostingBuildConflictError("build", "owner", "another live build exists"),
  );
  for (const value of [
    event(new Error("Postings base build is owned by another caller")),
    event(new DOMException("quota", "QuotaExceededError")),
    event(new PostingBuildConflictError("build", "owner", "chunk replay changed")),
    { ...contention, source: "unhandled-rejection" },
    { ...contention, context: "opfs recovery" },
    { ...contention, error: { ...contention.error, props: {} } },
    event(new CompactionJobConflictError("job", 1, 1), "automatic compaction for t"),
  ])
    expect(isMaintenanceContention(value)).toBe(false);
});
