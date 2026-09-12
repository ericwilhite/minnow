import { expect, it } from "vitest";
import {
  ConnectionLostError,
  OpfsCoordinationError,
  OpfsUncertainOutcomeError,
  StorageCorruptionError,
  StorageFormatVersionError,
  StorageResourceLimitError,
  UnknownOutcomeError,
  WriteConflictError,
} from "../storage/types.js";
import {
  classifyError,
  DatabaseReadBacklogError,
  DatabaseWorkerFailedError,
  DatabaseWorkerOutcomeUnknownError,
  DatabaseWorkerTimeoutError,
  SqlCompileError,
  TransactionExpiredError,
  UniqueConstraintError,
  UnknownTableError,
} from "./errors.js";

it("answers the three questions that decide what to do next, for every kind", () => {
  const cases: Array<[unknown, ReturnType<typeof classifyError>]> = [
    [
      new OpfsUncertainOutcomeError("commitTransaction"),
      {
        kind: "unknown-outcome",
        mayHavePublished: true,
        retry: "after-reconcile",
        connectionUsable: true,
      },
    ],
    [
      new DatabaseWorkerOutcomeUnknownError("insert", "r1", {
        cause: new DatabaseWorkerTimeoutError("insert", 60_000),
      }),
      {
        kind: "unknown-outcome",
        mayHavePublished: true,
        retry: "after-reconcile",
        connectionUsable: false,
      },
    ],
    [
      new DatabaseWorkerOutcomeUnknownError("insert", "r2", { cause: new Error("cancelled") }),
      {
        kind: "unknown-outcome",
        mayHavePublished: true,
        retry: "after-reconcile",
        connectionUsable: true,
      },
    ],
    [
      new DatabaseWorkerTimeoutError("query", 1),
      { kind: "connection-lost", mayHavePublished: false, retry: "never", connectionUsable: false },
    ],
    [
      new DatabaseWorkerFailedError("error", "boom"),
      { kind: "connection-lost", mayHavePublished: false, retry: "never", connectionUsable: false },
    ],
    [
      new WriteConflictError(1, 2),
      { kind: "conflict", mayHavePublished: false, retry: "safe", connectionUsable: true },
    ],
    [
      new OpfsCoordinationError("leader-unavailable", "getBlock"),
      { kind: "transient", mayHavePublished: false, retry: "safe", connectionUsable: true },
    ],
    [
      new DatabaseReadBacklogError(),
      { kind: "transient", mayHavePublished: false, retry: "safe", connectionUsable: true },
    ],
    [
      new TransactionExpiredError(),
      { kind: "transient", mayHavePublished: false, retry: "safe", connectionUsable: true },
    ],
    [
      new StorageResourceLimitError("lease", 5, 4),
      {
        kind: "resource",
        mayHavePublished: false,
        retry: "after-reconcile",
        connectionUsable: true,
      },
    ],
    [
      new DOMException("full", "QuotaExceededError"),
      {
        kind: "resource",
        mayHavePublished: false,
        retry: "after-reconcile",
        connectionUsable: true,
      },
    ],
    [
      new StorageCorruptionError("opfs", "wal", "bad frame"),
      { kind: "corruption", mayHavePublished: false, retry: "never", connectionUsable: true },
    ],
    [
      new StorageFormatVersionError("opfs", "format.json", 9, 5, "newer"),
      { kind: "corruption", mayHavePublished: false, retry: "never", connectionUsable: true },
    ],
    [
      new UniqueConstraintError("items", "id", 1),
      { kind: "rejected", mayHavePublished: false, retry: "never", connectionUsable: true },
    ],
    [
      new UnknownTableError("nope"),
      { kind: "rejected", mayHavePublished: false, retry: "never", connectionUsable: true },
    ],
    [
      new SqlCompileError("bad", 0, 1),
      { kind: "rejected", mayHavePublished: false, retry: "never", connectionUsable: true },
    ],
    [
      new TypeError("wrong shape"),
      { kind: "rejected", mayHavePublished: false, retry: "never", connectionUsable: true },
    ],
    [
      Object.assign(new Error("stop"), { name: "AbortError" }),
      { kind: "cancelled", mayHavePublished: false, retry: "safe", connectionUsable: true },
    ],
    [
      new Error("mystery"),
      { kind: "other", mayHavePublished: false, retry: "never", connectionUsable: true },
    ],
    [
      "not even an error",
      { kind: "other", mayHavePublished: false, retry: "never", connectionUsable: true },
    ],
  ];
  for (const [error, expected] of cases) {
    expect(classifyError(error), String(error)).toEqual(expected);
  }
});

it("exposes the two marker bases through instanceof", () => {
  expect(new OpfsUncertainOutcomeError("x")).toBeInstanceOf(UnknownOutcomeError);
  expect(new DatabaseWorkerOutcomeUnknownError("x", "r")).toBeInstanceOf(UnknownOutcomeError);
  expect(new DatabaseWorkerTimeoutError("x", 1)).toBeInstanceOf(ConnectionLostError);
  expect(new DatabaseWorkerFailedError("messageerror", "x")).toBeInstanceOf(ConnectionLostError);
  expect(new WriteConflictError(1, 2)).not.toBeInstanceOf(UnknownOutcomeError);
});
