import { describe, expect, it } from "vitest";
import { LogCorruptionError } from "./log-frame.js";
import { Replayer } from "./replay.js";

const empty = () => new Replayer({ globalLsn: 0, nextTableId: 1, tables: new Map() });

describe("replay decisions", () => {
  it("replays rows of a table created in the same transaction (review probe)", () => {
    expect(
      empty().next(1, [
        { kind: "createTable", tableId: 1 },
        { kind: "rows", tableId: 1, schemaVersion: 1 },
      ]),
    ).toEqual([true, true]);
  });

  it("applies several DDL statements and a mid-transaction schema change in order", () => {
    expect(
      empty().next(1, [
        { kind: "createTable", tableId: 1 },
        { kind: "rows", tableId: 1, schemaVersion: 1 },
        { kind: "alterTable", tableId: 1, schemaVersion: 2 },
        { kind: "rows", tableId: 1, schemaVersion: 2 },
        { kind: "createTable", tableId: 2 },
        { kind: "dropTable", tableId: 1 },
        { kind: "rows", tableId: 2, schemaVersion: 1 },
      ]),
    ).toEqual([true, true, true, true, true, true, true]);
    const replayer = empty();
    expect(() =>
      replayer.next(1, [
        { kind: "createTable", tableId: 1 },
        { kind: "dropTable", tableId: 1 },
        { kind: "rows", tableId: 1, schemaVersion: 1 },
      ]),
    ).toThrow(LogCorruptionError);
  });

  it("skips global effects and flushed rows at or below the checkpoints", () => {
    const replayer = new Replayer({
      globalLsn: 5,
      nextTableId: 3,
      tables: new Map([[1, { flushedThroughLsn: 6, schemaVersion: 2 }]]),
    });
    expect(
      replayer.next(5, [
        { kind: "sequence" },
        { kind: "rows", tableId: 1, schemaVersion: 2 },
        { kind: "rows", tableId: 2, schemaVersion: 1 },
      ]),
    ).toEqual([false, false, false]);
    expect(
      replayer.next(7, [{ kind: "request" }, { kind: "rows", tableId: 1, schemaVersion: 2 }]),
    ).toEqual([true, true]);
    expect(() => replayer.next(8, [{ kind: "rows", tableId: 2, schemaVersion: 1 }])).toThrow(
      LogCorruptionError,
    );
    expect(() => replayer.next(9, [{ kind: "rows", tableId: 1, schemaVersion: 1 }])).toThrow(
      LogCorruptionError,
    );
  });
});
