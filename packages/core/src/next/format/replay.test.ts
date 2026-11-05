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

describe("large transactions committed through a staged root (Phase B ruling)", () => {
  const staged = (flushedThroughLsn: number, rootSequence = 9) =>
    new Replayer({
      rootSequence,
      globalLsn: 0,
      nextTableId: 2,
      tables: new Map([
        [
          1,
          {
            flushedThroughLsn,
            schemaVersion: 1,
            staged: new Map(flushedThroughLsn >= 4 ? [] : [[7, { indexIds: [1, 2] }]]),
          },
        ],
      ]),
    });
  const commit = {
    kind: "publishedRows",
    tableId: 1,
    schemaVersion: 1,
    transaction: 7,
    rootSequence: 9,
    indexIds: [2, 1],
  } as const;

  it("applies a committed staging exactly once, and skips it once a later root folded it", () => {
    const replayer = staged(0);
    expect(replayer.next(4, [commit])).toEqual([true]);
    expect(replayer.unclaimedStaging()).toEqual([]);
    expect(() => replayer.next(5, [commit])).toThrow(LogCorruptionError);
    // A later root folded the rows (watermark past the frame, staging removed): skipped, not doubled.
    expect(staged(4).next(4, [commit])).toEqual([false]);
  });

  it("detects a rolled-back staging root and reports staging no commit claimed", () => {
    expect(() => staged(0, 8).next(4, [commit])).toThrow(LogCorruptionError);
    const replayer = staged(0);
    replayer.next(4, [{ kind: "sequence" }]);
    expect(replayer.unclaimedStaging()).toEqual([{ tableId: 1, transaction: 7, indexIds: [1, 2] }]);
  });
});

describe("published rows of a dropped table (review A1-4 finding 1)", () => {
  const published = {
    kind: "publishedRows",
    tableId: 1,
    schemaVersion: 1,
    transaction: 7,
    rootSequence: 9,
    indexIds: [1],
  } as const;

  it("skips them like ordinary rows when a checkpointed drop removed the table (review probe)", () => {
    for (const effect of [published, { kind: "rows", tableId: 1, schemaVersion: 1 } as const]) {
      const replayer = new Replayer({
        rootSequence: 10,
        globalLsn: 5,
        nextTableId: 2,
        tables: new Map(),
      });
      expect(replayer.next(4, [effect]), effect.kind).toEqual([false]);
    }
  });

  it("rejects them for an unknown table or after the drop", () => {
    expect(() =>
      new Replayer({ rootSequence: 10, globalLsn: 5, nextTableId: 1, tables: new Map() }).next(4, [
        published,
      ]),
    ).toThrow(LogCorruptionError);
    expect(() =>
      new Replayer({ rootSequence: 10, globalLsn: 5, nextTableId: 2, tables: new Map() }).next(6, [
        published,
      ]),
    ).toThrow(LogCorruptionError);
  });
});

describe("staged index runs travel with staged rows (ruling R13)", () => {
  const checkpoint = (indexIds: number[]) =>
    new Replayer({
      rootSequence: 9,
      globalLsn: 0,
      nextTableId: 2,
      tables: new Map([
        [1, { flushedThroughLsn: 0, schemaVersion: 1, staged: new Map([[7, { indexIds }]]) }],
      ]),
    });
  const commit = (indexIds: number[]) =>
    ({
      kind: "publishedRows",
      tableId: 1,
      schemaVersion: 1,
      transaction: 7,
      rootSequence: 9,
      indexIds,
    }) as const;

  it("applies rows and runs together when the commit names exactly the staged runs", () => {
    expect(checkpoint([1, 2]).next(3, [commit([2, 1])])).toEqual([true]);
  });

  it("refuses a commit whose runs differ from the staged runs, either way", () => {
    expect(() => checkpoint([1, 2]).next(3, [commit([1])])).toThrow(LogCorruptionError);
    expect(() => checkpoint([1]).next(3, [commit([1, 2])])).toThrow(LogCorruptionError);
  });
});
