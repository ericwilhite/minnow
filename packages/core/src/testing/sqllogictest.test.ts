import { describe, expect, it, vi } from "vitest";

import { MinnowDatabase } from "../engine/index.js";
import { MemoryBlockStore } from "../storage/index.js";
import {
  SqlLogicFailure,
  SqlLogicParseError,
  md5Hex,
  parseSqlLogicTest,
  parseSqlLogicTestLines,
  renderSqlLogicValue,
  runSqlLogicTest,
  type SqlLogicDatabase,
} from "./sqllogictest.js";

describe("SQLLogicTest parser", () => {
  it("parses original records, comments, conditionals, labels, hashes, and CRLF", () => {
    const records = parseSqlLogicTest(
      [
        "# source comment",
        "statement ok",
        "CREATE TABLE t (id INTEGER)",
        "",
        "skipif sqlite",
        "onlyif minnow",
        "query IT rowsort same-result",
        "# a comment inside SQL is removed without splitting the record",
        "SELECT id, 'x' FROM t",
        "----",
        "2 values hashing to 0123456789abcdef0123456789abcdef",
        "",
        "hash-threshold 20",
        "",
        "halt",
        "",
      ].join("\r\n"),
      "sample.test",
    );

    expect(records).toEqual([
      {
        kind: "statement",
        expectation: "ok",
        sql: "CREATE TABLE t (id INTEGER)",
        conditions: [],
        location: { file: "sample.test", line: 2 },
      },
      {
        kind: "query",
        types: ["I", "T"],
        sortMode: "rowsort",
        label: "same-result",
        sql: "SELECT id, 'x' FROM t",
        expected: {
          kind: "hash",
          valueCount: 2,
          hash: "0123456789abcdef0123456789abcdef",
        },
        conditions: [
          { kind: "skipif", engine: "sqlite", location: { file: "sample.test", line: 5 } },
          { kind: "onlyif", engine: "minnow", location: { file: "sample.test", line: 6 } },
        ],
        location: { file: "sample.test", line: 7 },
      },
      {
        kind: "hash-threshold",
        valueCount: 20,
        location: { file: "sample.test", line: 13 },
      },
      { kind: "halt", location: { file: "sample.test", line: 15 } },
    ]);
  });

  it("defaults queries to nosort and an empty result", () => {
    expect(parseSqlLogicTest("query I\nSELECT 1\n", "empty.test")).toEqual([
      {
        kind: "query",
        types: ["I"],
        sortMode: "nosort",
        sql: "SELECT 1",
        expected: { kind: "values", values: [] },
        conditions: [],
        location: { file: "empty.test", line: 1 },
      },
    ]);
  });

  it("streams records without retaining a whole corpus file", async () => {
    async function* lines(): AsyncGenerator<string> {
      yield "# ignored";
      yield "statement ok\r";
      yield "CREATE TABLE t (id INTEGER)\r";
      yield "\r";
      yield "query I";
      yield "SELECT 1";
      yield "----";
      yield "1";
    }

    const records = [];
    for await (const record of parseSqlLogicTestLines(lines(), "stream.test")) {
      records.push(record);
    }
    expect(records).toEqual(
      parseSqlLogicTest(
        "# ignored\nstatement ok\nCREATE TABLE t (id INTEGER)\n\nquery I\nSELECT 1\n----\n1",
        "stream.test",
      ),
    );
  });

  it.each([
    ["statement maybe\nSELECT 1", "statement must be"],
    ["query IX\nSELECT 1\n----\n1", "type string"],
    ["query I shuffle\nSELECT 1\n----\n1", "sort mode"],
    ["skipif minnow", "missing its statement"],
    ["hash-threshold -1", "non-negative whole number"],
    ["unknown", "unknown SQLLogicTest directive"],
  ])("rejects malformed input: %s", (source, message) => {
    expect(() => parseSqlLogicTest(source, "bad.test")).toThrow(SqlLogicParseError);
    expect(() => parseSqlLogicTest(source, "bad.test")).toThrow(message);
  });
});

describe("SQLLogicTest canonicalization", () => {
  it("matches standard MD5 vectors", () => {
    expect(md5Hex("")).toBe("d41d8cd98f00b204e9800998ecf8427e");
    expect(md5Hex("abc")).toBe("900150983cd24fb0d6963f7d28e17f72");
    expect(md5Hex("The quick brown fox jumps over the lazy dog")).toBe(
      "9e107d9d372bb6826bd81d3542a419d6",
    );
  });

  it("renders nulls, empty text, control characters, integers, reals, booleans, and dates", () => {
    expect(renderSqlLogicValue(null, "T")).toBe("NULL");
    expect(renderSqlLogicValue("", "T")).toBe("(empty)");
    expect(renderSqlLogicValue("a\nb\u007fc", "T")).toBe("a@b@c");
    expect(renderSqlLogicValue(4.9, "I")).toBe("4");
    expect(renderSqlLogicValue(-4.9, "I")).toBe("-4");
    expect(renderSqlLogicValue(1.2346, "R")).toBe("1.235");
    expect(renderSqlLogicValue(true, "I")).toBe("1");
    expect(renderSqlLogicValue(new Date("2026-01-02T03:04:05.000Z"), "T")).toBe(
      "2026-01-02T03:04:05.000Z",
    );
  });
});

describe("SQLLogicTest runner", () => {
  it("runs statements and all sort modes, verifies labels, and reports exact statistics", async () => {
    const database = fakeDatabase({
      "SELECT rows": {
        columns: ["a", "b"],
        rows: [
          { a: 2, b: "b" },
          { a: 1, b: "z" },
        ],
      },
      "SELECT values": {
        columns: ["a", "b"],
        rows: [
          { a: 2, b: 1 },
          { a: 4, b: 3 },
        ],
      },
    });
    const records = parseSqlLogicTest(
      [
        "statement ok",
        "CREATE TABLE t(a INTEGER)",
        "",
        "statement error",
        "FAIL",
        "",
        "hash-threshold 0",
        "",
        "query IT rowsort equivalent",
        "SELECT rows",
        "----",
        "1",
        "z",
        "2",
        "b",
        "",
        "query II valuesort",
        "SELECT values",
        "----",
        "1",
        "2",
        "3",
        "4",
      ].join("\n"),
    );

    await expect(runSqlLogicTest(records, database)).resolves.toEqual({
      files: 1,
      statements: 2,
      queries: 2,
      values: 8,
      skipped: 0,
      halted: false,
      hashThreshold: 0,
    });
    expect(database.closeSpy).toHaveBeenCalledOnce();
  });

  it("runs conditionals case-insensitively and never hides their count", async () => {
    const database = fakeDatabase({ "SELECT 2": { columns: ["n"], rows: [{ n: 2 }] } });
    const records = parseSqlLogicTest(
      [
        "skipif MINNOW",
        "statement ok",
        "DO NOT RUN",
        "",
        "onlyif minnow",
        "query I",
        "SELECT 2",
        "----",
        "2",
      ].join("\n"),
    );

    await expect(runSqlLogicTest(records, database)).resolves.toMatchObject({
      statements: 0,
      queries: 1,
      skipped: 1,
    });
    expect(database.executeSpy).not.toHaveBeenCalled();
  });

  it("checks hashed results and closes the database on failure", async () => {
    const database = fakeDatabase({
      "SELECT n": { columns: ["n"], rows: [{ n: 1 }, { n: 2 }] },
    });
    const correct = md5Hex("1\n2\n");
    const records = parseSqlLogicTest(
      `hash-threshold 1\n\nquery I\nSELECT n\n----\n2 values hashing to ${correct}\n`,
      "hash.test",
    );

    await expect(runSqlLogicTest(records, database)).resolves.toMatchObject({ values: 2 });
    const wrongHash = parseSqlLogicTest(
      "hash-threshold 1\n\nquery I\nSELECT n\n----\n2 values hashing to d41d8cd98f00b204e9800998ecf8427e\n",
    );
    await expect(
      runSqlLogicTest(
        wrongHash,
        fakeDatabase({
          "SELECT n": { columns: ["n"], rows: [{ n: 1 }, { n: 2 }] },
        }),
      ),
    ).rejects.toThrow("wrong result hash");
    await expect(runSqlLogicTest(records, fakeDatabase({}))).rejects.toThrow("query failed");
    expect(database.closeSpy).toHaveBeenCalledOnce();
  });

  it("enforces hash-threshold in verification mode exactly", async () => {
    const results = { "SELECT n": { columns: ["n"], rows: [{ n: 1 }, { n: 2 }] } };
    const literalAboveThreshold = parseSqlLogicTest(
      "hash-threshold 1\n\nquery I\nSELECT n\n----\n1\n2\n",
    );
    await expect(runSqlLogicTest(literalAboveThreshold, fakeDatabase(results))).rejects.toThrow(
      "expected result is not hashed",
    );

    const hashBelowThreshold = parseSqlLogicTest(
      `hash-threshold 0\n\nquery I\nSELECT n\n----\n2 values hashing to ${md5Hex("1\n2\n")}\n`,
    );
    await expect(runSqlLogicTest(hashBelowThreshold, fakeDatabase(results))).rejects.toThrow(
      "do not exceed hash-threshold",
    );
  });

  it("can audit later independent queries after recording a failure", async () => {
    const database = fakeDatabase({ "SELECT 2": { columns: ["n"], rows: [{ n: 2 }] } });
    const failures: SqlLogicFailure[] = [];
    const records = parseSqlLogicTest(
      "query I\nSELECT missing\n----\n1\n\nquery I\nSELECT 2\n----\n2\n",
    );
    const statistics = await runSqlLogicTest(records, database, {
      onFailure: (failure) => {
        failures.push(failure);
        return "continue";
      },
    });

    expect(failures).toHaveLength(1);
    expect(statistics).toMatchObject({ queries: 2, values: 1 });
  });

  it("runs the format through Minnow's public SQL and storage APIs", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      compression: "raw",
      autoCompact: false,
      autoCollect: false,
      createId: sequentialIds(),
    });
    const records = parseSqlLogicTest(
      [
        "statement ok",
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, amount REAL)",
        "",
        "statement ok",
        "INSERT INTO items (id, name, amount) VALUES (2, '', 1.25), (1, 'a', 2.5)",
        "",
        "query ITR rowsort",
        "SELECT id, name, amount FROM items",
        "----",
        "1",
        "a",
        "2.500",
        "2",
        "(empty)",
        "1.250",
      ].join("\n"),
      "minnow.slt",
    );

    await expect(
      runSqlLogicTest(records, {
        engineName: "minnow",
        execute: (sql) => database.execute(sql),
        query: (sql) => database.query(sql, { memoize: false }),
        close: () => database.close(),
      }),
    ).resolves.toMatchObject({ statements: 2, queries: 1, values: 6 });
  });

  it("preserves positional results when unnamed expressions share a default label", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      compression: "raw",
      autoCompact: false,
      autoCollect: false,
      createId: sequentialIds(),
    });
    const result = await database.query("SELECT 1 + 2, 3 + 4");
    expect(result).toEqual({
      columns: ["expression", "expression_2"],
      columnDomains: [null, null],
      rows: [{ expression: 3, expression_2: 7 }],
    });
    await database.close();
  });

  it("accepts the standard INSERT form that omits a target column list", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      compression: "raw",
      autoCompact: false,
      autoCollect: false,
      createId: sequentialIds(),
    });
    await database.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)");
    await database.execute("INSERT INTO items VALUES (1, 'one'), (2, 'two')");
    await expect(database.query("SELECT id, name FROM items ORDER BY id")).resolves.toEqual({
      columns: ["id", "name"],
      columnDomains: [null, null],
      rows: [
        { id: 1, name: "one" },
        { id: 2, name: "two" },
      ],
    });
    await expect(database.execute("INSERT INTO items VALUES (3)")).rejects.toThrow(
      "table column count",
    );
    await database.close();
  });
});

function fakeDatabase(
  results: Record<
    string,
    Omit<Awaited<ReturnType<SqlLogicDatabase["query"]>>, "columnDomains"> & {
      columnDomains?: Awaited<ReturnType<SqlLogicDatabase["query"]>>["columnDomains"];
    }
  >,
): SqlLogicDatabase & {
  executeSpy: ReturnType<typeof vi.fn<SqlLogicDatabase["execute"]>>;
  closeSpy: ReturnType<typeof vi.fn<SqlLogicDatabase["close"]>>;
} {
  const executeSpy = vi.fn(async (sql: string) => {
    if (sql === "FAIL") throw new Error("expected failure");
  });
  const closeSpy = vi.fn();
  return {
    engineName: "minnow",
    execute: executeSpy,
    query: vi.fn(async (sql: string) => {
      const result = results[sql];
      if (result === undefined) throw new Error(`unexpected query: ${sql}`);
      return {
        ...result,
        columnDomains: result.columnDomains ?? result.columns.map(() => null),
      };
    }),
    close: closeSpy,
    executeSpy,
    closeSpy,
  };
}

function sequentialIds(): () => string {
  let next = 0;
  return () => `slt-${String(next++)}`;
}
