import { describe, expect, it } from "vitest";
import type { DatabaseRow } from "./database.js";
import { QueryMemoryBudgetError } from "./memory.js";
import {
  compileQuery,
  createPreparedColumnarQuery,
  createPreparedQuery,
  executeQuery,
  executeRowQuery,
} from "./query.js";
import { createColumnarTable, type QuerySpillStore } from "./vector.js";

class TestSpillStore implements QuerySpillStore {
  readonly pages = new Map<string, Uint8Array>();
  putCount = 0;

  async putPage(ownerId: string, runId: string, pageIndex: number, bytes: Uint8Array) {
    this.putCount += 1;
    this.pages.set(`${ownerId}/${runId}/${String(pageIndex)}`, bytes.slice());
  }

  async getPage(ownerId: string, runId: string, pageIndex: number) {
    return this.pages.get(`${ownerId}/${runId}/${String(pageIndex)}`)?.slice();
  }

  async removeRun(ownerId: string, runId: string) {
    const prefix = `${ownerId}/${runId}/`;
    for (const key of this.pages.keys()) if (key.startsWith(prefix)) this.pages.delete(key);
  }

  async removeOwner(ownerId: string) {
    const prefix = `${ownerId}/`;
    for (const key of this.pages.keys()) if (key.startsWith(prefix)) this.pages.delete(key);
  }
}

describe("vector query execution", () => {
  it("recomputes BM25 corpus statistics per execution instead of freezing them into the plan", () => {
    const plan = compileQuery("SELECT id, BM25(text) AGAINST 'quick' AS score FROM rows");
    const small = new Map<string, DatabaseRow[]>([["rows", [{ id: 1, text: "quick" }]]]);
    const large = new Map<string, DatabaseRow[]>([
      [
        "rows",
        [
          { id: 1, text: "quick" },
          { id: 2, text: "quick slow" },
          { id: 3, text: "other words" },
        ],
      ],
    ]);
    const first = executeRowQuery(plan, small).rows;
    // A different corpus must produce a different score for the same document — the plan
    // object is shared, and stats must never persist across executions.
    const second = executeRowQuery(plan, large).rows;
    expect(second[0]?.score).not.toBe(first[0]?.score);
    expect(executeRowQuery(plan, small).rows).toEqual(first);
  });

  it("accounts the full-text dictionary match tables at an exact budget", () => {
    const rows: DatabaseRow[] = [
      { title: "quick brown fox", body: "jumps high" },
      { title: "lazy dog", body: "sleeps quick" },
      { title: "quick fox again", body: null },
    ];
    const tables = new Map([["rows", rows]]);
    const plan = compileQuery(
      "SELECT title FROM rows WHERE MATCH(title, body) AGAINST 'quick fox'",
    );
    // Measure the exact peak, then prove the accounting is tight: the same query succeeds at
    // the peak and fails one byte below it inside the match-table reservation.
    const measured = createPreparedQuery(plan, tables, {});
    const expected = measured.execute();
    expect(expected.rows.map((row) => row.title)).toEqual(["quick brown fox", "quick fox again"]);
    const peak = measured.memoryUsage.peakBytes;
    measured.close();
    expect(peak).toBeGreaterThan(0);

    const exact = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: peak });
    expect(exact.execute()).toEqual(expected);
    exact.close();

    const below = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: peak - 1 });
    expect(() => below.execute()).toThrow(QueryMemoryBudgetError);
    below.close();

    // The match tables themselves are part of the model: the title dictionary has 3 entries and
    // the body dictionary 2 (null never enters a dictionary), reserving 4 bytes per entry
    // beyond the plain projection's peak.
    const plain = createPreparedQuery(compileQuery("SELECT title FROM rows"), tables, {});
    plain.execute();
    expect(peak).toBeGreaterThanOrEqual(plain.memoryUsage.peakBytes + (3 + 2) * 4);
    plain.close();
  });

  it("spills stable ORDER BY runs under a budget and removes every temp page", async () => {
    const rows: DatabaseRow[] = Array.from({ length: 5_000 }, (_, index) => ({
      id: index,
      // Exercise PostgreSQL's implicit NULLS LAST through both the per-run radix sort and the
      // comparison-based spill merge. The row executor is the independent reference below.
      bucket: index % 17 === 0 ? null : index % 11,
    }));
    // No LIMIT: a limited ORDER BY retains only the top rows and fits the budget without spilling.
    const plan = compileQuery("SELECT id, bucket FROM rows ORDER BY bucket, id DESC");
    const prepared = createPreparedQuery(plan, new Map([["rows", rows]]), {
      executionMemoryBudgetBytes: 150_000,
    });
    expect(() => prepared.execute()).toThrow(QueryMemoryBudgetError);
    const spill = new TestSpillStore();
    const result = await prepared.executeAsync({ spillStore: spill, spillPageRows: 64 });
    expect(result).toEqual(executeRowQuery(plan, new Map([["rows", rows]])));
    expect(spill.pages.size).toBe(0);
    expect(prepared.memoryUsage.peakBytes).toBeLessThanOrEqual(150_000);
    prepared.close();
  });

  it("keeps a limited ORDER BY within budget by retaining only the top rows", () => {
    const rows: DatabaseRow[] = Array.from({ length: 5_000 }, (_, index) => ({
      id: index,
      bucket: index % 11,
    }));
    const plan = compileQuery(
      "SELECT id, bucket FROM rows ORDER BY bucket, id DESC LIMIT 137 OFFSET 3",
    );
    const prepared = createPreparedQuery(plan, new Map([["rows", rows]]), {
      executionMemoryBudgetBytes: 150_000,
    });
    // The full sort would not fit (the unlimited variant above proves it); the bounded top-N does.
    expect(prepared.execute()).toEqual(executeRowQuery(plan, new Map([["rows", rows]])));
    expect(prepared.memoryUsage.peakBytes).toBeLessThanOrEqual(150_000);
    prepared.close();
  });

  it("drops the top-N bound for a page that keeps a tenth of the scan or more", () => {
    const rows: DatabaseRow[] = Array.from({ length: 5_000 }, (_, index) => ({
      id: index,
      bucket: index % 11,
    }));
    const tables = new Map([["rows", rows]]);
    const peakOf = (sql: string): number => {
      const plan = compileQuery(sql);
      const prepared = createPreparedQuery(plan, tables, {});
      expect(prepared.execute(), sql).toEqual(executeRowQuery(plan, tables));
      const peak = prepared.memoryUsage.peakBytes;
      prepared.close();
      return peak;
    };
    const unlimited = peakOf("SELECT id, bucket FROM rows ORDER BY bucket, id DESC");
    // 450 of 5,000 rows is under the tenth: the bound holds, so the peak is a fraction of a
    // full sort's. 500 is the tenth exactly: every row is kept and sorted once, at the full
    // sort's cost in memory and less than its cost in time.
    expect(
      peakOf("SELECT id, bucket FROM rows ORDER BY bucket, id DESC LIMIT 400 OFFSET 50"),
    ).toBeLessThan(unlimited / 2);
    expect(
      peakOf("SELECT id, bucket FROM rows ORDER BY bucket, id DESC LIMIT 500"),
    ).toBeGreaterThan(unlimited / 2);
    expect(
      peakOf("SELECT id, bucket FROM rows ORDER BY bucket, id DESC LIMIT 4000 OFFSET 1000"),
    ).toBeGreaterThan(unlimited / 2);
    // The edges: a page that runs past the end, and one that starts past it.
    peakOf("SELECT id, bucket FROM rows ORDER BY bucket, id DESC LIMIT 10 OFFSET 4995");
    peakOf("SELECT id, bucket FROM rows ORDER BY bucket, id DESC LIMIT 10 OFFSET 6000");
  });

  it("partitions high-cardinality hash aggregates and merges ordered result runs", async () => {
    const rows: DatabaseRow[] = Array.from({ length: 5_000 }, (_, id) => ({ id }));
    const plan = compileQuery(
      "SELECT id, COUNT(*) AS count FROM rows GROUP BY id ORDER BY id DESC LIMIT 101",
    );
    const prepared = createPreparedQuery(plan, new Map([["rows", rows]]), {
      executionMemoryBudgetBytes: 100_000,
    });
    expect(() => prepared.execute()).toThrow(QueryMemoryBudgetError);
    const spill = new TestSpillStore();
    expect(await prepared.executeAsync({ spillStore: spill, spillPageRows: 64 })).toEqual(
      executeRowQuery(plan, new Map([["rows", rows]])),
    );
    expect(spill.pages.size).toBe(0);
    expect(prepared.memoryUsage.peakBytes).toBeLessThanOrEqual(100_000);
    prepared.close();
  });

  it("spills JSON_ARRAYAGG groups without dropping values or SQL nulls", async () => {
    const rows: DatabaseRow[] = Array.from({ length: 5_000 }, (_, id) => ({
      bucket: id % 400,
      value: id % 17 === 0 ? null : id,
    }));
    const tables = new Map([["rows", rows]]);
    const plan = compileQuery(
      "SELECT bucket, JSON_ARRAYAGG(value) AS values FROM rows GROUP BY bucket ORDER BY bucket",
    );
    const prepared = createPreparedQuery(plan, tables, {
      executionMemoryBudgetBytes: 100_000,
    });
    expect(() => prepared.execute()).toThrow(QueryMemoryBudgetError);
    const spill = new TestSpillStore();
    expect(await prepared.executeAsync({ spillStore: spill, spillPageRows: 64 })).toEqual(
      executeRowQuery(plan, tables),
    );
    expect(spill.putCount).toBeGreaterThan(0);
    expect(spill.pages.size).toBe(0);
    expect(prepared.memoryUsage.peakBytes).toBeLessThanOrEqual(100_000);
    prepared.close();
  });

  it("spills ordered STRING_AGG groups without changing aggregate order", async () => {
    const rows: DatabaseRow[] = Array.from({ length: 5_000 }, (_, id) => ({
      bucket: id % 400,
      label: `v${String(id).padStart(4, "0")}`,
      priority: (id * 37) % 101,
    }));
    const tables = new Map([["rows", rows]]);
    const plan = compileQuery(
      "SELECT bucket, STRING_AGG(label, ',' ORDER BY priority DESC, label) AS labels " +
        "FROM rows GROUP BY bucket ORDER BY bucket",
    );
    const prepared = createPreparedQuery(plan, tables, {
      executionMemoryBudgetBytes: 200_000,
    });
    expect(() => prepared.execute()).toThrow(QueryMemoryBudgetError);
    const spill = new TestSpillStore();
    expect(await prepared.executeAsync({ spillStore: spill, spillPageRows: 64 })).toEqual(
      executeRowQuery(plan, tables),
    );
    expect(spill.putCount).toBeGreaterThan(0);
    expect(spill.pages.size).toBe(0);
    expect(prepared.memoryUsage.peakBytes).toBeLessThanOrEqual(200_000);
    prepared.close();
  });

  it("accounts every retained JSON_ARRAYAGG member against the query budget", () => {
    const tables = new Map<string, DatabaseRow[]>([
      ["rows", [{ value: "alpha" }, { value: null }, { value: "charlie" }]],
    ]);
    const plan = compileQuery("SELECT JSON_ARRAYAGG(value) AS values FROM rows");
    const measured = createPreparedQuery(plan, tables, {});
    const expected = measured.execute();
    expect(expected.rows).toEqual([{ values: '["alpha",null,"charlie"]' }]);
    const peak = measured.memoryUsage.peakBytes;
    measured.close();

    const exact = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: peak });
    expect(exact.execute()).toEqual(expected);
    exact.close();

    const below = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: peak - 1 });
    expect(() => below.execute()).toThrow(QueryMemoryBudgetError);
    below.close();
  });
  it("accounts retained vectors, scan batches, results, and ordering at an exact budget", () => {
    const plan = compileQuery("SELECT id FROM rows ORDER BY id");
    const tables = new Map<string, DatabaseRow[]>([["rows", [{ id: 1 }, { id: 2 }]]]);
    // The ordering scratch carries one null-mask byte per row per order term beside the key
    // slot, which is what lets a numeric term compare as typed values rather than references.
    const prepared = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: 87 });
    expect(prepared.memoryUsage).toEqual({ budgetBytes: 87, usedBytes: 17, peakBytes: 17 });
    expect(prepared.execute()).toEqual({ columns: ["id"], rows: [{ id: 1 }, { id: 2 }] });
    expect(prepared.memoryUsage).toEqual({ budgetBytes: 87, usedBytes: 17, peakBytes: 87 });
    prepared.close();
    expect(prepared.memoryUsage).toEqual({ budgetBytes: 87, usedBytes: 0, peakBytes: 87 });

    const below = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: 86 });
    expect(() => below.execute()).toThrow(QueryMemoryBudgetError);
    expect(below.memoryUsage).toEqual({ budgetBytes: 86, usedBytes: 17, peakBytes: 59 });
    below.close();
  });

  it("accounts dictionary codes, validity, and string dictionary payload", () => {
    const plan = compileQuery("SELECT label FROM rows");
    const tables = new Map<string, DatabaseRow[]>([
      ["rows", [{ label: "é" }, { label: "é" }, { label: null }, { label: "x" }]],
    ]);
    // String payloads are accounted at one byte per UTF-16 code unit, so "é" counts 1.
    const prepared = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: 74 });
    expect(prepared.memoryUsage).toEqual({ budgetBytes: 74, usedBytes: 19, peakBytes: 19 });
    expect(prepared.execute().rows).toEqual([
      { label: "é" },
      { label: "é" },
      { label: null },
      { label: "x" },
    ]);
    expect(prepared.memoryUsage).toEqual({ budgetBytes: 74, usedBytes: 19, peakBytes: 74 });
    prepared.close();
  });

  it("accounts group state, aggregate values, result rows, ordering, and LIMIT buffers", () => {
    const plan = compileQuery(
      "SELECT category, COUNT(*) AS count, MAX(label) AS max_label FROM rows GROUP BY category ORDER BY category LIMIT 2",
    );
    const tables = new Map<string, DatabaseRow[]>([
      [
        "rows",
        [
          { category: "a", label: "one" },
          { category: "b", label: "two" },
          { category: "c", label: "three" },
        ],
      ],
    ]);
    const expected = {
      columns: ["category", "count", "max_label"],
      rows: [
        { category: "a", count: 1, max_label: "one" },
        { category: "b", count: 1, max_label: "two" },
      ],
    };

    const exact = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: 385 });
    expect(exact.memoryUsage).toEqual({ budgetBytes: 385, usedBytes: 40, peakBytes: 40 });
    expect(exact.execute()).toEqual(expected);
    expect(exact.memoryUsage).toEqual({ budgetBytes: 385, usedBytes: 40, peakBytes: 385 });
    exact.close();

    const groupBelow = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: 271 });
    let groupFailure: unknown;
    try {
      groupBelow.execute();
    } catch (error) {
      groupFailure = error;
    }
    expect(groupFailure).toMatchObject({
      name: "QueryMemoryBudgetError",
      label: "MAX aggregate value",
    });
    expect(groupBelow.memoryUsage).toEqual({ budgetBytes: 271, usedBytes: 40, peakBytes: 266 });
    groupBelow.close();

    const resultBelow = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: 330 });
    let resultFailure: unknown;
    try {
      resultBelow.execute();
    } catch (error) {
      resultFailure = error;
    }
    expect(resultFailure).toMatchObject({
      name: "QueryMemoryBudgetError",
      label: "Accumulated grouped result row",
    });
    expect(resultBelow.memoryUsage).toEqual({ budgetBytes: 330, usedBytes: 40, peakBytes: 306 });
    resultBelow.close();

    const below = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: 381 });
    let orderingFailure: unknown;
    try {
      below.execute();
    } catch (error) {
      orderingFailure = error;
    }
    expect(orderingFailure).toMatchObject({
      name: "QueryMemoryBudgetError",
      label: "Ordering typed scratch",
    });
    expect(below.memoryUsage).toEqual({ budgetBytes: 381, usedBytes: 40, peakBytes: 331 });
    below.close();
  });

  it("applies LIMIT in place without allocating a second boxed reference slice", () => {
    const plan = compileQuery("SELECT id FROM rows LIMIT 2");
    const tables = new Map<string, DatabaseRow[]>([["rows", [{ id: 1 }, { id: 2 }, { id: 3 }]]]);
    const exact = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: 71 });
    expect(exact.execute().rows).toEqual([{ id: 1 }, { id: 2 }]);
    expect(exact.memoryUsage).toEqual({ budgetBytes: 71, usedBytes: 25, peakBytes: 71 });
    exact.close();

    const below = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: 70 });
    let failure: unknown;
    try {
      below.execute();
    } catch (error) {
      failure = error;
    }
    expect(failure).toMatchObject({
      name: "QueryMemoryBudgetError",
      label: "Accumulated result row",
    });
    expect(below.memoryUsage).toEqual({ budgetBytes: 70, usedBytes: 25, peakBytes: 54 });
    below.close();
  });

  it("replaces retained aggregate payload reservations instead of accumulating old values", () => {
    const plan = compileQuery("SELECT MAX(label) AS maximum FROM rows");
    const tables = new Map<string, DatabaseRow[]>([
      ["rows", [{ label: "a" }, { label: "bb" }, { label: "ccc" }]],
    ]);
    const exact = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: 166 });
    expect(exact.execute()).toEqual({ columns: ["maximum"], rows: [{ maximum: "ccc" }] });
    expect(exact.memoryUsage).toEqual({ budgetBytes: 166, usedBytes: 19, peakBytes: 166 });
    exact.close();

    const below = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: 165 });
    let failure: unknown;
    try {
      below.execute();
    } catch (error) {
      failure = error;
    }
    expect(failure).toMatchObject({
      name: "QueryMemoryBudgetError",
      label: "MAX aggregate value",
    });
    expect(below.memoryUsage).toEqual({ budgetBytes: 165, usedBytes: 19, peakBytes: 164 });
    below.close();
  });

  it("releases temporary join reservations after success and budget failure", () => {
    const plan = compileQuery(
      "SELECT l.id, r.value FROM left_rows l JOIN right_rows r ON r.id = l.id ORDER BY l.id",
    );
    const tables = new Map<string, DatabaseRow[]>([
      ["left_rows", [{ id: 1 }, { id: 2 }]],
      [
        "right_rows",
        [
          { id: 1, value: "one" },
          { id: 2, value: "two" },
        ],
      ],
    ]);
    const measured = createPreparedQuery(plan, tables);
    const retainedBytes = measured.memoryUsage.usedBytes;
    measured.execute();
    const peakBytes = measured.memoryUsage.peakBytes;
    expect(peakBytes).toBeGreaterThan(retainedBytes);
    expect(measured.memoryUsage.usedBytes).toBe(retainedBytes);
    measured.close();

    const below = createPreparedQuery(plan, tables, {
      executionMemoryBudgetBytes: peakBytes - 1,
    });
    expect(() => below.execute()).toThrow(QueryMemoryBudgetError);
    expect(below.memoryUsage.usedBytes).toBe(retainedBytes);
    below.close();

    const exact = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: peakBytes });
    expect(exact.execute()).toEqual(executeRowQuery(plan, tables));
    expect(exact.memoryUsage.peakBytes).toBe(peakBytes);
    exact.close();
  });

  it("uses SQL equality for NaN join keys", () => {
    const plan = compileQuery(
      "SELECT l.label AS left_label, r.label AS right_label FROM left_rows l LEFT JOIN right_rows r ON r.value / r.value = l.value / l.value ORDER BY left_label",
    );
    const tables = new Map<string, DatabaseRow[]>([
      [
        "left_rows",
        [
          { label: "finite", value: 1 },
          { label: "not-a-number", value: 0 },
        ],
      ],
      [
        "right_rows",
        [
          { label: "finite-match", value: 1 },
          { label: "nan-must-not-match", value: 0 },
        ],
      ],
    ]);

    const expected = {
      columns: ["left_label", "right_label"],
      rows: [
        { left_label: "finite", right_label: "finite-match" },
        { left_label: "not-a-number", right_label: null },
      ],
    };
    expect(executeQuery(plan, tables)).toEqual(expected);
    expect(executeRowQuery(plan, tables)).toEqual(expected);
  });

  it("builds typed vectors with explicit validity and dictionary contracts", () => {
    const firstDate = new Date("2025-01-01T00:00:00.000Z");
    const secondDate = new Date("2025-01-02T00:00:00.000Z");
    const table = createColumnarTable(
      "typed_rows",
      new Map([
        ["id", { type: "number" as const, values: [1, 2, 3] }],
        ["active", { type: "boolean" as const, values: [true, null, false] }],
        ["label", { type: "string" as const, values: ["same", null, "same"] }],
        ["happened", { type: "datetime" as const, values: [firstDate, null, secondDate] }],
      ]),
      "id",
    );

    expect(table.rowCount).toBe(3);
    expect(table.uniqueKey).toBe("id");
    expect(table.columns.get("id")).toEqual({
      kind: "number",
      length: 3,
      validity: Uint8Array.of(0b0000_0111),
      values: Float64Array.of(1, 2, 3),
    });
    expect(table.columns.get("active")).toEqual({
      kind: "boolean",
      length: 3,
      validity: Uint8Array.of(0b0000_0101),
      values: Uint8Array.of(1, 0, 0),
    });
    expect(table.columns.get("label")).toEqual({
      kind: "string",
      length: 3,
      validity: Uint8Array.of(0b0000_0101),
      codes: Uint32Array.of(0, 0xffff_ffff, 0),
      dictionary: ["same"],
    });
    expect(table.columns.get("happened")).toEqual({
      kind: "datetime",
      length: 3,
      validity: Uint8Array.of(0b0000_0101),
      values: Float64Array.of(firstDate.getTime(), 0, secondDate.getTime()),
    });
  });

  it("rejects malformed and logically mistyped columnar inputs", () => {
    expect(() =>
      createColumnarTable(
        "mismatched",
        new Map([
          ["id", { type: "number" as const, values: [1, 2] }],
          ["label", { type: "string" as const, values: ["one"] }],
        ]),
      ),
    ).toThrow("Column row count mismatch");
    expect(() =>
      createColumnarTable(
        "missing_key",
        new Map([["id", { type: "number" as const, values: [1] }]]),
        "other_id",
      ),
    ).toThrow("Unique-key vector is missing");

    for (const [type, value] of [
      ["boolean", 1],
      ["string", 1],
      ["number", new Date("2025-01-01T00:00:00.000Z")],
      ["datetime", 1],
    ] as const) {
      expect(() =>
        createColumnarTable("mistyped", new Map([["value", { type, values: [value] }]])),
      ).toThrow(`Invalid ${type} vector value`);
    }
  });

  it("preserves projected columns for an empty row input", () => {
    expect(executeQuery(compileQuery("SELECT value FROM rows"), new Map([["rows", []]]))).toEqual({
      columns: ["value"],
      rows: [],
    });
  });

  it("rejects a modeled budget when schema-less empty row inputs require the oracle fallback", () => {
    const plan = compileQuery("SELECT value FROM rows");
    expect(() =>
      createPreparedQuery(plan, new Map<string, DatabaseRow[]>([["rows", []]]), {
        executionMemoryBudgetBytes: 1_024,
      }),
    ).toThrow("Query memory budgets require typed columnar schemas when an input table is empty");
  });

  it("does not validate unused heterogeneous row-adapter columns", () => {
    expect(
      executeQuery(
        compileQuery("SELECT id FROM rows ORDER BY id"),
        new Map([
          [
            "rows",
            [
              { id: 1, unused: 1 },
              { id: 2, unused: "mixed" },
            ],
          ],
        ]),
      ),
    ).toEqual({ columns: ["id"], rows: [{ id: 1 }, { id: 2 }] });
  });

  it("preserves row-executor grouping for non-finite arithmetic", () => {
    const plan = compileQuery(
      "SELECT numerator / denominator AS quotient, COUNT(*) AS count FROM rows GROUP BY numerator / denominator",
    );
    const input = new Map<string, DatabaseRow[]>([
      [
        "rows",
        [
          { numerator: 0, denominator: 0 },
          { numerator: 1, denominator: 0 },
          { numerator: -1, denominator: 0 },
          { numerator: 0, denominator: -1 },
        ],
      ],
    ]);
    expect(executeQuery(plan, input)).toEqual(executeRowQuery(plan, input));
  });

  it("partitions non-finite group keys consistently when spilling", async () => {
    const plan = compileQuery(
      "SELECT numerator / denominator AS quotient, COUNT(*) AS count FROM rows GROUP BY numerator / denominator ORDER BY quotient",
    );
    const input = new Map<string, DatabaseRow[]>([
      [
        "rows",
        [
          { numerator: 0, denominator: 0 },
          { numerator: 0, denominator: 0 },
          { numerator: 1, denominator: 0 },
          { numerator: -1, denominator: 0 },
          { numerator: 0, denominator: -1 },
        ],
      ],
    ]);
    const prepared = createPreparedQuery(plan, input, {
      executionMemoryBudgetBytes: 16_384,
    });
    const spill = new TestSpillStore();
    expect(await prepared.executeAsync({ spillStore: spill, spillPageRows: 2 })).toEqual(
      executeRowQuery(plan, input),
    );
    expect(spill.pages.size).toBe(0);
    prepared.close();
  });

  it("preserves logical left-to-right INNER JOIN order before LIMIT", () => {
    const plan = compileQuery(
      "SELECT a.id AS aid, b.sequence FROM a JOIN b ON b.id = a.id LIMIT 1",
    );
    const input = new Map<string, DatabaseRow[]>([
      ["a", [{ id: 1 }, { id: 2 }]],
      [
        "b",
        [
          { id: 2, sequence: "first" },
          { id: 1, sequence: "second" },
          { id: 1, sequence: "third" },
        ],
      ],
    ]);
    expect(executeQuery(plan, input)).toEqual(executeRowQuery(plan, input));
  });

  it("retains typed empty right-side columns in wildcard LEFT JOIN output", () => {
    const left = createColumnarTable(
      "left_rows",
      new Map([
        ["id", { type: "number" as const, values: [1] }],
        ["label", { type: "string" as const, values: ["left"] }],
      ]),
    );
    const right = createColumnarTable(
      "right_rows",
      new Map([
        ["id", { type: "number" as const, values: [] }],
        ["value", { type: "string" as const, values: [] }],
      ]),
    );
    const prepared = createPreparedColumnarQuery(
      compileQuery("SELECT * FROM left_rows l LEFT JOIN right_rows r ON r.id = l.id"),
      new Map([
        ["left_rows", left],
        ["right_rows", right],
      ]),
    );
    expect(prepared.execute()).toEqual({
      columns: ["l.id", "l.label", "r.id", "r.value"],
      rows: [{ "l.id": 1, "l.label": "left", "r.id": null, "r.value": null }],
    });
  });

  it("snapshots mutable values in the empty-table prepared fallback", () => {
    const happened = new Date("2025-01-01T00:00:00.000Z");
    const leftRows: DatabaseRow[] = [{ id: 1, happened }];
    const prepared = createPreparedQuery(
      compileQuery("SELECT l.happened FROM left_rows l LEFT JOIN right_rows r ON r.id = l.id"),
      new Map<string, DatabaseRow[]>([
        ["left_rows", leftRows],
        ["right_rows", []],
      ]),
    );
    happened.setUTCFullYear(2030);
    leftRows[0] = { id: 2, happened: new Date("2031-01-01T00:00:00.000Z") };
    const first = prepared.execute();
    const returned = first.rows[0]?.happened;
    if (!(returned instanceof Date)) throw new Error("Prepared fallback datetime is missing");
    returned.setUTCFullYear(2040);
    expect(prepared.execute()).toEqual({
      columns: ["happened"],
      rows: [{ happened: new Date("2025-01-01T00:00:00.000Z") }],
    });
  });

  it("returns null-filled right columns for an unmatched wildcard left join", () => {
    expect(
      executeQuery(
        compileQuery("SELECT * FROM left_rows l LEFT JOIN right_rows r ON r.id = l.id"),
        new Map([
          ["left_rows", [{ id: 1, label: "left" }]],
          ["right_rows", [{ id: 2, value: "right" }]],
        ]),
      ),
    ).toEqual({
      columns: ["l.id", "l.label", "r.id", "r.value"],
      rows: [{ "l.id": 1, "l.label": "left", "r.id": null, "r.value": null }],
    });
  });

  it("handles rows immediately around vector batch boundaries", () => {
    const plan = compileQuery(
      "SELECT parity, COUNT(*) AS count, SUM(value) AS total FROM rows WHERE value >= 0 GROUP BY parity ORDER BY parity",
    );
    for (const rowCount of [2_047, 2_048, 2_049]) {
      const rows: DatabaseRow[] = Array.from({ length: rowCount }, (_, index) => ({
        value: index - 3,
        parity: index % 2,
      }));
      const input = new Map([["rows", rows]]);
      expect(executeQuery(plan, input), `row count ${String(rowCount)}`).toEqual(
        executeRowQuery(plan, input),
      );
    }
  });

  it("preserves duplicate join fanout across a batch boundary", () => {
    const leftRows: DatabaseRow[] = Array.from({ length: 2_049 }, (_, index) => ({
      id: index,
      join_key: index % 2,
    }));
    const rightRows: DatabaseRow[] = Array.from({ length: 6 }, (_, sequence) => ({
      join_key: sequence % 2,
      weight: sequence + 1,
    }));
    const plan = compileQuery(
      "SELECT COUNT(*) AS count, SUM(r.weight) AS total FROM left_rows l JOIN right_rows r ON r.join_key = l.join_key",
    );
    const input = new Map([
      ["left_rows", leftRows],
      ["right_rows", rightRows],
    ]);

    expect(executeQuery(plan, input)).toEqual(executeRowQuery(plan, input));
    expect(executeQuery(plan, input).rows).toEqual([{ count: 6_147, total: 21_513 }]);
  });

  it("matches the row oracle across batch boundaries and operator shapes", () => {
    const events: DatabaseRow[] = Array.from({ length: 4_097 }, (_, index) => ({
      event_id: index + 1,
      owner_id: (index % 257) + 1,
      status: index % 13 === 0 ? null : (["new", "paid", "closed"][index % 3] ?? "new"),
      active: index % 11 === 0 ? null : index % 2 === 0,
      amount: index % 17 === 0 ? null : (index % 101) - 50,
      occurred_at: new Date(Date.UTC(2025, 0, (index % 28) + 1)),
    }));
    const owners: DatabaseRow[] = Array.from({ length: 257 }, (_, index) => ({
      owner_id: index + 1,
      region_id: (index % 7) + 1,
      tier: ["free", "team", "enterprise"][index % 3] ?? "free",
    }));
    const regions: DatabaseRow[] = Array.from({ length: 7 }, (_, index) => ({
      region_id: index + 1,
      region: `region-${String(index + 1)}`,
    }));
    const notes: DatabaseRow[] = Array.from({ length: 128 }, (_, index) => ({
      event_id: index * 3 + 1,
      note: `note-${String(index)}`,
    }));
    const tags: DatabaseRow[] = [
      { event_id: 1, tag: "a" },
      { event_id: 1, tag: "b" },
      { event_id: 2, tag: "b" },
      { event_id: 2, tag: "c" },
      { event_id: 3, tag: "a" },
      { event_id: 3, tag: "c" },
    ];
    const tables = new Map<string, DatabaseRow[]>([
      ["events", events],
      ["owners", owners],
      ["regions", regions],
      ["notes", notes],
      ["tags", tags],
    ]);
    const queries = [
      "SELECT event_id, amount * 2 AS doubled FROM events WHERE amount >= -10 AND amount < 10 ORDER BY event_id DESC LIMIT 37",
      "SELECT event_id, occurred_at FROM events WHERE occurred_at >= DATE '2025-01-20' ORDER BY event_id LIMIT 25",
      "SELECT COUNT(*) AS rows, COUNT(amount) AS values, SUM(amount) AS total, AVG(amount) AS average, MIN(amount) AS minimum, MAX(amount) AS maximum FROM events",
      "SELECT status, active, COUNT(*) AS rows, SUM(amount) AS total FROM events GROUP BY status, active ORDER BY status, active",
      "SELECT o.tier, COUNT(*) AS rows, SUM(e.amount) AS total FROM owners o JOIN events e ON e.owner_id = o.owner_id GROUP BY o.tier ORDER BY o.tier",
      "SELECT e.event_id, n.note FROM events e LEFT JOIN notes n ON n.event_id = e.event_id ORDER BY e.event_id LIMIT 300",
      "SELECT e.event_id, t.tag FROM events e JOIN tags t ON t.event_id = e.event_id ORDER BY e.event_id, t.tag",
      "SELECT r.region, COUNT(*) AS rows, SUM(e.amount) AS total FROM events e JOIN owners o ON o.owner_id = e.owner_id JOIN regions r ON r.region_id = o.region_id GROUP BY r.region ORDER BY r.region",
    ];

    for (const sql of queries) {
      const plan = compileQuery(sql);
      const requiredTables = new Map<string, DatabaseRow[]>(
        [plan.base.table, ...plan.joins.map((join) => join.table)].map((name) => [
          name,
          tables.get(name) ?? [],
        ]),
      );
      expect(executeQuery(plan, requiredTables), sql).toEqual(
        executeRowQuery(plan, requiredTables),
      );
    }
  });

  it("uses packed dictionary codes for sparse high-cardinality compound groups", () => {
    // 4 region slots (including NULL) * 16,385 label slots is just above the dense
    // 65,536-slot cutoff. This exercises the sparse numeric index used by COUNT DISTINCT's
    // desugared (region, label) grouping without making the common dense path pay for a Map.
    const rows: DatabaseRow[] = Array.from({ length: 16_384 }, (_, index) => ({
      region: `region-${String(index % 3)}`,
      label: `label-${String(index)}`,
    }));
    const tables = new Map([["rows", rows]]);
    const plan = compileQuery(
      "SELECT region, COUNT(DISTINCT label) AS labels FROM rows GROUP BY region ORDER BY region",
    );

    const result = executeQuery(plan, tables);
    expect(result).toEqual(executeRowQuery(plan, tables));
    expect(result.rows).toEqual([
      { region: "region-0", labels: 5_462 },
      { region: "region-1", labels: 5_461 },
      { region: "region-2", labels: 5_461 },
    ]);
  });

  it("preserves SQL empty-input aggregate results with explicit vectors", () => {
    const empty = createColumnarTable(
      "events",
      new Map([
        ["event_id", { type: "number" as const, values: [] }],
        ["amount", { type: "number" as const, values: [] }],
      ]),
    );
    const prepared = createPreparedColumnarQuery(
      compileQuery(
        "SELECT COUNT(*) AS rows, COUNT(amount) AS values, SUM(amount) AS total, AVG(amount) AS average, MIN(amount) AS minimum, MAX(amount) AS maximum FROM events",
      ),
      new Map([["events", empty]]),
    );

    expect(prepared.execute()).toEqual({
      columns: ["rows", "values", "total", "average", "minimum", "maximum"],
      rows: [{ rows: 0, values: 0, total: null, average: null, minimum: null, maximum: null }],
    });
  });

  it("rejects a join expression whose build side depends on an earlier source", () => {
    const plan = compileQuery("SELECT a.id FROM a JOIN b ON b.id + a.id = a.id");
    expect(() =>
      executeQuery(
        plan,
        new Map([
          ["a", [{ id: 1 }]],
          ["b", [{ id: 1 }]],
        ]),
      ),
    ).toThrow("JOIN build expression");
  });

  it("matches the row oracle across deterministic randomized null and join distributions", () => {
    for (let seed = 1; seed <= 32; seed += 1) {
      let state = seed;
      const random = () => {
        state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
        return state / 0x1_0000_0000;
      };
      const left: DatabaseRow[] = Array.from(
        { length: 1 + Math.floor(random() * 180) },
        (_, id) => ({
          id,
          bucket: random() < 0.15 ? null : `bucket-${String(Math.floor(random() * 7))}`,
          value: random() < 0.2 ? null : Math.floor(random() * 101) - 50,
        }),
      );
      const right: DatabaseRow[] = Array.from(
        { length: 1 + Math.floor(random() * 220) },
        (_, sequence) => ({
          left_id: Math.floor(random() * (left.length + 20)),
          sequence,
          weight: random() < 0.2 ? null : Math.floor(random() * 31) - 15,
        }),
      );
      const tables = new Map<string, DatabaseRow[]>([
        ["left_rows", left],
        ["right_rows", right],
      ]);
      for (const sql of [
        "SELECT bucket, COUNT(*) AS rows, COUNT(value) AS values, SUM(value) AS total, AVG(value) AS average, MIN(value) AS minimum, MAX(value) AS maximum FROM left_rows GROUP BY bucket ORDER BY bucket",
        "SELECT l.bucket, COUNT(*) AS rows, SUM(r.weight) AS total FROM left_rows l JOIN right_rows r ON r.left_id = l.id GROUP BY l.bucket ORDER BY l.bucket",
        "SELECT l.id AS left_id, r.sequence AS right_sequence FROM left_rows l LEFT JOIN right_rows r ON r.left_id = l.id ORDER BY left_id, right_sequence",
      ]) {
        const plan = compileQuery(sql);
        expect(executeQuery(plan, tables), `seed ${String(seed)}: ${sql}`).toEqual(
          executeRowQuery(plan, tables),
        );
      }
    }
  });
});
