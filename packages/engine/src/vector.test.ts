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
import { createColumnarTable } from "./vector.js";

describe("vector query execution", () => {
  it("accounts retained vectors and scan batches at an exact execution budget", () => {
    const plan = compileQuery("SELECT id FROM rows ORDER BY id");
    const tables = new Map<string, DatabaseRow[]>([["rows", [{ id: 1 }, { id: 2 }]]]);
    const prepared = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: 25 });
    expect(prepared.memoryUsage).toEqual({ budgetBytes: 25, usedBytes: 17, peakBytes: 17 });
    expect(prepared.execute()).toEqual({ columns: ["id"], rows: [{ id: 1 }, { id: 2 }] });
    expect(prepared.memoryUsage).toEqual({ budgetBytes: 25, usedBytes: 17, peakBytes: 25 });
    prepared.close();
    expect(prepared.memoryUsage).toEqual({ budgetBytes: 25, usedBytes: 0, peakBytes: 25 });

    const below = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: 24 });
    expect(() => below.execute()).toThrow(QueryMemoryBudgetError);
    expect(below.memoryUsage).toEqual({ budgetBytes: 24, usedBytes: 17, peakBytes: 17 });
    below.close();
  });

  it("accounts dictionary codes, validity, and UTF-8 dictionary payload", () => {
    const plan = compileQuery("SELECT label FROM rows");
    const tables = new Map<string, DatabaseRow[]>([
      ["rows", [{ label: "é" }, { label: "é" }, { label: null }, { label: "x" }]],
    ]);
    const prepared = createPreparedQuery(plan, tables, { executionMemoryBudgetBytes: 36 });
    expect(prepared.memoryUsage).toEqual({ budgetBytes: 36, usedBytes: 20, peakBytes: 20 });
    expect(prepared.execute().rows).toEqual([
      { label: "é" },
      { label: "é" },
      { label: null },
      { label: "x" },
    ]);
    expect(prepared.memoryUsage).toEqual({ budgetBytes: 36, usedBytes: 20, peakBytes: 36 });
    prepared.close();
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
