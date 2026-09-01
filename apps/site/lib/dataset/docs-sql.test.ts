import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { MinnowDatabase, type QueryValue } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { retailBatches, retailDefinition } from "./retail";

/** The SQL in the public guides, executed exactly as published. */

const guide = fileURLToPath(new URL("../../content/docs/sql/select.mdx", import.meta.url));
const docsRoot = fileURLToPath(new URL("../../content/docs", import.meta.url));

function sqlBlocks(path: string): string[] {
  return [...readFileSync(path, "utf8").matchAll(/```sql\n([\s\S]*?)```/g)]
    .map((match) => (match[1] ?? "").trim())
    .filter((block) => block.length > 0);
}

describe("the SELECT guide's examples", () => {
  it("all run against the playground dataset", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.migrate(retailDefinition);
    for (const batch of retailBatches({ scale: 0.05 })) {
      await database.insertBatch(batch.table, batch.rows);
    }

    const blocks = sqlBlocks(guide);
    expect(blocks.length).toBeGreaterThan(5);
    const failures: string[] = [];
    for (const block of blocks) {
      try {
        await database.query(block, { memoize: false });
      } catch (error) {
        const first = block.split("\n")[0] ?? "";
        failures.push(`${first} … — ${error instanceof Error ? error.message : String(error)}`);
      }
    }
    expect(failures.map(reason)).toEqual(knownGaps);
  }, 120_000);
});

/** The message an example failed with, without the example, so the list below stays readable. */
function reason(failure: string): string {
  return failure.slice(failure.indexOf("— ") + 2);
}

/**
 * Empty, and meant to stay that way. An example this page makes that the engine cannot keep is
 * listed here by its exact message rather than removed from the page, because each one is
 * ordinary SQL that belongs in a SELECT guide — the list is what has to shrink, not the
 * documentation. Adding a line is admitting a gap, so say why in a comment beside it.
 */
const knownGaps: string[] = [];

interface DocumentedSqlCase {
  file: string;
  block: number;
  setup?: string[];
  split?: (sql: string) => string[];
  params?: QueryValue[][];
}

function semicolonStatements(sql: string): string[] {
  return sql
    .split(";")
    .map((statement) => statement.trim())
    .filter(Boolean);
}

function splitBefore(sql: string, keyword: string): string[] {
  const marker = `\n${keyword}`;
  const at = sql.indexOf(marker);
  if (at < 0) throw new Error(`Expected ${keyword} in documented SQL`);
  return [sql.slice(0, at), sql.slice(at + 1)];
}

const documentedSqlCases: DocumentedSqlCase[] = [
  { file: "sql/ddl.mdx", block: 1 },
  { file: "sql/ddl.mdx", block: 2 },
  {
    file: "sql/ddl.mdx",
    block: 3,
    setup: [
      "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, total DOUBLE PRECISION, status TEXT)",
      "INSERT INTO orders VALUES (1, 7, 12.5, 'completed'), (2, 8, 4, 'pending')",
    ],
  },
  { file: "sql/ddl.mdx", block: 4 },
  {
    file: "sql/ddl.mdx",
    block: 5,
    setup: [
      "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY)",
      "CREATE TABLE notes (note_id INTEGER PRIMARY KEY)",
    ],
  },
  {
    file: "sql/ddl.mdx",
    block: 6,
    setup: [
      "CREATE TABLE order_items (order_id INTEGER, line_no INTEGER, PRIMARY KEY (order_id, line_no))",
    ],
  },
  {
    file: "sql/ddl.mdx",
    block: 7,
    setup: [
      "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, status TEXT NOT NULL, total DOUBLE PRECISION NOT NULL, customer_id INTEGER NOT NULL, placed_at TIMESTAMP NOT NULL)",
    ],
    split: semicolonStatements,
  },
  {
    file: "sql/ddl.mdx",
    block: 8,
    setup: [
      "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, status TEXT NOT NULL)",
      "CREATE INDEX orders_by_status ON orders (status)",
    ],
  },
  { file: "sql/ddl.mdx", block: 9, split: semicolonStatements },
  { file: "sql/ddl.mdx", block: 10, split: semicolonStatements },
  { file: "sql/ddl.mdx", block: 11 },
  { file: "sql/ddl.mdx", block: 12 },
  {
    file: "sql/ddl.mdx",
    block: 13,
    setup: ["CREATE TABLE orders (order_id INTEGER PRIMARY KEY)"],
  },
  { file: "sql/ddl.mdx", block: 14 },
  {
    file: "sql/dml.mdx",
    block: 1,
    setup: [
      "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, name TEXT, city TEXT, signed_up_on DATE)",
    ],
    params: [[1, "Ada", "Austin", "2026-01-01"]],
  },
  {
    file: "sql/dml.mdx",
    block: 2,
    setup: [
      "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, total DOUBLE PRECISION, placed_at TIMESTAMP)",
      "CREATE TABLE archived_orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, total DOUBLE PRECISION, placed_at TIMESTAMP)",
      "INSERT INTO orders VALUES (1, 7, 12.5, TIMESTAMP '2023-12-01')",
    ],
  },
  {
    file: "sql/dml.mdx",
    block: 3,
    setup: ["CREATE TABLE shipments (sku TEXT, qty INTEGER, at TIMESTAMP)"],
  },
  {
    file: "sql/dml.mdx",
    block: 4,
    setup: [
      "CREATE TABLE preferences (name TEXT DEFAULT 'standard')",
      "CREATE TABLE events (event_id INTEGER PRIMARY KEY, kind TEXT, noted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    ],
    split: semicolonStatements,
    params: [[], [1, "created"]],
  },
  {
    file: "sql/dml.mdx",
    block: 5,
    setup: [
      "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, status TEXT, total DOUBLE PRECISION, placed_at TIMESTAMP)",
      "INSERT INTO orders VALUES (1, 'paid', 20, TIMESTAMP '2023-01-01')",
    ],
    split: (sql) => splitBefore(sql, "DELETE"),
    params: [[5, 1], [new Date("2024-01-01T00:00:00.000Z")]],
  },
  {
    file: "sql/dml.mdx",
    block: 6,
    setup: [
      "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, name TEXT, city TEXT, signed_up_on DATE)",
      "INSERT INTO customers VALUES (1, 'Old', 'Dallas', DATE '2025-01-01')",
    ],
    params: [[1, "Ada", "Austin", "2026-01-01"]],
  },
  ...[7, 9].map<DocumentedSqlCase>((block) => ({
    file: "sql/dml.mdx",
    block,
    setup: [
      "CREATE TABLE inventory (sku TEXT PRIMARY KEY, received INTEGER, on_hand INTEGER DEFAULT 0)",
      "INSERT INTO inventory VALUES ('A-1', 0, 10)",
    ],
    params: [["A-1", 2]],
  })),
  {
    file: "sql/dml.mdx",
    block: 8,
    setup: [
      "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, name TEXT, city TEXT, signed_up_on DATE)",
      "INSERT INTO customers VALUES (1, 'Old', 'Dallas', DATE '2025-01-01')",
    ],
    params: [[1, "Ada", "Austin", "2026-01-01"]],
  },
  {
    file: "sql/dml.mdx",
    block: 10,
    setup: [
      "CREATE TABLE products (product_id INTEGER PRIMARY KEY, name TEXT, list_price DOUBLE PRECISION)",
      "INSERT INTO products VALUES (1, 'Kettle', 20)",
    ],
    params: [[1]],
  },
  {
    file: "sql/dml.mdx",
    block: 11,
    setup: [
      "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, status TEXT)",
      "CREATE TABLE audit (order_id INTEGER, old_status TEXT, new_status TEXT, at TIMESTAMP)",
    ],
  },
  {
    file: "sql/dml.mdx",
    block: 12,
    setup: [
      "CREATE TABLE stock (sku TEXT PRIMARY KEY, on_hand INTEGER)",
      "CREATE TABLE shipments (sku TEXT, qty INTEGER, at TIMESTAMP)",
      "INSERT INTO stock VALUES ('A-1', 2)",
    ],
    split: semicolonStatements,
  },
  {
    file: "sql/dml.mdx",
    block: 13,
    setup: [
      "CREATE TABLE stock (sku TEXT PRIMARY KEY, on_hand INTEGER)",
      "CREATE TABLE delivery (sku TEXT, qty INTEGER)",
      "INSERT INTO stock VALUES ('A-1', 2)",
      "INSERT INTO delivery VALUES ('A-1', 3), ('B-1', 4)",
    ],
  },
  ...[1, 2, 3, 4].map<DocumentedSqlCase>((block) => ({
    file: "sql/full-text-search.mdx",
    block,
    setup: [
      "CREATE TABLE products (product_id INTEGER PRIMARY KEY, name TEXT, brand TEXT)",
      "INSERT INTO products VALUES (1, 'espresso grinder', 'Acme'), (2, 'copper kettle', 'Yirgacheffe')",
    ],
    ...(block === 2 ? { split: (sql: string) => splitBefore(sql, "SELECT") } : {}),
  })),
  {
    file: "engine/transactions.mdx",
    block: 1,
    setup: [
      "CREATE TABLE order_items (order_id INTEGER, line_no INTEGER, sku TEXT, PRIMARY KEY (order_id, line_no))",
    ],
    split: semicolonStatements,
  },
  {
    file: "schema/index.mdx",
    block: 1,
    setup: ["CREATE TABLE parents (id INTEGER PRIMARY KEY)"],
  },
];

describe("the other SQL guides' examples", () => {
  it("execute every published SQL fence against its required schema", async () => {
    const files = [...new Set(documentedSqlCases.map(({ file }) => file))];
    const blocksByFile = new Map(
      files.map((file) => [file, sqlBlocks(`${docsRoot}/${file}`)] as const),
    );
    expect([...blocksByFile.values()].reduce((count, blocks) => count + blocks.length, 0)).toBe(33);
    expect(
      new Set(documentedSqlCases.map(({ file, block }) => `${file}#${String(block)}`)).size,
    ).toBe(33);

    const failures: string[] = [];
    for (const testCase of documentedSqlCases) {
      const store = new MemoryBlockStore();
      const database = new MinnowDatabase(store);
      const label = `${testCase.file} SQL block ${String(testCase.block)}`;
      try {
        for (const statement of testCase.setup ?? []) await database.execute(statement);
        const block = blocksByFile.get(testCase.file)?.[testCase.block - 1];
        if (block === undefined) throw new Error("documented SQL block is missing");
        const statements = testCase.split?.(block) ?? [block];
        if (testCase.params !== undefined && testCase.params.length !== statements.length) {
          throw new Error("documented parameter sets do not match statement count");
        }
        for (const [index, statement] of statements.entries()) {
          await database.execute(statement, testCase.params?.[index]);
        }
      } catch (error) {
        failures.push(`${label}: ${error instanceof Error ? error.message : String(error)}`);
      } finally {
        await database.close();
        store.close();
      }
    }
    expect(failures).toEqual([]);
  }, 120_000);
});
