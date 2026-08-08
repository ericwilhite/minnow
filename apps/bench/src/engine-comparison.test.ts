import { compileQuery, executeQuery, type DatabaseRow } from "@browserdatabase/engine";
import { describe, expect, it } from "vitest";
import { generateEntityBatch, getScenario } from "./benchmark.js";
import { comparisonQueries } from "./engine-comparison.js";

function generatedRows(table: string): DatabaseRow[] {
  const entity = getScenario("commerce").entities.find((candidate) => candidate.name === table);
  if (entity === undefined) throw new Error(`Missing entity: ${table}`);
  const rowCount = entity.rows(1);
  const columns = generateEntityBatch(entity, 0, rowCount, rowCount, 1);
  return Array.from({ length: rowCount }, (_, index) =>
    Object.fromEntries(
      entity.columns.map((column) => [column.name, columns[column.name]?.[index]]),
    ),
  ) as DatabaseRow[];
}

describe("BrowserDatabase comparison SQL", () => {
  it("matches the payment-funnel groups produced from the deterministic dataset", () => {
    const payments = generatedRows("payments");
    const transactions = generatedRows("payment_transactions");
    const query = comparisonQueries.find(({ id }) => id === "q6");
    if (query === undefined) throw new Error("Missing q6");
    const actual = executeQuery(
      compileQuery(query.sql),
      new Map([
        ["payments", payments],
        ["payment_transactions", transactions],
      ]),
    ).rows;
    expect(actual).toEqual([
      {
        payment_status: "captured",
        transaction_status: "declined",
        event_count: 90,
        amount: 16_753.75,
      },
      {
        payment_status: "captured",
        transaction_status: "succeeded",
        event_count: 1_624,
        amount: 309_852.5,
      },
      {
        payment_status: "failed",
        transaction_status: "declined",
        event_count: 16,
        amount: 3_225,
      },
      {
        payment_status: "failed",
        transaction_status: "succeeded",
        event_count: 270,
        amount: 51_418.75,
      },
    ]);
  });
});
