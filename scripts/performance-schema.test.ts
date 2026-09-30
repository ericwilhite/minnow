import { DatabaseSync } from "node:sqlite";
import { expect, it } from "vitest";
import { performanceMode, performanceIdDeclaration } from "./lib/performance-schema.mts";
it("makes the competitive schema enforce a real indexed primary key", () => {
  const db = new DatabaseSync(":memory:");
  try {
    db.exec(
      `CREATE TABLE data (${performanceIdDeclaration(performanceMode(["--compare"]))}, payload TEXT)`,
    );
    db.exec("INSERT INTO data VALUES (1,'first')");
    expect(() => db.exec("INSERT INTO data VALUES (1,'duplicate')")).toThrow(/UNIQUE/);
    const plan = db.prepare("EXPLAIN QUERY PLAN SELECT payload FROM data WHERE id = ?").all(1);
    expect(String(plan[0]?.detail)).toMatch(/INTEGER PRIMARY KEY/);
  } finally {
    db.close();
  }
});
it("prevents a different comparison schema from overwriting historical regression thresholds", () => {
  expect(() => performanceMode(["--compare", "--update"])).toThrow(/cannot update historical/);
  expect(performanceMode([])).toBe("regression");
});
