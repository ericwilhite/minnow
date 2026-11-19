/** The fixed ceiling applies to PostgreSQL-correct capture selection too. */
import { expect, it } from "vitest";
import { isolatedRegexWork } from "./regex-work-child.test-support.js";
import { SqlRegex } from "../../pattern/regex.js";
import { MAX_SQL_PATTERN_MATCH_STEPS, SqlPatternWorkLimitError } from "../../pattern/limits.js";

const pattern = ".{8}a(b+)(c+)",
  input = (prefix: number) => "x".repeat(prefix) + "a" + "b".repeat(2000) + "c".repeat(2000);
function measured(program: SqlRegex, text: string) {
  const spend = program.spend.bind(program);
  let steps = 0;
  program.spend = (work) => {
    if (++steps > MAX_SQL_PATTERN_MATCH_STEPS + 1)
      throw new Error("VM failed to enforce its work ceiling");
    spend(work);
  };
  try {
    return { result: program.exec(text), error: null, steps };
  } catch (error: unknown) {
    return { result: null, error, steps };
  } finally {
    program.spend = spend;
  }
}
it("records the released near-ceiling success", () => {
  expect(isolatedRegexWork("released", "capture", 929500)).toEqual({
    steps: 8383439,
    matched: true,
    correct: true,
  });
}, 60000);
it("recovers the earlier boundary through a reusable match witness", () => {
  const run = measured(new SqlRegex(pattern, ""), input(929000));
  expect(run.error).toBeNull();
  expect(run.result).not.toBeNull();
  expect(run.steps).toBeLessThanOrEqual(MAX_SQL_PATTERN_MATCH_STEPS);
}, 60000);
it("refuses the documented closer boundary with the typed work-limit error", () => {
  const run = measured(new SqlRegex(pattern, ""), input(929500));
  expect(run.result).toBeNull();
  expect(run.steps).toBe(MAX_SQL_PATTERN_MATCH_STEPS + 1);
  expect(run.error).toBeInstanceOf(SqlPatternWorkLimitError);
  expect(run.error).toBeInstanceOf(RangeError);
  expect(run.error).toMatchObject({ code: "MINNOW_SQL_PATTERN_WORK_LIMIT", sqlState: "54000" });
}, 60000);

// Each immutable reference/zero-capture measurement executes every original
// step in a foreground child, without profiling test-fixture work. Current
// capture selection above and generated work comparisons remain instrumented.
for (const mode of ["boolean", "group-free"] as const)
  for (const version of ["released", "current"] as const)
    it(`keeps the ${version} near-ceiling ${mode} work and outcome exactly`, () => {
      expect(isolatedRegexWork(version, mode, 929500)).toEqual({
        steps: mode === "boolean" ? 8373444 : 8377438,
        matched: true,
        correct: true,
      });
    }, 60000);
