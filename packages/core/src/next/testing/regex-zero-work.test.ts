/** Boolean matching and group-free extraction keep the exact released work/success sets. */
import { beforeAll, afterAll, expect, it, vi } from "vitest";
import fc from "fast-check";
import { RegexWorkParent } from "../../engine/sql-regex-work-parent.test-support.js";
import { SqlRegex } from "../../pattern/regex.js";
import * as captures from "../../pattern/captures.js";
import { MAX_SQL_PATTERN_MATCH_STEPS } from "../../pattern/limits.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, REGEX_ZERO_WORK_REACH } from "../../testing/reach.js";
import { shapes, scenarioFields, examples, regexWorkCase } from "./regex-work.test-support.js";

beforeAll(() => {
  vi.spyOn(captures, "captureSpans").mockImplementation(() => {
    throw new Error("A capture-free path entered capture dissection");
  });
});
afterAll(() => {
  vi.restoreAllMocks();
});
function run(program: SqlRegex | RegexWorkParent, input: string, mode: string) {
  const spend = program.spend.bind(program);
  let steps = 0;
  program.spend = (work) => {
    steps++;
    spend(work);
  };
  try {
    return {
      value:
        mode === "boolean"
          ? program.test(input)
          : mode === "substring"
            ? program.exec(input)
            : program[Symbol.replace](input, "_"),
      error: null,
      steps,
    };
  } catch (error: unknown) {
    if (!(error instanceof RangeError)) throw error;
    return { value: null, error: { name: error.name, message: error.message }, steps };
  } finally {
    program.spend = spend;
  }
}
const data = fc.record({
  ...scenarioFields,
  shape: fc.constantFrom(...shapes),
  insensitive: fc.boolean(),
});
type History = typeof data extends fc.Arbitrary<infer T> ? T : never;
const cases: Array<[History]> = shapes.map((shape, i) => {
  const sample = examples[i % 7]?.[0];
  if (!sample) throw new Error("Missing regex work control");
  return [{ ...sample, shape, insensitive: i % 2 === 0 }];
});
for (const seed of seedsFor("regex-zero-work", [49535, 49536]))
  for (const mode of ["boolean", "substring", "replace"] as const)
    it(`does zero capture work for ${mode} (${String(seed)})`, () => {
      const reach = createReach(`regex zero work ${mode}`, REGEX_ZERO_WORK_REACH);
      fc.assert(
        fc.property(data, (history) => {
          reach.beginHistory();
          const { pattern: full, input } = regexWorkCase(history.shape, history);
          const pattern = mode === "boolean" ? full : full.replace(/\((?!\?:)/g, "(?:"),
            flags = (history.insensitive ? "i" : "") + (mode === "replace" ? "g" : ""),
            parent = run(new RegexWorkParent(pattern, flags), input, mode),
            head = run(new SqlRegex(pattern, flags), input, mode);
          expect(head.error).toEqual(parent.error);
          expect(head.value).toEqual(parent.value);
          expect(head.steps).toBe(parent.steps);
          expect(head.steps).toBeLessThanOrEqual(MAX_SQL_PATTERN_MATCH_STEPS + 1);
          reach.reach(history.shape);
          reach.reach("identical work");
          reach.reach("identical outcome");
          reach.reach("zero capture work");
        }),
        { seed, numRuns: 31, examples: cases, endOnFailure: true },
      );
      reach.assertReached();
    });
