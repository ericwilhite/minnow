/** Capture dissection must not rescan a long prefix for every possible endpoint. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import fc from "fast-check";
import { shapes, scenario, examples, regexWorkCase } from "./regex-work.test-support.js";
import { RegexWorkParent } from "../../engine/sql-regex-work-parent.test-support.js";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/memory.js";
import * as captureModule from "../../pattern/captures.js";
import { SqlRegex } from "../../pattern/regex.js";
import { StringRegexInput } from "../../pattern/input.js";
import {
  MAX_SQL_PATTERN_MATCH_STEPS,
  MAX_SQL_SCALAR_RESULT_CHARACTERS,
} from "../../pattern/limits.js";
import { Utf8RegexInput } from "../vector/regex-input.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  REGEX_CAPTURE_WORK_REACH,
  REGEX_CAPTURE_DEPTH_REACH,
  REGEX_CAPTURE_OVERLAP_REACH,
  REGEX_CAPTURE_ZERO_REACH,
} from "../../testing/reach.js";

let pg: PGlite, db: MinnowDatabase;
let captureSpends = 0;
const actualCapture = captureModule.captureSpans;
const profiles: Array<{
  seed: number;
  shape: string;
  maxRatio: number;
  matching: number;
  captures: number;
  released: number;
  total: number;
}> = [];
beforeAll(async () => {
  vi.spyOn(captureModule, "captureSpans").mockImplementation(
    (root, ranges, program, input, from, to, slots, spend, trace) =>
      actualCapture(
        root,
        ranges,
        program,
        input,
        from,
        to,
        slots,
        () => {
          captureSpends++;
          spend();
        },
        trace,
      ),
  );
  pg = new PGlite();
  await pg.waitReady;
  db = new MinnowDatabase(new MemoryBlockStore(), { autoCompact: false, autoCollect: false });
}, 60000);
afterAll(async () => {
  await db.close();
  await pg.close();
  vi.restoreAllMocks();
  console.info("REGEX_CAPTURE_WORK_PROFILE", JSON.stringify(profiles));
});

interface Counted {
  spend(work: { remaining: number }): void;
}
function counted<T>(program: Counted, run: () => T, bound: number) {
  const spend = program.spend.bind(program);
  let steps = 0;
  const beforeCapture = captureSpends;
  program.spend = (work) => {
    if (++steps > bound)
      throw new RangeError(
        `capture prefix work ${String(steps)} exceeds linear bound ${String(bound)}`,
      );
    spend(work);
  };
  try {
    return { value: run(), error: null, steps, captureSteps: captureSpends - beforeCapture };
  } catch (error: unknown) {
    return { value: null, error, steps, captureSteps: captureSpends - beforeCapture };
  } finally {
    program.spend = spend;
  }
}
const encoder = new TextEncoder(),
  decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
for (const seed of seedsFor("regex-capture-work", [49533, 49534]))
  for (const shape of shapes) {
    const reach = createReach(
      `regex capture work ${shape}`,
      shape === "zero-body" || shape === "zero-prefix"
        ? [...REGEX_CAPTURE_WORK_REACH, ...REGEX_CAPTURE_ZERO_REACH]
        : shape === "overlap-chain"
          ? [
              ...REGEX_CAPTURE_WORK_REACH,
              ...REGEX_CAPTURE_DEPTH_REACH,
              ...REGEX_CAPTURE_OVERLAP_REACH,
            ]
          : shape === "chain"
            ? [...REGEX_CAPTURE_WORK_REACH, ...REGEX_CAPTURE_DEPTH_REACH]
            : REGEX_CAPTURE_WORK_REACH,
    );
    const profile = { seed, shape, maxRatio: 0, matching: 0, captures: 0, released: 0, total: 0 };
    profiles.push(profile);
    // Preserve all controls and eight random histories, with bounded bodies under coverage.
    for (const part of ["examples", "random"] as const)
      it(`keeps ${shape} capture work bounded (${String(seed)}, ${part})`, async () => {
        await fc.assert(
          fc.asyncProperty(scenario, async (data) => {
            reach.beginHistory();
            const { pattern, input } = regexWorkCase(shape, data);
            const expected = (
              await pg.query<{ g: Array<string | null> }>(
                "SELECT regexp_match($1::text, '(' || $2::text || ')') AS g",
                [input, pattern],
              )
            ).rows[0]?.g;
            if (!expected) throw new Error("Missing PostgreSQL capture match");
            const parent = new RegexWorkParent(pattern, ""),
              released = counted(parent, () => parent.exec(input), MAX_SQL_PATTERN_MATCH_STEPS + 1);
            if (released.error !== null) expect(released.error).toBeInstanceOf(RangeError);
            else expect(released.value).not.toBeNull();
            // The released matcher is the work baseline, never the capture-value authority.
            const bound =
                released.error === null
                  ? Math.min(MAX_SQL_PATTERN_MATCH_STEPS, 3 * released.steps)
                  : MAX_SQL_PATTERN_MATCH_STEPS + 1,
              source = new StringRegexInput(input, MAX_SQL_SCALAR_RESULT_CHARACTERS),
              bytes = encoder.encode(input),
              stringProgram = new SqlRegex(pattern, ""),
              byteProgram = new SqlRegex(pattern, ""),
              text = counted(stringProgram, () => stringProgram.spans(source), bound),
              utf8 = counted(
                byteProgram,
                () => byteProgram.spans(new Utf8RegexInput(bytes)),
                bound,
              );
            if (released.error === null) {
              expect(
                text.error,
                `${pattern} must keep a released success (${String(released.steps)} steps)`,
              ).toBeNull();
              expect(utf8.error).toBeNull();
              expect(text.steps).toBeLessThanOrEqual(MAX_SQL_PATTERN_MATCH_STEPS);
              reach.reach("parent success preserved");
              reach.reach("linear counted work");
            }
            if (text.error === null) {
              const matching = text.steps - text.captureSteps;
              if (shape !== "zero-body") expect(text.captureSteps).toBeGreaterThan(0);
              if (released.error === null) {
                expect(matching).toBeLessThanOrEqual(released.steps);
                expect(text.steps).toBeLessThanOrEqual(3 * released.steps);
                if (text.steps / released.steps > profile.maxRatio) {
                  profile.maxRatio = text.steps / released.steps;
                  profile.matching = matching;
                  profile.captures = text.captureSteps;
                  profile.released = released.steps;
                  profile.total = text.steps;
                }
              }
            }
            if (text.error !== null || utf8.error !== null) {
              expect(released.error).not.toBeNull();
              expect(text.error).toBeInstanceOf(RangeError);
              expect(utf8.error).toBeInstanceOf(RangeError);
            } else {
              const values = (
                spans: number[] | null,
                read: (from: number, to: number) => string,
              ) =>
                spans === null
                  ? null
                  : Array.from({ length: spans.length / 2 }, (_, group) => {
                      const from = spans[group * 2] ?? -1,
                        to = spans[group * 2 + 1] ?? -1;
                      return from < 0 || to < 0 ? null : read(from, to);
                    });
              expect(values(text.value, (from, to) => input.slice(from, to))).toEqual(expected);
              expect(
                values(utf8.value, (from, to) => decoder.decode(bytes.subarray(from, to))),
              ).toEqual(expected);
              expect(utf8.steps).toBe(text.steps);
              if (shape === "zero-body" || shape === "zero-prefix")
                reach.reach(data.depth % 2 ? "variable zero body" : "literal zero body");
              if (shape === "overlap-chain") {
                reach.reach("overlapping captures");
                if (data.depth >= 8 && data.a + data.b >= 64)
                  reach.reach("coupled length and nesting");
              }
              reach.reach("PG exact values");
              reach.reach("byte/span equality");
            }
            reach.reach(
              data.a < 10 ? "small length" : data.a < 1000 ? "hundreds length" : "thousands length",
            );
            if (data.a > 3000) reach.reach("above3000 length");
            if ((shape === "chain" || shape === "overlap-chain") && data.depth >= 16)
              reach.reach("deep chain");
            if ((shape === "chain" || shape === "overlap-chain") && data.depth === 32)
              reach.reach("32 captures");
            reach.reach(
              data.atom === "a"
                ? "ASCII atom"
                : data.atom === "😀"
                  ? "astral atom"
                  : "multibyte atom",
            );
            reach.reach(data.prefix === 0 ? "no prefix" : "prefix");
          }),
          {
            seed,
            numRuns: part === "examples" ? examples.length : 8,
            examples: part === "examples" ? examples : [],
            endOnFailure: true,
          },
        );
      });
    it(`reaches ${shape} length states (${String(seed)})`, () => {
      reach.assertReached();
    });
  }
it("keeps the 2000/2000 shipped substring and replacement query equal to PGlite", async () => {
  const input = "a".repeat(2000) + "b".repeat(2000),
    sql =
      "SELECT SUBSTRING($1::text FROM '(a+)(b+)') AS sub, REGEXP_REPLACE($1::text, '(a+)(b+)', '_') AS replaced";
  expect(
    (await db.query(sql.replaceAll("::text", "").replaceAll("$1", "'" + input + "'"))).rows,
  ).toEqual((await pg.query(sql, [input])).rows);
});

it("keeps boolean operators and group-free SQL out of capture dissection", async () => {
  const sql =
    "SELECT 'AaBb' ~ '(a+)(b+)' AS a, 'AaBb' ~* '(a+)(b+)' AS b, 'AaBb' !~ '(a+)(b+)' AS c, 'AaBb' !~* '(a+)(b+)' AS d, SUBSTRING('AAabbb' FROM 'a+b+') AS sub, REGEXP_REPLACE('AAabbb', 'a+b+', '_', 'gi') AS replaced";
  const before = captureSpends;
  expect((await db.query(sql)).rows).toEqual((await pg.query(sql)).rows);
  expect(captureSpends).toBe(before);
});

it.each(["(a){0}", "(a){0}(b+)", "((a+){0})(b+)"])(
  "keeps skipped capture slots exact in shipped SQL for %s",
  async (pattern) => {
    const sql = `SELECT SUBSTRING('ab' FROM '${pattern}') AS sub, REGEXP_REPLACE('ab', '${pattern}', '[\\1]', 'g') AS replaced`;
    expect((await db.query(sql)).rows).toEqual((await pg.query(sql)).rows);
  },
);
