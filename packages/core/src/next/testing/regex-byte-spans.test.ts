/** ARE capture values and leftmost positions are independent of UTF8/UTF16 representation. */
import { PGlite } from "@electric-sql/pglite";
import { beforeAll, afterAll, expect, it } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, REGEX_BYTE_SPANS_REACH } from "../../testing/reach.js";
import { SqlRegex } from "../../pattern/regex.js";
import { Utf8RegexInput } from "../vector/regex-input.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const shapes = [
  "nested",
  "optional",
  "noncapturing",
  "star",
  "alternative",
  "prefix",
  "bounded",
  "anchored",
  "empty",
] as const;
const scenario = fc.record({
  shape: fc.constantFrom(...shapes),
  atom: fc.constantFrom("a", "Ж", "中", "😀"),
  count: fc.integer({ min: 0, max: 8 }),
  flags: fc.constantFrom("", "i", "n", "in", "ic", "ci"),
  prefix: fc.constantFrom("", "z", "🌿", "\n"),
  suffix: fc.constantFrom("", "b", "!", "\n"),
  random: fc.boolean(),
  longest: fc.boolean(),
  characters: fc.array(fc.constantFrom("a", "b", "Ж", "中", "😀", "\n", "A"), { maxLength: 16 }),
});
type Scenario = typeof scenario extends fc.Arbitrary<infer T> ? T : never;
const example = (
  shape: (typeof shapes)[number],
  atom: Scenario["atom"],
  flags: Scenario["flags"],
  prefix: Scenario["prefix"] = "",
): Scenario => ({
  shape,
  atom,
  flags,
  prefix,
  count: 3,
  suffix: "b",
  random: false,
  longest: false,
  characters: [],
});
const examples: Array<[Scenario]> = shapes.map((shape, i) => [
  example(shape, i % 2 ? "😀" : "Ж", i === 4 ? "ic" : i === 5 ? "ci" : i % 3 ? "n" : "i", "🌿"),
]);
// Keep the original alternative+tail family, and add a competing longer branch.
examples.push([{ ...example("alternative", "a", "i", "🌿"), count: 1, longest: true }]);
const encoder = new TextEncoder(),
  decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
for (const seed of seedsFor("regex-byte-spans", [49529, 49530]))
  it(`matches PGlite capture values and positions through borrowed bytes (${String(seed)})`, async () => {
    const reach = createReach("regex byte spans", REGEX_BYTE_SPANS_REACH);
    await fc.assert(
      fc.asyncProperty(scenario, async (data) => {
        reach.beginHistory();
        const a = data.atom;
        const pattern =
          data.shape === "nested"
            ? `((${a}+)*)`
            : data.shape === "optional"
              ? `(${a})?(b*)`
              : data.shape === "noncapturing"
                ? `(?:${a}|b)+(${a}?)`
                : data.shape === "star"
                  ? `(${a}(b)?)*`
                  : data.shape === "alternative"
                    ? `(${a}|(${a}b?))${data.longest ? "" : "(b*)"}`
                    : data.shape === "prefix"
                      ? `.*(${a}*)`
                      : data.shape === "bounded"
                        ? `(${a}){0,3}(b)?`
                        : data.shape === "anchored"
                          ? `^(${a}*)$`
                          : "(?:)";
        const source = data.random
          ? data.characters.join("")
          : data.prefix + a.repeat(data.count) + data.suffix;
        const bytes = encoder.encode(source),
          program = new SqlRegex(pattern, data.flags);
        const expected = (
          await pg.query<{ g: Array<string | null> | null; p: number }>(
            "SELECT regexp_match($1::text, '(' || $2::text || ')', $3::text) AS g, regexp_instr($1::text, $2::text, 1, 1, 0, $3::text) AS p",
            [source, pattern, data.flags],
          )
        ).rows[0];
        if (!expected) throw new Error("Missing PGlite row");
        const spans = program.spans(new Utf8RegexInput(bytes));
        const values =
          spans === null
            ? null
            : Array.from({ length: spans.length / 2 }, (_, group) => {
                const from = spans[group * 2] ?? -1,
                  to = spans[group * 2 + 1] ?? -1;
                return from < 0 || to < 0 ? null : decoder.decode(bytes.subarray(from, to));
              });
        expect(values).toEqual(expected.g);
        const legacy = program.exec(source);
        expect(
          legacy === null ? null : Array.from(legacy, (value: string | undefined) => value ?? null),
        ).toEqual(expected.g);
        if (spans === null) {
          expect(expected.p).toBe(0);
          reach.reach("no match");
        } else {
          const start = encoder.encode(
            Array.from(source)
              .slice(0, expected.p - 1)
              .join(""),
          ).length;
          expect(spans[0]).toBe(start);
          if (legacy) expect(encoder.encode(source.slice(0, legacy.index)).length).toBe(start);
          if (spans[0] === spans[1]) reach.reach("empty match");
          if (values?.includes(null)) reach.reach("nonparticipating group");
        }
        if (data.shape === "alternative" && data.longest && values?.[0] === a + "b")
          reach.reach("longest alternative");
        reach.reach(data.shape);
        if (source.includes("😀") || source.includes("🌿")) reach.reach("astral bytes");
        if (data.flags.includes("n")) reach.reach("newline mode");
        if (data.flags === "ic" || data.flags === "ci") reach.reach("ordered case flags");
      }),
      { seed, numRuns: 65, examples, endOnFailure: true },
    );
    reach.assertReached();
  });
