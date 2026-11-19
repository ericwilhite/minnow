import fc from "fast-check";
export const shapes = [
  "separate",
  "prefix",
  "nested",
  "alternative",
  "bounded",
  "nullable",
  "anchored",
  "repeat",
  "chain",
  "overlap-chain",
  "captured-repeat",
  "optional-repeat",
  "star",
  "zero-body",
  "zero-prefix",
] as const;
export const scenarioFields = {
  a: fc.integer({ min: 1, max: 6000 }),
  b: fc.integer({ min: 1, max: 6000 }),
  atom: fc.constantFrom("a", "Ж", "😀"),
  prefix: fc.integer({ min: 0, max: 12 }),
  depth: fc.integer({ min: 1, max: 32 }),
};
export const scenario = fc.record(scenarioFields);
export type Scenario = typeof scenario extends fc.Arbitrary<infer T> ? T : never;
export const examples: Array<[Scenario]> = [
  [{ a: 2000, b: 2000, atom: "a", prefix: 0, depth: 2 }],
  [{ a: 4000, b: 1500, atom: "😀", prefix: 7, depth: 8 }],
  [{ a: 7, b: 5, atom: "Ж", prefix: 0, depth: 2 }],
  [{ a: 400, b: 250, atom: "a", prefix: 3, depth: 4 }],
  [{ a: 1, b: 1, atom: "a", prefix: 0, depth: 32 }],
  [{ a: 40, b: 25, atom: "Ж", prefix: 2, depth: 16 }],
  [{ a: 6000, b: 6000, atom: "a", prefix: 0, depth: 2 }],
  [{ a: 1, b: 1, atom: "a", prefix: 0, depth: 7 }],
];

export function regexWorkCase(
  shape: (typeof shapes)[number],
  data: Scenario,
): { pattern: string; input: string } {
  const a = data.atom,
    prefix = "🌿".repeat(data.prefix);
  const letters = [
    a,
    ...Array.from({ length: 31 }, (_, i) => String.fromCodePoint(0x4e00 + i)),
  ].slice(0, data.depth);
  const pattern =
    shape === "zero-body"
      ? data.depth % 2
        ? `(${a}+){0}`
        : `(${a}){0}`
      : shape === "zero-prefix"
        ? data.depth % 2
          ? `((${a}+){0})(b+)`
          : `(${a}){0}(b+)`
        : shape === "captured-repeat"
          ? `((${a})+)(b+)`
          : shape === "optional-repeat"
            ? `((${a}(b)?)+)(c+)`
            : shape === "star"
              ? `((${a}(b)?)*)(c+)`
              : shape === "chain" || shape === "overlap-chain"
                ? letters.reduceRight(
                    (tail, char) => `(${shape === "overlap-chain" ? a : char}+${tail})`,
                    "",
                  )
                : shape === "separate"
                  ? `(${a}+)(b+)`
                  : shape === "prefix"
                    ? `${a}*(b+)`
                    : shape === "nested"
                      ? `((${a}+))(b+)`
                      : shape === "alternative"
                        ? `((?:${a}|c)+)(b+)`
                        : shape === "bounded"
                          ? `(${a}{1,3})(${a}*)(b+)`
                          : shape === "nullable"
                            ? `(${a}+)(b*)()`
                            : shape === "anchored"
                              ? `^🌿*((${a}+))(b+)$`
                              : `((?:${a}b)+)(c+)`;
  const input =
    prefix +
    (shape === "optional-repeat" || shape === "star"
      ? (a + "b").repeat(data.a) + a + "c".repeat(data.b)
      : shape === "overlap-chain"
        ? a.repeat(Math.max(data.depth, data.a + data.b))
        : shape === "chain"
          ? letters.map((char, i) => char.repeat(i % 2 ? data.b : data.a)).join("")
          : shape === "repeat"
            ? (a + "b").repeat(data.a) + "c".repeat(data.b)
            : a.repeat(data.a) + "b".repeat(data.b));
  return { pattern, input };
}
