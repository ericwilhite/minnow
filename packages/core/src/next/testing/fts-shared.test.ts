/** Independent fixtures + SQL differential: neither expected tokens nor stats use the shared helpers. */
import { expect, it } from "vitest";
import * as native from "../full-text.js";
import * as shipped from "../../engine/fts.js";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_SHARED_REACH } from "../../testing/reach.js";
import {
  dateDomainValue,
  timeDomainValue,
  intervalDomainValue,
  protectedSqlTextValue,
} from "../../engine/sql-domains.js";

it("pins independent v1 tokenizer, renderer and scorer fixtures", () => {
  expect([
    native.FTS_TOKENIZER_VERSION,
    native.FTS_RENDER_VERSION,
    native.FTS_SCORER_VERSION,
  ]).toEqual([1, 1, 1]);
  expect(native.tokenize).toBe(shipped.tokenize);
  expect(native.bm25Score).toBe(shipped.bm25Score);
  const corpus = new native.FtsStatsAccumulator(native.tokenizeQuery("alpha al*"));
  corpus.addDocument(["alpha", "alpine"]);
  corpus.addDocument([]);
  expect(corpus.stats).toEqual({ docCount: 2, totalTokens: 2, dfByTerm: [1, 1] });
  for (const [text, expected] of [
    ["ﬁle ＡＢＣ, Straße", ["file", "abc", "straße"]],
    ["abc東京タワーdef", ["abc", "東京", "京タ", "タワ", "ワー", "def"]],
    ["a".repeat(31) + "\u{10400}" + "b", ["a".repeat(31) + "\ud801"]],
    ["!!!", []],
  ] as const) {
    expect(native.tokenize(text)).toEqual(expected);
    expect(shipped.tokenize(text)).toEqual(expected);
  }
  expect(native.tokenize("a ".repeat(4097))).toHaveLength(4096);
  expect(() => native.tokenize("a".repeat(16385))).toThrow(RangeError);
  expect(() => native.tokenize("\ud800")).toThrow();
  expect(native.tokenizeQuery("data-base* data data*")).toEqual([
    { term: "data", prefix: false },
    { term: "base", prefix: true },
    { term: "data", prefix: false },
    { term: "data", prefix: true },
  ]);
  expect(() => native.validateFtsQuery(Array.from({ length: 33 }, () => "a").join(" "))).toThrow(
    "32",
  );
  expect(native.fullTermsMask(32)).toBe(-1);
  const date = new Date("2026-01-02T03:04:05.000Z");
  date.toISOString = () => "wrong";
  for (const [value, expected] of [
    [date, "2026-01-02T03:04:05.000Z"],
    [42, "42"],
    [false, undefined],
    [null, undefined],
  ] as const)
    expect(native.renderDocumentValue(value)).toBe(expected);
  for (const [value, text] of [
    [dateDomainValue("2026-01-02"), "2026-01-02"],
    [timeDomainValue("10:00:00.500"), "10:00:00.500"],
    [intervalDomainValue("1 day 2 hours"), "0 mons 1 days 7200000000 usecs"],
    [protectedSqlTextValue("\0minnow-domain:time:literal"), "\0minnow-domain:time:literal"],
  ] as const) {
    expect(shipped.renderDocumentValue(value)).toBe(text);
    expect(native.renderDocumentValue(text)).toBe(text);
  }
  expect(native.bm25Score(2, 5, 4, 10, 100)).toBe(1.4301086016353546);
  expect(native.bm25Score(2, 20, 4, 10, 100)).toBe(0.9592191840237135);
  expect(native.ftsMatchTruth([], [null])).toBeNull();
  expect(native.ftsMatchTruth([], [""])).toBe(false);
});

it("applies representation callbacks once per renderable member", () => {
  const cell = {},
    seen: unknown[] = [];
  const renderer = (value: unknown): string | undefined => {
    seen.push(value);
    return value === cell ? "alpha" : undefined;
  };
  const terms = native.tokenizeQuery("alpha");
  expect(native.ftsMatchTruth(terms, [null, cell], renderer)).toBe(true);
  expect(
    native.ftsBm25Row(
      terms,
      [cell, null],
      { docCount: 1, totalTokens: 1, dfByTerm: [1] },
      renderer,
    ),
  ).toBe(0.28768207245178085);
  expect(seen).toEqual([cell, cell]);
});

it("does not render or tokenize NULL-only documents", () => {
  let calls = 0;
  const renderer = (): string => {
    calls++;
    throw new Error("unexpected renderer");
  };
  const terms = native.tokenizeQuery("alpha al*");
  const stats = { docCount: 1, totalTokens: 0, dfByTerm: [0, 0] };
  expect(native.ftsMatchTruth(terms, [null, undefined, false], renderer)).toBeNull();
  expect(native.ftsBm25Row(terms, [null, undefined, false], stats, renderer)).toBeNull();
  expect(calls).toBe(0);
  expect(shipped.ftsMatchTruth(terms, [null, null])).toBeNull();
  expect(shipped.ftsBm25Row(terms, [null, null], stats)).toBeNull();
});

for (const seed of seedsFor("next-fts-shared", [0xb7f1, 0xb7f2]))
  it(`matches independent ASCII model and shipped SQL, seed ${String(seed)}`, async () => {
    const random = mulberry32(seed),
      reach = createReach(`fts shared ${String(seed)}`, FTS_SHARED_REACH);
    const db = new MinnowDatabase(new MemoryBlockStore(), { ftsAutoIndexRows: 1_000_000 });
    const vocab = ["alpha", "alpine", "beta", "gamma"];
    const pick = () => vocab[Math.floor(random() * vocab.length)] ?? "alpha";
    const literal = (value: string | null) => (value === null ? "NULL" : `'${value}'`);
    try {
      await db.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY,a TEXT,b TEXT)");
      for (let round = 0; round < 12; round++) {
        reach.beginHistory();
        const rows: Array<[string | null, string | null]> = [
          [null, null],
          ["", null],
          ["alpha", "beta"],
          ["alpha alpha", "alpine"],
          ...Array.from({ length: 8 }, (): [string | null, string | null] => [
            random() < 0.2 ? null : `${pick()} ${pick()}`,
            random() < 0.2 ? null : pick(),
          ]),
        ];
        await db.execute("DELETE FROM docs");
        await db.execute(
          `INSERT INTO docs VALUES ${rows.map((row, id) => `(${String(id)},${literal(row[0])},${literal(row[1])})`).join(",")}`,
        );
        for (const query of ["alpha beta", "al*", "alpha alpha", "", `${pick()} ${pick()}`]) {
          // The ASCII model deliberately does not call tokenize/query/frequency/stats helpers.
          const terms = query
            .split(" ")
            .filter(Boolean)
            .map((t) => ({ word: t.replace(/\*$/, ""), prefix: t.endsWith("*") }));
          const docs = rows.map((row) =>
            row.flatMap((v) => (v === null ? [] : v.split(/[^a-z0-9]+/).filter(Boolean))),
          );
          const tf = docs.map((tokens) =>
            terms.map(
              (t) => tokens.filter((v) => (t.prefix ? v.startsWith(t.word) : v === t.word)).length,
            ),
          );
          const total = docs.reduce((n, t) => n + t.length, 0),
            df = terms.map((_, i) => tf.filter((f) => (f[i] ?? 0) > 0).length);
          const stats = { docCount: rows.length, totalTokens: total, dfByTerm: df };
          const expected = rows.map((row, id) => {
            const present = row.some((v) => v !== null),
              frequencies = tf[id] ?? [];
            let score = 0;
            for (let i = 0; i < terms.length; i++) {
              const n = frequencies[i] ?? 0,
                d = df[i] ?? 0;
              if (n && d)
                score +=
                  Math.log(1 + (rows.length - d + 0.5) / (d + 0.5)) *
                  ((n * 2.2) /
                    (n +
                      1.2 *
                        (1 -
                          0.75 +
                          (0.75 * (docs[id]?.length ?? 0)) / (total / rows.length || 1))));
            }
            return {
              id,
              m: present ? terms.length > 0 && frequencies.every((n) => n > 0) : null,
              s: present ? score : null,
            };
          });
          const actual = (
            await db.query(
              `SELECT id,MATCH(a,b) AGAINST '${query}' AS m,BM25(a,b) AGAINST '${query}' AS s FROM docs ORDER BY id`,
              { memoize: false },
            )
          ).rows;
          expect(actual).toEqual(expected);
          const ranked = (
            await db.query(
              `SELECT id,BM25(a,b) AGAINST '${query}' AS s FROM docs WHERE id>=2 ORDER BY s DESC NULLS LAST,id LIMIT 5`,
              { memoize: false },
            )
          ).rows;
          expect(ranked).toEqual(
            expected
              .filter((r) => r.id >= 2)
              .map(({ id, s }) => ({ id, s }))
              .sort((a, b) => {
                if (a.s === null) return b.s === null ? a.id - b.id : 1;
                if (b.s === null) return -1;
                return b.s - a.s || a.id - b.id;
              })
              .slice(0, 5),
          );
          reach.reach("filtered ranking retains whole-corpus stats");
          rows.forEach((row, id) => {
            const parsed = native.tokenizeQuery(query);
            expect(native.ftsMatchTruth(parsed, row)).toBe(expected[id]?.m);
            expect(native.ftsBm25Row(parsed, row, stats)).toBe(expected[id]?.s);
          });
          reach.reach("whole corpus includes NULL/empty rows");
          if (query === "alpha beta") reach.reach("cross-column AND");
          if (query === "al*") reach.reach("prefix df union");
          if (query === "alpha alpha") reach.reach("repeated query terms");
          if (query === "") reach.reach("empty query and NULL truth");
        }
      }
      reach.assertReached();
    } finally {
      await db.close();
    }
  }, 120_000);
