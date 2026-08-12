import { describe, expect, it } from "vitest";
import {
  bm25DocumentScore,
  bm25Score,
  ftsMatchTruth,
  fullTermsMask,
  renderDocumentValue,
  termFrequencies,
  termsMask,
  tokenize,
  tokenizeQuery,
  validateFtsQuery,
} from "./fts.js";

describe("tokenizer", () => {
  it("lowercases, NFKC-normalizes, and splits on non-alphanumerics", () => {
    expect(tokenize("Hello, World! it's 2026")).toEqual(["hello", "world", "it", "s", "2026"]);
    // NFKC folds the ligature and fullwidth forms before lowercasing.
    expect(tokenize("ﬁle ＡＢＣ")).toEqual(["file", "abc"]);
    expect(tokenize("Grüße")).toEqual(["grüße"]);
    expect(tokenize("kebab-case snake_case")).toEqual(["kebab", "case", "snake", "case"]);
    expect(tokenize("")).toEqual([]);
    expect(tokenize("!!! ...")).toEqual([]);
  });

  it("emits deterministic CJK character bigrams", () => {
    expect(tokenize("東京タワー")).toEqual(["東京", "京タ", "タワ", "ワー"]);
    expect(tokenize("猫")).toEqual(["猫"]);
    // Mixed runs keep the latin part whole and bigram only the CJK subsequence.
    expect(tokenize("abc東京def")).toEqual(["abc", "東京", "def"]);
  });

  it("caps token length at 32 code points", () => {
    const long = "a".repeat(40);
    expect(tokenize(long)).toEqual(["a".repeat(32)]);
  });

  it("is deterministic across repeated calls", () => {
    const text = "The Quick ﬁre 東京 2026-08-12 déjà";
    expect(tokenize(text)).toEqual(tokenize(text));
  });
});

describe("query grammar", () => {
  it("splits terms and marks trailing-star chunks as prefixes", () => {
    expect(tokenizeQuery("quick data*")).toEqual([
      { term: "quick", prefix: false },
      { term: "data", prefix: true },
    ]);
    // Only the last token produced by a starred chunk is a prefix.
    expect(tokenizeQuery("data-base*")).toEqual([
      { term: "data", prefix: false },
      { term: "base", prefix: true },
    ]);
    expect(tokenizeQuery("  ")).toEqual([]);
    expect(tokenizeQuery("*")).toEqual([]);
  });

  it("caps queries at 32 terms", () => {
    const wide = Array.from({ length: 33 }, (_, index) => `t${String(index)}`).join(" ");
    expect(() => validateFtsQuery(wide)).toThrow("at most 32 terms");
    expect(() => validateFtsQuery("just fine")).not.toThrow();
  });
});

describe("document matching", () => {
  it("ANDs terms across a document and ORs across columns", () => {
    const terms = tokenizeQuery("quick fox");
    expect(ftsMatchTruth(terms, ["the quick brown", "lazy fox"])).toBe(true);
    expect(ftsMatchTruth(terms, ["the quick brown", "lazy dog"])).toBe(false);
    expect(ftsMatchTruth(terms, [null, null])).toBeNull();
    expect(ftsMatchTruth(terms, [null, "quick fox"])).toBe(true);
    // Empty queries match nothing, but null documents stay unknown.
    expect(ftsMatchTruth([], ["anything"])).toBe(false);
    expect(ftsMatchTruth([], [null])).toBeNull();
  });

  it("matches prefixes and rendered numbers and dates", () => {
    expect(ftsMatchTruth(tokenizeQuery("dat*"), ["my database"])).toBe(true);
    expect(ftsMatchTruth(tokenizeQuery("42"), [42])).toBe(true);
    expect(ftsMatchTruth(tokenizeQuery("2026 08"), [new Date("2026-08-12T10:30:00Z")])).toBe(true);
    expect(ftsMatchTruth(tokenizeQuery("2027"), [new Date("2026-08-12T10:30:00Z")])).toBe(false);
    // Booleans are excluded from documents entirely.
    expect(ftsMatchTruth(tokenizeQuery("true"), [true])).toBeNull();
  });

  it("renders values per column type", () => {
    expect(renderDocumentValue("text")).toBe("text");
    expect(renderDocumentValue(1.5)).toBe("1.5");
    expect(renderDocumentValue(new Date("2026-08-12T00:00:00.000Z"))).toBe(
      "2026-08-12T00:00:00.000Z",
    );
    expect(renderDocumentValue(null)).toBeUndefined();
    expect(renderDocumentValue(true)).toBeUndefined();
  });

  it("packs term masks with full coverage detection", () => {
    const terms = tokenizeQuery("aa bb");
    expect(termsMask(["aa"], terms)).toBe(1);
    expect(termsMask(["bb"], terms)).toBe(2);
    expect(termsMask(["aa", "bb", "cc"], terms)).toBe(3);
    expect(fullTermsMask(2)).toBe(3);
    expect(fullTermsMask(32)).toBe(-1);
  });
});

describe("BM25", () => {
  it("computes the standard formula from integer inputs", () => {
    // One document corpus: df=1, docCount=1, totalTokens=4, tf=2, docLength=4.
    const idf = Math.log(1 + (1 - 1 + 0.5) / (1 + 0.5));
    const expected = idf * ((2 * 2.2) / (2 + 1.2 * (1 - 0.75 + (0.75 * 4) / 4)));
    expect(bm25Score(2, 4, 1, 1, 4)).toBeCloseTo(expected, 12);
    expect(bm25Score(0, 4, 1, 1, 4)).toBe(0);
    expect(bm25Score(2, 4, 0, 1, 4)).toBe(0);
  });

  it("is identical regardless of accumulation order", () => {
    const terms = tokenizeQuery("alpha beta");
    const stats = { docCount: 10, totalTokens: 100, dfByTerm: [4, 7] };
    const frequencies = termFrequencies(["alpha", "beta", "alpha"], terms);
    const forward = bm25DocumentScore(frequencies, 3, stats);
    const reversedStats = { docCount: 10, totalTokens: 100, dfByTerm: [7, 4] };
    const reversed = bm25DocumentScore([...frequencies].reverse(), 3, reversedStats);
    expect(forward).toBe(reversed);
  });
});
