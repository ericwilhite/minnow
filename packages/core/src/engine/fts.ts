/**
 * Full-text search primitives: the tokenizer, the query grammar, and the BM25 scorer.
 *
 * The tokenizer is deliberately hand-rolled rather than `Intl.Segmenter`: segmentation output
 * must be identical across browsers, ICU versions, and time, because phase-2 persists tokens in
 * a multi-tab shared index — an environment-varying tokenizer would silently split one logical
 * index into two vocabularies. Any change to tokenization must bump `FTS_TOKENIZER_VERSION`;
 * a persisted index stamped with another version reads as absent and rebuilds.
 *
 * The scorer takes only integers (term frequency, document length, document frequency, document
 * count, total token count), so the row and columnar executors produce bit-identical scores no
 * matter what order they accumulate in. Keep it that way: never feed it a running float.
 */

export const FTS_TOKENIZER_VERSION = 1;

/** BM25 constants — the standard defaults. */
const BM25_K1 = 1.2;
const BM25_B = 0.75;

const MAX_TOKEN_LENGTH = 32;
const MAX_TOKENS_PER_DOCUMENT = 4096;

const TOKEN_RUN = /[\p{L}\p{N}]+/gu;
// Script_Extensions rather than Script: shared marks like the katakana prolonged sound mark
// (U+30FC, script Common) must stay inside the kana run they extend.
const CJK =
  /[\p{Script_Extensions=Han}\p{Script_Extensions=Hiragana}\p{Script_Extensions=Katakana}]/u;

/** One parsed query term; `prefix` terms match any token they prefix. */
export interface FtsQueryTerm {
  readonly term: string;
  readonly prefix: boolean;
}

/**
 * Corpus statistics for one (columns, query) signature — O(#query terms) integers. Every row of
 * the corpus counts as a document (an all-null document has length 0): this definition is what
 * lets a persisted index serve exact statistics without a scan, since the document count is
 * just the table's row count. All producers — the row executor, the columnar scan, and the
 * postings index — must share it, or BM25 scores drift between paths.
 */
export interface FtsStats {
  /** Every row of the corpus, including all-null documents. */
  docCount: number;
  /** Total tokens across all documents. */
  totalTokens: number;
  /** Documents containing each query term, aligned with the query's term order. */
  dfByTerm: number[];
}

function pushToken(tokens: string[], token: string): void {
  if (tokens.length >= MAX_TOKENS_PER_DOCUMENT) return;
  tokens.push(token.length > MAX_TOKEN_LENGTH ? token.slice(0, MAX_TOKEN_LENGTH) : token);
}

/**
 * Splits one normalized run into tokens: contiguous CJK subsequences emit character bigrams
 * (a single character when alone), everything else stays one token. Bigrams give usable CJK
 * search without a dictionary while staying fully deterministic.
 */
function emitRun(tokens: string[], run: string): void {
  if (!CJK.test(run)) {
    pushToken(tokens, run);
    return;
  }
  const characters = [...run];
  let start = 0;
  while (start < characters.length) {
    const isCjk = CJK.test(characters[start] ?? "");
    let end = start + 1;
    while (end < characters.length && CJK.test(characters[end] ?? "") === isCjk) end += 1;
    if (isCjk) {
      if (end - start === 1) pushToken(tokens, characters[start] ?? "");
      for (let index = start; index < end - 1; index += 1) {
        pushToken(tokens, (characters[index] ?? "") + (characters[index + 1] ?? ""));
      }
    } else {
      pushToken(tokens, characters.slice(start, end).join(""));
    }
    start = end;
  }
}

/** Tokenizes one cell's text: NFKC, locale-independent lowercase, letter/number runs. */
export function tokenize(text: string): string[] {
  const tokens: string[] = [];
  const normalized = text.normalize("NFKC").toLowerCase();
  for (const match of normalized.matchAll(TOKEN_RUN)) {
    emitRun(tokens, match[0]);
    if (tokens.length >= MAX_TOKENS_PER_DOCUMENT) break;
  }
  return tokens;
}

/**
 * Parses a query: whitespace-separated chunks, each tokenized with the document pipeline; a
 * chunk ending in `*` marks its last produced token as a prefix term. Zero terms means the
 * query matches nothing.
 */
export function tokenizeQuery(query: string): FtsQueryTerm[] {
  const terms: FtsQueryTerm[] = [];
  for (const chunk of query.split(/\s+/)) {
    if (chunk.length === 0) continue;
    const prefix = chunk.endsWith("*");
    const tokens = tokenize(prefix ? chunk.slice(0, -1) : chunk);
    tokens.forEach((term, index) => {
      terms.push({ term, prefix: prefix && index === tokens.length - 1 });
    });
  }
  return terms;
}

export function termMatches(token: string, term: FtsQueryTerm): boolean {
  return term.prefix ? token.startsWith(term.term) : token === term.term;
}

/**
 * Compile-time cap on query terms: the columnar fast path packs per-term presence into one
 * 32-bit mask per dictionary entry.
 */
export const FTS_MAX_QUERY_TERMS = 32;

const queryTermsCache = new Map<string, FtsQueryTerm[]>();

/** Tokenized query terms with the same bounded caching scheme as the LIKE pattern cache. */
export function cachedQueryTerms(query: string): FtsQueryTerm[] {
  const cached = queryTermsCache.get(query);
  if (cached !== undefined) return cached;
  const terms = tokenizeQuery(query);
  if (queryTermsCache.size >= 128) queryTermsCache.clear();
  queryTermsCache.set(query, terms);
  return terms;
}

/** Compile-time validation shared by the SQL parser and the DSL expression builder. */
export function validateFtsQuery(query: string): void {
  if (cachedQueryTerms(query).length > FTS_MAX_QUERY_TERMS) {
    throw new TypeError(`Full-text queries support at most ${String(FTS_MAX_QUERY_TERMS)} terms`);
  }
}

/** Bitmask of query terms present in a token list: bit i set means terms[i] matched. */
export function termsMask(tokens: readonly string[], terms: readonly FtsQueryTerm[]): number {
  let mask = 0;
  for (const token of tokens) {
    for (let index = 0; index < terms.length; index += 1) {
      const term = terms[index];
      if (term !== undefined && (mask & (1 << index)) === 0 && termMatches(token, term)) {
        mask |= 1 << index;
      }
    }
  }
  return mask;
}

/** The all-terms-present mask; terms.length is capped at 32 so the shift stays in range. */
export function fullTermsMask(termCount: number): number {
  return termCount >= 32 ? -1 : (1 << termCount) - 1;
}

/**
 * Document-level MATCH truth over one row's rendered column values: every term must appear in
 * some column (AND across terms, OR across columns). All columns null → SQL unknown (null);
 * an empty query matches nothing.
 */
export function ftsMatchTruth(
  terms: readonly FtsQueryTerm[],
  values: Iterable<unknown>,
): boolean | null {
  let anyPresent = false;
  const present = new Array<boolean>(terms.length).fill(false);
  let satisfied = 0;
  for (const value of values) {
    const rendered = renderDocumentValue(value);
    if (rendered === undefined) continue;
    anyPresent = true;
    if (terms.length === 0 || satisfied === terms.length) continue;
    for (const token of tokenize(rendered)) {
      for (let index = 0; index < terms.length; index += 1) {
        const term = terms[index];
        if (term !== undefined && present[index] !== true && termMatches(token, term)) {
          present[index] = true;
          satisfied += 1;
        }
      }
      if (satisfied === terms.length) break;
    }
  }
  if (!anyPresent) return null;
  return terms.length > 0 && satisfied === terms.length;
}

/**
 * Per-term frequencies of the query's terms in one token list, aligned with the term order.
 * A document matches when every slot is non-zero.
 */
export function termFrequencies(tokens: readonly string[], terms: readonly FtsQueryTerm[]): number[] {
  const frequencies = new Array<number>(terms.length).fill(0);
  for (const token of tokens) {
    for (let index = 0; index < terms.length; index += 1) {
      const term = terms[index];
      if (term !== undefined && termMatches(token, term)) {
        frequencies[index] = (frequencies[index] ?? 0) + 1;
      }
    }
  }
  return frequencies;
}

/**
 * One term's BM25 contribution. Integer inputs only — see the module comment. `df` of zero
 * contributes nothing (the term appears in no document, so no row scores on it).
 */
export function bm25Score(
  tf: number,
  docLength: number,
  df: number,
  docCount: number,
  totalTokens: number,
): number {
  if (tf === 0 || df === 0 || docCount === 0) return 0;
  const idf = Math.log(1 + (docCount - df + 0.5) / (df + 0.5));
  const averageLength = totalTokens / docCount;
  const normalized =
    (tf * (BM25_K1 + 1)) /
    (tf + BM25_K1 * (1 - BM25_B + (BM25_B * docLength) / (averageLength === 0 ? 1 : averageLength)));
  return idf * normalized;
}

/** Sums the contributions of every query term for one document. */
export function bm25DocumentScore(
  frequencies: readonly number[],
  docLength: number,
  stats: FtsStats,
): number {
  let score = 0;
  for (let index = 0; index < frequencies.length; index += 1) {
    score += bm25Score(
      frequencies[index] ?? 0,
      docLength,
      stats.dfByTerm[index] ?? 0,
      stats.docCount,
      stats.totalTokens,
    );
  }
  return score;
}

/** Accumulates corpus statistics one document at a time; every producer feeds this. */
export class FtsStatsAccumulator {
  readonly #terms: readonly FtsQueryTerm[];
  readonly #stats: FtsStats;

  constructor(terms: readonly FtsQueryTerm[]) {
    this.#terms = terms;
    this.#stats = { docCount: 0, totalTokens: 0, dfByTerm: new Array<number>(terms.length).fill(0) };
  }

  /** Adds one document; an all-null row contributes a document of length 0. */
  addDocument(tokens: readonly string[]): void {
    this.#stats.docCount += 1;
    this.#stats.totalTokens += tokens.length;
    const frequencies = termFrequencies(tokens, this.#terms);
    for (let index = 0; index < frequencies.length; index += 1) {
      if ((frequencies[index] ?? 0) > 0) {
        this.#stats.dfByTerm[index] = (this.#stats.dfByTerm[index] ?? 0) + 1;
      }
    }
  }

  /** Adds one document from pre-aggregated counts (the dictionary-table producers). */
  addDocumentCounts(termMask: number, length: number): void {
    this.#stats.docCount += 1;
    this.#stats.totalTokens += length;
    for (let index = 0; index < this.#terms.length; index += 1) {
      if ((termMask & (1 << index)) !== 0) {
        this.#stats.dfByTerm[index] = (this.#stats.dfByTerm[index] ?? 0) + 1;
      }
    }
  }

  get stats(): FtsStats {
    return this.#stats;
  }
}

/**
 * Document-level BM25 for one row's rendered column values: term frequencies and document
 * length sum across columns (the document is the concatenation of its fields). All columns
 * null → SQL null; a document containing no query term scores 0.
 */
export function ftsBm25Row(
  terms: readonly FtsQueryTerm[],
  values: Iterable<unknown>,
  stats: FtsStats,
): number | null {
  let anyPresent = false;
  let docLength = 0;
  const frequencies = new Array<number>(terms.length).fill(0);
  for (const value of values) {
    const rendered = renderDocumentValue(value);
    if (rendered === undefined) continue;
    anyPresent = true;
    const tokens = tokenize(rendered);
    docLength += tokens.length;
    const partial = termFrequencies(tokens, terms);
    for (let index = 0; index < terms.length; index += 1) {
      frequencies[index] = (frequencies[index] ?? 0) + (partial[index] ?? 0);
    }
  }
  if (!anyPresent) return null;
  return bm25DocumentScore(frequencies, docLength, stats);
}

/** Renders one cell for the document, per column type. Booleans are excluded from documents. */
export function renderDocumentValue(value: unknown): string | undefined {
  if (value === null || value === undefined || typeof value === "boolean") return undefined;
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "number") return String(value);
  return typeof value === "string" ? value : undefined;
}
