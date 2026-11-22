/**
 * Reviewed parity gaps: SQL the shipped 0.x engine supports and next/ declares unsupported for
 * now, each refused with SQLSTATE 0A000 (feature_not_supported) and a stable message. Generated
 * suites that run 0.x-shaped work against next/ (the interaction simulator's next/ driver; the
 * feature-matrix parity audit) skip a read refused by an entry here and fail on anything else:
 * a wrong answer, an internal error, or a refusal that is not listed. Adding an entry is a
 * reviewed decision; closing a gap removes it.
 */
export interface ParityGap {
  /** Stable key, used in counts and reports. */
  readonly id: string;
  /** The refusal's message, as the engine words it. */
  readonly message: RegExp;
  /** Why it is open, and where closing it is tracked. */
  readonly reason: string;
}

export const NEXT_PARITY_GAPS: readonly ParityGap[] = [
  {
    id: "trigger-indexed-domain-preflight",
    message: /^The executor does not support native trigger indexed-domain preflight$/u,
    reason:
      "a rekey whose native trigger bodies need an indexed-domain preflight (writes stop the plan)",
  },
  {
    // 0.20 runs these. Cause: expressions with variable-size output (text and array
    // concatenation, casts to text, JSON constructors and casts) compile only inside the
    // pipeline's resumable projection/filter, and these sites evaluate outside it. Closing it:
    // a row host (design note P1-phase-expression-design.md, mechanism A); each step removes
    // its sites from the reason, and the last removes this entry.
    id: "row-site-variable-expression",
    message:
      /^The executor does not support variable expression outside a resumable projection\/filter$/u,
    reason:
      "such an expression in INSERT ... VALUES, a CHECK constraint, a generated column, ON CONFLICT DO UPDATE SET/WHERE, or RETURNING",
  },
  {
    // 0.20 runs these. Cause: a pipeline JSON expression needs a bounded source envelope, and
    // jsonInputsBounded (exec/json-plan.ts) accepts table columns, literals and their
    // projections but not a parameter, although its bytes are known once bound. Closing it:
    // accept bound parameters as a bounded source (their bound size is the envelope).
    id: "json-parameter-document",
    message: /^The executor does not support JSON document without a bounded source envelope$/u,
    reason:
      "a JSON value built from a parameter in a query, such as INSERT ... SELECT ($1::text)::jsonb",
  },
  {
    // 0.20 runs these. Cause: a producer-phase cut restarts the graph from the root, and the
    // UNION ALL concat recipe keeps no continuation, so it would replay arms already delivered.
    // Closing it: record completed arms across a cut (skip them on restart), allow UNION ALL in
    // phaseRetainingParents, drop the refusal; then the differential's concatenating arms
    // compare with PostgreSQL. Estimate: about a day (~100 engine lines, ~200 test lines).
    id: "union-all-phase-arm",
    message:
      /^The executor does not support an expression evaluated in producer phases \(text concatenation, JSON expansion or a JSON aggregate\) inside a UNION ALL arm/u,
    reason:
      "text concatenation or JSON work inside a UNION ALL arm, or in the input of ROLLUP, CUBE or GROUPING SETS",
  },
];

/** The listed gap `error` (or an error in its cause chain) is, or null. */
export function listedGap(error: unknown): ParityGap | null {
  const seen = new Set<unknown>();
  let current: unknown = error;
  while (typeof current === "object" && current !== null && !seen.has(current)) {
    seen.add(current);
    const { sqlState, message, cause } = current as {
      sqlState?: unknown;
      message?: unknown;
      cause?: unknown;
    };
    if (sqlState === "0A000" && typeof message === "string")
      for (const gap of NEXT_PARITY_GAPS) if (gap.message.test(message)) return gap;
    current = cause;
  }
  return null;
}

/**
 * Open findings: 0.x-shaped plans where next/ gives a different answer than the documented one,
 * each reduced, owned and tracked. A plan whose FIRST failure matches one is counted as blocked
 * by it, never passed; any other failure fails. Fixing a finding removes its entry, and its
 * seeds join regression-seeds.json.
 */
export interface OpenFinding {
  readonly id: string;
  /** The simulator's failure message for it. */
  readonly failure: RegExp;
  readonly reason: string;
  readonly owner: string;
}

export const NEXT_OPEN_FINDINGS: readonly OpenFinding[] = [
  {
    id: "rekey-forwards",
    failure:
      /^rekey-forward: reading retired key \d+: |^duplicate insert (inside a transaction )?was accepted, but the model refuses it \(unique\)/u,
    reason:
      "a key retired by a rekey neither forwards to its row nor counts as taken " +
      "(sql/dml.mdx, engine/rekeying.mdx)",
    owner: "C1",
  },
];

/** The open finding a simulator failure is, or null. */
export function openFinding(error: unknown): OpenFinding | null {
  const message = (error as { message?: unknown } | null)?.message;
  if (typeof message !== "string") return null;
  return NEXT_OPEN_FINDINGS.find((finding) => finding.failure.test(message)) ?? null;
}
