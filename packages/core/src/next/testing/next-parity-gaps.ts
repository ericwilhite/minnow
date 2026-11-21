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
    id: "union-all",
    message: /^The executor does not support set operation other than binary EXCEPT$/u,
    reason: "UNION / UNION ALL / INTERSECT: the executor runs binary EXCEPT only (set operations)",
  },
  {
    id: "trigger-indexed-domain-preflight",
    message: /^The executor does not support native trigger indexed-domain preflight$/u,
    reason:
      "a rekey whose native trigger bodies need an indexed-domain preflight (writes stop the plan)",
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
