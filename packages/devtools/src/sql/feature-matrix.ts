/** One unsupported feature, as the generated `unsupported-features.ts` records it. */
export interface UnsupportedFeatureRecord {
  id: string;
  /** The fragment of the engine's error message that names this feature. */
  error: string;
  notes?: string;
}

export interface UnsupportedFeature {
  id: string;
  notes?: string;
}

/**
 * Matches a failure against the features the engine records as unsupported, so "Expected SELECT,
 * found BEGIN" can also say which capability it ran into and what stands in for it.
 *
 * Only fragments recorded against exactly one feature are used. `Expected SELECT` covers both DDL
 * and transactions — and is what a perfectly supported `DELETE` reports when run through the
 * read-only path — so an ambiguous fragment explains nothing rather than guessing wrong.
 */
export function buildFailureIndex(
  features: readonly UnsupportedFeatureRecord[],
): Map<string, UnsupportedFeature> {
  const byFragment = new Map<string, UnsupportedFeatureRecord[]>();
  for (const feature of features) {
    byFragment.set(feature.error, [...(byFragment.get(feature.error) ?? []), feature]);
  }
  const index = new Map<string, UnsupportedFeature>();
  for (const [fragment, matches] of byFragment) {
    const only = matches.length === 1 ? matches[0] : undefined;
    if (only === undefined) continue;
    index.set(fragment, {
      id: only.id,
      ...(only.notes === undefined ? {} : { notes: only.notes }),
    });
  }
  return index;
}

export function lookupFailure(
  index: ReadonlyMap<string, UnsupportedFeature>,
  message: string,
): UnsupportedFeature | undefined {
  for (const [fragment, feature] of index) {
    if (message.includes(fragment)) return feature;
  }
  return undefined;
}

/** The sentence appended to a failure, naming the feature and what replaces it. */
export function describeUnsupported(feature: UnsupportedFeature): string {
  return feature.notes === undefined
    ? `${feature.id} is a known unsupported feature.`
    : `${feature.id} is not supported: ${feature.notes}`;
}
