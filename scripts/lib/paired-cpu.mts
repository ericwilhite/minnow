/** Adjacent AB/BA CPU deltas. Quartiles use linear interpolation at (n - 1) * p. */
export function pairedCPU(control: readonly number[], candidate: readonly number[]) {
  if (control.length < 9 || control.length !== candidate.length)
    throw new RangeError("At least nine complete CPU pairs are required");
  const pairs = control.map((before, index) => {
    const after = candidate[index];
    if (after === undefined || !Number.isFinite(before) || !Number.isFinite(after))
      throw new RangeError("CPU pairs must contain finite samples");
    return { control: before, candidate: after, delta: after - before };
  });
  const sorted = pairs.map((pair) => pair.delta).sort((a, b) => a - b);
  const quantile = (p: number) => {
    const at = (sorted.length - 1) * p,
      lo = Math.floor(at),
      lower = sorted[lo],
      upper = sorted[Math.ceil(at)];
    if (lower === undefined || upper === undefined)
      throw new RangeError("Missing paired CPU samples");
    return lower + (upper - lower) * (at - lo);
  };
  const median = quantile(0.5),
    q1 = quantile(0.25),
    q3 = quantile(0.75),
    iqr = q3 - q1,
    first = quantile(0),
    last = quantile(1),
    range = last - first;
  const favoursCandidate = pairs.filter((pair) => pair.delta < 0).length;
  const favoursControl = pairs.filter((pair) => pair.delta > 0).length;
  const requiredFavourable = Math.ceil((7 * pairs.length) / 9);
  const significantRegression = median > iqr && favoursControl >= requiredFavourable;
  return {
    pairs,
    medianDeltaMs: median,
    q1DeltaMs: q1,
    q3DeltaMs: q3,
    iqrDeltaMs: iqr,
    minDeltaMs: first,
    maxDeltaMs: last,
    rangeDeltaMs: range,
    /** Compatibility field: full range width, not the acceptance statistic. */
    spreadDeltaMs: range,
    favoursCandidate,
    favoursControl,
    requiredFavourable,
    acceptedGain: -median > iqr && favoursCandidate >= requiredFavourable,
    significantRegression,
    // Absence of an accepted reverse-direction regression, not proof of equivalence.
    noRegression: !significantRegression,
  };
}
