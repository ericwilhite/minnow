/** CPU comparisons pair adjacent AB/BA runs over one immutable fixture. */
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
  const lower = sorted[(sorted.length - 1) >>> 1];
  const upper = sorted[sorted.length >>> 1];
  const first = sorted[0],
    last = sorted.at(-1);
  if (lower === undefined || upper === undefined || first === undefined || last === undefined)
    throw new RangeError("Missing paired CPU samples");
  const median = (lower + upper) / 2;
  const spread = last - first;
  const favoursCandidate = pairs.filter((pair) => pair.delta < 0).length;
  const favoursControl = pairs.filter((pair) => pair.delta > 0).length;
  const requiredFavourable = Math.ceil((7 * pairs.length) / 9);
  return {
    pairs,
    medianDeltaMs: median,
    spreadDeltaMs: spread,
    favoursCandidate,
    favoursControl,
    requiredFavourable,
    acceptedGain: -median > spread && favoursCandidate >= requiredFavourable,
    significantRegression: median > spread && favoursControl >= requiredFavourable,
    // A whole query may be indistinguishable. Reject even an uncertain positive
    // delta beyond its spread; never rescue a failed component with this check.
    noRegression: median <= spread,
  };
}
