/** Lead's write-load rule: paired CPU median > IQR, with at least 7/9 wins.
 * Quartiles use linear interpolation at (n-1)*p (R type 7). Keep the full range too.
 * The reverse rule detects regressions; a result passing neither rule is inconclusive.
 */
export function summarizePairDeltas(input: readonly number[]) {
  if (input.length < 9 || input.some((delta) => !Number.isFinite(delta)))
    throw new Error("Paired comparisons need at least nine finite deltas");
  const deltas = [...input];
  const sorted = [...deltas].sort((a, b) => a - b);
  const quantile = (values: readonly number[], p: number): number => {
    const at = (values.length - 1) * p;
    const lower = values[Math.floor(at)] ?? NaN;
    return lower + ((values[Math.ceil(at)] ?? lower) - lower) * (at - Math.floor(at));
  };
  const gain = quantile(sorted, 0.5);
  const iqr = quantile(sorted, 0.75) - quantile(sorted, 0.25);
  const mad = quantile(
    deltas.map((delta) => Math.abs(delta - gain)).sort((a, b) => a - b),
    0.5,
  );
  const wins = deltas.filter((delta) => delta > 0).length;
  const losses = deltas.filter((delta) => delta < 0).length;
  const required = Math.ceil((input.length * 7) / 9);
  return {
    deltas,
    medianGainMs: gain,
    iqrMs: iqr,
    fullRangeMs: (sorted.at(-1) ?? NaN) - (sorted[0] ?? NaN),
    madMs: mad,
    wins,
    losses,
    accepted: gain > iqr && wins >= required,
    regression: -gain > iqr && losses >= required,
  };
}
