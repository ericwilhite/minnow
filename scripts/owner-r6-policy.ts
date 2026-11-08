/** Quiet-host admission: contaminated measurements are retained as raw evidence, never reported. */
export class QuietHost {
  readonly samples: Array<{ at: string; load: readonly number[] }> = [];
  observe(load: readonly number[]): void {
    this.samples.push({ at: new Date().toISOString(), load: [...load] });
  }
  assertQuiet(): void {
    if (
      this.samples.length === 0 ||
      this.samples.some(
        ({ load }) => !Number.isFinite(load[0]) || (load[0] ?? -1) < 0 || (load[0] ?? Infinity) > 1,
      )
    )
      throw new Error("R6 report refused: load1 must stay <= 1 throughout measurement");
  }
}
