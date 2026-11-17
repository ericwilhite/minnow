/** Measurement policy. Qualified wrappers must audit host/load before AND after their children. */
export interface R6Environment {
  readonly MINNOW_PERF_LANE?: string;
  readonly MINNOW_R6_MODE?: string;
  readonly MINNOW_R6_QUALIFIED?: string;
}
export function r6Policy(env: R6Environment): { enforce: boolean; status: string } {
  const mode = env.MINNOW_R6_MODE ?? (env.MINNOW_PERF_LANE === "1" ? "accept" : "observe");
  if (mode === "observe") return { enforce: false, status: "NOT ACCEPTED — observation only" };
  if (mode !== "accept" || env.MINNOW_R6_QUALIFIED !== "1")
    throw new Error("R6 NOT ACCEPTED: absolute judging requires a qualified quiet-host wrapper");
  return { enforce: true, status: "PENDING — qualified host audit must finish" };
}

export interface LatencySample {
  readonly count: number;
  readonly p50: number;
  readonly p95: number;
}
export function validateLatencySample(sample: LatencySample, count: number): void {
  if (
    sample.count !== count ||
    ![sample.p50, sample.p95].every((n) => Number.isFinite(n) && n >= 0) ||
    sample.p95 < sample.p50
  )
    throw new Error("Invalid R6 finite-value/sample invariant");
}
