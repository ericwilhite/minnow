/** Acceptance policy, independent of the runner and engine. Any busy sample invalidates the run. */
export class QuietLoad {
  readonly samples: Array<{ phase: string; at: string; load1: number }> = [];
  observe(load1: number, phase: string) {
    this.samples.push({ phase, at: new Date().toISOString(), load1 });
  }
  assert() {
    if (this.samples.some((s) => !Number.isFinite(s.load1) || s.load1 < 0 || s.load1 > 1))
      throw new Error("REFUSED: load1 exceeded 1 or was unavailable; no performance report");
  }
}
interface Pass {
  rows: number;
  mode: string;
  label: string;
  residentBeforeMiB: number;
  rowsPerSecond: number;
  exportCpuSeconds: number;
  rendererCampaignGrowthMiB: number;
  executorStats: { path: string };
  host: { before: { load: number[] }; after: { load: number[] } };
  fixture: { seed: number; hash: string; independent: boolean };
}
export function assessGateA(
  report: {
    built: { groups: number; generatedGroups: number };
    exportedRows: number;
    results: Pass[];
  },
  seed: number,
  hash: string,
  load: QuietLoad,
) {
  if (
    report.exportedRows !== 1048576 ||
    report.built.groups !== 17 ||
    report.built.generatedGroups !== 17 ||
    report.results.length !== 5
  )
    throw new Error("REFUSED: not the independent 17-group, five-pass W1M fixture");
  for (const [index, p] of report.results.entries()) {
    if (
      p.rows !== 1048576 ||
      p.mode !== "scan" ||
      !p.label.startsWith("cold persistent candidate") ||
      p.executorStats.path !== "production-owner-api" ||
      p.fixture.seed !== seed ||
      p.fixture.hash !== hash ||
      !p.fixture.independent ||
      !Number.isFinite(p.residentBeforeMiB) ||
      p.residentBeforeMiB < 0 ||
      p.residentBeforeMiB > 1
    )
      throw new Error("REFUSED: fixture, route or cold residency mismatch");
    for (const at of ["before", "after"] as const)
      load.observe(p.host[at].load[0] ?? NaN, `pass-${index}-${at}`);
    if (
      ![p.rowsPerSecond, p.exportCpuSeconds, p.rendererCampaignGrowthMiB].every(Number.isFinite) ||
      p.rowsPerSecond <= 0 ||
      p.exportCpuSeconds <= 0 ||
      p.rendererCampaignGrowthMiB < 0
    )
      throw new Error("REFUSED: missing metrics");
  }
  load.assert();
  const summary = (key: "rowsPerSecond" | "exportCpuSeconds" | "rendererCampaignGrowthMiB") => {
    const v = report.results.map((p) => p[key]).sort((a, b) => a - b);
    return { median: v[2], min: v[0], max: v[4] };
  };
  return {
    seed,
    fixtureHash: hash,
    rowsPerSecond: summary("rowsPerSecond"),
    workerCpuSeconds: summary("exportCpuSeconds"),
    rendererGrowthMiB: summary("rendererCampaignGrowthMiB"),
    allRatesMeetTarget: report.results.every((p) => p.rowsPerSecond >= 190000),
    allRssWithinBound: report.results.every((p) => p.rendererCampaignGrowthMiB <= 256),
    hostLoad: load.samples,
  };
}
