export const PERFORMANCE_ENGINES = ["sqlite", "pglite"] as const;
export type PerformanceEngine = (typeof PERFORMANCE_ENGINES)[number];

export type PerformanceThresholds = Record<string, Partial<Record<PerformanceEngine, number>>>;

export interface PerformanceBaselineFile {
  readonly schemaVersion: 1;
  readonly rows: number;
  readonly profiles: Record<string, { readonly thresholds: PerformanceThresholds }>;
}

export interface PerformanceTimingRange {
  readonly median: number;
  readonly best: number;
  readonly worst: number;
}

export interface PerformanceRatioRange {
  /** Median Minnow time divided by median comparison-engine time. */
  readonly typical: number;
  /** Minnow's fastest sample divided by the comparison engine's slowest sample. */
  readonly lower: number;
  /** Minnow's slowest sample divided by the comparison engine's fastest sample. */
  readonly upper: number;
}

/** A tiny floor protects ratios from a clock sample that rounds to zero. */
export function performanceRatio(minnowMs: number, engineMs: number): number {
  return Math.max(minnowMs, 0.0001) / Math.max(engineMs, 0.0001);
}

/** Returns the complete ratio range observed across both engines' timing samples. */
export function performanceRatioRange(
  minnow: PerformanceTimingRange,
  engine: PerformanceTimingRange,
): PerformanceRatioRange {
  return {
    typical: performanceRatio(minnow.median, engine.median),
    lower: performanceRatio(minnow.best, engine.worst),
    upper: performanceRatio(minnow.worst, engine.best),
  };
}

/**
 * A slowdown is conclusive only when the whole current sample range is beyond the recorded
 * threshold. Comparing only the medians turns harmless movement inside a fast shape's normal
 * range into a failure, especially when one side takes microseconds and the other milliseconds.
 */
export function hasPerformanceRegression(
  minnow: PerformanceTimingRange,
  engine: PerformanceTimingRange,
  threshold: number,
): boolean {
  return performanceRatioRange(minnow, engine).lower > threshold;
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

/** Performance ratios are runtime-specific enough to need an explicit host profile. */
export function runtimePerformanceProfile(
  platform = process.platform,
  architecture = process.arch,
  nodeVersion = process.versions.node,
): string {
  const [major] = nodeVersion.split(".");
  if (major === undefined || !/^\d+$/.test(major)) {
    throw new TypeError(`Cannot identify the Node major version from ${nodeVersion}`);
  }
  return `${platform}-${architecture}-node${major}`;
}

/** Parses the checked-in file without trusting a JSON type assertion. */
export function parsePerformanceBaseline(
  value: unknown,
  expectedRows: number,
): PerformanceBaselineFile {
  const root = record(value, "Performance baseline");
  if (root.schemaVersion !== 1) throw new Error("Unsupported performance baseline schema");
  if (root.rows !== expectedRows) {
    throw new Error(
      `Performance baseline covers ${String(root.rows)} rows; this gate uses ${String(expectedRows)}`,
    );
  }
  const rawProfiles = record(root.profiles, "Performance baseline profiles");
  const profiles: PerformanceBaselineFile["profiles"] = {};
  for (const [profileName, rawProfile] of Object.entries(rawProfiles)) {
    if (profileName.length === 0) throw new Error("Performance baseline profile name is empty");
    const profile = record(rawProfile, `Performance profile ${profileName}`);
    const rawThresholds = record(
      profile.thresholds,
      `Performance profile ${profileName} thresholds`,
    );
    const thresholds: PerformanceThresholds = {};
    for (const [workload, rawEngines] of Object.entries(rawThresholds)) {
      if (workload.length === 0)
        throw new Error(`Performance profile ${profileName} has an empty workload`);
      const engines = record(rawEngines, `${profileName}/${workload}`);
      const threshold: Partial<Record<PerformanceEngine, number>> = {};
      for (const engine of PERFORMANCE_ENGINES) {
        const candidate = engines[engine];
        if (candidate === undefined) continue;
        if (typeof candidate !== "number" || !Number.isFinite(candidate) || candidate <= 0) {
          throw new Error(`${profileName}/${workload}/${engine} must be a positive finite number`);
        }
        threshold[engine] = candidate;
      }
      thresholds[workload] = threshold;
    }
    profiles[profileName] = { thresholds };
  }
  return { schemaVersion: 1, rows: expectedRows, profiles };
}

/** Fails closed when a runtime or workload has never been calibrated. */
export function selectPerformanceThresholds(
  baseline: PerformanceBaselineFile | undefined,
  profile: string,
  workloads: readonly string[],
  updating: boolean,
): PerformanceThresholds | undefined {
  const thresholds = baseline?.profiles[profile]?.thresholds;
  if (thresholds === undefined) {
    if (updating) return undefined;
    throw new Error(
      `No performance baseline for ${profile}; run npm run benchmark:gate -- --update on that runtime`,
    );
  }
  if (!updating) {
    for (const workload of workloads) {
      for (const engine of PERFORMANCE_ENGINES) {
        if (thresholds[workload]?.[engine] === undefined) {
          throw new Error(`Performance baseline is missing ${profile}/${workload}/${engine}`);
        }
      }
    }
  }
  return thresholds;
}

/** Replaces one runtime profile while preserving calibration for every other host. */
export function updatedPerformanceBaseline(
  baseline: PerformanceBaselineFile | undefined,
  rows: number,
  profile: string,
  thresholds: PerformanceThresholds,
): PerformanceBaselineFile {
  return {
    schemaVersion: 1,
    rows,
    profiles: {
      ...baseline?.profiles,
      [profile]: { thresholds },
    },
  };
}
