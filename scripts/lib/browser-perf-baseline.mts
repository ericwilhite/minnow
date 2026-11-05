/**
 * The browser lane's checked-in baseline (`packages/core/browser-perf-baseline.json`).
 *
 * It mirrors the Node gate's design: a cell is judged by the ratio of Minnow's time to a
 * competitor's time measured in the same run, never by a wall-clock budget, and each runtime
 * profile (OS, architecture, Chromium major) is calibrated separately with `--update`.
 *
 * Unlike the Node gate it also records what a cell *does*. Many analytics cells fail at default
 * options today, so each cell carries an expected status: `completes`, or `fails` with its failure
 * class. The lane flags:
 *   - a database that will not reopen after a failed Minnow cell (always fatal, never retried);
 *   - a Minnow result that disagrees with the reference engine (always fatal);
 *   - a cell that used to complete and now fails (confirmed by a re-run);
 *   - a ratio past its threshold across the whole sampled range (confirmed by a re-run).
 *
 * The file also holds the design targets from the 1.0 plan (Gates A–D and the point targets).
 * They are progress markers: the lane prints how far each cell is from its target and never
 * fails on one. `--update` rewrites a profile's calibration and leaves the targets alone.
 */
import type { EngineId } from "../browser-perf/catalogue.ts";
import type { FailureClass, ResultCheck, TimingSummary } from "./browser-perf-results.mts";
import { isMismatch } from "./browser-perf-results.mts";

export interface CellBaseline {
  readonly reference: EngineId;
  readonly status: "completes" | "fails";
  /** The failure class a known-failing cell fails with. */
  readonly failure?: FailureClass;
  /** Minnow ÷ reference warm medians when the baseline was recorded. */
  readonly ratio?: number;
  /** The ratio regression threshold. Absent when the reference engine itself fails the cell. */
  readonly threshold?: number;
}

export interface CellTarget {
  /** `A`–`D` for the analytics gates, `point` for the point-lane targets. */
  readonly gate: string;
  /** Target Minnow ÷ reference ratio (or ÷ `versus`), lower is better. */
  readonly ratio?: number;
  /** Target absolute warm median, in the cell's unit. */
  readonly ms?: number;
  /** Target: the cell completes at default options. */
  readonly completes?: boolean;
  /** Compare with another Minnow cell instead of the reference engine. */
  readonly versus?: string;
  readonly note?: string;
}

export interface BaselineProfile {
  /** True while the numbers come from a noisy or unrepresentative host. */
  readonly provisional: boolean;
  readonly recorded: string;
  readonly note?: string;
  readonly cells: Record<string, CellBaseline>;
}

export interface BrowserPerfBaseline {
  readonly schemaVersion: 1;
  readonly targets: Record<string, CellTarget>;
  readonly profiles: Record<string, BaselineProfile>;
}

function object(value: unknown, label: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new TypeError(`${label} must be an object`);
  return value as Record<string, unknown>;
}

const positive = (value: unknown, label: string): number => {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0)
    throw new Error(`${label} must be a positive finite number`);
  return value;
};

/** Parses the checked-in file field by field rather than trusting a type assertion. */
export function parseBrowserPerfBaseline(value: unknown): BrowserPerfBaseline {
  const root = object(value, "Browser performance baseline");
  if (root.schemaVersion !== 1) throw new Error("Unsupported browser performance baseline schema");
  const targets: Record<string, CellTarget> = {};
  for (const [cell, raw] of Object.entries(object(root.targets ?? {}, "targets"))) {
    const target = object(raw, `target ${cell}`);
    if (typeof target.gate !== "string") throw new Error(`target ${cell} needs a gate`);
    targets[cell] = {
      gate: target.gate,
      ...(target.ratio === undefined
        ? {}
        : { ratio: positive(target.ratio, `target ${cell}.ratio`) }),
      ...(target.ms === undefined ? {} : { ms: positive(target.ms, `target ${cell}.ms`) }),
      ...(target.completes === true ? { completes: true } : {}),
      ...(typeof target.versus === "string" ? { versus: target.versus } : {}),
      ...(typeof target.note === "string" ? { note: target.note } : {}),
    };
  }
  const profiles: Record<string, BaselineProfile> = {};
  for (const [name, raw] of Object.entries(object(root.profiles ?? {}, "profiles"))) {
    const profile = object(raw, `profile ${name}`);
    const cells: Record<string, CellBaseline> = {};
    for (const [cell, rawCell] of Object.entries(object(profile.cells, `profile ${name} cells`))) {
      const entry = object(rawCell, `${name}/${cell}`);
      if (entry.status !== "completes" && entry.status !== "fails")
        throw new Error(`${name}/${cell} has no valid status`);
      if (
        entry.reference !== "duckdb" &&
        entry.reference !== "sqlite" &&
        entry.reference !== "minnow"
      ) {
        throw new Error(`${name}/${cell} has no valid reference engine`);
      }
      cells[cell] = {
        reference: entry.reference,
        status: entry.status,
        ...(typeof entry.failure === "string" ? { failure: entry.failure as FailureClass } : {}),
        ...(entry.ratio === undefined
          ? {}
          : { ratio: positive(entry.ratio, `${name}/${cell}.ratio`) }),
        ...(entry.threshold === undefined
          ? {}
          : { threshold: positive(entry.threshold, `${name}/${cell}.threshold`) }),
      };
    }
    profiles[name] = {
      provisional: profile.provisional === true,
      recorded: typeof profile.recorded === "string" ? profile.recorded : "",
      ...(typeof profile.note === "string" ? { note: profile.note } : {}),
      cells,
    };
  }
  return { schemaVersion: 1, targets, profiles };
}

/** `linux-x64-chromium148`: ratios are specific to the OS, CPU architecture and browser major. */
export function browserPerfProfile(
  browserVersion: string,
  platform = process.platform,
  architecture = process.arch,
): string {
  const major = browserVersion.split(".")[0] ?? "";
  if (!/^\d+$/.test(major))
    throw new Error(`Cannot identify the Chromium major version from ${browserVersion}`);
  return `${platform}-${architecture}-chromium${major}`;
}

/** What one engine did in one cell, reduced to what the baseline judges. */
export interface EngineOutcome {
  readonly ok: boolean;
  readonly failure?: FailureClass;
  readonly timing?: TimingSummary;
  /** Measured once (a load), so it has no spread to read. */
  readonly onceOnly?: boolean;
}

export interface CellObservation {
  readonly cell: string;
  readonly reference: EngineId;
  readonly minnow: EngineOutcome & {
    /** The store would not reopen after this cell failed. */
    readonly reopenFailed?: boolean;
    /** Minnow's result against the reference engine's. */
    readonly check?: ResultCheck;
  };
  readonly referenceOutcome?: EngineOutcome;
}

export type VerdictKind =
  | "ok"
  | "reopen-failure"
  | "wrong-result"
  | "newly-failing"
  | "ratio-regression"
  | "uncalibrated"
  | "newly-completing"
  | "failure-changed"
  | "reference-failed";

export interface Verdict {
  readonly cell: string;
  readonly kind: VerdictKind;
  /** Fails the run on the first sighting; never dismissed by a re-run. */
  readonly fatal: boolean;
  /** Fails the run only if a fresh re-run flags the same cell again. */
  readonly confirmable: boolean;
  readonly message: string;
}

/** Minnow ÷ reference on medians, and the least favourable reading the samples allow. */
export function ratioRange(
  minnow: TimingSummary,
  reference: TimingSummary,
): { typical: number; lower: number; upper: number } {
  const r = (a: number, b: number): number => Math.max(a, 1e-6) / Math.max(b, 1e-6);
  return {
    typical: r(minnow.median, reference.median),
    lower: r(minnow.best, reference.worst),
    upper: r(minnow.worst, reference.best),
  };
}

/**
 * Headroom for a recorded threshold. The spread of both engines' samples picks the margin and
 * the bounds keep it honest: never tighter than 1.5× (one browser run on a shared runner moves
 * further than nine Node samples do), never looser than 3×. A once-only measurement has no
 * spread to read and gets the ceiling.
 */
export const MARGIN_FLOOR = 1.5;
export const MARGIN_CEILING = 3;

export function thresholdFor(
  minnow: TimingSummary,
  reference: TimingSummary,
  onceOnly: boolean,
): number {
  const ratio = ratioRange(minnow, reference).typical;
  if (onceOnly || minnow.count < 2 || reference.count < 2) return ratio * MARGIN_CEILING;
  const spread = (minnow.worst / minnow.best) * (reference.worst / reference.best) * 1.1;
  return ratio * Math.min(MARGIN_CEILING, Math.max(MARGIN_FLOOR, spread));
}

const significant = (value: number): number => Number(value.toPrecision(3));

/** The baseline entry this observation would record. */
export function calibrate(observation: CellObservation): CellBaseline {
  const { minnow, referenceOutcome, reference } = observation;
  if (!minnow.ok) return { reference, status: "fails", failure: minnow.failure ?? "error" };
  if (
    referenceOutcome?.ok !== true ||
    minnow.timing === undefined ||
    referenceOutcome.timing === undefined
  ) {
    return { reference, status: "completes" };
  }
  const onceOnly = minnow.onceOnly === true || referenceOutcome.onceOnly === true;
  return {
    reference,
    status: "completes",
    ratio: significant(ratioRange(minnow.timing, referenceOutcome.timing).typical),
    threshold: significant(thresholdFor(minnow.timing, referenceOutcome.timing, onceOnly)),
  };
}

/** Judges one observed cell against its recorded baseline. */
export function evaluate(
  observation: CellObservation,
  baseline: CellBaseline | undefined,
): Verdict[] {
  const { cell, minnow, referenceOutcome } = observation;
  const verdicts: Verdict[] = [];
  const add = (kind: VerdictKind, message: string, fatal = false, confirmable = false): void => {
    verdicts.push({ cell, kind, fatal, confirmable, message });
  };
  if (minnow.reopenFailed === true) {
    add(
      "reopen-failure",
      "the Minnow database would not reopen after this cell failed (Gate A)",
      true,
    );
  }
  if (minnow.ok && isMismatch(minnow.check)) {
    add(
      "wrong-result",
      `Minnow's result differs from ${observation.reference}'s (${String(minnow.check)})`,
      true,
    );
  }
  if (baseline === undefined) {
    add(
      "uncalibrated",
      "no baseline entry for this cell on this profile; record one with --update",
      true,
    );
    return verdicts;
  }
  if (baseline.status === "completes" && !minnow.ok) {
    add("newly-failing", `used to complete; now fails (${String(minnow.failure)})`, false, true);
  } else if (baseline.status === "fails" && minnow.ok) {
    add(
      "newly-completing",
      `used to fail (${String(baseline.failure)}); now completes — record it with --update`,
    );
  } else if (baseline.status === "fails" && !minnow.ok && baseline.failure !== minnow.failure) {
    add(
      "failure-changed",
      `still fails, now with ${String(minnow.failure)} instead of ${String(baseline.failure)}`,
    );
  }
  if (minnow.ok && minnow.timing !== undefined && baseline.threshold !== undefined) {
    if (referenceOutcome?.ok !== true || referenceOutcome.timing === undefined) {
      add(
        "reference-failed",
        `${observation.reference} did not complete, so the ratio was not judged`,
      );
    } else {
      const range = ratioRange(minnow.timing, referenceOutcome.timing);
      if (range.lower > baseline.threshold) {
        add(
          "ratio-regression",
          `ratio ${significant(range.typical)} (sampled lower bound ${significant(range.lower)}) exceeds threshold ${String(baseline.threshold)}`,
          false,
          true,
        );
      }
    }
  }
  if (verdicts.length === 0) add("ok", "within baseline");
  return verdicts;
}

/** A flag that repeats in a fresh browser is real; one that does not was noise. */
export function confirmedFlags(first: readonly Verdict[], second: readonly Verdict[]): Verdict[] {
  const repeated = new Set(second.filter((v) => v.confirmable).map((v) => v.cell));
  return first.filter((v) => v.confirmable && repeated.has(v.cell));
}

/** Merges recalibrated cells into one profile, keeping every other cell, profile and target. */
export function updatedBaseline(
  baseline: BrowserPerfBaseline | undefined,
  profile: string,
  cells: Record<string, CellBaseline>,
  meta: { provisional: boolean; recorded: string; note?: string },
): BrowserPerfBaseline {
  const previous = baseline?.profiles[profile]?.cells ?? {};
  const merged: Record<string, CellBaseline> = { ...previous, ...cells };
  const sorted = Object.fromEntries(
    Object.entries(merged).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)),
  );
  return {
    schemaVersion: 1,
    targets: baseline?.targets ?? {},
    profiles: { ...baseline?.profiles, [profile]: { ...meta, cells: sorted } },
  };
}

/** How far a cell is from its design target, in words. Never a failure. */
export function targetProgress(
  target: CellTarget | undefined,
  minnow: EngineOutcome,
  reference: EngineOutcome | undefined,
  versus: EngineOutcome | undefined,
): string {
  if (target === undefined) return "";
  if (!minnow.ok)
    return target.completes === true || target.ratio !== undefined || target.ms !== undefined
      ? "not met: fails"
      : "";
  const parts: string[] = [];
  if (target.ms !== undefined && minnow.timing !== undefined) {
    const ok = minnow.timing.median <= target.ms;
    parts.push(
      `≤${String(target.ms)} ms: ${ok ? "met" : `${significant(minnow.timing.median / target.ms)}× over`}`,
    );
  }
  if (target.ratio !== undefined && minnow.timing !== undefined) {
    const against = target.versus === undefined ? reference : versus;
    if (against?.ok === true && against.timing !== undefined) {
      const ratio = minnow.timing.median / against.timing.median;
      const ok = ratio <= target.ratio;
      parts.push(
        `≤${String(target.ratio)}×: ${ok ? "met" : `${significant(ratio / target.ratio)}× to go`}`,
      );
    } else {
      parts.push(`≤${String(target.ratio)}×: no comparison`);
    }
  }
  if (target.completes === true && parts.length === 0) parts.push("completes: met");
  return `${target.gate}: ${parts.join("; ")}`;
}
