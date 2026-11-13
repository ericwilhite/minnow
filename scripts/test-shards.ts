/**
 * Balances `vitest --shard` by how long each test file takes rather than by file count.
 *
 * Vitest's own sharding sorts files by a hash of their path and cuts the list into equal counts.
 * With a few files taking a quarter of an hour and most taking under a second, equal counts gave
 * one CI part 6,500 s of test time and another 2,800 s. This sequencer gives each part the same
 * files a whole run would have — every file lands in exactly one part — but chooses the parts so
 * their measured durations are even, and starts each part's longest files first so none of them
 * begins near the end.
 *
 * The weights in `test-durations.json` only steer the split. A missing, stale, or wrong weight
 * makes a part slower; it can never drop a file or run one twice, because every part computes the
 * same deterministic assignment from the same file list. `npm run test:durations` refreshes them.
 */
import { relative } from "node:path";
import { BaseSequencer, type TestSpecification } from "vitest/node";
import table from "./test-durations.json" with { type: "json" };
import { REACH_UNIONS } from "../packages/core/src/testing/reach-unions.ts";
import { assignParts, defaultWeight } from "./test-parts.mjs";

/**
 * Files that must share a part: a split reach campaign's files and the file that asserts their
 * union, which reads what they recorded in this run (packages/core/src/testing/reach-union.ts).
 */
const together = new Map<string, string>(
  Object.entries(REACH_UNIONS).flatMap(([campaign, { files, union }]) =>
    [...files, union].map((file) => [file, `reach:${campaign}`] as const),
  ),
);

/** Milliseconds per repository-relative test file, measured under one way of running the suite. */
export type DurationProfile = Readonly<Record<string, number>>;

export interface DurationTable {
  /** `vitest run --coverage` on Node 24, as the CI coverage parts run it. */
  readonly coverage: DurationProfile;
  /** `vitest run` without coverage, as the oldest-Node parts and local runs use it. */
  readonly plain: DurationProfile;
}

export const durations: DurationTable = table;

export default class DurationSequencer extends BaseSequencer {
  private profile(): DurationProfile {
    return this.ctx.config.coverage.enabled ? durations.coverage : durations.plain;
  }

  private key(spec: TestSpecification): string {
    return relative(this.ctx.config.root, spec.moduleId).split("\\").join("/");
  }

  private weigher(): (key: string) => number {
    const profile = this.profile();
    const fallback = defaultWeight(profile);
    return (key) => profile[key] ?? fallback;
  }

  override async shard(files: TestSpecification[]): Promise<TestSpecification[]> {
    const { index, count } = this.ctx.config.shard ?? { index: 1, count: 1 };
    const weight = this.weigher();
    const workers = (config: { maxWorkers: unknown }) =>
      Math.max(1, Number(config.maxWorkers) || 1);
    const all = workers(this.ctx.config);
    // A project with fewer workers than the run (`fileParallelism: false` resolves to one: the
    // maintenance workload) holds the rest of its part's workers idle while its files run.
    const weights = files.map(
      (spec) => (weight(this.key(spec)) * all) / Math.min(all, workers(spec.project.config)),
    );
    // One unit per group of files that must share a part; any other file is its own unit. The
    // project name keeps a file that two projects both include apart.
    const unitOf = files.map(
      (spec) => together.get(this.key(spec)) ?? `${this.key(spec)}\0${spec.project.name}`,
    );
    const units = [...new Set(unitOf)];
    const unitWeight = new Map(units.map((unit) => [unit, 0]));
    unitOf.forEach((unit, i) =>
      unitWeight.set(unit, (unitWeight.get(unit) ?? 0) + (weights[i] ?? 0)),
    );
    // Other work the first part also does before its tests (`MINNOW_SHARD_RESERVE`, milliseconds
    // of wall time: the oldest-Node part that runs the full build) holds all its workers.
    const reserve = (Number(process.env.MINNOW_SHARD_RESERVE ?? "") || 0) * all;
    const unitParts = assignParts(
      units,
      units.map((unit) => unitWeight.get(unit) ?? 0),
      count,
      [reserve],
    );
    const partOf = new Map(units.map((unit, i) => [unit, unitParts[i]]));
    const parts = unitOf.map((unit) => partOf.get(unit));
    return files.filter((_, i) => parts[i] === index - 1);
  }

  override async sort(files: TestSpecification[]): Promise<TestSpecification[]> {
    const weight = this.weigher();
    const cache = this.ctx.cache;
    return [...files].sort((a, b) => {
      // The same precedence as Vitest's own sequencer: group order, then projects one at a time,
      // then isolated files first.
      const group = a.project.config.sequence.groupOrder - b.project.config.sequence.groupOrder;
      if (group !== 0) return group;
      if (a.project.name !== b.project.name) return a.project.name < b.project.name ? -1 : 1;
      if (a.project.config.isolate !== b.project.config.isolate)
        return a.project.config.isolate ? -1 : 1;
      // A file that failed in this checkout's last run goes first, as Vitest's sequencer does.
      const failed = (spec: TestSpecification) =>
        cache.getFileTestResults(
          `${spec.project.name}:${relative(this.ctx.config.root, spec.moduleId)}`,
        )?.failed === true;
      const fa = failed(a),
        fb = failed(b);
      if (fa !== fb) return fa ? -1 : 1;
      const ka = this.key(a),
        kb = this.key(b);
      return weight(kb) - weight(ka) || (ka < kb ? -1 : ka > kb ? 1 : 0);
    });
  }
}
