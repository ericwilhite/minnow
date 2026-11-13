/**
 * Declared reach for a campaign whose histories are split across several test files.
 *
 * A campaign asserts its reach over every history it ran (./reach.ts). When its histories are
 * spread over files so parallel runners can share them, no single file sees them all, and a file
 * that asserted its own subset would weaken the law. So each file records what its histories
 * reached to the run's reach directory, and one `*.reach.test.ts` file, which the test sequencer
 * keeps in the same part and runs after them, asserts the union against the declared reach.
 *
 * The union fails rather than passing vacuously: every declared file must have left a record, every
 * record must hold at least one history, no undeclared file may record, and every declared state
 * must be entered by at least `minimum` histories across all of them. As with `assertReached`, a
 * `MINNOW_SEED` replay checks one history rather than the campaign, so the union stands aside then.
 *
 * Like ./reach.ts, this module is excluded from the published tarball.
 */
import { mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll } from "vitest";
import type { Reach } from "./reach.js";
import { REACH_UNIONS } from "./reach-unions.js";

export { REACH_UNIONS };

export type UnionCampaign = keyof typeof REACH_UNIONS;

/** What one file's histories reached. */
export interface ReachRecord {
  readonly campaign: string;
  readonly file: string;
  readonly histories: number;
  readonly counts: Readonly<Record<string, number>>;
}

const repositoryRoot = fileURLToPath(new URL("../../../..", import.meta.url));

/** The run's reach directory, which the test run's global setup creates (vitest.config.ts). */
export function reachDirectory(): string {
  const dir = process.env.MINNOW_REACH_DIR ?? "";
  if (dir.trim() === "")
    throw new Error(
      "MINNOW_REACH_DIR is not set: split reach campaigns record through the run's global setup " +
        "(scripts/reach-directory.ts in vitest.config.ts)",
    );
  return dir;
}

function replaying(): boolean {
  return (process.env.MINNOW_SEED ?? "").trim() !== "";
}

/**
 * Records `reach` for one file of a split campaign when that file's tests finish. Returns the same
 * reach, counting its histories. `fileUrl` is the test file's `import.meta.url`.
 */
export function recordReach<State extends string>(
  campaign: UnionCampaign,
  reach: Reach<State>,
  fileUrl: string,
): Reach<State> {
  const file = relative(repositoryRoot, fileURLToPath(fileUrl)).split("\\").join("/");
  const declared: readonly string[] = REACH_UNIONS[campaign].files;
  if (!declared.includes(file))
    throw new Error(`${file} records reach for ${campaign} but is not one of its declared files`);
  let histories = 0;
  const begin = reach.beginHistory.bind(reach);
  reach.beginHistory = () => {
    histories += 1;
    begin();
  };
  afterAll(() => {
    const record: ReachRecord = {
      campaign,
      file,
      histories,
      counts: Object.fromEntries(reach.counts()),
    };
    const dir = join(reachDirectory(), campaign);
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, `${file.replace(/[\\/]/gu, "__")}.json`), JSON.stringify(record));
  });
  return reach;
}

/**
 * The union law over a campaign's records: every reason the campaign's split histories fall short
 * of its declared reach. Empty when the union holds.
 */
export function unionShortfalls(
  campaign: string,
  declaration: { readonly files: readonly string[] },
  states: readonly string[],
  records: readonly ReachRecord[],
  minimum = 1,
): string[] {
  const problems: string[] = [];
  const byFile = new Map<string, ReachRecord>();
  for (const record of records) {
    if (record.campaign !== campaign) continue;
    if (!declaration.files.includes(record.file))
      problems.push(`an undeclared file recorded reach: ${record.file}`);
    if (byFile.has(record.file)) problems.push(`two records from ${record.file}`);
    byFile.set(record.file, record);
  }
  for (const file of declaration.files) {
    const record = byFile.get(file);
    if (record === undefined) problems.push(`no reach record from ${file}: it did not run`);
    else if (record.histories < 1) problems.push(`${file} recorded no histories`);
  }
  const totals = new Map(states.map((state) => [state, 0]));
  let histories = 0;
  for (const record of byFile.values()) {
    histories += record.histories;
    for (const [state, count] of Object.entries(record.counts)) {
      const total = totals.get(state);
      if (total === undefined) problems.push(`${record.file} recorded undeclared state ${state}`);
      else totals.set(state, total + count);
    }
  }
  const missed = [...totals]
    .filter(([, count]) => count < minimum)
    .map(([state, count]) => `${state} (${String(count)})`);
  if (missed.length > 0)
    problems.push(
      `of ${String(histories)} generated histories across ${String(byFile.size)} files, fewer ` +
        `than ${String(minimum)} reached these states, so their laws were not enforced there: ` +
        missed.join(", "),
    );
  return problems;
}

/** Asserts the union of a split campaign's records against its declared reach. */
export function assertReachUnion(
  campaign: UnionCampaign,
  states: readonly string[],
  { minimum = 1 }: { minimum?: number } = {},
): void {
  if (replaying()) return;
  const dir = join(reachDirectory(), campaign);
  let names: string[] = [];
  try {
    names = readdirSync(dir).filter((name) => name.endsWith(".json"));
  } catch {
    // No directory: no file recorded; the shortfalls below name every missing one.
  }
  const records = names.map(
    (name) => JSON.parse(readFileSync(join(dir, name), "utf8")) as ReachRecord,
  );
  const problems = unionShortfalls(campaign, REACH_UNIONS[campaign], states, records, minimum);
  if (problems.length > 0) throw new Error(`${campaign}: ${problems.join("; ")}`);
}
