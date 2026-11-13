/**
 * Vitest global setup: a fresh reach directory for this run (packages/core/src/testing/reach-union.ts).
 *
 * Files of a split reach campaign record what they reached there, and the campaign's
 * `*.reach.test.ts` file asserts the union. A directory per run keeps one run's records out of
 * another's. A directory set from outside (`MINNOW_REACH_DIR`) is used as is and left in place.
 * Every project runs this setup; the first one to run creates the directory, and only it removes it.
 */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

export default function setup(): (() => void) | undefined {
  if ((process.env.MINNOW_REACH_DIR ?? "").trim() !== "") return undefined;
  const dir = mkdtempSync(join(tmpdir(), "minnow-reach-"));
  process.env.MINNOW_REACH_DIR = dir;
  return () => {
    rmSync(dir, { recursive: true, force: true });
  };
}
