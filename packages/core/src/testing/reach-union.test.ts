import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DIRECTORY_TRANSITION_REACH } from "./reach.js";
import {
  assertReachUnion,
  REACH_UNIONS,
  unionShortfalls,
  type ReachRecord,
} from "./reach-union.js";

const campaign = "next-directory-transition";
const declaration = REACH_UNIONS[campaign];
const states: readonly string[] = DIRECTORY_TRANSITION_REACH;

/**
 * Records shaped like the split campaign's: each file alone reaches a share of the declared
 * states, so the union holds only with every file.
 */
function records(): ReachRecord[] {
  return declaration.files.map((file, i) => {
    const own = states.filter((_, s) => s % declaration.files.length === i);
    return {
      campaign,
      file,
      histories: 10,
      counts: Object.fromEntries(states.map((state) => [state, own.includes(state) ? 3 : 0])),
    };
  });
}

describe("split reach union", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("holds when every declared file recorded and together they reach every state", () => {
    expect(unionShortfalls(campaign, declaration, states, records())).toEqual([]);
  });

  it("fails when one file's histories are dropped (the mutant)", () => {
    const all = records();
    for (let drop = 0; drop < all.length; drop++) {
      const kept = all.filter((_, i) => i !== drop);
      const problems = unionShortfalls(campaign, declaration, states, kept);
      expect(problems.join("\n")).toContain(
        `no reach record from ${declaration.files[drop] ?? ""}`,
      );
      // And the states only that file reached are named as unreached.
      expect(problems.join("\n")).toMatch(/fewer than 1 reached these states/u);
    }
  });

  it("fails on a file that ran no histories, never passing vacuously", () => {
    const all = records().map((record, i) => (i === 0 ? { ...record, histories: 0 } : record));
    expect(unionShortfalls(campaign, declaration, states, all).join("\n")).toContain(
      "recorded no histories",
    );
    expect(unionShortfalls(campaign, declaration, states, []).length).toBeGreaterThanOrEqual(
      declaration.files.length,
    );
  });

  it("fails on an undeclared file, a duplicate record, or an undeclared state", () => {
    const all = records();
    const first = all[0];
    if (first === undefined) throw new Error("no records");
    const stray = { ...first, file: "packages/core/src/elsewhere.test.ts" };
    expect(unionShortfalls(campaign, declaration, states, [...all, stray]).join("\n")).toContain(
      "undeclared file",
    );
    expect(unionShortfalls(campaign, declaration, states, [...all, first]).join("\n")).toContain(
      "two records",
    );
    const odd = { ...first, counts: { ...first.counts, "never declared": 1 } };
    expect(
      unionShortfalls(campaign, declaration, states, [odd, ...all.slice(1)]).join("\n"),
    ).toContain("undeclared state never declared");
  });

  it("holds each state to the campaign's minimum over the union", () => {
    expect(unionShortfalls(campaign, declaration, states, records(), 3)).toEqual([]);
    expect(unionShortfalls(campaign, declaration, states, records(), 4).join("\n")).toMatch(
      /fewer than 4 reached/u,
    );
  });

  it("reads the run's records, fails with none, and stands aside for a seed replay", () => {
    const dir = mkdtempSync(join(tmpdir(), "minnow-reach-test-"));
    try {
      vi.stubEnv("MINNOW_SEED", "");
      vi.stubEnv("MINNOW_REACH_DIR", dir);
      expect(() => assertReachUnion(campaign, states)).toThrow(/no reach record from/u);
      vi.stubEnv("MINNOW_REACH_DIR", "");
      expect(() => assertReachUnion(campaign, states)).toThrow(/MINNOW_REACH_DIR is not set/u);
      vi.stubEnv("MINNOW_SEED", "28011");
      expect(() => assertReachUnion(campaign, states)).not.toThrow();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
