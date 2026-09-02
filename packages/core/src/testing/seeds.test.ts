import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import regressions from "../../regression-seeds.json" with { type: "json" };
import { seedsFor } from "./seeds.js";

const regressionSeeds = regressions as { suites: Record<string, readonly number[]> };

const coreSource = fileURLToPath(new URL("..", import.meta.url));
const repoRoot = fileURLToPath(new URL("../../../..", import.meta.url));

/** Every test file under packages/core/src, with its text. */
function testFiles(): Array<{ path: string; text: string }> {
  return (
    readdirSync(coreSource, { recursive: true, encoding: "utf8" })
      // This file quotes registry keys in its own assertions; it iterates no suite.
      .filter((name) => name.endsWith(".test.ts") && !name.endsWith("seeds.test.ts"))
      .map((name) => ({
        path: join(coreSource, name),
        text: readFileSync(join(coreSource, name), "utf8"),
      }))
  );
}

describe("regression seeds", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("runs a suite's defaults first and then every seed recorded for it", () => {
    const recorded = regressionSeeds.suites["auto-compaction-soak"] ?? [];
    // The registry is the fixture here; an empty entry would make the check vacuous.
    expect(recorded.length).toBeGreaterThan(0);
    expect(seedsFor("auto-compaction-soak", [0x7a5c])).toEqual([0x7a5c, ...recorded]);
    expect(seedsFor("never-recorded", [1, 2, 1])).toEqual([1, 2]);
  });

  it("replays exactly the MINNOW_SEED override and refuses a non-numeric one", () => {
    vi.stubEnv("MINNOW_SEED", "123456");
    expect(seedsFor("auto-compaction-soak", [0x7a5c])).toEqual([123456]);
    vi.stubEnv("MINNOW_SEED", "");
    expect(seedsFor("never-recorded", [7])).toEqual([7]);
    vi.stubEnv("MINNOW_SEED", "soon");
    expect(() => seedsFor("never-recorded", [7])).toThrow("MINNOW_SEED must be a number");
  });

  it("executes every recorded seed: each registry key is iterated by exactly one suite", () => {
    // A suite that took only the first seed of the list would list its regressions without ever
    // running them, so every key must be consumed by a call whose whole result is iterated.
    const files = testFiles();
    for (const suite of Object.keys(regressionSeeds.suites)) {
      const call = `seedsFor("${suite}"`;
      const consumers = files.filter(({ text }) => text.includes(call));
      expect(
        consumers.map(({ path }) => path),
        `suite ${suite}`,
      ).toHaveLength(1);
      const text = consumers[0]?.text ?? "";
      const line = text.slice(text.indexOf(call)).split("\n")[0] ?? "";
      expect(line, `suite ${suite} takes a single seed`).not.toMatch(/\)\[0\]|\.at\(0\)/);
    }
  });

  it("names every suite the soak runner explores", () => {
    const soak = readFileSync(join(repoRoot, "scripts/soak.mts"), "utf8");
    const suites = [...soak.matchAll(/name: "([^"]+)",\s*file: "([^"]+)"/g)].map((match) => ({
      name: match[1] ?? "",
      file: match[2] ?? "",
    }));
    expect(suites.length).toBeGreaterThan(0);
    const files = testFiles();
    for (const { name, file } of suites) {
      expect(regressionSeeds.suites, `soak suite ${name}`).toHaveProperty(name);
      expect(existsSync(join(repoRoot, file)), `soak file ${file}`).toBe(true);
    }
    // Every seeded suite is explored: a key the soak never varies could only ever hold seeds
    // somebody typed in by hand.
    const soakFiles = new Set(suites.map(({ file }) => join(repoRoot, file)));
    for (const suite of Object.keys(regressionSeeds.suites)) {
      const consumer = files.find(({ text }) => text.includes(`seedsFor("${suite}"`));
      expect(consumer && soakFiles.has(consumer.path), `soak explores ${suite}`).toBe(true);
    }
  });
});
