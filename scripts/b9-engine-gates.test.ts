import { mkdtempSync, readFileSync, rmSync, mkdirSync } from "node:fs";
import { resolve } from "node:path";
import { homedir } from "node:os";
import { expect, it } from "vitest";
import { GATES, executeGates } from "./b9-engine-gates.mts";
const scratch = resolve(homedir(), ".cache", "b9-gate-driver-tests");
mkdirSync(scratch, { recursive: true });

it("runs all exact required gates serially for both arms and preserves failures", () => {
  const directory = mkdtempSync(resolve(scratch, "run-"));
  try {
    const calls: string[] = [],
      arms = [
        { name: "parent", root: "parent" },
        { name: "candidate", root: "candidate" },
      ],
      result = executeGates(arms, directory, (arm, gate) => {
        calls.push(`${arm.name}/${gate.name}`);
        return { status: arm.name === "candidate" && gate.name === "lint" ? 1 : 0, signal: null };
      });
    expect(calls).toEqual(arms.flatMap((a) => GATES.map((g) => `${a.name}/${g.name}`)));
    expect(result.complete).toBe(true);
    expect(result.passed).toBe(false);
    expect(result.records).toHaveLength(20);
    expect(result.records.filter((r) => !r.passed).map((r) => `${r.arm}/${r.gate}`)).toEqual([
      "candidate/lint",
    ]);
    expect(JSON.parse(readFileSync(resolve(directory, "results.json"), "utf8"))).toEqual(result);
    expect(
      readFileSync(resolve(directory, "events.jsonl"), "utf8").trim().split("\n"),
    ).toHaveLength(20);
    const tests = result.records.filter(
      (record) => record.arm === "candidate" && record.gate === "tests",
    );
    expect(tests.map((record) => record.shard)).toEqual(["1/4", "2/4", "3/4", "4/4"]);
    expect(new Set(tests.map((record) => record.log)).size).toBe(4);
    for (const [i, test] of tests.entries())
      expect(test.command).toEqual([
        "node",
        "node_modules/vitest/vitest.mjs",
        "run",
        "--maxWorkers=2",
        "--reporter=verbose",
        "--shard",
        `${i + 1}/4`,
        "packages/core/src/next",
        "scripts",
        "packages/core/src/engine",
      ]);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

it("never labels a signal, a spawn failure, or an exception as success", () => {
  const directory = mkdtempSync(resolve(scratch, "run-"));
  try {
    const result = executeGates(
      [{ name: "candidate", root: "candidate" }],
      directory,
      (_arm, gate) => {
        if (gate.name === "build") return { status: null, signal: "SIGTERM" };
        if (gate.name === "typecheck")
          return { status: 0, signal: null, error: "spawn unavailable" };
        if (gate.name === "lint") throw new Error("driver failed");
        if ("shard" in gate && gate.shard === "2/4") return { status: 1, signal: null };
        return { status: 0, signal: null };
      },
    );
    expect(result.passed).toBe(false);
    expect(result.records.filter((r) => !r.passed)).toHaveLength(4);
    expect(result.records.at(-1)?.gate).toBe("tests");
    expect(result.records.at(-1)?.shard).toBe("4/4");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
