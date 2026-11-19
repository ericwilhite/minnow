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
    expect(result.records).toHaveLength(14);
    expect(result.records.filter((r) => !r.passed).map((r) => `${r.arm}/${r.gate}`)).toEqual([
      "candidate/lint",
    ]);
    expect(JSON.parse(readFileSync(resolve(directory, "results.json"), "utf8"))).toEqual(result);
    expect(
      readFileSync(resolve(directory, "events.jsonl"), "utf8").trim().split("\n"),
    ).toHaveLength(14);
    expect(GATES.find((g) => g.name === "tests")?.args).toEqual([
      "node_modules/vitest/vitest.mjs",
      "run",
      "--maxWorkers=2",
      "--reporter=verbose",
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
        return { status: 0, signal: null };
      },
    );
    expect(result.passed).toBe(false);
    expect(result.records.filter((r) => !r.passed)).toHaveLength(3);
    expect(result.records.at(-1)?.gate).toBe("tests");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
