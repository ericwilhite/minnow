/** Regression checks from the #12 reviews: completion guards, baseline labels and queues. */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { report, type HeadToHeadResult } from "./head-to-head.mts";

const repository = join(import.meta.dirname, "..");

function result(options: {
  ref: string;
  rounds: number;
  completed: number;
  candidate?: boolean;
  faster?: boolean;
  error?: string;
}): HeadToHeadResult {
  const samples = [];
  for (let round = 0; round < options.completed; round++) {
    samples.push({ arm: "next" as const, boundary: "frames", round, cpuMs: 100, wallMs: 100 });
    samples.push({
      arm: "shipped" as const,
      boundary: options.candidate === true ? "frames" : "rows",
      round,
      cpuMs: options.faster === true ? 80 : 100 + round,
      wallMs: 100,
    });
    if (options.candidate !== true)
      samples.push({ arm: "next" as const, boundary: "rows", round, cpuMs: 120, wallMs: 120 });
  }
  return {
    dataset: "W1M",
    started: "",
    finished: "",
    revision: "0123456789abcdef",
    baseline: { ref: options.ref, revision: "0123456789abcdef" },
    shipped: { version: "0.20.0", root: "", integrity: null },
    ...(options.candidate === true
      ? { candidate: { root: "", revision: "fedcba9876543210" } }
      : {}),
    rows: 1000,
    scale: 1,
    rounds: options.rounds,
    capBytes: 1,
    node: "v24",
    cpu: "test CPU",
    memoryBytes: 2 ** 30,
    loadMs: { next: 1, shipped: 1 },
    cells: [
      {
        id: "scan_proj3_agg",
        verification: {},
        answers: { same: true, method: "inline" },
        samples,
        errors: options.error === undefined ? {} : { next: options.error },
      },
    ],
  };
}

describe("head-to-head report", () => {
  it("names the measured ref everywhere, never main for a dispatched branch", async () => {
    const text = await report([
      result({ ref: "perf/some-branch", rounds: 5, completed: 5, error: "boom" }),
    ]);
    expect(text).toContain("## Head-to-head: perf/some-branch vs published 0.20.0");
    expect(text).toContain("perf/some-branch: boom");
    expect(text).toContain("perf/some-branch/0.20");
    expect(text).toMatch(/Internal diagnostic, not a published benchmark/u);
    expect(text).not.toMatch(/\bmain\b/u);
  });

  it("gives a candidate lead only with every requested pair and at least nine", async () => {
    const partial = await report([
      result({ ref: "main", rounds: 9, completed: 8, candidate: true, faster: true }),
    ]);
    expect(partial).toContain("insufficient pairs");
    expect(partial).toContain("8/9");
    expect(partial).not.toContain("lead: faster");
    const few = await report([
      result({ ref: "main", rounds: 5, completed: 5, candidate: true, faster: true }),
    ]);
    expect(few).toContain("insufficient pairs");
    const full = await report([
      result({ ref: "main", rounds: 9, completed: 9, candidate: true, faster: true }),
    ]);
    expect(full).toContain("lead: faster");
    expect(full).toContain("## Candidate screen: fedcba9876 vs main");
  });
});

describe("workflow queues", () => {
  const group = (file: string) => {
    const text = readFileSync(join(repository, ".github/workflows", file), "utf8");
    const match = /concurrency:\s*\n(?:\s*#.*\n)*\s*group:\s*(.+)\n/u.exec(text);
    if (match?.[1] === undefined) throw new Error(`${file} has no concurrency group`);
    return match[1];
  };

  it("keeps schedule, dispatch, refs and candidates in separate head-to-head queues", () => {
    const key = group("head-to-head.yml");
    expect(key).toContain("github.event_name");
    expect(key).toContain("github.ref");
    expect(key).toContain("inputs.candidate");
  });

  it("keys a claim queue by workflow ref, control and candidate", () => {
    const key = group("next-claim.yml");
    expect(key).toContain("github.ref");
    expect(key).toContain("inputs.control");
    expect(key).toContain("inputs.candidate");
  });
});
