/** Whole-query frames decide acceptance; the percentile component must independently gain. */
import { isDeepStrictEqual } from "node:util";
import { join } from "node:path";
import { pairedCPU } from "./paired-cpu.mts";

export interface QuietObservation {
  query: string;
  boundary: string;
  rows: number;
  digest: string;
  cpuMs?: number;
  userMs?: number;
  systemMs?: number;
  wallMs?: number;
  load: number[];
  identity: { root: string; revision: string; pid: number; module: string; sha256: string };
}
export interface QuietRecord {
  query: string;
  boundary: string;
  samples: Array<{ control: QuietObservation; candidate: QuietObservation; order: string }>;
  paired?: ReturnType<typeof pairedCPU>;
}
export interface QuietPolicy {
  primary: string;
  targetMs: number;
  queries: readonly string[];
  control: { root: string; revision: string; moduleSha: string };
  candidate: { root: string; revision: string; moduleSha: string };
}
export function assessQuiet(records: readonly QuietRecord[], policy: QuietPolicy) {
  if (
    new Set(policy.queries).size !== policy.queries.length ||
    !(policy.targetMs > 0) ||
    !Number.isFinite(policy.targetMs)
  )
    throw new Error("Invalid frozen policy");
  const workers = new Map<string, number>();
  const expected = policy.queries.flatMap((query) =>
    (query === "pk-source" || query === "percentile-component"
      ? ["frames"]
      : ["frames", "rows"]
    ).map((boundary) => `${query}/${boundary}`),
  );
  const actual = records.map((r) => `${r.query}/${r.boundary}`);
  if (!isDeepStrictEqual(actual.sort(), [...expected].sort()))
    throw new Error("Missing, duplicate or unexpected boundary");
  const results = records.map((record) => {
    if (record.samples.length !== 9) throw new Error("Exactly nine retained pairs required");
    const cpu = { control: [] as number[], candidate: [] as number[] };
    for (const [i, pair] of record.samples.entries()) {
      if (pair.order !== (i % 2 === 0 ? "AB" : "BA")) throw new Error("Pair order changed");
      if (pair.control.identity.pid === pair.candidate.identity.pid)
        throw new Error("Arms share a process");
      if (pair.control.rows !== pair.candidate.rows) throw new Error("Output count differs");
      for (const name of ["control", "candidate"] as const) {
        const sample = pair[name],
          arm = policy[name];
        if (
          sample.query !== record.query ||
          sample.boundary !== record.boundary ||
          sample.identity.root !== arm.root ||
          sample.identity.revision !== arm.revision ||
          sample.identity.module !==
            join(arm.root, "packages/core/dist/next/testing/storage-core.js") ||
          sample.identity.sha256 !== arm.moduleSha ||
          !Number.isInteger(sample.identity.pid) ||
          sample.identity.pid <= 0 ||
          !Number.isSafeInteger(sample.rows) ||
          sample.rows < 0
        )
          throw new Error("Observation provenance mismatch");
        if (workers.has(name) && workers.get(name) !== sample.identity.pid)
          throw new Error("Worker restarted");
        workers.set(name, sample.identity.pid);
        const { cpuMs, userMs, systemMs, wallMs } = sample;
        if (
          ![cpuMs, userMs, systemMs, wallMs].every(
            (n) => typeof n === "number" && Number.isFinite(n) && n >= 0,
          ) ||
          typeof cpuMs !== "number" ||
          typeof userMs !== "number" ||
          typeof systemMs !== "number" ||
          Math.abs(cpuMs - userMs - systemMs) > 1e-6 ||
          sample.load.length !== 3 ||
          !sample.load.every((n) => Number.isFinite(n) && n >= 0)
        )
          throw new Error("Invalid CPU/load sample");
        cpu[name].push(cpuMs);
      }
    }
    const paired = pairedCPU(cpu.control, cpu.candidate);
    if (record.paired && !isDeepStrictEqual(record.paired, paired))
      throw new Error("Claimed paired arithmetic differs");
    return {
      query: record.query,
      boundary: record.boundary,
      paired,
      candidateMedianMs: cpu.candidate.sort((a, b) => a - b)[4] ?? NaN,
    };
  });
  const primary = results.find((r) => r.query === policy.primary && r.boundary === "frames");
  if (!primary) throw new Error("Primary frames result missing");
  const regression = results.some(
    (r) =>
      r.boundary === "frames" &&
      r.query !== "pk-source" &&
      r.query !== "percentile-component" &&
      r.paired.significantRegression,
  );
  const percentile = results.find(
    (r) => r.query === "percentile-component" && r.boundary === "frames",
  );
  const gain = primary.paired.acceptedGain && (percentile?.paired.acceptedGain ?? true);
  return {
    decision: regression ? "REGRESSION" : gain ? "ACCEPT" : "HELD",
    primary: policy.primary,
    targetMs: policy.targetMs,
    targetMet: primary.candidateMedianMs <= policy.targetMs,
    results,
  };
}
