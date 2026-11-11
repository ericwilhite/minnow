import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach } from "../packages/core/src/testing/reach.js";
import {
  assessQuiet,
  type QuietPolicy,
  type QuietRecord,
  type QuietObservation,
} from "./lib/next-quiet-policy.mts";

const policy: QuietPolicy = {
  primary: "sort_deep_offset_pk",
  targetMs: 20,
  queries: ["sort_deep_offset_pk", "pk-source", "scan_first_page"],
  control: { root: "/control", revision: "a".repeat(40), moduleSha: "1".repeat(64) },
  candidate: { root: "/candidate", revision: "b".repeat(40), moduleSha: "2".repeat(64) },
};
function record(query: string, boundary: string, delta: number): QuietRecord {
  const sample = (name: "control" | "candidate", cpu: number): QuietObservation => ({
    query,
    boundary,
    rows: 100,
    digest: "",
    cpuMs: cpu,
    userMs: cpu,
    systemMs: 0,
    wallMs: cpu,
    load: [0.2, 0.3, 0.4],
    identity: {
      root: policy[name].root,
      revision: policy[name].revision,
      pid: name === "control" ? 101 : 102,
      module: `${policy[name].root}/packages/core/dist/next/testing/storage-core.js`,
      sha256: policy[name].moduleSha,
    },
  });
  return {
    query,
    boundary,
    samples: Array.from({ length: 9 }, (_, i) => ({
      order: i % 2 === 0 ? "AB" : "BA",
      control: sample("control", 200 + i),
      candidate: sample("candidate", 200 + i + delta),
    })),
  };
}
const records = (delta: number) =>
  policy.queries.flatMap((query) =>
    (query === "pk-source" ? ["frames"] : ["frames", "rows"]).map((boundary) =>
      record(query, boundary, query === policy.primary && boundary === "frames" ? delta : 0),
    ),
  );

it.each(seedsFor("next-executor-quiet-policy", [0xe145, 0xe146]))(
  "keeps whole frames primary and rejects malformed pairs (seed %i)",
  async (seed) => {
    const reach = createReach("executor quiet policy", [
      "gain",
      "regression",
      "held",
      "component cannot promote",
      "rows cannot promote",
      "guard regression",
      "order",
      "missing",
      "revision",
      "root",
      "CPU",
      "restart",
      "percentile frames primary",
      "percentile component diagnostic",
      "percentile component required",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 100 }), fc.boolean(), async (gain, slower) => {
        reach.beginHistory();
        const result = assessQuiet(records(slower ? gain : -gain), policy);
        expect(result.decision).toBe(slower ? "REGRESSION" : "ACCEPT");
        reach.reach(slower ? "regression" : "gain");
        const held = records(0);
        expect(assessQuiet(held, policy).decision).toBe("HELD");
        reach.reach("held");
        held[1] = record(policy.primary, "rows", -gain);
        expect(assessQuiet(held, policy).decision).toBe("HELD");
        reach.reach("rows cannot promote");
        held[2] = record("pk-source", "frames", -gain);
        expect(assessQuiet(held, policy).decision).toBe("HELD");
        reach.reach("component cannot promote");
        held[3] = record("scan_first_page", "frames", gain);
        expect(assessQuiet(held, policy).decision).toBe("REGRESSION");
        reach.reach("guard regression");
        const medianPolicy = {
          ...policy,
          primary: "win_median_pct",
          targetMs: 75,
          queries: ["win_median_pct", "percentile-component", "scan_first_page"],
        };
        const medianRecords = [
          record("win_median_pct", "frames", 0),
          record("win_median_pct", "rows", -gain),
          record("percentile-component", "frames", -gain),
          record("scan_first_page", "frames", 0),
          record("scan_first_page", "rows", 0),
        ];
        expect(assessQuiet(medianRecords, medianPolicy).decision).toBe("HELD");
        reach.reach("percentile component diagnostic");
        medianRecords[0] = record("win_median_pct", "frames", -gain);
        expect(assessQuiet(medianRecords, medianPolicy).decision).toBe("ACCEPT");
        reach.reach("percentile frames primary");
        for (const delta of [0, gain]) {
          medianRecords[2] = record("percentile-component", "frames", delta);
          expect(assessQuiet(medianRecords, medianPolicy).decision).toBe("HELD");
        }
        reach.reach("percentile component required");
        for (const fault of ["order", "missing", "revision", "root", "CPU", "restart"] as const) {
          const bad = records(-gain),
            first = bad[0];
          if (!first) throw new Error("Fixture record");
          const pair = first.samples[1];
          if (!pair) throw new Error("Fixture pair");
          if (fault === "order") pair.order = "AB";
          if (fault === "missing") first.samples.pop();
          if (fault === "revision") pair.candidate.identity.revision = "c".repeat(40);
          if (fault === "root") pair.control.identity.root = "/foreign";
          if (fault === "CPU") pair.candidate.cpuMs = NaN;
          if (fault === "restart") pair.candidate.identity.pid++;
          expect(() => assessQuiet(bad, policy)).toThrow();
          reach.reach(fault);
        }
      }),
      {
        seed,
        numRuns: 16,
        examples: [
          [1, false],
          [1, true],
        ],
      },
    );
    reach.assertReached();
  },
);
