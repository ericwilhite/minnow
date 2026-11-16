import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import fc from "fast-check";
import { expect, it } from "vitest";
import {
  browserWorkload,
  calibrationProvenance,
  workloadHash,
  requireMeasuredCalibration,
} from "./lib/browser-perf-workload.mts";
import { calibrate, evaluate, parseBrowserPerfBaseline } from "./lib/browser-perf-baseline.mts";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach, BROWSER_WORKLOAD_REACH } from "../packages/core/src/testing/reach.js";

for (const seed of seedsFor("browser-perf-workload", [47140, 47141]))
  it(`refuses changed SQL, dataset and scale before ratio comparison seed ${seed}`, () => {
    const reach = createReach("browser workload", BROWSER_WORKLOAD_REACH);
    fc.assert(
      fc.property(
        fc.string({ maxLength: 80 }),
        fc.integer({ min: 1, max: 10000 }),
        (sql, scale) => {
          reach.beginHistory();
          const hash = (text: string, dataset: string, size: number) =>
            workloadHash(text, { id: dataset, scale: size });
          expect(hash(sql, "S1", scale)).toBe(
            createHash("sha256")
              .update(JSON.stringify({ sql, dataset: { id: "S1", scale } }))
              .digest("hex"),
          );
          const observation = {
            cell: "S1/q",
            reference: "duckdb" as const,
            workload: hash(sql, "S1", scale),
            minnow: { ok: true, timing: { median: 10, best: 9, worst: 11, p95: 11, count: 5 } },
            referenceOutcome: {
              ok: true,
              timing: { median: 1, best: 1, worst: 1, p95: 1, count: 5 },
            },
          };
          const baseline = {
            ...calibrate(observation),
            workload: observation.workload,
            threshold: 1,
          };
          expect(evaluate(observation, baseline).some((v) => v.kind === "ratio-regression")).toBe(
            true,
          );
          for (const [state, workload] of [
            ["SQL changed", hash(sql + " ", "S1", scale)],
            ["dataset changed", hash(sql, "S2", scale)],
            ["scale changed", hash(sql, "S1", scale + 1)],
          ] as const) {
            const verdicts = evaluate({ ...observation, workload }, baseline);
            expect(verdicts).toContainEqual(
              expect.objectContaining({
                kind: "changed-workload",
                fatal: true,
                confirmable: false,
              }),
            );
            expect(verdicts.some((v) => v.kind === "ratio-regression")).toBe(false);
            expect(verdicts.map((v) => v.message).join()).toContain(
              "changed workload — recalibrate",
            );
            reach.reach(state);
          }
        },
      ),
      { seed, numRuns: 64 },
    );
    reach.assertReached();
  });

it("identifies all checked-in cells and keeps S_q7's old SQL identity until hosted calibration", () => {
  const baseline = parseBrowserPerfBaseline(
    JSON.parse(
      readFileSync(new URL("../packages/core/browser-perf-baseline.json", import.meta.url), "utf8"),
    ),
  );
  for (const [name, profile] of Object.entries(baseline.profiles)) {
    for (const [cell, entry] of Object.entries(profile.cells)) {
      if (cell.endsWith("/S_q7") && entry.calibration === undefined) {
        // Original Nov5 FLOAT query, before authority4e7b6a0c changed its SQL.
        expect(entry.workload).toBe(
          "5dec4ecac3cf661e391d321e0799cd1b7ac4ea9b65ec377f86d0f0b0f6a1ff48",
        );
        expect(entry.workload).not.toBe(browserWorkload(cell));
        expect(entry.threshold).toBe(name.endsWith("-ci") ? 9.56 : 7.97);
      } else expect(entry.workload).toBe(browserWorkload(cell));
    }
  }
});

it("requires hosted provenance for S_q7 and refuses missing baseline identities", () => {
  expect(() => calibrationProvenance("S1/S_q7", {})).toThrow("hosted recalibrate");
  expect(() =>
    calibrationProvenance("S1/S_q7", {
      GITHUB_ACTIONS: "true",
      GITHUB_RUN_ID: "oops",
      GITHUB_REPOSITORY: "org/repo",
    }),
  ).toThrow("hosted recalibrate");
  expect(
    calibrationProvenance("S1/S_q7", {
      GITHUB_ACTIONS: "true",
      GITHUB_RUN_ID: "123",
      GITHUB_REPOSITORY: "org/repo",
    }),
  ).toEqual({
    authority: "4e7b6a0c: PostgreSQL-valid NUMERIC ROUND workload correction",
    run: "https://github.com/org/repo/actions/runs/123",
  });
  expect(calibrationProvenance("S1/S_q1", {})).toBeUndefined();
  for (const workload of [undefined, "", "sha256", "a".repeat(63)]) {
    expect(() =>
      parseBrowserPerfBaseline({
        schemaVersion: 2,
        profiles: {
          test: { cells: { q: { reference: "duckdb", status: "completes", workload } } },
        },
      }),
    ).toThrow("changed workload — recalibrate");
  }
});

it("will not recalibrate S_q7 to normalize failed or unverified measurements", () => {
  const timing = { median: 10, best: 9, worst: 11, p95: 11, count: 5 };
  const valid = {
    cell: "S1/S_q7",
    workload: browserWorkload("S1/S_q7"),
    reference: "duckdb" as const,
    minnow: { ok: true, timing, check: "match" as const },
    referenceOutcome: { ok: true, timing },
  };
  expect(() => requireMeasuredCalibration(valid)).not.toThrow();
  for (const broken of [
    { ...valid, minnow: { ok: false } },
    { ...valid, referenceOutcome: { ok: false } },
    { ...valid, minnow: { ok: true, timing } },
    { ...valid, minnow: { ok: true, check: "match" as const } },
    { ...valid, minnow: { ...valid.minnow, check: "mismatch" as const } },
  ])
    expect(() => requireMeasuredCalibration(broken)).toThrow("result-verified");
});
