import { spawnSync } from "node:child_process";
import {
  chmodSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";

const fixtures: string[] = [];
afterEach(() => {
  for (const path of fixtures.splice(0)) rmSync(path, { recursive: true, force: true });
});
function fixture(version: string) {
  const root = mkdtempSync(join(tmpdir(), "minnow-r6-release-"));
  fixtures.push(root);
  for (const dir of ["scripts/lib", "packages/core", "apps/site/public", "apps/site/content/docs"])
    mkdirSync(join(root, dir), { recursive: true });
  copyFileSync(new URL("./version.mts", import.meta.url), join(root, "scripts/version.mts"));
  const helper = new URL("./lib/r6-acceptance.mts", import.meta.url);
  if (existsSync(helper)) copyFileSync(helper, join(root, "scripts/lib/r6-acceptance.mts"));
  writeFileSync(
    join(root, "package.json"),
    JSON.stringify({
      name: "r6-fixture",
      private: true,
      type: "module",
      version,
      workspaces: ["packages/*"],
    }),
  );
  writeFileSync(
    join(root, "packages/core/package.json"),
    JSON.stringify({ name: "@minnowdb/core", version }),
  );
  writeFileSync(
    join(root, "apps/site/public/versions.json"),
    JSON.stringify({
      current: { version, label: `${version.split(".")[0]}.x`, path: "/" },
      archived: [],
    }),
  );
  writeFileSync(
    join(root, "apps/site/content/docs/changelog.mdx"),
    `## ${version} — test\n\n\`@minnowdb/core@${version}\`\n`,
  );
  return root;
}
function check(root: string) {
  return spawnSync(process.execPath, [join(root, "scripts/version.mts"), "check"], {
    cwd: root,
    encoding: "utf8",
  });
}
it("keeps 0.x releasable without R6 acceptance", () => {
  const result = check(fixture("0.20.1"));
  expect(result.status, result.stderr).toBe(0);
});
it("refuses 1.0 and later versions without a committed R6 acceptance record", () => {
  for (const version of ["1.0.0", "1.0.0-rc.1", "1.2.3", "2.0.0"]) {
    const result = check(fixture(version));
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain("R6 NOT ACCEPTED");
  }
});

function git(root: string, ...args: string[]) {
  const result = spawnSync(
    "git",
    [
      "-c",
      "user.name=R6 fixture",
      "-c",
      "user.email=r6@fixture.invalid",
      "-c",
      "commit.gpgsign=false",
      "-c",
      "core.hooksPath=/dev/null",
      ...args,
    ],
    { cwd: root, encoding: "utf8" },
  );
  expect(result.status, result.stderr).toBe(0);
  return result.stdout.trim();
}
function validRecord() {
  return {
    schemaVersion: 1 as const,
    runner: "qualified-fixture",
    recordedAt: "2026-11-16T00:00:00.000Z",
    commit: "a".repeat(40),
    qualification: { platform: "linux" as const, maxLoad1: 0.5, sampleCount: 20 },
    browsers: (["chromium", "firefox", "webkit"] as const).map((name) => ({
      name,
      runs: 5 as const,
      p50Us: 100,
      loadedP95Us: 1000,
    })),
  };
}
it("requires the exact acceptance bytes in HEAD, not merely an untracked or staged file", () => {
  const root = fixture("1.0.0");
  git(root, "init", "--quiet");
  git(root, "add", ".");
  git(root, "commit", "-qm", "fixture");
  const record = { ...validRecord(), commit: git(root, "rev-parse", "HEAD") };
  const path = join(root, "packages/core/r6-acceptance.json");
  const text = JSON.stringify(record);
  writeFileSync(path, text);
  expect(check(root).stderr).toContain("must be committed");
  git(root, "add", "packages/core/r6-acceptance.json");
  expect(check(root).stderr).toContain("must be committed");
  git(root, "commit", "-qm", "qualified acceptance");
  expect(check(root).status).toBe(0);
  writeFileSync(path, text + "\n");
  expect(check(root).stderr).toContain("uncommitted edits");
  writeFileSync(path, JSON.stringify({ ...record, browsers: record.browsers.slice(1) }));
  git(root, "add", ".");
  git(root, "commit", "-qm", "incomplete evidence");
  expect(check(root).stderr).toContain("all three browsers");
});

import fc from "fast-check";
import {
  acceptanceFromQuietRun,
  parseR6Acceptance,
  requiresR6Acceptance,
} from "./lib/r6-acceptance.mts";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach, R6_RELEASE_REACH } from "../packages/core/src/testing/reach.js";
for (const seed of seedsFor("r6-release", [47144, 47145]))
  it(`refuses invalid acceptance and never hides an over-target history seed ${seed}`, () => {
    const reach = createReach("R6 release", R6_RELEASE_REACH);
    fc.assert(
      fc.property(
        fc.record({
          p50: fc.integer({ min: 0, max: 250 }),
          p95: fc.integer({ min: 0, max: 5000 }),
          excess: fc.integer({ min: 1, max: 1000 }),
          index: fc.integer({ min: 0, max: 4 }),
          major: fc.integer({ min: 0, max: 5 }),
        }),
        ({ p50, p95, excess, index, major }) => {
          reach.beginHistory();
          const record = validRecord();
          const metric = () => ({
            timers: { isolated: true },
            lockCalls: 0,
            uncontended: { count: 3000, p50 },
            loaded: { count: 1000, p95 },
          });
          const histories = record.browsers.map((b) => ({
            browser: b.name,
            lane: "latency",
            exit: 0,
            metrics: Array.from({ length: 5 }, metric),
          }));
          const report = {
            status: "pass",
            qualifiedRunner: record.runner,
            revision: record.commit,
            records: histories,
          };
          const samples = [{ load: [0.25, 2, 3] }, { load: [1, 1, 1] }];
          const receipt = acceptanceFromQuietRun(report, samples, "linux", record.recordedAt);
          expect(parseR6Acceptance(receipt)).toBe(receipt);
          const limits = { chromium: 250, firefox: 400, webkit: 600 } as const;
          const boundary = {
            ...receipt,
            browsers: receipt.browsers.map((b) => ({
              ...b,
              p50Us: limits[b.name],
              loadedP95Us: 5000,
            })),
          };
          expect(parseR6Acceptance(boundary)).toBe(boundary);
          for (const browser of boundary.browsers)
            expect(() =>
              parseR6Acceptance({
                ...boundary,
                browsers: boundary.browsers.map((b) =>
                  b.name === browser.name ? { ...b, p50Us: b.p50Us + excess } : b,
                ),
              }),
            ).toThrow("targets");
          reach.reach("valid record");
          expect(requiresR6Acceptance([`${major}.0.0-rc.1`])).toBe(major >= 1);
          reach.reach(major === 0 ? "0.x unchanged" : "1.x gated");
          expect(() =>
            parseR6Acceptance({
              ...receipt,
              browsers: receipt.browsers.map((b, i) =>
                i === 0 ? { ...b, p50Us: 250 + excess } : b,
              ),
            }),
          ).toThrow("targets");
          for (const value of [5000 + excess, NaN, Infinity, -excess])
            expect(() =>
              parseR6Acceptance({
                ...receipt,
                browsers: receipt.browsers.map((b, i) =>
                  i === 0 ? { ...b, loadedP95Us: value } : b,
                ),
              }),
            ).toThrow("targets");
          reach.reach("over target refused");
          expect(() =>
            acceptanceFromQuietRun(
              report,
              [{ load: [1 + excess / 10000, 0, 0] }],
              "linux",
              record.recordedAt,
            ),
          ).toThrow("host-load");
          expect(() =>
            parseR6Acceptance({
              ...receipt,
              qualification: { ...receipt.qualification, sampleCount: 0 },
            }),
          ).toThrow("host-load");
          reach.reach("unqualified refused");
          expect(() =>
            parseR6Acceptance({ ...receipt, browsers: receipt.browsers.slice(1) }),
          ).toThrow("three browsers");
          expect(() =>
            parseR6Acceptance({
              ...receipt,
              browsers: [receipt.browsers[0], receipt.browsers[0], receipt.browsers[2]],
            }),
          ).toThrow("duplicate");
          reach.reach("incomplete browsers refused");
          const history = histories[0]?.metrics[index];
          if (history === undefined) throw new Error("Missing generated history");
          history.loaded.p95 = 5000 + excess;
          expect(() => acceptanceFromQuietRun(report, samples, "linux", record.recordedAt)).toThrow(
            "targets",
          );
          reach.reach("per-run overshoot refused");
        },
      ),
      {
        seed,
        numRuns: 64,
        examples: [
          [{ p50: 250, p95: 5000, excess: 1, index: 0, major: 1 }],
          [{ p50: 0, p95: 0, excess: 1, index: 4, major: 0 }],
        ],
      },
    );
    for (const patch of [
      { schemaVersion: 2 },
      { runner: "" },
      { recordedAt: "2026-02-30T00:00:00.000Z" },
      { commit: "0".repeat(40) },
    ])
      expect(() => parseR6Acceptance({ ...validRecord(), ...patch })).toThrow("R6 NOT ACCEPTED");
    reach.assertReached();
  });

// The fake registry command keeps this test safe even if the publication guard is removed.
it("refuses a 1.x publisher before registry access, even for a dry run", () => {
  const root = fixture("1.0.0");
  copyFileSync(
    new URL("./publish-packages.mts", import.meta.url),
    join(root, "scripts/publish-packages.mts"),
  );
  const bin = join(root, "bin");
  mkdirSync(bin);
  const called = join(root, "registry-called");
  const npm = join(bin, "npm");
  writeFileSync(
    npm,
    `#!${process.execPath}\nimport {writeFileSync} from 'node:fs'; writeFileSync(${JSON.stringify(called)}, 'called'); process.exit(79);\n`,
  );
  chmodSync(npm, 0o755);
  const result = spawnSync(
    process.execPath,
    [join(root, "scripts/publish-packages.mts"), "--dry-run"],
    {
      cwd: root,
      encoding: "utf8",
      env: { ...process.env, PATH: bin },
    },
  );
  expect(result.status).not.toBe(0);
  expect(result.stderr).toContain("R6 NOT ACCEPTED");
  expect(existsSync(called)).toBe(false);
});

it("cannot generate an acceptance record from a failed or incomplete quiet run", () => {
  expect(() =>
    acceptanceFromQuietRun({ status: "targets-missed" }, [{ load: [0, 0, 0] }], "linux"),
  ).toThrow("did not pass");
  for (const records of [[], [{ lane: "latency", browser: "chromium", exit: 0, metrics: [] }]])
    expect(() =>
      acceptanceFromQuietRun({ status: "pass", records }, [{ load: [0, 0, 0] }], "linux"),
    ).toThrow("incomplete browser");
});
