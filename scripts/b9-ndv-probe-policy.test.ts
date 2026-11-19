/** Disposable probe scaffolding; its source patch remains isolated on the validated main. */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
it("keeps MST2 probes branch-only, own-writer, serial, fully gated and source-covered", () => {
  const workflow = readFileSync(".github/workflows/b9-perf-probe.yml", "utf8");
  const manifest = JSON.parse(readFileSync("scripts/b9-candidate.json", "utf8")) as {
    validatedBase: string;
    sha256: string;
    patch: string;
    fullGate: boolean;
    prototypeModuleSha256: Record<string, string>;
  };
  const patch = readFileSync(manifest.patch, "utf8");
  for (const text of [
    "branches: [ci/b9-probe]",
    "workflow_dispatch:",
    "group: b9-perf-probe-${{ github.ref }}",
    "cancel-in-progress: true",
    "github.ref == 'refs/heads/ci/b9-probe'",
    manifest.validatedBase,
    "--diagnostics-only",
    "--pairs-only",
    "scripts/b9-engine-gates.mts --parent",
    "ndv_write:",
    "--rows 1000000 --group-rows 8192 --transactions batch --rounds 9",
    "Native-write runtime differs from prototype",
  ])
    expect(workflow).toContain(text);
  const component = readFileSync("scripts/b9-ndv-write-component.mts", "utf8");
  expect(component).toContain("--cpu-prof");
  expect(component).toContain("complete published NDV in the measured workload");
  expect(workflow).toContain("--profile-dir");
  expect(component).toContain("run(arm, true)");
  expect(component).toContain("samples[arm] = run(arm)");
  expect(workflow).not.toContain("b9-demand-admission-component");
  expect(manifest.fullGate).toBe(true);
  expect(createHash("sha256").update(patch).digest("hex")).toBe(manifest.sha256);
  const changed = [...patch.matchAll(/^\+\+\+ [^/]+\/(packages\/core\/src\/next\/.*\.ts)$/gmu)]
    .map((m) => m[1])
    .filter((f): f is string => f !== undefined && !f.endsWith(".test.ts"));
  expect(changed.length).toBeGreaterThan(0);
  for (const source of changed)
    expect(Object.keys(manifest.prototypeModuleSha256), source).toContain(
      source.slice("packages/core/src/".length).replace(/\.ts$/u, ".js"),
    );
  for (const file of [
    "format/ndv.ts",
    "format/sketch-registers.ts",
    "write/statistics-owner.ts",
    "write/statistics-ndv-refusal.test.ts",
    "write/statistics-ndv-publication-cut.test.ts",
    "write/statistics-ndv-root-selection.test.ts",
    "testing/ndv-advisory-differential.test.ts",
  ])
    expect(patch).toContain(`packages/core/src/next/${file}`);
  expect(patch).not.toContain("executor-demand-admission.test.ts");
  expect(patch).not.toContain("diff --git a/.github/");
});
