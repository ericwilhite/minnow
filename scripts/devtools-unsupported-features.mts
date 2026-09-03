/**
 * Writes `packages/devtools/src/sql/unsupported-features.ts`: the slice of core's SQL feature
 * matrix the devtools use to explain a failure. The whole matrix is a hundred kilobytes of every
 * feature the engine supports; the panel needs only the unsupported ones that carry an error
 * fragment, which is a few kilobytes. A devtools test fails when this file is stale.
 *
 *   npm run devtools:matrix          # rewrite the module
 *   npm run devtools:matrix -- check # exit non-zero when it is stale
 */
import { readFileSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const matrixPath = `${root}packages/core/sql-feature-matrix.json`;
const outputPath = `${root}packages/devtools/src/sql/unsupported-features.ts`;

interface MatrixFeature {
  id: string;
  status: string;
  error?: string;
  notes?: string;
}

export function renderUnsupportedFeatures(features: readonly MatrixFeature[]): string {
  const kept = features
    .filter((feature) => feature.status === "unsupported" && feature.error !== undefined)
    .map((feature) => ({
      id: feature.id,
      error: feature.error ?? "",
      ...(feature.notes === undefined ? {} : { notes: feature.notes }),
    }))
    .sort((left, right) => left.id.localeCompare(right.id));
  const body = JSON.stringify(kept, null, 2);
  return [
    "// Generated from packages/core/sql-feature-matrix.json by scripts/devtools-unsupported-features.mts.",
    "// Do not edit: run `npm run devtools:matrix` after the matrix changes.",
    'import type { UnsupportedFeatureRecord } from "./feature-matrix.js";',
    "",
    "/** Every feature the engine records as unsupported, with the error fragment that names it. */",
    `export const unsupportedFeatures: readonly UnsupportedFeatureRecord[] = ${body};`,
    "",
  ].join("\n");
}

function format(source: string): string {
  return execFileSync("npx", ["prettier", "--stdin-filepath", outputPath], {
    input: source,
    encoding: "utf8",
    cwd: root,
  });
}

const matrix = JSON.parse(readFileSync(matrixPath, "utf8")) as { features: MatrixFeature[] };
const rendered = format(renderUnsupportedFeatures(matrix.features));
const mode = process.argv[2] ?? "write";
if (mode === "check") {
  let current = "";
  try {
    current = readFileSync(outputPath, "utf8");
  } catch {
    // Missing is stale.
  }
  if (current !== rendered) {
    console.error(`${outputPath} is stale; run npm run devtools:matrix`);
    process.exit(1);
  }
  console.log("devtools unsupported-features module is current");
} else {
  writeFileSync(outputPath, rendered);
  console.log(`wrote ${outputPath}`);
}
