/**
 * Measures what a browser downloads to run each comparison engine, and writes a scratch report
 * to apps/site/components/bench/library-sizes.json. Copy the rounded result into the benchmark
 * configuration and comparison prose; the generated report is not a page data source.
 *
 * Every engine gets identical treatment: its browser entry is bundled with the same esbuild
 * settings (ESM, browser platform, minified, production), and the binary assets the bundle
 * fetches at runtime — Wasm modules and packed data directories — are added at their shipped
 * size. Sizes are reported raw and gzipped, because gzip is what a plain static host serves.
 *
 * Run with `npm run benchmark:sizes` after changing dependencies or the public entry.
 */
import { writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { ENGINES, measure } from "./library-sizes.mjs";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUTPUT = path.join(repoRoot, "apps/site/components/bench/library-sizes.json");

const engines = [];
for (const spec of ENGINES) engines.push(await measure(spec));

const bundle = {
  schemaVersion: 1,
  kind: "library-sizes",
  measuredAt: new Date().toISOString(),
  method:
    "Each engine's browser entry bundled with esbuild (ESM, minified, production), plus the Wasm and data files it fetches at run time. Gzip is level 9.",
  engines,
};
// Two-space indent so the generated file is already in Prettier style.
writeFileSync(OUTPUT, JSON.stringify(bundle, null, 2) + "\n");

const kb = (bytes: number): string => `${(bytes / 1024).toFixed(1)} KB`;
for (const engine of engines) {
  console.log(
    `${engine.name.padEnd(14)} ${kb(engine.totalGzipBytes).padStart(12)} gzip   ${kb(engine.totalBytes).padStart(12)} raw`,
  );
  for (const asset of engine.assets) {
    console.log(
      `  ${asset.name.padEnd(32)} ${kb(asset.gzipBytes).padStart(12)} ${kb(asset.bytes).padStart(12)}`,
    );
  }
}
console.log(`\nWrote ${path.relative(repoRoot, OUTPUT)}`);
