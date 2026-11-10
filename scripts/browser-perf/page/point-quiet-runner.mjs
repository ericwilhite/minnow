/** Frozen quiet-host entry: nine unprofiled pairs, then one diagnostic profile per arm. */
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { loadavg } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
const dir = dirname(fileURLToPath(import.meta.url));
const manifest = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8"));
if (manifest.parent !== "97192fcd" || !/^[a-f0-9]{40}$/.test(manifest.candidate))
  throw Error("Expected parent and frozen tip required");
for (const [name, hash] of Object.entries(manifest.files))
  if (
    createHash("sha256")
      .update(readFileSync(join(dir, name)))
      .digest("hex") !== hash
  )
    throw Error("Frozen input changed: " + name);
if (process.argv.includes("--check")) {
  console.log(JSON.stringify(manifest));
  process.exit(0);
}
const out = process.env.MINNOW_QUIET_ITEM_OUT;
if (!out) throw Error("MINNOW_QUIET_ITEM_OUT required");
mkdirSync(out, { recursive: true });
const rows = [];
function invoke(variant, profile) {
  const before = loadavg();
  if (before[0] > 1) throw Error("Busy host before point invocation");
  const env = { ...process.env };
  delete env.MINNOW_POINT_PROFILE_DIR;
  if (profile) env.MINNOW_POINT_PROFILE_DIR = join(out, "profile-" + variant);
  const stdout = execFileSync(process.execPath, [join(dir, variant + ".mjs")], {
    encoding: "utf8",
    env,
  });
  const after = loadavg();
  const result = JSON.parse(stdout.trim());
  if (before[0] > 1 || after[0] > 1) throw Error("Busy host: refuse point interpretation");
  if (result.warmPoint.backendReads !== 0) throw Error("Point phase was not cached");
  return { before, after, result };
}
for (let round = 0; round < 9; round++)
  for (const variant of round % 2 ? ["candidate", "baseline"] : ["baseline", "candidate"]) {
    const row = { round, variant, ...invoke(variant, false) };
    rows.push(row);
    writeFileSync(join(out, "pairs.json"), JSON.stringify(rows, null, 2));
    console.log(JSON.stringify(row));
  }
function quantile(xs, q) {
  const s = [...xs].sort((a, b) => a - b),
    i = (s.length - 1) * q;
  return s[Math.floor(i)] + (s[Math.ceil(i)] - s[Math.floor(i)]) * (i % 1);
}
const metrics = {};
for (const key of ["cpuMs", "wallMs"]) {
  const deltas = Array.from(
    { length: 9 },
    (_, i) =>
      rows.find((r) => r.round === i && r.variant === "candidate").result.warmPoint[key] -
      rows.find((r) => r.round === i && r.variant === "baseline").result.warmPoint[key],
  );
  const median = quantile(deltas, 0.5),
    iqr = quantile(deltas, 0.75) - quantile(deltas, 0.25),
    positive = deltas.filter((v) => v > 0).length,
    negative = deltas.filter((v) => v < 0).length;
  metrics[key] = {
    deltas,
    median,
    iqr,
    positive,
    negative,
    decision:
      median > iqr && positive >= 7
        ? "REGRESSION"
        : -median > iqr && negative >= 7
          ? "IMPROVEMENT"
          : "HELD",
  };
}
writeFileSync(join(out, "comparison.json"), JSON.stringify({ manifest, metrics }, null, 2));
for (const variant of ["baseline", "candidate"]) {
  const row = invoke(variant, true);
  writeFileSync(join(out, "profile-" + variant, "diagnostic.json"), JSON.stringify(row, null, 2));
}
console.log(
  JSON.stringify({
    summary: true,
    metrics,
    profiles: "Separate diagnostic invocations; excluded from nine pairs",
  }),
);
