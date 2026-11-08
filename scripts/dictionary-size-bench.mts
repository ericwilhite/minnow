/** Five fresh-process interleaved rounds, actual varlenBytesOf, includes code-loop CPU only. */
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { loadavg, tmpdir } from "node:os";
import { join } from "node:path";
import { build } from "esbuild";
import { executorSnapshot } from "./browser-perf/executor-comparison.js";
const reference = process.argv.find((a) => a.startsWith("--compare="))?.slice(10);
if (!reference) throw new Error("Use --compare=REV");
const scratch = mkdtempSync(join(tmpdir(), "minnow-dictionary-size-"));
try {
  const worker =
    'import{dictionarySizing}from"./scripts/browser-perf/dictionary-size.ts"; console.log(JSON.stringify({cases:[dictionarySizing(false),dictionarySizing(true)]}));';
  for (const name of ["baseline", "candidate"])
    await build({
      stdin: { contents: worker, resolveDir: process.cwd(), loader: "ts" },
      bundle: true,
      platform: "node",
      format: "esm",
      outfile: join(scratch, name + ".mjs"),
      ...(name === "baseline" ? { plugins: [executorSnapshot(reference).esbuild] } : {}),
    });
  const samples: Array<{
    round: number;
    variant: string;
    cases: Array<{ sparse: boolean; cpuMs: number; threadCpuMs: number; wallMs: number }>;
  }> = [];
  for (let round = 0; round < 5; round++)
    for (const variant of round % 2 ? ["candidate", "baseline"] : ["baseline", "candidate"]) {
      const row = {
        round,
        variant,
        load: loadavg(),
        ...(JSON.parse(
          execFileSync(process.execPath, [join(scratch, variant + ".mjs")], {
            encoding: "utf8",
            timeout: 120000,
          }),
        ) as { cases: (typeof samples)[number]["cases"] }),
      };
      samples.push(row);
      console.log(JSON.stringify(row));
    }
  for (const sparse of [false, true])
    for (const variant of ["baseline", "candidate"]) {
      const rows = samples
        .filter((r) => r.variant === variant)
        .flatMap((r) => r.cases.filter((c) => c.sparse === sparse));
      console.log(
        JSON.stringify({
          variant,
          sparse,
          ...Object.fromEntries(
            (["cpuMs", "threadCpuMs", "wallMs"] as const).map((key) => {
              const v = rows.map((r) => r[key]).sort((a, b) => a - b);
              return [key, { median: v[2], min: v[0], max: v[4] }];
            }),
          ),
        }),
      );
    }
  if (process.argv.includes("--assert-gain")) {
    for (const sparse of [false, true]) {
      const values = (variant: string) =>
        samples
          .filter((s) => s.variant === variant)
          .flatMap((r) => r.cases.filter((c) => c.sparse === sparse).map((c) => c.cpuMs))
          .sort((a, b) => a - b);
      const b = values("baseline"),
        c = values("candidate");
      if (
        b.length !== 5 ||
        c.length !== 5 ||
        (b[2] ?? 0) - (c[2] ?? Infinity) <=
          Math.max((b[4] ?? 0) - (b[0] ?? 0), (c[4] ?? 0) - (c[0] ?? 0))
      )
        throw new Error("Component CPU gain does not beat the spread");
    }
  }
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
