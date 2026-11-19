/** Run immutable reference cost, and large zero-work controls, outside V8 profiling.
 * Production capture selection stays instrumented in the calling suites. Each
 * subprocess reads the actual sources, executes every counted step, then closes
 * before its private files are removed. No build output or shell is involved. */
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import ts from "typescript";

interface WorkMeasurement {
  steps: number;
  matched: boolean;
  correct: boolean;
}
export function isolatedRegexWork(
  version: "released" | "current",
  mode: "capture" | "boolean" | "group-free",
  prefix: number,
): WorkMeasurement {
  const directory = mkdtempSync(join(tmpdir(), "minnow-regex-work-"));
  try {
    const sources = {
      limits: new URL("../../pattern/limits.ts", import.meta.url),
      input: new URL("../../pattern/input.ts", import.meta.url),
      captures: new URL("../../pattern/captures.ts", import.meta.url),
      regex: new URL("../../pattern/regex.ts", import.meta.url),
      "cache-limits": new URL("../../engine/cache-limits.ts", import.meta.url),
      released: new URL("../../engine/sql-regex-work-parent.test-support.ts", import.meta.url),
    };
    for (const [name, source] of Object.entries(sources)) {
      const emitted = ts.transpileModule(readFileSync(source, "utf8"), {
        compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
      }).outputText;
      const linked = emitted
        .replaceAll("../pattern/limits.js", "./limits.mjs")
        .replaceAll("./limits.js", "./limits.mjs")
        .replaceAll("./input.js", "./input.mjs")
        .replaceAll("./captures.js", "./captures.mjs")
        .replaceAll("./cache-limits.js", "./cache-limits.mjs");
      writeFileSync(join(directory, name + ".mjs"), linked);
    }
    const url = pathToFileURL(
        join(directory, version === "released" ? "released.mjs" : "regex.mjs"),
      ),
      name = version === "released" ? "RegexWorkParent" : "SqlRegex";
    const script = `
      import { ${name} as Program } from ${JSON.stringify(url.href)};
      import { MAX_SQL_PATTERN_MATCH_STEPS } from ${JSON.stringify(pathToFileURL(join(directory, "limits.mjs")).href)};
      const mode = ${JSON.stringify(mode)}, prefix = ${String(prefix)};
      const input = "x".repeat(prefix) + "a" + "b".repeat(2000) + "c".repeat(2000);
      const pattern = mode === "group-free" ? ".{8}a(?:b+)(?:c+)" : ".{8}a(b+)(c+)";
      const program = new Program(pattern, ""), spend = program.spend.bind(program);
      let steps = 0;
      program.spend = (work) => {
        if (++steps > MAX_SQL_PATTERN_MATCH_STEPS + 1) throw new Error("Missing fixed work ceiling");
        spend(work);
      };
      const result = mode === "boolean" ? program.test(input) : program.exec(input);
      const matched = mode === "boolean" ? result === true : result !== null;
      const correct = mode === "boolean" ? result === true : result !== null &&
        result.index === prefix - 8 && result.input === input &&
        result[0] === "x".repeat(8) + "a" + "b".repeat(2000) + "c".repeat(2000) &&
        (mode !== "capture" || (result[1] === "b".repeat(2000) && result[2] === "c".repeat(2000)));
      console.log(JSON.stringify({ steps, matched, correct }));
    `;
    const env = { ...process.env };
    delete env.NODE_V8_COVERAGE;
    return JSON.parse(
      execFileSync(process.execPath, ["--input-type=module", "--eval", script], {
        cwd: directory,
        env,
        encoding: "utf8",
        timeout: 50_000,
        maxBuffer: 128 * 1024,
      }),
    ) as WorkMeasurement;
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}
