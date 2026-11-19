/** NEVER MERGE: serial hosted gate for the exact parent/candidate worktrees.
 * Runs every required check after a failure, preserving command status and logs.
 */
import { spawnSync, execFileSync } from "node:child_process";
import { appendFileSync, closeSync, mkdirSync, openSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

export const GATES = [
  { name: "build", command: "npm", args: ["run", "build"] },
  { name: "typecheck", command: "npm", args: ["run", "typecheck"] },
  { name: "lint", command: "npm", args: ["run", "lint"] },
  { name: "format", command: "npm", args: ["run", "format"] },
  { name: "api", command: "npm", args: ["run", "api:check"] },
  { name: "version", command: "npm", args: ["run", "version:check"] },
  // Four serial commands target 30–45 minute shards; every required path stays.
  ...[1, 2, 3, 4].map((shard) => ({
    name: "tests" as const,
    shard: `${shard}/4`,
    command: "node",
    args: [
      "node_modules/vitest/vitest.mjs",
      "run",
      "--maxWorkers=2",
      "--reporter=verbose",
      "--shard",
      `${shard}/4`,
      "packages/core/src/next",
      "scripts",
      "packages/core/src/engine",
    ],
  })),
] as const;

interface Arm {
  readonly name: string;
  readonly root: string;
}
interface Status {
  readonly status: number | null;
  readonly signal: string | null;
  readonly error?: unknown;
}
export function executeGates(
  arms: readonly Arm[],
  output: string,
  execute: (arm: Arm, gate: (typeof GATES)[number], log: string) => Status,
) {
  mkdirSync(output, { recursive: true });
  const records = [];
  writeFileSync(resolve(output, "events.jsonl"), "");
  for (const arm of arms)
    for (const gate of GATES) {
      const started = Date.now(),
        log = resolve(
          output,
          `${arm.name}-${gate.name}${"shard" in gate ? `-${gate.shard.replace("/", "-of-")}` : ""}.log`,
        );
      console.log(JSON.stringify({ arm: arm.name, gate: gate.name, phase: "start" }));
      let result: Status;
      try {
        result = execute(arm, gate, log);
      } catch (error) {
        result = { status: null, signal: null, error: String(error) };
      }
      const record = {
        arm: arm.name,
        gate: gate.name,
        shard: "shard" in gate ? gate.shard : null,
        command: [gate.command, ...gate.args],
        exit: result.status,
        signal: result.signal,
        error: result.error === undefined ? null : String(result.error),
        passed: result.status === 0 && result.signal === null && result.error === undefined,
        elapsedMs: Date.now() - started,
        log,
      };
      records.push(record);
      appendFileSync(resolve(output, "events.jsonl"), `${JSON.stringify(record)}\n`);
      writeFileSync(
        resolve(output, "results.json"),
        JSON.stringify({ complete: false, records }, null, 2),
      );
      console.log(JSON.stringify(record));
    }
  const result = { complete: true, passed: records.every((r) => r.passed), records };
  writeFileSync(resolve(output, "results.json"), `${JSON.stringify(result, null, 2)}\n`);
  return result;
}

if (
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  const args = process.argv.slice(2),
    option = (name: string) => {
      const i = args.indexOf(name),
        value = i < 0 ? undefined : args[i + 1];
      if (value === undefined) throw new Error(`${name} required`);
      return resolve(value);
    },
    output = option("--output"),
    arms = [
      { name: "parent", root: option("--parent") },
      { name: "candidate", root: option("--candidate") },
    ];
  mkdirSync(output, { recursive: true });
  const provenance = arms.map((arm) => ({
    ...arm,
    revision: execFileSync("git", ["rev-parse", "HEAD"], {
      cwd: arm.root,
      encoding: "utf8",
    }).trim(),
    sourceDiffSHA256: createHash("sha256")
      .update(
        execFileSync("git", ["diff", "HEAD", "--binary"], { cwd: arm.root, maxBuffer: 16 << 20 }),
      )
      .digest("hex"),
  }));
  writeFileSync(resolve(output, "provenance.json"), `${JSON.stringify(provenance, null, 2)}\n`);
  const result = executeGates(arms, output, (arm, gate, log) => {
    const fd = openSync(log, "w");
    try {
      const result = spawnSync(gate.command, [...gate.args], {
        cwd: arm.root,
        stdio: ["ignore", fd, fd],
        env: {
          ...process.env,
          CI: "true",
          NODE_OPTIONS:
            gate.name === "lint" ? "--max-old-space-size=8192" : "--max-old-space-size=4096",
        },
      });
      return { status: result.status, signal: result.signal, error: result.error };
    } finally {
      closeSync(fd);
    }
  });
  process.exitCode = result.passed ? 0 : 1;
}
