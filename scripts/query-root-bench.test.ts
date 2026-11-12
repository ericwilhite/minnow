/** End-to-end lifecycle law, independent of host timing and usable against an older entrypoint. */
import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { cp, mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import { auditRootWorkers } from "./query-root-bench.mts";
import { seedsFor, mulberry32 } from "../packages/core/src/testing/seeds.js";
import { createReach, QUERY_ROOT_VM_REACH } from "../packages/core/src/testing/reach.js";
const run = promisify(execFile);
const hash = (b: Uint8Array | string) => createHash("sha256").update(b).digest("hex");
for (const seed of seedsFor("query-root-independent-entry", [0xb2f1, 0xb2f2]))
  it(`starts fresh real executor workers (${String(seed)})`, async () => {
    const reach = createReach("query-root real VM", QUERY_ROOT_VM_REACH);
    reach.beginHistory();
    const dir = await mkdtemp(join(tmpdir(), "minnow-root-entry-")),
      runtime = join(dir, "runtime"),
      output = join(dir, "report");
    try {
      // Test-only compiled package snapshot: no performance claim uses this small manifest.
      await mkdir(join(runtime, "packages"), { recursive: true });
      const core = resolve("packages/core");
      await cp(core, join(runtime, "packages/core"), {
        recursive: true,
        filter: (path) => !path.includes("node_modules"),
      });
      await mkdir(join(runtime, "node_modules/@minnowdb"), { recursive: true });
      for (const entry of await readdir(resolve("node_modules"))) {
        if (entry === "@minnowdb") continue;
        await symlink(resolve("node_modules", entry), join(runtime, "node_modules", entry));
      }
      await symlink("../../packages/core", join(runtime, "node_modules/@minnowdb/core"));
      await writeFile(join(runtime, "source.tar"), "test-only package snapshot");
      const names = [
        "source.tar",
        "packages/core/package.json",
        "packages/core/dist/next/exec/create.js",
        "packages/core/dist/next/exec/plan.js",
        "packages/core/dist/next/exec/key-order.js",
        "packages/core/dist/next/testing/storage-core.js",
      ];
      const files = Object.fromEntries(
        await Promise.all(
          names.map(async (name) => [name, hash(await readFile(join(runtime, name)))]),
        ),
      );
      const revision = "0".repeat(40);
      await writeFile(
        join(runtime, "manifest.json"),
        JSON.stringify({ revision, tree: revision, node: process.version, files }),
      );
      const entry =
        process.env.MINNOW_ROOT_BENCH_ENTRY ??
        fileURLToPath(new URL("./query-root-bench.mts", import.meta.url));
      const rows = 7 + Math.floor(mulberry32(seed)() * 8);
      const result = await run(
        process.execPath,
        [
          entry,
          "--baseline",
          runtime,
          "--candidate",
          runtime,
          "--aa",
          "--smoke",
          "--rows",
          String(rows),
          "--warmups",
          "1",
          "--repeats",
          "1",
          "--only",
          "scan_plain3",
          "--output",
          output,
        ],
        { maxBuffer: 4 << 20, env: { ...process.env, TMPDIR: dir } },
      );
      expect(result.stderr).not.toContain("leaked native resources");
      const report = JSON.parse(await readFile(join(output, "results.json"), "utf8")) as
        | {
            smoke?: boolean;
            result?: {
              records: Array<{
                samples: Array<{
                  arm: string;
                  cohort: number;
                  rows: number;
                  identity: { pid: number };
                }>;
              }>;
            };
          }
        | Array<{
            a: Array<{ identity: { pid: number } }>;
            b: Array<{ identity: { pid: number } }>;
          }>;
      const first = Array.isArray(report) ? report[0] : report.result?.records[0];
      if (first === undefined) throw new Error("Missing lifecycle record");
      const pids =
        "samples" in first
          ? first.samples.map((sample) => sample.identity.pid)
          : [...first.a, ...first.b].map((sample) => sample.identity.pid);
      expect(pids).toHaveLength(18);
      expect(new Set(pids).size).toBe(18);
      reach.reach("eighteen distinct real workers");
      if (Array.isArray(report)) throw new Error("Persistent worker report");
      expect(report.smoke).toBe(true);
      const all = report.result?.records[0]?.samples;
      if (all === undefined) throw new Error("Missing independent samples");
      for (let pair = 0; pair < 9; pair++) {
        const samples = all.filter((sample) => sample.cohort === pair);
        expect(samples.map((s) => s.arm)).toEqual(
          pair % 2 ? ["candidate", "control"] : ["control", "candidate"],
        );
        expect(samples.every((s) => s.rows === rows)).toBe(true);
        reach.reach(pair % 2 ? "BA build startup" : "AB build startup");
      }
      const valid = report.result as Parameters<typeof auditRootWorkers>[0];
      auditRootWorkers(valid, 1);
      const reused = structuredClone(valid);
      const a = reused.records[0]?.samples[0],
        b = reused.records[0]?.samples[1];
      if (a === undefined || b === undefined) throw new Error("Missing worker identities");
      b.identity.pid = a.identity.pid;
      expect(() => auditRootWorkers(reused, 1)).toThrow("reused");
      reach.reach("reused worker refusal");
      const forged = structuredClone(valid);
      const verdict = forged.records[0]?.acceptance;
      if (verdict === undefined) throw new Error("Missing paired arithmetic");
      verdict.medianDeltaMs += 1;
      expect(() => auditRootWorkers(forged, 1)).toThrow("arithmetic mismatch");
      reach.reach("forged arithmetic refusal");
      reach.reach("independent fixture restore");
      reach.reach("exact row volume");
      // All child exit fences are awaited by the runner before its successful exit above.
      reach.reach("all workers drained");
      reach.assertReached();
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 180000);
