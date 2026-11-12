/** End-to-end lifecycle law, independent of host timing and usable against an older entrypoint. */
import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { cp, mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import { auditRootWorkers, biasAdjustedGain } from "./query-root-bench.mts";
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
      const componentOutput = join(dir, "complete-decode");
      await run(
        process.execPath,
        [
          join(dirname(entry), "query-root-component.mts"),
          "--baseline",
          runtime,
          "--candidate",
          runtime,
          "--aa",
          "--smoke",
          "--decode-only",
          "--warmups",
          "1",
          "--repeats",
          "1",
          "--output",
          componentOutput,
        ],
        { maxBuffer: 4 << 20, env: { ...process.env, TMPDIR: dir } },
      );
      const component = JSON.parse(
        await readFile(join(componentOutput, "results.json"), "utf8"),
      ) as {
        result: {
          records: Array<{
            samples: Array<{
              rows: number;
              identity: { pid: number; identity: { cached: boolean; decodeOnly?: boolean } };
            }>;
          }>;
        };
      };
      const complete = component.result.records[0]?.samples;
      if (complete === undefined) throw new Error("Missing complete-root samples");
      expect(complete).toHaveLength(18);
      for (const sample of complete) {
        expect(sample.identity.identity.cached).toBe(false);
        expect(sample.identity.identity.decodeOnly).toBe(true);
        expect(sample.rows).toBe(978);
      }
      expect(new Set([...pids, ...complete.map((sample) => sample.identity.pid)]).size).toBe(36);
      reach.reach("complete decode in both arms");
      const sharedOutput = join(dir, "validated-owner");
      await run(
        process.execPath,
        [
          join(dirname(entry), "query-root-component.mts"),
          "--baseline",
          runtime,
          "--candidate",
          runtime,
          "--aa",
          "--smoke",
          "--validated-roots",
          "--warmups",
          "1",
          "--repeats",
          "1",
          "--output",
          sharedOutput,
        ],
        { maxBuffer: 4 << 20, env: { ...process.env, TMPDIR: dir } },
      );
      const shared = JSON.parse(await readFile(join(sharedOutput, "results.json"), "utf8")) as {
        result: typeof component.result;
      };
      const owners = shared.result.records[0]?.samples;
      if (owners === undefined) throw new Error("Missing owner-cache samples");
      expect(owners).toHaveLength(18);
      for (const sample of owners) {
        expect(sample.identity.identity.cached).toBe(true);
        expect((sample.identity.identity as { validatedRoots?: boolean }).validatedRoots).toBe(
          true,
        );
        expect(sample.rows).toBe(978);
      }
      expect(
        new Set([
          ...pids,
          ...complete.map((s) => s.identity.pid),
          ...owners.map((s) => s.identity.pid),
        ]).size,
      ).toBe(54);
      reach.reach("owner cache in both arms");
      const chunksOutput = join(dir, "chunk-facts");
      await run(
        process.execPath,
        [
          join(dirname(entry), "query-chunk-component.mts"),
          "--baseline",
          runtime,
          "--candidate",
          runtime,
          "--aa",
          "--smoke",
          "--warmups",
          "1",
          "--repeats",
          "1",
          "--output",
          chunksOutput,
        ],
        { maxBuffer: 4 << 20, env: { ...process.env, TMPDIR: dir } },
      );
      const chunks = JSON.parse(await readFile(join(chunksOutput, "results.json"), "utf8")) as {
        result: {
          records: Array<{
            samples: Array<{
              rows: number;
              identity: { pid: number; identity: { reuse: boolean; groups: number } };
            }>;
          }>;
        };
      };
      const prepared = chunks.result.records[0]?.samples;
      if (!prepared) throw new Error("Missing chunk fact samples");
      expect(prepared).toHaveLength(18);
      for (const sample of prepared) {
        expect(sample.identity.identity.reuse).toBe(true);
        expect(sample.identity.identity.groups).toBe(3);
        expect(sample.rows).toBe(18);
      }
      expect(
        new Set([
          ...pids,
          ...complete.map((s) => s.identity.pid),
          ...owners.map((s) => s.identity.pid),
          ...prepared.map((s) => s.identity.pid),
        ]).size,
      ).toBe(72);
      reach.reach("chunk preparation in both control arms");
      reach.assertReached();
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 180000);

it("requires gain beyond A/A bias plus IQR and the favourable-pair threshold", () => {
  const cpu = { medianDeltaMs: -5, iqrDeltaMs: 2, favoursCandidate: 9, requiredFavourable: 7 };
  expect(biasAdjustedGain(cpu, 2.807)).toBe(true);
  expect(biasAdjustedGain({ ...cpu, medianDeltaMs: -4.807 }, 2.807)).toBe(false);
  expect(biasAdjustedGain({ ...cpu, favoursCandidate: 6 }, 2.807)).toBe(false);
  expect(() => biasAdjustedGain(cpu, -1)).toThrow("Invalid A/A bias");
});
