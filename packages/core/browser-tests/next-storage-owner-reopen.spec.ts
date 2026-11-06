/** Real S5/W5M owner-ready lane. See the bench comment in next-storage-owner-reopen.ts.
 * CPU samples bracket phases using /proc ticks (10 ms resolution on this host), and therefore
 * include IPC boundary work; wall-only samples never pause for the sampler. OS caches stay warm,
 * but every reopen uses a fresh dedicated worker and a fresh backend connection/real write path.
 */
import { chromium, expect, test } from "@playwright/test";
import { mkdirSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { requireWorkerOpfs } from "./fixtures.js";
import { reopenCpuSampler } from "./reopen-cpu.js";
import { cpuDelta, type ReopenCpuSnapshot } from "../browser/next-storage-listed-reopen.js";
import type { OwnerPhase } from "../browser/next-storage-owner-reopen.js";
import type { OwnerBenchRequest } from "../browser/next-storage-owner-worker.js";

type Run = typeof import("../browser/next-storage-owner-run.js");
interface ReopenResult {
  result: {
    phases: OwnerPhase[];
    root: {
      groups: number;
      extents: number;
      listPages: number;
      rows: Array<{ name: string; rows: number }>;
    };
    count: { count: string; executor: string };
    preparedAtReady: number;
  };
  readyWallMs: number;
  cpu: ReopenCpuSnapshot[];
}
const sampleAt = (values: readonly ReopenCpuSnapshot[], at: number) => {
  const value = values[at];
  if (value === undefined) throw new Error(`Missing CPU sample ${String(at)}`);
  return value;
};
const RUN = "/packages/core/browser/next-storage-owner-run.ts";
for (const store of ["opfs", "indexeddb"] as const)
  for (const dataset of ["S5", "W5M"] as const)
    test(`real owner ready ${dataset} ${store}`, async ({ browserName, baseURL }, info) => {
      const enabled = process.env.MINNOW_OWNER_REOPEN === "1";
      test.skip(!enabled || browserName !== "chromium", "opt-in Chromium owner-ready measurement");
      test.setTimeout(7_200_000);
      const directory = process.env.MINNOW_OWNER_REOPEN_CACHE ?? info.outputPath("owner-dataset");
      mkdirSync(directory, { recursive: true });
      const profile = join(directory, `${dataset}-${store}`);
      const context = await chromium.launchPersistentContext(profile, { baseURL: baseURL ?? "" });
      try {
        const page = await context.newPage();
        await page.goto("/packages/core/browser/");
        if (store === "opfs") await requireWorkerOpfs(page);
        page.on("console", (message) => {
          if (message.text().startsWith("owner-load:")) console.log(message.text());
        });
        const sampler = reopenCpuSampler(profile);
        await page.exposeFunction("ownerBenchCpu", sampler);
        const testRows = process.env.MINNOW_OWNER_REOPEN_SMOKE === "1" ? 1000 : undefined;
        const db = `${dataset}-${store}-${process.env.MINNOW_OWNER_REOPEN_REUSE === "1" ? "retained-v1" : String(Date.now())}`;
        const marker = join(directory, `${db}.json`);
        const base = {
          store,
          dataset,
          db,
          cpu: false,
          ...(testRows === undefined ? {} : { testRows }),
        };
        const run = async (request: OwnerBenchRequest): Promise<ReopenResult> =>
          page.evaluate(
            async ({ url, request }) => {
              const module = (await import(url)) as Run;
              const sample = request.cpu
                ? () => (window as unknown as { ownerBenchCpu(): Promise<unknown> }).ownerBenchCpu()
                : null;
              return (await module.runOwnerReopen(request, sample)) as ReopenResult;
            },
            { url: RUN, request },
          );
        const build = existsSync(marker)
          ? (JSON.parse(readFileSync(marker, "utf8")) as unknown)
          : await run({ ...base, op: "build" });
        writeFileSync(marker, JSON.stringify(build));
        await info.attach("actual-data-load", {
          body: JSON.stringify(build, null, 2),
          contentType: "application/json",
        });
        const wall: ReopenResult[] = [];
        const cpu = [];
        for (let repeat = 0; repeat < 3; repeat++) {
          const sample = await run({ ...base, op: "open" });
          wall.push(sample);
          expect(sample.result.preparedAtReady).toBe(0);
          expect(sample.result.root.rows).toHaveLength(dataset === "S5" ? 13 : 4);
          if (testRows === undefined) {
            expect(sample.result.root.groups).toBeGreaterThan(0);
            expect(sample.result.root.rows.reduce((sum, table) => sum + table.rows, 0)).toBe(
              dataset === "S5" ? 18_733_746 : 5_001_150,
            );
          }
          expect(sample.result.count.executor).toBe("statistics-only stand-in");
          const expected = dataset === "W5M" ? (testRows ?? 5_000_000) : (testRows ?? 5_001_000);
          expect(Number(sample.result.count.count)).toBe(expected);
        }
        for (let repeat = 0; repeat < 3; repeat++) {
          const sample = await run({ ...base, op: "open", cpu: true });
          expect(sample.cpu).toHaveLength(sample.result.phases.length + 1);
          cpu.push({
            root: sample.result.root,
            phases: sample.result.phases.map((phase, at) => ({
              ...phase,
              ...cpuDelta(sampleAt(sample.cpu, at), sampleAt(sample.cpu, at + 1)),
            })),
          });
        }
        const report = {
          dataset,
          store,
          testRows: testRows ?? null,
          governorBytes: 256 << 20,
          executor: "unused stand-in at startup; first count is statistics-only",
          cache:
            "fresh worker/backend; OS/browser disk caches not dropped; test-only cached-byte rehash guard off",
          readBytes:
            "OPFS returned bytes / IndexedDB cloned byte payloads; native IDB disk bytes opaque; numeric index/meta values and keys separate",
          cpu: "Linux /proc owner-thread and browser-tree ticks, bracketed separately from wall-only samples; includes phase-boundary IPC",
          wall,
          profiled: cpu,
        };
        await info.attach("owner-ready-reopen", {
          body: JSON.stringify(report, null, 2),
          contentType: "application/json",
        });
        console.log(`OWNER_REOPEN ${JSON.stringify(report)}`);
      } finally {
        await context.close();
      }
    });
