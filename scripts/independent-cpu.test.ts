import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../packages/core/src/testing/seeds.js";
import { createReach, INDEPENDENT_CPU_REACH } from "../packages/core/src/testing/reach.js";
import { runIndependentCPU, startCPUWorker, type CPUArm } from "./lib/independent-cpu.mts";

interface Event {
  kind: string;
  pid: number;
  arm: string;
  cohort: number;
  boundary: string;
}
for (const seed of seedsFor("independent-cpu-policy", [0xb7c4, 0xb7c5]))
  it(`isolates and balances named CPU boundaries (${String(seed)})`, async () => {
    const reach = createReach(`independent CPU ${String(seed)}`, INDEPENDENT_CPU_REACH);
    reach.beginHistory();
    const directory = await mkdtemp(join(tmpdir(), "minnow-cpu-policy-")),
      trace = join(directory, "events.jsonl");
    const module = fileURLToPath(new URL("./fixtures/cpu-worker.mjs", import.meta.url));
    const control: CPUArm = { module, cwd: directory, revision: "parent", data: { trace } };
    const candidate: CPUArm = { module, cwd: directory, revision: "candidate", data: { trace } };
    const boundaries = mulberry32(seed)() < 0.5 ? ["roots", "sizing"] : ["sizing", "roots"];
    const events = async (): Promise<Event[]> =>
      (await readFile(trace, "utf8"))
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line) as Event);
    const balanced = async () => {
      const all = await events();
      expect(
        all
          .filter((e) => e.kind === "start")
          .map((e) => e.pid)
          .sort(),
      ).toEqual(
        all
          .filter((e) => e.kind === "close")
          .map((e) => e.pid)
          .sort(),
      );
    };
    try {
      const report = await runIndependentCPU({
        control,
        candidate,
        workloads: ["cell"],
        boundaries,
        primaryBoundary: "roots",
        settleMs: 0,
      });
      const pids = report.records.flatMap((record) =>
        record.samples.map((sample) => sample.identity.pid),
      );
      expect(new Set(pids).size).toBe(36);
      reach.reach("fresh process per pair");
      for (const record of report.records) {
        reach.reach(record.boundary === "roots" ? "roots hook" : "sizing hook");
        expect(record.primary).toBe(record.boundary === "roots");
        expect(record.samples).toHaveLength(18);
        for (let cohort = 0; cohort < 9; cohort++) {
          const pair = record.samples.filter((sample) => sample.cohort === cohort);
          expect(pair.map((sample) => sample.arm)).toEqual(
            cohort % 2 === 0 ? ["control", "candidate"] : ["candidate", "control"],
          );
          reach.reach(cohort % 2 === 0 ? "AB startup" : "BA startup");
          for (const sample of pair)
            expect(sample.cpuMs).toBeCloseTo(sample.userMs + sample.systemMs, 6);
        }
      }
      reach.reach("all-thread CPU");
      await balanced();
      reach.reach("drained close");
      const all = await events();
      let live = 0;
      for (const event of all) {
        if (event.kind === "start") live++;
        if (event.kind === "close") live--;
        expect(live).toBeGreaterThanOrEqual(0);
        expect(live).toBeLessThanOrEqual(1);
      }
      expect(live).toBe(0);
      reach.reach("one live VM");
      for (const boundary of boundaries)
        for (let cohort = 0; cohort < 9; cohort++)
          expect(
            all
              .filter((e) => e.kind === "start" && e.boundary === boundary && e.cohort === cohort)
              .map((e) => e.arm),
          ).toEqual(cohort % 2 === 0 ? ["control", "candidate"] : ["candidate", "control"]);
      for (const pid of pids)
        expect(all.filter((e) => e.pid === pid && e.kind === "verify")).toHaveLength(2);
      reach.reach("before and after verification");
      const context = { arm: "candidate" as const, cohort: 0, workload: "cell", boundary: "roots" };
      await expect(
        startCPUWorker({ ...candidate, data: { trace, wrongRevision: true } }, context),
      ).rejects.toThrow("revision/Node mismatch");
      await balanced();
      reach.reach("identity refusal");
      await expect(
        runIndependentCPU({
          control,
          candidate: { ...candidate, data: { trace, mismatch: true } },
          workloads: ["cell"],
          boundaries: ["roots"],
          primaryBoundary: "roots",
          settleMs: 0,
        }),
      ).rejects.toThrow("workload differs");
      await balanced();
      reach.reach("fingerprint refusal");
      await expect(
        runIndependentCPU({
          control,
          candidate: { ...candidate, data: { trace, drift: true } },
          workloads: ["cell"],
          boundaries: ["roots"],
          primaryBoundary: "roots",
          settleMs: 0,
        }),
      ).rejects.toThrow("output changed after measurement");
      await balanced();
      reach.reach("post-run refusal");
      const abort = new AbortController();
      await expect(
        runIndependentCPU({
          control: { ...control, data: { trace, wait: true } },
          candidate,
          workloads: ["cell"],
          boundaries: ["roots"],
          primaryBoundary: "roots",
          settleMs: 0,
          signal: abort.signal,
          onEvent: (value) => {
            if ((value as { kind?: string }).kind === "run-start")
              abort.abort(new Error("policy cancel"));
          },
        }),
      ).rejects.toThrow("policy cancel");
      await balanced();
      reach.reach("active cancellation");
      const setupAbort = new AbortController();
      await expect(
        startCPUWorker(
          { ...candidate, data: { trace, waitSetup: true } },
          context,
          (value) => {
            if ((value as { kind?: string }).kind === "setup-start")
              setupAbort.abort(new Error("setup cancel"));
          },
          setupAbort.signal,
        ),
      ).rejects.toThrow("setup cancel");
      await balanced();
      reach.reach("setup cancellation");
      reach.assertReached();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }, 120000);
