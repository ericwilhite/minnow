// eslint-disable-next-line no-restricted-imports -- Node-only benchmark driver: record host contention.
import { availableParallelism, loadavg } from "node:os";
import { expect } from "@playwright/test";
import { test } from "./fixtures.js";
import type { SchedulingVariant, SchedulingWork } from "../browser/next-owner/scheduling-worker.js";

/**
 * Isolate browser task-source scheduling from SQL, ledger admission, frames and storage. This
 * is a diagnostic experiment, not R6 acceptance: next-owner-latency.spec.ts keeps that gate.
 * Every variant completes the same integer kernel and reports its completion time as well as
 * echo latency, so a smaller quantum cannot win by doing less work. busyMs includes preemption;
 * it is elapsed slice time, not an OS CPU-time measurement. Run browsers sequentially.
 */
test.describe("rebuild owner scheduling experiment", () => {
  test.skip(process.env.MINNOW_OWNER_SCHEDULING_BENCH !== "1", "opt-in R6 lever benchmark");
  test.describe.configure({ timeout: 300_000 });

  test("isolated owner scheduling: fixed work and latency tradeoff", async ({
    page,
    browserName,
  }, info) => {
    await page.goto("/packages/core/browser/next-owner/blank.html");
    const loadStart = loadavg();
    const records = await page.evaluate(async () => {
      const variants: readonly SchedulingVariant[] = [
        { name: "fixture-4ms", quantumMs: 4, scheduler: "fixture" },
        { name: "shared-4ms", quantumMs: 4, scheduler: "shared" },
        { name: "shared-1ms", quantumMs: 1, scheduler: "shared" },
        { name: "shared-0.5ms", quantumMs: 0.5, scheduler: "shared" },
      ];
      const records = [];
      // Rotate order across five runs, retaining every observation, including cold starts.
      for (let run = 0; run < 5; run++) {
        for (let offset = 0; offset < variants.length; offset++) {
          const variant = variants[(run + offset) % variants.length];
          if (variant === undefined) throw new Error("Missing scheduling variant");
          const worker = new Worker("/packages/core/browser/next-owner/scheduling-worker.ts", {
            type: "module",
          });
          const channel = new MessageChannel();
          try {
            await new Promise<void>((resolve, reject) => {
              worker.onerror = (event) => {
                reject(new Error(event.message || "Scheduling worker failed to start"));
              };
              worker.onmessage = () => {
                resolve();
              };
              worker.postMessage({ port: channel.port2 }, [channel.port2]);
            });
            const ping = () =>
              new Promise<void>((resolve) => {
                channel.port1.onmessage = () => {
                  resolve();
                };
                channel.port1.postMessage(1);
              });
            for (let i = 0; i < 200; i++) await ping();
            let finished = false;
            const isFinished = () => finished;
            let started: () => void = () => undefined;
            const begun = new Promise<void>((resolve) => {
              started = resolve;
            });
            const done = new Promise<SchedulingWork>((resolve, reject) => {
              worker.onerror = (event) => {
                reject(new Error(event.message || "Scheduling worker failed"));
              };
              worker.onmessage = (
                event: MessageEvent<{ started?: true; done?: SchedulingWork }>,
              ) => {
                if (event.data.started === true) started();
                if (event.data.done !== undefined) {
                  finished = true;
                  resolve(event.data.done);
                }
              };
            });
            worker.postMessage({ variant });
            await begun;
            const times: number[] = [];
            while (!isFinished()) {
              const start = performance.now();
              await ping();
              if (!isFinished()) times.push((performance.now() - start) * 1000);
            }
            const work = await done;
            times.sort((a, b) => a - b);
            const quantile = (q: number) => times[Math.floor((times.length - 1) * q)] ?? Number.NaN;
            records.push({
              run,
              variant,
              count: times.length,
              p50: quantile(0.5),
              p95: quantile(0.95),
              p99: quantile(0.99),
              work,
            });
          } finally {
            channel.port1.close();
            worker.terminate();
          }
        }
      }
      return records;
    });
    expect(records).toHaveLength(20);
    for (const record of records) {
      expect(record.count).toBeGreaterThan(0);
      expect(record.work.isolated).toBe(true);
      expect(record.work.units).toBe(1_000_000);
      expect(record.work.checksum).toBe(1707356217);
    }
    const result = {
      schemaVersion: 1,
      browser: browserName,
      host: { cpus: availableParallelism(), loadStart, loadEnd: loadavg() },
      isolated: await page.evaluate(() => crossOriginIsolated),
      records,
    };
    expect(result.isolated).toBe(true);
    console.log(JSON.stringify(result));
    await info.attach("next-owner-scheduling", {
      body: JSON.stringify(result, null, 2),
      contentType: "application/json",
    });
  });
});
