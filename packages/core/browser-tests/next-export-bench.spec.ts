/**
 * Gate A's streaming export on real OPFS in Chromium (opt-in: MINNOW_NEXT_EXPORT_BENCH=1;
 * MINNOW_EXPORT_ROWS, default 1,048,576). The wide table (200 columns) is written into OPFS
 * through the rebuild core's backend in a dedicated worker (browser/next-export-worker.ts), on
 * the persistent profile on disk (fixtures.ts); each pass then streams every row in a fresh
 * worker (scripts/browser-perf/page/export-stream.ts).
 *
 * - Cold: the profile's OPFS files are synced and dropped from the OS page cache first
 *   (`dd iflag=nocache count=0`, posix_fadvise DONTNEED on a file this user owns), and `fincore`
 *   confirms how much stayed resident.
 * - Warm: the OS cache holds the files (residency reported) but the worker's page cache is new.
 * - Cached: a prefix of the table read twice with `once` off and a cap that holds it; the second
 *   pass never reads storage (the decode half in the browser).
 *
 * The table has one group more than the export reads; the bench's hot set (protected, outside
 * the export) and the export's first group read once (probation) are established first, and after
 * the pass everything but the hot set's size is evicted: a scan that promoted what it hit loses
 * the hot set. The cached-bytes guard is off for this run (vite.library.config.ts), and asserted
 * off, since it re-hashes every release.
 *
 * Memory, sampled every 200 ms while a pass runs: the renderer processes' RSS (/proc, the
 * processes under this profile's browser process) and the export worker's JS heap
 * (CDP Runtime.getHeapUsage on the auto-attached worker target), as peaks over the value just
 * before the pass. `performance.measureUserAgentSpecificMemory` needs a cross-origin isolated
 * page, which this runner's pages are not.
 */
// eslint-disable-next-line no-restricted-imports -- Node-only Playwright bench (cache control, /proc); never shipped.
import { execFileSync } from "node:child_process";
// eslint-disable-next-line no-restricted-imports -- Node-only Playwright bench (cache control, /proc); never shipped.
import { readdirSync, readFileSync, statSync } from "node:fs";
// eslint-disable-next-line no-restricted-imports -- Node-only Playwright bench (cache control, /proc); never shipped.
import { join } from "node:path";
import { expect, type CDPSession, type Page } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

type Run = typeof import("../browser/next-export-run.js");
const RUN = "/packages/core/browser/next-export-run.ts";
const MiB = 1024 * 1024;
/**
 * Groups generated in the browser (MINNOW_EXPORT_DISTINCT, default 4); the rest are byte copies
 * at new addresses (distinct file bytes, the same encoded content), so a 1M-row table builds in
 * minutes on a loaded host.
 */
const DISTINCT = Number(process.env.MINNOW_EXPORT_DISTINCT ?? 4);

/** Every file under the profile's OPFS storage ("File System"). */
function opfsFiles(profile: string): string[] {
  const out: string[] = [];
  const walk = (dir: string, inside: boolean): void => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name);
      const under = inside || entry.name === "File System";
      if (entry.isDirectory()) walk(path, under);
      else if (under && entry.isFile()) out.push(path);
    }
  };
  walk(profile, false);
  return out;
}

const totalBytes = (files: readonly string[]): number =>
  files.reduce((sum, file) => sum + statSync(file).size, 0);

/** Bytes of `files` resident in the OS page cache. */
function resident(files: readonly string[]): number {
  let bytes = 0;
  for (const file of files) {
    const out = execFileSync("fincore", ["--bytes", "--noheadings", "--output", "RES", file], {
      encoding: "utf8",
    });
    bytes += Number(out.trim()) || 0;
  }
  return bytes;
}

/** Flushes `files` and drops their pages from the OS page cache. */
function dropCache(files: readonly string[]): void {
  for (const file of files) execFileSync("sync", [file]);
  for (const file of files)
    execFileSync("dd", [`if=${file}`, "iflag=nocache", "count=0", "status=none"]);
}

/** The processes of the browser running `profile`, by pid: [ppid, kind, rss bytes]. */
function chromeProcesses(profile: string): Map<number, { kind: string; rss: number }> {
  const table = new Map<number, { ppid: number; cmdline: string; rss: number }>();
  for (const name of readdirSync("/proc")) {
    const pid = Number(name);
    if (!Number.isInteger(pid)) continue;
    try {
      const stat = readFileSync(`/proc/${name}/stat`, "utf8");
      const ppid = Number(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[1]);
      const cmdline = readFileSync(`/proc/${name}/cmdline`, "utf8").replaceAll("\0", " ");
      const rss = /VmRSS:\s+(\d+) kB/u.exec(readFileSync(`/proc/${name}/status`, "utf8"));
      table.set(pid, { ppid, cmdline, rss: Number(rss?.[1] ?? 0) * 1024 });
    } catch {
      // The process exited while it was being read.
    }
  }
  const roots = [...table].filter(
    ([, process]) =>
      process.cmdline.includes(`--user-data-dir=${profile}`) &&
      !process.cmdline.includes("--type="),
  );
  const out = new Map<number, { kind: string; rss: number }>();
  const add = (pid: number): void => {
    const process = table.get(pid);
    if (process === undefined || out.has(pid)) return;
    out.set(pid, {
      kind: /--type=(\S+)/u.exec(process.cmdline)?.[1] ?? "browser",
      rss: process.rss,
    });
    for (const [child, entry] of table) if (entry.ppid === pid) add(child);
  };
  for (const [pid] of roots) add(pid);
  return out;
}

const rendererRss = (profile: string): number =>
  [...chromeProcesses(profile).values()]
    .filter((process) => process.kind === "renderer")
    .reduce((sum, process) => sum + process.rss, 0);

interface CpuProfile {
  readonly nodes: ReadonlyArray<{
    readonly id: number;
    readonly callFrame: { readonly functionName: string; readonly url: string };
  }>;
  readonly samples: readonly number[];
  readonly timeDeltas: readonly number[];
}

/**
 * The page's dedicated workers through CDP (auto-attach, non-flat sessions): their JS heap, and a
 * CPU profile of a pass that brackets itself with console.profile / profileEnd (the Profiler
 * domain is enabled on every worker as it attaches).
 */
async function workers(page: Page) {
  const cdp: CDPSession = await page.context().newCDPSession(page);
  const sessions = new Set<string>();
  const pending = new Map<number, (value: unknown) => void>();
  const profiles: CpuProfile[] = [];
  let next = 1;
  const send = (sessionId: string, method: string, params: object = {}): Promise<unknown> => {
    const id = next++;
    const answer = new Promise<unknown>((resolve) => {
      pending.set(id, resolve);
      setTimeout(() => {
        pending.delete(id);
        resolve(null);
      }, 1000);
    });
    cdp
      .send("Target.sendMessageToTarget", {
        sessionId,
        message: JSON.stringify({ id, method, params }),
      })
      .catch(() => undefined);
    return answer;
  };
  cdp.on("Target.attachedToTarget", (event) => {
    if (event.targetInfo.type !== "worker") return;
    sessions.add(event.sessionId);
    void send(event.sessionId, "Profiler.enable").then(() =>
      send(event.sessionId, "Profiler.setSamplingInterval", { interval: 500 }),
    );
  });
  cdp.on("Target.detachedFromTarget", (event) => {
    sessions.delete(event.sessionId);
  });
  cdp.on("Target.receivedMessageFromTarget", (event) => {
    const message = JSON.parse(event.message) as {
      id?: number;
      method?: string;
      result?: unknown;
      params?: { profile?: CpuProfile };
    };
    if (message.method === "Profiler.consoleProfileFinished" && message.params?.profile)
      profiles.push(message.params.profile);
    if (message.id === undefined) return;
    pending.get(message.id)?.(message.result ?? null);
    pending.delete(message.id);
  });
  let ok = true;
  try {
    await cdp.send("Target.setAutoAttach", {
      autoAttach: true,
      waitForDebuggerOnStart: false,
      flatten: false,
    });
  } catch {
    ok = false;
  }
  return {
    ok,
    profiles,
    async heap(): Promise<number> {
      let used = 0;
      for (const sessionId of sessions) {
        const result = (await send(sessionId, "Runtime.getHeapUsage")) as {
          usedSize?: number;
        } | null;
        used += result?.usedSize ?? 0;
      }
      return used;
    },
  };
}

/** Self time by function (name and file), largest first, as shares of the profile. */
function selfTimes(profile: CpuProfile, top: number): string[] {
  const byNode = new Map<number, string>();
  for (const node of profile.nodes) {
    const file = node.callFrame.url.split("/").at(-1)?.split("?")[0] ?? "";
    byNode.set(node.id, `${node.callFrame.functionName || "(anonymous)"} ${file}`.trim());
  }
  const totals = new Map<string, number>();
  let all = 0;
  profile.samples.forEach((id, k) => {
    const delta = profile.timeDeltas[k] ?? 0;
    const name = byNode.get(id) ?? "?";
    totals.set(name, (totals.get(name) ?? 0) + delta);
    all += delta;
  });
  return [...totals]
    .sort((a, b) => b[1] - a[1])
    .slice(0, top)
    .map(([name, micros]) => `${((100 * micros) / all).toFixed(1)}% ${name}`);
}

/** Sequential read throughput of `file` through the OS page cache with dd (MB/s). */
function ddRead(file: string): number {
  const started = performance.now();
  execFileSync("dd", [`if=${file}`, "of=/dev/null", "bs=1M", "status=none"]);
  return statSync(file).size / 1e6 / ((performance.now() - started) / 1000);
}

interface Row {
  readonly label: string;
  readonly residentBeforeMiB: number;
  readonly rows: number;
  readonly rowsPerSecond: number;
  readonly seconds: number;
  readonly loadShare: number;
  readonly mbPerSecond: number;
  readonly platformReads: number;
  readonly governorPeakMiB: number;
  readonly cacheResidentMiB: number;
  /** The page cache's protected segment before → after the export. */
  readonly protectedMiB: string;
  /** Hot set and scanned pages left after evicting everything but the hot set's size. */
  readonly hotKept: string;
  readonly scannedKept: string;
  readonly rendererRssPeakMiB: number;
  readonly workerHeapPeakMiB: number;
}

test.describe("rebuild core export on OPFS (bench)", () => {
  test("SELECT * over the wide table: cold, warm and cached, readahead, memory", async ({
    page,
    browserName,
  }, info) => {
    test.skip(process.env.MINNOW_NEXT_EXPORT_BENCH !== "1", "opt-in bench");
    test.skip(browserName !== "chromium", "measured on Chromium (CDP and /proc)");
    test.setTimeout(60 * 60_000);
    const rows = Number(process.env.MINNOW_EXPORT_ROWS ?? 1_048_576);
    await page.goto("/packages/core/browser/");
    await requireWorkerOpfs(page);
    const profile = info.outputPath("storage-profile");
    const targets = await workers(page);

    // One group more than the export reads: the measured hot set comes from it, so the export
    // never touches it (export-stream.ts `establishWorkingSet`).
    const exported = Math.ceil(rows / 65_536);
    const built = await page.evaluate(
      async ([url, count, distinct]) => ((await import(url)) as Run).build(count, distinct),
      [RUN, rows + 65_536, DISTINCT] as const,
    );
    const files = opfsFiles(profile);
    const onDisk = totalBytes(files);
    console.log(
      `built ${String(rows)} rows × 200 in ${(built.ms / 1000).toFixed(0)} s: ${String(built.groups)} groups, data ${(built.dataBytes / MiB).toFixed(0)} MiB, OPFS files ${(onDisk / MiB).toFixed(0)} MiB in ${String(files.length)} files`,
    );

    // The device and the OS page cache on their own: the largest OPFS file read sequentially
    // with dd, from disk (dropped first) and again from the page cache.
    const largest = [...files].sort((a, b) => statSync(b).size - statSync(a).size)[0] ?? "";
    dropCache(files);
    const device = { coldMBps: Math.round(ddRead(largest)), warmMBps: Math.round(ddRead(largest)) };
    console.log(
      `dd of the data file: cold ${String(device.coldMBps)} MB/s, warm ${String(device.warmMBps)} MB/s`,
    );

    const results: Row[] = [];
    const measure = async (
      label: string,
      cold: boolean,
      options: Parameters<Run["exportPass"]>[0],
    ): Promise<void> => {
      if (cold) dropCache(files);
      const residentBefore = resident(files);
      const rssBefore = rendererRss(profile);
      let rssPeak = rssBefore;
      let heapPeak = 0;
      const sampling = { on: true };
      const sampler = (async () => {
        while (sampling.on) {
          rssPeak = Math.max(rssPeak, rendererRss(profile));
          heapPeak = Math.max(heapPeak, await targets.heap());
          await new Promise((resolve) => setTimeout(resolve, 200));
        }
      })();
      const passes = await page
        .evaluate(async ([url, run]) => ((await import(url)) as Run).exportPass(run), [
          RUN,
          options,
        ] as const)
        .finally(() => {
          sampling.on = false;
        });
      await sampler;
      const pass = passes.at(-1);
      if (pass === undefined) throw new Error("no pass");
      // vite.library.config.ts drops the cached-bytes guard when this bench is opted in.
      expect(pass.verifying, "the cached-bytes guard re-hashes every release").toBe(false);
      const bytes = (built.dataBytes * pass.rows) / (rows + 65_536);
      const row: Row = {
        label,
        residentBeforeMiB: Math.round(residentBefore / MiB),
        rows: pass.rows,
        rowsPerSecond: Math.round(pass.rows / (pass.ms / 1000)),
        seconds: Number((pass.ms / 1000).toFixed(1)),
        loadShare: Number((pass.loadMs / pass.ms).toFixed(2)),
        mbPerSecond: Math.round(bytes / 1e6 / (pass.ms / 1000)),
        platformReads: pass.platformReads,
        governorPeakMiB: Math.round(pass.governorPeakMiB),
        cacheResidentMiB: Number(pass.cacheResidentMiB.toFixed(1)),
        protectedMiB: `${pass.protectedBeforeMiB.toFixed(1)} → ${pass.protectedAfterMiB.toFixed(1)}`,
        hotKept: pass.kept?.hot ?? "",
        scannedKept: pass.kept?.scanned ?? "",
        rendererRssPeakMiB: Math.round((rssPeak - rssBefore) / MiB),
        workerHeapPeakMiB: Math.round(heapPeak / MiB),
      };
      results.push(row);
      console.log(JSON.stringify(row));
    };

    const base = { once: true, capMiB: 512, passes: 1, groups: exported };
    for (const readaheadKiB of [0, 64, 256, 1024])
      await measure(`cold, readahead ${String(readaheadKiB)} KiB`, true, {
        ...base,
        readaheadKiB,
      });
    for (const readaheadKiB of [0, 256, 1024])
      await measure(`warm, readahead ${String(readaheadKiB)} KiB`, false, {
        ...base,
        readaheadKiB,
      });
    await measure("cold, once off, readahead 256 KiB", true, {
      ...base,
      once: false,
      readaheadKiB: 256,
    });
    await measure("cached (pass 2, 4 groups, once off)", false, {
      once: false,
      capMiB: 1024,
      passes: 2,
      groups: 4,
      readaheadKiB: 256,
    });

    // Where a warm pass spends its time: a CPU profile of the worker (CDP), by self time.
    await measure("warm, readahead 256 KiB, profiled", false, {
      ...base,
      readaheadKiB: 256,
      profile: true,
    });
    const recorded = targets.profiles.at(-1);
    const hotspots = recorded === undefined ? [] : selfTimes(recorded, 15);
    console.log(`warm pass profile (self time):\n  ${hotspots.join("\n  ")}`);

    await info.attach("export-bench", {
      body: JSON.stringify(
        { rows, built, opfsBytes: onDisk, heapSampling: targets.ok, device, hotspots, results },
        null,
        2,
      ),
      contentType: "application/json",
    });
    await page.evaluate(async (url) => ((await import(url)) as Run).remove(), RUN);
  });
});
