/**
 * Page-stream diagnostics and scan/result-transport export on Chromium OPFS (opt-in:
 * MINNOW_NEXT_EXPORT_BENCH=1; MINNOW_EXPORT_ROWS, default 1,048,576). The 200-column table is
 * written through the rebuild core backend in a dedicated worker (browser/next-export-worker.ts), on
 * the persistent profile on disk (fixtures.ts); each pass then streams every row in a fresh
 * worker (scripts/browser-perf/page/export-stream.ts).
 *
 * MINNOW_EXPORT_SCAN_COMPARE=1 adds a second mode: B4's real TableScan and pipeline driver,
 * a labelled bench adapter standing in for the executor's future frame assembly (not yet on
 * main), real ResultChannel/frameByteLength/owner transfer policy and MessagePort credits.
 * Fixture descriptors replace SQL/catalog/group-root loading. The scan uses its normal
 * cached ChunkReader policy and byte target; the page-stream mode uses one-pass pooled reads.
 * Five interleaved cold/warm attempts per mode; sparse CDP profiles bracket setup-excluded
 * thread CPU (1-second sampling interval, not a hotspot profile). Both await all frame credits.
 *
 * - Cold: the profile's OPFS files are synced and dropped from the OS page cache first
 *   (`dd iflag=nocache count=0`, posix_fadvise DONTNEED on a file this user owns), and `fincore`
 *   confirms how much stayed resident.
 * - Warm: the files are read through the OS page cache (dd) right before the pass and `fincore`
 *   must find at least 98% of them resident (review B2-p2h #3); a pass that finds less is
 *   labelled "partial residency" with its share, not warm. The worker's page cache is new.
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
import { exportTrace, hostSample } from "./export-diagnostics.js";
import { requireWorkerOpfs, test } from "./fixtures.js";

type Run = typeof import("../browser/next-export-run.js");
const RUN = "/packages/core/browser/next-export-run.ts";
const MiB = 1024 * 1024;
/** Clock ticks per second of /proc's utime and stime (Linux USER_HZ). */
const CLOCK_TICKS = 100;
/** A warm pass needs at least this share of the store's files resident when it starts. */
const WARM_SHARE = 0.98;
/**
 * Groups generated in the browser (MINNOW_EXPORT_DISTINCT, default 4); the rest are byte copies
 * at new addresses (distinct file bytes, the same encoded content), so a 1M-row table builds in
 * minutes on a loaded host.
 */
const DISTINCT = Number(process.env.MINNOW_EXPORT_DISTINCT ?? 4);
/** MINNOW_EXPORT_FOCUS=1: only Gate A's configuration (cold ×5, warm ×5, a profiled pass). */
const COMPARE_STREAM = process.env.MINNOW_EXPORT_COMPARE_STREAM === "1";
const SCAN_COMPARE = process.env.MINNOW_EXPORT_SCAN_COMPARE === "1";
const DEPTH_COMPARE = process.env.MINNOW_EXPORT_DEPTH_COMPARE === "1";
const READ_COMPARE = process.env.MINNOW_EXPORT_READ_COMPARE === "1";
const VARIANCE = process.env.MINNOW_EXPORT_VARIANCE === "1";
const PROFILE_ONLY = process.env.MINNOW_EXPORT_PROFILE_ONLY === "1" || VARIANCE;
const FOCUS = process.env.MINNOW_EXPORT_FOCUS === "1";
/** The row window of the focused passes (MINNOW_EXPORT_WINDOW_MIB, default 64). */
const WINDOW_MIB = Number(process.env.MINNOW_EXPORT_WINDOW_MIB ?? 64);

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

/** CPU seconds (utime + stime) of each dedicated-worker thread of the renderers, by thread id. */
function workerThreadCpu(profile: string): Map<string, number> {
  const out = new Map<string, number>();
  for (const [pid, process] of chromeProcesses(profile)) {
    if (process.kind !== "renderer") continue;
    let tasks: string[];
    try {
      tasks = readdirSync(`/proc/${String(pid)}/task`);
    } catch {
      continue;
    }
    for (const tid of tasks)
      try {
        const comm = readFileSync(`/proc/${String(pid)}/task/${tid}/comm`, "utf8");
        if (!comm.startsWith("DedicatedWorker")) continue;
        const stat = readFileSync(`/proc/${String(pid)}/task/${tid}/stat`, "utf8");
        const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
        out.set(`${String(pid)}/${tid}`, (Number(fields[11]) + Number(fields[12])) / CLOCK_TICKS);
      } catch {
        // The thread ended while it was being read.
      }
  }
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
async function workers(page: Page, profileDirectory: string) {
  const cdp: CDPSession = await page.context().newCDPSession(page);
  const sessions = new Set<string>();
  const pending = new Map<number, (value: unknown) => void>();
  const profiles: CpuProfile[] = [];
  const profileCpuSeconds: number[] = [];
  const starts = new Map<string, Map<string, number>>();
  let next = 1;
  let external = 0;
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
    void send(event.sessionId, "Profiler.enable")
      .then(() =>
        send(event.sessionId, "Profiler.setSamplingInterval", {
          interval: SCAN_COMPARE ? 1_000_000 : 500,
        }),
      )
      .then(() =>
        PROFILE_ONLY || SCAN_COMPARE
          ? send(event.sessionId, "Runtime.runIfWaitingForDebugger")
          : undefined,
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
    if (message.method === "Profiler.consoleProfileStarted")
      starts.set(event.sessionId, workerThreadCpu(profileDirectory));
    if (message.method === "Profiler.consoleProfileFinished" && message.params?.profile) {
      profiles.push(message.params.profile);
      const before = starts.get(event.sessionId);
      let used = 0;
      for (const [tid, seconds] of workerThreadCpu(profileDirectory))
        if (before?.has(tid)) used += seconds - (before.get(tid) ?? seconds);
      profileCpuSeconds.push(used);
      starts.delete(event.sessionId);
    }
    if (message.id === undefined) return;
    pending.get(message.id)?.(message.result ?? null);
    pending.delete(message.id);
  });
  let ok = true;
  try {
    await cdp.send("Target.setAutoAttach", {
      autoAttach: true,
      waitForDebuggerOnStart: PROFILE_ONLY || SCAN_COMPARE,
      flatten: false,
    });
  } catch {
    ok = false;
  }
  return {
    ok,
    profiles,
    profileCpuSeconds,
    async collectPage() {
      const before = await cdp.send("Runtime.getHeapUsage");
      await cdp.send("HeapProfiler.collectGarbage");
      const after = await cdp.send("Runtime.getHeapUsage");
      return { before, after };
    },
    async heap(): Promise<number> {
      let used = 0;
      for (const sessionId of sessions) {
        const result = (await send(sessionId, "Runtime.getHeapUsage")) as {
          usedSize?: number;
          backingStorageSize?: number;
        } | null;
        used += result?.usedSize ?? 0;
        external = Math.max(external, result?.backingStorageSize ?? 0);
      }
      return used;
    },
    /** The largest ArrayBuffer backing-store size seen since the last call (Chromium's CDP). */
    takeExternal(): number {
      const seen = external;
      external = 0;
      return seen;
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
  readonly readMiB: number;
  readonly mode: "pages" | "scan";
  readonly scanStats?: Awaited<ReturnType<Run["exportPass"]>>[number]["scanStats"];
  readonly afterWorkerClose?: Awaited<ReturnType<Run["exportPass"]>>[number]["afterWorkerClose"];
  readonly seconds: number;
  readonly exportMs: number;
  readonly loadShare: number | null;
  readonly mbPerSecond: number;
  readonly platformReads: number;
  readonly governorPeakMiB: number;
  readonly cacheResidentMiB: number;
  /** Idle pooled one-pass buffers at the end of the pass. */
  readonly poolMiB: number;
  /** The worker's largest ArrayBuffer backing store seen during the pass (CDP), or 0. */
  readonly arrayBufferPeakMiB: number;
  /** The page cache's protected segment before → after the export. */
  readonly protectedMiB: string;
  /** Hot set and scanned pages left after evicting everything but the hot set's size. */
  readonly hotKept: string;
  readonly scannedKept: string;
  readonly rendererRssPeakMiB: number;
  readonly workerHeapPeakMiB: number;
  /** The export worker's thread CPU (whole worker, setup included; sampled every 200 ms). */
  readonly workerCpuSeconds: number;
  /** Thread CPU delta at CDP profile boundaries; setup excluded, tick/CDP latency applies. */
  readonly exportCpuSeconds?: number;
  readonly phases?: Awaited<ReturnType<Run["exportPass"]>>[number]["phases"];
  /** Rows per second of that CPU: the pass's cost, whatever share of a core the host gave it. */
  readonly rowsPerCpuSecond: number;
  readonly diagnostics?: unknown;
  readonly host?: unknown;
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
    if (!Number.isInteger(rows) || rows < 1 || !Number.isInteger(DISTINCT) || DISTINCT < 1)
      throw new Error("Export rows/distinct must be positive integers");
    await page.goto("/packages/core/browser/");
    await requireWorkerOpfs(page);
    const profile = info.outputPath("storage-profile");
    const targets = await workers(page, profile);

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
      /** Prime and verify OS-cache residency first (else the pass inherits what is there). */
      warm: boolean,
      options: Parameters<Run["exportPass"]>[0],
    ): Promise<void> => {
      if (cold) dropCache(files);
      // Warm is established, not assumed: primed now and checked (review B2-p2h #3).
      if (warm) for (const file of files) ddRead(file);
      const residentBefore = resident(files);
      const share = residentBefore / onDisk;
      const established = !warm || share >= WARM_SHARE;
      const hostBefore = VARIANCE || SCAN_COMPARE ? hostSample() : undefined;
      const stopTrace = VARIANCE ? await exportTrace(page) : undefined;
      const rssBefore = rendererRss(profile);
      const samples: unknown[] = [];
      let ticks = 0;
      let rssPeak = rssBefore;
      let heapPeak = 0;
      // The export worker's thread CPU: threads that appear during the pass, at their last sample.
      const threadsBefore = new Set(workerThreadCpu(profile).keys());
      const passCpu = new Map<string, number>();
      targets.takeExternal();
      const sampling = { on: true };
      const sampler = (async () => {
        while (sampling.on) {
          const rss = rendererRss(profile);
          rssPeak = Math.max(rssPeak, rss);
          if (VARIANCE && ++ticks % 5 === 0)
            samples.push({ rss, resident: resident(files), host: hostSample() });
          for (const [tid, seconds] of workerThreadCpu(profile))
            if (!threadsBefore.has(tid)) passCpu.set(tid, seconds);
          heapPeak = Math.max(heapPeak, await targets.heap());
          await new Promise((resolve) => setTimeout(resolve, 200));
        }
      })();
      const profilesBefore = targets.profiles.length;
      const passes = await page
        .evaluate(async ([url, run]) => ((await import(url)) as Run).exportPass(run), [
          RUN,
          options,
        ] as const)
        .finally(() => {
          sampling.on = false;
        });
      await sampler;
      const trace = await stopTrace?.();
      if (trace)
        await info.attach(`gc-trace-${String(results.length)}`, {
          body: JSON.stringify({ traceEvents: trace.events }),
          contentType: "application/json",
        });
      const pass = passes.at(-1);
      if (pass === undefined) throw new Error("no pass");
      // vite.library.config.ts drops the cached-bytes guard when this bench is opted in.
      if (COMPARE_STREAM)
        expect(pass.baselineCommit).toBe(process.env.MINNOW_EXPORT_BASELINE_COMMIT ?? "18548e34");
      expect(pass.verifying, "the cached-bytes guard re-hashes every release").toBe(false);
      const bytes = (built.dataBytes * pass.rows) / (rows + 65_536);
      const cpuSeconds = [...passCpu.values()].reduce((sum, seconds) => sum + seconds, 0);
      const exportCpu = targets.profileCpuSeconds.at(-1);
      if ((SCAN_COMPARE || VARIANCE) && options.profile) {
        expect(targets.profiles).toHaveLength(profilesBefore + 1);
        expect(exportCpu).toBeDefined();
      }
      const row: Row = {
        ...(pass.phases ? { phases: pass.phases } : {}),
        ...(options.profile && exportCpu !== undefined ? { exportCpuSeconds: exportCpu } : {}),
        ...(hostBefore ? { host: { before: hostBefore, after: hostSample() } } : {}),
        ...(trace
          ? {
              diagnostics: {
                hostBefore,
                hostAfter: hostSample(),
                samples,
                rssBefore,
                rssEnd: rendererRss(profile),
                residentAfter: resident(files),
                gcMs: trace.gcMs,
                gcCount: trace.gcCount,
                traceMs: trace.traceMs,
              },
            }
          : {}),
        label: established
          ? label
          : `${label.replace("warm", "partial residency")} (${(100 * share).toFixed(0)}% resident)`,
        residentBeforeMiB: Math.round(residentBefore / MiB),
        rows: pass.rows,
        readMiB: pass.readMiB,
        mode: pass.mode,
        ...(pass.scanStats ? { scanStats: pass.scanStats } : {}),
        ...(pass.afterWorkerClose ? { afterWorkerClose: pass.afterWorkerClose } : {}),
        rowsPerSecond: Math.round(pass.rows / (pass.ms / 1000)),
        seconds: Number((pass.ms / 1000).toFixed(1)),
        exportMs: pass.ms,
        loadShare: pass.mode === "scan" ? null : Number((pass.loadMs / pass.ms).toFixed(2)),
        mbPerSecond: Math.round(bytes / 1e6 / (pass.ms / 1000)),
        platformReads: pass.platformReads,
        governorPeakMiB: Math.round(pass.governorPeakMiB),
        cacheResidentMiB: Number(pass.cacheResidentMiB.toFixed(1)),
        poolMiB: Number(pass.poolMiB.toFixed(1)),
        arrayBufferPeakMiB: Math.round(targets.takeExternal() / MiB),
        protectedMiB: `${pass.protectedBeforeMiB.toFixed(1)} → ${pass.protectedAfterMiB.toFixed(1)}`,
        hotKept: pass.kept?.hot ?? "",
        scannedKept: pass.kept?.scanned ?? "",
        rendererRssPeakMiB: Math.round((rssPeak - rssBefore) / MiB),
        workerHeapPeakMiB: Math.round(heapPeak / MiB),
        workerCpuSeconds: Number(cpuSeconds.toFixed(2)),
        rowsPerCpuSecond: cpuSeconds > 0 ? Math.round(pass.rows / cpuSeconds) : 0,
      };
      results.push(row);
      console.log(JSON.stringify(row));
    };

    const base = { once: true, capMiB: 512, passes: 1, groups: exported };
    if (SCAN_COMPARE) {
      const options = {
        ...base,
        readaheadKiB: 256,
        windowMiB: WINDOW_MIB,
        auditWorkingSet: false,
        profile: true,
      };
      const rounds = Number(process.env.MINNOW_EXPORT_SCAN_ROUNDS ?? 5);
      if (!Number.isInteger(rounds) || rounds < 1)
        throw new Error("Scan rounds must be a positive integer");
      for (let round = 0; round < rounds; round++)
        for (const mode of round % 2 === 0
          ? (["pages", "scan"] as const)
          : (["scan", "pages"] as const)) {
          await measure(`cold ${mode} round ${String(round)}`, true, false, { ...options, mode });
          if (process.env.MINNOW_EXPORT_SCAN_COLD_ONLY !== "1")
            await measure(`warm ${mode} round ${String(round)}`, false, true, { ...options, mode });
        }
    } else if (DEPTH_COMPARE) {
      const options = { ...base, readaheadKiB: 256, windowMiB: WINDOW_MIB, auditWorkingSet: false };
      for (let round = 0; round < 5; round++)
        for (const groupDepth of round % 2 === 0 ? ([1, 2] as const) : ([2, 1] as const)) {
          await measure(`cold depth ${String(groupDepth)} round ${String(round)}`, true, false, {
            ...options,
            groupDepth,
          });
          await measure(`warm depth ${String(groupDepth)} round ${String(round)}`, false, true, {
            ...options,
            groupDepth,
          });
        }
    } else if (READ_COMPARE) {
      const options = { ...base, readaheadKiB: 256, windowMiB: WINDOW_MIB, auditWorkingSet: false };
      for (let round = 0; round < 5; round++)
        for (const readMiB of round % 2 === 0 ? ([4, 8] as const) : ([8, 4] as const)) {
          await measure(`cold read ${String(readMiB)} MiB round ${String(round)}`, true, false, {
            ...options,
            readMiB,
          });
          await measure(`warm read ${String(readMiB)} MiB round ${String(round)}`, false, true, {
            ...options,
            readMiB,
          });
        }
    } else if (COMPARE_STREAM) {
      const options = { ...base, readaheadKiB: 256, windowMiB: WINDOW_MIB, auditWorkingSet: false };
      for (let round = 0; round < 5; round++) {
        for (const baselineStream of round % 2 === 0 ? [true, false] : [false, true]) {
          const variant = baselineStream ? "baseline" : "current";
          await measure(`cold ${variant} round ${String(round)}`, true, false, {
            ...options,
            baselineStream,
          });
          await measure(`warm ${variant} round ${String(round)}`, false, true, {
            ...options,
            baselineStream,
          });
        }
      }
    } else if (VARIANCE) {
      const options = {
        ...base,
        readaheadKiB: 256,
        windowMiB: WINDOW_MIB,
        profile: true,
        profilePhases: true,
      };
      for (let round = 0; round < 9; round++)
        for (const cold of round % 2 === 0 ? [true, false] : [false, true])
          await measure(
            `${cold ? "cold" : "warm"} diagnostic round ${String(round)}`,
            cold,
            !cold,
            options,
          );
      expect(targets.profiles).toHaveLength(18);
    } else if (PROFILE_ONLY) {
      const options = {
        ...base,
        readaheadKiB: 256,
        windowMiB: WINDOW_MIB,
        profile: true,
        profilePhases: true,
      };
      await measure(`cold, window ${String(WINDOW_MIB)} MiB, phase profile`, true, false, options);
      await measure(`warm, window ${String(WINDOW_MIB)} MiB, phase profile`, false, true, options);
      expect(targets.profiles).toHaveLength(2);
      // Unprofiled references omit the same setup and hot-set prefill as the profiles.
      const reference = {
        ...base,
        readaheadKiB: 256,
        windowMiB: WINDOW_MIB,
        auditWorkingSet: false,
      };
      await measure("cold, unprofiled reference", true, false, reference);
      await measure("warm, unprofiled reference", false, true, reference);
    } else if (FOCUS) {
      // Gate A's configuration, repeated and interleaved (per-column readahead, then the row
      // window, export-stream.ts `windowBytes`), so host drift falls on both alike.
      const perColumn = { ...base, readaheadKiB: 256 };
      const windowed = { ...base, readaheadKiB: 256, windowMiB: WINDOW_MIB };
      for (let run = 1; run <= 5; run++) {
        await measure(`cold, readahead 256 KiB, run ${String(run)}`, true, false, perColumn);
        await measure(
          `cold, window ${String(WINDOW_MIB)} MiB, run ${String(run)}`,
          true,
          false,
          windowed,
        );
      }
      for (let run = 1; run <= 5; run++) {
        await measure(`warm, readahead 256 KiB, run ${String(run)}`, false, true, perColumn);
        await measure(
          `warm, window ${String(WINDOW_MIB)} MiB, run ${String(run)}`,
          false,
          true,
          windowed,
        );
      }
    } else {
      for (const readaheadKiB of [0, 64, 256, 1024])
        await measure(`cold, readahead ${String(readaheadKiB)} KiB`, true, false, {
          ...base,
          readaheadKiB,
        });
      for (const readaheadKiB of [0, 256, 1024])
        await measure(`warm, readahead ${String(readaheadKiB)} KiB`, false, true, {
          ...base,
          readaheadKiB,
        });
      await measure("cold, once off, readahead 256 KiB", true, false, {
        ...base,
        once: false,
        readaheadKiB: 256,
      });
      await measure("cached (pass 2, 4 groups, once off)", false, false, {
        once: false,
        capMiB: 1024,
        passes: 2,
        groups: 4,
        readaheadKiB: 256,
      });
    }
    // Where a warm pass spends its time: a CPU profile of the worker (CDP), by self time.
    if (!PROFILE_ONLY && !COMPARE_STREAM && !READ_COMPARE && !DEPTH_COMPARE && !SCAN_COMPARE)
      await measure(`warm, window ${String(WINDOW_MIB)} MiB, profiled`, false, true, {
        ...base,
        readaheadKiB: 256,
        windowMiB: WINDOW_MIB,
        profile: true,
      });
    for (const [index, profile] of targets.profiles.entries())
      await info.attach(`export-cpu-${String(index)}`, {
        body: JSON.stringify(profile),
        contentType: "application/json",
      });
    const recorded = SCAN_COMPARE ? undefined : targets.profiles.at(-1);
    const hotspots = recorded === undefined ? [] : selfTimes(recorded, 15);
    console.log(`warm pass profile (self time):\n  ${hotspots.join("\n  ")}`);

    const gcRssBefore = rendererRss(profile);
    const campaignGc = SCAN_COMPARE ? await targets.collectPage() : undefined;
    const gcRssAfter = rendererRss(profile);
    const report = {
      rows,
      built,
      opfsBytes: onDisk,
      generatedGroups: built.generatedGroups,
      exportedRows: results[0]?.rows,
      exportedGroups: exported,
      governorCapMiB: base.capMiB,
      rowWindowMiB: WINDOW_MIB,
      heapSampling: targets.ok,
      ...(SCAN_COMPARE
        ? {
            scanAdapter:
              "bench stand-in for future executor frame assembly; real TableScan/ResultChannel/owner transfer policy/MessagePort credits; fixture descriptors exclude SQL/catalog/root loading",
            cpuSamplingIntervalUs: 1_000_000,
            rounds: Number(process.env.MINNOW_EXPORT_SCAN_ROUNDS ?? 5),
            coldOnly: process.env.MINNOW_EXPORT_SCAN_COLD_ONLY === "1",
          }
        : {}),
      device,
      hotspots,
      profileHotspots: SCAN_COMPARE
        ? []
        : targets.profiles.map((profile) => selfTimes(profile, 30)),
      ...(campaignGc
        ? {
            afterCampaignGc: {
              rendererBeforeMiB: gcRssBefore / MiB,
              rendererAfterMiB: gcRssAfter / MiB,
              page: campaignGc,
            },
          }
        : {}),
      ...(COMPARE_STREAM
        ? {
            comparisonScope: "stream loop only; both use the current engine",
            baselineStreamRevision: process.env.MINNOW_EXPORT_BASELINE_COMMIT ?? "18548e34",
          }
        : {}),
      results,
    };
    await info.attach("export-bench", {
      body: JSON.stringify(report, null, 2),
      contentType: "application/json",
    });
    // One line the nightly workflow keeps (browser-performance.yml): recorded, not judged.
    console.log(`EXPORT_BENCH ${JSON.stringify(report)}`);
    await page.evaluate(async (url) => ((await import(url)) as Run).remove(), RUN);
  });
});
