/** Node-only diagnostics for the opt-in export bench; never imported by the engine. */
// eslint-disable-next-line no-restricted-imports -- Playwright diagnostics on the local host.
import { readFileSync } from "node:fs";
// eslint-disable-next-line no-restricted-imports -- Playwright diagnostics on the local host.
import { loadavg } from "node:os";
import type { Page } from "@playwright/test";

export function hostSample() {
  const pressure = (name: string) => {
    try {
      return readFileSync(`/proc/pressure/${name}`, "utf8").trim();
    } catch {
      return "unavailable";
    }
  };
  return {
    at: new Date().toISOString(),
    load: loadavg(),
    cpu: readFileSync("/proc/stat", "utf8").split("\n")[0],
    memory: readFileSync("/proc/meminfo", "utf8"),
    vm: readFileSync("/proc/vmstat", "utf8"),
    pressure: { cpu: pressure("cpu"), io: pressure("io"), memory: pressure("memory") },
  };
}

interface TraceEvent {
  name: string;
  cat?: string;
  ph: string;
  pid: number;
  tid: number;
  ts: number;
  dur?: number;
}

/** GC wall slices on the exporting worker, restricted to its user-timing marks. */
export async function exportTrace(page: Page) {
  const session = await page.context().newCDPSession(page);
  const events: TraceEvent[] = [];
  session.on("Tracing.dataCollected", ({ value }) =>
    events.push(...(value as unknown as TraceEvent[])),
  );
  await session.send("Tracing.start", {
    categories: "toplevel,v8,devtools.timeline,disabled-by-default-v8.gc,blink.user_timing",
    options: "record-until-full",
  });
  return async () => {
    const complete = new Promise<void>((resolve) =>
      session.once("Tracing.tracingComplete", () => resolve()),
    );
    await session.send("Tracing.end");
    await complete;
    await session.detach();
    const start = events.find((e) => e.name === "minnow-export-start");
    const end = events.find((e) => e.name === "minnow-export-end");
    if (!start || !end) throw new Error("Export timing marks missing from GC trace");
    const gc = events.filter(
      (e) =>
        e.pid === start.pid &&
        e.tid === start.tid &&
        e.ts >= start.ts &&
        e.ts < end.ts &&
        (e.name === "MajorGC" || e.name === "MinorGC"),
    );
    // The timeline emits paired B/E GC events or complete X events depending on Chromium.
    const stack: TraceEvent[] = [];
    let gcUs = 0;
    let gcCount = 0;
    for (const e of gc.sort((a, b) => a.ts - b.ts)) {
      if (e.ph === "B") stack.push(e);
      else if (e.ph === "E") {
        const before = stack.pop();
        if (before && stack.length === 0) {
          gcUs += Math.min(end.ts, e.ts) - before.ts;
          gcCount++;
        }
      } else if (e.ph === "X") {
        gcUs += Math.min(e.dur ?? 0, end.ts - e.ts);
        gcCount++;
      }
    }
    return { gcMs: gcUs / 1000, gcCount, traceMs: (end.ts - start.ts) / 1000, events };
  };
}

/** /proc process counters include all threads, including threads that have already exited. */
export interface CpuSnapshot {
  readonly at: number;
  readonly processes: Array<{ pid: number; kind: string; seconds: number; rss: number }>;
  readonly threads: Array<{ pid: number; tid: string; name: string; seconds: number }>;
}
/** Diagnostics only. Process table is restricted to this runner's Chrome descendants. */
export function allThreadCpu(
  processes: ReadonlyMap<number, { kind: string; rss: number }>,
  tasks: (pid: number) => readonly string[],
  ticks: number,
): CpuSnapshot {
  const result: CpuSnapshot = { at: Date.now(), processes: [], threads: [] };
  const cpu = (path: string) => {
    const stat = readFileSync(path, "utf8");
    const f = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
    return (Number(f[11]) + Number(f[12])) / ticks;
  };
  for (const [pid, process] of processes) {
    try {
      result.processes.push({ pid, ...process, seconds: cpu(`/proc/${String(pid)}/stat`) });
      if (process.kind !== "renderer") continue;
      for (const tid of tasks(pid)) {
        try {
          result.threads.push({
            pid,
            tid,
            name: readFileSync(`/proc/${String(pid)}/task/${tid}/comm`, "utf8").trim(),
            seconds: cpu(`/proc/${String(pid)}/task/${tid}/stat`),
          });
        } catch {
          /* A helper thread ended. Its CPU remains in the process counter. */
        }
      }
    } catch {
      /* This owned process ended between samples. */
    }
  }
  return result;
}
