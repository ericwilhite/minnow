/** CPU counters of this persistent Chromium profile only, including its dedicated worker threads. */
import { readFileSync, readdirSync } from "node:fs";
import { execFileSync } from "node:child_process";
import type { ReopenCpuSnapshot } from "../browser/next-storage-listed-reopen.js";

function text(path: string): string {
  try {
    return readFileSync(path, "utf8");
  } catch {
    return "";
  }
}
function entries(path: string): string[] {
  try {
    return readdirSync(path);
  } catch {
    return [];
  }
}
function ticks(path: string): number {
  const stat = text(path);
  const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
  return Number(fields[11] ?? 0) + Number(fields[12] ?? 0);
}

export function reopenCpuSampler(profile: string): () => ReopenCpuSnapshot {
  const ticksPerSecond = Number(execFileSync("getconf", ["CLK_TCK"], { encoding: "utf8" }));
  const root = entries("/proc").find((pid) => {
    // Chromium may replace argv with one process-title string, rather than NUL-separated flags.
    const command = text(`/proc/${pid}/cmdline`).replaceAll("\0", " ");
    return command.includes(`--user-data-dir=${profile} `) && !/(?:^|\s)--type=/.test(command);
  });
  if (root === undefined) throw new Error(`No Chromium process for profile ${profile}`);
  return () => {
    const processes: Record<string, number> = {};
    const workers: Record<string, number> = {};
    const pending = [root];
    const seen = new Set<string>();
    while (pending.length > 0) {
      const pid = pending.pop();
      if (pid === undefined || seen.has(pid)) continue;
      seen.add(pid);
      processes[pid] = ticks(`/proc/${pid}/stat`);
      for (const tid of entries(`/proc/${pid}/task`)) {
        pending.push(
          ...text(`/proc/${pid}/task/${tid}/children`).trim().split(/\s+/).filter(Boolean),
        );
        if (text(`/proc/${pid}/task/${tid}/comm`).trim() === "DedicatedWorker")
          workers[`${pid}:${tid}`] = ticks(`/proc/${pid}/task/${tid}/stat`);
      }
    }
    return { ticksPerSecond, processes, workers };
  };
}
