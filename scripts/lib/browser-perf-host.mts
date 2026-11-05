/**
 * Host facts the browser performance lane depends on: where its browser profile lives, that the
 * profile is on a real disk, and the renderer's memory and CPU read from `/proc`.
 *
 * OPFS lives inside the browser profile. A profile on tmpfs or ramfs puts every "durable" write
 * in memory, which makes write cells meaningless and read cells optimistic, so the lane refuses
 * to start there. (An ephemeral Playwright context has the same effect, which is why the lane only
 * ever uses `launchPersistentContext`.)
 */
import { execFileSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import path from "node:path";

export const LANE_DIR_ENV = "MINNOW_BROWSER_PERF_DIR";

/** `$MINNOW_BROWSER_PERF_DIR`, else `~/.cache/minnow-browser-perf`. Never under /tmp by default. */
export function laneDirectory(env: NodeJS.ProcessEnv = process.env): string {
  const configured = env[LANE_DIR_ENV];
  if (configured !== undefined && configured.length > 0) return path.resolve(configured);
  return path.join(homedir(), ".cache", "minnow-browser-perf");
}

const MEMORY_FILESYSTEMS = new Set(["tmpfs", "ramfs", "devtmpfs", "rootfs"]);

/** `/proc/mounts` escapes spaces, tabs, newlines and backslashes as three-digit octal. */
function unescapeMountField(field: string): string {
  return field.replace(/\\([0-7]{3})/g, (_, octal: string) =>
    String.fromCharCode(parseInt(octal, 8)),
  );
}

export interface MountEntry {
  readonly mountPoint: string;
  readonly type: string;
}

export function parseMounts(text: string): MountEntry[] {
  const entries: MountEntry[] = [];
  for (const line of text.split("\n")) {
    const fields = line.trim().split(/\s+/);
    if (fields.length < 3) continue;
    entries.push({ mountPoint: unescapeMountField(fields[1] ?? ""), type: fields[2] ?? "" });
  }
  return entries;
}

/** The filesystem type of the mount that holds `target` (an absolute, resolved path). */
export function filesystemTypeFor(
  target: string,
  mounts: readonly MountEntry[],
): string | undefined {
  let best: MountEntry | undefined;
  for (const entry of mounts) {
    const point = entry.mountPoint;
    const inside =
      point === "/" ||
      target === point ||
      target.startsWith(point.endsWith("/") ? point : `${point}/`);
    // Later entries shadow earlier ones on the same mount point (over-mounts).
    if (inside && (best === undefined || point.length >= best.mountPoint.length)) best = entry;
  }
  return best?.type;
}

export function isMemoryFilesystem(type: string | undefined): boolean {
  return type !== undefined && MEMORY_FILESYSTEMS.has(type);
}

/** The nearest existing ancestor, so a directory not yet created is judged by where it will be. */
function existingAncestor(target: string): string {
  let candidate = path.resolve(target);
  while (!existsSync(candidate)) {
    const parent = path.dirname(candidate);
    if (parent === candidate) break;
    candidate = parent;
  }
  return realpathSync(candidate);
}

export interface DiskCheck {
  readonly filesystem: string;
  readonly checked: boolean;
}

/**
 * Throws when `directory` is on a memory filesystem. On Linux this reads `/proc/mounts`; on macOS
 * it asks `stat -f`. Elsewhere the check is reported as not performed rather than passed.
 */
export function assertOnRealDisk(directory: string): DiskCheck {
  const resolved = existingAncestor(directory);
  let type: string | undefined;
  if (process.platform === "linux" && existsSync("/proc/mounts")) {
    type = filesystemTypeFor(resolved, parseMounts(readFileSync("/proc/mounts", "utf8")));
  } else if (process.platform === "darwin") {
    type = execFileSync("stat", ["-f", "%T", resolved], { encoding: "utf8" }).trim();
  } else {
    return { filesystem: "unknown", checked: false };
  }
  if (isMemoryFilesystem(type)) {
    throw new Error(
      `${directory} is on ${String(type)}: OPFS in a browser profile there lives in memory, so ` +
        `durable writes and cold reads would not touch a disk. Set ${LANE_DIR_ENV} to a directory on a real disk.`,
    );
  }
  return { filesystem: type ?? "unknown", checked: type !== undefined };
}

// ------------------------------------------------------------------------------------------------
// /proc sampling (Linux only)

export const PROC_AVAILABLE = process.platform === "linux" && existsSync("/proc/self/status");

function readProc(pid: number, file: string): string | undefined {
  try {
    return readFileSync(`/proc/${String(pid)}/${file}`, "utf8");
  } catch {
    return undefined;
  }
}

/** Fields after the parenthesised command name, which may itself contain spaces. */
function statFields(pid: number): string[] | undefined {
  const stat = readProc(pid, "stat");
  if (stat === undefined) return undefined;
  return stat.slice(stat.lastIndexOf(")") + 2).split(" ");
}

let clockTicksPerSecond: number | undefined;
function clockTicks(): number {
  if (clockTicksPerSecond === undefined) {
    try {
      clockTicksPerSecond =
        Number(execFileSync("getconf", ["CLK_TCK"], { encoding: "utf8" }).trim()) || 100;
    } catch {
      clockTicksPerSecond = 100;
    }
  }
  return clockTicksPerSecond;
}

/** User plus system CPU time of every thread of the process, in milliseconds. */
export function processCpuMs(pid: number): number | undefined {
  const fields = statFields(pid);
  if (fields === undefined) return undefined;
  // utime and stime are fields 14 and 15 of /proc/<pid>/stat; here indices 11 and 12.
  return ((Number(fields[11]) + Number(fields[12])) * 1000) / clockTicks();
}

export interface MemoryStatus {
  readonly rssMB: number;
  readonly peakMB: number;
}

export function memoryStatus(pid: number): MemoryStatus | undefined {
  const status = readProc(pid, "status");
  if (status === undefined) return undefined;
  const field = (key: string): number =>
    Number(new RegExp(`${key}:\\s+(\\d+)`).exec(status)?.[1] ?? 0) / 1024;
  return { rssMB: field("VmRSS"), peakMB: field("VmHWM") };
}

/** Resets VmHWM to the current RSS (`clear_refs` 5), so the next peak belongs to one cell. */
export function resetPeakMemory(pid: number): boolean {
  try {
    writeFileSync(`/proc/${String(pid)}/clear_refs`, "5");
    return true;
  } catch {
    return false;
  }
}

function parentOf(pid: number): number {
  return Number(statFields(pid)?.[1] ?? -1);
}

function commandLine(pid: number): string {
  return (readProc(pid, "cmdline") ?? "").replaceAll("\0", " ");
}

/** Renderer processes of the browser launched with this profile directory. */
export function rendererProcesses(profileDirectory: string): number[] {
  if (!PROC_AVAILABLE) return [];
  const pids = readdirSync("/proc")
    .filter((entry) => /^\d+$/.test(entry))
    .map(Number);
  const root = pids.find((pid) => {
    const command = commandLine(pid);
    return command.includes(`--user-data-dir=${profileDirectory}`) && !command.includes("--type=");
  });
  if (root === undefined) return [];
  const children = new Map<number, number[]>();
  for (const pid of pids) {
    const parent = parentOf(pid);
    const list = children.get(parent) ?? [];
    list.push(pid);
    children.set(parent, list);
  }
  const tree: number[] = [];
  const stack = [root];
  for (let pid = stack.pop(); pid !== undefined; pid = stack.pop()) {
    tree.push(pid);
    stack.push(...(children.get(pid) ?? []));
  }
  return tree.filter((pid) => commandLine(pid).includes("--type=renderer"));
}

/** Bytes under a directory, as `du` reports them on disk (`-b` is GNU; BSD du counts KiB). */
export function diskBytes(directory: string): number {
  if (!existsSync(directory)) return 0;
  const run = (flags: string, scale: number): number =>
    Number(execFileSync("du", [flags, directory], { encoding: "utf8" }).split("\t")[0] ?? 0) *
    scale;
  try {
    return run("-sb", 1);
  } catch {
    try {
      return run("-sk", 1024);
    } catch {
      return 0;
    }
  }
}
