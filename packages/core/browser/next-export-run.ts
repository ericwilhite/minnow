/**
 * Page side of the OPFS export bench (browser-tests/next-export-bench.spec.ts): builds the wide
 * table once in a dedicated worker (next-export-worker.ts), keeps its group descriptions here, and
 * runs each export pass in a fresh worker, so no pass starts with another's page cache.
 */
import type { BuildResult, ExportPass, ExportRequest } from "./next-export-worker.js";

const DIR = "wide";
let built: BuildResult | null = null;

async function withWorker<T>(request: ExportRequest): Promise<T> {
  const url = new URL("./next-export-worker.ts", import.meta.url);
  if (request.op === "export" && request.readMiB !== undefined)
    url.searchParams.set("readMiB", String(request.readMiB));
  const worker = new Worker(url, {
    type: "module",
  });
  try {
    return await new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error("export worker did not start"));
      }, 20_000);
      worker.addEventListener("error", (event) => {
        clearTimeout(timer);
        reject(new Error(`export worker error: ${event.message}`));
      });
      worker.addEventListener("message", (event: MessageEvent<unknown>) => {
        if (event.data === "next-export-ready") {
          clearTimeout(timer);
          worker.postMessage({ id: 1, request });
          return;
        }
        const data = event.data as { result?: unknown; error?: string };
        if (data.error !== undefined) reject(new Error(data.error));
        else resolve(data.result as T);
      });
    });
  } finally {
    worker.terminate();
  }
}

/**
 * Writes `rows` rows × 200 columns into OPFS (replacing any earlier table): `distinct` groups
 * generated, the rest byte copies of them at new addresses.
 */
export async function build(
  rows: number,
  distinct: number,
): Promise<{ groups: number; dataBytes: number; ms: number }> {
  await withWorker({ op: "remove", dir: DIR });
  built = await withWorker<BuildResult>({ op: "build", dir: DIR, rows, distinct });
  return { groups: built.groups.length, dataBytes: built.dataBytes, ms: built.ms };
}

/** One export of the first `groups` groups (all by default) in a fresh worker. */
export async function exportPass(options: {
  readonly once: boolean;
  readonly readaheadKiB: number;
  readonly windowMiB?: number;
  readonly capMiB: number;
  readonly passes: number;
  readonly groups?: number;
  readonly profile?: boolean;
  readonly profilePhases?: boolean;
  readonly auditWorkingSet?: boolean;
  /** Bench-only comparison: both loop modules are imported before timing. */
  readonly baselineStream?: boolean;
  readonly readMiB?: 4 | 8;
}): Promise<ExportPass[]> {
  if (built === null) throw new Error("build the table first");
  const groups = built.groups.slice(0, options.groups ?? built.groups.length);
  const tail = built.groups.at(-1);
  // The measured hot set comes from a group the export does not read, when there is one.
  const outside = tail !== undefined && !groups.includes(tail) ? tail : groups.at(-1);
  if (outside === undefined) throw new Error("no groups");
  return withWorker({
    op: "export",
    dir: DIR,
    groups,
    outside,
    once: options.once,
    readaheadKiB: options.readaheadKiB,
    ...(options.windowMiB === undefined ? {} : { windowMiB: options.windowMiB }),
    capMiB: options.capMiB,
    passes: options.passes,
    ...(options.readMiB === undefined ? {} : { readMiB: options.readMiB }),
    ...(options.profile === true ? { profile: true } : {}),
    ...(options.profilePhases === true ? { profilePhases: true } : {}),
    ...(options.auditWorkingSet === false ? { auditWorkingSet: false } : {}),
    ...(options.baselineStream === undefined ? {} : { baselineStream: options.baselineStream }),
  });
}

export async function remove(): Promise<void> {
  built = null;
  await withWorker({ op: "remove", dir: DIR });
}
