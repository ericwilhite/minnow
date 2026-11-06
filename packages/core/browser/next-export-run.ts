/**
 * Page side of the OPFS export bench (browser-tests/next-export-bench.spec.ts): builds the wide
 * table once in a dedicated worker (next-export-worker.ts), keeps its group descriptions here, and
 * runs each export pass in a fresh worker, so no pass starts with another's page cache.
 */
import { StreamDictionaries, payloadBytes } from "../src/next/owner/frame.js";
import { DEFAULT_RESULT_WINDOW_BYTES, RELEASE_DELAY_MS } from "../src/next/owner/protocol.js";
import type { FramePayload } from "../src/next/exec/result.js";
import type { BuildResult, ExportPass, ExportRequest } from "./next-export-worker.js";

const DIR = "wide";
let built: BuildResult | null = null;

async function withWorker<T>(request: ExportRequest): Promise<T> {
  const url = new URL("./next-export-worker.ts", import.meta.url);
  if (request.op === "export" && request.readMiB !== undefined)
    url.searchParams.set("readMiB", String(request.readMiB));
  if (request.op === "export" && request.frameCopy !== undefined)
    url.searchParams.set("frameCopy", request.frameCopy);
  if (request.op === "export" && request.scanRange !== undefined)
    url.searchParams.set("scanRange", request.scanRange);
  const worker = new Worker(url, {
    type: "module",
  });
  const results = request.op === "export" && request.mode === "scan" ? new MessageChannel() : null;
  let releaseTimer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await new Promise<T>((resolve, reject) => {
      let receivedRows = 0,
        receivedFrames = 0,
        receivedBytes = 0,
        unreleased = 0,
        through = -1;
      const dictionaries = new StreamDictionaries();
      const release = () => {
        if (!results) return;
        if (releaseTimer !== undefined) {
          clearTimeout(releaseTimer);
          releaseTimer = undefined;
        }
        results.port1.postMessage({ t: "release", target: 1, through });
        unreleased = 0;
      };
      if (results) {
        results.port1.onmessage = (event: MessageEvent<{ t: string; frames: FramePayload[] }>) => {
          try {
            if (event.data.t !== "rows") throw new Error("Unexpected bench result message");
            for (const frame of event.data.frames) {
              if (
                request.op !== "export" ||
                frame.columns.length !== request.groups[0]?.chunks.length ||
                frame.columns.some((c) => c.length !== frame.rowCount)
              )
                throw new Error("Malformed bench frame");
              dictionaries.apply(frame);
              receivedRows += frame.rowCount;
              receivedFrames++;
              const bytes = payloadBytes(frame);
              receivedBytes += bytes;
              unreleased += bytes;
              through = frame.sequence;
            }
            if (unreleased >= DEFAULT_RESULT_WINDOW_BYTES / 2) release();
            else releaseTimer ??= setTimeout(release, RELEASE_DELAY_MS);
          } catch (error) {
            reject(error instanceof Error ? error : new Error(String(error)));
          }
        };
        results.port1.onmessageerror = () => reject(new Error("Bench frame could not be received"));
        results.port1.start();
      }
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
          if (results && request.op === "export")
            worker.postMessage({ id: 1, request: { ...request, resultPort: results.port2 } }, [
              results.port2,
            ]);
          else worker.postMessage({ id: 1, request });
          return;
        }
        const data = event.data as { result?: unknown; error?: string };
        if (data.error !== undefined) reject(new Error(data.error));
        else {
          if (results) {
            const pass = (data.result as ExportPass[]).at(-1);
            if (
              pass?.rows !== receivedRows ||
              pass.scanStats?.frames !== receivedFrames ||
              pass.scanStats.wireBytes !== receivedBytes
            ) {
              reject(new Error("Scan transport totals disagree"));
              return;
            }
          }
          resolve(data.result as T);
        }
      });
    });
  } finally {
    if (releaseTimer !== undefined) clearTimeout(releaseTimer);
    results?.port1.close();
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
): Promise<{ groups: number; generatedGroups: number; dataBytes: number; ms: number }> {
  await withWorker({ op: "remove", dir: DIR });
  built = await withWorker<BuildResult>({ op: "build", dir: DIR, rows, distinct });
  return {
    groups: built.groups.length,
    generatedGroups: built.generatedGroups,
    dataBytes: built.dataBytes,
    ms: built.ms,
  };
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
  readonly groupDepth?: 1 | 2;
  readonly mode?: "pages" | "scan";
  readonly frameCopy?: "baseline" | "candidate";
  readonly scanRange?: "baseline" | "candidate";
  readonly scanIO?: "batch" | "window";
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
    ...(options.mode === undefined ? {} : { mode: options.mode }),
    ...(options.frameCopy === undefined ? {} : { frameCopy: options.frameCopy }),
    ...(options.scanRange === undefined ? {} : { scanRange: options.scanRange }),
    ...(options.scanIO === undefined ? {} : { scanIO: options.scanIO }),
    ...(options.groupDepth === undefined ? {} : { groupDepth: options.groupDepth }),
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
