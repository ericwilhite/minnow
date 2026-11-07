/**
 * Page side of the OPFS export bench (browser-tests/next-export-bench.spec.ts): builds the wide
 * table once in a dedicated worker (next-export-worker.ts), keeps its group descriptions here, and
 * runs ordinary passes in fresh workers. The explicit RSS-series mode reuses one worker/space/pool
 * across queries so retained memory and GC lag cannot be hidden by worker termination.
 */
import { StreamDictionaries, payloadBytes } from "../src/next/owner/frame.js";
import { DEFAULT_RESULT_WINDOW_BYTES, RELEASE_DELAY_MS } from "../src/next/owner/protocol.js";
import type { FramePayload } from "../src/next/exec/result.js";
import type { BuildResult, ExportPass, ExportRequest } from "./next-export-worker.js";

import { recyclingFrame } from "../src/next/owner/recycling-frame.js";
import { DiscardReceiver } from "../../../scripts/browser-perf/page/discard-receiver.js";

const DIR = "wide";
let built: BuildResult | null = null;
let seriesWorker: Worker | null = null;
let nextRequest = 1;

async function withWorker<T>(request: ExportRequest, discard = false): Promise<T> {
  const url = new URL("./next-export-worker.ts", import.meta.url);
  if (request.op === "export" && request.readMiB !== undefined)
    url.searchParams.set("readMiB", String(request.readMiB));
  if (request.op === "export" && request.frameCopy !== undefined)
    url.searchParams.set("frameCopy", request.frameCopy);
  if (request.op === "export" && request.scanRange !== undefined)
    url.searchParams.set("scanRange", request.scanRange);
  if (request.op === "export" && request.scanShape !== undefined)
    url.searchParams.set("scanShape", request.scanShape);
  const persistent =
    request.op === "series-open" || request.op === "series-pass" || request.op === "series-close";
  if (request.op === "series-open" && seriesWorker) throw new Error("Export series already open");
  const reused = persistent && seriesWorker !== null;
  const worker = reused && seriesWorker ? seriesWorker : new Worker(url, { type: "module" });
  const id = nextRequest++;
  const job =
    request.op === "export" ? request : request.op === "series-pass" ? request.request : null;
  const results =
    job && (job.mode === "scan" || job.mode === "scan-adapter") ? new MessageChannel() : null;
  let onMessage: ((event: MessageEvent<unknown>) => void) | undefined;
  let onError: ((event: ErrorEvent) => void) | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let succeeded = false;
  const receiver = discard ? new DiscardReceiver() : null;
  let releaseTimer: ReturnType<typeof setTimeout> | undefined;
  try {
    const answer = await new Promise<T>((resolve, reject) => {
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
        results.port1.onmessage = (
          event: MessageEvent<{ t: string; frames: FramePayload[]; bufferIds?: number[] }>,
        ) => {
          try {
            if (event.data.t !== "rows") throw new Error("Unexpected bench result message");
            for (const frame of event.data.frames) {
              if (
                !job ||
                frame.columns.length !== job.groups[0]?.chunks.length ||
                frame.columns.some((c) => c.length !== frame.rowCount)
              )
                throw new Error("Malformed bench frame");
              dictionaries.apply(
                event.data.bufferIds
                  ? {
                      ...frame,
                      dictionaries: frame.dictionaries.map((delta) => {
                        const d = delta.values.data;
                        if (d.layout !== "varlen")
                          throw new Error("Recycling export expects text dictionaries");
                        return {
                          ...delta,
                          values: {
                            ...delta.values,
                            data: { ...d, bytes: d.bytes.slice(), offsets: d.offsets.slice() },
                            validity: delta.values.validity?.slice() ?? null,
                          },
                        };
                      }),
                    }
                  : frame,
              );
              receivedRows += frame.rowCount;
              receivedFrames++;
              const bytes = payloadBytes(frame);
              receivedBytes += bytes;
              unreleased += bytes;
              through = frame.sequence;
              receiver?.consume(frame);
              if (event.data.bufferIds)
                recyclingFrame(frame, event.data.bufferIds, (buffers, transfer) =>
                  results.port1.postMessage(
                    { t: "return", target: 1, through: frame.sequence, buffers },
                    transfer,
                  ),
                ).release();
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
      timer = setTimeout(() => {
        reject(new Error("export worker did not start"));
      }, 20_000);
      const send = () => {
        if (timer !== undefined) clearTimeout(timer);
        if (results && job) {
          const posted = { ...job, resultPort: results.port2 };
          worker.postMessage(
            {
              id,
              request:
                request.op === "series-pass" ? { op: "series-pass", request: posted } : posted,
            },
            [results.port2],
          );
        } else worker.postMessage({ id, request });
      };
      onError = (event) => {
        if (timer !== undefined) clearTimeout(timer);
        reject(new Error(`export worker error: ${event.message}`));
      };
      worker.addEventListener("error", onError);
      onMessage = (event: MessageEvent<unknown>) => {
        if (event.data === "next-export-ready") {
          if (!reused) send();
          return;
        }
        const data = event.data as { id?: number; result?: unknown; error?: string };
        if (data.id !== id) return;
        if (data.error !== undefined) reject(new Error(data.error));
        else {
          if (results) {
            const pass = (data.result as ExportPass[]).at(-1);
            if (
              pass?.rows !== receivedRows ||
              (pass.executorStats ?? pass.scanStats)?.frames !== receivedFrames ||
              (pass.executorStats ?? pass.scanStats)?.wireBytes !== receivedBytes
            ) {
              reject(new Error("Scan transport totals disagree"));
              return;
            }
          }
          resolve(data.result as T);
        }
      };
      worker.addEventListener("message", onMessage);
      if (reused) send();
    });
    succeeded = true;
    if (request.op === "series-open") seriesWorker = worker;
    return answer;
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    if (releaseTimer !== undefined) clearTimeout(releaseTimer);
    if (onMessage) worker.removeEventListener("message", onMessage);
    if (onError) worker.removeEventListener("error", onError);
    results?.port1.close();
    receiver?.close();
    if (!persistent || request.op === "series-close" || !succeeded) {
      worker.terminate();
      if (seriesWorker === worker) seriesWorker = null;
    }
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
  readonly persistent?: boolean;
  /** Bench consumer relinquishes all column views after consuming each frame. */
  readonly discard?: boolean;
  readonly recycle?: boolean;
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
  readonly mode?: "pages" | "scan" | "scan-core" | "scan-adapter";
  readonly frameCopy?: "baseline" | "candidate";
  readonly scanRange?: "baseline" | "candidate";
  readonly scanShape?: "baseline" | "candidate";
  readonly scanTargetMiB?: 1 | 2 | 3;
  readonly executorVariant?: "baseline" | "candidate";
  readonly executorReference?: string;
  readonly scanIO?: "batch" | "window";
  readonly scanOutput?: "borrowed" | "flat" | "dictionary";
}): Promise<ExportPass[]> {
  if (built === null) throw new Error("build the table first");
  if (options.recycle && options.discard) throw new Error("Choose recycling or discard disposal");
  if (options.recycle && !options.persistent)
    throw new Error("Return-pool experiment requires a persistent series");
  const groups = built.groups.slice(0, options.groups ?? built.groups.length);
  const tail = built.groups.at(-1);
  // The measured hot set comes from a group the export does not read, when there is one.
  const outside = tail !== undefined && !groups.includes(tail) ? tail : groups.at(-1);
  if (outside === undefined) throw new Error("no groups");
  const request: Extract<ExportRequest, { op: "export" }> = {
    op: "export",
    dir: DIR,
    groups,
    outside,
    once: options.once,
    ...(options.recycle ? { recycle: true } : {}),
    readaheadKiB: options.readaheadKiB,
    ...(options.windowMiB === undefined ? {} : { windowMiB: options.windowMiB }),
    capMiB: options.capMiB,
    passes: options.passes,
    ...(options.mode === undefined ? {} : { mode: options.mode }),
    ...(options.frameCopy === undefined ? {} : { frameCopy: options.frameCopy }),
    ...(options.scanRange === undefined ? {} : { scanRange: options.scanRange }),
    ...(options.scanShape === undefined ? {} : { scanShape: options.scanShape }),
    ...(options.scanTargetMiB === undefined ? {} : { scanTargetMiB: options.scanTargetMiB }),
    ...(options.executorVariant === undefined ? {} : { executorVariant: options.executorVariant }),
    ...(options.executorReference === undefined
      ? {}
      : { executorReference: options.executorReference }),
    ...(options.scanIO === undefined ? {} : { scanIO: options.scanIO }),
    ...(options.scanOutput === undefined ? {} : { scanOutput: options.scanOutput }),
    ...(options.groupDepth === undefined ? {} : { groupDepth: options.groupDepth }),
    ...(options.readMiB === undefined ? {} : { readMiB: options.readMiB }),
    ...(options.profile === true ? { profile: true } : {}),
    ...(options.profilePhases === true ? { profilePhases: true } : {}),
    ...(options.auditWorkingSet === false ? { auditWorkingSet: false } : {}),
    ...(options.baselineStream === undefined ? {} : { baselineStream: options.baselineStream }),
  };
  return withWorker(options.persistent ? { op: "series-pass", request } : request, options.discard);
}

export async function openSeries(
  groups: number,
  capMiB: number,
  executorReference?: string,
): Promise<unknown> {
  if (built === null) throw new Error("Build first");
  const selected = built.groups.slice(0, groups),
    outside = built.groups.at(-1);
  if (!outside) throw new Error("No group");
  return withWorker({
    op: "series-open",
    request: {
      op: "export",
      dir: DIR,
      groups: selected,
      outside,
      capMiB,
      once: true,
      readaheadKiB: 256,
      mode: "scan",
      passes: 1,
      auditWorkingSet: false,
      ...(executorReference ? { executorReference } : {}),
    },
  });
}
export async function closeSeries(): Promise<unknown> {
  return withWorker({ op: "series-close" });
}

export async function remove(): Promise<void> {
  built = null;
  await withWorker({ op: "remove", dir: DIR });
}
