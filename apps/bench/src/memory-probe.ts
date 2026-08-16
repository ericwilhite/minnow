/**
 * Memory probes shared by the storage benchmark and the engine comparison.
 *
 * `performance.memory.usedJSHeapSize` only counts the JavaScript heap, which is the wrong
 * number for a comparison against SQLite and PGlite: those engines keep nearly all
 * of their state inside a WebAssembly memory that never appears in the JS heap.
 * `performance.measureUserAgentSpecificMemory()` does count it, so it is the primary signal
 * here and the JS heap is reported beside it as a narrower one.
 *
 * The API is Chromium-only, needs a cross-origin-isolated page (the dev and preview servers
 * both send COOP/COEP), and Chromium additionally refuses it in browser builds that run
 * without full site isolation — Electron shells and Playwright's bundled Chromium among them.
 * Every failure is reported as a status with a reason rather than as a zero.
 */

export type AgentMemoryStatus = "measured" | "unsupported" | "not-isolated" | "refused";

export interface AgentMemorySample {
  /** Total bytes attributed to this JavaScript agent, including WebAssembly memories. */
  bytes: number;
  /** Bytes per memory type exactly as the browser labels them; labels are not standardized. */
  byType: Record<string, number>;
}

export interface AgentMemoryResult {
  status: AgentMemoryStatus;
  sample: AgentMemorySample | null;
  /** Human-readable reason, shown verbatim when no sample could be taken. */
  detail: string;
}

interface MeasuredMemory {
  bytes: number;
  breakdown: Array<{ bytes: number; types: string[] }>;
}

interface MemoryCapablePerformance {
  measureUserAgentSpecificMemory?: () => Promise<MeasuredMemory>;
  memory?: { usedJSHeapSize?: number };
}

/**
 * Resolves after the browser's next garbage collection, so this can take a second or more and
 * must never sit inside a timed region.
 */
export async function measureAgentMemory(): Promise<AgentMemoryResult> {
  const api = performance as Performance & MemoryCapablePerformance;
  if (typeof api.measureUserAgentSpecificMemory !== "function") {
    return {
      status: "unsupported",
      sample: null,
      detail:
        "performance.measureUserAgentSpecificMemory() is not implemented by this browser. Only Chromium exposes it, so Firefox and Safari report the JavaScript heap alone.",
    };
  }
  if (!globalThis.crossOriginIsolated) {
    return {
      status: "not-isolated",
      sample: null,
      detail:
        "The page is not cross-origin isolated, so the measurement is refused. Serve it with Cross-Origin-Opener-Policy: same-origin and Cross-Origin-Embedder-Policy: require-corp.",
    };
  }
  try {
    const measured = await api.measureUserAgentSpecificMemory();
    const byType: Record<string, number> = {};
    for (const entry of measured.breakdown) {
      // An entry with no type still counts toward the total, so it is kept under a label
      // rather than dropped.
      const types = entry.types.length === 0 ? ["Unattributed"] : entry.types;
      for (const type of types) byType[type] = (byType[type] ?? 0) + entry.bytes;
    }
    return {
      status: "measured",
      sample: { bytes: measured.bytes, byType },
      detail: "Whole-agent memory including WebAssembly memories.",
    };
  } catch (error) {
    return {
      status: "refused",
      sample: null,
      detail: `The browser refused the measurement: ${error instanceof Error ? error.message : String(error)}. Chromium also requires full site isolation, which Electron shells and automation builds often disable.`,
    };
  }
}

/**
 * JavaScript heap only. Chromium exposes it on Window; it is absent in workers, which is why
 * every sample in this app is taken on the main thread.
 */
export function heapBytes(): number | null {
  const value = (performance as Performance & MemoryCapablePerformance).memory?.usedJSHeapSize;
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/**
 * One memory reading, shaped to cross the worker boundary as a structured clone.
 *
 * Both APIs behind it exist only on the main thread: a worker has neither
 * `performance.memory` nor, in the browsers checked here, the measurement API. The worker
 * therefore asks the window to sample on its behalf at each checkpoint. That is also the more
 * accurate place to ask, because a window's measurement covers the whole agent — the benchmark
 * worker's own heap included.
 */
export interface MemorySample {
  agentStatus: AgentMemoryStatus;
  agentDetail: string;
  agentBytes: number | null;
  agentByType: Record<string, number> | null;
  heapBytes: number | null;
}

/** Called on the main thread only. Never call inside a timed region. */
export async function sampleMemory(): Promise<MemorySample> {
  const agent = await measureAgentMemory();
  return {
    agentStatus: agent.status,
    agentDetail: agent.detail,
    agentBytes: agent.sample?.bytes ?? null,
    agentByType: agent.sample?.byType ?? null,
    heapBytes: heapBytes(),
  };
}

export const unavailableMemorySample: MemorySample = {
  agentStatus: "unsupported",
  agentDetail: "The page did not answer the memory checkpoint in time.",
  agentBytes: null,
  agentByType: null,
  heapBytes: null,
};
