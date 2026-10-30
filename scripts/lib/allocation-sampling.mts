import type { Session } from "node:inspector/promises";

/** Bytes allocated in one isolate over a window, garbage included, and where they came from. */
export interface AllocationReport {
  readonly allocatedBytes: number;
  readonly top: ReadonlyArray<{ readonly site: string; readonly bytes: number }>;
}

interface SamplingNode {
  readonly callFrame: { readonly functionName: string; readonly url: string };
  readonly selfSize: number;
  readonly children: readonly SamplingNode[];
}

/**
 * Starts V8's sampling heap profiler, keeping objects that a later GC collects: a burst of
 * short-lived garbage is exactly what this measures, and a retained-heap snapshot would miss it.
 * The returned function stops sampling and totals the window by allocating function.
 */
export async function startAllocationSampling(
  session: Session,
): Promise<() => Promise<AllocationReport>> {
  await session.post("HeapProfiler.enable");
  await session.post("HeapProfiler.collectGarbage");
  await session.post("HeapProfiler.startSampling", {
    samplingInterval: 4096,
    includeObjectsCollectedByMajorGC: true,
    includeObjectsCollectedByMinorGC: true,
  });
  return async () => {
    const { profile } = (await session.post("HeapProfiler.stopSampling")) as unknown as {
      profile: { head: SamplingNode };
    };
    const bySite = new Map<string, number>();
    let allocatedBytes = 0;
    const visit = (node: SamplingNode): void => {
      if (node.selfSize > 0) {
        const file = node.callFrame.url.split("/").pop() ?? "";
        const site = `${node.callFrame.functionName || "(anonymous)"} ${file}`.trim();
        bySite.set(site, (bySite.get(site) ?? 0) + node.selfSize);
        allocatedBytes += node.selfSize;
      }
      for (const child of node.children) visit(child);
    };
    visit(profile.head);
    const top = [...bySite]
      .sort((left, right) => right[1] - left[1])
      .slice(0, 12)
      .map(([site, bytes]) => ({ site, bytes }));
    return { allocatedBytes, top };
  };
}
