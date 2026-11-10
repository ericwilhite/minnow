/** Diagnostic only; the quiet runner never mixes this overhead into its nine timing pairs. */
import { Session } from "node:inspector";
import { PerformanceObserver, performance } from "node:perf_hooks";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
export async function pointProfile(directory: string) {
  mkdirSync(directory, { recursive: true });
  const session = new Session();
  session.connect();
  const post = (method: string, params: object = {}) =>
    new Promise<object>((resolve, reject) => {
      session.post(method, params, (error, result) =>
        error ? reject(error) : resolve(result ?? {}),
      );
    });
  const events: Array<{ startTime: number; duration: number; detail: unknown }> = [];
  const observer = new PerformanceObserver((list) => {
    for (const event of list.getEntries())
      events.push({
        startTime: event.startTime,
        duration: event.duration,
        detail: "detail" in event ? event.detail : null,
      });
  });
  observer.observe({ entryTypes: ["gc"] });
  await post("Profiler.enable");
  await post("Profiler.setSamplingInterval", { interval: 100 });
  await post("Profiler.start");
  const start = performance.now();
  return async () => {
    const end = performance.now();
    const profile = await post("Profiler.stop");
    await new Promise<void>((resolve) => setImmediate(resolve));
    observer.disconnect();
    session.disconnect();
    writeFileSync(
      join(directory, "point.cpuprofile"),
      JSON.stringify((profile as { profile: unknown }).profile),
    );
    writeFileSync(
      join(directory, "gc.json"),
      JSON.stringify(
        {
          scope: "point loop only; GC entries are wall duration, not all-thread GC CPU",
          start,
          end,
          events: events.filter((e) => e.startTime >= start && e.startTime < end),
        },
        null,
        2,
      ),
    );
  };
}
