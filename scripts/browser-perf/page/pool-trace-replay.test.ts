/** Frozen diagnostic chronology checks ownership/charge cleanup, not export values or speed. */
import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { expect, it } from "vitest";
import { replayPoolTrace, type PoolRequestTrace } from "./pool-trace-replay.js";

const trace = JSON.parse(
  gunzipSync(
    readFileSync(new URL("../experiments/pool-requests-prefix.json.gz", import.meta.url)),
  ).toString(),
) as PoolRequestTrace;

it("replays every observed hit with real transfers and releases all charges", async () => {
  const result = await replayPoolTrace(trace, true);
  expect(result).toMatchObject({
    requests: 49260,
    hits: 19205,
    allocations: 33565,
    accepted: 51488,
    rejected: 0,
    checksum: 6272766,
  });
});

it("rejects a wrong hit assertion, cleaning up partial output", async () => {
  const events = [...trace.events];
  expect(events[0]).toBe(0);
  events[2] = 1;
  await expect(replayPoolTrace({ ...trace, events }, true)).rejects.toThrow("Trace hit mismatch");
});
