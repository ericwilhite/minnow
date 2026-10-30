/**
 * The worker half of `scripts/live-wide-bench.mts`: a stock Minnow worker host on one port, and
 * this isolate's allocation profiler on another. Run by Node directly (type stripping), so it
 * imports the built packages, exactly as an application's worker would.
 */
import { Session } from "node:inspector/promises";
import { parentPort, workerData } from "node:worker_threads";
import type { MessagePort } from "node:worker_threads";
import { attachDatabaseWorker } from "@minnowdb/core/worker-host";
import { startAllocationSampling } from "./allocation-sampling.mts";

const { database, control } = workerData as { database: MessagePort; control: MessagePort };
attachDatabaseWorker(database as unknown as Parameters<typeof attachDatabaseWorker>[0]);
database.start();

const session = new Session();
session.connect();
let stop: (() => Promise<unknown>) | undefined;
control.on("message", (command: "start" | "stop") => {
  void (async () => {
    if (command === "start") {
      stop = await startAllocationSampling(session);
      control.postMessage({ started: true });
    } else {
      const report = await stop?.();
      stop = undefined;
      control.postMessage(report);
    }
  })();
});
parentPort?.postMessage("ready");
