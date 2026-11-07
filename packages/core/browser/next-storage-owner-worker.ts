import { beginTask } from "../src/next/task.js";
/** Fresh worker per load or reopen; dataset generation is never part of the measured open. */
import { buildOwnerDataset, type OwnerDataset } from "./next-storage-owner-load.js";
import {
  startMeasuredOwner,
  firstStatisticsCount,
  type OwnerReopenStore,
  type OwnerPhase,
} from "./next-storage-owner-reopen.js";
export interface OwnerBenchRequest {
  op: "build" | "open";
  store: OwnerReopenStore;
  db: string;
  dataset: OwnerDataset;
  cpu: boolean;
  testRows?: number;
  loadGovernorBytes?: number;
  resume?: boolean;
  traceReads?: boolean;
  deferInitialMaintenance?: boolean;
}
const barriers = new Map<number, () => void>();
let next = 0;
let readyReceived: (() => void) | undefined;
self.onmessage = (
  event: MessageEvent<OwnerBenchRequest | { ack: number } | { readyAck: true }>,
) => {
  beginTask();
  if ("readyAck" in event.data) {
    readyReceived?.();
    return;
  }
  if ("ack" in event.data) {
    barriers.get(event.data.ack)?.();
    barriers.delete(event.data.ack);
    return;
  }
  const request = event.data;
  void (async () => {
    if (
      (globalThis as unknown as { __MINNOW_VERIFY_CACHED_BYTES__?: boolean })
        .__MINNOW_VERIFY_CACHED_BYTES__ === true
    )
      throw new Error("Owner-ready timing must run without the test-only cached-byte rehash guard");
    if (request.op === "build")
      return buildOwnerDataset(
        request.store,
        request.db,
        request.dataset,
        (message) => self.postMessage({ progress: message }),
        request.testRows,
        request.loadGovernorBytes,
        request.resume,
      );
    const phases: OwnerPhase[] = [];
    const phase = (value: OwnerPhase) => {
      phases.push(value);
      if (request.cpu) {
        const id = ++next;
        const done = new Promise<void>((resolve) => barriers.set(id, resolve));
        self.postMessage({ phase: value, id });
        return done;
      }
    };
    const readyDelivered = new Promise<void>((resolve) => {
      readyReceived = resolve;
    });
    const owner = await startMeasuredOwner(
      request.store,
      request.db,
      phase,
      () => self.postMessage({ ready: true }),
      { traceReads: request.traceReads, deferInitialMaintenance: request.deferInitialMaintenance },
    );
    try {
      if (request.deferInitialMaintenance !== false) {
        await readyDelivered;
        owner.storage.path.startInitialMaintenance();
      }
      const count = await firstStatisticsCount(
        owner,
        request.dataset === "S5" ? "order_items" : "events_wide",
      );
      return {
        phases,
        readTrace: owner.storage.readTrace,
        root: owner.rootInfo,
        count,
        preparedAtReady: 0,
        maintenanceAfterCount: { ...owner.storage.path.maintenance },
        maintenanceStateAfterCount: owner.storage.path.maintenanceState,
      };
    } finally {
      await owner.close();
    }
  })().then(
    (result) => self.postMessage({ result }),
    (error: unknown) =>
      self.postMessage({
        error: error instanceof Error ? (error.stack ?? error.message) : String(error),
      }),
  );
};
self.postMessage({ boot: true });
