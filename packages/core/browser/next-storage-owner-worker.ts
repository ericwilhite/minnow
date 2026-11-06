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
}
const barriers = new Map<number, () => void>();
let next = 0;
self.onmessage = (event: MessageEvent<OwnerBenchRequest | { ack: number }>) => {
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
    const owner = await startMeasuredOwner(request.store, request.db, phase, () =>
      self.postMessage({ ready: true }),
    );
    try {
      const count = await firstStatisticsCount(
        owner,
        request.dataset === "S5" ? "order_items" : "events_wide",
      );
      return {
        phases,
        root: owner.rootInfo,
        count,
        preparedAtReady: 0,
        maintenanceAfterCount: { ...owner.storage.path.maintenance },
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
