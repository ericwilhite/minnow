import { OpfsBlockStore } from "@minnowdb/core/storage/opfs";
import { serializeError } from "@minnowdb/core/worker-protocol";
import { OpfsLeader } from "../dist/storage/opfs/leader.js";

type Mode = "prefix" | "replay";
interface Request {
  name: string;
  mode: Mode;
  phase: "prepare" | "verify";
}

async function run({ name, mode, phase }: Request): Promise<unknown> {
  let leader: OpfsLeader | undefined;
  const recover = OpfsLeader.recover.bind(OpfsLeader);
  OpfsLeader.recover = async (...args: Parameters<typeof OpfsLeader.recover>) => {
    leader = await recover(...args);
    return leader;
  };
  const store = await OpfsBlockStore.open({ name, checkpointEntries: 1_000_000 });
  const versions = async () =>
    (await store.listManifestPage(null, 100)).records.map((record) => record.version);
  if (phase === "verify") {
    try {
      return {
        versions: await versions(),
        current: (await store.getCurrentManifest())?.version,
        integrity: (await store.checkIntegrity({ mode: "full" })).ok,
      };
    } finally {
      store.close();
    }
  }
  const start = Date.now();
  const time = (offset: number) => new Date(start + offset).toISOString();
  for (let version = 0; version <= 7; version++) {
    const transactionId = `transaction-${String(version)}`;
    await store.beginTransaction({
      record: {
        id: transactionId,
        ownerId: "owner",
        revision: 0,
        status: "active",
        startedAt: time(0),
        updatedAt: time(0),
        expiresAt: time(60_000),
        pendingBlockIds: [],
        pendingSegmentIds: [],
        committedVersion: null,
      },
    });
    const committed = await store.commitTransaction({
      transactionId,
      expectedTransactionRevision: 0,
      expectedManifestVersion: version === 0 ? null : version - 1,
      committedAt: time(version),
    });
    if (committed.version !== version) throw new Error("Unexpected committed manifest version");
  }
  const prune = async (id: string, candidates: number[]) => {
    const job = await store.createGarbageCollectionJob({
      id,
      candidateManifestVersions: candidates,
      candidateSegmentIds: [],
      candidateBlockIds: [],
      createdAt: time(10),
      leaseCutoff: time(10),
    });
    return store.runGarbageCollectionStep({
      jobId: id,
      expectedRevision: job.revision,
      maxItems: 100,
      updatedAt: time(11),
    });
  };
  await prune(
    "later-history",
    [1, 2, 3, 4, 5, 6].filter((version) => mode !== "replay" || version !== 3),
  );
  const firstRemoval = await store.removePrunedManifestRecords(2);
  if (leader === undefined) throw new Error("Expected the native worker to own the OPFS leader");
  // Force the real checkpoint at the boundary where the old implementation retained an
  // in-memory cleanup cursor that its durable state did not contain.
  leader.checkpointNow();
  if ((await store.getStorageStats()).walBytes !== 0)
    throw new Error("Expected an empty checkpoint WAL");
  await prune("oldest-history", [0]);
  const secondRemoval = await store.removePrunedManifestRecords(2);
  const before = await versions();
  // One case verifies every intermediate checkpoint is chain-valid; the other keeps this
  // exact operation in the WAL to prove replay selects the same records as the live call.
  if (mode === "prefix") leader.checkpointNow();
  return {
    firstRemoval,
    secondRemoval,
    versions: before,
    walBytes: (await store.getStorageStats()).walBytes,
  };
}

self.addEventListener("message", (event: MessageEvent<Request>) => {
  void run(event.data).then(
    (result) => self.postMessage({ result }),
    (error: unknown) => self.postMessage({ error: serializeError(error) }),
  );
});
