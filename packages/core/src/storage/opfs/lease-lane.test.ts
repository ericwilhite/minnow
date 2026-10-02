/**
 * The lease lane. A reader's lease — create, renew, move, release — is logged like any other
 * mutation, but it is a tiny frame, and it must not wait out a large commit's prepare and
 * encode or a sliced checkpoint. These tests hold such a step part-way and prove that a lease
 * still completes; that it lands in the log before the commit it raced, under its own request
 * identity; that recovery keeps it at every crash point, including power loss and torn writes
 * inside a checkpoint that kept its log; that collection honours it; and that a lease whose
 * frame is refused poisons nothing it should not: the commit it raced reloads and publishes.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { MinnowDatabase } from "../../engine/database.js";
import { heavyTestTimeout } from "../../engine/storage-test-helpers.js";
import { encodeBlock } from "../../block-format/index.js";
import { RecordCore } from "../toolkit/record-core.js";
import { iterateWalFrames } from "../toolkit/wal.js";
import { decodeRecordJson, decodeSyncCheckpoint, encodeRecordJson } from "../toolkit/wire.js";
import type { SyncFileHandle } from "../toolkit/sync-file.js";
import type { LeaseRecord, TableRecord } from "../types.js";
import { PowerLossModel } from "./power-loss-model.js";
import { renumberEncodedFrame } from "./leader.js";
import { OpfsBlockStore } from "./index.js";

vi.setConfig({ testTimeout: heavyTestTimeout(60_000) });

afterEach(() => {
  vi.restoreAllMocks();
});

const OWNER = "lane-reader";

function leaseRecord(id: string, manifestVersion: number | null): LeaseRecord {
  const now = Date.now();
  return {
    id,
    kind: "reader",
    manifestVersion,
    ownerId: OWNER,
    createdAt: new Date(now).toISOString(),
    expiresAt: new Date(now + 10 * 60_000).toISOString(),
    revision: 0,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(condition: () => boolean | Promise<boolean>, what: string): Promise<void> {
  for (let attempt = 0; attempt < 1_000; attempt += 1) {
    if (await condition()) return;
    await sleep(5);
  }
  throw new Error(`Timed out waiting for ${what}`);
}

interface WalFrame {
  seq: number;
  op: string;
  key?: string;
  record?: { id: string };
  request?: { key: string; method: string };
}

/** Every complete frame in a WAL file's bytes. */
function walFrames(bytes: Uint8Array | undefined): WalFrame[] {
  const content = bytes ?? new Uint8Array();
  const handle: SyncFileHandle = {
    getSize: () => content.byteLength,
    read: (buffer, { at }) => {
      const chunk = content.subarray(at, at + buffer.byteLength);
      buffer.set(chunk);
      return chunk.byteLength;
    },
    write: () => 0,
    truncate: () => undefined,
    flush: () => undefined,
    close: () => undefined,
  };
  return [...iterateWalFrames(handle)].map((frame) => frame.payload as WalFrame);
}

function frameFor(frames: WalFrame[], leaseId: string): WalFrame {
  const frame = frames.find((entry) => entry.op === "createLease" && entry.record?.id === leaseId);
  if (frame === undefined) throw new Error(`No createLease frame for ${leaseId}`);
  return frame;
}

function lastCommitFrame(frames: WalFrame[]): WalFrame {
  const frame = frames.filter((entry) => entry.op === "writeTransaction").at(-1);
  if (frame === undefined) throw new Error("No writeTransaction frame");
  return frame;
}

function rows(start: number, count: number): Array<{ id: number; label: string }> {
  return Array.from({ length: count }, (_, index) => ({
    id: start + index,
    label: `row-${String(start + index)}`,
  }));
}

/** A keyed table with `count` rows: a commit of 16,384 keys or more prepares in slices. */
async function keyedDatabase(store: OpfsBlockStore, count: number): Promise<MinnowDatabase> {
  const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  await db.createTable({
    name: "t",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
    ],
  });
  await db.insertBatch("t", rows(0, count));
  return db;
}

async function rowCount(db: MinnowDatabase): Promise<number> {
  const result = await db.query("SELECT COUNT(*) AS n FROM t", { memoize: false });
  return Number((result.rows[0] as { n: number | bigint }).n);
}

interface HeldOutcome<T> {
  value?: T;
  error?: unknown;
  /** Whether `action` settled while the commit's prepare was held at its first pause. */
  settledWhileHeld: boolean;
  ms: number;
}

/** Work for the next large commit's prepare: at its first pause, and once it has returned. */
const nextPrepare: { during: Array<() => Promise<void>>; after: Array<() => void> } = {
  during: [],
  after: [],
};
let prepareSpied = false;

function spyOnPrepare(): void {
  if (prepareSpied) return;
  prepareSpied = true;
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Called with the mock receiver below.
  const original = RecordCore.prototype.prepareCommit;
  vi.spyOn(RecordCore.prototype, "prepareCommit").mockImplementation(async function (
    this: RecordCore,
    input,
    pause,
  ) {
    const during = nextPrepare.during.splice(0);
    const after = nextPrepare.after.splice(0);
    let first = true;
    await original.call(this, input, async () => {
      if (first) {
        first = false;
        for (const hook of during) await hook();
      }
      await pause();
    });
    for (const hook of after) hook();
  });
}

afterEach(() => {
  prepareSpied = false;
  nextPrepare.during.length = 0;
  nextPrepare.after.length = 0;
});

/**
 * Holds the next large commit's prepare at its first pause and runs `action` there. The
 * prepare does not continue until `action` settles (or three seconds pass), so an action that
 * settles at all proves it did not wait for the commit. With `hang`, the prepare never
 * continues: what a crashed tab's commit does after the crash.
 */
function duringNextPrepare<T>(
  action: () => Promise<T>,
  options: { hang?: boolean } = {},
): Promise<HeldOutcome<T>> {
  spyOnPrepare();
  return new Promise((resolveOutcome) => {
    nextPrepare.during.push(async () => {
      const started = performance.now();
      const settled = action().then(
        (value) => ({ value }),
        (error: unknown) => ({ error }),
      );
      const winner = await Promise.race([settled, sleep(3_000).then(() => undefined)]);
      resolveOutcome({
        ...(winner ?? {}),
        settledWhileHeld: winner !== undefined,
        ms: performance.now() - started,
      });
      if (options.hang === true) await new Promise<never>(() => undefined);
    });
  });
}

/**
 * Runs `action` on the first macrotask after the next large commit's prepare returns: after
 * the commit has captured its frame's sequence number and while it encodes or waits to apply.
 */
function afterNextPrepare<T>(action: () => Promise<T>): Promise<{ value?: T; error?: unknown }> {
  spyOnPrepare();
  return new Promise((resolveOutcome) => {
    nextPrepare.after.push(() => {
      setImmediate(() => {
        void action().then(
          (value) => {
            resolveOutcome({ value });
          },
          (error: unknown) => {
            resolveOutcome({ error });
          },
        );
      });
    });
  });
}

/** Throws from the next write to `path` only, then stands down. */
function refuseNextWrite(shim: MemoryOpfs, path: string): { refused: () => boolean } {
  let refused = false;
  shim.setWriteFault((target, phase) => {
    if (!refused && target === path && phase === "write") {
      refused = true;
      throw new DOMException("injected WAL refusal", "QuotaExceededError");
    }
  });
  return { refused: () => refused };
}

describe("lease lane during a large commit", () => {
  it("completes a lease while a 100k-row commit prepares; collection keeps its version", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "pin", root: shim.root });
    const db = await keyedDatabase(store, 100_000);
    const pinned = await store.getCurrentManifestVersion();
    const lease = leaseRecord("pinning-lease", pinned);
    const held = duringNextPrepare(() => store.createLease(lease));
    await db.insertBatch("t", rows(100_000, 100_000));
    const outcome = await held;
    expect(outcome.error).toBeUndefined();
    expect(outcome.settledWhileHeld).toBe(true);
    expect(outcome.ms).toBeLessThan(50);
    expect(await rowCount(db)).toBe(200_000);
    expect(await store.getCurrentManifestVersion()).toBeGreaterThan(pinned ?? -1);

    // The lease logged before the commit pins its version through the collection after it.
    await db.collectGarbage({ retainRecentVersions: 0 });
    expect(await store.getManifest(pinned ?? 0)).toBeDefined();
    expect(await store.removeLease({ id: lease.id, ownerId: OWNER })).toBe(true);
    await db.collectGarbage({ retainRecentVersions: 0 });
    expect(await store.getManifest(pinned ?? 0)).toBeUndefined();
    await db.close();
    store.close();
  });

  it("runs a lease while a staged commit of 20k keys prepares and encodes", async () => {
    const shim = new MemoryOpfs();
    const name = "staged-commit";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    const createdAt = new Date().toISOString();
    const later = new Date(Date.now() + 60 * 60_000).toISOString();
    await store.addTable({
      id: "keyed",
      name: "keyed",
      columns: [{ id: "col-id", name: "id", type: "number", nullable: false }],
      managed: false,
      revision: 0,
      createdAt,
      uniqueKeyColumnId: "col-id",
    });
    const transactionId = "staged-transaction";
    await store.createTransaction({
      id: transactionId,
      ownerId: "staged-owner",
      expiresAt: later,
      snapshotVersion: null,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "active",
      revision: 0,
      startedAt: createdAt,
      updatedAt: createdAt,
      committedVersion: null,
    });
    const staged = await store.stageTransactionArtifacts({
      transactionId,
      expectedRevision: 0,
      blocks: [{ id: "staged-block", bytes: Uint8Array.of(1, 2, 3, 4) }],
      segments: [
        {
          id: "staged-segment",
          tableId: "keyed",
          transactionId,
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { "col-id": ["staged-block"] },
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          createdAt,
        },
      ],
      updatedAt: createdAt,
    });
    const keyTokens = Array.from({ length: 20_000 }, (_, index) => `number:${String(index)}`);
    const prepared = leaseRecord("prepared-lease", null);
    const encoded = leaseRecord("encoded-lease", null);
    const held = duringNextPrepare(() => store.createLease(prepared));
    const raced = afterNextPrepare(() => store.createLease(encoded));
    const summary = await store.commitTransaction({
      transactionId,
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "keyed", limit: 4096 }],
      uniqueKeyChanges: [{ tableId: "keyed", keyTokens, requireAbsent: true }],
      committedAt: later,
    });
    expect((await held).settledWhileHeld).toBe(true);
    expect((await raced).error).toBeUndefined();

    const frames = walFrames(shim.readFileBytes(`minnowdb/${name}/wal`));
    const commit = frames.find((frame) => frame.op === "commitTransaction");
    expect(frameFor(frames, prepared.id).seq).toBe((commit?.seq ?? 0) - 2);
    expect(frameFor(frames, encoded.id).seq).toBe((commit?.seq ?? 0) - 1);
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect(await reopened.getCurrentManifestVersion()).toBe(summary.version);
    expect(await reopened.getExistingUniqueKeys("keyed", ["number:0", "number:19999"])).toEqual([
      "number:0",
      "number:19999",
    ]);
    expect((await reopened.listLeases()).map((lease) => lease.id)).toEqual([
      encoded.id,
      prepared.id,
    ]);
    reopened.close();
  });

  it("logs the lease before the commit it raced, and recovers both", async () => {
    const shim = new MemoryOpfs();
    const name = "order";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    const db = await keyedDatabase(store, 20_000);
    const pinned = await store.getCurrentManifestVersion();
    const lease = leaseRecord("ordered-lease", pinned);
    const held = duringNextPrepare(() => store.createLease(lease));
    await db.insertBatch("t", rows(20_000, 20_000));
    expect((await held).settledWhileHeld).toBe(true);

    const frames = walFrames(shim.readFileBytes(`minnowdb/${name}/wal`));
    expect(frameFor(frames, lease.id).seq).toBe(lastCommitFrame(frames).seq - 1);
    store._crashForTests();
    await db.close();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    const again = new MinnowDatabase(reopened, { autoCompact: false, autoCollect: false });
    expect(await reopened.getLease(lease.id)).toMatchObject({ manifestVersion: pinned });
    expect(await rowCount(again)).toBe(40_000);
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    await again.close();
    reopened.close();
  });

  it("keeps a lease logged before a power loss that took the commit it raced", async () => {
    const shim = new MemoryOpfs();
    const model = new PowerLossModel(shim);
    const name = "power-between";
    const store = await OpfsBlockStore.open({ name, root: shim.root, durability: "strict" });
    const db = await keyedDatabase(store, 20_000);
    const pinned = await store.getCurrentManifestVersion();
    const lease = leaseRecord("surviving-lease", pinned);
    const held = duringNextPrepare(
      async () => {
        await store.createLease(lease);
        // Power fails with the lease acknowledged and the commit still preparing.
        store._crashForTests();
        model.powerLoss();
      },
      { hang: true },
    );
    void db.insertBatch("t", rows(20_000, 20_000)).catch(() => undefined);
    expect((await held).settledWhileHeld).toBe(true);

    const frames = walFrames(shim.readFileBytes(`minnowdb/${name}/wal`));
    expect(frames.at(-1)).toMatchObject({ op: "createLease", record: { id: lease.id } });
    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    const again = new MinnowDatabase(reopened, { autoCompact: false, autoCollect: false });
    expect(await reopened.getLease(lease.id)).toMatchObject({ manifestVersion: pinned });
    expect(await rowCount(again)).toBe(20_000);
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    // The dead tab's commit still holds its write turn in this process; write at the store.
    await reopened.createLease(leaseRecord("after-recovery", pinned));
    expect(await reopened.getLease("after-recovery")).toBeDefined();
    await again.close();
    reopened.close();
  });

  it("renumbers a commit frame that lease frames overtook while it encoded", async () => {
    const shim = new MemoryOpfs();
    const name = "renumber";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    const db = await keyedDatabase(store, 20_000);
    const pinned = await store.getCurrentManifestVersion();
    const lease = leaseRecord("overtaking-lease", pinned);
    const raced = afterNextPrepare(() => store.createLease(lease));
    await db.insertBatch("t", rows(20_000, 20_000));
    expect((await raced).error).toBeUndefined();

    const frames = walFrames(shim.readFileBytes(`minnowdb/${name}/wal`));
    const seqs = frames.map((frame) => frame.seq);
    expect(seqs).toEqual(seqs.map((_, index) => (seqs[0] ?? 0) + index));
    expect(frameFor(frames, lease.id).seq).toBe(lastCommitFrame(frames).seq - 1);
    store._crashForTests();
    await db.close();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    const again = new MinnowDatabase(reopened, { autoCompact: false, autoCollect: false });
    expect(await reopened.getLease(lease.id)).toBeDefined();
    expect(await rowCount(again)).toBe(40_000);
    await again.close();
    reopened.close();
  });

  it("renumbers an encoded frame in place, or by one copy when the sequence gains a digit", () => {
    const body = { op: "writeTransaction", input: { note: "x".repeat(64) }, blocks: [] };
    const request = { key: "follower:request", method: "writeTransaction" };
    for (const [from, to] of [
      [41, 47],
      [99, 100],
      [999_999, 1_000_003],
    ] as const) {
      const encoded = {
        seq: from,
        request,
        bytes: encodeRecordJson({ seq: from, ...body, request }),
      };
      const original = encoded.bytes;
      expect(renumberEncodedFrame(encoded, to)).toBe(true);
      expect(encoded.seq).toBe(to);
      expect(encoded.bytes).toEqual(encodeRecordJson({ seq: to, ...body, request }));
      expect(encoded.bytes === original).toBe(String(from).length === String(to).length);
      expect(decodeRecordJson(encoded.bytes)).toEqual({ seq: to, ...body, request });
    }
    const foreign = { seq: 5, request: undefined, bytes: encodeRecordJson({ op: "x", seq: 5 }) };
    expect(renumberEncodedFrame(foreign, 6)).toBe(false);
  });

  it("serves a follower's lease during the leader's commit under its own identity", async () => {
    const shim = new MemoryOpfs();
    const name = "served";
    const leader = await OpfsBlockStore.open({ name, root: shim.root });
    const leaderDb = await keyedDatabase(leader, 20_000);
    const follower = await OpfsBlockStore.open({ name, root: shim.root });
    expect(follower._isLeaderForTests()).toBe(false);
    const followerDb = new MinnowDatabase(follower, { autoCompact: false, autoCollect: false });
    const pinned = await leader.getCurrentManifestVersion();
    const servedLease = leaseRecord("served-lease", pinned);
    const localLease = leaseRecord("local-lease", pinned);
    const held = duringNextPrepare(() =>
      Promise.all([follower.createLease(servedLease), leader.createLease(localLease)]),
    );
    // The follower's commit is itself served: its frame must keep its own request identity.
    await followerDb.insertBatch("t", rows(20_000, 20_000));
    const outcome = await held;
    expect(outcome.error).toBeUndefined();
    expect(outcome.settledWhileHeld).toBe(true);

    const frames = walFrames(shim.readFileBytes(`minnowdb/${name}/wal`));
    const served = frameFor(frames, servedLease.id);
    const local = frameFor(frames, localLease.id);
    const commit = lastCommitFrame(frames);
    expect(served.request?.method).toBe("createLease");
    expect(local.request).toBeUndefined();
    expect(commit.request?.method).toBe("writeTransaction");
    expect(commit.request?.key).not.toBe(served.request?.key);
    // Each served frame's result follows it in the same synchronous step or completion.
    const servedResult = frames.find((frame) => frame.seq === served.seq + 1);
    expect(servedResult).toMatchObject({ op: "servedResult", key: served.request?.key });
    expect(
      frames.some((frame) => frame.op === "servedResult" && frame.key === commit.request?.key),
    ).toBe(true);
    expect(Math.max(served.seq, local.seq)).toBeLessThan(commit.seq);
    expect(await follower.getLease(servedLease.id)).toBeDefined();
    expect(await rowCount(followerDb)).toBe(40_000);

    await followerDb.close();
    follower.close();
    leader._crashForTests();
    await leaderDb.close();
    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect((await reopened.listLeases()).map((lease) => lease.id).sort()).toEqual(
      expect.arrayContaining([servedLease.id, localLease.id]),
    );
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    reopened.close();
  });

  it("reloads and publishes a commit whose prepare a refused lease poisoned", async () => {
    const shim = new MemoryOpfs();
    const name = "poison-prepare";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    const db = await keyedDatabase(store, 20_000);
    const lease = leaseRecord("refused-lease", await store.getCurrentManifestVersion());
    const held = duringNextPrepare(() => {
      const fault = refuseNextWrite(shim, `minnowdb/${name}/wal`);
      const call = store.createLease(lease);
      expect(fault.refused()).toBe(true);
      return call;
    });
    await db.insertBatch("t", rows(20_000, 20_000));
    const outcome = await held;
    shim.setWriteFault(null);
    expect(outcome.settledWhileHeld).toBe(true);
    expect(outcome.error).toMatchObject({ name: "QuotaExceededError" });
    expect(await store.getLease(lease.id)).toBeUndefined();
    expect(await rowCount(db)).toBe(40_000);
    expect((await store.checkIntegrity({ mode: "full" })).ok).toBe(true);
    store._crashForTests();
    await db.close();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    const again = new MinnowDatabase(reopened, { autoCompact: false, autoCollect: false });
    expect(await reopened.getLease(lease.id)).toBeUndefined();
    expect(await rowCount(again)).toBe(40_000);
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    await again.close();
    reopened.close();
  });

  it("drops the blocks of a write whose frame encode a refused lease poisoned, then retries", async () => {
    const shim = new MemoryOpfs();
    const name = "poison-encode";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    const db = await keyedDatabase(store, 20_000);
    const lease = leaseRecord("refused-lease", await store.getCurrentManifestVersion());
    let refused = false;
    const raced = afterNextPrepare(() => {
      const fault = refuseNextWrite(shim, `minnowdb/${name}/wal`);
      const call = store.createLease(lease);
      refused = fault.refused();
      shim.setWriteFault(null);
      return call;
    });
    await db.insertBatch("t", rows(20_000, 20_000));
    expect(refused).toBe(true);
    expect((await raced).error).toMatchObject({ name: "QuotaExceededError" });
    expect(await store.getLease(lease.id)).toBeUndefined();
    expect(await rowCount(db)).toBe(40_000);
    expect((await store.checkIntegrity({ mode: "full" })).ok).toBe(true);
    store._crashForTests();
    await db.close();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    const again = new MinnowDatabase(reopened, { autoCompact: false, autoCollect: false });
    expect(await rowCount(again)).toBe(40_000);
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    await again.close();
    reopened.close();
  });
});

/** Wide records, so a few hundred tables make a checkpoint several write slices long. */
const TABLES = 240;
const COLUMNS = 96;

function wideTable(index: number): TableRecord {
  return {
    id: `table-${String(index)}`,
    name: `wide_${String(index)}`,
    columns: Array.from({ length: COLUMNS }, (_, column) => ({
      id: `c${String(column)}`,
      name: `measurement_column_with_a_long_descriptive_name_${String(column)}`,
      type: "number" as const,
      nullable: true,
    })),
    managed: false,
    revision: 0,
    createdAt: "2026-10-01T00:00:00.000Z",
  };
}

/** A store one write short of its scheduled checkpoint, which then runs as its own step. */
async function storeBeforeCheckpoint(
  shim: MemoryOpfs,
  name: string,
  durability: "strict" | "relaxed" = "strict",
): Promise<OpfsBlockStore> {
  const store = await OpfsBlockStore.open({
    name,
    root: shim.root,
    durability,
    checkpointEntries: TABLES + 1,
  });
  await waitFor(
    () => (shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength ?? -1) === 0,
    "the initial checkpoint",
  );
  for (let index = 0; index < TABLES; index += 1) await store.addTable(wideTable(index));
  return store;
}

function slotLastSeq(shim: MemoryOpfs, path: string): number {
  return (decodeSyncCheckpoint(shim.readFileBytes(path) ?? new Uint8Array()) as { lastSeq: number })
    .lastSeq;
}

describe("lease lane during a sliced checkpoint", () => {
  it("runs leases between the checkpoint's slices and keeps the log they landed in", async () => {
    const shim = new MemoryOpfs();
    const name = "checkpoint-lane";
    const prefix = `minnowdb/${name}/`;
    const store = await storeBeforeCheckpoint(shim, name);
    const version = await store.getCurrentManifestVersion();
    let checkpointOps = 0;
    let slotFlushes = 0;
    const settledAtOp: number[] = [];
    const leases: Array<Promise<void>> = [];
    shim.setWriteFault((path, phase) => {
      if (!path.startsWith(`${prefix}checkpoint-`)) return;
      checkpointOps += 1;
      if (phase === "flush") slotFlushes += 1;
      if (leases.length < 3) {
        const id = `checkpoint-lease-${String(leases.length)}`;
        // Queued, not called here: a lease arrives between slices, never inside a write.
        let resolveLease!: () => void;
        leases.push(new Promise((resolve) => (resolveLease = resolve)));
        queueMicrotask(() => {
          void store.createLease(leaseRecord(id, version)).then(() => {
            settledAtOp.push(checkpointOps);
            resolveLease();
          });
        });
      }
    });
    await store.addTable(wideTable(TABLES));
    await waitFor(() => slotFlushes >= 2, "both checkpoint slots");
    await Promise.all(leases);
    shim.setWriteFault(null);
    expect(settledAtOp).toHaveLength(3);
    // Every lease finished while the checkpoint still had slices left to write.
    expect(Math.max(...settledAtOp)).toBeLessThan(checkpointOps);

    // Their frames follow the checkpoint's capture, so the log was kept, not reset.
    const frames = walFrames(shim.readFileBytes(`${prefix}wal`));
    const captured = slotLastSeq(shim, `${prefix}checkpoint-a`);
    expect(slotLastSeq(shim, `${prefix}checkpoint-b`)).toBe(captured);
    const leaseFrames = frames.filter((frame) => frame.op === "createLease");
    expect(leaseFrames).toHaveLength(3);
    expect(leaseFrames.every((frame) => frame.seq > captured)).toBe(true);
    expect(frames.some((frame) => frame.seq <= captured)).toBe(true);
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect((await reopened.listLeases()).map((lease) => lease.id)).toEqual([
      "checkpoint-lease-0",
      "checkpoint-lease-1",
      "checkpoint-lease-2",
    ]);
    expect((await reopened.listTables()).length).toBe(TABLES + 1);
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    reopened.close();
  });

  it("closes the lane for the next checkpoint while covered frames remain, and resets", async () => {
    const shim = new MemoryOpfs();
    const name = "checkpoint-bound";
    const prefix = `minnowdb/${name}/`;
    const store = await storeBeforeCheckpoint(shim, name);
    const version = await store.getCurrentManifestVersion();
    let armed = true;
    let slotFlushes = 0;
    let leases = 0;
    let leaseSettledAtFlushes = -1;
    shim.setWriteFault((path, phase) => {
      if (!path.startsWith(`${prefix}checkpoint-`)) return;
      if (phase === "flush") slotFlushes += 1;
      if (!armed) return;
      armed = false;
      leases += 1;
      const id = `bound-${String(leases)}`;
      queueMicrotask(() => {
        void store.createLease(leaseRecord(id, version)).then(() => {
          leaseSettledAtFlushes = slotFlushes;
        });
      });
    });
    await store.addTable(wideTable(TABLES));
    await waitFor(() => slotFlushes >= 2 && leaseSettledAtFlushes >= 0, "the first checkpoint");
    expect(leaseSettledAtFlushes).toBeLessThan(2);
    expect(shim.readFileBytes(`${prefix}wal`)?.byteLength ?? 0).toBeGreaterThan(0);

    // The next due checkpoint starts while the log still holds covered frames: it keeps the
    // lane closed, so the lease asked for during it waits, and the log is reset.
    leaseSettledAtFlushes = -1;
    for (let index = 1; index <= TABLES + 1; index += 1) {
      if (index === TABLES + 1) armed = true;
      await store.addTable(wideTable(TABLES + index));
    }
    await waitFor(() => slotFlushes >= 4 && leaseSettledAtFlushes >= 0, "the second checkpoint");
    shim.setWriteFault(null);
    expect(leaseSettledAtFlushes).toBe(4);
    const frames = walFrames(shim.readFileBytes(`${prefix}wal`));
    const captured = slotLastSeq(shim, `${prefix}checkpoint-a`);
    expect(frames.length).toBeGreaterThan(0);
    expect(frames.every((frame) => frame.seq > captured)).toBe(true);
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect((await reopened.listLeases()).length).toBe(2);
    expect((await reopened.listTables()).length).toBe(2 * TABLES + 2);
    reopened.close();
  });

  it("resets a kept log once quiet, and deletes the drained extents that waited for it", async () => {
    const shim = new MemoryOpfs();
    const name = "covered-debt";
    const prefix = `minnowdb/${name}/`;
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    let lease: Promise<unknown> | undefined;
    let armed = false;
    let version: number | null = null;
    shim.setWriteFault((path) => {
      if (!armed || !path.startsWith(`${prefix}checkpoint-`)) return;
      armed = false;
      queueMicrotask(() => {
        lease = store.createLease(leaseRecord("debt-lease", version));
      });
    });
    // Extent 0 holds only the retired block; the survivor rolls into extent 1.
    const retired = await encodeBlock(
      { type: "string", values: ["x".repeat(6 * 1024 * 1024)] },
      "raw",
    );
    const survivor = await encodeBlock(
      { type: "string", values: ["y".repeat(3 * 1024 * 1024)] },
      "raw",
    );
    await retireBlockThroughGc(
      store,
      [
        { id: "retired", bytes: retired },
        { id: "survivor", bytes: survivor },
      ],
      async () => {
        version = await store.getCurrentManifestVersion();
        armed = true;
      },
    );
    await lease;
    shim.setWriteFault(null);
    expect(lease).toBeDefined();
    // The lease landed in the deletion's checkpoint, which therefore kept the log: the drained
    // extent stays as cleanup debt rather than outliving the history that names it.
    expect(shim.readFileBytes(`${prefix}extents/000000`)).toBeDefined();
    expect(shim.readFileBytes(`${prefix}wal`)?.byteLength ?? 0).toBeGreaterThan(0);
    expect((await store.getStorageStats()).orphanBytes).toBeGreaterThan(6 * 1024 * 1024);

    // Once the log is quiet, a checkpoint resets it and the debt is paid.
    await waitFor(
      () =>
        shim.readFileBytes(`${prefix}extents/000000`) === undefined &&
        shim.readFileBytes(`${prefix}wal`)?.byteLength === 0,
      "the quiet reset",
    );
    expect((await store.getStorageStats()).orphanBytes).toBe(0);
    expect(await store.getBlock("survivor")).toEqual(survivor);
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect(await reopened.getLease("debt-lease")).toBeDefined();
    expect(await reopened.getBlock("retired")).toBeUndefined();
    expect(await reopened.getBlock("survivor")).toEqual(survivor);
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    reopened.close();
  });

  it("finishes a checkpoint whose lease was refused, and reloads before the next write", async () => {
    const shim = new MemoryOpfs();
    const name = "checkpoint-poison";
    const prefix = `minnowdb/${name}/`;
    const store = await storeBeforeCheckpoint(shim, name);
    const version = await store.getCurrentManifestVersion();
    let lease: Promise<unknown> | undefined;
    let refuseWal = false;
    let walRefused = false;
    let scheduled = false;
    let slotFlushes = 0;
    shim.setWriteFault((path, phase) => {
      if (path === `${prefix}wal` && refuseWal && phase === "write") {
        refuseWal = false;
        walRefused = true;
        throw new DOMException("injected WAL refusal", "QuotaExceededError");
      }
      if (!path.startsWith(`${prefix}checkpoint-`)) return;
      if (phase === "flush") slotFlushes += 1;
      if (scheduled) return;
      scheduled = true;
      queueMicrotask(() => {
        refuseWal = true;
        lease = store.createLease(leaseRecord("refused-lease", version));
        refuseWal = false;
        lease.catch(() => undefined);
      });
    });
    await store.addTable(wideTable(TABLES));
    await waitFor(() => slotFlushes >= 2 && lease !== undefined, "the checkpoint");
    await expect(lease).rejects.toMatchObject({ name: "QuotaExceededError" });
    shim.setWriteFault(null);
    expect(walRefused).toBe(true);
    await store.addTable(wideTable(TABLES + 1));
    expect(await store.getLease("refused-lease")).toBeUndefined();
    expect((await store.listTables()).length).toBe(TABLES + 2);
    expect((await store.checkIntegrity({ mode: "full" })).ok).toBe(true);
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect((await reopened.listTables()).length).toBe(TABLES + 2);
    expect(await reopened.getLease("refused-lease")).toBeUndefined();
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    reopened.close();
  });

  it("survives power loss and torn writes at every file operation of a checkpoint that logged leases", async () => {
    const operations = await countLaneCheckpointOperations();
    expect(operations).toBeGreaterThanOrEqual(20);
    // One boundary past the last operation: power fails only after the checkpoint completed.
    for (let boundary = 1; boundary <= operations + 1; boundary += 1) {
      const shim = new MemoryOpfs();
      const name = `lane-power-${String(boundary)}`;
      const prefix = `minnowdb/${name}/`;
      let armed = false;
      let seen = 0;
      let checkpointOps = 0;
      let stopped = false;
      const acknowledged: string[] = [];
      const opened: { store?: OpfsBlockStore } = {};
      let version: number | null = null;
      const model = new PowerLossModel(shim, (path) => {
        if (!armed || !path.startsWith(prefix)) return;
        const checkpointPath = path.includes("/checkpoint-");
        if (seen === 0 && !checkpointPath) return;
        seen += 1;
        if (checkpointPath) {
          checkpointOps += 1;
          if (checkpointOps <= LANE_LEASES) scheduleLease(`power-lease-${String(checkpointOps)}`);
        }
        // Power is gone from this operation on: nothing after it reaches the disk either.
        if (seen >= boundary) {
          stopped = true;
          throw new Error(`power lost before operation ${String(boundary)}`);
        }
      });
      const scheduleLease = (id: string): void => {
        queueMicrotask(() => {
          opened.store?.createLease(leaseRecord(id, version)).then(
            () => acknowledged.push(id),
            () => undefined,
          );
        });
      };
      const store = await storeBeforeCheckpoint(shim, name);
      opened.store = store;
      version = await store.getCurrentManifestVersion();
      armed = true;
      await store.addTable(wideTable(TABLES));
      await waitFor(
        () => stopped || (checkpointOps >= 4 && acknowledged.length === LANE_LEASES),
        `operation ${String(boundary)}`,
      );
      armed = false;
      store._crashForTests();
      // A torn write: half of whatever was appended since the last flush survives.
      model.powerLoss((unflushed) => Math.floor(unflushed / 2));
      shim.setWriteFault(null);

      const reopened = await OpfsBlockStore.open({ name, root: shim.root });
      const leases = (await reopened.listLeases()).map((lease) => lease.id);
      // Strict durability acknowledged the table before the checkpoint, and every lease that
      // resolved; a lease cut off by the power loss may or may not have reached the disk.
      expect(leases, `leases at boundary ${String(boundary)}`).toEqual(
        expect.arrayContaining(acknowledged),
      );
      expect((await reopened.listTables()).length).toBe(TABLES + 1);
      expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
      await reopened.addTable(wideTable(TABLES + 1));
      await reopened.createLease(leaseRecord("after-recovery", version));
      reopened._crashForTests();
      const again = await OpfsBlockStore.open({ name, root: shim.root });
      expect((await again.listTables()).length).toBe(TABLES + 2);
      expect(await again.getLease("after-recovery")).toBeDefined();
      expect((await again.listLeases()).map((lease) => lease.id)).toEqual(
        expect.arrayContaining(acknowledged),
      );
      again.close();
    }
  });
});

const LANE_LEASES = 3;

/**
 * File operations — checkpoint slot writes and flushes, plus the lease frames and their
 * acknowledgements logged between slices — from the checkpoint's first slot write on.
 */
async function countLaneCheckpointOperations(): Promise<number> {
  const shim = new MemoryOpfs();
  const name = "lane-count";
  const prefix = `minnowdb/${name}/`;
  const store = await storeBeforeCheckpoint(shim, name);
  const version = await store.getCurrentManifestVersion();
  let operations = 0;
  let checkpointOps = 0;
  let acknowledged = 0;
  shim.setWriteFault((path) => {
    if (!path.startsWith(prefix)) return;
    const checkpointPath = path.includes("/checkpoint-");
    if (operations === 0 && !checkpointPath) return;
    operations += 1;
    if (!checkpointPath) return;
    checkpointOps += 1;
    if (checkpointOps <= LANE_LEASES) {
      const id = `count-lease-${String(checkpointOps)}`;
      queueMicrotask(() => {
        void store.createLease(leaseRecord(id, version)).then(() => (acknowledged += 1));
      });
    }
  });
  await store.addTable(wideTable(TABLES));
  await waitFor(() => acknowledged === LANE_LEASES, "the counted leases");
  await sleep(50);
  shim.setWriteFault(null);
  store.close();
  return operations;
}

/** Commits `blocks`, all live, then retires the first through a completed collection step. */
async function retireBlockThroughGc(
  store: OpfsBlockStore,
  blocks: ReadonlyArray<{ id: string; bytes: Uint8Array }>,
  beforeCollect?: () => Promise<void>,
): Promise<void> {
  const [retired, ...kept] = blocks.map((block) => block.id);
  if (retired === undefined) throw new Error("Nothing to retire");
  const column = { id: "c1", name: "id", type: "number" as const, nullable: false };
  const base = {
    columns: [column],
    managed: false,
    revision: 0,
    createdAt: new Date().toISOString(),
  };
  await store.addTable({ ...base, id: "retired-table", name: "retired" });
  await store.addTable({ ...base, id: "kept-table", name: "kept" });
  const at = (offsetMs: number): string => new Date(Date.now() + offsetMs).toISOString();
  const transactionId = "retiring-transaction";
  await store.createTransaction({
    id: transactionId,
    ownerId: "retiring-owner",
    expiresAt: at(60 * 60_000),
    snapshotVersion: null,
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: at(0),
    updatedAt: at(0),
    committedVersion: null,
  });
  const segment = (id: string, tableId: string, blockIds: string[], commitOrdinal: number) => ({
    id,
    kind: "insert" as const,
    level: 0,
    logicalOrder: 0,
    commitOrdinal,
    rowIdSpans: [],
    tableId,
    transactionId,
    rowCount: 1,
    rowIdStart: 1n,
    rowIdEndExclusive: 2n,
    columnBlockIds: { c1: blockIds },
    createdAt: at(0),
  });
  const staged = await store.stageTransactionArtifacts({
    transactionId,
    expectedRevision: 0,
    blocks,
    segments: [
      segment("retired-segment", "retired-table", [retired], 0),
      segment("kept-segment", "kept-table", kept, 1),
    ],
    updatedAt: at(1),
  });
  const first = await store.commitTransaction({
    transactionId,
    expectedTransactionRevision: staged.revision,
    expectedManifestVersion: null,
    levelZeroSegmentLimits: [
      { tableId: "retired-table", limit: 4096 },
      { tableId: "kept-table", limit: 4096 },
    ],
    committedAt: at(2),
  });
  await store.dropTable({
    tableId: "retired-table",
    expectedTableRevision: 0,
    expectedManifestVersion: first.version,
    expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
    committedAt: at(3),
  });
  const job = await store.createGarbageCollectionJob({
    id: "retiring-job",
    candidateManifestVersions: [first.version],
    candidateSegmentIds: [],
    candidateBlockIds: [retired],
    leaseCutoff: at(4),
    createdAt: at(4),
  });
  await beforeCollect?.();
  await store.runGarbageCollectionStep({
    jobId: job.id,
    expectedRevision: job.revision,
    maxItems: 10,
    updatedAt: at(5),
  });
}
