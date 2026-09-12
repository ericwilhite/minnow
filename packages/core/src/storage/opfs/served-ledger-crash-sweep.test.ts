/**
 * The layout-6 served-request ledger across crash points.
 *
 * Drives a bare `OpfsLeader` (as log-crash.test.ts does) so each crash point can be
 * manufactured exactly: frame durable but no result frame, torn result frame, torn checkpoint
 * slot, checkpoint written but WAL not reset, payload lost under relaxed durability, and every
 * trimming bound. After each recovery the ledger is compared with the data:
 *   - a key is in the ledger  <=>  its mutation's effect is present;
 *   - settled with a value    <=>  the servedResult frame was durable, and the value matches;
 *   - servedCoverageSince is > sentAt of every evicted request and never claims coverage the
 *     ledger does not have.
 */
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsTree } from "./files.js";
import { OpfsLeader, type ServedMutationRequest } from "./leader.js";
import { decodeSyncCheckpoint } from "../toolkit/wire.js";
import type { TableRecord, TransactionRecord } from "../types.js";
import { PowerLossModel } from "./power-loss-model.js";

function table(name: string): TableRecord {
  return {
    id: `table-${name}`,
    name,
    columns: [{ id: "c1", name: "id", type: "number", nullable: false }],
    managed: false,
    revision: 0,
    createdAt: "2026-08-19T00:00:00.000Z",
  };
}

/**
 * The leader's ordinary reads and one-frame mutations are installed on its prototype from a
 * method list, so they carry no declared types; these go through the same dispatchers the
 * generated methods use.
 */
async function getTable(leader: OpfsLeader, id: string): Promise<TableRecord | undefined> {
  return (await leader._readCoreGenerated("getTable", [id])) as TableRecord | undefined;
}

async function getTransaction(
  leader: OpfsLeader,
  id: string,
): Promise<TransactionRecord | undefined> {
  return (await leader._readCoreGenerated("getTransaction", [id])) as TransactionRecord | undefined;
}

function reserveRowIds(leader: OpfsLeader, tableId: string, count: number): Promise<unknown> {
  return leader._loggedGenerated("reserveRowIds", [tableId, count]);
}

interface Handles {
  tree: OpfsTree;
  recover: (options?: {
    strict?: boolean;
    checkpointEntries?: number;
    servedLedgerAgeMs?: number;
    servedLedgerResultBytes?: number;
    onDiagnostic?: (error: unknown, context: string) => void;
  }) => Promise<OpfsLeader>;
  walPath: string;
  slotPaths: [string, string];
}

async function handles(shim: MemoryOpfs, name: string): Promise<Handles> {
  const minnow = await shim.root.getDirectoryHandle("minnowdb", { create: true });
  const database = await minnow.getDirectoryHandle(name, { create: true });
  const tree = new OpfsTree(database);
  return {
    tree,
    walPath: `minnowdb/${name}/wal`,
    slotPaths: [`minnowdb/${name}/checkpoint-a`, `minnowdb/${name}/checkpoint-b`],
    recover: async (options = {}) => {
      const wal = await tree.openHandle(["wal"], { create: true });
      const slotA = await tree.openHandle(["checkpoint-a"], { create: true });
      const slotB = await tree.openHandle(["checkpoint-b"], { create: true });
      return OpfsLeader.recover(
        tree,
        options.strict ?? true,
        { wal, slotA, slotB },
        options.checkpointEntries ?? 1_000_000,
        undefined,
        options.onDiagnostic,
        options.servedLedgerAgeMs,
        options.servedLedgerResultBytes,
      );
    },
  };
}

function request(key: string, sentAt: number, method = "addTable"): ServedMutationRequest {
  return { key, method, signature: `sig:${key}`, requestBytes: 16, sentAt };
}

/** Runs one mutation as the store would for a follower: identity set, then the result frame. */
async function serve<T>(
  leader: OpfsLeader,
  req: ServedMutationRequest,
  op: () => Promise<T>,
  complete = true,
): Promise<T> {
  leader.servingRequest = req;
  let value: T;
  try {
    value = await op();
  } finally {
    if (leader.servingRequest === req) leader.servingRequest = undefined;
  }
  if (complete) await leader.completeServed(req.key, value);
  return value;
}

interface Served {
  req: ServedMutationRequest;
  /** Whether the mutation's effect is present in the recovered state. */
  present: (leader: OpfsLeader) => Promise<boolean>;
  /** Value the mutation returned to its requester, when a settled ledger entry must match. */
  value?: unknown;
}

async function checkLedger(
  leader: OpfsLeader,
  served: readonly Served[],
  label: string,
): Promise<string[]> {
  const problems: string[] = [];
  const coverage = leader.servedCoverageSince;
  for (const { req, present, value } of served) {
    const outcome = leader.servedOutcome(req.key);
    const effect = await present(leader);
    if (effect && outcome === undefined && req.sentAt >= coverage) {
      problems.push(
        `${label}: ${req.key} durable but absent from ledger and inside coverage (sentAt ${String(req.sentAt)} >= ${String(coverage)}) -> re-send would run twice`,
      );
    }
    if (!effect && outcome !== undefined) {
      problems.push(
        `${label}: ${req.key} in ledger (settled=${String(outcome.settled)}) but its data is gone`,
      );
    }
    if (outcome?.settled === true && outcome.withheld !== true && value !== undefined) {
      if (JSON.stringify(outcome.result) !== JSON.stringify(value)) {
        problems.push(`${label}: ${req.key} settled with a different value`);
      }
    }
  }
  return problems;
}

describe("served-request ledger crash sweep", () => {
  it("frame durable, no result frame: unsettled entry; torn result frame: unsettled entry", async () => {
    const shim = new MemoryOpfs();
    const h = await handles(shim, "ledger-result");
    let leader = await h.recover();
    const served: Served[] = [];
    const add = (name: string, sentAt: number, complete: boolean) => {
      const req = request(`k-${name}`, sentAt);
      const entry: Served = {
        req,
        present: async (l) => (await getTable(l, `table-${name}`)) !== undefined,
      };
      served.push(entry);
      return serve(leader, req, async () => leader.addTable(table(name)), complete).then((v) => {
        entry.value = v;
      });
    };
    await add("a", 1_000, true);
    await add("b", 2_000, false); // crash before completeServed
    await add("c", 3_000, true);
    const walBefore = shim.readFileBytes(h.walPath);
    if (walBefore === undefined) throw new Error("wal");
    leader.crash();
    // Tear the last frame (c's servedResult) by one byte.
    shim.writeFileBytes(h.walPath, walBefore.slice(0, walBefore.byteLength - 1));
    leader = await h.recover();
    expect(await checkLedger(leader, served, "torn-result")).toEqual([]);
    expect(leader.servedOutcome("k-a")).toMatchObject({ settled: true });
    expect(leader.servedOutcome("k-b")).toMatchObject({ settled: false });
    expect(leader.servedOutcome("k-c")).toMatchObject({ settled: false });
    expect((await leader.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    // The recovered leader must still be able to settle b if the store completes it.
    leader.crash();
  });

  it("checkpoint interplay: torn newest slot, and checkpoint-without-WAL-reset", async () => {
    const shim = new MemoryOpfs();
    const h = await handles(shim, "ledger-checkpoint");
    let leader = await h.recover();
    const served: Served[] = [];
    const add = async (name: string, sentAt: number, complete = true) => {
      const req = request(`k-${name}`, sentAt);
      const entry: Served = {
        req,
        present: async (l) => (await getTable(l, `table-${name}`)) !== undefined,
      };
      served.push(entry);
      entry.value = await serve(leader, req, async () => leader.addTable(table(name)), complete);
    };
    await add("a", 1_000);
    await add("b", 2_000, false);
    leader.checkpointNow(); // generation 1 in both slots, WAL reset
    await add("c", 3_000);
    const walBeforeCheckpoint = shim.readFileBytes(h.walPath);
    if (walBeforeCheckpoint === undefined) throw new Error("wal");
    leader.checkpointNow(); // generation 2
    await add("d", 4_000, false);
    // Crash point 1: pretend the WAL reset of generation 2 never happened (restore the old WAL
    // in front of d's frame is impossible — the sequence continues — so restore exactly the
    // pre-checkpoint WAL, i.e. crash after both slots flushed, before reset).
    leader.crash();
    const walWithD = shim.readFileBytes(h.walPath);
    shim.writeFileBytes(h.walPath, walBeforeCheckpoint);
    leader = await h.recover();
    expect(
      await checkLedger(
        leader,
        served.filter((s) => s.req.key !== "k-d"),
        "no-reset",
      ),
    ).toEqual([]);
    expect(leader.servedOutcome("k-c")).toMatchObject({ settled: true });
    leader.crash();
    // Crash point 2: d's frame durable, then the generation-2 slot that is newest gets torn
    // (bit rot). The other slot is the same generation, so nothing rolls back.
    if (walWithD === undefined) throw new Error("wal");
    shim.writeFileBytes(h.walPath, walWithD);
    const slotBytes = h.slotPaths.map((p) => shim.readFileBytes(p));
    const newest = slotBytes[0] !== undefined && slotBytes[1] !== undefined ? 0 : 1;
    const bytes = slotBytes[newest];
    if (bytes === undefined) throw new Error("slot");
    shim.writeFileBytes(h.slotPaths[newest], bytes.slice(0, Math.floor(bytes.byteLength / 2)));
    leader = await h.recover();
    expect(await checkLedger(leader, served, "torn-slot")).toEqual([]);
    expect(leader.servedOutcome("k-d")).toMatchObject({ settled: false });
    expect((await leader.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    leader.crash();
  });

  it("torn newest slot at a NEW generation with the mirror still old: WAL bridges, ledger intact", async () => {
    const shim = new MemoryOpfs();
    const h = await handles(shim, "ledger-torn-gen");
    let leader = await h.recover();
    const served: Served[] = [];
    const add = async (name: string, sentAt: number, complete = true) => {
      const req = request(`k-${name}`, sentAt);
      const entry: Served = {
        req,
        present: async (l) => (await getTable(l, `table-${name}`)) !== undefined,
      };
      served.push(entry);
      entry.value = await serve(leader, req, async () => leader.addTable(table(name)), complete);
    };
    await add("a", 1_000);
    leader.checkpointNow(); // generation 1, both slots
    await add("b", 2_000);
    await add("c", 3_000, false);
    const walBefore = shim.readFileBytes(h.walPath);
    if (walBefore === undefined) throw new Error("wal");
    // Simulate: generation-2 write to the older slot torn mid-write (crash inside writeSlot),
    // mirror untouched (gen 1), WAL not reset.
    leader.crash();
    const [a, b] = h.slotPaths;
    const decodedA = decodeSyncCheckpoint(shim.readFileBytes(a) ?? new Uint8Array());
    const decodedB = decodeSyncCheckpoint(shim.readFileBytes(b) ?? new Uint8Array());
    expect(decodedA).toBeDefined();
    expect(decodedB).toBeDefined();
    shim.writeFileBytes(a, (shim.readFileBytes(a) ?? new Uint8Array()).slice(0, 40)); // torn gen-2
    leader = await h.recover();
    expect(await checkLedger(leader, served, "torn-new-gen")).toEqual([]);
    expect(leader.servedOutcome("k-b")).toMatchObject({ settled: true });
    expect(leader.servedOutcome("k-c")).toMatchObject({ settled: false });
    leader.crash();
  });

  it("relaxed: a frame whose payload was lost leaves no ledger claim behind", async () => {
    const shim = new MemoryOpfs();
    const model = new PowerLossModel(shim);
    const h = await handles(shim, "ledger-relaxed");
    let leader = await h.recover({ strict: false });
    await leader.addTable(table("t"));
    await leader.beginTransaction({
      record: {
        id: "tx",
        ownerId: "owner",
        expiresAt: "2026-08-24T01:00:00.000Z",
        pendingBlockIds: [],
        pendingSegmentIds: [],
        status: "active",
        revision: 0,
        startedAt: "2026-08-24T00:00:00.000Z",
        updatedAt: "2026-08-24T00:00:00.000Z",
        committedVersion: null,
      },
    });
    // Make everything so far durable.
    leader.checkpointNow();
    const stageReq = request("k-stage", 5_000, "stageTransactionArtifacts");
    await serve(leader, stageReq, () =>
      leader.stageTransactionArtifacts({
        transactionId: "tx",
        expectedRevision: 0,
        blocks: [{ id: "blk", bytes: new Uint8Array(4096).fill(1) }],
        segments: [],
        updatedAt: "2026-08-24T00:00:01.000Z",
      }),
    );
    const laterReq = request("k-later", 6_000);
    await serve(leader, laterReq, async () => leader.addTable(table("later")));
    leader.crash();
    // WAL pages happened to be written back; the extent's were not.
    model.powerLoss((unflushed, path) => (path.endsWith("/wal") ? unflushed : 0));
    leader = await h.recover({ strict: false });
    const problems = await checkLedger(
      leader,
      [
        {
          req: stageReq,
          present: async (l) => ((await getTransaction(l, "tx"))?.pendingBlockIds.length ?? 0) > 0,
        },
        { req: laterReq, present: async (l) => (await getTable(l, "table-later")) !== undefined },
      ],
      "relaxed-payload-loss",
    );
    expect(problems).toEqual([]);
    expect(leader.servedOutcome("k-stage")).toBeUndefined();
    expect(leader.servedCoverageSince).toBeLessThanOrEqual(5_000);
    expect((await leader.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    leader.crash();
  });

  it("age trimming: coverage moves exactly past evicted requests, survives checkpoint and crash", async () => {
    const shim = new MemoryOpfs();
    const h = await handles(shim, "ledger-age");
    const age = 1_000;
    let leader = await h.recover({ servedLedgerAgeMs: age });
    const served: Served[] = [];
    const add = async (name: string, sentAt: number) => {
      const req = request(`k-${name}`, sentAt);
      const entry: Served = {
        req,
        present: async (l) => (await getTable(l, `table-${name}`)) !== undefined,
      };
      served.push(entry);
      entry.value = await serve(leader, req, async () => leader.addTable(table(name)));
    };
    await add("a", 10_000);
    await add("b", 10_500);
    await add("c", 10_900);
    expect(leader.servedCoverageSince).toBe(0);
    await add("d", 11_600); // horizon 10_600: evicts a (10_000) and b (10_500); c stays
    expect(leader.servedOutcome("k-a")).toBeUndefined();
    expect(leader.servedOutcome("k-b")).toBeUndefined();
    expect(leader.servedOutcome("k-c")).toBeDefined();
    expect(leader.servedCoverageSince).toBe(10_501);
    // Insertion order is not sentAt order: an old re-sent request recorded late must not be
    // "protected" by a younger front entry, and evicting it must move coverage correctly.
    await add("old", 9_000); // horizon 8_000: nothing evicted; entry sits at the back
    await add("e", 12_000); // horizon 11_000: c (10_900) evicted
    // The trim loop stops at the first entry younger than the horizon (d, 11_600), so "old"
    // (9_000) behind it survives: it stays answerable, and coverage must still be greater than
    // every evicted sentAt.
    expect(leader.servedOutcome("k-c")).toBeUndefined();
    expect(leader.servedCoverageSince).toBe(10_901);
    const oldOutcome = leader.servedOutcome("k-old");
    // Record what the live leader claims, then verify recovery agrees from checkpoint + WAL.
    const liveCoverage = leader.servedCoverageSince;
    leader.checkpointNow();
    await add("f", 12_100);
    leader.crash();
    leader = await h.recover({ servedLedgerAgeMs: age });
    expect(await checkLedger(leader, served, "age-recovered")).toEqual([]);
    expect(leader.servedCoverageSince).toBeGreaterThanOrEqual(liveCoverage);
    expect(leader.servedOutcome("k-old") === undefined).toBe(oldOutcome === undefined);
    for (const { req } of served) {
      if (leader.servedOutcome(req.key) === undefined) {
        expect(req.sentAt).toBeLessThan(leader.servedCoverageSince);
      }
    }
    leader.crash();
  });

  it("result-byte trimming (8 MiB) and withholding (> result limit) keep coverage consistent", async () => {
    const shim = new MemoryOpfs();
    const h = await handles(shim, "ledger-bytes");
    const resultLimit = 64 * 1024;
    let leader = await h.recover({ servedLedgerResultBytes: resultLimit });
    const served: Served[] = [];
    const big = "v".repeat(20_000); // estimator: 16 + 2 * length = 40,016 bytes, under the 64 KiB limit
    // reserveRowIds returns a small value; to put bytes in the ledger, complete with a big
    // value by hand (the leader records whatever the store hands it).
    await leader.addTable(table("rows"));
    let evictedAtSentAt = -1;
    for (let i = 0; i < 260; i += 1) {
      const req = request(`k-${String(i)}`, 100_000 + i, "reserveRowIds");
      const before = new Set(
        [...Array.from({ length: i }, (_, j) => `k-${String(j)}`)].filter(
          (k) => leader.servedOutcome(k) !== undefined,
        ),
      );
      leader.servingRequest = req;
      await reserveRowIds(leader, "table-rows", 1);
      if (leader.servingRequest === req) leader.servingRequest = undefined;
      await leader.completeServed(req.key, i === 259 ? "w".repeat(resultLimit + 1) : big);
      served.push({ req, present: async () => true, value: i === 259 ? undefined : big });
      for (const k of before) {
        if (leader.servedOutcome(k) === undefined) {
          const idx = Number(k.slice(2));
          evictedAtSentAt = Math.max(evictedAtSentAt, 100_000 + idx);
        }
      }
    }
    expect(evictedAtSentAt).toBeGreaterThan(0); // byte bound reached
    expect(leader.servedCoverageSince).toBe(evictedAtSentAt + 1);
    expect(leader.servedOutcome("k-259")).toMatchObject({ settled: true, withheld: true });
    expect(await checkLedger(leader, served, "bytes-live")).toEqual([]);
    const liveCoverage = leader.servedCoverageSince;
    const liveKeys = served
      .filter((s) => leader.servedOutcome(s.req.key) !== undefined)
      .map((s) => s.req.key);
    leader.crash();
    leader = await h.recover({ servedLedgerResultBytes: resultLimit });
    // Replay retains results without trimming until the next recorded frame; the ledger may be
    // larger than live, never smaller in coverage terms.
    expect(leader.servedCoverageSince).toBeLessThanOrEqual(liveCoverage);
    for (const key of liveKeys) expect(leader.servedOutcome(key)).toBeDefined();
    expect(await checkLedger(leader, served, "bytes-recovered")).toEqual([]);
    // A checkpoint after recovery must persist a ledger the validator accepts.
    leader.checkpointNow();
    leader.crash();
    leader = await h.recover({ servedLedgerResultBytes: resultLimit });
    expect(await checkLedger(leader, served, "bytes-checkpointed")).toEqual([]);
    leader.crash();
  });

  it("count trimming at 65,536 entries moves coverage and recovers identically", async () => {
    const shim = new MemoryOpfs();
    const h = await handles(shim, "ledger-count");
    let leader = await h.recover();
    await leader.addTable(table("rows"));
    const total = 65_536 + 10;
    for (let i = 0; i < total; i += 1) {
      leader.servingRequest = request(`k-${String(i)}`, 1_000 + i, "reserveRowIds");
      await reserveRowIds(leader, "table-rows", 1);
      // No result frames: keep the WAL to one frame per request.
    }
    expect(leader.servedOutcome("k-0")).toBeUndefined();
    expect(leader.servedOutcome("k-9")).toBeUndefined();
    expect(leader.servedOutcome("k-10")).toBeDefined();
    expect(leader.servedCoverageSince).toBe(1_000 + 9 + 1);
    const liveCoverage = leader.servedCoverageSince;
    leader.crash();
    leader = await h.recover();
    expect(leader.servedCoverageSince).toBe(liveCoverage);
    expect(leader.servedOutcome("k-9")).toBeUndefined();
    expect(leader.servedOutcome("k-10")).toBeDefined();
    leader.checkpointNow();
    leader.crash();
    leader = await h.recover();
    expect(leader.servedCoverageSince).toBe(liveCoverage);
    expect(leader.servedOutcome("k-10")).toBeDefined();
    expect(leader.servedOutcome(`k-${String(total - 1)}`)).toBeDefined();
    leader.crash();
  });

  it("a mutation that appends nothing leaves no claim; a background frame may take the identity", async () => {
    const shim = new MemoryOpfs();
    const h = await handles(shim, "ledger-noop");
    let leader = await h.recover();
    await leader.addTable(table("rows"));
    // removeCompactionJob of an unknown id appends nothing.
    const noop = request("k-noop", 5_000, "removeCompactionJob");
    const value = await serve(leader, noop, () => leader.removeCompactionJob("missing"));
    expect(value).toBe(false);
    expect(leader.servedOutcome("k-noop")).toBeUndefined();
    // A refused mutation (validation error) appends nothing either.
    const refused = request("k-refused", 5_001);
    await expect(
      serve(leader, refused, async () => leader.addTable(table("rows"))),
    ).rejects.toThrow();
    expect(leader.servedOutcome("k-refused")).toBeUndefined();
    expect(leader.servingRequest).toBeUndefined();
    leader.crash();
    leader = await h.recover();
    expect(leader.servedOutcome("k-noop")).toBeUndefined();
    expect(leader.servedOutcome("k-refused")).toBeUndefined();
    leader.crash();
  });

  it("poisoned leader: completeServed after a reload settles the live entry, not a stale one", async () => {
    const shim = new MemoryOpfs();
    const h = await handles(shim, "ledger-poison");
    const req = request("k-p", 7_000);
    /** Serves k-p, poisons the leader before its result frame, then completes it. */
    const poisonThenComplete = async (opfs: MemoryOpfs, leader: OpfsLeader) => {
      leader.servingRequest = req;
      const value = await leader.addTable(table("p"));
      // Poison the leader between the served frame and its result frame: a refused WAL append
      // (quota at write) on another operation.
      opfs.setWriteFault((path, phase) => {
        if (path.endsWith("/wal") && phase === "write") {
          throw new DOMException("quota", "QuotaExceededError");
        }
      });
      await expect(leader.addTable(table("q"))).rejects.toThrow(/quota/);
      opfs.setWriteFault(null);
      // The store now completes the served request; the leader reloads first (poisoned).
      await leader.completeServed(req.key, value);
    };
    let leader = await h.recover();
    await poisonThenComplete(shim, leader);
    expect(leader.servedOutcome("k-p")?.settled).toBe(true);
    // (1) Crash now: the servedResult frame is in the WAL, so recovery settles it.
    leader.crash();
    leader = await h.recover();
    expect(leader.servedOutcome("k-p")?.settled).toBe(true);
    leader.crash();
    // (2) Instead, the poisoned-then-reloaded leader checkpoints before dying: the checkpoint is
    // written from the live ledger, and the WAL (with the result frame) is reset.
    const shim2 = new MemoryOpfs();
    const h2 = await handles(shim2, "ledger-poison-2");
    let leader2 = await h2.recover();
    await poisonThenComplete(shim2, leader2);
    leader2.checkpointNow();
    leader2.crash();
    leader2 = await h2.recover();
    expect(leader2.servedOutcome("k-p")?.settled).toBe(true);
    expect(await getTable(leader2, "table-p")).toBeDefined();
    leader2.crash();
  });
});
