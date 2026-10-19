/**
 * Shared scaffolding for tests that drive a bare `OpfsLeader` over the in-memory shim: handle
 * opening by database name, the generated read methods (which carry no declared types), a
 * frame walker over a WAL image, and a minimal keyed table plus staged transaction.
 */
import type { MemoryOpfs } from "../../testing/opfs-shim.js";
import { encodeBlock } from "../../block-format/index.js";
import { OpfsTree } from "./files.js";
import { OpfsLeader } from "./leader.js";
import type { TableRecord, TransactionRecord } from "../types.js";

export function leaderTable(name: string): TableRecord {
  return {
    id: `table-${name}`,
    name,
    columns: [{ id: "c1", name: "id", type: "number", nullable: false }],
    managed: false,
    revision: 0,
    createdAt: "2026-08-19T00:00:00.000Z",
  };
}

export interface LeaderHandles {
  tree: OpfsTree;
  recover: (options?: { strict?: boolean; checkpointEntries?: number }) => Promise<OpfsLeader>;
  walPath: string;
  ackPath: string;
  slotPaths: [string, string];
}

export async function leaderHandles(shim: MemoryOpfs, name: string): Promise<LeaderHandles> {
  const minnow = await shim.root.getDirectoryHandle("minnowdb", { create: true });
  const database = await minnow.getDirectoryHandle(name, { create: true });
  const tree = new OpfsTree(database);
  return {
    tree,
    walPath: `minnowdb/${name}/wal`,
    ackPath: `minnowdb/${name}/wal-acknowledgements`,
    slotPaths: [`minnowdb/${name}/checkpoint-a`, `minnowdb/${name}/checkpoint-b`],
    recover: async (options = {}) => {
      const wal = await tree.openHandle(["wal"], { create: true });
      const slotA = await tree.openHandle(["checkpoint-a"], { create: true });
      const slotB = await tree.openHandle(["checkpoint-b"], { create: true });
      const acknowledgements = await tree.openHandle(["wal-acknowledgements"], { create: true });
      return OpfsLeader.recover(
        tree,
        options.strict ?? true,
        { wal, slotA, slotB, acknowledgements },
        options.checkpointEntries ?? 1_000_000,
      );
    },
  };
}

function coreRead<T>(leader: OpfsLeader, method: string, args: unknown[]): Promise<T> {
  return (
    leader as unknown as { _readCoreGenerated(m: string, a: unknown[]): Promise<T> }
  )._readCoreGenerated(method, args);
}

export async function leaderTableNames(leader: OpfsLeader): Promise<string[]> {
  const records = await coreRead<TableRecord[]>(leader, "listTables", []);
  return records.map((record) => record.name).sort();
}

export function leaderTransaction(
  leader: OpfsLeader,
  id: string,
): Promise<TransactionRecord | undefined> {
  return coreRead(leader, "getTransaction", [id]);
}

export function leaderUniqueKeys(
  leader: OpfsLeader,
  tableId: string,
  tokens: readonly string[],
): Promise<string[]> {
  return coreRead(leader, "getExistingUniqueKeys", [tableId, tokens]);
}

/** Frame boundaries of a WAL image: `[start, end)` per frame, continuation pieces included. */
export function walFrameRanges(
  bytes: Uint8Array,
): Array<{ start: number; end: number; magic: string }> {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const frames: Array<{ start: number; end: number; magic: string }> = [];
  let offset = 0;
  while (offset + 12 <= bytes.byteLength) {
    const magic = String.fromCharCode(...bytes.subarray(offset, offset + 4));
    const length = view.getUint32(offset + 4, true);
    const end = offset + 12 + length;
    if (end > bytes.byteLength) break;
    frames.push({ start: offset, end, magic });
    offset = end;
  }
  return frames;
}

export const LEADER_T0 = "2026-08-19T00:00:00.000Z";
export const LEADER_T1 = "2026-08-19T00:30:00.000Z";

/**
 * A keyed table and one active transaction with a staged row, ready for a `commitTransaction`
 * whose `uniqueKeyChanges` decide whether the commit is large enough to stage in pieces.
 */
export async function stageKeyedTransaction(
  leader: OpfsLeader,
  transactionId: string,
): Promise<{ revision: number }> {
  await leader.addTable({ ...leaderTable("keyed"), id: "keyed", uniqueKeyColumnId: "c1" });
  await leader._loggedGenerated("createTransaction", [
    {
      id: transactionId,
      ownerId: `owner-${transactionId}`,
      expiresAt: LEADER_T1,
      snapshotVersion: null,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "active",
      revision: 0,
      startedAt: LEADER_T0,
      updatedAt: LEADER_T0,
      committedVersion: null,
    },
  ]);
  const staged = await leader.stageTransactionArtifacts({
    transactionId,
    expectedRevision: 0,
    blocks: [
      { id: "keyed-block", bytes: await encodeBlock({ type: "number", values: [1] }, "raw") },
    ],
    segments: [
      {
        id: "keyed-segment",
        tableId: "keyed",
        transactionId,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { c1: ["keyed-block"] },
        kind: "insert",
        level: 0,
        logicalOrder: 0,
        commitOrdinal: 0,
        rowIdSpans: [],
        createdAt: LEADER_T0,
      },
    ],
    updatedAt: LEADER_T0,
  });
  return { revision: staged.revision };
}

export function keyTokens(count: number): string[] {
  return Array.from({ length: count }, (_, index) => `number:${String(index)}`);
}

export function errorText(error: unknown): string {
  if (!(error instanceof Error)) return String(error);
  const cause = (error as { cause?: unknown }).cause;
  return `${error.name}: ${error.message}${cause === undefined ? "" : ` <- ${errorText(cause)}`}`;
}
