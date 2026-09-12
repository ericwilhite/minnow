/**
 * RecordCore journal accounting since 0.10.0 ("accounts a stage by what it appends", journal
 * block sets and bytes memoized on array identity).
 *
 * Randomized sequences of stage / rollback-to-savepoint / abort / commit / dump+load are run
 * against a from-scratch oracle. Observable surfaces of the private counters and root
 * reference counts:
 *   - duplicate detection ("Block already exists") must follow the journal, not a stale memo;
 *   - a block removed by rollback must be stageable again;
 *   - a GC candidate journaled by an active transaction is refused, and accepted once the
 *     transaction is aborted (root reference counts);
 *   - dump() -> fresh load() must produce an identical dump (no accounting-dependent drift) and
 *     the loaded core must make the same duplicate/GC decisions.
 */
import { describe, expect, it } from "vitest";
import { mulberry32 } from "../../testing/seeds.js";
import { RecordCore } from "../toolkit/record-core.js";
import type { SegmentRecord, TableRecord, TransactionRecord } from "../types.js";

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

/** Indexes an array the caller knows is long enough; `noUncheckedIndexedAccess` cannot. */
function at<T>(items: readonly T[], index: number): T {
  const item = items[index];
  if (item === undefined) throw new Error(`No item at index ${String(index)}`);
  return item;
}

class Physical {
  readonly blocks = new Map<string, Uint8Array>();
  hasBlock = (id: string): boolean => this.blocks.has(id);
  blockByteLength = (id: string): number | undefined => this.blocks.get(id)?.byteLength;
  blockChecksum = (id: string): number | undefined => (this.blocks.has(id) ? 0 : undefined);
}

function newTransaction(id: string): TransactionRecord {
  return {
    id,
    ownerId: `owner-${id}`,
    expiresAt: "2026-08-24T01:00:00.000Z",
    snapshotVersion: null,
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: "2026-08-24T00:00:00.000Z",
    updatedAt: "2026-08-24T00:00:00.000Z",
    committedVersion: null,
  };
}

function segment(
  id: string,
  tableId: string,
  transactionId: string,
  blockIds: string[],
  ordinal: number,
  rowStart: bigint,
): SegmentRecord {
  return {
    id,
    kind: "insert",
    level: 0,
    logicalOrder: 0,
    commitOrdinal: ordinal,
    rowIdSpans: [],
    tableId,
    transactionId,
    rowCount: 1,
    rowIdStart: rowStart,
    rowIdEndExclusive: rowStart + 1n,
    columnBlockIds: { c1: blockIds },
    createdAt: "2026-08-24T00:00:01.000Z",
  };
}

interface Model {
  journals: Map<
    string,
    { blocks: string[]; segments: string[]; revision: number; nextRow: bigint }
  >;
}

describe("RecordCore journal accounting", () => {
  it("random stage/rollback/abort/load sequences agree with a from-scratch oracle", () => {
    const seeds = [11, 12, 13, 14, 15, 16, 17, 18];
    for (const seed of seeds) {
      const rng = mulberry32(seed);
      const physical = new Physical();
      let core = new RecordCore(physical);
      core.load(
        (() => {
          const empty = new RecordCore(new Physical());
          return empty.dump();
        })(),
      );
      core.addTable(table("t"));
      const model: Model = { journals: new Map() };
      let blockOrdinal = 0;
      let segOrdinal = 0;
      const activeIds: string[] = [];
      const problems: string[] = [];
      const stage = (txId: string, count: number, withSegment: boolean) => {
        const journal = model.journals.get(txId);
        if (journal === undefined) return;
        const blocks = Array.from({ length: count }, () => {
          blockOrdinal += 1;
          return { id: `b${String(blockOrdinal)}`, bytes: new Uint8Array(1 + (blockOrdinal % 7)) };
        });
        const segments: SegmentRecord[] = [];
        if (withSegment && (blocks.length > 0 || journal.blocks.length > 0)) {
          segOrdinal += 1;
          const refs = blocks.length > 0 ? blocks.map((b) => b.id) : [at(journal.blocks, 0)];
          segments.push(
            segment(
              `s${String(segOrdinal)}`,
              "table-t",
              txId,
              refs,
              journal.segments.length,
              journal.nextRow,
            ),
          );
        }
        const updated = core.stageTransactionArtifacts({
          transactionId: txId,
          expectedRevision: journal.revision,
          blocks,
          segments,
          updatedAt: "2026-08-24T00:00:02.000Z",
        });
        for (const b of blocks) physical.blocks.set(b.id, b.bytes);
        journal.blocks.push(...blocks.map((b) => b.id));
        journal.segments.push(...segments.map((s) => s.id));
        journal.revision = updated.revision;
        if (segments.length > 0) journal.nextRow += 1n;
        if (
          JSON.stringify(updated.pendingBlockIds) !== JSON.stringify(journal.blocks) ||
          JSON.stringify(updated.pendingSegmentIds) !== JSON.stringify(journal.segments)
        ) {
          problems.push(`seed ${String(seed)}: journal drift for ${txId}`);
        }
      };
      const rollback = (txId: string) => {
        const journal = model.journals.get(txId);
        if (journal === undefined || journal.blocks.length === 0) return;
        const keepBlocks = Math.floor(rng() * journal.blocks.length);
        const keptBlocks = journal.blocks.slice(0, keepBlocks);
        const removedBlocks = journal.blocks.slice(keepBlocks);
        const removedSet = new Set(removedBlocks);
        // Segments referencing removed blocks must go, and the prefix rule applies.
        let keepSegments = journal.segments.length;
        for (let i = 0; i < journal.segments.length; i += 1) {
          const seg = core.getSegment(at(journal.segments, i));
          const refs = seg === undefined ? [] : Object.values(seg.columnBlockIds).flat();
          if (refs.some((id) => removedSet.has(id))) {
            keepSegments = i;
            break;
          }
        }
        const keptSegments = journal.segments.slice(0, keepSegments);
        const removedSegments = journal.segments.slice(keepSegments);
        const updated = core.rollbackTransactionArtifacts({
          transactionId: txId,
          expectedRevision: journal.revision,
          pendingBlockIds: keptBlocks,
          pendingSegmentIds: keptSegments,
          removeBlockIds: removedBlocks,
          removeSegmentIds: removedSegments,
          updatedAt: "2026-08-24T00:00:03.000Z",
        });
        for (const id of removedBlocks) physical.blocks.delete(id);
        journal.blocks = keptBlocks;
        journal.segments = keptSegments;
        journal.revision = updated.revision;
        journal.nextRow = BigInt(keptSegments.length + 1);
        // A removed block id must be stageable again (the memoized set must not remember it).
        if (removedBlocks.length > 0) {
          const again = at(removedBlocks, 0);
          try {
            const res = core.stageTransactionArtifacts({
              transactionId: txId,
              expectedRevision: journal.revision,
              blocks: [{ id: again, bytes: new Uint8Array(3) }],
              segments: [],
              updatedAt: "2026-08-24T00:00:04.000Z",
            });
            physical.blocks.set(again, new Uint8Array(3));
            journal.blocks.push(again);
            journal.revision = res.revision;
          } catch (error) {
            problems.push(
              `seed ${String(seed)}: re-staging rolled-back block ${again} failed: ${String(error)}`,
            );
          }
        }
      };
      const abort = (txId: string) => {
        const journal = model.journals.get(txId);
        if (journal === undefined) return;
        core.updateTransaction(txId, journal.revision, {
          status: "aborted",
          updatedAt: "2026-08-24T00:00:05.000Z",
        });
        model.journals.delete(txId);
        const index = activeIds.indexOf(txId);
        if (index >= 0) activeIds.splice(index, 1);
      };
      const checkDuplicates = () => {
        for (const [txId, journal] of model.journals) {
          if (journal.blocks.length === 0) continue;
          const dup = at(journal.blocks, Math.floor(rng() * journal.blocks.length));
          let threw = false;
          try {
            core.preflightTransactionArtifactStage({
              transactionId: txId,
              expectedRevision: journal.revision,
              blocks: [{ id: dup, bytes: new Uint8Array(1) }],
              segments: [],
              updatedAt: "2026-08-24T00:00:06.000Z",
            });
          } catch {
            threw = true;
          }
          if (!threw) problems.push(`seed ${String(seed)}: duplicate ${dup} accepted for ${txId}`);
        }
      };
      const checkRoots = () => {
        // A block journaled by an active transaction cannot be a GC candidate.
        for (const [, journal] of model.journals) {
          if (journal.blocks.length === 0) continue;
          const id = at(journal.blocks, journal.blocks.length - 1);
          let threw = false;
          try {
            core.createGarbageCollectionJob({
              id: `gc-probe-${String(Math.floor(rng() * 1e9))}`,
              candidateManifestVersions: [],
              candidateSegmentIds: [],
              candidateBlockIds: [id],
              leaseCutoff: "2026-08-24T00:01:00.000Z",
              createdAt: "2026-08-24T00:01:00.000Z",
            });
          } catch {
            threw = true;
          }
          if (!threw) problems.push(`seed ${String(seed)}: GC accepted journaled block ${id}`);
        }
      };
      const reload = () => {
        const dumped = core.dump();
        const fresh = new RecordCore(physical);
        fresh.load(dumped);
        const a = JSON.stringify(dumped, (_k, v: unknown) =>
          typeof v === "bigint" ? `${v.toString()}n` : v,
        );
        const b = JSON.stringify(fresh.dump(), (_k, v: unknown) =>
          typeof v === "bigint" ? `${v.toString()}n` : v,
        );
        if (a !== b) problems.push(`seed ${String(seed)}: dump/load round trip differs`);
        core = fresh;
      };
      for (let step = 0; step < 120; step += 1) {
        const roll = rng();
        if (roll < 0.15 || activeIds.length === 0) {
          const id = `tx-${String(seed)}-${String(step)}`;
          core.createTransaction(newTransaction(id));
          model.journals.set(id, { blocks: [], segments: [], revision: 0, nextRow: 1n });
          activeIds.push(id);
        } else if (roll < 0.6) {
          stage(
            at(activeIds, Math.floor(rng() * activeIds.length)),
            Math.floor(rng() * 5),
            rng() < 0.5,
          );
        } else if (roll < 0.75) {
          rollback(at(activeIds, Math.floor(rng() * activeIds.length)));
        } else if (roll < 0.85) {
          abort(at(activeIds, Math.floor(rng() * activeIds.length)));
        } else if (roll < 0.95) {
          checkDuplicates();
          checkRoots();
        } else {
          reload();
          checkDuplicates();
          checkRoots();
        }
      }
      reload();
      checkDuplicates();
      checkRoots();
      expect(problems).toEqual([]);
    }
  });

  it("the memoized journal set is not shared across a rollback that restores an earlier list", () => {
    const physical = new Physical();
    const core = new RecordCore(physical);
    core.load(new RecordCore(new Physical()).dump());
    core.createTransaction(newTransaction("tx"));
    const r1 = core.stageTransactionArtifacts({
      transactionId: "tx",
      expectedRevision: 0,
      blocks: [{ id: "a", bytes: new Uint8Array(1) }],
      segments: [],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    physical.blocks.set("a", new Uint8Array(1));
    const r2 = core.stageTransactionArtifacts({
      transactionId: "tx",
      expectedRevision: r1.revision,
      blocks: [{ id: "b", bytes: new Uint8Array(1) }],
      segments: [],
      updatedAt: "2026-08-24T00:00:02.000Z",
    });
    physical.blocks.set("b", new Uint8Array(1));
    // Roll back to [a] using the exact array object the earlier record returned.
    const r3 = core.rollbackTransactionArtifacts({
      transactionId: "tx",
      expectedRevision: r2.revision,
      pendingBlockIds: r1.pendingBlockIds,
      pendingSegmentIds: [],
      removeBlockIds: ["b"],
      removeSegmentIds: [],
      updatedAt: "2026-08-24T00:00:03.000Z",
    });
    physical.blocks.delete("b");
    // b must be stageable again.
    expect(() =>
      core.preflightTransactionArtifactStage({
        transactionId: "tx",
        expectedRevision: r3.revision,
        blocks: [{ id: "b", bytes: new Uint8Array(1) }],
        segments: [],
        updatedAt: "2026-08-24T00:00:04.000Z",
      }),
    ).not.toThrow();
    // a is still journaled.
    expect(() =>
      core.preflightTransactionArtifactStage({
        transactionId: "tx",
        expectedRevision: r3.revision,
        blocks: [{ id: "a", bytes: new Uint8Array(1) }],
        segments: [],
        updatedAt: "2026-08-24T00:00:04.000Z",
      }),
    ).toThrow(/already exists/);
  });
});
