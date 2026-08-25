import type { BlockStore, SegmentRecord, TransactionRecord } from "../storage/index.js";
import type { MinnowDatabase, VisibleSegment, VisibleSegmentPageCursor } from "./database.js";

/** Exhausts the bounded public segment cursor for finite test fixtures. */
export async function allVisibleSegments(
  database: MinnowDatabase,
  tableName: string,
  version?: number,
): Promise<VisibleSegment[]> {
  const records: VisibleSegment[] = [];
  let cursor: VisibleSegmentPageCursor | null = null;
  do {
    const page = await database.listVisibleSegmentPage(
      tableName,
      cursor === null ? { ...(version === undefined ? {} : { version }) } : { cursor },
    );
    records.push(...page.records);
    cursor = page.nextCursor;
  } while (cursor !== null);
  return records
    .sort((left, right) => {
      const leftPage = left as VisibleSegment & {
        logicalOrder: number;
        committedVersion: number;
        commitOrdinal: number;
      };
      const rightPage = right as typeof leftPage;
      return (
        leftPage.logicalOrder - rightPage.logicalOrder ||
        leftPage.committedVersion - rightPage.committedVersion ||
        leftPage.commitOrdinal - rightPage.commitOrdinal ||
        left.id.localeCompare(right.id)
      );
    })
    .map(({ id, rowCount, columnBlockIds }) => ({ id, rowCount, columnBlockIds }));
}

/** Exhausts a bounded segment cursor for tests that need the complete finite fixture. */
export async function allSegmentRecords(
  store: BlockStore,
  tableId?: string,
): Promise<SegmentRecord[]> {
  const records: SegmentRecord[] = [];
  let cursor: string | null = null;
  do {
    const page: { records: SegmentRecord[]; nextCursor: string | null } =
      tableId === undefined
        ? await store.listSegmentPage(cursor, 1_024)
        : await store.listTableSegmentPage(tableId, cursor, 1_024);
    records.push(...page.records);
    cursor = page.nextCursor;
  } while (cursor !== null);
  return records;
}

/** Exhausts a bounded transaction cursor for tests that need the complete finite fixture. */
export async function allTransactionRecords(store: BlockStore): Promise<TransactionRecord[]> {
  const records: TransactionRecord[] = [];
  let cursor: string | null = null;
  do {
    const page = await store.listTransactionPage(cursor, 1_024);
    records.push(...page.records);
    cursor = page.nextCursor;
  } while (cursor !== null);
  return records;
}
