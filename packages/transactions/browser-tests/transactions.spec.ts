import { expect, test } from "@playwright/test";

test("coordinates and recovers transactions in real IndexedDB", async ({ page }) => {
  await page.goto("/packages/transactions/browser/");
  await expect(page.locator("#ready")).toHaveText("Transaction tests ready");
  const result = await page.evaluate(async () => {
    const target = window as typeof window & {
      runTransactionBrowserTest(): Promise<{
        competingCommits: {
          fulfilled: number;
          conflicts: number;
          finalVersion?: number;
          finalBlockIds: string[];
          stableBlockIds: string[];
        };
        lostResponseRecovered: boolean;
        leases: { renewed: boolean; released: boolean };
        rowIdsDisjoint: boolean;
        batchWrite: {
          tables: string[];
          rowCount: number;
          blockCount: number;
          storedBytesPositive: boolean;
          visibleSegments: number;
          upsertInserted: number;
          upsertUpdated: number;
          finalRows: number;
          updatedValue: number | null;
          partialUpdatedRows: number;
          projectedColumns: string[];
          deletedRows: number;
          writeMetricsValid: boolean;
          bufferedRows: number;
          compaction: {
            compacted: boolean;
            sourceSegments: number;
            visibleSegments: number;
            currentRows: number;
            oldSnapshotRows: number;
            physicallyReclaimedBytes: number;
          };
          mutationCompaction: {
            compacted: boolean;
            sourceSegments: number;
            visibleSegments: number;
            currentNames: string[];
            historicalRows: number;
          };
        };
      }>;
    };
    return target.runTransactionBrowserTest();
  });

  expect(result).toEqual({
    competingCommits: {
      fulfilled: 1,
      conflicts: 1,
      finalVersion: 1,
      finalBlockIds: ["first", "second"],
      stableBlockIds: [expect.stringMatching(/^(first|second)$/)],
    },
    lostResponseRecovered: true,
    leases: { renewed: true, released: true },
    rowIdsDisjoint: true,
    batchWrite: {
      tables: ["events", "people"],
      rowCount: 3,
      blockCount: 4,
      storedBytesPositive: true,
      visibleSegments: 4,
      upsertInserted: 1,
      upsertUpdated: 1,
      finalRows: 3,
      updatedValue: 26,
      partialUpdatedRows: 1,
      projectedColumns: ["name"],
      deletedRows: 1,
      writeMetricsValid: true,
      bufferedRows: 3,
      compaction: {
        compacted: true,
        sourceSegments: 2,
        visibleSegments: 1,
        currentRows: 3,
        oldSnapshotRows: 2,
        physicallyReclaimedBytes: 0,
      },
      mutationCompaction: {
        compacted: true,
        sourceSegments: 4,
        visibleSegments: 1,
        currentNames: ["Grace", "Linus", "Katherine"],
        historicalRows: 3,
      },
    },
  });
});
