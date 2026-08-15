import { expect, test } from "@playwright/test";

test("coordinates and recovers transactions in real IndexedDB", async ({ page }) => {
  await page.goto("/packages/core/browser/");
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
        writeScopes: {
          scopeTotal: number | null;
          scopeRolledBack: boolean;
          ledgerAfterScopes: number[];
          ledgerAuditActions: string[];
          missingKeyDeleted: number;
          auditRowsAfterDelete: number | null;
          reopenedLedgerRows: number;
          reopenedAuditRows: number | null;
        };
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
          vectorQuery: {
            prunedNames: string[];
            spilledNames: string[];
            prepared: { count: number; total: number };
            historical: { count: number; total: number };
            current: { count: number; total: number };
            compacted: { count: number; total: number };
            reopened: { count: number; total: number };
            memoryBudget: {
              prepareRejected: boolean;
              executeRejected: boolean;
              exactSucceeded: boolean;
            };
          };
          l2Compaction: {
            sourceSegments: number[];
            levels: number[];
            ordinals: number[];
            currentValues: number[];
            historicalRows: number;
            reopenedRows: number;
            stablePartitionIds: boolean;
            budgetWithinBounds: boolean;
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
    writeScopes: {
      // The in-scope read saw the staged update (60) plus the staged insert (40).
      scopeTotal: 100,
      scopeRolledBack: true,
      // The failed scope published nothing, so only the committed rows remain.
      ledgerAfterScopes: [40, 60],
      // One insert trigger for the seed row, one per scope-staged mutation.
      ledgerAuditActions: ["ins", "ins", "upd"],
      // Only the key that existed was deleted, and the delete fires no audit rows.
      missingKeyDeleted: 1,
      auditRowsAfterDelete: 3,
      reopenedLedgerRows: 1,
      reopenedAuditRows: 3,
    },
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
      vectorQuery: {
        prunedNames: ["Linus"],
        spilledNames: ["Linus", "Grace", "Ada"],
        prepared: { count: 3, total: 60 },
        historical: { count: 3, total: 60 },
        current: { count: 3, total: 96 },
        compacted: { count: 3, total: 96 },
        reopened: { count: 3, total: 96 },
        memoryBudget: {
          prepareRejected: true,
          executeRejected: true,
          exactSucceeded: true,
        },
      },
      l2Compaction: {
        sourceSegments: [2, 2],
        levels: [2, 2],
        ordinals: [0, 1],
        currentValues: [1, 2, 3, 4],
        historicalRows: 1,
        reopenedRows: 4,
        stablePartitionIds: true,
        budgetWithinBounds: true,
      },
    },
  });
});
