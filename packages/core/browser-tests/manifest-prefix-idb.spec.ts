import { expect } from "@playwright/test";
import { test } from "./fixtures.js";

test("native IndexedDB removes only the oldest pruned manifest prefix across reopen", async ({
  page,
}) => {
  await page.goto("/packages/core/browser/");
  const result = await page.evaluate(async () => {
    const url = "/packages/core/browser/manifest-prefix-idb-run.ts";
    return (
      (await import(url)) as typeof import("../browser/manifest-prefix-idb-run.js")
    ).runNativeIndexedDbFreshManifestPrefix();
  });

  expect(result).toEqual({
    discoveryPages: [0, 0],
    concurrentlyPrunedVersions: [8],
    prunedBoundaryIntegrity: true,
    firstRemoval: 5,
    versionsAfterFirstRemoval: [5, 6, 7, 8, 9],
    firstIntegrity: true,
    reopenedIntegrity: true,
    finalRemoval: 3,
    finalVersions: [8, 9],
    finalIntegrity: true,
  });
});

test("native IndexedDB resumes an authenticated legacy manifest gap and rejects corruption", async ({
  page,
}) => {
  await page.goto("/packages/core/browser/");
  const result = await page.evaluate(async () => {
    const url = "/packages/core/browser/manifest-prefix-idb-run.ts";
    return (
      (await import(url)) as typeof import("../browser/manifest-prefix-idb-run.js")
    ).runNativeIndexedDbLegacyManifestPrefix();
  });

  expect(result).toEqual({
    initialIntegrity: true,
    extraGapIssue: {
      code: "invalid-manifest",
      location: "5",
      message: "indexeddb storage corruption at manifests/5: manifest predecessor is unavailable",
    },
    malformedMarkerIssue: {
      code: "invalid-catalog-record",
      location: "manifest/prune-cleanup",
      message: "indexeddb storage corruption at manifest/prune-cleanup: delete cursor is invalid",
    },
    missingBoundaryIssue: {
      code: "invalid-manifest-prune-cleanup",
      location: "manifest/prune-cleanup",
      message: "prune cleanup boundary is missing",
    },
    missingBoundaryError: {
      name: "StorageCorruptionError",
      message:
        "indexeddb storage corruption at manifest/prune-cleanup: cleanup boundary is missing",
    },
    firstRemoval: 2,
    versionsAfterFirstRemoval: [2, 3, 4, 5, 8, 9],
    reopenedRemovals: [2, 2, 0],
    finalVersions: [8, 9],
    finalIntegrity: true,
  });
});
