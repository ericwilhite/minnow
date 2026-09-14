import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

test("an uncertain native storage probe cannot shadow an existing database", async ({
  storageContext,
}) => {
  const page = await storageContext.newPage();
  await page.goto("/packages/core/browser/");
  await requireWorkerOpfs(page);
  const result = await page.evaluate(async () => {
    const url = "/packages/core/browser/storage-probe.ts";
    return (
      (await import(url)) as typeof import("../browser/storage-probe.js")
    ).runFailedStorageProbe();
  });

  expect(result).toEqual({
    errorName: "UnknownError",
    indexedDbShadowCreated: false,
    rows: [{ id: 1, amount: 700 }],
  });
});

test("a native IndexedDB probe failure cannot open an alternate store", async ({
  storageContext,
}) => {
  const page = await storageContext.newPage();
  await page.goto("/packages/core/browser/");
  const result = await page.evaluate(async () => {
    const url = "/packages/core/browser/storage-probe.ts";
    return (
      (await import(url)) as typeof import("../browser/storage-probe.js")
    ).runFailedIndexedDbProbe();
  });

  expect(result).toEqual({
    errorName: "VersionError",
    alternativeOpened: false,
    rows: [{ id: 1, amount: 900 }],
  });
});

test("native IndexedDB GC retries a segment deleted during bounded planning", async ({
  storageContext,
}) => {
  const page = await storageContext.newPage();
  await page.goto("/packages/core/browser/");
  const result = await page.evaluate(async () => {
    const url = "/packages/core/browser/storage-probe.ts";
    return (
      (await import(url)) as typeof import("../browser/storage-probe.js")
    ).runNativeIndexedDbGarbageCollectionRace();
  });

  expect(result).toEqual({
    insertedAfterSnapshot: true,
    droppedBeforeNomination: true,
    provenanceRefusals: 1,
    missingSegmentCount: 0,
    tablesAfterReopen: [],
    completedJobs: 1,
  });
});

test("a native IndexedDB sequence keeps its key metadata across reopen", async ({
  storageContext,
}) => {
  const page = await storageContext.newPage();
  await page.goto("/packages/core/browser/");
  const result = await page.evaluate(async () => {
    const url = "/packages/core/browser/storage-probe.ts";
    return (
      (await import(url)) as typeof import("../browser/storage-probe.js")
    ).runNativeIndexedDbSequenceReopen();
  });

  expect(result).toEqual({
    first: [{ id: 1 }],
    second: [{ id: 2 }],
    durableKeyMetadata: true,
  });
});
