/* eslint-disable no-restricted-imports -- Node-only browser harness discovers every frozen native writer fixture. */
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

/**
 * The OPFS layout this build writes; layout 12 admits compaction plans whose sources lack a column
 * added after them, and delta merges, and layout 13 columns declared TIMESTAMP without time zone.
 * Layout 11 was never released.
 */
const OPFS_LAYOUT = 13;

/** A TIMESTAMP and a TIMESTAMPTZ column declared after the upgrade, read back after a reopen. */
const TIMESTAMP_TEXT = [
  {
    local_at: "2026-01-02 03:04:05.5",
    zoned_at: "2026-01-02 03:04:05.5+00",
    doc: '"2026-01-02T03:04:05.5"',
  },
];

const directory = new URL("../format-fixtures/", import.meta.url);
for (const file of readdirSync(fileURLToPath(directory)).filter((name) =>
  /^opfs-layout[0-9]+\.json$/u.test(name),
)) {
  const fixture = JSON.parse(readFileSync(new URL(file, directory), "utf8")) as {
    files: Record<string, string>;
    expectations: { tables: string[]; blockValues: unknown[] };
  };
  test(`automatic upgrade: ${file} retains old data and new writes across a native worker restart`, async ({
    page,
  }) => {
    await page.goto("/packages/core/browser/");
    await requireWorkerOpfs(page);
    const result = await page.evaluate(async (files) => {
      const url = "/packages/core/browser/upgrade-run.ts";
      const module = (await import(url)) as typeof import("../browser/upgrade-run.js");
      return module.runNativeUpgrade(files);
    }, fixture.files);
    expect(result).toEqual({
      tables: fixture.expectations.tables,
      blockValues: fixture.expectations.blockValues,
      walOnlyPreserved: true,
      rows: [{ id: 2, value: "retained" }],
      keyForwards: true,
      timestampText: TIMESTAMP_TEXT,
      format: OPFS_LAYOUT,
      integrity: true,
      olderReaderRefused: true,
    });
  });
}

test("automatic upgrade: OPFS written by 0.14.1 keeps its rows, index, and sequence, rekeys, and refuses the released readers", async ({
  page,
}) => {
  test.setTimeout(120_000);
  await page.goto("/packages/core/browser/");
  await requireWorkerOpfs(page);
  const result = await page.evaluate(async () => {
    const url = "/packages/core/browser/upgrade-run.ts";
    const module = (await import(url)) as typeof import("../browser/upgrade-run.js");
    return module.runReleasedOpfsUpgrade();
  });
  expect(result).toEqual({
    releasedFormat: 9,
    format: OPFS_LAYOUT,
    answersPreserved: true,
    nextValue: [{ id: 4 }],
    rekeyed: [{ id: 90_000 }],
    keyForwards: true,
    integrity: true,
    olderReadersRefused: true,
    finalFormat: OPFS_LAYOUT,
  });
});

/**
 * The IndexedDB schema this build writes; schema 7 admits compaction plans whose sources lack a
 * column added after them, and delta merges, and schema 8 columns declared TIMESTAMP without time
 * zone. Schema 6 was never released.
 */
const INDEXEDDB_SCHEMA = 8;

for (const writer of ["0.10.0", "0.12.1", "0.13.1", "0.14.1", "0.15.0", "0.16.0"] as const) {
  test(`automatic upgrade: IndexedDB written by ${writer} keeps its index deltas, writes past the old delta limit, and refuses the released reader`, async ({
    page,
  }) => {
    test.setTimeout(180_000);
    await page.goto("/packages/core/browser/");
    const result = await page.evaluate(async (version) => {
      const url = "/packages/core/browser/upgrade-run.ts";
      const module = (await import(url)) as typeof import("../browser/upgrade-run.js");
      return module.runIndexedDbUpgrade(version);
    }, writer);
    expect(result).toEqual({
      releasedSchema: {
        "0.10.0": 2,
        "0.12.1": 2,
        "0.13.1": 3,
        "0.14.1": 4,
        "0.15.0": 5,
        "0.16.0": 7,
      }[writer],
      schema: INDEXEDDB_SCHEMA,
      answersPreserved: true,
      newRow: [{ id: 70_000 }],
      matches: [[{ n: 2_000 }], [{ n: 1 }], [{ n: 1 }], [{ n: 1 }]],
      reopenedSame: true,
      rekeyed: [{ id: 90_000 }],
      keyForwards: true,
      foldedAcrossAddColumn: [
        { tag: "new", n: 1 },
        { tag: "old", n: 70_700 },
      ],
      foldedFullText: {
        states: ["ready"],
        matches: [[{ n: 2_000 }], [{ n: 1 }], [{ n: 1 }], [{ n: 1 }]],
      },
      timestampText: TIMESTAMP_TEXT,
      integrity: true,
      olderReadersRefused: true,
      finalSchema: INDEXEDDB_SCHEMA,
    });
  });
}
