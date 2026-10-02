/* eslint-disable no-restricted-imports -- Node-only browser harness discovers every frozen native writer fixture. */
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { expect } from "@playwright/test";
import { requireWorkerOpfs, test } from "./fixtures.js";

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
      rows: [{ id: 1, value: "retained" }],
      format: 9,
      integrity: true,
      olderReaderRefused: true,
    });
  });
}

/** The IndexedDB schema this build writes; schema 4 stores postings deltas as ordered parts. */
const INDEXEDDB_SCHEMA = 4;

for (const writer of ["0.10.0", "0.12.1", "0.13.1"] as const) {
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
      releasedSchema: writer === "0.13.1" ? 3 : 2,
      schema: INDEXEDDB_SCHEMA,
      answersPreserved: true,
      newRow: [{ id: 70_000 }],
      matches: [[{ n: 2_000 }], [{ n: 1 }], [{ n: 1 }], [{ n: 1 }]],
      reopenedSame: true,
      integrity: true,
      olderReadersRefused: true,
      finalSchema: INDEXEDDB_SCHEMA,
    });
  });
}
