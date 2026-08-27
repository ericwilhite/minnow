import { createHash } from "node:crypto";
import { createReadStream, readFileSync } from "node:fs";
import { open } from "node:fs/promises";
import { createInterface } from "node:readline";

import { describe, expect, it } from "vitest";
import { parseSqlLogicTestLines } from "../packages/core/src/testing/sqllogictest.js";

interface CorpusSource {
  readonly source: string;
  readonly sourceSha256: string;
  readonly sourceRecords: number;
  readonly standardIncludedRecords: number;
  readonly fullIncludedRecords: number;
  readonly allProfileExcludedRecords: number;
  readonly standardOnlyExcludedRecords: number;
  readonly standardOutput: string;
  readonly standardOutputSha256: string;
  readonly fullOutput: string;
  readonly fullOutputSha256: string;
}

interface Exclusion {
  readonly source: string;
  readonly line: number;
  readonly kind: string;
  readonly scope: "all-profiles" | "standard-only";
  readonly reason: string;
}

interface ExclusionLedger {
  readonly schemaVersion: 1;
  readonly revision: string;
  readonly sources: readonly CorpusSource[];
  readonly sourceRecords: number;
  readonly standardIncludedRecords: number;
  readonly fullIncludedRecords: number;
  readonly allProfileExcludedRecords: number;
  readonly standardOnlyExcludedRecords: number;
  readonly exclusions: readonly Exclusion[];
}

interface UpstreamManifest {
  readonly revision: string;
  readonly standardSources: ReadonlyArray<{ readonly path: string; readonly sha256: string }>;
}

const corpusDirectory = new URL("../packages/core/testdata/sqllogictest/", import.meta.url);
const ledger = readJson("standard-exclusions.json") as ExclusionLedger;
const upstream = readJson("upstream.json") as UpstreamManifest;

describe("committed SQLLogicTest corpus", () => {
  it("balances every included and excluded record with an exact reason", () => {
    expect(ledger.schemaVersion).toBe(1);
    expect(ledger.revision).toBe(upstream.revision);
    expect(ledger.sources.map((source) => source.source)).toEqual(
      upstream.standardSources.map((source) => source.path),
    );
    expect(ledger.sourceRecords).toBe(sum(ledger.sources, "sourceRecords"));
    expect(ledger.standardIncludedRecords).toBe(sum(ledger.sources, "standardIncludedRecords"));
    expect(ledger.fullIncludedRecords).toBe(sum(ledger.sources, "fullIncludedRecords"));
    expect(ledger.allProfileExcludedRecords).toBe(sum(ledger.sources, "allProfileExcludedRecords"));
    expect(ledger.standardOnlyExcludedRecords).toBe(
      sum(ledger.sources, "standardOnlyExcludedRecords"),
    );
    expect(ledger.exclusions).toHaveLength(
      ledger.allProfileExcludedRecords + ledger.standardOnlyExcludedRecords,
    );

    const identities = ledger.exclusions.map(
      (entry) => `${entry.source}:${String(entry.line)}:${entry.scope}`,
    );
    expect(new Set(identities).size).toBe(identities.length);
    for (const exclusion of ledger.exclusions) {
      expect(exclusion.line).toBeGreaterThan(0);
      expect(exclusion.kind).toMatch(/^(?:statement|query)$/u);
      expect(exclusion.reason.trim().length).toBeGreaterThan(20);
      expect(ledger.sources.some((source) => source.source === exclusion.source)).toBe(true);
    }
  });

  it("runs every selected source record in the full profile", () => {
    expect(ledger.allProfileExcludedRecords).toBe(0);
    expect(ledger.fullIncludedRecords).toBe(ledger.sourceRecords);
    expect(ledger.exclusions.every(({ scope }) => scope === "standard-only")).toBe(true);
  });

  it.each(ledger.sources)("pins generated subsets for $source", async (source) => {
    const upstreamSource = upstream.standardSources.find((entry) => entry.path === source.source);
    expect(upstreamSource?.sha256).toBe(source.sourceSha256);
    expect(source.sourceRecords - source.fullIncludedRecords).toBe(
      source.allProfileExcludedRecords,
    );
    expect(source.fullIncludedRecords - source.standardIncludedRecords).toBe(
      source.standardOnlyExcludedRecords,
    );

    await expectSubset(
      source.standardOutput,
      source.standardOutputSha256,
      source.standardIncludedRecords,
      source.sourceSha256,
    );
    await expectSubset(
      source.fullOutput,
      source.fullOutputSha256,
      source.fullIncludedRecords,
      source.sourceSha256,
    );
  });
});

async function expectSubset(
  name: string,
  expectedHash: string,
  expectedRecords: number,
  sourceHash: string,
): Promise<void> {
  const file = new URL(name, corpusDirectory);
  expect(await sha256(file)).toBe(expectedHash);
  const handle = await open(file, "r");
  const headerBytes = Buffer.alloc(1024);
  try {
    const { bytesRead } = await handle.read(headerBytes, 0, headerBytes.length, 0);
    expect(headerBytes.toString("utf8", 0, bytesRead).split("\n", 5)).toContain(
      `# Source SHA-256: ${sourceHash}`,
    );
  } finally {
    await handle.close();
  }

  const input = createReadStream(file, { encoding: "utf8" });
  const lines = createInterface({ input, crlfDelay: Infinity });
  let records = 0;
  try {
    for await (const record of parseSqlLogicTestLines(lines, name)) {
      expect(record.location.line).toBeGreaterThan(0);
      records++;
    }
  } finally {
    lines.close();
    input.destroy();
  }
  expect(records).toBe(expectedRecords);
}

async function sha256(file: URL): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(file)) hash.update(chunk as Buffer);
  return hash.digest("hex");
}

function readJson(name: string): unknown {
  return JSON.parse(readFileSync(new URL(name, corpusDirectory), "utf8"));
}

function sum(
  sources: readonly CorpusSource[],
  key:
    | "sourceRecords"
    | "standardIncludedRecords"
    | "fullIncludedRecords"
    | "allProfileExcludedRecords"
    | "standardOnlyExcludedRecords",
): number {
  return sources.reduce((total, source) => total + source[key], 0);
}
