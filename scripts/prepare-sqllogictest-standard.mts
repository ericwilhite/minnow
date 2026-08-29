import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { basename, resolve } from "node:path";

import {
  parseSqlLogicTest,
  type SqlLogicQuery,
  type SqlLogicRecord,
} from "../packages/core/src/testing/sqllogictest.js";
import { ensureSqlLogicCorpus, sqlLogicCorpusManifest } from "./lib/sqllogictest-corpus.mts";

const explicitSources = argumentValues("--source");
const corpusDirectory = explicitSources.length === 0 ? await ensureSqlLogicCorpus() : undefined;
const sourcePaths =
  explicitSources.length > 0
    ? explicitSources.map((source) => resolve(source))
    : sqlLogicCorpusManifest.standardSources.map((source) =>
        resolve(corpusDirectory ?? "", source.path),
      );
const outputDirectory = resolve(
  argument("--output-directory", "packages/core/testdata/sqllogictest"),
);
const ledgerPath = resolve(
  argument("--ledger", "packages/core/testdata/sqllogictest/standard-exclusions.json"),
);
const sourceSummaries: Array<{
  source: string;
  sourceSha256: string;
  sourceRecords: number;
  standardIncludedRecords: number;
  fullIncludedRecords: number;
  allProfileExcludedRecords: number;
  standardOnlyExcludedRecords: number;
  standardOutput: string;
  standardOutputSha256: string;
  fullOutput: string;
  fullOutputSha256: string;
}> = [];
const excluded: Array<{
  source: string;
  line: number;
  kind: string;
  scope: "all-profiles" | "standard-only";
  reason: string;
}> = [];
/** Small, formerly pathological families that make join planning part of every local test run. */
const STANDARD_JOIN_REGRESSIONS = new Set(["join3", "join-17-1"]);
for (const sourcePath of sourcePaths) {
  const sourceName = basename(sourcePath);
  const manifestEntry = sqlLogicCorpusManifest.standardSources.find(
    (entry) => basename(entry.path) === sourceName,
  );
  if (manifestEntry === undefined)
    throw new Error(`source is not pinned in upstream.json: ${sourceName}`);
  const source = readFileSync(sourcePath, "utf8");
  const sourceHash = createHash("sha256").update(source).digest("hex");
  if (sourceHash !== manifestEntry.sha256) {
    throw new Error(
      `${sourceName} checksum mismatch: expected ${manifestEntry.sha256}, received ${sourceHash}`,
    );
  }
  const records = parseSqlLogicTest(source, manifestEntry.path);
  const standardIncluded: SqlLogicRecord[] = [];
  const fullIncluded: SqlLogicRecord[] = [];
  const fileExcluded: typeof excluded = [];
  for (const record of records) {
    fullIncluded.push(record);
    const standardReason = standardOnlyReason(record);
    if (standardReason === undefined) standardIncluded.push(record);
    else {
      fileExcluded.push({
        source: manifestEntry.path,
        line: record.location.line,
        kind: record.kind,
        scope: "standard-only",
        reason: standardReason,
      });
    }
  }
  excluded.push(...fileExcluded);
  const standardOutput = `standard-${sourceName}`;
  const standardPath = resolve(outputDirectory, standardOutput);
  const standardOutputSha256 = writeSelectedFile(
    standardPath,
    manifestEntry.path,
    sourceHash,
    standardIncluded,
    records.length - standardIncluded.length,
  );
  const fullOutput =
    standardIncluded.length === fullIncluded.length ? standardOutput : `full-${sourceName}`;
  let fullOutputSha256 = standardOutputSha256;
  if (fullOutput !== standardOutput) {
    fullOutputSha256 = writeSelectedFile(
      resolve(outputDirectory, fullOutput),
      manifestEntry.path,
      sourceHash,
      fullIncluded,
      records.length - fullIncluded.length,
    );
  }
  const allProfileExcluded = fileExcluded.filter(
    (exclusion) => exclusion.scope === "all-profiles",
  ).length;
  const standardOnlyExcluded = fileExcluded.length - allProfileExcluded;
  sourceSummaries.push({
    source: manifestEntry.path,
    sourceSha256: sourceHash,
    sourceRecords: records.length,
    standardIncludedRecords: standardIncluded.length,
    fullIncludedRecords: fullIncluded.length,
    allProfileExcludedRecords: allProfileExcluded,
    standardOnlyExcludedRecords: standardOnlyExcluded,
    standardOutput,
    standardOutputSha256,
    fullOutput,
    fullOutputSha256,
  });
  console.log(
    `Prepared ${String(standardIncluded.length)} standard / ${String(fullIncluded.length)} full records from ${sourceName}; ${String(fileExcluded.length)} exclusions.`,
  );
}
writeFileSync(
  ledgerPath,
  `${JSON.stringify(
    {
      schemaVersion: 1,
      revision: sqlLogicCorpusManifest.revision,
      sources: sourceSummaries,
      sourceRecords: sourceSummaries.reduce((sum, source) => sum + source.sourceRecords, 0),
      standardIncludedRecords: sourceSummaries.reduce(
        (sum, source) => sum + source.standardIncludedRecords,
        0,
      ),
      fullIncludedRecords: sourceSummaries.reduce(
        (sum, source) => sum + source.fullIncludedRecords,
        0,
      ),
      allProfileExcludedRecords: sourceSummaries.reduce(
        (sum, source) => sum + source.allProfileExcludedRecords,
        0,
      ),
      standardOnlyExcludedRecords: sourceSummaries.reduce(
        (sum, source) => sum + source.standardOnlyExcludedRecords,
        0,
      ),
      exclusions: excluded,
    },
    null,
    2,
  )}\n`,
);
console.log(`${String(excluded.length)} explicit exclusions recorded at ${ledgerPath}.`);

function writeSelectedFile(
  path: string,
  sourcePath: string,
  sourceHash: string,
  records: readonly SqlLogicRecord[],
  excludedRecords: number,
): string {
  const header = [
    "# Generated by scripts/prepare-sqllogictest-standard.mts; do not edit by hand.",
    `# Source: ${sqlLogicCorpusManifest.mirrorUrl}/blob/${sqlLogicCorpusManifest.revision}/${sourcePath}`,
    `# Source SHA-256: ${sourceHash}`,
    `# Included records: ${String(records.length)}; excluded records: ${String(excludedRecords)}.`,
    "",
  ].join("\n");
  const serialized = records.map(serializeRecord).join("");
  // Records are separated by one blank line, but the final record needs only its terminating
  // newline. Keeping the separator after EOF makes `git diff --check` report generated files as
  // carrying a new blank line.
  const output = header + (serialized.endsWith("\n\n") ? serialized.slice(0, -1) : serialized);
  writeFileSync(path, output);
  return createHash("sha256").update(output).digest("hex");
}

function standardOnlyReason(record: SqlLogicRecord): string | undefined {
  if (
    record.kind === "query" &&
    record.label?.startsWith("join") === true &&
    !STANDARD_JOIN_REGRESSIONS.has(record.label)
  ) {
    return "exhaustive multi-way join permutations run in the full profile";
  }
  return undefined;
}

function serializeRecord(record: SqlLogicRecord): string {
  const provenance = `# upstream-line: ${String(record.location.line)}\n`;
  if (record.kind === "statement") {
    return `${provenance}${conditions(record)}statement ${record.expectation}\n${record.sql}\n\n`;
  }
  if (record.kind === "query") {
    const label = record.label === undefined ? "" : ` ${record.label}`;
    const expected =
      record.expected.kind === "hash"
        ? `${String(record.expected.valueCount)} values hashing to ${record.expected.hash}`
        : record.expected.values.join("\n");
    return (
      `${provenance}${conditions(record)}query ${record.types.join("")} ${record.sortMode}${label}\n` +
      `${record.sql}\n----\n${expected}\n\n`
    );
  }
  if (record.kind === "hash-threshold") {
    return `${provenance}hash-threshold ${String(record.valueCount)}\n\n`;
  }
  return `${provenance}halt\n\n`;
}

function conditions(
  record: SqlLogicQuery | Extract<SqlLogicRecord, { kind: "statement" }>,
): string {
  return record.conditions.map((condition) => `${condition.kind} ${condition.engine}\n`).join("");
}

function argument(name: string, fallback?: string): string {
  const index = process.argv.indexOf(name);
  const value = index < 0 ? fallback : process.argv[index + 1];
  if (value === undefined || value.startsWith("--")) {
    throw new Error(`${name} requires a value`);
  }
  return value;
}

function argumentValues(name: string): string[] {
  const values: string[] = [];
  for (let index = 0; index < process.argv.length; index++) {
    if (process.argv[index] !== name) continue;
    const value = process.argv[index + 1];
    if (value === undefined || value.startsWith("--")) throw new Error(`${name} requires a value`);
    values.push(value);
    index++;
  }
  return values;
}
