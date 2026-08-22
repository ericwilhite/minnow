import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  createReadStream,
  createWriteStream,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { basename, relative, resolve, sep } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

export interface SqlLogicCorpusManifest {
  readonly schemaVersion: 1;
  readonly project: string;
  readonly canonicalUrl: string;
  readonly mirrorUrl: string;
  readonly revision: string;
  readonly revisionDate: string;
  readonly archiveUrl: string;
  readonly archiveSha256: string;
  readonly archiveBytes: number;
  readonly archiveRoot: string;
  readonly testFileCount: number;
  readonly standardSources: ReadonlyArray<{ readonly path: string; readonly sha256: string }>;
}

const repositoryRoot = resolve(import.meta.dirname, "../..");
const manifestPath = resolve(repositoryRoot, "packages/core/testdata/sqllogictest/upstream.json");
export const sqlLogicCorpusManifest = JSON.parse(
  readFileSync(manifestPath, "utf8"),
) as SqlLogicCorpusManifest;

export function defaultSqlLogicCorpusDirectory(): string {
  return resolve(repositoryRoot, ".cache/sqllogictest", sqlLogicCorpusManifest.revision, "source");
}

/** Downloads, checksum-verifies, and extracts the immutable upstream archive once. */
export async function ensureSqlLogicCorpus(): Promise<string> {
  const override = process.env.MINNOW_SQLLOGICTEST_DIR;
  if (override !== undefined) {
    const directory = resolve(override);
    verifyCorpusDirectory(directory);
    return directory;
  }

  const directory = defaultSqlLogicCorpusDirectory();
  const marker = resolve(directory, ".minnow-corpus.json");
  if (validMarker(marker)) {
    verifyCorpusDirectory(directory);
    return directory;
  }

  const revisionRoot = resolve(directory, "..");
  const cacheRoot = resolve(revisionRoot, "..");
  mkdirSync(cacheRoot, { recursive: true });
  mkdirSync(revisionRoot, { recursive: true });
  const archive = resolve(revisionRoot, `${sqlLogicCorpusManifest.revision}.tar.gz`);
  if (!existsSync(archive) || (await sha256(archive)) !== sqlLogicCorpusManifest.archiveSha256) {
    await downloadArchive(archive);
  }
  const actualHash = await sha256(archive);
  if (actualHash !== sqlLogicCorpusManifest.archiveSha256) {
    throw new Error(
      `SQLLogicTest archive checksum mismatch: expected ${sqlLogicCorpusManifest.archiveSha256}, received ${actualHash}`,
    );
  }
  if (statSync(archive).size !== sqlLogicCorpusManifest.archiveBytes) {
    throw new Error(
      `SQLLogicTest archive size mismatch: expected ${String(sqlLogicCorpusManifest.archiveBytes)}, received ${String(statSync(archive).size)}`,
    );
  }

  const staging = resolve(revisionRoot, `source-staging-${String(process.pid)}`);
  assertInside(revisionRoot, staging);
  rmSync(staging, { recursive: true, force: true });
  mkdirSync(staging);
  try {
    const extracted = spawnSync("tar", ["-xzf", archive, "--strip-components=1", "-C", staging], {
      encoding: "utf8",
    });
    if (extracted.status !== 0) {
      throw new Error(`Could not extract SQLLogicTest: ${extracted.stderr || extracted.stdout}`);
    }
    const files = listSqlLogicTestFiles(resolve(staging, "test"));
    if (files.length !== sqlLogicCorpusManifest.testFileCount) {
      throw new Error(
        `SQLLogicTest file count mismatch: expected ${String(sqlLogicCorpusManifest.testFileCount)}, received ${String(files.length)}`,
      );
    }
    writeFileSync(
      resolve(staging, ".minnow-corpus.json"),
      `${JSON.stringify(
        {
          revision: sqlLogicCorpusManifest.revision,
          archiveSha256: sqlLogicCorpusManifest.archiveSha256,
          testFileCount: files.length,
        },
        null,
        2,
      )}\n`,
    );
    if (existsSync(directory)) {
      assertInside(revisionRoot, directory);
      rmSync(directory, { recursive: true });
    }
    renameSync(staging, directory);
  } catch (error) {
    rmSync(staging, { recursive: true, force: true });
    throw error;
  }
  return directory;
}

export function listSqlLogicTestFiles(directory: string): string[] {
  const files: string[] = [];
  const visit = (current: string): void => {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      const path = resolve(current, entry.name);
      if (entry.isDirectory()) visit(path);
      else if (entry.isFile() && entry.name.endsWith(".test")) files.push(path);
    }
  };
  visit(directory);
  return files.sort((left, right) => left.localeCompare(right));
}

export async function sha256(file: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(file)) hash.update(chunk as Buffer);
  return hash.digest("hex");
}

async function downloadArchive(destination: string): Promise<void> {
  const temporary = `${destination}.download-${String(process.pid)}`;
  const response = await fetch(sqlLogicCorpusManifest.archiveUrl, { redirect: "follow" });
  if (!response.ok || response.body === null) {
    throw new Error(
      `Could not download SQLLogicTest archive: ${String(response.status)} ${response.statusText}`,
    );
  }
  try {
    await pipeline(Readable.fromWeb(response.body), createWriteStream(temporary, { flags: "wx" }));
    renameSync(temporary, destination);
  } catch (error) {
    rmSync(temporary, { force: true });
    throw error;
  }
}

function validMarker(path: string): boolean {
  if (!existsSync(path)) return false;
  try {
    const marker = JSON.parse(readFileSync(path, "utf8")) as Record<string, unknown>;
    return (
      marker.revision === sqlLogicCorpusManifest.revision &&
      marker.archiveSha256 === sqlLogicCorpusManifest.archiveSha256 &&
      marker.testFileCount === sqlLogicCorpusManifest.testFileCount
    );
  } catch {
    return false;
  }
}

function verifyCorpusDirectory(directory: string): void {
  for (const entry of sqlLogicCorpusManifest.standardSources) {
    const source = resolve(directory, entry.path);
    if (!existsSync(source)) throw new Error(`SQLLogicTest corpus is missing ${entry.path}`);
  }
}

function assertInside(parent: string, candidate: string): void {
  const path = relative(resolve(parent), resolve(candidate));
  if (path.length === 0 || path === ".." || path.startsWith(`..${sep}`)) {
    throw new Error(`Refusing to modify a path outside ${parent}: ${candidate}`);
  }
}

export function describeCorpus(directory: string): string {
  return `${sqlLogicCorpusManifest.project} ${sqlLogicCorpusManifest.revision.slice(0, 12)} at ${basename(directory)}`;
}
