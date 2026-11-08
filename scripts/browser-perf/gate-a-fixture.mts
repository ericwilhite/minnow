/** Independently generated seed-keyed file-store image, copied into OPFS outside measurement.
 * Immutable completed caches live beneath os.tmpdir(). Never opens an existing cache for writes.
 */
import { createHash } from "node:crypto";
import {
  createReadStream,
  closeSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import { FileStoreSession } from "../../packages/core/src/next/storage/file-store.js";
import { openExtentSpace } from "../../packages/core/src/next/storage/space.js";
import { GroupWriter } from "../../packages/core/src/next/page/writer.js";
import type { ExtentRef } from "../../packages/core/src/next/storage/extents.js";
import {
  countingBudget,
  countingMemory,
  jsonReachable,
} from "../../packages/core/src/next/storage/space-test-support.js";
import { openFileSet } from "../node-sync-file.mjs";
import { wideSpec } from "./page/wide-data.js";
import { typeOf, vectorOf } from "./page/wide-vectors.js";
import { fixtureReplacer } from "./page/fixture-json.js";

export const FIXTURE_VERSION = 1;
export const IMAGE_FILES = ["data", "log", "root-0", "root-1", "groups.json"] as const;
export interface FixtureManifest {
  version: 1;
  seed: number;
  exportedRows: number;
  groupRows: number;
  columns: 200;
  generatedGroups: number;
  sourceHash: string;
  files: Array<{ name: string; bytes: number; sha256: string }>;
  hash: string;
}
export async function fileHash(file: string): Promise<string> {
  const digest = createHash("sha256");
  for await (const part of createReadStream(file)) digest.update(part as Buffer);
  return digest.digest("hex");
}
function manifestHash(m: Omit<FixtureManifest, "hash">): string {
  return createHash("sha256").update(JSON.stringify(m)).digest("hex");
}
/** Content fingerprint, including writer/format/storage dependencies; exec-only tuning reuses cache. */
export function fixtureSourceHash(): string {
  const tracked = execFileSync(
    "git",
    [
      "ls-files",
      "packages/core/src/next/page",
      "packages/core/src/next/format",
      "packages/core/src/next/storage",
      "packages/core/src/next/hash",
      "packages/core/src/next/types.ts",
      "packages/core/src/next/vector",
      "scripts/browser-perf/page/wide-data.ts",
      "scripts/browser-perf/page/wide-vectors.ts",
      "scripts/browser-perf/page/fixture-json.ts",
    ],
    { encoding: "utf8" },
  )
    .trim()
    .split("\n")
    .filter((file) => file && !file.endsWith(".test.ts"))
    .sort();
  const files = [
    ...new Set([
      ...tracked,
      "scripts/browser-perf/page/fixture-json.ts",
      "scripts/browser-perf/page/wide-data.ts",
      "scripts/browser-perf/page/wide-vectors.ts",
      "scripts/browser-perf/gate-a-fixture.mts",
      "scripts/node-sync-file.mts",
    ]),
  ].sort();
  const digest = createHash("sha256").update(String(FIXTURE_VERSION));
  for (const file of files)
    digest.update(file).update("\0").update(readFileSync(file)).update("\0");
  return digest.digest("hex");
}
export async function verifyFixture(
  dir: string,
  expected: { seed: number; exportedRows: number; groupRows: number; sourceHash: string },
): Promise<FixtureManifest> {
  const m = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")) as FixtureManifest;
  const { hash, ...body } = m;
  if (
    m.version !== FIXTURE_VERSION ||
    m.columns !== 200 ||
    m.seed !== expected.seed ||
    m.exportedRows !== expected.exportedRows ||
    m.groupRows !== expected.groupRows ||
    m.sourceHash !== expected.sourceHash ||
    m.generatedGroups !== Math.ceil(m.exportedRows / m.groupRows) + 1 ||
    hash !== manifestHash(body) ||
    JSON.stringify(m.files.map((f) => f.name)) !== JSON.stringify(IMAGE_FILES)
  )
    throw new Error("Fixture identity mismatch");
  for (const file of m.files)
    if (
      statSync(join(dir, file.name)).size !== file.bytes ||
      (await fileHash(join(dir, file.name))) !== file.sha256
    )
      throw new Error(`Fixture hash mismatch: ${file.name}`);
  return m;
}
export async function prepareFixture(
  seed: number,
  exportedRows = 1048576,
  groupRows = 65536,
  sourceHash = fixtureSourceHash(),
) {
  if (
    !Number.isSafeInteger(seed) ||
    seed < 0 ||
    seed > 0xffffffff ||
    !Number.isSafeInteger(exportedRows) ||
    exportedRows < 1 ||
    !Number.isSafeInteger(groupRows) ||
    groupRows < 1 ||
    groupRows > 65536 ||
    exportedRows % groupRows !== 0
  )
    throw new Error("Invalid fixture dimensions or seed");
  const parent = join(
    tmpdir(),
    "minnow-gate-a",
    `seed-${seed}`,
    `${exportedRows}-${groupRows}-${sourceHash}`,
  );
  mkdirSync(parent, { recursive: true });
  const dir = join(parent, "fixture");
  if (existsSync(dir))
    return {
      dir,
      manifest: await verifyFixture(dir, { seed, exportedRows, groupRows, sourceHash }),
      reused: true,
    };
  const lock = join(parent, "build.lock"),
    fd = openSync(lock, "wx");
  let stage: string | undefined;
  try {
    stage = mkdtempSync(join(parent, "building-"));
    const files = openFileSet(stage, true),
      session = new FileStoreSession(files, 1, () => undefined);
    let space: Awaited<ReturnType<typeof openExtentSpace>> | undefined;
    const spec = wideSpec(200),
      types = spec.map(typeOf),
      groups = [];
    const extents: ExtentRef[] = [];
    const rows = exportedRows + groupRows,
      offset = seed % rows;
    try {
      space = await openExtentSpace(session, {
        reachable: jsonReachable,
        budget: countingBudget(Infinity),
        registerCache: () => countingMemory(Infinity),
      });
      for (let first = 0; first < rows; first += groupRows) {
        const transaction = space.beginWrite(`seed ${seed} rows ${first}`);
        const writer = new GroupWriter({
          columns: types.map((type, k) => ({ columnId: k + 1, type })),
          writer: transaction,
        });
        await writer.append(
          spec.map((c, k) => {
            const type = types[k];
            if (!type) throw new Error("Missing fixture type");
            return vectorOf(c, type, first, groupRows, rows, offset);
          }),
          groupRows,
        );
        const made = await writer.finish();
        extents.push(...made.extents);
        groups.push({ rows: groupRows, chunks: made.columns });
        await space.publish(new TextEncoder().encode(JSON.stringify({ extents })), {
          writers: [transaction],
          retired: [],
        });
      }
      writeFileSync(join(stage, "groups.json"), JSON.stringify(groups, fixtureReplacer));
    } finally {
      try {
        await space?.close();
      } finally {
        await session.close();
        for (const file of [files.log, files.data, ...files.roots]) file.close();
      }
    }
    const body: Omit<FixtureManifest, "hash"> = {
      version: 1,
      seed,
      exportedRows,
      groupRows,
      columns: 200,
      generatedGroups: groups.length,
      sourceHash,
      files: [],
    };
    for (const name of IMAGE_FILES)
      body.files.push({
        name,
        bytes: statSync(join(stage, name)).size,
        sha256: await fileHash(join(stage, name)),
      });
    writeFileSync(
      join(stage, "manifest.json"),
      JSON.stringify({ ...body, hash: manifestHash(body) }, null, 2) + "\n",
    );
    renameSync(stage, dir);
    return {
      dir,
      manifest: await verifyFixture(dir, { seed, exportedRows, groupRows, sourceHash }),
      reused: false,
    };
  } finally {
    closeSync(fd);
    rmSync(lock, { force: true });
    if (stage) rmSync(stage, { recursive: true, force: true });
  }
}
