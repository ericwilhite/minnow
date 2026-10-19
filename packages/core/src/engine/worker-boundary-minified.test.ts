/**
 * Error identity across the worker boundary must not depend on class names surviving the
 * application's bundler. The client is bundled and minified with esbuild — class identifiers
 * become `h`, `W`, … while the literal `name` each error stamps on its instances stays — and
 * every error class the engine can throw is round-tripped through that minified client: it
 * must arrive with the bundle's own prototype, its fields, and the same classification the
 * unminified module gives. A second test checks that every error class the public barrels
 * export is in that table, so a new class cannot ship unregistered.
 */
import { build } from "esbuild";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as core from "../index.js";
import * as storage from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import type { MinnowDatabaseClient } from "./client.js";
import { classifyError } from "./errors.js";
import { exposeDatabase } from "./worker-server.js";
import { createBoundary, faultyStore } from "./client-audit-harness.js";

type ErrorFactory = () => Error;

/** One instance of every error class the public barrels export, as the engine would throw it. */
const factories: Record<string, ErrorFactory> = {
  BlockReadBatchTooLargeError: () => new storage.BlockReadBatchTooLargeError(10, 5),
  CompactionBacklogError: () => new core.CompactionBacklogError("t", 5, 4),
  CompactionJobCancelledError: () => new core.CompactionJobCancelledError("job"),
  CompactionJobConflictError: () => new storage.CompactionJobConflictError("job", 1, 2),
  CompactionMemoryBudgetError: () => new core.CompactionMemoryBudgetError(1, 2),
  CompactionWriteAmplificationError: () => new core.CompactionWriteAmplificationError(2, 1),
  ConnectionLostError: () => new storage.ConnectionLostError("lost"),
  DatabaseReadBacklogError: () => new core.DatabaseReadBacklogError(256),
  DatabaseStoreUnavailableError: () =>
    new core.DatabaseStoreUnavailableError("opfs", "db", "cannot open"),
  DatabaseWorkerFailedError: () => new core.DatabaseWorkerFailedError("error", "failed"),
  DatabaseWorkerOutcomeUnknownError: () =>
    new core.DatabaseWorkerOutcomeUnknownError("insertBatch", "req"),
  DatabaseWorkerTimeoutError: () => new core.DatabaseWorkerTimeoutError("query", 10),
  GarbageCollectionJobConflictError: () =>
    new storage.GarbageCollectionJobConflictError("job", 1, 2),
  IndexedDbSchemaUpgradeBlockedError: () =>
    new storage.IndexedDbSchemaUpgradeBlockedError("db", 1, 2),
  LeaseConflictError: () => new storage.LeaseConflictError("lease", 1, 2),
  LeaseExpiredError: () =>
    new storage.LeaseExpiredError("lease", "2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z"),
  LeaseOwnerConflictError: () => new storage.LeaseOwnerConflictError("lease", "a", "b"),
  LiveQueryLimitError: () => new core.LiveQueryLimitError("set", 1),
  MaintenanceBacklogError: () => new core.MaintenanceBacklogError(1, null),
  MissingKeyError: () => new core.MissingKeyError("t", "id", 1),
  OpfsCoordinationError: () => new storage.OpfsCoordinationError("leader-unavailable", "m"),
  OpfsDatabaseInUseError: () => new storage.OpfsDatabaseInUseError("db"),
  OpfsUncertainOutcomeError: () => new storage.OpfsUncertainOutcomeError("writeTransaction"),
  OriginPersistenceRequiredError: () => new storage.OriginPersistenceRequiredError("denied"),
  PostingBuildConflictError: () => new storage.PostingBuildConflictError("b", "o", "r"),
  QueryMemoryBudgetError: () => new core.QueryMemoryBudgetError("sort", 1, 2, 3),
  SchemaConflictError: () => new storage.SchemaConflictError(1, 2),
  SnapshotImportConflictError: () => new storage.SnapshotImportConflictError("i", "o", "m"),
  SnapshotManifestMissingError: () => new storage.SnapshotManifestMissingError(3),
  SqlCompileError: () => new core.SqlCompileError("bad", 0, 1),
  StorageConnectionClosedError: () =>
    new storage.StorageConnectionClosedError("OPFS", "db", "superseded"),
  StorageCorruptionError: () => new storage.StorageCorruptionError("opfs", "wal", "bad"),
  StorageFormatVersionError: () =>
    new storage.StorageFormatVersionError("opfs", "wal", 5, 6, "older"),
  StorageResourceLimitError: () => new storage.StorageResourceLimitError("lease", 2, 1),
  StorageUnresponsiveError: () => new storage.StorageUnresponsiveError("IndexedDB", "db", 100),
  TableInUseError: () => new storage.TableInUseError("t", "transaction", "owner"),
  TableRecordConflictError: () => new storage.TableRecordConflictError("t", 1, 2),
  TempOwnerConflictError: () => new storage.TempOwnerConflictError("o", 1, 2),
  TransactionExpiredError: () => new core.TransactionExpiredError(),
  TransactionRecordConflictError: () => new storage.TransactionRecordConflictError("t", 1, 2),
  UniqueConstraintError: () => new core.UniqueConstraintError("t", "id", 1),
  UniqueIndexCoverageError: () => new storage.UniqueIndexCoverageError("t", "idx"),
  UniqueKeyBuildConflictError: () => new storage.UniqueKeyBuildConflictError("b", "r"),
  UniqueKeyConflictError: () => new storage.UniqueKeyConflictError("t", "k"),
  UnknownOutcomeError: () => new storage.UnknownOutcomeError("unknown"),
  UnknownTableError: () => new core.UnknownTableError("t"),
  VisibleSegmentCursorStaleError: () => new core.VisibleSegmentCursorStaleError("t", "a", "b"),
  WriteAdmissionStalledError: () => new core.WriteAdmissionStalledError(1, "this-context"),
  WriteConflictError: () => new storage.WriteConflictError(1, 2),
  WriteNotPublishedError: () =>
    new core.WriteNotPublishedError(new storage.OpfsUncertainOutcomeError("reserveRowIds")),
};

function exportedErrorClasses(module: Record<string, unknown>): string[] {
  return Object.entries(module)
    .filter(([, value]) => typeof value === "function" && value.prototype instanceof Error)
    .map(([name]) => name);
}

interface MinifiedModule {
  MinnowDatabaseClient: typeof MinnowDatabaseClient;
  classifyError: typeof classifyError;
  classes: Record<string, new (...args: never[]) => Error>;
}

let directory: string;
let minified: MinifiedModule;

beforeAll(async () => {
  directory = mkdtempSync(join(tmpdir(), "minnow-minified-client-"));
  const entry = join(directory, "entry.ts");
  const names = Object.keys(factories).sort();
  writeFileSync(
    entry,
    [
      `export { MinnowDatabaseClient } from ${JSON.stringify(new URL("./client.ts", import.meta.url).pathname)};`,
      `export { classifyError } from ${JSON.stringify(new URL("./errors.ts", import.meta.url).pathname)};`,
      `import * as core from ${JSON.stringify(new URL("../index.ts", import.meta.url).pathname)};`,
      `import * as storage from ${JSON.stringify(new URL("../storage/index.ts", import.meta.url).pathname)};`,
      `export const classes = { ${names
        .map((name) => `${name}: (core as any).${name} ?? (storage as any).${name}`)
        .join(", ")} };`,
    ].join("\n"),
  );
  const outfile = join(directory, "client.min.mjs");
  await build({
    entryPoints: [entry],
    bundle: true,
    minify: true,
    format: "esm",
    platform: "neutral",
    target: "es2022",
    outfile,
    logLevel: "silent",
  });
  minified = (await import(pathToFileURL(outfile).href)) as MinifiedModule;
}, 60_000);

afterAll(() => {
  rmSync(directory, { recursive: true, force: true });
});

function connect(fault: (method: string) => Error | undefined) {
  const boundary = createBoundary();
  let armed = false;
  const { store } = faultyStore(async (method, _args, run) => {
    if (armed) {
      const error = fault(method);
      if (error !== undefined) throw error;
    }
    return run();
  });
  const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  exposeDatabase(database, boundary.workerSide);
  const client = new minified.MinnowDatabaseClient(boundary.clientSide, {
    requestTimeoutMs: 5_000,
  });
  return {
    client,
    arm: () => {
      armed = true;
    },
  };
}

describe("a minified client rehydrates every engine error by its literal name", () => {
  it("minification did mangle the class identifiers", () => {
    const identifiers = Object.values(minified.classes).map((constructor) => constructor.name);
    expect(identifiers.some((identifier) => identifier.length <= 2)).toBe(true);
  });

  it("every error class exported from the public barrels has a factory here", () => {
    const exported = new Set([...exportedErrorClasses(core), ...exportedErrorClasses(storage)]);
    expect([...exported].sort()).toEqual(Object.keys(factories).sort());
  });

  for (const [name, make] of Object.entries(factories)) {
    it(`${name} crosses with the bundle's prototype, fields, and classification`, async () => {
      const { client, arm } = connect((method) =>
        method === "writeTransaction" ? make() : undefined,
      );
      await client.createTable({
        name: "t",
        columns: [{ name: "id", type: "number" }],
        uniqueKey: "id",
      });
      arm();
      const error = await client.insert("t", { id: 1 }).catch((caught: unknown) => caught);
      expect(error, name).toBeInstanceOf(Error);
      expect((error as Error).name).toBe(name);
      expect(error, `${name} prototype`).toBeInstanceOf(minified.classes[name] as never);
      const original = make();
      for (const key of Object.keys(original)) {
        if (key === "name" || key === "cause") continue;
        expect((error as Record<string, unknown>)[key], `${name}.${key}`).toEqual(
          (original as unknown as Record<string, unknown>)[key],
        );
      }
      expect(minified.classifyError(error)).toEqual(classifyError(original));
      await client.close().catch(() => undefined);
    });
  }
});

describe("classifyError recognises an error by name when its prototype is foreign", () => {
  for (const [name, make] of Object.entries(factories)) {
    it(name, () => {
      const original = make();
      // A second copy of the module: same name and fields, a plain Error prototype.
      const foreign = new Error(original.message);
      for (const key of Object.keys(original)) {
        Object.defineProperty(foreign, key, {
          value: (original as unknown as Record<string, unknown>)[key],
          writable: true,
          configurable: true,
          enumerable: key !== "name",
        });
      }
      if (original.cause !== undefined) {
        Object.defineProperty(foreign, "cause", { value: original.cause, configurable: true });
      }
      expect(Object.getPrototypeOf(foreign)).toBe(Error.prototype);
      expect(classifyError(foreign)).toEqual(classifyError(original));
    });
  }
});
