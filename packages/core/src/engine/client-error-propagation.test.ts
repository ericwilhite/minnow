/**
 * Every typed error survives the worker boundary with its class, fields and cause — from direct
 * calls, write scopes, live subscriptions and init. Faults are injected at the block store so
 * the engine's own translation is exercised, not bypassed.
 */
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "./database.js";
import { MinnowDatabaseClient } from "./client.js";
import { exposeDatabase, attachWorkerHost } from "./worker-server.js";
import { classifyError, DatabaseStoreUnavailableError, UnknownTableError } from "./errors.js";
import {
  OpfsCoordinationError,
  OpfsUncertainOutcomeError,
  SchemaConflictError,
  StorageFormatVersionError,
  WriteConflictError,
} from "../storage/types.js";
import { createBoundary, faultyStore, settled, type Fault } from "./client-audit-harness.js";

function connect(fault: Fault, hide: string[] = []) {
  const boundary = createBoundary();
  const { store, calls } = faultyStore(fault, { hide });
  const database = new MinnowDatabase(store);
  exposeDatabase(database, boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide, { requestTimeoutMs: 5_000 });
  return { client, calls, boundary, database };
}

async function seed(client: MinnowDatabaseClient): Promise<void> {
  await client.createTable({
    name: "t",
    columns: [
      { name: "id", type: "number" },
      { name: "v", type: "string" },
    ],
    uniqueKey: "id",
  });
}

const chained = <T extends Error>(error: T): T => {
  (error as { cause?: unknown }).cause = new TypeError("inner cause", {
    cause: new DOMException("root cause", "QuotaExceededError"),
  });
  return error;
};

describe("direct calls", () => {
  const cases: Array<[string, () => Error, Record<string, unknown>]> = [
    [
      "OpfsUncertainOutcomeError",
      () => chained(new OpfsUncertainOutcomeError("writeTransaction")),
      { method: "writeTransaction" },
    ],
    [
      "OpfsCoordinationError",
      () => chained(new OpfsCoordinationError("leader-unavailable", "writeTransaction")),
      { reason: "leader-unavailable", method: "writeTransaction", backend: "opfs" },
    ],
    [
      "SchemaConflictError",
      () => chained(new SchemaConflictError(1, 2)),
      { expectedEpoch: 1, actualEpoch: 2 },
    ],
    [
      "StorageFormatVersionError",
      () => chained(new StorageFormatVersionError("opfs", "wal", 5, 6, "older")),
      {
        backend: "opfs",
        location: "wal",
        actualVersion: 5,
        supportedVersion: 6,
        relation: "older",
      },
    ],
    [
      "WriteConflictError",
      () => chained(new WriteConflictError(1, 2)),
      { expectedVersion: 1, actualVersion: 2 },
    ],
  ];
  for (const [name, make, fields] of cases) {
    it(`${name} arrives typed with fields and cause from an autocommit insert`, async () => {
      let armed = false;
      const { client } = connect(async (method, _args, run) => {
        if (armed && method === "writeTransaction") throw make();
        return run();
      });
      await seed(client);
      armed = true;
      const error = await client.insert("t", { id: 1, v: "x" }).catch((e: unknown) => e);
      expect(error, name).toBeInstanceOf(Error);
      expect((error as Error).name).toBe(name);
      expect((error as Error).constructor.name, `${name} prototype`).toBe(name);
      expect(error).toMatchObject(fields);
      const cause = (error as Error).cause;
      expect(cause, `${name} cause`).toBeInstanceOf(TypeError);
      expect((cause as Error).cause).toBeInstanceOf(DOMException);
      expect(((cause as Error).cause as DOMException).name).toBe("QuotaExceededError");
      await client.close().catch(() => undefined);
    });
  }

  it("QuotaExceededError (DOMException) arrives as a DOMException and classifies as resource", async () => {
    let armed = false;
    const { client } = connect(async (method, _args, run) => {
      if (armed && method === "writeTransaction") {
        throw new DOMException("no space", "QuotaExceededError");
      }
      return run();
    });
    await seed(client);
    armed = true;
    const error = await client.insert("t", { id: 1, v: "x" }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(DOMException);
    expect((error as DOMException).name).toBe("QuotaExceededError");
    expect(classifyError(error).kind).toBe("resource");
    await client.close().catch(() => undefined);
  });

  it("DatabaseStoreUnavailableError from an auto init arrives typed", async () => {
    const boundary = createBoundary();
    attachWorkerHost(boundary.workerSide, () => {
      throw new DatabaseStoreUnavailableError("opfs", "shop", "cannot open");
    });
    const client = new MinnowDatabaseClient(boundary.clientSide, {
      store: { kind: "auto", name: "shop" },
    });
    const error = await client.ready().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(DatabaseStoreUnavailableError);
    expect(error).toMatchObject({ store: "opfs", databaseName: "shop" });
  });
});

describe("write scopes", () => {
  it("a commit-time OpfsUncertainOutcomeError inside client.write() arrives typed", async () => {
    let armed = false;
    const { client } = connect(
      async (method, _args, run) => {
        if (armed && method === "commitTransaction") {
          throw chained(new OpfsUncertainOutcomeError("commitTransaction"));
        }
        return run();
      },
      ["writeTransaction"],
    );
    await seed(client);
    armed = true;
    const error = await client
      .write(async (s) => {
        await s.insertBatch("t", [{ id: 1, v: "x" }]);
      })
      .catch((e: unknown) => e);
    expect(error).toBeInstanceOf(OpfsUncertainOutcomeError);
    expect(error).toMatchObject({ method: "commitTransaction" });
    expect((error as Error).cause).toBeInstanceOf(TypeError);
    await client.close().catch(() => undefined);
  });

  it("a stage-time error inside client.write() reaches the session call typed", async () => {
    let armed = false;
    const { client } = connect(
      async (method, _args, run) => {
        if (armed && method === "stageTransactionArtifacts") {
          throw chained(
            new OpfsCoordinationError("leader-queue-full", "stageTransactionArtifacts"),
          );
        }
        return run();
      },
      ["writeTransaction"],
    );
    await seed(client);
    armed = true;
    let stageError: unknown;
    const scopeError = await client
      .write(async (s) => {
        // Enough statements to spill the coalesced batch into a durable stage.
        for (let i = 0; i < 70; i += 1) {
          try {
            await s.insertBatch("t", [{ id: i, v: "x".repeat(10) }]);
          } catch (e) {
            stageError = e;
            throw e;
          }
        }
      })
      .catch((e: unknown) => e);
    expect(scopeError).toBeInstanceOf(Error);
    // Either the stage call itself or the scope must carry the typed error somewhere in the chain.
    const chain: unknown[] = [];
    for (let e: unknown = scopeError; e instanceof Error; e = e.cause) chain.push(e);
    const typed = chain.find((e) => e instanceof OpfsCoordinationError) ?? stageError;
    expect(
      typed,
      `scope error chain: ${chain.map((e) => (e as Error).name).join(" <- ")}`,
    ).toBeInstanceOf(OpfsCoordinationError);
    await client.close().catch(() => undefined);
  });
});

describe("live subscriptions", () => {
  it("an error on re-execution reaches onError typed", async () => {
    const { client } = connect(async (_m, _a, run) => run());
    await seed(client);
    await client.insert("t", { id: 1, v: "x" });
    const live = client.liveQueries({ pollIntervalMs: 50 });
    const errors: unknown[] = [];
    let changes = 0;
    const sub = await live.subscribe("SELECT id FROM t ORDER BY id", {
      onChange: () => {
        changes += 1;
      },
      onError: (error) => errors.push(error),
    });
    await client.dropTable("t");
    await settled(200);
    expect(changes).toBeGreaterThanOrEqual(1);
    expect(errors.length, "live onError fired").toBeGreaterThanOrEqual(1);
    expect(errors[0]).toBeInstanceOf(UnknownTableError);
    expect(errors[0]).toMatchObject({ tableName: "t" });
    await sub.close().catch(() => undefined);
    await live.close().catch(() => undefined);
    await client.close().catch(() => undefined);
  });
});
