import { afterEach, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { createMinnowDriver } from "./engines/minnow";
import type { DatasetRecord, EngineId } from "./protocol";
import {
  runReferenceSuite,
  measureOnSession,
  type ReferenceQueryDefinition,
} from "./worker/reference-suite";
import { runWriteSuite } from "./worker/write-suite";
import { measureLiveCase, runLiveSuite } from "./worker/live-suite";
import { loadDriver } from "./engines/session";
import { withBenchmarkCleanup } from "./worker/support";
import { runQuery } from "./worker/run-query";

vi.mock("./worker/registry", () => ({ getDataset: vi.fn(async () => record) }));
vi.mock("./engines/session", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./engines/session")>()),
  loadDriver: vi.fn(),
}));
const record: DatasetRecord = {
  id: "audit",
  createdAt: new Date(0).toISOString(),
  scale: 0.1,
  totalRows: 0,
  tableRows: {},
  compression: "raw",
  targetBlockBytes: 1_048_576,
  durability: "relaxed",
  secondaryIndexes: "none",
  engines: {
    minnow: {
      engine: "minnow",
      status: "ready",
      storageName: "audit",
      version: "test",
      buildMs: 0,
      insertMs: 0,
      indexMs: 0,
      storedBytes: 0,
      dataStoredBytes: 0,
      indexStoredBytes: 0,
      persistence: "memory",
    },
  },
};
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.mocked(loadDriver).mockReset();
});
function failingDriver(engine: EngineId) {
  const close = vi.fn(async () => undefined);
  const fail = async (): Promise<never> => {
    throw new Error("injected database failure");
  };
  return {
    driver: {
      id: engine,
      openSession: async () => ({ engine, prepare: fail, close }),
      openWriteSession: async () => ({ engine, createTable: fail, close }),
      openLiveSession: async () => ({
        engine,
        createTable: fail,
        dropTable: fail,
        insert: fail,
        subscribe: fail,
        close,
      }),
      loadDataset: fail,
      deleteDataset: fail,
    },
    close,
  };
}
it.each([runReferenceSuite, runWriteSuite, runLiveSuite])(
  "cannot report a passing suite after database execution fails",
  async (run) => {
    vi.stubGlobal("self", { postMessage: vi.fn() });
    const { driver, close } = failingDriver("minnow");
    vi.mocked(loadDriver).mockResolvedValue(driver);
    const result = await run("audit", { datasetId: "audit", engines: ["minnow"] });
    expect(result.passed).toBe(false);
    const reports = "queries" in result ? result.queries : result.cases;
    expect(
      reports.every((report) =>
        report.engines.every(
          (value) => !value.verified && value.error?.includes("injected database failure"),
        ),
      ),
    ).toBe(true);
    expect(close).toHaveBeenCalledOnce();
  },
);
it("does not credit a missing engine as passing", async () => {
  vi.stubGlobal("self", { postMessage: vi.fn() });
  vi.mocked(loadDriver).mockRejectedValue(new Error("module unavailable"));
  expect(
    (await runReferenceSuite("audit", { datasetId: "audit", engines: ["minnow"] })).passed,
  ).toBe(false);
});
it("reports cleanup failures after trying every subscription and table", async () => {
  const closed: string[] = [];
  const fail = async () => {
    throw new Error("injected subscribe failure");
  };
  await expect(
    measureLiveCase(
      {
        engine: "minnow",
        createTable: async () => undefined,
        insert: async () => undefined,
        subscribe: fail,
        close: async () => undefined,
        dropTable: async (table) => {
          closed.push(table);
          throw new Error("drop failed");
        },
      },
      { id: "audit", name: "audit", subscriptions: 1, affected: 1 },
      "audit",
    ),
  ).rejects.toBeInstanceOf(AggregateError);
  expect(closed).toEqual(["bl_audit_w", "bl_audit_q"]);
});
it("drops Minnow sample tables and closes the database before the store", async () => {
  const store = new MemoryBlockStore();
  const closeStore = vi.spyOn(store, "close");
  const driver = createMinnowDriver({
    id: "minnow",
    persistence: "memory",
    openStore: async () => store,
    deleteDataset: async () => undefined,
  });
  const session = await driver.openWriteSession(record);
  const target = await session.createTable({
    name: "sample",
    primaryKey: "id",
    columns: [{ name: "id", type: "number" }],
  });
  const insert = target.prepareInsert({ rowCount: 1, columns: { id: [1] } });
  await insert();
  await target.drop();
  expect(await store.getTableByName("sample")).toBeUndefined();
  await session.close();
  expect(closeStore).toHaveBeenCalledOnce();
  await expect(insert()).rejects.toThrow(/closed/i);
});

it("retains both execution and cleanup errors", async () => {
  const operationError = new Error("operation failed");
  const cleanupError = new Error("cleanup failed");
  const error = await withBenchmarkCleanup(
    async () => {
      throw operationError;
    },
    async () => {
      throw cleanupError;
    },
  ).catch((error: unknown) => error);
  expect(error).toBeInstanceOf(AggregateError);
  expect((error as AggregateError).errors).toEqual([operationError, cleanupError]);
});

it("retains a write failure when table cleanup also fails and closes the session", async () => {
  vi.stubGlobal("self", { postMessage: vi.fn() });
  const operationError = new Error("write failed");
  const cleanupError = new Error("drop failed");
  const close = vi.fn(async () => undefined);
  const drop = vi.fn(async () => {
    throw cleanupError;
  });
  const { driver } = failingDriver("minnow");
  const writeDriver = {
    ...driver,
    openWriteSession: async () => ({
      engine: "minnow" as const,
      close,
      createTable: async () => ({
        prepareInsert: () => async () => {
          throw operationError;
        },
        prepareUpdate: () => async () => {
          throw operationError;
        },
        prepareUpsert: () => async () => {
          throw operationError;
        },
        readAll: async () => [],
        drop,
      }),
    }),
  };
  vi.mocked(loadDriver).mockResolvedValue(writeDriver);
  const error = await runWriteSuite("audit", { datasetId: "audit", engines: ["minnow"] }).catch(
    (error: unknown) => error,
  );
  expect(error).toBeInstanceOf(AggregateError);
  const failures = (error as AggregateError).errors as unknown[];
  expect(failures[0]).toBe(operationError);
  expect(failures[1]).toBeInstanceOf(AggregateError);
  expect((failures[1] as AggregateError).errors).toEqual([cleanupError]);
  expect(drop).toHaveBeenCalledOnce();
  expect(close).toHaveBeenCalledOnce();
});

it("fails when a declared live engine loses its subscription driver", async () => {
  vi.stubGlobal("self", { postMessage: vi.fn() });
  const { driver } = failingDriver("minnow");
  const { openLiveSession: _removed, ...withoutLive } = driver;
  void _removed;
  vi.mocked(loadDriver).mockResolvedValue(withoutLive);
  const result = await runLiveSuite("audit", { datasetId: "audit", engines: ["minnow"] });
  expect(result.passed).toBe(false);
  expect(result.coverageByEngine.minnow).toEqual({
    expected: 4,
    attempted: 4,
    supported: 0,
    verified: 0,
    failed: 4,
  });
});

it.each(["sqlite", "pglite"] as const)(
  "declares %s live queries unsupported without attempting to load a driver",
  async (engine) => {
    vi.stubGlobal("self", { postMessage: vi.fn() });
    const result = await runLiveSuite("audit", { datasetId: "audit", engines: [engine] });
    expect(loadDriver).not.toHaveBeenCalled();
    expect(result.passed).toBe(true);
    expect(result.coverageByEngine[engine]).toEqual({
      expected: 0,
      attempted: 4,
      supported: 0,
      verified: 0,
      failed: 0,
    });
  },
);

const cleanupQuery: ReferenceQueryDefinition = {
  id: "cleanup",
  name: "cleanup",
  complexity: "simple",
  workload: "oltp",
  sql: "SELECT 1 AS n",
  columns: ["n"],
  project: (row) => [row.n],
  tables: [],
  expectedRows: 1,
  baseline: () => [[1]],
  oracle: () => [[1]],
};
it("keeps execution and asynchronous statement cleanup errors in the read report", async () => {
  const measurement = await measureOnSession(
    {
      engine: "pglite",
      close: async () => undefined,
      prepare: async () => ({
        execute: async () => {
          throw new Error("execution failed");
        },
        close: async () => {
          throw new Error("deallocation failed");
        },
      }),
    },
    cleanupQuery,
    "pglite",
    [[1]],
  );
  expect(measurement.supported).toBe(false);
  expect(measurement.verified).toBe(false);
  expect(measurement.error).toContain("execution failed");
  expect(measurement.error).toContain("deallocation failed");
});
it("does not publish a passing read measurement before asynchronous deallocation completes", async () => {
  let release!: () => void;
  const cleanup = new Promise<void>((resolve) => {
    release = resolve;
  });
  let admitted = false;
  let published = false;
  const pending = measureOnSession(
    {
      engine: "pglite",
      close: async () => undefined,
      prepare: async () => ({
        execute: async () => [{ n: 1 }],
        close: () => {
          admitted = true;
          return cleanup;
        },
      }),
    },
    cleanupQuery,
    "pglite",
    [[1]],
  ).then((result) => {
    published = true;
    return result;
  });
  await vi.waitFor(() => expect(admitted).toBe(true));
  expect(published).toBe(false);
  release();
  expect((await pending).verified).toBe(true);
});

it("retains execution, statement and session cleanup errors in an ad-hoc query report", async () => {
  const { driver } = failingDriver("minnow");
  vi.mocked(loadDriver).mockResolvedValue({
    ...driver,
    openSession: async () => ({
      engine: "minnow",
      prepare: async () => ({
        execute: async () => {
          throw new Error("query execution failed");
        },
        close: async () => {
          throw new Error("statement cleanup failed");
        },
      }),
      close: async () => {
        throw new Error("session cleanup failed");
      },
    }),
  });
  const result = await runQuery({ datasetId: "audit", sql: "SELECT 1", engines: ["minnow"] });
  expect(result.runs[0]?.ok).toBe(false);
  expect(result.runs[0]?.error).toContain("query execution failed");
  expect(result.runs[0]?.error).toContain("statement cleanup failed");
  expect(result.runs[0]?.error).toContain("session cleanup failed");
});
