import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { DatasetRecord } from "./protocol";
const fake = vi.hoisted(() => ({ create: vi.fn(), exec: vi.fn(), close: vi.fn() }));
vi.mock("./engines/vendored", () => ({
  loadPglite: async () => ({
    PGlite: { create: fake.create },
    types: { TIMESTAMP: 1114, TIMESTAMPTZ: 1184, DATE: 1082, NUMERIC: 1700 },
  }),
}));
const { pgliteDriver } = await import("./engines/pglite");
const record: DatasetRecord = {
  id: "audit",
  createdAt: new Date(0).toISOString(),
  scale: 0.1,
  totalRows: 0,
  tableRows: {},
  compression: "raw",
  targetBlockBytes: 1024,
  durability: "strict",
  secondaryIndexes: "none",
  engines: {},
};
beforeEach(() => {
  fake.create.mockResolvedValue({ closed: false, exec: fake.exec, close: fake.close });
  fake.exec.mockResolvedValue(undefined);
  fake.close.mockResolvedValue(undefined);
});
afterEach(() => vi.resetAllMocks());
it("propagates database-close failures instead of crediting a clean session", async () => {
  const failure = new Error("close I/O failed");
  fake.close.mockRejectedValueOnce(failure);
  const session = await pgliteDriver.openSession(record);
  await expect(session.close()).rejects.toBe(failure);
});
it("awaits prepared-statement deallocation and propagates its failure", async () => {
  const failure = new Error("deallocation I/O failed");
  const session = await pgliteDriver.openSession(record);
  const prepared = await session.prepare("SELECT 1");
  fake.exec.mockRejectedValueOnce(failure);
  await expect(Promise.resolve(prepared.close())).rejects.toBe(failure);
  await session.close();
});
it("retains setup and close failures when loading the dataset fails", async () => {
  const operation = new Error("schema I/O failed");
  const cleanup = new Error("close I/O failed");
  fake.exec.mockRejectedValueOnce(operation);
  fake.close.mockRejectedValueOnce(cleanup);
  const failure: unknown = await pgliteDriver
    .loadDataset({
      record,
      checkCancelled: () => undefined,
      report: () => undefined,
    })
    .catch((error: unknown) => error);
  expect(failure).toBeInstanceOf(AggregateError);
  expect((failure as AggregateError).errors).toEqual([operation, cleanup]);
});
