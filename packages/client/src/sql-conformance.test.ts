import {
  attachDatabaseWorker,
  MinnowDatabase,
  MinnowDatabaseClient,
  type ClientTransport,
  type RpcScope,
} from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage";
import { describe, expect, it } from "vitest";
import rawMatrix from "../../core/sql-feature-matrix.json";
import { Minnow } from "./db.js";

interface MatrixFeature {
  id: string;
  status: "supported" | "unsupported";
  example: string;
  setup?: string[];
  params?: Array<string | number | boolean | null>;
  error?: string;
}

const features = (rawMatrix as { features: MatrixFeature[] }).features;

type FixtureDriver = MinnowDatabase | MinnowDatabaseClient;

async function populate(database: FixtureDriver): Promise<void> {
  await database.createTable({
    name: "rows",
    columns: [
      { name: "region", type: "string", nullable: true },
      { name: "amount", type: "number" },
      { name: "active", type: "boolean", nullable: true },
      { name: "joined", type: "datetime", nullable: true },
    ],
  });
  await database.insertBatch("rows", [
    { region: "west", amount: 10, active: true, joined: new Date("2026-01-02T00:00:00Z") },
    { region: "west", amount: 6, active: false, joined: new Date("2025-12-30T00:00:00Z") },
    { region: "east", amount: 3, active: true, joined: new Date("2026-02-01T00:00:00Z") },
    { region: null, amount: 8, active: true, joined: null },
  ]);
  await database.createTable({
    name: "dims",
    columns: [
      { name: "region", type: "string" },
      { name: "label", type: "string" },
      { name: "amount", type: "number" },
    ],
  });
  await database.insertBatch("dims", [
    { region: "west", label: "West Coast", amount: 1 },
    { region: "north", label: "North", amount: 2 },
  ]);
  await database.createTable({
    name: "keyed",
    uniqueKey: "name",
    columns: [
      { name: "name", type: "string" },
      { name: "score", type: "number" },
      { name: "bonus", type: "number", nullable: true },
    ],
  });
  await database.insertBatch("keyed", [
    { name: "x", score: 1, bonus: null },
    { name: "y", score: -1, bonus: null },
  ]);
}

async function fixture(): Promise<{
  client: Minnow<Record<string, never>>;
  database: MinnowDatabase;
}> {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    rowsPerBlock: 8,
    compression: "raw",
  });
  await populate(database);
  return { client: new Minnow(database), database };
}

function createBoundary(): { clientSide: ClientTransport; workerSide: RpcScope } {
  const clientListeners: Array<(event: MessageEvent<unknown>) => void> = [];
  const workerListeners: Array<(event: MessageEvent<unknown>) => void> = [];
  let chain = Promise.resolve();
  const deliver = (
    listeners: Array<(event: MessageEvent<unknown>) => void>,
    message: unknown,
  ): void => {
    const data = structuredClone(message);
    chain = chain.then(() => {
      for (const listener of listeners) listener({ data } as MessageEvent<unknown>);
    });
  };
  return {
    clientSide: {
      postMessage: (message) => deliver(workerListeners, message),
      addEventListener: (type, listener) => {
        if (type === "message") clientListeners.push(listener);
      },
    },
    workerSide: {
      postMessage: (message) => deliver(clientListeners, message),
      addEventListener: (_type, listener) => workerListeners.push(listener),
    },
  };
}

async function workerFixture(): Promise<{
  client: Minnow<Record<string, never>>;
  database: MinnowDatabaseClient;
}> {
  const { clientSide, workerSide } = createBoundary();
  attachDatabaseWorker(workerSide);
  const database = new MinnowDatabaseClient(clientSide, { store: { kind: "memory" } });
  await populate(database);
  return { client: new Minnow(database), database };
}

async function run(feature: MatrixFeature): Promise<unknown> {
  const { client } = await fixture();
  try {
    for (const statement of feature.setup ?? []) await client.execute(statement);
    const result = await client.execute(feature.example, feature.params);
    if (result.kind === "transaction" && result.action === "begin") {
      await client.execute("ROLLBACK");
    }
    return result;
  } finally {
    await client.close();
  }
}

async function runInWorker(feature: MatrixFeature): Promise<unknown> {
  const { client, database } = await workerFixture();
  try {
    for (const statement of feature.setup ?? []) await client.execute(statement);
    const result = await client.execute(feature.example, feature.params);
    if (result.kind === "transaction" && result.action === "begin") {
      await client.execute("ROLLBACK");
    }
    return result;
  } finally {
    await client.close();
    await database.close();
  }
}

describe("client SQL feature-matrix conformance", () => {
  for (const feature of features.filter(({ status }) => status === "supported")) {
    it(`executes supported ${feature.id}`, async () => {
      await expect(run(feature)).resolves.toBeDefined();
    });
  }

  for (const feature of features.filter(({ status }) => status === "unsupported")) {
    it(`preserves the rejection for ${feature.id}`, async () => {
      await expect(run(feature)).rejects.toThrow(feature.error);
    });
  }

  it("preserves the complete matrix across the worker boundary", async () => {
    for (const feature of features) {
      if (feature.status === "supported") {
        await expect(runInWorker(feature), feature.id).resolves.toBeDefined();
      } else {
        await expect(runInWorker(feature), feature.id).rejects.toThrow(feature.error);
      }
    }
  });
});
