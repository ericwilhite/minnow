import { expect, it } from "vitest";
import {
  OpfsCoordinationError,
  OpfsUncertainOutcomeError,
  column,
  schema,
  table,
} from "../../index.js";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsBlockStore } from "./index.js";

it("classifies exhausted follower catalog reads and resumes migration without losing rows", async () => {
  const options = {
    name: `coordination-${crypto.randomUUID()}`,
    root: new MemoryOpfs().root,
    rpcTimeoutMs: 5,
    dispatchBudgetMs: 500,
  };
  const leader = await OpfsBlockStore.open(options);
  const follower = await OpfsBlockStore.open(options);
  const first = new MinnowDatabase(leader);
  const second = new MinnowDatabase(follower);
  const definition = schema([
    table("items", { id: column.number().unique(), value: column.string() }),
  ]);
  try {
    await first.migrate(definition);
    await first.insert("items", { id: 1, value: "kept" });
    await second.migrate(definition);
    const resume = leader._pauseCoordinationForTests();
    try {
      const error = await second.migrate(definition).catch((error: unknown) => error);
      expect(error).toBeInstanceOf(OpfsCoordinationError);
      expect(error).not.toBeInstanceOf(OpfsUncertainOutcomeError);
      expect(error).toMatchObject({ reason: "leader-unavailable", method: "getCatalogProbe" });
    } finally {
      resume();
    }
    await expect(second.migrate(definition)).resolves.toMatchObject({ steps: [] });
    expect((await second.query("SELECT * FROM items")).rows).toEqual([{ id: 1, value: "kept" }]);
    const refusal = await second
      .migrate(schema([table("items", { id: column.number().unique() })]))
      .catch((error: unknown) => error);
    expect(refusal).toBeInstanceOf(TypeError);
    expect(refusal).not.toBeInstanceOf(OpfsCoordinationError);
    if (!(refusal instanceof Error)) throw new Error("Expected migration refusal");
    expect(refusal.message).toContain("would destroy data");
  } finally {
    await first.close();
    await second.close();
    leader._crashForTests();
    follower._crashForTests();
  }
});

it("boots and migrates two clients concurrently on a cold OPFS catalog", async () => {
  const options = { name: `cold-${crypto.randomUUID()}`, root: new MemoryOpfs().root };
  const stores = await Promise.all([OpfsBlockStore.open(options), OpfsBlockStore.open(options)]);
  const databases = stores.map((store) => new MinnowDatabase(store));
  const definition = schema([
    table("items", { id: column.number().unique(), value: column.string() }),
  ]);
  try {
    await Promise.all(databases.map((database) => database.migrate(definition)));
    for (const database of databases) {
      await expect(database.migrate(definition)).resolves.toMatchObject({ steps: [] });
    }
  } finally {
    await Promise.all(databases.map((database) => database.close()));
    for (const store of stores) store._crashForTests();
  }
});
