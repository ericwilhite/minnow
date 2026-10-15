// @vitest-environment happy-dom
/**
 * One commit, one render: a sweep hands every changed live query its result in one turn, so a
 * component reading several queries never renders a state where some reflect the commit and
 * others do not — the rekey case, where a form and a list would otherwise disagree on a key.
 */
import { createElement } from "react";
import { createRoot } from "react-dom/client";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "@minnowdb/core";
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { createLiveQueryManager } from "@minnowdb/core/live";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { attachDatabaseWorker } from "@minnowdb/core/worker-host";
import { useLiveQuery } from "./index.js";

interface Row {
  readonly id: string;
}

async function until(check: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 400; attempt += 1) {
    if (check()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("timed out");
}

type Database = Pick<MinnowDatabase, "execute" | "query" | "liveQueries">;

async function rendersOnce(database: Database): Promise<void> {
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = false;
  await database.execute("CREATE TABLE orders (id TEXT PRIMARY KEY, note TEXT)");
  await database.execute(
    "CREATE TABLE lines (id TEXT PRIMARY KEY, order_id TEXT REFERENCES orders(id))",
  );
  await database.execute("INSERT INTO orders VALUES ('o:1', 'a')");
  await database.execute("INSERT INTO lines VALUES ('l1', 'o:1'), ('l2', 'o:1')");
  const manager = createLiveQueryManager(database);
  const watch = (sql: string) =>
    manager.watch<Row>({
      query: sql,
      execute: async () => (await database.query(sql)).rows as unknown as Row[],
      decode: (result) => result.rows as unknown as Row[],
    });
  const form = watch("SELECT id FROM orders ORDER BY id");
  const list = watch("SELECT order_id AS id FROM lines ORDER BY id");
  const renders: string[] = [];
  function View(): ReturnType<typeof createElement> {
    const order = useLiveQuery(form);
    const lines = useLiveQuery(list);
    renders.push(
      `${order.rows.map((row) => row.id).join(",")}|${lines.rows.map((row) => row.id).join(",")}`,
    );
    return createElement("span");
  }
  const root = createRoot(document.createElement("div"));
  root.render(createElement(View));
  await until(() => renders.at(-1) === "o:1|o:1,o:1");
  const before = renders.length;
  await database.execute("UPDATE orders SET id = '900' WHERE id = 'o:1'");
  await until(() => renders.at(-1) === "900|900,900");
  // Nothing in between: neither query rendered ahead of the other.
  expect(renders.slice(before)).toEqual(["900|900,900"]);
  root.unmount();
  await manager.close();
}

describe("commit batching", () => {
  it("renders a rekey once, with every query on the new key", async () => {
    await rendersOnce(new MinnowDatabase(new MemoryBlockStore()));
  });

  it("renders it once through a worker client too", async () => {
    const { port1, port2 } = new MessageChannel();
    attachDatabaseWorker(port2);
    port2.start();
    const client = new MinnowDatabaseClient(port1, {
      store: { kind: "memory" },
      onWorkerError: () => undefined,
    });
    port1.start();
    await rendersOnce(client as unknown as Database);
    await client.close();
    port1.close();
  });
});
