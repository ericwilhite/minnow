// @vitest-environment happy-dom
/**
 * What one commit costs a React tree reading live queries through a worker client: renders,
 * and frames on the channel. Counted rather than timed, so a regression that re-renders on an
 * unrelated commit, renders twice for one, or sends the query back to the worker to execute
 * again fails here whatever the machine's speed.
 */
import { createElement } from "react";
import { createRoot } from "react-dom/client";
import { describe, expect, it } from "vitest";
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { KeyedLiveQuery, createLiveQueryManager } from "@minnowdb/core/live";
import { attachDatabaseWorker } from "@minnowdb/core/worker-host";
import { useLiveQuery, useLiveSelector } from "./index.js";

interface Row {
  readonly id: number;
  readonly label: string;
}

async function until(check: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 400; attempt += 1) {
    if (check()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("timed out");
}

/** Gives the channel time to deliver anything a commit could still have caused. */
async function settle(client: MinnowDatabaseClient): Promise<void> {
  await client.listTables();
  await new Promise((resolve) => setTimeout(resolve, 30));
  await client.listTables();
}

describe("live query renders through a worker client", () => {
  it("renders once per visible commit, never for an unrelated one, and never re-executes", async () => {
    (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = false;
    const { port1, port2 } = new MessageChannel();
    attachDatabaseWorker(port2);
    port2.start();
    const calls: string[] = [];
    const transport = {
      postMessage: (message: unknown, options?: { transfer: ArrayBuffer[] }) => {
        const method = (message as { method?: unknown }).method;
        if (typeof method === "string") calls.push(method);
        port1.postMessage(message, options);
      },
      addEventListener: port1.addEventListener.bind(port1),
      removeEventListener: port1.removeEventListener.bind(port1),
    };
    const client = new MinnowDatabaseClient(transport, {
      store: { kind: "memory" },
      onWorkerError: () => undefined,
    });
    port1.start();
    await client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT, shelf INTEGER)");
    await client.execute(
      "INSERT INTO items VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 1), (4, 'z', 2), (5, 'y', 2)",
    );
    const manager = createLiveQueryManager(client);
    const sql = "SELECT id, label FROM items WHERE shelf = 1 ORDER BY id";
    const list = manager.watch<Row>({
      query: sql,
      execute: async () => (await client.query(sql)).rows as unknown as Row[],
      decode: (result) => result.rows as unknown as Row[],
    });
    const keyed = new KeyedLiveQuery(list, { key: "id" });
    let listRenders = 0;
    let rowRenders = 0;
    let lastLabels = "";
    function List(): ReturnType<typeof createElement> {
      listRenders += 1;
      const snapshot = useLiveQuery(list);
      lastLabels = snapshot.rows.map((row) => row.label).join(",");
      return createElement("span");
    }
    function SecondRow(): ReturnType<typeof createElement> {
      rowRenders += 1;
      useLiveSelector(keyed, { select: (snapshot) => snapshot.rows.find((row) => row.id === 2) });
      return createElement("span");
    }
    const root = createRoot(document.createElement("div"));
    root.render(createElement("div", null, createElement(List), createElement(SecondRow)));
    await until(() => lastLabels === "a,b,c");
    await settle(client);
    const rendersBefore = { list: listRenders, row: rowRenders };
    calls.length = 0;

    // A row in the list: the list renders once; the component reading row 2 does not.
    await client.execute("UPDATE items SET label = 'A' WHERE id = 1");
    await until(() => lastLabels === "A,b,c");
    await settle(client);
    expect(listRenders - rendersBefore.list).toBe(1);
    expect(rowRenders - rendersBefore.row).toBe(0);
    // The change arrived as the delivered result: the only calls are the write and the
    // settling probes, with no query sent back to execute.
    expect(calls).toEqual(["execute", "listTables", "listTables"]);

    // A row the list does not hold: no render anywhere, and still nothing re-executed.
    calls.length = 0;
    await client.execute("UPDATE items SET label = 'Z' WHERE id = 4");
    await settle(client);
    expect(listRenders - rendersBefore.list).toBe(1);
    expect(rowRenders - rendersBefore.row).toBe(0);
    expect(calls).toEqual(["execute", "listTables", "listTables"]);

    // Row 2 itself: both render, once.
    await client.execute("UPDATE items SET label = 'B' WHERE id = 2");
    await until(() => lastLabels === "A,B,c");
    await settle(client);
    expect(listRenders - rendersBefore.list).toBe(2);
    expect(rowRenders - rendersBefore.row).toBe(1);

    root.unmount();
    keyed.close();
    await manager.close();
    await client.close();
    port1.close();
  });
});
