// @vitest-environment happy-dom
import { act, createElement, StrictMode, Suspense, use, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { expect, it } from "vitest";
import { MinnowDatabaseClient, type ClientTransport } from "@minnowdb/core/client";
import { attachDatabaseWorker, type RpcScope } from "@minnowdb/core/worker-host";
import { MinnowDialect, createKyselyLiveQueries } from "@minnowdb/kysely";
import { Kysely } from "kysely";
import { useSuspenseLiveQuery } from "./index.js";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

interface Frame {
  readonly method?: string;
  readonly event?: string;
  readonly handleId?: string;
}

/**
 * An in-process worker boundary that structured-clones every frame, as a real one does, and
 * counts the engine subscriptions the client opens and the results the worker delivers.
 */
function countingBoundary(): {
  clientSide: ClientTransport;
  workerSide: RpcScope;
  counts: () => { subscribes: number; deliveries: number };
} {
  const clientListeners: Array<(event: MessageEvent<unknown>) => void> = [];
  const workerListeners: Array<(event: MessageEvent<unknown>) => void> = [];
  let subscribes = 0;
  let deliveries = 0;
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
      postMessage: (message) => {
        if ((message as Frame).method === "subscribe") subscribes += 1;
        deliver(workerListeners, message);
      },
      addEventListener: (type, listener) => {
        if (type === "message") clientListeners.push(listener);
      },
    },
    workerSide: {
      postMessage: (message) => {
        if ((message as Frame).event === "change") deliveries += 1;
        deliver(clientListeners, message);
      },
      // The host also listens for "error", "unhandledrejection" and "messageerror"; those are
      // the scope's own events, never frames from the client.
      addEventListener: (type: string, listener) => {
        if (type === "message") workerListeners.push(listener);
      },
    },
    counts: () => ({ subscribes, deliveries }),
  };
}

const wait = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

it.each([false, true])(
  "a Kysely live query loaded by Suspense reaches the engine once when the boundary commits later (StrictMode %s)",
  async (strict) => {
    const boundary = countingBoundary();
    attachDatabaseWorker(boundary.workerSide);
    const client = new MinnowDatabaseClient(boundary.clientSide, { store: { kind: "memory" } });
    await client.execute("CREATE TABLE tickets (id INTEGER PRIMARY KEY, name TEXT)");
    await client.execute("INSERT INTO tickets VALUES (1, 'gravel'), (2, 'sand')");
    const db = new Kysely<{ tickets: { id: number; name: string } }>({
      dialect: new MinnowDialect({ driver: client }),
    });
    const live = createKyselyLiveQueries({ driver: client, db, pollIntervalMs: 2000 });
    const tickets = live(db.selectFrom("tickets").selectAll());
    // Another query in the same boundary that is still loading, so the commit waits on it.
    let releaseOther!: () => void;
    const other = new Promise<void>((resolve) => (releaseOther = resolve));

    function Tickets(): ReactNode {
      const snapshot = useSuspenseLiveQuery(tickets, { staleWhileRevalidate: true });
      return createElement("span", null, `${String(snapshot.rows.length)} tickets`);
    }
    function Other(): ReactNode {
      use(other);
      return createElement("span", null, ", other");
    }
    const page = createElement(
      Suspense,
      { fallback: "loading" },
      createElement(Tickets),
      createElement(Other),
    );

    const container = document.createElement("div");
    document.body.append(container);
    const root = createRoot(container);
    try {
      await act(async () => root.render(strict ? createElement(StrictMode, null, page) : page));
      while (tickets.getSnapshot().status === "loading") await act(() => wait(5));
      // The refresh that loaded the query has settled; the boundary is still waiting.
      await act(() => wait(250));
      expect(container.textContent).toBe("loading");
      expect(boundary.counts()).toEqual({ subscribes: 1, deliveries: 1 });

      await act(async () => releaseOther());
      await act(() => wait(250));
      expect(container.textContent).toBe("2 tickets, other");
      // The committed component took over the subscription the refresh opened: the query was
      // not registered, executed, and delivered again for it.
      expect(boundary.counts()).toEqual({ subscribes: 1, deliveries: 1 });
    } finally {
      await act(async () => root.unmount());
      container.remove();
      tickets.close();
      await live.close();
      await client.close();
    }
  },
);
