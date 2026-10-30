// @vitest-environment happy-dom
import { act, createElement, StrictMode, Suspense, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { expect, it, vi } from "vitest";
import {
  LiveQuery,
  type LiveQueryBackend,
  type LiveQuerySubscribeOptions,
} from "@minnowdb/core/live";
import { useSuspenseLiveQuery } from "./index.js";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

/**
 * A decoding backend that counts engine subscriptions. With `manual`, a subscription delivers its
 * first result only when `deliver()` is called, as a worker does once the query has executed.
 */
function countingBackend(manual = false) {
  let opens = 0;
  let active = 0;
  const waiting: Array<() => void> = [];
  const backend: LiveQueryBackend = {
    observe: async () => {
      throw new Error("decode should subscribe for rows");
    },
    subscribe: (_query, options: LiveQuerySubscribeOptions) => {
      opens += 1;
      active += 1;
      const handle = {
        dependencyTableIds: [],
        close: () => {
          active -= 1;
        },
      };
      // The first result routes ahead of the subscribe call's own result, as over a worker.
      const open = (): typeof handle => {
        options.onChange(
          { columns: ["id"], columnDomains: [null], rows: [{ id: 7 }] },
          { initial: true, manifestVersion: 1, catalogEpoch: 1 },
        );
        return handle;
      };
      if (!manual) return Promise.resolve(open());
      return new Promise((resolve) => waiting.push(() => resolve(open())));
    },
    refresh: async () => undefined,
    close: () => undefined,
  };
  return {
    backend,
    counts: () => ({ opens, active }),
    deliver: () => {
      for (const resolve of waiting.splice(0)) resolve();
    },
  };
}

function decodedQuery(backend: LiveQueryBackend): LiveQuery<{ id: number }> {
  return new LiveQuery(backend, {
    query: "SELECT 7 AS id",
    execute: async () => {
      throw new Error("decode should use delivered rows");
    },
    decode: (result) => result.rows.map((row) => ({ id: Number(row.id) })),
  });
}

function Result({ query }: { query: LiveQuery<{ id: number }> }) {
  const snapshot = useSuspenseLiveQuery(query);
  return createElement("span", null, String(snapshot.rows[0]?.id));
}

function suspended(query: LiveQuery<{ id: number }>): ReactNode {
  return createElement(
    Suspense,
    { fallback: createElement("span", null, "loading") },
    createElement(Result, { query }),
  );
}

const nextTask = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

it.each([false, true])(
  "loads a cold decoded query once and hands its subscription to the commit (StrictMode %s)",
  async (strict) => {
    const fixture = countingBackend();
    const query = decodedQuery(fixture.backend);
    const container = document.createElement("div");
    document.body.append(container);
    const root = createRoot(container);
    try {
      await act(async () => {
        const tree = suspended(query);
        root.render(strict ? createElement(StrictMode, null, tree) : tree);
      });
      await act(nextTask);
      expect(container.textContent).toBe("7");
      // One engine subscription loaded the query and serves the committed component: the
      // refresh's subscription is not closed and reopened, so the query runs and delivers once.
      expect(fixture.counts()).toEqual({ opens: 1, active: 1 });
    } finally {
      await act(async () => root.unmount());
      container.remove();
    }
    await nextTask();
    expect(fixture.counts()).toEqual({ opens: 1, active: 0 });
    query.close();
  },
);

it("releases the subscription of a suspended render that never commits", async () => {
  const fixture = countingBackend(true);
  const query = decodedQuery(fixture.backend);
  const container = document.createElement("div");
  document.body.append(container);
  const root = createRoot(container);
  await act(async () => root.render(suspended(query)));
  expect(container.textContent).toBe("loading");
  expect(fixture.counts()).toEqual({ opens: 1, active: 1 });
  // Abandoned while the query loads: the component that suspended will never subscribe.
  await act(async () => root.unmount());
  container.remove();
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  try {
    fixture.deliver();
    await vi.advanceTimersByTimeAsync(0);
    expect(query.getSnapshot().status).toBe("ready");
    expect(fixture.counts()).toEqual({ opens: 1, active: 1 });
    await vi.advanceTimersByTimeAsync(30_000);
    expect(fixture.counts()).toEqual({ opens: 1, active: 0 });
  } finally {
    vi.useRealTimers();
    query.close();
  }
});
