// @vitest-environment happy-dom
import { act, createElement, Suspense } from "react";
import { createRoot } from "react-dom/client";
import { expect, it } from "vitest";
import { LiveQuery, type LiveQueryBackend } from "@minnowdb/core/live";
import { useSuspenseLiveQuery } from "./index.js";

it("loads a cold decoded live query before the Suspense subscription commits", async () => {
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  let active = 0;
  let opens = 0;
  const backend: LiveQueryBackend = {
    observe: async () => {
      throw new Error("decode should subscribe for rows");
    },
    subscribe: async (_query, options) => {
      opens += 1;
      active += 1;
      options.onChange(
        { columns: ["id"], columnDomains: [null], rows: [{ id: 7 }] },
        { initial: true, manifestVersion: 1, catalogEpoch: 1 },
      );
      return {
        dependencyTableIds: [],
        close: () => {
          active -= 1;
        },
      };
    },
    refresh: async () => undefined,
    close: () => undefined,
  };
  const query = new LiveQuery(backend, {
    query: "SELECT 7 AS id",
    execute: async () => {
      throw new Error("decode should use delivered rows");
    },
    decode: (result) => result.rows.map((row) => ({ id: Number(row.id) })),
  });
  function Result() {
    const snapshot = useSuspenseLiveQuery(query);
    return createElement("span", null, String(snapshot.rows[0]?.id));
  }
  const container = document.createElement("div");
  document.body.append(container);
  const root = createRoot(container);
  try {
    await act(async () => {
      root.render(
        createElement(
          Suspense,
          { fallback: createElement("span", null, "loading") },
          createElement(Result),
        ),
      );
    });
    expect(container.textContent).toBe("7");
    expect(opens).toBeGreaterThanOrEqual(1);
    expect(active).toBe(1);
  } finally {
    await act(async () => root.unmount());
    query.close();
    container.remove();
  }
  expect(active).toBe(0);
});
