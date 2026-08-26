import { createElement, Suspense } from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, expectTypeOf, it, vi } from "vitest";
import {
  useLiveQuery,
  useSuspenseLiveQuery,
  type LiveExternalStore,
  type RefreshableLiveExternalStore,
} from "./index.js";

describe("useLiveQuery", () => {
  it("uses the stable server snapshot without starting a subscription", () => {
    const subscribe = vi.fn(() => () => undefined);
    const snapshot = { status: "ready" as const, rows: [{ id: 7 }], version: 4 };
    const store: LiveExternalStore<typeof snapshot> = {
      getSnapshot: () => snapshot,
      subscribe,
    };

    function Result(): ReturnType<typeof createElement> {
      const current = useLiveQuery(store);
      expectTypeOf(current.rows[0]).toEqualTypeOf<{ id: number } | undefined>();
      return createElement("span", null, `${current.status}:${String(current.rows[0]?.id)}`);
    }

    expect(renderToString(createElement(Result))).toContain("ready:7");
    expect(subscribe).not.toHaveBeenCalled();
  });

  it("preserves an arbitrary keyed snapshot type", () => {
    interface Snapshot {
      readonly status: "ready";
      readonly rows: ReadonlyArray<{ readonly sku: string }>;
      readonly changes: ReadonlyArray<{ readonly type: "insert"; readonly key: string }>;
    }
    const snapshot: Snapshot = {
      status: "ready",
      rows: [{ sku: "A-1" }],
      changes: [{ type: "insert", key: "A-1" }],
    };
    const store: LiveExternalStore<Snapshot> = {
      getSnapshot: () => snapshot,
      subscribe: () => () => undefined,
    };

    function Result(): ReturnType<typeof createElement> {
      const current = useLiveQuery(store);
      expectTypeOf(current).toEqualTypeOf<Snapshot>();
      return createElement("span", null, current.changes[0]?.key);
    }

    expect(renderToString(createElement(Result))).toContain("A-1");
  });

  it("suspends a cold query and starts one cached refresh", () => {
    type Snapshot =
      | { readonly status: "loading"; readonly rows: readonly [] }
      | { readonly status: "ready"; readonly rows: readonly [{ readonly id: 1 }] };
    const refresh = vi.fn(() => new Promise<void>(() => undefined));
    const store: RefreshableLiveExternalStore<Snapshot> = {
      getSnapshot: () => ({ status: "loading", rows: [] }),
      subscribe: () => () => undefined,
      refresh,
    };
    function Result(): ReturnType<typeof createElement> {
      const snapshot = useSuspenseLiveQuery(store);
      expectTypeOf(snapshot.status).toEqualTypeOf<"ready">();
      return createElement("span", null, snapshot.status);
    }
    const rendered = renderToString(
      createElement(
        Suspense,
        { fallback: createElement("span", null, "cold") },
        createElement(Result),
      ),
    );
    expect(rendered).toContain("cold");
    expect(refresh).toHaveBeenCalledOnce();
  });
});
