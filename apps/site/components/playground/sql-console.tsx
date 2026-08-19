"use client";
/**
 * The SQL half of the console: the devtools panel, inline, over the page's database.
 *
 * This is the shipped component rather than a copy of it — the same `mountMinnowDevtools` an
 * application calls, in the same inline mode the docs describe. Nothing about it is special to
 * this site, which is the reason to embed it here instead of writing a console for the marketing
 * page.
 */
import { useEffect, useRef } from "react";
import type { MinnowDatabaseClient } from "@minnowdb/core/client";
import type { MountedDevtools } from "@minnowdb/devtools";
import { defaultQuery, sampleQueries } from "./queries";

export function SqlConsole({
  client,
  height,
  theme,
}: {
  client: MinnowDatabaseClient;
  height: number;
  theme: "light" | "dark";
}) {
  const mount = useRef<HTMLDivElement>(null);
  const panel = useRef<MountedDevtools | undefined>(undefined);

  // Read through a ref by the mount below, so flipping the site's theme repaints the panel
  // instead of rebuilding it.
  const current = useRef(theme);
  useEffect(() => {
    current.current = theme;
    panel.current?.setTheme(theme);
  }, [theme]);

  useEffect(() => {
    let cancelled = false;
    // Read through a call rather than the variable: TypeScript keeps a narrowed local narrowed
    // across awaits, which would quietly turn the cancellation check below into dead code.
    const stopped = (): boolean => cancelled;

    void (async () => {
      const { mountMinnowDevtools } = await import("@minnowdb/devtools");
      const container = mount.current;
      if (stopped() || container === null) return;
      container.replaceChildren();
      panel.current = mountMinnowDevtools(client, {
        container,
        mode: "inline",
        initialQuery: defaultQuery,
        storageKey: "minnow-playground",
        theme: current.current,
      });
    })();

    return () => {
      cancelled = true;
      panel.current?.destroy();
      panel.current = undefined;
    };
  }, [client]);

  return (
    <div className="flex flex-col gap-3">
      {/* A box with a real height, because that is what the panel fills. */}
      <div ref={mount} style={{ height }} />

      <div className="flex flex-wrap gap-1.5 text-xs">
        {sampleQueries.map((query) => (
          <button
            key={query.id}
            type="button"
            title={query.note}
            onClick={() => {
              panel.current?.setQuery(query.sql);
            }}
            className="rounded-full border border-fd-border px-2.5 py-1 text-fd-muted-foreground hover:bg-fd-accent hover:text-fd-foreground"
          >
            {query.label}
          </button>
        ))}
      </div>
    </div>
  );
}
