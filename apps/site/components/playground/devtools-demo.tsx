"use client";
/**
 * The floating panel, over these docs.
 *
 * The playground shows the inline shape, which is the one this site embeds; this is the shape an
 * application ships — a window over the page, with the page still usable underneath it. It runs a
 * small in-memory database rather than the playground's IndexedDB one, so opening it costs a
 * second, leaves nothing on the reader's machine, and closing it takes the database with it.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import type { MountedDevtools } from "@minnowdb/devtools";
import { useSiteTheme } from "./use-site-theme";

const DEMO_QUERY = `SELECT c.city, COUNT(*) AS orders, ROUND(SUM(o.total), 2) AS revenue
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.status = 'completed'
GROUP BY c.city
ORDER BY revenue DESC`;

type State = "closed" | "building" | "mounted";

export function DevtoolsDemo() {
  const panel = useRef<MountedDevtools | undefined>(undefined);
  const [state, setState] = useState<State>("closed");
  const [failure, setFailure] = useState<string>();
  const theme = useSiteTheme();

  useEffect(() => {
    panel.current?.setTheme(theme);
  }, [theme]);

  // A panel left mounted after the reader navigates away would float over an unrelated page.
  useEffect(
    () => () => {
      panel.current?.destroy();
      panel.current = undefined;
    },
    [],
  );

  const open = useCallback(async () => {
    if (panel.current !== undefined) {
      panel.current.open();
      return;
    }
    setState("building");
    setFailure(undefined);

    const [{ MinnowDatabase }, { MemoryBlockStore }, { mountMinnowDevtools }, dataset] =
      await Promise.all([
        import("@minnowdb/core"),
        import("@minnowdb/core/storage"),
        import("@minnowdb/devtools"),
        import("@/lib/dataset/retail"),
      ]);

    const database = new MinnowDatabase(new MemoryBlockStore());
    for (const table of dataset.retailSchema) {
      await database.createTable({
        name: table.name,
        uniqueKey: table.uniqueKey,
        columns: table.columns,
      });
    }
    // A fiftieth of the playground's data: enough for a query to have something to say, small
    // enough to build while the reader is still looking at the button they pressed.
    for (const batch of dataset.retailBatches({ scale: 0.02 })) {
      await database.insertBatch(batch.table, batch.rows);
    }

    panel.current = mountMinnowDevtools(database, {
      defaultOpen: true,
      initialQuery: DEMO_QUERY,
      storageKey: "minnow-devtools-demo",
      theme,
    });
    setState("mounted");
  }, [theme]);

  const remove = useCallback(() => {
    panel.current?.destroy();
    panel.current = undefined;
    setState("closed");
  }, []);

  return (
    <div className="not-prose my-6 flex flex-wrap items-center gap-3">
      <button
        className="rounded-lg bg-fd-primary px-4 py-2 text-sm font-medium text-fd-primary-foreground disabled:opacity-60"
        disabled={state === "building"}
        onClick={() => {
          open().catch((error: unknown) => {
            setFailure(error instanceof Error ? error.message : String(error));
            setState("closed");
          });
        }}
        type="button"
      >
        {state === "building" ? "Building a database…" : "Open the floating panel"}
      </button>
      {state === "mounted" ? (
        <button
          className="rounded-lg border border-fd-border px-4 py-2 text-sm font-medium hover:bg-fd-accent"
          onClick={remove}
          type="button"
        >
          Remove it
        </button>
      ) : null}
      <p className="text-sm text-fd-muted-foreground">
        {failure ??
          "A launcher appears in the corner, and the panel opens over this page — drag it by the title bar, and keep reading underneath."}
      </p>
    </div>
  );
}
