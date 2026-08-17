"use client";
/**
 * The playground: a real database, built in the visitor's browser, with the devtools SQL console
 * over it.
 *
 * The engine runs in a worker and stores its blocks in IndexedDB, so the first visit builds the
 * dataset behind a progress bar and every visit after that opens what is already on disk. That
 * persistence is not a detail of the demo — it is the demo.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import type { MinnowDatabaseClient } from "@minnowdb/core/client";
import type { MountedDevtools } from "@minnowdb/devtools";
import { PLAYGROUND_DATABASE, isLoaded, loadRetailDataset } from "@/lib/dataset/load";
import { retailSizes } from "@/lib/dataset/retail";
import { defaultQuery, sampleQueries } from "./queries";

type Status =
  | { kind: "starting" }
  | { kind: "building"; label: string; done: number; total: number }
  | { kind: "ready"; rows: number; fresh: boolean }
  | { kind: "failed"; message: string };

const SIZE_KEY = "minnow-playground-size";

function format(value: number): string {
  return value.toLocaleString("en-US");
}

export function PlaygroundConsole({ height = 620 }: { height?: number }) {
  const mount = useRef<HTMLDivElement>(null);
  const panel = useRef<MountedDevtools | undefined>(undefined);
  const client = useRef<MinnowDatabaseClient | undefined>(undefined);
  const worker = useRef<Worker | undefined>(undefined);
  const [status, setStatus] = useState<Status>({ kind: "starting" });
  const [scale, setScale] = useState(0.25);
  const [generation, setGeneration] = useState(0);

  useEffect(() => {
    const stored = window.localStorage.getItem(SIZE_KEY);
    const match = retailSizes.find((size) => size.id === stored);
    if (match !== undefined) setScale(match.scale);
  }, []);

  useEffect(() => {
    let cancelled = false;
    // Read through a call rather than the variable: TypeScript keeps a narrowed local narrowed
    // across awaits, which would quietly turn every later cancellation check into dead code.
    const stopped = (): boolean => cancelled;

    async function start(): Promise<void> {
      const [{ MinnowDatabaseClient: Client }, { mountMinnowDevtools }] = await Promise.all([
        import("@minnowdb/core/client"),
        import("@minnowdb/devtools"),
      ]);
      if (stopped()) return;

      const spawned = new Worker(new URL("@minnowdb/core/worker", import.meta.url), {
        type: "module",
      });
      worker.current = spawned;
      const connection = new Client(spawned, {
        store: { kind: "indexeddb", name: PLAYGROUND_DATABASE },
      });
      client.current = connection;
      await connection.ready();
      if (stopped()) return;

      const already = await isLoaded(connection);
      let rows = 0;
      if (already) {
        setStatus({ kind: "building", label: "Opening your database", done: 0, total: 1 });
      } else {
        rows = await loadRetailDataset(connection, {
          scale,
          onProgress: (progress) => {
            if (stopped()) return;
            setStatus({
              kind: "building",
              label:
                progress.phase === "schema"
                  ? "Creating tables"
                  : `Loading ${progress.table.replace("_", " ")}`,
              done: progress.rows,
              total: progress.estimatedRows,
            });
          },
        });
      }
      if (stopped()) return;

      const container = mount.current;
      if (container === null) return;
      container.replaceChildren();
      panel.current = mountMinnowDevtools(connection, {
        container,
        mode: "inline",
        initialQuery: defaultQuery,
        storageKey: "minnow-playground",
      });
      setStatus({ kind: "ready", rows, fresh: !already });
    }

    setStatus({ kind: "starting" });
    start().catch((error: unknown) => {
      if (stopped()) return;
      setStatus({
        kind: "failed",
        message: error instanceof Error ? error.message : String(error),
      });
    });

    return () => {
      cancelled = true;
      panel.current?.destroy();
      panel.current = undefined;
      void client.current?.close();
      client.current = undefined;
      worker.current?.terminate();
      worker.current = undefined;
    };
  }, [scale, generation]);

  const reset = useCallback(() => {
    panel.current?.destroy();
    panel.current = undefined;
    void client.current?.close();
    client.current = undefined;
    worker.current?.terminate();
    worker.current = undefined;
    indexedDB.deleteDatabase(PLAYGROUND_DATABASE);
    setGeneration((value) => value + 1);
  }, []);

  const chooseSize = useCallback((id: string, next: number) => {
    window.localStorage.setItem(SIZE_KEY, id);
    indexedDB.deleteDatabase(PLAYGROUND_DATABASE);
    setScale(next);
    setGeneration((value) => value + 1);
  }, []);

  return (
    <div className="not-prose flex flex-col gap-3">
      <div className="flex flex-wrap items-center gap-2 text-sm">
        <span className="text-fd-muted-foreground">Dataset</span>
        {retailSizes.map((size) => (
          <button
            key={size.id}
            type="button"
            onClick={() => {
              chooseSize(size.id, size.scale);
            }}
            title={size.description}
            aria-pressed={size.scale === scale}
            className={
              size.scale === scale
                ? "rounded-md border border-fd-primary bg-fd-primary/10 px-2.5 py-1 font-medium text-fd-primary"
                : "rounded-md border border-fd-border px-2.5 py-1 hover:bg-fd-accent"
            }
          >
            {size.label}
          </button>
        ))}
        <button
          type="button"
          onClick={reset}
          className="ml-auto rounded-md border border-fd-border px-2.5 py-1 hover:bg-fd-accent"
        >
          Rebuild
        </button>
      </div>

      <div className="minnow-panel" style={{ minHeight: height }}>
        {status.kind === "ready" ? null : (
          <div className="flex h-full min-h-[inherit] flex-col items-center justify-center gap-3 p-8 text-center">
            {status.kind === "failed" ? (
              <p className="max-w-md text-sm text-red-500">
                The playground could not start: {status.message}
              </p>
            ) : (
              <>
                <p className="text-sm text-fd-muted-foreground">
                  {status.kind === "starting" ? "Starting a database worker" : status.label}
                </p>
                <div className="h-1.5 w-64 overflow-hidden rounded-full bg-fd-border">
                  <div
                    className="h-full bg-fd-primary transition-[width] duration-200"
                    style={{
                      width:
                        status.kind === "building" && status.total > 0
                          ? `${String(Math.min(100, Math.round((status.done / status.total) * 100)))}%`
                          : "12%",
                    }}
                  />
                </div>
                {status.kind === "building" && status.done > 0 ? (
                  <p className="text-xs text-fd-muted-foreground">
                    {format(status.done)} rows written
                  </p>
                ) : null}
              </>
            )}
          </div>
        )}
        <div ref={mount} style={{ minHeight: status.kind === "ready" ? height : 0 }} />
      </div>

      {status.kind === "ready" ? (
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
      ) : null}

      <p className="text-xs text-fd-muted-foreground">
        {status.kind === "ready" && !status.fresh
          ? "Opened from IndexedDB — this database was already on your machine."
          : "Everything here runs in your browser. The data is generated locally and stored in IndexedDB, so a reload opens it instantly."}
      </p>
    </div>
  );
}
