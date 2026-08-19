"use client";
/**
 * The playground: a real database, built in the visitor's browser, with two consoles over it.
 *
 * The engine runs in a worker and stores its blocks in IndexedDB, so the first visit builds the
 * dataset behind a progress bar and every visit after that opens what is already on disk. That
 * persistence is not a detail of the demo — it is the demo.
 *
 * This component owns the database and nothing else draws it. Both tabs are handed the same
 * client, so switching between them costs nothing and a row written through one is visible to the
 * other. The TypeScript tab is mounted the first time it is picked and kept afterwards: its type
 * checker is a large download, and a visitor who never opens it never fetches it.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import type { MinnowDatabaseClient } from "@minnowdb/core/client";
import { PLAYGROUND_DATABASE, isLoaded, loadRetailDataset } from "@/lib/dataset/load";
import { retailSizes } from "@/lib/dataset/retail";
import { SqlConsole } from "./sql-console";
import { TypeScriptConsole } from "./typescript-console";
import { useSiteTheme } from "./use-site-theme";

type Status =
  | { kind: "starting" }
  | { kind: "building"; label: string; done: number; total: number }
  | { kind: "ready"; client: MinnowDatabaseClient; rows: number; fresh: boolean }
  | { kind: "failed"; message: string };

type Language = "sql" | "typescript";

const SIZE_KEY = "minnow-playground-size";
const LANGUAGE_KEY = "minnow-playground-language";

const TABS: ReadonlyArray<{ id: Language; label: string; note: string }> = [
  { id: "sql", label: "SQL", note: "Write SQL against the database in this browser." },
  {
    id: "typescript",
    label: "TypeScript",
    note: "The same data through the typed client, checked against the schema as you type.",
  },
];

function format(value: number): string {
  return value.toLocaleString("en-US");
}

export function PlaygroundConsole({ height = 620 }: { height?: number }) {
  const client = useRef<MinnowDatabaseClient | undefined>(undefined);
  const worker = useRef<Worker | undefined>(undefined);
  const [status, setStatus] = useState<Status>({ kind: "starting" });
  const [scale, setScale] = useState(0.25);
  const [generation, setGeneration] = useState(0);
  const [language, setLanguage] = useState<Language>("sql");
  /** Sticky: once the type checker has been fetched, its tab stays mounted and keeps its state. */
  const [everTyped, setEverTyped] = useState(false);
  const theme = useSiteTheme();

  useEffect(() => {
    const stored = window.localStorage.getItem(SIZE_KEY);
    const match = retailSizes.find((size) => size.id === stored);
    if (match !== undefined) setScale(match.scale);

    const chosen = window.localStorage.getItem(LANGUAGE_KEY);
    if (chosen === "typescript") {
      setLanguage("typescript");
      setEverTyped(true);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    // Read through a call rather than the variable: TypeScript keeps a narrowed local narrowed
    // across awaits, which would quietly turn every later cancellation check into dead code.
    const stopped = (): boolean => cancelled;

    async function start(): Promise<void> {
      const { MinnowDatabaseClient: Client } = await import("@minnowdb/core/client");
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
      setStatus({ kind: "ready", client: connection, rows, fresh: !already });
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
      void client.current?.close();
      client.current = undefined;
      worker.current?.terminate();
      worker.current = undefined;
    };
  }, [scale, generation]);

  const reset = useCallback(() => {
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

  const choose = useCallback((next: Language) => {
    window.localStorage.setItem(LANGUAGE_KEY, next);
    setLanguage(next);
    if (next === "typescript") setEverTyped(true);
  }, []);

  return (
    <div className="not-prose flex flex-col gap-3">
      <div className="flex flex-wrap items-center gap-2 text-sm">
        <div
          role="tablist"
          aria-label="Console language"
          className="flex rounded-lg border border-fd-border p-0.5"
        >
          {TABS.map((tab) => (
            <button
              key={tab.id}
              type="button"
              role="tab"
              aria-selected={language === tab.id}
              title={tab.note}
              onClick={() => {
                choose(tab.id);
              }}
              className={
                language === tab.id
                  ? "rounded-md bg-fd-primary px-3 py-1 text-xs font-medium text-fd-primary-foreground"
                  : "rounded-md px-3 py-1 text-xs font-medium text-fd-muted-foreground hover:text-fd-foreground"
              }
            >
              {tab.label}
            </button>
          ))}
        </div>

        <span className="ml-2 text-fd-muted-foreground">Dataset</span>
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

      {status.kind === "ready" ? (
        <>
          <div data-minnow-console="sql" hidden={language !== "sql"}>
            <SqlConsole client={status.client} height={height} theme={theme} />
          </div>
          {everTyped ? (
            <div data-minnow-console="typescript" hidden={language !== "typescript"}>
              <TypeScriptConsole client={status.client} height={height} theme={theme} />
            </div>
          ) : null}
        </>
      ) : (
        <div
          className="minnow-placeholder flex flex-col items-center justify-center gap-3 p-8 text-center"
          style={{ height }}
        >
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

      <p className="text-xs text-fd-muted-foreground">
        {status.kind === "ready" && !status.fresh
          ? "Opened from IndexedDB — this database was already on your machine."
          : "Everything here runs in your browser. The data is generated locally and stored in IndexedDB, so a reload opens it instantly."}
      </p>
    </div>
  );
}
