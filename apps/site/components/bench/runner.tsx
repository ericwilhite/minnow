"use client";
/**
 * The benchmarks page.
 *
 * Every number on it is produced here, now, on this machine. There are no published captures to
 * compare against and nothing is uploaded: pick the engines, the suites, and the dataset size,
 * and the browser does the work.
 *
 * All engine work happens inside one worker — SQLite's OPFS persistence requires it — and each
 * engine's driver is dynamically imported, so choosing Minnow alone downloads neither
 * WebAssembly build.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import type {
  DatasetRecord,
  EngineId,
  FeatureSuiteResult,
  LiveSuiteResult,
  ReferenceSuiteResult,
  SecondaryIndexMode,
  WorkProgress,
  WriteSuiteResult,
} from "@/bench/protocol";
import type { BenchWorker } from "@/bench/worker-client";
import {
  ENGINES,
  SCALES,
  SUITES,
  columnsFor,
  enginesForColumns,
  storageLabel,
  estimateBytes,
  formatBytes,
  formatRows,
  type ColumnId,
} from "./config";
import { FeatureResults, LiveResults, ReadResults, StorageResults, WriteResults } from "./results";

type Phase =
  | { kind: "idle" }
  | { kind: "running"; label: string; done: number; total: number }
  | { kind: "done" }
  | { kind: "failed"; message: string };

interface Results {
  dataset?: DatasetRecord;
  reference?: ReferenceSuiteResult;
  write?: WriteSuiteResult;
  live?: LiveSuiteResult;
  features?: FeatureSuiteResult;
}

export function BenchRunner() {
  const worker = useRef<BenchWorker | undefined>(undefined);
  const running = useRef<string | undefined>(undefined);
  const [columns, setColumns] = useState<ColumnId[]>(["minnow", "minnow-cached", "sqlite"]);
  const [suites, setSuites] = useState<string[]>(["write", "reference", "live"]);
  const [scale, setScale] = useState(0.5);
  const [secondaryIndexes, setSecondaryIndexes] = useState<SecondaryIndexMode>("foreign-keys");
  const [phase, setPhase] = useState<Phase>({ kind: "idle" });
  const [results, setResults] = useState<Results>({});

  useEffect(() => {
    return () => {
      worker.current?.terminate();
      worker.current = undefined;
    };
  }, []);

  const toggle = <T extends string>(
    list: T[],
    value: T,
    set: (next: T[]) => void,
    atLeastOne = true,
  ): void => {
    const next = list.includes(value) ? list.filter((item) => item !== value) : [...list, value];
    if (atLeastOne && next.length === 0) return;
    set(next);
  };

  const run = useCallback(async () => {
    setResults({});
    setPhase({ kind: "running", label: "Starting the worker", done: 0, total: 1 });

    try {
      const { BenchWorker: Client } = await import("@/bench/worker-client");
      worker.current?.terminate();
      const client = new Client();
      worker.current = client;

      const onProgress = (label: string) => (progress: WorkProgress) => {
        setPhase({
          kind: "running",
          label: progress.message.length > 0 ? progress.message : label,
          done: progress.completed,
          total: Math.max(progress.total, 1),
        });
      };

      const wanted = SUITES.filter((suite) => suites.includes(suite.id));
      const engines = enginesForColumns(columns);
      const next: Results = {};

      if (wanted.some((suite) => suite.needsDataset)) {
        setPhase({ kind: "running", label: "Building the dataset", done: 0, total: 1 });
        const started = client.start<DatasetRecord>(
          "datasetCreate",
          {
            scale,
            compression: "gzip",
            targetBlockBytes: 1_048_576,
            durability: "relaxed",
            secondaryIndexes,
            engines,
          },
          onProgress("Building the dataset"),
        );
        running.current = started.requestId;
        const dataset = await started.result;
        next.dataset = dataset;
        setResults({ ...next });
        const failed = Object.entries(dataset.engines).filter(
          ([, entry]) => entry.status !== "ready",
        );
        if (failed.length === Object.keys(dataset.engines).length) {
          throw new Error(
            `No engine could load the dataset: ${failed
              .map(([engine, entry]) => `${engine}: ${entry.error ?? "unknown"}`)
              .join("; ")}`,
          );
        }
        const ready = Object.entries(dataset.engines)
          .filter(([, entry]) => entry.status === "ready")
          .map(([engine]) => engine as EngineId);

        // Writes first: a database is written before it is read, and the read suite then runs
        // against storage that has been through the write suite, which is the ordinary state of
        // an application's database rather than a pristine bulk load.
        if (suites.includes("write")) {
          const task = client.start<WriteSuiteResult>(
            "suiteWrite",
            { datasetId: dataset.id, engines: ready },
            onProgress("Running writes"),
          );
          running.current = task.requestId;
          next.write = await task.result;
          setResults({ ...next });
        }
        if (suites.includes("reference")) {
          const task = client.start<ReferenceSuiteResult>(
            "suiteReference",
            { datasetId: dataset.id, engines: ready },
            onProgress("Running reads"),
          );
          running.current = task.requestId;
          next.reference = await task.result;
          setResults({ ...next });
        }
        // Last, because it registers subscriptions and commits through the worker client on
        // the same storage; a Wasm engine in the selection is reported as having no live layer.
        if (suites.includes("live")) {
          const task = client.start<LiveSuiteResult>(
            "suiteLive",
            { datasetId: dataset.id, engines: ready },
            onProgress("Running live queries"),
          );
          running.current = task.requestId;
          next.live = await task.result;
          setResults({ ...next });
        }
      }

      if (suites.includes("features")) {
        const task = client.start<FeatureSuiteResult>(
          "suiteFeatureMatrix",
          { engines },
          onProgress("Checking PostgreSQL compatibility"),
        );
        running.current = task.requestId;
        next.features = await task.result;
        setResults({ ...next });
      }

      running.current = undefined;
      setPhase({ kind: "done" });
    } catch (error) {
      running.current = undefined;
      const message = error instanceof Error ? error.message : String(error);
      setPhase(message.includes("cancelled") ? { kind: "idle" } : { kind: "failed", message });
    }
  }, [columns, suites, scale, secondaryIndexes]);

  const cancel = useCallback(() => {
    const id = running.current;
    if (id !== undefined) worker.current?.cancel(id);
    running.current = undefined;
    setPhase({ kind: "idle" });
  }, []);

  const needsDataset = SUITES.some((suite) => suites.includes(suite.id) && suite.needsDataset);
  const chosen = columnsFor(columns);
  const bytes = estimateBytes(enginesForColumns(columns), scale, secondaryIndexes);
  const busy = phase.kind === "running";

  return (
    <div className="flex flex-col gap-6">
      <div className="grid gap-5 rounded-xl border border-fd-border p-5 sm:grid-cols-3">
        <fieldset>
          <legend className="mb-2 text-sm font-semibold">Engines</legend>
          <div className="flex flex-col gap-1.5">
            {ENGINES.map((engine) => (
              <label key={engine.id} className="flex items-start gap-2 text-sm" title={engine.note}>
                <input
                  type="checkbox"
                  className="mt-1"
                  checked={columns.includes(engine.id)}
                  disabled={busy}
                  onChange={() => {
                    toggle(columns, engine.id, setColumns);
                  }}
                />
                <span>
                  {engine.label}
                  {engine.download === undefined ? null : (
                    <span className="ml-1.5 text-xs text-fd-muted-foreground">
                      {engine.download}
                    </span>
                  )}
                  <span className="block text-xs text-fd-muted-foreground">
                    {storageLabel(engine, results.dataset?.engines[engine.engine]?.persistence)}
                  </span>
                </span>
              </label>
            ))}
          </div>
        </fieldset>

        <fieldset>
          <legend className="mb-2 text-sm font-semibold">Suites</legend>
          <div className="flex flex-col gap-1.5">
            {SUITES.map((suite) => (
              <label key={suite.id} className="flex items-start gap-2 text-sm" title={suite.note}>
                <input
                  type="checkbox"
                  className="mt-1"
                  checked={suites.includes(suite.id)}
                  disabled={busy}
                  onChange={() => {
                    toggle(suites, suite.id, setSuites);
                  }}
                />
                <span>{suite.label}</span>
              </label>
            ))}
          </div>
        </fieldset>

        <fieldset>
          <legend className="mb-2 text-sm font-semibold">Dataset</legend>
          <div className="flex flex-wrap gap-1.5">
            {SCALES.map((choice) => (
              <button
                key={choice.scale}
                type="button"
                disabled={busy || !needsDataset}
                onClick={() => {
                  setScale(choice.scale);
                }}
                className={
                  choice.scale === scale
                    ? "rounded-md border border-fd-primary bg-fd-primary/10 px-2 py-1 text-sm font-medium text-fd-primary"
                    : "rounded-md border border-fd-border px-2 py-1 text-sm hover:bg-fd-accent disabled:opacity-50"
                }
              >
                {choice.label}
              </button>
            ))}
          </div>
          <div className="mt-2 flex flex-wrap gap-1.5">
            {(
              [
                { id: "none", label: "Primary keys only" },
                { id: "foreign-keys", label: "+ 81 FK indexes" },
              ] as const
            ).map((choice) => (
              <button
                key={choice.id}
                type="button"
                aria-pressed={choice.id === secondaryIndexes}
                disabled={busy || !needsDataset}
                onClick={() => {
                  setSecondaryIndexes(choice.id);
                }}
                className={
                  choice.id === secondaryIndexes
                    ? "rounded-md border border-fd-primary bg-fd-primary/10 px-2 py-1 text-sm font-medium text-fd-primary"
                    : "rounded-md border border-fd-border px-2 py-1 text-sm hover:bg-fd-accent disabled:opacity-50"
                }
              >
                {choice.label}
              </button>
            ))}
          </div>
          {needsDataset ? (
            <p className="mt-2 text-xs text-fd-muted-foreground">
              {formatRows(SCALES.find((c) => c.scale === scale)?.rows ?? 0)} rows across 50 tables,
              about {formatBytes(bytes)} of browser storage for the engines selected.
            </p>
          ) : (
            <p className="mt-2 text-xs text-fd-muted-foreground">
              PostgreSQL compatibility needs no dataset.
            </p>
          )}
        </fieldset>
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <button
          type="button"
          onClick={busy ? cancel : () => void run()}
          className="rounded-lg bg-fd-primary px-4 py-2 font-medium text-fd-primary-foreground"
        >
          {busy ? "Cancel" : "Run"}
        </button>
        {busy ? (
          <div className="flex flex-1 items-center gap-3">
            <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-fd-border">
              <div
                className="h-full bg-fd-primary transition-[width] duration-200"
                style={{
                  width: `${String(Math.min(100, Math.round((phase.done / phase.total) * 100)))}%`,
                }}
              />
            </div>
            <span className="text-sm text-fd-muted-foreground">{phase.label}</span>
          </div>
        ) : null}
      </div>

      {phase.kind === "failed" ? (
        <p className="rounded-lg border border-red-500/40 bg-red-500/5 p-3 text-sm text-red-500">
          {phase.message}
        </p>
      ) : null}

      {results.write ? <WriteResults result={results.write} columns={chosen} /> : null}
      {results.reference ? <ReadResults result={results.reference} columns={chosen} /> : null}
      {results.live ? <LiveResults result={results.live} columns={chosen} /> : null}
      {results.dataset ? <StorageResults record={results.dataset} /> : null}
      {results.features ? <FeatureResults result={results.features} /> : null}

      {phase.kind === "done" && Object.keys(results).length === 0 ? (
        <p className="text-sm text-fd-muted-foreground">Nothing ran — select at least one suite.</p>
      ) : null}
    </div>
  );
}
