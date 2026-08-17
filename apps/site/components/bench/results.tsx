"use client";
/**
 * Rendering a completed run.
 *
 * Two rules the tables follow throughout. OLTP and OLAP stay split, because a blended score
 * hides the trade-off that actually distinguishes these engines — Minnow leads on scans and
 * SQLite leads on single-key lookups, and one number would bury both facts. And nothing is
 * reported unless it was verified: a timing for a wrong answer is not a timing.
 */
import type {
  EngineId,
  FeatureSuiteResult,
  ReferenceSuiteResult,
  WriteSuiteResult,
  WorkloadKind,
} from "@/bench/protocol";
import { ENGINES, formatMs } from "./config";

const WORKLOADS: ReadonlyArray<{ kind: WorkloadKind; label: string; note: string }> = [
  { kind: "oltp", label: "OLTP", note: "Selective lookups and small mutations." },
  { kind: "olap", label: "OLAP", note: "Scans, joins, aggregates, and bulk loads." },
];

function engineLabel(engine: EngineId): string {
  return ENGINES.find((choice) => choice.id === engine)?.label ?? engine;
}

/** Fastest verified engine in a row, so the winner can be marked rather than eyeballed. */
function fastest(entries: ReadonlyArray<{ engine: EngineId; ms: number | null }>): EngineId | null {
  let best: { engine: EngineId; ms: number } | undefined;
  for (const entry of entries) {
    if (entry.ms === null) continue;
    if (best === undefined || entry.ms < best.ms) best = { engine: entry.engine, ms: entry.ms };
  }
  return best?.engine ?? null;
}

function Cell({ ms, best, note }: { ms: number | null; best: boolean; note?: string }) {
  if (ms === null) {
    return (
      <td className="px-3 py-1.5 text-right text-fd-muted-foreground" title={note}>
        —
      </td>
    );
  }
  return (
    <td
      className={`px-3 py-1.5 text-right tabular-nums ${best ? "font-semibold text-fd-primary" : ""}`}
    >
      {formatMs(ms)}
    </td>
  );
}

function Table({
  caption,
  note,
  engines,
  rows,
}: {
  caption: string;
  note: string;
  engines: readonly EngineId[];
  rows: ReadonlyArray<{
    id: string;
    name: string;
    detail: string;
    cells: ReadonlyArray<{ engine: EngineId; ms: number | null; note?: string }>;
  }>;
}) {
  if (rows.length === 0) return null;
  return (
    <div className="my-5">
      <h4 className="mb-1 font-semibold">{caption}</h4>
      <p className="mb-2 text-sm text-fd-muted-foreground">{note}</p>
      <div className="overflow-x-auto rounded-lg border border-fd-border">
        <table className="w-full min-w-[36rem] text-sm">
          <thead>
            <tr className="border-b border-fd-border bg-fd-muted">
              <th className="px-3 py-2 text-left font-medium">Case</th>
              {engines.map((engine) => (
                <th key={engine} className="px-3 py-2 text-right font-medium">
                  {engineLabel(engine)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const best = fastest(row.cells);
              return (
                <tr
                  key={row.id}
                  className="border-b border-fd-border last:border-0 even:bg-fd-muted/40"
                >
                  <td className="px-3 py-1.5">
                    <span className="font-medium">{row.name}</span>
                    <span className="ml-2 text-xs text-fd-muted-foreground">{row.detail}</span>
                  </td>
                  {row.cells.map((cell) => (
                    <Cell
                      key={cell.engine}
                      ms={cell.ms}
                      best={best === cell.engine}
                      {...(cell.note === undefined ? {} : { note: cell.note })}
                    />
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export function ReadResults({ result }: { result: ReferenceSuiteResult }) {
  return (
    <section>
      <h3 className="text-xl font-semibold">Reads</h3>
      <p className="mt-1 text-sm text-fd-muted-foreground">
        Median of {result.sampleCount} executions per query, after one untimed warm-up.{" "}
        {result.passed
          ? "Every query every engine could run agreed with the independent oracle."
          : "Some results disagreed with the oracle — treat these timings as suspect."}
      </p>
      {WORKLOADS.map((workload) => (
        <Table
          key={workload.kind}
          caption={workload.label}
          note={workload.note}
          engines={result.engines}
          rows={result.queries
            .filter((query) => query.workload === workload.kind)
            .map((query) => ({
              id: query.id,
              name: query.name,
              detail: `${String(query.oracleRows)} rows`,
              cells: result.engines.map((engine) => {
                const measured = query.engines.find((entry) => entry.engine === engine);
                return {
                  engine,
                  // An unsupported query, or one whose answer did not match the oracle, has no
                  // number worth printing.
                  ms: measured?.supported === true && measured.verified ? measured.medianMs : null,
                  ...(measured?.error === undefined ? {} : { note: measured.error }),
                };
              }),
            }))}
        />
      ))}
    </section>
  );
}

export function WriteResults({ result }: { result: WriteSuiteResult }) {
  return (
    <section>
      <h3 className="text-xl font-semibold">Writes</h3>
      <p className="mt-1 text-sm text-fd-muted-foreground">
        Median of {result.sampleCount} runs against a fresh table each time. Only the engine&rsquo;s
        own call is timed — reshaping rows into the form each API wants is the harness&rsquo;s cost,
        not the engine&rsquo;s.
      </p>
      {WORKLOADS.map((workload) => (
        <Table
          key={workload.kind}
          caption={workload.label}
          note={workload.note}
          engines={result.engines}
          rows={result.cases
            .filter((entry) => entry.workload === workload.kind)
            .map((entry) => ({
              id: entry.id,
              name: entry.name,
              detail: `${entry.rows.toLocaleString("en-US")} rows`,
              cells: result.engines.map((engine) => {
                const measured = entry.engines.find((item) => item.engine === engine);
                return {
                  engine,
                  ms: measured?.supported === true && measured.verified ? measured.medianMs : null,
                  ...(measured?.error === undefined ? {} : { note: measured.error }),
                };
              }),
            }))}
        />
      ))}
    </section>
  );
}

export function FeatureResults({ result }: { result: FeatureSuiteResult }) {
  return (
    <section>
      <h3 className="text-xl font-semibold">SQL conformance</h3>
      <p className="mt-1 text-sm text-fd-muted-foreground">
        {result.supportedCount} supported and {result.unsupportedCount} rejected forms from the
        published matrix, executed just now.{" "}
        {result.driftFailures === 0
          ? "Minnow matched the matrix exactly."
          : `${String(result.driftFailures)} results contradict the published matrix.`}
      </p>
      <div className="mt-3 overflow-x-auto rounded-lg border border-fd-border">
        <table className="w-full min-w-[32rem] text-sm">
          <thead>
            <tr className="border-b border-fd-border bg-fd-muted">
              <th className="px-3 py-2 text-left font-medium">Engine</th>
              <th className="px-3 py-2 text-right font-medium">Accepted</th>
              <th className="px-3 py-2 text-right font-medium">Rejected</th>
            </tr>
          </thead>
          <tbody>
            {result.engines.map((engine) => {
              // "pass" on a supported form and "accepts" on a rejected one both mean the engine
              // ran the example; the two spellings exist because they mean different things
              // about Minnow, which is being checked for drift rather than surveyed.
              const accepted = result.features.filter((feature) => {
                const outcome = feature.engines.find((e) => e.engine === engine)?.outcome;
                return feature.status === "supported"
                  ? outcome === "pass"
                  : outcome === "accepts" || (engine === "minnow" && outcome === "fail");
              }).length;
              return (
                <tr key={engine} className="border-b border-fd-border last:border-0">
                  <td className="px-3 py-1.5">{engineLabel(engine)}</td>
                  <td className="px-3 py-1.5 text-right tabular-nums">{accepted}</td>
                  <td className="px-3 py-1.5 text-right tabular-nums">
                    {result.features.length - accepted}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="mt-2 text-xs text-fd-muted-foreground">
        The other engines are informational: a form Minnow rejects and Postgres accepts is a
        difference, not a defect on either side.
      </p>
    </section>
  );
}
