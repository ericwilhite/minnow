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
  DatasetRecord,
  EngineId,
  FeatureSuiteResult,
  LiveSuiteResult,
  ReferenceSuiteResult,
  WriteSuiteResult,
  WorkloadKind,
} from "@/bench/protocol";
import { formatBytes as formatStorage } from "./config";
import { ENGINES, formatMs, type EngineChoice } from "./config";

const WORKLOADS: ReadonlyArray<{ kind: WorkloadKind; label: string; note: string }> = [
  { kind: "oltp", label: "OLTP", note: "Selective lookups and small mutations." },
  { kind: "olap", label: "OLAP", note: "Scans, joins, aggregates, and bulk loads." },
];

function engineLabel(engine: EngineId): string {
  return ENGINES.find((choice) => choice.id === engine)?.label ?? engine;
}

/** Fastest verified column in a row, so the winner can be marked rather than eyeballed. */
function fastest(entries: ReadonlyArray<{ id: string; ms: number | null }>): string | null {
  let best: { id: string; ms: number } | undefined;
  for (const entry of entries) {
    if (entry.ms === null) continue;
    if (best === undefined || entry.ms < best.ms) best = { id: entry.id, ms: entry.ms };
  }
  return best?.id ?? null;
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
  columns,
  rows,
}: {
  caption: string;
  note: string;
  columns: readonly EngineChoice[];
  rows: ReadonlyArray<{
    id: string;
    name: string;
    detail: string;
    cells: ReadonlyArray<{ id: string; ms: number | null; note?: string }>;
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
              {columns.map((column) => (
                <th
                  key={column.id}
                  className="px-3 py-2 text-right font-medium"
                  title={column.note}
                >
                  {column.label}
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
                      key={cell.id}
                      ms={cell.ms}
                      best={best === cell.id}
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

export function ReadResults({
  result,
  columns,
}: {
  result: ReferenceSuiteResult;
  columns: readonly EngineChoice[];
}) {
  const shown = columns.filter((column) => result.engines.includes(column.engine));
  // The batch size the harness settled on, which is the honest answer to "how was 4µs measured".
  const batched = result.queries
    .flatMap((query) => query.engines.map((entry) => entry.batchSize ?? 1))
    .reduce((highest, size) => Math.max(highest, size), 1);
  return (
    <section>
      <h3 className="text-xl font-semibold">Reads</h3>
      <p className="mt-1 text-sm text-fd-muted-foreground">
        Median of {result.sampleCount} timed windows per query, after one untimed warm-up.{" "}
        {batched > 1
          ? `A query quicker than the clock is executed up to ${batched.toLocaleString("en-US")} times per window and divided back down, so a microsecond lookup reads as microseconds rather than as a clock tick. `
          : ""}
        {result.passed
          ? "Every query every engine could run agreed with the independent oracle."
          : "Some results disagreed with the oracle — treat these timings as suspect."}
        {result.secondaryIndexes === "foreign-keys"
          ? " This run includes the same 81 foreign-key secondary indexes in every engine."
          : result.secondaryIndexes === "none"
            ? " This run has primary keys only and no secondary indexes."
            : " This saved run predates index-mode labels, so its secondary-index configuration is unknown."}
      </p>
      {WORKLOADS.map((workload) => (
        <Table
          key={workload.kind}
          caption={workload.label}
          note={workload.note}
          columns={shown}
          rows={result.queries
            .filter((query) => query.workload === workload.kind)
            .map((query) => ({
              id: query.id,
              name: query.name,
              detail: `${String(query.oracleRows)} rows`,
              cells: shown.map((column) => {
                const measured = query.engines.find((entry) => entry.engine === column.engine);
                // An unsupported query, or one whose answer did not match the oracle, has no
                // number worth printing. Neither has a variant column for an engine without
                // that layer — there is no result memo to report.
                const usable = measured?.supported === true && measured.verified;
                const ms = !usable
                  ? null
                  : column.variant === "cached"
                    ? (measured.cachedMedianMs ?? null)
                    : measured.medianMs;
                return {
                  id: column.id,
                  ms,
                  ...(measured?.error === undefined ? {} : { note: measured.error }),
                };
              }),
            }))}
        />
      ))}
    </section>
  );
}

export function WriteResults({
  result,
  columns,
}: {
  result: WriteSuiteResult;
  columns: readonly EngineChoice[];
}) {
  // Nothing caches a write and the write path is not measured through the client, so the
  // variant columns would be stripes of dashes.
  const shown = columns.filter(
    (column) => column.variant === undefined && result.engines.includes(column.engine),
  );
  const batched = result.cases
    .flatMap((entry) => entry.engines.map((measured) => measured.batchSize ?? 1))
    .reduce((highest, size) => Math.max(highest, size), 1);
  return (
    <section>
      <h3 className="text-xl font-semibold">Writes</h3>
      <p className="mt-1 text-sm text-fd-muted-foreground">
        Median of {result.sampleCount} timed windows, each against tables created and seeded for it
        alone.{" "}
        {batched > 1
          ? `A write quicker than the clock is repeated across ${batched.toLocaleString("en-US")} of those tables inside one window and divided back down. `
          : ""}
        Only the engine&rsquo;s own call is timed — reshaping rows into the form each API wants is
        the harness&rsquo;s cost, not the engine&rsquo;s.
      </p>
      {WORKLOADS.map((workload) => (
        <Table
          key={workload.kind}
          caption={workload.label}
          note={workload.note}
          columns={shown}
          rows={result.cases
            .filter((entry) => entry.workload === workload.kind)
            .map((entry) => ({
              id: entry.id,
              name: entry.name,
              detail: `${entry.rows.toLocaleString("en-US")} rows`,
              cells: shown.map((column) => {
                const measured = entry.engines.find((item) => item.engine === column.engine);
                return {
                  id: column.id,
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

export function LiveResults({
  result,
  columns,
}: {
  result: LiveSuiteResult;
  columns: readonly EngineChoice[];
}) {
  // Every live number already goes through the worker client — that is the path a notification
  // takes — so the variant columns have nothing separate to show. Engines without a live-query
  // layer (the Wasm engines) report no number at all, and the table says so rather than
  // printing a stripe of dashes against them.
  const shown = columns.filter(
    (column) =>
      column.variant === undefined &&
      result.engines.includes(column.engine) &&
      (result.supportedByEngine[column.engine] ?? 0) > 0,
  );
  const missing = columns
    .filter(
      (column) =>
        column.variant === undefined &&
        result.engines.includes(column.engine) &&
        (result.supportedByEngine[column.engine] ?? 0) === 0,
    )
    .map((column) => column.label);
  return (
    <section>
      <h3 className="text-xl font-semibold">Live queries</h3>
      <p className="mt-1 text-sm text-fd-muted-foreground">
        Median of {result.sampleCount} commits per case, after one untimed warm-up: from issuing the
        write through the worker client until the last affected subscription&rsquo;s{" "}
        <code>onChange</code> has fired with its rows rebuilt on this side of the channel.{" "}
        {result.passed
          ? "Every affected subscription fired exactly once per commit and none of the others did."
          : "Some subscriptions fired the wrong number of times or on the wrong result — treat these timings as suspect."}
        {missing.length > 0
          ? ` ${missing.join(" and ")} ${missing.length === 1 ? "has" : "have"} no live-query layer, so there is nothing to time.`
          : ""}
      </p>
      {shown.length === 0 ? (
        <p className="mt-3 text-sm text-fd-muted-foreground">
          No selected engine could run the live-query suite.
        </p>
      ) : (
        <Table
          caption="Commit to notification"
          note="Subscriptions count the rows above a threshold in a table this suite owns; one commit adds a row, and the clock stops when every subscription that depends on that table has been told. The quiet subscriptions watch a table the commit never touches."
          columns={shown}
          rows={result.cases.map((entry) => ({
            id: entry.id,
            name: entry.name,
            detail: `${String(entry.affected)} of ${String(entry.subscriptions)} affected`,
            cells: shown.map((column) => {
              const measured = entry.engines.find((item) => item.engine === column.engine);
              return {
                id: column.id,
                ms: measured?.supported === true && measured.verified ? measured.medianMs : null,
                ...(measured?.error === undefined ? {} : { note: measured.error }),
              };
            }),
          }))}
        />
      )}
    </section>
  );
}

/**
 * What each engine's copy of the same dataset costs on disk, and the storage it went through.
 * Every engine reports the size the way its own documentation does — Minnow sums its blocks,
 * SQLite multiplies page count by page size, PostgreSQL sums its relation sizes — so these are
 * each engine's own accounting of the same rows rather than one outsider's guess about all three.
 */
export function StorageResults({ record }: { record: DatasetRecord }) {
  const ready = Object.values(record.engines).filter((entry) => entry.status === "ready");
  if (ready.length === 0) return null;
  const smallest = ready.reduce<number | null>(
    (best, entry) =>
      entry.storedBytes === null || (best !== null && best <= entry.storedBytes)
        ? best
        : entry.storedBytes,
    null,
  );
  return (
    <section>
      <h3 className="text-xl font-semibold">Storage</h3>
      <p className="mt-1 text-sm text-fd-muted-foreground">
        The same {record.totalRows.toLocaleString("en-US")} rows, as each engine stored them just
        now. Every copy has the workload&rsquo;s primary keys
        {record.secondaryIndexes === "foreign-keys"
          ? " and the same 81 foreign-key secondary indexes"
          : record.secondaryIndexes === "none"
            ? " and no secondary indexes"
            : "; this saved run predates secondary-index labels"}
        . Inserts and index builds are timed separately; ready total also includes schema creation,
        verification, persistence, and reopen work.
      </p>
      <div className="mt-3 overflow-x-auto rounded-lg border border-fd-border">
        <table className="w-full min-w-[72rem] text-sm">
          <thead>
            <tr className="border-b border-fd-border bg-fd-muted">
              <th className="px-3 py-2 text-left font-medium">Engine</th>
              <th className="px-3 py-2 text-right font-medium">Table data</th>
              <th className="px-3 py-2 text-right font-medium">Secondary indexes</th>
              <th className="px-3 py-2 text-right font-medium">On disk</th>
              <th className="px-3 py-2 text-right font-medium">Per row</th>
              <th className="px-3 py-2 text-right font-medium">Relative</th>
              <th className="px-3 py-2 text-right font-medium">Bulk inserts</th>
              <th className="px-3 py-2 text-right font-medium">Index build</th>
              <th className="px-3 py-2 text-right font-medium">Ready total</th>
              <th className="px-3 py-2 text-left font-medium">Storage</th>
            </tr>
          </thead>
          <tbody>
            {ready.map((entry) => (
              <tr key={entry.engine} className="border-b border-fd-border last:border-0">
                <td className="px-3 py-1.5">{engineLabel(entry.engine)}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">
                  {entry.dataStoredBytes === null ? "—" : formatStorage(entry.dataStoredBytes)}
                </td>
                <td className="px-3 py-1.5 text-right tabular-nums">
                  {entry.indexStoredBytes === null ? "—" : formatStorage(entry.indexStoredBytes)}
                </td>
                <td
                  className={`px-3 py-1.5 text-right tabular-nums ${
                    entry.storedBytes !== null && entry.storedBytes === smallest
                      ? "font-semibold text-fd-primary"
                      : ""
                  }`}
                >
                  {entry.storedBytes === null ? "—" : formatStorage(entry.storedBytes)}
                </td>
                <td className="px-3 py-1.5 text-right tabular-nums">
                  {entry.storedBytes === null || record.totalRows === 0
                    ? "—"
                    : `${(entry.storedBytes / record.totalRows).toFixed(1)} B`}
                </td>
                <td className="px-3 py-1.5 text-right tabular-nums">
                  {entry.storedBytes === null || smallest === null || smallest === 0
                    ? "—"
                    : `${(entry.storedBytes / smallest).toFixed(2)}×`}
                </td>
                <td className="px-3 py-1.5 text-right tabular-nums">{formatMs(entry.insertMs)}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{formatMs(entry.indexMs)}</td>
                <td className="px-3 py-1.5 text-right tabular-nums">{formatMs(entry.buildMs)}</td>
                <td className="px-3 py-1.5 text-xs text-fd-muted-foreground">
                  {entry.persistence}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export function FeatureResults({ result }: { result: FeatureSuiteResult }) {
  return (
    <section>
      <h3 className="text-xl font-semibold">PostgreSQL compatibility</h3>
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
              // One question for every column: did the engine run the statement? The per-engine
              // verdicts answer different questions — matrix drift for Minnow, surface survey for
              // the others — so counting them together compared nothing.
              const accepted = result.features.filter(
                (feature) => feature.engines.find((e) => e.engine === engine)?.accepted === true,
              ).length;
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
        Accepted counts the forms an engine executed. Minnow&rsquo;s rejections are the forms the
        matrix publishes as unsupported — refusing them is the matrix being accurate, and executing
        one would be the drift reported above. For the other engines this is a survey of dialect,
        not a score: a form Minnow rejects and Postgres accepts is a difference, not a defect on
        either side.
      </p>
    </section>
  );
}
