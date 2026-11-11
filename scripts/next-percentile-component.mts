/** Decoded, dictionary-keyed component fixture. Only run() belongs inside CPU timing. */
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { createHash } from "node:crypto";
import type { StorageCore } from "../packages/core/dist/next/testing/storage-core.js";
import type { InputValue } from "../packages/core/dist/next/testing/executor-input.js";
import type { Rel } from "../packages/core/dist/next/ir/ir.js";
import type { Batch } from "../packages/core/dist/next/vector/vector.js";
import type { Row } from "../packages/core/dist/next/exec/join-fixtures.js";
import type { Operator } from "../packages/core/dist/next/exec/pipeline.js";

export async function percentileComponent(
  root: string,
  core: StorageCore,
  count: number,
  candidate: boolean,
  generator: {
    wideBatch(
      spec: unknown,
      first: number,
      rows: number,
      count: number,
    ): Record<string, InputValue[]>;
  },
  spec: unknown,
) {
  const load = (name: string) =>
    import(pathToFileURL(join(root, "packages/core/dist/next", name + ".js")).href);
  const memory = (await load(
    "memory/governor",
  )) as typeof import("../packages/core/dist/next/memory/governor.js");
  const input = (await load(
    "testing/executor-input",
  )) as typeof import("../packages/core/dist/next/testing/executor-input.js");
  const fixtures = (await load(
    "exec/join-fixtures",
  )) as typeof import("../packages/core/dist/next/exec/join-fixtures.js");
  const arenas = (await load(
    "exec/kernels/vectors",
  )) as typeof import("../packages/core/dist/next/exec/kernels/vectors.js");
  const ordered = (await load(
    "exec/agg/ordered-set",
  )) as typeof import("../packages/core/dist/next/exec/agg/ordered-set.js");
  const selected = candidate
    ? ((await load(
        "exec/agg/percentile",
      )) as typeof import("../packages/core/dist/next/exec/agg/percentile.js"))
    : null;
  const relation = core.prepare(
    "SELECT d02,percentile_cont(0.5) WITHIN GROUP(ORDER BY f08) med,percentile_disc(0.9) WITHIN GROUP(ORDER BY i08) p90 FROM events_wide GROUP BY d02",
  ).plan.root;
  function find(rel: Rel): Extract<Rel, { op: "aggregate" }> {
    if (rel.op === "aggregate") return rel;
    if ("input" in rel) return find(rel.input);
    throw Error("Missing component aggregate");
  }
  const rel = find(relation),
    project = rel.input;
  if (project.op !== "project" || project.input.op !== "scan")
    throw Error("Unproven component input layout");
  const scan = project.input;
  const table = core.harness.path.catalog.tableByName("events_wide");
  if (!table) throw Error("No fixture table");
  const columns = project.exprs.map((expr) => {
    if (expr.op !== "column") throw Error("Computed component input");
    const id = scan.columns[expr.index],
      column = table.columns.find((c) => c.id === id);
    if (!column) throw Error("Missing component column");
    return column;
  });
  const types = columns.map((c) => c.type);
  const fixtureGov = new memory.Governor(256 << 20),
    fixtureGrant = await fixtureGov.admit({
      label: "quiet/decoded-percentile-fixture",
      minimumBytes: 128 << 20,
      spillable: false,
    });
  const batches: Batch[] = [],
    closes: Array<() => void> = [];
  let dictionaryBytes = 0;
  try {
    for (let first = 0; first < count; first += 2048) {
      const n = Math.min(2048, count - first),
        raw = generator.wideBatch(spec, first, n, count);
      const batch = input.inputBatch({ ...table, columns }, raw, fixtureGrant);
      closes.push(() => batch.close());
      const vectors = [...batch.batch.columns];
      for (const key of rel.groupBy) {
        const column = columns[key];
        if (!column) throw Error("Missing component key");
        const values = raw[column.name];
        if (!values) throw Error("Missing fixture values");
        const bytes = 128 * 1024;
        if (!fixtureGrant.allocate(bytes)) throw Error("Decoded dictionary fixture not admitted");
        dictionaryBytes += bytes;
        vectors[key] = fixtures.vectorOf(
          column.type,
          values.map((value) => (value === null ? null : String(value))),
          { dictionary: true },
        );
      }
      batches.push({ ...batch.batch, columns: vectors });
    }
  } catch (error) {
    for (const close of closes) close();
    if (dictionaryBytes) fixtureGrant.free(dictionaryBytes);
    fixtureGrant.close();
    throw error;
  }
  const bound = { rows: count, groups: 1024, keyMinimum: 8 << 20 };
  const result: Array<readonly Row[]> = [];
  return {
    async run() {
      result.length = 0;
      const governor = new memory.Governor(256 << 20),
        minimum = selected
          ? selected.percentileMinimum(rel, bound, 0)
          : ordered.orderedSetMinimumBytes(rel, types);
      const grants = await governor.admitGroup({
        label: "quiet/percentile-component",
        includesProgress: true,
        grants: [
          { label: "fixed", minimumBytes: minimum, spillable: true },
          ...Array.from({ length: 4 }, (_, i) => ({
            label: `loan${i}`,
            minimumBytes: 2 << 20,
            spillable: true,
          })),
        ],
      });
      const grant = grants[0];
      if (!grant) throw Error("No component grant");
      const arena = new arenas.OutputArena(grant, 2048),
        signal = new AbortController().signal;
      const sink: Operator = {
        push(batch) {
          result.push(fixtures.rowsOf(batch));
          return "ready";
        },
        ready: () => Promise.resolve(),
        finish: () => undefined,
        abort: () => undefined,
      };
      const context = { input: types, arena, params: { slots: [] } };
      let operator: (Operator & { readonly closed: Promise<void> }) | null = null;
      try {
        operator = selected
          ? new selected.PercentileOperator(rel, context, grant, bound, signal, sink)
          : new ordered.OrderedSetOperator(
              rel,
              context,
              { grant, loans: grants.slice(1), spill: core.spill, queryId: 0x7fffffff, signal },
              sink,
            );
        for (const batch of batches) if (operator.push(batch) === "blocked") await operator.ready();
        await operator.finish();
        await operator.closed;
      } catch (error) {
        operator?.abort(error);
        await operator?.closed.catch(() => undefined);
        throw error;
      } finally {
        arena.close();
        for (const grant of grants) grant.close();
      }
      if (governor.usedBytes !== 0 || core.spill.bytesInUse !== 0)
        throw Error("Component resource leak");
      return { rows: result.reduce((n, rows) => n + rows.length, 0), digest: "" };
    },
    digest() {
      const rows = result
        .flat()
        .map((row) =>
          JSON.stringify(row, (_key, value: unknown) =>
            typeof value === "number"
              ? ["number", Object.is(value, -0) ? "-0" : String(value)]
              : value,
          ),
        )
        .sort();
      return createHash("sha256").update(JSON.stringify(rows)).digest("hex");
    },
    close() {
      for (const close of closes) close();
      fixtureGrant.free(dictionaryBytes);
      fixtureGrant.close();
      if (fixtureGov.usedBytes !== 0) throw Error("Decoded fixture leak");
    },
  };
}
