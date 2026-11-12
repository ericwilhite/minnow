/** Component input only: the four referenced fact fields and complete dimensions are written
 * through the real native storage path, then scanned/owned before any component clock.
 * This deliberately narrow physical fixture excludes the 196 unused stored fields: it is
 * NOT a whole-query benchmark. Full wide-query timing uses the shared balanced runner.
 */
import { strict as assert } from "node:assert";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { Rel, PhysicalPlan } from "../../packages/core/dist/next/ir/ir.js";
import type { Batch, FlatVector, Vector } from "../../packages/core/dist/next/vector/vector.js";
import type { StorageCore } from "../../packages/core/dist/next/testing/storage-core.js";
export const STAR_SQL =
  "SELECT a.tier AS tier_a,b.tier AS tier_b,c.label AS label_c,COUNT(*) AS n,SUM(e.f00) AS s FROM events_wide e JOIN dim_d03 a ON a.k=e.d03 JOIN dim_d04 b ON b.k=e.d04 JOIN dim_d07 c ON c.k=e.d07 WHERE a.active AND b.weight>1 AND c.tier='t1' GROUP BY a.tier,b.tier,c.label ORDER BY s DESC,tier_a,tier_b,label_c LIMIT 50";
export interface StarRow {
  tier_a: string;
  tier_b: string;
  label_c: string;
  n: number;
  s: number;
}
export interface StarInput {
  readonly plan: PhysicalPlan;
  readonly aggregate: Extract<Rel, { op: "aggregate" }>;
  readonly scans: ReadonlyMap<number, readonly Batch[]>;
  readonly named: ReadonlyMap<
    string,
    { readonly columns: readonly string[]; readonly batches: readonly Batch[] }
  >;
  readonly expected: readonly StarRow[];
  readonly rows: number;
  readonly shape: unknown;
}
export async function starFixture(runtime: string, rows: number): Promise<StarInput> {
  const load = (name: string) =>
    import(pathToFileURL(join(runtime, "packages/core/dist/next", name + ".js")).href);
  const [
    { StorageCore },
    { inputBatch },
    { TableScan },
    { readTable },
    { ChunkReader },
    { OutputArena },
    { oracleRows },
  ] = (await Promise.all([
    load("testing/storage-core"),
    load("testing/executor-input"),
    load("exec/table-scan"),
    load("exec/read-table"),
    load("page/reader"),
    load("exec/kernels/vectors"),
    load("testing/differential-adapter"),
  ])) as [
    typeof import("../../packages/core/dist/next/testing/storage-core.js"),
    typeof import("../../packages/core/dist/next/testing/executor-input.js"),
    typeof import("../../packages/core/dist/next/exec/table-scan.js"),
    typeof import("../../packages/core/dist/next/exec/read-table.js"),
    typeof import("../../packages/core/dist/next/page/reader.js"),
    typeof import("../../packages/core/dist/next/exec/kernels/vectors.js"),
    typeof import("../../packages/core/dist/next/testing/differential-adapter.js"),
  ];
  const generator = (await import(
    new URL("../next-executor-workload/gen.mjs", import.meta.url).href
  )) as {
    wideSpec(n: number): Array<{ name: string; pg: string }>;
    wideBatch(
      spec: unknown,
      from: number,
      n: number,
      rows: number,
    ): Record<string, Array<boolean | number | string | null>>;
    dimTables(): Array<{
      name: string;
      rows: Array<{ key: string; label: string; tier: string; weight: number; active: boolean }>;
    }>;
  };
  const core: StorageCore = await new StorageCore(8192).open();
  try {
    const selected = new Set(["d03", "d04", "d07", "f00"]),
      spec = generator.wideSpec(200).filter((c) => selected.has(c.name));
    await core.execute(
      `CREATE TABLE events_wide (${spec.map((c) => `${c.name} ${c.pg}`).join(",")})`,
    );
    const table = core.harness.path.catalog.tableByName("events_wide");
    if (!table) throw new Error("fact table");
    const input = await core.harness.governor.admit({
      label: "star fixture input",
      minimumBytes: 8 << 20,
      spillable: true,
    });
    try {
      for (let from = 0; from < rows; from += 2048) {
        const values = inputBatch(
          table,
          generator.wideBatch(spec, from, Math.min(2048, rows - from), rows),
          input,
        );
        try {
          await core.insertVectors(table, values.batch);
        } finally {
          values.close();
        }
      }
    } finally {
      input.close();
    }
    for (const dim of generator.dimTables()) {
      await core.execute(
        `CREATE TABLE ${dim.name} (k TEXT PRIMARY KEY,label TEXT,tier TEXT,weight DOUBLE PRECISION,active BOOLEAN)`,
      );
      await core.writeBatch(
        "insertBatch",
        dim.name,
        dim.rows.map(({ key, ...rest }) => ({ k: key, ...rest })),
      );
    }
    await core.harness.path.recycleLog();
    await core.harness.path.quiesce();
    const prepared = core.prepare(STAR_SQL),
      lowered = prepared.plan;
    const result = await core.execute(STAR_SQL, []);
    if (result.kind !== "rows" || !("batches" in result)) throw new Error("missing SQL reference");
    const expected = oracleRows(result).map((row) => ({
      tier_a: String(row.tier_a),
      tier_b: String(row.tier_b),
      label_c: String(row.label_c),
      n: Number(row.n),
      s: Number(row.s),
    }));
    const found: Array<Extract<Rel, { op: "scan" }>> = [];
    let aggregate: Extract<Rel, { op: "aggregate" }> | null = null;
    const visit = (rel: Rel) => {
      if (rel.op === "scan") found.push(rel);
      if (rel.op === "aggregate") aggregate = rel;
      if ("input" in rel) visit(rel.input);
      if ("left" in rel && "right" in rel) {
        visit(rel.left);
        visit(rel.right);
      }
    };
    visit(lowered.root);
    if (!aggregate) throw new Error("star aggregate missing");
    const captured = new Map<number, readonly Batch[]>(),
      named = new Map<string, { columns: string[]; batches: readonly Batch[] }>();
    const pin = core.harness.path.pin(),
      grant = await core.harness.governor.admit({
        label: "star input capture",
        minimumBytes: 64 << 20,
        spillable: false,
      }),
      arena = new OutputArena(grant, 2048);
    const dictionaries = new Map<number, FlatVector>();
    const flat = (vector: FlatVector): FlatVector => {
      const d = vector.data,
        validity = vector.validity?.slice() ?? null;
      if (d.layout === "varlen") {
        const from = d.offsets[0] ?? 0,
          end = d.offsets[vector.length] ?? 0;
        return {
          ...vector,
          validity,
          data: {
            layout: "varlen",
            bytes: d.bytes.slice(from, end),
            offsets: Int32Array.from(d.offsets.subarray(0, vector.length + 1), (p) => p - from),
          },
        };
      }
      if (d.layout === "f64" || d.layout === "bool")
        return {
          ...vector,
          validity,
          data: {
            layout: d.layout,
            values: d.values.slice(0, vector.length),
          } as FlatVector["data"],
        };
      throw new Error(`Unexpected star fixture layout ${d.layout}`);
    };
    const own = (vector: Vector): Vector => {
      if (vector.form === "flat") return flat(vector);
      if (vector.form === "constant") return { ...vector, value: flat(vector.value) };
      let value = dictionaries.get(vector.dictionaryId);
      if (!value) {
        value = flat(vector.dictionary);
        dictionaries.set(vector.dictionaryId, value);
      }
      return {
        ...vector,
        validity: vector.validity?.slice() ?? null,
        codes: vector.codes.slice(0, vector.length),
        dictionary: value,
      };
    };
    try {
      for (const source of found) {
        const def = core.harness.path.catalog.table(source.tableId);
        if (!def) throw new Error("source definition");
        // Capture before pushed predicates; the component includes the predicate kernels.
        const table = readTable(
          pin.table(source.tableId, { budget: grant }),
          pin.pages,
          grant,
          () => undefined,
        );
        const scan = new TableScan({
          table,
          pages: pin.pages,
          reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
          budget: grant,
          columns: source.columns,
          filter: null,
          emitLocator: false,
          context: { arena, input: [], params: { slots: [] } },
        });
        const batches: Batch[] = [];
        try {
          for (;;) {
            const item = await scan.next();
            if (item === null) break;
            try {
              batches.push({
                length: item.batch.length,
                columns: item.batch.columns.map(own),
                selection: item.batch.selection
                  ? { count: item.batch.selection.count, rows: item.batch.selection.rows.slice() }
                  : null,
              });
            } finally {
              item.transfer?.release();
            }
          }
        } finally {
          scan.abort();
        }
        captured.set(source.id, batches);
        named.set(def.name, {
          columns: source.columns.map((id) => {
            const c = def.columns.find((c) => c.id === id);
            if (!c) throw new Error("capture column");
            return c.name;
          }),
          batches,
        });
      }
    } finally {
      arena.close();
      assert.equal(grant.allocated, 0);
      grant.close();
      pin.release();
    }
    const capturedRows = (name: string) =>
      named
        .get(name)
        ?.batches.reduce((n, batch) => n + (batch.selection?.count ?? batch.length), 0);
    assert.equal(capturedRows("events_wide"), rows, "incomplete decoded fact fixture");
    for (const dim of generator.dimTables())
      assert.equal(capturedRows(dim.name), dim.rows.length, "incomplete decoded dimension fixture");
    const shape = (rel: Rel): unknown => ({
      id: rel.id,
      op: rel.op,
      columns: rel.output.map((c) => c.name),
      ...(rel.op === "scan"
        ? { table: core.harness.path.catalog.table(rel.tableId)?.name, filter: rel.filter }
        : {}),
      ...(rel.op === "join"
        ? { leftKeys: rel.leftKeys, rightKeys: rel.rightKeys, kind: rel.kind }
        : {}),
      ...("input" in rel ? { input: shape(rel.input) } : {}),
      ...("left" in rel && "right" in rel
        ? { left: shape(rel.left), right: shape(rel.right) }
        : {}),
    });
    return {
      plan: lowered,
      aggregate,
      scans: captured,
      named,
      expected,
      rows,
      shape: shape(lowered.root),
    };
  } finally {
    await core.close();
  }
}
