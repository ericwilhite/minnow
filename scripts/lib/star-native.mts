/** Three native joins plus the actual lowered filters/projections/aggregate, over owned decoded
 * batches. The final 4k-row ordering is common JS ASCII order in both component kernels.
 */
import { strict as assert } from "node:assert";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { Rel, ScalarExpr } from "../../packages/core/dist/next/ir/ir.js";
import type { PhysicalType } from "../../packages/core/dist/next/types.js";
import type { Operator } from "../../packages/core/dist/next/exec/pipeline.js";
import type { StarInput, StarRow } from "./star-fixture.mts";
export function finishStar(rows: StarRow[]): StarRow[] {
  rows.sort(
    (a, b) =>
      b.s - a.s ||
      (a.tier_a < b.tier_a ? -1 : a.tier_a > b.tier_a ? 1 : 0) ||
      (a.tier_b < b.tier_b ? -1 : a.tier_b > b.tier_b ? 1 : 0) ||
      (a.label_c < b.label_c ? -1 : a.label_c > b.label_c ? 1 : 0),
  );
  return rows.slice(0, 50);
}
export async function nativeStar(
  runtime: string,
  fixture: StarInput,
  encoded = false,
  signal?: AbortSignal,
) {
  const load = (name: string) =>
    import(pathToFileURL(join(runtime, "packages/core/dist/next", name + ".js")).href);
  const [
    { Governor },
    { MemorySpillService },
    { HashJoin },
    { OutputArena },
    { FilterOperator, ProjectOperator },
    { AggregateOperator },
    { drive },
    { valueAt },
  ] = (await Promise.all([
    load("memory/governor"),
    load("spill/memory-spill"),
    load("exec/join"),
    load("exec/kernels/vectors"),
    load("exec/filter"),
    load("exec/agg/aggregate"),
    load("exec/pipeline"),
    load("exec/kernels/test-support"),
  ])) as [
    typeof import("../../packages/core/dist/next/memory/governor.js"),
    typeof import("../../packages/core/dist/next/spill/memory-spill.js"),
    typeof import("../../packages/core/dist/next/exec/join.js"),
    typeof import("../../packages/core/dist/next/exec/kernels/vectors.js"),
    typeof import("../../packages/core/dist/next/exec/filter.js"),
    typeof import("../../packages/core/dist/next/exec/agg/aggregate.js"),
    typeof import("../../packages/core/dist/next/exec/pipeline.js"),
    typeof import("../../packages/core/dist/next/exec/kernels/test-support.js"),
  ];
  const shape = encoded
    ? (
        (await load(
          "exec/star-shape",
        )) as typeof import("../../packages/core/dist/next/exec/star-shape.js")
      ).starDictionaryShape(fixture.aggregate, false)
    : null;
  if (encoded && !shape) throw new Error("Uncertified component shape");
  const Books = encoded
    ? (
        (await load(
          "exec/star-dictionary",
        )) as typeof import("../../packages/core/dist/next/exec/star-dictionary.js")
      ).StarDictionaryBooks
    : null;
  const typed = (rel: Rel) => shape?.typed.get(rel.id) ?? rel;
  const nodes: Rel[] = [];
  const visit = (rel: Rel) => {
    nodes.push(rel);
    if ("input" in rel) visit(rel.input);
    if ("left" in rel && "right" in rel) {
      visit(rel.left);
      visit(rel.right);
    }
  };
  visit(fixture.aggregate);
  const originals = new Map(nodes.map((node) => [node.id, node]));
  const governor = new Governor(512 << 20);
  const columns = (types: readonly PhysicalType[]): ScalarExpr[] =>
    types.map((type, index) => ({ op: "column", index, type }));
  return {
    async prepare() {
      return {
        async run(): Promise<StarRow[]> {
          const granted = await governor.admitGroup({
            label: "decoded star component",
            ...(signal === undefined ? {} : { signal }),
            grants: [
              ...(encoded ? [{ label: "books", minimumBytes: 16 << 20, spillable: false }] : []),
              ...nodes.flatMap((rel) => [
                { label: `arena-${rel.id}`, minimumBytes: 2 << 20, spillable: false },
                ...(rel.op === "join"
                  ? [{ label: `state-${rel.id}`, minimumBytes: 64 << 20, spillable: true }]
                  : rel.op === "aggregate"
                    ? [{ label: `state-${rel.id}`, minimumBytes: 16 << 20, spillable: true }]
                    : []),
              ]),
            ],
          });
          const budget = new Map(granted.map((grant) => [grant.label, grant]));
          const grant = (label: string) => {
            const result = budget.get(label);
            if (!result) throw new Error(`Missing component grant ${label}`);
            return result;
          };
          const arenas: Array<InstanceType<typeof OutputArena>> = [],
            joins: Array<InstanceType<typeof HashJoin>> = [],
            all: Operator[] = [];
          let books: InstanceType<NonNullable<typeof Books>> | null = null;
          try {
            books = shape && Books ? new Books(shape, grant("books")) : null;
            const spill = new MemorySpillService(),
              rows: StarRow[] = [];
            const context = (rel: Rel, input: readonly PhysicalType[]) => {
              const arena = new OutputArena(grant(`arena-${rel.id}`), 2048);
              arenas.push(arena);
              return { arena, input, params: { slots: [] } };
            };
            const sink: Operator = {
              push(inputBatch) {
                const batch = books?.decode(inputBatch) ?? inputBatch;
                for (let row = 0; row < batch.length; row++) {
                  const values = batch.columns.map((column) => valueAt(column, row));
                  const [tier_a, tier_b, label_c, n, s] = values;
                  if (
                    typeof tier_a !== "string" ||
                    typeof tier_b !== "string" ||
                    typeof label_c !== "string" ||
                    typeof s !== "number" ||
                    !(typeof n === "bigint" || typeof n === "number")
                  )
                    throw new Error("Unexpected star aggregate output");
                  assert.ok(Number.isSafeInteger(Number(n)));
                  rows.push({ tier_a, tier_b, label_c, n: Number(n), s });
                }
                return "ready";
              },
              ready: () => Promise.resolve(),
              finish: () => undefined,
              abort: () => undefined,
            };
            const aggregate = new AggregateOperator(
              shape?.aggregate ?? fixture.aggregate,
              context(
                fixture.aggregate,
                (shape?.aggregate ?? fixture.aggregate).input.output.map((c) => c.type),
              ),
              grant(`state-${fixture.aggregate.id}`),
              sink,
              signal === undefined ? {} : { signal },
            );
            all.push(aggregate);
            const build = (original: Rel, downstream: Operator): (() => Promise<void>) => {
              const rel = typed(original);
              const input = "input" in rel ? rel.input.output.map((c) => c.type) : [];
              if (rel.op === "scan") {
                const batches = fixture.scans.get(rel.id);
                if (!batches) throw new Error("Missing decoded source");
                const sourceBooks = books;
                const terminal: Operator = sourceBooks
                  ? {
                      push: (batch) => downstream.push(sourceBooks.encode(rel.id, batch)),
                      ready: () => downstream.ready(),
                      finish: () => downstream.finish(),
                      abort: (reason) => downstream.abort(reason),
                    }
                  : downstream;
                const scan = originals.get(original.id) as Extract<Rel, { op: "scan" }>;
                const op = scan.filter
                  ? new FilterOperator(
                      scan.filter,
                      context(
                        scan,
                        scan.output.map((c) => c.type),
                      ),
                      terminal,
                    )
                  : terminal;
                if (op !== downstream) all.push(op);
                return async () => {
                  let at = 0;
                  await drive(
                    {
                      next: () => {
                        if (at === batches.length) return null;
                        const batch = batches[at++];
                        if (!batch) throw new Error("Missing input batch");
                        return { batch };
                      },
                      abort: () => undefined,
                    },
                    op,
                    signal,
                  );
                };
              }
              if (rel.op === "project" || rel.op === "filter") {
                const op =
                  rel.op === "project"
                    ? new ProjectOperator(rel.exprs, context(rel, input), downstream)
                    : new FilterOperator(rel.predicate, context(rel, input), downstream);
                all.push(op);
                return build(rel.input, op);
              }
              if (
                rel.op !== "join" ||
                rel.kind !== "inner" ||
                rel.residual !== null ||
                (rel.filter ?? null) !== null
              )
                throw new Error(`Unsupported component node ${rel.op}`);
              const lt = rel.left.output.map((c) => c.type),
                rt = rel.right.output.map((c) => c.type),
                ctx = context(rel, [...lt, ...rt]);
              const keys = (types: readonly PhysicalType[], exprs: readonly ScalarExpr[]) => {
                const output = columns(types),
                  indexes = exprs.map((expr) => {
                    if (expr.op === "column") return expr.index;
                    const index = output.length;
                    output.push(expr);
                    return index;
                  });
                return { exprs: output, indexes, types: output.map((expr) => expr.type) };
              };
              const left = keys(lt, rel.leftKeys),
                right = keys(rt, rel.rightKeys);
              const join = new HashJoin(
                {
                  kind: "inner",
                  probeTypes: left.types,
                  buildTypes: right.types,
                  probeKeys: left.indexes,
                  buildKeys: right.indexes,
                  nullSafe: rel.nullSafe,
                  residual: null,
                  publishes: [],
                },
                {
                  grant: grant(`state-${rel.id}`),
                  spill,
                  queryId: 1,
                  relId: rel.id,
                  ...(signal === undefined ? {} : { signal }),
                },
              );
              joins.push(join);
              const out = [
                ...columns(lt),
                ...rt.map((type, c): ScalarExpr => ({
                  op: "column",
                  index: left.types.length + c,
                  type,
                })),
              ];
              const output = new ProjectOperator(
                out,
                { ...ctx, input: [...left.types, ...right.types] },
                downstream,
              );
              const probe = new ProjectOperator(
                  left.exprs,
                  { ...ctx, input: lt },
                  join.probe(output),
                ),
                buildSink = new ProjectOperator(right.exprs, { ...ctx, input: rt }, join.build);
              all.push(output, probe, buildSink);
              const rightJob = build(rel.right, buildSink),
                leftJob = build(rel.left, probe);
              return async () => {
                await rightJob();
                await leftJob();
              };
            };
            await build(fixture.aggregate.input, aggregate)();
            return finishStar(rows);
          } finally {
            for (const op of all) op.abort(undefined);
            for (const join of joins) join.abort(undefined);
            await Promise.all(joins.map((join) => join.closed));
            for (const arena of arenas) arena.close();
            books?.close();
            for (const value of granted) {
              assert.equal(value.allocated, 0);
              value.close();
            }
            assert.equal(governor.usedBytes, 0);
          }
        },
        close() {
          assert.equal(governor.usedBytes, 0);
        },
      };
    },
  };
}
