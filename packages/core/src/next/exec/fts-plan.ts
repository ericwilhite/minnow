/** FTS compiler seam: query validation, workspace sizing and borrowed-source lifetimes.
 * Generic planner routes supply actual expressions, progress hooks and counted sinks.
 * This module never chooses scan batchRows or aggregate/result admission strategies.
 */
import type { PhysicalPlan, Rel, ScalarExpr } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import type { MaybePromise } from "../access/key-index.js";
import type { Grant } from "../memory/governor.js";
import { boundSource, readValue, type ReadSource } from "../page/read-bounds.js";
import { FtsExpressions } from "./fts-expressions.js";
import { ftsExpression } from "../ir/fts-expression.js";
import { FtsScanOperator, ftsScanMinimum } from "./fts-scan.js";
import { FTS_WORKSPACE_BYTES, ftsWorkspaceBytes, ftsDeclaredBytes } from "./fts-value.js";
import { queryTerms, queryText, configureFtsWorkspace, ftsRuntimeBytes } from "./kernels/fts.js";
import type { CompileContext } from "./kernels/compile.js";
import type { OutputArena } from "./kernels/vectors.js";
import type { QueryRuntime, Program, OperatorIdentity } from "./run.js";
import type { ResultSink } from "./result-sink.js";
import { validateParameterCount } from "./parameters.js";
import type { TableScan } from "./table-scan.js";
import type { ProjectOperator } from "./filter.js";
import { drive, isPromise, type Operator, type BatchSource } from "./pipeline.js";
import { Slice, runSliced, type SlicedJob } from "./slices.js";

export function configurePlanFtsArena(runtime: QueryRuntime, arena: OutputArena): void {
  configureFtsWorkspace(arena, ftsRuntimeBytes.get(runtime) ?? FTS_WORKSPACE_BYTES);
}
interface FtsOwner {
  readonly closed: Promise<void>;
}
export class FtsPlan {
  readonly #workspaces = new Set<number>();
  readonly #owners = new WeakMap<QueryRuntime, Set<FtsOwner>>();
  declared = 0;
  constructor(readonly plan: PhysicalPlan) {}
  get enabled(): boolean {
    return this.#workspaces.size > 0;
  }
  inspect(rel: Rel): void {
    if (this.plan.ftsQueries?.length || rel.op === "fts") {
      let ownsFts = rel.op === "fts";
      const inspect = (item: unknown): boolean => {
        if (!item || typeof item !== "object" || ArrayBuffer.isView(item)) return false;
        let found = false;
        if ("op" in item && item.op === "call") {
          const call = item as Extract<ScalarExpr, { op: "call" }>;
          if (call.fn === "fts_match") {
            found = true;
            for (const arg of call.args.slice(1))
              this.declared = Math.max(this.declared, ftsDeclaredBytes(arg.type));
          }
        }
        for (const value of Object.values(item)) if (inspect(value)) found = true;
        return found;
      };
      for (const [key, value] of Object.entries(rel))
        if (!["input", "left", "right", "inputs", "base", "step"].includes(key) && inspect(value))
          ownsFts = true;
      if (rel.op === "fts")
        for (const call of rel.calls)
          for (const column of call.columns) {
            const type = rel.input.output[column]?.type;
            if (type) this.declared = Math.max(this.declared, ftsDeclaredBytes(type));
          }
      if (ownsFts) this.#workspaces.add(rel.id);
    }
  }
  #retain(runtime: QueryRuntime, owner: FtsOwner): void {
    let owners = this.#owners.get(runtime);
    if (!owners) {
      owners = new Set();
      this.#owners.set(runtime, owners);
    }
    owners.add(owner);
  }
  #release(runtime: QueryRuntime, owner: FtsOwner): void {
    this.#owners.get(runtime)?.delete(owner);
  }
  borrow(runtime: QueryRuntime, id: number, owner: FtsOwner): void {
    if (this.#workspaces.has(id)) this.#retain(runtime, owner);
  }
  forget(runtime: QueryRuntime, owner: FtsOwner): void {
    this.#release(runtime, owner);
  }
  minimum(input: readonly PhysicalType[], calls: number): number {
    return ftsScanMinimum(input, calls);
  }
  extra(runtime: QueryRuntime, id: number): number {
    if (!this.#workspaces.has(id)) return 0;
    const bytes = ftsRuntimeBytes.get(runtime) ?? ftsWorkspaceBytes(0);
    ftsRuntimeBytes.set(runtime, bytes);
    return bytes - FTS_WORKSPACE_BYTES;
  }
  size(runtime: QueryRuntime, sourceBytes: number): void {
    if (this.#workspaces.size)
      ftsRuntimeBytes.set(
        runtime,
        Math.max(ftsRuntimeBytes.get(runtime) ?? 0, ftsWorkspaceBytes(sourceBytes)),
      );
  }
  *sourceBytes(runtime: QueryRuntime, source: ReadSource, rows: number | null): SlicedJob<number> {
    if (!this.#workspaces.size) return 0;
    let bytes = 0;
    // Catalog missing constants are not represented in old groups' page directories.
    const table = runtime.view.catalog.table(source.tableId);
    for (const column of source.columns) {
      const value = table?.columns.find((def) => def.id === column)?.missing;
      if (value?.data.layout === "varlen") bytes = Math.max(bytes, value.data.bytes.length);
    }
    const row = yield* readValue(
      boundSource(
        runtime.sourceView(),
        {
          ...source,
          singleValueBound: source.columns.length === 1,
          windowBytes: 0,
          stagingBytes: 0,
          rows,
        },
        runtime.request.governor,
        runtime.signal,
        1,
        1,
      ),
    );
    return Math.max(bytes, row.valueBytes ?? row.progress / 4);
  }
  scan(
    runtime: QueryRuntime,
    rel: OperatorIdentity,
    scan: TableScan,
    downstream: Operator,
    stops: ReadonlyArray<() => boolean>,
  ): MaybePromise<void> {
    let borrowed: Promise<void> | undefined;
    const abortScan = () => {
      if (borrowed) return;
      const owners = [...(this.#owners.get(runtime) ?? [])];
      if (!owners.length) {
        scan.abort();
        return;
      }
      borrowed = Promise.resolve()
        .then(() => Promise.allSettled(owners.map((owner) => owner.closed)))
        .then(() => scan.abort());
    };
    runtime.onAbort(abortScan);
    const source: BatchSource = {
      next: () => (stops.some((stop) => stop()) ? (scan.abort(), null) : scan.next()),
      abort: abortScan,
    };
    runtime.pipeline();

    const profile = () => {
      const p = runtime.profile(rel);
      p.pagesRead = scan.stats.pagesRead;
      p.pagesPruned = scan.stats.pagesPruned;
    };
    if (this.plan.ftsQueries?.length)
      return (async () => {
        try {
          await drive(source, downstream, runtime.signal);
        } finally {
          await borrowed;
          scan.close();
        }
        profile();
      })();
    const result = drive(source, downstream, runtime.signal);
    const finish = () => {
      profile();
      scan.close();
    };
    return isPromise(result) ? result.then(finish) : finish();
  }
  operator(
    runtime: QueryRuntime,
    input: readonly PhysicalType[],
    calls: Extract<Rel, { op: "fts" }>["calls"],
    context: CompileContext,
    grant: Grant,
    sink: Operator,
  ): FtsScanOperator {
    return new FtsScanOperator(
      input,
      calls,
      context,
      {
        grant,
        spill: runtime.request.spill,
        queryId: runtime.request.queryId,
        signal: runtime.signal,
      },
      sink,
    );
  }
  async corpus(
    runtime: QueryRuntime,
    rel: OperatorIdentity,
    operator: FtsScanOperator,
    run: () => MaybePromise<void>,
  ): Promise<void> {
    this.#retain(runtime, operator);
    runtime.onAbort((reason) => operator.abort(reason));
    runtime.dispose(() => operator.closed);
    try {
      await run();
    } catch (error) {
      operator.abort(error);
      await operator.closed;
      throw error;
    } finally {
      this.#release(runtime, operator);
    }
    const profile = runtime.profile(rel);
    profile.spilledBytes = operator.profile.spilledBytes;
    profile.spillPasses = operator.profile.mergePasses;
  }
  expressions(
    exprs: readonly ScalarExpr[] | ScalarExpr,
    context: CompileContext,
    sink: Operator,
    runtime: QueryRuntime | undefined,
    progress: ProjectOperator["progress"],
    stops: ReadonlyArray<() => boolean>,
  ): FtsExpressions | null {
    if (!(Array.isArray(exprs) ? exprs.some(ftsExpression) : ftsExpression(exprs as ScalarExpr)))
      return null;
    return new FtsExpressions(
      exprs,
      context,
      sink,
      runtime
        ? { signal: runtime.signal, stopped: () => stops.some((stop) => stop()), progress }
        : {},
    );
  }
  runExpressions(
    runtime: QueryRuntime,
    operator: Operator,
    run: () => MaybePromise<void>,
  ): MaybePromise<void> {
    if (!(operator instanceof FtsExpressions)) return run();
    this.#retain(runtime, operator);
    runtime.dispose(() => operator.closed);
    return (async () => {
      try {
        await run();
      } catch (error) {
        operator.abort(error);
        await operator.closed;
        throw error;
      } finally {
        this.#release(runtime, operator);
      }
    })();
  }
  wrap(program: Program, nextPhysicalId: () => number): Program {
    const plan = this.plan;
    if (plan.ftsQueries?.length) {
      const queries = plan.ftsQueries;
      const originalQuerySlot = (slot: number) =>
        plan.ftsParameters?.find((mapping) => mapping.slot === slot)?.source ?? slot;
      const validation: OperatorIdentity = { id: nextPhysicalId(), op: "fts" };
      return Object.assign(
        async (runtime: QueryRuntime, sink: ResultSink) => {
          validateParameterCount(plan, runtime.request.parameters);
          for (const query of queries)
            if (
              query.op === "param" &&
              typeof runtime.request.parameters[originalQuerySlot(query.slot)] !== "string"
            )
              throw new TypeError("A full-text search query parameter must be a string");
          let queryBytes = 0;
          for (const query of queries)
            queryBytes = Math.max(
              queryBytes,
              query.op === "param"
                ? (runtime.request.parameters[originalQuerySlot(query.slot)] as string).length
                : query.op === "literal" && query.value.data.layout === "varlen"
                  ? query.value.data.bytes.length
                  : 16384,
            );
          const workspace = ftsWorkspaceBytes(queryBytes);
          ftsRuntimeBytes.set(runtime, workspace);
          const grant = await runtime.admit(
            `ftsQuery#${String(validation.id)}`,
            workspace,
            false,
            validation,
          );
          if (!grant.allocate(workspace)) throw new Error("FTS query scratch was not admitted");
          try {
            const slice = new Slice(4);
            const validate = function* (): SlicedJob<void> {
              for (const query of queries) {
                if (query.op === "param")
                  queryTerms(runtime.request.parameters[originalQuerySlot(query.slot)] as string);
                else if (query.op === "literal") queryTerms(queryText(query.value));
                else throw new TypeError("FTS queries must be literals or parameters");
                if (slice.due()) yield;
              }
            };
            await runSliced(validate(), slice, () => runtime.signal.throwIfAborted());
          } finally {
            grant.free(workspace);
            grant.close();
          }
          await program(runtime, sink);
        },
        { atomic: true },
      );
    }
    return Object.assign(program, { atomic: true });
  }
}
