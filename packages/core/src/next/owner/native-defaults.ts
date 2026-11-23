/** Ordered RI defaults over the same lowered, typed expressions as ordinary INSERT.
 * Kernels without a native implementation still refuse at preparation, before writes. */
import type { CatalogView } from "../access/catalog.js";
import { retryGuard } from "../memory/retry-guard.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { FlatVector } from "../vector/vector.js";
import { samePhysicalType } from "../types.js";
import { compileExpr } from "../exec/kernels/compile.js";
import { OutputArena, ExpressionMemoryError, type Compiled } from "../exec/kernels/vectors.js";
import { retainedBytes } from "./rule-cache.js";
import type { RegistryImagePatch } from "./registry-images.js";

export interface NativeDefaultSource {
  readonly tableId: number;
  readonly columnId: number;
  readonly expr: ScalarExpr | null;
}
export interface NativeDefaultScope {
  readonly catalog: CatalogView;
  readonly sources: readonly NativeDefaultSource[];
  readonly clock: FlatVector;
  readonly charge: ByteCharge;
  readonly signal: AbortSignal;
  /** Root-owned work step, invoked with no image/source borrow. */
  prepare(bytes: number, signal: AbortSignal): Promise<void>;
}
function compile(
  sources: readonly NativeDefaultSource[],
  arena: OutputArena,
  clock: FlatVector,
): Array<Compiled | null> {
  const context = { arena, params: { slots: [clock] }, input: [] };
  return sources.map((source) => (source.expr === null ? null : compileExpr(source.expr, context)));
}
export function nativeDefaultMinimumBytes(
  sources: readonly NativeDefaultSource[],
  clock: FlatVector,
): number {
  let used = 0,
    peak = 0;
  const arena = new OutputArena(
    {
      allocate: (n) => {
        used += n;
        peak = Math.max(peak, used);
        return true;
      },
      free: (n) => {
        used -= n;
      },
    },
    1,
  );
  try {
    compile(sources, arena, clock);
    return retainedBytes(sources) + 4096 + sources.length * 512 + peak;
  } finally {
    arena.close();
  }
}
/** Exactly one invocation may be live; callers archive values then retire BEFORE a
 * BEFORE callback or writer wait. A failed pure native kernel can retry after freeing
 * its entire execution-local context; parameter memoization cannot retain old buffers. */
export class NativeDefaults {
  #scope: NativeDefaultScope | null;
  #arena: OutputArena | null = null;
  #values: RegistryImagePatch[] = [];
  #active = false;
  #minimum = 0;
  readonly #metadata: number;
  constructor(scope: NativeDefaultScope) {
    scope.signal.throwIfAborted();
    this.#metadata = retainedBytes(scope.sources) + 4096 + scope.sources.length * 512;
    if (!scope.charge.allocate(this.#metadata))
      throw new RangeError("Default program metadata must be admitted");
    this.#scope = scope;
    try {
      for (const [i, source] of scope.sources.entries()) {
        const column = scope.catalog
          .table(source.tableId)
          ?.columns.find((column) => column.id === source.columnId);
        if (
          !column ||
          (source.expr === null &&
            (column.defaultSql !== null || column.identitySequence !== null)) ||
          scope.sources.some(
            (other, k) =>
              k < i && other.tableId === source.tableId && other.columnId === source.columnId,
          ) ||
          (source.expr !== null && !samePhysicalType(source.expr.type, column.type))
        )
          throw new TypeError("Invalid native default program");
      }
      let used = 0;
      const counted: ByteCharge = {
        allocate: (bytes) => {
          if (!scope.charge.allocate(bytes)) return false;
          used += bytes;
          this.#minimum = Math.max(this.#minimum, used);
          return true;
        },
        free: (bytes) => {
          used -= bytes;
          scope.charge.free(bytes);
        },
      };
      const arena = new OutputArena(counted, 1);
      try {
        compile(scope.sources, arena, scope.clock);
      } finally {
        arena.close();
      }
    } catch (error) {
      this.#scope = null;
      scope.charge.free(this.#metadata);
      throw error;
    }
  }
  private get scope(): NativeDefaultScope {
    if (!this.#scope) throw new TypeError("Native defaults closed");
    return this.#scope;
  }
  async evaluate(
    tableId: number,
    columns: readonly number[],
    signal: AbortSignal,
  ): Promise<readonly RegistryImagePatch[]> {
    const scope = this.scope;
    if (this.#active || this.#arena) throw new TypeError("Unretired native defaults");
    this.#active = true;
    const check = () => {
      signal.throwIfAborted();
      scope.signal.throwIfAborted();
    };
    try {
      const table = scope.catalog.table(tableId);
      if (!table) throw new TypeError("Unprepared default table");
      const sources = table.columns
        .filter((column) => columns.includes(column.id))
        .map(
          (column) =>
            scope.sources.find(
              (source) => source.tableId === tableId && source.columnId === column.id,
            ) ??
            (() => {
              throw new TypeError("Unprepared default column");
            })(),
        );
      if (sources.length !== columns.length)
        throw new TypeError("Duplicate or missing default column");
      let requested = this.#minimum;
      const guard = retryGuard("native defaults preparation");
      for (;;) {
        // Progress: each retry requests more.
        guard.round(requested);
        check();
        await scope.prepare(requested, signal);
        check();
        let used = 0,
          denied = 0;
        const counted: ByteCharge = {
            allocate: (n) => {
              if (!scope.charge.allocate(n)) {
                denied = Math.max(denied, used + n);
                return false;
              }
              used += n;
              return true;
            },
            free: (n) => {
              used -= n;
              scope.charge.free(n);
            },
          },
          arena = new OutputArena(counted, 1);
        this.#arena = arena;
        try {
          const kernels = compile(sources, arena, scope.clock),
            row = { length: 1, selection: null, columns: [] };
          for (const [i, source] of sources.entries()) {
            check();
            const kernel = kernels[i];
            if (source.expr !== null && !kernel)
              throw new TypeError("Missing compiled default expression");
            this.#values.push({
              columnId: source.columnId,
              value: kernel?.evaluate(row, null) ?? null,
            });
          }
          return this.#values;
        } catch (error) {
          this.retireInternal();
          // No unsupported/SQL error is retried. Current native function bindings
          // refuse volatile generators during preparation; no side effect is repeated.
          if (!(error instanceof ExpressionMemoryError) || denied === 0) throw error;
          requested = denied;
        }
      }
    } finally {
      this.#active = false;
    }
  }
  private retireInternal(): void {
    this.#values.length = 0;
    this.#arena?.close();
    this.#arena = null;
  }
  retire(): void {
    if (this.#active) throw new TypeError("Default retirement requires evaluation fence");
    this.retireInternal();
  }
  close(): void {
    if (!this.#scope) return;
    if (this.#active) throw new TypeError("Default close requires evaluation fence");
    this.retireInternal();
    this.#scope.charge.free(this.#metadata);
    this.#scope = null;
  }
}
