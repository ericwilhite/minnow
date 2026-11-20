/** Private presentation binding. Reuses the existing editor/grid and never owns the database. */
import type { Param, CallOptions, Result } from "../../../core/dist/next/owner/api.js";
import { el } from "../../dist/dom.js";
import { editorSchema, renderCatalog, renderExplain } from "./catalog.js";
import { createNativeResultGrid } from "./result.js";
import { nativeTarget, type NativeDevtoolsTarget } from "./target.js";

export interface NativeDevtoolsBinding {
  readonly node: HTMLElement;
  readonly target: ReturnType<typeof nativeTarget>;
  query(sql: string, params?: readonly Param[], options?: CallOptions): Promise<Result>;
  execute(sql: string, params?: readonly Param[], options?: CallOptions): Promise<Result>;
  refreshCatalog(
    options?: CallOptions,
  ): Promise<import("../../dist/editor/editor.js").EditorSchema>;
  explain(
    sql: string,
    options?: CallOptions,
  ): Promise<import("../../../core/dist/next/owner/inspection.js").ExplainDescription>;
  cancel(reason?: unknown): void;
  destroy(): Promise<void>;
}
export function bindNativeDevtools(client: NativeDevtoolsTarget): NativeDevtoolsBinding {
  const target = nativeTarget(client),
    grid = createNativeResultGrid(),
    catalog = el("div"),
    explain = el("div"),
    status = el("output", { attrs: { "aria-live": "polite" } }),
    node = el("div", { class: "native-devtools" }, [status, grid.node, catalog, explain]);
  let closed = false,
    pending: Promise<unknown> | null = null,
    stop: AbortController | null = null;
  function run<T>(
    work: (options: CallOptions) => Promise<T>,
    show: (value: T) => void,
    options: CallOptions,
  ): Promise<T> {
    if (closed) return Promise.reject(new TypeError("Native devtools binding is closed"));
    if (pending !== null)
      return Promise.reject(new TypeError("Native devtools operation is already running"));
    const controller = new AbortController();
    stop = controller;
    const signal = options.signal
      ? AbortSignal.any([controller.signal, options.signal])
      : controller.signal;
    status.textContent = "Running";
    const executing = Promise.resolve()
      .then(() => {
        signal.throwIfAborted();
        return work({ signal });
      })
      .then(
        (value) => {
          if (!closed) show(value);
          return value;
        },
        (error: unknown) => {
          if (!closed) {
            const state =
              typeof error === "object" &&
              error !== null &&
              "sqlState" in error &&
              typeof error.sqlState === "string"
                ? `${error.sqlState}: `
                : "";
            status.textContent =
              state + (error instanceof Error ? error.message : "Operation failed");
          }
          throw error;
        },
      );
    const done = executing.finally(() => {
      if (pending === done) {
        pending = null;
        stop = null;
      }
    });
    pending = done;
    return done;
  }
  return {
    node,
    target,
    query(sql: string, params: readonly Param[] = [], options: CallOptions = {}) {
      return run(
        (options) => target.query(sql, params, options),
        (result) => {
          const shown = grid.show(result);
          status.textContent = `${result.command}: ${String(shown.visibleRows)} of ${String(result.rowCount)} rows${shown.truncated ? " (preview)" : ""}${result.replayed ? " (replayed)" : ""}`;
        },
        options,
      );
    },
    execute(sql: string, params: readonly Param[] = [], options: CallOptions = {}) {
      return run(
        (options) => target.execute(sql, params, options),
        (result) => {
          grid.show(result);
          status.textContent = `${result.command}${result.affectedRows === null ? "" : `: ${String(result.affectedRows)} affected`}${result.replayed ? " (replayed)" : ""}`;
        },
        options,
      );
    },
    refreshCatalog(options: CallOptions = {}) {
      return run(
        (options) => target.describeCatalog(options),
        (description) => {
          catalog.replaceChildren(renderCatalog(description));
          status.textContent = `Catalog ${String(description.catalogVersion)}`;
        },
        options,
      ).then(editorSchema);
    },
    explain(sql: string, options: CallOptions = {}) {
      return run(
        (options) => target.explain(sql, options),
        (description) => {
          explain.replaceChildren(renderExplain(description));
          status.textContent = "Logical plan; not executed";
        },
        options,
      );
    },
    cancel(reason: unknown = new Error("Native devtools operation cancelled")) {
      stop?.abort(reason);
    },
    async destroy() {
      if (!closed) {
        closed = true;
        stop?.abort(new Error("Native devtools binding closed"));
      }
      await pending?.catch(() => undefined);
      grid.destroy();
      catalog.replaceChildren();
      explain.replaceChildren();
      node.remove();
    },
  };
}
