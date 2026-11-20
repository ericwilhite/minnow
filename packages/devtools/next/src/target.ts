/** Internal 1.0 binding; never adapts canonical Result into the released QueryResult. */
import type {
  CallOptions,
  Cursor,
  CursorOptions,
  Param,
  Result,
} from "../../../core/dist/next/owner/api.js";
import type {
  CatalogDescription,
  ExplainDescription,
} from "../../../core/dist/next/owner/inspection.js";

export class NativeDevtoolsNotBoundError extends Error {
  readonly code = "MINNOW_DEVTOOLS_NOT_BOUND";
  readonly sqlState = "0A000";
  constructor(readonly feature: string) {
    super(`Native devtools ${feature} is not yet bound`);
    this.name = "NativeDevtoolsNotBoundError";
  }
}
export interface NativeDevtoolsTarget {
  query(sql: string, params?: readonly Param[], options?: CallOptions): Promise<Result>;
  execute(sql: string, params?: readonly Param[], options?: CallOptions): Promise<Result>;
  cursor(sql: string, params?: readonly Param[], options?: CursorOptions): Cursor;
  describeCatalog(options?: CallOptions): Promise<CatalogDescription>;
  explain(sql: string, options?: CallOptions): Promise<ExplainDescription>;
}
/** Keep the client's real cancellation, session and outcome contracts. The panel owns no
 * client connection and cannot close it. Live-store lifetime will be supplied by B8. */
export function nativeTarget(
  client: NativeDevtoolsTarget,
): NativeDevtoolsTarget & { follow(): never } {
  return {
    query: (sql: string, params: readonly Param[] = [], options: CallOptions = {}) =>
      client.query(sql, params, options),
    execute: (sql: string, params: readonly Param[] = [], options: CallOptions = {}) =>
      client.execute(sql, params, options),
    cursor: (sql: string, params: readonly Param[] = [], options: CursorOptions = {}) =>
      client.cursor(sql, params, options),
    describeCatalog: (options: CallOptions = {}) => client.describeCatalog(options),
    explain: (sql: string, options: CallOptions = {}) => client.explain(sql, options),
    follow(): never {
      throw new NativeDevtoolsNotBoundError("live queries");
    },
  };
}
