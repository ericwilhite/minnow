/** Codec/executor seam for synchronous borrowed page consumption.
 *
 * Metadata is available before payload through ChunkPages.pageEntry. Payload consumption is
 * synchronous under the reader's existing leases. Selection uses the same coordinates as
 * selectPage: range endpoints are page-local; selected rows are ascending group positions.
 * Repeated selected positions retain their multiplicity. The owner provides checked bounds.
 * No callback may retain a borrowed page/dictionary or mutate its bytes. The undefined
 * return type excludes async callbacks. Metadata/payload access itself does not normalize
 * a page; an earlier predicate may already have cached a derived plain lane.
 */
import type { PageEntry } from "../format/manifest.js";

// Raw page metadata and encoding dispatch belong to this page-consumption contract.
// Executor callers depend on this seam, without importing the stored manifest layer.
export { PageEncoding } from "../format/manifest.js";
export type { PageEntry } from "../format/manifest.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";
import type { PageView } from "./decode.js";

export type PageRows = Uint16Array | Uint32Array | Int32Array;

/** One prebuilt operation chosen per page, with state owned/admitted by its consumer.
 * Separate arguments avoid manufacturing an intermediate vector or payload-context object.
 * A dictionary may be null when a kernel only needs the verified codes/validity.
 */
export type EncodedPageKernel<State> = (
  state: State,
  view: PageView,
  entry: PageEntry,
  type: PhysicalType,
  dictionary: FlatVector | null,
  rows: PageRows | null,
  from: number,
  to: number,
) => undefined;

/** Optional loaded-payload capability check, before any transition. Metadata-only consumers
 * need not invoke it. No new descriptors, retention or asynchronous checks are permitted.
 */
export type EncodedPageCheck = (view: PageView, entry: PageEntry, type: PhysicalType) => boolean;
