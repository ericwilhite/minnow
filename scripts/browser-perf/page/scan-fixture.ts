/** Fixture descriptors shared by scan diagnostics; no executor or frame builder. */
import type { ColumnDef } from "../../../packages/core/src/next/access/catalog.js";
import type {
  GroupSegment,
  TableSnapshot,
} from "../../../packages/core/src/next/access/snapshot.js";
import type { ExportGroup } from "./export-stream.js";

/** BENCH-only snapshot descriptors; stands in for catalog/group-root reads. */
export function benchTableOf(groups: readonly ExportGroup[]): TableSnapshot {
  const first = groups[0];
  if (!first) throw new Error("No scan groups");
  const columns: ColumnDef[] = first.chunks.map((chunk) => ({
    id: chunk.columnId,
    name: `c${String(chunk.columnId)}`,
    type: chunk.type,
    get declared(): never {
      throw new Error("Bench has no SQL catalog descriptors");
    },
    nullable: true,
    defaultSql: null,
    generatedSql: null,
    identitySequence: null,
    missing: null,
  }));
  const segments: GroupSegment[] = groups.map((g, index) => ({
    kind: "group",
    index,
    rowCount: g.rows,
    liveRowCount: g.rows,
    staged: false,
    get group(): never {
      throw new Error("Bench supplies decoded group descriptors, not B3 GroupRefs");
    },
    root: () => ({ rowCount: g.rows, rowIds: { kind: "dense" }, columns: g.chunks }),
    chunk: (id: number) => g.chunks.find((c) => c.columnId === id) ?? null,
    deletions: () => null,
    statistics: () => null,
    pages: () => {
      throw new Error("Unfiltered bench does not supply B3 page summaries");
    },
  }));
  return {
    tableId: 1,
    schemaVersion: 1,
    columns,
    segments,
    liveRowCount: groups.reduce((n, g) => n + g.rows, 0),
  };
}
