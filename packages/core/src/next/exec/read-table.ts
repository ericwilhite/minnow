/** Query-scoped group roots close with the scan, without changing B3's snapshot implementation. */
import type { GroupSegment, Segment, TableSnapshot } from "../access/snapshot.js";
import type { ByteBudget, SpaceReader } from "../access/space.js";
import { PREPARE_GROUP, type PreparedGroup, type QueryRoots } from "../page/query-roots.js";
import { GroupRead } from "../page/group-read.js";

export function readTable(
  table: TableSnapshot,
  pages: SpaceReader,
  budget: ByteBudget,
  check: () => void,
  caches?: QueryRoots,
): TableSnapshot {
  let last = -1,
    cached: Segment | undefined;
  const original = table.segments;
  // An empty facade avoids violating Proxy invariants for a frozen snapshot array.
  // Native iteration/map read length/indices through the facade; no group-sized copy.
  const segments = new Proxy<readonly Segment[]>([], {
    has: (_target, key) => Reflect.has(original, key),
    get(_target, key, receiver): unknown {
      if (typeof key !== "string" || !/^\d+$/.test(key))
        return Reflect.get(original, key, receiver);
      const index = Number(key),
        source = original[index];
      if (source?.kind !== "group") return source;
      if (last === index) return cached;
      const root = new GroupRead(source.group.root, pages, budget, caches, table.tableId);
      const group: GroupSegment & PreparedGroup = {
        [PREPARE_GROUP]: (columns) => {
          check();
          return root.prepare(columns);
        },
        kind: "group",
        index: source.index,
        rowCount: source.rowCount,
        get liveRowCount() {
          return source.liveRowCount;
        },
        group: source.group,
        staged: source.staged,
        deletions: () => source.deletions(),
        root: () => {
          check();
          return root.root();
        },
        releaseRoot: () => root.release(),
        chunk: (id) => {
          check();
          return root.chunk(id);
        },
        statistics: (id) => {
          check();
          return root.chunk(id)?.statistics ?? null;
        },
        pages: (id) => {
          check();
          if (root.chunk(id) !== null) return root.summaries(id);
          const missing = table.columns.find((column) => column.id === id)?.missing;
          const nil =
            missing == null ||
            (missing.validity !== null && ((missing.validity[0] ?? 0) & 1) === 0);
          return [
            {
              firstRow: 0,
              rowCount: source.rowCount,
              nullCount: nil ? source.rowCount : 0,
              min: null,
              max: null,
              dictionarySize: null,
            },
          ];
        },
      };
      last = index;
      cached = group;
      return group;
    },
  });
  return {
    tableId: table.tableId,
    schemaVersion: table.schemaVersion,
    columns: table.columns,
    get liveRowCount() {
      return table.liveRowCount;
    },
    segments,
  };
}
