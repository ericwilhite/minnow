/** Recovery-only identity metadata. Decoded vectors never enter this cache. */
import type { CatalogView, ColumnDef, TableDef } from "../access/catalog.js";
import { LogCorruptionError } from "../format/log-frame.js";
import { charged, need, type Charge } from "./bytes.js";

export interface ReplayColumnMapping {
  readonly definition: TableDef;
  readonly sourceIds: Float64Array;
  readonly sourcePositions: Int32Array;
  readonly indexPositions: Int32Array;
}
interface Entry extends ReplayColumnMapping {
  readonly bytes: number;
  readonly sortedIds: Float64Array;
  readonly seen: Uint32Array;
}
interface TableMappings {
  readonly definition: TableDef;
  readonly columns: Map<number, number>;
  readonly versions: Map<number, Entry>;
  bytes: number;
}

const BASE = 256;

function sortedIndex(ids: Float64Array, id: number): number {
  let lo = 0,
    hi = ids.length;
  while (lo < hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (need(ids[mid]) < id) lo = mid + 1;
    else hi = mid;
  }
  return ids[lo] === id ? lo : -1;
}

/** Memoizes each (table, physical version) until its catalog definition changes.
 * Arrays and map capacities are reserved before construction. Under pressure discard
 * inactive mappings before retrying; the caller then uses its cancellable metadata policy.
 * A memtable must copy any non-catalog column array before the cache can be invalidated. */
export class ReplayColumnMappings {
  readonly #tables: Map<number, TableMappings>;
  #bytes: number;
  #closed = false;
  builds = 0;
  hits = 0;
  evictions = 0;
  peakBytes = BASE;

  constructor(readonly charge: Charge) {
    charged(charge, BASE);
    this.#bytes = BASE;
    this.#tables = new Map();
  }
  get bytes(): number {
    return this.#bytes;
  }

  get(def: TableDef, schemaVersion: number, ids?: readonly number[]): ReplayColumnMapping {
    if (this.#closed) throw new TypeError("Replay column mappings are closed");
    for (let retry = 0; ; retry++) {
      let table = this.#tables.get(def.id);
      if (table !== undefined && table.definition !== def) {
        this.#invalidate(def.id, table);
        table = undefined;
      }
      const count = ids?.length ?? def.columns.length;
      const known = table?.versions.get(schemaVersion);
      if (known !== undefined) {
        if (known.sourceIds.length !== count)
          throw new LogCorruptionError("Column identities change within one logged schema");
        let ordered = true;
        for (let c = 0; c < count; c++)
          if (known.sourceIds[c] !== (ids?.[c] ?? def.columns[c]?.id)) ordered = false;
        if (!ordered) {
          // Native writers keep catalog order. Explicit identities also admit permutations:
          // update the numeric source positions in place, never build another map/vector.
          known.seen.fill(0);
          for (let c = 0; c < count; c++) {
            const found = sortedIndex(known.sortedIds, need(ids?.[c] ?? def.columns[c]?.id));
            const slot = found >>> 5,
              bit = 1 << (found & 31);
            if (found < 0 || ((known.seen[slot] ?? 0) & bit) !== 0)
              throw new LogCorruptionError("Column identities change within one logged schema");
            known.seen[slot] = (known.seen[slot] ?? 0) | bit;
          }
          for (let c = 0; c < count; c++) {
            const id = need(ids?.[c] ?? def.columns[c]?.id);
            const target = need(table).columns.get(id);
            const physical = target === undefined ? -1 : need(known.indexPositions[target]);
            if (physical >= 0) known.sourcePositions[physical] = c;
            known.sourceIds[c] = id;
          }
        }
        this.hits++;
        return known;
      }
      // Headers, hash-table capacity, source IDs, physical/index positions, and column refs.
      const tableBytes = table === undefined ? 256 + def.columns.length * 64 : 0;
      const entryBytes = 768 + count * 32 + def.columns.length * 8;
      const bytes = tableBytes + entryBytes;
      try {
        charged(this.charge, bytes);
      } catch (error) {
        if (retry > 0 || this.#tables.size === 0) throw error;
        for (const cached of this.#tables.values()) this.evictions += cached.versions.size;
        this.clear();
        continue;
      }
      try {
        if (table === undefined) {
          const columns = new Map<number, number>();
          for (const [c, column] of def.columns.entries()) columns.set(column.id, c);
          table = { definition: def, columns, versions: new Map(), bytes: tableBytes };
        }
        const sourceIds = new Float64Array(count);
        const sourcePositions = new Int32Array(count);
        const indexPositions = new Int32Array(def.columns.length).fill(-1);
        const columns: ColumnDef[] = [];
        for (let c = 0; c < count; c++) {
          const id = need(ids?.[c] ?? def.columns[c]?.id);
          sourceIds[c] = id;
          const target = table.columns.get(id);
          if (target === undefined) continue;
          sourcePositions[columns.length] = c;
          indexPositions[target] = columns.length;
          columns.push(need(def.columns[target]));
        }
        if (schemaVersion === def.schemaVersion && columns.length !== def.columns.length)
          throw new LogCorruptionError("A logged insert lacks a catalog column");
        const aligned =
          columns.length === def.columns.length &&
          columns.every((column, c) => column === def.columns[c]);
        const entry: Entry = {
          definition: { ...def, schemaVersion, columns: aligned ? def.columns : columns },
          sourceIds,
          sourcePositions,
          indexPositions,
          sortedIds: sourceIds.slice().sort(),
          seen: new Uint32Array(Math.ceil(count / 32)),
          bytes: entryBytes,
        };
        table.versions.set(schemaVersion, entry);
        table.bytes += entryBytes;
        this.#tables.set(def.id, table);
        this.#bytes += bytes;
        this.peakBytes = Math.max(this.peakBytes, this.#bytes);
        this.builds++;
        return entry;
      } catch (error) {
        this.charge.free(bytes);
        throw error;
      }
    }
  }

  /** Unrelated DDL leaves table descriptors identical and must not rebuild their maps. */
  synchronize(catalog: CatalogView): void {
    for (const [id, table] of this.#tables)
      if (catalog.table(id) !== table.definition) this.#invalidate(id, table);
  }
  #invalidate(id: number, table: TableMappings): void {
    this.#tables.delete(id);
    this.#bytes -= table.bytes;
    this.charge.free(table.bytes);
  }
  clear(): void {
    if (this.#closed) return;
    const bytes = this.#bytes - BASE;
    this.#tables.clear();
    this.#bytes = BASE;
    if (bytes > 0) this.charge.free(bytes);
  }
  close(): void {
    if (this.#closed) return;
    this.clear();
    this.#closed = true;
    this.#bytes = 0;
    this.charge.free(BASE);
  }
}
