/**
 * Test support for the write path: a write path over the simulated disk (log and root slots) and
 * an in-memory extent space, reopenable over the same bytes; small batch builders; and a table
 * reader that returns rows as plain values through the access contracts.
 */
import type { SqlTypeDescriptor } from "../types.js";
import type { ColumnDef, TableInput } from "../access/catalog.js";
import type { TableRules } from "../access/modify.js";
import type { Snapshot, TableSnapshot } from "../access/snapshot.js";
import type { Durability, WritePath, WriteTransaction } from "../access/write-path.js";
import { Governor } from "../memory/governor.js";
import { SimBackend, SimDisk } from "../storage/sim-disk.js";
import type { PhysicalType } from "../types.js";
import { int64At } from "../vector/int64.js";
import { allocateSelection } from "../vector/validity.js";
import { allocateFlat, type Batch, type FlatVector } from "../vector/vector.js";
import { decodeUtf8, VarlenBuilder } from "../vector/varlen.js";
import { MEMORY_CHUNK_ROWS } from "./columns.js";
import { WritePathImpl } from "./engine.js";
import { reachableExtents } from "../format/reachable.js";
import type { StorageBackend } from "../storage/contract.js";
import type { ExtentSpace } from "../storage/extents.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { openExtentSpace } from "../storage/space.js";
import { manifestCodecs } from "./manifest-codec.js";

export const INT: PhysicalType = { kind: "int32", bits: 32 };
export const BIG: PhysicalType = { kind: "int64" };
export const TEXT: PhysicalType = { kind: "text" };

export const NO_RULES: TableRules = {
  check: () => null,
  generate: () => undefined,
};

export const rules = () => NO_RULES;

const DECLARED: Record<string, SqlTypeDescriptor> = {
  int32: { type: "number", integer: true, declaredTypeName: "INTEGER" },
  int64: { type: "number", integer: true, declaredTypeName: "BIGINT" },
  text: { type: "string", declaredTypeName: "TEXT" },
};

export function column(
  name: string,
  type: PhysicalType,
  nullable = true,
): TableInput["columns"][number] {
  return {
    name,
    type,
    declared: DECLARED[type.kind] ?? { type: "string" },
    nullable,
    defaultSql: null,
    generatedSql: null,
    identitySequence: null,
    missing: null,
  } satisfies Omit<ColumnDef, "id">;
}

export function tableInput(
  name: string,
  columns: TableInput["columns"],
  extra: Partial<TableInput> = {},
): TableInput {
  return { name, columns, primaryKey: [0], unique: [], foreignKeys: [], checks: [], ...extra };
}

/**
 * A write path that can be reopened over the same store: the simulated disk by default (crash
 * model), or the memory backend (`"memory"`) for benchmarks. The data space is B1's extent space
 * (`openExtentSpace`) with format's reachability walk over the write path's manifest codecs.
 */
export class Harness {
  readonly disk: SimDisk = new SimDisk();
  readonly backend: StorageBackend;
  readonly governor = new Governor(256 * 1024 * 1024);
  #release: (() => Promise<void>) | null = null;
  space!: ExtentSpace;
  path!: WritePathImpl;

  constructor(
    readonly durability: Durability = "strict",
    readonly store: "sim" | "memory" = "sim",
    readonly memtableRows?: number,
    disk?: SimDisk,
  ) {
    if (disk !== undefined) this.disk = disk;
    this.backend = store === "sim" ? new SimBackend(this.disk) : new MemoryBackend();
  }

  async open(): Promise<WritePathImpl> {
    const lease = await this.backend.acquire();
    const session = await this.backend.open(lease);
    const startup = await this.governor.admit({
      label: "startup",
      minimumBytes: 8 * 1024 * 1024,
      spillable: true,
    });
    this.space = await openExtentSpace(session, {
      reachable: reachableExtents(manifestCodecs),
      budget: startup,
      registerCache: (cache) => this.governor.registerCache(cache),
    });
    this.path = await WritePathImpl.open({
      session,
      space: this.space,
      governor: this.governor,
      options: { durability: this.durability },
      ...(this.memtableRows === undefined ? {} : { tuning: { memtableRows: this.memtableRows } }),
    });
    this.#release = async () => {
      await this.space.close();
      startup.close();
      await session.close();
      await lease.release();
    };
    return this.path;
  }

  /** Closes cleanly and opens again (replay of the log tail). */
  async reopen(): Promise<WritePathImpl> {
    await this.path.close();
    await this.#release?.();
    return this.open();
  }

  /** Abandons the write path without closing it (no final header), then opens again. */
  async crash(): Promise<WritePathImpl> {
    await this.path.flush();
    await this.#release?.();
    if (this.backend instanceof SimBackend) this.backend.restart();
    return this.open();
  }

  async close(): Promise<void> {
    await this.path.close();
    await this.#release?.();
  }
}

export function ints(values: ReadonlyArray<number | null>, type: PhysicalType = INT): FlatVector {
  const nulls = values.some((value) => value === null);
  const validity = nulls ? new Uint32Array(Math.ceil(values.length / 32)) : null;
  values.forEach((value, k) => {
    if (validity !== null && value !== null)
      validity[k >>> 5] = (validity[k >>> 5] ?? 0) | (1 << (k & 31));
  });
  if (type.kind === "int64")
    return {
      form: "flat",
      type,
      length: values.length,
      validity,
      data: {
        layout: "int64",
        values: { repr: "safe", values: Float64Array.from(values, (value) => value ?? 0) },
      },
    };
  return {
    form: "flat",
    type,
    length: values.length,
    validity,
    data: { layout: "i32", values: Int32Array.from(values, (value) => value ?? 0) },
  };
}

export function texts(values: ReadonlyArray<string | null>): FlatVector {
  const builder = new VarlenBuilder(
    values.length,
    { allocate: () => true, free: () => undefined },
    1 << 30,
  );
  const validity = values.some((value) => value === null)
    ? new Uint32Array(Math.ceil(values.length / 32))
    : null;
  values.forEach((value, k) => {
    if (value === null) builder.appendEmpty();
    else {
      builder.appendString(value);
      if (validity !== null) validity[k >>> 5] = (validity[k >>> 5] ?? 0) | (1 << (k & 31));
    }
  });
  return { form: "flat", type: TEXT, length: values.length, validity, data: builder.data() };
}

export function batch(...columns: FlatVector[]): Batch {
  return { length: columns[0]?.length ?? 0, columns, selection: null };
}

export function locators(values: readonly number[], tableId: number): FlatVector {
  return {
    form: "flat",
    type: { kind: "locator", tableId },
    length: values.length,
    validity: null,
    data: { layout: "f64", values: Float64Array.from(values) },
  };
}

export type Value = number | bigint | string | null;

function valueAt(vector: FlatVector, row: number): Value {
  if (vector.validity !== null && ((vector.validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0)
    return null;
  const data = vector.data;
  switch (data.layout) {
    case "i32":
      return data.values[row] ?? 0;
    case "int64": {
      const value = int64At(data.values, row);
      return value >= -(2n ** 53n) && value <= 2n ** 53n ? Number(value) : value;
    }
    case "varlen":
      return decodeUtf8(data, row);
    default:
      throw new Error(`test reader: ${data.layout} is not supported`);
  }
}

/** Every live row of a table in a view, as value arrays in catalog order (segment order). */
export async function rowsOf(view: Snapshot, tableId: number): Promise<Value[][]> {
  const table: TableSnapshot = await view.table(tableId, {
    budget: { allocate: () => true, free: () => undefined },
  });
  const out: Value[][] = [];
  const live = allocateSelection(MEMORY_CHUNK_ROWS);
  for (const segment of table.segments) {
    const deletions = await segment.deletions();
    if (segment.kind === "memory") {
      for (let chunk = 0; chunk * MEMORY_CHUNK_ROWS < segment.rowCount; chunk++) {
        const from = chunk * MEMORY_CHUNK_ROWS;
        const length = Math.min(MEMORY_CHUNK_ROWS, segment.rowCount - from);
        const vectors: FlatVector[] = [];
        for (const def of table.columns) {
          const output = {
            flat: allocateFlat(def.type, MEMORY_CHUNK_ROWS),
            codes: new Int32Array(MEMORY_CHUNK_ROWS),
            varlen: new VarlenBuilder(
              MEMORY_CHUNK_ROWS,
              { allocate: () => true, free: () => undefined },
              1 << 30,
            ),
          };
          const result = await segment.chunk(def.id, chunk, output);
          if (!result.complete || result.vector.form !== "flat")
            throw new Error("test reader: partial chunk");
          vectors.push(result.vector);
        }
        if (deletions === null) {
          live.count = length;
          for (let k = 0; k < length; k++) live.rows[k] = k;
        } else deletions.liveRows(from, length, null, live);
        for (let k = 0; k < live.count; k++) {
          const row = live.rows[k] ?? 0;
          out.push(vectors.map((vector) => valueAt(vector, row)));
        }
      }
      continue;
    }
    const { ChunkReader } = await import("../page/reader.js");
    await segment.root();
    const vectors: FlatVector[] = [];
    for (const def of table.columns) {
      const chunk = segment.chunk(def.id);
      if (chunk === null) throw new Error("test reader: missing column");
      const reader = await ChunkReader.open(view.pages, chunk, {
        budget: { allocate: () => true, free: () => undefined },
      });
      const pages = Array.from({ length: reader.directory.pages.length }, (_, k) => k);
      await reader.load(pages);
      const { allocateData } = await import("../vector/vector.js");
      const target: FlatVector = {
        form: "flat",
        type: def.type,
        length: segment.rowCount,
        validity: new Uint32Array(Math.ceil(segment.rowCount / 32)),
        data: allocateData(
          def.type,
          segment.rowCount,
          def.type.kind === "text" ? reader.varlenBytes(0, segment.rowCount) : 0,
        ),
      };
      reader.decodeRange(0, segment.rowCount, target, 0);
      reader.close();
      vectors.push(target);
    }
    for (let position = 0; position < segment.rowCount; position++)
      if (!deletions?.isDeleted(position))
        out.push(vectors.map((vector) => valueAt(vector, position)));
  }
  return out;
}

/** Runs one autocommit statement and waits for durability. */
export async function autocommit(
  path: WritePath,
  work: (statement: ReturnType<WriteTransaction["statement"]>) => unknown,
): Promise<number> {
  const tx = await path.begin();
  const statement = tx.statement({ rules });
  try {
    await work(statement);
    await statement.end();
  } catch (error) {
    statement.rollback();
    tx.rollback();
    throw error;
  }
  const receipt = await tx.commit(null);
  await receipt.durable;
  return receipt.lsn;
}
