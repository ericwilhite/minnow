/**
 * Test support for the write path: a write path over the simulated disk (log and root slots) and
 * an in-memory extent space, reopenable over the same bytes; small batch builders; and a table
 * reader that returns rows as plain values through the access contracts.
 */
import type { SqlTypeDescriptor } from "../types.js";
import type { ColumnDef, TableInput } from "../access/catalog.js";
import type { TableRules } from "../access/modify.js";
import type { Snapshot, TableSnapshot } from "../access/snapshot.js";
import type {
  Durability,
  ExtentDeltaSink,
  WritePath,
  WriteTransaction,
} from "../access/write-path.js";
import { Governor, type MemoryGovernor, type Grant } from "../memory/governor.js";
import { SimBackend, SimDisk } from "../storage/sim-disk.js";
import type { PhysicalType } from "../types.js";
import { int64At } from "../vector/int64.js";
import { allocateSelection } from "../vector/validity.js";
import { allocateFlat, type Batch, type FlatVector } from "../vector/vector.js";
import { decodeUtf8, VarlenBuilder } from "../vector/varlen.js";
import { MEMORY_CHUNK_ROWS } from "./columns.js";
import { WritePathImpl, type EngineInput } from "./engine.js";
import type { StatementImpl } from "./transaction.js";
import { listedRootExtents, reachableExtents } from "../format/reachable.js";
import type { StorageBackend } from "../storage/contract.js";
import type { ExtentSpace } from "../storage/extents.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { openExtentSpace } from "../storage/space.js";

export const INT: PhysicalType = { kind: "int32", bits: 32 };

/** A test root's last native step, after its bodies and before its statement ends: the writer
 * funds the statement-end work (FTS) then, so ending never waits (as NativeCommandRoot does).
 * `rootBytes` is what the test root still holds. */
export async function nativeEndStep(
  statement: { prepareNativeStep: StatementImpl["prepareNativeStep"] },
  signal: AbortSignal,
  rootBytes = 0,
): Promise<void> {
  const boundary = await statement.prepareNativeStep(rootBytes, signal, {
    includesProgress: true,
  });
  boundary.release();
}
export const BIG: PhysicalType = { kind: "int64" };
export const TEXT: PhysicalType = { kind: "text" };

export const NO_RULES: TableRules = {
  check: () => null,
  generate: () => undefined,
};

export const rules = () => NO_RULES;

function declared(type: PhysicalType): SqlTypeDescriptor {
  switch (type.kind) {
    case "bool":
      return { type: "boolean" };
    case "int32":
      return {
        type: "number",
        integer: true,
        declaredTypeName: type.bits === 16 ? "SMALLINT" : "INTEGER",
      };
    case "int64":
      return { type: "number", integer: true, declaredTypeName: "BIGINT" };
    case "float32":
      return { type: "number", declaredTypeName: "REAL" };
    case "float64":
      return { type: "number", declaredTypeName: "DOUBLE PRECISION" };
    case "decimal":
      return {
        type: "string",
        sqlDomain: {
          kind: "numeric",
          ...(type.precision === null ? {} : { precision: type.precision }),
          ...(type.scale === null ? {} : { scale: type.scale }),
        },
      };
    case "bytes":
      return { type: "string", declaredTypeName: "BYTEA" };
    case "text":
      return {
        type: "string",
        ...(type.maxLength === undefined
          ? {}
          : { declaredLength: { length: type.maxLength, fixed: type.fixed === true } }),
      };
    case "timestamp":
      return { type: "datetime", ...(type.zoned ? {} : { withoutTimeZone: true }) };
    case "json":
      return { type: "string", sqlDomain: { kind: type.binary ? "jsonb" : "json" } };
    case "enum":
      return {
        type: "string",
        sqlDomain: { kind: "enum", name: type.name, values: [...type.labels] },
      };
    case "date":
    case "time":
    case "interval":
    case "uuid":
      return { type: "string", sqlDomain: { kind: type.kind } };
    case "array": {
      const element =
        type.element.kind === "int32"
          ? type.element.bits === 16
            ? "smallint"
            : "integer"
          : type.element.kind === "int64"
            ? "bigint"
            : type.element.kind === "float32"
              ? "real"
              : type.element.kind === "float64"
                ? "double precision"
                : type.element.kind === "bool"
                  ? "boolean"
                  : type.element.kind === "timestamp"
                    ? type.element.zoned
                      ? "timestamptz"
                      : "timestamp"
                    : type.element.kind;
      return { type: "string", sqlDomain: { kind: "array", element } };
    }
    default:
      return { type: "string" };
  }
}

export function column(
  name: string,
  type: PhysicalType,
  nullable = true,
): TableInput["columns"][number] {
  return {
    name,
    type,
    declared: declared(type),
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
  /** The R25 extent-delta sink passed to every open (access/write-path.ts `ExtentDeltaSink`). */
  extentDeltas: ExtentDeltaSink | null = null;
  /** Permanent owner grants passed to every open (`EngineInput.ownerHoldings`). */
  ownerHoldings: (() => number) | null = null;
  /** Called with the extent space before the write path opens over it (tests instrument reads). */
  onSpace: ((space: ExtentSpace) => void) | null = null;
  /** The next open's signal (`EngineInput.options.signal`): cancels an open that waits. */
  signal: AbortSignal | undefined = undefined;
  /** Wall time of the last open's two phases: the extent space (its walk), then the write path. */
  lastOpen = { spaceMs: 0, pathMs: 0 };

  constructor(
    readonly durability: Durability = "strict",
    readonly store: "sim" | "memory" = "sim",
    readonly memtableRows?: number,
    disk?: SimDisk,
    /** Internal tuning beyond `memtableRows` (merges: engine.ts `EngineInput`). */
    readonly tuning: Omit<NonNullable<EngineInput["tuning"]>, "memtableRows"> = {},
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
    const started = performance.now();
    // Ruling R25: startup reads the roots' extent lists; every open (and audit) cross-checks
    // them against the full walk.
    this.space = await openExtentSpace(session, {
      reachable: listedRootExtents(),
      walk: reachableExtents(),
      verifyAtOpen: true,
      budget: startup,
      registerCache: (cache) => this.governor.registerCache(cache),
    });
    const spaceDone = performance.now();
    this.onSpace?.(this.space);
    const release = async () => {
      await this.space.close();
      startup.close();
      await session.close();
      await lease.release();
    };
    try {
      const opening = WritePathImpl.open({
        session,
        space: this.space,
        governor: this.governor,
        options: {
          durability: this.durability,
          ...(this.signal === undefined ? {} : { signal: this.signal }),
        },
        ...(this.extentDeltas === null ? {} : { extentDeltas: this.extentDeltas }),
        ...(this.ownerHoldings === null ? {} : { ownerHoldings: this.ownerHoldings }),
        tuning: {
          ...this.tuning,
          ...(this.memtableRows === undefined ? {} : { memtableRows: this.memtableRows }),
        },
      });
      // Native storage retains its own admitted roots after open. Startup's unused estimate
      // yields at write admission; allocated buffers and loans keep their original ownership.
      startup.release(startup.held - startup.allocated - startup.loaned);
      this.path = await opening;
    } catch (error) {
      // A refused open leaves nothing held: the next open can take the lock.
      await release();
      throw error;
    }
    this.#release = release;
    this.lastOpen = { spaceMs: spaceDone - started, pathMs: performance.now() - spaceDone };
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
    WritePathImpl.abandonForCrash(this.path);
    await this.#release?.();
    if (this.backend instanceof SimBackend) this.backend.restart();
    return this.open();
  }

  async close(): Promise<void> {
    try {
      await this.path.close();
    } finally {
      await this.#release?.();
    }
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
      await reader.loadValues(0, segment.rowCount);
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

/** Independent test-only inventory of descriptors charged to the write path's metadata owner.
 * Observe raw space pin calls/releases, including re-point pins and shared older states.
 * The returned sum is separate from decoded root/directory/run metadata.
 */
export function countMetadataPinBytes(harness: Harness): () => number {
  let bytes = 0;
  const previous = harness.onSpace;
  harness.onSpace = (space) => {
    previous?.(space);
    const original = space.pin.bind(space);
    space.pin = (label, budget) => {
      const pin = original(label, budget);
      if (budget === undefined) return pin;
      const charge = 320 + 2 * (label ?? "pin").length; // RootPin contract, independently inventoried.
      bytes += charge;
      let released = false;
      return {
        generation: pin.generation,
        label: pin.label,
        get released() {
          return pin.released;
        },
        release: () => {
          if (released) return;
          released = true;
          bytes -= charge;
          pin.release();
        },
      };
    };
  };
  return () => bytes;
}

/** Observe native accounts as well as standalone admissions; no budgets or faults are replaced. */
export function watchGrants(governor: MemoryGovernor, observe: (grant: Grant) => void): void {
  const admit = governor.admit.bind(governor);
  governor.admit = async (options) => {
    const grant = await admit(options);
    observe(grant);
    return grant;
  };
  const admitGroup = governor.admitGroup.bind(governor);
  governor.admitGroup = async (options) => {
    const grants = await admitGroup(options);
    for (const grant of grants) observe(grant);
    return grants;
  };
}

/**
 * Microtask turns `onGroupWait` lets pass before it counts an unsettled admission as a wait. A
 * refusal is a rejected promise that reaches the caller only after the governor's own `await`
 * and one more for each async wrapper between them (`watchGrants` adds one); this is well above
 * any stack of wrappers these tests compose.
 */
const GROUP_WAIT_TURNS = 16;

/**
 * Reports, without a timer, whether a native group admission waited in the governor behind
 * another holder. The governor serves a request that fits inside the call that queues it, and a
 * queued one inside the release that makes room for it, and reports each grant as it makes it
 * (`Governor.observeGrants`; a group is served from the admission or holder queue). Each
 * admission labelled `label` is counted once:
 * - `immediate`: its grant was reported by the end of the task that made it (a release that task
 *   itself makes, such as the harness returning its startup credit, comes first);
 * - `refused`: it settled without a grant. `admitGroup` is async, so a refusal is a rejected
 *   promise, never a throw, and it may reach this wrapper several microtask turns later (from
 *   inside the governor's queue, through async wrappers such as `watchGrants`). The handlers that
 *   count it are attached before the caller's, so the caller sees the count when it resumes;
 * - `waits`: neither, after `GROUP_WAIT_TURNS` microtask turns; `onWait` then runs (the place to
 *   release the competing holder). A grant served later, by another release, is a wait too.
 * One admission of `label` at a time. Wraps the instance's `admitGroup`, as `watchGrants` does,
 * and may wrap it in either order.
 */
export function onGroupWait(
  governor: Governor,
  label: string,
  onWait: () => void,
): { readonly waits: number; readonly immediate: number; readonly refused: number } {
  const counts = { waits: 0, immediate: 0, refused: 0 };
  const admitGroup = governor.admitGroup.bind(governor);
  governor.admitGroup = (options) => {
    // A zero-total group is served at once and reports no grant.
    if (options.label !== label || options.grants.every((part) => part.minimumBytes === 0))
      return admitGroup(options);
    let served = false,
      taskEnded = false,
      classified = false;
    const stop = governor.observeGrants((event) => {
      if (event.label === label && (event.source === "admission" || event.source === "holder"))
        served = true;
    });
    const count = (kind: keyof typeof counts) => {
      if (classified) return;
      classified = true;
      counts[kind]++;
      if (kind === "waits") onWait();
    };
    const admitted = admitGroup(options);
    // Attached before the caller's handlers: a settlement is counted before the caller resumes.
    admitted.then(
      () => {
        stop();
        // Before the end of the caller's task (an inner promise already settled): immediate.
        // After it, unless counted immediate there: served later, by another release.
        count(taskEnded ? "waits" : "immediate");
      },
      () => {
        stop();
        count("refused");
      },
    );
    // The end of the caller's task: served by then is immediate.
    queueMicrotask(() => {
      taskEnded = true;
      if (served) count("immediate");
    });
    let turns = GROUP_WAIT_TURNS;
    const fallback = () => {
      if (classified) return;
      if (--turns > 0) queueMicrotask(fallback);
      else count("waits");
    };
    queueMicrotask(fallback);
    return admitted;
  };
  return counts;
}
