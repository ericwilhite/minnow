/**
 * The in-page half of the browser performance lane: `window.BrowserPerf`.
 *
 * The Node driver opens one engine per page, in a fresh worker, and calls these functions through
 * Playwright. Timings are taken here with `performance.now()`; memory and CPU are read by the
 * driver from `/proc` around each call, because the page cannot see its own process.
 */
import * as duckdb from "@duckdb/duckdb-wasm";
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { deleteOpfsDatabase } from "@minnowdb/core/storage/opfs";
import type * as arrow from "apache-arrow";
import {
  commerceEntities,
  generateEntityBatch,
  type EntityDefinition,
} from "../../../apps/site/bench/benchmark.ts";
import { STAR_TABLES, type EngineId } from "../catalogue.ts";
import type {
  LoadResult,
  OpenRequest,
  OpenResult,
  PageFailure,
  PageResult,
  PointSamples,
  QueryRequest,
  QueryResult,
  SqlitePragmaSet,
} from "../protocol.ts";
import { boolVector, float64Vector, int32Vector, table, utf8Vector } from "./arrow.ts";
import { digestResult, type ResultView } from "./canonical.ts";
import {
  DAY_MS,
  dimensionTables,
  wideBatch,
  wideSpec,
  type WideColumn,
  type WideValue,
} from "./wide-data.ts";

const DUCKDB_PACKAGE = "1.32.0";
const DUCKDB_ASSETS = "/vendor/duckdb/";
const LOAD_BATCH_ROWS = 50_000;

type Params = ReadonlyArray<string | number | boolean | null>;

interface Engine {
  readonly kind: EngineId;
  readonly config: Record<string, unknown>;
  exec(sql: string): Promise<void>;
  query(sql: string): Promise<ResultView>;
  exportAll(sql: string): Promise<{ rows: number; check: string }>;
  /** One parameterized write statement through connection `connection`. */
  write(sql: string, params: Params, connection?: number): Promise<void>;
  /** One parameterized read; returns its row count. */
  read(sql: string, params: Params, connection?: number): Promise<number>;
  readonly connections: number;
  close(): Promise<void>;
  /** Stops the engine's workers at once; used when a deadline fires. */
  kill(): void;
}

let current: Engine | undefined;

function failureOf(error: unknown): PageFailure {
  if (error instanceof Error) {
    return {
      name: error.name === "Error" ? error.constructor.name : error.name,
      message: error.message.slice(0, 2000),
      timeout: error instanceof DeadlineError,
    };
  }
  return { name: typeof error, message: String(error).slice(0, 2000), timeout: false };
}

class DeadlineError extends Error {
  override readonly name = "DeadlineError";
}

/** Races the call against a deadline; on expiry the engine's workers are terminated. */
async function deadline<T>(
  promise: Promise<T>,
  ms: number,
  engine: Engine | undefined,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expired = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      engine?.kill();
      reject(new DeadlineError(`no answer within ${String(ms)} ms; worker terminated`));
    }, ms);
  });
  try {
    return await Promise.race([promise, expired]);
  } finally {
    clearTimeout(timer);
  }
}

async function guarded<T extends object>(action: () => Promise<T>): Promise<PageResult<T>> {
  try {
    return { ok: true, ...(await action()) };
  } catch (error) {
    return { ok: false, failure: failureOf(error) };
  }
}

function engine(): Engine {
  if (current === undefined) throw new Error("No engine is open");
  return current;
}

// ------------------------------------------------------------------------------------------------
// Minnow

async function openMinnow(request: OpenRequest): Promise<Engine> {
  if (request.fresh === true) await deleteOpfsDatabase({ name: request.db });
  const durability = request.durability ?? "strict";
  const workers: Worker[] = [];
  const clients: MinnowDatabaseClient[] = [];
  const connections = Math.max(1, request.connections ?? 1);
  const workerErrors: string[] = [];
  // One at a time: the first connection becomes the leader, and later ones find it.
  for (let index = 0; index < connections; index++) {
    const worker = new Worker("/minnow-worker.js", { type: "module" });
    workers.push(worker);
    const client = new MinnowDatabaseClient(worker, {
      store: { kind: "opfs", name: request.db, durability },
      requestTimeoutMs: 180_000,
      whenSuperseded: "prompt",
      ...(request.minnowOptions === undefined ? {} : { databaseOptions: request.minnowOptions }),
      onWorkerError: (event) => {
        const message = `${event.kind} ${event.context}: ${event.error.name}: ${event.error.message}`;
        workerErrors.push(message.slice(0, 500));
        console.log(`minnow-worker-error ${message.slice(0, 300)}`);
      },
    });
    await client.ready();
    clients.push(client);
  }
  const primary = clients[0];
  if (primary === undefined) throw new Error("no Minnow connection opened");
  minnowPrimary = primary;
  const pick = (connection = 0): MinnowDatabaseClient =>
    clients[connection % clients.length] ?? primary;
  return {
    kind: "minnow",
    connections,
    config: {
      store: "opfs",
      durability,
      connections,
      databaseOptions: request.minnowOptions ?? "defaults",
      memoize: false,
      workerErrors,
    },
    async exec(sql) {
      await primary.execute(sql);
    },
    async query(sql) {
      const result = await primary.query(sql, { memoize: false });
      const names = result.columns;
      return {
        rows: result.rows.length,
        columns: names.length,
        get: (row, column) => result.rows[row]?.[names[column] ?? ""],
      };
    },
    async exportAll(sql) {
      let rows = 0;
      let sumId = 0;
      let nonNull = 0;
      for await (const page of primary.queryCursor(sql, { batchRows: 10_000, memoize: false })) {
        for (const row of page.rows) {
          rows++;
          sumId += Number(row.id);
          if (row.x00 !== null) nonNull++;
        }
      }
      return { rows, check: `${String(rows)}:${String(sumId)}:${String(nonNull)}` };
    },
    async write(sql, params, connection) {
      await pick(connection).execute(sql, params);
    },
    async read(sql, params, connection) {
      const options = params.length === 0 ? { memoize: false } : { params, memoize: false };
      return (await pick(connection).query(sql, options)).rows.length;
    },
    async close() {
      for (const client of clients.slice().reverse()) {
        await Promise.race([
          client.close({ terminateWorker: true }).catch(() => undefined),
          new Promise((resolve) => setTimeout(resolve, 20_000)),
        ]);
      }
      for (const worker of workers) worker.terminate();
    },
    kill() {
      for (const worker of workers) worker.terminate();
    },
  };
}

// ------------------------------------------------------------------------------------------------
// DuckDB-Wasm 1.32.0, EH bundle (single thread), on an `opfs://` database

async function removeOpfsFile(name: string): Promise<void> {
  const root = await navigator.storage.getDirectory();
  await root.removeEntry(name).catch((error: unknown) => {
    if (!(error instanceof DOMException && error.name === "NotFoundError")) throw error;
  });
}

async function opfsFileBytes(name: string): Promise<number> {
  const root = await navigator.storage.getDirectory();
  try {
    return (await (await root.getFileHandle(name)).getFile()).size;
  } catch {
    return 0;
  }
}

async function openDuckdb(request: OpenRequest): Promise<Engine> {
  if (duckdb.PACKAGE_VERSION !== DUCKDB_PACKAGE) {
    // 1.33.x dev builds keep `opfs://` databases in memory without saying so.
    throw new Error(
      `The lane requires DuckDB-Wasm ${DUCKDB_PACKAGE}; this build is ${duckdb.PACKAGE_VERSION}`,
    );
  }
  const file = `${request.db}.duckdb`;
  if (request.fresh === true) {
    await removeOpfsFile(file);
    await removeOpfsFile(`${file}.wal`);
  }
  const worker = new Worker(`${DUCKDB_ASSETS}duckdb-browser-eh.worker.js`);
  const database = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
  await database.instantiate(`${DUCKDB_ASSETS}duckdb-eh.wasm`);
  await database.open({
    path: `opfs://${file}`,
    accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
    query: { castBigIntToDouble: true, castDecimalToDouble: true, castTimestampToDate: true },
  });
  const connection = await database.connect();
  duckdbPrimary = connection;
  if (request.duckdbMemoryLimitBytes !== undefined) {
    await connection.query(
      `SET memory_limit='${String(Math.round(request.duckdbMemoryLimitBytes / 1048576))}MB'`,
    );
  }
  const settings = (
    await connection.query(
      "SELECT current_setting('threads') AS threads, current_setting('memory_limit') AS memory_limit, version() AS version",
    )
  ).toArray()[0] as { toJSON(): Record<string, unknown> } | undefined;
  const view = (result: arrow.Table): ResultView => {
    const vectors = result.schema.fields.map((_, index) => result.getChildAt(index));
    return {
      rows: result.numRows,
      columns: vectors.length,
      get: (row, column) => vectors[column]?.get(row) as unknown,
    };
  };
  return {
    kind: "duckdb",
    connections: 1,
    config: {
      package: duckdb.PACKAGE_VERSION,
      bundle: "eh",
      path: `opfs://${file}`,
      fileBytes: await opfsFileBytes(file),
      ...(settings?.toJSON() ?? {}),
    },
    async exec(sql) {
      await connection.query(sql);
    },
    async query(sql) {
      return view(await connection.query(sql));
    },
    async exportAll(sql) {
      let rows = 0;
      let sumId = 0;
      let nonNull = 0;
      const reader = await connection.send(sql);
      for await (const batch of reader) {
        const ids = batch.getChild("id");
        const xs = batch.getChild("x00");
        for (let i = 0; i < batch.numRows; i++) {
          sumId += Number(ids?.get(i));
          if (xs?.get(i) !== null) nonNull++;
        }
        rows += batch.numRows;
      }
      return { rows, check: `${String(rows)}:${String(sumId)}:${String(nonNull)}` };
    },
    async write(sql, params) {
      const statement = await connection.prepare(sql);
      try {
        await statement.query(...params);
      } finally {
        await statement.close();
      }
    },
    async read(sql, params) {
      const statement = await connection.prepare(sql);
      try {
        return (await statement.query(...params)).numRows;
      } finally {
        await statement.close();
      }
    },
    async close() {
      await Promise.race([
        (async () => {
          await connection.close();
          await database.terminate();
        })().catch(() => undefined),
        new Promise((resolve) => setTimeout(resolve, 20_000)),
      ]);
      worker.terminate();
    },
    kill() {
      worker.terminate();
    },
  };
}

// ------------------------------------------------------------------------------------------------
// SQLite-Wasm on the OPFS SAH pool, in its own worker

interface SqliteCall {
  call(op: string, args?: Record<string, unknown>): Promise<unknown>;
}

async function openSqlite(request: OpenRequest): Promise<Engine & SqliteCall> {
  const worker = new Worker("/sqlite-worker.js", { type: "module" });
  let sequence = 0;
  const pending = new Map<
    number,
    { resolve: (value: unknown) => void; reject: (error: Error) => void }
  >();
  worker.addEventListener(
    "message",
    (
      event: MessageEvent<{
        id: number;
        result?: unknown;
        error?: { name: string; message: string };
      }>,
    ) => {
      const message = event.data;
      const waiter = pending.get(message.id);
      pending.delete(message.id);
      if (waiter === undefined) return;
      if (message.error !== undefined) {
        const error = new Error(message.error.message);
        error.name = message.error.name;
        waiter.reject(error);
      } else waiter.resolve(message.result);
    },
  );
  const call = (op: string, args: Record<string, unknown> = {}): Promise<unknown> =>
    new Promise((resolve, reject) => {
      const id = ++sequence;
      pending.set(id, { resolve, reject });
      worker.postMessage({ id, op, ...args });
    });
  const pragmas: SqlitePragmaSet = request.sqlitePragmas ?? "default";
  const config = (await call("open", {
    file: `/${request.db}.sqlite3`,
    pragmas,
    fresh: request.fresh === true,
  })) as Record<string, unknown>;
  return {
    kind: "sqlite",
    connections: 1,
    config,
    call,
    async exec(sql) {
      await call("exec", { sql });
    },
    async query(sql) {
      const result = (await call("query", { sql })) as { columns: string[]; rows: unknown[][] };
      return {
        rows: result.rows.length,
        columns: result.columns.length,
        get: (row, column) => result.rows[row]?.[column],
      };
    },
    async exportAll(sql) {
      return (await call("stream", { sql })) as { rows: number; check: string };
    },
    async write(sql, params) {
      await call("run", { sql, params });
    },
    async read(sql, params) {
      // A literal statement is prepared and finalized each time, as an app would; a
      // parameterized one reuses its prepared statement.
      return (await call(params.length === 0 ? "readLiteral" : "readOne", {
        sql,
        params,
      })) as number;
    },
    async close() {
      await Promise.race([
        call("close").catch(() => undefined),
        new Promise((resolve) => setTimeout(resolve, 20_000)),
      ]);
      worker.terminate();
    },
    kill() {
      worker.terminate();
    },
  };
}

// ------------------------------------------------------------------------------------------------
// Loading the wide table

const duckdbWideSelect = (column: WideColumn): string => {
  switch (column.family) {
    case "id":
      return "CAST(id AS BIGINT) AS id";
    case "ts":
      return "epoch_ms(CAST(ts AS BIGINT)) AS ts";
    case "day":
      return "CAST(DATE '1970-01-01' + day AS DATE) AS day";
    case "n":
      return `CAST(${column.name} AS DECIMAL(12,2)) AS ${column.name}`;
    default:
      return column.name;
  }
};

function duckdbWideVector(
  column: WideColumn,
  columns: Record<string, WideValue[]>,
  count: number,
): arrow.Vector {
  const values = columns[column.name] ?? [];
  switch (column.family) {
    case "id":
    case "ts":
    case "f":
      return float64Vector(values);
    case "day": {
      const ts = columns.ts ?? [];
      const days: number[] = new Array<number>(count);
      for (let j = 0; j < count; j++) days[j] = Math.floor(Number(ts[j]) / DAY_MS);
      return int32Vector(days);
    }
    case "i":
      return int32Vector(values);
    case "b":
      return boolVector(values);
    case "x":
      return column.mixed === 0
        ? int32Vector(values)
        : column.mixed === 1
          ? float64Vector(values)
          : column.mixed === 2
            ? utf8Vector(values)
            : boolVector(values);
    default:
      return utf8Vector(values); // d, h, and NUMERIC as decimal text
  }
}

async function loadWide(input: { columns: 200 | 500; rows: number }): Promise<LoadResult> {
  const target = engine();
  const spec = wideSpec(input.columns);
  const timings = { genMs: 0, convertMs: 0, engineMs: 0 };
  const started = performance.now();
  const typeOf = (column: WideColumn): string => column[target.kind];
  await target.exec(
    `CREATE TABLE events_wide (${spec.map((c) => `${c.name} ${typeOf(c)}`).join(", ")})`,
  );
  for (let start = 0; start < input.rows; start += LOAD_BATCH_ROWS) {
    const count = Math.min(LOAD_BATCH_ROWS, input.rows - start);
    let t0 = performance.now();
    const columns = wideBatch(spec, start, count, input.rows);
    timings.genMs += performance.now() - t0;
    if (target.kind === "minnow") {
      t0 = performance.now();
      const batch: Record<string, unknown[]> = {
        ...columns,
        ts: (columns.ts ?? []).map((ms) => new Date(Number(ms))),
      };
      timings.convertMs += performance.now() - t0;
      t0 = performance.now();
      await minnowClient().insertBatch("events_wide", { columns: batch } as never);
      timings.engineMs += performance.now() - t0;
    } else if (target.kind === "duckdb") {
      t0 = performance.now();
      const vectors: Record<string, arrow.Vector> = {};
      for (const column of spec) vectors[column.name] = duckdbWideVector(column, columns, count);
      const staged = table(vectors);
      timings.convertMs += performance.now() - t0;
      t0 = performance.now();
      await duckdbConnection().insertArrowTable(staged, { name: "stg", create: true });
      await target.exec(
        `INSERT INTO events_wide SELECT ${spec.map(duckdbWideSelect).join(", ")} FROM stg`,
      );
      await target.exec("DROP TABLE stg");
      timings.engineMs += performance.now() - t0;
    } else {
      t0 = performance.now();
      const arrays = spec.map((column) => {
        const values = columns[column.name] ?? [];
        return column.family === "b" || (column.family === "x" && column.mixed === 3)
          ? values.map((v) => (v === null ? null : v === true ? 1 : 0))
          : values;
      });
      timings.convertMs += performance.now() - t0;
      t0 = performance.now();
      await sqliteCall().call("insertColumns", { table: "events_wide", columns: arrays });
      timings.engineMs += performance.now() - t0;
    }
    if ((start / LOAD_BATCH_ROWS) % 10 === 0) {
      console.log(`load ${target.kind} events_wide ${String(start + count)}/${String(input.rows)}`);
    }
  }
  const t0 = performance.now();
  for (const dimension of dimensionTables()) {
    const ddl = {
      minnow:
        "k TEXT PRIMARY KEY, label TEXT NOT NULL, tier TEXT NOT NULL, weight DOUBLE PRECISION NOT NULL, active BOOLEAN NOT NULL",
      duckdb:
        "k VARCHAR PRIMARY KEY, label VARCHAR NOT NULL, tier VARCHAR NOT NULL, weight DOUBLE NOT NULL, active BOOLEAN NOT NULL",
      sqlite:
        "k TEXT PRIMARY KEY, label TEXT NOT NULL, tier TEXT NOT NULL, weight REAL NOT NULL, active INTEGER NOT NULL",
    }[target.kind];
    await target.exec(`CREATE TABLE ${dimension.name} (${ddl})`);
    const values = dimension.rows
      .map((row) => {
        const active = target.kind === "sqlite" ? (row.active ? "1" : "0") : String(row.active);
        return `('${row.key}', '${row.label}', '${row.tier}', ${String(row.weight)}, ${active})`;
      })
      .join(", ");
    await target.exec(`INSERT INTO ${dimension.name} VALUES ${values}`);
  }
  if (target.kind === "duckdb") await target.exec("CHECKPOINT");
  timings.engineMs += performance.now() - t0;
  return { ...timings, totalMs: performance.now() - started, rows: { events_wide: input.rows } };
}

// ------------------------------------------------------------------------------------------------
// Loading the star schema (the site bench's commerce generator)

function starTypes(
  entity: EntityDefinition,
  scale: number,
): Array<{ duckdb: string; sqlite: string }> {
  return entity.columns.map((column) => {
    if (column.type === "string") return { duckdb: "VARCHAR", sqlite: "TEXT" };
    if (column.type === "boolean") return { duckdb: "BOOLEAN", sqlite: "INTEGER" };
    if (column.type === "datetime") return { duckdb: "TIMESTAMP", sqlite: "TEXT" };
    // Numbers are DOUBLE in Minnow's logical model; the others get INTEGER where every sampled
    // value is integral, as measurements.md did.
    const rows = entity.rows(scale);
    let integral = true;
    for (let i = 0; i < Math.min(rows, 5000) && integral; i++) {
      const value = column.valueAt(i, rows, scale);
      if (value !== null && !Number.isInteger(value)) integral = false;
    }
    return integral
      ? { duckdb: "INTEGER", sqlite: "INTEGER" }
      : { duckdb: "DOUBLE", sqlite: "REAL" };
  });
}

async function loadStar(input: { scale: number }): Promise<LoadResult> {
  const target = engine();
  const entities = commerceEntities.filter((entity) => STAR_TABLES.includes(entity.name));
  const timings = { genMs: 0, convertMs: 0, engineMs: 0 };
  const rowsByTable: Record<string, number> = {};
  const started = performance.now();
  for (const entity of entities) {
    const rows = entity.rows(input.scale);
    rowsByTable[entity.name] = rows;
    const types = starTypes(entity, input.scale);
    if (target.kind === "minnow") {
      await minnowClient().createTable({
        name: entity.name,
        ...(entity.primaryKey === undefined ? {} : { uniqueKey: entity.primaryKey }),
        columns: entity.columns.map((column) => ({ name: column.name, type: column.type })),
      } as never);
    } else {
      const columns = entity.columns.map((column, j) => {
        const type = types[j]?.[target.kind === "duckdb" ? "duckdb" : "sqlite"] ?? "TEXT";
        return `${column.name} ${type}${column.name === entity.primaryKey ? " PRIMARY KEY" : ""}`;
      });
      await target.exec(`CREATE TABLE ${entity.name} (${columns.join(", ")})`);
    }
    for (let start = 0; start < rows; start += LOAD_BATCH_ROWS) {
      const count = Math.min(LOAD_BATCH_ROWS, rows - start);
      let t0 = performance.now();
      const columns = generateEntityBatch(entity, start, count, rows, input.scale);
      timings.genMs += performance.now() - t0;
      if (target.kind === "minnow") {
        t0 = performance.now();
        await minnowClient().insertBatch(entity.name, { columns } as never);
        timings.engineMs += performance.now() - t0;
      } else if (target.kind === "duckdb") {
        t0 = performance.now();
        const vectors: Record<string, arrow.Vector> = {};
        entity.columns.forEach((column, j) => {
          const values = columns[column.name] ?? [];
          vectors[column.name] =
            column.type === "string"
              ? utf8Vector(values as WideValue[])
              : column.type === "boolean"
                ? boolVector(values as WideValue[])
                : column.type === "datetime"
                  ? float64Vector(values.map((d) => (d instanceof Date ? d.getTime() : null)))
                  : types[j]?.duckdb === "INTEGER"
                    ? int32Vector(values as WideValue[])
                    : float64Vector(values as WideValue[]);
        });
        const staged = table(vectors);
        timings.convertMs += performance.now() - t0;
        t0 = performance.now();
        await duckdbConnection().insertArrowTable(staged, { name: "stg", create: true });
        const select = entity.columns
          .map((column) =>
            column.type === "datetime"
              ? `epoch_ms(CAST(${column.name} AS BIGINT)) AS ${column.name}`
              : column.name,
          )
          .join(", ");
        await target.exec(`INSERT INTO ${entity.name} SELECT ${select} FROM stg`);
        await target.exec("DROP TABLE stg");
        timings.engineMs += performance.now() - t0;
      } else {
        t0 = performance.now();
        const arrays = entity.columns.map((column) => {
          const values = columns[column.name] ?? [];
          if (column.type === "boolean")
            return values.map((v) => (v === null ? null : v === true ? 1 : 0));
          if (column.type === "datetime")
            return values.map((d) => (d instanceof Date ? d.toISOString() : null));
          return values;
        });
        timings.convertMs += performance.now() - t0;
        t0 = performance.now();
        await sqliteCall().call("insertColumns", { table: entity.name, columns: arrays });
        timings.engineMs += performance.now() - t0;
      }
    }
    console.log(`load ${target.kind} ${entity.name} ${String(rows)} rows`);
  }
  if (target.kind === "duckdb") await target.exec("CHECKPOINT");
  return { ...timings, totalMs: performance.now() - started, rows: rowsByTable };
}

// The load paths need engine-specific calls the generic handle does not expose.
let minnowPrimary: MinnowDatabaseClient | undefined;
let duckdbPrimary: duckdb.AsyncDuckDBConnection | undefined;
function minnowClient(): MinnowDatabaseClient {
  if (minnowPrimary === undefined) throw new Error("Minnow is not open");
  return minnowPrimary;
}
function duckdbConnection(): duckdb.AsyncDuckDBConnection {
  if (duckdbPrimary === undefined) throw new Error("DuckDB is not open");
  return duckdbPrimary;
}
function sqliteCall(): SqliteCall {
  const target = engine() as Partial<SqliteCall>;
  if (target.call === undefined) throw new Error("SQLite is not open");
  return target as SqliteCall;
}

// ------------------------------------------------------------------------------------------------
// Point workloads

/** Deterministic pseudo-random sequence for key choices. */
function keySequence(seed: number, count: number, max: number): number[] {
  let state = seed >>> 0;
  const keys: number[] = [];
  for (let i = 0; i < count; i++) {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    keys.push((state % max) + 1);
  }
  return keys;
}

export function pointUuid(id: number): string {
  const h = (v: number): string =>
    (Math.imul(v ^ 0x9e3779b9, 0x85ebca6b) >>> 0).toString(16).padStart(8, "0");
  const a = h(id);
  const b = h(id + 0x1000);
  const c = h(id + 0x2000);
  const d = h(id + 0x3000);
  return `${a}-${b.slice(0, 4)}-4${b.slice(4, 7)}-${c.slice(0, 4)}-${c.slice(4)}${d}`;
}

const POINT_TABLES = {
  pt_int: {
    minnow:
      "id BIGINT PRIMARY KEY, a INTEGER NOT NULL, b DOUBLE PRECISION NOT NULL, c TEXT NOT NULL",
    sqlite: "id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b REAL NOT NULL, c TEXT NOT NULL",
  },
  pt_uuid: {
    minnow: "id TEXT PRIMARY KEY, a INTEGER NOT NULL, b DOUBLE PRECISION NOT NULL, c TEXT NOT NULL",
    sqlite: "id TEXT PRIMARY KEY, a INTEGER NOT NULL, b REAL NOT NULL, c TEXT NOT NULL",
  },
} as const;
type PointTable = keyof typeof POINT_TABLES;

const pointKey = (table: PointTable, id: number): string | number =>
  table === "pt_uuid" ? pointUuid(id) : id;
const pointRow = (table: PointTable, id: number): Array<string | number> => [
  pointKey(table, id),
  id % 1000,
  (id % 10007) / 7,
  `row-${String(id)}-${"x".repeat(id % 24)}`,
];

async function pointCreate(input: { tables: PointTable[] }): Promise<Record<string, never>> {
  const target = engine();
  for (const name of input.tables) {
    const ddl = target.kind === "sqlite" ? POINT_TABLES[name].sqlite : POINT_TABLES[name].minnow;
    await target.exec(`CREATE TABLE ${name} (${ddl})`);
  }
  return {};
}

async function pointLoad(input: { table: PointTable; rows: number }): Promise<LoadResult> {
  const target = engine();
  const timings = { genMs: 0, convertMs: 0, engineMs: 0 };
  const started = performance.now();
  for (let start = 0; start < input.rows; start += LOAD_BATCH_ROWS) {
    const count = Math.min(LOAD_BATCH_ROWS, input.rows - start);
    let t0 = performance.now();
    const columns: Array<Array<string | number>> = [[], [], [], []];
    for (let j = 0; j < count; j++) {
      const row = pointRow(input.table, start + j + 1);
      columns.forEach((column, c) => column.push(row[c] ?? ""));
    }
    timings.genMs += performance.now() - t0;
    t0 = performance.now();
    if (target.kind === "minnow") {
      await minnowClient().insertBatch(input.table, {
        columns: { id: columns[0], a: columns[1], b: columns[2], c: columns[3] },
      } as never);
    } else {
      await sqliteCall().call("insertColumns", { table: input.table, columns });
    }
    timings.engineMs += performance.now() - t0;
  }
  return { ...timings, totalMs: performance.now() - started, rows: { [input.table]: input.rows } };
}

const INSERT_SQL = (table: PointTable): string =>
  `INSERT INTO ${table} (id, a, b, c) VALUES (?, ?, ?, ?)`;
const READ_SQL = (table: PointTable): string => `SELECT id, a, b, c FROM ${table} WHERE id = ?`;

/** Sequential single-row INSERTs, each its own commit: `batches` × `perBatch`. */
async function pointInsertSequential(input: {
  table: PointTable;
  firstId: number;
  batches: number;
  perBatch: number;
}): Promise<PointSamples> {
  const target = engine();
  const samples: number[] = [];
  const latencies: number[] = [];
  let id = input.firstId;
  for (let b = 0; b < input.batches; b++) {
    const started = performance.now();
    for (let i = 0; i < input.perBatch; i++) {
      const t0 = performance.now();
      await target.write(INSERT_SQL(input.table), pointRow(input.table, id++));
      latencies.push(performance.now() - t0);
    }
    samples.push((performance.now() - started) / input.perBatch);
  }
  return { samples, latencies, operations: input.perBatch };
}

/** `clients` callers, each inserting `perClient` rows one commit at a time, all at once. */
async function pointInsertConcurrent(input: {
  table: PointTable;
  firstId: number;
  clients: number;
  perClient: number;
  bursts: number;
}): Promise<PointSamples> {
  const target = engine();
  const samples: number[] = [];
  const latencies: number[] = [];
  let next = input.firstId;
  for (let burst = 0; burst < input.bursts; burst++) {
    const started = performance.now();
    await Promise.all(
      Array.from({ length: input.clients }, async (_, client) => {
        for (let i = 0; i < input.perClient; i++) {
          const id = next++;
          const t0 = performance.now();
          await target.write(INSERT_SQL(input.table), pointRow(input.table, id), client);
          latencies.push(performance.now() - t0);
        }
      }),
    );
    samples.push((performance.now() - started) / (input.clients * input.perClient));
  }
  return {
    samples,
    latencies,
    operations: input.clients * input.perClient,
    note: `${String(input.clients)} callers over ${String(target.connections)} connection(s)`,
  };
}

/**
 * Sequential primary-key reads through the worker client, timed per batch. The page is not
 * cross-origin isolated, so its clock is coarsened to about 0.1 ms; a single read is shorter
 * than that, and only a batch mean is a real measurement.
 */
async function pointRead(input: {
  table: PointTable;
  rows: number;
  seed: number;
  batches: number;
  perBatch: number;
  /** Inline the key in the SQL text instead of binding it. */
  literal?: boolean;
  /** Alternate with the keys `pointUpdates` wrote under this seed, so half the reads meet them. */
  updatedSeed?: number;
  updatedCount?: number;
}): Promise<PointSamples> {
  const target = engine();
  const total = input.batches * input.perBatch;
  let keys = keySequence(input.seed, total, input.rows);
  if (input.updatedSeed !== undefined) {
    const updated = keySequence(input.updatedSeed, input.updatedCount ?? total, input.rows);
    keys = keys.map((key, i) => (i % 2 === 0 ? (updated[(i / 2) % updated.length] ?? key) : key));
  }
  const readOne = (key: number): Promise<number> => {
    const value = pointKey(input.table, key);
    if (input.literal !== true) return target.read(READ_SQL(input.table), [value]);
    const inline = typeof value === "number" ? String(value) : `'${value}'`;
    return target.read(`SELECT id, a, b, c FROM ${input.table} WHERE id = ${inline}`, []);
  };
  // One untimed read first, so the batch timings measure warm reads.
  await readOne(keys[0] ?? 1);
  const samples: number[] = [];
  let found = 0;
  for (let b = 0; b < input.batches; b++) {
    const started = performance.now();
    for (let i = 0; i < input.perBatch; i++) {
      const key = keys[(b * input.perBatch + i) % keys.length] ?? 1;
      found += await readOne(key);
    }
    samples.push((performance.now() - started) / input.perBatch);
  }
  const expected = input.batches * input.perBatch;
  if (found !== expected)
    throw new Error(`point reads found ${String(found)} rows, expected ${String(expected)}`);
  return { samples, operations: input.perBatch };
}

/** The first read on a freshly opened database. */
async function pointColdRead(input: { table: PointTable; key: number }): Promise<{ ms: number }> {
  const target = engine();
  const started = performance.now();
  const found = await target.read(READ_SQL(input.table), [pointKey(input.table, input.key)]);
  const ms = performance.now() - started;
  if (found !== 1) throw new Error(`cold point read found ${String(found)} rows`);
  return { ms };
}

/** `count` single-row UPDATEs, each its own commit, on keys from `seed`. */
async function pointUpdates(input: {
  table: PointTable;
  rows: number;
  seed: number;
  count: number;
}): Promise<{ ms: number; keys: number[] }> {
  const target = engine();
  const keys = keySequence(input.seed, input.count, input.rows);
  const started = performance.now();
  for (const key of keys) {
    await target.write(`UPDATE ${input.table} SET a = a + 1, c = ? WHERE id = ?`, [
      `updated-${String(key)}`,
      pointKey(input.table, key),
    ]);
  }
  return { ms: performance.now() - started, keys };
}

async function pointCompact(input: {
  table: PointTable;
}): Promise<{ ms: number; result: unknown }> {
  const started = performance.now();
  const result =
    engine().kind === "minnow" ? await minnowClient().compactTable(input.table) : "not applicable";
  return { ms: performance.now() - started, result: JSON.parse(JSON.stringify(result)) as unknown };
}

/**
 * Waits for Minnow's background maintenance to finish: no compaction or collection job active,
 * collection neither running nor requested, and the store's footprint unchanged for ten
 * observations 250 ms apart. The same rule the Node gate uses for its settled shapes. Other
 * engines have no background work and settle at once.
 */
async function settle(input: {
  timeoutMs: number;
}): Promise<{ settledMs: number; footprint: string }> {
  const started = performance.now();
  if (engine().kind !== "minnow") return { settledMs: 0, footprint: "" };
  const client = minnowClient();
  let previous = "";
  let quiet = 0;
  for (;;) {
    if (performance.now() - started > input.timeoutMs) {
      throw new Error(
        `maintenance did not settle within ${String(input.timeoutMs)} ms (last footprint ${previous})`,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
    const compacting = (await client.listCompactionJobs()).some(
      (job) => job.state !== "published" && job.state !== "cancelled" && job.state !== "aborted",
    );
    const collecting = (await client.listGarbageCollectionJobs()).some(
      (job) => job.state === "planned" || job.state === "running",
    );
    const status = await client.maintenanceStatus();
    if (compacting || collecting || status.collectionRunning || status.collectionRequested) {
      quiet = 0;
      previous = "";
      continue;
    }
    const stats = await client.storageStats();
    const footprint = JSON.stringify([
      stats.liveBlockCount + stats.obsoleteBlockCount,
      stats.manifestCount,
      stats.segmentCount,
    ]);
    quiet = footprint === previous ? quiet + 1 : 0;
    previous = footprint;
    if (quiet >= 10) return { settledMs: performance.now() - started - 2500, footprint };
  }
}

// ------------------------------------------------------------------------------------------------
// The API the driver calls

async function open(request: OpenRequest): Promise<PageResult<OpenResult>> {
  return guarded(async () => {
    if (current !== undefined) await close();
    const started = performance.now();
    if (request.engine === "minnow") {
      current = await openMinnow(request);
    } else if (request.engine === "duckdb") {
      current = await openDuckdb(request);
    } else {
      current = await openSqlite(request);
    }
    return { openMs: performance.now() - started, config: current.config };
  });
}

async function close(): Promise<boolean> {
  const engineToClose = current;
  current = undefined;
  minnowPrimary = undefined;
  duckdbPrimary = undefined;
  if (engineToClose !== undefined) await engineToClose.close();
  return true;
}

async function runQuery(request: QueryRequest): Promise<PageResult<QueryResult>> {
  return guarded(async () => {
    const target = engine();
    const times: number[] = [];
    let summary: Omit<QueryResult, "times"> = {};
    for (let rep = 0; rep < request.reps; rep++) {
      const started = performance.now();
      if (request.kind === "export") {
        const exported = await deadline(target.exportAll(request.sql), request.timeoutMs, target);
        times.push(performance.now() - started);
        if (rep === 0) summary = { rows: exported.rows, hash: exported.check };
      } else {
        const result = await deadline(target.query(request.sql), request.timeoutMs, target);
        times.push(performance.now() - started);
        if (rep === 0) {
          if (request.check === "count") {
            summary = {
              rows: result.rows,
              columns: result.columns,
              hash: `${String(result.rows)}x${String(result.columns)}`,
            };
          } else if (request.digest) {
            const digest = digestResult(result, request.ordered);
            summary = {
              rows: digest.rows,
              columns: digest.columns,
              hash: digest.hash,
              ...(digest.dump === undefined ? {} : { dump: digest.dump }),
            };
          } else {
            summary = { rows: result.rows, columns: result.columns };
          }
        }
      }
    }
    return { times, ...summary };
  });
}

async function count(input: {
  sql: string;
  timeoutMs: number;
}): Promise<PageResult<{ count: number; ms: number }>> {
  return guarded(async () => {
    const target = engine();
    const started = performance.now();
    const result = await deadline(target.query(input.sql), input.timeoutMs, target);
    return { count: Number(result.get(0, 0)), ms: performance.now() - started };
  });
}

async function storedBytes(): Promise<number> {
  return (await navigator.storage.estimate()).usage ?? 0;
}

/** Writes a file through OPFS so the driver can prove the profile's OPFS lives on its disk. */
async function opfsProbe(input: { bytes: number; remove: boolean }): Promise<boolean> {
  const root = await navigator.storage.getDirectory();
  if (input.remove) {
    await root.removeEntry("browser-perf-probe.bin").catch(() => undefined);
    return true;
  }
  const handle = await root.getFileHandle("browser-perf-probe.bin", { create: true });
  const writable = await handle.createWritable();
  const chunk = new Uint8Array(1 << 20);
  for (let i = 0; i < chunk.length; i++) chunk[i] = (i * 31) & 255;
  for (let written = 0; written < input.bytes; written += chunk.length) await writable.write(chunk);
  await writable.close();
  return true;
}

function wrap<I, O extends object>(
  action: (input: I) => Promise<O>,
): (input: I) => Promise<PageResult<O>> {
  return (input) => guarded(() => action(input));
}

const api = {
  open,
  close,
  runQuery,
  count,
  storedBytes,
  opfsProbe,
  loadWide: wrap(loadWide),
  loadStar: wrap(loadStar),
  pointCreate: wrap(pointCreate),
  pointLoad: wrap(pointLoad),
  pointInsertSequential: wrap(pointInsertSequential),
  pointInsertConcurrent: wrap(pointInsertConcurrent),
  pointRead: wrap(pointRead),
  pointColdRead: wrap(pointColdRead),
  pointUpdates: wrap(pointUpdates),
  pointCompact: wrap(pointCompact),
  settle: wrap(settle),
};

export type BrowserPerfApi = typeof api;

declare global {
  interface Window {
    BrowserPerf: BrowserPerfApi;
    browserPerfReady: boolean;
  }
}
window.BrowserPerf = api;
window.browserPerfReady = true;
