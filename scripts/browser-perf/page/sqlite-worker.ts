/**
 * SQLite-Wasm in a dedicated worker, on the OPFS SAH-pool VFS.
 *
 * The SAH pool is the VFS that needs no cross-origin isolation, like Minnow's OPFS store. It
 * holds every file of its pool exclusively, so one worker owns the pool at a time: concurrent
 * clients are concurrent requests to this worker, which runs them one after another.
 *
 * Pragmas are applied and then read back, and the read-back values are what the lane records.
 * WAL takes effect on this VFS only with `locking_mode=EXCLUSIVE`, so the strict and relaxed sets
 * ask for that first; whatever SQLite actually reports is what the cell names.
 */
import type {
  Database,
  PreparedStatement,
  SAHPoolUtil,
  Sqlite3Static,
  SqlValue,
} from "@sqlite.org/sqlite-wasm";
import type { SqlitePragmaSet } from "../protocol.ts";

const SQLITE_ENTRY = "/vendor/sqlite/index.mjs";
const POOL_NAME = "browser-perf-sah";

interface SqliteModule {
  default: () => Promise<Sqlite3Static>;
}

let sqlite3: Sqlite3Static | undefined;
let pool: SAHPoolUtil | undefined;
let database: Database | undefined;
const statements = new Map<string, PreparedStatement>();

const PRAGMAS: Record<SqlitePragmaSet, string[]> = {
  default: [],
  strict: ["PRAGMA locking_mode=EXCLUSIVE", "PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL"],
  relaxed: [
    "PRAGMA locking_mode=EXCLUSIVE",
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
  ],
};

function db(): Database {
  if (database === undefined) throw new Error("SQLite database is not open");
  return database;
}

function statement(sql: string): PreparedStatement {
  let prepared = statements.get(sql);
  if (prepared === undefined) {
    prepared = db().prepare(sql);
    statements.set(sql, prepared);
  }
  return prepared;
}

interface Request {
  readonly id: number;
  readonly op: string;
  readonly [key: string]: unknown;
}

type Columns = SqlValue[][];

const operations: Record<string, (request: Request) => unknown> = {
  async open(request) {
    const module = (await import(/* @vite-ignore */ SQLITE_ENTRY)) as SqliteModule;
    sqlite3 ??= await module.default();
    pool ??= await sqlite3.installOpfsSAHPoolVfs({
      name: POOL_NAME,
      directory: `/${POOL_NAME}`,
      initialCapacity: 8,
    });
    const file = request.file as string;
    if (request.fresh === true) pool.unlink(file);
    if (pool.getCapacity() - pool.getFileCount() < 3) await pool.addCapacity(4);
    database = new pool.OpfsSAHPoolDb(file);
    for (const pragma of PRAGMAS[request.pragmas as SqlitePragmaSet]) database.exec(pragma);
    // Touch the file so the pragmas (WAL in particular) are in force before anything is timed.
    database.exec("CREATE TABLE IF NOT EXISTS __perf_probe (x INTEGER); DROP TABLE __perf_probe");
    return {
      version: sqlite3.version.libVersion,
      vfs: "opfs-sahpool",
      pragmaSet: request.pragmas,
      journal_mode: database.selectValue("PRAGMA journal_mode"),
      synchronous: database.selectValue("PRAGMA synchronous"),
      locking_mode: database.selectValue("PRAGMA locking_mode"),
      cache_size: database.selectValue("PRAGMA cache_size"),
      page_size: database.selectValue("PRAGMA page_size"),
    };
  },
  exec(request) {
    db().exec(request.sql as string);
    return true;
  },
  /** One statement with parameters, as its own autocommit transaction. */
  run(request) {
    const prepared = statement(request.sql as string);
    prepared.bind(request.params as SqlValue[]).stepReset();
    return true;
  },
  /** One parameterized read; returns its row count so the work cannot be skipped. */
  readOne(request) {
    const prepared = statement(request.sql as string);
    let rows = 0;
    prepared.bind(request.params as SqlValue[]);
    while (prepared.step()) rows++;
    prepared.reset();
    return rows;
  },
  readLiteral(request) {
    const prepared = db().prepare(request.sql as string);
    try {
      let rows = 0;
      while (prepared.step()) rows++;
      return rows;
    } finally {
      prepared.finalize();
    }
  },
  query(request) {
    const prepared = db().prepare(request.sql as string);
    try {
      const columns = prepared.getColumnNames();
      const rows: SqlValue[][] = [];
      while (prepared.step()) rows.push(prepared.get([]));
      return { columns, rows };
    } finally {
      prepared.finalize();
    }
  },
  stream(request) {
    const prepared = db().prepare(request.sql as string);
    try {
      const columns = prepared.getColumnNames();
      const idColumn = columns.indexOf("id");
      const xColumn = columns.indexOf("x00");
      let rows = 0;
      let sumId = 0;
      let nonNull = 0;
      while (prepared.step()) {
        const row = prepared.get([]);
        rows++;
        sumId += Number(row[idColumn]);
        if (row[xColumn] !== null) nonNull++;
      }
      return { rows, check: `${String(rows)}:${String(sumId)}:${String(nonNull)}` };
    } finally {
      prepared.finalize();
    }
  },
  /** Inserts columns of values in one transaction through a cached prepared statement. */
  insertColumns(request) {
    const table = request.table as string;
    const columns = request.columns as Columns;
    const width = columns.length;
    const prepared = statement(
      `INSERT INTO ${table} VALUES (${new Array(width).fill("?").join(",")})`,
    );
    const count = columns[0]?.length ?? 0;
    const row = new Array<SqlValue>(width);
    db().exec("BEGIN");
    for (let r = 0; r < count; r++) {
      for (let c = 0; c < width; c++) row[c] = columns[c]?.[r] ?? null;
      prepared.bind(row).stepReset();
    }
    db().exec("COMMIT");
    return count;
  },
  storedBytes() {
    return (
      Number(db().selectValue("PRAGMA page_count")) * Number(db().selectValue("PRAGMA page_size"))
    );
  },
  close() {
    for (const prepared of statements.values()) prepared.finalize();
    statements.clear();
    database?.close();
    database = undefined;
    return true;
  },
};

addEventListener("message", (event: MessageEvent<Request>) => {
  const request = event.data;
  void (async () => {
    try {
      const operation = operations[request.op];
      if (operation === undefined) throw new Error(`Unknown SQLite operation ${request.op}`);
      postMessage({ id: request.id, result: await operation(request) });
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error));
      postMessage({ id: request.id, error: { name: failure.name, message: failure.message } });
    }
  })();
});
