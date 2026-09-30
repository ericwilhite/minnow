import { MinnowDatabaseClient } from "@minnowdb/core/client";
import type { DatabaseRow } from "@minnowdb/core";

function open(
  kind: "indexeddb" | "opfs",
  name: string,
): { client: MinnowDatabaseClient; worker: Worker; diagnostics: string[] } {
  const worker = new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
  const diagnostics: string[] = [];
  const client = new MinnowDatabaseClient(worker, {
    store: { kind, name, durability: "strict" },
    databaseOptions: { autoCompact: false, autoCollect: false },
    onWorkerError: (event) => diagnostics.push(String(event.error)),
  });
  return { client, worker, diagnostics };
}
export async function runAuditCorrectness(kind: "indexeddb" | "opfs"): Promise<{
  rows: DatabaseRow[];
  refusals: number;
  diagnostics: string[];
  regexBounded: boolean;
  arrayValue: string;
}> {
  const name = `audit-${kind}-${crypto.randomUUID()}`;
  let connection = open(kind, name);
  const diagnostics: string[] = [];
  try {
    await connection.client.execute(
      "CREATE TABLE exact_json (id INTEGER PRIMARY KEY, j JSONB, n NUMERIC(35,16), s TEXT)",
    );
    await connection.client.execute(
      `INSERT INTO exact_json VALUES (1,'{"n":9007199254740993.0001}',9007199254740993.0001,'1'),(2,'{"n":9007199254740992.0001}',-1.25,'2')`,
    );
    await connection.client.execute(
      "CREATE TABLE exact_arrays (id INTEGER PRIMARY KEY, a INTEGER[])",
    );
    let refusedArray = false;
    try {
      await connection.client.insert("exact_arrays", { id: 1, a: "[9007199254740993]" });
    } catch (error) {
      if (!(error instanceof RangeError)) throw error;
      refusedArray = true;
    }
    if (!refusedArray) throw new Error("Lossy ARRAY input was accepted");
    await connection.client.insert("exact_arrays", { id: 1, a: '["9007199254740993"]' });
    await connection.client.close();
    connection.worker.terminate();
    diagnostics.push(...connection.diagnostics);
    connection = open(kind, name);
    const arrayValue = (await connection.client.query("SELECT a FROM exact_arrays")).rows[0]?.a;
    if (typeof arrayValue !== "string") throw new Error("Persisted ARRAY fixture is missing");
    const rows = (
      await connection.client.query(
        `SELECT id, j->>'n' AS extracted, DIV(n,1) AS quotient,
      TO_CHAR(n,'FM9999999999999999.0000') AS formatted, s FROM exact_json ORDER BY id`,
        { memoize: false },
      )
    ).rows;
    const scalars = (
      await connection.client
        .query(`SELECT NULL IN (SELECT id FROM exact_json WHERE id < 0) AS included,
      NULL NOT IN (SELECT id FROM exact_json WHERE id < 0) AS excluded,
      SUBSTRING('abc' FROM 'a|ab') AS longest, 'A19!' ~ '^[[:alpha:]][[:digit:]]+[[:punct:]]$' AS classes,
      FORMAT('%2$s%s','a','b','c') AS positional, QUOTE_IDENT('select') AS identifier,
      TO_CHAR(MAKE_TIMESTAMP(1,1,1,0,0,0),'YYYY-MM-DD') AS year`)
    ).rows;
    rows.push(...scalars);
    let refusals = 0;
    for (const sql of [
      "SELECT s = 1 FROM exact_json",
      "SELECT UPPER(s) = 1 FROM exact_json",
      "SELECT TIMESTAMP '2026-02-30 12:00:00'",
      "SELECT FORMAT('%q','x')",
      "SELECT 'aa' ~ '(a)\\1'",
    ]) {
      try {
        await connection.client.query(sql);
      } catch {
        refusals += 1;
      }
    }
    // The browser page supplies an independent watchdog: a stuck database worker cannot block it.
    // Exercise constant folding as well as matching, then prove the worker remains usable.
    let watchdog: ReturnType<typeof setTimeout> | undefined;
    try {
      const response = await Promise.race([
        connection.client.query(`SELECT '${"a".repeat(1000)}!' ~ '(a+)+$' AS matched`),
        new Promise<never>((_resolve, reject) => {
          watchdog = setTimeout(
            () => reject(new Error("Regex exceeded the external watchdog")),
            8000,
          );
        }),
      ]);
      if (response.rows[0]?.matched !== false)
        throw new Error("Pathological regex returned a wrong match");
    } catch (error) {
      if (!(error instanceof RangeError)) throw error;
    } finally {
      clearTimeout(watchdog);
    }
    rows.push(...(await connection.client.query("SELECT COUNT(*) AS intact FROM exact_json")).rows);
    diagnostics.push(...connection.diagnostics);
    return { rows, refusals, diagnostics, regexBounded: true, arrayValue };
  } finally {
    await connection.client.close();
    connection.worker.terminate();
  }
}

export async function runAcknowledgedWalDamage(): Promise<string> {
  const name = `audit-wal-${crypto.randomUUID()}`;
  const connection = open("opfs", name);
  await connection.client.execute("CREATE TABLE t (n INTEGER)");
  await connection.client.execute("INSERT INTO t VALUES (42)");
  // Crash without a shutdown checkpoint: this strict write has already returned success.
  connection.worker.terminate();
  const root = await navigator.storage.getDirectory();
  const directory = await (await root.getDirectoryHandle("minnowdb")).getDirectoryHandle(name);
  const wal = await directory.getFileHandle("wal");
  const bytes = await (await wal.getFile()).arrayBuffer();
  if (bytes.byteLength === 0) throw new Error("Test needs an acknowledged WAL frame");
  let writable: FileSystemWritableFileStream | undefined;
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      writable = await wal.createWritable();
      break;
    } catch (error) {
      if (
        !(error instanceof DOMException) ||
        !["NoModificationAllowedError", "InvalidStateError"].includes(error.name)
      )
        throw error;
      await new Promise<void>((resolve) => setTimeout(resolve, 20));
    }
  }
  if (writable === undefined) throw new Error("Terminated worker did not release its WAL handle");
  await writable.write(new Uint8Array(bytes.byteLength));
  await writable.close();
  const reopened = open("opfs", name);
  try {
    await reopened.client.query("SELECT * FROM t");
    throw new Error("Damaged acknowledged WAL reopened without reporting corruption");
  } catch (error) {
    if (!(error instanceof Error)) throw error;
    return error.name;
  } finally {
    await reopened.client.close();
    reopened.worker.terminate();
  }
}

/** Prototype-named fields cross worker RPC and survive compaction and reopening. */
export async function runAuditNamedColumns(kind: "indexeddb" | "opfs"): Promise<{
  values: number[][];
  ownProperties: boolean;
  plainRows: boolean;
  checkRefused: boolean;
  diagnostics: string[];
}> {
  const name = `audit-names-${kind}-${crypto.randomUUID()}`;
  let connection = open(kind, name);
  const diagnostics: string[] = [];
  try {
    await connection.client.execute(
      'CREATE TABLE named_columns (id INTEGER PRIMARY KEY, "__proto__" REAL NOT NULL DEFAULT 9 CHECK ("__proto__">=0), doubled REAL GENERATED ALWAYS AS ("__proto__"*2) STORED)',
    );
    await connection.client.execute("INSERT INTO named_columns VALUES (1,DEFAULT,DEFAULT)");
    await connection.client.insert("named_columns", { id: 2, ["__proto__"]: 19 });
    await connection.client.execute(
      'UPDATE named_columns SET "__proto__"="__proto__"+1 WHERE id=1',
    );
    await connection.client.execute("BEGIN");
    await connection.client.execute(
      'UPDATE named_columns SET "__proto__"="__proto__"+2 WHERE id=1',
    );
    const staged = await connection.client.query(
      'SELECT "__proto__",doubled FROM named_columns WHERE id=1',
      { memoize: false },
    );
    const stagedRow = staged.rows[0];
    if (stagedRow?.__proto__ !== 12 || stagedRow.doubled !== 24)
      throw new Error("Scoped named-column result was lost");
    await connection.client.execute("ROLLBACK");
    let checkRefused = false;
    try {
      await connection.client.execute('UPDATE named_columns SET "__proto__"=-1 WHERE id=2');
    } catch (error) {
      if (!(error instanceof TypeError) || !error.message.includes("CHECK")) throw error;
      checkRefused = true;
    }
    await connection.client.compactTable("named_columns");
    await connection.client.close();
    connection.worker.terminate();
    diagnostics.push(...connection.diagnostics);
    connection = open(kind, name);
    const result = await connection.client.query(
      'SELECT id,"__proto__",doubled FROM named_columns ORDER BY id',
      { memoize: false },
    );
    diagnostics.push(...connection.diagnostics);
    return {
      values: result.rows.map((row) => [
        Number(row.id),
        Number(row.__proto__),
        Number(row.doubled),
      ]),
      ownProperties: result.rows.every((row) => Object.hasOwn(row, "__proto__")),
      plainRows: result.rows.every((row) => Object.getPrototypeOf(row) === Object.prototype),
      checkRefused,
      diagnostics,
    };
  } finally {
    await connection.client.close();
    connection.worker.terminate();
  }
}
