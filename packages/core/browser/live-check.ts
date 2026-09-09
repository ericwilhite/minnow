import { MinnowDatabaseClient } from "@minnowdb/core/client";
import type { QueryRow } from "@minnowdb/core";

/** Real-storage counterpart to the unit patch tests: transferable buffers, private subscriber
 * payloads, numeric ordering and Date reconstruction all cross an actual module worker. */
export async function runLiveCorrectness(store: "indexeddb" | "opfs"): Promise<{
  checked: number;
  patchRows: number[];
}> {
  const worker = new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
  const client = new MinnowDatabaseClient(worker, {
    store: { kind: store, name: `live-correctness-${crypto.randomUUID()}` },
  });
  const patchRows: number[] = [];
  worker.addEventListener("message", (event) => {
    const frame = event.data as {
      kind?: string;
      event?: string;
      payload?: { result: { rowCount: number } };
    };
    if (frame.kind === "rpc-event" && frame.event === "patch" && frame.payload !== undefined)
      patchRows.push(frame.payload.result.rowCount);
  });
  const live = client.liveQueries();
  let rows: QueryRow[] = [];
  const errors: unknown[] = [];
  let checked = 0;
  try {
    await client.ready();
    await client.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x NUMERIC(10,2), d TIMESTAMP)");
    await client.execute("INSERT INTO t VALUES (1,2,'2026-01-01'),(2,10,'2026-01-02')");
    const sql = "SELECT id,x,d FROM t ORDER BY x,id LIMIT 2";
    await live.subscribePatches(sql, {
      onPatch: (patch) => {
        if (patch.type === "patch") patch.retained.fill(-100);
      },
      onError: (error) => errors.push(error),
    });
    await live.subscribePatches(sql, {
      onPatch: (patch) => {
        if (patch.type === "reset") rows = patch.result.rows;
        else {
          const changed = new Map(patch.changedRows.map(({ index, row }) => [index, row]));
          rows = Array.from(patch.retained, (was, index) => {
            const row = was >= 0 ? rows[was] : changed.get(index);
            if (row === undefined) throw new Error("Patch baseline is missing a row");
            return row;
          });
        }
      },
      onError: (error) => errors.push(error),
    });
    for (const sqlWrite of [
      "INSERT INTO t VALUES (3,3,'2026-01-03')",
      "UPDATE t SET x=20 WHERE id=1",
      "DELETE FROM t WHERE id=3",
      "DELETE FROM t",
      "INSERT INTO t VALUES (4,-2,'2026-01-04')",
    ]) {
      await client.execute(sqlWrite);
      await live.refresh();
      const expected = (await client.query(sql, { memoize: false })).rows;
      if (JSON.stringify(rows) !== JSON.stringify(expected))
        throw new Error(`Incorrect patch after ${sqlWrite}`);
      if (rows.some((row) => !(row.d instanceof Date)))
        throw new Error("Date did not survive transfer");
      // Exercise the peer-free and shared-peer window paths over real stored NUMERIC and
      // datetime columns, including the empty result after deleting every row.
      const source = (await client.query("SELECT id,x,d FROM t ORDER BY id", { memoize: false }))
        .rows;
      const key = source[0]?.id ?? 1;
      for (let repeat = 0; repeat < 2; repeat += 1) {
        const point = await client.query("SELECT id,x,d FROM t WHERE id=?", {
          params: [key],
          memoize: false,
        });
        if (JSON.stringify(point.rows) !== JSON.stringify(source.filter((row) => row.id === key)))
          throw new Error(`Incorrect keyed read after ${sqlWrite}`);
        if (point.rows.some((row) => !(row.d instanceof Date)))
          throw new Error("Keyed-read Date did not survive transfer");
      }
      for (const ranking of [false, true]) {
        const result = await client.query(
          `SELECT id,x,d,SUM(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s${ranking ? ", RANK() OVER (ORDER BY id) AS r" : ""} FROM t ORDER BY id`,
          { memoize: false },
        );
        let total = 0;
        const expectedWindow = source.map((row, index) => {
          total += Number(row.x);
          return { ...row, s: total.toFixed(2), ...(ranking ? { r: index + 1 } : {}) };
        });
        if (JSON.stringify(result.rows) !== JSON.stringify(expectedWindow))
          throw new Error(`Incorrect window after ${sqlWrite}`);
        if (result.rows.some((row) => !(row.d instanceof Date)))
          throw new Error("Window Date did not survive transfer");
      }
      if (errors.length > 0) throw new Error(`Live subscription failed: ${String(errors[0])}`);
      checked += 1;
    }
    return { checked, patchRows };
  } finally {
    await live.close();
    await client.close({ terminateWorker: true });
  }
}
