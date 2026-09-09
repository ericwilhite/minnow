import { MinnowDatabaseClient } from "@minnowdb/core/client";

export let lastDatabaseName = "";
export let lastMeasurements: unknown;

export async function captureOpfsFiles() {
  const root = await navigator.storage.getDirectory();
  const namespace = await root.getDirectoryHandle("minnowdb");
  const directory = await namespace.getDirectoryHandle(lastDatabaseName);
  const files: Array<{ path: string; base64: string }> = [];
  async function visit(dir: FileSystemDirectoryHandle, prefix: string): Promise<void> {
    for await (const [name, entry] of dir.entries()) {
      const path = `${prefix}${name}`;
      if (entry.kind === "directory") await visit(entry as FileSystemDirectoryHandle, `${path}/`);
      else {
        const bytes = new Uint8Array(
          await (await (entry as FileSystemFileHandle).getFile()).arrayBuffer(),
        );
        let binary = "";
        for (let i = 0; i < bytes.length; i += 32768)
          binary += String.fromCharCode(...bytes.subarray(i, i + 32768));
        files.push({ path, base64: btoa(binary) });
      }
    }
  }
  await visit(directory, "");
  return { name: lastDatabaseName, measurements: lastMeasurements, files };
}

function summarize(samples: number[]) {
  const sorted = [...samples].sort((a, b) => a - b);
  const percentile = (p: number) => sorted[Math.ceil(sorted.length * p) - 1] ?? 0;
  return {
    samples: sorted.length,
    p50: percentile(0.5),
    p95: percentile(0.95),
    p99: percentile(0.99),
    max: sorted.at(-1) ?? 0,
  };
}

/** Sustained strict-durability workload; maintenance stays at its shipped defaults. */
export async function run(
  kind: "indexeddb" | "opfs",
  count: number,
  onProgress?: (completed: number) => void,
) {
  if (!Number.isSafeInteger(count) || count < 300 || count > 10_000)
    throw new Error("POS count must be 300–10,000");
  const name = `pos-${crypto.randomUUID()}`;
  lastDatabaseName = name;
  lastMeasurements = undefined;
  const connect = () =>
    new MinnowDatabaseClient(
      new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" }),
      {
        store: { kind, name, durability: "strict" },
        requestTimeoutMs: 30_000,
      },
    );
  let db = connect();
  const writes: number[] = [],
    reads: number[] = [];
  try {
    await db.ready();
    await db.execute("CREATE TABLE stock(id INTEGER PRIMARY KEY, qty INTEGER)");
    await db.execute("CREATE TABLE sales(id INTEGER PRIMARY KEY, total INTEGER)");
    await db.execute(
      "CREATE TABLE lines(id INTEGER PRIMARY KEY, sale_id INTEGER, sku INTEGER, qty INTEGER)",
    );
    await db.insertBatch(
      "stock",
      Array.from({ length: 1000 }, (_, id) => ({ id, qty: 10_000 })),
    );
    onProgress?.(0);
    for (let id = 0; id < count; id++) {
      const start = performance.now();
      await db.write(async (tx) => {
        await tx.execute("UPDATE stock SET qty = qty - 1 WHERE id = ?", [id % 1000]);
        await tx.insertBatch("sales", [{ id, total: 1000 }]);
        await tx.insertBatch("lines", [{ id, sale_id: id, sku: id % 1000, qty: 1 }]);
      });
      writes.push(performance.now() - start);
      const reading = performance.now();
      const qty = (await db.query("SELECT qty FROM stock WHERE id = ?", { params: [id % 1000] }))
        .rows[0]?.qty;
      reads.push(performance.now() - reading);
      if (qty !== 9999 - Math.floor(id / 1000))
        throw new Error(`Incorrect stock after sale ${String(id)}`);
      if ((id + 1) % 100 === 0) onProgress?.(id + 1);
    }
    const maintenance = await db.maintenanceStatus();
    lastMeasurements = {
      kind,
      count,
      writes: summarize(writes),
      reads: summarize(reads),
      maintenance,
    };
    await db.close({ terminateWorker: true });
    db = connect();
    await db.ready();
    const totals = await db.snapshot(async (tx) => ({
      sales: (await tx.query("SELECT COUNT(*) AS n, SUM(total) AS total FROM sales")).rows,
      lines: (await tx.query("SELECT COUNT(*) AS n, SUM(qty) AS qty FROM lines")).rows,
      stock: (await tx.query("SELECT SUM(qty) AS qty FROM stock")).rows,
    }));
    return {
      kind,
      count,
      writes: summarize(writes),
      reads: summarize(reads),
      first100: summarize(writes.slice(0, 100)),
      last100: summarize(writes.slice(-100)),
      maintenance,
      totals,
    };
  } finally {
    await db.close({ terminateWorker: true });
  }
}
