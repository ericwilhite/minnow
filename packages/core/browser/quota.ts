import { MinnowDatabaseClient } from "@minnowdb/core/client";

let name: string;
let kind: "indexeddb" | "opfs";
let acknowledged = 0;
let payloads: string[];
const connect = () =>
  new MinnowDatabaseClient(
    new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" }),
    {
      store: { kind, name, durability: "strict" },
      requestTimeoutMs: 15_000,
    },
  );

export async function initialize(adapter: "indexeddb" | "opfs") {
  kind = adapter;
  name = `quota-${crypto.randomUUID()}`;
  acknowledged = 0;
  payloads = [];
  const filling = connect();
  await filling.ready();
  await filling.execute("CREATE TABLE counter(id INTEGER PRIMARY KEY, n INTEGER)");
  await filling.execute("CREATE TABLE receipts(id INTEGER PRIMARY KEY, payload TEXT)");
  await filling.insertBatch("counter", [{ id: 1, n: 0 }]);
  await filling.close({ terminateWorker: true });
}

export async function fill() {
  const db = connect();
  try {
    await db.ready();
    for (let id = 0; id < 128; id++) {
      const payload = btoa(
        Array.from({ length: 4 }, () =>
          Array.from(crypto.getRandomValues(new Uint8Array(65_536)), (byte) =>
            String.fromCharCode(byte),
          ).join(""),
        ).join(""),
      );
      payloads[id] = payload;
      try {
        await db.write(async (tx) => {
          await tx.execute("UPDATE counter SET n = n + 1 WHERE id = 1");
          await tx.insertBatch("receipts", [{ id, payload }]);
        });
        acknowledged++;
      } catch (error) {
        return {
          acknowledged,
          failure:
            error instanceof Error
              ? { name: error.name, message: error.message }
              : { name: "unknown", message: String(error) },
        };
      }
    }
    const stored = (await db.query("SELECT payload FROM receipts WHERE id = 0")).rows[0]?.payload;
    throw new Error(
      JSON.stringify({
        reason: "The native quota override did not refuse any write",
        estimate: await navigator.storage.estimate(),
        storedLength: typeof stored === "string" ? stored.length : null,
        expectedLength: payloads[0]?.length,
      }),
    );
  } finally {
    await db.close({ terminateWorker: true }).catch(() => undefined);
  }
}

export async function verify() {
  const db = connect();
  try {
    await db.ready();
    return await db.snapshot(async (tx) => {
      const counter = (await tx.query("SELECT n FROM counter WHERE id = 1")).rows[0]?.n;
      const rows = (await tx.query("SELECT id, payload FROM receipts ORDER BY id")).rows;
      return {
        acknowledged,
        counter,
        rows: rows.length,
        exact: rows.every((row, index) => row.id === index && row.payload === payloads[index]),
      };
    });
  } finally {
    await db.close({ terminateWorker: true });
  }
}
