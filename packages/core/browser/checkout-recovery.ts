import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { parseRpcRequest, parseRpcResponse } from "@minnowdb/core/worker-protocol";

let client: MinnowDatabaseClient;
let worker: Worker;
let fault: "before-commit" | "after-commit" | undefined;
let commitRequest: string | undefined;
let faultObserved = false;

export async function open(name: string, kind: "indexeddb" | "opfs"): Promise<void> {
  worker = new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
  fault = undefined;
  commitRequest = undefined;
  faultObserved = false;
  // Install the fault interceptor before the client's response listener.
  worker.addEventListener("message", (event: MessageEvent<unknown>) => {
    const response = parseRpcResponse(event.data);
    if (
      fault === "after-commit" &&
      response?.kind === "rpc-result" &&
      response.requestId === commitRequest
    ) {
      event.stopImmediatePropagation();
      faultObserved = true;
      worker.terminate();
    }
  });
  const post = worker.postMessage.bind(worker);
  worker.postMessage = (
    message: unknown,
    options?: StructuredSerializeOptions | Transferable[],
  ) => {
    const request = parseRpcRequest(message);
    if (fault !== undefined && request?.kind === "rpc-call" && request.method === "commit") {
      commitRequest = request.requestId;
      if (fault === "before-commit") {
        faultObserved = true;
        worker.terminate();
        return;
      }
    }
    if (Array.isArray(options)) post(message, options);
    else post(message, options);
  };
  client = new MinnowDatabaseClient(worker, {
    store: { kind, name, durability: "strict" },
    requestTimeoutMs: 3_000,
  });
  await client.ready();
}

export async function initialize(): Promise<void> {
  await client.execute("CREATE TABLE stock(id INTEGER PRIMARY KEY, qty INTEGER)");
  await client.execute(
    "CREATE TABLE sales(id TEXT PRIMARY KEY, sku INTEGER, qty INTEGER, total INTEGER)",
  );
  await client.execute(
    "CREATE TABLE lines(id TEXT PRIMARY KEY, sale_id TEXT REFERENCES sales(id), sku INTEGER REFERENCES stock(id), qty INTEGER)",
  );
  await client.execute(
    "CREATE TABLE checkout_intents(id TEXT PRIMARY KEY, sku INTEGER, qty INTEGER, total INTEGER, state TEXT)",
  );
  await client.execute("CREATE INDEX sales_sku ON sales(sku)");
  await client.execute(
    "CREATE TABLE stock_audit(sku INTEGER, before_qty INTEGER, after_qty INTEGER)",
  );
  await client.execute(
    "CREATE TRIGGER stock_changed AFTER UPDATE ON stock BEGIN INSERT INTO stock_audit VALUES(NEW.id, OLD.qty, NEW.qty); END",
  );
  await client.insertBatch("stock", [{ id: 1, qty: 100 }]);
}

/** The application owns the stable operation ID, payload check, and explicit conflict retry. */
export async function recordSale(
  id: string,
  total = 1000,
): Promise<"recorded" | "already-recorded"> {
  for (let attempt = 0; ; attempt += 1) {
    try {
      const outcome = await client.write(async (tx) => {
        const intent = (
          await tx.query("SELECT sku, qty, total FROM checkout_intents WHERE id = ?", {
            params: [id],
          })
        ).rows[0];
        if (intent === undefined) throw new Error("Sale has no durable intent");
        if (intent.sku !== 1 || intent.qty !== 1 || intent.total !== total)
          throw new Error("Sale ID reused with different details");
        const existing = (
          await tx.query("SELECT sku, qty, total FROM sales WHERE id = ?", { params: [id] })
        ).rows[0];
        if (existing !== undefined) {
          if (existing.sku !== 1 || existing.qty !== 1 || existing.total !== total) {
            throw new Error("Sale ID reused with different details");
          }
          return "already-recorded" as const;
        }
        await tx.insertBatch("sales", [{ id, sku: 1, qty: 1, total }]);
        await tx.insertBatch("lines", [{ id, sale_id: id, sku: 1, qty: 1 }]);
        await tx.execute("UPDATE stock SET qty = qty - 1 WHERE id = 1");
        await tx.execute("UPDATE checkout_intents SET state = 'complete' WHERE id = ?", [id]);
        return "recorded" as const;
      });
      return outcome.result;
    } catch (error) {
      // Retry only a confirmed database conflict. Unknown outcomes require reopening and
      // reconciliation using this same sale ID; external payment work never runs in the scope.
      if (
        !(error instanceof Error) ||
        !["WriteConflictError", "UniqueConstraintError", "UniqueKeyConflictError"].includes(
          error.name,
        ) ||
        attempt >= 7
      )
        throw error;
    }
  }
}

export async function interruptSale(id: string, when: "before-commit" | "after-commit") {
  // Save the application intent separately, before dispatching the potentially lost commit.
  // It survives a device/browser restart even if the caller loses all in-memory state.
  await client.insertBatch("checkout_intents", [
    { id, sku: 1, qty: 1, total: 1000, state: "pending" },
  ]);
  fault = when;
  try {
    await recordSale(id);
    throw new Error("Expected the interrupted client to reject");
  } catch (error) {
    return { error: error instanceof Error ? error.name : String(error), faultObserved };
  } finally {
    await client.close({ terminateWorker: true }).catch(() => undefined);
  }
}

export async function state() {
  return client.snapshot(async (tx) => ({
    stock: (await tx.query("SELECT * FROM stock")).rows,
    sales: (await tx.query("SELECT * FROM sales ORDER BY id")).rows,
    lines: (await tx.query("SELECT * FROM lines ORDER BY id")).rows,
    intents: (await tx.query("SELECT * FROM checkout_intents ORDER BY id")).rows,
    audit: (await tx.query("SELECT * FROM stock_audit ORDER BY before_qty DESC")).rows,
  }));
}

export async function close(): Promise<void> {
  await client.close({ terminateWorker: true });
}
