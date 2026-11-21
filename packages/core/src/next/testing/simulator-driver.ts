/**
 * The interaction-plan simulator (testing/interaction-simulator.ts) over the rebuild engine:
 * every connection is its own client session of one public owner (the owner protocol, the native
 * write path and the executor), so the generated plans, shadow model and seeds that judge the
 * shipped engine judge next/ too. Node only for now: no storage faults, crashes or maintenance.
 */
import type { SimulatedConnection, SimulationDriver } from "../../testing/interaction-simulator.js";
import { ProtocolClient } from "../owner/client.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { listedGap } from "./next-parity-gaps.js";
import { publicOwner } from "./public-owner.test.fixture.js";

/** `?` placeholders (the simulator's dialect) as `$1, $2, ...`, outside quoted text. */
export function numberPlaceholders(sql: string): string {
  let out = "",
    n = 0,
    quote: string | null = null;
  for (const ch of sql) {
    if (quote !== null) {
      if (ch === quote) quote = null;
      out += ch;
    } else if (ch === "'" || ch === '"') {
      quote = ch;
      out += ch;
    } else if (ch === "?") out += `$${String(++n)}`;
    else out += ch;
  }
  return out;
}

export async function createNextDriver(
  options: { capBytes?: number; transactionIdleTimeoutMs?: number } = {},
): Promise<SimulationDriver & { close(): Promise<void> }> {
  const owner = await publicOwner(options);
  const clients = new Set<{
    protocol: ProtocolClient;
    db: MinnowDatabaseClient;
    port: MessagePort;
  }>();
  const connect = async () => {
    const channel = new MessageChannel();
    const protocol = new ProtocolClient();
    owner.server.accept(channel.port2);
    await protocol.connect(channel.port1);
    const db = new MinnowDatabaseClient(protocol, { ready: Promise.resolve() });
    const entry = { protocol, db, port: channel.port1 };
    clients.add(entry);
    return entry;
  };
  const disconnect = async (entry: {
    protocol: ProtocolClient;
    db: MinnowDatabaseClient;
    port: MessagePort;
  }) => {
    clients.delete(entry);
    await entry.db.close().catch(() => undefined);
    entry.protocol.close();
    entry.port.close();
  };
  return {
    listedGap: (error) => listedGap(error)?.id ?? null,
    open: async (): Promise<SimulatedConnection> => {
      let entry = await connect();
      return {
        execute: async (sql, params) => {
          const result = await entry.db.execute(numberPlaceholders(sql), params ?? []);
          return { kind: result.command.toLowerCase(), rowCount: result.affectedRows ?? 0 };
        },
        query: async (sql, params) => {
          const result = await entry.db.query(numberPlaceholders(sql), params ?? []);
          return { columns: result.columns.map((column) => column.name), rows: result.rows() };
        },
        reopen: async () => {
          await disconnect(entry);
          entry = await connect();
        },
      };
    },
    close: async () => {
      for (const entry of [...clients]) await disconnect(entry);
      await owner.close();
    },
  };
}
