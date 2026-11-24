/**
 * A read that could never be served beside what its own snapshot keeps from release is refused
 * typed (53200) at once, even when another snapshot keeps the same memory.
 *
 * Found by the admission-ordering council (2026-11-24, S9), pure SQL on main: two read-only
 * transactions open, then another connection drops two tables whose rows (about 8.5 MB) stay for
 * readers older than the drop. Each transaction then reads 19 MB at 28 MiB. Both kept the dropped
 * rows from release, but `ReaderRegistry.exclusiveBytes` counted a hold only for the oldest
 * reader, and only up to the next live reader: with two readers it counted 0 for both, so
 * neither read was refused, neither could be served, and both waited forever.
 *
 * Law: GOVERNOR-WAIT-POLICY point 5 (next/memory/README.md): a request that could never be served
 * beside what its statement keeps is refused typed (53200), never left waiting. A hold returns only
 * once every reader older than it closes (write/state.ts `holdUntilOlderClose`), so each of those
 * readers keeps all of it while it stays open (council R*, adopted 2026-11-24:
 * `ReaderRegistry.blockedBytes`).
 */
import { expect, it } from "vitest";
import { chunkReads } from "../exec/scan.js";
import { createLowerer } from "../lower/create.js";
import { Governor } from "../memory/governor.js";
import { ProtocolClient } from "../owner/client.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { Harness } from "../write/test-support.js";

const MiB = 1 << 20;

const describe = (error: unknown) => {
  const e = error as { name?: string; sqlState?: string; message?: string };
  return `${String(e.name)} ${e.sqlState ?? ""} ${String(e.message)}`;
};

async function world(capMiB: number, clients: number) {
  Governor.stallAfterMs = 5_000;
  const h = new Harness("strict", "memory");
  const governor = new Governor(capMiB * MiB);
  Object.defineProperty(h, "governor", { value: governor });
  await h.open();
  const engine = composeEngine({
    planner: createLowerer(),
    writePath: h.path,
    governor,
    spill: new MemorySpillService(),
    reads: chunkReads,
  });
  const server = new OwnerServer(engine, { ownerId: "shared-pins", epoch: 1, build: "test" });
  const connected: ProtocolClient[] = [];
  const ports: MessagePort[] = [];
  for (let k = 0; k < clients; k++) {
    const { port1, port2 } = new MessageChannel();
    ports.push(port1, port2);
    server.accept(port1);
    const client = new ProtocolClient();
    await client.connect(port2);
    connected.push(client);
  }
  const close = async () => {
    for (const client of connected) client.close();
    const stop = server.quiesce("closing");
    await stop.drained;
    stop.finish();
    for (const port of ports) port.close();
    await engine.close();
    await h.close();
    Governor.stallAfterMs = null;
  };
  return { h, governor, clients: connected, close };
}

it("refuses typed both reads of two read-only transactions sharing a drop, never waiting", async () => {
  const w = await world(28, 3);
  const [r1, r2, dropper] = w.clients as [ProtocolClient, ProtocolClient, ProtocolClient];
  try {
    await dropper.query("CREATE TABLE big (id integer PRIMARY KEY, v text)");
    await dropper.query("CREATE TABLE spare (id integer PRIMARY KEY)");
    for (let k = 1; k <= 8; k++)
      await dropper.query("INSERT INTO big VALUES ($1, $2)", [k, "b".repeat(3_000_000)]);
    await dropper.query("CREATE TABLE small (id integer PRIMARY KEY)");
    await dropper.query("INSERT INTO small VALUES (1)");
    await w.h.path.quiesce();
    await dropper.query("CREATE TABLE d1 (id integer PRIMARY KEY, v text)");
    await dropper.query("CREATE TABLE d2 (id integer PRIMARY KEY, v text)");
    for (let k = 1; k <= 4; k++)
      await dropper.query("INSERT INTO d1 VALUES ($1, $2)", [k, "d".repeat(1_500_000)]);
    for (let k = 1; k <= 2; k++)
      await dropper.query("INSERT INTO d2 VALUES ($1, $2)", [k, "d".repeat(1_500_000)]);
    // Both read-only transactions are older than the drop: they keep its rows from release.
    for (const reader of [r1, r2]) {
      await reader.query("BEGIN READ ONLY");
      await reader.query("SELECT id FROM small");
    }
    await dropper.query("DROP TABLE d1");
    await dropper.query("DROP TABLE d2");
    const reads = [r1, r2].map((reader) =>
      reader.query("SELECT id, v FROM big").then(() => "served", describe),
    );
    const outcomes = await Promise.all(reads);
    for (const outcome of outcomes)
      expect(outcome, "beside the drop both keep: refused typed, never waiting").toMatch(
        /^MemoryAdmissionError 53200 /u,
      );
    // Once both end, the drop's rows return and the read is served.
    for (const reader of [r1, r2]) await reader.query("ROLLBACK");
    await w.h.path.quiesce();
    expect(await r1.query("SELECT id, v FROM big").then(() => "served", describe)).toBe("served");
  } finally {
    await w.close();
  }
}, 60_000);
