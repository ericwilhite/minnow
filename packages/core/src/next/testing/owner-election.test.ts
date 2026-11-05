/**
 * Election, the broker, owner death and resends, step-down and version skew, in Node: contexts
 * are fake Web Lock contexts (fake-locks.ts), owners run in process on the stand-ins with an
 * in-memory "file" per database whose open handle is the fence, and every connection is a real
 * MessageChannel. The browser suite (browser-tests/next-owner.spec.ts) runs the same scenarios
 * across real tabs.
 */
import { afterEach, describe, expect, it } from "vitest";
import type { ChunkReads } from "../exec/scan.js";
import { Governor } from "../memory/governor.js";
import { Broker } from "../owner/broker.js";
import { ProtocolClient } from "../owner/client.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerConnector, type OwnerHandle, type StartOwner } from "../owner/connector.js";
import { hostLockName, type HostContext } from "../owner/election.js";
import { startOwnerHost, type OwnerEngineFactory, type OwnerHost } from "../owner/host.js";
import { PROTOCOL_VERSION, type StepDown } from "../owner/protocol.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { FakeLockRegistry, type FakeLockContext } from "./fake-locks.js";
import {
  StandinExecutor,
  StandinPlanner,
  StandinWritePath,
  standinRules,
} from "./owner-standins.js";

const cleanups: Array<() => void> = [];
afterEach(() => {
  for (const cleanup of cleanups.splice(0).reverse()) cleanup();
});

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

async function until(condition: () => boolean, ms = 3_000): Promise<void> {
  const end = Date.now() + ms;
  while (!condition()) {
    if (Date.now() > end) throw new Error("condition not reached");
    await sleep(2);
  }
}

interface World {
  readonly registry: FakeLockRegistry;
  readonly files: Map<string, string>;
  readonly broker: Broker;
  /** Owners started, newest last. */
  readonly owners: Array<{ host: OwnerHost | null; kill: () => void; context: FakeLockContext }>;
}

function world(): World {
  const registry = new FakeLockRegistry();
  return { registry, files: new Map(), broker: new Broker(registry.context()), owners: [] };
}

/** The engine on an in-memory file; the open "handle" is the fence (busy while open). */
function factory(world: World): OwnerEngineFactory<{ readonly db: string }> {
  const open = new Set<string>();
  return {
    openFence(db) {
      if (open.has(db)) {
        const busy = new Error("busy");
        busy.name = "NoModificationAllowedError";
        throw busy;
      }
      open.add(db);
      return { db };
    },
    closeFence(fence) {
      open.delete(fence.db);
    },
    open(fence, settings) {
      const onSave = settings.onSave as ((text: string) => void) | undefined;
      const writePath = new StandinWritePath({
        durability: settings.durability === "relaxed" ? "relaxed" : "strict",
        flushDelayMs: typeof settings.flushDelayMs === "number" ? settings.flushDelayMs : 0,
        ...(typeof settings.flushHeld === "function"
          ? { flushHeld: settings.flushHeld as () => boolean }
          : {}),
        persistence: {
          load: () => world.files.get(fence.db) ?? null,
          save: (text) => {
            world.files.set(fence.db, text);
            onSave?.(text);
          },
        },
      });
      return composeEngine({
        planner: new StandinPlanner(),
        executor: new StandinExecutor(),
        writePath,
        governor: new Governor(64 * 1024 * 1024),
        spill: new MemorySpillService(),
        reads: {} as ChunkReads,
        rules: standinRules,
      });
    },
  };
}

/**
 * Starts owners in process. Each owner gets its own lock context (its worker); `kill` drops its
 * locks and closes every port it serves without a word, as a terminated worker does.
 */
function inProcessOwner(world: World, engines = factory(world)): StartOwner {
  return (init) => {
    const context = world.registry.context();
    const ports: MessagePort[] = [init.clientPort];
    let resolveStopped: (reason: StepDown["reason"]) => void = () => undefined;
    const stopped = new Promise<StepDown["reason"]>((resolve) => {
      resolveStopped = resolve;
    });
    const entry: World["owners"][number] = { host: null, context, kill: () => undefined };
    const started = startOwnerHost({ ...init, t: "minnow:host" }, engines, {
      locks: context,
      post: (status) => {
        if (status.t === "minnow:stopped") resolveStopped(status.reason);
      },
    }).then((host) => {
      entry.host = host;
      const accept = host.server.accept.bind(host.server);
      host.server.accept = (port) => {
        ports.push(port as MessagePort);
        accept(port);
      };
      return host;
    });
    started.catch(() => undefined);
    let dead = false;
    entry.kill = () => {
      if (dead) return;
      dead = true;
      // Its ports go silent first, so nothing it does while dying reaches a client.
      for (const port of ports) {
        port.onmessage = null;
        port.close();
      }
      // The platform frees a dead worker's handles and locks.
      void entry.host?.stop("closing", false);
      context.kill();
      resolveStopped("closing");
    };
    world.owners.push(entry);
    cleanups.push(() => {
      void entry.host?.stop("closing", false);
      entry.kill();
    });
    const handle: OwnerHandle = {
      ready: started.then((host) => host.ownerId),
      stopped,
      lifecycle: (event) => {
        void started.then((host) => {
          host.lifecycle(event, { mobile: true });
        });
      },
      terminate: () => {
        void started.then((host) => host.stop("pagehide", false));
        entry.kill();
      },
    };
    return handle;
  };
}

interface Tab {
  readonly client: ProtocolClient;
  readonly connector: OwnerConnector;
  readonly locks: FakeLockContext;
  readonly context: { visible: boolean; superseded: boolean };
}

function tab(
  world: World,
  options: {
    readonly settings?: Readonly<Record<string, unknown>>;
    readonly protocol?: number;
    readonly mobile?: boolean;
    readonly onSuperseded?: () => void;
  } = {},
): Tab {
  const locks = world.registry.context();
  const client = new ProtocolClient(
    options.protocol === undefined ? {} : { protocol: options.protocol },
  );
  const state = { visible: true, superseded: false };
  const connector = new OwnerConnector({
    database: "db",
    client,
    locks,
    openBroker: () => {
      const { port1, port2 } = new MessageChannel();
      world.broker.accept(port1);
      cleanups.push(() => {
        port1.close();
        port2.close();
      });
      return port2;
    },
    startOwner: inProcessOwner(world),
    context: (): HostContext => ({
      kind: "page",
      visible: state.visible,
      platform: { mobile: options.mobile ?? false },
      superseded: state.superseded,
    }),
    settings: options.settings ?? {},
    retryMs: 50,
    onSuperseded: () => {
      state.superseded = true;
      options.onSuperseded?.();
    },
  });
  cleanups.push(() => {
    connector.close();
  });
  return { client, connector, locks, context: state };
}

/** A tab dies: its page's locks and its owner worker go, and none of its code runs again. */
function killTab(world: World, victim: Tab, owner: number | null): void {
  victim.locks.kill();
  if (owner !== null) world.owners[owner]?.kill();
  victim.connector.close();
}

const value = async (client: ProtocolClient): Promise<unknown> =>
  (await client.query("SELECT * FROM t WHERE id = $1", [1])).rows()[0]?.[1];

describe("election and the broker", () => {
  it("elects one host; another context reaches it through the broker", async () => {
    const w = world();
    const a = tab(w);
    await a.connector.start();
    expect(a.connector.hosting).toBe(true);
    expect(w.registry.held(hostLockName("db"))).toBe(true);
    const b = tab(w);
    await b.connector.start();
    expect(b.connector.hosting).toBe(false);
    await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await b.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 5, "x"]);
    expect(await value(a.client)).toBe(5);
    expect(w.owners).toHaveLength(1);
  });

  it("answers noOwner for a dead registration and unreachable for an unregistered live host", async () => {
    const w = world();
    const a = tab(w);
    await a.connector.start();
    const ask = (nonce: number) =>
      new Promise<unknown>((resolve) => {
        const { port1, port2 } = new MessageChannel();
        w.broker.accept(port1);
        port2.onmessage = (event: MessageEvent) => {
          resolve((event.data as { t: string }).t);
          port1.close();
          port2.close();
        };
        port2.postMessage({ t: "connect", v: 1, db: "other", clientId: "x", nonce });
      });
    expect(await ask(1)).toBe("noOwner");
    const held = w.registry.context();
    void held.request(hostLockName("other"), {}, () => new Promise<void>(() => undefined));
    await sleep(1);
    expect(await ask(2)).toBe("unreachable");
    held.kill();
  });
});

describe("the broker's liveness check", () => {
  it("never hands out a registration whose holder no longer holds the host lock", async () => {
    const w = world();
    const ownerSide = new MessageChannel();
    w.broker.accept(ownerSide.port1);
    const owner = w.registry.context();
    let releaseOwner: () => void = () => undefined;
    void owner.request(
      hostLockName("db"),
      {},
      () =>
        new Promise<void>((resolve) => {
          releaseOwner = resolve;
        }),
    );
    await sleep(1);
    ownerSide.port2.postMessage({
      t: "register",
      v: 1,
      db: "db",
      ownerId: "old",
      protocol: PROTOCOL_VERSION,
      build: "",
      lockHolder: owner.id,
    });
    await sleep(1);
    // The owner dies; a successor holds the host lock but has not registered yet.
    releaseOwner();
    const successor = w.registry.context();
    void successor.request(hostLockName("db"), {}, () => new Promise<void>(() => undefined));
    await sleep(1);
    const answer = await new Promise<string>((resolve) => {
      const { port1, port2 } = new MessageChannel();
      w.broker.accept(port1);
      port2.onmessage = (event: MessageEvent) => {
        resolve((event.data as { t: string }).t);
        port1.close();
        port2.close();
      };
      port2.postMessage({ t: "connect", v: 1, db: "db", clientId: "c", nonce: 1 });
    });
    expect(answer).toBe("unreachable");
    successor.kill();
    owner.kill();
    ownerSide.port1.close();
    ownerSide.port2.close();
  });
});

describe("owner death and resends", () => {
  it("replaces a killed host with a waiting client, which resends its in-flight write exactly once", async () => {
    const w = world();
    const gate = { held: false };
    const a = tab(w, { settings: { flushHeld: () => gate.held } });
    await a.connector.start();
    await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await a.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
    const b = tab(w);
    await b.connector.start();
    gate.held = true;
    const update = b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    await sleep(20); // applied in A's memory; its flush is held, so it never becomes durable
    // A's tab dies: its page and its owner worker, without a word.
    killTab(w, a, 0);
    // B saw the host lock free, elected itself, and resent the UPDATE to its own owner, which
    // recovered A's durable state (without the lost commit) and ran it once.
    const result = await update;
    expect(result.summary).toMatchObject({ command: "UPDATE", counts: { t: 1 } });
    expect(b.connector.hosting).toBe(true);
    expect(await value(b.client)).toBe(1);
  });

  it("answers a COMMIT whose reply died with its owner from the next owner's ledger", async () => {
    const w = world();
    const killOnSave = { armed: false };
    const a: Tab = tab(w, {
      settings: {
        onSave: () => {
          // The commit is durable (its frame and ledger outcome saved): the owner dies before its
          // reply leaves.
          if (!killOnSave.armed) return;
          killOnSave.armed = false;
          killTab(w, a, 0);
        },
      },
    });
    await a.connector.start();
    await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await a.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
    const b = tab(w);
    await b.connector.start();
    await b.client.begin(1);
    await b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session: 1 });
    killOnSave.armed = true;
    const result = await b.client.commit(1);
    expect(result.summary).toMatchObject({ replayed: true, counts: { t: 1 } });
    expect(w.owners).toHaveLength(2);
    expect(await value(b.client)).toBe(1);
  });

  it("rolls back the transaction of a client whose context died", async () => {
    const w = world();
    const a = tab(w);
    await a.connector.start();
    await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await a.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
    const b = tab(w);
    await b.connector.start();
    await b.client.begin(1);
    await b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session: 1 });
    await sleep(10); // B holds and declared its liveness lock
    // The owner watches the lock B really holds: B's transaction is alive and keeps working.
    await b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session: 1 });
    expect(w.registry.query().pending.map((lock) => lock.name)).toContain(
      `minnow:db:client:${b.client.clientId}`,
    );
    killTab(w, b, null);
    // The writer slot is free again: A's write commits, and B's uncommitted one is gone.
    await a.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    expect(await value(a.client)).toBe(1);
  });
});

describe("step-down and version skew", () => {
  it("hands over on a graceful step-down (hidden on mobile)", async () => {
    const w = world();
    const a = tab(w, { mobile: true });
    await a.connector.start();
    await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await a.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 7, "x"]);
    const b = tab(w);
    await b.connector.start();
    a.context.visible = false;
    a.connector.lifecycle("hidden");
    await until(() => b.connector.hosting, 5_000);
    expect(await value(b.client)).toBe(7);
    // The hidden page still reaches the database through the new owner.
    expect(await value(a.client)).toBe(7);
    expect(a.connector.hosting).toBe(false);
  });

  it("lets a newer build supersede an older owner, and supersedes an older client", async () => {
    const w = world();
    let olderSuperseded = 0;
    const older = tab(w, {
      onSuperseded: () => {
        olderSuperseded++;
      },
    });
    await older.connector.start();
    await older.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    const newer = tab(w, { protocol: PROTOCOL_VERSION + 1 });
    await newer.connector.start();
    expect(newer.connector.hosting).toBe(true);
    await until(() => olderSuperseded === 1);
    expect(older.connector.status).toBe("superseded");
    // The data survived the hand-over to the newer build.
    expect((await newer.client.query("SELECT * FROM t")).rows()).toEqual([]);
    let lateSuperseded = 0;
    const late = tab(w, {
      onSuperseded: () => {
        lateSuperseded++;
      },
    });
    await late.connector.start();
    expect(lateSuperseded).toBe(1);
    expect(late.connector.status).toBe("superseded");
  });
});
