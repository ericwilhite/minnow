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
import { LIVENESS_IDLE_RELEASE_MS, hostLockName, type HostContext } from "../owner/election.js";
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

describe("review of increment 2 (B5-p2b-review-1)", () => {
  const seedTable = async (client: ProtocolClient) => {
    await client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
  };

  it("1: a transaction lost with its owner never falls through to autocommit", async () => {
    const w = world();
    const a = tab(w);
    await a.connector.start();
    await seedTable(a.client);
    const b = tab(w);
    await b.connector.start();
    await b.client.begin(1);
    await b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session: 1 });
    killTab(w, a, 0);
    // Session 0 reconnects b (electing it).
    await b.client.query("SELECT * FROM t");
    const update = () =>
      b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session: 1 }).then(
        (result) => result.summary.transaction,
        (error: unknown) => (error as Error).name,
      );
    expect(await update()).toBe("SessionLostError");
    // Every statement, COMMIT included, until the session is ended explicitly.
    expect(await update()).toBe("SessionLostError");
    expect(
      await b.client.query("COMMIT", [], { session: 1 }).then(
        () => "committed",
        (error: unknown) => (error as Error).name,
      ),
    ).toBe("SessionLostError");
    expect(await value(b.client)).toBe(0);
    await b.client.rollback(1);
    expect(await update()).toBe("idle");
    expect(await value(b.client)).toBe(1);
  });

  it("1: the same holds for session 0 after BEGIN, and for a statement in flight at the loss", async () => {
    const w = world();
    const a = tab(w);
    await a.connector.start();
    await seedTable(a.client);
    const b = tab(w);
    await b.connector.start();
    await b.client.query("BEGIN");
    await b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    const host = w.owners[0]?.host;
    if (host === undefined || host === null) throw new Error("no owner");
    // Swallowed by the dying owner: in flight at the loss.
    host.server.accept = () => undefined;
    killTab(w, a, 0);
    const inFlight = b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]).then(
      () => "ran",
      (error: unknown) => (error as Error).name,
    );
    expect(await inFlight).toBe("SessionLostError");
    expect(
      await b.client.query("SELECT * FROM t").then(
        () => "ran",
        (error: unknown) => (error as Error).name,
      ),
    ).toBe("SessionLostError");
    expect(b.client.lostSessions.has(0)).toBe(true);
    expect((await b.client.query("ROLLBACK")).summary.transaction).toBe("idle");
    expect(await value(b.client)).toBe(0);
  });

  it("2: a fence stolen while the engine opens fails startup and tears everything down", async () => {
    const w = world();
    const base = factory(w);
    let unblock: (() => void) | undefined;
    const channel = new MessageChannel();
    let closes = 0;
    const posted: string[] = [];
    const locks = w.registry.context();
    const calls: string[] = [];
    const request = locks.request.bind(locks);
    locks.request = ((name: string, options: object, callback: () => Promise<void>) => {
      calls.push(name);
      return request(name, options, callback);
    }) as typeof locks.request;
    const starting = startOwnerHost(
      {
        t: "minnow:host",
        database: "steal",
        build: "",
        clientPort: channel.port2,
        brokerPort: null,
        settings: {},
      },
      {
        ...base,
        closeFence(fence) {
          closes++;
          base.closeFence(fence);
        },
        async open(fence, settings) {
          await new Promise<void>((resolve) => {
            unblock = resolve;
          });
          return base.open(fence, settings);
        },
      },
      {
        locks,
        post: (status) => {
          posted.push(status.t);
        },
      },
    );
    await until(() => unblock !== undefined);
    const thief = w.registry.context();
    void thief.request("minnow:steal", { steal: true }, () => new Promise<void>(() => undefined));
    await sleep(5);
    unblock?.();
    await expect(starting).rejects.toMatchObject({ name: "OwnershipLostError" });
    // Checked right after the open: the host lock was never even requested.
    expect(calls).toEqual(["minnow:steal"]);
    expect(closes).toBe(1);
    expect(posted).toEqual(["minnow:failed"]);
    // Never took the host lock, never served the page's client.
    expect(w.registry.held(hostLockName("steal"))).toBe(false);
    thief.kill();
    channel.port1.close();
  });

  it("2: a fence stolen during the host-lock steal fails startup and releases the host lock", async () => {
    const w = world();
    const channel = new MessageChannel();
    const locks = w.registry.context();
    const posted: string[] = [];
    const original = locks.request.bind(locks);
    const thief = w.registry.context();
    let queries = 0;
    const query = locks.query.bind(locks);
    locks.query = () => {
      queries++;
      return query();
    };
    // The host-lock request completes only after the fence was stolen.
    locks.request = ((name: string, options: object, callback: () => Promise<void>) => {
      if (name === hostLockName("steal2"))
        void thief.request(
          "minnow:steal2",
          { steal: true },
          () => new Promise<void>(() => undefined),
        );
      return original(name, options, callback);
    }) as typeof locks.request;
    await expect(
      startOwnerHost(
        {
          t: "minnow:host",
          database: "steal2",
          build: "",
          clientPort: channel.port2,
          brokerPort: null,
          settings: {},
        },
        factory(w),
        { locks, post: (status) => posted.push(status.t) },
      ),
    ).rejects.toMatchObject({ name: "OwnershipLostError" });
    // Checked right after the steal: startup went no further.
    expect(queries).toBe(0);
    expect(posted).toEqual(["minnow:failed"]);
    expect(w.registry.held(hostLockName("steal2"))).toBe(false);
    thief.kill();
    channel.port1.close();
  });

  it("3: an owner dying during the handshake does not hang the connector", async () => {
    const w = world();
    const a = tab(w);
    await a.connector.start();
    const host = w.owners[0]?.host;
    if (host === undefined || host === null) throw new Error("no owner");
    host.server.accept = (port) => {
      (port as MessagePort).close();
      w.owners[0]?.kill();
    };
    const b = tab(w);
    const result = await Promise.race([
      b.connector.start().then(() => "connected"),
      sleep(1_000).then(() => "hung"),
    ]);
    expect(result).toBe("connected");
    // A successor was elected (a's page or b) and serves b.
    expect(a.connector.hosting || b.connector.hosting).toBe(true);
    expect(await b.client.query("SHOW timezone").then(() => "served")).toBe("served");
  });

  it("3: an owner that never answers the handshake is abandoned after the bound", async () => {
    const locks = new FakeLockRegistry();
    const context = locks.context();
    // Someone holds the host lock but never answers.
    void locks
      .context()
      .request(hostLockName("silent"), {}, () => new Promise<void>(() => undefined));
    const client = new ProtocolClient();
    let asks = 0;
    const connector = new OwnerConnector({
      database: "silent",
      client,
      locks: context,
      openBroker: () => {
        const { port1, port2 } = new MessageChannel();
        port1.onmessage = (event: MessageEvent) => {
          const message = event.data as { t: string; nonce: number };
          if (message.t !== "connect") return;
          asks++;
          const silent = new MessageChannel();
          cleanups.push(() => {
            silent.port1.close();
          });
          port1.postMessage({ t: "port", nonce: message.nonce, ownerId: "x", port: silent.port2 }, [
            silent.port2,
          ]);
        };
        cleanups.push(() => {
          port1.close();
        });
        return port2;
      },
      startOwner: null,
      context: () => ({
        kind: "page",
        visible: true,
        platform: { mobile: false },
        superseded: false,
      }),
      handshakeTimeoutMs: 30,
      retryMs: 10,
    });
    void connector.start().catch(() => undefined);
    await until(() => asks >= 3, 1_000);
    connector.close();
  });

  it("4: liveness is re-announced to a replacement owner, which then watches it", async () => {
    const w = world();
    const a = tab(w);
    await a.connector.start();
    await seedTable(a.client);
    const b = tab(w);
    await b.connector.start();
    await b.client.query("BEGIN");
    await b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    await sleep(20);
    killTab(w, a, 0);
    await b.client.query("SELECT * FROM t").catch(() => undefined);
    // The lost session must be ended before it is used again (finding 1's ruling).
    await b.client.query("ROLLBACK");
    await b.client.query("BEGIN");
    await b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    const c = tab(w);
    await c.connector.start();
    // b's page dies without a word; the owner (b's own worker survives in this world) must see
    // b's liveness lock go and roll its transaction back.
    b.locks.kill();
    const update = c.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    const outcome = await Promise.race([update.then(() => "done"), sleep(500).then(() => "hung")]);
    expect(outcome).toBe("done");
  });

  it("5: closing while the owner starts terminates it and releases the host lock", async () => {
    const w = world();
    const client = new ProtocolClient();
    let ready: ((id: string) => void) | undefined;
    let terminated = 0;
    const connector = new OwnerConnector({
      database: "late",
      client,
      locks: w.registry.context(),
      openBroker: null,
      context: () => ({
        kind: "page",
        visible: true,
        platform: { mobile: false },
        superseded: false,
      }),
      startOwner: () => ({
        ready: new Promise((resolve) => {
          ready = resolve;
        }),
        stopped: new Promise(() => undefined),
        lifecycle() {
          return undefined;
        },
        terminate() {
          terminated++;
        },
      }),
    });
    const starting = connector.start().then(
      () => "started",
      (error: unknown) => (error as Error).name,
    );
    await until(() => ready !== undefined);
    connector.close();
    // Terminated at close, not when (if ever) it would have become ready; and start() settles at
    // close although this owner never reports ready (review 2, finding 4).
    expect(terminated).toBe(1);
    expect(await Promise.race([starting, sleep(200).then(() => "hung")])).toBe("ClientClosedError");
    await sleep(1);
    expect(w.registry.held(hostLockName("late"))).toBe(false);
    expect({ terminated, hosting: connector.hosting }).toEqual({ terminated: 1, hosting: false });
  });

  it("5: closing while queued for the host lock withdraws the request", async () => {
    const w = world();
    const holder = w.registry.context();
    let release: (() => void) | undefined;
    void holder.request(
      hostLockName("queued"),
      {},
      () =>
        new Promise<void>((resolve) => {
          release = resolve;
        }),
    );
    let started = 0;
    const connector = new OwnerConnector({
      database: "queued",
      client: new ProtocolClient(),
      locks: w.registry.context(),
      openBroker: null,
      context: () => ({
        kind: "page",
        visible: true,
        platform: { mobile: false },
        superseded: false,
      }),
      startOwner: () => {
        started++;
        return {
          ready: new Promise(() => undefined),
          stopped: new Promise(() => undefined),
          lifecycle: () => undefined,
          terminate: () => undefined,
        };
      },
    });
    const starting = connector.start();
    starting.catch(() => undefined);
    await sleep(10);
    const queued = () =>
      w.registry.query().pending.filter((lock) => lock.name === hostLockName("queued"));
    expect(queued()).toHaveLength(1);
    connector.close();
    await sleep(1);
    // Withdrawn at once, not left to be granted (and released) later.
    expect(queued()).toHaveLength(0);
    release?.();
    await expect(starting).rejects.toMatchObject({ name: "ClientClosedError" });
    await sleep(10);
    expect(started).toBe(0);
    expect(w.registry.held(hostLockName("queued"))).toBe(false);
  });

  it("7: a liveness grant that arrives after the client went idle is released after the hysteresis", async () => {
    const w = world();
    const a = tab(w);
    await a.connector.start();
    await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    const b = tab(w);
    await b.connector.start();
    const blocker = w.registry.context();
    let release: (() => void) | undefined;
    const name = `minnow:db:client:${b.client.clientId}`;
    void blocker.request(
      name,
      {},
      () =>
        new Promise<void>((resolve) => {
          release = resolve;
        }),
    );
    await b.client.query("SELECT * FROM t");
    release?.();
    await sleep(LIVENESS_IDLE_RELEASE_MS + 200);
    expect(w.registry.held(name)).toBe(false);
    blocker.kill();
  });

  it("8: the broker keeps no port of a context that never asked, and forgets askers on registration", async () => {
    const broker = new Broker({ query: () => Promise.resolve({ held: [] }) });
    let broadcasts = 0;
    for (let i = 0; i < 100; i++) {
      const port = {
        onmessage: null,
        postMessage: () => {
          broadcasts++;
        },
        close() {
          return undefined;
        },
      } as unknown as MessagePort;
      broker.accept(port);
      port.close();
    }
    const asker = {
      onmessage: null as ((event: MessageEvent) => void) | null,
      postMessage: () => {
        broadcasts++;
      },
    };
    broker.accept(asker as unknown as MessagePort);
    asker.onmessage?.({
      data: { t: "connect", v: 1, db: "db", clientId: "c", nonce: 1 },
    } as MessageEvent);
    await sleep(1);
    expect(broker.heldPorts).toBe(1);
    broadcasts = 0;
    const owner = {
      onmessage: null as ((event: MessageEvent) => void) | null,
      postMessage: () => undefined,
    };
    broker.accept(owner as unknown as MessagePort);
    const register = {
      t: "register",
      v: 1,
      db: "db",
      ownerId: "o",
      protocol: 1,
      build: "",
      lockHolder: null,
    };
    owner.onmessage?.({ data: register } as MessageEvent);
    await sleep(1);
    // Only the asker heard, once; then it is forgotten.
    expect(broadcasts).toBe(1);
    expect(broker.heldPorts).toBe(0);
    owner.onmessage?.({ data: { ...register, ownerId: "o2" } } as MessageEvent);
    await sleep(1);
    expect(broadcasts).toBe(1);
  });

  it("8: the broker forgets a port when the platform reports it closed", async () => {
    const broker = new Broker({ query: () => Promise.resolve({ held: [] }) });
    const { port1, port2 } = new MessageChannel();
    broker.accept(port1);
    port2.postMessage({ t: "connect", v: 1, db: "db", clientId: "c", nonce: 1 });
    await until(() => broker.heldPorts === 1);
    port2.close();
    await until(() => broker.heldPorts === 0);
  });

  it("1: the client never resends a lost session's in-flight statement (only its COMMIT)", async () => {
    const sent: Array<Record<string, unknown>> = [];
    const fake = () => {
      const port = {
        onmessage: null as ((event: { data: unknown }) => void) | null,
        close: () => undefined,
        postMessage(message: Record<string, unknown>) {
          sent.push(message);
          if (message.t === "hello")
            queueMicrotask(() =>
              port.onmessage?.({
                data: { t: "welcome", ownerId: "o", protocol: PROTOCOL_VERSION },
              }),
            );
        },
      };
      return port;
    };
    const client = new ProtocolClient();
    const first = fake();
    await client.connect(first as unknown as MessagePort);
    const begin = client.begin(1);
    const beginSeq = sent.at(-1)?.seq as number;
    first.onmessage?.({
      data: {
        t: "done",
        seq: beginSeq,
        columns: [],
        frames: [],
        summary: {
          command: "BEGIN",
          rowCount: null,
          counts: null,
          lsn: null,
          replayed: false,
          closedEarly: false,
          transaction: "inTransaction",
          catalogVersion: 1,
        },
      },
    });
    await begin;
    const update = client.query("UPDATE t SET n = 1", [], { session: 1 });
    const updateSeq = sent.at(-1)?.seq as number;
    const commit = client.commit(1);
    const commitSeq = sent.at(-1)?.seq as number;
    sent.length = 0;
    await client.reconnect(fake() as unknown as MessagePort);
    const resent = sent.filter((message) => message.retry === true).map((message) => message.seq);
    expect(resent).not.toContain(updateSeq);
    expect(resent).toEqual([commitSeq]);
    expect(
      await Promise.race([
        update.then(
          () => "ran",
          (error: unknown) => (error as { lost?: string }).lost,
        ),
        sleep(50).then(() => "pending"),
      ]),
    ).toBe("transaction");
    client.close();
    await commit.catch(() => undefined);
  });

  it("6: freeze and pagehide terminate without calling back into the closing page", async () => {
    const w = world();
    let closing = false;
    let terminated = 0;
    const start = inProcessOwner(w);
    const connector = new OwnerConnector({
      database: "db",
      client: new ProtocolClient(),
      locks: w.registry.context(),
      openBroker: null,
      startOwner: (init) => {
        const handle = start(init);
        return {
          ...handle,
          terminate: () => {
            terminated++;
            handle.terminate();
          },
        };
      },
      context: () => {
        // Firefox can abandon a closing page's handler inside a call like this one.
        if (closing) throw new Error("abandoned");
        return { kind: "page", visible: true, platform: { mobile: false }, superseded: false };
      },
    });
    cleanups.push(() => {
      connector.close();
    });
    await connector.start();
    expect(connector.hosting).toBe(true);
    closing = true;
    connector.lifecycle("pagehide");
    expect({ terminated, hosting: connector.hosting }).toEqual({ terminated: 1, hosting: false });
  });
});

describe("review 2 of increment 2 (B5-p2b-review-2)", () => {
  it("2: a prepared COMMIT whose reply died with a durable commit is answered from the ledger", async () => {
    const w = world();
    const killOnSave = { armed: false };
    const a: Tab = tab(w, {
      settings: {
        onSave: () => {
          // The commit is durable (its frame and ledger outcome saved): the owner dies before
          // its reply leaves.
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
    const prepared = await b.client.prepare("COMMIT");
    await b.client.begin(1);
    await b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session: 1 });
    killOnSave.armed = true;
    const result = await b.client.execute(prepared.id, [], 1).then(
      (answer) => answer.summary.replayed,
      (error: unknown) => (error as Error).name,
    );
    expect({ result, stored: await value(b.client) }).toEqual({ result: true, stored: 1 });
    // A successor owner answered it, and the session is usable again.
    expect(w.owners).toHaveLength(2);
    expect(b.client.lostSessions.has(1)).toBe(false);
  });
});
