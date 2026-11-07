/**
 * Election, the broker, owner death and resends, step-down and version skew, in Node: contexts
 * are fake Web Lock contexts (fake-locks.ts), owners run in process on the stand-ins with an
 * in-memory "file" per database whose open handle is the fence, and every connection is a real
 * MessageChannel. The browser suite (browser-tests/next-owner.spec.ts) runs the same scenarios
 * across real tabs.
 */
import fc from "fast-check";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  BARRIER_RESOLUTION_STATES,
  BROKER_RESTART_STATES,
  OWNER_DISCOVERY_STATES,
  BROKER_REGISTRATION_STATES,
  LEDGER_RESOLUTION_STATES,
  createReach,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { ChunkReads } from "../exec/scan.js";
import { Governor } from "../memory/governor.js";
import { Broker } from "../owner/broker.js";
import { ProtocolClient } from "../owner/client.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerConnector, type OwnerHandle, type StartOwner } from "../owner/connector.js";
import {
  LIVENESS_IDLE_RELEASE_MS,
  hostLockName,
  skewChannelName,
  type HostContext,
} from "../owner/election.js";
import { startOwnerHost, type OwnerEngineFactory, type OwnerHost } from "../owner/host.js";
import {
  ALIVE_INTERVAL_MS,
  PROTOCOL_VERSION,
  SILENCE_DEADLINE_MS,
  SILENT_PROBES_LIMIT,
  type StepDown,
} from "../owner/protocol.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { FakeLockRegistry, type FakeLockContext } from "./fake-locks.js";
import { harness as ownerHarness } from "./owner-harness.js";
import { RestartingBroker } from "./restarting-broker.js";
import { createLowerer } from "../lower/create.js";
import { StandinExecutor, StandinWritePath, withTestStatements } from "./owner-standins.js";

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
  /** The last fence epoch handed out per database. */
  readonly epochs: Map<string, number>;
  /** Discovery requests (`connect` to the broker) per client. */
  readonly connects: Map<ProtocolClient, number>;
  /** The broker SharedWorker; it can be replaced under its ports (restarting-broker.ts). */
  readonly broker: RestartingBroker;
  /** Ports each client opened to the broker. */
  readonly brokerPorts: Map<ProtocolClient, number>;
  /** Owners started, newest last. */
  readonly owners: Array<{ host: OwnerHost | null; kill: () => void; context: FakeLockContext }>;
}

function world(options: { readonly brokerQueryDelayMs?: number } = {}): World {
  const registry = new FakeLockRegistry();
  const locks = registry.context();
  const delay = options.brokerQueryDelayMs ?? 0;
  return {
    registry,
    files: new Map(),
    epochs: new Map(),
    connects: new Map(),
    broker: new RestartingBroker(
      delay === 0 ? locks : { query: () => sleep(delay).then(() => locks.query()) },
    ),
    brokerPorts: new Map(),
    owners: [],
  };
}

/** The engine on an in-memory file; the open "handle" is the fence (busy while open). */
function factory(
  world: World,
): OwnerEngineFactory<{ readonly db: string; readonly epoch: number }> {
  /** The open fence per database: a save through a closed one is a dead owner's, and is lost. */
  const open = new Map<string, { readonly db: string; readonly epoch: number }>();
  return {
    openFence(db) {
      if (open.has(db)) {
        const busy = new Error("busy");
        busy.name = "NoModificationAllowedError";
        throw busy;
      }
      // The fence epoch: strictly increasing per database across acquisitions (OwnerLease.epoch).
      const epoch = (world.epochs.get(db) ?? 0) + 1;
      world.epochs.set(db, epoch);
      const fence = { db, epoch };
      open.set(db, fence);
      return fence;
    },
    epochOf: (fence) => fence.epoch,
    closeFence(fence) {
      if (open.get(fence.db) === fence) open.delete(fence.db);
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
            // A killed owner's timers may still fire in process; its handles are closed.
            if (open.get(fence.db) !== fence) return;
            world.files.set(fence.db, text);
            onSave?.(text);
          },
        },
      });
      return composeEngine({
        planner: withTestStatements(createLowerer()),
        executor: new StandinExecutor(),
        writePath,
        governor: new Governor(64 * 1024 * 1024),
        spill: new MemorySpillService(),
        reads: {} as ChunkReads,
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
      ...(typeof init.settings.registrationTimeoutMs === "number"
        ? { registrationTimeoutMs: init.settings.registrationTimeoutMs }
        : {}),
      ...(typeof init.settings.registrationBoundMs === "number"
        ? { registrationBoundMs: init.settings.registrationBoundMs }
        : {}),
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
      // Graceful (DECISIONS.md "Step-down"): the owner drains, closes and releases, then dies.
      stepDown: async (reason) => {
        const host = await started.catch(() => null);
        if (host !== null) await host.stop(reason, true);
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
    readonly silenceDeadlineMs?: number;
    readonly handshakeTimeoutMs?: number;
    readonly announcements?: boolean;
    readonly brokerTimeoutMs?: number;
    readonly brokerBoundMs?: number;
    readonly retryMs?: number;
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
      world.brokerPorts.set(client, (world.brokerPorts.get(client) ?? 0) + 1);
      world.broker.accept(port1, port2);
      // Count discovery: every `connect` this tab asks the broker.
      const serve = port1.onmessage;
      port1.onmessage = (event: MessageEvent) => {
        if ((event.data as { t?: string }).t === "connect")
          world.connects.set(client, (world.connects.get(client) ?? 0) + 1);
        serve?.call(port1, event);
      };
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
    retryMs: options.retryMs ?? 50,
    ...(options.silenceDeadlineMs === undefined
      ? {}
      : { silenceDeadlineMs: options.silenceDeadlineMs }),
    ...(options.handshakeTimeoutMs === undefined
      ? {}
      : { handshakeTimeoutMs: options.handshakeTimeoutMs }),
    ...(options.announcements === undefined ? {} : { announcements: options.announcements }),
    ...(options.brokerTimeoutMs === undefined ? {} : { brokerTimeoutMs: options.brokerTimeoutMs }),
    ...(options.brokerBoundMs === undefined ? {} : { brokerBoundMs: options.brokerBoundMs }),
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
          // The instance announcement comes first on every port; the answer follows.
          if ((event.data as { t: string }).t === "instance") return;
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
        // The instance announcement comes first on every port; the answer follows.
        if ((event.data as { t: string }).t === "instance") return;
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
          return base.closeFence(fence);
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

  it("1: a lost session's in-flight requests all go back to the ledger, none resendable (review 3)", async () => {
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
    // Both are resent, in send order, and neither may run on the new owner: its ledger answers.
    const resent = sent.filter((message) => message.retry === true);
    expect(resent.map((message) => [message.seq, message.resendable])).toEqual([
      [updateSeq, false],
      [commitSeq, false],
    ]);
    client.close();
    await update.catch(() => undefined);
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
          // Stopping it gracefully counts too (p2e: the page steps its owner down).
          stepDown: (reason) => {
            terminated++;
            return handle.stepDown?.(reason) ?? Promise.resolve();
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

/**
 * A client connected straight to owner `index`'s server through a port whose replies can be
 * dropped from a given moment (a reply lost with its owner), and reconnected to another owner.
 */
function wiredClient(w: World, index: number) {
  const channel = new MessageChannel();
  const drop = { all: false, commitReplies: false };
  const serverPort = {
    onmessage: null as ((event: MessageEvent) => void) | null,
    postMessage(message: { t: string; summary?: { command?: string } }) {
      if (drop.all) return;
      if (drop.commitReplies && message.t === "done" && message.summary?.command === "COMMIT")
        return;
      channel.port1.postMessage(message);
    },
    close() {
      channel.port1.close();
    },
  };
  channel.port1.onmessage = (event) => serverPort.onmessage?.(event);
  const host = w.owners[index]?.host;
  if (host === undefined || host === null) throw new Error("no owner");
  host.server.accept(serverPort);
  const client = new ProtocolClient();
  cleanups.push(() => {
    client.close();
    channel.port1.close();
    channel.port2.close();
  });
  const reconnectTo = async (owner: number) => {
    const next = new MessageChannel();
    const target = w.owners[owner]?.host;
    if (target === undefined || target === null) throw new Error("no owner");
    target.server.accept(next.port1);
    cleanups.push(() => {
      next.port1.close();
      next.port2.close();
    });
    await client.reconnect(next.port2);
  };
  return { client, drop, connect: () => client.connect(channel.port2), reconnectTo };
}

describe("review 3 of increment 2 (B5-p2b-review-3): pending requests resolve through the ledger", () => {
  async function setup(settings: Readonly<Record<string, unknown>> = {}) {
    const w = world();
    const saves = { count: 0, killAt: Number.POSITIVE_INFINITY };
    const a: Tab = tab(w, {
      settings: {
        ...settings,
        onSave: () => {
          saves.count++;
          if (saves.count === saves.killAt) killTab(w, a, 0);
        },
      },
    });
    await a.connector.start();
    await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await a.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
    const wired = wiredClient(w, 0);
    await wired.connect();
    /** Kills owner 0 at the `n`-th save from now. */
    const killAtSave = (n: number) => {
      saves.killAt = saves.count + n;
    };
    /** A new tab elects a new owner over the same file; the wired client moves to it. */
    const failover = async () => {
      const d = tab(w);
      await d.connector.start();
      await wired.reconnectTo(w.owners.length - 1);
      return d;
    };
    return { w, a, wired, saves, killAtSave, failover };
  }

  it("1: a durable autocommit after an unanswered COMMIT is answered from the ledger (the review's probe)", async () => {
    const { wired, saves, killAtSave, failover } = await setup();
    const c = wired.client;
    await c.begin(0);
    await c.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    wired.drop.commitReplies = true;
    // The old owner commits, runs the next increment as autocommit, saves both, then dies.
    killAtSave(2);
    const commit = c.commit(0);
    const write = c.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    await until(() => saves.count >= saves.killAt);
    const d = await failover();
    const [committed, written] = await Promise.all([commit, write]);
    expect(committed.summary).toMatchObject({ command: "COMMIT", replayed: true });
    expect(written.summary).toMatchObject({ command: "UPDATE", replayed: true });
    expect(await value(d.client)).toBe(2);
    expect(c.lostSessions.has(0)).toBe(false);
  });

  it("1: when the COMMIT never became durable: it is none, and the statement after it unknown", async () => {
    let held = false;
    const { wired, failover, w } = await setup({ flushHeld: () => held });
    const c = wired.client;
    await c.begin(0);
    await c.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    wired.drop.all = true;
    held = true;
    const commit = c.commit(0).then(
      () => "ok",
      (error: unknown) => (error as { effect?: string }).effect,
    );
    const write = c.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]).then(
      () => "ok",
      (error: unknown) => (error as { effect?: string }).effect,
    );
    await sleep(30);
    // Applied, never durable: the owner dies with both.
    w.owners[0]?.kill();
    held = false;
    const d = await failover();
    expect(await commit).toBe("none");
    expect(await write).toBe("unknown");
    expect(await value(d.client)).toBe(0);
  });

  type Op = "begin" | "inc" | "commit" | "rollback";
  /** A valid session-0 script: BEGIN only when idle, COMMIT/ROLLBACK only inside a transaction. */
  const script = fc
    .array(fc.constantFrom<Op>("begin", "inc", "commit", "rollback"), {
      minLength: 1,
      maxLength: 7,
    })
    .map((ops) => {
      let open = false;
      const valid: Op[] = [];
      for (const op of ops) {
        if (op === "begin" && open) continue;
        if ((op === "commit" || op === "rollback") && !open) continue;
        if (op === "begin") open = true;
        if (op === "commit" || op === "rollback") open = false;
        valid.push(op);
      }
      return valid.length === 0 ? (["inc"] as Op[]) : valid;
    });

  const SEEDS = seedsFor("next-owner-ledger-resolution", [0x1ed6]);

  it.each(SEEDS)(
    "generated: every reported effect is consistent with the durable state (seed %i)",
    async (seed) => {
      const reach = createReach("next-owner-ledger-resolution", LEDGER_RESOLUTION_STATES, {
        minimum: 3,
      });
      await fc.assert(
        fc.asyncProperty(script, fc.integer({ min: 1, max: 4 }), async (ops, killAt) => {
          const entered = new Set<string>();
          const { wired, saves, killAtSave, failover, w } = await setup();
          const c = wired.client;
          wired.drop.all = true;
          killAtSave(killAt);
          const sql: Record<Op, string> = {
            begin: "BEGIN",
            inc: "UPDATE t SET n = n + 1 WHERE id = $1",
            commit: "COMMIT",
            rollback: "ROLLBACK",
          };
          const results = ops.map((op) =>
            c.query(sql[op], op === "inc" ? [1] : []).then(
              (result) => ({ ok: true, replayed: result.summary.replayed, effect: null }),
              (error: unknown) => ({
                ok: false,
                replayed: false,
                effect:
                  (error as { effect?: string }).effect ??
                  `error ${(error as Error).name}: ${(error as Error).message}`,
              }),
            ),
          );
          // Let the old owner run what it gets to, then make sure it is dead.
          await until(() => saves.count >= saves.killAt, 300).catch(() => undefined);
          w.owners[0]?.kill();
          const d = await failover();
          const outcomes = await Promise.all(results);
          const durable = (await value(d.client)) as number;
          // Bounds from what the client was told, in send order.
          let confirmed = 0;
          let unknown = 0;
          let open = false;
          let txIncs = 0;
          ops.forEach((op, i) => {
            const result = outcomes[i];
            if (result === undefined) return;
            if (result.replayed) entered.add(`${op} replayed`);
            if (!result.ok && result.effect === "none") entered.add(`${op} none`);
            if (!result.ok && result.effect === "unknown") entered.add(`${op} unknown`);
            if (op === "begin") {
              open = true;
              txIncs = 0;
            } else if (op === "inc") {
              if (open) txIncs++;
              else if (result.ok) confirmed++;
              else if (result.effect !== "none") unknown++;
            } else if (op === "commit") {
              if (result.ok) confirmed += txIncs;
              else if (result.effect !== "none") unknown += txIncs;
              open = false;
            } else {
              open = false;
            }
          });
          reach.recordHistory(entered);
          // Every request ends: answered, or refused with what is known about its effect.
          for (const result of outcomes)
            expect(result.ok || result.effect === "none" || result.effect === "unknown").toBe(true);
          // A "none" that took effect, or a lost "ok", breaks these.
          expect(durable).toBeGreaterThanOrEqual(confirmed);
          expect(durable).toBeLessThanOrEqual(confirmed + unknown);
          for (const cleanup of cleanups.splice(0).reverse()) cleanup();
        }),
        { seed, numRuns: 60 },
      );
      reach.assertReached();
    },
    120_000,
  );
});

describe("B1 (B5-p2d review 1): after a reconnect, a session's state is the resolution of its whole unanswered sequence", () => {
  type Op = "begin" | "inc" | "commit" | "rollback";
  interface Step {
    readonly session: 0 | 1;
    readonly op: Op;
  }
  /** Valid interleaved scripts for two sessions, with an optional acknowledged BEGIN first. */
  const scenario = fc
    .record({
      // At most one session open before the loss: the stand-in has one writer slot, and a second
      // acknowledged BEGIN would wait for it forever.
      openFirst: fc.constantFrom<[boolean, boolean]>([false, false], [true, false], [false, true]),
      steps: fc.array(
        fc.record({
          session: fc.constantFrom<0 | 1>(0, 1),
          // Weighted towards ends, so confirmed ends with nothing lost after them are common.
          op: fc.oneof(
            { weight: 3, arbitrary: fc.constant<Op>("begin") },
            { weight: 2, arbitrary: fc.constant<Op>("inc") },
            { weight: 4, arbitrary: fc.constant<Op>("commit") },
            { weight: 1, arbitrary: fc.constant<Op>("rollback") },
          ),
        }),
        { minLength: 1, maxLength: 8 },
      ),
      failover: fc.boolean(),
      settleMs: fc.integer({ min: 10, max: 40 }),
    })
    .map(({ openFirst, steps, failover, settleMs }) => {
      const open = [openFirst[0], openFirst[1]];
      const valid: Step[] = [];
      for (const step of steps) {
        if (step.op === "begin" && open[step.session]) continue;
        if ((step.op === "commit" || step.op === "rollback") && !open[step.session]) continue;
        if (step.op === "begin") open[step.session] = true;
        if (step.op === "commit" || step.op === "rollback") open[step.session] = false;
        valid.push(step);
      }
      return { openFirst, steps: valid, failover, settleMs };
    });

  const SEEDS = seedsFor("next-owner-barrier-resolution", [0xb1b1]);

  it.each(SEEDS)(
    "generated: a fresh request runs only if the last loss before it was discharged by a confirmed end (seed %i)",
    async (seed) => {
      const reach = createReach("next-owner-barrier-resolution", BARRIER_RESOLUTION_STATES, {
        minimum: 3,
      });
      await fc.assert(
        fc.asyncProperty(scenario, async ({ openFirst, steps, failover, settleMs }) => {
          const entered = new Set<string>();
          const w = world();
          const a: Tab = tab(w);
          await a.connector.start();
          await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
          await a.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
          const wired = wiredClient(w, 0);
          await wired.connect();
          const c = wired.client;
          // A session open at the loss has written: its COMMIT, if it lands, records an outcome
          // (a COMMIT of a transaction that wrote nothing commits nothing and records none).
          for (const session of [0, 1] as const)
            if (openFirst[session]) {
              await c.query("BEGIN", [], { session });
              await c.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session });
            }
          wired.drop.all = true;
          const sql: Record<Op, string> = {
            begin: "BEGIN",
            inc: "UPDATE t SET n = n + 1 WHERE id = $1",
            commit: "COMMIT",
            rollback: "ROLLBACK",
          };
          const sent = steps.map((step) => ({
            step,
            seq: 0,
            result: c.query(sql[step.op], step.op === "inc" ? [1] : [], { session: step.session }),
          }));
          await sleep(settleMs);
          if (failover) {
            w.owners[0]?.kill();
            const d = tab(w);
            await d.connector.start();
            await wired.reconnectTo(w.owners.length - 1);
          } else {
            await wired.reconnectTo(0);
          }
          const settled = Promise.all(
            sent.map(({ result }) =>
              result.then(
                (answer) => ({ ok: true, transaction: answer.summary.transaction }),
                () => ({ ok: false, transaction: null }),
              ),
            ),
          );
          const outcomes = await Promise.race([settled, sleep(3_000).then(() => null)]);
          if (outcomes === null) throw new Error("pipeline outcomes did not settle");
          // The ruling, in send order: a loss cause for a session open at the loss and for each
          // BEGIN; an end the owner confirmed left the session idle discharges the causes before it.
          const confirmedThenLost = [false, false];
          const expectLost = [0, 1].map((session) => {
            let lost = openFirst[session];
            let confirmed = false;
            steps.forEach((step, i) => {
              if (step.session !== session) return;
              if (step.op === "begin") {
                lost = true;
                if (confirmed) confirmedThenLost[session] = true;
              }
              const outcome = outcomes[i];
              if (
                (step.op === "commit" || step.op === "rollback") &&
                outcome?.ok === true &&
                outcome.transaction === "idle"
              ) {
                lost = false;
                confirmed = true;
              }
            });
            return lost;
          });
          const run = (session: 0 | 1) =>
            c.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session }).then(
              (answer) => ({ ran: true, transaction: answer.summary.transaction }),
              (error: unknown) => ({
                ran: false,
                transaction: `${(error as Error).name}:${String((error as { effect?: string }).effect)}`,
              }),
            );
          for (const session of [0, 1] as const) {
            expect(c.lostSessions.has(session)).toBe(expectLost[session]);
            const fresh = await run(session);
            const hadCause =
              openFirst[session] ||
              steps.some((step) => step.session === session && step.op === "begin");
            if (expectLost[session]) {
              entered.add(fresh.ran ? "ran while lost" : "refused while lost");
              // The review's case: a confirmed end, then a later BEGIN lost again.
              if (confirmedThenLost[session]) entered.add("lost again after a confirmed end");
              expect(fresh).toEqual({ ran: false, transaction: "SessionLostError:none" });
              // The explicit end: a ROLLBACK the owner confirms makes the session usable again.
              const rollback = await c.query("ROLLBACK", [], { session });
              expect(rollback.summary.transaction).toBe("idle");
              expect(c.lostSessions.has(session)).toBe(false);
              expect(await run(session)).toEqual({ ran: true, transaction: "idle" });
              entered.add("ran after an explicit ROLLBACK");
            } else {
              entered.add(hadCause ? "ran after a confirmed end" : "ran, never lost");
              // Never inside a transaction it did not open: an autocommit.
              expect(fresh).toEqual({ ran: true, transaction: "idle" });
            }
          }
          reach.recordHistory(entered);
          for (const cleanup of cleanups.splice(0).reverse()) cleanup();
        }),
        { seed, numRuns: 100 },
      );
      reach.assertReached();
    },
    200_000,
  );
});

describe("p2d: a client notices that its owner was replaced", () => {
  /**
   * a hosts; b is connected to a's owner and idle. a's owner dies; a's page elects a new owner at
   * once (a request of its own). b's port to the dead owner never says so.
   */
  async function replaced(options: Parameters<typeof tab>[1] = {}) {
    const w = world();
    const a = tab(w);
    await a.connector.start();
    await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await a.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
    const b = tab(w, options);
    await b.connector.start();
    const generation = b.client.generation;
    const firstOwner = b.client.ownerId;
    w.owners[0]?.kill();
    await a.client.query("SELECT * FROM t WHERE id = $1", [1]);
    await until(() => w.owners.length === 2 && w.owners[1]?.host !== null);
    return { w, a, b, generation, firstOwner };
  }

  it("1: the new owner's announcement moves an idle client at once", async () => {
    const { b, generation, firstOwner } = await replaced();
    // Before b sends anything: it reconnected on the announcement alone.
    // (The generation counts from the start of a reconnect; the owner id changes at its welcome.)
    await until(() => b.client.connected && b.client.ownerId !== firstOwner, 1_000);
    expect(b.client.generation).toBeGreaterThan(generation);
    const started = Date.now();
    await b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    expect(Date.now() - started).toBeLessThan(1_000);
    expect(await value(b.client)).toBe(1);
  });

  it("2: without the announcement, the silence deadline reconnects and the write runs once", async () => {
    const { b, generation } = await replaced({ announcements: false, silenceDeadlineMs: 300 });
    await sleep(100);
    expect(b.client.generation).toBe(generation);
    const started = Date.now();
    // Sent to the dead owner's port: no reply, no alive. The deadline, then the lock check.
    const write = b.client
      .query("UPDATE t SET n = n + 1 WHERE id = $1", [1])
      .then(() => "answered");
    expect(await Promise.race([write, sleep(3_000).then(() => "hung")])).toBe("answered");
    const took = Date.now() - started;
    expect(took).toBeGreaterThanOrEqual(250);
    expect(took).toBeLessThan(3_000);
    expect(b.client.generation).toBeGreaterThan(generation);
    // Resent once to the new owner, which ran it once: the ledger, not a second run.
    expect(await value(b.client)).toBe(1);
  });

  it("2: an owner alive but silent past the deadline is confirmed by its lock, not left", async () => {
    const w = world();
    // An owner that sends `alive` only once a minute: silent, but alive and holding its lock.
    const a = tab(w, { settings: { aliveIntervalMs: 60_000 } });
    await a.connector.start();
    await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await a.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
    const b = tab(w, { silenceDeadlineMs: 150 });
    await b.connector.start();
    const generation = b.client.generation;
    await b.client.query("BEGIN", [], { session: 1 });
    await b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session: 1 });
    // A long statement: no message for 800 ms, five deadlines.
    const spin = await b.client.query("SELECT spin($1)", [800], { session: 1 });
    expect(spin.summary.command).toBe("SELECT");
    // No reconnect: the transaction is intact and commits.
    expect(b.client.generation).toBe(generation);
    expect(b.client.lostSessions.size).toBe(0);
    await b.client.query("COMMIT", [], { session: 1 });
    expect(await value(b.client)).toBe(1);
  });

  it("2: pins the deadline at 2.5 alive intervals", () => {
    expect(SILENCE_DEADLINE_MS).toBe(2_500);
    expect(SILENCE_DEADLINE_MS).toBe(2.5 * ALIVE_INTERVAL_MS);
  });
});

describe("D1 (B5-p2d review 1): announcements are hints, checked before anything is torn down", () => {
  /** A hosts with a table; b is connected with an open transaction on session 1. */
  async function liveTransaction(options: Parameters<typeof tab>[1] = {}) {
    const w = world();
    const a = tab(w);
    await a.connector.start();
    await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await a.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
    const b = tab(w, options);
    await b.connector.start();
    await b.client.query("BEGIN", [], { session: 1 });
    await b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session: 1 });
    const announce = (ownerId: string, epoch: number | undefined) => {
      const channel = new BroadcastChannel(skewChannelName("db"));
      channel.postMessage({ t: "owner", protocol: PROTOCOL_VERSION, build: "x", ownerId, epoch });
      channel.close();
    };
    /** Whether b asked the broker again from now on (a rediscovery, even one that resumes). */
    const watchStatus = () => {
      const before = w.connects.get(b.client) ?? 0;
      return () => (w.connects.get(b.client) ?? 0) > before;
    };
    return {
      w,
      a,
      b,
      announce,
      watchStatus,
      generation: b.client.generation,
      owner: b.client.ownerId,
    };
  }

  /** The transaction is intact: same connection, no lost session, and it commits. */
  async function intact(b: Tab, generation: number) {
    expect(b.client.generation).toBe(generation);
    expect(b.client.lostSessions.size).toBe(0);
    await b.client.query("COMMIT", [], { session: 1 });
    expect(await value(b.client)).toBe(1);
  }

  it("a stale announcement (an older or equal epoch) is ignored", async () => {
    const { b, announce, generation, watchStatus } = await liveTransaction();
    const left = watchStatus();
    announce("stale-1", 0);
    announce("stale-2", b.client.ownerEpoch);
    announce("stale-3", undefined);
    await sleep(100);
    expect(left()).toBe(false);
    await intact(b, generation);
  });

  it("a repeated announcement does not repeat anything", async () => {
    const { b, announce, generation, watchStatus } = await liveTransaction();
    const left = watchStatus();
    for (let i = 0; i < 20; i++) announce(`again-${String(i)}`, 0);
    await sleep(100);
    expect(left()).toBe(false);
    await intact(b, generation);
  });

  it("a newer epoch is checked against the host lock: our owner still holds it, so nothing happens", async () => {
    const { b, announce, generation, watchStatus } = await liveTransaction();
    const left = watchStatus();
    announce("forged", b.client.ownerEpoch + 5);
    await sleep(100);
    // Not even a rediscovery: the host lock answered.
    expect(left()).toBe(false);
    await intact(b, generation);
  });

  it("with holders hidden, a stale epoch alone keeps the connector where it is", async () => {
    const { b, announce, generation, watchStatus } = await liveTransaction();
    const locks = b.locks as unknown as { query?: unknown };
    const query = locks.query;
    locks.query = undefined;
    const left = watchStatus();
    announce("stale", b.client.ownerEpoch);
    await sleep(100);
    locks.query = query;
    expect(left()).toBe(false);
    await intact(b, generation);
  });

  it("rediscovering the same live owner resumes the connection, never a new generation", async () => {
    // Locks whose holders the platform does not report: the check cannot confirm, and the
    // connector rediscovers. The broker hands back the same live owner.
    const { w, b, announce, generation, owner } = await liveTransaction();
    const locks = b.locks as unknown as { query?: unknown };
    const query = locks.query;
    locks.query = undefined;
    announce("forged", b.client.ownerEpoch + 5);
    await sleep(150);
    locks.query = query;
    expect(b.client.ownerId).toBe(owner);
    expect(w.owners).toHaveLength(1);
    await intact(b, generation);
  });

  it("a BroadcastChannel whose constructor throws means no announcements, not a failure", async () => {
    const original = globalThis.BroadcastChannel;
    // A constructor that throws, as in a restricted context.
    globalThis.BroadcastChannel = function refusing() {
      throw new Error("SecurityError");
    } as unknown as typeof BroadcastChannel;
    try {
      const w = world();
      const a = tab(w);
      await a.connector.start();
      await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
      const b = tab(w);
      await b.connector.start();
      await b.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
      expect(await value(a.client)).toBe(0);
    } finally {
      globalThis.BroadcastChannel = original;
    }
  });
});

describe("D2 (B5-p2d review 2): a silent connection is probed, backed off, and eventually replaced", () => {
  /**
   * The review's probe: a hosts; b connects through an owner whose posts to b's ports (old and
   * new) can be dropped, while the owner and its locks live. `hide()` makes `locks.query` absent,
   * as on a platform that hides lock holders.
   */
  async function silentOwner(options: Parameters<typeof tab>[1] = {}) {
    const w = world();
    const a = tab(w);
    await a.connector.start();
    await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await a.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
    const host = w.owners[0]?.host;
    if (host === null || host === undefined) throw new Error("no owner");
    const accept = host.server.accept.bind(host.server);
    const drop = { on: false, dropped: 0 };
    host.server.accept = (port) => {
      const post = port.postMessage.bind(port);
      port.postMessage = (...args: Parameters<typeof post>) => {
        if (drop.on) drop.dropped++;
        else post(...args);
      };
      accept(port);
    };
    const b = tab(w, options);
    await b.connector.start();
    const locks = b.locks as unknown as { query?: unknown };
    const hide = () => {
      locks.query = undefined;
    };
    const connects = () => w.connects.get(b.client) ?? 0;
    return { w, a, b, host, drop, hide, connects };
  }

  it("the review's probe at the production deadline: no rediscovery loop", async () => {
    const { b, drop, hide, connects } = await silentOwner();
    hide();
    drop.on = true;
    const before = connects();
    const generation = b.client.generation;
    const pending = b.client.query("SELECT 1").catch(() => undefined);
    await sleep(2_800);
    // Was 68. Now: the deadline (2.5 s), then a probe that waits its answer: none yet.
    expect(connects() - before).toBeLessThanOrEqual(1);
    expect(b.client.generation).toBe(generation);
    b.connector.close();
    await pending;
  }, 10_000);

  it("bounded attempts, then a new generation, and the request resolves once the owner answers", async () => {
    const deadline = 40;
    const { b, drop, hide, connects, w } = await silentOwner({
      silenceDeadlineMs: deadline,
      handshakeTimeoutMs: 200,
    });
    hide();
    drop.on = true;
    const before = connects();
    const generation = b.client.generation;
    const started = Date.now();
    const pending = b.client.query("SELECT * FROM t WHERE id = $1", [1]);
    // Three unanswered probes, backing off (deadlines × 1, 2, 4, each plus a probe's wait):
    // two rediscoveries that resume, then the connection is dead and a new generation starts.
    await until(() => b.client.generation > generation, 3_000);
    const elapsed = Date.now() - started;
    expect(elapsed).toBeGreaterThanOrEqual(deadline * (1 + 2 + 4) + deadline * 3 - 20);
    expect(connects() - before).toBe(3);
    // Its handshake is dropped too: one attempt per handshake timeout, never a loop.
    await sleep(500);
    const attempts = connects() - before;
    expect(attempts).toBeGreaterThanOrEqual(4);
    expect(attempts).toBeLessThanOrEqual(3 + Math.ceil(500 / 200) + 1);
    // The owner answers again: the next handshake is welcomed and the read resent, answered.
    drop.on = false;
    const result = await Promise.race([pending, sleep(2_000).then(() => null)]);
    expect(result?.rows()).toEqual([[1, 0, "x"]]);
    expect(w.owners).toHaveLength(1);
    expect(connects() - before).toBeLessThanOrEqual(3 + Math.ceil((Date.now() - started) / 200));
  });

  it("a healthy transaction under a long statement is kept: the probe is answered, nothing else happens", async () => {
    const w = world();
    // Silent owner (`alive` once a minute) on a platform that hides lock holders: only the probe
    // can tell that the connection lives.
    const a = tab(w, { settings: { aliveIntervalMs: 60_000 } });
    await a.connector.start();
    await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await a.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
    const b = tab(w, { silenceDeadlineMs: 100 });
    await b.connector.start();
    (b.locks as unknown as { query?: unknown }).query = undefined;
    const generation = b.client.generation;
    const before = w.connects.get(b.client) ?? 0;
    await b.client.query("BEGIN", [], { session: 1 });
    await b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session: 1 });
    // 1.5 s without a message of the owner's own: fifteen deadlines.
    await b.client.query("SELECT spin($1)", [1_500], { session: 1 });
    // Each deadline's probe is answered. On a loaded host one answer can come later than its
    // wait (100 ms): one rediscovery, which resumes; never a loop, and never a new generation.
    expect((w.connects.get(b.client) ?? 0) - before).toBeLessThanOrEqual(1);
    expect(b.client.generation).toBe(generation);
    expect(b.client.lostSessions.size).toBe(0);
    await b.client.query("COMMIT", [], { session: 1 });
    expect(await value(b.client)).toBe(1);
  });

  it("a connection that ended without a word is replaced, even while the owner holds its lock", async () => {
    const w = world();
    const a = tab(w);
    await a.connector.start();
    await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
    await a.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
    const host = w.owners[0]?.host;
    if (host === null || host === undefined) throw new Error("no owner");
    const accept = host.server.accept.bind(host.server);
    let endOld: () => void = () => undefined;
    host.server.accept = (port) => {
      accept(port);
      const receive = port.onmessage;
      endOld = () => {
        receive?.call(port as MessagePort, { data: { t: "bye" } } as MessageEvent);
      };
    };
    const b = tab(w, { silenceDeadlineMs: 40 });
    await b.connector.start();
    const generation = b.client.generation;
    await b.client.query("BEGIN", [], { session: 1 });
    // The owner ends b's connection (rolling its transaction back); b is not told.
    endOld();
    // Holders visible: the lock shows b's owner alive. The probe is what notices.
    const outcome = await Promise.race([
      b.client.query("SELECT 1", [], { session: 1 }).then(
        () => "resolved",
        (error: unknown) => (error as Error).name,
      ),
      sleep(3_000).then(() => "hung"),
    ]);
    expect(outcome).toBe("SessionLostError");
    expect(b.client.generation).toBeGreaterThan(generation);
    expect(b.client.ownerId).toBe(w.owners[0]?.host?.ownerId);
    // The session is usable once its loss is acknowledged.
    await b.client.query("ROLLBACK", [], { session: 1 });
    await b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session: 1 });
    expect(await value(b.client)).toBe(1);
  });

  it("pins the probe limit", () => {
    expect(SILENT_PROBES_LIMIT).toBe(3);
  });
});

/** a hosts with one row; returns a's owner. */
async function hosted(w: World, options: Parameters<typeof tab>[1] = {}) {
  const a = tab(w, options);
  await a.connector.start();
  await a.client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT, name TEXT)");
  await a.client.query("INSERT INTO t VALUES ($1, $2, $3)", [1, 0, "x"]);
  const owner = w.owners[0]?.host;
  if (owner === null || owner === undefined) throw new Error("no owner");
  return { a, owner };
}

describe("a broker replaced under its ports (WebKit 26.6, owner/webkit-sharedworker.md)", () => {
  const within = <T>(promise: Promise<T>, ms: number): Promise<T | "timed out"> =>
    Promise.race([promise, sleep(ms).then(() => "timed out" as const)]);

  it("the owner's tab closes while the broker restarts: the survivor elects itself and its request resolves", async () => {
    const w = world();
    const { a } = await hosted(w);
    const b = tab(w, { brokerTimeoutMs: 50 });
    await b.connector.start();
    // In flight, so b watches the host lock and asks the broker the moment a's owner dies.
    const spin = b.client.query("SELECT spin($1)", [200]);
    await sleep(30);
    // WebKit: the page that created the SharedWorker closes, and the broker is replaced; b's
    // question to it is sent inside the switch and lost.
    const restarted = w.broker.restart({ dropMs: 120 });
    killTab(w, a, 0);
    const result = await within(spin, 3_000);
    await restarted;
    expect(result === "timed out" ? result : result.summary.command).toBe("SELECT");
    expect(w.broker.dropped.get("connect") ?? 0).toBeGreaterThan(0);
    expect(b.connector.status).toBe("connected");
    expect(b.connector.hosting).toBe(true);
    // The new owner registered with the new instance.
    expect(w.broker.current?.registrations.get("db")).toBe(b.client.ownerId);
  });

  it("a new instance makes an unanswered request ask again at once, not at its timeout", async () => {
    const w = world();
    const { owner } = await hosted(w);
    // The restart starts before c asks: c's question is dropped. Its retry is 5 s away; only the
    // new instance's announcement can bring the answer sooner.
    const restarted = w.broker.restart({ dropMs: 100 });
    const c = tab(w, { brokerTimeoutMs: 5_000 });
    const started = Date.now();
    expect(await within(c.connector.start(), 2_000)).toBeUndefined();
    await restarted;
    expect(Date.now() - started).toBeLessThan(1_000);
    expect(w.broker.dropped.get("connect") ?? 0).toBeGreaterThan(0);
    expect(c.client.ownerId).toBe(owner.ownerId);
  });

  it("a live owner registers again with the new instance, and a new tab reaches it", async () => {
    const w = world();
    const { owner } = await hosted(w);
    await w.broker.restart({ dropMs: 30 });
    await until(() => w.broker.current?.registrations.get("db") === owner.ownerId, 1_000);
    const c = tab(w);
    expect(await within(c.connector.start(), 2_000)).toBeUndefined();
    expect(c.client.ownerId).toBe(owner.ownerId);
    expect(await value(c.client)).toBe(0);
  });

  it("a stale owner (its host lock stolen) never registers again", async () => {
    const w = world();
    const { owner } = await hosted(w);
    // A successor took the host lock; the old owner has not noticed anything else yet.
    void w.registry
      .context()
      .request(hostLockName("db"), { steal: true }, () => new Promise<void>(() => undefined));
    await sleep(5);
    await w.broker.restart({ dropMs: 10 });
    await sleep(50);
    expect(w.broker.current?.registrations.has("db")).toBe(false);
    expect(owner.ownerId).toBeDefined();
  });

  it("registering again is idempotent: one registration, one connection per client", async () => {
    const w = world();
    const { owner } = await hosted(w);
    w.broker.duplicateNext("register");
    await w.broker.restart({ dropMs: 10 });
    await until(() => w.broker.current?.registrations.get("db") === owner.ownerId, 1_000);
    expect(w.broker.duplicated).toBe(1);
    expect(w.broker.current?.registrations.size).toBe(1);
    const before = owner.server.stats.connections;
    const c = tab(w);
    await c.connector.start();
    await sleep(20);
    expect(owner.server.stats.connections).toBe(before + 1);
  });

  it("a duplicate connect reaching the same instance makes no second connection", async () => {
    const w = world();
    const { owner } = await hosted(w);
    const before = owner.server.stats.connections;
    w.broker.duplicateNext("connect");
    const c = tab(w);
    await c.connector.start();
    await sleep(20);
    // The law first: one connection for c.
    expect(owner.server.stats.connections).toBe(before + 1);
    expect(w.broker.duplicated).toBe(1);
    // The injected copy, and the client's own repeat on the instance's announcement.
    expect(w.broker.duplicates).toBeGreaterThanOrEqual(1);
    expect(await value(c.client)).toBe(0);
  });

  it("a retry of a slow answer reaches the same instance: answered once, one connection", async () => {
    // The broker's lock query takes 60 ms; c's first attempt times out after 15 ms.
    const w = world({ brokerQueryDelayMs: 60 });
    const { owner } = await hosted(w);
    const before = owner.server.stats.connections;
    const c = tab(w, { brokerTimeoutMs: 15 });
    await c.connector.start();
    await sleep(100);
    expect(owner.server.stats.connections).toBe(before + 1);
    expect(w.broker.duplicates).toBeGreaterThan(0);
    expect(await value(c.client)).toBe(0);
  });
});

/**
 * A connector (that never hosts) whose broker ports, old and new, drop every message while
 * `silent.on`, and reach the world's broker otherwise.
 */
function silentTab(
  w: World,
  unreachable: Error[],
  options: { readonly silent?: boolean; readonly unresponsiveAfterMs?: number } = {},
) {
  /** `posts`: messages the broker side dropped; `sent`: messages the client sent (at once). */
  const silent = { on: options.silent ?? true, ports: 0, posts: 0, sent: 0 };
  const client = new ProtocolClient();
  const connector = new OwnerConnector({
    database: "db",
    client,
    locks: w.registry.context(),
    openBroker: () => {
      const { port1, port2 } = new MessageChannel();
      silent.ports++;
      // A port opened while silent reaches the broker (and hears its announcement) only once the
      // silence ends; a port that reached it drops what it sends while silent.
      let route: ((event: MessageEvent) => void) | null = null;
      const reach = () => {
        if (route !== null) return;
        w.broker.accept(port1);
        route = port1.onmessage;
        port1.onmessage = receive;
      };
      const receive = (event: MessageEvent) => {
        if (silent.on) {
          silent.posts++;
          return;
        }
        reach();
        route?.call(port1, event);
      };
      if (silent.on) port1.onmessage = receive;
      else reach();
      const post = port2.postMessage.bind(port2);
      port2.postMessage = ((message: unknown, transfer?: Transferable[]) => {
        silent.sent++;
        post(message, transfer ?? []);
      }) as typeof port2.postMessage;
      cleanups.push(() => {
        port1.close();
        port2.close();
      });
      return port2;
    },
    startOwner: null,
    context: (): HostContext => ({
      kind: "page",
      visible: true,
      platform: { mobile: false },
      superseded: false,
    }),
    announcements: false,
    retryMs: 50,
    brokerTimeoutMs: 20,
    brokerBoundMs: 150,
    ...(options.unresponsiveAfterMs === undefined
      ? {}
      : { unresponsiveAfterMs: options.unresponsiveAfterMs }),
    onUnreachable: (error) => {
      unreachable.push(error);
    },
  });
  cleanups.push(() => {
    connector.close();
  });
  return { connector, client, silent };
}

/**
 * Tracks the timers the connector and the client set (not earlier tests' in-process owners, which
 * keep scheduling theirs in this worker). `pending` is what is still to fire; `restore()` puts the
 * real functions back.
 */
function trackOwnerTimers() {
  const pending = new Set<unknown>();
  const realSet = globalThis.setTimeout;
  const realClear = globalThis.clearTimeout;
  globalThis.setTimeout = ((handler: () => void, ms?: number) => {
    const id = realSet(() => {
      pending.delete(id);
      handler();
    }, ms);
    if (/owner\/(connector|client)\.ts/.test(new Error().stack ?? "")) pending.add(id);
    return id;
  }) as typeof setTimeout;
  globalThis.clearTimeout = ((id: ReturnType<typeof setTimeout>) => {
    pending.delete(id);
    realClear(id);
  }) as typeof clearTimeout;
  return {
    pending,
    wait: (ms: number) => new Promise((resolve) => realSet(resolve, ms)),
    restore: () => {
      globalThis.setTimeout = realSet;
      globalThis.clearTimeout = realClear;
    },
  };
}

describe("a broker request is bounded (connector.ts ConnectorStatus)", () => {
  it("a broker that never answers: OwnerUnreachableError at the bound, never left connecting", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const w = world();
    const unreachable: Error[] = [];
    const { connector, silent } = silentTab(w, unreachable);
    let settled = false;
    const start = connector.start().then(
      () => {
        settled = true;
        return null;
      },
      (caught: unknown) => {
        settled = true;
        return caught as Error;
      },
    );
    try {
      // Timer progress, rather than host scheduling latency, defines the 150 ms policy bound.
      await vi.advanceTimersByTimeAsync(149);
      expect(settled).toBe(false);
      expect(connector.status).toBe("connecting");
      // Requests at 0, 20, 60 and 140 ms; the last two each open a fresh port. Count sends
      // synchronously: delivery on real MessageChannels has an independent task queue.
      expect(silent.sent).toBe(4);
      expect(silent.ports).toBe(3);
      await vi.advanceTimersByTimeAsync(1);
      // Assert settlement before awaiting: a missing bound must fail here, never time out.
      expect(settled).toBe(true);
      const error = await start;
      expect(error?.name).toBe("OwnerUnreachableError");
      expect((error as { code?: string } | null)?.code).toBe("MINNOW_OWNER_UNREACHABLE");
      expect(connector.status).toBe("unreachable");
      expect(unreachable).toHaveLength(1);
    } finally {
      connector.close();
      await start;
      vi.useRealTimers();
    }
  });

  it("after giving up, the next request makes it connect again once the broker answers", async () => {
    const w = world();
    const { owner } = await hosted(w);
    const unreachable: Error[] = [];
    const { connector, client, silent } = silentTab(w, unreachable);
    await connector.start().catch(() => undefined);
    expect(connector.status).toBe("unreachable");
    silent.on = false;
    // A request is activity: connecting starts again (bounded, as always). This client never
    // had a connection, and a request made before the first one is not sent on it (client.ts
    // sends unsent requests on a reconnect only), so it is the next request that is answered.
    void client.query("SELECT 1").catch(() => undefined);
    await until(() => connector.status === "connected", 2_000);
    expect(client.ownerId).toBe(owner.ownerId);
    expect(await value(client)).toBe(0);
  });

  it("a second answer to a request already answered is closed with bye, never used", async () => {
    const w = world();
    const { owner } = await hosted(w);
    let brokerSide: MessagePort | null = null;
    const c = tab(w);
    // The world's broker side of c's port: the last port the broker accepted for c.
    const accept = w.broker.accept.bind(w.broker);
    w.broker.accept = (port: MessagePort) => {
      brokerSide = port;
      accept(port);
    };
    await c.connector.start();
    w.broker.accept = accept;
    const before = owner.server.stats.connections;
    const extra = new MessageChannel();
    const bye = new Promise<unknown>((resolve) => {
      extra.port2.onmessage = (event: MessageEvent) => {
        resolve(event.data);
      };
    });
    // A late answer to c's first request (nonce 1), as a dead instance's could be.
    (brokerSide as MessagePort | null)?.postMessage(
      { t: "port", nonce: 1, ownerId: owner.ownerId, port: extra.port1 },
      [extra.port1],
    );
    expect(await Promise.race([bye, sleep(1_000).then(() => "no bye")])).toEqual({ t: "bye" });
    expect(owner.server.stats.connections).toBe(before);
    expect(c.connector.status).toBe("connected");
    extra.port2.close();
  });

  it("close() mid-request leaves no timer and sends nothing more", async () => {
    const w = world();
    const timers = trackOwnerTimers();
    try {
      const { connector, silent } = silentTab(w, []);
      const start = connector.start().catch((error: unknown) => (error as Error).name);
      await timers.wait(50);
      expect(timers.pending.size).toBeGreaterThan(0);
      connector.close();
      expect(await start).toBe("ClientClosedError");
      expect(timers.pending.size).toBe(0);
      const sent = silent.sent;
      await timers.wait(200);
      expect(silent.sent).toBe(sent);
    } finally {
      timers.restore();
    }
  });
});

describe("generated: a broker restart at any point never strands a tab", () => {
  const scenario = fc.record({
    killOwner: fc.boolean(),
    /** When the broker is replaced, relative to the owner's tab closing (or to the write). */
    restartAt: fc.integer({ min: -40, max: 80 }),
    dropMs: fc.integer({ min: 0, max: 200 }),
    duplicate: fc.constantFrom("none", "connect", "register"),
    newTab: fc.boolean(),
  });

  const SEEDS = seedsFor("next-owner-broker-restart", [0xb0b5]);

  it.each(SEEDS)(
    "every live tab connects to one registered owner, and the write runs once (seed %i)",
    async (seed) => {
      const reach = createReach("next-owner-broker-restart", BROKER_RESTART_STATES, {
        minimum: 3,
      });
      await fc.assert(
        fc.asyncProperty(scenario, async ({ killOwner, restartAt, dropMs, duplicate, newTab }) => {
          const entered = new Set<string>();
          const w = world();
          const { a } = await hosted(w, { brokerTimeoutMs: 30 });
          const b = tab(w, { brokerTimeoutMs: 30 });
          await b.connector.start();
          if (duplicate !== "none") w.broker.duplicateNext(duplicate);
          // b's write, in flight when the owner's tab closes (if it does).
          const write = b.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
          const spin = b.client.query("SELECT spin($1)", [60]);
          const events: Array<[number, () => void]> = [
            [restartAt, () => void w.broker.restart({ dropMs })],
          ];
          if (killOwner) events.push([0, () => killTab(w, a, 0)]);
          // A tab opened during the switch (set from a scheduled event, so held in an object).
          const opened: { c: Tab | null } = { c: null };
          if (newTab)
            events.push([
              restartAt + Math.floor(dropMs / 2),
              () => {
                const c = tab(w, { brokerTimeoutMs: 30 });
                opened.c = c;
                void c.connector.start().catch(() => undefined);
              },
            ]);
          events.sort((x, y) => x[0] - y[0]);
          let now = Math.min(0, ...events.map(([at]) => at));
          for (const [at, run] of events) {
            await sleep(at - now);
            now = at;
            run();
          }
          // Laws, within a bound: every live tab connected, all to one owner, registered with
          // the broker instance of the moment; the write applied exactly once.
          const live = [...(killOwner ? [] : [a]), b, ...(opened.c === null ? [] : [opened.c])];
          await until(
            () =>
              w.broker.current !== null && live.every((t) => t.connector.status === "connected"),
            3_000,
          );
          await Promise.all([write, spin]);
          const ownerIds = new Set(live.map((t) => t.client.ownerId));
          expect(ownerIds.size).toBe(1);
          const ownerId = [...ownerIds][0] ?? null;
          await until(() => w.broker.current?.registrations.get("db") === ownerId, 2_000);
          expect(await value(b.client)).toBe(1);
          // No second session from a duplicate: once unused handshakes ended, one connection per
          // live tab.
          const owner = w.owners.find((entry) => entry.host?.ownerId === ownerId)?.host;
          await until(() => owner?.server.stats.connections === live.length, 2_000);

          const restart = w.broker.restarts[0];
          if ((w.broker.dropped.get("connect") ?? 0) > 0 || w.broker.lostReplies > 0)
            entered.add("restart during connect");
          if (restart !== undefined && restart.registered > 0)
            entered.add("restart while the owner is registered");
          if (restart !== undefined && restart.waiting > 0)
            entered.add("restart with a client waiting");
          if (w.broker.duplicates > 0) entered.add("duplicate request delivered");
          reach.recordHistory(entered);
          for (const cleanup of cleanups.splice(0).reverse()) cleanup();
        }),
        { seed, numRuns: 40 },
      );
      reach.assertReached();
    },
    200_000,
  );
});

describe("requests end in bounded time too (client.ts connect and giveUp)", () => {
  const settledAs = (promise: Promise<unknown>) =>
    promise.then(
      () => ({ ok: true as const, error: null }),
      (error: unknown) => ({
        ok: false as const,
        error: error as Error & { effect?: string; code?: string; sequence?: number | null },
      }),
    );

  it("requests made while connecting go out on the first connection, once each, in order", async () => {
    const w = world();
    const { a } = await hosted(w);
    await a.client.query("INSERT INTO t VALUES ($1, $2, $3)", [2, 0, "y"]);
    const c = tab(w);
    // Not connected yet: none of these can be sent. Order matters: (0 × 2) + 1 = 1, but
    // (0 + 1) × 2 = 2, and any request run twice moves n further.
    const double = c.client.query("UPDATE t SET n = n * 2 WHERE id = $1", [1]);
    const inc = c.client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1]);
    const read = c.client.query("SELECT * FROM t WHERE id = $1", [1]);
    // A transaction opened before the first connection (on another row: sessions run
    // concurrently): BEGIN never sent opened nothing, so it goes like any other request.
    const begin = c.client.query("BEGIN", [], { session: 1 });
    const inTx = c.client.query("UPDATE t SET n = n + 10 WHERE id = $1", [2], { session: 1 });
    const commit = c.client.query("COMMIT", [], { session: 1 });
    await c.connector.start();
    await Promise.all([double, inc, begin, inTx, commit]);
    expect((await read).rows()[0]?.[1]).toBe(1);
    expect(c.client.lostSessions.size).toBe(0);
    expect(await value(a.client)).toBe(1);
    const second = await a.client.query("SELECT * FROM t WHERE id = $1", [2]);
    expect(second.rows()[0]?.[1]).toBe(10);
  });

  it("a request issued during the first handshake is posted once, not again after the welcome", async () => {
    const w = world();
    const { a, owner } = await hosted(w);
    const client = new ProtocolClient();
    const { port1, port2 } = new MessageChannel();
    cleanups.push(() => {
      client.close();
      port1.close();
      port2.close();
    });
    const posts = new Map<number, number>();
    const post = port2.postMessage.bind(port2);
    port2.postMessage = ((message: unknown, transfer?: Transferable[]) => {
      const seq = (message as { seq?: number }).seq;
      if (typeof seq === "number") posts.set(seq, (posts.get(seq) ?? 0) + 1);
      post(message, transfer ?? []);
    }) as typeof port2.postMessage;
    const connecting = client.connect(port2);
    // The port is open but not welcomed: this goes right after the hello.
    const { seq, promise } = client.send({
      t: "statement",
      session: 0,
      text: "UPDATE t SET n = n + 1 WHERE id = $1",
      prepared: null,
      knownColumnsVersion: -1,
      params: [1],
      result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
    });
    owner.server.accept(port1);
    await connecting;
    await promise;
    expect(posts.get(seq)).toBe(1);
    expect(await value(a.client)).toBe(1);
  });

  it("the deadline (fake timers): unreachable for unresponsiveAfterMs, then waiting requests fail", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    // Over the fake timers: counts only the connector's and the client's.
    const timers = trackOwnerTimers();
    try {
      const w = world();
      const { connector, client } = silentTab(w, [], { unresponsiveAfterMs: 1_000 });
      // Never sent: there is no connection yet.
      const waiting = settledAs(client.query("SELECT 1"));
      let settled = false;
      void waiting.then(() => {
        settled = true;
      });
      void connector.start().catch(() => undefined);
      await vi.advanceTimersByTimeAsync(150);
      expect(connector.status).toBe("unreachable");
      await vi.advanceTimersByTimeAsync(999);
      expect(settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      const outcome = await waiting;
      expect(outcome.ok).toBe(false);
      expect(outcome.error?.name).toBe("OwnerUnreachableError");
      expect(outcome.error?.code).toBe("MINNOW_OWNER_UNREACHABLE");
      expect(outcome.error?.effect).toBe("none");
      expect(typeof outcome.error?.sequence).toBe("number");
      // Past the deadline a new request fails at once, never sent.
      const late = await settledAs(client.query("SELECT 2"));
      expect(late.error?.effect).toBe("none");
      // close() leaves nothing scheduled (the deadline was armed and fired; retries stopped).
      connector.close();
      expect(timers.pending.size).toBe(0);
    } finally {
      timers.restore();
      vi.useRealTimers();
    }
  });

  it("a sent, unanswered request fails unknown; the session stays lost; a reconnect recovers", async () => {
    const w = world();
    const { a } = await hosted(w);
    const { connector, client, silent } = silentTab(w, [], {
      silent: false,
      unresponsiveAfterMs: 100,
    });
    await connector.start();
    await client.query("BEGIN", [], { session: 1 });
    await client.query("UPDATE t SET n = n + 1 WHERE id = $1", [1], { session: 1 });
    // Sent, and its owner dies before answering.
    const spin = settledAs(client.query("SELECT spin($1)", [5_000], { session: 1 }));
    await sleep(20);
    silent.on = true;
    killTab(w, a, 0);
    // Posted on the dead owner's port: sent, never answered. A BEGIN may have opened a
    // transaction, so its session must stay lost.
    const begin2 = settledAs(client.query("BEGIN", [], { session: 2 }));
    const outcome = await Promise.race([spin, sleep(3_000).then(() => null)]);
    expect((await begin2).error?.effect).toBe("unknown");
    // Check before recovery: a missing loss cause must fail here, not hang later.
    expect(client.lostSessions.has(2)).toBe(true);
    expect(outcome?.error?.name).toBe("OwnerUnreachableError");
    // The dangerous direction: it was sent, so it may have taken effect. Never "none".
    expect(outcome?.error?.effect).toBe("unknown");
    expect(client.gaveUp).toBe(true);
    // A new owner, and the broker answers again.
    const d = tab(w);
    await d.connector.start();
    silent.on = false;
    // Refused at once (never sent), and it makes the connector try again.
    const refused = await settledAs(client.query("SELECT 1"));
    expect(refused.error?.effect).toBe("none");
    await until(() => connector.status === "connected", 2_000);
    expect(client.gaveUp).toBe(false);
    expect(client.ownerId).toBe(d.client.ownerId);
    expect((await client.query("SELECT * FROM t WHERE id = $1", [1])).rows()[0]?.[1]).toBe(0);
    // Session 1's transaction went with the dead owner: refused until an explicit ROLLBACK.
    expect(client.lostSessions.has(1)).toBe(true);
    expect(client.lostSessions.has(2)).toBe(true);
    const inLost = await settledAs(client.query("SELECT 1", [], { session: 1 }));
    expect(inLost.error?.name).toBe("SessionLostError");
    await client.query("ROLLBACK", [], { session: 1 });
    expect(client.lostSessions.has(1)).toBe(false);
  });

  it("close() while unreachable, deadline armed, leaves no timer", async () => {
    const w = world();
    const timers = trackOwnerTimers();
    try {
      const { connector, client } = silentTab(w, [], { unresponsiveAfterMs: 10_000 });
      const waiting = settledAs(client.query("SELECT 1"));
      await connector.start().catch(() => undefined);
      expect(connector.status).toBe("unreachable");
      expect(timers.pending.size).toBeGreaterThan(0);
      connector.close();
      expect(timers.pending.size).toBe(0);
      expect((await waiting).error?.name).toBe("ClientClosedError");
    } finally {
      timers.restore();
    }
  });
});

/** FIFO delivery independent of the host poll phase; transfer/serialization use real-port suites. */
function discoveryChannel(): { port1: MessagePort; port2: MessagePort } {
  const endpoint = () => ({
    onmessage: null as ((event: MessageEvent) => void) | null,
    closed: false,
    close() {
      this.closed = true;
    },
    postMessage(message: unknown) {
      void message; // replaced after pairing
    },
  });
  const a = endpoint();
  const b = endpoint();
  const send = (peer: typeof a, message: unknown) => {
    queueMicrotask(() => {
      if (!peer.closed) peer.onmessage?.({ data: message } as MessageEvent);
    });
  };
  a.postMessage = (message) => {
    if (!a.closed) send(b, message);
  };
  b.postMessage = (message) => {
    if (!b.closed) send(a, message);
  };
  return { port1: a as unknown as MessagePort, port2: b as unknown as MessagePort };
}

/** Whole-attempt liveness, independent of whether individual discovery messages are answered. */
describe("review P1: responsive discovery still has a deadline", () => {
  const seeds = seedsFor("next-owner-discovery-bound", [0xd15c]);
  it.each(seeds)(
    "generated negative replies and failed handshakes (seed %i)",
    async (seed) => {
      const reach = createReach("next-owner-discovery-bound", OWNER_DISCOVERY_STATES, {
        minimum: 2,
      });
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            mode: fc.constantFrom(
              "noOwner",
              "unreachable",
              "handshake timeout",
              "handshake refusal",
              "handshake newer",
              "never ready",
              "lock wait",
              "startup throws",
              "broker throws",
            ),
            bound: fc.integer({ min: 30, max: 60 }),
            unresponsive: fc.integer({ min: 15, max: 30 }),
            sent: fc.boolean(),
          }),
          async ({ mode, bound, unresponsive, sent }) => {
            vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
            try {
              const registry = new FakeLockRegistry();
              const held = registry.context();
              const ports: MessagePort[] = [];
              let requests = 0;
              const timing = { stalled: false };
              let terminated = 0;
              let spawned = 0;
              if (mode === "lock wait" || mode.startsWith("handshake")) {
                await new Promise<void>((resolve) => {
                  void held.request(hostLockName("bounded-discovery"), {}, () => {
                    resolve();
                    return new Promise<void>(() => undefined);
                  });
                });
              }
              const client = new ProtocolClient();
              let warm: ReturnType<typeof ownerHarness> | null = null;
              if (sent) {
                warm = ownerHarness();
                const channel = discoveryChannel();
                ports.push(channel.port1, channel.port2);
                const post = channel.port1.postMessage.bind(channel.port1);
                channel.port1.postMessage = ((
                  message: { t: string },
                  transfer?: Transferable[],
                ) => {
                  if (message.t === "welcome") post(message, transfer ?? []);
                }) as typeof channel.port1.postMessage;
                warm.server.accept(channel.port1);
                await client.connect(channel.port2);
              }
              const connector = new OwnerConnector({
                database: "bounded-discovery",
                client,
                locks: registry.context(),
                announcements: false,
                brokerBoundMs: bound,
                brokerTimeoutMs: 5,
                retryMs: 2,
                handshakeTimeoutMs: 5,
                unresponsiveAfterMs: unresponsive,
                context: () => ({
                  kind: "page",
                  visible: true,
                  superseded: false,
                  platform: { mobile: false },
                }),
                openBroker:
                  mode === "never ready" || mode === "lock wait" || mode === "startup throws"
                    ? null
                    : () => {
                        if (mode === "broker throws") throw new Error("broker construction failed");
                        const channel = discoveryChannel();
                        ports.push(channel.port1, channel.port2);
                        channel.port1.onmessage = (event) => {
                          const request = event.data as { t: string; nonce: number };
                          if (request.t !== "connect") return;
                          requests++;
                          if (requests === 2) {
                            // Witness a repeated reply before the deliberately delayed timer turn.
                            timing.stalled = true;
                          }
                          if (!mode.startsWith("handshake")) {
                            channel.port1.postMessage({ t: mode, nonce: request.nonce });
                            return;
                          }
                          const owner = discoveryChannel();
                          ports.push(owner.port1);
                          owner.port1.onmessage = () => {
                            if (mode === "handshake newer")
                              owner.port1.postMessage({
                                t: "refused",
                                reason: "version",
                                ownerProtocol: PROTOCOL_VERSION + 1,
                                ownerBuild: "newer",
                              });
                            if (mode === "handshake refusal")
                              owner.port1.postMessage({
                                t: "refused",
                                reason: "steppingDown",
                                ownerProtocol: PROTOCOL_VERSION,
                                ownerBuild: "test",
                              });
                          };
                          channel.port1.postMessage(
                            {
                              t: "port",
                              ownerId: "silent",
                              nonce: request.nonce,
                              port: owner.port2,
                            },
                            [owner.port2],
                          );
                        };
                        return channel.port2;
                      },
                startOwner:
                  mode === "never ready" || mode === "lock wait" || mode === "startup throws"
                    ? () => {
                        spawned++;
                        if (mode === "startup throws")
                          throw new Error("worker construction failed");
                        return {
                          ready: new Promise(() => undefined),
                          stopped: new Promise(() => undefined),
                          lifecycle() {
                            return undefined;
                          },
                          terminate() {
                            terminated++;
                          },
                        };
                      }
                    : null,
              });
              let ended = false;
              let effect: string | undefined;
              const waiting = client
                .query("SELECT 1")
                .catch((error: unknown) => {
                  effect = (error as { effect?: string }).effect;
                })
                .finally(() => {
                  ended = true;
                });
              let startEnded = false;
              let atEnd = {
                status: connector.status,
                requests,
                spawned,
                terminated,
                stalled: timing.stalled,
                hostHeld: registry.held(hostLockName("bounded-discovery")),
              };
              const connecting = connector
                .start()
                .catch(() => undefined)
                .finally(() => {
                  startEnded = true;
                  atEnd = {
                    status: connector.status,
                    requests,
                    spawned,
                    terminated,
                    stalled: timing.stalled,
                    hostHeld: registry.held(hostLockName("bounded-discovery")),
                  };
                });
              try {
                // Let responsive discovery repeat with deterministic FIFO delivery. A late turn
                // fires due timers synchronously, withholding promise continuations until afterward.
                await vi.advanceTimersByTimeAsync(bound - 1);
                vi.advanceTimersByTime(timing.stalled ? 200 : 1);
                await vi.advanceTimersByTimeAsync(0);
                expect(startEnded).toBe(true);
                // giveUp changes activity and can start another attempt. Check this attempt's
                // result and cleanup at its completion, independently of a later retry.
                expect(atEnd.status).toBe(
                  mode === "handshake newer" ? "superseded" : "unreachable",
                );
                if (mode === "startup throws") expect(atEnd.hostHeld).toBe(false);
                if (
                  mode === "noOwner" ||
                  (mode.startsWith("handshake") && mode !== "handshake newer")
                )
                  expect(atEnd.requests).toBeGreaterThan(1);
                if (mode === "never ready") {
                  expect(atEnd.spawned).toBeGreaterThan(0);
                  expect(atEnd.terminated).toBe(atEnd.spawned);
                }
                await vi.advanceTimersByTimeAsync(unresponsive + 1);
                expect(ended).toBe(true);
                expect(effect).toBe(sent ? "unknown" : "none");
                const entered = new Set([mode, sent ? "sent unanswered" : "never sent"]);
                if (atEnd.stalled) entered.add("delayed event-loop turn");
                reach.recordHistory(entered);
              } finally {
                connector.close();
                held.kill();
                for (const port of ports) port.close();
                warm?.server.stepDown("closing");
                await warm?.engine.close();
                await Promise.all([waiting, connecting]);
              }
            } finally {
              vi.useRealTimers();
            }
          },
        ),
        { seed, numRuns: 80 },
      );
      reach.assertReached();
    },
    60_000,
  );
});

it("P1: recovery to the same owner after give-up creates a usable connection", async () => {
  const w = world();
  const { owner } = await hosted(w);
  const { connector, client, silent } = silentTab(w, [], {
    silent: false,
    unresponsiveAfterMs: 30,
  });
  await connector.start();
  const generation = client.generation;
  const waiting = client.query("SELECT spin($1)", [5_000]).catch((error: unknown) => error);
  silent.on = true;
  client.onStepDown?.("closing");
  await until(() => client.gaveUp, 2_000);
  expect(await waiting).toMatchObject({ effect: "unknown" });
  silent.on = false;
  await client.query("SELECT 1").catch(() => undefined); // activity retries discovery
  await until(() => connector.status === "connected", 2_000);
  expect(client.ownerId).toBe(owner.ownerId);
  expect(client.gaveUp).toBe(false);
  expect(client.generation).toBeGreaterThan(generation);
  expect((await client.query("SELECT 1")).rows()).toEqual([[1]]);
});

it("P2: losing one register after the instance announcement does not strand its owner", async () => {
  const w = world();
  const { owner } = await hosted(w, {
    settings: { registrationTimeoutMs: 5, registrationBoundMs: 200 },
  });
  await until(() => w.broker.current?.registrations.get("db") === owner.ownerId);
  w.broker.dropRegistrations(1);
  await w.broker.restart({ dropMs: 0 });
  await sleep(150);
  expect(w.broker.dropped.get("register")).toBe(1);
  expect(w.broker.current?.registrations.get("db")).toBe(owner.ownerId);
});

describe("P2: acknowledged registration survives independent loss", () => {
  const seeds = seedsFor("next-owner-broker-registration", [0xb052]);
  it.each(seeds)(
    "generated registration and acknowledgement loss (seed %i)",
    async (seed) => {
      const reach = createReach("next-owner-broker-registration", BROKER_REGISTRATION_STATES, {
        minimum: 2,
      });
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            outcome: fc.constantFrom("recover", "fail", "stop", "host fence"),
            drops: fc.integer({ min: 1, max: 2 }),
            ackDrops: fc.integer({ min: 0, max: 1 }),
            repeatAnnouncements: fc.boolean(),
            failureAtAck: fc.boolean(),
          }),
          async ({ outcome, drops, ackDrops, repeatAnnouncements, failureAtAck }) => {
            const w = world();
            const { owner } = await hosted(w, {
              settings: { registrationTimeoutMs: 5, registrationBoundMs: 200 },
            });
            await until(() => w.broker.current?.registrations.get("db") === owner.ownerId);
            const before = w.broker.dropped.get("register") ?? 0;
            const beforeCurrentDrop = w.broker.currentInstanceRegistrationDrops;
            const failAtAck = outcome === "fail" && failureAtAck;
            w.broker.dropRegistrations(outcome === "recover" ? drops : failAtAck ? 0 : Infinity);
            const beforeAck = w.broker.lostRegistrationAcks;
            w.broker.dropRegistrationAcks(failAtAck ? Infinity : ackDrops);
            await w.broker.restart({ dropMs: 0 });
            const announcements = repeatAnnouncements
              ? setInterval(() => w.broker.announceAgain(), 3)
              : null;
            const thief = w.registry.context();
            const entered = new Set<string>();
            try {
              await until(() =>
                failAtAck
                  ? w.broker.lostRegistrationAcks > beforeAck
                  : (w.broker.dropped.get("register") ?? 0) > before,
              );
              if (failAtAck) entered.add("ack lost");
              if (w.broker.currentInstanceRegistrationDrops > beforeCurrentDrop)
                entered.add("register lost after announcement");
              if (outcome === "stop") await owner.stop("closing", false);
              if (outcome === "host fence")
                await new Promise<void>((resolve) => {
                  void thief.request(hostLockName("db"), { steal: true }, () => {
                    resolve();
                    return new Promise<void>(() => undefined);
                  });
                });
              const count = w.broker.registrationAttempts.get(owner.ownerId) ?? 0;
              await sleep(outcome === "recover" || outcome === "fail" ? 300 : 50);
              if (outcome === "recover") {
                expect(w.broker.current?.registrations.get("db")).toBe(owner.ownerId);
                expect(owner.registration.confirmed).toBe(true);
                expect(owner.registration.error).toBe(null);
                const c = tab(w);
                await c.connector.start();
                expect(c.client.ownerId).toBe(owner.ownerId);
                expect(await value(c.client)).toBe(0);
                entered.add("recovered registration");
                if (w.broker.lostRegistrationAcks > 0) entered.add("ack lost");
              } else if (outcome === "fail") {
                expect(w.broker.registrationAttempts.get(owner.ownerId) ?? 0).toBeGreaterThan(1);
                expect(owner.registration.error?.name).toBe("OwnerUnreachableError");
                expect(owner.registration.confirmed).toBe(false);
                entered.add("terminal failure");
              } else {
                expect(w.broker.registrationAttempts.get(owner.ownerId) ?? 0).toBe(count);
                expect(w.broker.current?.registrations.get("db")).not.toBe(owner.ownerId);
                entered.add(outcome === "stop" ? "stop cancels retry" : "host fence cancels retry");
              }
              if (outcome === "fail" && repeatAnnouncements && w.broker.repeatedAnnouncements > 0)
                entered.add("same instance repeated");
              reach.recordHistory(entered);
            } finally {
              if (announcements !== null) clearInterval(announcements);
              thief.kill();
              for (const cleanup of cleanups.splice(0).reverse()) cleanup();
            }
          },
        ),
        { seed, numRuns: 80 },
      );
      reach.assertReached();
    },
    90_000,
  );
});
