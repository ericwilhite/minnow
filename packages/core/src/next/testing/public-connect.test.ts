/** Real native election/boot/SQL behind the client-only connection factory. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  PUBLIC_CONNECT_REACH,
  PUBLIC_CONNECT_CLOSE_REACH,
} from "../../testing/reach.js";
import { connectNativeClient } from "../owner/client-connect.js";
import type { OwnerHandle, StartOwner } from "../owner/connector.js";
import type { OwnerHost } from "../owner/host.js";
import type { HostContext } from "../owner/election.js";
import type { StepDown } from "../owner/protocol.js";
import { startNativeOwnerHost } from "../owner/native-boot.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { FakeLockRegistry } from "./fake-locks.js";

function deferred<T>() {
  let resolve: (value: T) => void = () => undefined,
    reject: (reason: unknown) => void = () => undefined;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  // Gates may be retired before a test reaches its await.
  promise.catch(() => undefined);
  return { promise, resolve, reject };
}
function nativeHosts(
  registry: FakeLockRegistry,
  backend: MemoryBackend,
  beforeOpen: () => Promise<void> = () => Promise.resolve(),
  beforeReturn: () => void = () => undefined,
) {
  const started: Array<Promise<OwnerHost>> = [],
    handles: OwnerHandle[] = [],
    settings: Array<Readonly<Record<string, unknown>>> = [];
  const startOwner: StartOwner = (init) => {
    const ready = deferred<string>(),
      stopped = deferred<StepDown["reason"]>();
    settings.push(init.settings);
    const host = startNativeOwnerHost(
      { ...init, t: "minnow:host" },
      async () => {
        await beforeOpen();
        return backend;
      },
      { memoryLimitBytes: 32 << 20 },
      {
        locks: registry.context(),
        post(status) {
          if (status.t === "minnow:ready") ready.resolve(status.ownerId);
          else if (status.t === "minnow:stopped") stopped.resolve(status.reason);
          else ready.reject(status.error);
        },
      },
    );
    host.catch((error: unknown) => {
      ready.reject(error);
      stopped.reject(error);
    });
    started.push(host);
    const handle: OwnerHandle = {
      ready: ready.promise,
      stopped: stopped.promise,
      lifecycle: (event) => void host.then((owner) => owner.lifecycle(event, { mobile: false })),
      stepDown: async (reason) => (await host).stop(reason, true),
      terminate: () => void host.then((owner) => owner.stop("closing", false)),
    };
    handles.push(handle);
    beforeReturn();
    return handle;
  };
  return { startOwner, started, handles, settings };
}
const context = (): HostContext => ({
  kind: "page",
  visible: true,
  platform: { mobile: false },
  superseded: false,
});

for (const seed of seedsFor("next-public-connect", [0xb758, 0xb759]))
  it(`elects native owners, freezes settings and reconnects exact rows (${String(seed)})`, async () => {
    const reach = createReach("native client connector", PUBLIC_CONNECT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.bigInt({ min: -(1n << 60n), max: 1n << 60n }),
        fc.string({ maxLength: 24 }).map((value) => value.replaceAll("\u0000", "�")),
        async (restart, value, text) => {
          reach.beginHistory();
          const backend = new MemoryBackend(),
            registry = new FakeLockRegistry(),
            gate = deferred<undefined>(),
            entered = deferred<undefined>();
          const owners = nativeHosts(registry, backend, async () => {
            entered.resolve(undefined);
            await gate.promise;
          });
          const settings = { memoryLimitBytes: 32 << 20 };
          const db = connectNativeClient({
            database: `public-connect-${String(seed)}`,
            locks: registry.context(),
            openBroker: null,
            startOwner: owners.startOwner,
            context,
            announcements: false,
            settings,
            lifecycleTarget: null,
          });
          // A different VALID cap makes uncaptured settings a direct assertion,
          // not an invalid-startup retry or timeout. Election has not taken its lock yet.
          settings.memoryLimitBytes = 64 << 20;
          try {
            const queued = db.query("SELECT $1::bigint n,$2::text v", [value, text]);
            await entered.promise;
            expect(db.status).toBe("connecting");

            gate.resolve(undefined);
            expect((await queued).tuples()).toEqual([[value, text]]);
            await db.ready();
            expect(db.status).toBe("ready");
            expect(owners.settings[0]?.memoryLimitBytes).toBe(32 << 20);
            reach.reach("queued SQL and captured settings");
            await db.execute("CREATE TABLE t(id INT PRIMARY KEY,n BIGINT,v TEXT)");
            await db.execute("INSERT INTO t VALUES(1,$1,$2)", [value, text]);
            if (restart) {
              const first = owners.handles[0];
              if (first === undefined) throw new Error("No elected owner");
              await first.stepDown?.("closing");
              await first.stopped;
              // Existing connector gets its actual step-down/host-lock notifications;
              // no new dispatcher, polling timer or hand-made reconnect is substituted.
              await db.ready();

              reach.reach("native owner restart and recovery");
            } else reach.reach("native owner retained");
            expect((await db.query("SELECT n,v FROM t")).tuples()).toEqual([[value, text]]);
            await db.execute("INSERT INTO t VALUES(2,$1,$2)", [value + 1n, text]);
            expect((await db.query("SELECT n,v FROM t ORDER BY id")).tuples()).toEqual([
              [value, text],
              [value + 1n, text],
            ]);
            expect(owners.started).toHaveLength(restart ? 2 : 1);
            reach.reach("exact continued native writes");
          } finally {
            gate.resolve(undefined);
            const closing = db.close();
            expect(db.close()).toBe(closing);
            await closing;
            expect(db.status).toBe("closed");
            await Promise.all(owners.handles.map((handle) => handle.stopped));
            expect(registry.query()).toEqual({ held: [], pending: [] });
            // Real lease release, not just client protocol shutdown.
            const lease = await backend.acquire();
            await lease.release();
            reach.reach("all started owners and locks drained");
          }
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [false, 9007199254740993n, "水"],
          [true, -9007199254740993n, "\n🙂"],
        ],
      },
    );
    reach.assertReached();
  }, 60000);

for (const seed of seedsFor("next-public-connect-close", [0xb75a, 0xb75b]))
  it(`drains blocked and reentrant owner startup on close (${String(seed)})`, async () => {
    const reach = createReach("native connector startup close", PUBLIC_CONNECT_CLOSE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (reentrant) => {
        reach.beginHistory();
        const backend = new MemoryBackend(),
          registry = new FakeLockRegistry(),
          entered = deferred<undefined>(),
          gate = deferred<undefined>();
        let closing: Promise<void> | undefined;
        const owners = nativeHosts(
          registry,
          backend,
          async () => {
            entered.resolve(undefined);
            await gate.promise;
          },
          () => {
            if (reentrant) closing = db.close();
          },
        );
        const db = connectNativeClient({
          database: `public-close-${String(seed)}`,
          locks: registry.context(),
          openBroker: null,
          startOwner: owners.startOwner,
          context,
          announcements: false,
          lifecycleTarget: null,
        });
        const result = db.query("SELECT 1").then(
          () => ({ code: "unexpected success" }),
          (error: unknown) => error,
        );
        try {
          await entered.promise;
          closing ??= db.close();
          let drained = false;
          const retired = closing.then(() => {
            drained = true;
          });
          // A whole task turn, not a wall-clock race: backend startup is still gated.
          await new Promise<void>((resolve) => setImmediate(resolve));
          expect(drained).toBe(false);
          gate.resolve(undefined);
          await retired;
          expect(await result).toMatchObject({ code: "MINNOW_CLIENT_CLOSED" });
          expect(registry.query()).toEqual({ held: [], pending: [] });
          expect(owners.handles).toHaveLength(1);
          await owners.handles[0]?.stopped;
          const lease = await backend.acquire();
          await lease.release();
          reach.reach(reentrant ? "reentrant" : "blocked");
        } finally {
          gate.resolve(undefined);
          await db.close();
          await Promise.allSettled(owners.handles.map((handle) => handle.stopped));
        }
      }),
      { seed, numRuns: 6, examples: [[false], [true]] },
    );
    reach.assertReached();
  }, 60000);
