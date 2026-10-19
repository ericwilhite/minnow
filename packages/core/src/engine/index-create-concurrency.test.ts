/**
 * `CREATE INDEX ... IF NOT EXISTS` against another connection's build of the same index, and
 * builds whose connection died, on the memory, IndexedDB, and OPFS stores.
 *
 * IF NOT EXISTS used to succeed the moment it saw the name, while the connection that created
 * the index was still building it. For a UNIQUE index that meant success with nothing enforcing:
 * a duplicate written next was accepted, the first CREATE then failed on it and removed the
 * index, and the second caller had been told the index existed. Now IF NOT EXISTS waits for the
 * creator's statement, and succeeds only once the index is ready (a UNIQUE one enforcing); when
 * that statement fails, it creates the index itself and reports its own outcome.
 *
 * A build's session also outlived its connection by its whole one-hour lease: a tab that closed
 * mid-build kept every other connection's build of the index -- a waiting CREATE, a query's
 * rebuild, a full-text build -- waiting that long. Every engine now holds a Web Lock for as long
 * as it is open, and a session whose owner holds none is taken over at once. A live owner's
 * build is never taken.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  type AppendPostingBuildChunkInput,
  type AppendUniqueKeyBuildChunkInput,
  type BeginPostingBuildInput,
  type BeginUniqueKeyBuildInput,
  type BlockStore,
  type UniqueKeyBuildRecord,
} from "../storage/index.js";
import { OpfsBlockStore } from "../storage/opfs/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";
import { departOwnerForTests } from "./owner-presence.js";
import { column, schema, table } from "./schema.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

/**
 * Older Node releases (22.12, for one) have no Web Locks: a store other contexts can share then
 * gives no proof that a build's owner is gone, and its session lasts out its lease.
 */
const webLocks = (globalThis as { navigator?: { locks?: unknown } }).navigator?.locks !== undefined;

/** Far below the one-hour build lease a departed owner's session used to hold. */
const PROMPT_MS = 15_000;

interface Hooks {
  writeFtsBaseBuildChunk?: (
    store: BlockStore,
    input: AppendPostingBuildChunkInput,
  ) => Promise<void>;
  appendUniqueKeyBuildChunk?: (
    store: BlockStore,
    input: AppendUniqueKeyBuildChunkInput,
  ) => Promise<UniqueKeyBuildRecord>;
  beginFtsBaseBuild?: (store: BlockStore, input: BeginPostingBuildInput) => Promise<void>;
  beginUniqueKeyBuild?: (
    store: BlockStore,
    input: BeginUniqueKeyBuildInput,
  ) => Promise<UniqueKeyBuildRecord>;
}

/** Applied to every connection's store at call time; hooks tell connections apart by owner id. */
let hooks: Hooks = {};

function hooked(store: BlockStore): BlockStore {
  return new Proxy(store, {
    get(target, property) {
      const hook = (hooks as Record<PropertyKey, unknown>)[property];
      if (typeof hook === "function") {
        return (...args: unknown[]) => (hook as (...all: unknown[]) => unknown)(target, ...args);
      }
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? (value as () => unknown).bind(target) : value;
    },
  });
}

interface Shared {
  connect(): Promise<MinnowDatabase>;
  /** Reads catalog state outside every connection. */
  inspect: BlockStore;
  close(): Promise<void>;
}

const stores: Array<{ name: string; open: () => Promise<Shared> }> = [
  {
    name: "memory",
    open: async () => {
      // One store object for every connection, so they take writer turns in one queue.
      const store = hooked(new MemoryBlockStore());
      return shared(async () => store, true);
    },
  },
  {
    name: "indexeddb",
    open: async () => {
      const indexedDB = new IDBFactory();
      const name = crypto.randomUUID();
      return shared(async () => hooked(await IndexedDbBlockStore.open({ name, indexedDB })));
    },
  },
  {
    name: "opfs",
    open: async () => {
      const root = new MemoryOpfs().root;
      const name = `index-create-${crypto.randomUUID()}`;
      return shared(async () => hooked(await OpfsBlockStore.open({ name, root })));
    },
  },
];

let reports: string[] = [];

async function shared(openStore: () => Promise<BlockStore>, oneStore = false): Promise<Shared> {
  const opened: Array<{ db: MinnowDatabase; store: BlockStore }> = [];
  const inspect = await openStore();
  return {
    inspect,
    connect: async () => {
      const store = await openStore();
      const db = new MinnowDatabase(store, {
        rowsPerBlock: 8,
        onBackgroundError: (error, context) => {
          reports.push(`${context}: ${error instanceof Error ? error.message : String(error)}`);
        },
      });
      opened.push({ db, store });
      return db;
    },
    close: async () => {
      for (const { db } of opened) await db.close();
      if (!oneStore) for (const { store } of opened) store.close();
      inspect.close();
    },
  };
}

/** The first owner to reach `method` parks there -- and, with `fail`, then throws -- until released. */
function parkFirst(
  method: "writeFtsBaseBuildChunk" | "appendUniqueKeyBuildChunk",
  fail?: Error,
): { reached: Promise<void>; release: () => void; owner: () => string } {
  let owner: string | undefined;
  let reach = (): void => undefined;
  const reached = new Promise<void>((resolve) => {
    reach = resolve;
  });
  let release = (): void => undefined;
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  const park = async (input: { ownerId: string }): Promise<void> => {
    owner ??= input.ownerId;
    if (input.ownerId !== owner) return;
    reach();
    await released;
    if (fail !== undefined) throw fail;
  };
  if (method === "writeFtsBaseBuildChunk") {
    hooks.writeFtsBaseBuildChunk = async (store, input) => {
      await park(input);
      return store.writeFtsBaseBuildChunk(input);
    };
  } else {
    hooks.appendUniqueKeyBuildChunk = async (store, input) => {
      await park(input);
      return store.appendUniqueKeyBuildChunk(input);
    };
  }
  return {
    reached,
    release,
    owner: () => {
      if (owner === undefined) throw new Error("Nothing reached the park yet");
      return owner;
    },
  };
}

/** Records the owner of every build session and staged key set that began. */
function recordBuilds(): Array<{ kind: "postings" | "keys"; ownerId: string }> {
  const begun: Array<{ kind: "postings" | "keys"; ownerId: string }> = [];
  hooks.beginFtsBaseBuild = async (store, input) => {
    await store.beginFtsBaseBuild(input);
    begun.push({ kind: "postings", ownerId: input.ownerId });
  };
  hooks.beginUniqueKeyBuild = async (store, input) => {
    const record = await store.beginUniqueKeyBuild(input);
    begun.push({ kind: "keys", ownerId: input.ownerId });
    return record;
  };
  return begun;
}

/** A statement's outcome, readable before it settles. */
function track(statement: Promise<unknown>): { settled: () => boolean; outcome: Promise<string> } {
  const state = { settled: false };
  const outcome = statement.then(
    () => "ok",
    (error: unknown) =>
      error instanceof Error ? `${error.name}: ${error.message}` : String(error),
  );
  void outcome.then(() => {
    state.settled = true;
  });
  return { settled: () => state.settled, outcome };
}

async function seed(db: MinnowDatabase, rows = 200): Promise<void> {
  await db.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT NOT NULL)");
  await db.insertBatch(
    "people",
    Array.from({ length: rows }, (_, id) => ({ id, email: `e${String(id)}` })),
  );
}

async function indexNamed(store: BlockStore, name: string) {
  for (const record of await store.listTables()) {
    for (const index of Object.values(record.secondaryIndexes ?? {})) {
      if (index.name === name) return index;
    }
  }
  return undefined;
}

async function expectEnforced(...connections: MinnowDatabase[]): Promise<void> {
  for (const [position, db] of connections.entries()) {
    await expect(
      db.execute("INSERT INTO people VALUES (?, ?)", [50_000 + position, "e7"]),
    ).rejects.toThrow(/unique|duplicate/i);
  }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe.each(stores)("CREATE INDEX IF NOT EXISTS beside another build ($name)", ({ open }) => {
  it("waits for another connection's CREATE UNIQUE INDEX and succeeds once it enforces", async () => {
    hooks = {};
    reports = [];
    const database = await open();
    try {
      const creator = await database.connect();
      const waiter = await database.connect();
      await seed(creator);
      const begun = recordBuilds();
      const park = parkFirst("writeFtsBaseBuildChunk");
      const creating = track(creator.execute("CREATE UNIQUE INDEX people_email ON people (email)"));
      await park.reached;
      const waiting = track(
        waiter.execute("CREATE UNIQUE INDEX IF NOT EXISTS people_email ON people (email)"),
      );
      // The index is named but nothing enforces it yet: success now would be a lie.
      await sleep(300);
      expect(waiting.settled()).toBe(false);
      park.release();
      expect(await creating.outcome).toBe("ok");
      expect(await waiting.outcome).toBe("ok");
      expect((await indexNamed(database.inspect, "people_email"))?.uniqueEnforced).toBe(true);
      await expectEnforced(creator, waiter);
      // The waiter left the live creator's build alone: every build was the creator's.
      expect(begun.length).toBeGreaterThan(0);
      expect(begun.every(({ ownerId }) => ownerId === park.owner())).toBe(true);
      expect(reports).toEqual([]);
    } finally {
      await database.close();
    }
  });

  it("fails with a CREATE UNIQUE INDEX that finds a duplicate, and leaves no index", async () => {
    hooks = {};
    reports = [];
    const database = await open();
    try {
      const creator = await database.connect();
      const waiter = await database.connect();
      const writer = await database.connect();
      await seed(creator);
      const park = parkFirst("writeFtsBaseBuildChunk");
      const creating = track(creator.execute("CREATE UNIQUE INDEX people_email ON people (email)"));
      await park.reached;
      const waiting = track(
        waiter.execute("CREATE UNIQUE INDEX IF NOT EXISTS people_email ON people (email)"),
      );
      // Not enforced yet, so this duplicate lands -- and then no UNIQUE index can be built.
      await writer.execute("INSERT INTO people VALUES (1000, 'e7')");
      expect(waiting.settled()).toBe(false);
      park.release();
      expect(await creating.outcome).toMatch(/Duplicate value for people\.email/);
      // Its own attempt, after the creator's failed: the same duplicate, the same failure.
      expect(await waiting.outcome).toMatch(/Duplicate value for people\.email/);
      expect(await indexNamed(database.inspect, "people_email")).toBeUndefined();
      expect(reports).toEqual([]);
    } finally {
      await database.close();
    }
  });

  it("creates the index itself when another connection's CREATE INDEX fails", async () => {
    hooks = {};
    reports = [];
    const database = await open();
    try {
      const creator = await database.connect();
      const waiter = await database.connect();
      await seed(creator);
      const park = parkFirst("writeFtsBaseBuildChunk", new Error("injected storage failure"));
      const creating = track(creator.execute("CREATE INDEX people_email ON people (email)"));
      await park.reached;
      const waiting = track(
        waiter.execute("CREATE INDEX IF NOT EXISTS people_email ON people (email)"),
      );
      await sleep(200);
      expect(waiting.settled()).toBe(false);
      park.release();
      expect(await creating.outcome).toBe("Error: injected storage failure");
      expect(await waiting.outcome).toBe("ok");
      expect((await indexNamed(database.inspect, "people_email"))?.state).toBe("ready");
      expect(
        (await creator.query("SELECT id FROM people WHERE email = ?", { params: ["e42"] })).rows,
      ).toEqual([{ id: 42 }]);
      expect(reports).toEqual([]);
    } finally {
      await database.close();
    }
  });

  it("agrees across connections that migrate and create one UNIQUE index at start-up", async () => {
    hooks = {};
    reports = [];
    const declared = schema([
      table("people", { id: column.number().unique(), email: column.string() }),
    ]);
    for (const duplicate of [false, true]) {
      const database = await open();
      try {
        const tabs = await Promise.all([1, 2, 3].map(() => database.connect()));
        const [first] = tabs;
        if (first === undefined) throw new Error("No connection");
        await first.migrate(declared);
        await first.insertBatch(
          "people",
          Array.from({ length: 300 }, (_, id) => ({
            id,
            email: duplicate && id === 299 ? "e0" : `e${String(id)}`,
          })),
        );
        const outcomes = await Promise.all(
          tabs.map(
            (tab) =>
              track(
                (async () => {
                  await tab.migrate(declared);
                  await tab.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS people_email ON people (email)",
                  );
                })(),
              ).outcome,
          ),
        );
        const index = await indexNamed(database.inspect, "people_email");
        if (duplicate) {
          // Every start-up reports the duplicate; none reports an index, and none is left.
          for (const outcome of outcomes) {
            expect(outcome).toMatch(/Duplicate value for people\.email/);
          }
          expect(index).toBeUndefined();
        } else {
          expect(outcomes).toEqual(["ok", "ok", "ok"]);
          expect(index?.uniqueEnforced).toBe(true);
          await expectEnforced(...tabs);
        }
        expect(reports).toEqual([]);
      } finally {
        await database.close();
      }
    }
  });

  it("stops waiting when its own connection closes", async () => {
    hooks = {};
    reports = [];
    const database = await open();
    const park = parkFirst("writeFtsBaseBuildChunk");
    try {
      const creator = await database.connect();
      const waiter = await database.connect();
      await seed(creator);
      const creating = track(creator.execute("CREATE UNIQUE INDEX people_email ON people (email)"));
      await park.reached;
      const waiting = track(
        waiter.execute("CREATE UNIQUE INDEX IF NOT EXISTS people_email ON people (email)"),
      );
      await sleep(100);
      const closing = performance.now();
      await waiter.close();
      // Closing never waits for another connection's statement.
      expect(performance.now() - closing).toBeLessThan(PROMPT_MS);
      expect(await waiting.outcome).toMatch(/Database is closed/);
      park.release();
      expect(await creating.outcome).toBe("ok");
    } finally {
      park.release();
      await database.close();
    }
  });
});

describe.each(stores)("index builds whose connection is gone ($name)", ({ name, open }) => {
  // A store other contexts can reach proves an owner gone only through Web Locks.
  const provable = webLocks || name === "memory";

  it.skipIf(!provable).each([
    { unique: true, parkedIn: "writeFtsBaseBuildChunk" as const },
    { unique: true, parkedIn: "appendUniqueKeyBuildChunk" as const },
    { unique: false, parkedIn: "writeFtsBaseBuildChunk" as const },
  ])(
    "finishes an index whose creator closed mid-build (unique: $unique, parked in $parkedIn)",
    async ({ unique, parkedIn }) => {
      hooks = {};
      reports = [];
      const database = await open();
      const park = parkFirst(parkedIn);
      try {
        const creator = await database.connect();
        const other = await database.connect();
        await seed(creator);
        const kind = unique ? "UNIQUE INDEX" : "INDEX";
        const creating = track(creator.execute(`CREATE ${kind} people_email ON people (email)`));
        await park.reached;
        // The creator's tab closes: its locks go, its build session and staged set stay.
        departOwnerForTests(park.owner());
        const started = performance.now();
        expect(
          await track(other.execute(`CREATE ${kind} IF NOT EXISTS people_email ON people (email)`))
            .outcome,
        ).toBe("ok");
        expect(performance.now() - started).toBeLessThan(PROMPT_MS);
        const index = await indexNamed(database.inspect, "people_email");
        expect(index?.state).toBe("ready");
        if (unique) {
          expect(index?.uniqueEnforced).toBe(true);
          await expectEnforced(other);
        }
        expect(
          (await other.query("SELECT id FROM people WHERE email = ?", { params: ["e42"] })).rows,
        ).toEqual([{ id: 42 }]);
        // Were the departed build ever to run on, the store refuses it: it changes nothing.
        park.release();
        await creating.outcome;
        expect((await indexNamed(database.inspect, "people_email"))?.state).toBe("ready");
        expect(reports).toEqual([]);
      } finally {
        park.release();
        await database.close();
      }
    },
  );

  it.skipIf(!provable)(
    "fails, leaving no index, when the build its creator abandoned finds a duplicate",
    async () => {
      hooks = {};
      reports = [];
      const database = await open();
      const park = parkFirst("writeFtsBaseBuildChunk");
      try {
        const creator = await database.connect();
        const other = await database.connect();
        await seed(creator);
        const creating = track(
          creator.execute("CREATE UNIQUE INDEX people_email ON people (email)"),
        );
        await park.reached;
        departOwnerForTests(park.owner());
        await other.execute("INSERT INTO people VALUES (1000, 'e7')");
        expect(
          await track(
            other.execute("CREATE UNIQUE INDEX IF NOT EXISTS people_email ON people (email)"),
          ).outcome,
        ).toMatch(/Duplicate value for people\.email/);
        // Nobody else is left to clean up: a failed first build leaves no index.
        expect(await indexNamed(database.inspect, "people_email")).toBeUndefined();
        park.release();
        await creating.outcome;
        expect(await indexNamed(database.inspect, "people_email")).toBeUndefined();
      } finally {
        park.release();
        await database.close();
      }
    },
  );

  it.skipIf(!provable)(
    "rebuilds an index from a query while a departed build holds it",
    async () => {
      hooks = {};
      reports = [];
      const database = await open();
      try {
        const reader = await database.connect();
        await seed(reader);
        const people = await database.inspect.getTableByName("people");
        const email = people?.columns.find((candidate) => candidate.name === "email");
        if (people === undefined || email === undefined) throw new Error("No table");
        // What a tab that died mid-CREATE INDEX leaves: a building index and its live session.
        await database.inspect.updateTable(people.id, people.revision, {
          secondaryIndexes: {
            departed: {
              name: "people_email",
              columnId: email.id,
              columnIds: [email.id],
              directions: ["asc"],
              termEncoding: "tuple-v2",
              storage: "postings-v1",
              storageColumnId: "secondary-index:departed",
              locator: "key-hash-v1",
              state: "building",
              buildId: "departed-build",
              buildFromVersion: -1,
            },
          },
        });
        const now = Date.now();
        await database.inspect.beginFtsBaseBuild({
          tableId: people.id,
          columnId: "secondary-index:departed",
          buildId: "departed-build",
          ownerId: "minnow/departed-tab",
          createdAt: new Date(now).toISOString(),
          expiresAt: new Date(now + 3_600_000).toISOString(),
        });
        // A query over a building index schedules its build; the departed session stops none.
        await vi.waitFor(
          async () => {
            expect(
              (
                await reader.query("SELECT id FROM people WHERE email = ?", {
                  params: ["e42"],
                  memoize: false,
                })
              ).rows,
            ).toEqual([{ id: 42 }]);
            expect((await indexNamed(database.inspect, "people_email"))?.state).toBe("ready");
          },
          { timeout: PROMPT_MS, interval: 20 },
        );
        expect(reports).toEqual([]);
      } finally {
        await database.close();
      }
    },
  );

  it.skipIf(!provable)(
    "builds a full-text index whose departed build holds its column",
    async () => {
      hooks = {};
      reports = [];
      const database = await open();
      const park = parkFirst("writeFtsBaseBuildChunk");
      try {
        const builder = await database.connect();
        const other = await database.connect();
        await builder.createTable({ name: "docs", columns: [{ name: "body", type: "string" }] });
        await builder.insertBatch("docs", {
          columns: { body: Array.from({ length: 64 }, (_, n) => `note ${String(n)} quick fox`) },
        });
        const building = track(builder.buildFtsIndex("docs", "body"));
        await park.reached;
        departOwnerForTests(park.owner());
        const started = performance.now();
        expect(await track(other.buildFtsIndex("docs", "body")).outcome).toBe("ok");
        expect(performance.now() - started).toBeLessThan(PROMPT_MS);
        expect(
          (await other.query("SELECT COUNT(*) AS n FROM docs WHERE MATCH(body) AGAINST 'quick'"))
            .rows,
        ).toEqual([{ n: 64 }]);
        park.release();
        await building.outcome;
        const docs = await database.inspect.getTableByName("docs");
        expect(Object.values(docs?.ftsColumns ?? {}).map((index) => index.state)).toEqual([
          "ready",
        ]);
      } finally {
        park.release();
        await database.close();
      }
    },
  );

  it("never takes over a live owner's build", async () => {
    hooks = {};
    reports = [];
    const database = await open();
    const park = parkFirst("writeFtsBaseBuildChunk");
    try {
      const creator = await database.connect();
      const waiter = await database.connect();
      const reader = await database.connect();
      await seed(creator);
      const begun = recordBuilds();
      const creating = track(creator.execute("CREATE UNIQUE INDEX people_email ON people (email)"));
      await park.reached;
      const waiting = track(
        waiter.execute("CREATE UNIQUE INDEX IF NOT EXISTS people_email ON people (email)"),
      );
      // Queries over the building index ask for its build again and again meanwhile.
      const until = performance.now() + 1_500;
      while (performance.now() < until) {
        await reader.query("SELECT id FROM people WHERE email = ?", {
          params: ["e9"],
          memoize: false,
        });
        await sleep(25);
      }
      expect(waiting.settled()).toBe(false);
      expect(begun.every(({ ownerId }) => ownerId === park.owner())).toBe(true);
      park.release();
      expect(await creating.outcome).toBe("ok");
      expect(await waiting.outcome).toBe("ok");
      expect(begun.every(({ ownerId }) => ownerId === park.owner())).toBe(true);
      await expectEnforced(creator, waiter, reader);
      expect(reports).toEqual([]);
    } finally {
      park.release();
      await database.close();
    }
  });
});
