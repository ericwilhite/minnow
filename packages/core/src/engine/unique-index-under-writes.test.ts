/**
 * UNIQUE indexes under a steady stream of single-row commits, on the memory, IndexedDB, and
 * OPFS stores, with one or two connections.
 *
 * A UNIQUE index's postings fold their delta tail into a base like any index's. That fold used
 * to restage the whole key set and publish it only over an unchanged manifest, so any commit
 * during the fold failed it, invalidated the postings, and reported a `WriteConflictError`; on
 * IndexedDB the fold could never succeed even on an idle table, because the store refuses to
 * replace a populated membership. A first build -- `CREATE UNIQUE INDEX` on a populated table --
 * raced every commit the same way, and could return with the index taken over by another
 * connection's build and not yet enforcing anything, even over duplicate rows. Now the statement
 * builds again inside the writer turn when commits outrun it; a background build of an abandoned
 * one stays quiet until a run of losses shows no progress; and a fold that a newly enforcing
 * index lands under plans again instead of reporting.
 *
 * Throughout, uniqueness must hold: a duplicate is refused, and of two connections inserting the
 * same value at once exactly one wins.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { OpfsBlockStore } from "../storage/opfs/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase, type MinnowDatabaseOptions } from "./database.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

/** Connections over one database: each opens its own store, except memory, which is shared. */
interface Shared {
  connect(wrap?: (store: BlockStore) => BlockStore): Promise<MinnowDatabase>;
  /** A store to read catalog and postings state from, outside every connection. */
  inspect: BlockStore;
  close(): Promise<void>;
}

const stores: Array<{ name: string; open: () => Promise<Shared> }> = [
  {
    name: "memory",
    open: async () => {
      const store = new MemoryBlockStore();
      return shared(async () => store, true);
    },
  },
  {
    name: "indexeddb",
    open: async () => {
      const indexedDB = new IDBFactory();
      const name = crypto.randomUUID();
      return shared(() => IndexedDbBlockStore.open({ name, indexedDB }));
    },
  },
  {
    name: "opfs",
    open: async () => {
      const root = new MemoryOpfs().root;
      return shared(() => OpfsBlockStore.open({ name: "unique-under-writes", root }));
    },
  },
];

/**
 * `oneStore`: every connection is handed the same store (memory), closed once at the end. Its
 * connections take writer turns in one queue per store object, so they all get the first
 * connection's wrapper too: a wrapper per connection would give each a queue of its own.
 */
async function shared(openStore: () => Promise<BlockStore>, oneStore = false): Promise<Shared> {
  const opened: Array<{ db: MinnowDatabase; store: BlockStore }> = [];
  const inspect = await openStore();
  let wrapped: BlockStore | undefined;
  return {
    inspect,
    connect: async (wrap = (store) => store) => {
      const store = await openStore();
      const handed = oneStore ? (wrapped ??= wrap(store)) : wrap(store);
      const db = new MinnowDatabase(handed, {
        rowsPerBlock: 8,
        onBackgroundError: (error, context) => reports.push({ error, context }),
      } satisfies MinnowDatabaseOptions);
      opened.push({ db, store });
      return db;
    },
    close: async () => {
      // close joins every in-flight fold and build first.
      for (const { db } of opened) await db.close();
      if (!oneStore) for (const { store } of opened) store.close();
      inspect.close();
    },
  };
}

let reports: Array<{ error: unknown; context: string }> = [];

function reported(): string[] {
  return reports.map(
    ({ error, context }) =>
      `${context}: ${error instanceof Error ? `${error.name}: ${error.message}` : String(error)}`,
  );
}

async function indexState(store: BlockStore, table: string, name: string) {
  const record = await store.getTableByName(table);
  const index = Object.values(record?.secondaryIndexes ?? {}).find(
    (candidate) => candidate.name === name,
  );
  const version = (await store.getCurrentManifestVersion()) ?? -1;
  const tail =
    record === undefined || index === undefined || index.state === "invalid"
      ? undefined
      : (await store.readFtsCandidates(record.id, index.storageColumnId, [], version))
          .deltaChunkCount;
  return { state: index?.state, enforced: index?.uniqueEnforced === true, tail };
}

/** Wraps `store` so `hooks` intercept calls; every other call reaches the store bound to it. */
function intercept(store: BlockStore, hooks: Partial<BlockStore>): BlockStore {
  return new Proxy(store, {
    get(target, property) {
      if (Object.hasOwn(hooks, property)) return hooks[property as keyof BlockStore];
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? (value as () => unknown).bind(target) : value;
    },
  });
}

describe.each(stores)("UNIQUE index under single-row commits ($name)", ({ open }) => {
  it("keeps a UNIQUE column's index served through a tight insert loop", async () => {
    reports = [];
    const database = await open();
    try {
      const staged: string[] = [];
      const db = await database.connect((store) =>
        intercept(store, {
          beginUniqueKeyBuild: async (input) => {
            staged.push(input.buildId);
            return store.beginUniqueKeyBuild(input);
          },
        }),
      );
      await db.execute(
        "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT UNIQUE, n INTEGER NOT NULL)",
      );
      // Each insert its own commit: the index tail crosses its fold bound again and again.
      for (let id = 0; id < 120; id += 1) {
        await db.execute("INSERT INTO people VALUES (?, ?, ?)", [id, `e${String(id)}`, id]);
      }
      await expect(
        db.execute("INSERT INTO people VALUES (?, ?, ?)", [1_000, "e7", 0]),
      ).rejects.toThrow(/unique|duplicate/i);
      // The folds land without a query asking for a rebuild: none of them failed.
      await vi.waitFor(
        async () => {
          expect(await indexState(database.inspect, "people", "people_email_key")).toMatchObject({
            state: "ready",
            enforced: true,
          });
        },
        { timeout: 60_000, interval: 50 },
      );
      const { tail } = await indexState(database.inspect, "people", "people_email_key");
      expect(tail).toBeLessThanOrEqual(64);
      // An enforced membership is maintained by every commit; a fold rebuilds postings only.
      expect(staged).toEqual([]);
      expect(
        (await db.query("SELECT id FROM people WHERE email = ?", { params: ["e93"] })).rows,
      ).toEqual([{ id: 93 }]);
      expect(reported()).toEqual([]);
    } finally {
      await database.close();
    }
  });

  it("lets exactly one of two connections inserting one value win while folds run", async () => {
    reports = [];
    const database = await open();
    try {
      const left = await database.connect();
      const right = await database.connect();
      await left.execute(
        "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT UNIQUE, n INTEGER NOT NULL)",
      );
      let next = 0;
      for (let round = 0; round < 40; round += 1) {
        const email = `shared${String(round)}`;
        const outcomes = await Promise.allSettled([
          left.execute("INSERT INTO people VALUES (?, ?, ?)", [next, email, round]),
          right.execute("INSERT INTO people VALUES (?, ?, ?)", [next + 1, email, round]),
        ]);
        next += 2;
        expect(outcomes.filter((outcome) => outcome.status === "fulfilled")).toHaveLength(1);
        const [refused] = outcomes.flatMap((outcome) =>
          outcome.status === "rejected" ? [outcome.reason as Error] : [],
        );
        expect(refused?.message).toMatch(/unique|duplicate/i);
        // Single-row commits from both sides keep the index tail crossing its fold bound.
        await (round % 2 === 0 ? left : right).execute("INSERT INTO people VALUES (?, ?, ?)", [
          next,
          `solo${String(round)}`,
          round,
        ]);
        next += 1;
      }
      const duplicates = await left.query(
        "SELECT email FROM people GROUP BY email HAVING COUNT(*) > 1",
        { memoize: false },
      );
      expect(duplicates.rows).toEqual([]);
      expect((await right.query("SELECT COUNT(*) AS n FROM people")).rows).toEqual([{ n: 80 }]);
      expect(reported()).toEqual([]);
    } finally {
      await database.close();
    }
  });

  it("publishes CREATE UNIQUE INDEX inside the writer turn when commits land during its build", async () => {
    reports = [];
    const database = await open();
    try {
      // The writer stops well short of the index's tail bound, so only publication is contested.
      const run = await createUnderWriter(database, { parkUntil: 24, writes: 30 });
      expect(run.outcome).toBe("created");
      // The first attempt found the manifest moved and built again inside the turn instead of
      // trying a publication that had to fail; that one could not lose.
      expect(run.finishes).toEqual(["published"]);
      expect(run.stagedBuilds).toBe(2);
      await expectEnforced(database, run);
      expect(reported()).toEqual([]);
    } finally {
      await database.close();
    }
  });

  it("finishes CREATE UNIQUE INDEX while another connection commits past the index's tail bound", async () => {
    reports = [];
    const database = await open();
    try {
      // The writer's commits outgrow the tail bound while the first attempt builds, so its
      // connection cuts the tail and the attempt's build with it. The writer never pauses.
      const run = await createUnderWriter(database, { parkUntil: 72, writes: 4_000 });
      expect(run.outcome).toBe("created");
      expect(run.finishes).toEqual(["published"]);
      await expectEnforced(database, run);
      expect(reported()).toEqual([]);
    } finally {
      await database.close();
    }
  });

  it("plans a fold again when a UNIQUE index began enforcing under it", async () => {
    reports = [];
    const database = await open();
    try {
      const peer: { other?: MinnowDatabase } = {};
      let armed = false;
      let raced = false;
      // The fold has read its table; before it begins its transaction, another connection's
      // CREATE UNIQUE INDEX finishes. The transaction is current, the table record it covers
      // UNIQUE memberships from is not, and its commit cannot name one it never saw.
      const folder = await database.connect((store) =>
        intercept(store, {
          getCompactionJob: async (id) => {
            if (armed && !raced && peer.other !== undefined) {
              raced = true;
              await peer.other.execute("CREATE UNIQUE INDEX people_email ON people (email)");
            }
            return store.getCompactionJob(id);
          },
        }),
      );
      peer.other = await database.connect();
      await folder.execute(
        "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT NOT NULL, n INTEGER NOT NULL)",
      );
      for (let id = 0; id < 40; id += 1) {
        await folder.execute("INSERT INTO people VALUES (?, ?, ?)", [id, `e${String(id)}`, id]);
      }
      armed = true;
      await folder.compactTable("people");
      expect(raced).toBe(true);
      expect((await indexState(database.inspect, "people", "people_email")).enforced).toBe(true);
      await expect(
        folder.execute("INSERT INTO people VALUES (?, ?, ?)", [100, "e7", 0]),
      ).rejects.toThrow(/unique|duplicate/i);
      expect((await folder.query("SELECT COUNT(*) AS n FROM people")).rows).toEqual([{ n: 40 }]);
      expect(reported()).toEqual([]);
    } finally {
      await database.close();
    }
  });

  it("retries an abandoned UNIQUE build quietly while commits outrun it, and reports a run of losses", async () => {
    reports = [];
    const database = await open();
    try {
      const peer: { other?: MinnowDatabase } = {};
      let outrun = true;
      let begun = 0;
      // Every background build of the index begins its key set after its snapshot; while
      // `outrun` holds, another connection commits right then, so the publication finds the
      // manifest moved. Background work must not hold writers for a whole build, so each such
      // build is a race lost: quiet, until a run of them shows the build is not progressing.
      const reader = await database.connect((store) =>
        intercept(store, {
          beginUniqueKeyBuild: async (input) => {
            const started = await store.beginUniqueKeyBuild(input);
            begun += 1;
            if (outrun && peer.other !== undefined) {
              await peer.other.execute("INSERT INTO people VALUES (?, ?, ?)", [
                1_000 + begun,
                `x${String(begun)}`,
                0,
              ]);
            }
            return started;
          },
        }),
      );
      const other = await database.connect();
      peer.other = other;
      await other.execute(
        "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT NOT NULL, n INTEGER NOT NULL)",
      );
      await other.insertBatch(
        "people",
        Array.from({ length: 200 }, (_, id) => ({ id, email: `e${String(id)}`, n: id })),
      );
      // A CREATE UNIQUE INDEX whose tab died before its index enforced, and whose delta tail
      // was cut since: invalid, never enforced, left for another connection to finish.
      const table = await database.inspect.getTableByName("people");
      const email = table?.columns.find((column) => column.name === "email");
      if (table === undefined || email === undefined) throw new Error("No table");
      await database.inspect.updateTable(table.id, table.revision, {
        secondaryIndexes: {
          abandoned: {
            name: "people_email",
            columnId: email.id,
            columnIds: [email.id],
            directions: ["asc"],
            unique: true,
            termEncoding: "tuple-v2",
            storage: "postings-v1",
            storageColumnId: "secondary-index:abandoned",
            locator: "key-hash-v1",
            state: "invalid",
            buildFromVersion: -1,
          },
        },
      });
      const query = async () =>
        (
          await reader.query("SELECT id FROM people WHERE email = ?", {
            params: ["e42"],
            memoize: false,
          })
        ).rows;
      // A query over the index schedules a build unless one is still settling, so each poll asks
      // again until this attempt's build has begun, and no further.
      for (let attempt = 1; attempt <= 8; attempt += 1) {
        await vi.waitFor(
          async () => {
            if (begun < attempt) expect(await query()).toEqual([{ id: 42 }]);
            expect(begun).toBe(attempt);
            expect((await indexState(database.inspect, "people", "people_email")).state).toBe(
              "invalid",
            );
            // Seven in a row stay quiet; the eighth reports, once.
            expect(reported()).toHaveLength(attempt < 8 ? 0 : 1);
          },
          { timeout: 30_000, interval: 10 },
        );
      }
      expect(reported()[0]).toMatch(
        /^secondary index build for people\/abandoned \(8 attempts in a row without progress\): WriteConflictError/,
      );
      outrun = false;
      await vi.waitFor(
        async () => {
          expect(await query()).toEqual([{ id: 42 }]);
          expect(await indexState(database.inspect, "people", "people_email")).toMatchObject({
            state: "ready",
            enforced: true,
          });
        },
        { timeout: 30_000, interval: 10 },
      );
      await expect(
        other.execute("INSERT INTO people VALUES (?, ?, ?)", [5_000, "e7", 0]),
      ).rejects.toThrow(/unique|duplicate/i);
      expect(reported()).toHaveLength(1);
    } finally {
      await database.close();
    }
  });

  it("fails CREATE UNIQUE INDEX over a duplicate another connection commits during it", async () => {
    reports = [];
    const database = await open();
    try {
      // Every tenth row repeats an email the table already holds.
      const run = await createUnderWriter(database, {
        parkUntil: 24,
        writes: 4_000,
        email: (id) => (id % 10 === 0 ? `e${String(id % 1_000)}` : `w${String(id)}`),
      });
      const duplicates = await run.writer.query(
        "SELECT email FROM people GROUP BY email HAVING COUNT(*) > 1",
        { memoize: false },
      );
      // The duplicates landed before the index could enforce, so the statement fails and leaves
      // no index. An index that reported success would be enforcing over duplicates.
      expect(duplicates.rows.length).toBeGreaterThan(0);
      expect(run.outcome).toBeInstanceOf(Error);
      expect((run.outcome as Error).message).toMatch(/duplicate/i);
      expect((await indexState(database.inspect, "people", "people_email")).state).toBe(undefined);
      expect(reported()).toEqual([]);
    } finally {
      await database.close();
    }
  });
});

interface CreateRun {
  outcome: "created" | Error;
  builder: MinnowDatabase;
  writer: MinnowDatabase;
  writerCommits: number;
  /** Each UNIQUE publication the builder attempted: "published", or the refusal's name. */
  finishes: string[];
  /** UNIQUE key sets the builder began staging. */
  stagedBuilds: number;
}

/**
 * `CREATE UNIQUE INDEX` over 1,000 rows while a second connection commits one row at a time.
 * The first build attempt parks after its snapshot until the writer has committed `parkUntil`
 * rows, so commits land inside it however quickly the store builds a small table. The writer
 * stops after `writes` rows or once the statement settles, whichever is first.
 */
async function createUnderWriter(
  database: Shared,
  options: { parkUntil: number; writes: number; email?: (id: number) => string },
): Promise<CreateRun> {
  const email = options.email ?? ((id: number) => `w${String(id)}`);
  const run = { writerCommits: 0, finishes: [] as string[], stagedBuilds: 0 };
  let parked: Promise<void> | undefined;
  const builder = await database.connect((store) =>
    intercept(store, {
      writeFtsBaseBuildChunk: async (input) => {
        parked ??= vi.waitFor(
          () => expect(run.writerCommits).toBeGreaterThanOrEqual(options.parkUntil),
          { timeout: 60_000, interval: 5 },
        );
        await parked;
        return store.writeFtsBaseBuildChunk(input);
      },
      beginUniqueKeyBuild: async (input) => {
        run.stagedBuilds += 1;
        return store.beginUniqueKeyBuild(input);
      },
      finishUniqueKeyBuild: async (input) => {
        try {
          const published = await store.finishUniqueKeyBuild(input);
          run.finishes.push("published");
          return published;
        } catch (error) {
          run.finishes.push(error instanceof Error ? error.name : String(error));
          throw error;
        }
      },
    }),
  );
  const writer = await database.connect();
  await builder.execute(
    "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT NOT NULL, n INTEGER NOT NULL)",
  );
  await builder.insertBatch(
    "people",
    Array.from({ length: 1_000 }, (_, id) => ({ id, email: `e${String(id)}`, n: id })),
  );
  // Set when the statement settles; read through the object, which narrowing leaves alone.
  const statement = { settled: false };
  const writing = (async () => {
    for (let id = 10_000; !statement.settled && id < 10_000 + options.writes; id += 1) {
      await writer.execute("INSERT INTO people VALUES (?, ?, ?)", [id, email(id), id]).then(
        () => undefined,
        (error: unknown) => {
          if (!/unique|duplicate/i.test((error as Error).message)) throw error;
        },
      );
      run.writerCommits += 1;
    }
  })();
  const outcome = await builder
    .execute("CREATE UNIQUE INDEX people_email ON people (email)")
    .then(
      () => "created" as const,
      (error: unknown) => error as Error,
    )
    .finally(() => {
      statement.settled = true;
    });
  await writing;
  expect(run.writerCommits).toBeGreaterThanOrEqual(options.parkUntil);
  return { outcome, builder, writer, ...run };
}

/** The statement returned with the index enforcing in every connection, not merely marked. */
async function expectEnforced(database: Shared, run: CreateRun): Promise<void> {
  expect((await indexState(database.inspect, "people", "people_email")).enforced).toBe(true);
  for (const db of [run.builder, run.writer]) {
    await expect(
      db.execute("INSERT INTO people VALUES (?, ?, ?)", [50_000, "e7", 0]),
    ).rejects.toThrow(/unique|duplicate/i);
    await expect(
      db.execute("INSERT INTO people VALUES (?, ?, ?)", [50_001, "w10003", 0]),
    ).rejects.toThrow(/unique|duplicate/i);
  }
  expect(
    (await run.writer.query("SELECT id FROM people WHERE email = ?", { params: ["w10005"] })).rows,
  ).toEqual([{ id: 10_005 }]);
}
