/* eslint-disable no-restricted-imports -- Released-writer and failure-injection tests run only in Node. */
/**
 * Layouts 7 to 12 to 13: the store's bytes stay as they are and only the marker moves. Layout 8
 * admits compaction jobs with replayed merge plans, which a layout-7 reader cannot parse;
 * layout 9 admits WAL frames whose payload spans continuation frames and checkpointed index
 * deltas of any size, which a layout-8 reader cannot; layout 10 admits the internal key-forwards
 * catalog record, which a layout-9 reader would read as an ordinary table while ignoring its
 * forwards; layout 11 admits compaction plans whose sources lack columns added after them or
 * keep only their key, which a layout-10 reader refuses as corruption; layout 12 admits delta
 * merges, which a layout-11 reader refuses as corruption; layout 13 admits a column declared
 * TIMESTAMP without time zone, which a layout-12 reader would render as `timestamp with time
 * zone`. For each released layout these tests use its frozen fixture and its last released
 * writer — 0.12.1 for layout 7, 0.13.1 for layout 8, 0.14.1 for layout 9, 0.15.0 for layout 10
 * (also schema-change-fold-upgrade.test.ts and delta-merge-upgrade.test.ts), 0.16.0 for layout
 * 12 (timestamp-kind-upgrade.test.ts) — upgrade through the ordinary open API, keep writing,
 * reopen, and check that the released reader is refused afterwards without a byte changing.
 * Layout 11 was never released (0.16.0 writes layout 12), so its marker step runs over the
 * layout-10 fixture under a layout-11 marker. Every I/O boundary of the marker publication is
 * cut, by tab death and by power loss, and retried.
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { OpfsBlockStore as Layout10OpfsStore } from "@minnowdb/core-layout10/storage/opfs";
import { OpfsBlockStore as Layout12OpfsStore } from "@minnowdb/core-layout12/storage/opfs";
import { MinnowDatabase as ReleasedDatabase } from "@minnowdb/core-layout7";
import { OpfsBlockStore as ReleasedOpfsStore } from "@minnowdb/core-layout7/storage/opfs";
import { MinnowDatabase as Layout8Database } from "@minnowdb/core-layout8";
import { OpfsBlockStore as Layout8OpfsStore } from "@minnowdb/core-layout8/storage/opfs";
import { MinnowDatabase as Layout9Database } from "@minnowdb/core-layout9";
import { OpfsBlockStore as Layout9OpfsStore } from "@minnowdb/core-layout9/storage/opfs";
import { MinnowDatabase } from "../../engine/database.js";
import { internalsOf } from "../../engine/internals.js";
import { keyForwardsTableRecord } from "../../engine/key-forwards.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { StorageCorruptionError, StorageFormatVersionError, type TableRecord } from "../types.js";
import { OpfsTree } from "./files.js";
import { OpfsBlockStore } from "./index.js";
import { walStagingTestHooks } from "./leader.js";
import { PowerLossModel } from "./power-loss-model.js";

const NAME = "layout7";
const PREFIX = `minnowdb/${NAME}/`;
const LAYOUT7 = '{"formatVersion":7}';
const LAYOUT8 = '{"formatVersion":8}';
const LAYOUT9 = '{"formatVersion":9}';
const LAYOUT10 = '{"formatVersion":10}';
const LAYOUT11 = '{"formatVersion":11}';
const LAYOUT12 = '{"formatVersion":12}';
const CURRENT_LAYOUT = 13;
const CURRENT = '{"formatVersion":13}';
/** The witness this build writes while it rewrites an older marker. */
const WITNESS = "upgrade-12-13";
/**
 * Every marker-rewrite witness: a 0.13 build's, a 0.14 build's, a 0.15.0 build's, an unreleased
 * layout-11 build's, a 0.16 build's, and this build's.
 */
const WITNESSES = [
  ["upgrade-7-8", '{"from":7,"to":8}'],
  ["upgrade-8-9", '{"from":8,"to":9}'],
  ["upgrade-9-10", '{"from":9,"to":10}'],
  ["upgrade-10-11", '{"from":10,"to":11}'],
  ["upgrade-11-12", '{"from":11,"to":12}'],
  [WITNESS, '{"from":12,"to":13}'],
] as const;
const failure = new DOMException("injected upgrade I/O failure", "QuotaExceededError");

/** The engine's internal key-forwards table, as the engine creates it on its first rekey. */
const KEY_FORWARDS: TableRecord = keyForwardsTableRecord("2026-10-01T12:00:00.000Z");
/** An engine on the fixture's clock, so the forwards table a rekey creates is KEY_FORWARDS. */
const ENGINE_OPTIONS = {
  autoCompact: false,
  autoCollect: false,
  now: () => new Date("2026-10-01T12:00:00.000Z"),
} as const;

interface Fixture {
  files: Record<string, string>;
  expectations: { tables: string[]; blockId: string; blockValues: Array<string | null> };
}

function readFixture(layout: number): Fixture {
  return JSON.parse(
    readFileSync(
      new URL(`../../../format-fixtures/opfs-layout${String(layout)}.json`, import.meta.url),
      "utf8",
    ),
  ) as Fixture;
}

/**
 * Layout 11 was never released, so no writer froze a fixture for it. A layout-11 database is
 * the layout-10 fixture under a layout-11 marker: the bytes the marker step must carry across.
 */
function synthesizedLayout11(): Fixture {
  const layout10 = readFixture(10);
  const files = { ...layout10.files, "minnowdb/native-fixture/format.json": btoa(LAYOUT11) };
  return { ...layout10, files };
}

const fixtures = new Map<number, Fixture>([
  ...[7, 8, 9, 10, 12].map((layout) => [layout, readFixture(layout)] as const),
  [11, synthesizedLayout11()],
]);
function fixtureFor(layout: number): Fixture {
  const fixture = fixtures.get(layout);
  if (fixture === undefined) throw new Error(`No layout-${String(layout)} fixture`);
  return fixture;
}

/** `listTables` order: names compared with `localeCompare`. */
function listed(names: readonly string[]): string[] {
  return [...names].sort((left, right) => left.localeCompare(right));
}

function marker(shim: MemoryOpfs): string {
  return new TextDecoder().decode(shim.readFileBytes(`${PREFIX}format.json`));
}

function rows(version: number, count = 600) {
  return Array.from({ length: count }, (_, id) => ({
    id,
    label: `label-${String((id + version) % 13)}`,
    amount: id * 10 + version,
  }));
}

/** A deterministic shuffle, so the released fold's plan holds a range per replaced cell. */
function shuffled<T>(values: readonly T[]): T[] {
  const result = [...values];
  let state = 17;
  for (let index = result.length - 1; index > 0; index -= 1) {
    state = (state * 1_103_515_245 + 12_345) % 2 ** 31;
    const other = state % (index + 1);
    const value = result[index];
    const swap = result[other];
    if (value === undefined || swap === undefined) continue;
    result[index] = swap;
    result[other] = value;
  }
  return result;
}

/**
 * Leaves the released writer's merge-v1 fold half written: planned, one output block staged and
 * checkpointed, its transaction open. Returns the job and the rows the table must still read.
 */
async function releasedFoldInFlight(shim: MemoryOpfs) {
  const store = await ReleasedOpfsStore.open({ name: NAME, root: shim.root });
  const database = new ReleasedDatabase(store, { autoCompact: false, autoCollect: false });
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
      { name: "amount", type: "number" },
    ],
  });
  await database.insertBatch("items", rows(0));
  await database.upsertBatch("items", shuffled(rows(1)));
  await database.execute("UPDATE items SET label = 'patched' WHERE id % 5 = 0");
  const progress = await database.compactTableStep("items", { maxBlocks: 1 });
  if (progress.jobId === null || progress.result !== null) {
    throw new Error("Expected the released writer to leave a fold in flight");
  }
  const job = (await database.listCompactionJobs("items")).find(
    (candidate) => candidate.id === progress.jobId,
  );
  expect(job?.rewritePlan.kind).toBe("merge-v1");
  expect(job?.outputBlockIds.length).toBeGreaterThan(0);
  const expected = await database.readTable("items");
  await database.close();
  store._crashForTests();
  return { jobId: progress.jobId, expected };
}

function hydrate(source: Fixture): MemoryOpfs {
  const shim = new MemoryOpfs();
  for (const [path, base64] of Object.entries(source.files)) {
    shim.writeFileBytes(
      path.replace("minnowdb/native-fixture/", PREFIX),
      Uint8Array.from(atob(base64), (character) => character.charCodeAt(0)),
    );
  }
  return shim;
}

/** Opens an upgraded fixture and checks the marker, the witnesses, the catalog, and integrity. */
async function verifyFixture(shim: MemoryOpfs, source: Fixture, extra: string[] = []) {
  const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
  try {
    expect(marker(shim)).toBe(CURRENT);
    for (const [name] of WITNESSES) expect(shim.readFileBytes(`${PREFIX}${name}`)).toBeUndefined();
    expect((await store.listTables()).map(({ name }) => name)).toEqual(
      listed([...source.expectations.tables, ...extra]),
    );
    const block = await store.getBlock(source.expectations.blockId);
    expect(block).toBeDefined();
    expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true, issueCount: 0 });
    return store;
  } catch (error) {
    store._crashForTests();
    throw error;
  }
}

function table(name: string): TableRecord {
  return {
    id: name,
    name,
    managed: false,
    columns: [{ id: "value", name: "value", type: "string" as const, nullable: true }],
    revision: 0,
    createdAt: "2026-10-01T12:00:00.000Z",
  };
}

/** Adds the key-forwards table and checks it moves the catalog epoch and not the schema epoch. */
async function addKeyForwards(store: OpfsBlockStore): Promise<void> {
  const before = await store.getCatalogProbe();
  await store.addTable(KEY_FORWARDS);
  const after = await store.getCatalogProbe();
  expect(after.catalogEpoch).toBeGreaterThan(before.catalogEpoch);
  expect(after.schemaEpoch).toBe(before.schemaEpoch);
}

/** Records every write and delete, so a refusal can be shown to have changed nothing. */
function recordWrites(shim: MemoryOpfs): string[] {
  const writes: string[] = [];
  shim.setWriteFault((path, phase) => {
    if (phase !== "flush") writes.push(`${phase}: ${path}`);
  });
  shim.setDeleteFault((path) => writes.push(`delete: ${path}`));
  return writes;
}

function stopRecording(shim: MemoryOpfs): void {
  shim.setWriteFault(null);
  shim.setDeleteFault(null);
}

// ---------------------------------------------------------------------------------------------
// Each frozen older layout through the marker publication
// ---------------------------------------------------------------------------------------------

describe.each([
  { layout: 7, text: LAYOUT7 },
  { layout: 8, text: LAYOUT8 },
  { layout: 9, text: LAYOUT9 },
  { layout: 10, text: LAYOUT10 },
  { layout: 11, text: LAYOUT11 },
  { layout: 12, text: LAYOUT12 },
])("OPFS frozen layout $layout to the current layout", ({ layout, text }) => {
  const source = fixtureFor(layout);
  // Layout 10 and later froze the key-forwards table into the fixture itself.
  const forwardsFrozen = source.expectations.tables.includes(KEY_FORWARDS.name);
  const forwardsAdded = forwardsFrozen ? [] : [KEY_FORWARDS.name];
  const addForwards = async (store: OpfsBlockStore) => {
    if (!forwardsFrozen) await addKeyForwards(store);
  };

  it("upgrades the frozen fixture through the ordinary open API, then writes and reopens", async () => {
    const shim = hydrate(source);
    expect(marker(shim)).toBe(text);
    const store = await verifyFixture(shim, source);
    await store.addTable(table("after-upgrade"));
    await addForwards(store);
    store._crashForTests();
    const reopened = await verifyFixture(shim, source, ["after-upgrade", ...forwardsAdded]);
    expect((await reopened.getTable(KEY_FORWARDS.id))?.keyForwards).toBe(true);
    reopened._crashForTests();
  });

  it("rekeys a row through the ordinary API after the upgrade, and the forward survives", async () => {
    const shim = hydrate(source);
    const store = await verifyFixture(shim, source);
    const database = new MinnowDatabase(store, ENGINE_OPTIONS);
    await database.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, note TEXT)");
    await database.execute(
      "CREATE TABLE lines (id INTEGER PRIMARY KEY, order_id INTEGER REFERENCES orders(id))",
    );
    await database.execute("INSERT INTO orders VALUES (-1, 'offline')");
    await database.execute("INSERT INTO lines VALUES (1, -1)");
    const before = await store.getCatalogProbe();
    await database.execute("UPDATE orders SET id = 500 WHERE id = -1");
    expect((await store.getCatalogProbe()).schemaEpoch).toBe(before.schemaEpoch);
    await database.close();
    store._crashForTests();
    const reopened = await verifyFixture(shim, source, ["lines", "orders", ...forwardsAdded]);
    expect((await reopened.getTable(KEY_FORWARDS.id))?.keyForwards).toBe(true);
    const again = new MinnowDatabase(reopened, ENGINE_OPTIONS);
    expect((await again.query("SELECT id, note FROM orders WHERE id = -1")).rows).toEqual([
      { id: 500, note: "offline" },
    ]);
    expect((await again.query("SELECT order_id FROM lines")).rows).toEqual([{ order_id: 500 }]);
    expect(await again.resolveKey("orders", -1)).toBe(500);
    await again.close();
    reopened._crashForTests();
  });

  it("folds a table across ADD and DROP COLUMN after the upgrade, and reopens", async () => {
    const shim = hydrate(source);
    const store = await verifyFixture(shim, source);
    const database = new MinnowDatabase(store, ENGINE_OPTIONS);
    await database.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT, old INTEGER)");
    await database.execute("INSERT INTO notes VALUES (1, 'a', 1), (2, 'b', 2)");
    await database.execute("UPDATE notes SET old = 20 WHERE id = 2");
    await database.execute("ALTER TABLE notes ADD COLUMN tag TEXT DEFAULT 'none'");
    await database.execute("ALTER TABLE notes DROP COLUMN old");
    await database.execute("INSERT INTO notes VALUES (3, 'c', 'new')");
    const expected = [
      { id: 1, body: "a", tag: "none" },
      { id: 2, body: "b", tag: "none" },
      { id: 3, body: "c", tag: "new" },
    ];
    // Layout 11's record: a fold whose sources lack the added column or keep only their key.
    const folded = await database.compactTable("notes", { minimumLevel0Segments: 1 });
    expect(folded.compacted).toBe(true);
    const job = folded.jobId === undefined ? undefined : await store.getCompactionJob(folded.jobId);
    if (job?.rewritePlan.kind !== "merge-v2") throw new Error("Expected a replayed merge fold");
    expect(job.rewritePlan.sourceSegments.some(({ columns }) => columns.length === 1)).toBe(true);
    expect((await database.query("SELECT * FROM notes ORDER BY id")).rows).toEqual(expected);
    await database.close();
    store._crashForTests();
    const reopened = await verifyFixture(shim, source, ["notes"]);
    const reader = new MinnowDatabase(reopened, ENGINE_OPTIONS);
    expect((await reader.query("SELECT * FROM notes ORDER BY id")).rows).toEqual(expected);
    await reader.close();
    reopened._crashForTests();
  });

  it("merges deltas after the upgrade, and reopens", async () => {
    const shim = hydrate(source);
    const store = await verifyFixture(shim, source);
    const database = new MinnowDatabase(store, ENGINE_OPTIONS);
    await database.execute("CREATE TABLE tallies (id INTEGER PRIMARY KEY, n INTEGER, note TEXT)");
    for (let start = 0; start < 600; start += 200) {
      await database.insertBatch(
        "tallies",
        Array.from({ length: 200 }, (_, index) => ({ id: start + index, n: 0, note: "x" })),
      );
    }
    await database.compactTable("tallies", { partitionRows: 200 });
    for (let id = 0; id < 12; id += 1) {
      await database.execute("UPDATE tallies SET n = n + 1 WHERE id = ?", [id * 41]);
    }
    await database.execute("DELETE FROM tallies WHERE id = 1");
    // Layout 12's record: a delta merge, which takes the deltas' place under their order.
    const progress = await internalsOf(database).automaticCompactionStep("tallies");
    const job =
      progress.result?.jobId === undefined
        ? undefined
        : await store.getCompactionJob(progress.result.jobId);
    expect(job?.rewritePlan.kind).toBe("delta-v1");
    const expected = (await database.query("SELECT * FROM tallies ORDER BY id")).rows;
    expect(expected).toHaveLength(599);
    await database.close();
    store._crashForTests();
    const reopened = await verifyFixture(shim, source, ["tallies"]);
    const reader = new MinnowDatabase(reopened, ENGINE_OPTIONS);
    expect((await reader.query("SELECT * FROM tallies ORDER BY id")).rows).toEqual(expected);
    await reader.close();
    reopened._crashForTests();
  });

  it("declares TIMESTAMP columns after the upgrade, renders them without a zone, and reopens", async () => {
    const shim = hydrate(source);
    const store = await verifyFixture(shim, source);
    const database = new MinnowDatabase(store, ENGINE_OPTIONS);
    // Layout 13's record: a column declared TIMESTAMP without time zone.
    await database.execute(
      "CREATE TABLE stamps (id INTEGER PRIMARY KEY, local_at TIMESTAMP, zoned_at TIMESTAMPTZ)",
    );
    await database.execute(
      "INSERT INTO stamps VALUES (1, '2026-01-02 03:04:05.5', '2026-01-02 03:04:05.5')",
    );
    await database.execute("ALTER TABLE stamps ADD COLUMN later TIMESTAMP");
    await database.execute("UPDATE stamps SET later = local_at + INTERVAL '1 day'");
    const text = "SELECT local_at::text AS l, zoned_at::text AS z, later::text AS a FROM stamps";
    const expected = [
      { l: "2026-01-02 03:04:05.5", z: "2026-01-02 03:04:05.5+00", a: "2026-01-03 03:04:05.5" },
    ];
    expect((await database.query(text)).rows).toEqual(expected);
    await database.close();
    store._crashForTests();
    const reopened = await verifyFixture(shim, source, ["stamps"]);
    const columns = (await reopened.getTableByName("stamps"))?.columns ?? [];
    expect(columns.map(({ name, withoutTimeZone }) => [name, withoutTimeZone])).toEqual([
      ["id", undefined],
      ["local_at", true],
      ["zoned_at", undefined],
      ["later", true],
    ]);
    const reader = new MinnowDatabase(reopened, ENGINE_OPTIONS);
    expect((await reader.query(text)).rows).toEqual(expected);
    await reader.close();
    reopened._crashForTests();
  });

  it("serializes concurrent openers through the marker upgrade", async () => {
    const shim = hydrate(source);
    const stores = await Promise.all(
      Array.from({ length: 4 }, () => OpfsBlockStore.open({ name: NAME, root: shim.root })),
    );
    try {
      expect(marker(shim)).toBe(CURRENT);
      for (const store of stores) {
        expect((await store.listTables()).map(({ name }) => name)).toEqual(
          source.expectations.tables,
        );
      }
      await stores[3]?.addTable(table("concurrent-write"));
      if (!forwardsFrozen) await stores[1]?.addTable(KEY_FORWARDS);
      expect((await stores[0]?.listTables())?.map(({ name }) => name)).toEqual(
        listed([...source.expectations.tables, "concurrent-write", ...forwardsAdded]),
      );
    } finally {
      for (const store of stores) store._crashForTests();
    }
    (await verifyFixture(shim, source, ["concurrent-write", ...forwardsAdded]))._crashForTests();
  });

  for (const mode of ["tab-death", "power-loss"] as const) {
    it(`recovers every marker publication ${mode} boundary`, async () => {
      const trace: string[] = [];
      const traced = hydrate(source);
      traced.setWriteFault((path, phase) => trace.push(`${phase}: ${path}`));
      traced.setDeleteFault((path) => trace.push(`delete: ${path}`));
      (await OpfsBlockStore.open({ name: NAME, root: traced.root }))._crashForTests();
      const boundaries = trace.filter(
        (entry) => entry.endsWith(WITNESS) || entry.endsWith("format.json"),
      );
      // Witness written and flushed, then the marker rewritten (a truncate and a write) and
      // flushed, then the witness removed.
      expect(boundaries).toEqual([
        `create: ${PREFIX}${WITNESS}`,
        `write: ${PREFIX}${WITNESS}`,
        `write: ${PREFIX}${WITNESS}`,
        `flush: ${PREFIX}${WITNESS}`,
        `write: ${PREFIX}format.json`,
        `write: ${PREFIX}format.json`,
        `flush: ${PREFIX}format.json`,
        `delete: ${PREFIX}${WITNESS}`,
      ]);
      for (const [cut, boundary] of boundaries.entries()) {
        const shim = hydrate(source);
        // Repeated entries are cut at their own occurrence, not the first.
        const occurrence = boundaries.slice(0, cut).filter((entry) => entry === boundary).length;
        let seen = 0;
        let armed = false;
        const fault = (path: string, phase: string) => {
          if (armed && `${phase}: ${path}` === boundary && seen++ === occurrence) {
            armed = false;
            throw failure;
          }
        };
        const model = new PowerLossModel(shim, fault);
        // The frozen files are already durable on the device.
        const tree = new OpfsTree(shim.root);
        for (const path of Object.keys(source.files)) {
          const handle = await tree.openHandle(
            path.replace("minnowdb/native-fixture/", PREFIX).split("/"),
            { create: false },
          );
          handle.flush();
          handle.close();
        }
        shim.setDeleteFault((path) => fault(path, "delete"));
        armed = true;
        await expect(
          OpfsBlockStore.open({ name: NAME, root: shim.root }),
          `${mode} at ${boundary}`,
        ).rejects.toThrow();
        if (mode === "power-loss") model.powerLoss();
        stopRecording(shim);
        const resumed = await verifyFixture(shim, source);
        await resumed.addTable(table("after-retry"));
        await addForwards(resumed);
        resumed._crashForTests();
        (await verifyFixture(shim, source, ["after-retry", ...forwardsAdded]))._crashForTests();
      }
    });
  }

  it("finishes a marker torn mid-rewrite when any build's witness survived", async () => {
    // A 0.13 build tearing the marker on its way to layout 8 left the first witness, a 0.14
    // build on its way to 9 the second, a 0.15.0 build on its way to 10 the third, an unreleased
    // layout-11 build on its way to 11 the fourth, a 0.16 build on its way to 12 the fifth, this
    // build the sixth. Each finishes at the current layout.
    for (const [name, bytes] of WITNESSES) {
      const shim = hydrate(source);
      shim.writeFileBytes(`${PREFIX}${name}`, new TextEncoder().encode(bytes));
      shim.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"formatVe'));
      (await verifyFixture(shim, source))._crashForTests();
    }
  });

  it("rewrites a half-written witness beside an intact older marker", async () => {
    for (const [name] of WITNESSES) {
      const shim = hydrate(source);
      shim.writeFileBytes(`${PREFIX}${name}`, new Uint8Array());
      (await verifyFixture(shim, source))._crashForTests();
    }
  });

  it("removes a witness that outlived a finished rewrite", async () => {
    for (const [name] of WITNESSES) {
      const shim = hydrate(source);
      (await OpfsBlockStore.open({ name: NAME, root: shim.root }))._crashForTests();
      shim.writeFileBytes(`${PREFIX}${name}`, new TextEncoder().encode("leftover"));
      (await verifyFixture(shim, source))._crashForTests();
    }
  });

  it("refuses a torn marker whose witness is missing or damaged, without changing files", async () => {
    for (const [name, witness] of [
      [WITNESS, undefined],
      ["upgrade-7-8", new TextEncoder().encode('{"from":7,"to":9}')],
      ["upgrade-8-9", new TextEncoder().encode('{"from":8,"to":10}')],
      ["upgrade-9-10", new TextEncoder().encode('{"from":9,"to":11}')],
      ["upgrade-10-11", new TextEncoder().encode('{"from":10,"to":12}')],
      ["upgrade-11-12", new TextEncoder().encode('{"from":11,"to":13}')],
      [WITNESS, new TextEncoder().encode('{"from":12,"to":14}')],
      [WITNESS, new TextEncoder().encode('{"from":12,"to":1')],
    ] as const) {
      const shim = hydrate(source);
      if (witness !== undefined) shim.writeFileBytes(`${PREFIX}${name}`, witness);
      shim.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"formatVe'));
      const before = shim.readFileBytes(`${PREFIX}checkpoint-b`);
      const writes = recordWrites(shim);
      await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toBeInstanceOf(
        StorageCorruptionError,
      );
      stopRecording(shim);
      expect(writes).toEqual([]);
      expect(shim.readFileBytes(`${PREFIX}checkpoint-b`)).toEqual(before);
      expect(marker(shim)).toBe('{"formatVe');
    }
  });

  it("refuses a layout newer than the current one without changing files", async () => {
    const shim = hydrate(source);
    shim.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"formatVersion":14}'));
    const writes = recordWrites(shim);
    await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toBeInstanceOf(
      StorageFormatVersionError,
    );
    stopRecording(shim);
    expect(writes).toEqual([]);
    expect(marker(shim)).toBe('{"formatVersion":14}');
  });
});

// ---------------------------------------------------------------------------------------------
// Released writers
// ---------------------------------------------------------------------------------------------

describe("OPFS layout 7 to current from the released 0.12.1 writer", () => {
  it("finishes the released writer's fold in flight, then writes and reopens", async () => {
    const shim = new MemoryOpfs();
    const { jobId, expected } = await releasedFoldInFlight(shim);
    expect(marker(shim)).toBe(LAYOUT7);

    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(marker(shim)).toBe(CURRENT);
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    expect(await database.readTable("items")).toEqual(expected);
    let progress = await database.resumeCompactionJob(jobId, { maxBlocks: 2 });
    while (progress.result === null) {
      progress = await database.resumeCompactionJob(jobId, { maxBlocks: 2 });
    }
    expect(progress.result.compacted).toBe(true);
    expect(await database.readTable("items")).toEqual(expected);

    // The current writer's next fold of the same table records a replayed plan.
    await database.upsertBatch("items", shuffled(rows(2)));
    const second = await database.compactTable("items", { minimumLevel0Segments: 1 });
    expect(second.compacted).toBe(true);
    const job = second.jobId === undefined ? undefined : await store.getCompactionJob(second.jobId);
    expect(job?.rewritePlan.kind).toBe("merge-v2");
    const latest = await database.readTable("items");
    expect(latest).toHaveLength(600);
    await database.close();
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    const reader = new MinnowDatabase(reopened, { autoCompact: false, autoCollect: false });
    expect(await reader.readTable("items")).toEqual(latest);
    expect(await reopened.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
    await reader.close();
    reopened._crashForTests();
  });

  it("refuses the released reader after the upgrade without changing a byte", async () => {
    const shim = new MemoryOpfs();
    await releasedFoldInFlight(shim);
    (await OpfsBlockStore.open({ name: NAME, root: shim.root }))._crashForTests();
    const writes = recordWrites(shim);
    await expect(ReleasedOpfsStore.open({ name: NAME, root: shim.root })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: CURRENT_LAYOUT,
      supportedVersion: 7,
      relation: "newer",
    });
    expect(writes).toEqual([]);
    stopRecording(shim);
    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
    store._crashForTests();
  });
});

/** A keyed, indexed table written by the released 0.13.1 build — the last layout-8 writer. */
async function releasedLayout8Database(shim: MemoryOpfs) {
  const store = await Layout8OpfsStore.open({ name: NAME, root: shim.root });
  const database = new Layout8Database(store, { autoCompact: false, autoCollect: false });
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
      { name: "amount", type: "number" },
    ],
  });
  await database.execute("CREATE INDEX items_amount ON items (amount)");
  await database.insertBatch("items", rows(0));
  await database.upsertBatch("items", shuffled(rows(1)));
  const expected = await database.readTable("items");
  await database.close();
  store._crashForTests();
  return expected;
}

async function amountLookup(database: MinnowDatabase, amount: number): Promise<unknown[]> {
  return (await database.query("SELECT id FROM items WHERE amount = ?", { params: [amount] })).rows;
}

describe("OPFS layout 8 to current from the released 0.13.1 writer", () => {
  it("upgrades the released writer's database, then logs a commit too large for one frame", async () => {
    const shim = new MemoryOpfs();
    const expected = await releasedLayout8Database(shim);
    expect(marker(shim)).toBe(LAYOUT8);

    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(marker(shim)).toBe(CURRENT);
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    expect(await database.readTable("items")).toEqual(expected);
    expect(await amountLookup(database, 10 * 7 + 1)).toEqual([{ id: 7 }]);

    // Layout 9's records: a frame spanning continuation pieces, carrying more distinct index
    // values than one layout-8 delta chunk holds.
    const previous = walStagingTestHooks.pieceBytes;
    walStagingTestHooks.pieceBytes = 64 * 1024;
    try {
      await database.insertBatch(
        "items",
        Array.from({ length: 70_000 }, (_, index) => ({
          id: 1_000 + index,
          label: "wide",
          amount: 1_000_000 + index,
        })),
      );
    } finally {
      walStagingTestHooks.pieceBytes = previous;
    }
    const wal = shim.readFileBytes(`${PREFIX}wal`) ?? new Uint8Array();
    expect(new TextDecoder("latin1").decode(wal).split("MNWC").length - 1).toBeGreaterThan(1);
    await database.close();
    store._crashForTests();

    // The continuation frames replay; then a checkpoint holds the large delta, and loads.
    for (const step of ["replay", "checkpoint"] as const) {
      const reopened = await OpfsBlockStore.open({ name: NAME, root: shim.root });
      const reader = new MinnowDatabase(reopened, { autoCompact: false, autoCollect: false });
      expect((await reader.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: 70_600 }]);
      expect(await amountLookup(reader, 1_000_000 + 69_999)).toEqual([{ id: 70_999 }]);
      expect(await amountLookup(reader, 10 * 7 + 1)).toEqual([{ id: 7 }]);
      expect(await reopened.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
      await reader.close();
      if (step === "replay") reopened.close();
      else reopened._crashForTests();
    }
  });

  it("refuses the released reader after the upgrade without changing a byte", async () => {
    const shim = new MemoryOpfs();
    await releasedLayout8Database(shim);
    (await OpfsBlockStore.open({ name: NAME, root: shim.root }))._crashForTests();
    const writes = recordWrites(shim);
    await expect(Layout8OpfsStore.open({ name: NAME, root: shim.root })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: CURRENT_LAYOUT,
      supportedVersion: 8,
      relation: "newer",
    });
    expect(writes).toEqual([]);
    stopRecording(shim);
    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
    store._crashForTests();
  });
});

/**
 * Rows, a secondary index over base and delta postings, and a sequence, written by the released
 * 0.14.1 build — the last layout-9 writer.
 */
async function releasedLayout9Database(shim: MemoryOpfs) {
  const store = await Layout9OpfsStore.open({ name: NAME, root: shim.root });
  const database = new Layout9Database(store, { autoCompact: false, autoCollect: false });
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
      { name: "amount", type: "number" },
    ],
  });
  await database.insertBatch("items", rows(0, 300));
  await database.execute("CREATE INDEX items_amount ON items (amount)");
  await database.insertBatch(
    "items",
    rows(0).filter(({ id }) => id >= 300),
  );
  await database.upsertBatch("items", shuffled(rows(1)));
  await database.execute("DELETE FROM items WHERE id = 3");
  await database.execute("CREATE SEQUENCE item_ids");
  for (let call = 0; call < 3; call += 1) {
    await database.query("SELECT NEXTVAL('item_ids') AS id");
  }
  const expected = await database.readTable("items");
  await database.close();
  store._crashForTests();
  return expected;
}

describe("OPFS layout 9 to current from the released 0.14.1 writer", () => {
  it("upgrades the released writer's database, writes the key-forwards table, and reopens", async () => {
    const shim = new MemoryOpfs();
    const expected = await releasedLayout9Database(shim);
    expect(marker(shim)).toBe(LAYOUT9);

    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(marker(shim)).toBe(CURRENT);
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    expect(await database.readTable("items")).toEqual(expected);
    expect(await amountLookup(database, 10 * 7 + 1)).toEqual([{ id: 7 }]);
    expect(await amountLookup(database, 3 * 10 + 1)).toEqual([]);
    // The sequence continues past the released writer's three calls.
    expect((await database.query("SELECT NEXTVAL('item_ids') AS id")).rows).toEqual([{ id: 4 }]);

    await database.insertBatch(
      "items",
      Array.from({ length: 2_000 }, (_, index) => ({
        id: 10_000 + index,
        label: "after",
        amount: 5_000_000 + index,
      })),
    );
    await database.execute("CREATE TABLE after_upgrade (id INTEGER PRIMARY KEY, value TEXT)");
    await database.execute("INSERT INTO after_upgrade VALUES (1, 'retained')");
    await addKeyForwards(store);
    // An ordinary write after the key-forwards table still commits under the same schema epoch.
    await database.execute("UPDATE items SET label = 'later' WHERE id = 10");
    const latest = await database.readTable("items");
    await database.close();
    store._crashForTests();

    // Replayed from the log; a graceful close checkpoints; then loaded from that checkpoint.
    for (const step of ["replay", "checkpoint"] as const) {
      const reopened = await OpfsBlockStore.open({ name: NAME, root: shim.root });
      const reader = new MinnowDatabase(reopened, { autoCompact: false, autoCollect: false });
      expect(await reader.readTable("items")).toEqual(latest);
      expect(await amountLookup(reader, 5_000_000 + 1_999)).toEqual([{ id: 11_999 }]);
      expect((await reader.query("SELECT id, value FROM after_upgrade")).rows).toEqual([
        { id: 1, value: "retained" },
      ]);
      expect(await reopened.getTable(KEY_FORWARDS.id)).toEqual(KEY_FORWARDS);
      expect(await reopened.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
      await reader.close();
      if (step === "replay") reopened.close();
      else reopened._crashForTests();
    }
    const final = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    const reader = new MinnowDatabase(final, { autoCompact: false, autoCollect: false });
    expect((await reader.query("SELECT NEXTVAL('item_ids') AS id")).rows).toEqual([{ id: 5 }]);
    expect(await final.getTable(KEY_FORWARDS.id)).toEqual(KEY_FORWARDS);
    await reader.close();
    final._crashForTests();
  });

  it("refuses the released reader after the upgrade without changing a byte", async () => {
    const shim = new MemoryOpfs();
    await releasedLayout9Database(shim);
    const upgraded = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    await addKeyForwards(upgraded);
    upgraded._crashForTests();
    const writes = recordWrites(shim);
    await expect(Layout9OpfsStore.open({ name: NAME, root: shim.root })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: CURRENT_LAYOUT,
      supportedVersion: 9,
      relation: "newer",
    });
    // Every older released reader refuses it the same way.
    await expect(Layout8OpfsStore.open({ name: NAME, root: shim.root })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: CURRENT_LAYOUT,
    });
    await expect(ReleasedOpfsStore.open({ name: NAME, root: shim.root })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: CURRENT_LAYOUT,
    });
    expect(writes).toEqual([]);
    stopRecording(shim);
    expect(marker(shim)).toBe(CURRENT);
    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(await store.getTable(KEY_FORWARDS.id)).toEqual(KEY_FORWARDS);
    expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
    store._crashForTests();
  });

  it("lets concurrent openers of the released writer's database share one upgrade", async () => {
    const shim = new MemoryOpfs();
    const expected = await releasedLayout9Database(shim);
    const stores = await Promise.all(
      Array.from({ length: 3 }, () => OpfsBlockStore.open({ name: NAME, root: shim.root })),
    );
    try {
      expect(marker(shim)).toBe(CURRENT);
      for (const store of stores) {
        const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
        expect(await database.readTable("items")).toEqual(expected);
      }
      await stores[2]?.addTable(KEY_FORWARDS);
      expect(await stores[0]?.getTable(KEY_FORWARDS.id)).toEqual(KEY_FORWARDS);
    } finally {
      for (const store of stores) store._crashForTests();
    }
    expect(shim.readFileBytes(`${PREFIX}${WITNESS}`)).toBeUndefined();
  });

  it("finishes a 0.14 build's rewrite to layout 9 torn before its marker became whole", async () => {
    const shim = new MemoryOpfs();
    const expected = await releasedLayout9Database(shim);
    shim.writeFileBytes(`${PREFIX}upgrade-8-9`, new TextEncoder().encode('{"from":8,"to":9}'));
    shim.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"formatVersion"'));
    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(marker(shim)).toBe(CURRENT);
    expect(shim.readFileBytes(`${PREFIX}upgrade-8-9`)).toBeUndefined();
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    expect(await database.readTable("items")).toEqual(expected);
    await database.close();
    store._crashForTests();
  });
});

describe("OPFS current layout fixture", () => {
  it("refuses a current-layout database to every released reader without changing a byte", async () => {
    const current = readFixture(CURRENT_LAYOUT);
    const shim = hydrate(current);
    expect(marker(shim)).toBe(CURRENT);
    const writes = recordWrites(shim);
    for (const [open, supported] of [
      [(root: FileSystemDirectoryHandle) => ReleasedOpfsStore.open({ name: NAME, root }), 7],
      [(root: FileSystemDirectoryHandle) => Layout8OpfsStore.open({ name: NAME, root }), 8],
      [(root: FileSystemDirectoryHandle) => Layout9OpfsStore.open({ name: NAME, root }), 9],
      [(root: FileSystemDirectoryHandle) => Layout10OpfsStore.open({ name: NAME, root }), 10],
      [(root: FileSystemDirectoryHandle) => Layout12OpfsStore.open({ name: NAME, root }), 12],
    ] as const) {
      await expect(open(shim.root)).rejects.toMatchObject({
        name: "StorageFormatVersionError",
        actualVersion: CURRENT_LAYOUT,
        supportedVersion: supported,
        relation: "newer",
      });
    }
    expect(writes).toEqual([]);
    stopRecording(shim);
    const store = await verifyFixture(shim, current);
    expect((await store.getTableByName(KEY_FORWARDS.name))?.keyForwards).toBe(true);
    // Layout 11's record: a fold whose source lacks a column added after it, beside an update
    // left holding only its key.
    const job = await store.getCompactionJob("fixture-layout11-fold");
    expect(job?.state).toBe("aborted");
    const plan = job?.rewritePlan;
    if (plan?.kind !== "merge-v2") throw new Error("The layout-11 fold is missing");
    expect(plan.columns.map(({ columnId }) => columnId)).toEqual(["id", "note"]);
    expect(
      plan.sourceSegments.map(({ kind, columns }) => [kind, columns.map((c) => c.columnId)]),
    ).toEqual([
      ["insert", ["id"]],
      ["update", ["id"]],
    ]);
    // Layout 12's record: a delta merge of two updates into one, under the later's order.
    const delta = await store.getCompactionJob("fixture-layout12-delta");
    expect(delta?.state).toBe("aborted");
    const merge = delta?.rewritePlan;
    if (merge?.kind !== "delta-v1") throw new Error("The layout-12 delta merge is missing");
    expect(merge.logicalOrder).toBe(2);
    expect(merge.sourceSegments.map(({ kind, logicalOrder }) => [kind, logicalOrder])).toEqual([
      ["update", 1],
      ["update", 2],
    ]);
    expect(merge.outputs).toEqual([
      {
        kind: "update",
        columns: [
          { columnId: "id", type: "number" },
          { columnId: "note", type: "string" },
        ],
        rowCount: 1,
        blockRows: [1],
      },
    ]);
    // Layout 13's record: a column declared TIMESTAMP without time zone beside a TIMESTAMPTZ
    // one, in a table the checkpoint holds and in one the log's tail holds.
    for (const name of ["checkpoint-b", "wal-tail"]) {
      const columns = (await store.getTableByName(name))?.columns ?? [];
      expect(
        columns.map(({ name: column, type, withoutTimeZone }) => [column, type, withoutTimeZone]),
        name,
      ).toEqual([
        ["value", "string", undefined],
        ["local_at", "datetime", true],
        ["zoned_at", "datetime", undefined],
      ]);
    }
    store._crashForTests();
  });
});
