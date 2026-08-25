/**
 * How many durable storage commits the engine's simple writes cost. Every IndexedDB readwrite
 * transaction is one — Chromium fsyncs each — so the counts below are the write-path budget:
 * a keyed insert begins (reserving its row ids) and then stages and commits in one step, a
 * point update or delete needs no reservation and is a single step from begin to publish, and
 * the first query after a commit re-pins the shared reader lease in place rather than creating
 * a new one and removing the old one later. A count that grows here is a regression the
 * benchmark gate would only show as a slower browser.
 */
import { IDBFactory } from "fake-indexeddb";
import { expect, it } from "vitest";
import { IndexedDbBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";

/** Wraps a factory so every database it opens logs its transactions by mode and store set. */
function countingFactory(inner: IDBFactory, log: string[]): IDBFactory {
  return {
    open(name: string, version?: number) {
      const request = inner.open(name, version);
      request.addEventListener(
        "success",
        () => {
          const database = request.result;
          const original = database.transaction.bind(database);
          database.transaction = (
            stores: string | string[],
            mode?: IDBTransactionMode,
            options?: IDBTransactionOptions,
          ) => {
            const names = Array.isArray(stores) ? [...stores].sort().join("+") : stores;
            log.push(`${mode === "readwrite" ? "rw" : "ro"}[${names}]`);
            return original(stores, mode, options);
          };
        },
        { once: true },
      );
      return request;
    },
  } as IDBFactory;
}

it("costs the budgeted number of readwrite transactions per simple write and read", async () => {
  const log: string[] = [];
  const store = await IndexedDbBlockStore.open({
    name: "round-trips",
    indexedDB: countingFactory(new IDBFactory(), log),
  });
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "people",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
    ],
  });
  await database.createTable({
    name: "events",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number", defaultValue: { kind: "autoincrement" } },
      { name: "label", type: "string" },
    ],
  });
  await database.insertBatch("people", [{ id: 0, name: "zero" }]);
  await database.query("SELECT COUNT(*) AS n FROM people");

  const readwrite = async (work: () => Promise<unknown>): Promise<string[]> => {
    log.length = 0;
    await work();
    return log.filter((entry) => entry.startsWith("rw"));
  };
  const WRITE =
    "rw[blocks+catalog+leases+manifests+segments+snapshotHeaders+statistics+transactions]";
  const BEGIN = "rw[catalog+leases+manifests+statistics+transactions]";
  const REPIN_READER = "rw[blocks+catalog+leases+manifests+transactions]";

  // Inserts reserve row ids at begin, then stage and commit as one step.
  expect(
    await readwrite(() =>
      database.insertBatch("people", [
        { id: 1, name: "one" },
        { id: 2, name: "two" },
      ]),
    ),
  ).toEqual([BEGIN, WRITE]);
  expect(await readwrite(() => database.insertBatch("events", [{ label: "a" }]))).toEqual([
    BEGIN,
    WRITE,
  ]);
  expect(
    await readwrite(() =>
      database.upsertBatch("people", [
        { id: 2, name: "deux" },
        { id: 3, name: "three" },
      ]),
    ),
  ).toEqual([BEGIN, WRITE]);
  const people = await store.getTableByName("people");
  if (people === undefined) throw new Error("Missing people table");
  expect(
    await store.getExistingUniqueKeys(people.id, ["number:0", "number:1", "number:2", "number:3"]),
  ).toEqual(["number:0", "number:1", "number:2", "number:3"]);
  // The first read after a commit re-pins the shared lease in place; the next read is free.
  expect(await readwrite(() => database.query("SELECT COUNT(*) AS n FROM people"))).toEqual([
    REPIN_READER,
  ]);
  expect(await readwrite(() => database.query("SELECT name FROM people WHERE id = 1"))).toEqual([]);
  // A standalone SQL mutation reads under the deferred transaction's exact manifest pin, then
  // publishes its one bounded artifact batch with the transaction record in one atomic write.
  // A concurrent writer still loses the manifest/schema CAS; no durable staging journal is
  // needed unless a scope grows past one storage-sized batch.
  expect(
    await readwrite(() => database.execute("UPDATE people SET name = 'uno' WHERE id = 1")),
  ).toEqual([WRITE]);
  expect(await readwrite(() => database.update("people", 2, { name: "dos" }))).toEqual([WRITE]);
  expect(await readwrite(() => database.deleteBatch("people", { keys: [2] }))).toEqual([WRITE]);
  // Direct writes moved the shared reader pin behind, so DELETE re-pins once and then publishes.
  expect(await readwrite(() => database.execute("DELETE FROM people WHERE id = 3"))).toEqual([
    REPIN_READER,
    WRITE,
  ]);
  // A delete that matches nothing writes nothing at all.
  expect(await readwrite(() => database.deleteBatch("people", { keys: [404] }))).toEqual([]);
  expect(await database.query("SELECT id, name FROM people ORDER BY id")).toMatchObject({
    rows: [
      { id: 0, name: "zero" },
      { id: 1, name: "uno" },
    ],
  });
  store.close();
});
